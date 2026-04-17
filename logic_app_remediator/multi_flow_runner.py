"""
Multi-workflow failed-run orchestration via Azure Log Analytics (AzureDiagnostics).

Queries failed Logic App runs from a Log Analytics workspace, then delegates each
row to the existing ``run_remediation()`` — no changes to core remediation logic.

Prerequisites:
  - A Log Analytics workspace receiving Logic Apps diagnostic logs (AzureDiagnostics).
  - Caller identity with ``Log Analytics Reader`` (query) and workflow permissions
    for ``run_remediation`` (read runs + optional write for remediation).
  - ``LOG_ANALYTICS_WORKSPACE_ID`` env var OR pass ``workspace_id`` explicitly.
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential
from azure.monitor.query import LogsQueryClient, LogsQueryStatus

from logic_app_remediator.agent import run_remediation
from logic_app_remediator.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Default Kusto query (time window and row cap parameterized below).
_BASE_KUSTO = """
AzureDiagnostics
| where ResourceType == "WORKFLOWS/RUNS"
| where status_s == "Failed"
| where TimeGenerated > ago({hours}h)
| project workflowName_s, resource_runId_s, TimeGenerated
| order by TimeGenerated desc
| take {top_n}
"""


def _build_kusto_query(hours: int, top_n: int) -> str:
    hours = max(1, min(int(hours), 168))
    top_n = max(1, min(int(top_n), 5000))
    return _BASE_KUSTO.format(hours=hours, top_n=top_n).strip()


def _credential_for_logs(settings: Settings):
    """Use only service principal credentials loaded from environment."""
    if not (settings.tenant_id and settings.client_id and settings.client_secret):
        raise ValueError(
            "Missing service principal credentials for Log Analytics query. Set "
            "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET."
        )
    return ClientSecretCredential(
        settings.tenant_id, settings.client_id, settings.client_secret
    )


def _row_to_dict(table: Any, row: Any) -> Dict[str, Any]:
    """Convert a Logs table row to a dict keyed by column name."""
    names = [c.name for c in table.columns]
    if hasattr(row, "__iter__") and not isinstance(row, (str, bytes, dict)):
        return {names[i]: row[i] for i in range(min(len(names), len(row)))}
    if isinstance(row, dict):
        return row
    return {}


def query_failed_runs_from_workspace(
    workspace_id: str,
    hours: int = 1,
    top_n: int = 100,
    settings: Optional[Settings] = None,
) -> List[Dict[str, Any]]:
    """
    Execute the AzureDiagnostics query and return list of dict rows:
    workflowName_s, resource_runId_s, TimeGenerated (when present).
    """
    settings = settings or get_settings()
    cred = _credential_for_logs(settings)
    client = LogsQueryClient(cred)
    query = _build_kusto_query(hours=hours, top_n=top_n)
    timespan = timedelta(hours=hours + 1)

    logger.info(
        "Executing Log Analytics query (workspace=%s, timespan=%sh)",
        workspace_id[:8] + "...",
        hours,
    )
    try:
        response = client.query_workspace(
            workspace_id=workspace_id,
            query=query,
            timespan=timespan,
        )
    except HttpResponseError as e:
        logger.error("Log Analytics query failed: %s", e)
        raise

    if response.status == LogsQueryStatus.PARTIAL:
        logger.warning(
            "Log Analytics returned PARTIAL results: %s",
            getattr(response, "partial_error", None),
        )
    if not response.tables:
        logger.info("Log Analytics returned no tables (no matching rows).")
        return []

    table = response.tables[0]
    rows_out: List[Dict[str, Any]] = []
    for row in table.rows:
        d = _row_to_dict(table, row)
        rows_out.append(d)
    logger.info("Log Analytics returned %s row(s)", len(rows_out))
    return rows_out


def _normalize_row(row: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Extract (workflow_name, run_id) from a query row.
    Column names may vary slightly; try common variants.
    """
    wf = (
        row.get("workflowName_s")
        or row.get("workflowName")
        or row.get("WorkflowName_s")
    )
    rid = (
        row.get("resource_runId_s")
        or row.get("resource_runId")
        or row.get("Resource_runId_s")
    )
    if wf is None or rid is None:
        return None
    wf_s, rid_s = str(wf).strip(), str(rid).strip()
    if not wf_s or not rid_s:
        return None
    return wf_s, rid_s


def _summarize_remediation_result(
    workflow: str, run_id: str, result: Dict[str, Any]
) -> Dict[str, Any]:
    """Shape one row for the consolidated JSON report."""
    return {
        "workflow": workflow,
        "run_id": run_id,
        "status": result.get("status", ""),
        "error_type": result.get("error_type", ""),
        "exact_error_code": result.get("exact_error_code", ""),
        "exact_error_message": result.get("exact_error_message", ""),
        "root_cause": result.get("root_cause", ""),
        "fix_applied": result.get("fix_applied", ""),
        "recommendation": result.get("recommendation", ""),
        "failed_action": result.get("failed_action"),
        "arm_error_code": result.get("arm_error_code"),
        "detail": result.get("detail"),
    }


def process_failed_runs(
    subscription_id: str,
    resource_group: str,
    *,
    workspace_id: Optional[str] = None,
    hours: int = 1,
    top_n: int = 100,
    max_concurrency: int = 4,
    settings: Optional[Settings] = None,
    backup_dir: Optional[str] = None,
    trigger_name: Optional[str] = None,
    remediation_fn: Optional[
        Callable[..., Dict[str, Any]]
    ] = None,
) -> Dict[str, Any]:
    """
    Discover failed runs from Log Analytics and run the existing remediation agent per row.

    Parameters
    ----------
    subscription_id, resource_group
        Passed through to ``run_remediation`` for each failed run.
    workspace_id
        Log Analytics workspace GUID. If omitted, uses env ``LOG_ANALYTICS_WORKSPACE_ID``.
    hours
        Lookback window for ``ago(Nh)`` in the Kusto query (default 1, max 168).
    top_n
        Maximum rows returned from the query (default 100, capped at 5000).
    max_concurrency
        Parallel remediation calls (default 4). Set to 1 for strictly sequential.
    settings, backup_dir, trigger_name
        Forwarded to ``run_remediation`` unchanged.
    remediation_fn
        Injectable hook for tests; defaults to ``run_remediation``.

    Returns
    -------
    Consolidated report dict with total_failed_runs, processed, results, errors_skipped.
    """
    settings = settings or get_settings()
    ws = (workspace_id or os.getenv("LOG_ANALYTICS_WORKSPACE_ID") or "").strip()
    if not ws:
        raise ValueError(
            "Log Analytics workspace id is required: pass workspace_id=... "
            "or set LOG_ANALYTICS_WORKSPACE_ID in the environment."
        )

    fn = remediation_fn or run_remediation
    try:
        rows = query_failed_runs_from_workspace(
            workspace_id=ws, hours=hours, top_n=top_n, settings=settings
        )
    except Exception as ex:
        logger.error("Failed to query Log Analytics: %s", ex)
        return {
            "total_failed_runs": 0,
            "processed": 0,
            "results": [],
            "status": "log_query_failed",
            "detail": str(ex),
        }

    # De-duplicate (workflow, run_id) while preserving newest-first order from query.
    seen: Set[Tuple[str, str]] = set()
    tasks: List[Tuple[str, str]] = []
    for row in rows:
        parsed = _normalize_row(row)
        if not parsed:
            logger.debug("Skipping unparsable row: %s", row)
            continue
        key = parsed
        if key in seen:
            continue
        seen.add(key)
        tasks.append(parsed)
        wf, rid = parsed
        logger.info(
            "Detected failed run from logs: workflowName_s=%s resource_runId_s=%s",
            wf,
            rid,
        )

    total_failed = len(tasks)
    results: List[Dict[str, Any]] = []

    def _one(workflow_name: str, run_id: str) -> Dict[str, Any]:
        try:
            return fn(
                subscription_id=subscription_id,
                resource_group=resource_group,
                workflow_name=workflow_name,
                run_id=run_id,
                settings=settings,
                backup_dir=backup_dir,
                trigger_name=trigger_name,
            )
        except Exception as ex:
            logger.exception(
                "run_remediation failed for workflow=%s run_id=%s", workflow_name, run_id
            )
            return {
                "workflow": workflow_name,
                "run_id": run_id,
                "status": "runner_error",
                "error_type": "",
                "root_cause": "",
                "fix_applied": "none",
                "recommendation": "",
                "detail": str(ex),
            }

    workers = max(1, int(max_concurrency))
    if workers == 1 or total_failed <= 1:
        for wf, rid in tasks:
            out = _one(wf, rid)
            results.append(_summarize_remediation_result(wf, rid, out))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {
                pool.submit(_one, wf, rid): (wf, rid) for wf, rid in tasks
            }
            for fut in as_completed(future_map):
                wf, rid = future_map[fut]
                try:
                    out = fut.result()
                except Exception as ex:
                    logger.exception("Future failed for workflow=%s run_id=%s", wf, rid)
                    out = {
                        "status": "runner_error",
                        "error_type": "",
                        "root_cause": "",
                        "fix_applied": "none",
                        "recommendation": "",
                        "detail": str(ex),
                    }
                results.append(_summarize_remediation_result(wf, rid, out))

    # Preserve deterministic order by workflow + run_id for reporting
    results.sort(key=lambda x: (x.get("workflow") or "", x.get("run_id") or ""))

    return {
        "total_failed_runs": total_failed,
        "processed": len(results),
        "results": results,
    }
