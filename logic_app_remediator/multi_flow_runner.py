"""
Multi-workflow failed-run orchestration via Azure Log Analytics (AzureDiagnostics).
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

# Set logger to only show warnings and errors
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# Suppress Azure SDK verbose logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Schema probe query (requested) + robust failed-runs query.
_SCHEMA_PROBE_KUSTO = "AzureDiagnostics | take 1"

# Robust query with all requested fields
_BASE_KUSTO = """
AzureDiagnostics
| extend _rid = tostring(column_ifexists("_ResourceId", ""))
| extend _resourceType = toupper(tostring(column_ifexists("ResourceType", "")))
| extend _status = tostring(column_ifexists("status_s", column_ifexists("Status_s", column_ifexists("status", column_ifexists("Status", "")))))
| extend _workflow =
    tostring(
        coalesce(
            column_ifexists("resource_workflowName_s", ""),
            column_ifexists("workflowName_s", ""),
            column_ifexists("workflowName", ""),
            extract(@"/workflows/([^/]+)", 1, _rid)
        )
    )
| extend _runId =
    tostring(
        coalesce(
            column_ifexists("resource_runId_s", ""),
            column_ifexists("runId_s", ""),
            column_ifexists("runId", ""),
            extract(@"/runs/([^/]+)", 1, _rid)
        )
    )
| extend _trigger =
    tostring(
        coalesce(
            column_ifexists("resource_triggerName_s", ""),
            column_ifexists("triggerName_s", ""),
            column_ifexists("triggerName", ""),
            column_ifexists("TriggerName", "")
        )
    )
| extend _error_message =
    tostring(
        coalesce(
            column_ifexists("error_message_s", ""),
            column_ifexists("ErrorMessage", ""),
            column_ifexists("message", ""),
            column_ifexists("Message", "")
        )
    )
| extend _error_code =
    tostring(
        coalesce(
            column_ifexists("error_code_s", ""),
            column_ifexists("ErrorCode", ""),
            column_ifexists("code", ""),
            column_ifexists("Code", "")
        )
    )
| extend _code = tostring(column_ifexists("code_s", column_ifexists("Code", "")))
| where TimeGenerated > ago({hours}h)
| where _status =~ "Failed"
| where _resourceType == "WORKFLOWS/RUNS" or _rid has "/providers/microsoft.logic/workflows/" and _rid has "/runs/"
| where isnotempty(_workflow) and isnotempty(_runId)
| project 
    TimeGenerated,
    Level = column_ifexists("Level", ""),
    status_s = _status,
    resource_runId_s = _runId,
    resource_workflowName_s = _workflow,
    resource_triggerName_s = _trigger,
    code_s = _code,
    error_code_s = _error_code,
    error_message_s = _error_message,
    _ResourceId = _rid
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
    names: List[str] = []
    for c in table.columns:
        if isinstance(c, str):
            names.append(c)
        else:
            names.append(getattr(c, "name", str(c)))
    if hasattr(row, "__iter__") and not isinstance(row, (str, bytes, dict)):
        return {names[i]: row[i] for i in range(min(len(names), len(row)))}
    if isinstance(row, dict):
        return row
    return {}


def _column_names(table: Any) -> List[str]:
    """Return column names across azure-monitor-query SDK variants."""
    names: List[str] = []
    for c in getattr(table, "columns", []) or []:
        if isinstance(c, str):
            names.append(c)
        else:
            names.append(getattr(c, "name", str(c)))
    return names


def query_failed_runs_from_workspace(
    workspace_id: str,
    hours: int = 1,
    top_n: int = 100,
    settings: Optional[Settings] = None,
) -> List[Dict[str, Any]]:
    """
    Execute the AzureDiagnostics query and return list of dict rows with all fields.
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
        # Schema probe: helps debugging schema drift across workspaces.
        schema_probe = client.query_workspace(
            workspace_id=workspace_id,
            query=_SCHEMA_PROBE_KUSTO,
            timespan=timespan,
        )
        if schema_probe.tables:
            cols = _column_names(schema_probe.tables[0])
            logger.info("AzureDiagnostics probe columns: %s", ", ".join(cols))
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


def _normalize_row(row: Dict[str, Any]) -> Optional[Tuple[str, str, str, str, str, str, str]]:
    """
    Extract (workflow_name, run_id, trigger_name, level, status, error_code, error_message) from query row.
    """
    wf = (
        row.get("resource_workflowName_s")
        or row.get("workflow_name")
        or row.get("workflowName_s")
        or row.get("workflowName")
    )
    rid = (
        row.get("resource_runId_s")
        or row.get("run_id")
        or row.get("resource_runId")
        or row.get("Resource_runId_s")
    )
    trigger = (
        row.get("resource_triggerName_s")
        or row.get("triggerName_s")
        or row.get("triggerName")
        or row.get("TriggerName")
        or ""
    )
    level = row.get("Level") or ""
    status = row.get("status_s") or ""
    error_code = row.get("error_code_s") or row.get("code_s") or ""
    error_msg = row.get("error_message_s") or row.get("error_message") or ""
    
    if wf is None or rid is None:
        return None
    wf_s, rid_s = str(wf).strip(), str(rid).strip()
    if not wf_s or not rid_s:
        return None
    return (wf_s, rid_s, str(trigger), str(level), str(status), str(error_code), str(error_msg))


def _summarize_remediation_result(
    workflow: str, 
    run_id: str, 
    trigger_name: str,
    level: str,
    status: str,
    error_code: str,
    error_message: str,
    result: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Shape one row for the consolidated JSON report with only relevant fields.
    """
    return {
        "Level": level,
        "status_s": status,
        "resource_runId_s": run_id,
        "resource_workflowName_s": workflow,
        "resource_triggerName_s": trigger_name,
        "code_s": result.get("log_error_code_s", error_code),
        "error_code_s": error_code,
        "error_message_s": error_message,
        "remediation_status": result.get("status", ""),
        "fix_applied": result.get("fix_applied", "none"),
        "remediation_detail": result.get("detail") if result.get("detail") else None
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

    # De-duplicate (workflow, run_id) while preserving newest-first order from query
    seen: Set[Tuple[str, str]] = set()
    tasks: List[Tuple[str, str, str, str, str, str, str]] = []
    for row in rows:
        parsed = _normalize_row(row)
        if not parsed:
            logger.debug("Skipping unparsable row: %s", row)
            continue
        wf, rid, trigger, level, status, error_code, error_msg = parsed
        key = (wf, rid)
        if key in seen:
            continue
        seen.add(key)
        tasks.append((wf, rid, trigger, level, status, error_code, error_msg))
        logger.info(
            "Detected failed run from logs: workflow=%s run_id=%s trigger=%s error_code=%s",
            wf, rid, trigger, error_code,
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
                "status": "runner_error",
                "fix_applied": "none",
                "detail": str(ex),
            }

    workers = max(1, int(max_concurrency))
    if workers == 1 or total_failed <= 1:
        for wf, rid, trigger, level, status, error_code, error_msg in tasks:
            out = _one(wf, rid)
            results.append(_summarize_remediation_result(wf, rid, trigger, level, status, error_code, error_msg, out))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {
                pool.submit(_one, wf, rid): (wf, rid, trigger, level, status, error_code, error_msg) 
                for wf, rid, trigger, level, status, error_code, error_msg in tasks
            }
            for fut in as_completed(future_map):
                wf, rid, trigger, level, status, error_code, error_msg = future_map[fut]
                try:
                    out = fut.result()
                except Exception as ex:
                    logger.exception("Future failed for workflow=%s run_id=%s", wf, rid)
                    out = {
                        "status": "runner_error",
                        "fix_applied": "none",
                        "detail": str(ex),
                    }
                results.append(_summarize_remediation_result(wf, rid, trigger, level, status, error_code, error_msg, out))

    # Preserve deterministic order by workflow + run_id for reporting
    results.sort(key=lambda x: (x.get("resource_workflowName_s") or "", x.get("resource_runId_s") or ""))

    return {
        "total_failed_runs": total_failed,
        "processed": len(results),
        "results": results,
    }


def collect_failed_run_errors(
    *,
    workspace_id: str,
    hours: int = 1,
    top_n: int = 100,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    """
    Log-only mode: read failed runs from AzureDiagnostics and display error fields
    without invoking run_remediation().
    """
    settings = settings or get_settings()
    try:
        rows = query_failed_runs_from_workspace(
            workspace_id=workspace_id, hours=hours, top_n=top_n, settings=settings
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

    seen: Set[Tuple[str, str]] = set()
    results: List[Dict[str, Any]] = []
    for row in rows:
        parsed = _normalize_row(row)
        if not parsed:
            continue
        wf, rid, trigger, level, status, error_code, error_msg = parsed
        key = (wf, rid)
        if key in seen:
            continue
        seen.add(key)
        logger.info(
            "Detected failed run from logs: workflow=%s run_id=%s error_code=%s error_message=%s",
            wf, rid, error_code, error_msg,
        )
        results.append(
            {
                "Level": level,
                "status_s": status,
                "resource_runId_s": rid,
                "resource_workflowName_s": wf,
                "resource_triggerName_s": trigger,
                "code_s": error_code,
                "error_code_s": error_code,
                "error_message_s": error_msg,
                "remediation_status": "detected_from_logs",
            }
        )

    return {
        "total_failed_runs": len(results),
        "processed": len(results),
        "results": results,
    }