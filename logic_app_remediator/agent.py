"""
Orchestrates fetch → analyze → remediate → re-run → structured output.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from logic_app_remediator.analyzer import analyze_error
from logic_app_remediator.api import (
    find_manual_or_recurrence_trigger,
    get_run,
    get_workflow,
    list_run_actions,
    post_trigger_run,
    put_workflow,
)
from logic_app_remediator.auth import get_arm_token
from logic_app_remediator.config import Settings, get_settings
from logic_app_remediator.remediation import (
    apply_remediation_patch,
    locate_action_node,
    strip_read_only_for_put,
)

# Set logger to only show warnings and errors
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# Scope / control actions: failure here is usually a rollup; real fix targets leaf actions.
_CONTAINER_TYPES = frozenset(
    {
        "scope",
        "foreach",
        "until",
        "if",
        "switch",
        "parallel",
        "parallelbranch",
    }
)

_NON_RETRYABLE_ARM_CODES = frozenset(
    {
        "ReadOnlyDisabledSubscription",
        "SubscriptionNotRegistered",
        "AuthorizationFailed",
    }
)


def _end_time(action: Dict[str, Any]) -> str:
    return (action.get("properties") or {}).get("endTime") or ""


def _deep_find_status_code(obj: Any, depth: int = 0) -> Optional[int]:
    """Find first numeric statusCode in nested run outputs (HTTP actions nest deeply)."""
    if depth > 14:
        return None
    if isinstance(obj, dict):
        sc = obj.get("statusCode")
        if isinstance(sc, int) and 100 <= sc <= 599:
            return sc
        if isinstance(sc, str) and sc.isdigit():
            v = int(sc)
            if 100 <= v <= 599:
                return v
        for v in obj.values():
            hit = _deep_find_status_code(v, depth + 1)
            if hit is not None:
                return hit
    elif isinstance(obj, list):
        for it in obj:
            hit = _deep_find_status_code(it, depth + 1)
            if hit is not None:
                return hit
    return None


def _score_failed_action(
    action: Dict[str, Any], definition: Optional[Dict[str, Any]]
) -> int:
    """
    Higher score = better target for auto-remediation (leaf HTTP errors, not scope rollups).
    """
    blob = _extract_error_blob(action)
    score = 0
    if blob.get("statusCode") is not None:
        score += 200
    msg = str(blob.get("message") or "")
    if "No dependent actions succeeded" in msg:
        score -= 180
    elif blob.get("code") == "ActionFailed" and "An action failed" in msg and len(msg) < 220:
        score -= 120
    if blob.get("code") == "ActionFailed" and blob.get("statusCode") is None:
        score -= 25

    if not definition:
        return score

    name = _action_display_name(action)
    try:
        _, node = locate_action_node(definition, name)
        t = (node.get("type") or "").lower()
        if t in ("http", "httpwebhook"):
            score += 90
        if t in _CONTAINER_TYPES:
            score -= 110
    except KeyError:
        pass
    return score


def _pick_failed_action(
    actions: List[Dict[str, Any]],
    definition: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    failed: List[Dict[str, Any]] = []
    for a in actions:
        props = a.get("properties") or {}
        if (props.get("status") or "").lower() == "failed":
            failed.append(a)
    if not failed:
        return None
    if len(failed) > 1:
        for a in failed:
            n = _action_display_name(a)
            logger.debug(
                "failed candidate %s score=%s",
                n,
                _score_failed_action(a, definition),
            )
    # Prefer highest specificity; tie-break by latest endTime
    return max(
        failed,
        key=lambda a: (_score_failed_action(a, definition), _end_time(a)),
    )


def _action_display_name(action_resource: Dict[str, Any]) -> str:
    raw = action_resource.get("name") or ""
    if "/" in raw:
        return raw.rsplit("/", 1)[-1]
    return raw


def _extract_error_blob(action_resource: Dict[str, Any]) -> Dict[str, Any]:
    props = action_resource.get("properties") or {}
    merged: Dict[str, Any] = {}

    def absorb(obj: Any) -> None:
        if not isinstance(obj, dict):
            return
        for k, v in obj.items():
            if v is not None and k not in merged:
                merged[k] = v

    absorb(props.get("error"))
    out = props.get("outputs")
    if isinstance(out, dict):
        absorb(out.get("error"))
        body = out.get("body")
        if isinstance(body, dict):
            absorb(body.get("error"))
            if "statusCode" in body:
                merged.setdefault("statusCode", body.get("statusCode"))
            if "message" in body and "message" not in merged:
                merged["message"] = body.get("message")
        deep_sc = _deep_find_status_code(out)
        if deep_sc is not None:
            merged.setdefault("statusCode", deep_sc)

    if not merged:
        merged["message"] = props.get("status") or "Unknown failure"
    return merged


def _summarize_detected_error(
    action_name: str, error_json: Dict[str, Any], analysis: Dict[str, Any]
) -> str:
    return (
        f"action={action_name} type={analysis.get('error_type')} "
        f"code={error_json.get('code')} status={error_json.get('statusCode')} "
        f"msg={str(error_json.get('message'))[:200]}"
    )


def _build_flow_context(
    failed: Dict[str, Any],
    workflow_name: str,
    run_id: str,
    definition: Optional[Dict[str, Any]],
    action_name: str,
    preview_limit: int = 6000,
) -> Dict[str, Any]:
    """Live run + definition slice passed into RAG (ground truth for the flow)."""
    props = failed.get("properties") or {}
    ctx: Dict[str, Any] = {
        "workflow_name": workflow_name,
        "run_id": run_id,
        "failed_action_name": action_name,
        "action_status": props.get("status"),
        "action_start_time": props.get("startTime"),
        "action_end_time": props.get("endTime"),
    }
    if definition and action_name:
        try:
            _, node = locate_action_node(definition, action_name)
            ctx["action_type"] = node.get("type")
        except KeyError:
            ctx["action_type"] = None
    else:
        ctx["action_type"] = None

    for key in ("inputs", "outputs"):
        raw = props.get(key)
        blob = json.dumps(raw, default=str) if raw is not None else ""
        if len(blob) > preview_limit:
            blob = blob[:preview_limit] + "...(truncated)"
        ctx[f"action_{key}_preview"] = blob
    return ctx


def _analysis_extras(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Fields to echo in API/CLI output for RAG and exact flow errors."""
    return {
        k: analysis.get(k)
        for k in (
            "exact_error_in_flow",
            "exact_error_code",
            "exact_error_message",
            "root_cause",
            "rag_enriched",
            "rag_retrieval_mode",
            "retrieved_sources",
            "rag_cited_source_ids",
        )
        if analysis.get(k) is not None
    }


def _extract_arm_error_code(resp_text: str) -> Optional[str]:
    if not resp_text:
        return None
    try:
        data = json.loads(resp_text)
        err = data.get("error")
        if isinstance(err, dict):
            code = err.get("code")
            if code:
                return str(code)
    except Exception:
        return None
    return None


def run_remediation(
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
    settings: Optional[Settings] = None,
    backup_dir: Optional[str] = None,
    trigger_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    End-to-end remediation with max attempts from settings.

    Returns structured result dict (see module docstring / CLI).
    """
    settings = settings or get_settings()
    token = get_arm_token(
        settings.tenant_id, settings.client_id, settings.client_secret
    )

    run = get_run(
        token, subscription_id, resource_group, workflow_name, run_id
    )
    run_status = (run.get("properties") or {}).get("status") or ""

    actions = list_run_actions(
        token, subscription_id, resource_group, workflow_name, run_id
    )
    wf_early = get_workflow(token, subscription_id, resource_group, workflow_name)
    definition_early = (wf_early.get("properties") or {}).get("definition")
    if not isinstance(definition_early, dict):
        definition_early = None

    failed = _pick_failed_action(actions, definition_early)
    if not failed:
        return {
            "run_id": run_id,
            "error_type": "none",
            "fix_applied": "none",
            "status": "no_error",
            "workflow_run_status": run_status,
            "message": "No error in flow.",
        }

    action_name = _action_display_name(failed)
    error_json = _extract_error_blob(failed)
    flow_context = _build_flow_context(
        failed, workflow_name, run_id, definition_early, action_name
    )
    analysis = analyze_error(error_json, settings, flow_context=flow_context)
    error_type = analysis.get("error_type") or "unknown"

    if (
        error_type == "unknown"
        and definition_early
        and _score_failed_action(failed, definition_early) < -50
    ):
        logger.warning(
            "Best failed action still looks like a container rollup (%s). "
            "If remediation stays unknown, run with --log-level DEBUG, "
            "enable Azure OpenAI in .env, or fix the inner HTTP/action shown in the portal.",
            action_name,
        )

    logger.info("Detected: %s", _summarize_detected_error(action_name, error_json, analysis))
    logger.info("Analysis: %s", json.dumps(analysis, default=str))

    if error_type == "401" and not (settings.auth_header_value or "").strip():
        logger.warning(
            "401 detected but REMEDIATION_AUTH_HEADER_VALUE is empty; "
            "workflow will not receive new auth headers until configured."
        )

    if error_type == "unknown":
        return {
            "run_id": run_id,
            "error_type": error_type,
            "fix_applied": "skipped",
            "status": "needs_manual_review",
            "workflow_run_status": run_status,
            "failed_action": action_name,
            "recommendation": analysis.get("recommendation"),
            **_analysis_extras(analysis),
        }

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path: Optional[str] = None

    last_result: Dict[str, Any] = {}
    force_wildcard_etag = False
    for attempt in range(1, settings.max_remediation_attempts + 1):
        wf = get_workflow(token, subscription_id, resource_group, workflow_name)
        if backup_dir:
            os.makedirs(backup_dir, exist_ok=True)
            backup_path = os.path.join(
                backup_dir, f"workflow_backup_{workflow_name}_{ts}_a{attempt}.json"
            )
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(wf, f, indent=2)
            logger.info("Backed up workflow to %s", backup_path)
        else:
            backup_path = None

        put_body = strip_read_only_for_put(wf)
        etag = "*" if force_wildcard_etag else wf.get("etag")
        try:
            patched = apply_remediation_patch(
                put_body, action_name, error_type, settings, analysis=analysis
            )
        except Exception as ex:
            logger.exception("Patch failed")
            return {
                "run_id": run_id,
                "error_type": error_type,
                "fix_applied": "patch_failed",
                "status": "failed",
                "failed_action": action_name,
                "detail": str(ex),
                "backup_path": backup_path,
                **_analysis_extras(analysis),
            }

        logger.info(
            "Applying fix %s to action %s (attempt %s/%s)",
            analysis.get("fix_type"),
            action_name,
            attempt,
            settings.max_remediation_attempts,
        )
        try:
            put_workflow(
                token,
                subscription_id,
                resource_group,
                workflow_name,
                patched,
                etag=etag,
            )
        except requests.HTTPError as he:
            resp_text = ""
            if he.response is not None and he.response.text:
                resp_text = he.response.text[:1000]
            arm_code = _extract_arm_error_code(resp_text)
            logger.warning(
                "PUT workflow failed (attempt %s): %s body=%s",
                attempt,
                he,
                resp_text,
            )
            if arm_code in _NON_RETRYABLE_ARM_CODES:
                return {
                    "run_id": run_id,
                    "error_type": error_type,
                    "fix_applied": analysis.get("fix_type", ""),
                    "status": "subscription_read_only" if arm_code == "ReadOnlyDisabledSubscription" else "deploy_blocked",
                    "failed_action": action_name,
                    "detail": str(he),
                    "error_body": resp_text,
                    "arm_error_code": arm_code,
                    "backup_path": backup_path,
                    "http_status": getattr(he.response, "status_code", None),
                    **_analysis_extras(analysis),
                }
            if attempt < settings.max_remediation_attempts and he.response is not None:
                code = he.response.status_code
                if code in (409, 412, 429) or 500 <= code < 600:
                    if code in (409, 412):
                        # Conflict/precondition: relax match to wildcard on next attempt.
                        force_wildcard_etag = True
                    time.sleep(3 * attempt)
                    continue
            return {
                "run_id": run_id,
                "error_type": error_type,
                "fix_applied": analysis.get("fix_type", ""),
                "status": "deploy_failed",
                "failed_action": action_name,
                "detail": str(he),
                "error_body": resp_text,
                "backup_path": backup_path,
                "http_status": getattr(he.response, "status_code", None),
                **_analysis_extras(analysis),
            }

        definition = (
            patched.get("properties", {}).get("definition") or {}
        )
        trig = trigger_name or find_manual_or_recurrence_trigger(definition)
        new_run_status = "trigger_skipped"
        new_run_id = None
        if trig:
            resp = post_trigger_run(
                token,
                subscription_id,
                resource_group,
                workflow_name,
                trig,
                body={},
            )
            if resp.status_code in (200, 202):
                try:
                    loc = resp.headers.get("Location") or ""
                    parts = [p for p in loc.rstrip("/").split("/") if p]
                    if parts:
                        new_run_id = parts[-1]
                except Exception:
                    pass
                new_run_status = "trigger_accepted"
            else:
                new_run_status = f"trigger_http_{resp.status_code}"
                logger.warning("Trigger run response: %s %s", resp.status_code, resp.text[:500])
        else:
            logger.warning("No trigger found; workflow updated but not re-run.")

        fix_desc = f"{analysis.get('fix_type')} on {action_name}"
        last_result = {
            "run_id": run_id,
            "error_type": error_type,
            "fix_applied": fix_desc,
            "status": "remediated",
            "workflow_run_status": run_status,
            "failed_action": action_name,
            "backup_path": backup_path,
            "new_trigger_status": new_run_status,
            "new_run_id": new_run_id,
            "remediation_attempt": attempt,
            "recommendation": analysis.get("recommendation"),
            **_analysis_extras(analysis),
        }
        logger.info("Result: %s", json.dumps(last_result, default=str))

        # Single successful deploy + trigger completes one attempt; no auto-loop on new run unless requested
        break

    return last_result
