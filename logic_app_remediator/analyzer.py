"""
Error analysis with dynamic recommendation generation and optional Azure OpenAI enrichment.
"""

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from logic_app_remediator.config import Settings, get_settings


def _infer_http_status_from_text(text: str) -> Optional[int]:
    """Pull 3-digit HTTP status from common Logic Apps / connector phrasing."""
    if not text:
        return None
    m = re.search(
        r"(?:status code|returned|response code|http)\s*[:=]?\s*(\d{3})\b",
        text,
        re.I,
    )
    if m:
        return int(m.group(1))
    m = re.search(r"\b(40[0-9]|41[0-9]|42[0-9]|43[0-9]|44[0-9]|45[0-9]|50[0-9]|502|503|504)\b", text)
    if m:
        return int(m.group(1))
    return None


def _text_blob(err: Any) -> str:
    if err is None:
        return ""
    if isinstance(err, str):
        return err
    try:
        return json.dumps(err, default=str)
    except TypeError:
        return str(err)


def _extract_signals(error_json: Dict[str, Any], message: str) -> Dict[str, Any]:
    """
    Extract actionable, real-time hints from current error payload text.
    """
    blob = _text_blob(error_json).replace("\\/", "/")

    def find(pattern: str, src: str, flags: int = re.I) -> Optional[str]:
        m = re.search(pattern, src, flags)
        if not m:
            return None
        # Some patterns use a capture group, others match the full token.
        if m.lastindex and m.lastindex >= 1:
            return m.group(1).strip()
        return m.group(0).strip()

    url = find(r"https?://[^\s\"'<>]+", blob, flags=re.I)
    method = find(r"\b(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b", blob, flags=re.I)
    timeout_value = (
        find(r"(?:timed?\s*out\s*after|timeout(?:\s*of)?)\s*[:=]?\s*([0-9]+(?:ms|s|m)?)", message)
        or find(r'"timeout"\s*:\s*"([^"]+)"', blob)
    )
    missing_field = (
        find(r"(?:missing|required)\s+(?:field|property|parameter)\s*[:=]?\s*['\"]?([A-Za-z0-9_.-]+)", message)
        or find(r"'([A-Za-z0-9_.-]+)'\s+is required", message)
        or find(r'"([A-Za-z0-9_.-]+)"\s*:\s*\[\s*"is required"', blob)
    )

    auth_hint = (
        "token"
        if re.search(r"\b(token|jwt|bearer|signature)\b", message, re.I)
        else "rbac"
        if re.search(r"\b(forbidden|insufficient|permission|scope|role)\b", message, re.I)
        else None
    )

    return {
        "url": url,
        "method": method,
        "timeout_value": timeout_value,
        "missing_field": missing_field,
        "auth_hint": auth_hint,
    }


def _dynamic_recommendation(error_type: str, signals: Dict[str, Any]) -> str:
    """
    Build recommendation from live run evidence instead of static templates.
    """
    url = signals.get("url")
    method = signals.get("method")
    timeout_value = signals.get("timeout_value")
    missing_field = signals.get("missing_field")
    auth_hint = signals.get("auth_hint")

    parts = []
    if error_type == "404":
        if url:
            parts.append(f"Target endpoint currently failing: {url}.")
            parts.append("Verify host/path and API version, then switch to a known-good fallback endpoint if needed.")
        else:
            parts.append("Endpoint appears unresolved (404). Validate URI host/path and API route mapping.")
    elif error_type == "401":
        if auth_hint == "rbac":
            parts.append("Authorization failure detected; validate RBAC role assignment and token scope/audience.")
        else:
            parts.append("Authentication failure detected; refresh token/API key or connection secret.")
        if method or url:
            parts.append(f"Failing call context: method={method or 'unknown'}, url={url or 'unknown'}.")
    elif error_type == "timeout":
        parts.append("Call timed out; increase request timeout and apply bounded retry policy.")
        if timeout_value:
            parts.append(f"Observed timeout signal: {timeout_value}.")
        if url:
            parts.append(f"Investigate latency/dependency for endpoint {url}.")
    elif error_type == "bad_request":
        parts.append("Payload/schema mismatch detected (400); validate request body against API contract.")
        if missing_field:
            parts.append(f"Populate required field: {missing_field}.")
        if method or url:
            parts.append(f"Failing call context: method={method or 'unknown'}, url={url or 'unknown'}.")
    else:
        parts.append("No deterministic pattern matched; inspect action inputs/outputs and connector-specific diagnostics.")
        if method or url:
            parts.append(f"Current call context: method={method or 'unknown'}, url={url or 'unknown'}.")

    return " ".join(parts)


def _root_cause_from_exact(code: str, message: str) -> str:
    c = (code or "").upper()
    m = (message or "").upper()
    if "UNRESOLVABLEHOSTNAME" in c or "COULD NOT BE RESOLVED" in m or "NAME OR SERVICE NOT KNOWN" in m:
        return "dns_resolution_error"
    if "CONNECTIONREFUSED" in c or "ECONNREFUSED" in m:
        return "connection_refused"
    if "CERTIFICATE" in c or "SSL" in m or "TLS" in m:
        return "tls_or_certificate_error"
    if "THROTTL" in c or "429" in m:
        return "throttling"
    if "UNAUTHORIZED" in c or "FORBIDDEN" in c:
        return "auth_or_authorization_error"
    if "TIMEOUT" in c or "TIMED OUT" in m:
        return "timeout"
    if "BADREQUEST" in c or "INVALID" in c:
        return "payload_or_schema_error"
    if "NOTFOUND" in c or "NOT FOUND" in m:
        return "not_found"
    return "unknown"


def analyze_error(
    error_json: Dict[str, Any],
    settings: Optional[Settings] = None,
    flow_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Classify Logic App / HTTP style errors.

    When settings.rag_enabled and Azure OpenAI is configured, runs RAG over
    knowledge/chunks.jsonl and synthesizes exact_error_in_flow from live FLOW_CONTEXT.

    Returns:
        Analysis dict including error_type, fix_type, recommendation, exact fields,
        and optional rag_* keys.
    """
    settings = settings or get_settings()

    code = str(error_json.get("code") or "")
    message = _text_blob(error_json.get("message"))
    status_code = error_json.get("statusCode")
    signals = _extract_signals(error_json, message)
    root_cause = _root_cause_from_exact(code, message)
    # Logic Apps often embed HTTP status only inside the message string
    inferred = _infer_http_status_from_text(message)
    if status_code is None and inferred is not None:
        status_code = inferred
    combined = f"{code} {message} {status_code}".upper()

    common = {
        "exact_error_code": code or None,
        "exact_error_message": message or None,
        "root_cause": root_cause,
    }

    # Handle connector/runtime exact error codes first
    if root_cause == "dns_resolution_error":
        err_type = "404"
        base = {
            "error_type": err_type,
            "fix_type": "replace_endpoint",
            "recommendation": (
                "Host cannot be resolved. Update HTTP URI host/DNS and verify environment DNS/network path."
            ),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    # HTTP status from outputs
    if status_code == 404 or "404" in combined or "NOT FOUND" in combined:
        err_type = "404"
        base = {
            "error_type": err_type,
            "fix_type": "replace_endpoint",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if status_code == 403 or "403" in combined or "FORBIDDEN" in combined:
        err_type = "401"
        base = {
            "error_type": err_type,
            "fix_type": "refresh_auth",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code or 403},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if status_code == 401 or "401" in combined or "UNAUTHORIZED" in combined:
        err_type = "401"
        base = {
            "error_type": err_type,
            "fix_type": "refresh_auth",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if (
        status_code == 408
        or "TIMEOUT" in combined
        or "TIMED OUT" in combined
        or "REQUEST TIMEOUT" in combined
        or "504" in combined
        or status_code == 504
    ):
        err_type = "timeout"
        base = {
            "error_type": err_type,
            "fix_type": "add_retry",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    if (
        status_code == 400
        or "400" in combined
        or "BAD REQUEST" in combined
        or "SCHEMA" in combined
        or "INVALID" in combined
        or "MALFORMED" in combined
    ):
        err_type = "bad_request"
        base = {
            "error_type": err_type,
            "fix_type": "fix_payload",
            "recommendation": _dynamic_recommendation(err_type, signals),
            "raw_signals": {"code": code, "statusCode": status_code},
            "dynamic_signals": signals,
            "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            **common,
        }
        return _finalize_analysis(error_json, base, settings, flow_context)

    err_type = "unknown"
    base = {
        "error_type": err_type,
        "fix_type": "manual_review",
        "recommendation": _dynamic_recommendation(err_type, signals),
        "raw_signals": {"code": code, "statusCode": status_code},
        "dynamic_signals": signals,
        "analysis_generated_at_utc": datetime.now(timezone.utc).isoformat(),
        **common,
    }
    return _finalize_analysis(error_json, base, settings, flow_context)


def _finalize_analysis(
    error_json: Dict[str, Any],
    base: Dict[str, Any],
    settings: Settings,
    flow_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(base)
    if settings.rag_enabled:
        if settings.azure_openai_endpoint and settings.azure_openai_api_key:
            from logic_app_remediator import rag

            rag_out = rag.analyze_with_rag(
                error_json, flow_context or {}, out, settings
            )
            if rag_out:
                out.update(rag_out)
        return out
    if settings.azure_openai_endpoint and settings.azure_openai_api_key:
        out = _maybe_openai_enrich(error_json, out, settings)
    return out


def _maybe_openai_enrich(
    error_json: Dict[str, Any],
    base: Dict[str, Any],
    settings: Settings,
) -> Dict[str, Any]:
    if not settings.azure_openai_endpoint or not settings.azure_openai_api_key:
        return base

    try:
        url = (
            f"{settings.azure_openai_endpoint.rstrip('/')}/openai/deployments/"
            f"{settings.azure_openai_deployment}/chat/completions"
            f"?api-version={settings.azure_openai_api_version}"
        )
        system = (
            "You are an SRE assistant. Given a Logic Apps action error JSON and baseline analysis, "
            "return ONLY compact JSON with keys: "
            '{"error_type":"404|401|timeout|bad_request|unknown",'
            '"fix_type":"string","recommendation":"string","confidence":0.0}. '
            "Recommendation must be evidence-based, specific to the provided payload, "
            "and include concrete next action. No markdown."
        )
        payload = {
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(
                        {"error_json": error_json, "baseline": base},
                        default=str,
                    )[:14000],
                },
            ],
            "temperature": 0.1,
            "max_tokens": 400,
        }
        r = requests.post(
            url,
            headers={
                "api-key": settings.azure_openai_api_key,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        m = re.search(r"\{[\s\S]*\}", content)
        if not m:
            return base
        parsed = json.loads(m.group())
        merged = dict(base)
        for k in ("error_type", "fix_type", "recommendation"):
            if k in parsed and parsed[k]:
                merged[k] = parsed[k]
        if "confidence" in parsed:
            merged["confidence"] = parsed["confidence"]
        merged["openai_enriched"] = True
        return merged
    except Exception:
        return base
