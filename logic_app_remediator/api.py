"""Azure Resource Manager calls for Logic Apps runs, actions, and workflow definition."""

from typing import Any, Dict, List, Optional

import requests

ARM_BASE = "https://management.azure.com"
# Runs/actions: 2019-05-01 is widely used for workflow runs
API_RUNS = "2019-05-01"
# Workflow definition GET/PUT
API_WORKFLOW = "2019-05-01"
# Manual trigger run (Logic Apps consumption)
API_TRIGGER_RUN = "2016-06-01"


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def workflow_base_url(
    subscription_id: str, resource_group: str, workflow_name: str
) -> str:
    return (
        f"{ARM_BASE}/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Logic/workflows/{workflow_name}"
    )


def get_run(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
) -> Dict[str, Any]:
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/runs/{run_id}?api-version={API_RUNS}"
    )
    r = requests.get(url, headers=_headers(token), timeout=120)
    r.raise_for_status()
    return r.json()


def list_run_actions(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/runs/{run_id}/actions?api-version={API_RUNS}"
    )
    items: List[Dict[str, Any]] = []
    while url:
        r = requests.get(url, headers=_headers(token), timeout=120)
        r.raise_for_status()
        data = r.json()
        items.extend(data.get("value", []))
        url = data.get("nextLink") or data.get("@odata.nextLink")
    return items


def get_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
) -> Dict[str, Any]:
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"?api-version={API_WORKFLOW}"
    )
    r = requests.get(url, headers=_headers(token), timeout=120)
    r.raise_for_status()
    return r.json()


def put_workflow(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    workflow_body: Dict[str, Any],
    etag: Optional[str] = None,
) -> Dict[str, Any]:
    """
    PUT full workflow resource. Caller must send complete resource shape ARM expects.
    When etag is provided, sends If-Match for optimistic concurrency.
    """
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"?api-version={API_WORKFLOW}"
    )
    h = _headers(token)
    if etag:
        # Use exact value from GET (often quoted weak ETag)
        h["If-Match"] = etag
    r = requests.put(url, headers=h, json=workflow_body, timeout=300)
    r.raise_for_status()
    return r.json() if r.text else {}


def post_trigger_run(
    token: str,
    subscription_id: str,
    resource_group: str,
    workflow_name: str,
    trigger_name: str,
    body: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    url = (
        f"{workflow_base_url(subscription_id, resource_group, workflow_name)}"
        f"/triggers/{trigger_name}/run?api-version={API_TRIGGER_RUN}"
    )
    return requests.post(
        url,
        headers=_headers(token),
        json=body if body is not None else {},
        timeout=120,
    )


def find_manual_or_recurrence_trigger(definition: Dict[str, Any]) -> Optional[str]:
    """Pick first manual trigger, else first recurrence, else None."""
    triggers = definition.get("triggers") or {}
    for name, t in triggers.items():
        if not isinstance(t, dict):
            continue
        ttype = (t.get("type") or "").lower()
        if ttype == "request" or ttype == "manual":
            return name
    for name, t in triggers.items():
        if isinstance(t, dict) and (t.get("type") or "").lower() == "recurrence":
            return name
    if triggers:
        return next(iter(triggers.keys()), None)
    return None
