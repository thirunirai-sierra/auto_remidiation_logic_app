"""
CLI entry: single failed run OR multi-workflow scan via Log Analytics.

Sample (single run):

    python -m logic_app_remediator.cli -s SUB -g RG -w WORKFLOW -r RUN_ID

Sample (all failed runs in last hour, all workflows in RG — via LA query):

    python -m logic_app_remediator.cli -s SUB -g RG --all-flows --workspace-id LA_WORKSPACE_GUID
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

from logic_app_remediator.agent import run_remediation
from logic_app_remediator.config import get_settings
from logic_app_remediator.multi_flow_runner import process_failed_runs


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Detect, analyze, and remediate Azure Logic Apps runs (single or multi-workflow)."
    )
    p.add_argument("-s", "--subscription-id", required=True, help="Azure subscription ID")
    p.add_argument("-g", "--resource-group", required=True, help="Resource group name")
    p.add_argument(
        "-w",
        "--workflow",
        default=None,
        help="Logic App (workflow) name (required unless --all-flows)",
    )
    p.add_argument(
        "-r",
        "--run-id",
        default=None,
        help="Failed run id (required unless --all-flows)",
    )
    p.add_argument(
        "-t",
        "--trigger",
        default=None,
        help="Trigger name for POST .../triggers/{name}/run (optional)",
    )
    p.add_argument(
        "-b",
        "--backup-dir",
        default=None,
        help="Directory for workflow JSON backups (default: cwd)",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )

    mf = p.add_argument_group("Multi-workflow (Azure Log Analytics)")
    mf.add_argument(
        "--all-flows",
        action="store_true",
        help="Query AzureDiagnostics for failed runs and process each with run_remediation()",
    )
    mf.add_argument(
        "--workspace-id",
        default=None,
        help="Log Analytics workspace GUID (or set LOG_ANALYTICS_WORKSPACE_ID)",
    )
    mf.add_argument(
        "--hours",
        type=int,
        default=1,
        help="Lookback hours for failed runs query (default: 1, max: 168)",
    )
    mf.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Max rows from Log Analytics (default: 100)",
    )
    mf.add_argument(
        "--max-concurrency",
        type=int,
        default=4,
        help="Parallel remediation workers (default: 4)",
    )
    mf.add_argument(
        "--schedule-minutes",
        type=int,
        default=0,
        help="If >0 with --all-flows, repeat scan every N minutes (simple loop; use systemd/cron in prod)",
    )

    args = p.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = get_settings()

    if args.all_flows:
        ws = (args.workspace_id or os.getenv("LOG_ANALYTICS_WORKSPACE_ID") or "").strip()
        if not ws:
            p.error("--workspace-id or LOG_ANALYTICS_WORKSPACE_ID is required with --all-flows")

        def _run_batch() -> dict:
            return process_failed_runs(
                subscription_id=args.subscription_id,
                resource_group=args.resource_group,
                workspace_id=ws,
                hours=args.hours,
                top_n=args.top_n,
                max_concurrency=args.max_concurrency,
                settings=settings,
                backup_dir=args.backup_dir,
                trigger_name=args.trigger,
            )

        if args.schedule_minutes and args.schedule_minutes > 0:
            while True:
                report = _run_batch()
                print(json.dumps(report, indent=2))
                logging.info(
                    "Sleeping %s minute(s) before next scan...", args.schedule_minutes
                )
                time.sleep(args.schedule_minutes * 60)
        else:
            report = _run_batch()
            print(json.dumps(report, indent=2))
            bad = any(
                (r.get("status") or "")
                in ("deploy_failed", "failed", "runner_error", "subscription_read_only")
                for r in report.get("results") or []
            )
            return 1 if bad else 0

    if not args.workflow or not args.run_id:
        p.error("--workflow and --run-id are required unless --all-flows is set")

    result = run_remediation(
        subscription_id=args.subscription_id,
        resource_group=args.resource_group,
        workflow_name=args.workflow,
        run_id=args.run_id,
        settings=settings,
        backup_dir=args.backup_dir,
        trigger_name=args.trigger,
    )
    print(json.dumps(result, indent=2))
    ok = result.get("status") in ("remediated", "no_error", "needs_manual_review")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
