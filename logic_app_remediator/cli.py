"""
CLI entry: single failed run OR multi-workflow scan via Log Analytics.

Now supports environment variables for multi-flow configuration.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

# Suppress verbose logging from Azure SDK and other libraries
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("msal").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

from logic_app_remediator.agent import run_remediation
from logic_app_remediator.config import get_settings
from logic_app_remediator.multi_flow_runner import (
    collect_failed_run_errors,
    process_failed_runs,
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Detect, analyze, and remediate Azure Logic Apps runs (single or multi-workflow)."
    )
    p.add_argument("-s", "--subscription-id", required=False, help="Azure subscription ID (or AZURE_SUBSCRIPTION_ID env)")
    p.add_argument("-g", "--resource-group", required=False, help="Resource group name (or AZURE_RESOURCE_GROUP env)")
    p.add_argument(
        "-w",
        "--workflow",
        default=None,
        help="Logic App (workflow) name (required for single run mode)",
    )
    p.add_argument(
        "-r",
        "--run-id",
        default=None,
        help="Failed run id (required for single run mode)",
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
        default="WARNING",  # Changed from INFO to WARNING
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Log level (default: WARNING)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed logs (overrides --log-level)",
    )

    mf = p.add_argument_group("Multi-workflow (Azure Log Analytics)")
    mf.add_argument(
        "--all-flows",
        action="store_true",
        help="Query AzureDiagnostics for failed runs (or MULTI_FLOW_ENABLED env)",
    )
    mf.add_argument(
        "--workspace-id",
        default=None,
        help="Log Analytics workspace GUID (or LOG_ANALYTICS_WORKSPACE_ID env)",
    )
    mf.add_argument(
        "--hours",
        type=int,
        default=None,
        help="Lookback hours for failed runs query (or LOOKBACK_HOURS env)",
    )
    mf.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Max rows from Log Analytics (or TOP_N_RUNS env)",
    )
    mf.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help="Parallel remediation workers (or MAX_CONCURRENCY env)",
    )
    mf.add_argument(
        "--schedule-minutes",
        type=int,
        default=None,
        help="If >0, repeat scan every N minutes (or SCHEDULE_MINUTES env)",
    )
    mf.add_argument(
        "--log-only",
        action="store_true",
        help="Only read and display failed runs (or LOG_ONLY env)",
    )
    mf.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress all output except final JSON",
    )

    args = p.parse_args(argv)
    
    # Configure logging
    if args.quiet:
        # Suppress ALL logs
        logging.basicConfig(level=logging.ERROR, format="%(message)s")
        logging.getLogger().setLevel(logging.ERROR)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    else:
        # Default: WARNING level (only show warnings and errors)
        logging.basicConfig(
            level=getattr(logging, args.log_level),
            format="%(message)s"  # Simpler format for warnings/errors
        )
    
    # Load settings from env
    settings = get_settings()
    
    # Determine if multi-flow is enabled (CLI flag OR env var)
    multi_flow_enabled = args.all_flows or settings.multi_flow_enabled
    
    # Resolve parameters with priority: CLI args > env vars
    subscription_id = args.subscription_id or settings.subscription_id
    resource_group = args.resource_group or settings.resource_group
    workspace_id = args.workspace_id or settings.log_analytics_workspace_id
    hours = args.hours if args.hours is not None else settings.lookback_hours
    top_n = args.top_n if args.top_n is not None else settings.top_n_runs
    max_concurrency = args.max_concurrency if args.max_concurrency is not None else settings.max_concurrency
    schedule_minutes = args.schedule_minutes if args.schedule_minutes is not None else settings.schedule_minutes
    log_only = args.log_only or settings.log_only
    
    # Single run mode (not multi-flow)
    if not multi_flow_enabled:
        if not args.workflow or not args.run_id:
            p.error("--workflow and --run-id are required for single run mode (or use --all-flows/MULTI_FLOW_ENABLED)")
        
        # For single run, we still need subscription and resource group
        if not subscription_id or not resource_group:
            p.error("subscription-id and resource-group required: use -s/-g flags or set AZURE_SUBSCRIPTION_ID/AZURE_RESOURCE_GROUP env vars")
        
        result = run_remediation(
            subscription_id=subscription_id,
            resource_group=resource_group,
            workflow_name=args.workflow,
            run_id=args.run_id,
            settings=settings,
            backup_dir=args.backup_dir,
            trigger_name=args.trigger,
        )
        # Only print the final result
        print(json.dumps(result, indent=2))
        ok = result.get("status") in ("remediated", "no_error", "needs_manual_review")
        return 0 if ok else 1
    
    # Multi-flow mode
    # Validate required parameters
    missing = []
    if not subscription_id:
        missing.append("subscription-id (AZURE_SUBSCRIPTION_ID)")
    if not resource_group:
        missing.append("resource-group (AZURE_RESOURCE_GROUP)")
    if not workspace_id:
        missing.append("workspace-id (LOG_ANALYTICS_WORKSPACE_ID)")
    
    if missing:
        p.error(f"Missing required parameters for multi-flow mode: {', '.join(missing)}")
    
    def _run_batch() -> dict:
        if log_only:
            return collect_failed_run_errors(
                workspace_id=workspace_id,
                hours=hours,
                top_n=top_n,
                settings=settings,
            )
        return process_failed_runs(
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace_id=workspace_id,
            hours=hours,
            top_n=top_n,
            max_concurrency=max_concurrency,
            settings=settings,
            backup_dir=args.backup_dir,
            trigger_name=args.trigger,
        )
    
    if schedule_minutes and schedule_minutes > 0:
        while True:
            report = _run_batch()
            # Only print JSON output
            print(json.dumps(report, indent=2))
            if not args.quiet:
                logging.warning("Sleeping %s minute(s) before next scan...", schedule_minutes)
            time.sleep(schedule_minutes * 60)
    else:
        report = _run_batch()
        # Only print JSON output
        print(json.dumps(report, indent=2))
        critical_statuses = ("deploy_failed", "failed", "runner_error", "subscription_read_only")
        bad = any(
            (r.get("remediation_status") or "") in critical_statuses
            for r in report.get("results") or []
        )
        return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))    