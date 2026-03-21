#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.monitoring.google_sheets_monitor import (
    GoogleSheetsClient,
    GoogleSheetsSyncService,
    MonitoringAggregator,
    extract_spreadsheet_id,
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="LalaCore Omega -> Google Sheets monitoring sync")
    p.add_argument("--sheet-url", default=os.getenv("LC9_GOOGLE_SHEET_URL", ""), help="Google sheet URL")
    p.add_argument("--sheet-id", default=os.getenv("LC9_GOOGLE_SHEET_ID", ""), help="Google sheet ID")
    p.add_argument(
        "--service-account-file",
        default=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE", "")),
        help="Service account JSON file path",
    )
    p.add_argument("--state-path", default="data/lc9/LC9_GOOGLE_SHEETS_SYNC_STATE.json")
    p.add_argument("--sync-log-path", default="data/lc9/LC9_GOOGLE_SHEETS_SYNC_LOG.jsonl")
    p.add_argument("--queue-path", default="data/lc9/LC9_FEEDER_QUEUE.jsonl")
    p.add_argument("--debug-path", default="data/lc9/LC9_SOLVER_DEBUG.jsonl")
    p.add_argument("--runtime-path", default="data/lc9/LC9_RUNTIME_TELEMETRY.jsonl")
    p.add_argument("--provider-stats-path", default="data/metrics/provider_stats.json")
    p.add_argument("--provider-circuit-path", default="data/metrics/provider_circuit.json")
    p.add_argument("--token-budget-path", default="data/metrics/token_budget.json")
    p.add_argument("--weekly-days", type=int, default=7)
    p.add_argument("--weekly-limit-tokens", type=int, default=1000000)
    p.add_argument("--debug-event-limit", type=int, default=2000)
    p.add_argument("--runtime-event-limit", type=int, default=2000)
    p.add_argument("--question-row-limit", type=int, default=2000)
    p.add_argument("--interval-s", type=int, default=60)
    p.add_argument("--once", action="store_true", default=False, help="Run one sync and exit")
    return p


def _resolve_root_path(value: str | Path) -> str:
    path = Path(str(value or "").strip()).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return str(path.resolve())


def _resolve_sheet_id(args: argparse.Namespace) -> str:
    if str(args.sheet_id or "").strip():
        return str(args.sheet_id).strip()
    return extract_spreadsheet_id(str(args.sheet_url or ""))


def _resolve_service_account(path: str) -> str:
    text = str(path or "").strip()
    if not text:
        return ""
    p = Path(text).expanduser()
    if not p.is_absolute():
        p = ROOT / p
    return str(p.resolve())


def _build_service(args: argparse.Namespace) -> GoogleSheetsSyncService:
    sheet_id = _resolve_sheet_id(args)
    if not sheet_id:
        raise RuntimeError("Missing sheet ID. Use --sheet-id or --sheet-url.")

    service_account_file = _resolve_service_account(args.service_account_file)
    if not service_account_file:
        raise RuntimeError(
            "Missing service account file. Set --service-account-file or GOOGLE_SERVICE_ACCOUNT_JSON."
        )

    sheet_client = GoogleSheetsClient(
        spreadsheet_id=sheet_id,
        service_account_file=service_account_file,
    )

    aggregator = MonitoringAggregator(
        queue_path=_resolve_root_path(str(args.queue_path)),
        debug_path=_resolve_root_path(str(args.debug_path)),
        runtime_path=_resolve_root_path(str(args.runtime_path)),
        provider_stats_path=_resolve_root_path(str(args.provider_stats_path)),
        provider_circuit_path=_resolve_root_path(str(args.provider_circuit_path)),
        token_budget_path=_resolve_root_path(str(args.token_budget_path)),
        debug_event_limit=int(args.debug_event_limit),
        runtime_event_limit=int(args.runtime_event_limit),
        question_row_limit=int(args.question_row_limit),
        weekly_days=int(args.weekly_days),
        weekly_limit_tokens=int(args.weekly_limit_tokens),
    )

    return GoogleSheetsSyncService(
        sheet_client=sheet_client,
        aggregator=aggregator,
        state_path=_resolve_root_path(str(args.state_path)),
        sync_log_path=_resolve_root_path(str(args.sync_log_path)),
    )


def main() -> None:
    args = _build_parser().parse_args()
    service = _build_service(args)

    if bool(args.once):
        out = service.sync_once()
        print(json.dumps(out, indent=2, ensure_ascii=True))
        return

    interval_s = max(15, int(args.interval_s))
    while True:
        try:
            out = service.sync_once()
            print(json.dumps(out, ensure_ascii=True))
        except KeyboardInterrupt:
            break
        except Exception as exc:
            print(json.dumps({"event": "sync_error", "error": str(exc)}, ensure_ascii=True), file=sys.stderr)
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
