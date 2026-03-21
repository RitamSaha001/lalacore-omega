#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.automation.feeder_engine import FeederEngine
from core.automation.orchestrator import AutomationOrchestrator


def _print(payload):
    print(json.dumps(payload, indent=2, ensure_ascii=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LalaCore Omega feeder/automation CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add", help="Add a question to LC9 feeder queue")
    add.add_argument("--question", required=True, help="Question text")
    add.add_argument("--subject", default="general", help="Subject label")
    add.add_argument("--difficulty", default="unknown", help="Difficulty label")
    add.add_argument(
        "--concept-cluster",
        default="",
        help="Comma-separated concept clusters",
    )
    add.add_argument("--source-tag", default="manual", help="Source tag")

    process = sub.add_parser("process", help="Process pending feeder items")
    process.add_argument("--max-items", type=int, default=10, help="Max items to process")

    status = sub.add_parser("status", help="Show feeder queue status")
    status.add_argument("--limit", type=int, default=20, help="Recent item limit")

    run_weekly = sub.add_parser("run-weekly", help="Run full automation weekly pipeline")
    run_weekly.add_argument("--trigger", default="manual", help="Trigger label")
    run_weekly.add_argument("--resume", action="store_true", default=False, help="Resume incomplete run if present")
    run_weekly.add_argument("--feeder-batch", type=int, default=12, help="Feeder batch before weekly jobs")
    run_weekly.add_argument("--replay-batch", type=int, default=None, help="Replay batch cap override")
    run_weekly.add_argument(
        "--no-replay-exec",
        action="store_true",
        default=False,
        help="Do not execute replay questions through solve pipeline",
    )

    tick = sub.add_parser("tick", help="Run scheduled tick if weekly job is due")
    tick.add_argument("--interval-days", type=int, default=7, help="Min interval between scheduled runs")

    return parser


async def main_async(args) -> None:
    feeder = FeederEngine()
    automation = AutomationOrchestrator(feeder=feeder)

    if args.cmd == "add":
        clusters = [c.strip() for c in str(args.concept_cluster).split(",") if c.strip()]
        out = feeder.enqueue_question(
            question=args.question,
            subject=args.subject,
            difficulty=args.difficulty,
            concept_cluster=clusters,
            source_tag=args.source_tag,
        )
        _print(out)
        return

    if args.cmd == "process":
        out = await feeder.process_pending(max_items=max(1, int(args.max_items)), trigger="cli")
        _print(out)
        return

    if args.cmd == "status":
        out = feeder.status(limit=max(1, int(args.limit)))
        _print(out)
        return

    if args.cmd == "run-weekly":
        out = await automation.run_weekly(
            trigger=str(args.trigger),
            resume=bool(args.resume),
            feeder_batch=max(1, int(args.feeder_batch)),
            replay_batch=args.replay_batch,
            execute_replay_pipeline=not bool(args.no_replay_exec),
        )
        _print(out)
        return

    if args.cmd == "tick":
        out = await automation.run_if_due(min_interval_days=max(1, int(args.interval_days)))
        _print(out)
        return

    raise RuntimeError(f"Unknown command {args.cmd}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
