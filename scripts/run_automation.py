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

from core.automation.orchestrator import AutomationOrchestrator


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run LalaCore Omega automation orchestrator")
    p.add_argument("--scheduled", action="store_true", default=False, help="Run only if due")
    p.add_argument("--interval-days", type=int, default=7, help="Scheduled min interval")
    p.add_argument("--trigger", default="manual", help="Trigger label for manual runs")
    p.add_argument("--resume", action="store_true", default=False, help="Resume incomplete job if present")
    p.add_argument("--feeder-batch", type=int, default=12, help="Feeder pre-batch size")
    p.add_argument("--replay-batch", type=int, default=None, help="Replay batch cap")
    p.add_argument("--no-replay-exec", action="store_true", default=False, help="Skip replay solve execution")
    return p


async def main_async(args) -> None:
    orchestrator = AutomationOrchestrator()
    if args.scheduled:
        out = await orchestrator.run_if_due(
            min_interval_days=max(1, int(args.interval_days)),
            feeder_batch=max(1, int(args.feeder_batch)),
            replay_batch=args.replay_batch,
            execute_replay_pipeline=not bool(args.no_replay_exec),
        )
    else:
        out = await orchestrator.run_weekly(
            trigger=str(args.trigger),
            resume=bool(args.resume),
            feeder_batch=max(1, int(args.feeder_batch)),
            replay_batch=args.replay_batch,
            execute_replay_pipeline=not bool(args.no_replay_exec),
        )
    print(json.dumps(out, indent=2, ensure_ascii=True))


def main() -> None:
    args = parser().parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
