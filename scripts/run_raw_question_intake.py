#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.automation.feeder_engine import FeederEngine
from core.automation.raw_question_intake import RawQuestionIntakeSystem
from core.automation.adaptive_question_classifier import AdaptiveQuestionClassifier
from core.intelligence.advanced_classifier import AdvancedSyllabusClassifier


def _print(payload):
    print(json.dumps(payload, indent=2, ensure_ascii=True))


def _load_raw_file(path: Path) -> List[Any]:
    suffix = path.suffix.lower()
    if suffix in {".json", ".jsonl"}:
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict) and isinstance(payload.get("raw_questions"), list):
                return list(payload["raw_questions"])
            raise ValueError("JSON must be a list or object with 'raw_questions'")

        out = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                out.append(obj)
        return out

    if suffix in {".txt", ".md"}:
        out = []
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if text:
                out.append(text)
        return out

    raise ValueError("Unsupported raw file type. Use .json/.jsonl/.txt/.md")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Adaptive raw-question intake -> feeder queue")
    parser.add_argument("--queue-path", default="data/lc9/LC9_FEEDER_QUEUE.jsonl")
    parser.add_argument("--training-cases-path", default="data/lc9/LC9_FEEDER_CASES.jsonl")
    parser.add_argument("--replay-cases-path", default="data/replay/feeder_cases.jsonl")
    parser.add_argument("--source-tag", default="raw_auto")
    parser.add_argument("--max-items", type=int, default=20)
    parser.add_argument("--classifier", choices=("advanced", "adaptive"), default="advanced")

    sub = parser.add_subparsers(dest="cmd", required=True)

    ingest = sub.add_parser("ingest", help="Ingest raw questions and optionally process")
    ingest.add_argument("--raw-python", default="", help="Python literal list, e.g. \"['q1','q2']\"")
    ingest.add_argument("--raw-json", default="", help="JSON list string")
    ingest.add_argument("--raw-file", default="", help="Path to JSON/JSONL/TXT file")
    ingest.add_argument("--process", action="store_true", default=False)

    classify = sub.add_parser("classify", help="Classify raw questions without enqueue")
    classify.add_argument("--raw-python", default="", help="Python literal list")
    classify.add_argument("--raw-json", default="", help="JSON list string")
    classify.add_argument("--raw-file", default="", help="Path to JSON/JSONL/TXT file")

    status = sub.add_parser("status", help="Show feeder status")
    status.add_argument("--limit", type=int, default=20)
    return parser


def _resolve_raw_inputs(args: argparse.Namespace) -> List[Any]:
    if str(args.raw_python or "").strip():
        from core.automation.raw_question_intake import RawQuestionIntakeSystem as _R

        return _R.parse_raw_python_literal(args.raw_python)
    if str(args.raw_json or "").strip():
        from core.automation.raw_question_intake import RawQuestionIntakeSystem as _R

        return _R.parse_raw_json(args.raw_json)
    if str(args.raw_file or "").strip():
        return _load_raw_file(Path(args.raw_file).resolve())
    raise ValueError("Provide one input source: --raw-python, --raw-json, or --raw-file")


async def main_async(args: argparse.Namespace) -> None:
    feeder = FeederEngine(
        queue_path=str(args.queue_path),
        training_cases_path=str(args.training_cases_path),
        replay_cases_path=str(args.replay_cases_path),
    )
    if str(args.classifier) == "adaptive":
        classifier = AdaptiveQuestionClassifier(
            feeder_cases_path=str(args.training_cases_path),
            replay_cases_path=str(args.replay_cases_path),
            queue_path=str(args.queue_path),
        )
    else:
        classifier = AdvancedSyllabusClassifier()
    system = RawQuestionIntakeSystem(feeder=feeder, classifier=classifier)

    if args.cmd == "status":
        _print(system.status(limit=max(1, int(args.limit))))
        return

    raw_questions = _resolve_raw_inputs(args)
    if args.cmd == "classify":
        classified = system.classify_raw_questions(raw_questions, default_source_tag=str(args.source_tag))
        _print(
            {
                "total": len(classified),
                "classified": [
                    {
                        "question": str(row.get("question", "")),
                        "subject": str(row.get("subject", "")),
                        "difficulty": str(row.get("difficulty", "")),
                        "concept_cluster": list(row.get("concept_cluster", [])),
                        "classification": row.get("_classification", {}),
                    }
                    for row in classified
                ],
            }
        )
        return

    if args.cmd == "ingest":
        out = await system.ingest(
            raw_questions,
            default_source_tag=str(args.source_tag),
            process=bool(args.process),
            max_items=max(1, int(args.max_items)),
            trigger="raw_question_intake_cli",
        )
        _print(out)
        return

    raise RuntimeError(f"Unknown command: {args.cmd}")


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
