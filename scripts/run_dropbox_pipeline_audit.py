#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import sympy as sp

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.api.entrypoint import lalacore_entry
from core.automation.feeder_engine import FeederEngine
from core.automation.raw_question_intake import RawQuestionIntakeSystem
from core.intelligence.advanced_classifier import AdvancedSyllabusClassifier
from core.math.contextual_math_solver import _X, _safe_parse_expr


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_dropbox_questions(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Drop file not found: {path}")

    out: List[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = str(raw or "").strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        if line.endswith(","):
            line = line[:-1].strip()
        if (line.startswith('"') and line.endswith('"')) or (line.startswith("'") and line.endswith("'")):
            line = line[1:-1].strip()
        if line:
            out.append(line)
    return out


def _parse_int(value: Any) -> int | None:
    m = re.search(r"-?\d+", str(value or "").replace(",", ""))
    return int(m.group(0)) if m else None


def _extract_constant_expr(question: str) -> str | None:
    q = str(question or "").strip()
    patterns = (
        r"^\s*find\s+the\s+constant\s+term\s+in\s+(.+?)\s*\.?\s*$",
        r"^\s*find\s+the\s+term\s+independent\s+of\s+x\s+in\s+(.+?)\s*\.?\s*$",
    )
    for pattern in patterns:
        m = re.match(pattern, q, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _expected_for_question(question: str) -> int | None:
    expr_text = _extract_constant_expr(question)
    if not expr_text:
        return None
    expr = _safe_parse_expr(expr_text)
    if expr is None:
        return None
    coeff = sp.simplify(sp.expand(expr).coeff(_X, 0))
    if getattr(coeff, "is_integer", False):
        return int(coeff)
    try:
        return int(sp.Integer(coeff))
    except Exception:
        return None


async def _run_pipeline_ingest(questions: List[str], args: argparse.Namespace) -> Dict[str, Any]:
    if bool(args.ignore_daily_cap):
        current = int(os.getenv("LC9_FEEDER_DAILY_CAP", "30"))
        os.environ["LC9_FEEDER_DAILY_CAP"] = str(max(current, len(questions) + 500))

    feeder = FeederEngine(
        queue_path=str(args.queue_path),
        training_cases_path=str(args.training_cases_path),
        replay_cases_path=str(args.replay_cases_path),
    )
    system = RawQuestionIntakeSystem(feeder=feeder, classifier=AdvancedSyllabusClassifier())
    return await system.ingest(
        questions,
        default_source_tag=str(args.source_tag),
        process=True,
        max_items=max(1, len(questions)),
        trigger="dropbox_pipeline_audit",
    )


async def _run_audit(questions: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for question in questions:
        raw = await lalacore_entry(
            input_data=question,
            input_type="text",
            options={"enable_persona": False, "enable_meta_verification": True},
        )
        persona = await lalacore_entry(
            input_data=question,
            input_type="text",
            options={"enable_persona": True, "enable_meta_verification": True},
        )

        expected = _expected_for_question(question)
        parsed = _parse_int(raw.get("final_answer"))
        correct = None if expected is None else (parsed == expected)
        verification = dict(raw.get("verification") or {})
        calibration = dict(raw.get("calibration_metrics") or {})
        diagnostics = dict(raw.get("provider_diagnostics") or {})
        persona_parsed = _parse_int(persona.get("final_answer"))
        confidence = float(calibration.get("confidence", 0.0) or 0.0)
        overconfidence = bool(correct is False and confidence > 0.85)

        rows.append(
            {
                "question": question,
                "expected": expected,
                "final_answer": raw.get("final_answer"),
                "parsed": parsed,
                "correct": correct,
                "verified": verification.get("verified"),
                "risk_score": verification.get("risk_score", calibration.get("risk_score")),
                "entropy": calibration.get("entropy"),
                "confidence": confidence,
                "winner_provider": raw.get("winner_provider"),
                "single_provider_mode": bool(diagnostics.get("single_provider_mode", False)),
                "persona_integrity": persona_parsed == parsed,
                "overconfidence": overconfidence,
                "with_persona_display_answer": persona.get("display_answer"),
            }
        )
    return rows


def _summarize(rows: List[Dict[str, Any]], ingest_result: Dict[str, Any] | None) -> Dict[str, Any]:
    expected_rows = [r for r in rows if r.get("expected") is not None]
    correct = [r for r in expected_rows if r.get("correct") is True]
    verified_true = [r for r in rows if r.get("verified") is True]
    single_provider = [r for r in rows if bool(r.get("single_provider_mode", False))]
    overconfidence = [r for r in rows if bool(r.get("overconfidence", False))]
    persona_ok = [r for r in rows if bool(r.get("persona_integrity", False))]
    risk_vals = [float(r.get("risk_score")) for r in rows if r.get("risk_score") is not None]
    entropy_vals = [float(r.get("entropy")) for r in rows if r.get("entropy") is not None]

    return {
        "generated_at": _utc_now(),
        "total_questions": len(rows),
        "expected_covered": len(expected_rows),
        "correct_on_expected": len(correct),
        "accuracy_on_expected": (len(correct) / len(expected_rows)) if expected_rows else None,
        "verified_true": len(verified_true),
        "overconfidence_count": len(overconfidence),
        "single_provider_count": len(single_provider),
        "persona_integrity_rate": (len(persona_ok) / len(rows)) if rows else 0.0,
        "risk_mean": (sum(risk_vals) / len(risk_vals)) if risk_vals else None,
        "entropy_mean": (sum(entropy_vals) / len(entropy_vals)) if entropy_vals else None,
        "wrong_questions": [r["question"] for r in expected_rows if r.get("correct") is False],
        "ingest_process_result": ingest_result or {},
    }


async def main_async(args: argparse.Namespace) -> None:
    questions = _load_dropbox_questions(Path(args.dropbox_file).resolve())
    if not questions:
        raise RuntimeError("No questions found in dropbox file.")

    ingest_result = None
    if not bool(args.skip_ingest):
        ingest_result = await _run_pipeline_ingest(questions, args)

    rows = await _run_audit(questions)
    summary = _summarize(rows, ingest_result=ingest_result)

    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    detail_path = out_dir / f"dropbox_pipeline_audit_{stamp}.json"
    summary_path = out_dir / f"dropbox_pipeline_audit_summary_{stamp}.json"
    detail_path.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(
        json.dumps(
            {
                "dropbox_file": str(Path(args.dropbox_file).resolve()),
                "detail_report": str(detail_path),
                "summary_report": str(summary_path),
                "summary": summary,
            },
            indent=2,
            ensure_ascii=True,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Load dropbox questions into natural pipeline and run audit.")
    p.add_argument("--dropbox-file", default="input/questions_dropbox.txt", help="Text file: one question per line.")
    p.add_argument("--skip-ingest", action="store_true", default=False, help="Skip feeder ingest/process step.")
    p.add_argument(
        "--ignore-daily-cap",
        action="store_true",
        default=False,
        help="Temporarily raise LC9_FEEDER_DAILY_CAP for this manual run.",
    )
    p.add_argument("--queue-path", default="data/lc9/LC9_FEEDER_QUEUE.jsonl")
    p.add_argument("--training-cases-path", default="data/lc9/LC9_FEEDER_CASES.jsonl")
    p.add_argument("--replay-cases-path", default="data/replay/feeder_cases.jsonl")
    p.add_argument("--source-tag", default="dropbox_manual")
    p.add_argument("--output-dir", default="data/audit")
    return p


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
