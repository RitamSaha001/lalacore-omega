#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.data.repair_engine.math_repair_engine import MathRepairEngine


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Input is not a JSON list: {path}")
    return [dict(row) for row in data if isinstance(row, dict)]


def _extract_layer_details(repair_actions: list[str]) -> dict[str, list[str]]:
    layers: dict[str, list[str]] = {}
    for token in repair_actions:
        if ":" not in token:
            continue
        layer_name, action = token.split(":", 1)
        layer_name = layer_name.strip()
        action = action.strip()
        if not layer_name or not action:
            continue
        layers.setdefault(layer_name, [])
        if action not in layers[layer_name]:
            layers[layer_name].append(action)
    return layers


def _normalize_options(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                label = _to_str(item.get("label")).upper() or chr(65 + min(idx, 25))
                text = _to_str(item.get("text") or item.get("value") or item.get("option")).strip()
            else:
                label = chr(65 + min(idx, 25))
                text = _to_str(item).strip()
            if text:
                out.append({"label": label, "text": text})
    elif isinstance(raw, dict):
        items = sorted(raw.items(), key=lambda kv: _to_str(kv[0]))
        for idx, (_, value) in enumerate(items):
            text = _to_str(value).strip()
            if text:
                out.append({"label": chr(65 + min(idx, 25)), "text": text})
    return out


def _options_to_map(options: list[dict[str, str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for opt in options:
        label = _to_str(opt.get("label")).upper()
        text = _to_str(opt.get("text")).strip()
        if label and text:
            out[label] = text
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build JEE BANK X: deterministic full-layer repaired question bank with detailed layer metadata."
    )
    parser.add_argument("--input", default="data/app/import_question_bank_layer7_final.live.json")
    parser.add_argument("--output", default="data/app/JEE_BANK_X.json")
    parser.add_argument("--report", default="data/app/JEE_BANK_X.report.json")
    parser.add_argument("--progress-file", default="data/app/JEE_BANK_X.progress.json")
    parser.add_argument("--progress-every", type=int, default=300)
    parser.add_argument("--teacher-id-filter", default="")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    report_path = Path(args.report)
    progress_path = Path(args.progress_file)

    rows = _load_json_list(input_path)
    total = len(rows)
    started_at = _now_iso()
    t0 = time.time()

    engine = MathRepairEngine()
    out_rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    qtype_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()

    _atomic_write_json(
        progress_path,
        {
            "stage": "jee_bank_x_build",
            "status": "running",
            "started_at": started_at,
            "done": 0,
            "total": total,
            "progress_pct": 0.0,
        },
    )
    _atomic_write_json(output_path, out_rows)

    teacher_filter = _to_str(args.teacher_id_filter).strip()

    for idx, row in enumerate(rows, start=1):
        teacher_id = _to_str(row.get("teacher_id"))
        if teacher_filter and teacher_id != teacher_filter:
            continue

        payload = {
            "question_id": row.get("question_id") or row.get("id") or f"row_{idx}",
            "question_text": row.get("question_text") or row.get("question") or "",
            "options": _normalize_options(row.get("options")),
            "correct_answer": row.get("correct_answer") or {},
            "type": row.get("type") or row.get("question_type") or "",
            "subject": row.get("subject") or "",
            "chapter": row.get("chapter") or "",
            "difficulty": row.get("difficulty") or row.get("difficulty_estimate") or "",
        }

        repaired = engine.repair_question(payload, corpus=None)
        layer_details = _extract_layer_details(repaired.repair_actions)

        merged = dict(row)
        merged["id"] = merged.get("id") or repaired.question_id
        merged["question_id"] = repaired.question_id or _to_str(merged.get("question_id") or merged.get("id"))
        merged["question_text"] = repaired.repaired_question_text
        merged["question_text_latex"] = repaired.clean_question_latex
        merged["options"] = repaired.options
        merged["options_latex"] = repaired.options_latex
        merged["correct_answer"] = repaired.correct_answer
        merged["type"] = repaired.detected_question_type
        merged["topic"] = repaired.topic
        merged["difficulty"] = repaired.difficulty
        merged["repair_actions"] = repaired.repair_actions
        merged["detected_issues"] = repaired.validation_issues
        merged["repair_confidence"] = repaired.repair_confidence
        merged["repair_status"] = repaired.repair_status
        merged["solver_notes"] = repaired.solver_notes
        merged["requires_human_review"] = repaired.requires_human_review
        merged["verification"] = repaired.verification
        merged["math_repair_engine_x"] = {
            "clean_question_text": repaired.clean_question_text,
            "clean_question_latex": repaired.clean_question_latex,
            "options": _options_to_map(repaired.options),
            "options_latex": dict(repaired.options_latex),
            "correct_answer": dict(repaired.correct_answer),
            "topic": repaired.topic,
            "difficulty": repaired.difficulty,
            "detected_repairs": list(repaired.detected_repairs),
            "verification": dict(repaired.verification),
            "confidence": repaired.repair_confidence,
            "requires_human_review": repaired.requires_human_review,
        }
        merged["jee_bank_x"] = {
            "version": "X",
            "built_at": _now_iso(),
            "input_source": str(input_path),
            "layer_order": [
                "layer1_symbol_normalization",
                "layer2_mathbb_repair",
                "layer3_fraction_repair",
                "layer4_matrix_repair",
                "layer5_limit_repair",
                "layer6_multiplication_repair",
                "layer7_sum_product_repair",
                "layer8_set_notation_repair",
                "layer9_linear_algebra_repair",
                "layer10_structural_repair",
            ],
            "layer_details": layer_details,
            "validation": {
                "latex_syntax_ok": "latex_syntax_invalid" not in repaired.validation_issues,
                "ast_ok": not any(x.startswith("ast_validation_") and x != "ast_validation_partial" for x in repaired.validation_issues),
                "no_dangling_tokens": all(x not in {"dangling_operator", "dangling_token"} for x in repaired.validation_issues),
                "question_structure_ok": "question_structure_invalid" not in repaired.validation_issues,
            },
            "detected_question_type": repaired.detected_question_type,
            "repair_status": repaired.repair_status,
            "repair_confidence": repaired.repair_confidence,
            "validation_issues": repaired.validation_issues,
            "solver_notes": repaired.solver_notes,
            "math_repair_engine_x": dict(merged["math_repair_engine_x"]),
        }

        out_rows.append(merged)
        status_counts[repaired.repair_status] += 1
        qtype_counts[repaired.detected_question_type] += 1
        for token in repaired.repair_actions:
            action_counts[token] += 1

        if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == total):
            elapsed = max(0.001, time.time() - t0)
            rate = idx / elapsed
            eta_s = int(max(0.0, (total - idx) / max(0.01, rate)))
            payload_progress = {
                "stage": "jee_bank_x_build",
                "status": "running",
                "updated_at": _now_iso(),
                "done": idx,
                "total": total,
                "progress_pct": round((idx / max(1, total)) * 100.0, 2),
                "rows_per_s": round(rate, 2),
                "eta_s": eta_s,
                "status_counts": dict(status_counts),
                "question_type_counts": dict(qtype_counts),
                "written": len(out_rows),
            }
            _atomic_write_json(progress_path, payload_progress)
            _atomic_write_json(output_path, out_rows)
            print(json.dumps(payload_progress, ensure_ascii=False))

    _atomic_write_json(output_path, out_rows)

    seen = max(1, len(out_rows))
    avg_conf = sum(float(_to_str(row.get("repair_confidence") or 0.0) or 0.0) for row in out_rows) / seen
    report = {
        "stage": "jee_bank_x_build",
        "started_at": started_at,
        "finished_at": _now_iso(),
        "input": str(input_path),
        "output": str(output_path),
        "rows_seen": len(rows),
        "rows_written": len(out_rows),
        "status_counts": dict(status_counts),
        "question_type_counts": dict(qtype_counts),
        "avg_repair_confidence": round(avg_conf, 4),
        "top_repair_actions": dict(action_counts.most_common(50)),
    }
    _atomic_write_json(report_path, report)
    _atomic_write_json(
        progress_path,
        {
            "stage": "jee_bank_x_build",
            "status": "done",
            "updated_at": _now_iso(),
            "done": len(rows),
            "total": len(rows),
            "progress_pct": 100.0,
            "rows_written": len(out_rows),
            "output": str(output_path),
            "report": str(report_path),
        },
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
