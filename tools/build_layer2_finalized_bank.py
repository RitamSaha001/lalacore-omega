#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _load_json_with_retries(path: Path, *, retries: int = 12, sleep_s: float = 0.8) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - runtime guard
            last_exc = exc
            if attempt < retries:
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"json_read_failed:{path}:{exc}") from exc
    raise RuntimeError(f"json_read_failed:{path}:{last_exc}")


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _normalize_options(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if isinstance(raw, dict):
        entries = sorted(raw.items(), key=lambda kv: _to_str(kv[0]).upper())
        for idx, (_, value) in enumerate(entries):
            text = _to_str(value).strip()
            if text:
                out.append({"label": chr(65 + min(idx, 25)), "text": text})
        return out
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                label = _to_str(item.get("label")).upper() or chr(65 + min(idx, 25))
                text = _to_str(item.get("text") or item.get("option") or item.get("value")).strip()
            else:
                label = chr(65 + min(idx, 25))
                text = _to_str(item).strip()
            if text:
                out.append({"label": label, "text": text})
    return out


def _extract_correct_answer(row: dict[str, Any]) -> dict[str, Any]:
    ans = row.get("correct_answer")
    if isinstance(ans, dict):
        multiple = ans.get("multiple")
        if not isinstance(multiple, list):
            multiple = []
        return {
            "single": (_to_str(ans.get("single")).upper() or None),
            "multiple": [_to_str(x).upper() for x in multiple if _to_str(x).strip()],
            "numerical": (_to_str(ans.get("numerical")).strip() or None),
            "tolerance": ans.get("tolerance"),
        }
    single = _to_str(row.get("_correct_option") or row.get("correct_option")).upper() or None
    multiple_raw = row.get("_correct_answers") or row.get("correct_answers") or []
    multiple = (
        [_to_str(x).upper() for x in multiple_raw if _to_str(x).strip()]
        if isinstance(multiple_raw, list)
        else []
    )
    numerical = _to_str(row.get("_numerical_answer") or row.get("numerical_answer")).strip() or None
    if not multiple and single:
        multiple = [single]
    return {
        "single": single,
        "multiple": multiple,
        "numerical": numerical,
        "tolerance": row.get("numerical_tolerance") or row.get("tolerance"),
    }


def _phase1_extra_cleanup(text: str) -> tuple[str, list[str], list[str]]:
    out = _to_str(text)
    actions: list[str] = []
    issues: list[str] = []

    if not out.strip():
        return "", actions, ["missing_function_definition"]

    replacements = [
        (r"\bxxfx\b", "f(x)"),
        (r"\bx\s+fx\b", "f(x)"),
        (r"\(\)\s*'Rf\s*a\b", "f'_R(a)"),
        (r"\(\)\s*'Lf\s*a\b", "f'_L(a)"),
        (r"\(\)\s*'\s*R\s*f\s*a\b", "f'_R(a)"),
        (r"\(\)\s*'\s*L\s*f\s*a\b", "f'_L(a)"),
    ]
    for pat, repl in replacements:
        out2 = re.sub(pat, repl, out, flags=re.IGNORECASE)
        if out2 != out:
            out = out2
            actions.append("ocr_symbol_swap")

    if re.search(r"\blim\s+[A-Za-z]\s*->\s*(?:$|[^\w])", out, flags=re.IGNORECASE):
        issues.append("broken_limit_expression")

    if re.search(r"[+\-*/^=]\s*$", out):
        issues.append("dangling_operator")
    if out.count("(") != out.count(")") or out.count("{") != out.count("}") or out.count("[") != out.count("]"):
        issues.append("unbalanced_parenthesis")

    return re.sub(r"\s+", " ", out).strip(), actions, issues


def _concept_tags(question_text: str, chapter: str, subject: str) -> list[str]:
    blob = f"{_to_str(question_text)} {_to_str(chapter)} {_to_str(subject)}".lower()
    tags: list[str] = []
    rules = [
        ("limits", ("lim", "x->", "approaches")),
        ("continuity", ("continuous", "continuity")),
        ("differentiability", ("differentiat", "derivative", "f'_", "f'(")),
        ("indefinite_integration", ("indefinite integral", "\\int", "integrate")),
        ("definite_integration", ("definite integral", "integral from", "dx")),
        ("functions", ("function", "f(x)", "g(x)")),
        ("relations", ("relation", "equivalence", "reflexive")),
        ("determinants", ("determinant", "|a|", "adjoint")),
        ("matrices", ("matrix", "eigen", "rank")),
        ("complex_numbers", ("complex", "arg", "modulus", "imaginary")),
        ("binomial_theorem", ("binomial", "coefficient", "(1+x)^n")),
        ("three_d_geometry", ("3d", "three dimensional", "direction ratio", "plane", "line")),
        ("probability", ("probability", "random", "bayes", "independent")),
    ]
    for tag, probes in rules:
        if any(tok in blob for tok in probes):
            tags.append(tag)
    if not tags:
        tags.append("jee_mixed")
    return tags


def _difficulty_estimate(row: dict[str, Any], integrity: float, tags: list[str]) -> str:
    explicit = _to_str(row.get("difficulty")).strip()
    if explicit:
        low = explicit.lower()
        if "advanced" in low:
            return "JEE Advanced"
        if "main" in low:
            return "JEE Main"
        if "hard" in low and integrity >= 0.75:
            return "JEE Advanced"
    if len(tags) >= 2 and integrity >= 0.82:
        return "JEE Advanced"
    return "JEE Main"


def _map_issue_tokens(
    *,
    phase1_issues: list[str],
    validation_issues: list[str],
    solver_notes: list[str],
) -> list[str]:
    mapped: list[str] = []
    issue_map = {
        "expression_fragmented": "expression_fragmented",
        "dangling_operator": "dangling_operator",
        "unbalanced_parenthesis": "unbalanced_parenthesis",
        "unbalanced_brackets": "unbalanced_parenthesis",
        "equation_rhs_missing": "missing_function_definition",
        "list_match_structure_missing": "corrupted_piecewise",
        "piecewise_case_incomplete": "corrupted_piecewise",
        "missing_options": "invalid_token_sequences",
        "expression_parse_failure": "invalid_token_sequences",
        "empty_question": "missing_function_definition",
    }
    for token in [*phase1_issues, *validation_issues]:
        normalized = issue_map.get(_to_str(token), _to_str(token))
        if normalized and normalized not in mapped:
            mapped.append(normalized)

    if any("limit_pattern_not_found" in _to_str(n) for n in solver_notes):
        if "broken_limit_expression" not in mapped:
            mapped.append("broken_limit_expression")
    if any("solver_failed" in _to_str(n) for n in solver_notes):
        if "invalid_token_sequences" not in mapped:
            mapped.append("invalid_token_sequences")

    return mapped


def _score_integrity(
    *,
    repair_confidence: float,
    detected_issues: list[str],
    reconstructed_question: str,
    options: list[dict[str, str]],
) -> float:
    score = max(0.0, min(1.0, float(repair_confidence)))
    if len(_to_str(reconstructed_question)) < 22:
        score -= 0.18
    if options and len(options) < 2:
        score -= 0.1

    severe = {
        "invalid_token_sequences",
        "missing_function_definition",
        "broken_limit_expression",
        "corrupted_piecewise",
        "dangling_operator",
        "unbalanced_parenthesis",
    }
    for token in detected_issues:
        if token in severe:
            score -= 0.08
        else:
            score -= 0.03
    return round(max(0.0, min(1.0, score)), 4)


def _publish_risk_score(integrity: float, detected_issues: list[str]) -> float:
    risk = 1.0 - integrity
    if any(x in {"invalid_token_sequences", "missing_function_definition"} for x in detected_issues):
        risk += 0.08
    return round(max(0.0, min(1.0, risk)), 4)


def _status_from_integrity(
    integrity: float,
    *,
    detected_issues: list[str],
    reconstructed_question: str,
) -> str:
    if integrity >= 0.9:
        return "safe"
    if integrity >= 0.75:
        return "review"
    if integrity < 0.45:
        return "unrecoverable"
    if len(_to_str(reconstructed_question).strip()) < 16 and any(
        token in {"missing_function_definition", "invalid_token_sequences"}
        for token in detected_issues
    ):
        return "unrecoverable"
    return "reject"


def _normalize_row_options_for_storage(options: list[dict[str, str]], raw_row: dict[str, Any]) -> Any:
    raw = raw_row.get("options")
    if isinstance(raw, list):
        # Keep app-compatible list format.
        if raw and isinstance(raw[0], str):
            return [opt.get("text", "") for opt in options]
        return options
    if isinstance(raw, dict):
        out: dict[str, str] = {}
        for opt in options:
            label = _to_str(opt.get("label")).upper()
            text = _to_str(opt.get("text"))
            if label and text:
                out[label] = text
        return out
    return options


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build layer-2 finalized OCR-reconstructed bank from import_question_bank.json"
    )
    parser.add_argument(
        "--input",
        default="data/app/import_question_bank.json",
        help="Source question bank JSON",
    )
    parser.add_argument(
        "--output",
        default="data/app/import_question_bank_layer2_finalized.json",
        help="Full finalized layer-2 output JSON",
    )
    parser.add_argument(
        "--best-output",
        default="data/app/import_question_bank_layer2_best.json",
        help="Best-only filtered output JSON",
    )
    parser.add_argument(
        "--report",
        default="data/app/repair_report_layer2.json",
        help="Layer-2 run report JSON",
    )
    parser.add_argument(
        "--snapshot",
        default="data/app/import_question_bank.layer2_snapshot.json",
        help="Snapshot copy created before processing",
    )
    parser.add_argument(
        "--safe-threshold",
        type=float,
        default=0.9,
        help="Integrity threshold for best-only safe set",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional cap for debugging (0 = all rows)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=250,
        help="Print progress every N rows",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    sys.path.insert(0, str((root / "app").resolve()))

    from latex_sanitizer import sanitize_latex
    from app.data.repair_engine.math_repair_engine import MathRepairEngine
    from app.data.repair_engine.symbol_normalizer import SymbolNormalizer

    input_path = (root / args.input).resolve()
    output_path = (root / args.output).resolve()
    best_path = (root / args.best_output).resolve()
    report_path = (root / args.report).resolve()
    snapshot_path = (root / args.snapshot).resolve()

    if not input_path.exists():
        raise SystemExit(f"missing input bank: {input_path}")

    source_payload = _load_json_with_retries(input_path)
    if not isinstance(source_payload, list):
        raise SystemExit(f"input is not a question list: {input_path}")

    _atomic_write_json(snapshot_path, source_payload)
    rows = source_payload
    if args.max_rows and args.max_rows > 0:
        rows = rows[: args.max_rows]

    repair_engine = MathRepairEngine()
    symbol_normalizer = SymbolNormalizer()

    finalized_rows: list[dict[str, Any]] = []
    best_rows: list[dict[str, Any]] = []

    status_counts: Counter[str] = Counter()
    issue_counts: Counter[str] = Counter()
    confidence_sum = 0.0
    integrity_sum = 0.0
    risk_sum = 0.0

    started_at = _now_iso()
    total = len(rows)
    for idx, raw_row in enumerate(rows, start=1):
        if not isinstance(raw_row, dict):
            continue

        qtext = _to_str(
            raw_row.get("question_text")
            or raw_row.get("question")
            or raw_row.get("text")
        )
        options = _normalize_options(raw_row.get("options"))
        correct_answer = _extract_correct_answer(raw_row)
        q_type = _to_str(raw_row.get("type") or raw_row.get("question_type")).upper()

        pass1_text, pass1_actions, pass1_issues = _phase1_extra_cleanup(qtext)
        norm1 = symbol_normalizer.normalize_text(pass1_text)
        if norm1.text:
            pass1_text = norm1.text
        pass1_actions.extend(norm1.actions)
        norm_opts, norm_opt_actions = symbol_normalizer.normalize_options(options)
        if norm_opts:
            options = norm_opts
        pass1_actions.extend(norm_opt_actions)

        repaired = repair_engine.repair_question(
            {
                "question_id": _to_str(raw_row.get("question_id") or raw_row.get("id")),
                "question_text": pass1_text,
                "options": options,
                "correct_answer": correct_answer,
                "type": q_type,
            },
            corpus=None,
        )

        reconstructed_question = _to_str(repaired.repaired_question_text).strip()
        latex_question = sanitize_latex(reconstructed_question)

        out_options: list[dict[str, str]] = []
        for opt in repaired.options:
            if not isinstance(opt, dict):
                continue
            label = _to_str(opt.get("label")).upper()
            text = _to_str(opt.get("text")).strip()
            if not label or not text:
                continue
            out_options.append(
                {
                    "label": label,
                    "text": text,
                    "latex": sanitize_latex(text),
                }
            )

        detected_issues = _map_issue_tokens(
            phase1_issues=pass1_issues,
            validation_issues=list(repaired.validation_issues or []),
            solver_notes=list(repaired.solver_notes or []),
        )

        structural_integrity_score = _score_integrity(
            repair_confidence=float(repaired.repair_confidence or 0.0),
            detected_issues=detected_issues,
            reconstructed_question=reconstructed_question,
            options=out_options,
        )
        publish_risk_score = _publish_risk_score(
            integrity=structural_integrity_score,
            detected_issues=detected_issues,
        )
        repair_status = _status_from_integrity(
            structural_integrity_score,
            detected_issues=detected_issues,
            reconstructed_question=reconstructed_question,
        )
        concept_tags = _concept_tags(
            reconstructed_question,
            _to_str(raw_row.get("chapter")),
            _to_str(raw_row.get("subject")),
        )
        difficulty_estimate = _difficulty_estimate(raw_row, structural_integrity_score, concept_tags)

        row_out = dict(raw_row)
        row_out["question_text"] = reconstructed_question
        if "question_text_latex" in row_out:
            row_out["question_text_latex"] = latex_question
        row_out["options"] = _normalize_row_options_for_storage(out_options, raw_row)
        if "options_latex" in row_out:
            row_out["options_latex"] = {
                _to_str(opt.get("label")).upper(): _to_str(opt.get("latex"))
                for opt in out_options
            }
        row_out["correct_answer"] = dict(repaired.correct_answer or {})
        row_out["repair_actions"] = list(
            dict.fromkeys([*pass1_actions, *list(repaired.repair_actions or [])])
        )
        row_out["repair_confidence"] = round(float(repaired.repair_confidence or 0.0), 4)
        row_out["repair_status"] = repair_status
        row_out["publish_risk_score"] = publish_risk_score
        row_out["structural_integrity_score"] = structural_integrity_score
        row_out["detected_issues"] = detected_issues
        row_out["reconstructed_question"] = reconstructed_question
        row_out["latex_question"] = latex_question
        row_out["concept_tags"] = concept_tags
        row_out["difficulty_estimate"] = difficulty_estimate
        row_out["layer2_reconstruction"] = {
            "repair_status": repair_status,
            "repair_confidence": round(float(repaired.repair_confidence or 0.0), 4),
            "publish_risk_score": publish_risk_score,
            "detected_issues": detected_issues,
            "reconstructed_question": reconstructed_question,
            "latex_question": latex_question,
            "options": out_options,
            "concept_tags": concept_tags,
            "difficulty_estimate": difficulty_estimate,
            "structural_integrity_score": structural_integrity_score,
        }

        finalized_rows.append(row_out)
        if structural_integrity_score >= float(args.safe_threshold):
            best_rows.append(row_out)

        status_counts[repair_status] += 1
        confidence_sum += float(repaired.repair_confidence or 0.0)
        integrity_sum += structural_integrity_score
        risk_sum += publish_risk_score
        for token in detected_issues:
            issue_counts[token] += 1

        if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == total):
            pct = (idx / max(1, total)) * 100.0
            print(
                json.dumps(
                    {
                        "stage": "layer2_repair",
                        "progress_pct": round(pct, 2),
                        "done": idx,
                        "total": total,
                        "safe_count": len(best_rows),
                        "status_counts": dict(status_counts),
                    },
                    ensure_ascii=False,
                )
            )

    _atomic_write_json(output_path, finalized_rows)
    _atomic_write_json(best_path, best_rows)

    seen = max(1, len(finalized_rows))
    report = {
        "started_at": started_at,
        "finished_at": _now_iso(),
        "input": str(input_path),
        "snapshot": str(snapshot_path),
        "output": str(output_path),
        "best_output": str(best_path),
        "safe_threshold": float(args.safe_threshold),
        "rows_seen": len(finalized_rows),
        "best_rows": len(best_rows),
        "status_counts": dict(status_counts),
        "avg_repair_confidence": round(confidence_sum / seen, 4),
        "avg_structural_integrity_score": round(integrity_sum / seen, 4),
        "avg_publish_risk_score": round(risk_sum / seen, 4),
        "top_detected_issues": dict(issue_counts.most_common(25)),
    }
    _atomic_write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
