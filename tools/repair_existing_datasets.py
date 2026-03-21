#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _option_dicts(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if isinstance(raw, dict):
        entries = sorted(raw.items(), key=lambda kv: _to_str(kv[0]).upper())
        for idx, (_, value) in enumerate(entries):
            text = _to_str(value).strip()
            if not text:
                continue
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
            if not text:
                continue
            out.append({"label": label, "text": text})
    return out


def _extract_correct_answer(row: dict[str, Any]) -> dict[str, Any]:
    existing = row.get("correct_answer")
    if isinstance(existing, dict):
        single = _to_str(existing.get("single")).upper()
        multiple = existing.get("multiple")
        if not isinstance(multiple, list):
            multiple = []
        multiple = [_to_str(x).upper() for x in multiple if _to_str(x).strip()]
        numerical = _to_str(existing.get("numerical")).strip()
        return {
            "single": single or None,
            "multiple": multiple,
            "numerical": numerical or None,
            "tolerance": existing.get("tolerance"),
        }

    single = _to_str(
        row.get("_correct_option")
        or row.get("correct_option")
        or row.get("single_correct")
    ).upper()
    multiple_raw = row.get("_correct_answers") or row.get("correct_answers") or row.get("multiple_correct")
    if isinstance(multiple_raw, list):
        multiple = [_to_str(x).upper() for x in multiple_raw if _to_str(x).strip()]
    else:
        multiple = []
    numerical = _to_str(
        row.get("_numerical_answer")
        or row.get("numerical_answer")
        or row.get("answer")
        or row.get("correct")
    ).strip()
    if not multiple and single:
        multiple = [single]
    return {
        "single": single or None,
        "multiple": multiple,
        "numerical": numerical or None,
        "tolerance": row.get("numerical_tolerance") or row.get("tolerance"),
    }


def _write_back_correct_answer(row: dict[str, Any], repaired: dict[str, Any]) -> None:
    single = _to_str(repaired.get("single")).upper() or None
    multiple = repaired.get("multiple")
    if not isinstance(multiple, list):
        multiple = []
    multiple = [_to_str(x).upper() for x in multiple if _to_str(x).strip()]
    numerical = _to_str(repaired.get("numerical")).strip() or None
    tolerance = repaired.get("tolerance")

    row["correct_answer"] = {
        "single": single,
        "multiple": multiple,
        "numerical": numerical,
        "tolerance": tolerance,
    }
    if "_correct_option" in row:
        row["_correct_option"] = single or ""
    if "_correct_answers" in row:
        row["_correct_answers"] = multiple
    if "_numerical_answer" in row:
        row["_numerical_answer"] = numerical or ""
    if "correct_option" in row:
        row["correct_option"] = single or ""
    if "correct_answers" in row:
        row["correct_answers"] = multiple
    if "numerical_answer" in row:
        row["numerical_answer"] = numerical or ""
    if "answer" in row and numerical is not None:
        row["answer"] = numerical


def _repair_question_record(
    row: dict[str, Any],
    *,
    engine: Any,
    preserve_string_options: bool = False,
) -> tuple[bool, dict[str, Any]]:
    before = json.dumps(row, ensure_ascii=False, sort_keys=True)
    question_text = _to_str(row.get("question_text") or row.get("question") or row.get("text"))
    question_type = _to_str(row.get("type") or row.get("question_type")).upper()
    options = _option_dicts(row.get("options"))
    correct_answer = _extract_correct_answer(row)
    repaired = engine.repair_question(
        question_text=question_text,
        options=options,
        correct_answer=correct_answer,
        question_type=question_type,
    )

    row["question_text"] = repaired.question_text
    if "question_text_latex" in row:
        row["question_text_latex"] = repaired.question_text

    if preserve_string_options:
        row["options"] = [opt.get("text", "") for opt in repaired.options]
    else:
        row["options"] = repaired.options

    if "options_latex" in row:
        row["options_latex"] = {
            _to_str(opt.get("label")).upper(): _to_str(opt.get("text"))
            for opt in repaired.options
            if isinstance(opt, dict)
        }

    _write_back_correct_answer(row, repaired.correct_answer)

    row["repair_actions"] = list(repaired.repair_actions or [])
    row["repair_confidence"] = float(repaired.repair_confidence or 0.0)
    row["repair_status"] = _to_str(repaired.repair_status) or "none"

    issues = [_to_str(x) for x in (repaired.repair_issues or []) if _to_str(x)]
    if issues:
        existing = row.get("validation_errors")
        merged: list[str] = []
        if isinstance(existing, list):
            merged.extend([_to_str(x) for x in existing if _to_str(x)])
        for issue in issues:
            if issue not in merged:
                merged.append(issue)
        row["validation_errors"] = merged

    status = _to_str(row.get("validation_status")).lower()
    if row["repair_status"] == "manual_review" and status != "invalid":
        row["validation_status"] = "review"

    after = json.dumps(row, ensure_ascii=False, sort_keys=True)
    changed = before != after
    stats = {
        "changed": int(changed),
        "repair_status": row.get("repair_status") or "none",
        "repair_confidence": float(row.get("repair_confidence") or 0.0),
        "actions": list(row.get("repair_actions") or []),
    }
    return changed, stats


def _backup_file(path: Path, backup_dir: Path, stamp: str) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{path.name}.{stamp}.bak"
    shutil.copy2(path, backup_path)
    return backup_path


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Repair OCR-damaged question datasets in data/app using deterministic graph-style rules."
    )
    parser.add_argument("--app-dir", default="data/app", help="Path to app data directory")
    parser.add_argument(
        "--targets",
        nargs="+",
        default=["import_question_bank.json", "import_drafts.json", "ai_generated_quizzes.json"],
        help="Target JSON filenames inside app dir",
    )
    parser.add_argument("--no-backup", action="store_true", help="Skip backup creation")
    parser.add_argument(
        "--report",
        default="data/app/repair_report.json",
        help="Report output JSON path",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    app_dir = (root / args.app_dir).resolve()
    if not app_dir.exists():
        raise SystemExit(f"app dir missing: {app_dir}")

    import sys

    sys.path.insert(0, str((root / "app" / "data").resolve()))
    from question_repair_engine import QuestionRepairEngine

    engine = QuestionRepairEngine()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = app_dir / "backups"
    report: dict[str, Any] = {
        "started_at_utc": stamp,
        "app_dir": str(app_dir),
        "targets": [],
        "totals": {
            "questions_seen": 0,
            "questions_changed": 0,
            "manual_review_count": 0,
            "avg_repair_confidence": 0.0,
            "action_counts": {},
        },
    }

    action_counts: Counter[str] = Counter()
    confidence_sum = 0.0

    for target_name in args.targets:
        path = app_dir / target_name
        if not path.exists():
            report["targets"].append(
                {"file": str(path), "status": "missing"}
            )
            continue

        backup_path = None
        if not args.no_backup:
            backup_path = _backup_file(path, backup_dir, stamp)

        payload = _load_json(path)
        target_stats = {
            "file": str(path),
            "backup": str(backup_path) if backup_path else "",
            "status": "ok",
            "questions_seen": 0,
            "questions_changed": 0,
            "manual_review_count": 0,
            "avg_repair_confidence": 0.0,
            "action_counts": {},
        }

        local_action_counts: Counter[str] = Counter()
        local_conf_sum = 0.0

        if target_name == "import_question_bank.json":
            if not isinstance(payload, list):
                target_stats["status"] = "invalid_payload"
            else:
                for row in payload:
                    if not isinstance(row, dict):
                        continue
                    changed, stats = _repair_question_record(row, engine=engine)
                    target_stats["questions_seen"] += 1
                    target_stats["questions_changed"] += int(changed)
                    if stats["repair_status"] == "manual_review":
                        target_stats["manual_review_count"] += 1
                    local_conf_sum += float(stats["repair_confidence"])
                    for action in stats["actions"]:
                        local_action_counts[action] += 1

        elif target_name == "import_drafts.json":
            if not isinstance(payload, list):
                target_stats["status"] = "invalid_payload"
            else:
                for draft in payload:
                    if not isinstance(draft, dict):
                        continue
                    questions = draft.get("questions")
                    if not isinstance(questions, list):
                        continue
                    for row in questions:
                        if not isinstance(row, dict):
                            continue
                        changed, stats = _repair_question_record(row, engine=engine)
                        target_stats["questions_seen"] += 1
                        target_stats["questions_changed"] += int(changed)
                        if stats["repair_status"] == "manual_review":
                            target_stats["manual_review_count"] += 1
                        local_conf_sum += float(stats["repair_confidence"])
                        for action in stats["actions"]:
                            local_action_counts[action] += 1
                    draft["question_count"] = len([q for q in questions if isinstance(q, dict)])

        elif target_name == "ai_generated_quizzes.json":
            if not isinstance(payload, list):
                target_stats["status"] = "invalid_payload"
            else:
                for quiz in payload:
                    if not isinstance(quiz, dict):
                        continue
                    raw_q = quiz.get("questions_json")
                    parsed = None
                    encoded = False
                    if isinstance(raw_q, str):
                        try:
                            parsed = json.loads(raw_q)
                            encoded = True
                        except Exception:
                            parsed = None
                    elif isinstance(raw_q, list):
                        parsed = raw_q
                    if not isinstance(parsed, list):
                        continue
                    for row in parsed:
                        if not isinstance(row, dict):
                            continue
                        changed, stats = _repair_question_record(
                            row,
                            engine=engine,
                            preserve_string_options=True,
                        )
                        target_stats["questions_seen"] += 1
                        target_stats["questions_changed"] += int(changed)
                        if stats["repair_status"] == "manual_review":
                            target_stats["manual_review_count"] += 1
                        local_conf_sum += float(stats["repair_confidence"])
                        for action in stats["actions"]:
                            local_action_counts[action] += 1
                    quiz["question_count"] = len([q for q in parsed if isinstance(q, dict)])
                    quiz["questions_json"] = (
                        json.dumps(parsed, ensure_ascii=False) if encoded else parsed
                    )

        if target_stats["questions_seen"] > 0:
            target_stats["avg_repair_confidence"] = round(
                local_conf_sum / target_stats["questions_seen"], 4
            )
        target_stats["action_counts"] = dict(sorted(local_action_counts.items()))
        _save_json(path, payload)

        report["targets"].append(target_stats)
        report["totals"]["questions_seen"] += target_stats["questions_seen"]
        report["totals"]["questions_changed"] += target_stats["questions_changed"]
        report["totals"]["manual_review_count"] += target_stats["manual_review_count"]
        confidence_sum += local_conf_sum
        for key, value in local_action_counts.items():
            action_counts[key] += value

    if report["totals"]["questions_seen"] > 0:
        report["totals"]["avg_repair_confidence"] = round(
            confidence_sum / report["totals"]["questions_seen"], 4
        )
    report["totals"]["action_counts"] = dict(sorted(action_counts.items()))
    report["finished_at_utc"] = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = (root / args.report).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    _save_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
