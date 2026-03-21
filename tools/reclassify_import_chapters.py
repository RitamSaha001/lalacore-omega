#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from app.data.local_app_data_service import LocalAppDataService


def atomic_write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _best_cross_track_chapter(
    svc: LocalAppDataService,
    row: dict[str, Any],
) -> tuple[str, str, float]:
    bag = svc._import_row_context_text(row)
    if not bag.strip():
        return "", "", 0.0
    best_track = ""
    best_chapter = ""
    best_score = 0.0
    for track in ("Mathematics", "Physics", "Chemistry"):
        candidates = svc._infer_import_chapter_candidates(
            track=track,
            text_bag=bag,
            max_candidates=1,
        )
        if not candidates:
            continue
        top = candidates[0]
        chapter = str(top.get("chapter") or "").strip()
        score = float(top.get("score") or 0.0)
        if not chapter:
            continue
        if score > best_score:
            best_track = track
            best_chapter = chapter
            best_score = score
    return best_track, best_chapter, best_score


def main() -> None:
    p = argparse.ArgumentParser(description="Reclassify chapter tags in import_question_bank.json")
    p.add_argument(
        "--bank",
        default="data/app/import_question_bank.json",
        help="Path to import question bank JSON",
    )
    p.add_argument(
        "--teacher-id",
        default="mathongo_offline_sync",
        help="Filter by teacher_id (empty means all rows)",
    )
    p.add_argument(
        "--min-updates",
        type=int,
        default=1,
        help="Minimum updates required to write changes",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute stats only; do not write file",
    )
    args = p.parse_args()

    bank_path = Path(args.bank)
    if not bank_path.exists():
        raise SystemExit(f"bank file not found: {bank_path}")

    raw = json.loads(bank_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit("bank file is not a JSON list")

    svc = LocalAppDataService()
    teacher_id_filter = str(args.teacher_id or "").strip()
    changed = 0
    scanned = 0
    subject_reassigned = 0
    cross_track_reclassified = 0
    by_old_new: dict[str, int] = {}

    for row in raw:
        if not isinstance(row, dict):
            continue
        if teacher_id_filter and str(row.get("teacher_id") or "").strip() != teacher_id_filter:
            continue
        scanned += 1
        old_subject = str(row.get("subject") or "").strip()
        old_chapter = str(row.get("chapter") or "").strip()
        new_chapter = svc._resolve_import_row_chapter(row)
        new_tags = svc._resolve_import_row_chapter_tags(row, max_tags=3)

        # For generic/mixed rows that still remain unresolved, try a cross-track
        # best-chapter guess directly from question/solution text.
        if (
            (not new_chapter or new_chapter == old_chapter)
            and svc._import_chapter_is_generic(old_chapter)
        ):
            best_track, best_chapter, best_score = _best_cross_track_chapter(svc, row)
            if best_chapter and best_score >= 1.4:
                new_chapter = best_chapter
                cross_track_reclassified += 1
                if best_track and best_track != old_subject:
                    row["subject"] = best_track
                    subject_reassigned += 1
                new_tags = svc._resolve_import_row_chapter_tags(
                    row,
                    subject_override=str(row.get("subject") or old_subject or "Mathematics"),
                    chapter_override=new_chapter,
                    max_tags=3,
                )

        if new_tags:
            row["chapter_tags"] = new_tags
        if new_chapter and new_chapter != old_chapter:
            row["chapter"] = new_chapter
            changed += 1
            key = f"{old_chapter} -> {new_chapter}"
            by_old_new[key] = by_old_new.get(key, 0) + 1

    print(
        json.dumps(
            {
                "bank": str(bank_path),
                "teacher_id_filter": teacher_id_filter,
                "rows_scanned": scanned,
                "rows_changed": changed,
                "subject_reassigned": subject_reassigned,
                "cross_track_reclassified": cross_track_reclassified,
                "top_transitions": sorted(
                    ({"transition": k, "count": v} for k, v in by_old_new.items()),
                    key=lambda item: int(item["count"]) * -1,
                )[:20],
                "dry_run": bool(args.dry_run),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.dry_run:
        return
    if changed < max(0, int(args.min_updates)):
        print("skip_write: below min-updates threshold")
        return
    atomic_write_json(bank_path, raw)
    print(f"written: {bank_path}")


if __name__ == "__main__":
    main()
