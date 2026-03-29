from __future__ import annotations

import asyncio
import base64
import csv
import html
import hashlib
import io
import json
import math
import mimetypes
import os
import random
import re
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from urllib.request import Request, urlopen

from grading_engine import (
    GradingError,
    GradingValidationError,
    evaluate_attempt,
)
try:
    from .question_repair_engine import QuestionRepairEngine
except Exception:  # pragma: no cover - fallback for direct module execution
    from question_repair_engine import QuestionRepairEngine
from latex_sanitizer import (
    QuestionStructureError,
    sanitize_latex,
    sanitize_question_payload,
    validate_question_structure,
)
from services.atlas_incident_email_service import AtlasIncidentEmailService
from services.atlas_memory_service import AtlasMemoryService
from app.storage.sqlite_json_store import SQLiteJsonBlobStore
from core.analytics_insight_engine import (
    analyze_exam_entry,
    class_summary_entry,
    student_intelligence_entry,
    student_profile_entry,
)
from core.material_generation_engine import material_generation_entry


class LocalAppDataService:
    """SQLite-backed app-data authority with JSON migration for legacy state."""

    _IMPORT_QUESTION_START_RE = re.compile(
        r"^\s*(?:[qg](?:uestion)?\s*)?\d+\s*[\).:\-]\s*",
        re.IGNORECASE,
    )
    _IMPORT_QUESTION_NUMBER_RE = re.compile(
        r"^\s*(?:[qg](?:uestion)?\s*)?(\d{1,4})\s*[\).:\-]",
        re.IGNORECASE,
    )
    _IMPORT_OPTION_START_RE = re.compile(
        r"^\s*(?:\(?([A-Za-z@]|[0-9])\)?[\).:\-]?)\s+(.+)$"
    )
    _IMPORT_ANSWER_LINE_RE = re.compile(
        r"^\s*(?:ans(?:wer)?|a(?:newer|neewr|nser)|correct(?:\s*answer)?)\s*[:.\-]\s*(.+)$",
        re.IGNORECASE,
    )
    _IMPORT_SOLUTION_LINE_RE = re.compile(
        r"^\s*(?:detailed\s+)?(?:solution|explanation|sol\.?)\s*[:\-]\s*(.*)$",
        re.IGNORECASE,
    )
    _IMPORT_NUMERIC_TOKEN_RE = re.compile(
        r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
    )

    def __init__(
        self,
        assessments_file: str | Path | None = None,
        materials_file: str | Path | None = None,
        live_class_schedule_file: str | Path | None = None,
        uploads_file: str | Path | None = None,
        ai_quizzes_file: str | Path | None = None,
        results_file: str | Path | None = None,
        teacher_review_file: str | Path | None = None,
        import_drafts_file: str | Path | None = None,
        import_question_bank_file: str | Path | None = None,
        jee_bank_x_file: str | Path | None = None,
        auth_users_file: str | Path | None = None,
        auth_storage_db_file: str | Path | None = None,
        storage_db_file: str | Path | None = None,
    ) -> None:
        root = Path(__file__).resolve().parents[2]
        app_dir = root / "data" / "app"
        auth_dir = root / "data" / "auth"
        app_dir.mkdir(parents=True, exist_ok=True)
        auth_dir.mkdir(parents=True, exist_ok=True)
        self._quizzes_dir = app_dir / "quizzes"
        self._uploads_dir = app_dir / "uploads"
        self._quizzes_dir.mkdir(parents=True, exist_ok=True)
        self._uploads_dir.mkdir(parents=True, exist_ok=True)

        self._assessments_file = (
            Path(assessments_file) if assessments_file else app_dir / "assessments.json"
        )
        self._materials_file = (
            Path(materials_file) if materials_file else app_dir / "materials.json"
        )
        self._live_class_schedule_file = (
            Path(live_class_schedule_file)
            if live_class_schedule_file
            else app_dir / "live_class_schedule.json"
        )
        self._uploads_file = (
            Path(uploads_file) if uploads_file else app_dir / "uploads.json"
        )
        self._ai_quizzes_file = (
            Path(ai_quizzes_file) if ai_quizzes_file else app_dir / "ai_generated_quizzes.json"
        )
        self._results_file = (
            Path(results_file) if results_file else app_dir / "results.json"
        )
        self._teacher_review_file = (
            Path(teacher_review_file)
            if teacher_review_file
            else app_dir / "teacher_review_queue.json"
        )
        self._import_drafts_file = (
            Path(import_drafts_file) if import_drafts_file else app_dir / "import_drafts.json"
        )
        self._import_question_bank_file = (
            Path(import_question_bank_file)
            if import_question_bank_file
            else app_dir / "import_question_bank.json"
        )
        self._jee_bank_x_file = (
            Path(jee_bank_x_file)
            if jee_bank_x_file
            else (
                self._import_question_bank_file.with_name("JEE_BANK_X.json")
                if import_question_bank_file
                else app_dir / "JEE_BANK_X.json"
            )
        )
        self._chat_users_file = app_dir / "chat_users.json"
        self._chat_threads_file = app_dir / "chat_threads.json"
        self._doubts_file = app_dir / "doubts.json"
        self._auth_users_file = (
            Path(auth_users_file) if auth_users_file else auth_dir / "users.json"
        )
        self._auth_storage = SQLiteJsonBlobStore(
            Path(auth_storage_db_file)
            if auth_storage_db_file
            else self._auth_users_file.parent / "auth_store.sqlite3"
        )
        default_storage_root = (
            self._assessments_file.parent
            if any(
                value is not None
                for value in (
                    assessments_file,
                    materials_file,
                    live_class_schedule_file,
                    uploads_file,
                    ai_quizzes_file,
                    results_file,
                    teacher_review_file,
                    import_drafts_file,
                    import_question_bank_file,
                    jee_bank_x_file,
                )
            )
            else app_dir
        )
        self._storage = SQLiteJsonBlobStore(
            Path(storage_db_file)
            if storage_db_file
            else default_storage_root / "app_data.sqlite3"
        )
        self._storage_keys: dict[Path, str] = {
            self._assessments_file.resolve(): "app_assessments",
            self._materials_file.resolve(): "app_materials",
            self._live_class_schedule_file.resolve(): "app_live_class_schedule",
            self._uploads_file.resolve(): "app_uploads",
            self._ai_quizzes_file.resolve(): "app_ai_generated_quizzes",
            self._results_file.resolve(): "app_results",
            self._teacher_review_file.resolve(): "app_teacher_review_queue",
            self._import_drafts_file.resolve(): "app_import_drafts",
            self._chat_users_file.resolve(): "app_chat_users",
            self._chat_threads_file.resolve(): "app_chat_threads",
            self._doubts_file.resolve(): "app_doubts",
        }

        self._lock = asyncio.Lock()
        self._loaded = False
        self._assessments: list[dict[str, Any]] = []
        self._materials: list[dict[str, Any]] = []
        self._live_class_schedule: list[dict[str, Any]] = []
        self._uploads: dict[str, dict[str, Any]] = {}
        self._ai_quizzes: list[dict[str, Any]] = []
        self._results: list[dict[str, Any]] = []
        self._teacher_review_queue: list[dict[str, Any]] = []
        self._import_drafts: list[dict[str, Any]] = []
        self._import_question_bank: list[dict[str, Any]] = []
        self._chat_users: dict[str, dict[str, Any]] = {}
        self._chat_threads: dict[str, dict[str, Any]] = {}
        self._doubts: list[dict[str, Any]] = []
        self._last_pyq_web_diagnostics: dict[str, Any] = {}
        self._web_search_cache: dict[str, dict[str, Any]] = {}
        self._web_page_evidence_cache: dict[str, dict[str, Any]] = {}
        self._web_cache_ttl_s = 14 * 60
        self._web_cache_max_entries = 320
        self._import_chapter_infer_cache: dict[str, str] = {}
        self._import_chapter_cache_max_entries = 12_000
        self._question_repair_engine = QuestionRepairEngine()
        self._atlas_memory = AtlasMemoryService(root=app_dir)
        self._atlas_incident_email = AtlasIncidentEmailService()
        self._live_class_schedule_event_queues: set[asyncio.Queue[dict[str, Any]]] = set()
        self._material_ai_cache: dict[str, dict[str, Any]] = {}
        self._material_ai_status: dict[str, dict[str, Any]] = {}

    def build_atlas_student_memory(
        self,
        *,
        account_id: str,
        fallback_profile: dict[str, Any] | None = None,
        recent_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._atlas_memory.get_student_memory(
            account_id=account_id,
            fallback_profile=fallback_profile,
            recent_context=recent_context,
        )

    def atlas_tool_stats_summary(self, *, limit: int = 18) -> dict[str, Any]:
        return self._atlas_memory.get_tool_stats_summary(limit=limit)

    def record_atlas_tool_execution(
        self,
        *,
        account_id: str,
        tool_name: str,
        category: str,
        success: bool,
        latency_ms: int,
        context: dict[str, Any] | None = None,
        args: dict[str, Any] | None = None,
        observation: str = "",
    ) -> dict[str, Any]:
        return self._atlas_memory.record_tool_execution(
            account_id=account_id,
            tool_name=tool_name,
            category=category,
            success=success,
            latency_ms=latency_ms,
            context=context,
            args=args,
            observation=observation,
        )

    def atlas_passive_events(
        self,
        *,
        account_id: str,
        context: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        return self._atlas_memory.generate_passive_events(
            account_id=account_id,
            context=context,
        )

    async def atlas_health_snapshot(
        self,
        *,
        role: str = "student",
        account_id: str = "",
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._ensure_loaded()
        account_id = self._str(account_id)
        context = dict(context or {})
        role = self._str(role).lower() or "student"
        attempted_ids = {
            self._safe_id(
                row.get("quiz_id") or row.get("id") or row.get("assessment_id")
            )
            for row in self._results
            if (
                not account_id
                or self._str(
                    row.get("account_id") or row.get("student_id") or row.get("user_id")
                )
                == account_id
            )
        }
        pending_homeworks = 0
        pending_exams = 0
        for row in self._assessments:
            quiz_id = self._safe_id(row.get("id") or row.get("quiz_id"))
            if quiz_id and quiz_id in attempted_ids:
                continue
            row_role = self._str(row.get("role") or row.get("viewer_role")).lower()
            if role == "student" and row_role == "teacher":
                continue
            quiz_type = self._str(row.get("type")).lower()
            if quiz_type == "homework":
                pending_homeworks += 1
            elif quiz_type == "exam" or "ai" in quiz_type:
                pending_exams += 1
        recent_material_ai = sorted(
            (dict(value) for value in self._material_ai_status.values()),
            key=lambda row: self._to_int(row.get("updated_at"), 0),
            reverse=True,
        )[:6]
        failed_material_ai = [
            row
            for row in recent_material_ai
            if self._str(row.get("status")).lower() == "failed"
        ]
        schedule_counts: dict[str, int] = {}
        for row in self._live_class_schedule:
            status = self._str(row.get("status")).lower() or "unknown"
            schedule_counts[status] = schedule_counts.get(status, 0) + 1
        unread_threads = 0
        for raw in self._chat_threads.values():
            thread = dict(raw) if isinstance(raw, dict) else {}
            unread_threads += self._to_int(thread.get("unread_count"), 0)
        open_doubts = 0
        resolved_doubts = 0
        for row in self._doubts:
            status = self._str(row.get("status") or row.get("state")).lower()
            if "resolved" in status:
                resolved_doubts += 1
            else:
                open_doubts += 1
        return {
            "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "role": role,
            "account_id": account_id,
            "storage_counts": {
                "assessments": len(self._assessments),
                "materials": len(self._materials),
                "results": len(self._results),
                "scheduled_classes": len(self._live_class_schedule),
                "uploads": len(self._uploads),
                "chat_threads": len(self._chat_threads),
                "doubts": len(self._doubts),
                "teacher_review_queue": len(self._teacher_review_queue),
            },
            "pending_counts": {
                "homeworks": self._to_int(
                    context.get("pending_homework_count"),
                    pending_homeworks,
                ),
                "exams": self._to_int(
                    context.get("pending_exam_count"),
                    pending_exams,
                ),
            },
            "schedule_status_counts": schedule_counts,
            "material_ai_status": {
                "recent": recent_material_ai,
                "failed_count": len(failed_material_ai),
            },
            "web_diagnostics": dict(self._last_pyq_web_diagnostics),
            "atlas_tool_stats": self.atlas_tool_stats_summary(limit=10),
            "cache_health": {
                "web_search_entries": len(self._web_search_cache),
                "web_page_evidence_entries": len(self._web_page_evidence_cache),
                "material_ai_cache_entries": len(self._material_ai_cache),
                "material_ai_status_entries": len(self._material_ai_status),
                "chapter_infer_cache_entries": len(self._import_chapter_infer_cache),
                "cache_ttl_s": self._web_cache_ttl_s,
                "cache_max_entries": self._web_cache_max_entries,
            },
            "runtime_health": {
                "storage_db_path": str(self._storage.path),
                "storage_db_exists": self._storage.path.exists(),
                "process_id": os.getpid(),
                "cwd": os.getcwd(),
                "loaded": self._loaded,
            },
            "messaging_health": {
                "unread_threads": unread_threads,
                "open_doubts": open_doubts,
                "resolved_doubts": resolved_doubts,
            },
            "smtp_health": {
                "configured": self._atlas_incident_email.smtp_configured(),
            },
            "context_excerpt": self._atlas_compact_value(context),
        }

    async def handle_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_loaded()
        action = self._str(payload.get("action")).lower()

        if action in {
            "create_quiz",
            "create_assessment",
            "add_quiz",
            "save_quiz",
            "publish_quiz",
            "publish_assessment",
            "create_exam",
            "add_exam",
            "publish_exam",
            "create_homework",
            "add_homework",
            "publish_homework",
        }:
            return await self._create_quiz(payload)

        if action in {"list_assessments", "get_assessments", "get_quizzes"}:
            return await self._list_assessments()

        if action in {
            "ai_generate_quiz",
            "generate_ai_quiz",
            "ai_quiz_generate",
            "create_ai_quiz",
        }:
            return await self._ai_generate_quiz(payload)

        if action in {
            "ai_solve",
            "ai_chat",
            "general_chat",
            "chat_ai",
        }:
            return await self._ai_chat_or_solve(payload)

        if action in {"material_generate", "ai_material_generate"}:
            return await self._material_generate(payload)

        if action in {"material_query", "ai_material_query"}:
            return await self._material_query(payload)

        if action in {"material_status", "material_generate_status"}:
            return await self._material_status(payload)

        if action in {"ai_class_summary", "ai_teacher_class_summary", "class_summary"}:
            return await self._class_summary(payload)

        if action in {"ai_student_profile", "student_profile_ai"}:
            return await self._student_profile(payload)

        if action in {"student_intelligence", "ai_student_intelligence"}:
            return await self._student_intelligence(payload)

        if action in {"analyze_exam", "ai_analyze_exam"}:
            return await self._analyze_exam(payload)

        if action in {
            "atlas_report_issue",
            "report_system_issue",
            "report_app_issue",
            "support_diagnostic",
        }:
            return await self._atlas_report_issue(payload)

        if action in {"health_check", "ping", "noop"}:
            return await self._atlas_health_probe(payload)

        if action in {"get_ai_quiz", "read_ai_quiz", "fetch_ai_quiz"}:
            return await self._get_ai_quiz(payload)

        if action in {
            "evaluate_quiz_submission",
            "evaluate_quiz",
            "submit_quiz",
            "submit_assessment",
            "submit_quiz_attempt",
            "grade_quiz",
        }:
            return await self._evaluate_quiz_submission(payload)

        if action == "get_master_csv":
            return await self._get_master_csv()

        if action in {
            "add_material",
            "create_material",
            "save_material",
            "upload_material",
            "add_study_material",
            "create_study_material",
            "publish_material",
            "publish_study_material",
        }:
            return await self._add_material(payload)

        if action in {"get_materials", "list_materials"}:
            return await self._get_materials()

        if action in {
            "schedule_live_class",
            "schedule_class",
            "create_live_class",
            "create_class_schedule",
        }:
            return await self._schedule_live_class(payload)

        if action in {
            "list_live_class_schedule",
            "list_class_schedule",
            "get_live_classes",
            "get_class_schedule",
        }:
            return await self._list_live_class_schedule(payload)

        if action in {
            "start_live_class",
            "mark_class_live",
            "update_class_schedule_status",
            "end_live_class",
            "cancel_live_class",
        }:
            return await self._update_live_class_schedule_status(payload)

        if action in {
            "save_result",
            "submit_result",
            "upsert_result",
            "record_result",
        }:
            return await self._save_result(payload)

        if action in {"get_results", "list_results", "get_all_results"}:
            return await self._get_results()

        if action in {
            "queue_teacher_review",
            "add_teacher_review",
            "send_to_teacher_review",
            "enqueue_teacher_review",
        }:
            return await self._add_teacher_review(payload)

        if action in {"get_teacher_review_queue", "list_teacher_review_queue"}:
            return await self._get_teacher_review_queue()

        if action in {
            "lc9_parse_questions",
            "lc9_parse_question_import",
            "parse_import_questions",
            "lc9_parse_question_paper",
            "parse_question_paper_import",
        }:
            if action in {"lc9_parse_question_paper", "parse_question_paper_import"}:
                payload = dict(payload)
                payload.setdefault("question_paper_mode", True)
                payload.setdefault("web_ocr_fusion_mode", True)
            return await self._lc9_parse_import_questions(payload)

        if action in {
            "lc9_save_import_drafts",
            "save_import_drafts",
            "save_question_import_drafts",
        }:
            return await self._lc9_save_import_drafts(payload)

        if action in {
            "lc9_publish_questions",
            "publish_import_questions",
            "publish_question_bank_questions",
        }:
            return await self._lc9_publish_import_questions(payload)

        if action in {
            "lc9_web_verify_query",
            "web_verify_query",
            "verify_import_web_query",
            "cached_web_verify_search",
        }:
            return await self._lc9_web_verify_query(payload)

        if action in {
            "lc9_list_import_chapters",
            "list_import_chapters",
            "ai_chapter_picker_catalog",
            "get_ai_chapter_picker_catalog",
        }:
            return await self._lc9_list_import_chapters(payload)

        if action in {
            "upload_file",
            "upload_file_data",
            "upload_to_drive",
            "upload_material_file",
        }:
            return await self._upload_file(payload)

        if action in {"upsert_user_identity", "chat_register", "upsert_user"}:
            return await self._upsert_chat_user(payload)

        if action in {"search_chat_users", "chat_search_users", "list_users"}:
            return await self._search_chat_users(payload)

        if action in {"list_chat_directory", "get_chat_directory"}:
            return await self._list_chat_directory(payload)

        if action in {"create_chat_group", "group_create"}:
            return await self._create_chat_group(payload)

        if action in {"send_message", "peer_send"}:
            if self._to_bool(payload.get("is_peer")):
                return await self._send_peer_message(payload)
            return await self._send_doubt_message(payload)

        if action in {"mark_chat_read", "chat_mark_seen"}:
            return await self._mark_chat_read(payload)

        if action in {"get_doubts"}:
            return await self._get_doubts(payload)

        if action in {"raise_doubt"}:
            return await self._raise_doubt(payload)

        if action in {"doubt_reply"}:
            return await self._send_doubt_message(payload)

        if action in {"update_status", "doubt_update_status"}:
            return await self._update_doubt_status(payload)

        return {
            "ok": False,
            "status": "UNKNOWN_ACTION",
            "message": f"Unknown Action: {action}",
        }

    def subscribe_live_class_schedule_events(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=16)
        self._live_class_schedule_event_queues.add(queue)
        return queue

    def unsubscribe_live_class_schedule_events(
        self, queue: asyncio.Queue[dict[str, Any]]
    ) -> None:
        self._live_class_schedule_event_queues.discard(queue)

    def _publish_live_class_schedule_event(
        self,
        event_type: str,
        *,
        item: dict[str, Any] | None = None,
        actor_role: str = "",
    ) -> None:
        if not self._live_class_schedule_event_queues:
            return
        payload: dict[str, Any] = {
            "type": event_type,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "actor_role": actor_role,
            "class_id": self._str((item or {}).get("class_id")),
            "status": self._str((item or {}).get("status")),
            "class": item or {},
        }
        stale: list[asyncio.Queue[dict[str, Any]]] = []
        for queue in list(self._live_class_schedule_event_queues):
            try:
                if queue.full():
                    queue.get_nowait()
                queue.put_nowait(payload)
            except Exception:
                stale.append(queue)
        for queue in stale:
            self._live_class_schedule_event_queues.discard(queue)

    async def get_uploaded_file(self, file_id: str) -> dict[str, Any] | None:
        await self._ensure_loaded()
        key = self._str(file_id)
        if not key:
            return None
        meta = self._uploads.get(key)
        if not meta:
            return None
        path = Path(self._str(meta.get("path")))
        if not path.exists():
            return None
        return {
            "path": str(path),
            "name": self._str(meta.get("name")) or f"{key}.bin",
            "mime": self._str(meta.get("mime")) or "application/octet-stream",
        }

    async def get_quiz_csv_file(self, quiz_id: str) -> str | None:
        await self._ensure_loaded()
        key = self._safe_id(quiz_id)
        if not key:
            return None
        path = self._quizzes_dir / f"{key}.csv"
        if not path.exists():
            return None
        return str(path)

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            self._assessments = self._read_list(self._assessments_file)
            self._materials = self._read_list(self._materials_file)
            self._live_class_schedule = self._read_list(self._live_class_schedule_file)
            self._uploads = self._read_map(self._uploads_file)
            self._ai_quizzes = self._read_list(self._ai_quizzes_file)
            self._results = self._read_list(self._results_file)
            self._teacher_review_queue = self._read_list(self._teacher_review_file)
            self._import_drafts = self._read_list(self._import_drafts_file)
            self._import_question_bank = self._load_preferred_question_bank()
            self._chat_users = self._read_map(self._chat_users_file)
            self._chat_threads = self._read_map(self._chat_threads_file)
            self._doubts = self._read_list(self._doubts_file)
            self._loaded = True

    def _read_list(self, path: Path) -> list[dict[str, Any]]:
        storage_key = self._storage_keys.get(path.resolve())
        if storage_key:
            cached = self._storage.read_json(storage_key)
            normalized = self._normalize_list_blob(cached)
            if cached is not None:
                return normalized
        try:
            if not path.exists():
                parts_dir = path.parent / f"{path.name}.parts"
                if not parts_dir.exists() or not parts_dir.is_dir():
                    return []
                combined: list[dict[str, Any]] = []
                for part_path in sorted(parts_dir.glob("*.json")):
                    text = part_path.read_text(encoding="utf-8").strip()
                    if not text:
                        continue
                    decoded = json.loads(text)
                    if isinstance(decoded, list):
                        combined.extend(
                            dict(x) for x in decoded if isinstance(x, dict)
                        )
                if storage_key and combined:
                    self._storage.write_json(storage_key, combined)
                return combined
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                return []
            decoded = json.loads(text)
            normalized = self._normalize_list_blob(decoded)
            if storage_key and normalized:
                self._storage.write_json(storage_key, normalized)
            return normalized
        except Exception:
            pass
        return []

    def _read_map(self, path: Path) -> dict[str, dict[str, Any]]:
        storage_key = self._storage_keys.get(path.resolve())
        if storage_key:
            cached = self._storage.read_json(storage_key)
            normalized = self._normalize_map_blob(cached)
            if cached is not None:
                return normalized
        try:
            if not path.exists():
                return {}
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                return {}
            decoded = json.loads(text)
            normalized = self._normalize_map_blob(decoded)
            if storage_key and normalized:
                self._storage.write_json(storage_key, normalized)
            return normalized
        except Exception:
            pass
        return {}

    def _load_preferred_question_bank(self) -> list[dict[str, Any]]:
        primary_rows = self._read_list(self._import_question_bank_file)
        jee_bank_x_rows = self._read_list(self._jee_bank_x_file)
        if jee_bank_x_rows:
            return jee_bank_x_rows
        return primary_rows

    def _question_bank_runtime_flags(self, row: dict[str, Any]) -> tuple[bool, float]:
        repair_status = self._str(row.get("repair_status")).lower()
        verification = row.get("verification")
        if not isinstance(verification, dict):
            verification = {}
        if not verification and isinstance(row.get("math_repair_engine_x"), dict):
            x_block = row.get("math_repair_engine_x") or {}
            nested_verification = x_block.get("verification")
            if isinstance(nested_verification, dict):
                verification = nested_verification
        requires_human_review = self._to_bool(
            row.get("requires_human_review")
            or (
                (row.get("math_repair_engine_x") or {}).get("requires_human_review")
                if isinstance(row.get("math_repair_engine_x"), dict)
                else False
            )
        )
        hard_block = requires_human_review or repair_status in {
            "manual_review",
            "reject",
            "unrecoverable",
        }
        quality_bonus = 0.0
        if repair_status == "safe":
            quality_bonus += 0.18
        elif repair_status == "review":
            quality_bonus += 0.04
        if self._to_bool(verification.get("mathematical_consistency")):
            quality_bonus += 0.16
        if self._to_bool(verification.get("answer_key_verified")):
            quality_bonus += 0.14
        return hard_block, quality_bonus

    def _question_bank_row_matches_scope(
        self,
        *,
        row: dict[str, Any],
        subject: str,
        chapters: list[str],
        subtopics: list[str],
    ) -> bool:
        probe_subject = self._str(row.get("subject") or subject) or subject
        chapter_tags = self._resolve_import_row_chapter_tags(
            row=row,
            subject_override=probe_subject,
            max_tags=3,
        )
        probe = dict(row)
        if chapter_tags and not isinstance(probe.get("chapter_tags"), list):
            probe["chapter_tags"] = chapter_tags[:]
        if chapter_tags and not isinstance(probe.get("concept_tags"), list):
            probe["concept_tags"] = chapter_tags[:]
        try:
            prepared = self._prepare_question_for_grading(
                probe,
                fallback_question_id=self._str(
                    row.get("question_id") or row.get("id") or "bank_probe"
                ),
            )
        except (QuestionStructureError, ValueError):
            text_bag = " ".join(
                [
                    self._str(row.get("question_text") or row.get("question")),
                    self._str(row.get("chapter")),
                    " ".join(chapter_tags),
                    self._str(row.get("topic")),
                ]
            )
            return self._scope_match_score(
                text_bag,
                self._pyq_scope_tokens(
                    subject=subject,
                    chapters=chapters,
                    subtopics=subtopics,
                ),
            ) > 0.0
        prepared["chapter_tags"] = chapter_tags[:]
        if chapter_tags and not prepared.get("concept_tags"):
            prepared["concept_tags"] = chapter_tags[:]
        return self._question_matches_requested_scope(
            question=prepared,
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
        )

    def _write_list(self, path: Path, data: list[dict[str, Any]]) -> None:
        storage_key = self._storage_keys.get(path.resolve())
        if storage_key:
            self._storage.write_json(storage_key, data)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

    def _write_map(self, path: Path, data: dict[str, dict[str, Any]]) -> None:
        storage_key = self._storage_keys.get(path.resolve())
        if storage_key:
            self._storage.write_json(storage_key, data)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

    def _normalize_list_blob(self, decoded: Any) -> list[dict[str, Any]]:
        if not isinstance(decoded, list):
            return []
        return [dict(x) for x in decoded if isinstance(x, dict)]

    def _normalize_map_blob(self, decoded: Any) -> dict[str, dict[str, Any]]:
        if not isinstance(decoded, dict):
            return {}
        out: dict[str, dict[str, Any]] = {}
        for k, v in decoded.items():
            if isinstance(v, dict):
                out[str(k)] = dict(v)
        return out

    def _str(self, value: Any) -> str:
        return str(value or "").strip()

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _safe_id(self, raw: Any) -> str:
        out = re.sub(r"[^A-Za-z0-9_-]", "", self._str(raw))
        return out[:96]

    def _safe_chat_id(self, raw: Any) -> str:
        text = self._str(raw)
        if not text:
            return ""
        # Keep direct-thread separators like "|" intact for deterministic routing.
        text = re.sub(r"\s+", "", text)
        text = re.sub(r"[^A-Za-z0-9_|:-]", "", text)
        return text[:128]

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}_{self._now_ms()}"

    def _request_base_url(self, payload: dict[str, Any] | None = None) -> str:
        if not isinstance(payload, dict):
            return ""
        raw = self._str(
            payload.get("_request_base_url") or payload.get("request_base_url")
        )
        if not raw:
            return ""
        parsed = urlparse(raw)
        if not parsed.scheme or not parsed.netloc:
            return ""
        return raw.rstrip("/")

    def _base_url(self, payload: dict[str, Any] | None = None) -> str:
        request_base = self._request_base_url(payload)
        if request_base:
            return request_base
        explicit = self._str(os.getenv("APP_PUBLIC_BASE_URL"))
        if explicit:
            return explicit.rstrip("/")
        host = self._str(os.getenv("APP_PUBLIC_HOST", "10.0.2.2")) or "10.0.2.2"
        scheme = self._str(os.getenv("APP_PUBLIC_SCHEME", "http")) or "http"
        port = self._str(
            os.getenv("APP_PUBLIC_PORT")
            or os.getenv("LC9_HTTP_PORT")
            or os.getenv("PORT")
            or "8000"
        )
        return f"{scheme}://{host}:{port}".rstrip("/")

    def _upsert_by_id(
        self, items: list[dict[str, Any]], item_id: str, item: dict[str, Any]
    ) -> None:
        for i, existing in enumerate(items):
            if self._str(existing.get("id")) == item_id:
                items[i] = item
                return
        items.append(item)

    def _parse_questions(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        raw = payload.get("questions")
        if isinstance(raw, list):
            return [dict(x) for x in raw if isinstance(x, dict)]
        for key in ("questions_json", "questions_data"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                try:
                    decoded = json.loads(candidate)
                    if isinstance(decoded, list):
                        return [dict(x) for x in decoded if isinstance(x, dict)]
                except Exception:
                    continue
        return []

    def _question_options(self, q: dict[str, Any]) -> list[str]:
        raw = q.get("options")
        if isinstance(raw, list):
            out = [self._str(x) for x in raw][:4]
            while len(out) < 4:
                out.append("")
            return out
        keys = ("option_a", "option_b", "option_c", "option_d")
        out = [self._str(q.get(k)) for k in keys]
        while len(out) < 4:
            out.append("")
        return out[:4]

    def _question_correct(self, q: dict[str, Any]) -> str:
        values = q.get("correctAnswers")
        if isinstance(values, list):
            return ", ".join([self._str(v) for v in values if self._str(v)])
        values = q.get("correct")
        if isinstance(values, list):
            return ", ".join([self._str(v) for v in values if self._str(v)])
        return self._str(values or q.get("answer"))

    def _to_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        text = self._str(value).lower()
        return text in {"1", "true", "yes", "y", "on"}

    def _to_int(self, value: Any, fallback: int) -> int:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        try:
            return int(float(self._str(value)))
        except Exception:
            return fallback

    def _normalize_float_map(self, raw: Any) -> dict[str, float]:
        if not isinstance(raw, dict):
            return {}
        out: dict[str, float] = {}
        for key, value in raw.items():
            token = self._str(key)
            if not token:
                continue
            out[token] = round(self._to_float(value, 0.0), 6)
        return out

    def _normalize_user_answers(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            return {}
        out: dict[str, Any] = {}
        for key, value in raw.items():
            token = self._str(key)
            if not token:
                continue
            if isinstance(value, list):
                out[token] = [self._str(item) for item in value if self._str(item)]
            elif isinstance(value, dict):
                out[token] = {
                    self._str(inner_key): inner_value
                    for inner_key, inner_value in value.items()
                    if self._str(inner_key)
                }
            else:
                out[token] = value
        return out

    def _normalized_result_row(
        self,
        payload: dict[str, Any],
        *,
        result_id: str = "",
    ) -> dict[str, Any]:
        submitted_at = self._str(
            payload.get("submitted_at")
            or payload.get("savedAt")
            or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        ts = self._to_int(payload.get("ts") or payload.get("savedAt"), self._now_ms())
        quiz_id = self._str(
            payload.get("quiz_id") or payload.get("quizId") or payload.get("assessment_id")
        )
        quiz_title = self._str(
            payload.get("quiz_title")
            or payload.get("quizTitle")
            or payload.get("topic")
            or payload.get("title")
        )
        student_name = self._str(
            payload.get("student_name")
            or payload.get("studentName")
            or payload.get("name")
            or payload.get("student")
        )
        student_id = self._str(
            payload.get("student_id")
            or payload.get("studentId")
            or payload.get("account_id")
            or payload.get("accountId")
            or payload.get("user_id")
        )
        max_score = self._to_float(
            payload.get("max_score")
            or payload.get("maxScore")
            or payload.get("total"),
            100.0,
        )
        max_score = max(max_score, 1.0)
        total_time = self._to_int(
            payload.get("total_time")
            or payload.get("totalTime")
            or payload.get("time")
            or payload.get("total_time_seconds"),
            0,
        )
        section_accuracy = self._normalize_float_map(
            payload.get("section_accuracy") or payload.get("sectionAccuracy")
        )
        user_answers = self._normalize_user_answers(
            payload.get("user_answers")
            or payload.get("userAnswers")
            or payload.get("answers")
        )
        row = {
            "id": result_id or self._safe_id(payload.get("id")) or self._new_id("res"),
            "quiz_id": quiz_id,
            "quizId": quiz_id,
            "topic": quiz_title,
            "quiz_title": quiz_title,
            "quizTitle": quiz_title,
            "title": quiz_title,
            "name": student_name,
            "student_name": student_name,
            "studentName": student_name,
            "student": student_name,
            "student_id": student_id,
            "studentId": student_id,
            "account_id": self._str(payload.get("account_id") or student_id),
            "accountId": self._str(payload.get("accountId") or payload.get("account_id") or student_id),
            "user_id": self._str(payload.get("user_id") or payload.get("account_id") or student_id),
            "score": self._to_float(payload.get("score"), 0.0),
            "total": max_score,
            "max_score": max_score,
            "maxScore": max_score,
            "correct": self._to_int(payload.get("correct"), 0),
            "wrong": self._to_int(payload.get("wrong"), 0),
            "skipped": self._to_int(payload.get("skipped"), 0),
            "total_time": total_time,
            "totalTime": total_time,
            "time": total_time,
            "submitted_at": submitted_at,
            "savedAt": ts,
            "ts": ts,
            "type": self._str(payload.get("type") or payload.get("quiz_type") or "Exam"),
        }
        if section_accuracy:
            row["section_accuracy"] = section_accuracy
            row["sectionAccuracy"] = section_accuracy
        if user_answers:
            row["user_answers"] = user_answers
            row["userAnswers"] = user_answers
        for key in (
            "duration_minutes",
            "quiz_type",
            "assessment_title",
            "subject",
            "source_surface",
        ):
            if key in payload and payload.get(key) not in (None, "", [], {}):
                row[key] = payload.get(key)
        return row

    def _to_float(self, value: Any, fallback: float) -> float:
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(self._str(value))
        except Exception:
            return fallback

    def _to_list_str(self, value: Any) -> list[str]:
        if isinstance(value, list):
            return [self._str(v) for v in value if self._str(v)]
        text = self._str(value)
        if not text:
            return []
        if text.startswith("["):
            try:
                decoded = json.loads(text)
                if isinstance(decoded, list):
                    return [self._str(v) for v in decoded if self._str(v)]
            except Exception:
                pass
        return [x.strip() for x in text.split(",") if x.strip()]

    def _import_meta_from_payload(self, payload: dict[str, Any]) -> dict[str, str]:
        raw_meta = payload.get("meta")
        if isinstance(raw_meta, str) and raw_meta.strip():
            try:
                decoded = json.loads(raw_meta)
                if isinstance(decoded, dict):
                    raw_meta = decoded
            except Exception:
                raw_meta = {}
        meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}
        out: dict[str, str] = {}
        for key in ("teacher_id", "subject", "chapter", "difficulty"):
            value = self._str(meta.get(key) or payload.get(key))
            if value:
                out[key] = value
        return out

    def _extract_import_search_seeds(self, raw_text: str, *, max_seeds: int = 4) -> list[str]:
        text = self._str(raw_text)
        if not text:
            return []
        if self._looks_like_binary_pdf_text(text):
            return []
        parsed = self._parse_import_raw_text(text, meta_defaults={})
        seeds: list[str] = []
        for row in parsed:
            q_text = self._str(row.get("question_text"))
            if q_text and q_text not in seeds:
                seeds.append(q_text)
            if len(seeds) >= max(1, max_seeds):
                break
        if seeds:
            return seeds[: max(1, max_seeds)]
        normalized = self._normalize_web_text(text)
        fragments = re.split(r"(?<=[?.!])\s+|\n+", normalized)
        for frag in fragments:
            token = self._str(frag)
            if len(token) < 24:
                continue
            if not (
                "?" in token
                or any(
                    key in token.lower()
                    for key in ("find", "evaluate", "determine", "value of", "coefficient")
                )
            ):
                continue
            if token not in seeds:
                seeds.append(token[:260])
            if len(seeds) >= max(1, max_seeds):
                break
        return seeds[: max(1, max_seeds)]

    def _equation_aware_normalize_text(self, text: str) -> str:
        raw = self._str(text)
        if not raw:
            return ""
        normalized = html.unescape(raw)
        replacements = {
            "∫": " integral ",
            "\\int": " integral ",
            "√": " sqrt ",
            "\\sqrt": " sqrt ",
            "≤": " <= ",
            "≥": " >= ",
            "−": "-",
            "×": "*",
            "·": "*",
            "÷": "/",
            "π": " pi ",
            "θ": " theta ",
            "∞": " infinity ",
            "∑": " sum ",
            "\\sum": " sum ",
            "∏": " prod ",
            "\\prod": " prod ",
            "∂": " d ",
            "\\frac": " frac ",
        }
        for src, dst in replacements.items():
            normalized = normalized.replace(src, dst)
        superscript_map = str.maketrans(
            {
                "⁰": "^0",
                "¹": "^1",
                "²": "^2",
                "³": "^3",
                "⁴": "^4",
                "⁵": "^5",
                "⁶": "^6",
                "⁷": "^7",
                "⁸": "^8",
                "⁹": "^9",
            }
        )
        subscript_map = str.maketrans(
            {
                "₀": "0",
                "₁": "1",
                "₂": "2",
                "₃": "3",
                "₄": "4",
                "₅": "5",
                "₆": "6",
                "₇": "7",
                "₈": "8",
                "₉": "9",
            }
        )
        normalized = normalized.translate(superscript_map)
        normalized = normalized.translate(subscript_map)
        normalized = re.sub(r"(?i)\boption\s*[A-D]\s*[\).:\-]?\s*", " ", normalized)
        normalized = re.sub(r"(?i)\bans(?:wer)?\s*[:=\-]\s*", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = re.sub(r"\s*([=+\-*/^(){}\[\],:;])\s*", r"\1", normalized)
        return normalized.strip().lower()

    def _semantic_embedding_vector(self, text: str) -> dict[str, float]:
        normalized = self._equation_aware_normalize_text(text)
        if not normalized:
            return {}
        vector: dict[str, float] = {}
        tokens = [tok for tok in re.split(r"[^a-z0-9^]+", normalized) if tok]
        for tok in tokens:
            vector[f"w:{tok}"] = vector.get(f"w:{tok}", 0.0) + 1.0
        compact = re.sub(r"[^a-z0-9^]", "", normalized)
        if len(compact) >= 3:
            for idx in range(0, len(compact) - 2):
                gram = compact[idx : idx + 3]
                vector[f"c3:{gram}"] = vector.get(f"c3:{gram}", 0.0) + 0.18
        return vector

    def _cosine_sparse_similarity(
        self, a: dict[str, float], b: dict[str, float]
    ) -> float:
        if not a or not b:
            return 0.0
        if len(a) > len(b):
            a, b = b, a
        dot = 0.0
        norm_a = 0.0
        norm_b = 0.0
        for val in a.values():
            norm_a += val * val
        for val in b.values():
            norm_b += val * val
        if norm_a <= 0.0 or norm_b <= 0.0:
            return 0.0
        for key, val in a.items():
            dot += val * b.get(key, 0.0)
        return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))

    def _import_similarity_score(self, left: str, right: str) -> float:
        a_norm = self._equation_aware_normalize_text(left)
        b_norm = self._equation_aware_normalize_text(right)
        if not a_norm or not b_norm:
            return 0.0
        emb_a = self._semantic_embedding_vector(a_norm)
        emb_b = self._semantic_embedding_vector(b_norm)
        semantic = self._cosine_sparse_similarity(emb_a, emb_b)
        stop = {
            "the",
            "and",
            "for",
            "with",
            "from",
            "that",
            "this",
            "find",
            "value",
            "question",
            "option",
            "answer",
        }
        a_tokens = {
            tok
            for tok in re.split(r"[^a-z0-9^]+", a_norm)
            if len(tok) >= 3 and tok not in stop
        }
        b_tokens = {
            tok
            for tok in re.split(r"[^a-z0-9^]+", b_norm)
            if len(tok) >= 3 and tok not in stop
        }
        if not a_tokens or not b_tokens:
            return max(0.0, min(1.0, semantic))
        inter = len(a_tokens & b_tokens)
        union = len(a_tokens | b_tokens)
        lexical = inter / max(1, union)
        return max(0.0, min(1.0, (semantic * 0.8) + (lexical * 0.2)))

    def _infer_import_web_difficulty(self, raw: str) -> int:
        low = self._str(raw).lower()
        if any(token in low for token in ("ultra", "very hard", "advanced", "jee advanced")):
            return 5
        if "hard" in low:
            return 4
        if any(token in low for token in ("easy", "basic")):
            return 2
        return 3

    def _import_source_reliability_weight(self, row: dict[str, Any]) -> float:
        origin = self._str(row.get("source_origin")).lower()
        if origin in {"ocr_raw_parse", "ocr_parsed"}:
            return 0.72
        url = self._str(row.get("source_url") or row.get("url"))
        if url and self._link_allowed_for_pyq(url):
            host = urlparse(url).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
            strong_hosts = (
                "jeeadv.ac.in",
                "nta.ac.in",
                "allen.ac.in",
                "resonance.ac.in",
                "fiitjee.com",
                "mathongo.com",
                "arihantplus.com",
            )
            if any(host == item or host.endswith(f".{item}") for item in strong_hosts):
                return 0.96
            return 0.86
        if origin.startswith("web_"):
            return 0.78
        return 0.65

    def _import_text_length_ratio_score(self, a_text: str, b_text: str) -> float:
        a_len = len(self._equation_aware_normalize_text(a_text))
        b_len = len(self._equation_aware_normalize_text(b_text))
        if a_len <= 0 or b_len <= 0:
            return 0.0
        return min(a_len, b_len) / max(a_len, b_len)

    def _import_answer_signature(self, row: dict[str, Any]) -> str:
        ans = row.get("correct_answer")
        if not isinstance(ans, dict):
            return ""
        single = self._str(ans.get("single")).upper()
        multiple = sorted(set(self._to_list_str(ans.get("multiple"))))
        numerical = self._str(ans.get("numerical"))
        return f"{single}|{','.join(multiple)}|{numerical}"

    def _import_answer_present(self, row: dict[str, Any]) -> bool:
        return bool(self._import_answer_signature(row).replace("|", "").strip())

    def _is_full_web_structured_candidate(self, row: dict[str, Any]) -> bool:
        q_text = self._str(row.get("question_text") or row.get("question_stub"))
        options_raw = row.get("options") if isinstance(row.get("options"), list) else []
        options = [self._str(x) for x in options_raw if self._str(x)]
        answer_token = self._extract_answer_token(
            self._str(row.get("correct_answer") or row.get("answer_stub"))
        )
        if len(q_text) < 24:
            return False
        if len(options) != 4:
            return False
        if len({opt.lower() for opt in options}) != 4:
            return False
        if answer_token not in {"A", "B", "C", "D"}:
            return False
        return True

    def _convert_web_row_to_import_question(
        self,
        *,
        row: dict[str, Any],
        index: int,
        meta_defaults: dict[str, str],
    ) -> dict[str, Any] | None:
        if not self._is_full_web_structured_candidate(row):
            return None
        subject = self._str(meta_defaults.get("subject") or "Mathematics")
        chapter = self._str(meta_defaults.get("chapter") or "PYQ")
        built = self._question_from_web_source(
            row=row,
            idx=index,
            subject=subject,
            chapters=[chapter] if chapter else [subject],
            subtopics=[],
            minimum_reasoning_steps=2,
        )
        if built is None:
            return None
        verify_ok, _ = self._deterministic_verify_candidate(
            question=built,
            subject=subject,
        )
        if not verify_ok:
            return None
        q_type = self._str(built.get("question_type"))
        if q_type != "MCQ_SINGLE":
            # For import web authority, accept only full MCQ with 4 options + one answer.
            return None
        options_raw = built.get("options") if isinstance(built.get("options"), list) else []
        options = [
            {"label": chr(ord("A") + idx), "text": self._str(opt)}
            for idx, opt in enumerate(options_raw[:4])
            if self._str(opt)
        ]
        correct_single = self._str(built.get("_correct_option")).upper()
        if len(options) != 4 or correct_single not in {"A", "B", "C", "D"}:
            return None
        out = {
            "question_id": self._str(built.get("question_id")) or f"imp_q_web_{index + 1}",
            "type": q_type,
            "question_text": self._str(built.get("question_text")),
            "options": options,
            "correct_answer": {
                "single": correct_single or None,
                "multiple": [correct_single] if correct_single else [],
                "numerical": None,
                "tolerance": None,
            },
            "subject": self._str(meta_defaults.get("subject")),
            "chapter": self._str(meta_defaults.get("chapter")),
            "difficulty": self._str(meta_defaults.get("difficulty")),
            "ai_confidence": max(
                0.6,
                min(
                    0.99,
                    self._to_float(row.get("quality_score"), 0.0) + 0.35,
                ),
            ),
            "validation_status": "review",
            "validation_errors": [],
            "source_origin": "web_verified",
            "source_url": self._str(row.get("url")),
            "source_stub": self._str(
                row.get("question_stub") or row.get("title") or row.get("snippet")
            ),
            "exam_type": self._str(row.get("exam_type")),
            "year": self._str(row.get("year")),
        }
        normalized, hard_errors = self._validate_and_normalize_import_question(
            row=out,
            index=index + 1,
            meta_defaults=meta_defaults,
        )
        if hard_errors:
            return None
        normalized["source_origin"] = "web_verified"
        normalized["source_url"] = self._str(row.get("url"))
        normalized["source_stub"] = self._str(
            row.get("question_stub") or row.get("title") or row.get("snippet")
        )
        normalized["exam_type"] = self._str(row.get("exam_type"))
        normalized["year"] = self._str(row.get("year"))
        normalized["verification_pass"] = True
        normalized["conflict_detected"] = False
        normalized["web_match_similarity"] = round(
            self._to_float(row.get("web_match_similarity"), 0.0),
            6,
        )
        return normalized

    def _import_question_quality_score(self, row: dict[str, Any]) -> float:
        existing_conf = self._to_float(row.get("confidence_score"), -1.0)
        if existing_conf >= 0.0:
            return round(existing_conf + (0.05 if self._str(row.get("source_origin")).startswith("web_") else 0.0), 6)
        status = self._str(row.get("validation_status")).lower()
        status_weight = 1.0 if status == "valid" else (0.75 if status == "review" else 0.2)
        has_answer = False
        ans = row.get("correct_answer")
        if isinstance(ans, dict):
            has_answer = bool(
                self._str(ans.get("single"))
                or self._to_list_str(ans.get("multiple"))
                or self._str(ans.get("numerical"))
            )
        source_origin = self._str(row.get("source_origin")).lower()
        source_bonus = 0.18 if source_origin.startswith("web_") else 0.0
        conf = self._to_float(row.get("ai_confidence"), 0.0)
        sim = self._to_float(row.get("web_match_similarity"), 0.0)
        options = row.get("options") if isinstance(row.get("options"), list) else []
        options_bonus = 0.08 if len(options) >= 4 else 0.0
        answer_bonus = 0.18 if has_answer else 0.0
        return round(
            status_weight + source_bonus + options_bonus + answer_bonus + (conf * 0.45) + (sim * 0.55),
            6,
        )

    def _import_option_structure_match(self, row: dict[str, Any]) -> bool:
        q_type = self._str(row.get("type") or row.get("question_type")).upper()
        options = row.get("options") if isinstance(row.get("options"), list) else []
        if q_type == "NUMERICAL":
            ans = row.get("correct_answer")
            if not isinstance(ans, dict):
                return False
            return bool(self._IMPORT_NUMERIC_TOKEN_RE.search(self._str(ans.get("numerical"))))
        if q_type in {"MCQ_SINGLE", "MCQ_MULTI"}:
            if len(options) < 4:
                return False
            texts = [self._str(opt.get("text") if isinstance(opt, dict) else opt).strip() for opt in options]
            texts = [t for t in texts if t]
            if len(texts) < 4:
                return False
            if len({t.lower() for t in texts}) != len(texts):
                return False
            return True
        return False

    def _import_deterministic_verify_row(self, row: dict[str, Any], *, subject: str) -> bool:
        q_type = self._str(row.get("type") or row.get("question_type")).upper()
        q_text = self._str(row.get("question_text"))
        if not q_text:
            return False
        if q_type not in {"MCQ_SINGLE", "MCQ_MULTI", "NUMERICAL"}:
            return False
        options_raw = row.get("options") if isinstance(row.get("options"), list) else []
        options: list[str] = []
        for opt in options_raw:
            if isinstance(opt, dict):
                options.append(self._str(opt.get("text")))
            else:
                options.append(self._str(opt))
        ans = row.get("correct_answer") if isinstance(row.get("correct_answer"), dict) else {}
        payload: dict[str, Any] = {
            "question_id": self._str(row.get("question_id")) or "imp_q_1",
            "question_type": q_type,
            "question_text": q_text,
            "options": options if q_type != "NUMERICAL" else [],
            "_correct_option": self._str(ans.get("single")).upper()
            if q_type in {"MCQ_SINGLE", "MCQ_MULTI"}
            else "",
            "_correct_answers": (
                [self._str(x).upper() for x in self._to_list_str(ans.get("multiple")) if self._str(x)]
                if q_type == "MCQ_MULTI"
                else (
                    [self._str(ans.get("single")).upper()]
                    if q_type == "MCQ_SINGLE"
                    else []
                )
            ),
            "_numerical_answer": self._str(ans.get("numerical")) if q_type == "NUMERICAL" else "",
            "_solution_explanation": self._str(row.get("solution_explanation") or ""),
        }
        ok, _ = self._deterministic_verify_candidate(question=payload, subject=subject)
        return bool(ok)

    def _import_multisignal_confidence(
        self,
        *,
        semantic_similarity: float,
        verification_pass: bool,
        answer_match: bool | None,
        option_structure_match: bool,
        source_reliability: float,
        text_length_ratio_score: float,
    ) -> float:
        semantic_similarity_weight = max(0.0, min(1.0, semantic_similarity)) * 0.34
        deterministic_verification_pass = 0.20 if verification_pass else 0.0
        if answer_match is True:
            answer_match_bonus = 0.14
        elif answer_match is None:
            answer_match_bonus = 0.07
        else:
            answer_match_bonus = 0.0
        option_structure = 0.12 if option_structure_match else 0.0
        source_reliability_weight = max(0.0, min(1.0, source_reliability)) * 0.12
        text_length_ratio_weight = max(0.0, min(1.0, text_length_ratio_score)) * 0.08
        confidence = (
            semantic_similarity_weight
            + deterministic_verification_pass
            + answer_match_bonus
            + option_structure
            + source_reliability_weight
            + text_length_ratio_weight
        )
        return round(max(0.0, min(1.0, confidence)), 6)

    def _import_apply_confidence_tier(
        self, row: dict[str, Any], *, confidence_score: float
    ) -> dict[str, Any]:
        updated = dict(row)
        updated["confidence_score"] = round(confidence_score, 6)
        errors = [self._str(x) for x in (updated.get("validation_errors") or []) if self._str(x)]
        if confidence_score < 0.75:
            updated["validation_status"] = "invalid"
            note = "Rejected by confidence gate (<0.75)."
            if note not in errors:
                errors.append(note)
        elif confidence_score < 0.9:
            updated["validation_status"] = "review"
        else:
            if self._str(updated.get("validation_status")).lower() != "invalid":
                updated["validation_status"] = "valid"
        updated["validation_errors"] = errors
        updated["publish_risk_score"] = self._import_publish_risk_score(updated)
        return updated

    async def _import_ai_critic_vote(
        self,
        *,
        question_text: str,
        options: list[str],
        web_answer: str,
        ocr_answer: str,
    ) -> str:
        prompt_lines = [
            "You are a strict exam-quality arbitrator.",
            "Choose whose answer is more likely correct for this question: WEB or OCR.",
            "Return exactly one token: WEB, OCR, or UNSURE.",
            f"Question: {question_text}",
        ]
        if options:
            prompt_lines.append("Options:")
            for idx, opt in enumerate(options[:4]):
                prompt_lines.append(f"{chr(ord('A') + idx)}) {opt}")
        prompt_lines.append(f"WEB answer: {web_answer}")
        prompt_lines.append(f"OCR answer: {ocr_answer}")
        response = await self._ai_chat_or_solve(
            {
                "action": "ai_chat",
                "prompt": "\n".join(prompt_lines),
                "function": "ai_solve",
                "response_style": "strict_classifier",
                "enable_persona": False,
            }
        )
        if not self._to_bool(response.get("ok")):
            return "UNSURE"
        answer = self._str(response.get("answer")).upper()
        if "WEB" in answer:
            return "WEB"
        if re.search(r"\bOCR\b", answer):
            return "OCR"
        return "UNSURE"

    async def _fuse_import_question_groups(
        self,
        *,
        web_questions: list[dict[str, Any]],
        parsed_questions: list[dict[str, Any]],
        payload: dict[str, Any],
        meta_defaults: dict[str, str],
    ) -> list[dict[str, Any]]:
        all_rows = [*web_questions, *parsed_questions]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in all_rows:
            q_text = self._equation_aware_normalize_text(self._str(row.get("question_text")))
            if not q_text:
                continue
            key = re.sub(r"[^a-z0-9^]+", "", q_text)[:360]
            if not key:
                continue
            grouped.setdefault(key, []).append(dict(row))

        fused: list[dict[str, Any]] = []
        subject = self._str(meta_defaults.get("subject") or payload.get("subject") or "Mathematics")
        use_critic = self._to_bool(payload.get("enable_conflict_critic") if payload.get("enable_conflict_critic") is not None else True)
        for rows in grouped.values():
            rows.sort(key=lambda r: self._import_question_quality_score(r), reverse=True)
            web = [r for r in rows if self._str(r.get("source_origin")).startswith("web_")]
            ocr = [r for r in rows if self._str(r.get("source_origin")) in {"ocr_raw_parse", "ocr_parsed"}]
            candidate_web = web[0] if web else None
            candidate_ocr = ocr[0] if ocr else None
            chosen: dict[str, Any] | None = None
            conflict_detected = False

            if candidate_web is not None and candidate_ocr is not None:
                conflict_detected = (
                    self._import_answer_present(candidate_web)
                    and self._import_answer_present(candidate_ocr)
                    and self._import_answer_signature(candidate_web) != self._import_answer_signature(candidate_ocr)
                )
                web_sem = self._to_float(candidate_web.get("semantic_similarity"), self._to_float(candidate_web.get("web_match_similarity"), 0.0))
                ocr_sem = max(0.0, min(1.0, self._import_similarity_score(
                    self._str(candidate_web.get("question_text")),
                    self._str(candidate_ocr.get("question_text")),
                )))
                len_ratio = self._import_text_length_ratio_score(
                    self._str(candidate_web.get("question_text")),
                    self._str(candidate_ocr.get("question_text")),
                )
                web_verify = self._import_deterministic_verify_row(candidate_web, subject=subject)
                ocr_verify = self._import_deterministic_verify_row(candidate_ocr, subject=subject)
                answer_match = (
                    self._import_answer_signature(candidate_web) == self._import_answer_signature(candidate_ocr)
                    if self._import_answer_present(candidate_web) and self._import_answer_present(candidate_ocr)
                    else None
                )
                web_conf = self._import_multisignal_confidence(
                    semantic_similarity=web_sem,
                    verification_pass=web_verify,
                    answer_match=answer_match,
                    option_structure_match=self._import_option_structure_match(candidate_web),
                    source_reliability=self._import_source_reliability_weight(candidate_web),
                    text_length_ratio_score=len_ratio,
                )
                ocr_conf = self._import_multisignal_confidence(
                    semantic_similarity=ocr_sem,
                    verification_pass=ocr_verify,
                    answer_match=answer_match,
                    option_structure_match=self._import_option_structure_match(candidate_ocr),
                    source_reliability=self._import_source_reliability_weight(candidate_ocr),
                    text_length_ratio_score=len_ratio,
                )
                critic_vote = "UNSURE"
                if conflict_detected and use_critic:
                    ocr_ans = self._import_answer_signature(candidate_ocr)
                    web_ans = self._import_answer_signature(candidate_web)
                    options = [
                        self._str(x.get("text") if isinstance(x, dict) else x)
                        for x in (candidate_web.get("options") or [])
                    ]
                    critic_vote = await self._import_ai_critic_vote(
                        question_text=self._str(candidate_web.get("question_text")),
                        options=options,
                        web_answer=web_ans,
                        ocr_answer=ocr_ans,
                    )
                web_score = web_conf + (0.08 if web_verify else 0.0) + (
                    0.06 if critic_vote == "WEB" else (-0.06 if critic_vote == "OCR" else 0.0)
                )
                ocr_score = ocr_conf + (0.06 if ocr_verify else 0.0) + (
                    0.06 if critic_vote == "OCR" else (-0.06 if critic_vote == "WEB" else 0.0)
                )
                if web_score >= ocr_score:
                    chosen = dict(candidate_web)
                    final_conf = min(0.99, max(web_conf, web_score))
                else:
                    chosen = dict(candidate_ocr)
                    final_conf = min(0.99, max(ocr_conf, ocr_score))
                chosen["source_origin"] = "fusion_verified"
                chosen["semantic_similarity"] = round(max(web_sem, ocr_sem), 6)
                chosen["verification_pass"] = bool(
                    web_verify if self._str(chosen.get("source_url")) else ocr_verify
                )
                chosen["conflict_detected"] = conflict_detected
                chosen["conflict_resolution"] = {
                    "critic_vote": critic_vote,
                    "web_score": round(web_score, 6),
                    "ocr_score": round(ocr_score, 6),
                    "web_verify": web_verify,
                    "ocr_verify": ocr_verify,
                }
                chosen = self._import_apply_confidence_tier(
                    chosen,
                    confidence_score=final_conf,
                )
                if conflict_detected and self._str(chosen.get("validation_status")) == "valid":
                    # Conflict still requires one level of human trust unless confidence is very high.
                    if final_conf < 0.94:
                        chosen["validation_status"] = "review"
            else:
                selected = candidate_web if candidate_web is not None else candidate_ocr
                if selected is None:
                    selected = rows[0]
                chosen = dict(selected)
                origin = self._str(chosen.get("source_origin")).lower()
                sem = self._to_float(
                    chosen.get("semantic_similarity"),
                    self._to_float(
                        chosen.get("web_match_similarity"),
                        1.0 if origin in {"ocr_raw_parse", "ocr_parsed"} else 0.0,
                    ),
                )
                verify = self._import_deterministic_verify_row(chosen, subject=subject)
                confidence = self._import_multisignal_confidence(
                    semantic_similarity=sem,
                    verification_pass=verify,
                    answer_match=None,
                    option_structure_match=self._import_option_structure_match(chosen),
                    source_reliability=self._import_source_reliability_weight(chosen),
                    text_length_ratio_score=1.0,
                )
                chosen["semantic_similarity"] = round(max(0.0, min(1.0, sem)), 6)
                chosen["verification_pass"] = verify
                chosen["conflict_detected"] = False
                chosen = self._import_apply_confidence_tier(chosen, confidence_score=confidence)
                origin = self._str(chosen.get("source_origin")).lower()
                if origin in {"ocr_raw_parse", "ocr_parsed"}:
                    chosen["source_origin"] = "ocr_parsed"
            if chosen is None:
                continue
            if self._to_float(chosen.get("confidence_score"), 0.0) < 0.75:
                continue
            fused.append(chosen)
        fused = self._dedupe_import_questions(fused)
        return fused

    def _dedupe_import_questions(
        self, questions: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        best_by_key: dict[str, dict[str, Any]] = {}
        best_score: dict[str, float] = {}
        for row in questions:
            q_text = self._str(row.get("question_text")).lower()
            if not q_text:
                continue
            key = re.sub(r"[^a-z0-9]+", "", q_text)[:280]
            if not key:
                continue
            score = self._import_question_quality_score(row)
            if key in best_by_key:
                current_sig = self._import_answer_signature(best_by_key[key])
                candidate_sig = self._import_answer_signature(row)
                if current_sig and candidate_sig and current_sig != candidate_sig:
                    notes = [
                        self._str(x) for x in (row.get("validation_errors") or []) if self._str(x)
                    ]
                    conflict_note = (
                        "Answer conflict detected across web/ocr sources; kept higher-confidence source."
                    )
                    if conflict_note not in notes:
                        notes.append(conflict_note)
                    row["validation_errors"] = notes
                    row["validation_status"] = "review"
            if key not in best_by_key or score > best_score.get(key, -1.0):
                best_by_key[key] = row
                best_score[key] = score
        out = list(best_by_key.values())
        out.sort(
            key=lambda row: self._import_question_quality_score(row),
            reverse=True,
        )
        return out

    def _import_pre_ocr_web_lookup(
        self,
        *,
        payload: dict[str, Any],
        raw_text: str,
        meta_defaults: dict[str, str],
    ) -> dict[str, Any]:
        enabled = self._to_bool(
            payload.get("web_ocr_fusion_mode")
            or payload.get("simultaneous_web_ocr_mode")
            or payload.get("web_ocr_parallel_mode")
            or payload.get("pre_ocr_web_search")
            or payload.get("web_lookup_before_ocr")
            or payload.get("question_paper_mode")
            or payload.get("import_question_paper")
        )
        if not enabled:
            return {
                "enabled": False,
                "questions": [],
                "diagnostics": {},
                "web_error_reason": "",
            }
        subject = self._str(meta_defaults.get("subject") or payload.get("subject")) or "Mathematics"
        chapter_list = (
            self._to_list_str(payload.get("chapters"))
            or self._to_list_str(meta_defaults.get("chapter"))
            or [self._str(payload.get("chapter") or meta_defaults.get("chapter")) or subject]
        )
        chapter_list = [self._str(x) for x in chapter_list if self._str(x)] or [subject]
        subtopics = self._to_list_str(payload.get("subtopics"))
        seeds = self._extract_import_search_seeds(raw_text, max_seeds=4)
        query_hint = "JEE PYQ question paper answer key detailed solution"
        if seeds:
            query_hint = f"{self._str(seeds[0])[:120]} JEE PYQ answer"
        difficulty = self._infer_import_web_difficulty(
            self._str(meta_defaults.get("difficulty") or payload.get("difficulty"))
        )
        target_count = max(
            1,
            self._to_int(
                payload.get("target_question_count")
                or payload.get("question_count")
                or payload.get("expected_questions"),
                max(3, len(seeds) or 3),
            ),
        )
        web_rows = self._fetch_pyq_web_snippets(
            subject=subject,
            chapters=chapter_list,
            subtopics=subtopics,
            query_suffix=query_hint,
            limit=max(8, target_count * 3),
            difficulty=difficulty,
        )
        diagnostics = dict(self._last_pyq_web_diagnostics)
        ranked_rows: list[dict[str, Any]] = []
        assistive_context_count = 0
        semantic_threshold = 0.85
        for row in web_rows:
            probe = " ".join(
                [
                    self._str(row.get("question_text")),
                    self._str(row.get("question_stub")),
                    self._str(row.get("title")),
                    self._str(row.get("snippet")),
                ]
            )
            best_sim = 0.0
            best_seed = ""
            for seed in seeds:
                sim = self._import_similarity_score(seed, probe)
                if sim > best_sim:
                    best_sim = sim
                    best_seed = seed
            if seeds and best_sim < semantic_threshold:
                assistive_context_count += 1
                continue
            scored = dict(row)
            scored["web_match_similarity"] = round(best_sim, 6)
            scored["semantic_similarity"] = round(best_sim, 6)
            scored["text_length_ratio_score"] = round(
                self._import_text_length_ratio_score(best_seed, probe) if best_seed else 0.0,
                6,
            )
            ranked_rows.append(scored)
        ranked_rows.sort(
            key=lambda row: (
                self._to_float(row.get("web_match_similarity"), 0.0),
                self._to_float(row.get("quality_score"), 0.0),
                self._to_float(row.get("difficulty_score"), 0.0),
                1.0 if self._to_bool(row.get("has_answer")) else 0.0,
                1.0 if self._to_bool(row.get("has_solution")) else 0.0,
            ),
            reverse=True,
        )
        import_questions: list[dict[str, Any]] = []
        for idx, row in enumerate(ranked_rows):
            converted = self._convert_web_row_to_import_question(
                row=row,
                index=idx,
                meta_defaults=meta_defaults,
            )
            if converted is None:
                assistive_context_count += 1
                continue
            converted["semantic_similarity"] = round(
                self._to_float(row.get("semantic_similarity"), 0.0),
                6,
            )
            import_questions.append(converted)
            if len(import_questions) >= target_count:
                break
        web_error_reason = self._str(diagnostics.get("web_error_reason"))
        if import_questions and web_error_reason:
            web_error_reason = ""
        return {
            "enabled": True,
            "questions": import_questions,
            "diagnostics": diagnostics,
            "web_error_reason": web_error_reason,
            "seed_count": len(seeds),
            "target_count": target_count,
            "candidate_count": len(web_rows),
            "matched_count": len(import_questions),
            "assistive_context_count": assistive_context_count,
            "semantic_threshold": semantic_threshold,
        }

    def _extract_import_questions_payload(
        self, payload: dict[str, Any]
    ) -> list[dict[str, Any]]:
        candidates: list[Any] = [
            payload.get("questions"),
            payload.get("question_list"),
            payload.get("items"),
            payload.get("questions_json"),
            payload.get("questions_data"),
        ]
        for candidate in candidates:
            current = candidate
            if isinstance(current, str) and current.strip():
                try:
                    current = json.loads(current)
                except Exception:
                    current = None
            if isinstance(current, list):
                return [dict(x) for x in current if isinstance(x, dict)]
        return []

    def _normalize_import_option_label(self, raw: Any, index: int) -> str:
        token = self._str(raw).upper()
        if re.fullmatch(r"[A-Z]", token):
            return token
        if re.fullmatch(r"[1-9]", token):
            return chr(64 + int(token))
        if token and token[0].isalpha():
            return token[0]
        if token and token[0].isdigit():
            number = int(token[0])
            if 1 <= number <= 9:
                return chr(64 + number)
        safe_index = max(0, min(index, 25))
        return chr(65 + safe_index)

    def _extract_import_label_token(self, raw: Any) -> str:
        token = self._str(raw).upper()
        if not token:
            return ""
        if token == "@":
            return "B"
        if token in {"O", "0"}:
            return "D"
        if re.fullmatch(r"[A-Z]", token):
            return token
        if re.fullmatch(r"[1-9]", token):
            return chr(64 + int(token))
        if len(token) >= 2 and token[0].isalpha() and token[1] in {")", ".", ":", "-"}:
            return token[0]
        return ""

    def _extract_import_question_number(self, raw: Any) -> int:
        match = self._IMPORT_QUESTION_NUMBER_RE.match(self._str(raw))
        if match is None:
            return 0
        return self._to_int(match.group(1), 0)

    def _normalize_import_question_type(self, raw: Any, has_options: bool) -> str:
        token = self._str(raw).upper()
        if token in {"MCQ_MULTI", "MULTI", "MULTIPLE"}:
            return "MCQ_MULTI"
        if token in {"NUMERICAL", "NUMERIC", "INTEGER"}:
            return "NUMERICAL"
        if token in {"MCQ_SINGLE", "MCQ", "SINGLE"}:
            return "MCQ_SINGLE"
        return "MCQ_SINGLE" if has_options else "NUMERICAL"

    def _normalize_import_options(self, raw: Any) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        if isinstance(raw, dict):
            entries = sorted(raw.items(), key=lambda kv: self._str(kv[0]).upper())
            raw = [
                {"label": self._str(k), "text": self._str(v)}
                for k, v in entries
                if self._str(v)
            ]
        if isinstance(raw, list):
            for idx, item in enumerate(raw):
                if isinstance(item, dict):
                    label = self._normalize_import_option_label(
                        item.get("label") or item.get("key"),
                        idx,
                    )
                    text = self._str(
                        item.get("text") or item.get("option") or item.get("value")
                    )
                else:
                    label = self._normalize_import_option_label("", idx)
                    text = self._str(item)
                if not text:
                    continue
                rows.append({"label": label, "text": text})
        seen: set[str] = set()
        out: list[dict[str, str]] = []
        for idx, row in enumerate(rows):
            label = self._normalize_import_option_label(row.get("label"), idx)
            while label in seen:
                label = self._normalize_import_option_label("", idx + len(seen))
            seen.add(label)
            out.append({"label": label, "text": self._str(row.get("text"))})
        return out

    def _parse_import_answer_hint(
        self,
        *,
        answer_hint: str,
        options: list[dict[str, str]],
    ) -> tuple[list[str], str]:
        compact = re.sub(r"\s+", " ", self._str(answer_hint)).strip()
        if not compact:
            return [], ""
        parts = [
            x.strip()
            for x in re.split(r"[,/;|]+", compact)
            if self._str(x).strip()
        ]
        labels: list[str] = []
        seen: set[str] = set()
        by_text = {
            self._str(row.get("text")).lower(): self._str(row.get("label")).upper()
            for row in options
            if self._str(row.get("text"))
        }
        for piece in parts:
            label = self._extract_import_label_token(piece)
            if not label and re.fullmatch(r"(?i)[A-D]{2,4}", piece):
                for ch in piece.upper():
                    if ch not in seen:
                        seen.add(ch)
                        labels.append(ch)
                continue
            if not label and re.fullmatch(r"[1-4]{2,4}", piece):
                for ch in piece:
                    mapped = chr(64 + int(ch))
                    if mapped not in seen:
                        seen.add(mapped)
                        labels.append(mapped)
                continue
            if not label:
                label = by_text.get(piece.lower(), "")
            if label and label not in seen:
                seen.add(label)
                labels.append(label)
        number_match = self._IMPORT_NUMERIC_TOKEN_RE.search(compact)
        numerical = number_match.group(0) if number_match else ""
        return labels, numerical

    def _looks_like_import_instruction(self, line: str) -> bool:
        lower = line.lower()
        return (
            lower.startswith("section")
            or "choose the correct option" in lower
            or "select all correct" in lower
            or "more than one correct" in lower
            or "numerical answer type" in lower
            or "integer type" in lower
            or "one or more options may be correct" in lower
        )

    def _looks_numeric_import_prompt(self, text: str) -> bool:
        lower = self._str(text).lower()
        return (
            "integer" in lower
            or "numerical" in lower
            or "enter value" in lower
            or "decimal places" in lower
            or "answer in" in lower
        )

    def _looks_like_binary_pdf_text(self, text: str) -> bool:
        raw = self._str(text)
        if not raw:
            return True
        sample = raw[:12000]
        if "%pdf-" in sample.lower() and " obj" in sample.lower():
            return True
        total = len(sample)
        if total <= 0:
            return True
        alpha_num = sum(1 for ch in sample if ch.isalnum())
        spaces = sum(1 for ch in sample if ch.isspace())
        readable_ratio = (alpha_num + spaces) / total
        if readable_ratio < 0.56:
            return True
        noisy = len(re.findall(r"[{}<>~`|\\]{3,}", sample))
        if noisy >= 6 and readable_ratio < 0.72:
            return True
        return False

    def _looks_like_image_blob(
        self, blob: bytes, *, mime: str = "", path: Path | None = None
    ) -> bool:
        low_mime = self._str(mime).lower()
        if low_mime.startswith("image/"):
            return True
        suffix = self._str(path.suffix if isinstance(path, Path) else "").lower()
        if suffix in {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif", ".tif", ".tiff", ".heic", ".heif"}:
            return True
        sig = bytes((blob or b"")[:16])
        if sig.startswith(b"\x89PNG\r\n\x1a\n"):
            return True
        if sig.startswith(b"\xff\xd8\xff"):
            return True
        if sig.startswith((b"GIF87a", b"GIF89a")):
            return True
        if sig.startswith(b"BM"):
            return True
        if sig.startswith((b"II*\x00", b"MM\x00*")):
            return True
        if sig.startswith(b"RIFF") and b"WEBP" in bytes((blob or b"")[8:16]):
            return True
        return False

    def _extract_text_from_pdf_bytes(self, blob: bytes) -> str:
        if not blob:
            return ""
        extracted = ""
        try:
            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(blob))
            parts: list[str] = []
            for page in reader.pages[:40]:
                text = self._str(page.extract_text())
                if text:
                    parts.append(text)
            extracted = "\n\n".join(parts).strip()
            if len(extracted) >= 160 and not self._looks_like_binary_pdf_text(extracted):
                return extracted
        except Exception:
            extracted = ""
        strings_bin = shutil.which("strings")
        if strings_bin:
            try:
                with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
                    tmp.write(blob)
                    tmp.flush()
                    res = subprocess.run(
                        [strings_bin, "-n", "6", tmp.name],
                        capture_output=True,
                        timeout=5.0,
                        check=False,
                    )
                if res.returncode == 0:
                    raw = res.stdout.decode("utf-8", errors="ignore")
                    lines = [self._str(x).strip() for x in raw.splitlines() if self._str(x).strip()]
                    compact = "\n".join(lines[:1800]).strip()
                    if len(compact) >= 200 and not self._looks_like_binary_pdf_text(compact):
                        return compact
            except Exception:
                pass
        return extracted if not self._looks_like_binary_pdf_text(extracted) else ""

    def _normalize_import_line(self, raw: str) -> str:
        normalized = self._normalize_symbol_font_artifacts(self._str(raw))
        normalized = re.sub(r"^\s*[Gg](\d+\s*[\).:\-]\s*)", r"Q\1", normalized)
        normalized = re.sub(
            r"^\s*(?:a(?:newer|neewr|nser))\b",
            "Answer",
            normalized,
            flags=re.IGNORECASE,
        )
        return re.sub(r"[ ]{2,}", " ", normalized.replace("\t", " ")).strip()

    def _symbol_font_mapping(self) -> dict[str, str]:
        return {
            "\uf022": "∅",
            "\uf03c": "<",
            "\uf03e": ">",
            "\uf061": "α",
            "\uf062": "β",
            "\uf066": "∅",
            "\uf07b": "{",
            "\uf07d": "}",
            "\uf0a3": "≤",
            "\uf0b3": "≥",
            "\uf0b4": "×",
            "\uf0b9": "≠",
            "\uf0c6": "∀",
            "\uf0c7": "∩",
            "\uf0c8": "∪",
            "\uf0ce": "∈",
            "\uf0cf": "∉",
            "\uf0db": "⇔",
            "\uf0e5": "∑",
            "\uf0ec": "{",
            "\uf0ed": "{",
            "\uf0ee": "{",
            "\uf0ef": "{",
            "\uf0fc": "}",
            "\uf0fd": "}",
            "\uf0fe": "}",
        }

    def _count_symbol_font_artifacts(self, raw: str) -> int:
        text = self._str(raw)
        if not text:
            return 0
        mapping = self._symbol_font_mapping()
        count = 0
        for token in mapping.keys():
            count += text.count(token)
        count += len(re.findall(r"[\uf000-\uf8ff]", text))
        return max(0, int(count))

    def _normalize_symbol_font_artifacts(self, raw: str) -> str:
        text = self._str(raw)
        if not text:
            return ""
        for src, dst in self._symbol_font_mapping().items():
            text = text.replace(src, dst)
        # Drop residual private-use glyphs and common white-box replacement chars.
        text = re.sub(r"[\uf000-\uf8ff]", " ", text)
        text = re.sub(r"[□■▢▣◻◼⬜⬛⧈�]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _balanced_brackets(self, text: str) -> bool:
        s = self._str(text)
        pairs = {")": "(", "]": "[", "}": "{"}
        stack: list[str] = []
        for ch in s:
            if ch in "([{":
                stack.append(ch)
            elif ch in ")]}":
                if not stack or stack[-1] != pairs[ch]:
                    return False
                stack.pop()
        return not stack

    def _looks_truncated_equation(self, text: str) -> bool:
        s = self._str(text).strip()
        if not s:
            return False
        if re.search(r"[=+\-*/^(]$", s):
            return True
        if re.search(r"\b(?:sin|cos|tan|log|ln|sqrt)\s*$", s, flags=re.IGNORECASE):
            return True
        return False

    def _looks_degraded_math_text(self, text: str) -> bool:
        s = self._str(text).strip()
        if not s:
            return True
        low = s.lower()
        if len(re.findall(r"[a-z0-9]", low)) < 10:
            return True
        degraded_patterns = (
            r"\bcoefficients?\s+of\s+and\s+terms?\b",
            r"\bterms?\s+respectively\s+in\s+the\s+binomial\s+expansion\s+of\s*\.\b",
            r"\bif\s*,\s*then\b",
            r"\bexpansion\s+of\s*\.\s*if\b",
            r"\(1\)\s*\(2\)\s*\(3\)\s*\(4\)\s*$",
            r"(?:\(\d\)\s*\d+\.\s*){3,}",
            r"\banswer\s*\([^)]+\)",
            r"\bsol\.\b",
        )
        if any(re.search(pat, low) for pat in degraded_patterns):
            return True
        signal_verbs = (
            "find",
            "evaluate",
            "determine",
            "compute",
            "let ",
            "if ",
            "show",
            "prove",
        )
        if (
            any(
                token in low
                for token in (
                    "jee main previous year paper",
                    "jee advanced previous year paper",
                    "mathongo",
                    "question paper",
                )
            )
            and not any(verb in low for verb in signal_verbs)
        ):
            return True
        if any(
            token in low
            for token in (
                "mathongo",
                "join the most relevant test series",
                "jee main previous year paper",
                "question paper",
                "www.allen.in",
            )
        ) and len(low) >= 90:
            return True
        if (
            len(re.findall(r"\d{2,}", low)) >= 18
            and low.count("(") >= 8
            and low.count(")") >= 8
        ):
            return True
        if self._looks_truncated_equation(s):
            return True
        return False

    def _options_logically_distinct(self, options: list[dict[str, str]]) -> bool:
        normalized = [
            self._equation_aware_normalize_text(self._str(opt.get("text")))
            for opt in options
            if isinstance(opt, dict) and self._str(opt.get("text"))
        ]
        if len(normalized) < 2:
            return True
        for idx in range(len(normalized)):
            for jdx in range(idx + 1, len(normalized)):
                sim = self._import_similarity_score(normalized[idx], normalized[jdx])
                if sim >= 0.93:
                    return False
        return True

    def _canonical_import_latex(self, raw: str) -> str:
        text = self._str(raw).strip()
        if not text:
            return ""
        try:
            return sanitize_latex(text)
        except Exception:
            return text

    def _import_chapter_is_generic(self, chapter: str) -> bool:
        low = re.sub(r"[^a-z0-9]+", " ", self._str(chapter).lower()).strip()
        if not low:
            return True
        generic_tokens = {
            "general",
            "general jee",
            "general jee mathematics",
            "general jee physics",
            "general jee chemistry",
            "general jee biology",
            "general mathematics",
            "general physics",
            "general chemistry",
            "general biology",
            "jee mathematics",
            "jee physics",
            "jee chemistry",
            "jee biology",
            "mathematics",
            "physics",
            "chemistry",
            "biology",
            "mixed",
            "mixed chapter",
            "mixed chapters",
            "all chapters",
            "all jee chapters",
            "auto",
        }
        if low in generic_tokens:
            return True
        if "mixed" in low and ("jee" in low or "chapter" in low or "general" in low):
            return True
        if low.startswith("jee ") and low.endswith(" mixed"):
            return True
        if low.startswith("general jee "):
            return True
        if low.startswith("all jee chapters"):
            return True
        return False

    def _chapter_signal_overrides(self, *, track: str) -> dict[str, tuple[str, ...]]:
        if track == "Mathematics":
            return {
                "sets relations and functions": (
                    "set",
                    "relation",
                    "function",
                    "domain",
                    "range",
                    "onto",
                    "one one",
                    "many one",
                    "inverse function",
                    "composition",
                    "fog",
                    "gof",
                ),
                "complex numbers and quadratic equations": (
                    "complex",
                    "argand",
                    "modulus",
                    "argument",
                    "conjugate",
                    "de moivre",
                    "iota",
                    "imaginary",
                    "quadratic",
                    "discriminant",
                    "nature of roots",
                    "sum of roots",
                    "product of roots",
                    "alpha",
                    "beta",
                ),
                "matrices and determinants": (
                    "matrix",
                    "determinant",
                    "adjoint",
                    "cofactor",
                    "minor",
                    "rank",
                    "inverse matrix",
                    "singular",
                    "non singular",
                    "det(",
                ),
                "permutations and combinations": (
                    "permutation",
                    "combination",
                    "arrangement",
                    "selection",
                    "factorial",
                    "npr",
                    "ncr",
                ),
                "binomial theorem": (
                    "binomial",
                    "coefficient",
                    "expansion",
                    "general term",
                    "middle term",
                    "greatest term",
                    "\\binom",
                    "(1+x)^",
                    "(1 + x)^",
                    "ncr",
                ),
                "sequences and series": (
                    "sequence",
                    "series",
                    "a.p",
                    "g.p",
                    "h.p",
                    "ap",
                    "gp",
                    "hp",
                    "sum to n",
                    "nth term",
                    "s_n",
                    "t_n",
                    "progression",
                ),
                "trigonometric functions": (
                    "trigonometric",
                    "sin",
                    "cos",
                    "tan",
                    "cot",
                    "sec",
                    "cosec",
                    "identity",
                    "periodic",
                ),
                "inverse trigonometric functions": (
                    "inverse trigonometric",
                    "principal value",
                    "arcsin",
                    "arccos",
                    "arctan",
                    "sin^-1",
                    "cos^-1",
                    "tan^-1",
                ),
                "limits continuity and differentiability": (
                    "limit",
                    "\\lim",
                    "continuity",
                    "continuous",
                    "differentiable",
                    "differentiability",
                    "left hand limit",
                    "right hand limit",
                    "l hospital",
                ),
                "application of derivatives": (
                    "d/dx",
                    "derivative",
                    "maxima",
                    "minima",
                    "increasing",
                    "decreasing",
                    "tangent",
                    "normal",
                    "rate of change",
                    "stationary point",
                ),
                "integral calculus": (
                    "integral",
                    "\\int",
                    "dx",
                    "definite integral",
                    "indefinite integral",
                    "by parts",
                    "substitution",
                    "partial fractions",
                    "area under curve",
                    "integration",
                ),
                "differential equations": (
                    "differential equation",
                    "dy/dx",
                    "integrating factor",
                    "general solution",
                    "particular solution",
                    "homogeneous",
                ),
                "coordinate geometry": (
                    "coordinate",
                    "straight line",
                    "circle",
                    "parabola",
                    "ellipse",
                    "hyperbola",
                    "focus",
                    "directrix",
                    "latus rectum",
                    "slope",
                ),
                "three dimensional geometry": (
                    "3d",
                    "three dimensional",
                    "line in 3d",
                    "plane",
                    "direction ratio",
                    "direction cosine",
                    "skew",
                    "coplanar",
                ),
                "vector algebra": (
                    "vector",
                    "dot product",
                    "cross product",
                    "scalar triple",
                    "vector triple",
                    "projection",
                    "\\hat i",
                    "\\hat j",
                    "\\hat k",
                ),
                "probability and statistics": (
                    "probability",
                    "conditional probability",
                    "bayes",
                    "random",
                    "mean",
                    "variance",
                    "standard deviation",
                    "coin",
                    "dice",
                    "distribution",
                ),
            }
        if track == "Physics":
            return {
                "kinematics": ("velocity", "acceleration", "displacement", "projectile"),
                "laws of motion": ("friction", "newton", "force", "tension"),
                "work energy and power": ("work", "energy", "power", "kinetic", "potential"),
                "electrostatics": ("electric field", "electric potential", "gauss", "capacitance"),
                "current electricity": ("current", "resistance", "kirchhoff", "wheatstone"),
                "ray optics": ("mirror formula", "lens formula", "refraction", "prism"),
                "wave optics": ("interference", "diffraction", "polarization"),
            }
        if track == "Chemistry":
            return {
                "atomic structure": ("quantum number", "orbital", "electron configuration"),
                "chemical equilibrium": ("equilibrium constant", "le chatelier", "ionic equilibrium"),
                "redox reactions": ("oxidation number", "redox", "electron transfer"),
                "coordination compounds": ("ligand", "coordination number", "cfse"),
                "electrochemistry and chemical kinetics": (
                    "nernst",
                    "electrode potential",
                    "rate law",
                    "arrhenius",
                    "half life",
                ),
            }
        return {}

    def _chapter_anchor_tokens(self, chapter: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if chapter == "Binomial Theorem":
            return (
                (
                    "binomial",
                    "\\binom",
                    "ncr",
                    "pascal",
                    "(1+x)",
                    "(1 + x)",
                    "expansion of",
                    "term in the expansion",
                    "general term",
                    "middle term",
                    "greatest term",
                    "constant term",
                    "independent term",
                ),
                (
                    "coefficient of friction",
                    "moment of inertia",
                    "thermistor",
                    "density varying",
                    "velocity",
                    "acceleration",
                ),
            )
        if chapter == "Matrices and Determinants":
            return (
                ("matrix", "determinant", "adjoint", "cofactor", "rank", "inverse matrix"),
                ("coefficient of friction",),
            )
        if chapter == "Three Dimensional Geometry":
            return (
                (
                    "three dimensional",
                    "line in 3d",
                    "plane in 3d",
                    "direction ratio",
                    "direction cosine",
                    "skew line",
                    "coplanar",
                ),
                ("moment of inertia", "density varying", "thermistor"),
            )
        if chapter == "Probability and Statistics":
            return (
                (
                    "probability",
                    "random variable",
                    "distribution",
                    "expectation",
                    "variance",
                    "standard deviation",
                    "conditional probability",
                    "bayes",
                    "coin",
                    "dice",
                ),
                ("friction", "moment of inertia"),
            )
        return (), ()

    def _infer_import_chapter_candidates(
        self,
        *,
        track: str,
        text_bag: str,
        max_candidates: int = 3,
    ) -> list[dict[str, Any]]:
        low = self._str(text_bag).lower()
        if not low:
            return []
        chapters, _ = self._jee_chapter_catalog(subject=track)
        if not chapters:
            return []
        stop = {
            "and",
            "of",
            "the",
            "chapter",
            "functions",
            "function",
            "equations",
            "equation",
            "calculus",
            "geometry",
            "algebra",
            "statistics",
            "some",
            "basic",
        }
        overrides = self._chapter_signal_overrides(track=track)
        has_3d_coordinates = bool(
            re.search(
                r"\(\s*[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*\)",
                low,
            )
        )
        has_2d_coordinates = bool(
            re.search(
                r"\(\s*[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*\)",
                low,
            )
        )

        score_rows: list[tuple[str, float]] = []
        for chapter in chapters:
            chapter_key = re.sub(
                r"[^a-z0-9]+",
                " ",
                self._str(chapter).lower().replace("&", "and"),
            ).strip()
            if not chapter_key:
                continue
            signals: set[str] = {chapter_key}
            signals.update(
                tok
                for tok in chapter_key.split()
                if len(tok) >= 4 and tok not in stop
            )
            signals.update(
                self._str(x).lower().strip()
                for x in overrides.get(chapter_key, ())
                if self._str(x).strip()
            )
            score = 0.0
            for signal in signals:
                if not signal:
                    continue
                if " " in signal:
                    if signal in low:
                        score += 2.6
                else:
                    if re.search(rf"\b{re.escape(signal)}\b", low):
                        score += 1.15

            if chapter == "Three Dimensional Geometry" and has_3d_coordinates:
                score += 3.0
            if chapter == "Coordinate Geometry" and has_2d_coordinates:
                score += 2.2
            if chapter == "Integral Calculus" and ("\\int" in low or " dx" in low):
                score += 2.0
            if chapter == "Limits, Continuity and Differentiability" and (
                "\\lim" in low or "limit" in low
            ):
                score += 1.8
            if chapter == "Application of Derivatives" and any(
                tok in low for tok in ("d/dx", "derivative", "maxima", "minima", "tangent", "normal")
            ):
                score += 1.8
            if chapter == "Vector Algebra" and any(
                tok in low for tok in ("dot product", "cross product", "\\hat i", "\\hat j", "\\hat k")
            ):
                score += 1.8

            anchors, anti_anchors = self._chapter_anchor_tokens(chapter)
            if anchors:
                anchor_hits = sum(1 for tok in anchors if tok in low)
                if chapter == "Binomial Theorem":
                    if anchor_hits <= 0:
                        score *= 0.12
                    elif anchor_hits == 1:
                        score *= 0.60
                elif anchor_hits <= 0:
                    score *= 0.35
            if anti_anchors:
                anti_hits = sum(1 for tok in anti_anchors if tok in low)
                if anti_hits > 0:
                    score -= min(3.0, anti_hits * 1.2)

            if score > 0.0:
                score_rows.append((chapter, score))

        if not score_rows:
            return []
        score_rows.sort(key=lambda item: item[1], reverse=True)
        best_score = score_rows[0][1]
        # Keep inference conservative for non-math tracks, but be more
        # permissive for math PYQ imports where OCR damage often weakens
        # lexical signals and otherwise leaves rows stuck in "mixed".
        min_best_score = 1.35 if track == "Mathematics" else 1.95
        if best_score < min_best_score:
            return []

        candidate_floor = max(1.0, best_score * 0.45)
        out: list[dict[str, Any]] = []
        for chapter, score in score_rows:
            if score < candidate_floor:
                continue
            out.append({"chapter": chapter, "score": round(score, 6)})
            if len(out) >= max(1, max_candidates):
                break
        return out

    def _infer_import_chapter_by_signals(self, *, track: str, text_bag: str) -> str:
        candidates = self._infer_import_chapter_candidates(
            track=track,
            text_bag=text_bag,
            max_candidates=1,
        )
        if not candidates:
            return ""
        return self._str(candidates[0].get("chapter"))

    def _infer_import_chapter(
        self,
        *,
        subject: str,
        question_text: str,
        fallback_chapter: str,
        context_text: str = "",
        allow_non_generic_override: bool = False,
    ) -> str:
        normalized_subject = self._str(subject).strip() or "Mathematics"
        fallback = self._str(fallback_chapter).strip()
        bag = " ".join([self._str(question_text), self._str(context_text)]).strip()
        cache_key = hashlib.sha256(
            f"{normalized_subject.lower()}|{fallback.lower()}|"
            f"{'1' if allow_non_generic_override else '0'}|{bag.lower()}".encode("utf-8")
        ).hexdigest()
        cached = self._import_chapter_infer_cache.get(cache_key)
        if cached:
            return cached

        clean_fallback = "" if self._import_chapter_is_generic(fallback) else fallback
        track = self._infer_subject_track(
            subject=normalized_subject,
            chapters=[],
            subtopics=[],
            concept_tags=[],
        )
        low = bag.lower()
        signal_chapter = self._infer_import_chapter_by_signals(track=track, text_bag=low)
        fallback_credible = True
        if fallback and not self._import_chapter_is_generic(fallback) and allow_non_generic_override:
            fallback_anchors, fallback_anti = self._chapter_anchor_tokens(fallback)
            anchor_hits = sum(1 for tok in fallback_anchors if tok in low) if fallback_anchors else 0
            anti_hits = sum(1 for tok in fallback_anti if tok in low) if fallback_anti else 0
            if fallback_anchors and anchor_hits <= 0:
                fallback_credible = False
            if anti_hits > 0 and anchor_hits <= 1:
                fallback_credible = False
            if fallback == "Binomial Theorem" and anchor_hits <= 0:
                fallback_credible = False
            if not fallback_credible:
                clean_fallback = ""
                fallback = ""
        if fallback and not self._import_chapter_is_generic(fallback) and fallback_credible:
            if (
                allow_non_generic_override
                and signal_chapter
                and signal_chapter != fallback
            ):
                result = signal_chapter
            else:
                result = fallback
        elif signal_chapter:
            result = signal_chapter
        elif track != "Mathematics":
            result = clean_fallback or fallback or track
        elif any(tok in low for tok in ("matrix", "determinant", "adjoint", "rank")):
            result = "Matrices and Determinants"
        elif any(
            tok in low
            for tok in (
                "integral",
                "\\int",
                "dx",
                "definite integral",
                "indefinite integral",
            )
        ):
            result = "Integral Calculus"
        elif any(
            tok in low
            for tok in (
                "lim",
                "\\lim",
                "continuity",
                "continuous",
                "differentiability",
                "differentiable",
            )
        ):
            result = "Limits, Continuity and Differentiability"
        elif any(
            tok in low
            for tok in (
                "d/dx",
                "derivative",
                "maxima",
                "minima",
                "tangent",
                "normal",
                "increasing",
                "decreasing",
            )
        ):
            result = "Application of Derivatives"
        elif any(
            tok in low
            for tok in (
                "vector",
                "dot product",
                "cross product",
                "line in 3d",
                "plane",
                "direction cosine",
                "direction ratio",
                "three dimensional",
                "3d",
            )
        ):
            result = "Three Dimensional Geometry"
        elif any(tok in low for tok in ("circle", "parabola", "ellipse", "hyperbola", "coordinate")):
            result = "Coordinate Geometry"
        elif any(
            tok in low
            for tok in (
                "function",
                "domain",
                "range",
                "one-one",
                "onto",
                "relation",
                "set ",
            )
        ):
            result = "Relations and Functions"
        elif any(
            tok in low
            for tok in (
                "binomial",
                "\\binom",
                "ncr",
                "(1+x)^",
                "(1 + x)^",
                "expansion of",
                "general term",
                "middle term",
                "greatest term",
                "constant term",
                "independent term",
            )
        ):
            result = "Binomial Theorem"
        elif any(
            tok in low
            for tok in ("permutation", "combination", "ncr", "npr", "arrangement")
        ):
            result = "Permutations and Combinations"
        elif any(tok in low for tok in ("sequence", "series", "a.p", "g.p", "progression")):
            result = "Sequences and Series"
        elif any(tok in low for tok in ("probability", "random", "coin", "dice", "variance")):
            result = "Probability and Statistics"
        else:
            result = clean_fallback or fallback or "General JEE Mathematics"

        self._import_chapter_infer_cache[cache_key] = result
        if len(self._import_chapter_infer_cache) > self._import_chapter_cache_max_entries:
            purge_count = max(200, self._import_chapter_cache_max_entries // 5)
            stale_keys = list(self._import_chapter_infer_cache.keys())[:purge_count]
            for key in stale_keys:
                self._import_chapter_infer_cache.pop(key, None)
        return result

    def _resolve_import_row_chapter(
        self,
        row: dict[str, Any],
        *,
        subject_override: str = "",
        chapter_override: str = "",
        question_text_override: str = "",
        options_override: list[dict[str, Any]] | None = None,
    ) -> str:
        if not isinstance(row, dict):
            return ""
        subject = self._str(subject_override or row.get("subject") or "Mathematics").strip()
        fallback_chapter = self._str(chapter_override or row.get("chapter")).strip()
        question_text = self._str(
            question_text_override or row.get("question_text") or row.get("question") or row.get("text")
        )
        context_text = self._import_row_context_text(
            row,
            question_text_override=question_text,
            options_override=options_override,
        )
        return self._infer_import_chapter(
            subject=subject,
            question_text=question_text,
            fallback_chapter=fallback_chapter,
            context_text=context_text,
            allow_non_generic_override=True,
        )

    def _import_row_context_text(
        self,
        row: dict[str, Any],
        *,
        question_text_override: str = "",
        options_override: list[dict[str, Any]] | None = None,
    ) -> str:
        if not isinstance(row, dict):
            return ""
        question_text = self._str(
            question_text_override or row.get("question_text") or row.get("question") or row.get("text")
        )
        context_parts: list[str] = [question_text]
        options_source: Any = options_override if options_override is not None else row.get("options")
        if isinstance(options_source, list):
            for opt in options_source:
                if isinstance(opt, dict):
                    context_parts.append(
                        self._str(opt.get("text") or opt.get("value") or opt.get("option"))
                    )
                else:
                    context_parts.append(self._str(opt))
        context_parts.extend(
            [
                self._str(row.get("solution") or row.get("solution_explanation")),
                self._str(row.get("concept") or row.get("topic")),
                self._str(row.get("section") or row.get("sub_part") or row.get("subpart")),
            ]
        )
        return " ".join(x for x in context_parts if self._str(x))

    def _resolve_import_row_chapter_tags(
        self,
        row: dict[str, Any],
        *,
        subject_override: str = "",
        chapter_override: str = "",
        question_text_override: str = "",
        options_override: list[dict[str, Any]] | None = None,
        max_tags: int = 3,
    ) -> list[str]:
        if not isinstance(row, dict):
            return []
        subject = self._str(subject_override or row.get("subject") or "Mathematics").strip()
        fallback_chapter = self._str(chapter_override or row.get("chapter")).strip()
        question_text = self._str(
            question_text_override or row.get("question_text") or row.get("question") or row.get("text")
        )
        context_text = self._import_row_context_text(
            row,
            question_text_override=question_text,
            options_override=options_override,
        )
        primary = self._infer_import_chapter(
            subject=subject,
            question_text=question_text,
            fallback_chapter=fallback_chapter,
            context_text=context_text,
            allow_non_generic_override=True,
        )
        track = self._infer_subject_track(
            subject=subject,
            chapters=[],
            subtopics=[],
            concept_tags=[],
        )
        candidates = self._infer_import_chapter_candidates(
            track=track,
            text_bag=f"{question_text} {context_text}",
            max_candidates=max(1, max_tags),
        )
        tags: list[str] = []
        if primary:
            tags.append(primary)
        for row in candidates:
            chapter = self._str(row.get("chapter"))
            if chapter and chapter not in tags:
                tags.append(chapter)
            if len(tags) >= max(1, max_tags):
                break
        return tags[: max(1, max_tags)]

    def _import_issue_severity(self, issue: str) -> str:
        token = self._str(issue).lower()
        if not token:
            return "review"
        critical_tokens = (
            "cannot",
            "requires",
            "must",
            "invalid",
            "duplicate",
            "unbalanced",
            "rejected",
            "missing",
            "empty",
        )
        if any(flag in token for flag in critical_tokens):
            return "critical"
        return "review"

    def _import_publish_risk_score(self, row: dict[str, Any]) -> float:
        status = self._str(row.get("validation_status")).lower()
        conf = self._to_float(
            row.get("confidence_score")
            if row.get("confidence_score") is not None
            else row.get("ai_confidence"),
            0.0,
        )
        conf = max(0.0, min(1.0, conf))
        base = 0.08
        if status == "review":
            base = 0.36
        elif status == "invalid":
            base = 0.86
        issues = [self._str(x) for x in (row.get("validation_errors") or []) if self._str(x)]
        critical = sum(1 for token in issues if self._import_issue_severity(token) == "critical")
        review = max(0, len(issues) - critical)
        source = self._str(row.get("answer_fill_source")).lower()
        source_penalty = 0.0 if source in {"inline_hint", "web_verified"} else (0.06 if source else 0.1)
        risk = base + ((1.0 - conf) * 0.48) + (critical * 0.24) + (review * 0.05) + source_penalty
        return round(max(0.0, min(1.0, risk)), 6)

    def _build_import_quality_dashboard(
        self,
        *,
        questions: list[dict[str, Any]],
        input_report: dict[str, Any],
        summary: dict[str, int],
    ) -> dict[str, Any]:
        per_page_conf = []
        for page in (input_report.get("per_page_ocr_confidence") or []):
            if not isinstance(page, dict):
                continue
            per_page_conf.append(
                {
                    "page_number": self._to_int(page.get("page_number"), 0),
                    "confidence": round(self._to_float(page.get("confidence"), 0.0), 6),
                }
            )
        answer_fill_source_counts: dict[str, int] = {}
        risk_values: list[float] = []
        for row in questions:
            source = self._str(row.get("answer_fill_source")).lower() or "unknown"
            answer_fill_source_counts[source] = answer_fill_source_counts.get(source, 0) + 1
            risk = self._to_float(row.get("publish_risk_score"), -1.0)
            if risk < 0.0:
                risk = self._import_publish_risk_score(row)
            risk_values.append(max(0.0, min(1.0, risk)))
        publish_risk_score = round(
            sum(risk_values) / max(1, len(risk_values)),
            6,
        )
        if summary.get("invalid", 0) > 0:
            publish_risk_score = round(min(1.0, publish_risk_score + 0.22), 6)
        elif summary.get("review", 0) > 0:
            publish_risk_score = round(min(1.0, publish_risk_score + 0.08), 6)
        return {
            "per_page_ocr_confidence": per_page_conf,
            "symbol_repair_count": self._to_int(input_report.get("symbol_repair_count"), 0),
            "answer_fill_source_counts": answer_fill_source_counts,
            "publish_risk_score": publish_risk_score,
            "question_count": len(questions),
        }

    def _validate_and_normalize_import_question(
        self,
        *,
        row: dict[str, Any],
        index: int,
        meta_defaults: dict[str, str],
    ) -> tuple[dict[str, Any], list[str]]:
        question_id = self._safe_id(row.get("question_id") or row.get("id"))
        if not question_id:
            question_id = f"imp_q_{index}"
        question_text = self._str(
            row.get("question_text") or row.get("question") or row.get("text")
        )

        options_raw = row.get("options")
        if options_raw is None:
            option_values = [
                row.get("option_a"),
                row.get("option_b"),
                row.get("option_c"),
                row.get("option_d"),
            ]
            if any(self._str(x) for x in option_values):
                options_raw = option_values
        options = self._normalize_import_options(options_raw)
        option_labels = [self._str(x.get("label")).upper() for x in options]

        answer_raw = row.get("correct_answer")
        answer_map = dict(answer_raw) if isinstance(answer_raw, dict) else {}
        single = self._str(
            answer_map.get("single")
            or row.get("correct_option")
            or row.get("single_correct")
        ).upper()
        multiple = [
            self._extract_import_label_token(x)
            for x in self._to_list_str(
                answer_map.get("multiple")
                or row.get("correct_answers")
                or row.get("multiple_correct")
            )
        ]
        multiple = [x for x in multiple if x]
        numerical = self._str(
            answer_map.get("numerical")
            or row.get("numerical_answer")
            or row.get("answer")
            or row.get("correct")
        )
        tolerance_raw = answer_map.get("tolerance")
        if tolerance_raw is None:
            tolerance_raw = row.get("tolerance")
        tolerance = None
        if self._str(tolerance_raw):
            tolerance = self._to_float(tolerance_raw, 0.0)

        answer_hint = self._str(row.get("answer_hint"))
        hint_labels, hint_numerical = self._parse_import_answer_hint(
            answer_hint=answer_hint,
            options=options,
        )
        if not multiple and hint_labels:
            multiple = hint_labels
        if not single and multiple:
            single = multiple[0]
        if not numerical and hint_numerical:
            numerical = hint_numerical

        solution_raw = row.get("solution_explanation")
        if solution_raw is None:
            solution_raw = row.get("_solution_explanation")
        if solution_raw is None:
            solution_raw = row.get("solution")
        if isinstance(solution_raw, dict):
            solution_explanation = " ".join(
                [
                    self._str(solution_raw.get("core_idea")),
                    self._str(solution_raw.get("intuition")),
                    self._str(solution_raw.get("formal_derivation")),
                    self._str(solution_raw.get("shortcut")),
                ]
            ).strip()
        else:
            solution_explanation = self._str(solution_raw)
        source_solution_stub = self._str(
            row.get("source_solution_stub") or row.get("solution_stub")
        )
        if not source_solution_stub and solution_explanation:
            source_solution_stub = solution_explanation
        if not solution_explanation and source_solution_stub:
            solution_explanation = source_solution_stub
        if solution_explanation:
            solution_explanation = re.sub(r"\s+", " ", solution_explanation).strip()[:2400]
        if source_solution_stub:
            source_solution_stub = re.sub(r"\s+", " ", source_solution_stub).strip()[:420]

        answer_fill_source = self._str(row.get("answer_fill_source")).lower()
        if not answer_fill_source:
            source_origin = self._str(row.get("source_origin")).lower()
            if source_origin.startswith("web_") or source_origin in {"fusion_verified", "web_verified"}:
                answer_fill_source = "web_verified"
            elif answer_hint.strip():
                answer_fill_source = "inline_hint"
            elif self._str(row.get("answer_key_hint")).strip():
                answer_fill_source = "global_answer_key"
            else:
                answer_fill_source = "manual"

        question_type = self._normalize_import_question_type(
            row.get("type") or row.get("question_type"),
            has_options=bool(options),
        )
        if question_type == "MCQ_SINGLE" and len(multiple) > 1:
            question_type = "MCQ_MULTI"
        if question_type != "NUMERICAL" and not options and self._looks_numeric_import_prompt(
            question_text
        ):
            question_type = "NUMERICAL"

        repair_actions: list[str] = []
        repair_issues: list[str] = []
        repair_confidence = 0.0
        repair_status = "none"
        try:
            repair = self._question_repair_engine.repair_question(
                question_text=question_text,
                options=options,
                correct_answer={
                    "single": single or None,
                    "multiple": multiple,
                    "numerical": numerical or None,
                    "tolerance": tolerance,
                },
                question_type=question_type,
            )
            question_text = self._str(repair.question_text)
            options = self._normalize_import_options(repair.options)
            option_labels = [self._str(x.get("label")).upper() for x in options]
            repaired_answer = dict(repair.correct_answer or {})
            single = self._str(repaired_answer.get("single")).upper()
            multiple = [
                self._extract_import_label_token(x)
                for x in self._to_list_str(repaired_answer.get("multiple"))
            ]
            multiple = [x for x in multiple if x]
            numerical = self._str(repaired_answer.get("numerical"))
            tolerance = repaired_answer.get("tolerance")
            if tolerance is not None:
                tolerance = self._to_float(tolerance, 0.0)
            repair_actions = list(repair.repair_actions or [])
            repair_issues = list(repair.repair_issues or [])
            repair_confidence = max(0.0, min(1.0, self._to_float(repair.repair_confidence, 0.0)))
            repair_status = self._str(repair.repair_status) or "none"
        except Exception:
            repair_actions = []
            repair_issues = []
            repair_confidence = 0.0
            repair_status = "none"

        hard_errors: list[str] = []
        review_notes: list[str] = []
        seen_options: set[str] = set()

        for issue in repair_issues:
            token = self._str(issue).lower()
            if token in {"empty_question", "invalid_numerical_answer", "answer_label_mismatch"}:
                mapped = {
                    "empty_question": "Question text cannot be empty.",
                    "invalid_numerical_answer": "NUMERICAL answer must be a valid number.",
                    "answer_label_mismatch": "Correct answer label does not match options.",
                }.get(token, "")
                if mapped and mapped not in hard_errors:
                    hard_errors.append(mapped)
            elif token in {
                "truncated_question",
                "expression_parse_failure",
                "missing_options",
                "unbalanced_brackets",
            }:
                mapped = {
                    "truncated_question": "Question appears truncated; review required.",
                    "expression_parse_failure": "Math expression parse failed; review required.",
                    "missing_options": "No options detected for non-numerical question.",
                    "unbalanced_brackets": "Question has unbalanced brackets.",
                }.get(token, "")
                if mapped and mapped not in review_notes:
                    review_notes.append(mapped)
        if repair_status == "manual_review":
            review_flag = "Repair confidence low; manual review required."
            if review_flag not in review_notes:
                review_notes.append(review_flag)

        if not question_text:
            hard_errors.append("Question text cannot be empty.")
        else:
            if not self._balanced_brackets(question_text):
                hard_errors.append("Question has unbalanced brackets.")
            if self._looks_truncated_equation(question_text):
                review_notes.append("Question appears truncated; review required.")

        for option in options:
            text = self._str(option.get("text"))
            label = self._str(option.get("label")).upper()
            if not text:
                hard_errors.append(f"Option {label or '?'} cannot be empty.")
            if text and self._looks_truncated_equation(text):
                review_notes.append(f"Option {label or '?'} appears truncated.")
            if text and not self._balanced_brackets(text):
                hard_errors.append(f"Option {label or '?'} has unbalanced brackets.")
            key = text.lower()
            if key and key in seen_options:
                hard_errors.append("Duplicate option text is invalid.")
            if key:
                seen_options.add(key)
        if options and not self._options_logically_distinct(options):
            review_notes.append("Options are too similar; review required.")

        if question_type == "NUMERICAL":
            options = []
            option_labels = []
            if not self._IMPORT_NUMERIC_TOKEN_RE.search(numerical):
                hard_errors.append("NUMERICAL answer must be a valid number.")
            single = ""
            multiple = []
        elif question_type == "MCQ_MULTI":
            if len(options) < 2:
                hard_errors.append("MCQ_MULTI requires at least 2 options.")
            filtered = [x for x in multiple if x in option_labels]
            invalid_labels = [x for x in multiple if x and x not in option_labels]
            for label in invalid_labels:
                hard_errors.append(
                    f'MCQ_MULTI contains invalid answer label "{label}".'
                )
            multiple = list(dict.fromkeys(filtered))
            if not multiple:
                hard_errors.append("MCQ_MULTI requires one or more correct answers.")
            elif len(multiple) == 1:
                review_notes.append(
                    "MCQ_MULTI has only one correct option; review required."
                )
            single = multiple[0] if multiple else ""
            numerical = ""
        else:
            if len(options) < 2:
                hard_errors.append("MCQ_SINGLE requires at least 2 options.")
            normalized_single = self._extract_import_label_token(single)
            if not normalized_single and multiple:
                normalized_single = multiple[0]
            if not normalized_single:
                hard_errors.append("MCQ_SINGLE requires exactly one correct answer.")
            elif normalized_single not in option_labels:
                hard_errors.append("Correct single answer label does not match options.")
            single = normalized_single
            multiple = [single] if single else []
            numerical = ""

        if question_type != "NUMERICAL" and not options:
            hard_errors.append("No options detected for non-numerical question.")

        ai_confidence = max(
            0.0,
            min(1.0, self._to_float(row.get("ai_confidence"), 0.0)),
        )
        requested_status = self._str(row.get("validation_status")).lower()
        if hard_errors:
            status = "invalid"
        elif review_notes or requested_status == "review":
            status = "review"
        else:
            status = "valid"

        merged_errors: list[str] = []
        for entry in (row.get("validation_errors") or []):
            text = self._str(entry)
            if text and text not in merged_errors:
                merged_errors.append(text)
        for entry in hard_errors + review_notes:
            if entry and entry not in merged_errors:
                merged_errors.append(entry)

        normalized_subject = self._str(row.get("subject") or meta_defaults.get("subject"))
        raw_chapter = self._str(row.get("chapter") or meta_defaults.get("chapter"))
        normalized_chapter_tags = self._resolve_import_row_chapter_tags(
            row=row,
            subject_override=normalized_subject,
            chapter_override=raw_chapter,
            question_text_override=question_text,
            options_override=options,
            max_tags=3,
        )
        normalized_chapter = self._resolve_import_row_chapter(
            row=row,
            subject_override=normalized_subject,
            chapter_override=raw_chapter,
            question_text_override=question_text,
            options_override=options,
        )
        if normalized_chapter and normalized_chapter not in normalized_chapter_tags:
            normalized_chapter_tags = [normalized_chapter, *normalized_chapter_tags]
        normalized: dict[str, Any] = {
            "question_id": question_id,
            "type": question_type,
            "question_text": question_text,
            "question_text_latex": self._canonical_import_latex(question_text),
            "options": options,
            "options_latex": {
                self._str(opt.get("label")).upper(): self._canonical_import_latex(
                    self._str(opt.get("text"))
                )
                for opt in options
                if isinstance(opt, dict)
            },
            "correct_answer": {
                "single": single or None,
                "multiple": multiple,
                "numerical": numerical or None,
                "tolerance": tolerance,
            },
            "answer_fill_source": answer_fill_source,
            "subject": normalized_subject,
            "chapter": normalized_chapter,
            "chapter_tags": normalized_chapter_tags[:3],
            "difficulty": self._str(
                row.get("difficulty") or meta_defaults.get("difficulty")
            ),
            "ai_confidence": ai_confidence,
            "repair_actions": repair_actions,
            "repair_confidence": repair_confidence,
            "repair_status": repair_status,
            "validation_status": status,
            "validation_errors": merged_errors,
        }
        if solution_explanation:
            normalized["solution_explanation"] = solution_explanation
        if source_solution_stub:
            normalized["source_solution_stub"] = source_solution_stub
            normalized["has_solution"] = True
        normalized["publish_risk_score"] = self._import_publish_risk_score(normalized)
        return normalized, hard_errors

    def _coerce_import_questions(
        self, payload: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
        meta = self._import_meta_from_payload(payload)
        rows = self._extract_import_questions_payload(payload)
        normalized: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=1):
            parsed, hard_errors = self._validate_and_normalize_import_question(
                row=row,
                index=index,
                meta_defaults=meta,
            )
            normalized.append(parsed)
            if hard_errors:
                invalid.append(
                    {
                        "question_id": self._str(parsed.get("question_id"))
                        or f"imp_q_{index}",
                        "errors": hard_errors,
                    }
                )
        return normalized, invalid, meta

    def _extract_global_answer_key_map(self, raw_text: str) -> dict[int, str]:
        lines = [
            self._normalize_import_line(line)
            for line in self._str(raw_text).replace("\r\n", "\n").replace("\r", "\n").split("\n")
        ]
        lines = [line for line in lines if line]
        answer_zone = False
        mapping: dict[int, str] = {}
        best_score: dict[int, int] = {}
        pending_qnos: list[int] = []

        def store(q_idx: int, token: str, score: int) -> None:
            if q_idx <= 0:
                return
            cleaned = self._str(token).strip().strip("()[]{}.")
            if not cleaned:
                return
            if q_idx not in mapping or score >= best_score.get(q_idx, -1):
                mapping[q_idx] = cleaned
                best_score[q_idx] = score

        def answer_tokens(line: str) -> list[str]:
            tokens = re.findall(
                r"(?i)\b(?:[A-D]|[1-4]|[-+]?\d+(?:\.\d+)?)\b",
                line,
            )
            out: list[str] = []
            for token in tokens:
                cleaned = self._str(token).strip().strip("()[]{}.")
                if cleaned:
                    out.append(cleaned)
            return out

        for line in lines:
            low = line.lower()
            number_header = bool(
                re.search(r"(?i)\bq(?:uestion)?\.?\s*no\b|\bqno\b|\bq\.?\b", line)
            )
            if any(
                token in low
                for token in (
                    "answer key",
                    "answerkey",
                    "final answers",
                    "solutions key",
                    "correct options",
                    "correct answers",
                    "key:",
                )
            ):
                answer_zone = True
            if answer_zone and re.search(
                r"(?i)^\s*(section|part|instruction|solution)\b",
                line,
            ):
                answer_zone = False
                pending_qnos = []
            has_answer_marker = bool(
                re.search(
                    r"(?i)\bans(?:wer)?\b|\bkey\b|\bcorrect\b|\bsolution\b",
                    line,
                )
            )
            if pending_qnos:
                tokens = answer_tokens(line)
                if tokens and (
                    answer_zone
                    or has_answer_marker
                    or len(tokens) >= len(pending_qnos)
                ):
                    score = (
                        4
                        if answer_zone and has_answer_marker
                        else 3
                        if answer_zone
                        else 2
                    )
                    for q_idx, token in zip(pending_qnos, tokens):
                        store(q_idx, token, score)
                    pending_qnos = []
                    continue
            pair_matches = re.findall(
                r"(?i)(?:^|[\s,;|])(?:q(?:uestion)?\s*)?(\d{1,3})\s*[\])\).:\-]?\s*(?:ans(?:wer)?\s*[:\-]?\s*)?(?:option\s*)?\(?([A-D]{1,4}|[1-4]{1,4}|[-+]?\d+(?:\.\d+)?)\)?\b",
                line,
            )
            all_pair_tokens_numeric = bool(pair_matches) and all(
                re.fullmatch(r"[-+]?\d+(?:\.\d+)?", self._str(token))
                for _, token in pair_matches
            )
            if number_header and all_pair_tokens_numeric and not has_answer_marker:
                pair_matches = []
            has_letter_token = any(re.search(r"(?i)[a-d]", token) for _, token in pair_matches)
            if pair_matches and (
                answer_zone
                or has_answer_marker
                or (len(pair_matches) >= 2 and has_letter_token)
            ):
                score = (
                    4
                    if answer_zone and has_answer_marker
                    else 3
                    if answer_zone
                    else 2
                    if has_answer_marker
                    else 1
                )
                for q_no, token in pair_matches:
                    store(self._to_int(q_no, 0), token, score)
                pending_qnos = []
                continue

            # Support tabular key layouts:
            # Q.No. 11 12 13
            # Ans.  C  A  D
            number_row = [
                self._to_int(x, 0)
                for x in re.findall(r"(?<!\d)(\d{1,3})(?!\d)", line)
                if self._to_int(x, 0) > 0
            ]
            if (answer_zone or number_header) and len(number_row) >= 2:
                mostly_numeric = len(re.findall(r"[A-Za-z]", line)) <= 4
                if number_header or mostly_numeric:
                    pending_qnos = number_row
                    continue
        return mapping

    def _extract_global_solution_map(self, raw_text: str) -> dict[int, str]:
        lines = [
            self._normalize_import_line(line)
            for line in self._str(raw_text).replace("\r\n", "\n").replace("\r", "\n").split("\n")
        ]
        lines = [line for line in lines if line]
        mapping_parts: dict[int, list[str]] = {}
        solution_zone = False
        current_q = 0

        for line in lines:
            low = line.lower()
            if re.search(r"(?i)\b(?:detailed\s+)?solutions?\b", line):
                solution_zone = True
                current_q = 0
                continue
            if not solution_zone:
                continue
            if re.search(r"(?i)^\s*(?:answer\s*key|correct\s*options?)\b", line):
                current_q = 0
                continue
            if re.search(r"(?i)^\s*(?:section|part|instruction)\b", line):
                current_q = 0
                continue
            q_match = self._IMPORT_QUESTION_NUMBER_RE.match(line)
            if q_match:
                current_q = self._to_int(q_match.group(1), 0)
                remainder = self._IMPORT_QUESTION_START_RE.sub("", line).strip()
                if current_q > 0 and remainder:
                    mapping_parts.setdefault(current_q, []).append(remainder)
                continue
            if current_q <= 0:
                continue
            parts = mapping_parts.setdefault(current_q, [])
            if len(parts) >= 24:
                continue
            parts.append(line)

        mapping: dict[int, str] = {}
        for q_no, parts in mapping_parts.items():
            merged = re.sub(
                r"\s+",
                " ",
                " ".join(self._str(x).strip() for x in parts if self._str(x).strip()),
            ).strip()
            if merged:
                mapping[q_no] = merged[:2400]
        return mapping

    async def _resolve_import_input_text(
        self, payload: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        raw_text = self._str(payload.get("raw_text") or payload.get("text"))
        if raw_text:
            return raw_text, {"input_source": "raw_text"}

        file_id = self._str(payload.get("file_id") or payload.get("upload_id"))
        file_path = self._str(
            payload.get("pdf_path")
            or payload.get("file_path")
            or payload.get("document_path")
        )
        mime = self._str(payload.get("mime_type") or payload.get("mime"))
        if not file_path and file_id:
            meta = self._uploads.get(file_id)
            if isinstance(meta, dict):
                file_path = self._str(meta.get("path"))
                if not mime:
                    mime = self._str(meta.get("mime"))
        if not file_path:
            return "", {"input_source": "none"}
        path = Path(file_path)
        if not path.exists():
            return "", {"input_source": "missing_file", "file_path": file_path}
        try:
            blob = path.read_bytes()
        except Exception:
            return "", {"input_source": "read_failed", "file_path": file_path}
        is_pdf = (
            path.suffix.lower() == ".pdf"
            or "pdf" in mime.lower()
            or blob.startswith(b"%PDF")
        )
        is_image = self._looks_like_image_blob(blob, mime=mime, path=path)
        if is_image and not is_pdf:
            try:
                from core.multimodal.ocr_engine import OCREngine

                ocr = await OCREngine().extract_async(
                    blob,
                    page_number=1,
                    math_aware=True,
                )
                merged_text = self._str(
                    ocr.get("clean_text")
                    or ocr.get("math_normalized_text")
                    or ocr.get("raw_text")
                )
                symbol_repair_count = self._count_symbol_font_artifacts(merged_text)
                return merged_text, {
                    "input_source": "image",
                    "file_path": str(path),
                    "file_size": len(blob),
                    "overall_confidence": self._to_float(ocr.get("confidence"), 0.0),
                    "per_page_ocr_confidence": [
                        {
                            "page_number": 1,
                            "confidence": self._to_float(ocr.get("confidence"), 0.0),
                        }
                    ],
                    "symbol_repair_count": symbol_repair_count,
                    "text_length": len(merged_text),
                }
            except Exception as exc:
                return "", {
                    "input_source": "image_ocr_failed",
                    "file_path": str(path),
                    "file_size": len(blob),
                    "image_error": f"{exc.__class__.__name__}:{self._str(exc)[:140]}",
                    "text_length": 0,
                }
        if not is_pdf:
            text = blob.decode("utf-8", errors="ignore")
            return text, {
                "input_source": "file_text",
                "file_path": str(path),
                "file_size": len(blob),
            }
        try:
            from core.multimodal.pdf_processor import PDFProcessor

            pdf_report = await PDFProcessor(max_pages=40, ocr_parallelism=4).process(blob)
            merged_text = self._str(pdf_report.get("merged_text"))
            text_quality = "ocr"
            if merged_text and self._looks_like_binary_pdf_text(merged_text):
                merged_text = ""
                text_quality = "ocr_binary_like"
            if not merged_text:
                extracted_pdf_text = self._extract_text_from_pdf_bytes(blob)
                if extracted_pdf_text:
                    merged_text = extracted_pdf_text
                    text_quality = "pdf_text_extract"
            if not merged_text:
                merged_text = ""
                text_quality = "no_text_extracted"
            per_page_ocr_confidence: list[dict[str, Any]] = []
            for page in (pdf_report.get("pages") or []):
                if not isinstance(page, dict):
                    continue
                per_page_ocr_confidence.append(
                    {
                        "page_number": self._to_int(page.get("page_number"), 0),
                        "confidence": self._to_float(page.get("confidence"), 0.0),
                    }
                )
            symbol_repair_count = self._count_symbol_font_artifacts(merged_text)
            return merged_text, {
                "input_source": "pdf",
                "file_path": str(path),
                "file_size": len(blob),
                "page_count": self._to_int(pdf_report.get("page_count"), 0),
                "overall_confidence": self._to_float(
                    pdf_report.get("overall_confidence"),
                    0.0,
                ),
                "lc_iie_question_count": len(
                    [x for x in (pdf_report.get("lc_iie_questions") or []) if isinstance(x, dict)]
                ),
                "per_page_ocr_confidence": per_page_ocr_confidence,
                "ocr_retry_report": dict(pdf_report.get("retry_report") or {}),
                "symbol_repair_count": symbol_repair_count,
                "text_quality": text_quality,
                "text_length": len(merged_text),
            }
        except Exception as exc:
            return "", {
                "input_source": "pdf_no_text_extracted",
                "file_path": str(path),
                "file_size": len(blob),
                "pdf_error": f"{exc.__class__.__name__}:{self._str(exc)[:140]}",
                "symbol_repair_count": 0,
                "text_quality": "no_text_extracted",
                "text_length": 0,
            }

    def _parse_import_raw_text(
        self, raw_text: str, *, meta_defaults: dict[str, str]
    ) -> list[dict[str, Any]]:
        lines = [
            self._normalize_import_line(line)
            for line in raw_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        ]
        lines = [line for line in lines if line]
        if not lines:
            return []
        answer_key_map = self._extract_global_answer_key_map(raw_text)
        solution_key_map = self._extract_global_solution_map(raw_text)

        active_instruction = ""
        blocks: list[dict[str, Any]] = []
        current: dict[str, Any] | None = None

        def flush_current() -> None:
            nonlocal current
            if current is None:
                return
            question_lines = current.get("question_lines") or []
            question_text = " ".join(
                [self._str(x).strip() for x in question_lines if self._str(x)]
            ).strip()
            if question_text:
                current["question_text"] = re.sub(r"\s+", " ", question_text).strip()
                blocks.append(current)
            current = None

        for line in lines:
            if current is None and self._looks_like_import_instruction(line):
                active_instruction = line
                continue
            if self._IMPORT_QUESTION_START_RE.match(line):
                flush_current()
                question_no = self._extract_import_question_number(line)
                current = {
                    "question_lines": [
                        self._IMPORT_QUESTION_START_RE.sub("", line).strip()
                    ],
                    "section_instruction": active_instruction,
                    "options": {},
                    "active_option": "",
                    "active_solution": False,
                    "solution_lines": [],
                    "answer_hint": "",
                    "question_no": question_no,
                }
                continue
            if current is None:
                if self._looks_like_import_instruction(line):
                    active_instruction = line
                continue

            answer_match = self._IMPORT_ANSWER_LINE_RE.match(line)
            if answer_match is not None:
                current["answer_hint"] = self._str(answer_match.group(1))
                current["active_solution"] = False
                continue

            solution_match = self._IMPORT_SOLUTION_LINE_RE.match(line)
            if solution_match is not None:
                solution_lines = current.get("solution_lines")
                if not isinstance(solution_lines, list):
                    solution_lines = []
                    current["solution_lines"] = solution_lines
                chunk = self._str(solution_match.group(1))
                if chunk:
                    solution_lines.append(chunk)
                current["active_option"] = ""
                current["active_solution"] = True
                continue

            option_match = self._IMPORT_OPTION_START_RE.match(line)
            if option_match is not None:
                label = self._extract_import_label_token(option_match.group(1))
                text = self._str(option_match.group(2))
                if label and text:
                    options_map = current.get("options")
                    if not isinstance(options_map, dict):
                        options_map = {}
                        current["options"] = options_map
                    previous = self._str(options_map.get(label))
                    options_map[label] = f"{previous} {text}".strip() if previous else text
                    current["active_option"] = label
                    current["active_solution"] = False
                    continue

            active_option = self._str(current.get("active_option"))
            if self._to_bool(current.get("active_solution")) and not self._IMPORT_QUESTION_START_RE.match(line):
                if self._looks_like_import_instruction(line):
                    current["active_solution"] = False
                    continue
                solution_lines = current.get("solution_lines")
                if not isinstance(solution_lines, list):
                    solution_lines = []
                    current["solution_lines"] = solution_lines
                solution_lines.append(line)
                continue
            if active_option and not self._IMPORT_QUESTION_START_RE.match(line):
                options_map = current.get("options")
                if not isinstance(options_map, dict):
                    options_map = {}
                    current["options"] = options_map
                previous = self._str(options_map.get(active_option))
                options_map[active_option] = f"{previous} {line}".strip()
            else:
                current["active_option"] = ""
                current["active_solution"] = False
                question_lines = current.get("question_lines")
                if isinstance(question_lines, list):
                    question_lines.append(line)
                else:
                    current["question_lines"] = [line]
        flush_current()

        out: list[dict[str, Any]] = []
        for index, block in enumerate(blocks, start=1):
            options_map = block.get("options")
            options_list: list[dict[str, str]] = []
            if isinstance(options_map, dict):
                for key in sorted(options_map.keys()):
                    value = self._str(options_map.get(key))
                    if not value:
                        continue
                    options_list.append(
                        {
                            "label": self._extract_import_label_token(key)
                            or self._normalize_import_option_label("", len(options_list)),
                            "text": value,
                        }
                    )
            answer_hint = self._str(block.get("answer_hint"))
            question_no = self._to_int(block.get("question_no"), 0)
            global_key_hint = ""
            if question_no > 0:
                global_key_hint = self._str(answer_key_map.get(question_no))
            if not global_key_hint:
                global_key_hint = self._str(answer_key_map.get(index))
            inline_solution = re.sub(
                r"\s+",
                " ",
                " ".join(
                    self._str(x).strip()
                    for x in (block.get("solution_lines") or [])
                    if self._str(x).strip()
                ),
            ).strip()
            global_solution_hint = ""
            if question_no > 0:
                global_solution_hint = self._str(solution_key_map.get(question_no))
            if not global_solution_hint:
                global_solution_hint = self._str(solution_key_map.get(index))
            solution_explanation = inline_solution or global_solution_hint
            if (
                inline_solution
                and global_solution_hint
                and global_solution_hint.lower() not in inline_solution.lower()
            ):
                solution_explanation = f"{inline_solution} {global_solution_hint}".strip()
            if solution_explanation:
                solution_explanation = solution_explanation[:2400]
            answer_fill_source = "manual"
            if answer_hint.strip():
                answer_fill_source = "inline_hint"
            elif global_key_hint.strip():
                answer_fill_source = "global_answer_key"
            if global_key_hint:
                answer_hint = (
                    f"{answer_hint}; {global_key_hint}" if answer_hint else global_key_hint
                )
            labels, numerical = self._parse_import_answer_hint(
                answer_hint=answer_hint,
                options=options_list,
            )
            question_text = self._str(block.get("question_text"))
            bag = f"{self._str(block.get('section_instruction'))} {question_text}".lower()
            question_type = "MCQ_SINGLE"
            if any(
                marker in bag
                for marker in (
                    "select all correct",
                    "more than one correct",
                    "multiple correct",
                    "multi correct",
                    "one or more options may be correct",
                )
            ):
                question_type = "MCQ_MULTI"
            elif any(
                marker in bag
                for marker in (
                    "integer type",
                    "numerical answer type",
                    "enter the correct value",
                    "answer in",
                    "answer upto",
                    "answer up to",
                    "numerical value",
                    "decimal places",
                )
            ):
                question_type = "NUMERICAL"

            if not options_list and (numerical or self._looks_numeric_import_prompt(question_text)):
                question_type = "NUMERICAL"
            if labels and options_list:
                question_type = (
                    "MCQ_MULTI"
                    if question_type == "MCQ_MULTI" or len(labels) > 1
                    else "MCQ_SINGLE"
                )
            elif numerical:
                question_type = "NUMERICAL"

            candidate = {
                "question_id": f"imp_q_{index}",
                "type": question_type,
                "question_text": question_text,
                "options": options_list if question_type != "NUMERICAL" else [],
                "correct_answer": {
                    "single": labels[0] if labels else None,
                    "multiple": labels,
                    "numerical": numerical or None,
                },
                "subject": meta_defaults.get("subject", ""),
                "chapter": meta_defaults.get("chapter", ""),
                "difficulty": meta_defaults.get("difficulty", ""),
                "ai_confidence": 0.0,
                "validation_status": "review",
                "validation_errors": [],
                "answer_hint": answer_hint,
                "answer_key_hint": global_key_hint,
                "answer_fill_source": answer_fill_source,
            }
            if solution_explanation:
                candidate["solution_explanation"] = solution_explanation
                candidate["source_solution_stub"] = solution_explanation[:420]
            normalized, _ = self._validate_and_normalize_import_question(
                row=candidate,
                index=index,
                meta_defaults=meta_defaults,
            )
            out.append(normalized)
        return out

    async def _lc9_parse_import_questions(self, payload: dict[str, Any]) -> dict[str, Any]:
        meta = self._import_meta_from_payload(payload)
        raw_text, input_report = await self._resolve_import_input_text(payload)
        if not isinstance(input_report, dict):
            input_report = {}
        if "symbol_repair_count" not in input_report:
            input_report["symbol_repair_count"] = self._count_symbol_font_artifacts(raw_text)
        fusion_mode = self._to_bool(
            payload.get("web_ocr_fusion_mode")
            or payload.get("simultaneous_web_ocr_mode")
            or payload.get("web_ocr_parallel_mode")
            or payload.get("question_paper_mode")
            or payload.get("import_question_paper")
            or payload.get("pre_ocr_web_search")
            or payload.get("web_lookup_before_ocr")
        )
        web_only_mode = self._to_bool(
            payload.get("web_ocr_fusion_web_only")
            or payload.get("question_paper_web_only_mode")
            or payload.get("pre_ocr_web_only")
        )
        if not raw_text and not fusion_mode:
            return {
                "ok": False,
                "status": "MISSING_RAW_TEXT",
                "message": "raw_text/pdf input is required",
                "questions": [],
                "input_report": input_report,
            }

        parsed_questions: list[dict[str, Any]] = []
        web_lookup: dict[str, Any] = {
            "enabled": False,
            "questions": [],
            "diagnostics": {},
            "web_error_reason": "",
        }
        parse_task = (
            asyncio.to_thread(self._parse_import_raw_text, raw_text, meta_defaults=meta)
            if raw_text
            else None
        )
        web_task = (
            asyncio.to_thread(
                self._import_pre_ocr_web_lookup,
                payload=payload,
                raw_text=raw_text,
                meta_defaults=meta,
            )
            if fusion_mode
            else None
        )
        if parse_task is not None and web_task is not None:
            parsed_questions, web_lookup = await asyncio.gather(parse_task, web_task)
        elif parse_task is not None:
            parsed_questions = await parse_task
        elif web_task is not None:
            web_lookup = await web_task

        for row in parsed_questions:
            if not self._str(row.get("source_origin")):
                row["source_origin"] = "ocr_parsed"
        web_questions = (
            [dict(x) for x in (web_lookup.get("questions") or []) if isinstance(x, dict)]
            if isinstance(web_lookup, dict)
            else []
        )

        if web_only_mode and not web_questions:
            return {
                "ok": False,
                "status": "NO_WEB_MATCH_FOUND",
                "message": "No verified web question match found in simultaneous web-ocr fusion mode",
                "questions": [],
                "meta": meta,
                "web_error_reason": self._str(web_lookup.get("web_error_reason")),
                "web_provider_diagnostics": web_lookup.get("diagnostics") or {},
                "input_report": input_report,
                "fusion_report": {
                    "mode": "simultaneous_web_ocr_fusion",
                    "web_enabled": self._to_bool(web_lookup.get("enabled")),
                    "web_count": 0,
                    "raw_count": len(parsed_questions),
                    "combined_count": 0,
                    "web_only_mode": True,
                },
            }

        questions = await self._fuse_import_question_groups(
            web_questions=web_questions,
            parsed_questions=parsed_questions,
            payload=payload,
            meta_defaults=meta,
        )
        requested_count = self._to_int(
            payload.get("target_question_count")
            or payload.get("question_count")
            or payload.get("expected_questions"),
            0,
        )
        if requested_count > 0:
            questions = questions[:requested_count]
        if not questions:
            return {
                "ok": False,
                "status": "NO_QUESTIONS_PARSED",
                "message": "No questions could be extracted from web+ocr fusion pipeline",
                "questions": [],
                "meta": meta,
                "web_error_reason": self._str(web_lookup.get("web_error_reason")),
                "web_provider_diagnostics": web_lookup.get("diagnostics") or {},
                "input_report": input_report,
            }
        summary = {"valid": 0, "review": 0, "invalid": 0}
        source_counts = {"web_fusion": 0, "ocr_parsed": 0, "other": 0}
        for row in questions:
            if not self._str(row.get("question_text_latex")):
                row["question_text_latex"] = self._canonical_import_latex(
                    self._str(row.get("question_text"))
                )
            options_raw = row.get("options") if isinstance(row.get("options"), list) else []
            if not isinstance(row.get("options_latex"), dict):
                row["options_latex"] = {
                    self._str(opt.get("label")).upper(): self._canonical_import_latex(
                        self._str(opt.get("text"))
                    )
                    for opt in options_raw
                    if isinstance(opt, dict)
                }
            if not self._str(row.get("answer_fill_source")):
                source_origin = self._str(row.get("source_origin")).lower()
                if source_origin.startswith("web_") or source_origin in {"fusion_verified", "web_verified"}:
                    row["answer_fill_source"] = "web_verified"
                else:
                    row["answer_fill_source"] = "manual"
            row["publish_risk_score"] = self._import_publish_risk_score(row)
            status = self._str(row.get("validation_status")).lower()
            if status == "valid":
                summary["valid"] += 1
            elif status == "invalid":
                summary["invalid"] += 1
            else:
                summary["review"] += 1
            source_origin = self._str(row.get("source_origin")).lower()
            if source_origin.startswith("web_") or source_origin == "fusion_verified":
                source_counts["web_fusion"] += 1
            elif source_origin == "ocr_parsed":
                source_counts["ocr_parsed"] += 1
            else:
                source_counts["other"] += 1
        quality_dashboard = self._build_import_quality_dashboard(
            questions=questions,
            input_report=input_report,
            summary=summary,
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "questions": questions,
            "count": len(questions),
            "summary": summary,
            "quality_dashboard": quality_dashboard,
            "meta": meta,
            "input_report": input_report,
            "fusion_report": {
                "mode": (
                    "simultaneous_web_ocr_fusion"
                    if fusion_mode
                    else "raw_parse_only"
                ),
                "web_enabled": self._to_bool(web_lookup.get("enabled")),
                "web_count": len(web_questions),
                "raw_count": len(parsed_questions),
                "combined_count": len(questions),
                "source_counts": source_counts,
                "web_only_mode": web_only_mode,
                "seed_count": self._to_int(web_lookup.get("seed_count"), 0),
                "candidate_count": self._to_int(web_lookup.get("candidate_count"), 0),
                "matched_count": self._to_int(web_lookup.get("matched_count"), 0),
            },
            "web_error_reason": self._str(web_lookup.get("web_error_reason")),
            "web_provider_diagnostics": web_lookup.get("diagnostics") or {},
        }

    def _invalid_import_response(self, invalid: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "ok": False,
            "status": "INVALID_IMPORT_QUESTIONS",
            "message": "One or more import questions are invalid",
            "invalid": invalid,
        }

    async def _lc9_save_import_drafts(self, payload: dict[str, Any]) -> dict[str, Any]:
        questions, invalid, meta = self._coerce_import_questions(payload)
        if not questions:
            return {
                "ok": False,
                "status": "MISSING_QUESTIONS",
                "message": "questions are required",
            }
        if invalid:
            return self._invalid_import_response(invalid)

        draft_id = self._new_id("impdraft")
        now_ms = self._now_ms()
        row = {
            "id": draft_id,
            "draft_id": draft_id,
            "meta": meta,
            "questions": questions,
            "question_count": len(questions),
            "published": False,
            "created_at": now_ms,
            "updated_at": now_ms,
        }
        async with self._lock:
            self._import_drafts.append(row)
            self._write_list(self._import_drafts_file, self._import_drafts)

        return {
            "ok": True,
            "status": "SUCCESS",
            "draft_id": draft_id,
            "saved_count": len(questions),
            "meta": meta,
            "questions": questions,
        }

    async def _lc9_publish_import_questions(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        questions, invalid, meta = self._coerce_import_questions(payload)
        if not questions:
            return {
                "ok": False,
                "status": "MISSING_QUESTIONS",
                "message": "questions are required",
            }
        if invalid:
            return self._invalid_import_response(invalid)

        gate_profile = self._str(
            payload.get("publish_gate_profile") or "legacy"
        ).strip().lower()
        if gate_profile not in {"legacy", "strict_critical_only", "strict"}:
            gate_profile = "legacy"
        fix_suggestions_applied = self._to_bool(payload.get("fix_suggestions_applied"))
        fix_report: dict[str, Any] = {
            "applied": fix_suggestions_applied,
            "changed_count": 0,
        }
        if fix_suggestions_applied:
            fixed_rows: list[dict[str, Any]] = []
            changed_count = 0
            for index, row in enumerate(questions, start=1):
                candidate = dict(row)
                changed = False
                options = candidate.get("options") if isinstance(candidate.get("options"), list) else []
                if options:
                    relabeled = []
                    for i, option in enumerate(options):
                        if isinstance(option, dict):
                            text = self._str(option.get("text"))
                        else:
                            text = self._str(option)
                        label = chr(ord("A") + i)
                        relabeled.append({"label": label, "text": text})
                    if relabeled != options:
                        candidate["options"] = relabeled
                        changed = True
                ans = candidate.get("correct_answer") if isinstance(candidate.get("correct_answer"), dict) else {}
                if candidate.get("type") in {"MCQ_SINGLE", "MCQ_MULTI"}:
                    multiple = [
                        self._extract_import_label_token(x)
                        for x in self._to_list_str(ans.get("multiple"))
                    ]
                    multiple = [x for x in multiple if x]
                    single = self._extract_import_label_token(ans.get("single"))
                    if not single and multiple:
                        single = multiple[0]
                        changed = True
                    candidate["correct_answer"] = {
                        "single": single or None,
                        "multiple": multiple,
                        "numerical": None,
                        "tolerance": ans.get("tolerance"),
                    }
                if changed:
                    changed_count += 1
                normalized, _ = self._validate_and_normalize_import_question(
                    row=candidate,
                    index=index,
                    meta_defaults=meta,
                )
                errors = [
                    self._str(x)
                    for x in (normalized.get("validation_errors") or [])
                    if self._str(x)
                ]
                note = "Fix suggestions applied before publish."
                if note not in errors:
                    errors.append(note)
                normalized["validation_errors"] = errors
                normalized["fix_suggestions_applied"] = True
                normalized["publish_risk_score"] = self._import_publish_risk_score(normalized)
                fixed_rows.append(normalized)
            questions = fixed_rows
            fix_report["changed_count"] = changed_count

        gate_critical: list[dict[str, Any]] = []
        gate_review: list[dict[str, Any]] = []
        for index, row in enumerate(questions, start=1):
            qid = self._str(row.get("question_id")) or f"imp_q_{index}"
            status = self._str(row.get("validation_status")).lower()
            issues = [
                self._str(x)
                for x in (row.get("validation_errors") or [])
                if self._str(x)
            ]
            if status == "invalid":
                gate_critical.append(
                    {
                        "question_id": qid,
                        "issue": issues[0] if issues else "Question marked invalid.",
                        "severity": "critical",
                    }
                )
                continue
            if not issues and status == "review":
                gate_review.append(
                    {
                        "question_id": qid,
                        "issue": "Review-required item needs teacher acknowledgement.",
                        "severity": "review",
                    }
                )
            for issue in issues:
                severity = self._import_issue_severity(issue)
                bucket = gate_critical if severity == "critical" else gate_review
                bucket.append(
                    {
                        "question_id": qid,
                        "issue": issue,
                        "severity": severity,
                    }
                )

        gate_risk_values = [self._import_publish_risk_score(row) for row in questions]
        publish_gate = {
            "profile": gate_profile,
            "critical_count": len(gate_critical),
            "review_count": len(gate_review),
            "critical": gate_critical[:80],
            "review": gate_review[:120],
            "fix_suggestions": dict(fix_report),
            "publish_risk_score": round(
                sum(gate_risk_values) / max(1, len(gate_risk_values)),
                6,
            ),
            "one_tap_action": {"fix_suggestions_applied": True},
        }
        if gate_critical:
            return {
                "ok": False,
                "status": "PUBLISH_GATE_BLOCKED",
                "message": "Publish gate blocked due to critical validation errors.",
                "publish_gate": publish_gate,
            }
        if gate_profile in {"strict", "strict_critical_only"} and gate_review and not fix_suggestions_applied:
            return {
                "ok": False,
                "status": "PUBLISH_GATE_REVIEW_CONFIRMATION_REQUIRED",
                "message": (
                    "Review-level items detected. Tap once to apply fix suggestions and publish."
                ),
                "publish_gate": publish_gate,
            }

        existing_by_key: dict[str, dict[str, Any]] = {}
        for row in self._import_question_bank:
            q_text = self._str(row.get("question_text"))
            if not q_text:
                continue
            key = re.sub(r"\s+", " ", q_text).strip().lower()
            if key:
                existing_by_key.setdefault(key, row)
        deduped_questions: list[dict[str, Any]] = []
        duplicate_count = 0
        solutions_enriched_count = 0
        chapter_reclassified_count = 0
        solution_enriched_at = self._now_ms()
        for row in questions:
            q_text = self._str(row.get("question_text"))
            key = re.sub(r"\s+", " ", q_text).strip().lower() if q_text else ""
            if key and key in existing_by_key:
                duplicate_count += 1
                existing_row = existing_by_key.get(key) or {}
                existing_solution = self._str(
                    existing_row.get("solution_explanation")
                    or existing_row.get("source_solution_stub")
                    or existing_row.get("solution")
                )
                incoming_solution = self._str(
                    row.get("solution_explanation")
                    or row.get("source_solution_stub")
                    or row.get("solution")
                )
                if incoming_solution and not existing_solution:
                    existing_row["solution_explanation"] = incoming_solution[:2400]
                    incoming_stub = self._str(
                        row.get("source_solution_stub") or row.get("solution_stub")
                    )
                    if incoming_stub:
                        existing_row["source_solution_stub"] = incoming_stub[:420]
                    existing_row["has_solution"] = True
                    existing_row["solution_enriched_at"] = solution_enriched_at
                    solutions_enriched_count += 1

                # Reclassify chapter tags on duplicate merges so legacy "mixed"
                # entries are progressively mapped to syllabus chapters.
                existing_subject = self._str(
                    existing_row.get("subject") or row.get("subject") or meta.get("subject")
                ).strip() or "Mathematics"
                existing_chapter = self._str(existing_row.get("chapter"))
                merged_probe = dict(existing_row)
                if incoming_solution and not existing_solution:
                    merged_probe["solution_explanation"] = incoming_solution[:2400]
                if not isinstance(merged_probe.get("options"), list) and isinstance(row.get("options"), list):
                    merged_probe["options"] = row.get("options")
                inferred_chapter = self._resolve_import_row_chapter(
                    merged_probe,
                    subject_override=existing_subject,
                    chapter_override=existing_chapter,
                )
                inferred_tags = self._resolve_import_row_chapter_tags(
                    merged_probe,
                    subject_override=existing_subject,
                    chapter_override=inferred_chapter or existing_chapter,
                    max_tags=3,
                )
                changed_chapter = False
                if inferred_chapter and inferred_chapter != existing_chapter:
                    existing_row["chapter"] = inferred_chapter
                    changed_chapter = True
                if inferred_tags:
                    previous_tags = [
                        self._str(x)
                        for x in (existing_row.get("chapter_tags") or [])
                        if self._str(x)
                    ]
                    if previous_tags != inferred_tags[:3]:
                        existing_row["chapter_tags"] = inferred_tags[:3]
                        changed_chapter = True
                if changed_chapter:
                    chapter_reclassified_count += 1
                continue
            if key:
                existing_by_key[key] = row
            deduped_questions.append(row)
        questions = deduped_questions
        if not questions:
            if solutions_enriched_count > 0 or chapter_reclassified_count > 0:
                async with self._lock:
                    self._write_list(self._import_question_bank_file, self._import_question_bank)
            return {
                "ok": True,
                "status": "NO_NEW_QUESTIONS",
                "publish_id": "",
                "published_count": 0,
                "duplicates_skipped": duplicate_count,
                "solutions_enriched_count": solutions_enriched_count,
                "chapter_reclassified_count": chapter_reclassified_count,
                "question_bank_total": len(self._import_question_bank),
                "meta": meta,
                "publish_gate": publish_gate,
                "quality_dashboard": self._build_import_quality_dashboard(
                    questions=[],
                    input_report={"symbol_repair_count": self._to_int(payload.get("symbol_repair_count"), 0)},
                    summary={"valid": 0, "review": 0, "invalid": 0},
                ),
            }

        publish_id = self._new_id("imppub")
        now_ms = self._now_ms()
        published_rows: list[dict[str, Any]] = []
        teacher_id = self._str(meta.get("teacher_id"))
        for idx, question in enumerate(questions, start=1):
            question_id = self._safe_id(question.get("question_id")) or f"imp_q_{idx}"
            stored = dict(question)
            stored["id"] = self._new_id("bankq")
            stored["question_id"] = question_id
            stored["source"] = "teacher_import"
            stored["teacher_id"] = teacher_id
            stored["publish_id"] = publish_id
            stored["published_at"] = now_ms
            published_rows.append(stored)

        draft_snapshot = {
            "id": self._new_id("impdraft"),
            "draft_id": self._new_id("impdraft_ref"),
            "meta": meta,
            "questions": questions,
            "question_count": len(questions),
            "published": True,
            "publish_id": publish_id,
            "created_at": now_ms,
            "updated_at": now_ms,
        }
        async with self._lock:
            self._import_question_bank.extend(published_rows)
            self._import_drafts.append(draft_snapshot)
            self._write_list(self._import_question_bank_file, self._import_question_bank)
            self._write_list(self._import_drafts_file, self._import_drafts)

        return {
            "ok": True,
            "status": "SUCCESS",
            "publish_id": publish_id,
            "published_count": len(published_rows),
            "duplicates_skipped": duplicate_count,
            "solutions_enriched_count": solutions_enriched_count,
            "chapter_reclassified_count": chapter_reclassified_count,
            "question_bank_total": len(self._import_question_bank),
            "meta": meta,
            "publish_gate": publish_gate,
            "quality_dashboard": self._build_import_quality_dashboard(
                questions=questions,
                input_report={"symbol_repair_count": self._to_int(payload.get("symbol_repair_count"), 0)},
                summary={
                    "valid": sum(
                        1
                        for row in questions
                        if self._str(row.get("validation_status")).lower() == "valid"
                    ),
                    "review": sum(
                        1
                        for row in questions
                        if self._str(row.get("validation_status")).lower() == "review"
                    ),
                    "invalid": 0,
                },
            ),
        }

    async def _lc9_web_verify_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        query = self._str(payload.get("query") or payload.get("q"))
        if not query:
            return {
                "ok": False,
                "status": "MISSING_QUERY",
                "message": "query is required",
                "rows": [],
            }
        max_rows = max(1, min(30, self._to_int(payload.get("max_rows"), 8)))
        timeout_s = max(0.8, min(8.0, self._to_float(payload.get("timeout_s"), 3.4)))
        search_scope = self._str(
            payload.get("search_scope") or payload.get("source_scope") or "pyq"
        ).strip().lower() or "pyq"
        rows, diagnostics = await asyncio.to_thread(
            self._search_rows_with_provider_fallback,
            query,
            max_rows=max_rows,
            search_scope=search_scope,
            total_timeout_s=timeout_s,
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "query": query,
            "rows": rows,
            "count": len(rows),
            "diagnostics": diagnostics,
            "cache_ttl_s": self._web_cache_ttl_s,
            "search_scope": search_scope,
            "timeout_s": timeout_s,
        }

    async def _lc9_list_import_chapters(self, payload: dict[str, Any]) -> dict[str, Any]:
        subject_filter = self._str(payload.get("subject")).strip()
        teacher_id_filter = self._str(payload.get("teacher_id")).strip()
        min_count = max(1, min(5000, self._to_int(payload.get("min_count"), 1)))
        include_generic = self._to_bool(
            payload.get("include_generic") or payload.get("include_mixed")
        )
        requested_track = self._infer_subject_track(
            subject=subject_filter or "Mathematics",
            chapters=[],
            subtopics=[],
            concept_tags=[],
        )

        subject_counts: dict[str, dict[str, int]] = {}
        scanned = 0
        for row in self._import_question_bank:
            if not isinstance(row, dict):
                continue
            scanned += 1
            if teacher_id_filter:
                if self._str(row.get("teacher_id")).strip() != teacher_id_filter:
                    continue
            row_subject = self._str(row.get("subject") or "Mathematics").strip()
            row_track = self._infer_subject_track(
                subject=row_subject or "Mathematics",
                chapters=[],
                subtopics=[],
                concept_tags=[],
            )
            if subject_filter and row_track != requested_track:
                continue
            raw_tags = row.get("chapter_tags")
            chapter_tags = (
                [self._str(x).strip() for x in raw_tags if self._str(x).strip()]
                if isinstance(raw_tags, list)
                else []
            )
            if not chapter_tags:
                chapter_tags = self._resolve_import_row_chapter_tags(
                    row=row,
                    subject_override=row_subject,
                    max_tags=3,
                )
            if not chapter_tags:
                continue
            bucket = subject_counts.setdefault(row_track, {})
            for chapter in list(dict.fromkeys(chapter_tags))[:3]:
                if not include_generic and self._import_chapter_is_generic(chapter):
                    continue
                bucket[chapter] = bucket.get(chapter, 0) + 1

        subject_chapters: dict[str, list[dict[str, Any]]] = {}
        subject_chapter_map: dict[str, list[str]] = {}
        for subject_name, counts in sorted(subject_counts.items()):
            rows = [
                {"chapter": chapter, "count": count}
                for chapter, count in counts.items()
                if count >= min_count
            ]
            rows.sort(
                key=lambda item: (
                    -self._to_int(item.get("count"), 0),
                    self._str(item.get("chapter")).lower(),
                )
            )
            if not rows:
                continue
            subject_chapters[subject_name] = rows
            subject_chapter_map[subject_name] = [
                self._str(item.get("chapter")) for item in rows if self._str(item.get("chapter"))
            ]

        if subject_filter and not subject_chapters:
            catalog, _ = self._jee_chapter_catalog(subject=requested_track)
            if catalog:
                subject_chapter_map[requested_track] = catalog[:]
                subject_chapters[requested_track] = [
                    {"chapter": chapter, "count": 0} for chapter in catalog
                ]

        return {
            "ok": True,
            "status": "SUCCESS",
            "subject_filter": subject_filter,
            "teacher_id_filter": teacher_id_filter,
            "min_count": min_count,
            "include_generic": include_generic,
            "rows_scanned": scanned,
            "subject_chapters": subject_chapters,
            "subject_chapter_map": subject_chapter_map,
            "subjects": sorted(subject_chapter_map.keys()),
        }

    def _route_engine_mode(
        self, difficulty: int, trap_intensity: str, cross_concept: bool
    ) -> str:
        if difficulty >= 4 or trap_intensity == "high" or cross_concept:
            return "ELITE_MODE"
        if difficulty <= 2:
            return "FAST_MODE"
        return "BALANCED_MODE"

    def _model_profile(self, engine_mode: str) -> dict[str, Any]:
        if engine_mode == "FAST_MODE":
            return {
                "provider_count": 1,
                "validation_passes": 1,
                "checks": [
                    "basic_numeric_verification",
                    "option_uniqueness_check",
                ],
            }
        if engine_mode == "BALANCED_MODE":
            return {
                "provider_count": 2,
                "validation_passes": 2,
                "checks": [
                    "cross_solve_validation",
                    "ambiguity_filtering",
                    "numeric_stability",
                ],
            }
        return {
            "provider_count": 3,
            "validation_passes": 3,
            "checks": [
                "arena_disagreement_detection",
                "deterministic_verification_engine",
                "symbolic_equivalence_check",
                "regenerate_on_instability",
                "difficulty_recalibration",
            ],
        }

    def _is_mathlike_subject(self, subject: str) -> bool:
        lowered = subject.lower()
        keys = (
            "math",
            "physics",
            "chem",
            "jee",
            "algebra",
            "calculus",
            "trigonometry",
            "mechanics",
            "electro",
        )
        return any(k in lowered for k in keys)

    def _seeded_random(self, seed_text: str) -> random.Random:
        digest = hashlib.sha256(seed_text.encode("utf-8")).hexdigest()
        return random.Random(int(digest[:16], 16))

    def _sign(self, value: int) -> str:
        return "+" if value >= 0 else "-"

    def _ncr(self, n: int, r: int) -> int:
        if n < 0 or r < 0 or r > n:
            return 0
        return math.comb(n, r)

    def _fact(self, n: int) -> int:
        if n < 0:
            return 0
        return math.factorial(n)

    def _is_permutation_combination_context(
        self,
        *,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
        concept_tags: list[str],
    ) -> bool:
        haystack = " ".join(
            [subject, *chapters, *subtopics, *concept_tags]
        ).lower()
        keywords = (
            "permutation",
            "combination",
            "combinatorics",
            "arrangement",
            "selection",
            "ncr",
            "npr",
            "circular",
            "derangement",
            "inclusion-exclusion",
            "stars and bars",
            "p&c",
            "p & c",
        )
        return any(k in haystack for k in keywords)

    def _finalize_numeric_options(
        self,
        *,
        correct_value: int,
        distractors: list[int],
        rng: random.Random,
    ) -> tuple[list[str], str]:
        picks: list[int] = []

        def add(v: int) -> None:
            if v <= 0:
                return
            if v not in picks:
                picks.append(v)

        add(int(correct_value))
        for d in distractors:
            add(int(d))

        spin = 0
        while len(picks) < 4 and spin < 64:
            span = max(3, abs(correct_value) // 6 + 2)
            candidate = correct_value + rng.choice((-1, 1)) * rng.randint(1, span)
            if candidate <= 0:
                candidate = abs(candidate) + rng.randint(2, 9)
            add(int(candidate))
            spin += 1

        if len(picks) < 4:
            # Last-resort deterministic fillers.
            base = max(2, abs(correct_value))
            while len(picks) < 4:
                add(base + len(picks) + 3)

        option_values = [str(v) for v in picks[:4]]
        rng.shuffle(option_values)
        correct_text = str(int(correct_value))
        if correct_text not in option_values:
            option_values[0] = correct_text
        correct_index = option_values.index(correct_text)
        return [f"${x}$" for x in option_values], "ABCD"[correct_index]

    def _finalize_text_options(
        self,
        *,
        correct_text: str,
        distractors: list[str],
        rng: random.Random,
    ) -> tuple[list[str], str]:
        picked: list[str] = []
        seen: set[str] = set()

        def add(text: str) -> None:
            cleaned = self._str(text)
            if not cleaned:
                return
            key = cleaned.lower()
            if key in seen:
                return
            seen.add(key)
            picked.append(cleaned)

        add(correct_text)
        for row in distractors:
            add(row)

        while len(picked) < 4:
            add(f"Insufficient condition {len(picked) + 1}")

        options = picked[:4]
        rng.shuffle(options)
        if correct_text not in options:
            options[0] = correct_text
        correct_index = options.index(correct_text)
        return options, "ABCD"[correct_index]

    def _to_stepwise_solution(self, explanation: str) -> str:
        text = self._str(explanation).strip()
        if not text:
            return (
                "Step 1: Identify the target quantity and constraints from the question. "
                "Step 2: Set up the governing formula or relation for the chapter. "
                "Step 3: Simplify/compute carefully with valid algebraic steps. "
                "Step 4: Therefore, state the final verified answer."
            )
        text = re.sub(
            r"(?i)(step\s*\d+\s*:\s*)(step\s*\d+\s*:\s*)+",
            r"\1",
            text,
        )

        def clean_fragment(raw: str) -> str:
            frag = self._str(raw)
            frag = re.sub(r"(?i)^step\s*\d+\s*:\s*", "", frag).strip()
            frag = re.sub(r"^\s*[-*•]\s*", "", frag).strip()
            frag = re.sub(r"\s+", " ", frag).strip()
            return frag

        def extract_rhs(candidate: str) -> str:
            eq = re.findall(r"=\s*([^=;,.]+)", candidate)
            if not eq:
                return ""
            rhs = self._str(eq[-1]).strip().strip("$").strip()
            if not rhs:
                return ""
            if len(rhs) > 80:
                return ""
            return rhs

        chunks = [
            clean_fragment(part)
            for part in re.split(r"\n+|;\s+|(?<=[.])\s+", text)
            if clean_fragment(part)
        ]
        if not chunks:
            chunks = [clean_fragment(text)]

        has_step_markers = bool(re.search(r"(?i)\bstep\s*1\b", text))
        rhs = extract_rhs(" ".join(chunks))
        rhs_fmt = ""
        if rhs:
            rhs_fmt = rhs if (rhs.startswith("$") and rhs.endswith("$")) else f"${rhs}$"

        if has_step_markers:
            renumbered = [f"Step {i + 1}: {chunk}" for i, chunk in enumerate(chunks[:6])]
            if len(renumbered) < 4:
                while len(renumbered) < 3:
                    renumbered.append(
                        f"Step {len(renumbered) + 1}: Simplify the expression carefully."
                    )
                final_line = (
                    f"Step {len(renumbered) + 1}: Therefore, the required answer is {rhs_fmt}."
                    if rhs_fmt
                    else f"Step {len(renumbered) + 1}: Therefore, state the final verified answer."
                )
                renumbered.append(final_line)
            return " ".join(renumbered[:6])

        steps: list[str] = [
            "Step 1: Identify the target quantity and all constraints from the question.",
            (
                f"Step 2: Set up the governing relation: {chunks[0]}"
                if chunks
                else "Step 2: Set up the governing relation using the standard formula."
            ),
        ]
        if len(chunks) >= 2:
            steps.append(f"Step 3: Simplify systematically: {chunks[1]}")
        else:
            steps.append(
                "Step 3: Expand/simplify carefully and keep algebraic consistency."
            )
        if len(chunks) >= 3:
            steps.append(f"Step 4: Apply the final substitution/check: {chunks[2]}")
        elif rhs_fmt:
            steps.append(f"Step 4: Therefore, the required answer is {rhs_fmt}.")
        else:
            steps.append(
                "Step 4: Perform final verification and state the required answer."
            )
        if rhs_fmt and not any("required answer is" in s.lower() for s in steps):
            steps.append(f"Step {len(steps) + 1}: Therefore, the required answer is {rhs_fmt}.")
        return " ".join(steps[:6])

    def _normalize_generated_question_text(self, text: str) -> str:
        out = self._str(text).strip()
        out = re.sub(
            r"(?i)^\s*for chapter\s*'[^']+'\s*,?\s*",
            "",
            out,
        )
        out = re.sub(r"(?i)^\s*for\s*'[^']+'\s*,?\s*", "", out)
        out = re.sub(r"(?i)^\s*in\s*'[^']+'\s*,?\s*", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        if not out:
            return ""
        return out[0].upper() + out[1:]

    def _infer_subject_track(
        self,
        *,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
        concept_tags: list[str] | None = None,
    ) -> str:
        tags = concept_tags or []
        bag = " ".join([subject, *chapters, *subtopics, *tags]).lower()
        biology_keys = (
            "biology",
            "living world",
            "plant",
            "animal kingdom",
            "cell",
            "biomolecule",
            "respiration",
            "reproduction",
            "inheritance",
            "evolution",
            "ecosystem",
            "biodiversity",
            "biotechnology",
            "human health",
        )
        chemistry_keys = (
            "chemistry",
            "atom",
            "periodicity",
            "bonding",
            "equilibrium",
            "redox",
            "electrochem",
            "kinetics",
            "coordination",
            "haloalkane",
            "aldehyde",
            "amine",
            "polymer",
            "solid state",
            "solutions",
            "surface chemistry",
            "hydrocarbon",
        )
        physics_keys = (
            "physics",
            "kinematic",
            "laws of motion",
            "work, energy",
            "rotational",
            "gravitation",
            "thermodynamics",
            "kinetic theory",
            "oscillation",
            "wave",
            "electrostatics",
            "current electricity",
            "magnetism",
            "electromagnetic",
            "optics",
            "dual nature",
            "nuclei",
            "semiconductor",
        )
        if any(k in bag for k in biology_keys):
            return "Biology"
        if any(k in bag for k in chemistry_keys):
            return "Chemistry"
        if any(k in bag for k in physics_keys):
            return "Physics"
        return "Mathematics"

    def _jee_chapter_catalog(self, *, subject: str) -> tuple[list[str], list[str]]:
        track = self._infer_subject_track(
            subject=subject,
            chapters=[],
            subtopics=[],
            concept_tags=[],
        )
        if track == "Mathematics":
            chapters = [
                "Sets, Relations and Functions",
                "Complex Numbers and Quadratic Equations",
                "Matrices and Determinants",
                "Permutations and Combinations",
                "Binomial Theorem",
                "Sequences and Series",
                "Trigonometric Functions",
                "Inverse Trigonometric Functions",
                "Limits, Continuity and Differentiability",
                "Application of Derivatives",
                "Integral Calculus",
                "Differential Equations",
                "Coordinate Geometry",
                "Three Dimensional Geometry",
                "Vector Algebra",
                "Probability and Statistics",
            ]
            subtopics = [
                "Domain and range",
                "Argand plane and roots",
                "Determinant and inverse",
                "Arrangement and selection",
                "General and middle terms",
                "AP GP HP",
                "Identities and equations",
                "Principal values",
                "Standard limits",
                "Maxima and minima",
                "Definite integral",
                "First order linear DE",
                "Circle and parabola",
                "Line and plane in 3D",
                "Dot and cross products",
                "Conditional probability",
            ]
            return chapters, subtopics
        if track == "Physics":
            chapters = [
                "Units and Dimensions",
                "Kinematics",
                "Laws of Motion",
                "Work, Energy and Power",
                "Rotational Motion",
                "Gravitation",
                "Properties of Matter",
                "Thermodynamics",
                "Kinetic Theory",
                "Oscillations and Waves",
                "Electrostatics",
                "Current Electricity",
                "Magnetic Effects of Current",
                "Electromagnetic Induction",
                "Alternating Current",
                "Ray Optics",
                "Wave Optics",
                "Dual Nature of Matter",
                "Atoms and Nuclei",
                "Semiconductor Electronics",
            ]
            subtopics = [
                "Dimensional analysis",
                "Relative motion",
                "Friction and constraints",
                "Work-energy theorem",
                "Moment of inertia",
                "Orbital motion",
                "Elasticity and fluid flow",
                "First law and cycles",
                "RMS speed",
                "SHM and superposition",
                "Potential and capacitance",
                "Kirchhoff laws",
                "Force on moving charge",
                "Lenz law",
                "RLC resonance",
                "Mirror and lens formula",
                "Interference and diffraction",
                "Photoelectric effect",
                "Bohr model",
                "Diodes and logic gates",
            ]
            return chapters, subtopics
        if track == "Chemistry":
            chapters = [
                "Some Basic Concepts of Chemistry",
                "Atomic Structure",
                "Periodic Classification",
                "Chemical Bonding",
                "States of Matter",
                "Thermodynamics",
                "Chemical Equilibrium",
                "Redox Reactions",
                "Hydrogen and s-Block",
                "p-Block Elements",
                "d and f Block Elements",
                "Coordination Compounds",
                "General Organic Chemistry",
                "Hydrocarbons",
                "Haloalkanes and Haloarenes",
                "Alcohols, Phenols and Ethers",
                "Aldehydes, Ketones and Carboxylic Acids",
                "Amines",
                "Biomolecules and Polymers",
                "Electrochemistry and Chemical Kinetics",
            ]
            subtopics = [
                "Mole concept",
                "Quantum numbers",
                "Periodic trends",
                "VSEPR and hybridization",
                "Gas laws",
                "Enthalpy and entropy",
                "Le Chatelier principle",
                "Oxidation number",
                "Hydrides",
                "Group trends",
                "Transition elements",
                "Crystal field theory",
                "Reaction mechanism basics",
                "Alkanes alkenes alkynes",
                "Substitution and elimination",
                "Acidity and basicity",
                "Carbonyl chemistry",
                "Diazotization and basicity",
                "Proteins and polymers",
                "Nernst and rate law",
            ]
            return chapters, subtopics
        return [], []

    def _domain_key_from_context(
        self,
        *,
        subject: str,
        concept_tags: list[str],
    ) -> str:
        text = " ".join([subject, *concept_tags]).lower()
        track = self._infer_subject_track(
            subject=subject,
            chapters=concept_tags,
            subtopics=concept_tags,
            concept_tags=concept_tags,
        )

        if any(
            k in text
            for k in (
                "binomial",
                "coefficient",
                "general term",
                "middle term",
                "greatest coefficient",
                "constant term",
                "expansion",
                "pascal",
                "(1+x)^",
            )
        ):
            return "math_binomial"

        if self._is_permutation_combination_context(
            subject=subject,
            chapters=concept_tags,
            subtopics=concept_tags,
            concept_tags=concept_tags,
        ):
            return "math_combinatorics"

        if track == "Mathematics":
            if any(k in text for k in ("trigon", "inverse trig")):
                return "math_trigonometry"
            if any(k in text for k in ("probability", "statistics")):
                return "math_probability"
            if any(
                k in text
                for k in (
                    "vector",
                    "vector algebra",
                    "3d geometry",
                    "3-dimensional",
                    "three dimensional",
                    "line in 3d",
                    "plane in 3d",
                    "direction ratio",
                    "direction cosine",
                    "skew line",
                )
            ):
                return "math_vector3d"
            if any(
                k in text
                for k in ("straight line", "circle", "conic", "coordinate")
            ):
                return "math_coordinate"
            if any(
                k in text
                for k in (
                    "limit",
                    "continuity",
                    "differentiab",
                    "derivative",
                    "integral",
                    "differential equation",
                    "application of derivatives",
                )
            ):
                return "math_calculus"
            if any(
                k in text
                for k in (
                    "relation",
                    "relations and functions",
                    "set",
                    "reasoning",
                    "quadratic",
                    "complex",
                    "sequence",
                    "series",
                    "binomial",
                    "matrix",
                    "determinant",
                    "linear programming",
                )
            ):
                return "math_algebra"
            return "math_algebra"

        if track == "Physics":
            if any(
                k in text
                for k in (
                    "kinematic",
                    "laws of motion",
                    "work, energy",
                    "work energy",
                    "rotational",
                    "gravitation",
                )
            ):
                return "physics_mechanics"
            if any(
                k in text
                for k in (
                    "thermal",
                    "thermodynamics",
                    "kinetic theory",
                )
            ):
                return "physics_thermal"
            if any(k in text for k in ("oscillation", "waves", "wave")):
                return "physics_waves"
            if any(
                k in text
                for k in ("electrostatics", "current electricity", "electromagnetic")
            ):
                return "physics_electric"
            if any(k in text for k in ("magnetism", "moving charges", "induction", "alternating current")):
                return "physics_magnetism"
            if any(k in text for k in ("optics",)):
                return "physics_optics"
            return "physics_modern"

        if track == "Chemistry":
            if any(
                k in text
                for k in (
                    "basic concepts",
                    "thermodynamics",
                    "equilibrium",
                    "states of matter",
                    "solutions",
                    "solid state",
                    "chemical kinetics",
                )
            ):
                return "chemistry_physical"
            if any(
                k in text
                for k in (
                    "electrochem",
                    "redox",
                )
            ):
                return "chemistry_electro"
            if any(
                k in text
                for k in (
                    "organic",
                    "hydrocarbon",
                    "haloalkane",
                    "alcohol",
                    "aldehyde",
                    "amine",
                    "biomolecules",
                    "polymer",
                )
            ):
                return "chemistry_organic"
            return "chemistry_inorganic"

        # Biology
        if any(
            k in text
            for k in (
                "inheritance",
                "variation",
                "molecular basis",
                "evolution",
                "reproduction",
            )
        ):
            return "biology_genetics"
        if any(
            k in text
            for k in (
                "ecosystem",
                "population",
                "biodiversity",
                "environment",
            )
        ):
            return "biology_ecology"
        return "biology_cell"

    def _requested_generation_type(self, raw: Any) -> str:
        token = self._str(raw)
        if not token:
            return ""
        return self._canonical_question_type(token)

    def _target_generation_type(
        self,
        *,
        idx: int,
        forced_type: str,
        require_type_variety: bool,
        seed_hint: str = "",
    ) -> str:
        if forced_type:
            return forced_type
        if not require_type_variety:
            return "MCQ_SINGLE"
        archetypes = (
            ("MCQ_SINGLE", "MCQ_MULTI", "NUMERICAL"),
            ("MCQ_SINGLE", "NUMERICAL", "MCQ_MULTI"),
            ("MCQ_MULTI", "MCQ_SINGLE", "NUMERICAL"),
            ("NUMERICAL", "MCQ_SINGLE", "MCQ_MULTI"),
        )
        token = self._str(seed_hint)
        bucket = (
            sum(ord(ch) for ch in token) % len(archetypes)
            if token
            else (idx % len(archetypes))
        )
        cycle = archetypes[bucket]
        # Shift phase every small block to avoid strict A-B-C repetition.
        phase = (idx + (idx // 4)) % len(cycle)
        return cycle[phase]

    def _single_digit_answer_from_question(
        self,
        *,
        question: dict[str, Any],
        options: list[str],
        seed_key: str,
    ) -> int | None:
        candidates: list[float] = []
        for key in ("numerical_answer", "_numerical_answer", "answer", "correct_answer"):
            value = self._first_numeric_value(self._str(question.get(key)))
            if value is not None:
                candidates.append(value)
        label = self._normalize_answer_token_to_label(
            question.get("correct_option") or question.get("_correct_option"),
            options,
        )
        if label in {"A", "B", "C", "D"}:
            idx = ord(label) - 65
            if 0 <= idx < len(options):
                value = self._first_numeric_value(options[idx])
                if value is not None:
                    candidates.append(value)
        if not candidates:
            value = self._first_numeric_value(self._str(question.get("question_text")))
            if value is not None:
                candidates.append(value)
        if not candidates:
            return None
        return int(round(candidates[0]))

    def _coerce_generated_question_type(
        self,
        question: dict[str, Any],
        *,
        target_type: str,
        seed_key: str,
    ) -> dict[str, Any]:
        q = dict(question)
        canonical = self._canonical_question_type(target_type)
        options = [self._str(x) for x in (q.get("options") or []) if self._str(x)]
        while len(options) < 4:
            options.append(f"Option {len(options) + 1}")
        options = [sanitize_latex(x) for x in options[:4]]

        if canonical == "NUMERICAL":
            value = self._single_digit_answer_from_question(
                question=q,
                options=options,
                seed_key=seed_key,
            )
            if value is None:
                return q
            statement = self._str(q.get("question_text"))
            if statement and "____" not in statement:
                statement = f"{statement.rstrip()} (Enter the integer answer.)"
            q["question_text"] = statement
            q["question_type"] = "NUMERICAL"
            q["options"] = []
            q["correct_option"] = ""
            q["correct_answers"] = []
            q["numerical_answer"] = str(value)
            q["solution_explanation"] = self._to_stepwise_solution(
                f"{self._str(q.get('solution_explanation'))} Final numerical answer is {value}."
            )
            return q

        primary = self._normalize_answer_token_to_label(
            q.get("correct_option") or q.get("_correct_option"),
            options,
        )
        if primary not in {"A", "B", "C", "D"}:
            primary = "A"

        if canonical == "MCQ_MULTI":
            existing_multi = [
                self._normalize_answer_token_to_label(x, options)
                for x in (q.get("correct_answers") or q.get("_correct_answers") or [])
            ]
            existing_multi = [x for x in existing_multi if x in {"A", "B", "C", "D"}]
            if len(existing_multi) < 2:
                # Preserve correctness: do not invent extra correct answers.
                q["question_type"] = "MCQ"
                q["options"] = options
                q["correct_option"] = primary
                q["correct_answers"] = [primary]
                q["numerical_answer"] = ""
                return q
            labels = ["A", "B", "C", "D"]
            numeric_by_label: dict[str, float] = {}
            for i, label in enumerate(labels):
                if i >= len(options):
                    break
                parsed = self._first_numeric_value(options[i])
                if parsed is not None:
                    numeric_by_label[label] = parsed

            selected: list[str] = []
            if primary in numeric_by_label and len(numeric_by_label) >= 2:
                primary_value = numeric_by_label[primary]
                selected = sorted(
                    [
                        label
                        for label, value in numeric_by_label.items()
                        if value <= primary_value
                    ]
                )
                if len(selected) < 2:
                    primary_parity = int(abs(round(primary_value))) % 2
                    selected = sorted(
                        [
                            label
                            for label, value in numeric_by_label.items()
                            if int(abs(round(value))) % 2 == primary_parity
                        ]
                    )
                if primary not in selected:
                    selected = sorted({*selected, primary})

            if len(selected) < 2:
                # Do not fabricate a second correct option if evidence is weak.
                selected = [primary]
            if len(selected) < 2:
                q["question_type"] = "MCQ"
                q["options"] = options
                q["correct_option"] = primary
                q["correct_answers"] = [primary]
                q["numerical_answer"] = ""
                return q

            stem = self._str(q.get("question_text"))
            if stem and "select all" not in stem.lower():
                stem = (
                    f"{stem.rstrip()} Select all correct options based on the computed value."
                )
            q["question_text"] = stem
            q["question_type"] = "MCQ_MULTI"
            q["options"] = options
            q["correct_option"] = selected[0]
            q["correct_answers"] = selected
            q["numerical_answer"] = ""
            q["solution_explanation"] = self._to_stepwise_solution(
                f"{self._str(q.get('solution_explanation'))} "
                f"Hence the valid options under the computed-value check are {', '.join(selected)}."
            )
            return q

        q["question_type"] = "MCQ"
        q["options"] = options
        q["correct_option"] = primary
        q["correct_answers"] = [primary]
        q["numerical_answer"] = ""
        return q

    def _pyq_scope_tokens(
        self,
        *,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
    ) -> list[str]:
        bag = " ".join([subject, *chapters[:3], *subtopics[:4]]).lower()
        stop = {
            "main",
            "advanced",
            "hard",
            "question",
            "questions",
            "answer",
            "answers",
            "solution",
            "solutions",
            "class",
            "chapter",
            "subject",
            "mathematics",
            "physics",
            "chemistry",
            "biology",
            "theorem",
            "term",
            "terms",
            "coefficient",
            "coefficients",
            "general",
            "middle",
            "greatest",
            "independent",
        }
        out: list[str] = []
        for token in re.split(r"[^a-z0-9]+", bag):
            tok = token.strip()
            if len(tok) < 4 or tok in stop:
                continue
            if tok not in out:
                out.append(tok)
        return out[:24]

    def _normalize_web_text(self, raw: str) -> str:
        text = html.unescape(self._str(raw))
        text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
        text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _scope_match_score(self, text: str, scope_tokens: list[str]) -> float:
        if not scope_tokens:
            return 0.0
        low = text.lower()
        hits = sum(1 for tok in scope_tokens if tok in low)
        return hits / max(1, len(scope_tokens))

    def _pyq_signal_score(self, text: str) -> float:
        low = text.lower()
        terms = (
            "pyq",
            "previous year",
            "jee",
            "iit",
            "mains",
            "advanced",
            "question paper",
            "answer key",
        )
        hits = sum(1 for term in terms if term in low)
        return min(1.0, hits / 4.0)

    def _hardness_signal_score(self, text: str) -> float:
        low = text.lower()
        terms = (
            "hard",
            "challenging",
            "advanced level",
            "olympiad",
            "integer type",
            "multi correct",
            "assertion",
            "match the following",
            "inclusion-exclusion",
            "casework",
            "proof",
            "tricky",
            "trap",
        )
        hits = sum(1 for term in terms if term in low)
        return min(1.0, hits / 4.0)

    def _build_pyq_query_variants(
        self,
        *,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
        query_suffix: str,
        ultra_hard: bool,
    ) -> list[str]:
        chapter = self._str(chapters[0] if chapters else subject)
        subtopic = self._str(subtopics[0] if subtopics else chapter)
        difficulty_phrase = "JEE Advanced" if ultra_hard else "JEE Main"
        year_span = "2006-2023"
        variants = [
            f"JEE Advanced {year_span} {chapter} difficult previous year question",
            f"IIT JEE Advanced {year_span} {chapter} difficult previous year question",
            f"JEE Advanced {chapter} integer answer hardest PYQ",
            f"IIT JEE Advanced {chapter} integer answer hardest PYQ with solution",
            f"JEE Advanced {chapter} multi concept problem",
            f"JEE Main {chapter} tricky numerical PYQ",
            f"site:allen.ac.in JEE {chapter} PYQ",
            f"site:resonance.ac.in JEE {chapter} previous year",
            f"site:mathongo.com JEE {chapter} PYQ",
            f"site:jeeadv.ac.in {chapter} previous year question paper",
            f"{difficulty_phrase} {subject} {chapter} {subtopic} {query_suffix}",
            f"site:fiitjee.com JEE {chapter} previous year questions with solution",
            f"site:arihantplus.com JEE {chapter} answer key solution",
        ]
        out: list[str] = []
        for q in variants:
            query = " ".join(self._str(x) for x in q.split(" ") if self._str(x)).strip()
            if query and query not in out:
                out.append(query)
        return out[:12]

    def _link_allowed_for_pyq(self, url: str, *, search_scope: str = "pyq") -> bool:
        link = self._str(url).lower()
        if not link.startswith(("http://", "https://")):
            return False
        parsed = urlparse(link)
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        path = parsed.path.lower()
        blocked_hosts = (
            "youtube.com",
            "facebook.com",
            "instagram.com",
            "reddit.com",
            "quora.com",
            "zhihu.com",
            "wikipedia.org",
            "linkedin.com",
            "brainly.com",
            "chegg.com",
            "blogspot.com",
        )
        if any(host.endswith(bad) for bad in blocked_hosts):
            return False
        scope = self._str(search_scope).strip().lower() or "pyq"
        trusted_hosts = (
            "jeeadv.ac.in",
            "nta.ac.in",
            "allen.ac.in",
            "resonance.ac.in",
            "fiitjee.com",
            "mathongo.com",
            "arihantplus.com",
            "arihantbooks.com",
            "careerpoint.ac.in",
            "vedantu.com",
            "selfstudys.com",
        )
        if any(host == ok or host.endswith(f".{ok}") for ok in trusted_hosts):
            return True
        content_bag = f"{host}{path}"
        pyq_tokens = ("jee", "iit", "advanced", "mains", "pyq", "question", "solution")
        if scope in {"general_ai", "general", "ai_chat", "evidence"}:
            evidence_hosts = (
                "stackexchange.com",
                "math.stackexchange.com",
                "physics.stackexchange.com",
                "physicsforums.com",
                "vedantu.com",
                "toppr.com",
                "byjus.com",
                "selfstudys.com",
                "mathsisfun.com",
                "khanacademy.org",
                "cuemath.com",
                "brilliant.org",
                "tutorialspoint.com",
            )
            if any(host == ok or host.endswith(f".{ok}") for ok in evidence_hosts):
                return True
            general_tokens = (
                "math",
                "physics",
                "chemistry",
                "question",
                "solution",
                "formula",
                "theorem",
                "proof",
                "hyperbola",
                "ellipse",
                "parabola",
                "calculus",
                "integral",
                "derivative",
                "matrix",
                "determinant",
            )
            if host.endswith((".gov.in", ".ac.in", ".edu", ".edu.in")) and any(
                token in content_bag for token in general_tokens
            ):
                return True
            if path.endswith(".pdf") and any(token in content_bag for token in general_tokens):
                return True
        if host.endswith((".gov.in", ".ac.in", ".edu", ".edu.in")) and any(
            token in content_bag for token in pyq_tokens
        ):
            return True
        if path.endswith(".pdf") and any(token in content_bag for token in pyq_tokens):
            return True
        return False

    def _detect_search_block_reason(self, raw: str) -> str:
        low = self._str(raw).lower()
        if not low:
            return ""
        if any(
            token in low
            for token in (
                "captcha",
                "verify you are human",
                "anomaly",
                "unusual traffic",
                "automated requests",
                "challenge",
            )
        ):
            return "bot_challenge"
        if "access denied" in low or "forbidden" in low:
            return "access_denied"
        return ""

    def _canonical_web_error_reason(self, error_text: str) -> str:
        low = self._str(error_text).lower()
        if not low:
            return ""
        if any(
            token in low
            for token in (
                "could not resolve host",
                "nodename nor servname provided",
                "name or service not known",
                "temporary failure in name resolution",
                "[errno 8]",
            )
        ):
            return "dns_resolution_failed"
        if "bot_challenge" in low or "captcha" in low or "anomaly" in low:
            return "bot_challenge"
        if "access denied" in low or "access_denied" in low or "forbidden" in low:
            return "access_denied"
        if "timeout" in low:
            return "network_timeout"
        if "empty_response" in low or "no_results" in low:
            return "no_results"
        if "curl_unavailable" in low:
            return "curl_unavailable"
        return self._str(error_text)[:120]

    def _web_cache_get(self, cache: dict[str, dict[str, Any]], key: str) -> dict[str, Any] | None:
        if not key:
            return None
        row = cache.get(key)
        if not isinstance(row, dict):
            return None
        expires_at = self._to_float(row.get("expires_at"), 0.0)
        now = time.time()
        if expires_at <= now:
            cache.pop(key, None)
            return None
        value = row.get("value")
        return dict(value) if isinstance(value, dict) else None

    def _web_cache_put(self, cache: dict[str, dict[str, Any]], key: str, value: dict[str, Any]) -> None:
        if not key:
            return
        now = time.time()
        cache[key] = {
            "value": dict(value),
            "expires_at": now + float(max(30, self._web_cache_ttl_s)),
            "updated_at": now,
        }
        if len(cache) <= self._web_cache_max_entries:
            return
        overflow = len(cache) - self._web_cache_max_entries
        if overflow <= 0:
            return
        oldest_keys = sorted(
            cache.keys(),
            key=lambda k: self._to_float((cache.get(k) or {}).get("updated_at"), 0.0),
        )[:overflow]
        for stale_key in oldest_keys:
            cache.pop(stale_key, None)

    def _fetch_text_urlopen(
        self,
        *,
        url: str,
        timeout_s: float,
        max_bytes: int,
        headers: dict[str, str],
    ) -> str:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read(max(2_000, max_bytes))
        return raw.decode("utf-8", errors="ignore")

    def _fetch_text_curl(
        self,
        *,
        url: str,
        timeout_s: float,
        max_bytes: int,
        headers: dict[str, str],
    ) -> str:
        curl_bin = shutil.which("curl")
        if not curl_bin:
            raise RuntimeError("curl_unavailable")
        user_agent = self._str(headers.get("User-Agent")) or (
            "Mozilla/5.0 (LalaCore/1.0; +https://lalacore.local)"
        )
        hard_timeout_s = max(0.9, float(timeout_s))
        connect_timeout_s = max(0.5, min(2.0, hard_timeout_s))
        cmd = [
            curl_bin,
            "-sSL",
            "--max-time",
            f"{hard_timeout_s:.2f}",
            "--connect-timeout",
            f"{connect_timeout_s:.2f}",
            "-A",
            user_agent,
        ]
        for key, value in headers.items():
            k = self._str(key).strip()
            v = self._str(value).strip()
            if not k or not v or k.lower() == "user-agent":
                continue
            cmd.extend(["-H", f"{k}: {v}"])
        cmd.append(url)
        res = subprocess.run(
            cmd,
            capture_output=True,
            check=False,
            timeout=max(1.4, hard_timeout_s + 0.7),
        )
        if res.returncode != 0:
            stderr = self._str(res.stderr.decode("utf-8", errors="ignore")).strip()
            token = stderr[:120] if stderr else f"returncode_{res.returncode}"
            raise RuntimeError(f"curl_fetch_failed:{token}")
        return res.stdout[: max(2_000, max_bytes)].decode("utf-8", errors="ignore")

    def _fetch_web_text(
        self,
        *,
        url: str,
        timeout_s: float = 6.0,
        max_bytes: int = 250_000,
        headers: dict[str, str] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        req_headers = {
            "User-Agent": "Mozilla/5.0 (LalaCore/1.0; +https://lalacore.local)"
        }
        if isinstance(headers, dict):
            for key, value in headers.items():
                k = self._str(key).strip()
                v = self._str(value).strip()
                if k and v:
                    req_headers[k] = v
        diag: dict[str, Any] = {
            "url": url,
            "transport": "",
            "error": "",
            "block_reason": "",
        }
        try:
            raw = self._fetch_text_urlopen(
                url=url,
                timeout_s=timeout_s,
                max_bytes=max_bytes,
                headers=req_headers,
            )
            diag["transport"] = "urlopen"
            diag["block_reason"] = self._detect_search_block_reason(raw)
            return raw, diag
        except Exception as exc:
            token = self._normalize_web_text(self._str(exc))[:120]
            diag["error"] = f"urlopen:{exc.__class__.__name__}:{token}"
        try:
            raw = self._fetch_text_curl(
                url=url,
                timeout_s=timeout_s,
                max_bytes=max_bytes,
                headers=req_headers,
            )
            diag["transport"] = "curl"
            diag["error"] = ""
            diag["block_reason"] = self._detect_search_block_reason(raw)
            return raw, diag
        except Exception as exc:
            token = self._normalize_web_text(self._str(exc))[:160]
            diag["error"] = (
                f"{self._str(diag.get('error'))};curl:{exc.__class__.__name__}:{token}"
            )
            return "", diag

    def _unwrap_search_result_link(self, link_raw: str) -> str:
        link = html.unescape(self._str(link_raw)).strip()
        if not link:
            return ""
        if link.startswith("//"):
            link = f"https:{link}"
        if "duckduckgo.com/l/?" in link and "uddg=" in link:
            parsed = urlparse(link)
            link = unquote(parse_qs(parsed.query).get("uddg", [link])[0])
        parsed = urlparse(link)
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if host.endswith("bing.com") and parsed.path.startswith("/ck/a"):
            query = parse_qs(parsed.query)
            for key in ("u", "url", "r", "ru"):
                token = self._str((query.get(key) or [""])[0]).strip()
                if not token:
                    continue
                token = unquote(token)
                if token.startswith(("http://", "https://")):
                    link = token
                    break
                payload = token
                if token.startswith("a1"):
                    payload = token[2:]
                if payload:
                    try:
                        padded = payload + ("=" * ((4 - (len(payload) % 4)) % 4))
                        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode(
                            "utf-8",
                            errors="ignore",
                        )
                        if decoded.startswith(("http://", "https://")):
                            link = decoded
                            break
                    except Exception:
                        pass
        return self._str(link).strip()

    def _extract_search_rows_from_rss(
        self, *, raw_xml: str, max_rows: int, search_scope: str = "pyq"
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        seen: set[str] = set()
        items = re.findall(r"(?is)<item>(.*?)</item>", raw_xml)
        for item in items:
            link_match = re.search(r"(?is)<link>(.*?)</link>", item)
            title_match = re.search(r"(?is)<title>(.*?)</title>", item)
            desc_match = re.search(r"(?is)<description>(.*?)</description>", item)
            link = self._unwrap_search_result_link(link_match.group(1) if link_match else "")
            if not link or not self._link_allowed_for_pyq(link, search_scope=search_scope):
                continue
            key = link.lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "title": self._normalize_web_text(
                        title_match.group(1) if title_match else ""
                    )[:220],
                    "url": link[:350],
                    "snippet": self._normalize_web_text(
                        desc_match.group(1) if desc_match else ""
                    )[:360],
                }
            )
            if len(rows) >= max(1, max_rows):
                break
        return rows

    def _infer_stackexchange_site(self, query: str) -> str:
        low = self._str(query).lower()
        physics_tokens = (
            "velocity",
            "acceleration",
            "projectile",
            "electrostatic",
            "current",
            "magnetic",
            "wavelength",
            "optics",
            "thermodynamics",
            "kinematics",
        )
        chemistry_tokens = (
            "mole",
            "stoichiometry",
            "orbital",
            "benzene",
            "alkane",
            "redox",
            "equilibrium",
            "ph",
            "enthalpy",
            "organic",
        )
        if any(token in low for token in physics_tokens):
            return "physics"
        if any(token in low for token in chemistry_tokens):
            return "chemistry"
        return "math"

    def _stackprinter_service_for_site(self, site: str) -> str:
        site_name = self._str(site).strip().lower()
        if site_name == "physics":
            return "physics.stackexchange"
        if site_name == "chemistry":
            return "chemistry.stackexchange"
        return "math.stackexchange"

    def _extract_search_rows_from_stackexchange_json(
        self,
        *,
        raw_json: str,
        max_rows: int,
        site: str,
        search_scope: str = "general_ai",
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        seen: set[str] = set()
        try:
            payload = json.loads(self._str(raw_json) or "{}")
        except Exception:
            return rows
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return rows
        service = self._stackprinter_service_for_site(site)
        for item in items:
            if not isinstance(item, dict):
                continue
            link = self._str(item.get("link")).strip()
            if not link or not self._link_allowed_for_pyq(link, search_scope=search_scope):
                continue
            key = link.lower()
            if key in seen:
                continue
            seen.add(key)
            title = self._normalize_web_text(self._str(item.get("title")))[:220]
            tags = [
                self._str(tag).strip()
                for tag in (item.get("tags") or [])
                if self._str(tag).strip()
            ][:4]
            answer_count = self._to_int(item.get("answer_count"), 0)
            score = self._to_int(item.get("score"), 0)
            question_id = self._to_int(item.get("question_id"), 0)
            snippet_bits = []
            if tags:
                snippet_bits.append("tags: " + ", ".join(tags))
            if answer_count > 0:
                snippet_bits.append(f"answers: {answer_count}")
            snippet_bits.append(f"score: {score}")
            fetch_url = ""
            if question_id > 0:
                fetch_url = (
                    "https://stackprinter.appspot.com/export"
                    f"?question={question_id}&service={service}&language=en"
                    "&hideAnswers=false&width=640"
                )
            rows.append(
                {
                    "title": title,
                    "url": link[:350],
                    "snippet": " | ".join(snippet_bits)[:360],
                    "fetch_url": fetch_url[:350],
                }
            )
            if len(rows) >= max(1, max_rows):
                break
        return rows

    def _extract_search_rows_from_html(
        self,
        *,
        raw_html: str,
        provider: str,
        max_rows: int,
        search_scope: str = "pyq",
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        seen: set[str] = set()

        def _append_result(link_raw: str, title_raw: str, snippet_raw: str = "") -> None:
            link = self._unwrap_search_result_link(link_raw)
            if not link:
                return
            clean_link = self._str(link).strip()
            if not self._link_allowed_for_pyq(clean_link, search_scope=search_scope):
                return
            key = clean_link.lower()
            if key in seen:
                return
            seen.add(key)
            rows.append(
                {
                    "title": self._normalize_web_text(title_raw)[:220],
                    "url": clean_link[:350],
                    "snippet": self._normalize_web_text(snippet_raw)[:360],
                }
            )

        def _extract_generic_anchors() -> None:
            anchor_matches = re.findall(
                r'(?is)<a[^>]*href="(https?://[^"]+)"[^>]*>(.*?)</a>',
                raw_html,
            )
            for href_raw, title_raw in anchor_matches:
                _append_result(href_raw, title_raw, "")
                if len(rows) >= max(1, max_rows):
                    break

        if provider.startswith("duckduckgo"):
            title_links = re.findall(
                r'<a[^>]*(?:class="[^"]*(?:result__a|result-link)[^"]*")[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                raw_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            snippets_text = re.findall(
                r'<a[^>]*class="[^"]*result__snippet[^"]*"[^>]*>(.*?)</a>',
                raw_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not snippets_text:
                snippets_text = re.findall(
                    r'<td[^>]*class="[^"]*result-snippet[^"]*"[^>]*>(.*?)</td>',
                    raw_html,
                    flags=re.IGNORECASE | re.DOTALL,
                )
            for idx, (href_raw, title_raw) in enumerate(title_links):
                snippet = snippets_text[idx] if idx < len(snippets_text) else ""
                _append_result(href_raw, title_raw, snippet)
                if len(rows) >= max(1, max_rows):
                    break
            if not rows:
                _extract_generic_anchors()
            return rows

        if provider == "bing_html":
            blocks = re.findall(
                r'(?is)<li[^>]*class="[^"]*\bb_algo\b[^"]*"[^>]*>(.*?)</li>',
                raw_html,
            )
            for block in blocks:
                m = re.search(r'(?is)<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', block)
                if not m:
                    continue
                snippet_match = re.search(r"(?is)<p[^>]*>(.*?)</p>", block)
                snippet = snippet_match.group(1) if snippet_match else ""
                _append_result(m.group(1), m.group(2), snippet)
                if len(rows) >= max(1, max_rows):
                    break
            if not rows:
                _extract_generic_anchors()
            return rows

        if provider == "brave_html":
            _extract_generic_anchors()
            if len(rows) < max(1, max_rows):
                card_pairs = re.findall(
                    r'(?is)title:"([^"]{3,260})"\s*,\s*url:"(https?://[^"]+)"',
                    raw_html,
                )
                for title_raw, href_raw in card_pairs:
                    _append_result(href_raw, title_raw, "")
                    if len(rows) >= max(1, max_rows):
                        break
            return rows

        _extract_generic_anchors()
        return rows

    def _search_rows_from_provider(
        self,
        *,
        query: str,
        provider: str,
        max_rows: int,
        search_scope: str = "pyq",
        timeout_s: float = 6.0,
    ) -> tuple[list[dict[str, str]], dict[str, Any]]:
        encoded_query = quote_plus(query)
        stackexchange_site = self._infer_stackexchange_site(query)
        provider_urls = {
            "stackexchange_api": (
                "https://api.stackexchange.com/2.3/search/advanced"
                f"?order=desc&sort=relevance&q={encoded_query}&site={stackexchange_site}"
                f"&pagesize={max(6, max_rows)}&filter=default"
            ),
            "duckduckgo_html": f"https://duckduckgo.com/html/?q={encoded_query}&kl=us-en",
            "duckduckgo_lite": f"https://lite.duckduckgo.com/lite/?q={encoded_query}&kl=us-en",
            "bing_html": f"https://www.bing.com/search?q={encoded_query}&setlang=en-US",
            "bing_rss": f"https://www.bing.com/search?format=rss&q={encoded_query}&setlang=en-US",
            "brave_html": f"https://search.brave.com/search?q={encoded_query}&source=web",
        }
        url = provider_urls.get(provider)
        if not url:
            return [], {
                "provider": provider,
                "query": query,
                "result_count": 0,
                "error": "unknown_provider",
            }
        cache_key = (
            f"{provider}|{self._str(search_scope).strip().lower()}|"
            f"{self._str(query).strip().lower()}|{max(1, max_rows)}"
        )
        cached = self._web_cache_get(self._web_search_cache, cache_key)
        if isinstance(cached, dict):
            cached_rows = [dict(x) for x in (cached.get("rows") or []) if isinstance(x, dict)]
            cached_diag = dict(cached.get("diag") or {})
            if cached_diag:
                cached_diag["cached"] = True
            return cached_rows[: max(1, max_rows)], cached_diag
        max_bytes = 260_000
        if provider == "brave_html":
            max_bytes = 1_400_000
        raw, fetch_diag = self._fetch_web_text(
            url=url,
            timeout_s=max(0.8, float(timeout_s)),
            max_bytes=max_bytes,
        )
        rows = (
            (
                self._extract_search_rows_from_stackexchange_json(
                    raw_json=raw,
                    max_rows=max_rows,
                    site=stackexchange_site,
                    search_scope=search_scope,
                )
                if provider == "stackexchange_api"
                else self._extract_search_rows_from_rss(
                    raw_xml=raw,
                    max_rows=max_rows,
                    search_scope=search_scope,
                )
                if provider == "bing_rss"
                else self._extract_search_rows_from_html(
                    raw_html=raw,
                    provider=provider,
                    max_rows=max_rows,
                    search_scope=search_scope,
                )
            )
            if raw
            else []
        )
        diag = {
            "provider": provider,
            "query": query,
            "url": url,
            "transport": self._str(fetch_diag.get("transport")),
            "result_count": len(rows),
            "error": "",
            "error_detail": self._str(fetch_diag.get("error")),
            "block_reason": self._str(fetch_diag.get("block_reason")),
            "search_scope": search_scope,
        }
        if diag["error_detail"]:
            diag["error"] = self._canonical_web_error_reason(diag["error_detail"])
        if not rows and not diag["error"] and not raw:
            diag["error"] = "empty_response"
        if not rows and not diag["error"] and diag["block_reason"]:
            diag["error"] = self._canonical_web_error_reason(diag["block_reason"])
        if rows:
            self._web_cache_put(
                self._web_search_cache,
                cache_key,
                {"rows": rows, "diag": diag},
            )
        return rows, diag

    def _search_rows_with_provider_fallback(
        self,
        query: str,
        *,
        max_rows: int,
        search_scope: str = "pyq",
        total_timeout_s: float = 6.0,
    ) -> tuple[list[dict[str, str]], dict[str, Any]]:
        scope = self._str(search_scope).strip().lower() or "pyq"
        if scope in {"general_ai", "general", "ai_chat", "evidence"}:
            providers = (
                "stackexchange_api",
                "bing_rss",
                "duckduckgo_lite",
                "duckduckgo_html",
                "bing_html",
                "brave_html",
            )
        else:
            providers = (
                "bing_rss",
                "duckduckgo_lite",
                "duckduckgo_html",
                "bing_html",
                "brave_html",
            )
        merged_rows: list[dict[str, str]] = []
        seen_links: set[str] = set()
        attempts: list[dict[str, Any]] = []
        started_at = time.time()
        deadline = started_at + max(0.8, float(total_timeout_s))
        for idx, provider in enumerate(providers):
            remaining_s = deadline - time.time()
            if remaining_s <= 0.12:
                break
            providers_left = max(1, len(providers) - idx)
            base_timeout_s = remaining_s / providers_left + 0.35
            provider_floor_s = 0.9
            provider_cap_s = 2.8
            if provider == "stackexchange_api":
                provider_floor_s = 1.6
                provider_cap_s = 4.2
                if scope in {"general_ai", "general", "ai_chat", "evidence"}:
                    base_timeout_s = max(base_timeout_s, remaining_s * 0.48)
            elif provider in {"duckduckgo_lite", "bing_rss"}:
                provider_floor_s = 1.0
                provider_cap_s = 3.0
            elif provider in {"duckduckgo_html", "bing_html", "brave_html"}:
                provider_floor_s = 1.1
                provider_cap_s = 3.4
            provider_timeout_s = max(
                provider_floor_s,
                min(provider_cap_s, base_timeout_s),
            )
            rows, diag = self._search_rows_from_provider(
                query=query,
                provider=provider,
                max_rows=max_rows,
                search_scope=search_scope,
                timeout_s=provider_timeout_s,
            )
            attempts.append(diag)
            for row in rows:
                url = self._str(row.get("url")).strip().lower()
                if not url or url in seen_links:
                    continue
                seen_links.add(url)
                merged_rows.append(row)
                if len(merged_rows) >= max(1, max_rows):
                    break
            if len(merged_rows) >= max(1, max_rows):
                break
        error_reason = ""
        if not merged_rows:
            for diag in attempts:
                block_reason = self._str(diag.get("block_reason"))
                error = self._str(diag.get("error"))
                if block_reason:
                    error_reason = self._canonical_web_error_reason(block_reason)
                    break
                if error and not error_reason:
                    error_reason = self._canonical_web_error_reason(error)
            if not error_reason:
                error_reason = "no_results"
        return merged_rows[: max(1, max_rows)], {
            "query": query,
            "providers": attempts,
            "result_count": len(merged_rows),
            "error_reason": error_reason,
            "search_scope": search_scope,
            "elapsed_s": round(max(0.0, time.time() - started_at), 4),
            "timeout_s": float(max(0.8, total_timeout_s)),
        }

    def _duckduckgo_search_rows(self, query: str, *, max_rows: int) -> list[dict[str, str]]:
        rows, _ = self._search_rows_with_provider_fallback(query, max_rows=max_rows)
        return rows

    def _merge_pyq_web_diagnostics(
        self, diagnostics_rows: list[dict[str, Any]]
    ) -> dict[str, Any]:
        attempts = [
            row
            for row in diagnostics_rows
            if isinstance(row, dict) and self._str(row.get("query"))
        ]
        provider_hits: dict[str, int] = {}
        provider_errors: dict[str, int] = {}
        transports: set[str] = set()
        challenge_hits = 0
        dns_error_hits = 0
        queries_with_results = 0
        for entry in attempts:
            if self._to_int(entry.get("result_count"), 0) > 0:
                queries_with_results += 1
            entry_error = self._str(entry.get("error_reason"))
            if self._canonical_web_error_reason(entry_error) == "dns_resolution_failed":
                dns_error_hits += 1
            for diag in entry.get("providers") or []:
                if not isinstance(diag, dict):
                    continue
                provider = self._str(diag.get("provider"))
                if provider:
                    provider_hits[provider] = provider_hits.get(provider, 0) + self._to_int(
                        diag.get("result_count"),
                        0,
                    )
                transport = self._str(diag.get("transport"))
                if transport:
                    transports.add(transport)
                block_reason = self._str(diag.get("block_reason"))
                if block_reason == "bot_challenge":
                    challenge_hits += 1
                err = self._str(diag.get("error"))
                if err:
                    provider_key = provider or "unknown"
                    provider_errors[provider_key] = provider_errors.get(provider_key, 0) + 1
                err_detail = self._str(diag.get("error_detail"))
                if (
                    self._canonical_web_error_reason(err) == "dns_resolution_failed"
                    or self._canonical_web_error_reason(err_detail)
                    == "dns_resolution_failed"
                ):
                    dns_error_hits += 1
        web_error_reason = ""
        web_error_detail = ""
        if queries_with_results == 0:
            for entry in attempts:
                token = self._str(entry.get("error_reason"))
                if token:
                    web_error_detail = token
                    break
            if not web_error_detail:
                web_error_detail = "no_results"
            web_error_reason = self._canonical_web_error_reason(web_error_detail) or "no_results"
        return {
            "query_attempts": len(attempts),
            "queries_with_results": queries_with_results,
            "provider_hits": provider_hits,
            "provider_errors": provider_errors,
            "transports_used": sorted(transports),
            "bot_challenge_hits": challenge_hits,
            "dns_error_hits": dns_error_hits,
            "web_error_reason": web_error_reason,
            "web_error_detail": web_error_detail,
            "attempts": attempts[:12],
        }

    def _extract_mcq_options_from_text(self, text: str) -> list[str]:
        raw = self._str(text)
        if not raw:
            return []
        opts: dict[str, str] = {}
        for label in ("A", "B", "C", "D"):
            pat = (
                rf"(?is)(?:\(|\b){label}\)?\s*[\).:\-]\s*"
                rf"(.{{1,220}}?)(?=(?:\(|\b)[A-D]\)?\s*[\).:\-]|\b(?:answer|solution|correct)\b|$)"
            )
            m = re.search(pat, raw)
            if m:
                value = self._normalize_web_text(m.group(1))
                if 1 <= len(value) <= 160:
                    opts[label] = value
        if len(opts) == 4:
            return [opts["A"], opts["B"], opts["C"], opts["D"]]

        numeric_opts: dict[int, str] = {}
        for num in (1, 2, 3, 4):
            pat = (
                rf"(?is)(?:\(|\b){num}\)?\s*[\).:\-]\s*"
                rf"(.{{1,220}}?)(?=(?:\(|\b)[1-4]\)?\s*[\).:\-]|\b(?:answer|solution|correct)\b|$)"
            )
            m = re.search(pat, raw)
            if m:
                value = self._normalize_web_text(m.group(1))
                if 1 <= len(value) <= 160:
                    numeric_opts[num] = value
        if len(numeric_opts) == 4:
            return [
                numeric_opts[1],
                numeric_opts[2],
                numeric_opts[3],
                numeric_opts[4],
            ]
        return []

    def _extract_year_and_exam(self, text: str) -> tuple[str, str]:
        low = self._str(text).lower()
        year = ""
        year_match = re.search(r"\b(20(?:0[6-9]|1\d|2[0-3]))\b", low)
        if year_match:
            year = self._str(year_match.group(1))
        exam = ""
        if "jee advanced" in low or "iit jee" in low:
            exam = "JEE Advanced"
        elif "jee main" in low:
            exam = "JEE Main"
        elif "neet" in low:
            exam = "NEET"
        return year, exam

    def _pyq_difficulty_score(
        self,
        *,
        question_text: str,
        solution_text: str,
        options: list[str],
        scope_score: float,
        exam_type: str,
        answer_token: str,
    ) -> float:
        q_low = self._str(question_text).lower()
        s_low = self._str(solution_text).lower()
        bag = f"{q_low} {s_low}"
        symbol_density = len(re.findall(r"\\binom|\\sum|\\prod|\\int|\\frac|\^|_", q_low))
        parameter_presence = len(set(re.findall(r"\b[a-z]\b", q_low)))
        trap_keywords = sum(
            1
            for token in (
                "case",
                "constraint",
                "integer",
                "greatest",
                "constant term",
                "middle term",
                "inclusion-exclusion",
                "trap",
                "hence",
                "therefore",
            )
            if token in bag
        )
        multi_concept = 1 if (("coefficient" in q_low and "term" in q_low) or ("general term" in q_low and "constant term" in q_low)) else 0
        numeric_answer = bool(re.fullmatch(r"[-+]?\d+(?:\.\d+)?", self._str(answer_token)))
        topic_match_weight = max(0.0, min(1.0, scope_score)) * 32.0
        advanced_exam_weight = 20.0 if exam_type == "JEE Advanced" else (10.0 if exam_type == "JEE Main" else 4.0)
        integer_type_weight = 11.0 if (numeric_answer and not options) else 4.0
        multi_step_indicator = 9.0 if any(k in s_low for k in ("step", "therefore", "hence")) else 2.0
        solution_length_weight = min(8.0, (len(solution_text) / 140.0) * 8.0)
        symbol_density_weight = min(9.0, symbol_density * 1.9)
        parameter_presence_weight = min(8.0, parameter_presence * 1.3)
        trap_keyword_weight = min(10.0, trap_keywords * 2.0)
        multi_concept_bonus = 7.0 if multi_concept else 0.0
        score = (
            topic_match_weight
            + advanced_exam_weight
            + integer_type_weight
            + multi_step_indicator
            + solution_length_weight
            + symbol_density_weight
            + parameter_presence_weight
            + trap_keyword_weight
            + multi_concept_bonus
        )
        return round(score, 6)

    def _classify_pyq_candidate(
        self,
        *,
        text_bag: str,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
        options: list[str],
        difficulty_score: float,
    ) -> dict[str, Any]:
        bag = self._str(text_bag).lower()
        requested_track = self._infer_subject_track(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
            concept_tags=[],
        )
        subject_guess = requested_track
        chapter_guess = self._str(chapters[0] if chapters else subject)
        subtopic_guess = self._str(subtopics[0] if subtopics else chapter_guess)
        if requested_track == "Mathematics":
            if not any(
                token in bag
                for token in (
                    "coefficient",
                    "expansion",
                    "integral",
                    "derivative",
                    "equation",
                    "binomial",
                    "ncr",
                    "term",
                    "probability",
                )
            ):
                subject_guess = "Unknown"
        elif requested_track == "Physics":
            if not any(
                token in bag
                for token in (
                    "velocity",
                    "acceleration",
                    "current",
                    "charge",
                    "field",
                    "work",
                    "power",
                    "force",
                )
            ):
                subject_guess = "Unknown"
        elif requested_track == "Chemistry":
            if not any(
                token in bag
                for token in (
                    "mole",
                    "equilibrium",
                    "enthalpy",
                    "reaction",
                    "oxidation",
                    "acid",
                    "base",
                )
            ):
                subject_guess = "Unknown"
        elif requested_track == "Biology":
            if not any(
                token in bag
                for token in ("cell", "gene", "species", "ecology", "plant", "animal")
            ):
                subject_guess = "Unknown"
        chapter_tokens = self._pyq_scope_tokens(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
        )
        chapter_hits = sum(1 for tok in chapter_tokens if tok in bag)
        chapter_ok = chapter_hits > 0 if chapter_tokens else True
        q_type = "MCQ_SINGLE" if len(options) == 4 else "NUMERICAL"
        diff_est = max(1, min(5, int(round((difficulty_score - 20) / 12))))
        return {
            "subject": subject_guess,
            "chapter": chapter_guess,
            "subtopic": subtopic_guess,
            "difficulty_estimate": diff_est,
            "question_type": q_type,
            "diagram_required": any(k in bag for k in ("diagram", "figure", "graph")),
            "subject_ok": subject_guess != "Unknown",
            "chapter_ok": chapter_ok,
        }

    def _extract_pyq_page_evidence(
        self, url: str, *, scope_tokens: list[str], timeout_s: float = 3.5
    ) -> dict[str, Any]:
        cache_key = (
            f"{self._str(url).strip().lower()}|"
            f"{','.join(sorted({self._str(x).lower() for x in scope_tokens if self._str(x)}))}"
        )
        cached = self._web_cache_get(self._web_page_evidence_cache, cache_key)
        if isinstance(cached, dict):
            out_cached = dict(cached)
            out_cached["cached"] = True
            return out_cached
        raw, fetch_diag = self._fetch_web_text(
            url=url,
            timeout_s=timeout_s,
            max_bytes=220_000,
        )
        if not raw:
            return {}

        page_text = self._normalize_web_text(raw)
        if not page_text:
            return {}
        page_text = page_text[:12000]
        lines = [
            line.strip()
            for line in re.split(r"[\r\n]+|(?<=[?.!])\s{2,}", page_text)
            if line.strip()
        ]
        lines = [line for line in lines if 10 <= len(line) <= 420]
        q_candidates = [
            line
            for line in lines
            if "?" in line
            and any(
                marker in line.lower()
                for marker in (
                    "find",
                    "evaluate",
                    "determine",
                    "coefficient",
                    "expansion",
                    "term",
                    "integral",
                )
            )
        ]
        question_stub = q_candidates[0] if q_candidates else ""
        if not question_stub:
            q_fallback = re.search(
                r"(?is)\b(?:find|evaluate|determine|if|in the expansion of)\b.{30,260}?\?",
                page_text,
            )
            if q_fallback:
                question_stub = self._normalize_web_text(q_fallback.group(0))
        options = self._extract_mcq_options_from_text(page_text)

        ans = ""
        for pat in (
            r"(?i)\b(?:ans(?:wer)?(?:\s*key)?|correct\s*(?:option|answer))\s*[:=\-]\s*(?:option\s*)?([A-D]|\d+(?:\.\d+)?)\b",
            r"(?i)\boption\s*([A-D])\s*(?:is\s*)?correct\b",
            r"(?i)\banswer\s*[:=\-]\s*\(?([A-D])\)?\b",
        ):
            m = re.search(pat, page_text)
            if m:
                ans = self._str(m.group(1)).upper()
                break
        if options and ans in {"1", "2", "3", "4"}:
            ans = chr(ord("A") + int(ans) - 1)
        if not ans and options:
            option_hint = re.search(
                r"(?i)\b(?:option\s*)?([A-D])\s*(?:is\s*)?(?:correct|final answer)\b",
                page_text,
            )
            if option_hint:
                ans = self._str(option_hint.group(1)).upper()
        solution_stub = ""
        sol_match = re.search(
            r"(?is)\b(?:solution|explanation|method)\b[:\-\s]{0,6}(.{40,380})",
            page_text,
        )
        if sol_match:
            solution_stub = self._normalize_web_text(sol_match.group(1))
        if not solution_stub:
            step_match = re.search(
                r"(?is)\b(?:step\s*1|therefore|hence)\b.{35,300}",
                page_text,
            )
            if step_match:
                solution_stub = self._normalize_web_text(step_match.group(0))
        year, exam_type = self._extract_year_and_exam(page_text)
        question_text = question_stub
        if not question_text and options:
            intro_match = re.search(
                r"(?is)(.{40,320}?)(?=(?:\(|\b)A\)?\s*[\).:\-])",
                page_text,
            )
            if intro_match:
                question_text = self._normalize_web_text(intro_match.group(1))

        scope_score = self._scope_match_score(
            f"{question_stub} {solution_stub} {page_text[:1800]}",
            scope_tokens,
        )
        out = {
            "question_text": question_text[:420],
            "options": options[:4],
            "correct_answer": ans[:24],
            "question_stub": question_stub[:360],
            "answer_stub": ans[:24],
            "solution_stub": solution_stub[:420],
            "year": year,
            "exam_type": exam_type,
            "answer_missing": not bool(ans),
            "solution_missing": not bool(solution_stub),
            "scope_score_page": round(scope_score, 6),
            "has_answer": bool(ans),
            "has_solution": bool(solution_stub),
            "fetch_transport": self._str(fetch_diag.get("transport")),
        }
        self._web_cache_put(self._web_page_evidence_cache, cache_key, out)
        return out

    def _merge_pyq_rows(
        self,
        primary_rows: list[dict[str, Any]],
        secondary_rows: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        by_url: dict[str, dict[str, Any]] = {}
        for row in [*primary_rows, *secondary_rows]:
            url = self._str(row.get("url")).strip().lower()
            if not url:
                continue
            if url not in by_url:
                by_url[url] = dict(row)
                continue
            cur = by_url[url]
            for key in (
                "title",
                "snippet",
                "question_stub",
                "answer_stub",
                "solution_stub",
                "query",
            ):
                if not self._str(cur.get(key)).strip() and self._str(row.get(key)).strip():
                    cur[key] = row.get(key)
            for key in ("scope_score", "pyq_score", "hardness_score", "quality_score"):
                cur[key] = max(
                    self._to_float(cur.get(key), 0.0),
                    self._to_float(row.get(key), 0.0),
                )
            cur["has_answer"] = self._to_bool(cur.get("has_answer")) or self._to_bool(
                row.get("has_answer")
            )
            cur["has_solution"] = self._to_bool(cur.get("has_solution")) or self._to_bool(
                row.get("has_solution")
            )
        merged = list(by_url.values())
        merged.sort(
            key=lambda row: (
                self._to_float(row.get("quality_score"), 0.0),
                self._to_float(row.get("hardness_score"), 0.0),
                self._to_float(row.get("scope_score"), 0.0),
                1.0 if self._to_bool(row.get("has_answer")) else 0.0,
                1.0 if self._to_bool(row.get("has_solution")) else 0.0,
            ),
            reverse=True,
        )
        return merged[: max(1, limit)]

    def _local_pyq_archive_rows(
        self,
        *,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        if not self._ai_quizzes and not self._import_question_bank:
            return []

        requested_track = self._infer_subject_track(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
            concept_tags=[],
        )
        scope_tokens = self._pyq_scope_tokens(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
        )
        requested_domain = self._domain_key_from_context(
            subject=subject,
            concept_tags=[*chapters[:3], *subtopics[:4]],
        )
        enforce_domain_alignment = (
            requested_domain.startswith(("math_", "physics_", "chemistry_", "biology_"))
            and len(chapters) <= 3
        )
        pyq_scope_bonus_tokens = (
            "jee",
            "advanced",
            "main",
            "pyq",
            "previous year",
            "teacher_import",
        )
        import_scope_available = False
        if self._import_question_bank:
            probe_budget = (
                len(self._import_question_bank)
                if len(self._import_question_bank) <= 25_000
                else 6_000
            )
            for probe in reversed(self._import_question_bank[-probe_budget:]):
                probe_blocked, _ = self._question_bank_runtime_flags(probe)
                if probe_blocked:
                    continue
                probe_bag = " ".join(
                    [
                        self._str(probe.get("question_text") or probe.get("question")),
                        self._str(probe.get("chapter")),
                        " ".join(self._to_list_str(probe.get("chapter_tags"))),
                        self._str(probe.get("topic")),
                    ]
                )
                if self._scope_match_score(probe_bag, scope_tokens) <= 0.0:
                    continue
                if not self._question_bank_row_matches_scope(
                    row=probe,
                    subject=subject,
                    chapters=chapters,
                    subtopics=subtopics,
                ):
                    continue
                probe_subject = self._str(probe.get("subject"))
                probe_chapter_tags = self._resolve_import_row_chapter_tags(
                    row=probe,
                    subject_override=probe_subject,
                    max_tags=3,
                )
                probe_chapter = (
                    probe_chapter_tags[0]
                    if probe_chapter_tags
                    else self._resolve_import_row_chapter(
                        row=probe,
                        subject_override=probe_subject,
                    )
                    or self._str(probe.get("chapter"))
                )
                probe_track = self._infer_subject_track(
                    subject=probe_subject,
                    chapters=probe_chapter_tags[:2] if probe_chapter_tags else ([probe_chapter] if probe_chapter else []),
                    subtopics=[probe_chapter] if probe_chapter else [],
                    concept_tags=[],
                )
                if requested_track and probe_track != requested_track:
                    continue
                if enforce_domain_alignment:
                    probe_domain = self._domain_key_from_context(
                        subject=probe_subject or subject,
                        concept_tags=[probe_chapter],
                    )
                    if probe_domain != requested_domain:
                        continue
                probe_text = self._str(probe.get("question_text") or probe.get("question"))
                if self._looks_degraded_math_text(probe_text):
                    continue
                probe_solution = self._str(
                    probe.get("solution_explanation")
                    or probe.get("solution")
                    or probe.get("source_solution_stub")
                )
                probe_bag = " ".join(
                    [probe_subject, probe_chapter, " ".join(probe_chapter_tags), probe_text, probe_solution]
                ).lower()
                if self._scope_match_score(probe_bag, scope_tokens) > 0.0:
                    import_scope_available = True
                    break

        rows: list[dict[str, Any]] = []
        seen_questions: set[str] = set()
        scan_budget = max(120, min(len(self._ai_quizzes), limit * 45))

        for quiz in reversed(self._ai_quizzes[-scan_budget:]):
            quiz_subject = self._str(quiz.get("subject") or quiz.get("title"))
            quiz_chapters = self._to_list_str(
                quiz.get("chapters_json")
                or quiz.get("chapters")
                or quiz.get("chapter")
            )
            quiz_subtopics = self._to_list_str(
                quiz.get("subtopics")
                or quiz.get("chapter")
                or quiz.get("concept_tags")
            )
            quiz_track = self._infer_subject_track(
                subject=quiz_subject,
                chapters=quiz_chapters,
                subtopics=quiz_subtopics,
                concept_tags=[],
            )
            if requested_track and quiz_track != requested_track:
                continue
            if enforce_domain_alignment:
                row_domain = self._domain_key_from_context(
                    subject=quiz_subject or subject,
                    concept_tags=[*quiz_chapters[:2], *quiz_subtopics[:2]],
                )
                if row_domain != requested_domain:
                    continue

            q_rows = self._parse_questions(quiz)
            if not q_rows:
                continue

            quiz_id = self._str(quiz.get("quiz_id") or quiz.get("id")) or "archive"
            quiz_title = self._str(quiz.get("title") or quiz.get("subject"))
            quiz_url = self._str(quiz.get("url") or quiz.get("quiz_url"))
            for q in q_rows:
                q_text = self._str(q.get("question_text") or q.get("question"))
                if not q_text:
                    continue
                normalized_key = re.sub(r"\s+", " ", q_text).strip().lower()
                if not normalized_key or normalized_key in seen_questions:
                    continue

                options_raw = q.get("options")
                options: list[str] = []
                if isinstance(options_raw, list):
                    for opt in options_raw:
                        if isinstance(opt, dict):
                            text = self._str(
                                opt.get("text") or opt.get("value") or opt.get("option")
                            )
                        else:
                            text = self._str(opt)
                        if text:
                            options.append(text)
                if len(options) >= 4:
                    options = options[:4]
                else:
                    options = []

                answer_raw = self._str(
                    q.get("correct_option")
                    or q.get("_correct_option")
                    or q.get("correct_answer")
                    or q.get("answer")
                    or q.get("numerical_answer")
                    or q.get("_numerical_answer")
                )
                if (
                    not answer_raw
                    and isinstance(q.get("correct_answers"), list)
                    and q.get("correct_answers")
                ):
                    answer_raw = self._str((q.get("correct_answers") or [""])[0])
                if (
                    not answer_raw
                    and isinstance(q.get("_correct_answers"), list)
                    and q.get("_correct_answers")
                ):
                    answer_raw = self._str((q.get("_correct_answers") or [""])[0])
                if not answer_raw:
                    answer_raw = self._str(q.get("source_answer_stub"))

                answer_token = (
                    self._normalize_answer_token_to_label(answer_raw, options)
                    if options
                    else self._extract_answer_token(answer_raw)
                )
                if options and answer_token not in {"A", "B", "C", "D"}:
                    answer_token = ""
                if not options and answer_token in {"A", "B", "C", "D"}:
                    answer_token = ""

                solution_stub = self._str(
                    q.get("solution_explanation")
                    or q.get("_solution_explanation")
                    or q.get("solution")
                    or q.get("source_solution_stub")
                )
                source_origin = self._str(
                    q.get("source_origin") or quiz.get("source_origin")
                ).lower()
                is_synth_archive = source_origin.startswith(("synthesized", "ai_synth"))
                if import_scope_available:
                    # Prefer imported bank rows over replaying archived AI quiz rows.
                    continue

                bag = " ".join(
                    [
                        quiz_title,
                        q_text,
                        solution_stub,
                        source_origin,
                        " ".join(self._str(x) for x in (q.get("concept_tags") or [])),
                    ]
                )
                scope_score = self._scope_match_score(bag, scope_tokens)
                pyq_score = self._pyq_signal_score(bag)
                if any(tok in bag.lower() for tok in pyq_scope_bonus_tokens):
                    pyq_score = min(1.0, pyq_score + 0.25)
                hardness_score = self._hardness_signal_score(bag)
                if scope_tokens and scope_score <= 0.0:
                    if len(scope_tokens) >= 2 or pyq_score < 0.75:
                        continue

                easy_integral_template = bool(
                    re.search(
                        r"(?i)evaluate\s*\$?\\int_0\^1.*multiplied by\s*\$?6",
                        q_text,
                    )
                )
                quality_score = (
                    (scope_score * 0.60)
                    + (pyq_score * 0.42)
                    + (hardness_score * 0.25)
                    + (0.18 if "pyq" in source_origin else 0.0)
                    + (0.30 if answer_token else -0.08)
                    + (0.16 if solution_stub else 0.0)
                    - (0.55 if is_synth_archive else 0.0)
                    - (0.35 if easy_integral_template else 0.0)
                )
                url = quiz_url or f"local://pyq/{quiz_id}/{self._str(q.get('question_id')) or len(rows)+1}"
                rows.append(
                    {
                        "title": (quiz_title or f"{subject} PYQ Archive")[:220],
                        "url": url[:350],
                        "snippet": q_text[:360],
                        "query": "local_pyq_archive",
                        "scope_score": round(scope_score, 6),
                        "pyq_score": round(pyq_score, 6),
                        "hardness_score": round(hardness_score, 6),
                        "quality_score": round(quality_score, 6),
                        "question_text": q_text[:420],
                        "question_stub": q_text[:360],
                        "options": options[:4],
                        "correct_answer": answer_token[:24],
                        "answer_stub": answer_token[:24],
                        "solution_stub": solution_stub[:420],
                        "has_answer": bool(answer_token),
                        "has_solution": bool(solution_stub),
                        "exam_type": "JEE",
                        "difficulty_estimate": max(
                            3, min(5, self._to_int(q.get("difficulty_estimate"), 4))
                        ),
                        "subject_ok": True,
                        "chapter_ok": True,
                        "source_provider": "local_pyq_archive_ai",
                        "source_origin": source_origin[:64],
                    }
                )
                seen_questions.add(normalized_key)
                if len(rows) >= max(8, limit * 4):
                    break
            if len(rows) >= max(8, limit * 4):
                break

        bank_scan_budget = (
            len(self._import_question_bank)
            if len(self._import_question_bank) <= 25_000
            else max(5000, min(len(self._import_question_bank), limit * 320))
        )
        for bank_row in reversed(self._import_question_bank[-bank_scan_budget:]):
            hard_block, bank_quality_bonus = self._question_bank_runtime_flags(bank_row)
            if hard_block:
                continue
            bank_probe_bag = " ".join(
                [
                    self._str(bank_row.get("question_text") or bank_row.get("question")),
                    self._str(bank_row.get("chapter")),
                    " ".join(self._to_list_str(bank_row.get("chapter_tags"))),
                    self._str(bank_row.get("topic")),
                ]
            )
            if self._scope_match_score(bank_probe_bag, scope_tokens) <= 0.0:
                continue
            if not self._question_bank_row_matches_scope(
                row=bank_row,
                subject=subject,
                chapters=chapters,
                subtopics=subtopics,
            ):
                continue
            q_text = self._str(bank_row.get("question_text") or bank_row.get("question"))
            if not q_text:
                continue
            if self._looks_degraded_math_text(q_text):
                continue
            normalized_key = re.sub(r"\s+", " ", q_text).strip().lower()
            if not normalized_key or normalized_key in seen_questions:
                continue

            bank_subject = self._str(bank_row.get("subject"))
            bank_chapter_tags = self._resolve_import_row_chapter_tags(
                row=bank_row,
                subject_override=bank_subject,
                max_tags=3,
            )
            bank_chapter = (
                bank_chapter_tags[0]
                if bank_chapter_tags
                else self._resolve_import_row_chapter(
                    row=bank_row,
                    subject_override=bank_subject,
                )
                or self._str(bank_row.get("chapter"))
            )
            bank_track = self._infer_subject_track(
                subject=bank_subject,
                chapters=bank_chapter_tags[:2] if bank_chapter_tags else ([bank_chapter] if bank_chapter else []),
                subtopics=[bank_chapter] if bank_chapter else [],
                concept_tags=[],
            )
            if requested_track and bank_track != requested_track:
                continue
            if enforce_domain_alignment:
                row_domain = self._domain_key_from_context(
                    subject=bank_subject or subject,
                    concept_tags=[bank_chapter, q_text[:90]],
                )
                if row_domain != requested_domain:
                    continue

            raw_options = bank_row.get("options")
            option_texts: list[str] = []
            if isinstance(raw_options, list):
                for opt in raw_options[:4]:
                    if isinstance(opt, dict):
                        text = self._str(opt.get("text") or opt.get("value") or opt.get("option"))
                    else:
                        text = self._str(opt)
                    if text:
                        option_texts.append(text)
            if len(option_texts) < 4:
                option_texts = []

            answer_raw = ""
            correct = bank_row.get("correct_answer")
            if isinstance(correct, dict):
                answer_raw = self._str(
                    correct.get("single")
                    or correct.get("numerical")
                    or (
                        (correct.get("multiple") or [""])[0]
                        if isinstance(correct.get("multiple"), list)
                        else ""
                    )
                )
            if not answer_raw:
                answer_raw = self._str(
                    bank_row.get("answer")
                    or bank_row.get("correct_option")
                    or bank_row.get("_correct_option")
                    or bank_row.get("correct")
                    or bank_row.get("numerical_answer")
                    or bank_row.get("_numerical_answer")
                )
            if (
                not answer_raw
                and isinstance(bank_row.get("_correct_answers"), list)
                and bank_row.get("_correct_answers")
            ):
                answer_raw = self._str((bank_row.get("_correct_answers") or [""])[0])

            answer_token = (
                self._normalize_answer_token_to_label(answer_raw, option_texts)
                if option_texts
                else self._extract_answer_token(answer_raw)
            )
            if option_texts and answer_token not in {"A", "B", "C", "D"}:
                answer_token = ""
            if not option_texts and answer_token in {"A", "B", "C", "D"}:
                answer_token = ""

            source_origin = self._str(bank_row.get("source") or "teacher_import").lower()
            solution_stub = self._str(
                bank_row.get("solution_explanation")
                or bank_row.get("solution")
                or bank_row.get("source_solution_stub")
            )
            bag = " ".join(
                [
                    self._str(bank_row.get("difficulty")),
                    bank_subject,
                    bank_chapter,
                    " ".join(bank_chapter_tags),
                    q_text,
                    source_origin,
                    solution_stub,
                ]
            )
            scope_score = self._scope_match_score(bag, scope_tokens)
            pyq_score = self._pyq_signal_score(bag)
            if any(tok in bag.lower() for tok in pyq_scope_bonus_tokens):
                pyq_score = min(1.0, pyq_score + 0.25)
            hardness_score = self._hardness_signal_score(bag)
            if scope_tokens and scope_score <= 0.0:
                if len(scope_tokens) >= 2 or pyq_score < 0.75:
                    continue

            quality_score = (
                (scope_score * 0.60)
                + (pyq_score * 0.45)
                + (hardness_score * 0.20)
                + (0.30 if answer_token else -0.10)
                + (0.18 if source_origin == "teacher_import" else 0.0)
                + (0.12 if solution_stub else 0.0)
                + bank_quality_bonus
            )
            bank_id = self._str(bank_row.get("id") or bank_row.get("question_id")) or "bank"
            rows.append(
                {
                    "title": self._str(bank_chapter or bank_row.get("subject") or f"{subject} PYQ"),
                    "url": f"local://question_bank/{bank_id}"[:350],
                    "snippet": q_text[:360],
                    "query": "local_import_bank",
                    "scope_score": round(scope_score, 6),
                    "pyq_score": round(pyq_score, 6),
                    "hardness_score": round(hardness_score, 6),
                    "quality_score": round(quality_score, 6),
                    "question_text": q_text[:420],
                    "question_stub": q_text[:360],
                    "options": option_texts[:4],
                    "correct_answer": answer_token[:24],
                    "answer_stub": answer_token[:24],
                    "solution_stub": solution_stub[:420],
                    "has_answer": bool(answer_token),
                    "has_solution": bool(solution_stub),
                    "exam_type": "JEE",
                    "difficulty_estimate": 4,
                    "subject_ok": True,
                    "chapter_ok": True,
                    "source_provider": "local_pyq_import_bank",
                    "source_origin": source_origin[:64],
                    "chapter_tags": bank_chapter_tags[:3],
                    "bank_payload": dict(bank_row),
                    "verification_safe": not hard_block,
                }
            )
            seen_questions.add(normalized_key)
            if len(rows) >= max(8, limit * 5):
                break

        rows.sort(
            key=lambda row: (
                self._to_float(row.get("quality_score"), 0.0),
                1.0 if self._to_bool(row.get("has_answer")) else 0.0,
                1.0 if self._to_bool(row.get("has_solution")) else 0.0,
                self._to_float(row.get("scope_score"), 0.0),
            ),
            reverse=True,
        )
        return rows[: max(1, limit)]

    def _fetch_pyq_web_snippets(
        self,
        *,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
        query_suffix: str = "JEE Main Advanced PYQ hard",
        limit: int = 6,
        difficulty: int = 3,
        search_timeout_s: float = 6.0,
        page_timeout_s: float = 3.5,
        query_budget_override: int | None = None,
        page_check_budget: int | None = None,
    ) -> list[dict[str, Any]]:
        scope_tokens = self._pyq_scope_tokens(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
        )
        deduped_queries = self._build_pyq_query_variants(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
            query_suffix=query_suffix,
            ultra_hard=difficulty >= 5,
        )

        if not deduped_queries:
            self._last_pyq_web_diagnostics = {
                "query_attempts": 0,
                "queries_with_results": 0,
                "provider_hits": {},
                "provider_errors": {},
                "transports_used": [],
                "bot_challenge_hits": 0,
                "web_error_reason": "empty_query_plan",
                "attempts": [],
            }
            return []

        max_candidates = max(8, limit * 4)
        compact_web_budget = limit <= 4
        default_query_budget = 4 if compact_web_budget else (8 if difficulty >= 5 else 6)
        query_budget = min(
            len(deduped_queries),
            max(1, int(query_budget_override))
            if query_budget_override is not None
            else default_query_budget,
        )
        candidates: list[dict[str, Any]] = []
        seen_links: set[str] = set()
        query_diagnostics: list[dict[str, Any]] = []
        for query in deduped_queries[:query_budget]:
            rows, query_diag = self._search_rows_with_provider_fallback(
                query,
                max_rows=max_candidates,
                total_timeout_s=max(0.8, float(search_timeout_s)),
            )
            query_diagnostics.append(query_diag)
            for row in rows:
                url = self._str(row.get("url")).strip().lower()
                if not url or url in seen_links:
                    continue
                seen_links.add(url)
                title = self._str(row.get("title"))
                snippet = self._str(row.get("snippet"))
                combined = f"{title} {snippet} {url}"
                scope_score = self._scope_match_score(combined, scope_tokens)
                pyq_score = self._pyq_signal_score(combined)
                hardness_score = self._hardness_signal_score(combined)
                quality_score = (
                    (scope_score * 0.55)
                    + (pyq_score * 0.35)
                    + (hardness_score * 0.25)
                )
                if scope_score <= 0.0 and pyq_score < 0.30:
                    continue
                candidates.append(
                    {
                        "title": title[:220],
                        "url": self._str(row.get("url"))[:350],
                        "snippet": snippet[:360],
                        "query": query[:240],
                        "scope_score": round(scope_score, 6),
                        "pyq_score": round(pyq_score, 6),
                        "hardness_score": round(hardness_score, 6),
                        "quality_score": round(quality_score, 6),
                        "question_stub": "",
                        "answer_stub": "",
                        "solution_stub": "",
                        "has_answer": False,
                        "has_solution": False,
                    }
                )
            if len(candidates) >= max_candidates:
                break

        merged_diag = self._merge_pyq_web_diagnostics(query_diagnostics)
        merged_diag["query_plan_size"] = len(deduped_queries)
        merged_diag["query_budget"] = query_budget
        local_archive_rows = self._local_pyq_archive_rows(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
            limit=max(4, limit * 2),
        )
        if local_archive_rows:
            seen_candidate_keys = {
                (
                    self._str(row.get("url")).strip().lower(),
                    self._str(row.get("question_stub")).strip().lower(),
                )
                for row in candidates
            }
            appended = 0
            for row in local_archive_rows:
                key = (
                    self._str(row.get("url")).strip().lower(),
                    self._str(row.get("question_stub")).strip().lower(),
                )
                if key in seen_candidate_keys:
                    continue
                candidates.append(dict(row))
                seen_candidate_keys.add(key)
                appended += 1
            merged_diag["local_archive_count"] = appended
            if merged_diag.get("queries_with_results", 0) <= 0 and appended > 0:
                merged_diag["fallback_source"] = "local_pyq_archive"
        merged_diag["candidate_count"] = len(candidates)
        self._last_pyq_web_diagnostics = merged_diag

        if not candidates:
            return []
        candidates.sort(
            key=lambda row: (
                self._to_float(row.get("quality_score"), 0.0),
                self._to_float(row.get("hardness_score"), 0.0),
                self._to_float(row.get("scope_score"), 0.0),
            ),
            reverse=True,
        )

        default_page_checks = max(2, min(4 if compact_web_budget else 8, limit + 2))
        page_checks = max(
            1,
            min(
                len(candidates),
                page_check_budget
                if page_check_budget is not None
                else default_page_checks,
            ),
        )
        enriched: list[dict[str, Any]] = []
        for row in candidates[:page_checks]:
            enriched_row = dict(row)
            source_provider = self._str(row.get("source_provider")).lower()
            url_token = self._str(row.get("url")).strip().lower()
            evidence: dict[str, Any] = {}
            if source_provider != "local_pyq_archive" and not url_token.startswith(
                "local://"
            ):
                evidence = self._extract_pyq_page_evidence(
                    self._str(row.get("url")),
                    scope_tokens=scope_tokens,
                    timeout_s=max(0.8, float(page_timeout_s)),
                )
            if evidence:
                for key, value in evidence.items():
                    enriched_row[key] = value
                bonus = 0.0
                if self._to_bool(evidence.get("has_answer")):
                    bonus += 0.18
                if self._to_bool(evidence.get("has_solution")):
                    bonus += 0.18
                bonus += self._to_float(evidence.get("scope_score_page"), 0.0) * 0.18
                enriched_row["quality_score"] = round(
                    self._to_float(row.get("quality_score"), 0.0) + bonus,
                    6,
                )
                enriched_row["scope_score"] = round(
                    max(
                        self._to_float(row.get("scope_score"), 0.0),
                        self._to_float(evidence.get("scope_score_page"), 0.0),
                    ),
                    6,
                )
            q_text = self._str(
                enriched_row.get("question_text") or enriched_row.get("question_stub")
            )
            q_options = (
                enriched_row.get("options")
                if isinstance(enriched_row.get("options"), list)
                else []
            )
            q_solution = self._str(enriched_row.get("solution_stub"))
            q_answer = self._str(
                enriched_row.get("correct_answer") or enriched_row.get("answer_stub")
            )
            exam_type = self._str(enriched_row.get("exam_type"))
            difficulty_score = self._pyq_difficulty_score(
                question_text=q_text,
                solution_text=q_solution,
                options=[self._str(x) for x in q_options],
                scope_score=self._to_float(enriched_row.get("scope_score"), 0.0),
                exam_type=exam_type,
                answer_token=q_answer,
            )
            classification = self._classify_pyq_candidate(
                text_bag=f"{q_text} {q_solution} {self._str(enriched_row.get('title'))}",
                subject=subject,
                chapters=chapters,
                subtopics=subtopics,
                options=[self._str(x) for x in q_options],
                difficulty_score=difficulty_score,
            )
            for key, value in classification.items():
                enriched_row[key] = value
            enriched_row["difficulty_score"] = difficulty_score
            if self._to_bool(classification.get("subject_ok")) and self._to_bool(
                classification.get("chapter_ok")
            ):
                enriched_row["quality_score"] = round(
                    self._to_float(enriched_row.get("quality_score"), 0.0)
                    + min(0.32, difficulty_score / 180.0),
                    6,
                )
            else:
                enriched_row["quality_score"] = round(
                    self._to_float(enriched_row.get("quality_score"), 0.0) * 0.45,
                    6,
                )
            enriched.append(enriched_row)

        if page_checks < len(candidates):
            enriched.extend(candidates[page_checks:])

        enriched.sort(
            key=lambda row: (
                self._to_float(row.get("quality_score"), 0.0),
                self._to_float(row.get("difficulty_score"), 0.0),
                self._to_float(row.get("hardness_score"), 0.0),
                self._to_float(row.get("scope_score"), 0.0),
                1.0 if self._to_bool(row.get("has_answer")) else 0.0,
                1.0 if self._to_bool(row.get("has_solution")) else 0.0,
            ),
            reverse=True,
        )
        hard_threshold = 62.0 if difficulty >= 5 else (50.0 if difficulty >= 4 else 36.0)
        verified = [
            row
            for row in enriched
            if self._to_bool(row.get("subject_ok"))
            and self._to_bool(row.get("chapter_ok"))
            and self._to_float(row.get("difficulty_score"), 0.0) >= hard_threshold
        ]
        if verified:
            return verified[: max(1, limit)]
        fallback_verified = [
            row
            for row in enriched
            if self._to_bool(row.get("subject_ok"))
            and self._to_bool(row.get("chapter_ok"))
        ]
        if fallback_verified:
            return fallback_verified[: max(1, limit)]
        return enriched[: max(1, limit)]

    def _question_from_chapter_template(
        self,
        *,
        idx: int,
        subject: str,
        concept_tags: list[str],
        difficulty: int,
        trap_intensity: str,
        cross_concept: bool,
        seed_key: str,
        forced_question_type: str = "",
    ) -> dict[str, Any]:
        domain = self._domain_key_from_context(
            subject=subject,
            concept_tags=concept_tags,
        )
        if domain == "math_binomial":
            base = self._question_from_binomial_template(
                idx=idx,
                subject=subject,
                concept_tags=concept_tags,
                difficulty=difficulty,
                trap_intensity=trap_intensity,
                cross_concept=cross_concept,
                seed_key=seed_key,
            )
            return self._coerce_generated_question_type(
                base,
                target_type=forced_question_type or "MCQ_SINGLE",
                seed_key=seed_key,
            )
        if domain == "math_combinatorics":
            base = self._question_from_combinatorics_template(
                idx=idx,
                subject=subject,
                concept_tags=concept_tags,
                difficulty=difficulty,
                trap_intensity=trap_intensity,
                cross_concept=cross_concept,
                seed_key=seed_key,
            )
            return self._coerce_generated_question_type(
                base,
                target_type=forced_question_type or "MCQ_SINGLE",
                seed_key=seed_key,
            )

        rng = self._seeded_random(seed_key)
        chapter_hint = self._str(concept_tags[0] if concept_tags else subject) or subject

        question_text = ""
        explanation = ""
        tag = chapter_hint
        options: list[str] = []
        correct_letter = "A"

        if domain == "math_algebra":
            chapter_low = chapter_hint.lower()
            if any(
                k in chapter_low
                for k in (
                    "complex",
                    "argand",
                    "de moivre",
                    "polar",
                    "modulus",
                    "argument",
                    "conjugate",
                    "imaginary",
                    "real part",
                    "locus",
                )
            ):
                complex_template = idx % 6
                tag = "Complex Numbers"
                if complex_template == 0:
                    a = rng.randint(2, 12)
                    b = rng.randint(2, 12)
                    value = (a * a) + (b * b)
                    opts, correct_letter = self._finalize_numeric_options(
                        correct_value=value,
                        distractors=[
                            abs(a * a - b * b),
                            (a + b) * (a + b),
                            (a * b) + a + b,
                        ],
                        rng=rng,
                    )
                    options = opts
                    question_text = (
                        f"In '{chapter_hint}', if $z={a}+{b}i$, then $|z|^2$ equals:"
                    )
                    explanation = (
                        f"$|z|^2=z\bar z={a}^2+{b}^2={a*a}+{b*b}={value}$."
                    )
                elif complex_template == 1:
                    a = rng.randint(2, 7)
                    b = rng.randint(2, 7)
                    c = rng.randint(3, 8)
                    d = rng.randint(2, 6)
                    value = (a * c) + (b * d)
                    opts, correct_letter = self._finalize_numeric_options(
                        correct_value=value,
                        distractors=[
                            abs((a * c) - (b * d)),
                            (a * d) + (b * c),
                            (a + b + c + d),
                        ],
                        rng=rng,
                    )
                    options = opts
                    question_text = (
                        f"In '{chapter_hint}', the real part of "
                        f"$({a}+{b}i)({c}-{d}i)$ is:"
                    )
                    explanation = (
                        f"$({a}+{b}i)({c}-{d}i)=({a*c}+{b*d})+({b*c}-{a*d})i$, "
                        f"so real part is {value}."
                    )
                elif complex_template == 2:
                    m = rng.randint(2, 9)
                    n = rng.randint(2, 9)
                    value = m * n
                    opts, correct_letter = self._finalize_numeric_options(
                        correct_value=value,
                        distractors=[
                            abs(m - n),
                            m + n,
                            (m * m) + (n * n),
                        ],
                        rng=rng,
                    )
                    options = opts
                    question_text = (
                        f"In '{chapter_hint}', if $|z_1|={m}$ and $|z_2|={n}$, then "
                        "$|z_1z_2|$ equals:"
                    )
                    explanation = (
                        f"Modulus property gives $|z_1z_2|=|z_1||z_2|={m}\cdot{n}={value}$."
                    )
                elif complex_template == 3:
                    theta = rng.choice([30, 36, 45, 60, 72])
                    n = rng.randint(2, 7)
                    value = (theta * n) % 360
                    if value == 0:
                        value = 360
                    opts, correct_letter = self._finalize_numeric_options(
                        correct_value=value,
                        distractors=[
                            (theta * (n - 1)) % 360 or 360,
                            (theta * (n + 1)) % 360 or 360,
                            ((360 - value) % 360) or 360,
                        ],
                        rng=rng,
                    )
                    options = opts
                    question_text = (
                        f"In '{chapter_hint}', for "
                        f"$z=(\cos {theta}^\circ+i\sin {theta}^\circ)^{{{n}}}$, "
                        "the argument in degrees is:"
                    )
                    explanation = (
                        "By De Moivre, argument multiplies by power, so "
                        f"$\arg(z)={n}\cdot {theta}^\circ={value}^\circ$ (mod $360^\circ$)."
                    )
                elif complex_template == 4:
                    n = rng.randint(21, 90)
                    cycle = n % 4
                    value = "1" if cycle == 0 else ("i" if cycle == 1 else ("-1" if cycle == 2 else "-i"))
                    options, correct_letter = self._finalize_text_options(
                        correct_text=f"${value}$",
                        distractors=["$1$", "$-1$", "$i$", "$-i$"],
                        rng=rng,
                    )
                    question_text = f"In '{chapter_hint}', evaluate $i^{{{n}}}$."
                    explanation = (
                        f"Powers of $i$ repeat every 4. Since {n}\equiv {cycle} \pmod 4, "
                        f"$i^{{{n}}}={value}$."
                    )
                else:
                    a, b, value = rng.choice(
                        [(3, 4, 5), (5, 12, 13), (8, 15, 17), (7, 24, 25)]
                    )
                    opts, correct_letter = self._finalize_numeric_options(
                        correct_value=value,
                        distractors=[
                            a + b,
                            abs(a - b),
                            (a * a) + (b * b),
                        ],
                        rng=rng,
                    )
                    options = opts
                    question_text = (
                        f"In '{chapter_hint}', if $z={a}-{b}i$, then $|z|$ equals:"
                    )
                    explanation = (
                        f"$|z|=\sqrt{{{a}^2+{b}^2}}=\sqrt{{{a*a}+{b*b}}}={value}$."
                    )
            elif any(
                k in chapter_low
                for k in ("set", "relation", "function", "domain", "range")
            ):
                set_template = idx % 2
                tag = "Sets, Relations and Functions"
                if set_template == 0:
                    a_count = rng.randint(14, 30)
                    b_count = rng.randint(10, 24)
                    inter = rng.randint(4, min(a_count, b_count) - 2)
                    value = a_count + b_count - inter
                    opts, correct_letter = self._finalize_numeric_options(
                        correct_value=value,
                        distractors=[
                            a_count + b_count,
                            abs(a_count - b_count),
                            inter,
                        ],
                        rng=rng,
                    )
                    options = opts
                    question_text = (
                        f"In '{chapter_hint}', if $n(A)={a_count}$, $n(B)={b_count}$ and "
                        f"$n(A\\cap B)={inter}$, then $n(A\\cup B)$ equals:"
                    )
                    explanation = (
                        f"Use inclusion-exclusion: $n(A\\cup B)=n(A)+n(B)-n(A\\cap B)="
                        f"{a_count}+{b_count}-{inter}={value}$."
                    )
                else:
                    p = rng.randint(2, 6)
                    q = rng.randint(-4, 5)
                    r = rng.randint(2, 6)
                    s = rng.randint(-5, 6)
                    value = r * (p + q) + s
                    opts, correct_letter = self._finalize_numeric_options(
                        correct_value=value,
                        distractors=[
                            p * r + q + s,
                            r * (p - q) + s,
                            p + q + r + s,
                        ],
                        rng=rng,
                    )
                    options = opts
                    question_text = (
                        f"In '{chapter_hint}', let $f(x)={p}x{self._sign(q)}{abs(q)}$ and "
                        f"$g(x)={r}x{self._sign(s)}{abs(s)}$. Find $(g\\circ f)(1)$."
                    )
                    explanation = (
                        f"First $f(1)={p}{self._sign(q)}{abs(q)}={p+q}$. Then "
                        f"$(g\\circ f)(1)=g(f(1))={r}({p+q}){self._sign(s)}{abs(s)}={value}$."
                    )
            elif any(k in chapter_low for k in ("matrix", "determinant")):
                a = rng.randint(-5, 6)
                b = rng.randint(-5, 6)
                c = rng.randint(-5, 6)
                d = rng.randint(-5, 6)
                det = (a * d) - (b * c)
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=det,
                    distractors=[
                        (a * d) + (b * c),
                        (a + b + c + d),
                        (a * c) - (b * d),
                    ],
                    rng=rng,
                )
                options = opts
                tag = "Matrices and Determinants"
                question_text = (
                    f"In '{chapter_hint}', for matrix "
                    f"$A=\\begin{{bmatrix}}{a}&{b}\\\\{c}&{d}\\end{{bmatrix}}$, find $\\det(A)$."
                )
                explanation = (
                    f"For $\\begin{{bmatrix}}a&b\\\\c&d\\end{{bmatrix}}$, "
                    f"$\\det(A)=ad-bc={a}({d})-{b}({c})={det}$."
                )
            elif any(
                k in chapter_low
                for k in ("sequence", "series", "ap", "g.p", "gp", "a.p", "progression")
            ):
                a1 = rng.randint(2, 12)
                diff = rng.randint(1, 6)
                n = rng.randint(6, 14)
                s_n = (n * (2 * a1 + (n - 1) * diff)) // 2
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=s_n,
                    distractors=[
                        n * (a1 + diff),
                        (n * (2 * a1 + n * diff)) // 2,
                        a1 + (n - 1) * diff,
                    ],
                    rng=rng,
                )
                options = opts
                tag = "Sequence and Series"
                question_text = (
                    f"In '{chapter_hint}', an A.P. has first term {a1}, common difference {diff}, "
                    f"and {n} terms. Find the sum $S_{{{n}}}$."
                )
                explanation = (
                    f"$S_n=\\frac{{n}}{{2}}[2a+(n-1)d]=\\frac{{{n}}}{{2}}"
                    f"[2({a1})+({n}-1)({diff})]={s_n}$."
                )
            elif any(k in chapter_low for k in ("quadratic", "roots")):
                p = rng.randint(2, 10)
                q = rng.randint(-18, 18)
                value = p * p - (2 * q)
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=value,
                    distractors=[p * p + (2 * q), abs(q), (2 * p) + q],
                    rng=rng,
                )
                options = opts
                tag = "Quadratic Equations"
                question_text = (
                    f"For chapter '{chapter_hint}', if roots of "
                    f"$x^2-{p}x{self._sign(q)}{abs(q)}=0$ are $\\alpha,\\beta$, find $\\alpha^2+\\beta^2$."
                )
                explanation = (
                    f"$\\alpha+\\beta={p},\\ \\alpha\\beta={q}$, so "
                    f"$\\alpha^2+\\beta^2=(\\alpha+\\beta)^2-2\\alpha\\beta={p*p}-2({q})={value}$."
                )
            else:
                a = rng.randint(2, 8)
                b = rng.randint(-6, 6)
                c = rng.randint(-5, 5)
                slope = 3 * a + 2 * b + c
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=slope,
                    distractors=[3 * a + b + c, a + 2 * b + c, 3 * a - 2 * b + c],
                    rng=rng,
                )
                options = opts
                tag = "Algebra"
                question_text = (
                    f"For chapter '{chapter_hint}', consider $f(x)={a}x^3"
                    f"{self._sign(b)}{abs(b)}x^2{self._sign(c)}{abs(c)}x+1$. "
                    "Find $f'(1)$."
                )
                explanation = (
                    f"$f'(x)={3*a}x^2{self._sign(2*b)}{abs(2*b)}x{self._sign(c)}{abs(c)}$, "
                    f"so $f'(1)={slope}$."
                )
        elif domain == "math_calculus":
            chapter_low = chapter_hint.lower()
            if any(k in chapter_low for k in ("limit", "continuity")):
                p = rng.randint(2, 11)
                q = rng.randint(-6, 6)
                value = p
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=value,
                    distractors=[p + q, abs(q), p - q],
                    rng=rng,
                )
                options = opts
                tag = "Limits and Continuity"
                question_text = (
                    f"In '{chapter_hint}', find the limit "
                    f"$\\lim_{{x\\to 1}} \\frac{{({p}x{self._sign(q)}{abs(q)})-({p + q})}}{{x-1}}$."
                )
                explanation = (
                    f"Numerator is ${p}(x-1)$, so limit equals coefficient of $(x-1)$, i.e. ${p}$."
                )
            elif any(
                k in chapter_low
                for k in (
                    "differential equation",
                    "differential equations",
                    "dy/dx",
                    "integrating factor",
                    "first order linear",
                )
            ):
                a = rng.randint(1, 5)
                y0 = rng.choice([6, 8, 10, 12, 14, 16, 18, 20])
                value = y0 // 2
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=value,
                    distractors=[y0, max(1, y0 // 4), y0 * 2],
                    rng=rng,
                )
                options = opts
                tag = "Differential Equations"
                question_text = (
                    f"In '{chapter_hint}', solve $\\dfrac{{dy}}{{dx}}+{a}y=0$ with $y(0)={y0}$. "
                    f"Find $y\\!\\left(\\dfrac{{\\ln 2}}{{{a}}}\\right)$."
                )
                explanation = (
                    f"Solution is $y={y0}e^{{-{a}x}}$. At $x=\\ln 2/{a}$, "
                    f"$y={y0}e^{{-\\ln 2}}={y0}/2={value}$."
                )
            elif any(
                k in chapter_low
                for k in ("differentiat", "derivative", "maxima", "minima", "application")
            ):
                a = rng.randint(2, 7)
                b = rng.randint(-5, 5)
                c = rng.randint(-6, 6)
                slope = 3 * a + 2 * b + c
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=slope,
                    distractors=[3 * a + b + c, a + 2 * b + c, 3 * a - 2 * b + c],
                    rng=rng,
                )
                options = opts
                tag = "Differentiation"
                question_text = (
                    f"In '{chapter_hint}', for $f(x)={a}x^3{self._sign(b)}{abs(b)}x^2"
                    f"{self._sign(c)}{abs(c)}x+1$, find the derivative value $f'(1)$."
                )
                explanation = (
                    f"$f'(x)={3*a}x^2{self._sign(2*b)}{abs(2*b)}x{self._sign(c)}{abs(c)}$, "
                    f"thus $f'(1)={slope}$."
                )
            else:
                p = rng.randint(2, 7)
                q = rng.randint(2, 8)
                r = rng.randint(1, 5)
                value = (2 * p) + (3 * q) + (6 * r)
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=value,
                    distractors=[
                        p + q + r,
                        (2 * p) + (2 * q) + (6 * r),
                        (2 * p) + (3 * q) + (3 * r),
                    ],
                    rng=rng,
                )
                options = opts
                tag = "Integral Calculus"
                question_text = (
                    f"In '{chapter_hint}', evaluate "
                    f"$\\int_0^1 ({p}x^2+{q}x+{r})\\,dx$ multiplied by $6$."
                )
                explanation = (
                    f"$\\int_0^1 ({p}x^2+{q}x+{r})dx={p}/3+{q}/2+{r}$, "
                    f"so multiplying by 6 gives "
                    f"$6\\left({p}/3+{q}/2+{r}\\right)={2*p}+{3*q}+{6*r}={value}$."
                )
        elif domain == "math_coordinate":
            x1, y1 = rng.randint(-5, 5), rng.randint(-5, 5)
            x2, y2 = x1 + rng.randint(2, 8), y1 + rng.randint(1, 7)
            d2 = (x2 - x1) ** 2 + (y2 - y1) ** 2
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=d2,
                distractors=[abs(x2 - x1) + abs(y2 - y1), (x2 - x1) ** 2, (y2 - y1) ** 2],
                rng=rng,
            )
            options = opts
            tag = "Coordinate Geometry"
            question_text = (
                f"For '{chapter_hint}', if $A({x1},{y1})$ and $B({x2},{y2})$, "
                "find $AB^2$."
            )
            explanation = (
                f"$AB^2=(x_2-x_1)^2+(y_2-y_1)^2={d2}$."
            )
        elif domain == "math_vector3d":
            pattern = (idx + rng.randint(0, 5)) % 6
            if pattern == 0:
                a1, a2, a3 = rng.randint(-4, 6), rng.randint(-5, 5), rng.randint(-3, 7)
                b1, b2, b3 = rng.randint(-6, 4), rng.randint(-4, 6), rng.randint(-5, 5)
                dot = a1 * b1 + a2 * b2 + a3 * b3
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=dot,
                    distractors=[
                        a1 * b1 - a2 * b2 + a3 * b3,
                        abs(dot),
                        a1 + b1 + a2 + b2 + a3 + b3,
                    ],
                    rng=rng,
                )
                options = opts
                tag = "Vector Algebra"
                question_text = (
                    f"In '{chapter_hint}', let "
                    f"$\\vec a=\\langle {a1},{a2},{a3}\\rangle$ and "
                    f"$\\vec b=\\langle {b1},{b2},{b3}\\rangle$. Find $\\vec a\\cdot\\vec b$."
                )
                explanation = f"Dot product = {a1}({b1})+{a2}({b2})+{a3}({b3})={dot}."
            elif pattern == 1:
                a1, a2, a3 = rng.randint(-4, 5), rng.randint(-4, 5), rng.randint(-4, 5)
                b1, b2, b3 = rng.randint(-4, 5), rng.randint(-4, 5), rng.randint(-4, 5)
                c1 = a2 * b3 - a3 * b2
                c2 = a3 * b1 - a1 * b3
                c3 = a1 * b2 - a2 * b1
                cross_sq = c1 * c1 + c2 * c2 + c3 * c3
                dot = a1 * b1 + a2 * b2 + a3 * b3
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=cross_sq,
                    distractors=[
                        abs(dot),
                        abs(c1) + abs(c2) + abs(c3),
                        (a1 * a1 + a2 * a2 + a3 * a3),
                    ],
                    rng=rng,
                )
                options = opts
                tag = "Vector Algebra"
                question_text = (
                    f"In '{chapter_hint}', let "
                    f"$\\vec a=\\langle {a1},{a2},{a3}\\rangle$ and "
                    f"$\\vec b=\\langle {b1},{b2},{b3}\\rangle$. Find $|\\vec a\\times\\vec b|^2$."
                )
                explanation = (
                    "Use $|\\vec a\\times\\vec b|^2=(a_2b_3-a_3b_2)^2+(a_3b_1-a_1b_3)^2+"
                    f"(a_1b_2-a_2b_1)^2={cross_sq}$."
                )
            elif pattern == 2:
                x1, y1, z1 = rng.randint(-5, 5), rng.randint(-5, 5), rng.randint(-5, 5)
                dx, dy, dz = rng.randint(1, 6), rng.randint(1, 6), rng.randint(1, 6)
                x2, y2, z2 = x1 + dx, y1 + dy, z1 + dz
                d2 = dx * dx + dy * dy + dz * dz
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=d2,
                    distractors=[
                        abs(dx) + abs(dy) + abs(dz),
                        dx * dx + dy * dy,
                        dy * dy + dz * dz,
                    ],
                    rng=rng,
                )
                options = opts
                tag = "Line in 3D"
                question_text = (
                    f"In '{chapter_hint}', points are $P({x1},{y1},{z1})$ and "
                    f"$Q({x2},{y2},{z2})$. Find $|PQ|^2$."
                )
                explanation = (
                    f"$|PQ|^2=({x2}-{x1})^2+({y2}-{y1})^2+({z2}-{z1})^2={d2}$."
                )
            elif pattern == 3:
                a, b, c, norm_root = rng.choice(
                    [(1, 2, 2, 3), (2, 3, 6, 7), (1, 4, 8, 9), (3, 4, 12, 13)]
                )
                x0, y0, z0 = rng.randint(-4, 4), rng.randint(-4, 4), rng.randint(-4, 4)
                k = rng.randint(1, 5)
                d = norm_root * k - (a * x0 + b * y0 + c * z0)
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=k,
                    distractors=[k + 1, max(0, k - 1), k + 2],
                    rng=rng,
                )
                options = opts
                tag = "Plane in 3D"
                question_text = (
                    f"In '{chapter_hint}', find the perpendicular distance of "
                    f"$P({x0},{y0},{z0})$ from plane "
                    f"${a}x{self._sign(b)}{abs(b)}y{self._sign(c)}{abs(c)}z{self._sign(d)}{abs(d)}=0$."
                )
                explanation = (
                    "Distance from point to plane is "
                    f"$\\frac{{|ax_0+by_0+cz_0+d|}}{{\\sqrt{{a^2+b^2+c^2}}}}={k}$."
                )
            elif pattern == 4:
                l, m, n = rng.randint(-4, 6), rng.randint(-4, 6), rng.randint(-4, 6)
                a, b, c = rng.randint(-4, 6), rng.randint(-4, 6), rng.randint(-4, 6)
                scalar = a * l + b * m + c * n
                sq = scalar * scalar
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=sq,
                    distractors=[
                        abs(scalar),
                        l * l + m * m + n * n,
                        a * a + b * b + c * c,
                    ],
                    rng=rng,
                )
                options = opts
                tag = "Angle between Line and Plane"
                question_text = (
                    f"In '{chapter_hint}', a line has direction ratios "
                    f"$\\langle {l},{m},{n}\\rangle$ and a plane has normal "
                    f"$\\langle {a},{b},{c}\\rangle$. Find $(al+bm+cn)^2$."
                )
                explanation = (
                    f"Compute $al+bm+cn={a}({l})+{b}({m})+{c}({n})={scalar}$, "
                    f"so required value is ${sq}$."
                )
            else:
                p, q, r = rng.randint(1, 6), rng.randint(1, 6), rng.randint(1, 6)
                scale = p * p + q * q + r * r
                answer = p * p
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=answer,
                    distractors=[q * q, r * r, scale],
                    rng=rng,
                )
                options = opts
                tag = "Direction Cosines and Direction Ratios"
                question_text = (
                    f"In '{chapter_hint}', a line has direction ratios "
                    f"$({p},{q},{r})$. If $l$ is direction cosine with x-axis, find "
                    f"$({scale})l^2$."
                )
                explanation = (
                    f"$l=\\frac{{{p}}}{{\\sqrt{{{scale}}}}}$, so "
                    f"$({scale})l^2={p*p}={answer}$."
                )
        elif domain == "math_probability":
            r = rng.randint(5, 9)
            b = rng.randint(4, 8)
            ways = self._ncr(r, 2) * self._ncr(b, 1)
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=ways,
                distractors=[self._ncr(r + b, 3), self._ncr(r, 3), self._ncr(b, 3)],
                rng=rng,
            )
            options = opts
            tag = "Probability"
            question_text = (
                f"For '{chapter_hint}', in a probability experiment an urn has ${r}$ red and ${b}$ blue balls. "
                "In how many ways can 3 balls be drawn with exactly 2 red?"
            )
            explanation = f"$\\binom{{{r}}}{{2}}\\binom{{{b}}}{{1}}={ways}$."
        elif domain == "math_trigonometry":
            p = rng.randint(2, 7)
            q = rng.randint(2, 7)
            value = p * q
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=value,
                distractors=[p + q, p * p + q * q, abs(p - q)],
                rng=rng,
            )
            options = opts
            tag = "Trigonometry"
            question_text = (
                f"In '{chapter_hint}', if $\\tan\\theta=\\frac{{{p}}}{{{q}}}$ "
                f"for acute $\\theta$, find $({p*p + q*q})\\sin\\theta\\cos\\theta$."
            )
            explanation = (
                f"$\\sin\\theta\\cos\\theta=\\frac{{pq}}{{p^2+q^2}}$, so value is {value}."
            )
        elif domain == "physics_mechanics":
            u = rng.randint(4, 18)
            a = rng.randint(1, 7)
            t = rng.randint(2, 9)
            s2 = 2 * u * t + a * t * t
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=s2,
                distractors=[u * t + a * t * t, 2 * u * t, a * t * t],
                rng=rng,
            )
            options = opts
            tag = "Mechanics"
            question_text = (
                f"For '{chapter_hint}', a particle has $u={u}\\,\\mathrm{{m/s}}$, "
                f"$a={a}\\,\\mathrm{{m/s^2}}$, $t={t}\\,\\mathrm{{s}}$. Compute $2s$."
            )
            explanation = f"$s=ut+\\tfrac12at^2\\Rightarrow 2s=2ut+at^2={s2}$."
        elif domain == "physics_thermal":
            t1 = rng.randint(250, 420)
            p_ratio = rng.randint(2, 5)
            t2 = t1 * p_ratio
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=t2,
                distractors=[t1 + p_ratio, t1 // p_ratio, t1 * (p_ratio - 1)],
                rng=rng,
            )
            options = opts
            tag = "Thermodynamics"
            question_text = (
                f"In '{chapter_hint}', ideal gas at constant volume has "
                f"$T_1={t1}\\,\\mathrm{{K}}$. If pressure becomes {p_ratio} times, find $T_2$."
            )
            explanation = f"At constant volume, $P\\propto T$, so $T_2={p_ratio}T_1={t2}$ K."
        elif domain == "physics_waves":
            v = rng.randint(180, 360)
            f = rng.choice([3, 4, 5, 6, 8, 9, 10, 12])
            lam = v // f
            v = lam * f
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=lam,
                distractors=[v * f, v + f, max(1, v - f)],
                rng=rng,
            )
            options = opts
            tag = "Waves"
            question_text = (
                f"In '{chapter_hint}', wave speed is $v={v}\\,\\mathrm{{m/s}}$ and "
                f"frequency is $f={f}\\,\\mathrm{{Hz}}$. Find wavelength $\\lambda$."
            )
            explanation = f"$\\lambda=v/f={v}/{f}={lam}$ m."
        elif domain == "physics_electric":
            r1 = rng.randint(2, 8)
            r2 = rng.randint(3, 12)
            r3 = rng.randint(2, 8)
            req = (r1 * r2) / (r1 + r2) + r3
            scaled = int(round(req * (r1 + r2)))
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=scaled,
                distractors=[int((r1 + r2 + r3) * (r1 + r2)), int(r1 * r2 + r3), int((r1 + r2) * r3)],
                rng=rng,
            )
            options = opts
            tag = "Current Electricity"
            question_text = (
                f"For '{chapter_hint}', $R_1={r1}\\Omega$ and $R_2={r2}\\Omega$ are in parallel, "
                f"then in series with $R_3={r3}\\Omega$. Compute $R_\\mathrm{{eq}}({r1+r2})$."
            )
            explanation = (
                f"$R_p=\\frac{{R_1R_2}}{{R_1+R_2}}$, so "
                f"$R_\\mathrm{{eq}}=R_p+R_3$. Thus scaled value is {scaled}."
            )
        elif domain == "physics_magnetism":
            b = rng.randint(1, 5)
            l = rng.randint(2, 8)
            v = rng.randint(3, 10)
            emf = b * l * v
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=emf,
                distractors=[b + l + v, b * v, l * v],
                rng=rng,
            )
            options = opts
            tag = "Electromagnetism"
            question_text = (
                f"For '{chapter_hint}', a conductor of length ${l}$ m moves "
                f"with speed ${v}$ m/s perpendicular to magnetic field ${b}$ T. "
                "Find induced emf (in V)."
            )
            explanation = f"$\\mathcal{{E}}=Blv={b}\\cdot{l}\\cdot{v}={emf}$."
        elif domain == "physics_optics":
            u = rng.choice([12, 15, 18, 20, 24, 30])
            v = rng.choice([12, 15, 18, 20, 24, 30])
            f_num = u * v
            f_den = u + v
            f = f_num // f_den
            if f * f_den != f_num:
                u, v = 30, 15
                f = 10
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=f,
                distractors=[u + v, abs(u - v), (u * v) // max(1, abs(u - v))],
                rng=rng,
            )
            options = opts
            tag = "Optics"
            question_text = (
                f"For '{chapter_hint}', an object and real image distances are "
                f"${u}$ cm and ${v}$ cm from a thin lens (magnitude form). Find focal length."
            )
            explanation = f"$1/f=1/u+1/v\\Rightarrow f=\\frac{{uv}}{{u+v}}={f}$ cm."
        elif domain == "physics_modern":
            h = rng.randint(4, 9)
            f0 = rng.randint(3, h - 1)
            vmax = h - f0
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=vmax,
                distractors=[h + f0, h * f0, abs(h - 2 * f0)],
                rng=rng,
            )
            options = opts
            tag = "Modern Physics"
            question_text = (
                f"In '{chapter_hint}', for photoelectric effect "
                f"$hf={h}e\\,\\mathrm{{V}}$ and work function $\\phi={f0}e\\,\\mathrm{{V}}$. "
                "Find stopping potential."
            )
            explanation = f"$eV_0=hf-\\phi\\Rightarrow V_0={h}-{f0}={vmax}$ V."
        elif domain == "chemistry_physical":
            mass = rng.randint(18, 180)
            molar = rng.choice([18, 24, 30, 36, 40, 44, 60, 90])
            moles = mass / molar
            scaled = int(round(moles * 100))
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=scaled,
                distractors=[mass + molar, max(1, mass - molar), mass * molar],
                rng=rng,
            )
            options = opts
            tag = "Physical Chemistry"
            question_text = (
                f"In '{chapter_hint}', a sample has mass ${mass}$ g and molar mass "
                f"${molar}$ g/mol. Compute $100\\times n$ (where $n$ is moles)."
            )
            explanation = f"$n=m/M={mass}/{molar}$, so $100n={scaled}$."
        elif domain == "chemistry_electro":
            electro_template = (idx + rng.randint(0, 5)) % 4
            tag = "Oxidation / Electrochemistry"
            if electro_template == 0:
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=6,
                    distractors=[4, 5, 7],
                    rng=rng,
                )
                options = opts
                question_text = (
                    f"For '{chapter_hint}', in $K_2Cr_2O_7$, find oxidation state of chromium."
                )
                explanation = "Let Cr = x: $2(+1)+2x+7(-2)=0\\Rightarrow x=+6$."
            elif electro_template == 1:
                e_cath = rng.randint(65, 165) / 100.0
                e_anode = rng.randint(-90, 35) / 100.0
                e_cell = round(e_cath - e_anode, 2)
                scaled = int(round(e_cell * 100))
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=scaled,
                    distractors=[
                        int(round((e_cath + e_anode) * 100)),
                        int(round(e_cath * 100)),
                        int(round(abs(e_anode) * 100)),
                    ],
                    rng=rng,
                )
                options = opts
                question_text = (
                    f"For '{chapter_hint}', a galvanic cell has "
                    f"$E^\\circ_\\text{{cathode}}={e_cath:.2f}\\,\\text{{V}}$ and "
                    f"$E^\\circ_\\text{{anode}}={e_anode:.2f}\\,\\text{{V}}$. Find $100E^\\circ_\\text{{cell}}$."
                )
                explanation = (
                    f"$E^\\circ_\\text{{cell}}=E^\\circ_\\text{{cathode}}-E^\\circ_\\text{{anode}}="
                    f"{e_cath:.2f}-({e_anode:.2f})={e_cell:.2f}\\,\\text{{V}}$, so $100E^\\circ={scaled}$."
                )
            elif electro_template == 2:
                e0 = rng.randint(105, 175) / 100.0
                n = rng.choice([1, 2, 2, 3])
                log_q = rng.choice([1, 1, 2])
                drop = (0.059 * log_q) / n
                e_val = round(e0 - drop, 3)
                scaled = int(round(e_val * 1000))
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=scaled,
                    distractors=[
                        int(round(e0 * 1000)),
                        int(round((e0 + drop) * 1000)),
                        int(round((e0 - 2 * drop) * 1000)),
                    ],
                    rng=rng,
                )
                options = opts
                question_text = (
                    f"In '{chapter_hint}', for a cell with $E^\\circ={e0:.2f}\\,\\text{{V}}$, "
                    f"$n={n}$ and $\\log Q={log_q}$ at $298\\,\\text{{K}}$, find $1000E$ using "
                    "$E=E^\\circ-\\dfrac{0.059}{n}\\log Q$."
                )
                explanation = (
                    f"$E={e0:.2f}-\\dfrac{{0.059\\times {log_q}}}{{{n}}}={e_val:.3f}\\,\\text{{V}}$, "
                    f"thus $1000E={scaled}$."
                )
            else:
                n = rng.choice([1, 2, 3])
                moles = rng.choice([1, 2, 3, 4])
                charge = n * 96500 * moles
                kcharge = int(round(charge / 1000))
                opts, correct_letter = self._finalize_numeric_options(
                    correct_value=kcharge,
                    distractors=[kcharge + (96500 // 1000), max(1, kcharge - 40), n * moles * 10],
                    rng=rng,
                )
                options = opts
                question_text = (
                    f"In '{chapter_hint}', if ${moles}$ mol of a metal ion requires "
                    f"${n}$ electrons per ion for discharge, find charge in kC "
                    "needed for complete deposition ($F=96500\\,\\text{C mol}^{-1}$)."
                )
                explanation = (
                    f"$Q=nFm={n}\\times 96500\\times {moles}={charge}\\,\\text{{C}}={kcharge}\\,\\text{{kC}}$."
                )
        elif domain == "chemistry_organic":
            c = rng.randint(3, 6)
            h = 2 * c + 2
            sigma = (c - 1) + h
            opts, correct_letter = self._finalize_numeric_options(
                correct_value=sigma,
                distractors=[c + h, h - c, 2 * c + h],
                rng=rng,
            )
            options = opts
            tag = "Organic Chemistry"
            question_text = (
                f"In '{chapter_hint}', consider alkane $C_{c}H_{h}$. "
                "How many $\\sigma$ bonds are present?"
            )
            explanation = (
                f"$C-C$ bonds: {c-1}, $C-H$ bonds: {h}; total $\\sigma={sigma}$."
            )
        elif domain == "chemistry_inorganic":
            tag = "Inorganic Chemistry"
            inorganic_template = (idx + rng.randint(0, 7)) % 4
            if inorganic_template == 0:
                options, correct_letter = self._finalize_text_options(
                    correct_text="+2",
                    distractors=["+1", "+3", "+4"],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', what is oxidation state of Fe in "
                    "$FeSO_4\\cdot 7H_2O$?"
                )
                explanation = "Sulfate carries $-2$, so Fe must be $+2$."
            elif inorganic_template == 1:
                options, correct_letter = self._finalize_text_options(
                    correct_text="+7",
                    distractors=["+5", "+6", "+4"],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', find oxidation state of Mn in $KMnO_4$."
                )
                explanation = "Let Mn = x: $(+1)+x+4(-2)=0\\Rightarrow x=+7$."
            elif inorganic_template == 2:
                options, correct_letter = self._finalize_text_options(
                    correct_text="+6",
                    distractors=["+4", "+5", "+7"],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', oxidation state of sulfur in $H_2SO_4$ is:"
                )
                explanation = "Use $2(+1)+x+4(-2)=0$, giving $x=+6$."
            else:
                options, correct_letter = self._finalize_text_options(
                    correct_text="+1",
                    distractors=["+2", "+3", "0"],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', oxidation state of copper in $Cu_2O$ is:"
                )
                explanation = "Let Cu = x: $2x+(-2)=0\\Rightarrow x=+1$."
        elif domain == "biology_genetics":
            tag = "Genetics"
            genetics_template = (idx + rng.randint(0, 9)) % 4
            if genetics_template == 0:
                options, correct_letter = self._finalize_text_options(
                    correct_text="$\\dfrac{1}{4}$",
                    distractors=[
                        "$\\dfrac{1}{2}$",
                        "$\\dfrac{3}{4}$",
                        "$\\dfrac{1}{8}$",
                    ],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', if two heterozygous parents ($Aa\\times Aa$) are crossed, "
                    "probability of homozygous recessive offspring is?"
                )
                explanation = "Genotype ratio is $1:2:1$, so $P(aa)=1/4$."
            elif genetics_template == 1:
                options, correct_letter = self._finalize_text_options(
                    correct_text="$\\dfrac{3}{16}$",
                    distractors=[
                        "$\\dfrac{1}{16}$",
                        "$\\dfrac{9}{16}$",
                        "$\\dfrac{1}{8}$",
                    ],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', for dihybrid cross $AaBb\\times AaBb$, "
                    "probability of genotype $A\\_bb$ is:"
                )
                explanation = "$P(A\\_)=3/4$ and $P(bb)=1/4$, so total probability is $3/16$."
            elif genetics_template == 2:
                options, correct_letter = self._finalize_text_options(
                    correct_text="$\\dfrac{1}{2}$",
                    distractors=[
                        "$\\dfrac{1}{4}$",
                        "$\\dfrac{3}{4}$",
                        "$\\dfrac{1}{8}$",
                    ],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', for test cross $Aa\\times aa$, "
                    "probability of heterozygous offspring is:"
                )
                explanation = "Gametes are $A,a$ and $a$; offspring ratio is $Aa:aa=1:1$, so $1/2$."
            else:
                options, correct_letter = self._finalize_text_options(
                    correct_text="$\\dfrac{1}{4}$",
                    distractors=[
                        "$\\dfrac{1}{2}$",
                        "$\\dfrac{3}{8}$",
                        "$\\dfrac{1}{8}$",
                    ],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', if parents are $I^AI^O$ and $I^BI^O$, "
                    "probability of blood group O child is:"
                )
                explanation = (
                    "Each parent contributes $I^O$ with probability $1/2$; "
                    "thus $P(I^OI^O)=1/2\\times 1/2=1/4$."
                )
        elif domain == "biology_ecology":
            tag = "Ecology"
            ecology_template = (idx + rng.randint(0, 7)) % 3
            if ecology_template == 0:
                options, correct_letter = self._finalize_text_options(
                    correct_text="$\\dfrac{rK}{4}$",
                    distractors=["$rK$", "$\\dfrac{rK}{2}$", "$\\dfrac{K}{r}$"],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', for logistic growth "
                    "$\\dfrac{dN}{dt}=rN\\left(1-\\dfrac{N}{K}\\right)$, "
                    "what is growth rate at $N=K/2$?"
                )
                explanation = "Substitute $N=K/2$: $dN/dt=r(K/2)(1-1/2)=rK/4$."
            elif ecology_template == 1:
                producer_energy = rng.choice([8000, 10000, 12000, 15000])
                tertiary = int(round(producer_energy * 0.001))
                options, correct_letter = self._finalize_numeric_options(
                    correct_value=tertiary,
                    distractors=[
                        int(round(producer_energy * 0.01)),
                        int(round(producer_energy * 0.1)),
                        int(round(producer_energy * 0.0001)),
                    ],
                    rng=rng,
                )
                tag = "Ecology Energy Flow"
                question_text = (
                    f"In '{chapter_hint}', if producers store {producer_energy} kJ energy, "
                    "how much reaches tertiary consumers (10% law at each transfer)?"
                )
                explanation = (
                    f"Three transfers occur: producer $\\to$ primary $\\to$ secondary $\\to$ tertiary, "
                    f"so energy = {producer_energy}$\\times(0.1)^3={tertiary}$ kJ."
                )
            else:
                n1 = rng.choice([200, 240, 300, 360])
                n2 = n1 + rng.choice([60, 80, 90, 120])
                dt = rng.choice([2, 3, 4])
                rate = round((n2 - n1) / dt, 2)
                options, correct_letter = self._finalize_numeric_options(
                    correct_value=rate,
                    distractors=[round((n2 + n1) / dt, 2), round((n2 - n1) / (dt + 1), 2), n2 - n1],
                    rng=rng,
                )
                tag = "Population Ecology"
                question_text = (
                    f"In '{chapter_hint}', a population increases from {n1} to {n2} in "
                    f"{dt} years. Find average growth rate (individuals/year)."
                )
                explanation = f"Average growth rate $=(N_2-N_1)/\\Delta t=({n2}-{n1})/{dt}={rate}$."
        else:
            tag = "Biology Core"
            cell_template = (idx + rng.randint(0, 11)) % 3
            if cell_template == 0:
                options, correct_letter = self._finalize_text_options(
                    correct_text="S phase",
                    distractors=[
                        "G1 phase",
                        "G2 phase",
                        "M phase",
                    ],
                    rng=rng,
                )
                question_text = (
                    f"For '{chapter_hint}', DNA replication in eukaryotic cell cycle primarily occurs in:"
                )
                explanation = "Replication is a hallmark of S (synthesis) phase."
            elif cell_template == 1:
                options, correct_letter = self._finalize_text_options(
                    correct_text="Mitochondria",
                    distractors=[
                        "Ribosome",
                        "Golgi apparatus",
                        "Lysosome",
                    ],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', ATP production by oxidative phosphorylation mainly occurs in:"
                )
                explanation = "Electron transport chain is located in inner mitochondrial membrane."
            else:
                options, correct_letter = self._finalize_text_options(
                    correct_text="Prophase",
                    distractors=[
                        "Metaphase",
                        "Anaphase",
                        "Telophase",
                    ],
                    rng=rng,
                )
                question_text = (
                    f"In '{chapter_hint}', chromosome condensation becomes clearly visible first in:"
                )
                explanation = "Condensation begins in prophase before alignment at metaphase."

        trap_archetypes = (
            "unit mismatch",
            "sign convention error",
            "hidden condition",
            "domain restriction",
        )
        if trap_intensity == "high":
            trap = trap_archetypes[idx % len(trap_archetypes)]
            explanation = f"Trap check: avoid {trap}. {explanation}"

        question_text = self._normalize_generated_question_text(question_text)

        tags = [self._str(x) for x in concept_tags if self._str(x)]
        if tag and tag not in tags:
            tags.append(tag)
        if cross_concept and len(tags) < 2:
            tags.append(f"{subject} mixed application")

        return self._coerce_generated_question_type(
            {
            "question_id": f"q_{idx + 1}",
            "question_text": question_text,
            "options": options,
            "correct_option": correct_letter,
            "solution_explanation": self._to_stepwise_solution(explanation),
            "difficulty_estimate": max(4, min(5, difficulty)),
            "concept_tags": tags,
            "question_type": "MCQ",
            },
            target_type=forced_question_type or "MCQ_SINGLE",
            seed_key=seed_key,
        )

    def _question_from_binomial_template(
        self,
        *,
        idx: int,
        subject: str,
        concept_tags: list[str],
        difficulty: int,
        trap_intensity: str,
        cross_concept: bool,
        seed_key: str,
    ) -> dict[str, Any]:
        rng = self._seeded_random(seed_key)
        template = idx % 6
        chapter_hint = self._str(concept_tags[0] if concept_tags else "Binomial Theorem")

        question_text = ""
        explanation = ""
        correct_value = 0
        distractors: list[int] = []
        tag = "Binomial Theorem"

        if template == 0:
            n = rng.randint(8, 14)
            r = rng.randint(2, n - 2)
            correct_value = self._ncr(n, r)
            distractors = [
                self._ncr(n, r - 1),
                self._ncr(n, min(n, r + 1)),
                self._ncr(n - 1, r),
            ]
            question_text = (
                f"In the expansion of $(1+x)^{{{n}}}$, the coefficient of "
                f"$x^{{{r}}}$ is:"
            )
            explanation = (
                f"Step 1: General term is $T_{{r+1}}=\\binom{{{n}}}{{r}}x^r$. "
                f"Step 2: So coefficient of $x^{{{r}}}$ is "
                f"$\\binom{{{n}}}{{{r}}}={correct_value}$."
            )
            tag = "Coefficient extraction"
        elif template == 1:
            n = rng.randint(6, 10)
            k = rng.randint(1, n - 1)
            a = rng.randint(1, 3)
            b = rng.randint(2, 4)
            correct_value = self._ncr(n, k) * (a ** (n - k)) * (b ** k)
            distractors = [
                self._ncr(n, k) * (a ** k) * (b ** (n - k)),
                self._ncr(n, max(0, k - 1)) * (a ** (n - k)) * (b ** k),
                self._ncr(n, min(n, k + 1)) * (a ** (n - k)) * (b ** k),
            ]
            question_text = (
                f"Find the coefficient of $x^{{{k}}}$ in "
                f"$({a}+{b}x)^{{{n}}}$."
            )
            explanation = (
                f"Step 1: General term is "
                f"$\\binom{{{n}}}{{r}}{a}^{{{n}-r}}({b}x)^r$. "
                f"Step 2: Put $r={k}$ to match $x^{{{k}}}$, giving "
                f"$\\binom{{{n}}}{{{k}}}{a}^{{{n-k}}}{b}^{{{k}}}={correct_value}$."
            )
            tag = "General term"
        elif template == 2:
            n = rng.choice([8, 10, 12])
            m = n // 2
            correct_value = self._ncr(n, m)
            distractors = [
                self._ncr(n, max(0, m - 1)),
                self._ncr(n, min(n, m + 1)),
                self._ncr(n - 1, m),
            ]
            question_text = (
                f"The middle term coefficient in the expansion of "
                f"$(1+x)^{{{n}}}$ is:"
            )
            explanation = (
                f"Step 1: For even $n={n}$, middle term index is $n/2={m}$. "
                f"Step 2: Coefficient is $\\binom{{{n}}}{{{m}}}={correct_value}$."
            )
            tag = "Middle term"
        elif template == 3:
            n = rng.choice([6, 9, 12])
            r = (2 * n) // 3
            correct_value = self._ncr(n, r)
            distractors = [
                self._ncr(n, max(0, r - 1)),
                self._ncr(n, min(n, r + 1)),
                self._ncr(n, n // 2),
            ]
            question_text = (
                f"Find the constant term coefficient in the expansion of "
                f"$(x^2+\\frac{{1}}{{x}})^{{{n}}}$."
            )
            explanation = (
                "Step 1: General term exponent of $x$ is "
                f"$2({n}-r)-r={2 * n}-3r$. "
                f"Step 2: For constant term, set ${2 * n}-3r=0\\Rightarrow r={r}$. "
                f"Step 3: Coefficient is $\\binom{{{n}}}{{{r}}}={correct_value}$."
            )
            tag = "Constant term"
        elif template == 4:
            n = rng.choice([9, 11, 13])
            m = n // 2
            correct_value = self._ncr(n, m)
            distractors = [
                self._ncr(n, max(0, m - 1)),
                self._ncr(n, min(n, m + 1)),
                self._ncr(n - 1, m),
            ]
            question_text = (
                f"In $(1+x)^{{{n}}}$, the common value of the two greatest "
                "coefficients is:"
            )
            explanation = (
                f"Step 1: For odd $n={n}$, two greatest coefficients are equal: "
                f"$\\binom{{{n}}}{{{m}}}$ and $\\binom{{{n}}}{{{m+1}}}$. "
                f"Step 2: Their common value is "
                f"$\\binom{{{n}}}{{{m}}}={correct_value}$."
            )
            tag = "Greatest coefficient"
        else:
            n = rng.randint(6, 12)
            correct_value = 2 ** (n - 1)
            distractors = [2**n, max(1, 2 ** max(0, n - 2)), n * (2 ** max(0, n - 2))]
            question_text = (
                f"In the expansion of $(1+x)^{{{n}}}$, the sum of coefficients "
                "of odd powers of $x$ is:"
            )
            explanation = (
                "Step 1: Let $S_+=\\sum\\binom{n}{r}$ and "
                "$S_-=\\sum(-1)^r\\binom{n}{r}$. "
                f"Step 2: For $(1+1)^{{{n}}}$, $S_+=2^{n}$. "
                "For $(1-1)^n$, $S_-=0$. "
                "Step 3: Odd-power sum is $(S_+ - S_-)/2 = "
                f"2^{{{n}-1}}={correct_value}$."
            )
            tag = "Binomial identities"

        options, correct_letter = self._finalize_numeric_options(
            correct_value=correct_value,
            distractors=distractors,
            rng=rng,
        )

        trap_archetypes = (
            "using r+1 instead of r in coefficient index",
            "mixing term number with power of x",
            "missing sign/exponent condition in constant-term setup",
            "wrong middle-term index for odd/even n",
        )
        if trap_intensity == "high":
            trap = trap_archetypes[idx % len(trap_archetypes)]
            explanation = f"Trap check: avoid {trap}. {explanation}"

        question_text = self._normalize_generated_question_text(question_text)

        tags = [self._str(x) for x in concept_tags if self._str(x)]
        if "Binomial Theorem" not in tags:
            tags.append("Binomial Theorem")
        if tag not in tags:
            tags.append(tag)
        if chapter_hint and chapter_hint not in tags:
            tags.append(chapter_hint)
        if cross_concept and len(tags) < 2:
            tags.append(f"{subject} mixed application")

        return {
            "question_id": f"q_{idx + 1}",
            "question_text": question_text,
            "options": options,
            "correct_option": correct_letter,
            "solution_explanation": self._to_stepwise_solution(explanation),
            "difficulty_estimate": max(4, min(5, difficulty)),
            "concept_tags": tags,
            "question_type": "MCQ",
        }

    def _question_from_combinatorics_template(
        self,
        *,
        idx: int,
        subject: str,
        concept_tags: list[str],
        difficulty: int,
        trap_intensity: str,
        cross_concept: bool,
        seed_key: str,
    ) -> dict[str, Any]:
        rng = self._seeded_random(seed_key)
        template = idx % 6

        question_text = ""
        explanation = ""
        correct_value = 0
        distractors: list[int] = []
        tag = "Permutation and Combination"

        if template == 0:
            girls = rng.randint(8, 12)
            boys = rng.randint(7, 11)
            team = rng.randint(6, 9)
            min_girls = rng.randint(2, min(4, team - 2))
            min_boys = rng.randint(2, min(4, team - min_girls))
            correct_value = 0
            for g_pick in range(min_girls, team - min_boys + 1):
                b_pick = team - g_pick
                if 0 <= g_pick <= girls and 0 <= b_pick <= boys:
                    correct_value += self._ncr(girls, g_pick) * self._ncr(boys, b_pick)
            if correct_value <= 0:
                girls, boys, team, min_girls, min_boys = 9, 8, 7, 2, 2
                correct_value = sum(
                    self._ncr(girls, g_pick) * self._ncr(boys, team - g_pick)
                    for g_pick in range(min_girls, team - min_boys + 1)
                )
            exact_min = self._ncr(girls, min_girls) * self._ncr(
                boys, max(0, team - min_girls)
            )
            ignore_boys_floor = sum(
                self._ncr(girls, g_pick) * self._ncr(boys, team - g_pick)
                for g_pick in range(min_girls, min(team, girls) + 1)
                if 0 <= team - g_pick <= boys
            )
            unrestricted = self._ncr(girls + boys, team)
            distractors = [exact_min, ignore_boys_floor, unrestricted]
            tag = "Restricted combinations"
            question_text = (
                f"In a combinatorics test, there are ${girls}$ girls and ${boys}$ boys. "
                f"How many committees of size ${team}$ can be formed if at least "
                f"${min_girls}$ girls and at least ${min_boys}$ boys are chosen?"
            )
            explanation = (
                f"Use constrained summation: "
                f"$\\sum_{{g={min_girls}}}^{{{team - min_boys}}}"
                f"\\binom{{{girls}}}{{g}}\\binom{{{boys}}}{{{team}-g}}={correct_value}$."
            )
        elif template == 1:
            n = rng.randint(7, 10)
            m = rng.randint(3, 4)
            correct_value = sum(
                ((-1) ** k) * self._ncr(m, k) * ((m - k) ** n)
                for k in range(0, m + 1)
            )
            total_maps = m ** n
            one_step_ie = total_maps - m * ((m - 1) ** n)
            wrong_partition = self._ncr(n - 1, m - 1) * self._fact(m)
            distractors = [total_maps, one_step_ie, wrong_partition]
            tag = "Inclusion-Exclusion"
            question_text = (
                f"Using inclusion-exclusion, how many onto mappings $f:[{n}]\\to[{m}]$ "
                "exist, where both domain and codomain elements are distinct?"
            )
            explanation = (
                f"Apply inclusion-exclusion on empty boxes: "
                f"$\\sum_{{k=0}}^{{{m}}}(-1)^k\\binom{{{m}}}{{k}}({m}-k)^{{{n}}}"
                f"={correct_value}$."
            )
        elif template == 2:
            vars_count = rng.randint(4, 6)
            total = rng.randint(13, 20)
            lower = rng.randint(1, 3)
            upper = rng.randint(2, 5)
            reduced_total = total - lower
            all_count = self._ncr(reduced_total + vars_count - 1, vars_count - 1)
            bad_rhs = reduced_total - (upper + 1)
            bad_count = (
                self._ncr(bad_rhs + vars_count - 1, vars_count - 1)
                if bad_rhs >= 0
                else 0
            )
            correct_value = all_count - bad_count
            if correct_value <= 0:
                vars_count, total, lower, upper = 5, 15, 2, 3
                reduced_total = total - lower
                all_count = self._ncr(
                    reduced_total + vars_count - 1, vars_count - 1
                )
                bad_rhs = reduced_total - (upper + 1)
                bad_count = self._ncr(
                    bad_rhs + vars_count - 1, vars_count - 1
                )
                correct_value = all_count - bad_count
            ignore_upper = all_count
            ignore_lower_and_upper = self._ncr(total + vars_count - 1, vars_count - 1)
            distractors = [ignore_upper, max(1, bad_count), ignore_lower_and_upper]
            tag = "Stars and Bars with bounds"
            question_text = (
                f"Using stars and bars with bounds, count the non-negative integer solutions of "
                f"$x_1+x_2+\\cdots+x_{vars_count}={total}$ "
                f"subject to $x_1\\ge {lower}$ and $x_2\\le {upper}$."
            )
            explanation = (
                f"Set $y_1=x_1-{lower}$. Then total becomes ${reduced_total}$. "
                f"Count all minus cases with $x_2\\ge {upper + 1}$: "
                f"$\\binom{{{reduced_total + vars_count - 1}}}{{{vars_count - 1}}}"
                f"-\\binom{{{max(0, bad_rhs + vars_count - 1)}}}{{{vars_count - 1}}}"
                f"={correct_value}$."
            )
        elif template == 3:
            m = rng.randint(5, 8)
            correct_value = self._fact(m - 1) * self._fact(m)
            distractors = [
                self._fact(m) * self._fact(m),
                self._fact(m - 1) * self._fact(m - 1),
                max(1, correct_value // 2),
            ]
            tag = "Circular permutations"
            question_text = (
                f"There are ${m}$ men and ${m}$ women. In how many distinct circular "
                "arrangements can they sit so that genders alternate "
                "(rotations same, reflections different)?"
            )
            explanation = (
                f"Arrange one gender on the circle in $(m-1)!$, then place the other "
                f"in $m!$ slots. Result: $({m}-1)!\\cdot {m}!={correct_value}$."
            )
        elif template == 4:
            correct_value = 7350
            distractors = [176400, 352800, 3675]
            tag = "Multiset permutations"
            question_text = (
                "How many distinct arrangements of the letters of "
                "$\\text{MISSISSIPPI}$ are possible if no two $I$'s are adjacent?"
            )
            explanation = (
                "Arrange non-$I$ letters first: "
                "$\\frac{7!}{4!2!}=105$. "
                "Choose 4 slots among 8 gaps for $I$: "
                "$\\binom{8}{4}=70$. Total $=105\\times70=7350$."
            )
        else:
            n = rng.randint(8, 10)
            pair_count = min(rng.randint(3, 4), n // 2)
            vals = list(range(1, n + 1))
            rng.shuffle(vals)
            pairs: list[tuple[int, int]] = []
            for p in range(pair_count):
                pairs.append((vals[2 * p], vals[2 * p + 1]))
            conditions = ", ".join([f"{a} before {b}" for a, b in pairs])
            correct_value = self._fact(n) // (2 ** pair_count)
            distractors = [
                self._fact(n),
                self._fact(n) // (2 ** max(1, pair_count - 1)),
                self._fact(n) // max(1, pair_count),
            ]
            tag = "Relative order constraints"
            question_text = (
                f"For permutations of the numbers $1,2,\\ldots,{n}$, how many satisfy "
                f"all constraints: {conditions}?"
            )
            explanation = (
                f"Each independent precedence constraint halves the count. "
                f"So answer is $\\frac{{{n}!}}{{2^{{{pair_count}}}}}={correct_value}$."
            )

        options, correct_letter = self._finalize_numeric_options(
            correct_value=correct_value,
            distractors=distractors,
            rng=rng,
        )

        trap_archetypes = (
            "double-counting symmetric cases",
            "ignoring minimum constraints",
            "forgetting onto-condition correction",
            "missing rotation equivalence",
        )
        if trap_intensity == "high":
            trap = trap_archetypes[idx % len(trap_archetypes)]
            explanation = f"Trap check: avoid {trap}. {explanation}"

        question_text = self._normalize_generated_question_text(question_text)

        tags = [self._str(x) for x in concept_tags if self._str(x)]
        if "Permutation and Combination" not in tags:
            tags.append("Permutation and Combination")
        if tag not in tags:
            tags.append(tag)
        if cross_concept and len(tags) < 2:
            tags.append(f"{subject} mixed application")

        return {
            "question_id": f"q_{idx + 1}",
            "question_text": question_text,
            "options": options,
            "correct_option": correct_letter,
            "solution_explanation": self._to_stepwise_solution(explanation),
            "difficulty_estimate": max(4, min(5, difficulty)),
            "concept_tags": tags,
            "question_type": "MCQ",
        }

    def _question_from_math_template(
        self,
        *,
        idx: int,
        subject: str,
        concept_tags: list[str],
        difficulty: int,
        trap_intensity: str,
        cross_concept: bool,
        seed_key: str,
    ) -> dict[str, Any]:
        rng = self._seeded_random(seed_key)
        template = idx % 4
        options: list[str] = []
        answer_text = ""
        explanation = ""

        if template == 0:
            a = rng.randint(2, 9)
            x = rng.randint(1, 18)
            b = rng.randint(-9, 9)
            c = a * x + b
            question_text = f"Solve for x: {a}x {self._sign(b)} {abs(b)} = {c}"
            answer_text = str(x)
            explanation = (
                f"Move the constant to RHS and divide by {a}. "
                f"x = ({c} {self._sign(-b)} {abs(b)})/{a} = {x}."
            )
            options = [
                answer_text,
                str(x + 1),
                str(max(0, x - 1)),
                str(x + (2 if b >= 0 else -2)),
            ]
        elif template == 1:
            u = rng.randint(3, 20)
            a = rng.randint(1, 8)
            t = rng.randint(2, 9)
            v = u + a * t
            question_text = (
                "A body starts with initial velocity "
                f"{u} m/s and acceleration {a} m/s^2 for {t} s. Final velocity?"
            )
            answer_text = str(v)
            explanation = f"Use v = u + at = {u} + {a}*{t} = {v}."
            options = [str(v), str(v - a), str(v + a), str(u * t)]
        elif template == 2:
            p = rng.randint(2, 10)
            q = rng.randint(2, 12)
            r = rng.randint(2, 9)
            value = p * q + r
            question_text = (
                f"Evaluate: {p} × {q} + {r}. "
                "Choose the correct numeric value."
            )
            answer_text = str(value)
            explanation = f"Multiplication first: {p*q}, then add {r}."
            options = [str(value), str(p + q + r), str((p * q) - r), str(p * (q + r))]
        else:
            m = rng.randint(2, 12)
            n = rng.randint(1, 8)
            value = m * m - n * n
            question_text = f"Compute {m}^2 - {n}^2."
            answer_text = str(value)
            explanation = (
                f"Use a^2 - b^2 = (a-b)(a+b) => ({m}-{n})({m}+{n}) = {value}."
            )
            options = [str(value), str(m * m + n * n), str((m - n) * (m - n)), str(m + n)]

        trap_archetypes = [
            "sign trap",
            "hidden constraint",
            "symmetry assumption",
            "overgeneralization",
        ]
        if trap_intensity == "high":
            trap = trap_archetypes[idx % len(trap_archetypes)]
            explanation = f"Trap check: avoid {trap}. {explanation}"
            # Replace one distractor with a trap-shaped option close to correct.
            options[-1] = str(int(float(answer_text)) * -1 if answer_text.lstrip("-").isdigit() else options[-1])

        question_text = self._normalize_generated_question_text(question_text)

        if cross_concept and len(concept_tags) < 2:
            concept_tags = [concept_tags[0], f"{subject} mixed application"]

        deduped = []
        seen: set[str] = set()
        for option in options:
            text = self._str(option)
            if not text or text in seen:
                continue
            deduped.append(text)
            seen.add(text)
        while len(deduped) < 4:
            filler = str(self._to_int(answer_text, 0) + len(deduped) + 2)
            if filler in seen:
                filler = f"{filler}_{len(deduped)}"
            deduped.append(filler)
            seen.add(filler)
        options = deduped[:4]
        correct_index = options.index(answer_text) if answer_text in options else 0

        return {
            "question_id": f"q_{idx + 1}",
            "question_text": question_text,
            "options": options,
            "correct_option": "ABCD"[correct_index],
            "solution_explanation": self._to_stepwise_solution(explanation),
            "difficulty_estimate": max(1, min(5, difficulty)),
            "concept_tags": concept_tags,
            "question_type": "MCQ",
        }

    def _question_from_concept_template(
        self,
        *,
        idx: int,
        subject: str,
        concept_tags: list[str],
        difficulty: int,
        cross_concept: bool,
        seed_key: str,
    ) -> dict[str, Any]:
        rng = self._seeded_random(seed_key)
        concept = concept_tags[0] if concept_tags else subject
        second = concept_tags[1] if len(concept_tags) > 1 else subject
        stem = (
            f"In {subject}, which statement best reflects the core idea of "
            f"'{concept}'?"
        )
        if cross_concept:
            stem = (
                f"In {subject}, which option correctly combines {concept} and {second}?"
            )
        options = [
            f"Apply {concept} with dimensional consistency and boundary checks.",
            f"Ignore boundary conditions and use memorized substitutions only.",
            f"Assume all variables are constants in every case.",
            f"Use random elimination without model assumptions.",
        ]
        if cross_concept:
            options[0] = f"Link {concept} with {second} and verify both constraints."
        correct = "A"
        return {
            "question_id": f"q_{idx + 1}",
            "question_text": stem,
            "options": options,
            "correct_option": correct,
            "solution_explanation": self._to_stepwise_solution(
                "The valid option keeps constraints and model assumptions explicit."
            ),
            "difficulty_estimate": max(1, min(5, difficulty)),
            "concept_tags": concept_tags,
            "question_type": "MCQ",
        }

    def _validate_generated_question(self, question: dict[str, Any]) -> bool:
        try:
            prepared = self._prepare_question_for_grading(
                question,
                fallback_question_id=self._str(question.get("question_id")) or "q_1",
            )
            validate_question_structure(prepared, student_mode=False)
        except (QuestionStructureError, ValueError):
            return False
        difficulty = self._to_int(prepared.get("difficulty_estimate"), 0)
        return 1 <= difficulty <= 5

    def _ensure_minimum_solution_steps(self, explanation: str, minimum_steps: int) -> str:
        target = max(1, min(6, int(minimum_steps)))
        normalized = self._to_stepwise_solution(explanation)
        normalized = re.sub(
            r"(?i)(step\s*\d+\s*:\s*)(step\s*\d+\s*:\s*)+",
            r"\1",
            normalized,
        )
        current = len(re.findall(r"(?i)\bstep\s*\d+\b", normalized))
        if current >= target:
            return normalized
        chunks = [
            re.sub(r"(?i)^step\s*\d+\s*:\s*", "", part).strip()
            for part in re.split(r";\s+|(?<=[.])\s+", normalized)
            if part.strip()
        ]
        if not chunks:
            chunks = ["Parse conditions", "Apply the right method", "Simplify the final result"]
        while len(chunks) < target:
            if len(chunks) == target - 1:
                chunks.append("Verify constraints and final value")
            else:
                chunks.append("Continue algebraic simplification carefully")
        return " ".join(f"Step {i + 1}: {chunks[i]}" for i in range(target))

    def _arena_provider_name(self, slot: int) -> str:
        labels = ("lalacore_alpha", "lalacore_beta", "lalacore_gamma")
        if 0 <= slot < len(labels):
            return labels[slot]
        return f"lalacore_provider_{slot + 1}"

    def _first_numeric_value(self, text: str) -> float | None:
        raw = self._str(text).replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", raw)
        if not match:
            return None
        try:
            return float(match.group(0))
        except Exception:
            return None

    def _question_hardness_score(
        self,
        *,
        question: dict[str, Any],
        requested_difficulty: int,
        trap_intensity: str,
        cross_concept: bool,
        minimum_reasoning_steps: int,
    ) -> float:
        try:
            prepared = self._prepare_question_for_grading(
                question,
                fallback_question_id=self._str(question.get("question_id")) or "q_1",
                derive_from_visible=True,
            )
        except Exception:
            return -1.0

        q_text = self._str(prepared.get("question_text")).lower()
        solution = self._str(prepared.get("_solution_explanation"))
        options = [self._str(x) for x in (prepared.get("options") or [])]
        inferred_diff = max(1, min(5, self._to_int(prepared.get("difficulty_estimate"), 3)))

        tokens = [t for t in re.split(r"[^a-z0-9]+", q_text) if t]
        unique_ratio = (len(set(tokens)) / len(tokens)) if tokens else 0.0
        long_tokens = sum(1 for t in tokens if len(t) >= 9)
        dense_formula_markers = len(
            re.findall(r"\\binom|\\sum|\\prod|\\int|\\frac|\^|_|\\to|\\ldots", q_text)
        )

        advanced_terms = (
            "inclusion-exclusion",
            "stars and bars",
            "onto",
            "derangement",
            "circular",
            "constraint",
            "bijection",
            "surjection",
            "equivalence",
            "parity",
            "casework",
            "non-negative integer",
            "committee",
            "arrangement",
            "permutation",
            "combination",
            "greatest coefficient",
            "middle term",
            "general term",
            "cross-concept",
        )
        advanced_hits = sum(1 for token in advanced_terms if token in q_text)

        step_count = len(re.findall(r"(?i)\bstep\s*\d+\b", solution))
        if step_count == 0:
            step_count = len(
                [
                    part.strip()
                    for part in re.split(r";\s+|(?<=[.])\s+", solution)
                    if part.strip()
                ]
            )
        if step_count == 0:
            step_count = 1

        closeness_bonus = 0.0
        if options:
            q_type = self._canonical_question_type(prepared.get("question_type"))
            if q_type == "MCQ_SINGLE":
                correct_letter = self._str(prepared.get("_correct_option")).upper()
                if correct_letter in {"A", "B", "C", "D"}:
                    idx = ord(correct_letter) - 65
                    if 0 <= idx < len(options):
                        correct_val = self._first_numeric_value(options[idx])
                        if correct_val is not None:
                            other_vals = []
                            for j, opt in enumerate(options):
                                if j == idx:
                                    continue
                                val = self._first_numeric_value(opt)
                                if val is not None:
                                    other_vals.append(abs(correct_val - val))
                            if other_vals:
                                min_gap = min(other_vals)
                                scale = max(1.0, abs(correct_val))
                                normalized_gap = min_gap / scale
                                closeness_bonus = max(0.0, 8.0 - (normalized_gap * 18.0))

        base = (requested_difficulty * 20.0) + (inferred_diff * 14.0)
        score = base
        score += min(14.0, long_tokens * 0.9)
        score += min(10.0, dense_formula_markers * 1.7)
        score += unique_ratio * 10.0
        score += advanced_hits * 4.8
        score += min(14.0, max(0, step_count - minimum_reasoning_steps + 1) * 3.0)
        score += closeness_bonus
        if trap_intensity == "high":
            score += 6.0
        if cross_concept:
            score += 5.0
        if len(options) >= 4:
            score += 2.0
        return round(score, 6)

    def _deterministic_verify_candidate(
        self, *, question: dict[str, Any], subject: str
    ) -> tuple[bool, str]:
        try:
            prepared = self._prepare_question_for_grading(
                question,
                fallback_question_id=self._str(question.get("question_id")) or "q_1",
                derive_from_visible=True,
            )
        except Exception:
            return False, "structure_parse_failed"
        if not self._validate_generated_question(prepared):
            return False, "structure_invalid"

        q_text = self._str(prepared.get("question_text")).lower()
        options = [self._str(x) for x in (prepared.get("options") or [])]
        q_type = self._canonical_question_type(prepared.get("question_type"))

        # Exact recomputation for common binomial coefficient patterns.
        if "expansion of $(1+x)^" in q_text and "coefficient of" in q_text:
            m = re.search(r"\(1\+x\)\^\{?(\d+)\}?", q_text)
            r = re.search(r"coefficient of .*x\^\{?(\d+)\}?", q_text)
            if m and r and q_type == "MCQ_SINGLE":
                n_val = self._to_int(m.group(1), -1)
                r_val = self._to_int(r.group(1), -1)
                if n_val >= 0 and r_val >= 0 and r_val <= n_val:
                    expected = self._ncr(n_val, r_val)
                    correct = self._str(prepared.get("_correct_option")).upper()
                    if correct in {"A", "B", "C", "D"}:
                        idx = ord(correct) - 65
                        if 0 <= idx < len(options):
                            chosen = self._first_numeric_value(options[idx])
                            if chosen is None or int(round(chosen)) != int(expected):
                                return False, "binomial_coeff_mismatch"
                    return True, "binomial_coeff_verified"

        # Exact recomputation for integral*6 polynomial pattern.
        if "evaluate $\\int_0^1" in q_text and "multiplied by $6$" in q_text:
            m = re.search(
                r"\(\s*([-+]?\d+)\s*x\^2\s*([+-]\s*\d+)\s*x\s*([+-]\s*\d+)\s*\)\s*\\,dx",
                q_text,
            )
            if m and q_type == "MCQ_SINGLE":
                try:
                    p = int(m.group(1).replace(" ", ""))
                    q_coef = int(m.group(2).replace(" ", ""))
                    r_coef = int(m.group(3).replace(" ", ""))
                except Exception:
                    p = q_coef = r_coef = 0
                expected = (2 * p) + (3 * q_coef) + (6 * r_coef)
                correct = self._str(prepared.get("_correct_option")).upper()
                if correct in {"A", "B", "C", "D"}:
                    idx = ord(correct) - 65
                    if 0 <= idx < len(options):
                        chosen = self._first_numeric_value(options[idx])
                        if chosen is None or int(round(chosen)) != int(expected):
                            return False, "integral_multiplier_mismatch"
                return True, "integral_multiplier_verified"

        # Subject-specific sanity heuristics.
        subj = self._str(subject).lower()
        if "physics" in subj:
            if any(k in q_text for k in ("velocity", "force", "current", "charge")):
                return True, "physics_sanity_pass"
        if "chemistry" in subj:
            if any(k in q_text for k in ("mole", "equilibrium", "reaction", "enthalpy")):
                return True, "chemistry_sanity_pass"
        if "biology" in subj:
            if any(k in q_text for k in ("cell", "gene", "species", "ecology")):
                return True, "biology_sanity_pass"
        return True, "structural_verified"

    def _arena_entropy(self, scores: list[float]) -> float:
        if len(scores) <= 1:
            return 0.0
        peak = max(scores)
        shifted = [math.exp((s - peak) / 8.0) for s in scores]
        total = sum(shifted)
        if total <= 0:
            return 0.0
        probs = [v / total for v in shifted]
        entropy = -sum(p * math.log(max(p, 1e-12)) for p in probs)
        cap = math.log(len(probs))
        if cap <= 0:
            return 0.0
        return entropy / cap

    def _is_binomial_scope(
        self, *, subject: str, chapters: list[str], subtopics: list[str]
    ) -> bool:
        bag = " ".join([subject, *chapters, *subtopics]).lower()
        explicit = any(
            k in bag
            for k in ("binomial", "\\binom", "ncr", "(1+x)^", "(a+b)^", "(x+y)^")
        )
        coeff_scope = "coefficient" in bag and (
            "term" in bag or "expansion" in bag
        )
        return explicit or coeff_scope

    def _has_cross_track_content_conflict(
        self, *, text_bag: str, requested_track: str
    ) -> bool:
        low = self._str(text_bag).lower()
        if not low or not requested_track:
            return False
        math_markers = (
            "integral",
            "derivative",
            "limit",
            "equation",
            "matrix",
            "determinant",
            "vector",
            "coordinate",
            "circle",
            "parabola",
            "ellipse",
            "hyperbola",
            "slope",
            "line",
            "plane",
            "probability",
            "ncr",
            "npr",
            "binomial",
            "complex",
            "function",
            "f(x)",
            "x^",
        )
        physics_markers = (
            "velocity",
            "acceleration",
            "force",
            "current",
            "conducting",
            "conductor",
            "rod",
            "mass",
            "friction",
            "resistance",
            "voltage",
            "resistance",
            "charge",
            "magnetic",
            "electric field",
            "momentum",
            "lens",
            "mirror",
            "slab",
            "light wave",
            "wavelength",
            "frequency",
            "thermodynamics",
            "heat engine",
            "pressure",
            "temperature",
        )
        chemistry_markers = (
            "mole",
            "reaction",
            "equilibrium",
            "enthalpy",
            "electrode",
            "oxidation",
            "reduction",
            "hybridization",
            "orbital",
            "acid",
            "base",
            "salt",
            "compound",
            "chemical",
            "ph ",
        )
        biology_markers = (
            "cell",
            "gene",
            "genetic",
            "species",
            "ecology",
            "photosynthesis",
            "mitosis",
            "meiosis",
            "organism",
            "dna",
        )
        math_hits = sum(1 for tok in math_markers if tok in low)
        physics_hits = sum(1 for tok in physics_markers if tok in low)
        chemistry_hits = sum(1 for tok in chemistry_markers if tok in low)
        biology_hits = sum(1 for tok in biology_markers if tok in low)
        if requested_track == "Mathematics":
            foreign_hits = physics_hits + chemistry_hits + biology_hits
            return foreign_hits >= 2 and math_hits <= 0
        if requested_track == "Physics":
            foreign_hits = math_hits + chemistry_hits + biology_hits
            return physics_hits <= 0 and foreign_hits >= 2
        if requested_track == "Chemistry":
            foreign_hits = math_hits + physics_hits + biology_hits
            return chemistry_hits <= 0 and foreign_hits >= 2
        if requested_track == "Biology":
            foreign_hits = math_hits + physics_hits + chemistry_hits
            return biology_hits <= 0 and foreign_hits >= 2
        return False

    def _question_matches_requested_scope(
        self,
        *,
        question: dict[str, Any],
        subject: str,
        chapters: list[str],
        subtopics: list[str],
    ) -> bool:
        raw_text = self._str(question.get("question_text"))
        text_without_scaffold = re.sub(
            r"(?i)^\s*(for chapter|in)\s*'[^']+'\s*,?\s*",
            "",
            raw_text,
        ).strip()
        text_without_scaffold = re.sub(
            r"(?i)\(trap:\s*[^)]*\)",
            "",
            text_without_scaffold,
        ).strip()
        semantic_bag = text_without_scaffold.lower().strip()
        scope_hints = [
            self._str(x)
            for x in (
                *(question.get("concept_tags") or []),
                *(question.get("chapter_tags") or []),
            )
            if self._str(x)
        ]
        bag = " ".join([text_without_scaffold, *scope_hints]).lower().strip()
        template_scaffold = bool(
            re.match(r"(?i)^\s*(for chapter|in)\s*'[^']+'", raw_text)
        )
        scope_bag = " ".join([subject, *chapters, *subtopics]).lower()
        requested_track = self._infer_subject_track(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
            concept_tags=[
                self._str(x) for x in (question.get("concept_tags") or []) if self._str(x)
            ],
        )
        if self._has_cross_track_content_conflict(
            text_bag=semantic_bag,
            requested_track=requested_track,
        ):
            return False

        if not self._is_binomial_scope(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
        ):
            scope_tokens = {
                token
                for token in re.split(r"[^a-z0-9]+", scope_bag)
                if len(token) >= 4
                and token
                not in {
                    "class",
                    "chapter",
                    "topic",
                    "subject",
                    "general",
                    "section",
                    "theorem",
                    "mathematics",
                    "maths",
                    "physics",
                    "chemistry",
                    "biology",
                }
            }
            if not scope_tokens:
                return True
            hits = sum(1 for token in scope_tokens if token in bag)
            domain_guard_applied = False
            if "trigon" in scope_bag:
                domain_guard_applied = True
                if not any(
                    k in bag for k in ("sin", "cos", "tan", "cot", "sec", "cosec", "trigon")
                ):
                    return False
            if any(k in scope_bag for k in ("probability", "statistics")):
                domain_guard_applied = True
                if not any(
                    k in bag
                    for k in ("probability", "random", "coin", "dice", "mean", "variance")
                ):
                    return False
            if any(k in scope_bag for k in ("matrix", "determinant")):
                domain_guard_applied = True
                if not any(
                    k in bag for k in ("matrix", "determinant", "adjoint", "inverse", "rank")
                ):
                    return False
            if any(
                k in scope_bag
                for k in (
                    "complex",
                    "argand",
                    "de moivre",
                    "modulus",
                    "argument",
                    "conjugate",
                )
            ):
                domain_guard_applied = True
                has_complex_mod_token = bool(
                    re.search(r"\|z[^|]*\|", bag)
                )
                if not any(
                    k in bag
                    for k in (
                        "complex",
                        "argand",
                        "conjugate",
                        "modulus",
                        "argument",
                        "de moivre",
                        "imaginary",
                        "real part",
                        "imaginary part",
                        "|z|",
                        "re(",
                        "im(",
                        "z1",
                        "z2",
                        "z_1",
                        "z_2",
                        "i^",
                    )
                ) and not has_complex_mod_token:
                    return False
            if any(
                k in scope_bag
                for k in (
                    "coordinate geometry",
                    "straight line",
                    "pair of straight lines",
                    "circle",
                    "parabola",
                    "ellipse",
                    "hyperbola",
                    "conic",
                )
            ):
                domain_guard_applied = True
                has_point_pair = bool(
                    re.search(
                        r"\(\s*[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*\)",
                        bag,
                    )
                )
                has_distance_pattern = ("ab^2" in bag) or ("distance" in bag)
                if (
                    not has_point_pair
                    and not has_distance_pattern
                    and not any(
                        k in bag
                        for k in (
                            "coordinate",
                            "slope",
                            "line",
                            "intercept",
                            "circle",
                            "radius",
                            "chord",
                            "tangent",
                            "parabola",
                            "ellipse",
                            "hyperbola",
                            "focus",
                            "directrix",
                            "eccentricity",
                            "conic",
                        )
                    )
                ):
                    return False
            if any(
                k in scope_bag
                for k in (
                    "vector",
                    "3d geometry",
                    "3-dimensional",
                    "three dimensional",
                    "line in 3d",
                    "plane in 3d",
                    "direction ratio",
                    "direction cosine",
                    "skew line",
                )
            ):
                domain_guard_applied = True
                if not any(
                    k in bag
                    for k in (
                        "vector",
                        "dot",
                        "cross",
                        "line",
                        "plane",
                        "direction ratio",
                        "direction cosine",
                        "skew",
                    )
                ):
                    return False
            if any(
                k in scope_bag
                for k in (
                    "calculus",
                    "limit",
                    "continuity",
                    "derivative",
                    "integral",
                    "differential",
                )
            ):
                domain_guard_applied = True
                if not any(
                    k in bag
                    for k in (
                        "limit",
                        "\\lim",
                        "lim_",
                        "derivative",
                        "integral",
                        "d/dx",
                        "dy/dx",
                        "dy}{dx",
                        "dy",
                        "dx",
                        "differential equation",
                        "integrating factor",
                        "separable",
                        "linear equation",
                        "f'(",
                        "\\int",
                        "differentiat",
                    )
                ):
                    return False
            if any(
                k in scope_bag
                for k in (
                    "permutation",
                    "combination",
                    "combinatorics",
                    "arrangement",
                    "selection",
                    "ncr",
                    "npr",
                    "circular",
                    "derangement",
                    "inclusion-exclusion",
                    "stars and bars",
                    "p&c",
                    "p & c",
                )
            ):
                domain_guard_applied = True
                has_comb_signal = any(
                    k in bag
                    for k in (
                        "permutation",
                        "combinatorics",
                        "arrangement",
                        "selection",
                        "factorial",
                        "circular permutation",
                        "circular arrangement",
                        "derangement",
                        "inclusion-exclusion",
                        "stars and bars",
                        "onto mapping",
                        "onto function",
                        "choose",
                        "ways can",
                        "committee",
                        "mapping",
                    )
                ) or any(
                    re.search(pat, bag)
                    for pat in (
                        r"\bncr\b",
                        r"\bnpr\b",
                    )
                )
                if not has_comb_signal:
                    return False
            if domain_guard_applied:
                return True
            if hits == 0:
                requested_domain = self._domain_key_from_context(
                    subject=subject,
                    concept_tags=[*chapters, *subtopics],
                )
                text_domain = self._domain_key_from_context(
                    subject=subject,
                    concept_tags=[text_without_scaffold],
                )
                if requested_domain != text_domain:
                    return False
                if requested_track == "Mathematics" and text_domain.startswith("math_"):
                    return True
                if requested_track == "Physics" and text_domain.startswith("physics_"):
                    return True
                if requested_track == "Chemistry" and text_domain.startswith("chemistry_"):
                    return True
                if requested_track == "Biology" and text_domain.startswith("biology_"):
                    return True
                return False
            if template_scaffold and hits <= 1 and len(scope_tokens) >= 2:
                return False
            return True

        combinatorics_scope = self._is_permutation_combination_context(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
            concept_tags=question.get("concept_tags") if isinstance(question.get("concept_tags"), list) else [],
        )

        has_binomial_signal = any(
            k in bag
            for k in (
                "binomial",
                "coefficient",
                "general term",
                "middle term",
                "greatest coefficient",
                "constant term",
                "expansion",
                "\\binom",
                "ncr",
                "(1+x)^",
                "(a+b)^",
                "(x+y)^",
            )
        )
        has_combinatorics_signal = any(
            k in bag
            for k in (
                "permutation",
                "combinatorics",
                "arrangement",
                "selection",
                "factorial",
                "circular permutation",
                "circular arrangement",
                "derangement",
                "inclusion-exclusion",
                "stars and bars",
                "onto mapping",
                "onto function",
                "choose",
                "ways can",
                "mapping",
                "committee",
            )
        ) or any(
            re.search(pat, bag)
            for pat in (
                r"\bncr\b",
                r"\bnpr\b",
            )
        )
        derivative_only = (
            ("f'(1)" in semantic_bag or "f(x)=" in semantic_bag or "derivative" in semantic_bag)
            and "coefficient" not in bag
            and "expansion" not in bag
        )
        if combinatorics_scope:
            if derivative_only:
                return False
            if not (has_binomial_signal or has_combinatorics_signal):
                return False
            return True
        if not has_binomial_signal or derivative_only:
            return False
        return True

    def _best_pyq_source_for_question(
        self,
        *,
        question: dict[str, Any],
        web_sources: list[dict[str, Any]],
        used_url_counts: dict[str, int],
        requested_difficulty: int = 3,
        strict_mode: bool = False,
    ) -> dict[str, Any] | None:
        if not web_sources:
            return None
        q_bag = " ".join(
            [
                self._str(question.get("question_text")),
                " ".join(
                    self._str(x)
                    for x in (question.get("concept_tags") or [])
                    if self._str(x)
                ),
            ]
        ).lower()
        q_tokens = [
            tok
            for tok in re.split(r"[^a-z0-9]+", q_bag)
            if len(tok) >= 4
            and tok
            not in {"find", "value", "term", "option", "correct", "question"}
        ][:16]

        best_row: dict[str, Any] | None = None
        best_score = -1.0
        best_token_match = 0.0
        best_scope = 0.0
        for row in web_sources:
            url = self._str(row.get("url")).strip().lower()
            if not url:
                continue
            row_bag = " ".join(
                [
                    self._str(row.get("title")),
                    self._str(row.get("snippet")),
                    self._str(row.get("question_stub")),
                    self._str(row.get("solution_stub")),
                ]
            ).lower()
            match_hits = sum(1 for tok in q_tokens if tok in row_bag) if q_tokens else 0
            token_match = match_hits / max(1, len(q_tokens)) if q_tokens else 0.0
            quality = self._to_float(row.get("quality_score"), 0.0)
            hardness = self._to_float(row.get("hardness_score"), 0.0)
            scope = self._to_float(row.get("scope_score"), 0.0)
            has_answer = self._to_bool(row.get("has_answer"))
            has_solution = self._to_bool(row.get("has_solution"))
            source_provider = self._str(row.get("source_provider")).lower()
            if requested_difficulty >= 5 and source_provider in {
                "local_pyq_archive_ai",
                "local_ai_archive",
            }:
                continue
            if (
                requested_difficulty >= 5
                and hardness < 0.35
                and not has_solution
                and not (source_provider == "local_pyq_import_bank" and has_answer)
            ):
                continue
            reuse_penalty = used_url_counts.get(url, 0) * 0.35
            score = (
                quality
                + (scope * 0.65)
                + (hardness * 0.35)
                + (token_match * 0.95)
                + (0.18 if has_answer else 0.0)
                + (0.22 if has_solution else 0.0)
                - reuse_penalty
            )
            if score > best_score:
                best_score = score
                best_row = row
                best_token_match = token_match
                best_scope = scope
        if best_row is None:
            return None
        if (not strict_mode) and q_tokens and best_token_match < 0.12 and best_scope < 0.28:
            return None
        return dict(best_row)

    def _extract_answer_token(self, text: str) -> str:
        raw = self._str(text).strip()
        if not raw:
            return ""
        letter = re.search(r"(?i)\b(?:option\s*)?\(?([A-D])\)?\b", raw)
        if letter:
            return self._str(letter.group(1)).upper()
        number = re.search(r"[-+]?\d+(?:\.\d+)?", raw.replace(",", ""))
        if number:
            return self._str(number.group(0))
        return ""

    async def _recover_solution_via_ai_engine(
        self,
        *,
        question: dict[str, Any],
        source_row: dict[str, Any] | None,
        minimum_reasoning_steps: int,
    ) -> dict[str, str]:
        q_text = self._str(question.get("question_text"))
        options = [self._str(x) for x in (question.get("options") or [])]
        correct_option = self._str(
            question.get("_correct_option") or question.get("correct_option")
        ).upper()
        prompt_lines = [
            "Solve the following hard JEE PYQ-style question.",
            "Return final answer first, then concise stepwise solution.",
            "Question:",
            q_text,
        ]
        if options:
            prompt_lines.append("Options:")
            for idx, opt in enumerate(options):
                label = chr(ord("A") + idx)
                prompt_lines.append(f"{label}) {opt}")
        if correct_option in {"A", "B", "C", "D"}:
            prompt_lines.append(
                f"Use internal consistency check: expected correct option is {correct_option}."
            )
        if source_row:
            ref_title = self._str(source_row.get("title"))
            ref_url = self._str(source_row.get("url"))
            ref_q = self._str(source_row.get("question_stub"))
            ref_sol = self._str(source_row.get("solution_stub"))
            prompt_lines.append("Reference context from web retrieval:")
            if ref_title:
                prompt_lines.append(f"- Title: {ref_title}")
            if ref_q:
                prompt_lines.append(f"- Question stub: {ref_q}")
            if ref_sol:
                prompt_lines.append(f"- Solution stub: {ref_sol}")
            if ref_url:
                prompt_lines.append(f"- URL: {ref_url}")

        payload = {
            "action": "ai_chat",
            "prompt": "\n".join(prompt_lines),
            "function": "ai_solve",
            "response_style": "structured_exam_solution",
            "enable_persona": False,
            "optional_web_snippets": [source_row] if source_row else [],
        }
        ai_res = await self._ai_chat_or_solve(payload)
        if not self._to_bool(ai_res.get("ok")):
            return {}
        answer_text = self._str(ai_res.get("answer"))
        explanation = self._str(ai_res.get("explanation"))
        if not explanation:
            explanation = answer_text
        low = explanation.lower()
        if any(
            bad in low
            for bad in (
                "provider error",
                "errno",
                "not known",
                "connection",
                "network",
                "timeout",
                "failed",
            )
        ):
            return {}
        explanation = self._ensure_minimum_solution_steps(
            explanation,
            minimum_reasoning_steps,
        )
        token = self._extract_answer_token(answer_text)
        return {
            "answer_token": token,
            "solution_explanation": explanation,
        }

    def _question_from_web_source(
        self,
        *,
        row: dict[str, Any],
        idx: int,
        subject: str,
        chapters: list[str],
        subtopics: list[str],
        minimum_reasoning_steps: int,
    ) -> dict[str, Any] | None:
        bank_payload = row.get("bank_payload")
        if isinstance(bank_payload, dict):
            exact_row = dict(bank_payload)
            exact_row.setdefault("source_origin", self._str(row.get("source_origin")) or "local_pyq_import_bank")
            exact_row.setdefault("source_url", self._str(row.get("url")))
            exact_row.setdefault(
                "source_stub",
                self._str(row.get("snippet") or row.get("question_stub") or row.get("title")),
            )
            if self._str(row.get("solution_stub")) and not self._str(exact_row.get("source_solution_stub")):
                exact_row["source_solution_stub"] = self._str(row.get("solution_stub"))
            if self._str(row.get("answer_stub")) and not self._str(exact_row.get("source_answer_stub")):
                exact_row["source_answer_stub"] = self._str(row.get("answer_stub"))
            try:
                exact_prepared = self._prepare_question_for_grading(
                    exact_row,
                    fallback_question_id=self._str(exact_row.get("question_id")) or f"q_{idx + 1}",
                )
            except (QuestionStructureError, ValueError):
                exact_prepared = None
            if exact_prepared is not None:
                exact_prepared["difficulty_estimate"] = max(
                    4,
                    min(
                        5,
                        self._to_int(
                            exact_row.get("difficulty_estimate")
                            or exact_row.get("difficulty_score")
                            or row.get("difficulty_estimate"),
                            4,
                        ),
                    ),
                )
                exact_prepared["concept_tags"] = [
                    self._str(x)
                    for x in (
                        exact_row.get("concept_tags")
                        or exact_row.get("chapter_tags")
                        or [self._str(chapters[0] if chapters else subject), "PYQ"]
                    )
                    if self._str(x)
                ][:4]
                exact_prepared["chapter_tags"] = [
                    self._str(x)
                    for x in (
                        exact_row.get("chapter_tags")
                        or row.get("chapter_tags")
                        or chapters
                    )
                    if self._str(x)
                ][:3]
                exact_prepared["verification_pass"] = self._to_bool(
                    row.get("verification_safe") if row.get("verification_safe") is not None else True
                )
                exact_prepared["source_quality_score"] = self._to_float(row.get("quality_score"), 0.0)
                if self._str(row.get("question_stub")):
                    exact_prepared["source_question_stub"] = self._str(row.get("question_stub"))
                if self._str(row.get("answer_stub")):
                    exact_prepared["source_answer_stub"] = self._str(row.get("answer_stub"))
                if self._str(row.get("solution_stub")):
                    exact_prepared["source_solution_stub"] = self._str(row.get("solution_stub"))
                if self._question_matches_requested_scope(
                    question=exact_prepared,
                    subject=subject,
                    chapters=chapters,
                    subtopics=subtopics,
                ):
                    return exact_prepared

        q_text = self._str(row.get("question_text") or row.get("question_stub"))
        if not q_text:
            return None
        requested_track = self._infer_subject_track(
            subject=subject,
            chapters=chapters,
            subtopics=subtopics,
            concept_tags=[self._str(x) for x in chapters if self._str(x)],
        )
        if self._has_cross_track_content_conflict(
            text_bag=q_text,
            requested_track=requested_track,
        ):
            return None
        if self._looks_degraded_math_text(q_text):
            return None
        options_raw = row.get("options")
        options = (
            [self._str(x) for x in options_raw]
            if isinstance(options_raw, list)
            else []
        )
        if options and any(re.fullmatch(r"Option\s*[1-4]", self._str(x), flags=re.IGNORECASE) for x in options):
            options = []
        if len(options) < 4:
            option_bag = " ".join(
                [
                    q_text,
                    self._str(row.get("question_stub")),
                    self._str(row.get("snippet")),
                ]
            )
            parsed_options = self._extract_mcq_options_from_text(option_bag)
            if len(parsed_options) >= 4:
                options = parsed_options[:4]
        answer_token = self._extract_answer_token(
            self._str(row.get("correct_answer") or row.get("answer_stub"))
        )
        solution_text = self._str(row.get("solution_stub"))
        if solution_text and (
            self._looks_degraded_math_text(solution_text)
            or self._has_cross_track_content_conflict(
                text_bag=solution_text,
                requested_track=requested_track,
            )
        ):
            solution_text = ""
        if not solution_text:
            solution_text = "Use the governing relation and solve carefully."
        solution_text = self._ensure_minimum_solution_steps(
            solution_text,
            minimum_reasoning_steps,
        )
        tags = [
            self._str(chapters[0] if chapters else subject),
            "PYQ",
            self._str(row.get("exam_type") or "JEE"),
        ]
        subtopic = self._str(subtopics[0] if subtopics else "")
        if subtopic and subtopic not in tags:
            tags.append(subtopic)
        if options and len(options) >= 4 and answer_token in {"A", "B", "C", "D"}:
            return {
                "question_id": f"q_{idx + 1}",
                "question_type": "MCQ_SINGLE",
                "question_text": q_text,
                "options": options[:4],
                "_correct_option": answer_token,
                "_correct_answers": [answer_token],
                "_numerical_answer": "",
                "_solution_explanation": solution_text,
                "difficulty_estimate": max(
                    4, min(5, self._to_int(row.get("difficulty_estimate"), 5))
                ),
                "concept_tags": tags,
                "marks_correct": 4.0,
                "marks_incorrect": -1.0,
                "marks_unattempted": 0.0,
                "partial_marking": False,
                "numerical_tolerance": 0.001,
            }
        if not options and re.fullmatch(r"[-+]?\d+(?:\.\d+)?", answer_token):
            return {
                "question_id": f"q_{idx + 1}",
                "question_type": "NUMERICAL",
                "question_text": q_text,
                "options": [],
                "_correct_option": "",
                "_correct_answers": [],
                "_numerical_answer": answer_token,
                "_solution_explanation": solution_text,
                "difficulty_estimate": max(
                    4, min(5, self._to_int(row.get("difficulty_estimate"), 5))
                ),
                "concept_tags": tags,
                "marks_correct": 4.0,
                "marks_incorrect": -1.0,
                "marks_unattempted": 0.0,
                "partial_marking": False,
                "numerical_tolerance": 0.001,
            }
        return None

    def _sanitize_ai_question_for_client(
        self, question: dict[str, Any], *, include_answer_key: bool = False
    ) -> dict[str, Any]:
        original = dict(question)
        prepared = self._prepare_question_for_grading(
            question,
            fallback_question_id=self._str(question.get("question_id")) or "q_1",
        )
        options = [self._str(x) for x in (prepared.get("options") or [])]
        source_stub_text = self._str(
            prepared.get("source_question_stub") or original.get("source_question_stub")
        )
        display_question_text = self._str(prepared.get("question_text"))
        source_origin_token = self._str(
            prepared.get("source_origin") or original.get("source_origin")
        ).lower()
        if (
            self._looks_degraded_math_text(display_question_text)
            and source_stub_text
            and not self._looks_degraded_math_text(source_stub_text)
            and source_origin_token.startswith("web_")
        ):
            display_question_text = source_stub_text
        q_type = self._str(prepared.get("question_type"))
        public_type = (
            "NUMERICAL"
            if q_type == "NUMERICAL"
            else ("MULTI" if q_type == "MCQ_MULTI" else "MCQ")
        )
        out: dict[str, Any] = {
            "question_id": self._str(prepared.get("question_id")),
            "question_text": sanitize_latex(display_question_text),
            "question_text_latex": self._canonical_import_latex(display_question_text),
            "options": [sanitize_latex(x) for x in options],
            "difficulty_estimate": self._to_int(prepared.get("difficulty_estimate"), 3),
            "concept_tags": [
                self._str(x) for x in (prepared.get("concept_tags") or []) if self._str(x)
            ],
            "chapter_tags": [
                self._str(x)
                for x in (
                    prepared.get("chapter_tags")
                    or original.get("chapter_tags")
                    or []
                )
                if self._str(x)
            ][:3],
            "question_type": public_type,
            "marks_correct": self._to_float(prepared.get("marks_correct"), 4.0),
            "marks_incorrect": self._to_float(prepared.get("marks_incorrect"), -1.0),
            "marks_unattempted": self._to_float(
                prepared.get("marks_unattempted"), 0.0
            ),
            "partial_marking": self._to_bool(prepared.get("partial_marking")),
            "numerical_tolerance": self._to_float(
                prepared.get("numerical_tolerance"), 0.001
            ),
            "source_origin": self._str(prepared.get("source_origin")),
            "source_stub": self._str(prepared.get("source_stub")),
            "source_url": self._str(prepared.get("source_url")),
            "source_question_stub": self._str(
                prepared.get("source_question_stub") or original.get("source_question_stub")
            ),
            "source_answer_stub": self._str(
                prepared.get("source_answer_stub") or original.get("source_answer_stub")
            ),
            "source_solution_stub": self._str(
                prepared.get("source_solution_stub") or original.get("source_solution_stub")
            ),
            "source_quality_score": self._to_float(
                prepared.get("source_quality_score")
                if prepared.get("source_quality_score") is not None
                else original.get("source_quality_score"),
                0.0,
            ),
            "verification_pass": (
                True
                if prepared.get("verification_pass") is None
                and original.get("verification_pass") is None
                else self._to_bool(
                    prepared.get("verification_pass")
                    if prepared.get("verification_pass") is not None
                    else original.get("verification_pass")
                )
            ),
            "critic_score": self._to_float(
                prepared.get("critic_score")
                if prepared.get("critic_score") is not None
                else original.get("critic_score"),
                0.0,
            ),
            "confidence_score": self._to_float(
                prepared.get("confidence_score")
                if prepared.get("confidence_score") is not None
                else original.get("confidence_score"),
                0.0,
            ),
            "difficulty_score": self._to_float(
                prepared.get("difficulty_score")
                if prepared.get("difficulty_score") is not None
                else original.get("difficulty_score"),
                0.0,
            ),
            "provider_used": self._str(
                prepared.get("provider_used") or original.get("provider_used")
            ),
            "fallback_used": self._to_bool(
                prepared.get("fallback_used")
                if prepared.get("fallback_used") is not None
                else original.get("fallback_used")
            ),
        }
        if include_answer_key:
            if q_type == "MCQ_SINGLE":
                out["correct_option"] = self._str(prepared.get("_correct_option")).upper()
            elif q_type == "MCQ_MULTI":
                out["correct_answers"] = [
                    self._str(x).upper()
                    for x in self._to_list_str(prepared.get("_correct_answers"))
                    if self._str(x)
                ]
            elif q_type == "NUMERICAL":
                out["numerical_answer"] = self._str(prepared.get("_numerical_answer"))
            out["solution_explanation"] = sanitize_latex(
                self._str(prepared.get("_solution_explanation"))
            )
        validate_question_structure(prepared, student_mode=False)
        return out

    def _canonical_question_type(self, raw: Any) -> str:
        token = self._str(raw).upper().replace("-", "_").replace(" ", "_")
        aliases = {
            "MCQ": "MCQ_SINGLE",
            "MCQ_SINGLE": "MCQ_SINGLE",
            "SINGLE": "MCQ_SINGLE",
            "MULTI": "MCQ_MULTI",
            "MCQ_MULTI": "MCQ_MULTI",
            "MULTIPLE": "MCQ_MULTI",
            "NUMERICAL": "NUMERICAL",
            "NUMERIC": "NUMERICAL",
            "INTEGER": "NUMERICAL",
        }
        return aliases.get(token, "MCQ_SINGLE")

    def _label_for_option_index(self, idx: int) -> str:
        if 0 <= idx < 26:
            return chr(65 + idx)
        return f"O{idx + 1}"

    def _labels_for_options(self, options: list[str]) -> list[str]:
        if not options:
            return ["A", "B", "C", "D"]
        return [self._label_for_option_index(i) for i in range(len(options))]

    def _normalize_answer_comparison_text(self, raw: Any) -> str:
        value = sanitize_latex(self._str(raw))
        if not value:
            return ""
        value = html.unescape(value)
        value = re.sub(
            r"\\(?:d|t)?frac\s*\{([^{}]+)\}\s*\{([^{}]+)\}",
            r"\1/\2",
            value,
        )
        value = re.sub(
            r"\\(?:d|t)?frac([A-Za-z0-9.+\-]+)([A-Za-z0-9.+\-]+)",
            r"\1/\2",
            value,
        )
        value = re.sub(r"\$+", "", value)
        value = (
            value.replace(r"\(", "")
            .replace(r"\)", "")
            .replace(r"\[", "")
            .replace(r"\]", "")
        )
        value = re.sub(r"[{}]", "", value)
        value = re.sub(r"\s+", "", value)
        return value.lower()

    def _normalize_answer_token_to_label(self, raw: Any, options: list[str]) -> str:
        value = self._str(raw)
        if not value:
            return ""
        labels = self._labels_for_options(options)
        upper = value.upper()
        if upper in set(labels):
            return upper
        if len(upper) >= 2 and upper[0] in set(labels) and upper[1] in {")", ".", ":"}:
            return upper[0]
        if upper.isdigit():
            idx = int(upper) - 1
            if 0 <= idx < len(labels):
                return labels[idx]
        for idx, opt in enumerate(options):
            if self._str(opt).lower() == value.lower():
                return labels[idx]
        normalized_value = self._normalize_answer_comparison_text(value)
        if normalized_value:
            for idx, opt in enumerate(options):
                if self._normalize_answer_comparison_text(opt) == normalized_value:
                    return labels[idx]
        return ""

    def _normalize_answer_tokens_to_labels(
        self, raw_values: Any, options: list[str]
    ) -> list[str]:
        out: list[str] = []
        for token in self._to_list_str(raw_values):
            label = self._normalize_answer_token_to_label(token, options)
            if label and label not in out:
                out.append(label)
        return out

    def _prepare_question_for_grading(
        self,
        question: dict[str, Any],
        *,
        fallback_question_id: str,
        derive_from_visible: bool = True,
    ) -> dict[str, Any]:
        q = dict(question)
        q_type = self._canonical_question_type(q.get("question_type") or q.get("type"))

        raw_options = q.get("options")
        options: list[str] = []
        if isinstance(raw_options, list):
            for item in raw_options:
                if isinstance(item, dict):
                    text = self._str(item.get("text") or item.get("value"))
                else:
                    text = self._str(item)
                if text:
                    options.append(sanitize_latex(text))
        else:
            fallback_opts = [
                self._str(q.get("option_a")),
                self._str(q.get("option_b")),
                self._str(q.get("option_c")),
                self._str(q.get("option_d")),
            ]
            options = [sanitize_latex(x) for x in fallback_opts if x]

        if q_type == "NUMERICAL":
            options = []

        question_id = self._str(q.get("question_id") or q.get("id") or fallback_question_id)
        question_text = sanitize_latex(
            self._str(q.get("question_text") or q.get("question") or q.get("text"))
        )
        solution_hidden = sanitize_latex(
            self._str(
                q.get("_solution_explanation")
                or q.get("solution_explanation")
                or q.get("solution")
            )
        )

        hidden_option = self._str(q.get("_correct_option"))
        hidden_answers = self._to_list_str(q.get("_correct_answers"))
        hidden_numerical = self._str(q.get("_numerical_answer"))

        if q_type == "MCQ_SINGLE":
            visible_correct = q.get("correct_answer")
            if isinstance(visible_correct, dict):
                if derive_from_visible and not hidden_option:
                    hidden_option = self._str(visible_correct.get("single"))
                if derive_from_visible and not hidden_answers:
                    hidden_answers = self._to_list_str(visible_correct.get("multiple"))
            if derive_from_visible and not hidden_option:
                hidden_option = self._str(q.get("correct_option") or q.get("correct_answer"))
            if derive_from_visible and not hidden_option and not hidden_answers:
                hidden_answers = self._to_list_str(
                    q.get("correct_answers") or q.get("correct")
                )
            label = self._normalize_answer_token_to_label(
                hidden_option or (hidden_answers[0] if hidden_answers else ""),
                options,
            )
            hidden_option = label
            hidden_answers = [label] if label else []
            hidden_numerical = ""
        elif q_type == "MCQ_MULTI":
            visible_correct = q.get("correct_answer")
            if isinstance(visible_correct, dict) and derive_from_visible and not hidden_answers:
                hidden_answers = self._to_list_str(visible_correct.get("multiple"))
            if derive_from_visible and not hidden_answers:
                hidden_answers = self._to_list_str(
                    q.get("correct_answers") or q.get("correct") or q.get("correct_answer")
                )
            labels = self._normalize_answer_tokens_to_labels(hidden_answers, options)
            hidden_answers = labels
            hidden_option = labels[0] if labels else ""
            hidden_numerical = ""
        else:
            visible_correct = q.get("correct_answer")
            if isinstance(visible_correct, dict) and derive_from_visible and not hidden_numerical:
                hidden_numerical = self._str(visible_correct.get("numerical"))
            if derive_from_visible and not hidden_numerical:
                hidden_numerical = self._str(
                    q.get("numerical_answer")
                    or q.get("correct_answer")
                    or q.get("correct")
                    or q.get("answer")
                )
            hidden_option = ""
            hidden_answers = []

        prepared: dict[str, Any] = {
            "question_id": question_id,
            "question_type": q_type,
            "question_text": question_text,
            "options": options,
            "_correct_option": hidden_option,
            "_correct_answers": hidden_answers,
            "_numerical_answer": hidden_numerical,
            "_solution_explanation": solution_hidden,
            "difficulty_estimate": self._to_int(q.get("difficulty_estimate"), 3),
            "concept_tags": [
                self._str(x) for x in (q.get("concept_tags") or []) if self._str(x)
            ],
            "marks_correct": self._to_float(
                q.get("marks_correct") or q.get("posMark") or q.get("positive"), 4.0
            ),
            "marks_unattempted": self._to_float(q.get("marks_unattempted"), 0.0),
            "partial_marking": self._to_bool(q.get("partial_marking")),
            "numerical_tolerance": self._to_float(q.get("numerical_tolerance"), 0.001),
            "source_origin": self._str(q.get("source_origin")),
            "source_stub": self._str(q.get("source_stub")),
            "source_url": self._str(q.get("source_url")),
        }
        raw_incorrect = self._to_float(
            q.get("marks_incorrect")
            if q.get("marks_incorrect") is not None
            else (
                q.get("negMark")
                if q.get("negMark") is not None
                else q.get("negative")
            ),
            -1.0,
        )
        prepared["marks_incorrect"] = (
            -raw_incorrect if raw_incorrect > 0 else raw_incorrect
        )
        return sanitize_question_payload(prepared, student_mode=False)

    def _sanitize_quiz_row_text(self, row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        for key in ("text", "question", "question_text"):
            if key in out:
                out[key] = sanitize_latex(self._str(out.get(key)))
        if isinstance(out.get("options"), list):
            sanitized_options: list[Any] = []
            for item in out.get("options", []):
                if isinstance(item, dict):
                    opt = dict(item)
                    opt["text"] = sanitize_latex(
                        self._str(opt.get("text") or opt.get("value"))
                    )
                    sanitized_options.append(opt)
                else:
                    sanitized_options.append(sanitize_latex(self._str(item)))
            out["options"] = sanitized_options
        for key in ("solution", "solution_explanation", "_solution_explanation"):
            if key in out:
                out[key] = sanitize_latex(self._str(out.get(key)))
        return out

    def _write_quiz_csv(
        self,
        quiz_id: str,
        questions: list[dict[str, Any]],
        *,
        include_correct: bool,
        include_solution: bool,
    ) -> str:
        csv_path = self._quizzes_dir / f"{quiz_id}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "Question",
                    "Image URL",
                    "Type",
                    "Section",
                    "Positive Mark",
                    "Negative Mark",
                    "Option A",
                    "Option B",
                    "Option C",
                    "Option D",
                    "Correct",
                    "Solution",
                    "Concept",
                ]
            )
            for row in questions:
                options = self._question_options(row)
                question_text = sanitize_latex(
                    self._str(
                        row.get("text")
                        or row.get("question")
                        or row.get("question_text")
                    )
                )
                solution_text = sanitize_latex(
                    self._str(
                        row.get("solution")
                        or row.get("solution_explanation")
                        or row.get("_solution_explanation")
                    )
                )
                writer.writerow(
                    [
                        question_text,
                        self._str(row.get("image") or row.get("imageUrl")),
                        self._str(
                            row.get("type") or row.get("question_type") or "MCQ"
                        ),
                        self._str(
                            row.get("section")
                            or row.get("chapter")
                            or ", ".join(self._to_list_str(row.get("concept_tags")))
                            or "General"
                        ),
                        self._str(row.get("posMark") or row.get("positive") or 4),
                        self._str(row.get("negMark") or row.get("negative") or 1),
                        sanitize_latex(options[0]),
                        sanitize_latex(options[1]),
                        sanitize_latex(options[2]),
                        sanitize_latex(options[3]),
                        self._question_correct(row) if include_correct else "",
                        (
                            solution_text
                            if include_solution
                            else ""
                        ),
                        self._str(
                            row.get("concept")
                            or ", ".join(self._to_list_str(row.get("concept_tags")))
                        ),
                    ]
                )
        return str(csv_path)

    def _build_assessment_item(
        self,
        *,
        quiz_id: str,
        title: str,
        quiz_type: str,
        deadline: str,
        duration: int,
        class_name: str,
        chapters: str,
        question_count: int,
        ai_generated: bool,
        is_unlimited_time: bool = False,
        ui_spec: dict[str, Any] | None = None,
        student_adaptive_data: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        questions: list[dict[str, Any]] | None = None,
        public_base_url: str = "",
    ) -> dict[str, Any]:
        now_ms = self._now_ms()
        base_url = (public_base_url or self._base_url()).rstrip("/")
        quiz_url = f"{base_url}/app/quiz/{quiz_id}.csv"
        return {
            "id": quiz_id,
            "title": title,
            "url": quiz_url,
            "deadline": deadline,
            "type": quiz_type,
            "duration": duration,
            "class": class_name,
            "chapters": chapters,
            "question_count": question_count,
            "ai_generated": ai_generated,
            "is_unlimited_time": is_unlimited_time,
            "ui_spec": dict(ui_spec or {}),
            "student_adaptive_data": dict(student_adaptive_data or {}),
            "metadata": dict(metadata or {}),
            "questions_json": json.dumps(questions or [], ensure_ascii=True),
            "created_at": now_ms,
            "updated_at": now_ms,
        }

    async def _create_quiz(self, payload: dict[str, Any]) -> dict[str, Any]:
        role = self._str(
            payload.get("role") or payload.get("user_role") or payload.get("request_role")
        ).lower()
        if role and role != "teacher":
            return {
                "ok": False,
                "status": "FORBIDDEN",
                "message": "Only teachers can publish quizzes",
            }

        title = self._str(
            payload.get("title") or payload.get("quiz_title") or payload.get("name")
        )
        if not title:
            return {
                "ok": False,
                "status": "MISSING_TITLE",
                "message": "Missing quiz title",
            }

        quiz_id = self._safe_id(payload.get("id")) or self._new_id("quiz")
        kind = self._str(payload.get("type") or "Exam") or "Exam"
        deadline = self._str(payload.get("deadline") or payload.get("date"))
        if not deadline:
            deadline = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 7 * 86400)
            )
        duration = max(
            1,
            self._to_int(
                payload.get("duration")
                or payload.get("duration_minutes")
                or payload.get("timer"),
                30,
            ),
        )
        class_name = self._str(
            payload.get("class")
            or payload.get("class_name")
            or payload.get("target_class")
        )
        chapters = self._str(
            payload.get("chapters")
            or payload.get("chapter")
            or payload.get("chapter_name")
        )
        ui_spec = payload.get("ui_spec") if isinstance(payload.get("ui_spec"), dict) else {}
        student_adaptive_data = (
            payload.get("student_adaptive_data")
            if isinstance(payload.get("student_adaptive_data"), dict)
            else {}
        )
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        ai_generated = self._to_bool(
            payload.get("ai_generated") or payload.get("is_ai_generated")
        )
        public_base_url = self._base_url(payload)
        is_unlimited_time = self._to_bool(
            payload.get("is_unlimited_time")
            or payload.get("unlimited_time_mode")
            or (self._str(payload.get("time_mode")).lower() == "unlimited")
        )
        questions = [
            self._sanitize_quiz_row_text(q) for q in self._parse_questions(payload)
        ]
        self._write_quiz_csv(
            quiz_id,
            questions,
            include_correct=False,
            include_solution=False,
        )

        item = self._build_assessment_item(
            quiz_id=quiz_id,
            title=title,
            quiz_type=kind,
            deadline=deadline,
            duration=duration,
            class_name=class_name,
            chapters=chapters,
            question_count=len(questions),
            ai_generated=ai_generated,
            is_unlimited_time=is_unlimited_time,
            ui_spec=ui_spec,
            student_adaptive_data=student_adaptive_data,
            metadata=metadata,
            questions=questions,
            public_base_url=public_base_url,
        )

        async with self._lock:
            self._upsert_by_id(self._assessments, quiz_id, item)
            self._write_list(self._assessments_file, self._assessments)

        quiz_url = self._str(item.get("url"))
        return {
            "ok": True,
            "status": "SUCCESS",
            "message": "Quiz created",
            "id": quiz_id,
            "assessment_id": quiz_id,
            "url": quiz_url,
            "quiz_url": quiz_url,
            "assessment": item,
        }

    async def _ai_generate_quiz(self, payload: dict[str, Any]) -> dict[str, Any]:
        subject = self._str(payload.get("subject") or payload.get("title") or "Physics")
        title = self._str(payload.get("title") or f"AI Quiz • {subject}")
        chapters = self._to_list_str(payload.get("chapters"))
        subtopics = self._to_list_str(payload.get("subtopics"))
        all_jee_chapters = self._to_bool(
            payload.get("all_jee_chapters")
            or payload.get("jee_all_chapters")
            or payload.get("full_jee_syllabus")
        )
        if not all_jee_chapters:
            chapter_scope = self._str(payload.get("chapter_scope")).lower()
            all_jee_chapters = chapter_scope in {
                "all_jee_chapters",
                "all_jee",
                "full_jee_syllabus",
            }
        if all_jee_chapters:
            auto_chapters, auto_subtopics = self._jee_chapter_catalog(subject=subject)
            if auto_chapters:
                chapters = auto_chapters[:]
                if not subtopics:
                    subtopics = auto_subtopics[:]
        weak_concepts = self._to_list_str(payload.get("weak_concepts_json"))
        if not chapters:
            chapters = self._to_list_str(payload.get("chapter"))
        if not chapters:
            chapters = [subject]
        if not subtopics:
            subtopics = chapters[:]
        difficulty = max(1, min(5, self._to_int(payload.get("difficulty"), 3)))
        raw_question_count = payload.get("question_count")
        question_count = max(
            1,
            min(60, self._to_int(raw_question_count, 10)),
        )
        if all_jee_chapters and raw_question_count is None:
            question_count = max(question_count, min(60, len(chapters)))
        trap_intensity = self._str(payload.get("trap_intensity") or "medium").lower()
        if trap_intensity == "extreme":
            trap_intensity = "high"
        if trap_intensity not in {"low", "medium", "high"}:
            trap_intensity = "medium"
        weakness_mode = self._to_bool(payload.get("weakness_mode"))
        cross_concept = self._to_bool(payload.get("cross_concept"))
        user_id = self._str(
            payload.get("user_id")
            or payload.get("student_id")
            or payload.get("account_id")
        )
        role = self._str(
            payload.get("role")
            or payload.get("user_role")
            or payload.get("request_role")
        ).lower()
        self_practice_mode = self._to_bool(
            payload.get("self_practice_mode")
            or payload.get("self_practice")
            or payload.get("practice_mode")
        )
        authoring_mode = self._to_bool(
            payload.get("authoring_mode") or payload.get("teacher_authoring_mode")
        )
        if role == "student" and not self_practice_mode:
            return {
                "ok": False,
                "status": "FORBIDDEN",
                "message": "Students can only generate self-practice quizzes",
            }
        if authoring_mode and role != "teacher":
            return {
                "ok": False,
                "status": "FORBIDDEN",
                "message": "Only teachers can generate authoring quizzes",
            }
        include_answer_key = self._to_bool(payload.get("include_answer_key")) and (
            role == "teacher" and authoring_mode and not self_practice_mode
        )
        pyq_focus = self._to_bool(
            payload.get("pyq_focus")
            or payload.get("prefer_pyq")
            or payload.get("use_pyq_patterns")
            or payload.get("prefer_previous_year_questions")
        )
        raw_allow_web = payload.get("allow_web_search")
        if raw_allow_web is None:
            raw_allow_web = payload.get("web_research_enabled")
        if raw_allow_web is None:
            raw_allow_web = payload.get("search_hard_pyq")
        allow_web_search = False
        pyq_answer_retrieval_required = self._to_bool(
            payload.get("pyq_answer_retrieval_required")
            or payload.get("require_answer_sources")
            or payload.get("require_pyq_answer_sources")
        )
        pyq_mode = self._str(payload.get("pyq_mode")).strip().lower()
        pyq_answer_retrieval_required = False
        pyq_web_only_mode = False
        strict_related_web_mode = False
        interactive_student_pyq_mode = (
            role == "student" and self_practice_mode and pyq_focus
        )
        require_type_variety = self._to_bool(payload.get("require_type_variety"))
        if (
            payload.get("require_type_variety") is None
            and role == "teacher"
            and authoring_mode
            and not self_practice_mode
            and question_count >= 6
        ):
            # Teacher authoring drafts should default to a mixed-type paper.
            require_type_variety = True
        if (
            payload.get("require_type_variety") is None
            and pyq_focus
            and strict_related_web_mode
            and question_count >= 3
        ):
            # Strict PYQ mode benefits from type spread to avoid repeated stems.
            require_type_variety = True
        forced_type = self._requested_generation_type(
            payload.get("forced_question_type")
            or payload.get("target_question_type")
            or payload.get("target_type")
            or payload.get("question_type")
        )
        if not forced_type and isinstance(payload.get("type_distribution_lock"), dict):
            lock = payload.get("type_distribution_lock") or {}
            keys = [self._requested_generation_type(k) for k in lock.keys()]
            keys = [k for k in keys if k]
            unique_keys = sorted(set(keys))
            if len(unique_keys) == 1:
                forced_type = unique_keys[0]

        engine_mode = self._route_engine_mode(difficulty, trap_intensity, cross_concept)
        profile = self._model_profile(engine_mode)
        profile_provider_count = max(
            1, min(3, self._to_int(profile.get("provider_count"), 1))
        )
        strict_hard_mode = self._to_bool(payload.get("strict_hard_mode"))
        ultra_hard_mode = self._to_bool(payload.get("ultra_hard_mode"))
        minimum_reasoning_steps = max(
            1, min(6, self._to_int(payload.get("minimum_reasoning_steps"), 2))
        )
        arena_raw = payload.get("arena_enabled")
        if arena_raw is None:
            arena_raw = payload.get("use_arena")
        if arena_raw is None:
            arena_raw = payload.get("arena_mode")
        arena_enabled = (
            self._to_bool(arena_raw)
            if arena_raw is not None
            else profile_provider_count > 1
        )
        arena_provider_raw = payload.get("arena_provider_count")
        if arena_provider_raw is None:
            arena_provider_raw = payload.get("provider_count")
        arena_provider_count = max(
            1, min(3, self._to_int(arena_provider_raw, profile_provider_count))
        )
        if strict_hard_mode or ultra_hard_mode or difficulty >= 4:
            arena_enabled = True
            arena_provider_count = max(arena_provider_count, profile_provider_count)
        if not arena_enabled:
            arena_provider_count = 1
        arena_select_hardest_raw = payload.get("arena_select_hardest")
        arena_select_hardest = (
            True
            if arena_select_hardest_raw is None
            else self._to_bool(arena_select_hardest_raw)
        )
        quiz_id = self._new_id("aiq")
        if self._safe_id(payload.get("quiz_id")) == quiz_id:
            # Prevent client forcing regeneration on same ID.
            quiz_id = self._new_id("aiq")

        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        deadline = self._str(payload.get("deadline"))
        if not deadline:
            deadline = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 7 * 86400)
            )
        duration = max(
            1,
            self._to_int(
                payload.get("duration") or payload.get("duration_minutes"),
                max(20, question_count * 2),
            ),
        )
        class_name = self._str(
            payload.get("class")
            or payload.get("class_name")
            or payload.get("target_class")
            or "Class 11",
        )
        web_scope_subject = self._str(payload.get("web_scope_subject") or subject) or subject
        web_scope_chapters = self._to_list_str(payload.get("web_scope_chapters")) or chapters
        web_scope_subtopics = self._to_list_str(payload.get("web_scope_subtopics")) or subtopics
        web_snippets: list[dict[str, Any]] = []
        solution_web_snippets: list[dict[str, Any]] = []
        offline_pyq_rows: list[dict[str, Any]] = []
        primary_web_diag: dict[str, Any] = {}
        solution_web_diag: dict[str, Any] = {}
        web_provider_diagnostics: dict[str, Any] = {}
        pyq_search_timeout_s = 2.4 if interactive_student_pyq_mode else 6.0
        pyq_page_timeout_s = 1.2 if interactive_student_pyq_mode else 3.5
        pyq_query_budget_override = 2 if interactive_student_pyq_mode else None
        pyq_page_check_budget = 2 if interactive_student_pyq_mode else None
        pyq_ai_recovery_limit = 0 if interactive_student_pyq_mode else max(2, question_count)
        is_single_question_pyq_shard = (
            pyq_focus
            and question_count <= 2
            and (
                bool(forced_type)
                or payload.get("question_slot") is not None
                or payload.get("target_question_type") is not None
                or payload.get("forced_question_type") is not None
            )
        )
        pyq_web_limit = (
            max(4, question_count * 2)
            if is_single_question_pyq_shard
            else max(3, min(6, question_count + 1))
            if interactive_student_pyq_mode
            else max(8, question_count * 3)
        )
        pyq_merge_limit = (
            max(6, question_count * 3)
            if is_single_question_pyq_shard
            else max(4, min(8, question_count * 2))
            if interactive_student_pyq_mode
            else max(8, question_count * 4)
        )
        if pyq_focus and allow_web_search:
            primary_pyq_rows = self._fetch_pyq_web_snippets(
                subject=web_scope_subject,
                chapters=web_scope_chapters,
                subtopics=web_scope_subtopics,
                query_suffix="JEE PYQ hard question",
                limit=pyq_web_limit,
                difficulty=difficulty,
                search_timeout_s=pyq_search_timeout_s,
                page_timeout_s=pyq_page_timeout_s,
                query_budget_override=pyq_query_budget_override,
                page_check_budget=pyq_page_check_budget,
            )
            primary_web_diag = dict(self._last_pyq_web_diagnostics)
            solution_pyq_rows = self._fetch_pyq_web_snippets(
                subject=web_scope_subject,
                chapters=web_scope_chapters,
                subtopics=web_scope_subtopics,
                query_suffix="JEE PYQ detailed solution answer key",
                limit=pyq_web_limit,
                difficulty=difficulty,
                search_timeout_s=pyq_search_timeout_s,
                page_timeout_s=pyq_page_timeout_s,
                query_budget_override=pyq_query_budget_override,
                page_check_budget=pyq_page_check_budget,
            )
            solution_web_diag = dict(self._last_pyq_web_diagnostics)
            merged_rows = self._merge_pyq_rows(
                primary_pyq_rows,
                solution_pyq_rows,
                limit=pyq_merge_limit,
            )
            quality_floor = 0.35 if strict_related_web_mode else 0.22
            web_snippets = [
                row
                for row in merged_rows
                if self._to_float(row.get("quality_score"), 0.0) >= quality_floor
                and self._to_float(row.get("scope_score"), 0.0) > 0.0
            ]
            if not web_snippets:
                web_snippets = merged_rows[: max(1, min(question_count * 2, 8))]
            solution_web_snippets = [
                row
                for row in web_snippets
                if self._to_bool(row.get("has_solution"))
                or self._to_bool(row.get("has_answer"))
            ]
            web_provider_diagnostics = {
                "primary": primary_web_diag,
                "solution": solution_web_diag,
                "combined": self._merge_pyq_web_diagnostics(
                    [
                        row
                        for row in (
                            *(primary_web_diag.get("attempts") or []),
                            *(solution_web_diag.get("attempts") or []),
                        )
                        if isinstance(row, dict)
                    ]
                ),
            }
        if pyq_focus and allow_web_search:
            offline_pyq_rows = self._local_pyq_archive_rows(
                subject=web_scope_subject,
                chapters=web_scope_chapters,
                subtopics=web_scope_subtopics,
                limit=max(8, question_count * 4),
            )
            if offline_pyq_rows:
                seen_keys = {
                    (
                        self._str(row.get("url")).strip().lower(),
                        self._str(row.get("question_stub")).strip().lower(),
                    )
                    for row in web_snippets
                }
                appended_offline = 0
                for row in offline_pyq_rows:
                    key = (
                        self._str(row.get("url")).strip().lower(),
                        self._str(row.get("question_stub")).strip().lower(),
                    )
                    if key in seen_keys:
                        continue
                    web_snippets.append(dict(row))
                    seen_keys.add(key)
                    appended_offline += 1
                if isinstance(web_provider_diagnostics, dict):
                    combined_diag = web_provider_diagnostics.get("combined")
                    if isinstance(combined_diag, dict):
                        combined_diag["offline_archive_appended"] = appended_offline
            if not solution_web_snippets:
                solution_web_snippets = [
                    row
                    for row in web_snippets
                    if self._to_bool(row.get("has_solution"))
                    or self._to_bool(row.get("has_answer"))
                ]

        dns_blocked_mode = False
        if pyq_focus and allow_web_search and isinstance(web_provider_diagnostics, dict):
            combined_diag = web_provider_diagnostics.get("combined")
            if isinstance(combined_diag, dict):
                queries_with_results = self._to_int(
                    combined_diag.get("queries_with_results"),
                    0,
                )
                dns_hits = self._to_int(combined_diag.get("dns_error_hits"), 0)
                dns_signals: list[str] = [
                    self._str(combined_diag.get("web_error_reason")),
                    self._str(combined_diag.get("web_error_detail")),
                ]
                for attempt in combined_diag.get("attempts") or []:
                    if not isinstance(attempt, dict):
                        continue
                    dns_signals.append(self._str(attempt.get("error_reason")))
                    for prov in attempt.get("providers") or []:
                        if not isinstance(prov, dict):
                            continue
                        dns_signals.append(self._str(prov.get("error")))
                        dns_signals.append(self._str(prov.get("error_detail")))
                if any(
                    self._canonical_web_error_reason(token) == "dns_resolution_failed"
                    for token in dns_signals
                    if token
                ):
                    dns_hits = max(dns_hits, 1)
                dns_blocked_mode = queries_with_results <= 0 and dns_hits > 0
        offline_pyq_only_mode = pyq_focus and allow_web_search and dns_blocked_mode
        strict_quality_lock = strict_hard_mode or ultra_hard_mode or difficulty >= 4
        hardness_floor = 150.0 if strict_quality_lock else 85.0
        if ultra_hard_mode or difficulty >= 5:
            hardness_floor = 170.0

        questions: list[dict[str, Any]] = []
        seen_question_stems: set[str] = set()
        attempts = 0
        max_attempts = question_count * (
            10
            if interactive_student_pyq_mode and strict_quality_lock
            else 6
            if interactive_student_pyq_mode
            else 24
            if strict_quality_lock
            else 10
        )
        arena_provider_wins: dict[str, int] = {}
        arena_entropy_values: list[float] = []
        arena_disagreement_values: list[float] = []
        web_source_usage: dict[str, int] = {}
        web_source_applied_count = 0
        web_source_online_applied_count = 0
        web_source_offline_applied_count = 0
        pyq_ai_solution_recovery_count = 0
        strict_web_requirement_unmet = False
        online_web_pool = [
            row
            for row in web_snippets
            if self._str(row.get("url")).strip().lower().startswith(("http://", "https://"))
        ]
        offline_web_pool = [
            row
            for row in web_snippets
            if not self._str(row.get("url")).strip().lower().startswith(("http://", "https://"))
        ]
        offline_usable_pool: list[dict[str, Any]] = []
        if pyq_focus and offline_web_pool:
            offline_probe_budget = min(
                len(offline_web_pool),
                max(12, question_count * 8),
            )
            probe_chapters = web_scope_chapters or chapters
            probe_subtopics = web_scope_subtopics or subtopics
            for probe_row in offline_web_pool[:offline_probe_budget]:
                probe_question = self._question_from_web_source(
                    row=probe_row,
                    idx=0,
                    subject=subject,
                    chapters=probe_chapters,
                    subtopics=probe_subtopics,
                    minimum_reasoning_steps=minimum_reasoning_steps,
                )
                if probe_question is None:
                    continue
                try:
                    probe_prepared = self._prepare_question_for_grading(
                        probe_question,
                        fallback_question_id=self._str(probe_question.get("question_id")) or "q_probe",
                    )
                    validate_question_structure(probe_prepared, student_mode=False)
                except (QuestionStructureError, ValueError):
                    continue
                if not self._question_matches_requested_scope(
                    question=probe_prepared,
                    subject=subject,
                    chapters=probe_chapters,
                    subtopics=probe_subtopics,
                ):
                    continue
                offline_usable_pool.append(dict(probe_row))
        offline_pyq_fallback_locked = (
            offline_pyq_only_mode and len(offline_usable_pool) >= max(1, question_count)
        )
        full_syllabus_template_preferred = bool(all_jee_chapters and pyq_focus)
        pyq_selection_pool = (
            [dict(row) for row in (offline_usable_pool or offline_web_pool)]
            if pyq_focus and offline_pyq_only_mode and (offline_usable_pool or offline_web_pool)
            else ([dict(row) for row in web_snippets] if pyq_focus else [])
        )
        if full_syllabus_template_preferred:
            # Imported PYQ rows are still noisy for cross-chapter all-syllabus generation;
            # prefer deterministic chapter templates for stable coverage.
            pyq_selection_pool = []
            offline_pyq_fallback_locked = False
        strict_online_only_mode = (
            pyq_focus
            and strict_related_web_mode
            and allow_web_search
            and not dns_blocked_mode
        )
        effective_strict_web_mode = (
            strict_online_only_mode and bool(online_web_pool)
        )
        mixed_pyq_source_mode = (
            pyq_focus
            and allow_web_search
            and not dns_blocked_mode
            and bool(online_web_pool)
            and bool(offline_web_pool)
            and not strict_online_only_mode
        )
        chapter_first_mode = all_jee_chapters and bool(chapters)
        while len(questions) < question_count and attempts < max_attempts:
            idx = len(questions)
            attempts += 1
            concept_tags = []
            base_pool = weak_concepts if weakness_mode and weak_concepts else subtopics
            if chapter_first_mode and chapters:
                concept_tags.append(chapters[idx % len(chapters)])
                if base_pool:
                    concept_tags.append(base_pool[idx % len(base_pool)])
            else:
                if base_pool:
                    concept_tags.append(base_pool[idx % len(base_pool)])
                if chapters:
                    concept_tags.append(chapters[idx % len(chapters)])
            concept_tags = [self._str(x) for x in concept_tags if self._str(x)]
            if not concept_tags:
                concept_tags = [subject]
            if cross_concept and len(concept_tags) < 2:
                extra = chapters[(idx + 1) % len(chapters)] if chapters else subject
                if self._str(extra) and self._str(extra) not in concept_tags:
                    concept_tags.append(self._str(extra))
            scope_chapter = (
                self._str(chapters[idx % len(chapters)]) if chapters else self._str(subject)
            )
            if chapter_first_mode:
                scope_subtopic = self._str(scope_chapter)
            else:
                scope_subtopic = (
                    self._str(base_pool[idx % len(base_pool)])
                    if base_pool
                    else self._str(scope_chapter)
                )
            scope_chapters = [scope_chapter] if scope_chapter else [self._str(subject)]
            scope_subtopics = [scope_subtopic] if scope_subtopic else scope_chapters[:]

            seed_key_root = (
                f"{quiz_id}|{subject}|{difficulty}|{trap_intensity}|{idx}|"
                f"{'|'.join(concept_tags)}"
            )
            type_seed_hint = f"{quiz_id}|{subject}|{'|'.join(chapters[:12])}"
            target_type_for_item = self._target_generation_type(
                idx=idx,
                forced_type=forced_type,
                require_type_variety=require_type_variety and question_count >= 3,
                seed_hint=type_seed_hint,
            )
            provider_count_for_item = max(1, min(3, arena_provider_count))
            candidates: list[tuple[float, dict[str, Any], str]] = []
            for provider_slot in range(provider_count_for_item):
                provider_name = self._arena_provider_name(provider_slot)
                seed_key = f"{seed_key_root}|provider={provider_name}|attempt={attempts}"
                candidate = self._question_from_chapter_template(
                    idx=idx,
                    subject=subject,
                    concept_tags=concept_tags,
                    difficulty=difficulty,
                    trap_intensity=trap_intensity,
                    cross_concept=cross_concept,
                    seed_key=seed_key,
                    forced_question_type=target_type_for_item,
                )
                candidate["solution_explanation"] = self._ensure_minimum_solution_steps(
                    self._str(candidate.get("solution_explanation")),
                    minimum_reasoning_steps,
                )
                if not self._question_matches_requested_scope(
                    question=candidate,
                    subject=subject,
                    chapters=scope_chapters,
                    subtopics=scope_subtopics,
                ):
                    continue
                if not self._validate_generated_question(candidate):
                    continue
                hardness_score = self._question_hardness_score(
                    question=candidate,
                    requested_difficulty=difficulty,
                    trap_intensity=trap_intensity,
                    cross_concept=cross_concept,
                    minimum_reasoning_steps=minimum_reasoning_steps,
                )
                if hardness_score < 0 or hardness_score < hardness_floor:
                    continue
                candidates.append((hardness_score, candidate, provider_name))

            if not candidates:
                continue
            candidates.sort(key=lambda x: x[0], reverse=True)
            if arena_select_hardest:
                chosen_score, question, winner_provider = candidates[0]
            else:
                chosen_score, question, winner_provider = candidates[0]
            arena_provider_wins[winner_provider] = (
                arena_provider_wins.get(winner_provider, 0) + 1
            )
            if len(candidates) > 1:
                candidate_scores = [row[0] for row in candidates]
                entropy = self._arena_entropy(candidate_scores)
                disagreement = max(candidate_scores) - min(candidate_scores)
                arena_entropy_values.append(entropy)
                arena_disagreement_values.append(disagreement)
            else:
                arena_entropy_values.append(0.0)
                arena_disagreement_values.append(0.0)
            question["_arena_provider"] = winner_provider
            question["_arena_hardness_score"] = round(chosen_score, 6)
            try:
                prepared = self._prepare_question_for_grading(
                    question,
                    fallback_question_id=self._str(question.get("question_id"))
                    or f"q_{idx + 1}",
                )
                validate_question_structure(prepared, student_mode=False)
            except (QuestionStructureError, ValueError):
                continue
            verify_ok, verify_note = self._deterministic_verify_candidate(
                question=prepared,
                subject=subject,
            )
            if not verify_ok:
                continue
            prepared["verification_note"] = verify_note
            source_origin = "synthesized"
            source_stub = ""
            source_url = ""
            source_question_stub = ""
            source_answer_stub = ""
            source_solution_stub = ""
            source_quality_score = 0.0
            candidate_web_sources = pyq_selection_pool if pyq_focus else web_snippets
            if pyq_focus and strict_online_only_mode and candidate_web_sources:
                candidate_web_sources = [
                    row
                    for row in candidate_web_sources
                    if self._str(row.get("url")).strip().lower().startswith(("http://", "https://"))
                ]
            if pyq_focus and effective_strict_web_mode and candidate_web_sources:
                strict_ready_rows: list[dict[str, Any]] = []
                strict_scan_budget = max(8, min(len(candidate_web_sources), question_count * 8))
                for strict_row in candidate_web_sources[:strict_scan_budget]:
                    if not (
                        self._to_bool(strict_row.get("has_answer"))
                        or self._to_bool(strict_row.get("has_solution"))
                    ):
                        continue
                    strict_web_question = self._question_from_web_source(
                        row=strict_row,
                        idx=idx,
                        subject=subject,
                        chapters=scope_chapters,
                        subtopics=scope_subtopics,
                        minimum_reasoning_steps=minimum_reasoning_steps,
                    )
                    if strict_web_question is None:
                        continue
                    try:
                        strict_prepared = self._prepare_question_for_grading(
                            strict_web_question,
                            fallback_question_id=self._str(strict_web_question.get("question_id"))
                            or f"q_{idx + 1}",
                        )
                    except (QuestionStructureError, ValueError):
                        continue
                    if not self._question_matches_requested_scope(
                        question=strict_prepared,
                        subject=subject,
                        chapters=scope_chapters,
                        subtopics=scope_subtopics,
                    ):
                        continue
                    strict_ready_rows.append(strict_row)
                if strict_ready_rows:
                    candidate_web_sources = strict_ready_rows
            selection_pool = candidate_web_sources
            if mixed_pyq_source_mode and candidate_web_sources:
                online_candidates = [
                    row
                    for row in candidate_web_sources
                    if self._str(row.get("url")).strip().lower().startswith(("http://", "https://"))
                ]
                offline_candidates = [
                    row
                    for row in candidate_web_sources
                    if not self._str(row.get("url")).strip().lower().startswith(("http://", "https://"))
                ]
                if online_candidates and offline_candidates:
                    prefer_online_for_item = (
                        web_source_online_applied_count <= web_source_offline_applied_count
                    )
                    if web_source_online_applied_count == web_source_offline_applied_count:
                        prefer_online_for_item = (idx % 2 == 0)
                    selection_pool = (
                        online_candidates if prefer_online_for_item else offline_candidates
                    )
            selected_web_source = (
                self._best_pyq_source_for_question(
                    question=prepared,
                    web_sources=selection_pool,
                    used_url_counts=web_source_usage,
                    requested_difficulty=difficulty,
                    strict_mode=effective_strict_web_mode,
                )
                if pyq_focus and selection_pool
                else None
            )
            if (
                selected_web_source is None
                and pyq_focus
                and selection_pool is not candidate_web_sources
                and candidate_web_sources
            ):
                selected_web_source = self._best_pyq_source_for_question(
                    question=prepared,
                    web_sources=candidate_web_sources,
                    used_url_counts=web_source_usage,
                    requested_difficulty=difficulty,
                    strict_mode=effective_strict_web_mode,
                )
            if selected_web_source is None and pyq_focus and candidate_web_sources:
                selected_web_source = max(
                    candidate_web_sources,
                    key=lambda row: (
                        self._to_float(row.get("quality_score"), 0.0),
                        self._to_float(row.get("scope_score"), 0.0),
                        1.0 if self._to_bool(row.get("has_answer")) else 0.0,
                        1.0 if self._to_bool(row.get("has_solution")) else 0.0,
                    ),
                )
            backup_web_sources: list[dict[str, Any]] = []
            if pyq_focus and candidate_web_sources:
                selected_url_token = (
                    self._str(selected_web_source.get("url")).strip().lower()
                    if isinstance(selected_web_source, dict)
                    else ""
                )
                selected_stub_token = (
                    self._str(selected_web_source.get("question_stub")).strip().lower()
                    if isinstance(selected_web_source, dict)
                    else ""
                )
                backup_web_sources = [
                    dict(row)
                    for row in candidate_web_sources
                    if (
                        self._str(row.get("url")).strip().lower(),
                        self._str(row.get("question_stub")).strip().lower(),
                    )
                    != (selected_url_token, selected_stub_token)
                ]
                backup_web_sources.sort(
                    key=lambda row: (
                        self._to_float(row.get("quality_score"), 0.0),
                        self._to_float(row.get("scope_score"), 0.0),
                        self._to_float(row.get("hardness_score"), 0.0),
                        1.0 if self._to_bool(row.get("has_answer")) else 0.0,
                        1.0 if self._to_bool(row.get("has_solution")) else 0.0,
                    ),
                    reverse=True,
                )
            if (
                pyq_focus
                and strict_online_only_mode
                and selected_web_source is None
            ):
                # Strict related-web mode should not silently fall back per item.
                continue
            web_question_used = False
            if selected_web_source is not None:
                source_stub = self._str(
                    selected_web_source.get("title")
                    or selected_web_source.get("question_stub")
                    or selected_web_source.get("snippet")
                )
                source_url = self._str(selected_web_source.get("url"))
                source_question_stub = self._str(selected_web_source.get("question_stub"))
                source_answer_stub = self._str(selected_web_source.get("answer_stub"))
                source_solution_stub = self._str(selected_web_source.get("solution_stub"))
                source_quality_score = self._to_float(
                    selected_web_source.get("quality_score"),
                    0.0,
                )
                web_question = self._question_from_web_source(
                    row=selected_web_source,
                    idx=idx,
                    subject=subject,
                    chapters=scope_chapters,
                    subtopics=scope_subtopics,
                    minimum_reasoning_steps=minimum_reasoning_steps,
                )
                if web_question is not None:
                    exact_local_bank_source = isinstance(
                        selected_web_source.get("bank_payload"),
                        dict,
                    )
                    web_variants: list[dict[str, Any]] = []
                    if target_type_for_item and not exact_local_bank_source:
                        raw_web_options = web_question.get("options")
                        has_real_options = (
                            isinstance(raw_web_options, list)
                            and len(raw_web_options) >= 4
                            and not any(
                                re.fullmatch(
                                    r"Option\s*[1-4]",
                                    self._str(x),
                                    flags=re.IGNORECASE,
                                )
                                for x in raw_web_options
                            )
                        )
                        if target_type_for_item == "NUMERICAL" or has_real_options:
                            web_variants.append(
                                self._coerce_generated_question_type(
                                    dict(web_question),
                                    target_type=target_type_for_item,
                                    seed_key=f"{seed_key_root}|web|{attempts}",
                                )
                            )
                    if not web_variants and target_type_for_item:
                        # Keep native web type if coercion would create placeholder options.
                        web_variants.append(
                            dict(web_question)
                        )
                    web_variants.append(dict(web_question))
                    # De-duplicate by type/text signature while preserving order.
                    unique_variants: list[dict[str, Any]] = []
                    seen_variant_keys: set[tuple[str, str]] = set()
                    for candidate_web in web_variants:
                        candidate_key = (
                            self._canonical_question_type(candidate_web.get("question_type")),
                            self._str(candidate_web.get("question_text")).strip().lower(),
                        )
                        if candidate_key in seen_variant_keys:
                            continue
                        seen_variant_keys.add(candidate_key)
                        unique_variants.append(candidate_web)
                    for candidate_web in unique_variants:
                        try:
                            prepared_web = self._prepare_question_for_grading(
                                candidate_web,
                                fallback_question_id=self._str(candidate_web.get("question_id"))
                                or f"q_{idx + 1}",
                            )
                            validate_question_structure(prepared_web, student_mode=False)
                        except (QuestionStructureError, ValueError):
                            continue
                        if not (
                            self._question_matches_requested_scope(
                                question=prepared_web,
                                subject=subject,
                                chapters=scope_chapters,
                                subtopics=scope_subtopics,
                            )
                            and self._validate_generated_question(prepared_web)
                        ):
                            continue
                        web_score = self._question_hardness_score(
                            question=prepared_web,
                            requested_difficulty=difficulty,
                            trap_intensity=trap_intensity,
                            cross_concept=cross_concept,
                            minimum_reasoning_steps=minimum_reasoning_steps,
                        )
                        web_score_floor = hardness_floor
                        source_hard = self._to_float(
                            selected_web_source.get("hardness_score"),
                            0.0,
                        )
                        source_quality = self._to_float(
                            selected_web_source.get("quality_score"),
                            0.0,
                        )
                        if effective_strict_web_mode and (
                            source_hard >= 0.7 or source_quality >= 0.9
                        ):
                            web_score_floor = min(web_score_floor, 118.0)
                        if exact_local_bank_source:
                            web_score_floor = min(web_score_floor, 90.0)
                        if web_score < web_score_floor:
                            continue
                        prepared = prepared_web
                        prepared["_arena_provider"] = "web_pyq_source"
                        prepared["_arena_hardness_score"] = round(web_score, 6)
                        verify_ok, verify_note = self._deterministic_verify_candidate(
                            question=prepared,
                            subject=subject,
                        )
                        if not verify_ok:
                            continue
                        prepared["verification_note"] = verify_note
                        web_question_used = True
                        break
                if pyq_focus and not web_question_used:
                    emergency_web = self._question_from_web_source(
                        row=selected_web_source,
                        idx=idx,
                        subject=subject,
                        chapters=scope_chapters,
                        subtopics=scope_subtopics,
                        minimum_reasoning_steps=minimum_reasoning_steps,
                    )
                    if emergency_web is not None:
                        try:
                            emergency_prepared = self._prepare_question_for_grading(
                                emergency_web,
                                fallback_question_id=self._str(emergency_web.get("question_id"))
                                or f"q_{idx + 1}",
                            )
                            if self._question_matches_requested_scope(
                                question=emergency_prepared,
                                subject=subject,
                                chapters=scope_chapters,
                                subtopics=scope_subtopics,
                            ):
                                prepared = emergency_prepared
                                prepared["_arena_provider"] = "web_pyq_source"
                                prepared["_arena_hardness_score"] = round(
                                    self._question_hardness_score(
                                        question=prepared,
                                        requested_difficulty=difficulty,
                                        trap_intensity=trap_intensity,
                                        cross_concept=cross_concept,
                                        minimum_reasoning_steps=minimum_reasoning_steps,
                                    ),
                                    6,
                                )
                                prepared["verification_note"] = "structural_verified"
                                web_question_used = True
                        except (QuestionStructureError, ValueError):
                            web_question_used = False
                if (
                    pyq_focus
                    and not web_question_used
                    and backup_web_sources
                ):
                    backup_budget = max(
                        2,
                        min(len(backup_web_sources), max(4, question_count * 2)),
                    )
                    for backup_row in backup_web_sources[:backup_budget]:
                        backup_web = self._question_from_web_source(
                            row=backup_row,
                            idx=idx,
                            subject=subject,
                            chapters=scope_chapters,
                            subtopics=scope_subtopics,
                            minimum_reasoning_steps=minimum_reasoning_steps,
                        )
                        if backup_web is None:
                            continue
                        try:
                            backup_prepared = self._prepare_question_for_grading(
                                backup_web,
                                fallback_question_id=self._str(backup_web.get("question_id"))
                                or f"q_{idx + 1}",
                            )
                            if not self._question_matches_requested_scope(
                                question=backup_prepared,
                                subject=subject,
                                chapters=scope_chapters,
                                subtopics=scope_subtopics,
                            ):
                                continue
                            backup_score = self._question_hardness_score(
                                question=backup_prepared,
                                requested_difficulty=difficulty,
                                trap_intensity=trap_intensity,
                                cross_concept=cross_concept,
                                minimum_reasoning_steps=minimum_reasoning_steps,
                            )
                            verify_ok, verify_note = self._deterministic_verify_candidate(
                                question=backup_prepared,
                                subject=subject,
                            )
                            if not verify_ok:
                                continue
                            prepared = backup_prepared
                            prepared["_arena_provider"] = "web_pyq_source"
                            prepared["_arena_hardness_score"] = round(backup_score, 6)
                            prepared["verification_note"] = verify_note
                            selected_web_source = dict(backup_row)
                            source_stub = self._str(
                                selected_web_source.get("title")
                                or selected_web_source.get("question_stub")
                                or selected_web_source.get("snippet")
                            )
                            source_url = self._str(selected_web_source.get("url"))
                            source_question_stub = self._str(
                                selected_web_source.get("question_stub")
                            )
                            source_answer_stub = self._str(selected_web_source.get("answer_stub"))
                            source_solution_stub = self._str(
                                selected_web_source.get("solution_stub")
                            )
                            source_quality_score = self._to_float(
                                selected_web_source.get("quality_score"),
                                0.0,
                            )
                            web_question_used = True
                            break
                        except (QuestionStructureError, ValueError):
                            continue
                if pyq_focus and effective_strict_web_mode and not web_question_used:
                    continue
                if pyq_focus and offline_pyq_fallback_locked and not web_question_used:
                    continue
                if web_question_used:
                    if source_answer_stub and source_solution_stub:
                        source_origin = "web_pyq_verified"
                    elif source_answer_stub or source_solution_stub:
                        source_origin = "web_pyq_ai_solution"
                    else:
                        source_origin = "web_pyq_unverified"
                else:
                    source_origin = (
                        "ai_synth_ultra_verified"
                        if pyq_focus and difficulty >= 5
                        else "synthesized_pyq"
                    )
                base_solution = self._str(
                    prepared.get("_solution_explanation")
                    or prepared.get("solution_explanation")
                )
                if source_solution_stub:
                    base_solution = (
                        f"{base_solution} Web solution cue: {source_solution_stub}."
                    ).strip()
                if source_answer_stub:
                    base_solution = (
                        f"{base_solution} Web answer cue: {source_answer_stub}."
                    ).strip()
                needs_ai_recovery = pyq_answer_retrieval_required and (
                    not source_answer_stub or not source_solution_stub
                )
                if web_question_used and not source_solution_stub and pyq_answer_retrieval_required:
                    needs_ai_recovery = True
                if needs_ai_recovery and (
                    pyq_ai_solution_recovery_count < pyq_ai_recovery_limit
                ):
                    recovered = await self._recover_solution_via_ai_engine(
                        question=prepared,
                        source_row=selected_web_source,
                        minimum_reasoning_steps=minimum_reasoning_steps,
                    )
                    rec_solution = self._str(recovered.get("solution_explanation"))
                    if rec_solution:
                        base_solution = rec_solution
                        pyq_ai_solution_recovery_count += 1
                        if web_question_used:
                            source_origin = "web_pyq_ai_solution"
                    rec_answer = self._extract_answer_token(
                        self._str(recovered.get("answer_token"))
                    )
                    if rec_answer and not source_answer_stub:
                        source_answer_stub = rec_answer
                prepared_solution = self._ensure_minimum_solution_steps(
                    base_solution,
                    minimum_reasoning_steps,
                )
                prepared["_solution_explanation"] = prepared_solution
                prepared["solution_explanation"] = prepared_solution
            elif pyq_focus:
                if effective_strict_web_mode or offline_pyq_fallback_locked:
                    continue
                source_origin = (
                    "ai_synth_ultra_verified"
                    if difficulty >= 5
                    else "synthesized_pyq"
                )
                if (
                    pyq_answer_retrieval_required
                    and pyq_ai_solution_recovery_count < pyq_ai_recovery_limit
                ):
                    recovered = await self._recover_solution_via_ai_engine(
                        question=prepared,
                        source_row=None,
                        minimum_reasoning_steps=minimum_reasoning_steps,
                    )
                    rec_solution = self._str(recovered.get("solution_explanation"))
                    if rec_solution:
                        prepared["_solution_explanation"] = rec_solution
                        prepared["solution_explanation"] = rec_solution
                        pyq_ai_solution_recovery_count += 1
            prepared["source_origin"] = source_origin
            prepared["source_stub"] = source_stub
            if source_url:
                prepared["source_url"] = source_url
            if source_question_stub:
                prepared["source_question_stub"] = source_question_stub
            if source_answer_stub:
                prepared["source_answer_stub"] = source_answer_stub
            if source_solution_stub:
                prepared["source_solution_stub"] = source_solution_stub
            if source_quality_score > 0:
                prepared["source_quality_score"] = round(source_quality_score, 6)
            if selected_web_source is not None:
                selected_tags = selected_web_source.get("chapter_tags")
                if chapter_first_mode and scope_chapter:
                    prepared["chapter_tags"] = [scope_chapter]
                elif isinstance(selected_tags, list) and selected_tags:
                    prepared["chapter_tags"] = [
                        self._str(x) for x in selected_tags if self._str(x)
                    ][:3]
                elif scope_chapter:
                    prepared["chapter_tags"] = [scope_chapter]
            if chapter_first_mode and scope_chapter:
                normalized_tags: list[str] = [scope_chapter]
                if scope_subtopic and scope_subtopic != scope_chapter:
                    normalized_tags.append(scope_subtopic)
                for raw_tag in (prepared.get("concept_tags") or []):
                    token = self._str(raw_tag)
                    if token and token not in normalized_tags:
                        normalized_tags.append(token)
                prepared["concept_tags"] = normalized_tags[:4]
            if (
                (not isinstance(prepared.get("chapter_tags"), list))
                or (not prepared.get("chapter_tags"))
            ) and scope_chapter:
                prepared["chapter_tags"] = [scope_chapter]
            prepared["verification_pass"] = True
            prepared["critic_score"] = round(
                self._to_float(prepared.get("_arena_hardness_score"), chosen_score),
                6,
            )
            prepared["confidence_score"] = round(
                max(
                    0.55,
                    min(
                        0.99,
                        self._to_float(prepared.get("source_quality_score"), 0.0)
                        + 0.35,
                    ),
                ),
                6,
            )
            prepared["difficulty_score"] = round(
                self._to_float(
                    prepared.get("difficulty_score")
                    or selected_web_source.get("difficulty_score")
                    if selected_web_source
                    else chosen_score,
                    chosen_score,
                ),
                6,
            )
            provider_used = self._str(prepared.get("_arena_provider") or winner_provider)
            prepared["provider_used"] = provider_used
            prepared["fallback_used"] = provider_used.startswith("template_")
            dedupe_key = re.sub(
                r"\s+",
                " ",
                self._str(prepared.get("question_text")).lower(),
            ).strip()
            strict_web_repeat_allowed = (
                pyq_focus and effective_strict_web_mode and web_question_used
            )
            if dedupe_key and dedupe_key in seen_question_stems and not strict_web_repeat_allowed:
                continue
            if dedupe_key and not strict_web_repeat_allowed:
                seen_question_stems.add(dedupe_key)
            if web_question_used:
                web_source_applied_count += 1
                src_url_low = self._str(source_url).strip().lower()
                if src_url_low:
                    web_source_usage[src_url_low] = web_source_usage.get(src_url_low, 0) + 1
                if src_url_low.startswith(("http://", "https://")):
                    web_source_online_applied_count += 1
                else:
                    web_source_offline_applied_count += 1
            questions.append(prepared)

        web_error_reason = ""
        if pyq_focus and allow_web_search and isinstance(web_provider_diagnostics, dict):
            web_error_reason = self._str(
                (web_provider_diagnostics.get("combined") or {}).get("web_error_reason")
            )

        if (
            pyq_focus
            and strict_online_only_mode
            and web_source_online_applied_count <= 0
        ):
            if not web_error_reason:
                web_error_reason = "web_fetch_mandatory_but_no_verified_pyq"
            strict_web_requirement_unmet = True

        # Reliability backfill: if PYQ mode produced fewer rows than requested,
        # complete the set with deterministic hard template generation instead
        # of failing the entire request.
        if len(questions) < question_count and pyq_focus:
            backfill_attempts = 0
            backfill_max_attempts = (
                max(16, question_count * 8)
                if interactive_student_pyq_mode
                else max(24, question_count * 18)
            )
            backfill_floor = 95.0 if strict_quality_lock else 75.0
            if difficulty >= 5:
                backfill_floor = 105.0
            while (
                len(questions) < question_count
                and backfill_attempts < backfill_max_attempts
            ):
                idx = len(questions)
                backfill_attempts += 1
                base_pool = weak_concepts if weakness_mode and weak_concepts else subtopics
                concept_tags: list[str] = []
                if chapter_first_mode and chapters:
                    concept_tags.append(self._str(chapters[idx % len(chapters)]))
                    if base_pool:
                        concept_tags.append(self._str(base_pool[idx % len(base_pool)]))
                else:
                    if base_pool:
                        concept_tags.append(self._str(base_pool[idx % len(base_pool)]))
                    if chapters:
                        concept_tags.append(self._str(chapters[idx % len(chapters)]))
                concept_tags = [self._str(x) for x in concept_tags if self._str(x)] or [
                    self._str(subject)
                ]
                if cross_concept and len(concept_tags) < 2 and chapters:
                    extra = self._str(chapters[(idx + 1) % len(chapters)])
                    if extra and extra not in concept_tags:
                        concept_tags.append(extra)
                scope_chapter = (
                    self._str(chapters[idx % len(chapters)])
                    if chapters
                    else self._str(subject)
                )
                if chapter_first_mode:
                    scope_subtopic = self._str(scope_chapter)
                else:
                    scope_subtopic = (
                        self._str(base_pool[idx % len(base_pool)])
                        if base_pool
                        else self._str(scope_chapter)
                    )
                scope_chapters = [scope_chapter] if scope_chapter else [self._str(subject)]
                scope_subtopics = [scope_subtopic] if scope_subtopic else scope_chapters[:]
                target_type_for_item = self._target_generation_type(
                    idx=idx,
                    forced_type=forced_type,
                    require_type_variety=require_type_variety and question_count >= 3,
                    seed_hint=f"{quiz_id}|backfill|{subject}|{'|'.join(chapters[:10])}",
                )
                seed_key = (
                    f"{quiz_id}|pyq_backfill|{subject}|{difficulty}|{idx}|"
                    f"{backfill_attempts}|{'|'.join(concept_tags)}"
                )
                candidate = self._question_from_chapter_template(
                    idx=idx,
                    subject=subject,
                    concept_tags=concept_tags,
                    difficulty=max(4, difficulty),
                    trap_intensity=trap_intensity if strict_quality_lock else "high",
                    cross_concept=cross_concept,
                    seed_key=seed_key,
                    forced_question_type=target_type_for_item,
                )
                candidate["solution_explanation"] = self._ensure_minimum_solution_steps(
                    self._str(candidate.get("solution_explanation")),
                    minimum_reasoning_steps,
                )
                if not self._question_matches_requested_scope(
                    question=candidate,
                    subject=subject,
                    chapters=scope_chapters,
                    subtopics=scope_subtopics,
                ):
                    continue
                if not self._validate_generated_question(candidate):
                    continue
                backfill_score = self._question_hardness_score(
                    question=candidate,
                    requested_difficulty=difficulty,
                    trap_intensity=trap_intensity,
                    cross_concept=cross_concept,
                    minimum_reasoning_steps=minimum_reasoning_steps,
                )
                if backfill_score < backfill_floor:
                    continue
                try:
                    prepared_backfill = self._prepare_question_for_grading(
                        candidate,
                        fallback_question_id=self._str(candidate.get("question_id"))
                        or f"q_{idx + 1}",
                    )
                    validate_question_structure(prepared_backfill, student_mode=False)
                except (QuestionStructureError, ValueError):
                    continue
                verify_ok, verify_note = self._deterministic_verify_candidate(
                    question=prepared_backfill,
                    subject=subject,
                )
                if not verify_ok:
                    continue
                dedupe_key = re.sub(
                    r"\s+",
                    " ",
                    self._str(prepared_backfill.get("question_text")).lower(),
                ).strip()
                if dedupe_key and dedupe_key in seen_question_stems:
                    continue
                if dedupe_key:
                    seen_question_stems.add(dedupe_key)
                prepared_backfill["verification_note"] = verify_note
                prepared_backfill["source_origin"] = "ai_synth_backfill"
                prepared_backfill["source_stub"] = "offline_pyq_backfill"
                prepared_backfill["verification_pass"] = True
                prepared_backfill["critic_score"] = round(backfill_score, 6)
                prepared_backfill["confidence_score"] = round(
                    max(
                        0.62,
                        min(
                            0.93,
                            0.62 + (0.01 * min(18, len(questions))),
                        ),
                    ),
                    6,
                )
                prepared_backfill["difficulty_score"] = round(backfill_score, 6)
                prepared_backfill["provider_used"] = "template_backfill"
                prepared_backfill["fallback_used"] = True
                if scope_chapter:
                    prepared_backfill["chapter_tags"] = [scope_chapter]
                if chapter_first_mode and scope_chapter:
                    normalized_tags: list[str] = [scope_chapter]
                    if scope_subtopic and scope_subtopic != scope_chapter:
                        normalized_tags.append(scope_subtopic)
                    for raw_tag in (prepared_backfill.get("concept_tags") or []):
                        token = self._str(raw_tag)
                        if token and token not in normalized_tags:
                            normalized_tags.append(token)
                    prepared_backfill["concept_tags"] = normalized_tags[:4]
                questions.append(prepared_backfill)

        # Final reliability pass for all modes: complete to exact requested count.
        if len(questions) < question_count:
            emergency_attempts = 0
            emergency_max_attempts = max(48, question_count * 30)
            while (
                len(questions) < question_count
                and emergency_attempts < emergency_max_attempts
            ):
                idx = len(questions)
                emergency_attempts += 1
                base_pool = weak_concepts if weakness_mode and weak_concepts else subtopics
                concept_tags: list[str] = []
                if chapter_first_mode and chapters:
                    concept_tags.append(self._str(chapters[idx % len(chapters)]))
                    if base_pool:
                        concept_tags.append(self._str(base_pool[idx % len(base_pool)]))
                else:
                    if base_pool:
                        concept_tags.append(self._str(base_pool[idx % len(base_pool)]))
                    if chapters:
                        concept_tags.append(self._str(chapters[idx % len(chapters)]))
                concept_tags = [self._str(x) for x in concept_tags if self._str(x)] or [
                    self._str(subject)
                ]
                if cross_concept and len(concept_tags) < 2 and chapters:
                    extra = self._str(chapters[(idx + 1) % len(chapters)])
                    if extra and extra not in concept_tags:
                        concept_tags.append(extra)
                scope_chapter = (
                    self._str(chapters[idx % len(chapters)])
                    if chapters
                    else self._str(subject)
                )
                if chapter_first_mode:
                    scope_subtopic = self._str(scope_chapter)
                else:
                    scope_subtopic = (
                        self._str(base_pool[idx % len(base_pool)])
                        if base_pool
                        else self._str(scope_chapter)
                    )
                scope_chapters = [scope_chapter] if scope_chapter else [self._str(subject)]
                scope_subtopics = [scope_subtopic] if scope_subtopic else scope_chapters[:]
                target_type_for_item = self._target_generation_type(
                    idx=idx,
                    forced_type=forced_type,
                    require_type_variety=require_type_variety and question_count >= 3,
                    seed_hint=f"{quiz_id}|emergency|{subject}|{'|'.join(chapters[:10])}",
                )
                seed_key = (
                    f"{quiz_id}|emergency_fill|{subject}|{difficulty}|"
                    f"{idx}|{emergency_attempts}|{'|'.join(concept_tags)}"
                )
                candidate = self._question_from_chapter_template(
                    idx=idx,
                    subject=subject,
                    concept_tags=concept_tags,
                    difficulty=max(3, difficulty),
                    trap_intensity=trap_intensity if trap_intensity in {"medium", "high"} else "medium",
                    cross_concept=cross_concept,
                    seed_key=seed_key,
                    forced_question_type=target_type_for_item,
                )
                candidate["solution_explanation"] = self._ensure_minimum_solution_steps(
                    self._str(candidate.get("solution_explanation")),
                    minimum_reasoning_steps,
                )
                if not self._question_matches_requested_scope(
                    question=candidate,
                    subject=subject,
                    chapters=scope_chapters,
                    subtopics=scope_subtopics,
                ):
                    continue
                if not self._validate_generated_question(candidate):
                    continue
                try:
                    prepared_emergency = self._prepare_question_for_grading(
                        candidate,
                        fallback_question_id=self._str(candidate.get("question_id"))
                        or f"q_{idx + 1}",
                    )
                    validate_question_structure(prepared_emergency, student_mode=False)
                except (QuestionStructureError, ValueError):
                    continue
                verify_ok, verify_note = self._deterministic_verify_candidate(
                    question=prepared_emergency,
                    subject=subject,
                )
                if not verify_ok:
                    continue
                emergency_score = self._question_hardness_score(
                    question=prepared_emergency,
                    requested_difficulty=difficulty,
                    trap_intensity=trap_intensity,
                    cross_concept=cross_concept,
                    minimum_reasoning_steps=minimum_reasoning_steps,
                )
                dedupe_key = re.sub(
                    r"\s+",
                    " ",
                    self._str(prepared_emergency.get("question_text")).lower(),
                ).strip()
                if dedupe_key and dedupe_key in seen_question_stems:
                    if emergency_attempts < (emergency_max_attempts // 2):
                        continue
                    prepared_emergency["question_text"] = (
                        f"{self._str(prepared_emergency.get('question_text')).strip()} "
                        f"(Variant {idx + 1}-{emergency_attempts})"
                    ).strip()
                    dedupe_key = re.sub(
                        r"\s+",
                        " ",
                        self._str(prepared_emergency.get("question_text")).lower(),
                    ).strip()
                if dedupe_key:
                    seen_question_stems.add(dedupe_key)
                prepared_emergency["verification_note"] = verify_note
                prepared_emergency["source_origin"] = "ai_emergency_fill"
                prepared_emergency["source_stub"] = "deterministic_emergency_backfill"
                prepared_emergency["verification_pass"] = True
                prepared_emergency["critic_score"] = round(max(0.0, emergency_score), 6)
                prepared_emergency["confidence_score"] = round(
                    max(
                        0.58,
                        min(
                            0.9,
                            0.58 + (0.01 * min(20, len(questions))),
                        ),
                    ),
                    6,
                )
                prepared_emergency["difficulty_score"] = round(max(0.0, emergency_score), 6)
                prepared_emergency["provider_used"] = "template_emergency"
                prepared_emergency["fallback_used"] = True
                if scope_chapter:
                    prepared_emergency["chapter_tags"] = [scope_chapter]
                if chapter_first_mode and scope_chapter:
                    normalized_tags: list[str] = [scope_chapter]
                    if scope_subtopic and scope_subtopic != scope_chapter:
                        normalized_tags.append(scope_subtopic)
                    for raw_tag in (prepared_emergency.get("concept_tags") or []):
                        token = self._str(raw_tag)
                        if token and token not in normalized_tags:
                            normalized_tags.append(token)
                    prepared_emergency["concept_tags"] = normalized_tags[:4]
                questions.append(prepared_emergency)

        # Absolute last-resort fill to avoid hard failure on count.
        if len(questions) < question_count and questions:
            replay_cursor = 0
            while len(questions) < question_count:
                base = dict(questions[replay_cursor % len(questions)])
                replay_cursor += 1
                next_idx = len(questions) + 1
                base["question_id"] = f"q_{next_idx}"
                text = self._str(base.get("question_text")).strip()
                if text:
                    base["question_text"] = (
                        f"{text} (Reframed variant {next_idx})"
                    )
                base["source_origin"] = "ai_replay_fill"
                base["source_stub"] = "deterministic_replay_backfill"
                base["provider_used"] = "template_replay"
                base["fallback_used"] = True
                base["verification_note"] = self._str(
                    base.get("verification_note") or "replay_backfill"
                )
                questions.append(base)

        if pyq_focus and strict_web_requirement_unmet and not interactive_student_pyq_mode:
            return {
                "ok": False,
                "status": "PARTIAL_SUCCESS",
                "generation_status": "partial_success",
                "error_reason": "insufficient_ultra_hard_verified_questions",
                "generated_count": len(questions),
                "required_count": question_count,
                "web_result_count": len(web_snippets),
                "web_source_applied_count": web_source_applied_count,
                "web_source_online_applied_count": web_source_online_applied_count,
                "web_source_offline_applied_count": web_source_offline_applied_count,
                "mixed_pyq_source_mode": mixed_pyq_source_mode,
                "dns_blocked_mode": dns_blocked_mode,
                "offline_pyq_only_mode": offline_pyq_only_mode,
                "ai_solution_recovery_count": pyq_ai_solution_recovery_count,
                "web_error_reason": web_error_reason
                or "insufficient_ultra_hard_verified_questions",
                "web_provider_diagnostics": web_provider_diagnostics,
            }

        if len(questions) < question_count:
            if pyq_focus:
                return {
                    "ok": False,
                    "status": "PARTIAL_SUCCESS",
                    "generation_status": "partial_success",
                    "error_reason": "insufficient_ultra_hard_verified_questions",
                    "generated_count": len(questions),
                    "required_count": question_count,
                    "web_result_count": len(web_snippets),
                    "web_source_applied_count": web_source_applied_count,
                    "web_source_online_applied_count": web_source_online_applied_count,
                    "web_source_offline_applied_count": web_source_offline_applied_count,
                    "mixed_pyq_source_mode": mixed_pyq_source_mode,
                    "dns_blocked_mode": dns_blocked_mode,
                    "offline_pyq_only_mode": offline_pyq_only_mode,
                    "ai_solution_recovery_count": pyq_ai_solution_recovery_count,
                    "web_error_reason": web_error_reason
                    or "insufficient_ultra_hard_verified_questions",
                    "web_provider_diagnostics": web_provider_diagnostics,
                }
            return {
                "ok": False,
                "status": "GENERATION_FAILED",
                "message": "Unable to generate stable quiz at requested settings",
            }

        dominant_provider = ""
        if arena_provider_wins:
            dominant_provider = max(
                arena_provider_wins.items(),
                key=lambda kv: (kv[1], kv[0]),
            )[0]
        mean_entropy = (
            (sum(arena_entropy_values) / len(arena_entropy_values))
            if arena_entropy_values
            else 0.0
        )
        mean_disagreement = (
            (sum(arena_disagreement_values) / len(arena_disagreement_values))
            if arena_disagreement_values
            else 0.0
        )
        arena_summary: dict[str, Any] = {
            "enabled": arena_enabled and arena_provider_count > 1,
            "provider_count": arena_provider_count,
            "selection": "hardest" if arena_select_hardest else "first_valid",
            "winner_distribution": arena_provider_wins,
            "mean_entropy": round(mean_entropy, 6),
            "mean_disagreement": round(mean_disagreement, 6),
            "dominant_provider": dominant_provider,
        }

        stored_questions = [dict(q) for q in questions]
        question_rows_for_csv: list[dict[str, Any]] = []
        for q in stored_questions:
            correct_letter = self._str(q.get("_correct_option")).upper()
            options = [self._str(x) for x in (q.get("options") or [])][:4]
            correct_text = ""
            if correct_letter in {"A", "B", "C", "D"}:
                idx = ord(correct_letter) - 65
                if 0 <= idx < len(options):
                    correct_text = options[idx]
            question_rows_for_csv.append(
                {
                    "question": self._str(q.get("question_text")),
                    "type": self._str(q.get("question_type") or "MCQ_SINGLE"),
                    "section": ", ".join(
                        [self._str(x) for x in (q.get("concept_tags") or [])]
                    ),
                    "options": options,
                    "correct": correct_text,
                    "solution_explanation": self._str(
                        q.get("_solution_explanation")
                    ),
                }
            )

        # Store quiz CSV but hide answers prior to submission.
        self._write_quiz_csv(
            quiz_id,
            question_rows_for_csv,
            include_correct=False,
            include_solution=False,
        )
        web_answer_evidence_count = sum(
            1 for row in web_snippets if self._to_bool(row.get("has_answer"))
        )
        web_solution_evidence_count = sum(
            1 for row in web_snippets if self._to_bool(row.get("has_solution"))
        )
        source_policy_mode = "synthesized"
        if pyq_focus and web_source_applied_count > 0:
            if strict_online_only_mode and web_source_online_applied_count > 0:
                source_policy_mode = "pyq_related_web_only"
            elif web_source_online_applied_count > 0 and web_source_offline_applied_count > 0:
                source_policy_mode = "hybrid"
            elif web_source_offline_applied_count > 0 and web_source_online_applied_count <= 0:
                source_policy_mode = "offline_pyq_only"
            elif web_source_online_applied_count > 0 and web_source_offline_applied_count <= 0:
                source_policy_mode = "online_pyq_only"
            else:
                source_policy_mode = "hybrid"
        elif pyq_focus and strict_web_requirement_unmet:
            source_policy_mode = "strict_web_unmet_fallback"
        elif pyq_focus:
            source_policy_mode = "synthesized_pyq_fallback"
        source_policy = {
            "mode": source_policy_mode,
            "scope_verified": (web_source_applied_count > 0) or not pyq_focus,
            "fallback_used": pyq_focus and (web_source_applied_count == 0),
            "web_search_used": bool(online_web_pool),
            "web_result_count": len(web_snippets),
            "online_pool_count": len(online_web_pool),
            "offline_pool_count": len(offline_web_pool),
            "offline_usable_pool_count": len(offline_usable_pool),
            "pyq_selection_pool_count": len(pyq_selection_pool),
            "offline_archive_count": len(offline_pyq_rows),
            "solution_result_count": len(solution_web_snippets),
            "web_source_applied_count": web_source_applied_count,
            "web_source_online_applied_count": web_source_online_applied_count,
            "web_source_offline_applied_count": web_source_offline_applied_count,
            "mixed_pyq_source_mode": mixed_pyq_source_mode,
            "dns_blocked_mode": dns_blocked_mode,
            "offline_pyq_only_mode": offline_pyq_only_mode,
            "offline_pyq_fallback_locked": offline_pyq_fallback_locked,
            "web_answer_evidence_count": web_answer_evidence_count,
            "web_solution_evidence_count": web_solution_evidence_count,
            "ai_solution_recovery_count": pyq_ai_solution_recovery_count,
            "answer_sources_verified": (
                (web_answer_evidence_count > 0 or pyq_ai_solution_recovery_count > 0)
                if pyq_focus and effective_strict_web_mode
                else (
                    web_solution_evidence_count > 0
                    or pyq_ai_solution_recovery_count > 0
                    or not pyq_focus
                )
            ),
            "strict_related_web_mode": strict_related_web_mode,
            "effective_strict_web_mode": effective_strict_web_mode,
            "strict_web_requirement_unmet": strict_web_requirement_unmet,
            "pyq_answer_retrieval_required": pyq_answer_retrieval_required,
            "web_error_reason": web_error_reason,
            "web_provider_diagnostics": web_provider_diagnostics,
        }
        record = {
            "quiz_id": quiz_id,
            "id": quiz_id,
            "user_id": user_id,
            "role": role,
            "self_practice_mode": self_practice_mode,
            "authoring_mode": authoring_mode,
            "title": title,
            "subject": subject,
            "chapters_json": json.dumps(chapters, ensure_ascii=True),
            "deadline": deadline,
            "duration": duration,
            "class_name": class_name,
            "difficulty": difficulty,
            "engine_mode": engine_mode,
            "arena_json": json.dumps(arena_summary, ensure_ascii=True),
            "questions_json": json.dumps(stored_questions, ensure_ascii=True),
            "question_count": len(stored_questions),
            "created_at": self._now_ms(),
            "created_at_iso": now_iso,
            "trap_intensity": trap_intensity,
            "weakness_mode": weakness_mode,
            "cross_concept": cross_concept,
            "pyq_focus": pyq_focus,
            "pyq_mode": pyq_mode,
            "pyq_web_only_mode": pyq_web_only_mode,
            "pyq_answer_retrieval_required": pyq_answer_retrieval_required,
            "strict_related_web_mode": strict_related_web_mode,
            "effective_strict_web_mode": effective_strict_web_mode,
            "strict_web_requirement_unmet": strict_web_requirement_unmet,
            "dns_blocked_mode": dns_blocked_mode,
            "offline_pyq_only_mode": offline_pyq_only_mode,
            "offline_pyq_fallback_locked": offline_pyq_fallback_locked,
            "web_result_count": len(web_snippets),
            "web_online_pool_count": len(online_web_pool),
            "web_offline_pool_count": len(offline_web_pool),
            "web_offline_usable_pool_count": len(offline_usable_pool),
            "solution_web_result_count": len(solution_web_snippets),
            "web_source_applied_count": web_source_applied_count,
            "web_source_online_applied_count": web_source_online_applied_count,
            "web_source_offline_applied_count": web_source_offline_applied_count,
            "web_answer_evidence_count": web_answer_evidence_count,
            "web_solution_evidence_count": web_solution_evidence_count,
            "ai_solution_recovery_count": pyq_ai_solution_recovery_count,
            "web_error_reason": web_error_reason,
            "mixed_pyq_source_mode": mixed_pyq_source_mode,
            "web_provider_diagnostics_json": json.dumps(
                web_provider_diagnostics,
                ensure_ascii=True,
            ),
            "generation_mode": source_policy_mode,
            "source_policy_json": json.dumps(source_policy, ensure_ascii=True),
        }
        quiz_url = f"{self._base_url(payload)}/app/quiz/{quiz_id}.csv"

        async with self._lock:
            self._upsert_by_id(self._ai_quizzes, quiz_id, record)
            self._write_list(self._ai_quizzes_file, self._ai_quizzes)

        client_questions = [
            self._sanitize_ai_question_for_client(
                q, include_answer_key=include_answer_key
            )
            for q in stored_questions
        ]
        ai_fallback_count = sum(
            1
            for q in stored_questions
            if self._to_bool(q.get("fallback_used"))
            or self._str(q.get("source_origin")).startswith("ai_synth")
            or self._str(q.get("source_origin")).startswith("synthesized")
        )
        verification_pass_count = sum(
            1 for q in stored_questions if self._validate_generated_question(q)
        )
        verification_pass_rate = round(
            verification_pass_count / max(1, len(stored_questions)),
            6,
        )
        answer_key_payload = []
        if include_answer_key:
            for q in client_questions:
                row = {
                    "question_id": self._str(q.get("question_id")),
                    "question_type": self._str(q.get("question_type")),
                }
                if self._str(q.get("correct_option")):
                    row["correct_option"] = self._str(q.get("correct_option"))
                if isinstance(q.get("correct_answers"), list) and q.get("correct_answers"):
                    row["correct_answers"] = list(q.get("correct_answers"))
                if self._str(q.get("numerical_answer")):
                    row["numerical_answer"] = self._str(q.get("numerical_answer"))
                answer_key_payload.append(row)
        aqie_mode = "TEACHER" if role == "teacher" and not self_practice_mode else "STUDENT"
        lc_aqie = self._build_lc_aqie_payload(
            title=title,
            mode=aqie_mode,
            duration_minutes=duration,
            shuffle_questions=True,
            difficulty_mix=self._str(payload.get("difficulty_mix") or "Mixed"),
            difficulty=difficulty,
            questions=stored_questions,
            weak_concepts=weak_concepts,
            chapters=chapters,
            subtopics=subtopics,
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "quiz_id": quiz_id,
            "id": quiz_id,
            "url": quiz_url,
            "quiz_url": quiz_url,
            "questions_json": client_questions,
            "questions": client_questions,
            "quiz_metadata": lc_aqie.get("quiz_metadata", {}),
            "aqie_questions": lc_aqie.get("questions", []),
            "ui_spec": lc_aqie.get("ui_spec", {}),
            "student_adaptive_data": lc_aqie.get("student_adaptive_data", {}),
            "lc_aqie": lc_aqie,
            "metadata": {
                "subject": subject,
                "chapters": chapters,
                "difficulty": difficulty,
                "engine_mode": engine_mode,
                "winner_provider": dominant_provider,
                "arena": arena_summary,
                "question_count": len(client_questions),
                "trap_intensity": trap_intensity,
                "weakness_mode": weakness_mode,
                "cross_concept": cross_concept,
                "self_practice_mode": self_practice_mode,
                "authoring_mode": authoring_mode,
                "verification_profile": profile,
                "pyq_focus": pyq_focus,
                "web_search_used": bool(online_web_pool),
                "web_result_count": len(web_snippets),
                "web_online_pool_count": len(online_web_pool),
                "web_offline_pool_count": len(offline_web_pool),
                "solution_web_result_count": len(solution_web_snippets),
                "web_source_applied_count": web_source_applied_count,
                "web_source_online_applied_count": web_source_online_applied_count,
                "web_source_offline_applied_count": web_source_offline_applied_count,
                "web_answer_evidence_count": web_answer_evidence_count,
                "web_solution_evidence_count": web_solution_evidence_count,
                "ai_solution_recovery_count": pyq_ai_solution_recovery_count,
                "mixed_pyq_source_mode": mixed_pyq_source_mode,
                "dns_blocked_mode": dns_blocked_mode,
                "offline_pyq_only_mode": offline_pyq_only_mode,
                "offline_pyq_fallback_locked": offline_pyq_fallback_locked,
                "pyq_mode": pyq_mode,
                "pyq_web_only_mode": pyq_web_only_mode,
                "strict_related_web_mode": strict_related_web_mode,
                "effective_strict_web_mode": effective_strict_web_mode,
                "strict_web_requirement_unmet": strict_web_requirement_unmet,
                "web_offline_usable_pool_count": len(offline_usable_pool),
                "pyq_answer_retrieval_required": pyq_answer_retrieval_required,
                "answer_sources_verified": self._to_bool(
                    source_policy.get("answer_sources_verified")
                ),
                "generation_mode": source_policy_mode,
                "web_count": web_source_applied_count,
                "ai_fallback_count": ai_fallback_count,
                "verification_pass_rate": verification_pass_rate,
                "web_error_reason": web_error_reason,
                "web_provider_diagnostics": web_provider_diagnostics,
            },
            "winner_provider": dominant_provider,
            "arena": arena_summary,
            "source_policy": source_policy,
            "web_provider_diagnostics": web_provider_diagnostics,
            "web_snippets": web_snippets[:6],
            "solution_web_snippets": solution_web_snippets[:6],
            "answer_key": answer_key_payload,
        }

    async def _ai_chat_or_solve(self, payload: dict[str, Any]) -> dict[str, Any]:
        from core.api.entrypoint import lalacore_entry

        prompt = self._str(
            payload.get("prompt")
            or payload.get("question")
            or payload.get("query")
            or payload.get("text")
        )
        image_payload = self._str(
            payload.get("image")
            or payload.get("base64_image")
            or payload.get("image_data")
        )
        pdf_payload: Any = payload.get("pdf") or payload.get("base64_pdf") or payload.get("pdf_data")
        card_payload = payload.get("card")
        if not pdf_payload and isinstance(card_payload, dict):
            pdf_url = self._str(card_payload.get("pdf_url") or card_payload.get("file_url"))
            if pdf_url:
                parsed = urlparse(pdf_url)
                file_id = self._str(parsed.path).rstrip("/").split("/")[-1]
                if file_id:
                    uploaded = await self.get_uploaded_file(file_id)
                    if isinstance(uploaded, dict):
                        path = Path(self._str(uploaded.get("path")))
                        if path.exists() and path.is_file():
                            try:
                                pdf_payload = path.read_bytes()
                            except Exception:
                                pdf_payload = None

        if not prompt and not image_payload and not pdf_payload:
            return {
                "ok": False,
                "status": "MISSING_PROMPT",
                "message": "Missing prompt/image/pdf input",
            }

        raw_options = payload.get("options")
        options = dict(raw_options) if isinstance(raw_options, dict) else {}
        for key in (
            "function",
            "response_style",
            "enable_persona",
            "persona_mode",
            "return_structured",
            "return_markdown",
            "return_latex",
            "count_tokens",
            "app_surface",
        ):
            if key in payload and key not in options:
                options[key] = payload.get(key)

        if "enable_pre_reasoning_context" not in options:
            options["enable_pre_reasoning_context"] = True
        options["enable_web_retrieval"] = False
        if "enable_graph_of_thought" not in options:
            options["enable_graph_of_thought"] = True
        if "enable_mcts_reasoning" not in options:
            options["enable_mcts_reasoning"] = True
        options["require_citations"] = "none"
        options["evidence_mode"] = "none"
        options["min_citation_count"] = 0
        if "min_evidence_score" not in options:
            options["min_evidence_score"] = 0.58

        web_snippets = payload.get("optional_web_snippets")
        if isinstance(web_snippets, list) and web_snippets and "optional_web_snippets" not in options:
            options["optional_web_snippets"] = web_snippets

        chat_function = self._str(
            options.get("function") or payload.get("function")
        ).lower()
        card_grounding = self._build_ai_card_surface_grounding(
            function=chat_function,
            card=card_payload if isinstance(card_payload, dict) else None,
        )
        if prompt and card_grounding:
            prompt = (
                f"{prompt}\n\n"
                "App context you must use directly for this answer:\n"
                f"{card_grounding}\n\n"
                "Ground your response in this app context. Do not say the context is missing if these details are present."
            )

        user_context = {}
        for source_key, target_key in (
            ("user_id", "user_id"),
            ("chat_id", "chat_id"),
            ("session_id", "session_id"),
            ("student_id", "student_id"),
        ):
            value = self._str(payload.get(source_key))
            if value:
                user_context[target_key] = value
        if isinstance(payload.get("card"), dict):
            user_context["card"] = dict(payload.get("card"))
        user_context["student_profile"] = self._atlas_memory.build_student_profile(
            user_context=user_context,
        )

        input_data: Any
        if image_payload and pdf_payload:
            input_type = "mixed"
            input_data = {"text": prompt, "image": image_payload, "pdf": pdf_payload}
        elif image_payload:
            input_type = "mixed"
            input_data = {"text": prompt, "image": image_payload}
        elif pdf_payload is not None:
            input_type = "mixed" if prompt else "pdf"
            input_data = {"text": prompt, "pdf": pdf_payload} if prompt else pdf_payload
        else:
            input_type = "text"
            input_data = prompt

        result = await lalacore_entry(
            input_data=input_data,
            input_type=input_type,
            user_context=user_context,
            options=options,
        )
        if not isinstance(result, dict):
            return {
                "ok": False,
                "status": "FAILED",
                "message": "Engine returned non-dict response",
            }

        if str(result.get("status", "")).lower() == "error":
            return {
                "ok": False,
                "status": result.get("status") or "FAILED",
                "message": self._str(
                    result.get("message")
                    or result.get("error")
                    or "AI solve failed"
                ),
                "provider": self._str(result.get("winner_provider")),
                "model": self._str((result.get("engine") or {}).get("version")),
                "raw": result,
            }

        answer = self._str(
            result.get("final_answer")
            or result.get("answer")
            or result.get("display_answer")
        )
        explanation = self._str(result.get("reasoning") or result.get("explanation"))
        if self._should_prefer_explanation_as_answer(
            function=chat_function,
            answer=answer,
            explanation=explanation,
        ):
            answer = explanation
        if self._should_use_card_surface_fallback(
            function=chat_function,
            answer=answer,
            explanation=explanation,
        ):
            fallback_answer = self._build_card_surface_fallback_answer(
                function=chat_function,
                card=card_payload if isinstance(card_payload, dict) else None,
            )
            if fallback_answer:
                answer = fallback_answer
        winner_provider = self._str(
            result.get("winner_provider")
            or (result.get("provider_diagnostics") or {}).get("winner_provider")
        )
        model_name = self._str((result.get("engine") or {}).get("version"))
        confidence_raw = (
            (result.get("calibration_metrics") or {}).get("confidence_score")
            if isinstance(result.get("calibration_metrics"), dict)
            else result.get("confidence")
        )
        confidence = self._to_float(confidence_raw, -1.0)

        engine_status = self._str(result.get("status") or "ok")
        final_status = self._str(result.get("final_status"))
        quality_gate = result.get("quality_gate")
        quality_reasons: list[str] = []
        if isinstance(quality_gate, dict):
            for row in quality_gate.get("reasons", []) or []:
                token = self._str(row)
                if token:
                    quality_reasons.append(token)

        if not answer and not explanation:
            message = self._str(result.get("message") or result.get("error"))
            if not message:
                if final_status.lower() == "failed" and quality_reasons:
                    message = (
                        "AI engine returned no usable answer "
                        f"({', '.join(quality_reasons)})"
                    )
                elif final_status.lower() == "failed":
                    message = "AI engine returned no usable answer"
                else:
                    message = "AI engine returned empty output"
            status_token = (
                engine_status if engine_status.lower() not in {"", "ok"} else "FAILED_EMPTY_RESULT"
            )
            failure = {
                "ok": False,
                "status": status_token,
                "message": message,
                "provider": winner_provider or "lalacore-local",
                "model": model_name or "lalacore-omega",
                "winner_provider": winner_provider,
                "raw": result,
            }
            if final_status:
                failure["final_status"] = final_status
            if quality_reasons:
                failure["quality_reasons"] = quality_reasons
            return failure

        out = {
            "ok": True,
            "status": engine_status or "ok",
            "answer": answer or explanation,
            "provider": winner_provider or "lalacore-local",
            "model": model_name or "lalacore-omega",
            "winner_provider": winner_provider,
            "raw": result,
        }
        if explanation and explanation != answer:
            out["explanation"] = explanation
        if 0.0 < confidence <= 1.0:
            out["confidence"] = round(confidence, 6)
        if isinstance(result.get("visualization"), dict):
            out["visualization"] = result.get("visualization")
        if isinstance(result.get("web_retrieval"), dict):
            out["web_retrieval"] = result.get("web_retrieval")
        if isinstance(result.get("mcts_search"), dict):
            out["mcts_search"] = result.get("mcts_search")
        if isinstance(result.get("reasoning_graph"), dict):
            out["reasoning_graph"] = result.get("reasoning_graph")
        if isinstance(result.get("input_analysis"), dict):
            out["input_analysis"] = result.get("input_analysis")
        if isinstance(result.get("citations"), list):
            out["citations"] = result.get("citations")
        if isinstance(result.get("sources_consulted"), list):
            out["sources_consulted"] = result.get("sources_consulted")
        concept = self._str((result.get("profile") or {}).get("subject"))
        if concept:
            out["concept"] = concept
        return out

    def _build_ai_card_surface_grounding(
        self,
        *,
        function: str,
        card: dict[str, Any] | None,
    ) -> str:
        if not isinstance(card, dict):
            return ""
        lines: list[str] = []
        if function == "analytics_review":
            weak_topics = ", ".join(
                self._str(topic)
                for topic in (card.get("weak_topics") or [])
                if self._str(topic)
            )
            if weak_topics:
                lines.append(f"Weak topics: {weak_topics}")
            score = self._str(card.get("score"))
            if score:
                lines.append(f"Score: {score}")
            percentile = self._str(card.get("percentile"))
            if percentile:
                lines.append(f"Percentile: {percentile}")
            rank = self._str(card.get("rank"))
            if rank:
                lines.append(f"Rank: {rank}")
        elif function == "study_material_chat":
            title = self._str(card.get("title"))
            if title:
                lines.append(f"Material title: {title}")
            subject = self._str(card.get("subject"))
            chapter = self._str(card.get("chapter"))
            if subject or chapter:
                lines.append(
                    "Scope: "
                    + " / ".join(part for part in [subject, chapter] if part)
                )
            notes = self._str(card.get("material_notes") or card.get("notes"))
            if notes:
                lines.append(f"Material notes: {notes}")
        elif function == "teacher_dashboard_review":
            recommended_focus = self._str(card.get("recommended_focus"))
            if recommended_focus:
                lines.append(f"Recommended focus: {recommended_focus}")
            attention_students = card.get("attention_students")
            if isinstance(attention_students, list) and attention_students:
                formatted: list[str] = []
                for row in attention_students[:5]:
                    if not isinstance(row, dict):
                        continue
                    name = self._str(row.get("name"))
                    issue = self._str(row.get("issue"))
                    if name and issue:
                        formatted.append(f"{name} ({issue})")
                    elif name:
                        formatted.append(name)
                if formatted:
                    lines.append("Attention students: " + "; ".join(formatted))
        return "\n".join(lines).strip()

    def _should_prefer_explanation_as_answer(
        self,
        *,
        function: str,
        answer: str,
        explanation: str,
    ) -> bool:
        if function not in {
            "study_material_chat",
            "analytics_review",
            "teacher_dashboard_review",
        }:
            return False
        explanation_text = self._str(explanation)
        if len(explanation_text) < 48:
            return False
        answer_text = self._str(answer)
        if not answer_text:
            return True
        if len(answer_text) < 24:
            return True
        if len(answer_text.split()) <= 4:
            return True
        return False

    def _should_use_card_surface_fallback(
        self,
        *,
        function: str,
        answer: str,
        explanation: str,
    ) -> bool:
        if function not in {
            "study_material_chat",
            "analytics_review",
            "teacher_dashboard_review",
        }:
            return False
        answer_text = self._str(answer).lower()
        explanation_text = self._str(explanation).lower()
        weak_markers = (
            "[unresolved]",
            "context is missing",
            "does not provide enough context",
            "cannot create a definitive answer",
            "identify unknown quantity and governing relations.",
        )
        if not answer_text:
            return True
        return any(marker in answer_text or marker in explanation_text for marker in weak_markers)

    def _build_card_surface_fallback_answer(
        self,
        *,
        function: str,
        card: dict[str, Any] | None,
    ) -> str:
        if not isinstance(card, dict):
            return ""
        if function == "analytics_review":
            weak_topics = [
                self._str(topic)
                for topic in (card.get("weak_topics") or [])
                if self._str(topic)
            ]
            primary_topic = weak_topics[0] if weak_topics else "the weakest topic"
            return (
                f"Biggest weakness: {primary_topic}. "
                f"Next step: attempt a focused practice quiz on {primary_topic} and review the first two mistakes before moving on."
            )
        if function == "study_material_chat":
            notes = self._str(card.get("material_notes") or card.get("notes"))
            chapter = self._str(card.get("chapter") or card.get("title") or "this material")
            if notes:
                return (
                    f"Study summary: {notes}. "
                    "Main trap: mixing units, signs, or definitions while switching between closely related formulas."
                )
            return (
                f"Study summary: focus on the key ideas from {chapter}. "
                "Main trap: mixing units, signs, or formula conditions."
            )
        if function == "teacher_dashboard_review":
            attention_students = card.get("attention_students")
            top_student = ""
            top_issue = ""
            if isinstance(attention_students, list) and attention_students:
                row = attention_students[0]
                if isinstance(row, dict):
                    top_student = self._str(row.get("name"))
                    top_issue = self._str(row.get("issue"))
            recommended_focus = self._str(card.get("recommended_focus") or "the weakest topic")
            student_line = top_student or "The flagged student"
            if top_issue:
                student_line = f"{student_line} ({top_issue})"
            return (
                f"- Student needing attention: {student_line}.\n"
                f"- Next quiz to create: a focused {recommended_focus} quiz targeting the current weakness."
            )
        return ""

    def _build_lc_aqie_payload(
        self,
        *,
        title: str,
        mode: str,
        duration_minutes: int,
        shuffle_questions: bool,
        difficulty_mix: str,
        difficulty: int,
        questions: list[dict[str, Any]],
        weak_concepts: list[str],
        chapters: list[str],
        subtopics: list[str],
    ) -> dict[str, Any]:
        per_question_mark = 4
        total_marks = max(0, len(questions) * per_question_mark)
        out_questions: list[dict[str, Any]] = []
        for idx, q in enumerate(questions, start=1):
            q_type = self._str(q.get("question_type") or "MCQ_SINGLE").upper()
            opts = [self._str(x) for x in (q.get("options") or [])]
            options_map = {
                "A": opts[0] if len(opts) > 0 else "",
                "B": opts[1] if len(opts) > 1 else "",
                "C": opts[2] if len(opts) > 2 else "",
                "D": opts[3] if len(opts) > 3 else "",
            }

            if q_type == "NUMERICAL":
                normalized_type = "Integer"
                options_map = {}
                correct_answer = self._str(q.get("_numerical_answer"))
            elif q_type == "MCQ_MULTI":
                normalized_type = "MCQ"
                answers = [self._str(x).upper() for x in self._to_list_str(q.get("_correct_answers")) if self._str(x)]
                correct_answer = ",".join(answers)
            else:
                normalized_type = "MCQ"
                correct_answer = self._str(q.get("_correct_option")).upper()

            statement = self._str(q.get("question_text"))
            solution_text = self._str(q.get("_solution_explanation") or q.get("solution_explanation"))
            formal_steps = []
            for step_i, chunk in enumerate([c.strip() for c in re.split(r"[.;]\s+", solution_text) if c.strip()], start=1):
                formal_steps.append(
                    {
                        "step": f"Step {step_i}",
                        "equation": "",
                        "justification": chunk,
                    }
                )
            if not formal_steps:
                formal_steps = [{"step": "Step 1", "equation": "", "justification": "Follow the core definition and compute carefully."}]

            diff_label = "Basic"
            if difficulty >= 4:
                diff_label = "Advanced"
            elif difficulty >= 2:
                diff_label = "Main"

            concept_tags = [self._str(x) for x in (q.get("concept_tags") or []) if self._str(x)]
            adaptive_score = 0.55
            if any(tag in set(weak_concepts) for tag in concept_tags):
                adaptive_score = 0.85

            out_questions.append(
                {
                    "id": idx,
                    "type": normalized_type,
                    "statement": statement,
                    "latex_statement": sanitize_latex(statement),
                    "source_origin": self._str(q.get("source_origin")),
                    "source_stub": self._str(q.get("source_stub")),
                    "source_url": self._str(q.get("source_url")),
                    "source_quality_score": self._to_float(
                        q.get("source_quality_score"), 0.0
                    ),
                    "options": options_map,
                    "correct_answer": correct_answer,
                    "solution": {
                        "core_idea": formal_steps[0]["justification"],
                        "formal_derivation": formal_steps,
                        "intuition": "Identify the governing relation first, then execute algebra/arithmetic in stable order.",
                        "shortcut": "Use elimination or symmetry where possible before full expansion.",
                        "diagrams": {
                            "mermaid": "",
                            "matplotlib_code": "",
                            "ascii": "",
                        },
                    },
                    "verification": {
                        "numeric_pass": (
                            True
                            if q.get("verification_pass") is None
                            else self._to_bool(q.get("verification_pass"))
                        ),
                        "symbolic_pass": (
                            True
                            if q.get("verification_pass") is None
                            else self._to_bool(q.get("verification_pass"))
                        ),
                        "dimension_pass": (
                            True
                            if q.get("verification_pass") is None
                            else self._to_bool(q.get("verification_pass"))
                        ),
                    },
                    "confidence_score": self._to_float(q.get("confidence_score"), 0.78),
                    "critic_score": self._to_float(q.get("critic_score"), 0.0),
                    "provider_used": self._str(q.get("provider_used")),
                    "fallback_used": self._to_bool(q.get("fallback_used")),
                    "difficulty_score": self._to_float(q.get("difficulty_score"), 0.0),
                    "difficulty": diff_label,
                    "adaptive_score": adaptive_score if mode == "STUDENT" else 0.0,
                }
            )

        ui_spec = {
            "design_language": "Minimal, glassmorphic, Apple-inspired",
            "layout": {
                "card_corner_radius": 24,
                "spacing_system": "8pt grid",
                "typography": "SF Pro style hierarchy",
                "shadows": "soft, layered",
            },
            "question_card": {
                "front": {
                    "shows_statement": True,
                    "shows_options": True,
                    "difficulty_dot": True,
                    "confidence_badge": True,
                },
                "back": {
                    "expandable_solution_tabs": ["Intuition", "Formal", "Shortcut", "Diagram"],
                },
            },
            "student_exam_navigation": {
                "timer": True,
                "progress_bar": True,
                "question_palette": True,
            },
            "animation_spec": {
                "card_flip": "spring 250ms",
                "animated_reveal": "staggered fade 120ms",
                "feedback": "subtle haptic pulse",
            },
        }

        recommendations = weak_concepts[:] if weak_concepts else subtopics[:3] if subtopics else chapters[:3]
        student_adaptive_data = {
            "mastery_status": {topic: "developing" for topic in recommendations},
            "recommendations": recommendations,
            "performance_trends": {
                "accuracy": 0.0,
                "time_per_question_s": 0.0,
                "mastery_curve": [],
            },
        }

        return {
            "quiz_metadata": {
                "title": title,
                "mode": mode,
                "duration_minutes": int(max(1, duration_minutes)),
                "total_marks": total_marks,
                "shuffle_questions": bool(shuffle_questions),
                "difficulty_mix": difficulty_mix or "Mixed",
            },
            "questions": out_questions,
            "ui_spec": ui_spec,
            "student_adaptive_data": student_adaptive_data if mode == "STUDENT" else {
                "mastery_status": {},
                "recommendations": [],
                "performance_trends": {},
            },
        }

    def _find_ai_quiz(self, quiz_id: str) -> dict[str, Any] | None:
        key = self._safe_id(quiz_id)
        if not key:
            return None
        for row in self._ai_quizzes:
            if self._safe_id(row.get("quiz_id") or row.get("id")) == key:
                return row
        return None

    def _find_assessment_quiz(self, quiz_id: str) -> dict[str, Any] | None:
        key = self._safe_id(quiz_id)
        if not key:
            return None
        for row in self._assessments:
            if self._safe_id(row.get("id") or row.get("quiz_id")) == key:
                return row
        return None

    async def _get_ai_quiz(self, payload: dict[str, Any]) -> dict[str, Any]:
        quiz_id = self._safe_id(payload.get("quiz_id") or payload.get("id"))
        if not quiz_id:
            return {
                "ok": False,
                "status": "MISSING_QUIZ_ID",
                "message": "quiz_id is required",
            }
        row = self._find_ai_quiz(quiz_id)
        if row is None:
            return {
                "ok": False,
                "status": "NOT_FOUND",
                "message": "AI quiz not found",
            }
        raw = self._str(row.get("questions_json"))
        questions = []
        if raw:
            try:
                decoded = json.loads(raw)
                if isinstance(decoded, list):
                    questions = [dict(x) for x in decoded if isinstance(x, dict)]
            except Exception:
                questions = []
        return {
            "ok": True,
            "status": "SUCCESS",
            "quiz_id": quiz_id,
            "questions_json": [
                self._sanitize_ai_question_for_client(q) for q in questions
            ],
            "metadata": {
                "subject": self._str(row.get("subject")),
                "difficulty": self._to_int(row.get("difficulty"), 3),
                "engine_mode": self._str(row.get("engine_mode")),
            },
        }

    def _parse_answer_map(self, payload: dict[str, Any]) -> dict[str, list[str]]:
        raw = payload.get("answers") or payload.get("user_answers")
        if isinstance(raw, str) and raw.strip():
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None
        out: dict[str, list[str]] = {}
        if not isinstance(raw, dict):
            return out
        for k, v in raw.items():
            key = self._str(k)
            if not key:
                continue
            if isinstance(v, list):
                items = [self._str(x) for x in v if self._str(x)]
            else:
                text = self._str(v)
                items = [x.strip() for x in text.split(",") if x.strip()] if text else []
            out[key] = items
        return out

    def _normalize_choice(self, raw_choice: str, options: list[str]) -> str:
        value = self._str(raw_choice)
        if not value:
            return ""
        compact = value.upper()
        if compact in {"A", "B", "C", "D"}:
            return compact
        if len(compact) >= 2 and compact[0] in {"A", "B", "C", "D"} and compact[1] in {")", ".", ":"}:
            return compact[0]
        if compact in {"1", "2", "3", "4"}:
            return "ABCD"[int(compact) - 1]
        for i, option in enumerate(options[:4]):
            if self._str(option).lower() == value.lower():
                return "ABCD"[i]
        return ""

    async def _evaluate_quiz_submission(self, payload: dict[str, Any]) -> dict[str, Any]:
        quiz_id = self._safe_id(payload.get("quiz_id") or payload.get("id"))
        if not quiz_id:
            return {
                "ok": False,
                "status": "MISSING_QUIZ_ID",
                "message": "quiz_id is required",
            }
        row = self._find_ai_quiz(quiz_id)
        assessment_row = None
        if row is None:
            assessment_row = self._find_assessment_quiz(quiz_id)
            row = assessment_row
        if row is None:
            return {
                "ok": False,
                "status": "NOT_FOUND",
                "message": "Quiz not found",
            }
        try:
            questions_raw = json.loads(self._str(row.get("questions_json")) or "[]")
        except Exception:
            questions_raw = []
        if not isinstance(questions_raw, list) or not questions_raw:
            return {
                "ok": False,
                "status": "INVALID_QUIZ_DATA",
                "message": "Stored quiz has no questions",
            }
        questions = [dict(x) for x in questions_raw if isinstance(x, dict)]
        answer_map = self._parse_answer_map(payload)

        score = 0.0
        correct = 0
        wrong = 0
        skipped = 0
        total = 0.0
        section_total: dict[str, int] = {}
        section_correct: dict[str, int] = {}
        answer_key: list[dict[str, Any]] = []
        uploaded_correct_only: list[dict[str, Any]] = []
        preview_only = self._to_bool(payload.get("preview_only"))
        role = self._str(
            payload.get("role")
            or payload.get("user_role")
            or payload.get("request_role")
        ).lower()
        include_answer_key = self._to_bool(
            payload.get("include_answer_key") or payload.get("require_answer_key")
        )
        per_question_results: list[dict[str, Any]] = []

        for i, question in enumerate(questions):
            try:
                prepared = self._prepare_question_for_grading(
                    question,
                    fallback_question_id=self._str(question.get("question_id"))
                    or f"q_{i+1}",
                    derive_from_visible=assessment_row is not None,
                )
            except (QuestionStructureError, GradingValidationError, ValueError) as exc:
                return {
                    "ok": False,
                    "status": "INVALID_QUIZ_DATA",
                    "message": f"Question {i + 1} is invalid for grading: {exc}",
                }
            options = [self._str(x) for x in (prepared.get("options") or [])]
            q_type = self._str(prepared.get("question_type"))
            concept_tags = [
                self._str(x)
                for x in (prepared.get("concept_tags") or [])
                if self._str(x)
            ]
            section = (
                self._str(question.get("section"))
                or self._str(question.get("chapter"))
                or (concept_tags[0] if concept_tags else "")
                or self._str(row.get("subject"))
                or "General"
            )
            section_total[section] = (section_total.get(section) or 0) + 1

            key_by_idx = str(i)
            key_by_id = self._str(prepared.get("question_id"))
            user_values = answer_map.get(key_by_idx, [])
            if not user_values and key_by_id:
                user_values = answer_map.get(key_by_id, [])
            student_payload: dict[str, Any]
            if q_type == "NUMERICAL":
                student_payload = {
                    "question_id": key_by_id,
                    "answer": user_values[0] if user_values else "",
                }
                user_rendered = self._str(user_values[0]) if user_values else "Skipped"
            else:
                student_payload = {"question_id": key_by_id, "answers": user_values}
                user_rendered = ", ".join(user_values) if user_values else "Skipped"

            try:
                grading = evaluate_attempt(prepared, student_payload)
            except GradingError as exc:
                return {
                    "ok": False,
                    "status": "EVALUATION_ERROR",
                    "message": f"Failed to grade question {i + 1}: {exc}",
                }

            answered = bool(grading.get("grading_metadata", {}).get("answered"))
            is_correct = bool(grading.get("is_correct"))
            score_awarded = self._to_float(grading.get("score_awarded"), 0.0)
            max_score = self._to_float(grading.get("max_score"), 4.0)

            total += max_score
            score += score_awarded
            if not answered:
                skipped += 1
            elif is_correct:
                correct += 1
                section_correct[section] = (section_correct.get(section) or 0) + 1
            else:
                wrong += 1

            per_question_results.append(
                {
                    "question_id": key_by_id,
                    "question_index": i,
                    "question_type": q_type,
                    "student_answer": user_rendered,
                    "is_correct": is_correct,
                    "score_awarded": round(score_awarded, 4),
                    "max_score": round(max_score, 4),
                    "grading_metadata": grading.get("grading_metadata", {}),
                }
            )

            if include_answer_key:
                answer_item: dict[str, Any] = {
                    "question_id": key_by_id,
                    "question_index": i,
                    "question_text": self._str(prepared.get("question_text")),
                    "question_type": q_type,
                    "question_image": self._str(
                        question.get("image")
                        or question.get("imageUrl")
                        or question.get("image_url")
                    ),
                    "options": options,
                    "student_answer": user_rendered,
                    "is_correct": is_correct,
                    "solution_explanation": self._str(
                        prepared.get("_solution_explanation")
                    ),
                    "section": section,
                    "concept_tags": concept_tags,
                    "difficulty_estimate": self._to_int(
                        prepared.get("difficulty_estimate"), 3
                    ),
                    "marks_correct": round(
                        self._to_float(prepared.get("marks_correct"), 4.0), 4
                    ),
                    "marks_incorrect": round(
                        self._to_float(prepared.get("marks_incorrect"), -1.0), 4
                    ),
                    "marks_unattempted": round(
                        self._to_float(prepared.get("marks_unattempted"), 0.0), 4
                    ),
                }
                if q_type == "MCQ_SINGLE":
                    correct_letter = self._str(prepared.get("_correct_option")).upper()
                    correct_idx = ord(correct_letter) - 65
                    correct_text = (
                        options[correct_idx]
                        if 0 <= correct_idx < len(options)
                        else ""
                    )
                    answer_item["correct_option"] = correct_letter
                    answer_item["correct_answer"] = f"{correct_letter}) {correct_text}"
                elif q_type == "MCQ_MULTI":
                    answer_item["correct_answers"] = [
                        self._str(x).upper()
                        for x in self._to_list_str(prepared.get("_correct_answers"))
                        if self._str(x)
                    ]
                else:
                    answer_item["correct_numerical"] = self._str(
                        prepared.get("_numerical_answer")
                    )
                answer_key.append(answer_item)
                if is_correct:
                    uploaded_correct_only.append(
                        {
                            "question_id": key_by_id,
                            "student_answer": user_rendered,
                        }
                    )

        section_accuracy: dict[str, float] = {}
        for sec, sec_total in section_total.items():
            sec_correct = section_correct.get(sec, 0)
            section_accuracy[sec] = round((sec_correct / max(1, sec_total)) * 100.0, 2)

        student_name = self._str(
            payload.get("name")
            or payload.get("student_name")
            or payload.get("student")
            or payload.get("user_id")
        )
        student_id = self._str(
            payload.get("student_id")
            or payload.get("user_id")
            or payload.get("account_id")
        )
        result_row = {
            "id": self._new_id("res"),
            "quiz_id": quiz_id,
            "topic": self._str(row.get("title") or row.get("subject") or "AI Quiz"),
            "name": student_name,
            "student_name": student_name,
            "student_id": student_id,
            "account_id": student_id,
            "score": round(score, 2),
            "total": round(total, 2),
            "correct": correct,
            "wrong": wrong,
            "skipped": skipped,
            "section_accuracy": section_accuracy,
            "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "ts": self._now_ms(),
            "engine_mode": self._str(row.get("engine_mode")),
            "type": self._str(row.get("type") or ("AIExam" if assessment_row is None else "Exam")),
            "correct_upload_count": len(uploaded_correct_only),
        }
        if not preview_only:
            async with self._lock:
                self._results.append(result_row)
                self._write_list(self._results_file, self._results)

        return {
            "ok": True,
            "status": "SUCCESS",
            "quiz_id": quiz_id,
            "evaluation_result": {
                "score": round(score, 2),
                "max_score": round(total, 2),
                "correct": correct,
                "wrong": wrong,
                "skipped": skipped,
                "section_accuracy": section_accuracy,
            },
            "score": round(score, 2),
            "max_score": round(total, 2),
            "total": round(total, 2),
            "correct": correct,
            "wrong": wrong,
            "skipped": skipped,
            "per_question_result": per_question_results,
            "answer_key": answer_key if include_answer_key else [],
            "uploaded_correct_only": uploaded_correct_only if include_answer_key else [],
            "preview_only": preview_only,
        }

    async def _save_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        result_id = self._safe_id(payload.get("id")) or self._new_id("res")
        row = self._normalized_result_row(payload, result_id=result_id)
        async with self._lock:
            self._upsert_by_id(self._results, result_id, row)
            self._write_list(self._results_file, self._results)
        return {"ok": True, "status": "SUCCESS", "result": row}

    async def _get_results(self) -> dict[str, Any]:
        rows = sorted(
            [
                self._normalized_result_row(
                    row,
                    result_id=self._safe_id(row.get("id")) or self._new_id("res"),
                )
                for row in self._results
                if isinstance(row, dict)
            ],
            key=lambda x: int(x.get("ts", 0) or 0),
            reverse=True,
        )
        return {"ok": True, "status": "SUCCESS", "list": rows}

    async def _add_teacher_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        item = {
            "id": self._new_id("trev"),
            "quiz_id": self._str(payload.get("quiz_id")),
            "quiz_title": self._str(payload.get("quiz_title")),
            "question_id": self._str(
                payload.get("question_id") or payload.get("question_index")
            ),
            "question_text": self._str(payload.get("question_text")),
            "question_image": self._str(payload.get("question_image") or payload.get("image")),
            "student_answer": self._str(payload.get("student_answer")),
            "correct_answer": self._str(payload.get("correct_answer")),
            "student_id": self._str(payload.get("student_id") or payload.get("user_id")),
            "student_name": self._str(payload.get("student_name") or payload.get("student")),
            "message": self._str(payload.get("message") or payload.get("doubt")),
            "subject": self._str(payload.get("subject")),
            "chapter": self._str(payload.get("chapter")),
            "source_surface": self._str(payload.get("source_surface")),
            "answer_key_card": dict(payload.get("answer_key_card"))
            if isinstance(payload.get("answer_key_card"), dict)
            else None,
            "timestamp": self._to_int(payload.get("timestamp"), self._now_ms()),
        }
        if not item["quiz_id"] or not item["question_id"] or not item["student_id"]:
            return {
                "ok": False,
                "status": "MISSING_FIELDS",
                "message": "quiz_id, question_id, student_id required",
            }
        async with self._lock:
            self._teacher_review_queue.append(item)
            self._write_list(self._teacher_review_file, self._teacher_review_queue)
        return {"ok": True, "status": "SUCCESS", "queue_item": item}

    async def _get_teacher_review_queue(self) -> dict[str, Any]:
        rows = sorted(
            self._teacher_review_queue,
            key=lambda x: int(x.get("timestamp", 0) or 0),
            reverse=True,
        )
        return {"ok": True, "status": "SUCCESS", "list": rows}

    async def _list_assessments(self) -> dict[str, Any]:
        out = sorted(
            self._assessments,
            key=lambda x: int(x.get("created_at", 0) or 0),
            reverse=True,
        )
        return {"ok": True, "status": "SUCCESS", "list": out}

    async def _get_master_csv(self) -> dict[str, Any]:
        rows: list[list[str]] = [["ID", "Title", "URL", "Deadline", "Type", "Duration"]]
        assessments = sorted(
            self._assessments,
            key=lambda x: int(x.get("created_at", 0) or 0),
            reverse=False,
        )
        for item in assessments:
            rows.append(
                [
                    self._str(item.get("id")),
                    self._str(item.get("title")),
                    self._str(item.get("url")),
                    self._str(item.get("deadline")),
                    self._str(item.get("type") or "Exam"),
                    self._str(item.get("duration") or 30),
                ]
            )
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerows(rows)
        return {"ok": True, "status": "SUCCESS", "csv": buffer.getvalue(), "list": assessments}

    async def _add_material(self, payload: dict[str, Any]) -> dict[str, Any]:
        role = self._str(
            payload.get("role") or payload.get("user_role") or payload.get("request_role")
        ).lower()
        if role and role != "teacher":
            return {
                "ok": False,
                "status": "FORBIDDEN",
                "message": "Only teachers can add study materials",
            }
        title = self._str(payload.get("title") or payload.get("material_title") or payload.get("name"))
        material_type = self._str(payload.get("type") or "pdf") or "pdf"
        url = self._str(payload.get("url") or payload.get("file_url") or payload.get("link"))
        notes = self._str(payload.get("notes"))
        if not title:
            return {"ok": False, "status": "MISSING_TITLE", "message": "Missing material title"}
        if material_type.lower() != "note" and not url:
            return {"ok": False, "status": "MISSING_URL", "message": "Missing material URL"}
        if material_type.lower() == "note" and not notes:
            return {"ok": False, "status": "MISSING_NOTES", "message": "Missing notes content"}

        material_id = self._safe_id(payload.get("material_id")) or self._new_id("mat")
        now_ms = self._now_ms()
        item = {
            "material_id": material_id,
            "title": title,
            "type": material_type,
            "url": url if url else "inline://note",
            "notes": notes,
            "description": self._str(payload.get("description")),
            "subject": self._str(payload.get("subject") or "General"),
            "chapters": self._str(payload.get("chapters") or payload.get("chapter")),
            "class": self._str(payload.get("class") or payload.get("class_name")),
            "artifact_type": self._str(payload.get("artifact_type")),
            "live_class_id": self._str(payload.get("live_class_id") or payload.get("class_id")),
            "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else None,
            "created_at": now_ms,
            "updated_at": now_ms,
        }

        async with self._lock:
            for i, existing in enumerate(self._materials):
                if self._str(existing.get("material_id")) == material_id:
                    self._materials[i] = item
                    break
            else:
                self._materials.append(item)
            self._write_list(self._materials_file, self._materials)

        return {
            "ok": True,
            "status": "SUCCESS",
            "message": "Material added",
            "material_id": material_id,
            "title": title,
            "url": item["url"],
            "material": item,
        }

    async def _get_materials(self) -> dict[str, Any]:
        out = sorted(
            self._materials,
            key=lambda x: int(x.get("created_at", 0) or 0),
            reverse=True,
        )
        return {"ok": True, "status": "SUCCESS", "list": out}

    def _find_material_item(self, material_id: str) -> dict[str, Any] | None:
        key = self._safe_id(material_id)
        if not key:
            return None
        for row in self._materials:
            if self._safe_id(row.get("material_id")) == key:
                return dict(row)
        return None

    def _material_ai_cache_key(
        self,
        *,
        material_id: str,
        mode: str,
        prompt: str,
        options: dict[str, Any],
    ) -> str:
        fingerprint = {
            "material_id": material_id,
            "mode": mode,
            "prompt": prompt,
            "options": options,
        }
        digest = hashlib.sha1(
            json.dumps(fingerprint, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        return f"{self._safe_id(material_id) or 'material'}:{mode}:{digest}"

    def _build_material_ai_card(
        self,
        *,
        payload: dict[str, Any],
        material: dict[str, Any] | None,
        mode: str,
        title: str,
    ) -> dict[str, Any]:
        card = dict(payload.get("card")) if isinstance(payload.get("card"), dict) else {}
        material = dict(material or {})
        notes = self._str(
            card.get("material_notes") or material.get("notes") or payload.get("source_notes")
        )
        source_type = self._str(
            card.get("material_type")
            or card.get("source_type")
            or material.get("type")
            or payload.get("source_type")
        )
        source_url = self._str(
            card.get("material_url")
            or card.get("source_url")
            or material.get("url")
            or payload.get("source_url")
        )
        subject = self._str(
            card.get("subject") or material.get("subject") or payload.get("subject")
        )
        chapter = self._str(
            card.get("chapter")
            or material.get("chapters")
            or payload.get("chapter")
            or payload.get("chapters")
        )
        ocr_required = self._to_bool(
            card.get("ocr_required")
            or payload.get("ocr_required")
            or (source_type.lower() in {"image", "pdf"})
            or source_url.lower().endswith(".pdf")
        )
        merged = {
            "material_id": self._safe_id(
                payload.get("material_id") or material.get("material_id")
            ),
            "mode": mode,
            "title": title,
            "subject": subject,
            "chapter": chapter,
            "material_type": source_type,
            "material_url": source_url,
            "class_name": self._str(
                card.get("class_name") or material.get("class") or payload.get("class_name")
            ),
            "artifact_type": self._str(
                card.get("artifact_type")
                or material.get("artifact_type")
                or payload.get("artifact_type")
            ),
            "enable_material_web_fusion": True,
            "enable_material_ocr": True,
            "prefer_visualization_when_math_detected": True,
            "prefer_source_groups": True,
            "material_context_depth": "deep",
            "study_mode": f"atlas_material_{re.sub(r'[^a-z0-9]+', '_', mode.lower()).strip('_') or 'qa'}",
        }
        if notes:
            merged["material_notes"] = notes[:6000]
        if ocr_required:
            merged["ocr_required"] = True
        for key, value in card.items():
            if key not in merged and value not in (None, "", []):
                merged[key] = value
        return merged

    def _build_material_ai_options(
        self,
        *,
        mode: str,
        raw_options: dict[str, Any],
        is_query: bool,
    ) -> dict[str, Any]:
        lowered = self._str(mode).lower()
        summary_mode = "summary" in lowered or "summarize" in lowered
        notes_mode = "note" in lowered
        formula_mode = "formula" in lowered or "sheet" in lowered
        flashcard_mode = "flashcard" in lowered
        revision_plan_mode = "revision_plan" in lowered or "study_plan" in lowered
        quiz_mode = "quiz" in lowered or "drill" in lowered
        merged = {
            "function": "material_query" if is_query else "material_generate",
            "response_style": "material_grounded",
            "enable_pre_reasoning_context": False,
            "enable_web_retrieval": False,
            "enable_graph_of_thought": False,
            "enable_mcts_reasoning": False,
            "enable_verification_reevaluation": False,
            "enable_meta_verification": False,
            "enable_citation_map": True,
            "require_citations": "auto",
            "evidence_mode": "auto",
            "min_citation_count": 2,
            "min_evidence_score": 0.60 if is_query else 0.62,
            "web_search_scope": "study_material",
            "web_search_timeout_s": 6.0 if is_query else 6.8,
            "web_fetch_timeout_s": 2.8 if is_query else 3.2,
            "web_similarity_threshold": 0.57 if is_query else 0.55,
            "search_max_matches": 12 if is_query else 14,
            "return_structured": True,
            "return_markdown": True,
            "return_latex": True,
            "count_tokens": True,
            "app_surface": "study_material",
            "ocr_mode": "strict",
            "strict_ocr": True,
            "verify_equations": True,
            "jee_quality_pass": True,
            "enable_material_web_fusion": True,
            "enable_material_ocr": True,
            "prefer_visualization_when_math_detected": True,
            "prefer_source_groups": True,
            "prefer_stepwise_reasoning": True,
            "prefer_revision_structure": summary_mode or revision_plan_mode,
            "prefer_deep_note_structure": notes_mode,
            "prefer_formula_sheet": formula_mode,
            "prefer_flashcards": flashcard_mode,
            "prefer_quiz_drill": quiz_mode,
            "prefer_material_grounding": True,
            "study_mode": (
                "atlas_material_qa"
                if is_query
                else "atlas_material_summary"
                if summary_mode
                else "atlas_material_notes"
            ),
            "material_context_depth": "deep",
        }
        merged.update(raw_options)
        return merged

    def _build_material_ai_prompt(
        self,
        *,
        mode: str,
        title: str,
        card: dict[str, Any],
        question: str = "",
        is_query: bool,
    ) -> str:
        notes = self._str(card.get("material_notes"))
        if is_query:
            lines = [
                "Task: answer the student's question using the selected study material as primary context.",
                "The answer may be conceptual; it does not need to be numeric.",
                "Use the selected study material as the primary source of truth. Use validated web retrieval only as supporting evidence when it materially improves the answer.",
                "Do not invent websites, books, citations, or source links that are not already provided by the material or engine citations.",
                "Put the full final response directly in the answer body. Do not return only a label, title, or meta-commentary.",
                f"Study material title: {title}",
            ]
        else:
            lines = [
                f"Task: produce a material-grounded JEE study output for '{title}'.",
                "The requested deliverable itself is the answer. Do not say the question is missing and do not return [UNRESOLVED].",
                "Use the selected study material as the primary source of truth. Use validated web retrieval only as supporting evidence when it materially improves the answer.",
                "Do not invent websites, books, citations, or source links that are not already provided by the material or engine citations.",
                "Put the complete markdown deliverable directly in the final answer. Do not return only a heading, title, or summary label.",
                f"Study material title: {title}",
            ]
        if self._str(card.get("subject")):
            lines.append(f"Subject: {self._str(card.get('subject'))}")
        if self._str(card.get("chapter")):
            lines.append(f"Chapter: {self._str(card.get('chapter'))}")
        if self._str(card.get("material_type")):
            lines.append(f"Material type: {self._str(card.get('material_type'))}")
        if self._str(card.get("material_url")):
            lines.append(f"Material URL: {self._str(card.get('material_url'))}")
        if notes:
            lines.append(
                "Material notes/context: "
                + (notes[:2200] if len(notes) > 2200 else notes)
            )
        if self._to_bool(card.get("ocr_required")):
            lines.append(
                "Strict OCR may be required. Preserve equations, labels, and symbols exactly before reasoning."
            )
        if is_query:
            lines.extend(
                [
                    f"Student question: {question.strip()}",
                    "Answer the question directly. The answer may be conceptual; it does not need to be numeric.",
                    "For strategy or how-to questions, answer with reusable methods or decision rules first, not invented example scenarios.",
                    "Return markdown with sections for Answer, Explanation, Key Takeaways, Common Traps, and Supporting Sources when available.",
                    "If a graph or visualization materially helps, include graph-ready equations or visualization metadata.",
                ]
            )
        elif "formula" in self._str(mode).lower() or "sheet" in self._str(mode).lower():
            lines.extend(
                [
                    "Deliverable: a compact JEE formula sheet.",
                    "Use sections: Formula Map, Symbol Legend, Validity / Constraints, Fast Use Cues, and Common Mistakes.",
                    "Include only the highest-yield formulas, notation, sign conventions, hidden constraints, and one-line usage cues.",
                    "Keep it scan-friendly and revision-first.",
                ]
            )
        elif "flashcard" in self._str(mode).lower():
            lines.extend(
                [
                    "Deliverable: high-yield JEE flashcards from this material.",
                    "Return a crisp front/back style deck with concepts, formulas, traps, and quick recall prompts.",
                    "Format explicitly as Card 1 / Front / Back, Card 2 / Front / Back, and keep each back concise.",
                    "Keep cards short, memory-friendly, and exam-useful.",
                ]
            )
        elif "revision_plan" in self._str(mode).lower() or "study_plan" in self._str(mode).lower():
            lines.extend(
                [
                    "Deliverable: a focused revision plan from this material.",
                    "Use sections: What to Read First, What to Memorize, Practice Order, Same-Day Checkpoint, 1-Day Plan, and 3-Day Plan.",
                    "Include a 1-day and 3-day path, what to memorize, what to practice, what to self-test, and a same-day checkpoint list.",
                ]
            )
        elif "quiz" in self._str(mode).lower() or "drill" in self._str(mode).lower():
            lines.extend(
                [
                    "Deliverable: a short self-test drill from this material.",
                    "Give 5 JEE-style questions with mixed difficulty, then provide a separate Answer Key section and one-line solving cue for each.",
                ]
            )
        elif "summary" in self._str(mode).lower() or "summarize" in self._str(mode).lower():
            lines.extend(
                [
                    "Deliverable: a high-accuracy JEE revision summary.",
                    "Use sections: Core Idea Map, Must-Know Formulas, Pattern Cues, Common Traps, Quick Recall, and Last-Minute Checklist.",
                    "Include core idea map, formulas, pattern cues, common traps, and quick recall bullets.",
                    "If retrieval finds reliable evidence, use it only to support or sharpen the material-grounded answer.",
                    "If a graph improves understanding, include graph-ready equations or metadata.",
                ]
            )
        else:
            lines.extend(
                [
                    "Deliverable: deep JEE Advanced notes.",
                    "Use sections: Concept Architecture, Derivation Flow, Shortcuts, Hidden Traps, Rank-Booster Tips, and Test Strategy.",
                    "Include concept architecture, derivation flow, shortcuts, hidden traps, rank-booster tips, and test strategy.",
                    "If retrieval finds reliable evidence, use it only to support or sharpen the material-grounded answer.",
                    "If a graph improves understanding, include graph-ready equations or metadata.",
                ]
            )
        lines.append(
            "Return polished markdown with clear section headers. Keep the output material-grounded, exam-useful, and do not fabricate citations."
        )
        return "\n".join(lines)

    def _material_mode_family(self, mode: str) -> str:
        lowered = self._str(mode).lower()
        if "formula" in lowered or "sheet" in lowered:
            return "formula"
        if "flashcard" in lowered:
            return "flashcards"
        if "revision_plan" in lowered or "study_plan" in lowered:
            return "revision_plan"
        if "quiz" in lowered or "drill" in lowered:
            return "quiz_drill"
        if "summary" in lowered or "summarize" in lowered:
            return "summary"
        if "note" in lowered:
            return "notes"
        if lowered == "qa" or "question" in lowered:
            return "qa"
        return "notes"

    def _material_fallback_content(self, *, mode: str, title: str) -> str:
        family = self._material_mode_family(mode)
        if family == "formula":
            return "\n".join(
                [
                    f"# Formula Sheet: {title}",
                    "",
                    "## Core Relations",
                    "- Write the canonical formula before substitution.",
                    "- Track notation, sign conventions, and conditions of use.",
                    "",
                    "## Quick Checks",
                    "- Verify units and limiting cases.",
                    "- Confirm the shortcut is valid for this regime.",
                ]
            )
        if family == "flashcards":
            return "\n".join(
                [
                    f"# Flashcards: {title}",
                    "",
                    "Q: What is the first principle to recall?",
                    "A: Start with the governing definition and the standard formula.",
                    "",
                    "Q: What is the most common trap?",
                    "A: Applying a shortcut outside its valid condition.",
                ]
            )
        if family == "revision_plan":
            return "\n".join(
                [
                    f"# Revision Plan: {title}",
                    "",
                    "## 20-minute rescue",
                    "- Read the key definition and formulas.",
                    "- Solve one direct example.",
                    "",
                    "## 60-minute revision",
                    "- Rebuild the logic once.",
                    "- Practice easy -> medium -> PYQ style.",
                ]
            )
        if family == "quiz_drill":
            return "\n".join(
                [
                    f"# Quiz Drill: {title}",
                    "",
                    "## Questions",
                    "1. State the core concept.",
                    "2. Write the main formula and define every symbol.",
                    "3. Identify one common trap.",
                    "4. Solve one direct application.",
                    "5. State the fastest final verification step.",
                ]
            )
        if family == "summary":
            return "\n".join(
                [
                    f"# Summary: {title}",
                    "",
                    "## Core Idea",
                    "- Main concept and where it is used.",
                    "",
                    "## Must-Know Points",
                    "- Key formulas and conditions.",
                    "- Common mistakes and validation checks.",
                ]
            )
        if family == "qa":
            return "\n".join(
                [
                    "**Answer**",
                    "Start with the governing concept, apply the correct relation, and verify the final statement.",
                    "",
                    "**Explanation**",
                    "1. Identify the right principle.",
                    "2. Substitute carefully.",
                    "3. Check sign, units, and limiting case.",
                ]
            )
        return "\n".join(
            [
                f"# JEE Notes: {title}",
                "",
                "## Concept Architecture",
                "- Core idea and chapter link.",
                "",
                "## Derivation / Logic Path",
                "- Move from the standard relation to the final usable form.",
                "",
                "## Practice Ladder",
                "- Easy check -> medium application -> PYQ challenge.",
            ]
        )

    def _material_ai_is_placeholder_text(self, value: Any) -> bool:
        text = re.sub(r"\s+", " ", self._str(value)).strip()
        if not text:
            return True
        lowered = text.lower()
        if lowered in {"[unresolved]", "unresolved", "n/a", "na"}:
            return True
        placeholder_tokens = (
            "[unresolved]",
            "uncertain answer:",
            "insufficient evidence",
            "provider error:",
            "actual question is missing",
            "do not provide a clear answer",
            "could not solve this reliably",
            "engine returned empty output",
        )
        return any(token in lowered for token in placeholder_tokens)

    def _material_ai_keywords(self, text: str, *, limit: int = 6) -> list[str]:
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9_\-]{2,}", self._str(text))
        if not tokens:
            return []
        stopwords = {
            "the",
            "and",
            "with",
            "from",
            "that",
            "this",
            "these",
            "those",
            "into",
            "using",
            "used",
            "what",
            "when",
            "where",
            "which",
            "student",
            "question",
            "material",
            "study",
            "notes",
            "title",
            "chapter",
            "subject",
            "return",
            "answer",
            "explanation",
            "confidence",
        }
        keywords: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            lowered = token.lower()
            if lowered in stopwords or lowered in seen:
                continue
            seen.add(lowered)
            keywords.append(token)
            if len(keywords) >= limit:
                break
        return keywords

    def _build_material_ai_retrieval_query(
        self,
        *,
        mode: str,
        title: str,
        card: dict[str, Any],
        question: str = "",
        is_query: bool,
    ) -> str:
        subject = self._str(card.get("subject"))
        chapter = self._str(card.get("chapter"))
        notes = self._str(card.get("material_notes"))
        lead = " ".join(
            chunk
            for chunk in [subject, chapter or title]
            if self._str(chunk)
        ).strip() or title
        note_keywords = " ".join(self._material_ai_keywords(notes, limit=6))
        lowered = self._str(mode).lower()
        if is_query:
            return " ".join(
                chunk
                for chunk in [lead, "JEE", self._str(question), note_keywords]
                if self._str(chunk)
            ).strip()
        if "formula" in lowered or "sheet" in lowered:
            intent = "formula sheet identities constraints shortcuts"
        elif "flashcard" in lowered:
            intent = "flashcards quick recall traps"
        elif "revision_plan" in lowered or "study_plan" in lowered:
            intent = "revision plan practice order checkpoint"
        elif "quiz" in lowered or "drill" in lowered:
            intent = "self test drill answer key"
        elif "summary" in lowered or "summarize" in lowered:
            intent = "revision summary formulas common traps quick recall"
        elif "note" in lowered:
            intent = "deep notes derivation shortcuts"
        else:
            intent = "study guide key concepts"
        return " ".join(
            chunk
            for chunk in [lead, "JEE", intent, note_keywords]
            if self._str(chunk)
        ).strip()

    def _compose_material_ai_content(
        self,
        response: dict[str, Any],
        *,
        mode: str = "",
        title: str = "",
    ) -> str:
        direct_content = self._str(response.get("content") or response.get("answer_text"))
        answer = self._str(response.get("answer"))
        explanation = self._str(response.get("explanation"))
        concept = self._str(response.get("concept"))
        confidence = self._str(response.get("confidence"))
        if (
            direct_content
            and not self._material_ai_is_placeholder_text(direct_content)
            and (
                self._material_mode_family(mode) != "qa"
                or "\n" in direct_content
                or direct_content.lstrip().startswith("#")
                or "**" in direct_content
            )
        ):
            return direct_content
        sections: list[str] = []
        if answer and not self._material_ai_is_placeholder_text(answer):
            sections.append(f"**Answer**\n{answer}")
        if (
            explanation
            and explanation != answer
            and not self._material_ai_is_placeholder_text(explanation)
        ):
            sections.append(f"**Explanation**\n{explanation}")
        if concept:
            sections.append(f"**Concept**: {concept}")
        if confidence:
            sections.append(f"**Confidence**: {confidence}")
        if sections:
            return "\n\n".join(sections).strip()
        if direct_content and not self._material_ai_is_placeholder_text(direct_content):
            return direct_content
        return ""

    def _material_ai_has_meaningful_payload(self, response: dict[str, Any]) -> bool:
        for value in (
            response.get("content"),
            response.get("answer_text"),
            response.get("answer"),
            response.get("explanation"),
        ):
            text = self._str(value)
            if text and not self._material_ai_is_placeholder_text(text):
                return True
        return False

    async def _material_generate(self, payload: dict[str, Any]) -> dict[str, Any]:
        material_id = self._safe_id(payload.get("material_id"))
        mode = self._str(payload.get("mode") or "summarize") or "summarize"
        material = self._find_material_item(material_id)
        title = self._str(payload.get("title") or (material or {}).get("title"))
        if not title:
            return {
                "ok": False,
                "status": "MISSING_TITLE",
                "message": "material title is required",
            }
        raw_options = dict(payload.get("options")) if isinstance(payload.get("options"), dict) else {}
        card = self._build_material_ai_card(
            payload=payload,
            material=material,
            mode=mode,
            title=title,
        )
        options = self._build_material_ai_options(
            mode=mode,
            raw_options=raw_options,
            is_query=False,
        )
        if "retrieval_query_override" not in options:
            options["retrieval_query_override"] = self._build_material_ai_retrieval_query(
                mode=mode,
                title=title,
                card=card,
                is_query=False,
            )
        prompt = self._str(payload.get("prompt")) or self._build_material_ai_prompt(
            mode=mode,
            title=title,
            card=card,
            is_query=False,
        )
        cache_key = self._material_ai_cache_key(
            material_id=material_id or title,
            mode=mode,
            prompt=prompt,
            options=options,
        )
        cached = self._material_ai_cache.get(cache_key)
        if isinstance(cached, dict):
            return {**cached, "cached": True}
        self._material_ai_status[cache_key] = {
            "status": "generating",
            "updated_at": self._now_ms(),
            "material_id": material_id,
            "mode": mode,
        }
        response = await material_generation_entry(
            prompt=prompt,
            title=title,
            mode=mode,
            card=card,
            options=options,
        )
        content = self._compose_material_ai_content(response, mode=mode, title=title)
        authoritative = self._material_ai_has_meaningful_payload(response)
        output = dict(response)
        if content:
            output["content"] = content
        if authoritative:
            output["ok"] = True
            self._material_ai_cache[cache_key] = dict(output)
        else:
            output["ok"] = False
            if self._str(output.get("status")).lower() in {"", "ok"}:
                output["status"] = "MATERIAL_ENGINE_EMPTY_OUTPUT"
            if not self._str(output.get("message")):
                output["message"] = "Material AI did not return usable content."
            self._material_ai_cache.pop(cache_key, None)
        self._material_ai_status[cache_key] = {
            "status": "ready" if output.get("ok") else "failed",
            "updated_at": self._now_ms(),
            "material_id": material_id,
            "mode": mode,
        }
        return output

    async def _material_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        material_id = self._safe_id(payload.get("material_id"))
        question = self._str(payload.get("question") or payload.get("prompt"))
        if not material_id:
            return {
                "ok": False,
                "status": "MISSING_MATERIAL_ID",
                "message": "material_id is required",
            }
        if not question:
            return {
                "ok": False,
                "status": "MISSING_QUESTION",
                "message": "question is required",
            }
        mode = self._str(payload.get("context_mode") or payload.get("mode") or "qa")
        material = self._find_material_item(material_id)
        title = self._str(payload.get("title") or (material or {}).get("title") or "Study material")
        raw_options = dict(payload.get("options")) if isinstance(payload.get("options"), dict) else {}
        card = self._build_material_ai_card(
            payload=payload,
            material=material,
            mode=mode,
            title=title,
        )
        options = self._build_material_ai_options(
            mode=mode,
            raw_options=raw_options,
            is_query=True,
        )
        if "retrieval_query_override" not in options:
            options["retrieval_query_override"] = self._build_material_ai_retrieval_query(
                mode=mode,
                title=title,
                card=card,
                question=question,
                is_query=True,
            )
        prompt = self._str(payload.get("prompt")) or self._build_material_ai_prompt(
            mode=mode,
            title=title,
            card=card,
            question=question,
            is_query=True,
        )
        response = await material_generation_entry(
            prompt=prompt,
            title=title,
            mode=mode,
            card=card,
            options=options,
            question=question,
        )
        content = self._compose_material_ai_content(
            response,
            mode=mode,
            title=title,
        )
        authoritative = self._material_ai_has_meaningful_payload(response)
        if content:
            response = {**response, "content": content}
        if authoritative:
            response["ok"] = True
        else:
            response["ok"] = False
            if self._str(response.get("status")).lower() in {"", "ok"}:
                response["status"] = "MATERIAL_ENGINE_EMPTY_OUTPUT"
            if not self._str(response.get("message")):
                response["message"] = "Material AI did not return usable content."
        return response

    async def _material_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        material_id = self._safe_id(payload.get("material_id"))
        if not material_id:
            return {
                "ok": False,
                "status": "MISSING_MATERIAL_ID",
                "message": "material_id is required",
            }
        status_row = next(
            (
                dict(value)
                for value in self._material_ai_status.values()
                if self._safe_id(value.get("material_id")) == material_id
            ),
            None,
        )
        if status_row is not None:
            return {"ok": True, **status_row}
        return {
            "ok": False,
            "status": "not_requested",
            "material_id": material_id,
            "updated_at": self._now_ms(),
        }

    async def _class_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        students = payload.get("students") if isinstance(payload.get("students"), list) else []
        exams = payload.get("exams") if isinstance(payload.get("exams"), list) else []
        homeworks = payload.get("homeworks") if isinstance(payload.get("homeworks"), list) else []
        study_materials = (
            payload.get("study_materials")
            if isinstance(payload.get("study_materials"), list)
            else []
        )
        scheduled_classes = (
            payload.get("scheduled_classes")
            if isinstance(payload.get("scheduled_classes"), list)
            else []
        )
        if not students:
            return {
                "ok": False,
                "status": "MISSING_STUDENTS",
                "message": "students is required",
            }
        return await class_summary_entry(
            students=[dict(item) for item in students if isinstance(item, dict)],
            exams=[dict(item) for item in exams if isinstance(item, dict)],
            homeworks=[dict(item) for item in homeworks if isinstance(item, dict)],
            study_materials=[dict(item) for item in study_materials if isinstance(item, dict)],
            scheduled_classes=[dict(item) for item in scheduled_classes if isinstance(item, dict)],
            options=dict(payload.get("options")) if isinstance(payload.get("options"), dict) else None,
        )

    async def _student_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        history = payload.get("history") if isinstance(payload.get("history"), list) else []
        if not history:
            return {
                "ok": False,
                "status": "MISSING_HISTORY",
                "message": "history is required",
            }
        return await student_profile_entry(
            history=[dict(item) for item in history if isinstance(item, dict)],
            options=dict(payload.get("options")) if isinstance(payload.get("options"), dict) else None,
        )

    async def _student_intelligence(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = self._str(payload.get("account_id") or payload.get("student_id"))
        latest_result = dict(payload.get("latest_result")) if isinstance(payload.get("latest_result"), dict) else {}
        history = payload.get("history") if isinstance(payload.get("history"), list) else []
        if not account_id:
            return {
                "ok": False,
                "status": "MISSING_ACCOUNT_ID",
                "message": "account_id is required",
            }
        if not latest_result:
            return {
                "ok": False,
                "status": "MISSING_LATEST_RESULT",
                "message": "latest_result is required",
            }
        return await student_intelligence_entry(
            account_id=account_id,
            latest_result=latest_result,
            history=[dict(item) for item in history if isinstance(item, dict)],
            options=dict(payload.get("options")) if isinstance(payload.get("options"), dict) else None,
        )

    async def _analyze_exam(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = dict(payload.get("result")) if isinstance(payload.get("result"), dict) else {}
        if not result:
            result = {
                key: value
                for key, value in payload.items()
                if key not in {"action", "options"}
            }
        if not result:
            return {
                "ok": False,
                "status": "MISSING_RESULT",
                "message": "result is required",
            }
        response = await analyze_exam_entry(
            result=result,
            options=dict(payload.get("options")) if isinstance(payload.get("options"), dict) else None,
        )
        if response.get("ok") is True:
            response.setdefault("ai_available", True)
        return response

    async def _atlas_report_issue(self, payload: dict[str, Any]) -> dict[str, Any]:
        issue = self._str(
            payload.get("issue")
            or payload.get("issue_summary")
            or payload.get("instruction")
            or payload.get("prompt")
            or payload.get("summary")
            or payload.get("message")
        )
        role = self._str(payload.get("role") or payload.get("atlas_role")).lower() or "student"
        context = dict(payload.get("context")) if isinstance(payload.get("context"), dict) else {}
        context["surface"] = self._atlas_incident_surface(payload=payload, context=context)
        account_id = self._str(
            payload.get("account_id")
            or payload.get("user_id")
            or payload.get("student_id")
            or context.get("account_id")
            or context.get("user_id")
            or context.get("student_id")
        )
        if not issue:
            if self._str(payload.get("action")).lower() in {"health_check", "ping", "noop"}:
                issue = "General app health check requested."
            else:
                return {
                    "ok": False,
                    "status": "MISSING_ISSUE",
                    "message": "issue or instruction is required",
                }
        diagnostics = await self.atlas_health_snapshot(
            role=role,
            account_id=account_id,
            context=context,
        )
        repair_attempt = await self._atlas_attempt_minor_repairs(
            issue=issue,
            role=role,
            context=context,
            diagnostics=diagnostics,
        )
        post_repair_diagnostics = diagnostics
        if repair_attempt["applied_fix_count"]:
            post_repair_diagnostics = await self.atlas_health_snapshot(
                role=role,
                account_id=account_id,
                context={
                    **context,
                    "health_repair_summary": repair_attempt["summary"],
                },
            )
        analysis = await self._atlas_issue_analysis(
            issue=issue,
            role=role,
            context=context,
            diagnostics=post_repair_diagnostics,
            repair_attempt=repair_attempt,
        )
        incident_id = self._new_id("atlas_incident")
        runtime_logs = self._atlas_collect_recent_runtime_logs(
            context=context,
            diagnostics=post_repair_diagnostics,
        )
        surface = self._atlas_incident_surface(payload=payload, context=context)
        report = {
            "incident_id": incident_id,
            "ok": True,
            "status": "SUCCESS",
            "issue_summary": issue,
            "surface": surface,
            "role": role,
            "reporter": {
                "account_id": account_id,
                "user_name": self._str(
                    payload.get("user_name")
                    or context.get("student_name")
                    or context.get("teacher_name")
                    or context.get("current_user_name")
                    or context.get("user_name")
                ),
                "email": self._str(
                    payload.get("email")
                    or context.get("email")
                    or (context.get("reporter") or {}).get("email")
                    if isinstance(context.get("reporter"), dict)
                    else context.get("email")
                ),
            },
            **analysis,
            "self_heal": repair_attempt,
            "diagnostics_before_repair": diagnostics,
            "diagnostics": post_repair_diagnostics,
            "runtime_logs": runtime_logs,
            "context": context,
            "maintenance": {
                "enabled": self._to_bool(context.get("maintenance_mode")),
                "artifacts_isolated": self._to_bool(
                    context.get("maintenance_artifacts_isolated")
                ),
                "trigger": self._str(context.get("trigger")),
                "scheduled_window_local": self._str(
                    context.get("scheduled_window_local")
                ),
                "scope": [
                    self._str(item)
                    for item in (context.get("maintenance_scope") or [])
                    if self._str(item)
                ],
                "failing_areas": [
                    self._str(item)
                    for item in (context.get("maintenance_failing_areas") or [])
                    if self._str(item)
                ],
                "audit": context.get("maintenance_audit")
                if isinstance(context.get("maintenance_audit"), dict)
                else None,
            },
        }
        if self._to_bool(payload.get("auto_email", True)):
            email_result = self._atlas_incident_email.send_incident_report(
                report=report,
                recipient=self._str(payload.get("recipient_email")) or None,
            )
        else:
            email_result = {
                "ok": True,
                "sent": False,
                "message": "auto_email was disabled for this report",
            }
        report["email"] = email_result
        report["mail_sent"] = email_result.get("sent") is True
        report["message"] = (
            "Atlas analyzed the issue, applied a safe repair, and emailed support."
            if report["mail_sent"] and repair_attempt["applied_fix_count"] > 0
            else "Atlas analyzed the issue and emailed support."
            if report["mail_sent"]
            else "Atlas analyzed the issue, but the support email was not sent."
        )
        return report

    async def _atlas_health_probe(self, payload: dict[str, Any]) -> dict[str, Any]:
        role = self._str(payload.get("role") or payload.get("atlas_role")).lower() or "student"
        context = dict(payload.get("context")) if isinstance(payload.get("context"), dict) else {}
        account_id = self._str(
            payload.get("account_id")
            or payload.get("user_id")
            or payload.get("student_id")
            or context.get("account_id")
            or context.get("user_id")
            or context.get("student_id")
        )
        diagnostics = await self.atlas_health_snapshot(
            role=role,
            account_id=account_id,
            context=context,
        )
        action = self._str(payload.get("action")).lower() or "health_check"
        return {
            "ok": True,
            "status": "SUCCESS",
            "message": "Atlas app health is reachable.",
            "probe_action": action,
            "surface": self._atlas_incident_surface(payload=payload, context=context),
            "diagnostics": diagnostics,
        }

    async def _atlas_issue_analysis(
        self,
        *,
        issue: str,
        role: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
        repair_attempt: dict[str, Any],
    ) -> dict[str, Any]:
        prompt = self._atlas_issue_prompt(
            issue=issue,
            role=role,
            context=context,
            diagnostics=diagnostics,
            repair_attempt=repair_attempt,
        )
        try:
            response = await self._ai_chat_or_solve(
                {
                    "prompt": prompt,
                    "user_id": "atlas_support",
                    "chat_id": f"atlas_support_{role}_{self._safe_id(context.get('surface')) or 'general'}",
                    "options": {
                        "function": "atlas_incident_triage",
                        "app_surface": "atlas_support_incident",
                        "return_structured": True,
                        "return_markdown": False,
                        "enable_web_retrieval": False,
                        "require_citations": "none",
                        "evidence_mode": "none",
                    },
                    "card": {
                        "issue": issue,
                        "role": role,
                        "surface": self._str(context.get("surface")),
                        "diagnostics": diagnostics,
                    },
                }
            )
            parsed = self._atlas_parse_json_candidate(
                self._str(response.get("answer") or response.get("explanation"))
            )
            if isinstance(parsed, dict):
                normalized = self._atlas_normalize_issue_analysis(parsed)
                if normalized:
                    return normalized
        except Exception:
            pass
        return self._atlas_fallback_issue_analysis(
            issue=issue,
            role=role,
            context=context,
            diagnostics=diagnostics,
            repair_attempt=repair_attempt,
        )

    def _atlas_issue_prompt(
        self,
        *,
        issue: str,
        role: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
        repair_attempt: dict[str, Any],
    ) -> str:
        payload = {
            "issue": issue,
            "role": role,
            "surface": self._str(context.get("surface")),
            "context_excerpt": self._atlas_compact_value(context),
            "diagnostics": diagnostics,
            "repair_attempt": repair_attempt,
        }
        return "\n".join(
            [
                "You are Atlas Incident Investigator for the LalaCore app.",
                "Analyze the issue deeply using the provided app context and diagnostics.",
                "Return strict JSON only with this shape:",
                '{"summary":"...","severity":"low|medium|high|critical","likely_root_causes":["..."],"plausible_causes_by_layer":{"client":["..."],"backend":["..."],"ai":["..."],"network":["..."],"data":["..."]},"evidence":["..."],"next_steps":["..."],"impact_assessment":"...","engineer_checklist":["..."],"user_safe_reply":"...","engineer_report":"..."}',
                "Keep the summary concrete and operational.",
                "Mention likely backend, network, AI, UI, or data causes only when evidence supports them.",
                "If a safe repair was already attempted, explain whether it likely helped and what still needs checking.",
                "",
                json.dumps(payload, ensure_ascii=False),
            ]
        )

    def _atlas_normalize_issue_analysis(
        self,
        raw: dict[str, Any],
    ) -> dict[str, Any]:
        severity = self._str(raw.get("severity")).lower()
        if severity not in {"low", "medium", "high", "critical"}:
            severity = "medium"
        by_layer = raw.get("plausible_causes_by_layer")
        normalized_layers = {}
        if isinstance(by_layer, dict):
            for key in ("client", "backend", "ai", "network", "data"):
                values = by_layer.get(key)
                if isinstance(values, list):
                    normalized_layers[key] = [
                        self._str(item)
                        for item in values
                        if self._str(item)
                    ][:4]
        return {
            "summary": self._str(raw.get("summary")),
            "severity": severity,
            "likely_root_causes": [
                self._str(item)
                for item in (raw.get("likely_root_causes") or [])
                if self._str(item)
            ][:6],
            "evidence": [
                self._str(item)
                for item in (raw.get("evidence") or [])
                if self._str(item)
            ][:8],
            "next_steps": [
                self._str(item)
                for item in (raw.get("next_steps") or [])
                if self._str(item)
            ][:6],
            "plausible_causes_by_layer": normalized_layers,
            "impact_assessment": self._str(raw.get("impact_assessment")),
            "engineer_checklist": [
                self._str(item)
                for item in (raw.get("engineer_checklist") or [])
                if self._str(item)
            ][:8],
            "user_safe_reply": self._str(raw.get("user_safe_reply")),
            "engineer_report": self._str(raw.get("engineer_report")),
        }

    def _atlas_fallback_issue_analysis(
        self,
        *,
        issue: str,
        role: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
        repair_attempt: dict[str, Any],
    ) -> dict[str, Any]:
        lowered_issue = issue.lower()
        evidence: list[str] = []
        likely_root_causes: list[str] = []
        next_steps: list[str] = []
        engineer_checklist: list[str] = []
        plausible_causes_by_layer: dict[str, list[str]] = {
            "client": [],
            "backend": [],
            "ai": [],
            "network": [],
            "data": [],
        }
        severity = "medium"
        last_error = self._str(context.get("last_error") or context.get("error"))
        if last_error:
            evidence.append(f"Last surfaced error: {last_error}")
            plausible_causes_by_layer["client"].append(
                "A user-visible error was surfaced on the current screen."
            )
        material_failed = self._to_int(
            (diagnostics.get("material_ai_status") or {}).get("failed_count"),
            0,
        )
        web_diag = diagnostics.get("web_diagnostics")
        if isinstance(web_diag, dict):
            web_error = self._str(
                web_diag.get("web_error_reason") or web_diag.get("error_reason")
            )
            if web_error:
                evidence.append(f"Recent web/AI retrieval issue: {web_error}")
                likely_root_causes.append(
                    "Recent AI or retrieval requests have failure signals in the backend diagnostics."
                )
                plausible_causes_by_layer["ai"].append(
                    "Recent retrieval or provider diagnostics already show failure reasons."
                )
        if material_failed > 0:
            evidence.append(
                f"{material_failed} recent material-AI generation task(s) are marked failed."
            )
            likely_root_causes.append(
                "Material or AI generation requests recently failed in the local app backend."
            )
            plausible_causes_by_layer["backend"].append(
                "Recent AI-generation jobs failed inside the app backend state."
            )
        tool_stats = diagnostics.get("atlas_tool_stats")
        if isinstance(tool_stats, dict):
            avoid = tool_stats.get("avoid_tools")
            if isinstance(avoid, list) and avoid:
                evidence.append(
                    f"Atlas recently observed failures on: {', '.join(str(item) for item in avoid[:5])}"
                )
                plausible_causes_by_layer["ai"].append(
                    "Atlas tool telemetry already shows recent tool failures."
                )
        context_excerpt = diagnostics.get("context_excerpt")
        network_quality = ""
        if isinstance(context_excerpt, dict):
            network_quality = self._str(
                context_excerpt.get("network_quality")
                or (context_excerpt.get("participantSnapshot") or {}).get("network_quality")
                if isinstance(context_excerpt.get("participantSnapshot"), dict)
                else context_excerpt.get("network_quality")
            ).lower()
        if network_quality in {"poor", "degraded"}:
            evidence.append(f"Current network quality is reported as {network_quality}.")
            likely_root_causes.append(
                "Client-side or classroom network degradation is likely contributing to lag or failures."
            )
            plausible_causes_by_layer["network"].append(
                "Network quality is already degraded in the current context snapshot."
            )
        maintenance_audit = context.get("maintenance_audit")
        if isinstance(maintenance_audit, dict):
            signature_map = maintenance_audit.get("failure_signatures")
            extracted_signatures: list[dict[str, Any]] = []
            if isinstance(signature_map, dict):
                for rows in signature_map.values():
                    if not isinstance(rows, list):
                        continue
                    for item in rows:
                        if isinstance(item, dict):
                            extracted_signatures.append(item)
            if extracted_signatures:
                labels = [
                    self._str(item.get("code") or item.get("area"))
                    for item in extracted_signatures
                    if self._str(item.get("code") or item.get("area"))
                ]
                if labels:
                    evidence.append(
                        "Maintenance signatures detected: "
                        + ", ".join(labels[:6])
                    )
                for item in extracted_signatures[:4]:
                    cause = self._str(item.get("root_cause"))
                    fix = self._str(item.get("atlas_fix"))
                    layer = self._str(item.get("layer")).lower()
                    if cause and cause not in likely_root_causes:
                        likely_root_causes.append(cause)
                    if fix:
                        next_steps.append(fix)
                    if cause and layer in plausible_causes_by_layer:
                        plausible_causes_by_layer[layer].append(cause)
        if repair_attempt.get("applied_fix_count"):
            evidence.append(
                f'Atlas applied {repair_attempt["applied_fix_count"]} safe repair(s): {self._str(repair_attempt.get("summary"))}'
            )
        local_backend_repair = context.get("local_backend_repair")
        if isinstance(local_backend_repair, dict) and local_backend_repair.get("attempted") is True:
            repair_summary = self._str(local_backend_repair.get("summary"))
            if repair_summary:
                evidence.append(f"Client-side backend repair: {repair_summary}")
            after_url = self._str(local_backend_repair.get("after_auth_backend_url"))
            if after_url:
                plausible_causes_by_layer["client"].append(
                    f"The app recently re-bound its backend routing to {after_url}."
                )
            if local_backend_repair.get("recovered") is True:
                likely_root_causes.append(
                    "A stale client-side backend routing override was preventing the app from reaching the healthy backend."
                )
                next_steps.append(
                    "Confirm the repaired backend route stays healthy across the next app launch."
                )
        if any(token in lowered_issue for token in ("crash", "crashing", "stopped working")):
            severity = "high"
            likely_root_causes.append(
                "The surface may be hitting an unhandled runtime error or an unstable action path."
            )
            plausible_causes_by_layer["client"].append(
                "Crash-like symptoms usually point to an unstable runtime path or bad state transition."
            )
        elif any(token in lowered_issue for token in ("lag", "slow", "delay", "stuck")):
            severity = "medium"
            likely_root_causes.append(
                "The affected surface is likely waiting on a slow backend or degraded network path."
            )
            plausible_causes_by_layer["backend"].append(
                "Lag symptoms often come from slow backend action paths or oversized cached state."
            )
        elif any(token in lowered_issue for token in ("ai", "atlas")) and (
            "not working" in lowered_issue or "failed" in lowered_issue
        ):
            severity = "high"
            likely_root_causes.append(
                "Atlas or AI execution may be degraded by backend request failures or invalid tool execution state."
            )
            plausible_causes_by_layer["ai"].append(
                "AI failure symptoms are consistent with provider, retrieval, or tool-state problems."
            )
        if not likely_root_causes:
            likely_root_causes.append(
                "The issue appears real, but Atlas could not isolate a single dominant cause from the available diagnostics."
            )
        next_steps.extend(
            [
                "Review the attached diagnostics JSON and compare the failing surface with recent Atlas/tool failures.",
                "Check whether the same issue reproduces on a second attempt with the same user flow.",
                "If AI is affected, inspect recent backend/provider failures and fallback paths first.",
            ]
        )
        engineer_checklist.extend(
            [
                "Reproduce the same user flow on the same surface using the same account context.",
                "Check the attached diagnostics JSON for recent AI, retrieval, and Atlas tool failures.",
                "Inspect network quality and any surfaced runtime error before changing feature logic.",
                "If the issue remains after Atlas safe repair, inspect the exact failing backend action or screen load path.",
            ]
        )
        summary = (
            f"Atlas reviewed the {role} issue on {self._str(context.get('surface')) or 'the current surface'} "
            f"and found {len(evidence)} supporting signal(s)."
        )
        return {
            "summary": summary,
            "severity": severity,
            "likely_root_causes": likely_root_causes[:6],
            "evidence": evidence[:8],
            "next_steps": next_steps[:6],
            "plausible_causes_by_layer": {
                key: value[:4]
                for key, value in plausible_causes_by_layer.items()
                if value
            },
            "impact_assessment": self._atlas_issue_impact_assessment(
                issue=issue,
                role=role,
                repair_attempt=repair_attempt,
            ),
            "engineer_checklist": engineer_checklist[:8],
            "user_safe_reply": self._atlas_user_safe_issue_reply(
                issue=issue,
                repair_attempt=repair_attempt,
            ),
            "engineer_report": summary,
        }

    async def _atlas_attempt_minor_repairs(
        self,
        *,
        issue: str,
        role: str,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> dict[str, Any]:
        lowered = issue.lower()
        repairs: list[dict[str, Any]] = []
        if any(
            token in lowered
            for token in ("lag", "slow", "sluggish", "delay", "stuck", "freeze")
        ):
            cache_repair = self._atlas_trim_runtime_caches()
            if cache_repair["removed_entries"] > 0:
                repairs.append(cache_repair)
        if any(
            token in lowered
            for token in ("ai", "atlas", "summary", "flashcard", "notes", "quiz")
        ) or self._to_int(
            (diagnostics.get("material_ai_status") or {}).get("failed_count"),
            0,
        ) > 0:
            stale_repair = self._atlas_repair_stale_material_jobs()
            if stale_repair["updated_jobs"] > 0:
                repairs.append(stale_repair)
        return {
            "attempted": True,
            "applied_fix_count": len(repairs),
            "summary": (
                "Atlas applied safe runtime cleanup."
                if repairs
                else "No safe self-heal was applicable without risking behavior changes."
            ),
            "actions": repairs,
            "role": role,
            "surface": self._str(context.get("surface")),
        }

    def _atlas_trim_runtime_caches(self) -> dict[str, Any]:
        now = time.time()
        removed_entries = 0
        removed_entries += self._atlas_trim_expired_cache(self._web_search_cache, now)
        removed_entries += self._atlas_trim_expired_cache(self._web_page_evidence_cache, now)
        if len(self._import_chapter_infer_cache) > self._import_chapter_cache_max_entries:
            overflow = len(self._import_chapter_infer_cache) - self._import_chapter_cache_max_entries
            stale_keys = list(self._import_chapter_infer_cache.keys())[:overflow]
            for key in stale_keys:
                self._import_chapter_infer_cache.pop(key, None)
            removed_entries += len(stale_keys)
        return {
            "type": "runtime_cache_cleanup",
            "removed_entries": removed_entries,
            "detail": "Trimmed expired runtime cache entries and bounded in-memory lookup caches.",
        }

    def _atlas_trim_expired_cache(
        self,
        cache: dict[str, dict[str, Any]],
        now: float,
    ) -> int:
        stale_keys = [
            key
            for key, value in cache.items()
            if float((value or {}).get("expires_at") or 0.0) <= now
        ]
        for key in stale_keys:
            cache.pop(key, None)
        if len(cache) > self._web_cache_max_entries:
            overflow = len(cache) - self._web_cache_max_entries
            oldest_keys = sorted(
                cache.keys(),
                key=lambda key: float((cache[key] or {}).get("saved_at") or 0.0),
            )[:overflow]
            for key in oldest_keys:
                cache.pop(key, None)
            stale_keys.extend(oldest_keys)
        return len(stale_keys)

    def _atlas_repair_stale_material_jobs(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        stale_before_ms = now_ms - int(timedelta(minutes=45).total_seconds() * 1000)
        updated_jobs = 0
        sample_keys: list[str] = []
        for key, value in self._material_ai_status.items():
            row = dict(value)
            status = self._str(row.get("status")).lower()
            updated_at = self._to_int(row.get("updated_at"), 0)
            if status not in {"queued", "running", "processing"}:
                continue
            if updated_at <= 0 or updated_at >= stale_before_ms:
                continue
            row["status"] = "failed"
            row["error"] = (
                "Atlas health triage marked this material-AI job as stale so the UI "
                "can recover cleanly. Retry the generation to restart with a fresh job."
            )
            row["updated_at"] = now_ms
            self._material_ai_status[key] = row
            updated_jobs += 1
            if len(sample_keys) < 5:
                sample_keys.append(key)
        return {
            "type": "stale_material_ai_repair",
            "updated_jobs": updated_jobs,
            "detail": (
                "Marked stale material-AI jobs as failed so the UI no longer appears stuck."
            ),
            "sample_job_keys": sample_keys,
        }

    def _atlas_collect_recent_runtime_logs(
        self,
        *,
        context: dict[str, Any],
        diagnostics: dict[str, Any],
    ) -> dict[str, Any]:
        recent_messages = context.get("recent_messages")
        recent_feed = context.get("atlas_runtime")
        snapshot_summary = context.get("snapshot_summary")
        return {
            "snapshot_summary": self._atlas_compact_value(snapshot_summary),
            "recent_messages": self._atlas_compact_value(recent_messages),
            "atlas_runtime": self._atlas_compact_value(recent_feed),
            "avoid_tools": self._atlas_compact_value(
                (diagnostics.get("atlas_tool_stats") or {}).get("avoid_tools")
            ),
            "material_ai_recent": self._atlas_compact_value(
                (diagnostics.get("material_ai_status") or {}).get("recent")
            ),
        }

    def _atlas_incident_surface(
        self,
        *,
        payload: dict[str, Any],
        context: dict[str, Any],
    ) -> str:
        context_card = context.get("context_card")
        if isinstance(context_card, dict):
            surface = self._str(context_card.get("surface"))
            if surface:
                return surface
        return self._str(payload.get("surface") or context.get("surface")) or "unknown_surface"

    def _atlas_issue_impact_assessment(
        self,
        *,
        issue: str,
        role: str,
        repair_attempt: dict[str, Any],
    ) -> str:
        lowered = issue.lower()
        if any(token in lowered for token in ("crash", "crashing", "stopped working")):
            return (
                f"The {role}-side flow is at risk of complete interruption until the failing path is stabilized."
            )
        if any(token in lowered for token in ("lag", "slow", "delay", "stuck")):
            return (
                f"The {role}-side experience is degraded and may feel unreliable, but some actions can still complete."
            )
        if repair_attempt.get("applied_fix_count"):
            return (
                "Atlas applied a low-risk repair, so the issue may already be reduced, but the attached diagnostics should still be reviewed."
            )
        return "The issue appears localized but real and should be reviewed before it spreads to more user flows."

    def _atlas_user_safe_issue_reply(
        self,
        *,
        issue: str,
        repair_attempt: dict[str, Any],
    ) -> str:
        if repair_attempt.get("applied_fix_count"):
            return (
                "I investigated the issue, applied a safe repair where possible, and sent a detailed report to support."
            )
        if any(token in issue.lower() for token in ("crash", "crashing", "stopped working")):
            return (
                "I captured the failing state and sent a detailed crash report to support."
            )
        return "I analyzed the issue in depth, captured diagnostics, and sent a detailed report to support."

    def _atlas_parse_json_candidate(self, text: str) -> dict[str, Any] | None:
        raw = self._str(text)
        if not raw:
            return None
        fenced_match = re.search(
            r"```(?:json)?\s*(\{.*?\})\s*```",
            raw,
            flags=re.DOTALL,
        )
        candidate = fenced_match.group(1).strip() if fenced_match else raw
        if not fenced_match:
            start = candidate.find("{")
            end = candidate.rfind("}")
            if start != -1 and end > start:
                candidate = candidate[start : end + 1]
        try:
            parsed = json.loads(candidate)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _atlas_compact_value(self, value: Any, *, depth: int = 0) -> Any:
        if depth >= 3:
            return self._str(value)[:280]
        if isinstance(value, str):
            return value[:1200]
        if isinstance(value, (int, float, bool)) or value is None:
            return value
        if isinstance(value, list):
            return [
                self._atlas_compact_value(item, depth=depth + 1)
                for item in value[:8]
            ]
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, item in list(value.items())[:20]:
                out[self._str(key)] = self._atlas_compact_value(
                    item,
                    depth=depth + 1,
                )
            return out
        return self._str(value)[:280]

    def _sort_live_class_schedule(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        status_rank = {"live": 0, "upcoming": 1, "scheduled": 1, "completed": 2, "ended": 2, "cancelled": 3}

        def key(row: dict[str, Any]) -> tuple[int, str, int]:
            status = self._str(row.get("status")).lower()
            start_time = self._str(row.get("start_time") or row.get("scheduled_at"))
            created_at = self._to_int(row.get("created_at"), 0)
            return (status_rank.get(status, 4), start_time, created_at)

        return sorted((dict(x) for x in rows), key=key)

    def _normalize_live_class_schedule_item(
        self,
        payload: dict[str, Any],
        *,
        existing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now_ms = self._now_ms()
        seed = dict(existing or {})
        class_id = self._safe_id(
            payload.get("class_id") or payload.get("id") or seed.get("class_id")
        ) or self._new_id("live")
        status = self._str(
            payload.get("status")
            or seed.get("status")
            or "upcoming"
        ).lower() or "upcoming"
        start_time = self._str(
            payload.get("start_time")
            or payload.get("scheduled_at")
            or seed.get("start_time")
            or seed.get("scheduled_at")
        )
        duration_minutes = self._to_int(
            payload.get("duration_minutes") or payload.get("duration") or seed.get("duration_minutes"),
            self._to_int(seed.get("duration_minutes"), 60),
        )
        reminder_offsets = payload.get("reminder_offsets_minutes")
        if not isinstance(reminder_offsets, list):
            reminder_offsets = seed.get("reminder_offsets_minutes")
        normalized_reminder_offsets = sorted(
            {
                max(1, self._to_int(value, 0))
                for value in (reminder_offsets or [])
                if self._to_int(value, 0) > 0
            },
            reverse=True,
        )
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = seed.get("metadata")
        normalized_metadata = dict(metadata or {})
        recurrence = payload.get("recurrence")
        if not isinstance(recurrence, dict):
            recurrence = seed.get("recurrence")
        normalized_recurrence = dict(recurrence or {})
        recurrence_id = self._safe_id(
            payload.get("recurrence_id")
            or normalized_recurrence.get("plan_id")
            or seed.get("recurrence_id")
        )
        occurrence_id = self._safe_id(
            payload.get("occurrence_id")
            or normalized_recurrence.get("occurrence_id")
            or seed.get("occurrence_id")
        )
        exception_dates = payload.get("exception_dates")
        if not isinstance(exception_dates, list):
            exception_dates = normalized_recurrence.get("exception_dates")
        normalized_exception_dates = [
            self._str(item)
            for item in (exception_dates or [])
            if self._str(item)
        ]
        override_metadata = payload.get("override_metadata")
        if not isinstance(override_metadata, dict):
            override_metadata = seed.get("override_metadata")
        normalized_override_metadata = dict(override_metadata or {})
        item = {
            "class_id": class_id,
            "teacher_id": self._safe_id(payload.get("teacher_id") or seed.get("teacher_id")),
            "teacher_name": self._str(payload.get("teacher_name") or seed.get("teacher_name")),
            "class_name": self._str(payload.get("class_name") or payload.get("class") or seed.get("class_name")),
            "title": self._str(payload.get("title") or seed.get("title")),
            "subject": self._str(payload.get("subject") or seed.get("subject")),
            "topic": self._str(payload.get("topic") or seed.get("topic")),
            "start_time": start_time,
            "scheduled_at": start_time,
            "duration_minutes": duration_minutes,
            "status": status,
            "description": self._str(payload.get("description") or seed.get("description")),
            "created_at": self._to_int(seed.get("created_at"), now_ms),
            "updated_at": now_ms,
            "reminder_offsets_minutes": normalized_reminder_offsets,
            "metadata": normalized_metadata,
            "recurrence": normalized_recurrence,
            "recurrence_id": recurrence_id,
            "occurrence_id": occurrence_id,
            "exception_dates": normalized_exception_dates,
            "override_metadata": normalized_override_metadata,
        }
        started_at = self._str(payload.get("started_at") or seed.get("started_at"))
        ended_at = self._str(payload.get("ended_at") or seed.get("ended_at"))
        if started_at:
            item["started_at"] = started_at
        if ended_at:
            item["ended_at"] = ended_at
        return item

    async def _schedule_live_class(self, payload: dict[str, Any]) -> dict[str, Any]:
        role = self._str(
            payload.get("role") or payload.get("user_role") or payload.get("request_role") or "teacher"
        ).lower()
        if role and role != "teacher":
            return {
                "ok": False,
                "status": "FORBIDDEN",
                "message": "Only teachers can schedule live classes",
            }

        title = self._str(payload.get("title"))
        teacher_id = self._safe_id(payload.get("teacher_id"))
        start_time = self._str(payload.get("start_time") or payload.get("scheduled_at"))
        if not title or not teacher_id or not start_time:
            return {
                "ok": False,
                "status": "MISSING_FIELDS",
                "message": "title, teacher_id and start_time are required",
            }

        class_id = self._safe_id(payload.get("class_id")) or self._new_id("live")
        async with self._lock:
            existing = next(
                (row for row in self._live_class_schedule if self._str(row.get("class_id")) == class_id),
                None,
            )
            item = self._normalize_live_class_schedule_item(
                {**payload, "class_id": class_id},
                existing=existing,
            )
            if existing is None:
                self._live_class_schedule.append(item)
            else:
                index = self._live_class_schedule.index(existing)
                self._live_class_schedule[index] = item
            self._write_list(self._live_class_schedule_file, self._live_class_schedule)
            schedule = self._sort_live_class_schedule(self._live_class_schedule)
        self._publish_live_class_schedule_event(
            "schedule_upserted" if existing is not None else "schedule_created",
            item=item,
            actor_role=role,
        )
        return {"ok": True, "status": "SUCCESS", "class": item, "schedule": schedule}

    async def _list_live_class_schedule(self, payload: dict[str, Any]) -> dict[str, Any]:
        viewer_role = self._str(payload.get("viewer_role") or payload.get("role")).lower()
        viewer_id = self._safe_id(payload.get("viewer_id") or payload.get("user_id"))
        rows = self._sort_live_class_schedule(self._live_class_schedule)
        if viewer_role == "teacher" and viewer_id:
            rows = [row for row in rows if self._str(row.get("teacher_id")) == viewer_id]
        else:
            rows = [row for row in rows if self._str(row.get("status")).lower() != "cancelled"]
        return {
            "ok": True,
            "status": "SUCCESS",
            "schedule": rows,
            "classes": rows,
            "list": rows,
        }

    async def _update_live_class_schedule_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        class_id = self._safe_id(payload.get("class_id") or payload.get("id"))
        if not class_id:
            return {"ok": False, "status": "MISSING_CLASS_ID", "message": "class_id required"}
        requested_status = self._str(payload.get("status")).lower()
        action = self._str(payload.get("action")).lower()
        if not requested_status:
            if action in {"start_live_class", "mark_class_live"}:
                requested_status = "live"
            elif action == "end_live_class":
                requested_status = "completed"
            elif action == "cancel_live_class":
                requested_status = "cancelled"
        if not requested_status:
            return {"ok": False, "status": "MISSING_STATUS", "message": "status required"}

        async with self._lock:
            existing = next(
                (row for row in self._live_class_schedule if self._str(row.get("class_id")) == class_id),
                None,
            )
            if existing is None:
                return {
                    "ok": False,
                    "status": "NOT_FOUND",
                    "message": "Live class not found",
                }
            patch = dict(payload)
            patch["class_id"] = class_id
            patch["status"] = requested_status
            if requested_status == "live" and not self._str(existing.get("started_at")):
                patch.setdefault("started_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            if requested_status in {"completed", "ended"} and not self._str(existing.get("ended_at")):
                patch.setdefault("ended_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            item = self._normalize_live_class_schedule_item(patch, existing=existing)
            index = self._live_class_schedule.index(existing)
            self._live_class_schedule[index] = item
            self._write_list(self._live_class_schedule_file, self._live_class_schedule)
            schedule = self._sort_live_class_schedule(self._live_class_schedule)
        self._publish_live_class_schedule_event(
            "schedule_status_changed",
            item=item,
            actor_role=self._str(payload.get("role") or payload.get("viewer_role")),
        )
        return {"ok": True, "status": "SUCCESS", "class": item, "schedule": schedule}

    def _normalize_chat_user_id(self, raw: Any) -> str:
        user_id = self._str(raw)
        if not user_id:
            return ""
        upper = user_id.upper()
        if upper in {"ADMIN", "ADMINISTRATOR", "TEACHER"}:
            return "TEACHER"
        return user_id

    def _search_norm(self, raw: Any) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", self._str(raw).lower())).strip()

    def _search_initials(self, raw: Any) -> str:
        parts = [x for x in self._search_norm(raw).split(" ") if x]
        if not parts:
            return ""
        return "".join(p[0] for p in parts)

    def _seed_chat_users_from_local_sources(self) -> None:
        def upsert_seed(
            *,
            user_id: Any,
            name: Any = "",
            role: Any = "",
            chat_id: Any = "",
            username: Any = "",
            email: Any = "",
        ) -> None:
            uid = self._normalize_chat_user_id(user_id)
            if not uid:
                return
            existing = dict(self._chat_users.get(uid, {}))
            resolved_name = self._str(name) or self._str(existing.get("name")) or uid
            resolved_role = self._str(role).lower() or self._str(existing.get("role")).lower()
            if uid == "TEACHER":
                resolved_role = "teacher"
                resolved_name = "Teacher (Direct)"
            if not resolved_role:
                resolved_role = "student"
            self._chat_users[uid] = {
                "user_id": uid,
                "chat_id": self._str(chat_id) or self._str(existing.get("chat_id")) or uid,
                "name": resolved_name,
                "username": self._str(username) or self._str(existing.get("username")) or resolved_name,
                "email": self._str(email) or self._str(existing.get("email")),
                "role": resolved_role,
                "updated_at": self._to_int(existing.get("updated_at"), self._now_ms()),
            }

        upsert_seed(user_id="TEACHER", name="Teacher (Direct)", role="teacher")

        for row in self._results:
            upsert_seed(
                user_id=(
                    row.get("student_id")
                    or row.get("account_id")
                    or row.get("user_id")
                    or row.get("chat_id")
                ),
                name=row.get("student_name") or row.get("name") or row.get("student"),
                role=row.get("role") or "student",
                chat_id=row.get("chat_id"),
                email=row.get("email"),
            )

        for row in self._teacher_review_queue:
            upsert_seed(
                user_id=row.get("student_id"),
                name=row.get("student_name") or row.get("student"),
                role="student",
            )

        for value in self._auth_users_from_json_file():
            candidate_id = (
                value.get("student_id")
                or value.get("chat_id")
                or value.get("username")
                or value.get("email")
            )
            upsert_seed(
                user_id=candidate_id,
                name=value.get("name") or value.get("username"),
                role=value.get("role") or "student",
                chat_id=value.get("chat_id"),
                username=value.get("username"),
                email=value.get("email"),
            )

        for value in self._auth_users_from_sqlite_store():
            candidate_id = (
                value.get("student_id")
                or value.get("chat_id")
                or value.get("username")
                or value.get("email")
            )
            upsert_seed(
                user_id=candidate_id,
                name=value.get("name") or value.get("username"),
                role=value.get("role") or "student",
                chat_id=value.get("chat_id"),
                username=value.get("username"),
                email=value.get("email"),
            )

    def _auth_users_from_json_file(self) -> list[dict[str, Any]]:
        try:
            if not self._auth_users_file.exists():
                return []
            text = self._auth_users_file.read_text(encoding="utf-8").strip()
            decoded = json.loads(text) if text else {}
            if not isinstance(decoded, dict):
                return []
            return [dict(value) for value in decoded.values() if isinstance(value, dict)]
        except Exception:
            return []

    def _auth_users_from_sqlite_store(self) -> list[dict[str, Any]]:
        try:
            decoded = self._auth_storage.read_json("auth_users")
            if not isinstance(decoded, dict):
                return []
            out: list[dict[str, Any]] = []
            for key, value in decoded.items():
                if not isinstance(value, dict):
                    continue
                row = dict(value)
                if self._str(row.get("email")).strip() == "" and self._str(key):
                    row["email"] = self._str(key)
                out.append(row)
            return out
        except Exception:
            return []

    def _chat_user_name(self, user_id: str) -> str:
        key = self._normalize_chat_user_id(user_id)
        row = self._chat_users.get(key, {})
        if key == "TEACHER":
            return "Teacher (Direct)"
        return self._str(row.get("name") or row.get("username") or key)

    def _direct_thread_signature(self, participants: list[str]) -> tuple[str, ...] | None:
        ids = [
            self._normalize_chat_user_id(x)
            for x in participants
            if self._normalize_chat_user_id(x)
        ]
        ids = list(dict.fromkeys(ids))
        if len(ids) != 2:
            return None
        return tuple(sorted((x.lower() for x in ids)))

    def _canonical_direct_chat_id(self, participants: list[str]) -> str:
        ids = [
            self._normalize_chat_user_id(x)
            for x in participants
            if self._normalize_chat_user_id(x)
        ]
        ids = list(dict.fromkeys(ids))
        if len(ids) != 2:
            return ""
        ids.sort(key=lambda value: value.lower())
        return f"{ids[0]}|{ids[1]}"

    def _matching_direct_thread_keys(self, participants: list[str]) -> list[str]:
        signature = self._direct_thread_signature(participants)
        if signature is None:
            return []
        keys: list[str] = []
        for key, raw in self._chat_threads.items():
            thread = dict(raw)
            if self._to_bool(thread.get("is_group")):
                continue
            thread_parts = [
                self._normalize_chat_user_id(x)
                for x in (thread.get("participants") or [])
                if self._normalize_chat_user_id(x)
            ]
            if len(thread_parts) < 2:
                thread_parts = [
                    self._normalize_chat_user_id(x)
                    for x in self._str(thread.get("chat_id") or key).split("|")
                    if self._normalize_chat_user_id(x)
                ]
            if self._direct_thread_signature(thread_parts) == signature:
                keys.append(key)
        return keys

    def _merge_message_lists(self, *message_lists: Any) -> list[dict[str, Any]]:
        merged_by_key: dict[str, dict[str, Any]] = {}
        fallback_index = 0
        for raw_list in message_lists:
            rows = raw_list if isinstance(raw_list, list) else []
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                item = dict(raw)
                message_id = self._str(item.get("id"))
                if message_id:
                    existing = merged_by_key.get(message_id)
                    if existing:
                        next_item = dict(existing)
                        next_item.update(item)
                        merged_by_key[message_id] = next_item
                    else:
                        merged_by_key[message_id] = item
                    continue
                anon_key = f"__anon__{fallback_index}"
                fallback_index += 1
                merged_by_key[anon_key] = item

        out = list(merged_by_key.values())
        out.sort(
            key=lambda row: (
                self._to_int(row.get("time"), 0),
                self._str(row.get("id")),
                self._str(row.get("sender")),
            )
        )
        return out[-500:]

    def _participants_from_payload(self, payload: dict[str, Any]) -> list[str]:
        raw = payload.get("participants")
        out: list[str] = []
        if isinstance(raw, list):
            out = [self._normalize_chat_user_id(x) for x in raw]
        else:
            text = self._str(raw)
            if text:
                out = [self._normalize_chat_user_id(x) for x in text.split(",")]
        out = [x for x in out if x]
        sender = self._normalize_chat_user_id(payload.get("user_id"))
        if sender:
            out.append(sender)
        return sorted(list(dict.fromkeys(out)))

    async def _upsert_chat_user(self, payload: dict[str, Any]) -> dict[str, Any]:
        user_id = self._normalize_chat_user_id(
            payload.get("user_id")
            or payload.get("chat_id")
            or payload.get("mobile")
            or payload.get("student_id")
            or payload.get("account_id")
            or payload.get("email")
        )
        if not user_id:
            return {
                "ok": False,
                "status": "MISSING_USER_ID",
                "message": "user_id/chat_id required",
            }
        role = self._str(payload.get("role") or ("teacher" if user_id == "TEACHER" else "student")).lower()
        name = self._str(payload.get("name") or payload.get("username") or user_id)
        if user_id == "TEACHER":
            role = "teacher"
            if name.lower() in {"admin", "administrator", "teacher"}:
                name = "Teacher (Direct)"
        row = {
            "user_id": user_id,
            "chat_id": self._str(payload.get("chat_id") or user_id) or user_id,
            "name": name,
            "username": self._str(payload.get("username") or name),
            "email": self._str(payload.get("email")),
            "role": role or "student",
            "updated_at": self._now_ms(),
        }
        existing = self._chat_users.get(user_id, {})
        if existing:
            row["updated_at"] = self._now_ms()
            row["name"] = self._str(row["name"] or existing.get("name") or user_id)
            row["role"] = self._str(row["role"] or existing.get("role") or "student")
        async with self._lock:
            self._chat_users[user_id] = row
            self._write_map(self._chat_users_file, self._chat_users)
        return {
            "ok": True,
            "status": "SUCCESS",
            "user_id": user_id,
            "chat_id": row["chat_id"],
            "name": row["name"],
            "role": row["role"],
        }

    async def _search_chat_users(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._seed_chat_users_from_local_sources()
        query = self._str(payload.get("q") or payload.get("query"))
        query_norm = self._search_norm(query)
        query_compact = query_norm.replace(" ", "")
        query_tokens = [x for x in query_norm.split(" ") if x]
        requester = self._normalize_chat_user_id(payload.get("user_id"))
        role = self._str(payload.get("role")).lower()
        users = [dict(x) for x in self._chat_users.values()]
        ranked: list[tuple[int, dict[str, Any]]] = []
        for user in users:
            user_id = self._normalize_chat_user_id(user.get("user_id"))
            if not user_id or user_id == requester:
                continue
            user_role = self._str(user.get("role") or "student").lower()
            if role == "teacher" and user_role == "teacher":
                continue
            name = self._str(user.get("name") or user_id)
            username = self._str(user.get("username"))
            email = self._str(user.get("email"))
            id_norm = self._search_norm(user_id)
            name_norm = self._search_norm(name)
            hay = self._search_norm(f"{name} {user_id} {user_role} {username} {email}")
            hay_compact = hay.replace(" ", "")

            score = 1 if not query_norm else 0
            if query_norm:
                if id_norm == query_norm or name_norm == query_norm:
                    score += 120
                if id_norm.startswith(query_norm):
                    score += 95
                if name_norm.startswith(query_norm):
                    score += 85
                if query_compact and query_compact in hay_compact:
                    score += 36
                if query_tokens:
                    hit_count = 0
                    for token in query_tokens:
                        if token in hay:
                            hit_count += 1
                    if hit_count == len(query_tokens):
                        score += 34 + (hit_count * 4)
                    elif hit_count > 0:
                        score += hit_count * 4
                initials = self._search_initials(f"{name} {username}")
                if query_compact and initials.startswith(query_compact):
                    score += 24

            if score <= 0:
                continue
            ranked.append(
                (
                    score,
                    {
                        "user_id": user_id,
                        "chat_id": self._str(user.get("chat_id") or user_id),
                        "name": name,
                        "role": user_role or "student",
                    },
                )
            )

        ranked.sort(
            key=lambda x: (
                -x[0],
                self._str(x[1].get("name")).lower(),
                self._str(x[1].get("user_id")).lower(),
            )
        )
        out = [row for _, row in ranked]
        return {"ok": True, "status": "SUCCESS", "list": out}

    async def _create_chat_group(self, payload: dict[str, Any]) -> dict[str, Any]:
        chat_id = self._safe_chat_id(payload.get("chat_id")) or self._new_id("group")
        participants = self._participants_from_payload(payload)
        creator = self._normalize_chat_user_id(payload.get("creator_id"))
        if creator and creator not in participants:
            participants.append(creator)
        participants = sorted(list(dict.fromkeys([x for x in participants if x])))
        if not participants:
            return {
                "ok": False,
                "status": "MISSING_PARTICIPANTS",
                "message": "participants required",
            }
        group_name = self._str(payload.get("group_name") or "Group chat")
        thread = {
            "chat_id": chat_id,
            "participants": participants,
            "is_group": True,
            "group_name": group_name,
            "messages": [],
            "updated_at": self._now_ms(),
            "read_by": {},
        }
        async with self._lock:
            self._chat_threads[chat_id] = thread
            self._write_map(self._chat_threads_file, self._chat_threads)
        return {"ok": True, "status": "SUCCESS", "chat_id": chat_id, "thread": thread}

    async def _send_peer_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        chat_id = self._safe_chat_id(payload.get("chat_id")) or self._new_id("chat")
        participants = self._participants_from_payload(payload)
        body = payload.get("payload")
        msg = dict(body) if isinstance(body, dict) else {}
        sender = self._normalize_chat_user_id(msg.get("sender") or payload.get("user_id"))
        if sender and sender not in participants:
            participants.append(sender)
        participants = sorted(list(dict.fromkeys([x for x in participants if x])))
        if not participants:
            return {
                "ok": False,
                "status": "MISSING_PARTICIPANTS",
                "message": "participants required",
            }

        now_ms = self._now_ms()
        if not msg:
            msg = {
                "id": self._new_id("msg"),
                "sender": sender,
                "senderName": self._chat_user_name(sender) if sender else "",
                "text": self._str(payload.get("text")),
                "type": self._str(payload.get("type") or "text") or "text",
                "time": now_ms,
            }
        msg["id"] = self._str(msg.get("id") or self._new_id("msg"))
        msg["sender"] = self._normalize_chat_user_id(msg.get("sender") or sender)
        msg["senderName"] = self._str(
            msg.get("senderName") or self._chat_user_name(msg.get("sender"))
        )
        msg["text"] = self._str(msg.get("text"))
        msg["type"] = self._str(msg.get("type") or "text") or "text"
        msg["time"] = self._to_int(msg.get("time"), now_ms)

        candidate_keys: list[str] = []
        direct_signature = self._direct_thread_signature(participants)
        for value in (
            chat_id,
            self._safe_id(chat_id),
            self._canonical_direct_chat_id(participants) if direct_signature else "",
        ):
            key = self._safe_chat_id(value)
            if key and key not in candidate_keys:
                candidate_keys.append(key)
        for key in self._matching_direct_thread_keys(participants):
            safe_key = self._safe_chat_id(key)
            if safe_key and safe_key not in candidate_keys:
                candidate_keys.append(safe_key)

        existing_threads = [
            dict(self._chat_threads.get(key, {}))
            for key in candidate_keys
            if self._chat_threads.get(key)
        ]
        existing_participants: list[str] = []
        messages: list[dict[str, Any]] = []
        read_by: dict[str, int] = {}
        thread_group_name = ""
        existing_is_group = self._to_bool(payload.get("is_group"))
        last_updated = 0

        for thread in existing_threads:
            existing_participants.extend(
                [
                    self._normalize_chat_user_id(x)
                    for x in (thread.get("participants") or [])
                    if self._normalize_chat_user_id(x)
                ]
            )
            messages = self._merge_message_lists(messages, thread.get("messages"))
            for raw_user, raw_time in dict(thread.get("read_by") or {}).items():
                key = self._normalize_chat_user_id(raw_user)
                if not key:
                    continue
                parsed_time = self._to_int(raw_time, 0)
                if parsed_time > self._to_int(read_by.get(key), 0):
                    read_by[key] = parsed_time
            thread_group_name = self._str(thread_group_name or thread.get("group_name"))
            existing_is_group = existing_is_group or self._to_bool(thread.get("is_group"))
            last_updated = max(last_updated, self._to_int(thread.get("updated_at"), 0))

        merged = participants + [x for x in existing_participants if x]
        merged = sorted(list(dict.fromkeys([x for x in merged if x])))
        is_group = existing_is_group or len(merged) > 2
        canonical_chat_id = chat_id
        if not is_group:
            canonical_chat_id = self._canonical_direct_chat_id(merged) or chat_id

        messages = self._merge_message_lists(messages, [msg])
        if msg["sender"]:
            read_by[msg["sender"]] = msg["time"]

        thread = {
            "chat_id": canonical_chat_id,
            "participants": merged,
            "is_group": is_group,
            "group_name": self._str(thread_group_name or payload.get("group_name")),
            "messages": messages,
            "updated_at": max(msg["time"], last_updated),
            "read_by": read_by,
        }

        for participant in merged:
            if participant not in self._chat_users:
                self._chat_users[participant] = {
                    "user_id": participant,
                    "chat_id": participant,
                    "name": self._chat_user_name(participant),
                    "role": "teacher" if participant == "TEACHER" else "student",
                    "updated_at": now_ms,
                }

        async with self._lock:
            for key in candidate_keys:
                if key and key != canonical_chat_id:
                    self._chat_threads.pop(key, None)
            self._chat_threads[canonical_chat_id] = thread
            self._write_map(self._chat_threads_file, self._chat_threads)
            self._write_map(self._chat_users_file, self._chat_users)
        return {
            "ok": True,
            "status": "SUCCESS",
            "chat_id": canonical_chat_id,
            "thread": thread,
        }

    async def _mark_chat_read(self, payload: dict[str, Any]) -> dict[str, Any]:
        chat_id = self._safe_chat_id(payload.get("chat_id"))
        user_id = self._normalize_chat_user_id(payload.get("user_id"))
        if not chat_id or not user_id:
            return {
                "ok": False,
                "status": "MISSING_FIELDS",
                "message": "chat_id and user_id required",
            }
        thread_key = chat_id
        thread = dict(self._chat_threads.get(thread_key, {}))
        if not thread and "|" in chat_id:
            participants = [
                self._normalize_chat_user_id(x)
                for x in chat_id.split("|")
                if self._normalize_chat_user_id(x)
            ]
            canonical = self._canonical_direct_chat_id(participants)
            if canonical:
                candidate = dict(self._chat_threads.get(canonical, {}))
                if candidate:
                    thread = candidate
                    thread_key = canonical
            if not thread:
                for key in self._matching_direct_thread_keys(participants):
                    candidate = dict(self._chat_threads.get(key, {}))
                    if candidate:
                        thread = candidate
                        thread_key = key
                        break
        if not thread:
            return {"ok": True, "status": "SUCCESS"}
        read_by = dict(thread.get("read_by") or {})
        read_by[user_id] = self._now_ms()
        thread["read_by"] = read_by
        async with self._lock:
            self._chat_threads[thread_key] = thread
            self._write_map(self._chat_threads_file, self._chat_threads)
        return {"ok": True, "status": "SUCCESS"}

    async def _list_chat_directory(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._seed_chat_users_from_local_sources()
        user_id = self._normalize_chat_user_id(payload.get("chat_id") or payload.get("user_id"))
        role = self._str(payload.get("role")).lower()
        grouped_threads: dict[str, dict[str, Any]] = {}
        grouped_read_by: dict[str, dict[str, int]] = {}
        grouped_updated_at: dict[str, int] = {}
        grouped_group_name: dict[str, str] = {}
        grouped_is_group: dict[str, bool] = {}

        for raw in self._chat_threads.values():
            thread = dict(raw)
            chat_id = self._str(thread.get("chat_id"))
            if not chat_id:
                continue
            participants = [
                self._normalize_chat_user_id(x)
                for x in (thread.get("participants") or [])
                if self._normalize_chat_user_id(x)
            ]
            is_group = self._to_bool(thread.get("is_group")) or len(participants) > 2
            grouped_key = chat_id if is_group else (self._canonical_direct_chat_id(participants) or chat_id)
            existing = grouped_threads.get(grouped_key, {})
            existing_participants = [
                self._normalize_chat_user_id(x)
                for x in (existing.get("participants") or [])
                if self._normalize_chat_user_id(x)
            ]
            merged_participants = sorted(
                list(
                    dict.fromkeys(
                        [x for x in (participants + existing_participants) if x]
                    )
                )
            )
            grouped_threads[grouped_key] = {
                "chat_id": grouped_key,
                "participants": merged_participants,
                "is_group": grouped_is_group.get(grouped_key, False) or is_group,
                "group_name": self._str(
                    grouped_group_name.get(grouped_key) or thread.get("group_name")
                ),
                "messages": self._merge_message_lists(
                    existing.get("messages"), thread.get("messages")
                ),
            }
            grouped_is_group[grouped_key] = grouped_threads[grouped_key]["is_group"] is True
            grouped_group_name[grouped_key] = self._str(grouped_threads[grouped_key]["group_name"])
            grouped_updated_at[grouped_key] = max(
                grouped_updated_at.get(grouped_key, 0),
                self._to_int(thread.get("updated_at"), 0),
            )
            read_bucket = dict(grouped_read_by.get(grouped_key, {}))
            for raw_reader, raw_time in dict(thread.get("read_by") or {}).items():
                reader = self._normalize_chat_user_id(raw_reader)
                if not reader:
                    continue
                parsed = self._to_int(raw_time, 0)
                if parsed > self._to_int(read_bucket.get(reader), 0):
                    read_bucket[reader] = parsed
            grouped_read_by[grouped_key] = read_bucket

        out: list[dict[str, Any]] = []
        for grouped_key, thread in grouped_threads.items():
            chat_id = self._str(grouped_key)
            if not chat_id:
                continue
            participants = [
                self._normalize_chat_user_id(x)
                for x in (thread.get("participants") or [])
                if self._normalize_chat_user_id(x)
            ]
            if user_id and user_id not in participants and role != "teacher":
                continue
            messages = [dict(x) for x in (thread.get("messages") or []) if isinstance(x, dict)]
            messages.sort(key=lambda x: self._to_int(x.get("time"), 0))
            last = messages[-1] if messages else {}
            read_by = dict(grouped_read_by.get(grouped_key, {}))
            unread = False
            if user_id and last:
                last_time = self._to_int(last.get("time"), 0)
                read_time = self._to_int(read_by.get(user_id), 0)
                last_sender = self._normalize_chat_user_id(last.get("sender"))
                unread = last_time > read_time and last_sender != user_id

            is_group = grouped_is_group.get(grouped_key, False) or len(participants) > 2
            peer_id = ""
            peer_name = ""
            if not is_group and user_id:
                peers = [p for p in participants if p != user_id]
                if peers:
                    peer_id = peers[0]
                    peer_name = self._chat_user_name(peer_id)
            item = {
                "chat_id": chat_id,
                "friend_id": peer_id,
                "friend_name": peer_name,
                "messages": messages[-300:],
                "last_msg": self._str(last.get("text")),
                "time": self._to_int(last.get("time"), grouped_updated_at.get(grouped_key, 0)),
                "unread": unread,
                "participants": participants,
                "is_group": is_group,
                "group_name": self._str(grouped_group_name.get(grouped_key)),
            }
            out.append(item)
        out.sort(key=lambda x: self._to_int(x.get("time"), 0), reverse=True)
        return {"ok": True, "status": "SUCCESS", "list": out}

    async def _raise_doubt(self, payload: dict[str, Any]) -> dict[str, Any]:
        doubt_id = self._safe_id(payload.get("id")) or self._new_id("doubt")
        now_ms = self._now_ms()
        message = self._str(payload.get("message"))
        row = {
            "id": doubt_id,
            "quiz_id": self._str(payload.get("quiz_id")),
            "quiz_title": self._str(payload.get("quiz_title") or "Doubt Thread"),
            "question": self._str(payload.get("question")),
            "student": self._str(payload.get("student") or payload.get("student_name")),
            "student_id": self._normalize_chat_user_id(payload.get("student_id")),
            "status": self._str(payload.get("status") or "open"),
            "messages": [],
            "time": now_ms,
            "unread": True,
        }
        if message:
            row["messages"].append(
                {
                    "id": self._new_id("msg"),
                    "sender": row["student_id"],
                    "senderName": row["student"] or row["student_id"],
                    "text": message,
                    "type": "text",
                    "time": now_ms,
                }
            )
        async with self._lock:
            for i, existing in enumerate(self._doubts):
                if self._str(existing.get("id")) == doubt_id:
                    self._doubts[i] = row
                    break
            else:
                self._doubts.append(row)
            self._write_list(self._doubts_file, self._doubts)
        return {"ok": True, "status": "SUCCESS", "id": doubt_id}

    async def _send_doubt_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        doubt_id = self._safe_id(payload.get("id"))
        body = payload.get("payload")
        msg = dict(body) if isinstance(body, dict) else {}
        if not msg:
            msg = {
                "id": self._new_id("msg"),
                "sender": self._normalize_chat_user_id(payload.get("user_id")),
                "senderName": self._str(payload.get("name")),
                "text": self._str(payload.get("message") or payload.get("text")),
                "type": self._str(payload.get("type") or "text") or "text",
                "time": self._now_ms(),
            }
        msg["id"] = self._str(msg.get("id") or self._new_id("msg"))
        msg["sender"] = self._normalize_chat_user_id(msg.get("sender"))
        msg["senderName"] = self._str(msg.get("senderName") or self._chat_user_name(msg.get("sender")))
        msg["text"] = self._str(msg.get("text"))
        msg["type"] = self._str(msg.get("type") or "text") or "text"
        msg["time"] = self._to_int(msg.get("time"), self._now_ms())

        if not doubt_id:
            return {"ok": True, "status": "SUCCESS"}

        updated = False
        async with self._lock:
            for row in self._doubts:
                if self._str(row.get("id")) != doubt_id:
                    continue
                row["messages"] = self._merge_message_lists(row.get("messages"), [msg])
                row["time"] = msg["time"]
                row["unread"] = True
                updated = True
                break
            if not updated:
                self._doubts.append(
                    {
                        "id": doubt_id,
                        "quiz_id": self._str(payload.get("quiz_id")),
                        "quiz_title": self._str(payload.get("quiz_title") or "Doubt Thread"),
                        "question": self._str(payload.get("question")),
                        "student": self._str(payload.get("student")),
                        "student_id": self._normalize_chat_user_id(payload.get("student_id")),
                        "status": "open",
                        "messages": [msg],
                        "time": msg["time"],
                        "unread": True,
                    }
                )
            self._write_list(self._doubts_file, self._doubts)
        return {"ok": True, "status": "SUCCESS"}

    async def _get_doubts(self, payload: dict[str, Any]) -> dict[str, Any]:
        user_id = self._normalize_chat_user_id(payload.get("user_id"))
        role = self._str(payload.get("role")).lower()
        out = []
        for row in self._doubts:
            item = dict(row)
            if role != "teacher":
                sid = self._normalize_chat_user_id(item.get("student_id"))
                if user_id and sid and sid != user_id:
                    continue
            out.append(item)
        out.sort(key=lambda x: self._to_int(x.get("time"), 0), reverse=True)
        return {"ok": True, "status": "SUCCESS", "list": out}

    async def _update_doubt_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        doubt_id = self._safe_id(payload.get("id"))
        role = self._str(payload.get("role")).lower()
        if not doubt_id:
            return {"ok": False, "status": "MISSING_ID", "message": "id required"}
        next_status = "resolved" if role == "teacher" else "in_progress"
        async with self._lock:
            for row in self._doubts:
                if self._str(row.get("id")) == doubt_id:
                    row["status"] = next_status
                    row["time"] = self._now_ms()
                    break
            self._write_list(self._doubts_file, self._doubts)
        return {"ok": True, "status": "SUCCESS"}

    def _decode_payload_bytes(self, data_value: str) -> tuple[bytes | None, str]:
        raw = data_value.strip()
        if not raw:
            return None, "application/octet-stream"
        if raw.startswith("data:") and "," in raw:
            head, b64 = raw.split(",", 1)
            mime = "application/octet-stream"
            if ";" in head:
                mime = head[5 : head.find(";")] or mime
            try:
                return base64.b64decode(b64), mime
            except Exception:
                return None, mime
        try:
            return base64.b64decode(raw), "application/octet-stream"
        except Exception:
            return None, "application/octet-stream"

    def _safe_filename(self, raw_name: str, fallback_ext: str) -> str:
        base = self._str(raw_name)
        if not base:
            base = f"file_{self._now_ms()}{fallback_ext}"
        base = re.sub(r"[^A-Za-z0-9._-]", "_", base)
        if "." not in base and fallback_ext:
            base = f"{base}{fallback_ext}"
        return base[:160]

    async def _upload_file(self, payload: dict[str, Any]) -> dict[str, Any]:
        file_name = self._str(payload.get("name") or payload.get("file_name") or payload.get("filename"))
        data_value = self._str(payload.get("data") or payload.get("file_data") or payload.get("content"))
        decoded, mime = self._decode_payload_bytes(data_value)
        if decoded is None:
            return {
                "ok": False,
                "status": "INVALID_FILE_DATA",
                "message": "Unable to decode file data",
            }

        guessed_ext = Path(file_name).suffix
        if not guessed_ext:
            guessed_ext = mimetypes.guess_extension(mime) or ".bin"
        safe_name = self._safe_filename(file_name, guessed_ext)
        file_id = self._new_id("file")
        path = self._uploads_dir / f"{file_id}_{safe_name}"
        path.write_bytes(decoded)
        file_url = f"{self._base_url(payload)}/app/file/{file_id}"

        meta = {
            "id": file_id,
            "name": safe_name,
            "mime": mime or "application/octet-stream",
            "path": str(path),
            "size": len(decoded),
            "created_at": self._now_ms(),
        }
        async with self._lock:
            self._uploads[file_id] = meta
            self._write_map(self._uploads_file, self._uploads)

        return {
            "ok": True,
            "status": "SUCCESS",
            "message": "Uploaded",
            "id": file_id,
            "url": file_url,
            "file_url": file_url,
            "link": file_url,
        }
