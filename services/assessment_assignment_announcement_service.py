from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.storage.sqlite_json_store import SQLiteJsonBlobStore
from services.atlas_incident_email_service import AtlasIncidentEmailService


class AssessmentAssignmentAnnouncementService:
    """Sends durable assignment announcement emails for exams and homework."""

    _STATE_KEY = "app_assignment_announcement_state"

    def __init__(
        self,
        *,
        email_service: AtlasIncidentEmailService | None = None,
        assessments_file: str | Path | None = None,
        results_file: str | Path | None = None,
        auth_users_file: str | Path | None = None,
        auth_storage_db_file: str | Path | None = None,
        app_storage_db_file: str | Path | None = None,
    ) -> None:
        root = Path(__file__).resolve().parents[1]
        app_dir = root / "data" / "app"
        auth_dir = root / "data" / "auth"
        self._email = email_service or AtlasIncidentEmailService()
        self._assessments_file = (
            Path(assessments_file) if assessments_file else app_dir / "assessments.json"
        )
        self._results_file = (
            Path(results_file) if results_file else app_dir / "results.json"
        )
        self._auth_users_file = (
            Path(auth_users_file) if auth_users_file else auth_dir / "users.json"
        )
        self._auth_storage = SQLiteJsonBlobStore(
            Path(auth_storage_db_file)
            if auth_storage_db_file
            else auth_dir / "auth_store.sqlite3"
        )
        self._app_storage = SQLiteJsonBlobStore(
            Path(app_storage_db_file)
            if app_storage_db_file
            else app_dir / "app_data.sqlite3"
        )

    def enabled(self) -> bool:
        raw = os.getenv("ATLAS_ASSIGNMENT_ANNOUNCEMENT_ENABLED")
        if raw is None:
            return True
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def notify_assessment_assigned(self, assessment: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled():
            return {"ok": True, "status": "DISABLED", "sent_count": 0}
        if not self._is_assignable_assessment(assessment):
            return {"ok": True, "status": "SKIPPED", "sent_count": 0}
        recipients = self._student_recipients()
        return self._send_to_recipients(assessment=assessment, recipients=recipients)

    def notify_pending_assessments_for_email(self, email: str) -> dict[str, Any]:
        if not self.enabled():
            return {"ok": True, "status": "DISABLED", "sent_count": 0}
        normalized = self._normalize_email(email)
        if not self._looks_like_deliverable_email(normalized):
            return {
                "ok": False,
                "status": "INVALID_EMAIL",
                "sent_count": 0,
                "message": "No deliverable student email was available",
            }
        assessments = self._all_assessments()
        total_sent = 0
        details: list[dict[str, Any]] = []
        all_ok = True
        for assessment in assessments:
            if not self._is_pending_assessment_for_email(assessment, normalized):
                continue
            result = self._send_to_recipients(
                assessment=assessment,
                recipients=[normalized],
            )
            total_sent += int(result.get("sent_count") or 0)
            details.append(result)
            all_ok = all_ok and bool(result.get("ok", False))
        return {
            "ok": all_ok,
            "status": "SUCCESS" if all_ok else "PARTIAL_FAILURE",
            "sent_count": total_sent,
            "results": details,
        }

    def _send_to_recipients(
        self,
        *,
        assessment: dict[str, Any],
        recipients: list[str],
    ) -> dict[str, Any]:
        state = self._read_state()
        assessment_id = self._assessment_id(assessment)
        if not assessment_id:
            return {
                "ok": False,
                "status": "MISSING_ASSESSMENT_ID",
                "sent_count": 0,
            }
        sent_map = self._sent_map_for_assessment(state, assessment_id)
        sent_count = 0
        failed: list[str] = []
        recipient_list: list[str] = []
        for raw_email in recipients:
            email = self._normalize_email(raw_email)
            if not self._looks_like_deliverable_email(email):
                continue
            if email in recipient_list:
                continue
            recipient_list.append(email)
            if str(sent_map.get(email) or "").strip():
                continue
            report = self._build_assignment_report(assessment=assessment, email=email)
            result = self._email.send_assignment_announcement(
                report=report,
                recipient=email,
            )
            if bool(result.get("ok")):
                sent_map[email] = datetime.now(timezone.utc).isoformat()
                sent_count += 1
            else:
                failed.append(email)
        self._write_state(state)
        return {
            "ok": not failed,
            "status": "SUCCESS" if not failed else "PARTIAL_FAILURE",
            "assessment_id": assessment_id,
            "recipients": recipient_list,
            "sent_count": sent_count,
            "failed_recipients": failed,
        }

    def _build_assignment_report(
        self,
        *,
        assessment: dict[str, Any],
        email: str,
    ) -> dict[str, Any]:
        question_count = self._to_int(assessment.get("question_count"), 0)
        metadata = (
            dict(assessment.get("metadata"))
            if isinstance(assessment.get("metadata"), dict)
            else {}
        )
        total_marks = self._to_int(
            metadata.get("total_marks")
            or metadata.get("max_marks")
            or metadata.get("totalMarks"),
            question_count * 4 if question_count > 0 else 0,
        )
        return {
            "report_type": "assignment_announcement",
            "assessment_id": self._assessment_id(assessment),
            "assessment_title": self._string(
                assessment.get("title") or assessment.get("quiz_title") or "Assessment"
            ),
            "assessment_type": self._string(
                assessment.get("type") or "Assessment"
            ),
            "class_name": self._string(
                assessment.get("class")
                or assessment.get("class_name")
                or metadata.get("class_name")
            ),
            "subject": self._string(
                assessment.get("subject")
                or metadata.get("subject")
                or assessment.get("chapters")
            ),
            "chapters": self._string(
                assessment.get("chapters") or metadata.get("chapters")
            ),
            "start_at": self._string(
                assessment.get("start_at")
                or assessment.get("start_time")
                or metadata.get("start_at")
                or metadata.get("scheduled_at")
            ),
            "deadline": self._string(assessment.get("deadline") or assessment.get("date")),
            "duration_minutes": self._to_int(
                assessment.get("duration") or assessment.get("duration_minutes"),
                0,
            ),
            "question_count": question_count,
            "total_marks": total_marks,
            "quiz_url": self._string(
                assessment.get("quiz_url") or assessment.get("url")
            ),
            "recipient_email": email,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _all_assessments(self) -> list[dict[str, Any]]:
        from_db = self._app_storage.read_json("app_assessments")
        if isinstance(from_db, list):
            return [dict(row) for row in from_db if isinstance(row, dict)]
        try:
            if not self._assessments_file.exists():
                return []
            raw = self._assessments_file.read_text(encoding="utf-8").strip()
            decoded = json.loads(raw) if raw else []
            if not isinstance(decoded, list):
                return []
            return [dict(row) for row in decoded if isinstance(row, dict)]
        except Exception:
            return []

    def _all_results(self) -> list[dict[str, Any]]:
        from_db = self._app_storage.read_json("app_results")
        if isinstance(from_db, list):
            return [dict(row) for row in from_db if isinstance(row, dict)]
        try:
            if not self._results_file.exists():
                return []
            raw = self._results_file.read_text(encoding="utf-8").strip()
            decoded = json.loads(raw) if raw else []
            if not isinstance(decoded, list):
                return []
            return [dict(row) for row in decoded if isinstance(row, dict)]
        except Exception:
            return []

    def _student_recipients(self) -> list[str]:
        recipients: list[str] = []
        seen: set[str] = set()
        for user in self._auth_users_from_json_file() + self._auth_users_from_sqlite_store():
            email = self._normalize_email(user.get("email"))
            if not self._looks_like_deliverable_email(email):
                continue
            role = self._string(user.get("role")).lower()
            if role in {"teacher", "admin", "administrator"}:
                continue
            if email in seen:
                continue
            seen.add(email)
            recipients.append(email)
        return recipients

    def _auth_users_from_json_file(self) -> list[dict[str, Any]]:
        try:
            if not self._auth_users_file.exists():
                return []
            raw = self._auth_users_file.read_text(encoding="utf-8").strip()
            decoded = json.loads(raw) if raw else {}
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
            rows: list[dict[str, Any]] = []
            for key, value in decoded.items():
                if not isinstance(value, dict):
                    continue
                row = dict(value)
                if self._string(row.get("email")) == "":
                    row["email"] = self._string(key)
                rows.append(row)
            return rows
        except Exception:
            return []

    def _iter_auth_users(self) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for row in self._auth_users_from_json_file() + self._auth_users_from_sqlite_store():
            email = self._normalize_email(row.get("email"))
            if not email:
                continue
            merged_row = dict(merged.get(email) or {})
            merged_row.update(dict(row))
            merged_row["email"] = email
            merged[email] = merged_row
        return list(merged.values())

    def _read_state(self) -> dict[str, Any]:
        decoded = self._app_storage.read_json(self._STATE_KEY)
        if not isinstance(decoded, dict):
            return {"sent": {}}
        sent = decoded.get("sent")
        if not isinstance(sent, dict):
            decoded["sent"] = {}
        return decoded

    def _write_state(self, state: dict[str, Any]) -> None:
        self._app_storage.write_json(self._STATE_KEY, state)

    def _sent_map_for_assessment(
        self,
        state: dict[str, Any],
        assessment_id: str,
    ) -> dict[str, str]:
        sent = state.setdefault("sent", {})
        row = sent.get(assessment_id)
        if not isinstance(row, dict):
            row = {}
            sent[assessment_id] = row
        return row

    def _is_assignable_assessment(self, assessment: dict[str, Any]) -> bool:
        kind = self._string(assessment.get("type")).lower()
        return "exam" in kind or "homework" in kind

    def _is_pending_assessment_for_email(
        self,
        assessment: dict[str, Any],
        email: str,
    ) -> bool:
        if not self._is_assignable_assessment(assessment):
            return False
        if self._assessment_is_hidden_or_inactive(assessment):
            return False
        if self._assessment_is_expired(assessment):
            return False
        if self._assessment_has_submission_for_email(assessment, email):
            return False
        return True

    def _assessment_is_hidden_or_inactive(self, assessment: dict[str, Any]) -> bool:
        metadata = (
            dict(assessment.get("metadata"))
            if isinstance(assessment.get("metadata"), dict)
            else {}
        )
        for raw in (
            assessment.get("archived"),
            assessment.get("is_archived"),
            assessment.get("deleted"),
            assessment.get("is_deleted"),
            assessment.get("disabled"),
            metadata.get("archived"),
            metadata.get("deleted"),
            metadata.get("disabled"),
        ):
            if self._to_bool(raw):
                return True
        published = assessment.get("published")
        if published is not None and not self._to_bool(published):
            return True
        status = self._string(
            assessment.get("status") or metadata.get("status")
        ).lower()
        return status in {
            "archived",
            "cancelled",
            "canceled",
            "deleted",
            "disabled",
            "draft",
            "inactive",
        }

    def _assessment_is_expired(self, assessment: dict[str, Any]) -> bool:
        deadline = self._parse_dt(assessment.get("deadline") or assessment.get("date"))
        if deadline is None:
            return False
        return deadline < datetime.now(timezone.utc)

    def _assessment_has_submission_for_email(
        self,
        assessment: dict[str, Any],
        email: str,
    ) -> bool:
        assessment_id = self._assessment_id(assessment)
        if not assessment_id:
            return False
        identity_tokens = self._identity_tokens_for_email(email)
        if not identity_tokens:
            return False
        for result in self._all_results():
            if self._result_assessment_id(result) != assessment_id:
                continue
            if self._result_matches_identity_tokens(result, identity_tokens):
                return True
        return False

    def _assessment_id(self, assessment: dict[str, Any]) -> str:
        return self._string(assessment.get("id") or assessment.get("quiz_id"))

    def _normalize_email(self, value: Any) -> str:
        return self._string(value).lower()

    def _looks_like_deliverable_email(self, value: str) -> bool:
        if "@" not in value:
            return False
        domain = value.rsplit("@", 1)[-1]
        if "." not in domain:
            return False
        if domain in {
            "example.com",
            "example.org",
            "example.net",
            "localhost",
            "invalid",
            "test",
        }:
            return False
        if domain.endswith(".invalid") or domain.endswith(".test"):
            return False
        return True

    def _string(self, value: Any) -> str:
        return str(value or "").strip()

    def _to_int(self, value: Any, fallback: int = 0) -> int:
        try:
            return int(float(str(value).strip()))
        except Exception:
            return fallback

    def _to_bool(self, value: Any) -> bool:
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    def _parse_dt(self, value: Any) -> datetime | None:
        raw = self._string(value)
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            try:
                parsed = datetime.strptime(raw, "%Y-%m-%d")
            except Exception:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _identity_tokens_for_email(self, email: str) -> set[str]:
        normalized_email = self._normalize_email(email)
        tokens: set[str] = set()
        if normalized_email:
            tokens.add(normalized_email)
        for user in self._iter_auth_users():
            if self._normalize_email(user.get("email")) != normalized_email:
                continue
            for key in (
                "account_id",
                "accountId",
                "student_id",
                "studentId",
                "user_id",
                "userId",
                "username",
                "roll_number",
                "rollNumber",
            ):
                value = self._string(user.get(key)).lower()
                if value:
                    tokens.add(value)
        return tokens

    def _result_assessment_id(self, row: dict[str, Any]) -> str:
        return self._string(
            row.get("quiz_id")
            or row.get("quizId")
            or row.get("assessment_id")
            or row.get("assessmentId")
            or row.get("id")
        )

    def _result_matches_identity_tokens(
        self,
        row: dict[str, Any],
        identity_tokens: set[str],
    ) -> bool:
        if not identity_tokens:
            return False
        for key in (
            "email",
            "student_email",
            "studentEmail",
            "account_id",
            "accountId",
            "student_id",
            "studentId",
            "user_id",
            "userId",
            "username",
            "roll_number",
            "rollNumber",
        ):
            value = self._string(row.get(key)).lower()
            if value and value in identity_tokens:
                return True
        return False
