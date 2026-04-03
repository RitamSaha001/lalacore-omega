from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import tempfile
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth.local_auth_service import LocalAuthService
from app.data.local_app_data_service import LocalAppDataService
from core.automation.state_manager import AutomationStateManager


class AtlasMaintenanceService:
    """Runs scheduled Atlas maintenance sweeps with support email reporting."""

    CHECKPOINT_SCOPE = "atlas_weekly_maintenance"

    def __init__(
        self,
        *,
        app_data: LocalAppDataService | None = None,
        state: AutomationStateManager | None = None,
        auditor: "_AtlasPipelineMaintenanceAuditor | None" = None,
    ) -> None:
        self._app_data = app_data or LocalAppDataService()
        self._state = state or AutomationStateManager()
        self._lock = asyncio.Lock()
        self._auditor = auditor or _AtlasPipelineMaintenanceAuditor()

    def timezone_name(self) -> str:
        return (
            os.getenv("ATLAS_MAINTENANCE_TZ", "").strip()
            or os.getenv("APP_TIMEZONE", "").strip()
            or os.getenv("TZ", "").strip()
            or "Asia/Kolkata"
        )

    def _zoneinfo(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.timezone_name())
        except Exception:
            return ZoneInfo("Asia/Kolkata")

    def _window_key(self, now: datetime) -> str:
        local_now = now.astimezone(self._zoneinfo())
        return local_now.strftime("%G-W%V")

    def _is_window_open(self, now: datetime) -> bool:
        local_now = now.astimezone(self._zoneinfo())
        return local_now.weekday() == 5 and local_now.hour == 1

    def _pid_is_alive(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def is_running(self) -> bool:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        if not bool(checkpoint.get("running")):
            return False
        try:
            lock_pid = int(checkpoint.get("lock_pid") or 0)
        except Exception:
            lock_pid = 0
        if lock_pid > 0 and self._pid_is_alive(lock_pid):
            return True
        self._state.checkpoint(
            self.CHECKPOINT_SCOPE,
            running=False,
            phase="idle",
            last_error="stale_maintenance_lock_cleared",
            lock_pid=0,
        )
        return False

    def status_snapshot(self) -> dict[str, Any]:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        expected = self._auditor.expected_duration_seconds()
        last_duration = checkpoint.get("last_duration_s")
        try:
            last_duration_s = float(last_duration)
        except Exception:
            last_duration_s = 0.0
        estimate_s = int(round(last_duration_s)) if last_duration_s > 0 else expected
        return {
            "running": bool(checkpoint.get("running")),
            "current_window": str(checkpoint.get("current_window") or ""),
            "timezone": self.timezone_name(),
            "phase": str(checkpoint.get("phase") or ""),
            "last_start_ts": checkpoint.get("last_start_ts"),
            "last_end_ts": checkpoint.get("last_end_ts"),
            "last_status": str(checkpoint.get("last_status") or ""),
            "last_duration_s": last_duration_s,
            "last_error": str(checkpoint.get("last_error") or ""),
            "lock_pid": int(checkpoint.get("lock_pid") or 0),
            "estimated_duration_s": estimate_s,
            "estimated_duration_minutes": round(estimate_s / 60.0, 1),
        }

    async def run_if_due(self, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        window_open = self._is_window_open(now)
        window_key = self._window_key(now)
        self._state.checkpoint(
            self.CHECKPOINT_SCOPE,
            last_tick_ts=now.isoformat(),
            timezone=self.timezone_name(),
            window_open=window_open,
            current_window=window_key,
        )
        if not window_open:
            return {
                "ok": True,
                "skipped": True,
                "reason": "outside_window",
                "window": window_key,
                "timezone": self.timezone_name(),
                "ts": now.isoformat(),
            }
        if str(checkpoint.get("last_completed_window") or "").strip() == window_key:
            return {
                "ok": True,
                "skipped": True,
                "reason": "already_ran_this_window",
                "window": window_key,
                "timezone": self.timezone_name(),
                "ts": now.isoformat(),
            }
        return await self.run_weekly_maintenance(trigger="scheduled", now=now)

    async def run_weekly_maintenance(
        self,
        *,
        trigger: str = "manual",
        now: datetime | None = None,
        recipient_email: str | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        async with self._lock:
            started = time.perf_counter()
            local_now = now.astimezone(self._zoneinfo())
            window_key = self._window_key(now)
            self._state.checkpoint(
                self.CHECKPOINT_SCOPE,
                running=True,
                last_start_ts=now.isoformat(),
                trigger=trigger,
                current_window=window_key,
                timezone=self.timezone_name(),
                phase="auditing",
                last_error="",
                lock_pid=os.getpid(),
                estimated_duration_s=self._auditor.expected_duration_seconds(),
            )
            audit: dict[str, Any] = {}
            report: dict[str, Any] = {}
            failing_areas: list[str] = []
            error_text = ""
            try:
                audit = await self._auditor.run()
                failing_areas = [
                    str(item)
                    for item in (audit.get("failing_areas") or [])
                    if str(item).strip()
                ]
                self._state.checkpoint(
                    self.CHECKPOINT_SCOPE,
                    phase="emailing",
                    last_audit_status=str(audit.get("overall_status") or ""),
                    last_audit_failures=failing_areas,
                )
                report = await self._app_data.handle_action(
                    {
                        "action": "report_system_issue",
                        "issue": self._maintenance_issue_text(
                            audit,
                            failing_areas=failing_areas,
                        ),
                        "role": "system",
                        "auto_email": True,
                        "recipient_email": (
                            recipient_email
                            or os.getenv("ATLAS_SUPPORT_EMAIL_RECIPIENT", "").strip()
                        ),
                        "context": {
                            "surface": "atlas_weekly_maintenance",
                            "maintenance_mode": True,
                            "trigger": trigger,
                            "scheduled_window_local": (
                                f"{local_now.strftime('%A')} 01:00-02:00 {self.timezone_name()}"
                            ),
                            "maintenance_scope": [
                                "auth",
                                "dashboard",
                                "study",
                                "ai",
                                "chat",
                                "live_classes",
                                "notifications",
                                "storage",
                            ],
                            "maintenance_audit": audit,
                            "maintenance_artifacts_isolated": True,
                            "maintenance_failing_areas": failing_areas,
                        },
                    }
                )
                return {
                    "ok": bool(report.get("ok")),
                    "trigger": trigger,
                    "window": window_key,
                    "timezone": self.timezone_name(),
                    "audit": audit,
                    "maintenance_report": report,
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                finished = datetime.now(timezone.utc)
                duration_s = round(time.perf_counter() - started, 2)
                checkpoint_update: dict[str, Any] = {
                    "running": False,
                    "phase": "idle",
                    "last_end_ts": finished.isoformat(),
                    "last_duration_s": duration_s,
                    "last_completed_window": window_key,
                    "last_audit_status": str(audit.get("overall_status") or ""),
                    "last_audit_failures": failing_areas,
                    "last_error": error_text,
                    "lock_pid": 0,
                }
                if report:
                    checkpoint_update.update(
                        {
                            "last_status": str(report.get("status") or ""),
                            "last_mail_sent": bool(report.get("mail_sent")),
                            "last_self_heal_count": int(
                                ((report.get("self_heal") or {}).get("applied_fix_count") or 0)
                            ),
                            "last_incident_id": str(report.get("incident_id") or ""),
                        }
                    )
                elif error_text:
                    checkpoint_update["last_status"] = "FAILED"
                self._state.checkpoint(self.CHECKPOINT_SCOPE, **checkpoint_update)

    def _maintenance_issue_text(
        self,
        audit: dict[str, Any],
        *,
        failing_areas: list[str],
    ) -> str:
        if failing_areas:
            return (
                "Scheduled weekly Atlas maintenance sweep found degraded or failing pipelines in: "
                + ", ".join(failing_areas[:8])
                + ". Atlas should analyze the full audit deeply, apply only safe low-risk fixes, "
                "and email the detailed engineering report."
            )
        return (
            "Scheduled weekly Atlas maintenance sweep completed across auth, dashboards, study, AI, "
            "chat, and live classes. Atlas should still analyze the audit deeply, confirm no latent "
            "issues remain, apply only safe low-risk fixes if useful, and email the full report."
        )


class _AtlasPipelineMaintenanceAuditor:
    def __init__(self) -> None:
        self._iterations = max(
            2,
            min(3, int(os.getenv("ATLAS_MAINTENANCE_AUDIT_ITERATIONS", "3"))),
        )
        self._ai_iterations = max(
            2,
            min(self._iterations, int(os.getenv("ATLAS_MAINTENANCE_AI_ITERATIONS", "3"))),
        )
        self._email_iterations = max(
            1,
            min(2, int(os.getenv("ATLAS_MAINTENANCE_EMAIL_ITERATIONS", "1"))),
        )
        self._request_timeout_seconds = max(
            10.0,
            float(os.getenv("ATLAS_MAINTENANCE_REQUEST_TIMEOUT_SECONDS", "40")),
        )
        self._heavy_ai_warmed = False

    def expected_duration_seconds(self) -> int:
        expected = (
            self._iterations * 3
            + self._email_iterations * 20
            + self._iterations * 4
            + self._ai_iterations * 45
            + self._ai_iterations * 40
            + self._ai_iterations * 35
            + self._ai_iterations * 45
            + self._ai_iterations * 20
            + self._iterations * 4
            + self._ai_iterations * 45
            + self._iterations * 3
            + self._iterations * 3
            + self._iterations * 3
            + self._iterations * 5
            + self._ai_iterations * 45
            + self._ai_iterations * 30
            + self._ai_iterations * 35
            + self._ai_iterations * 45
            + self._ai_iterations * 20
            + self._iterations * 3
            + 30
        )
        return int(expected)

    async def run(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._run_sync)

    def _run_sync(self) -> dict[str, Any]:
        app_routes, live_classes_api = self._runtime_modules()
        started = time.perf_counter()
        with tempfile.TemporaryDirectory(prefix="atlas_maintenance_") as tmp:
            root = Path(tmp)
            app_data = LocalAppDataService(
                assessments_file=root / "assessments.json",
                materials_file=root / "materials.json",
                live_class_schedule_file=root / "live_class_schedule.json",
                uploads_file=root / "uploads.json",
                ai_quizzes_file=root / "ai_quizzes.json",
                results_file=root / "results.json",
                teacher_review_file=root / "teacher_review.json",
                import_drafts_file=root / "import_drafts.json",
                import_question_bank_file=root / "import_question_bank.json",
                storage_db_file=root / "app_data.sqlite3",
            )
            auth = LocalAuthService(
                users_file=root / "users.json",
                otp_file=root / "otp.json",
                storage_db_file=root / "auth_store.sqlite3",
            )
            with self._isolated_backend(
                app_data=app_data,
                auth=auth,
                app_routes=app_routes,
                live_classes_api=live_classes_api,
            ):
                self._prewarm_heavy_ai_stack()
                test_app = FastAPI()
                test_app.include_router(app_routes.router)
                test_app.include_router(live_classes_api.router)
                with TestClient(test_app) as client:
                    areas = {
                        "auth_login": self._run_probe_series(
                            "auth_login",
                            self._iterations,
                            lambda idx, adaptive: self._probe_auth_login(client, idx, adaptive=adaptive),
                        ),
                        "auth_forgot_password": self._run_probe_series(
                            "auth_forgot_password",
                            self._email_iterations,
                            lambda idx, adaptive: self._probe_auth_forgot_password(
                                client,
                                auth,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "create_quiz_pipeline": self._run_probe_series(
                            "create_quiz_pipeline",
                            self._iterations,
                            lambda idx, adaptive: self._probe_create_quiz_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "ai_quiz_pipeline": self._run_probe_series(
                            "ai_quiz_pipeline",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_ai_quiz_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "study_material_pipeline": self._run_probe_series(
                            "study_material_pipeline",
                            self._iterations,
                            lambda idx, adaptive: self._probe_study_material_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "material_ai_pipeline": self._run_probe_series(
                            "material_ai_pipeline",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_material_ai_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "material_ai_query_pipeline": self._run_probe_series(
                            "material_ai_query_pipeline",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_material_ai_query_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "teacher_authoring_ai_pipeline": self._run_probe_series(
                            "teacher_authoring_ai_pipeline",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_teacher_authoring_ai_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "app_ai_chat": self._run_probe_series(
                            "app_ai_chat",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_app_ai_chat(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "app_atlas_planner": self._run_probe_series(
                            "app_atlas_planner",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_app_atlas_planner(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "chat_pipeline": self._run_probe_series(
                            "chat_pipeline",
                            self._iterations,
                            lambda idx, adaptive: self._probe_chat_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "schedule_pipeline": self._run_probe_series(
                            "schedule_pipeline",
                            self._iterations,
                            lambda idx, adaptive: self._probe_schedule_pipeline(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "question_search_ai": self._run_probe_series(
                            "question_search_ai",
                            self._iterations,
                            lambda idx, adaptive: self._probe_question_search_ai(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "attachment_import_ai": self._run_probe_series(
                            "attachment_import_ai",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_attachment_import_ai(
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "live_class_core": self._run_probe_series(
                            "live_class_core",
                            self._iterations,
                            lambda idx, adaptive: self._probe_live_class_core(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "live_class_ai": self._run_probe_series(
                            "live_class_ai",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_live_class_ai(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "live_class_support_ai": self._run_probe_series(
                            "live_class_support_ai",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_live_class_support_ai(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "live_attachment_poll_ai": self._run_probe_series(
                            "live_attachment_poll_ai",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_live_attachment_poll_ai(
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "live_class_agent_planner": self._run_probe_series(
                            "live_class_agent_planner",
                            self._ai_iterations,
                            lambda idx, adaptive: self._probe_live_class_agent_planner(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                        "live_class_transcription": self._run_probe_series(
                            "live_class_transcription",
                            self._iterations,
                            lambda idx, adaptive: self._probe_live_class_transcription(
                                client,
                                idx,
                                adaptive=adaptive,
                            ),
                        ),
                    }

        failing_areas = [
            name
            for name, summary in areas.items()
            if str(summary.get("status")) not in {"healthy"}
        ]
        degraded_areas = [
            name
            for name, summary in areas.items()
            if str(summary.get("status")) == "degraded"
        ]
        failure_signatures = {
            name: self._area_failure_signatures(name, summary)
            for name, summary in areas.items()
        }
        return {
            "iterations_default": self._iterations,
            "iterations_ai": self._ai_iterations,
            "iterations_email": self._email_iterations,
            "artifacts_isolated": True,
            "isolation_mode": "temporary_backend_sandbox",
            "areas": areas,
            "failure_signatures": failure_signatures,
            "failing_areas": failing_areas,
            "degraded_areas": degraded_areas,
            "overall_status": (
                "failing"
                if failing_areas
                else "degraded"
                if degraded_areas
                else "healthy"
            ),
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }

    def _runtime_modules(self):
        import app.live_classes_api as live_classes_api
        import app.routes as app_routes

        return app_routes, live_classes_api

    @contextlib.contextmanager
    def _isolated_backend(
        self,
        *,
        app_data: LocalAppDataService,
        auth: LocalAuthService,
        app_routes,
        live_classes_api,
    ):
        previous_app_data = app_routes._APP_DATA
        previous_auth = app_routes._AUTH
        previous_live_hub = live_classes_api._LIVE_HUB
        previous_maintenance_enabled = os.environ.get("ATLAS_MAINTENANCE_ENABLED")
        os.environ["ATLAS_MAINTENANCE_ENABLED"] = "false"
        app_routes._APP_DATA = app_data
        app_routes._AUTH = auth
        live_classes_api._LIVE_HUB = live_classes_api.LiveClassHub()
        try:
            yield
        finally:
            app_routes._APP_DATA = previous_app_data
            app_routes._AUTH = previous_auth
            live_classes_api._LIVE_HUB = previous_live_hub
            if previous_maintenance_enabled is None:
                os.environ.pop("ATLAS_MAINTENANCE_ENABLED", None)
            else:
                os.environ["ATLAS_MAINTENANCE_ENABLED"] = previous_maintenance_enabled

    def _run_probe_series(self, name: str, iterations: int, runner) -> dict[str, Any]:
        runs: list[dict[str, Any]] = []
        adaptive = False
        timeout_budget = self._probe_timeout_seconds(name)
        for idx in range(iterations):
            started = time.perf_counter()
            outcome_box: dict[str, Any] = {}
            error_box: dict[str, str] = {}

            def _target() -> None:
                try:
                    outcome_box["value"] = runner(idx, adaptive)
                except Exception as exc:
                    error_box["value"] = f"{type(exc).__name__}: {exc}"

            worker = threading.Thread(target=_target, daemon=True)
            worker.start()
            worker.join(timeout_budget)
            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            if worker.is_alive():
                runs.append(
                    {
                        "iteration": idx + 1,
                        "ok": False,
                        "degraded": True,
                        "latency_ms": latency_ms,
                        "timed_out": True,
                        "error": (
                            f"Probe iteration exceeded timeout budget "
                            f"({timeout_budget}s)"
                        ),
                    }
                )
                adaptive = True
                continue
            if "value" in error_box:
                runs.append(
                    {
                        "iteration": idx + 1,
                        "ok": False,
                        "degraded": False,
                        "latency_ms": latency_ms,
                        "error": error_box["value"],
                    }
                )
                adaptive = True
                continue
            outcome = dict(outcome_box.get("value") or {})
            ok = bool(outcome.get("ok"))
            degraded = bool(outcome.get("degraded"))
            runs.append(
                {
                    "iteration": idx + 1,
                    "ok": ok,
                    "degraded": degraded,
                    "latency_ms": latency_ms,
                    **outcome,
                }
            )
            adaptive = adaptive or (not ok) or degraded
        success_count = sum(1 for row in runs if row.get("ok"))
        degraded_count = sum(1 for row in runs if row.get("degraded"))
        avg_latency = round(
            sum(float(row.get("latency_ms") or 0.0) for row in runs) / max(1, len(runs)),
            2,
        )
        if success_count == len(runs) and degraded_count == 0:
            status = "healthy"
        elif success_count > 0:
            status = "degraded"
        else:
            status = "failing"
        return {
            "name": name,
            "iterations": len(runs),
            "success_count": success_count,
            "degraded_count": degraded_count,
            "avg_latency_ms": avg_latency,
            "status": status,
            "runs": runs,
        }

    def _probe_timeout_seconds(self, name: str) -> float:
        if name in {"live_class_ai"}:
            return max(self._request_timeout_seconds, 100.0)
        if name in {
            "app_ai_chat",
            "app_atlas_planner",
            "live_class_agent_planner",
            "teacher_authoring_ai_pipeline",
            "attachment_import_ai",
            "live_class_support_ai",
            "live_attachment_poll_ai",
        }:
            return max(self._request_timeout_seconds, 55.0)
        if name in {"material_ai_query_pipeline"}:
            return max(self._request_timeout_seconds, 45.0)
        return self._request_timeout_seconds

    def _prewarm_heavy_ai_stack(self) -> None:
        if self._heavy_ai_warmed:
            return
        try:
            from core.api.entrypoint import warm_atlas_runtime
            from services.question_search_engine import QuestionSearchEngine

            asyncio.run(warm_atlas_runtime())
            asyncio.run(QuestionSearchEngine().warm())
        except Exception:
            return
        self._heavy_ai_warmed = True

    def _decode_jsonish_map(self, *candidates: Any) -> dict[str, Any]:
        for raw in candidates:
            text = str(raw or "").strip()
            if not text:
                continue
            parsed = None
            try:
                parsed = json.loads(text)
            except Exception:
                start = text.find("{")
                end = text.rfind("}")
                if start >= 0 and end > start:
                    try:
                        parsed = json.loads(text[start : end + 1])
                    except Exception:
                        parsed = None
                if parsed is None:
                    list_start = text.find("[")
                    list_end = text.rfind("]")
                    if list_start >= 0 and list_end > list_start:
                        try:
                            parsed = {"items": json.loads(text[list_start : list_end + 1])}
                        except Exception:
                            parsed = None
            if isinstance(parsed, dict):
                return parsed
        return {}

    def _maintenance_attachment_image_data_url(
        self,
        *,
        lines: list[str],
    ) -> str:
        import base64
        import io

        from PIL import Image, ImageDraw, ImageFont

        width = 1400
        height = max(520, 140 + len(lines) * 88)
        image = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(image)
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 42)
        except Exception:
            font = ImageFont.load_default()
        y = 48
        for line in lines:
            draw.text((44, y), line, fill="black", font=font)
            y += 74
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
        return f"data:image/png;base64,{encoded}"

    def _attachment_ocr_payload(self, result: dict[str, Any]) -> dict[str, Any]:
        ocr = result.get("ocr_data")
        return dict(ocr) if isinstance(ocr, dict) else {}

    def _attachment_question_candidates(self, result: dict[str, Any]) -> list[str]:
        ocr = self._attachment_ocr_payload(result)
        out: list[str] = []
        for key in ("lc_iie_questions", "lc_iie_output"):
            rows = ocr.get(key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                statement = str(
                    row.get("statement")
                    or row.get("question_text")
                    or row.get("question")
                    or ""
                ).strip()
                if statement and statement not in out:
                    out.append(statement)
        return out

    def _attachment_options_from_ocr_text(self, raw_text: str) -> list[str]:
        options: list[str] = []
        for raw_line in str(raw_text or "").splitlines():
            line = " ".join(raw_line.strip().split())
            if not line:
                continue
            if re.match(r"^@\s+", line):
                line = re.sub(r"^@\s+", "B. ", line)
            elif re.match(r"^[o0]\s+", line, re.IGNORECASE):
                line = re.sub(r"^[o0]\s+", "D. ", line, flags=re.IGNORECASE)
            match = re.match(r"^\(?([A-Da-d]|[1-4])\)?[\).:\-]?\s+(.+)$", line)
            if match is None:
                continue
            option = str(match.group(2) or "").strip()
            if option and option not in options:
                options.append(option)
            if len(options) >= 4:
                break
        return options

    def _recover_attachment_import_questions(
        self,
        result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        ocr = self._attachment_ocr_payload(result)
        raw_text = str(
            ocr.get("raw_text") or ocr.get("clean_text") or ocr.get("math_normalized_text") or ""
        ).strip()
        if raw_text:
            parser = LocalAppDataService()
            parsed = parser._parse_import_raw_text(raw_text, meta_defaults={})
            recovered = [
                row
                for row in parsed
                if isinstance(row, dict)
                and str(row.get("question_text") or row.get("question") or "").strip()
            ]
            if recovered:
                return recovered
        out: list[dict[str, Any]] = []
        for key in ("lc_iie_questions", "lc_iie_output"):
            rows = ocr.get(key)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                statement = str(
                    row.get("statement")
                    or row.get("question_text")
                    or row.get("question")
                    or ""
                ).strip()
                if not statement:
                    continue
                raw_options = row.get("options")
                if isinstance(raw_options, dict):
                    options = [
                        {"label": str(k).strip(), "text": str(v).strip()}
                        for k, v in sorted(raw_options.items(), key=lambda item: str(item[0]))
                        if str(v).strip()
                    ]
                elif isinstance(raw_options, list):
                    options = [
                        {"label": chr(65 + idx), "text": str(item).strip()}
                        for idx, item in enumerate(raw_options)
                        if str(item).strip()
                    ]
                else:
                    options = []
                out.append(
                    {
                        "question_text": statement,
                        "options": options,
                        "question_type": "MCQ_SINGLE" if options else "NUMERICAL",
                    }
                )
        return out

    def _normalized_attachment_poll_payload(
        self,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        payload = self._decode_jsonish_map(
            result.get("final_answer"),
            result.get("answer"),
            result.get("display_answer"),
            result.get("reasoning"),
            result.get("explanation"),
        )
        if (not payload.get("question")) and isinstance(payload.get("polls"), list) and payload["polls"]:
            first = payload["polls"][0]
            if isinstance(first, dict):
                payload = dict(first)
        if (not payload.get("question")) and isinstance(payload.get("items"), list) and payload["items"]:
            first = payload["items"][0]
            if isinstance(first, dict):
                payload = dict(first)
        question = str(payload.get("question") or payload.get("prompt") or "").strip()
        options = payload.get("options")
        option_values = options if isinstance(options, list) else []
        usable_options = [str(item).strip() for item in option_values if str(item).strip()]
        if question and len(usable_options) >= 2:
            return {
                **payload,
                "question": question,
                "options": usable_options[:4],
            }
        candidates = self._attachment_question_candidates(result)
        raw_text = str(
            self._attachment_ocr_payload(result).get("raw_text")
            or self._attachment_ocr_payload(result).get("clean_text")
            or self._attachment_ocr_payload(result).get("math_normalized_text")
            or ""
        ).strip()
        parsed_options = self._attachment_options_from_ocr_text(raw_text)
        if candidates and len(parsed_options) >= 2:
            return {
                "question": candidates[0],
                "options": parsed_options[:4],
                "correct_option": 0,
                "timer_seconds": 30,
                "topic": "Attachment Poll",
                "difficulty": "medium",
            }
        return payload

    def _repair_attachment_poll_payload(
        self,
        result: dict[str, Any],
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        from core.api.entrypoint import lalacore_entry

        raw_text = str(
            self._attachment_ocr_payload(result).get("raw_text")
            or self._attachment_ocr_payload(result).get("clean_text")
            or self._attachment_ocr_payload(result).get("math_normalized_text")
            or ""
        ).strip()
        candidates = self._attachment_question_candidates(result)
        answer_hint = str(
            result.get("display_answer")
            or result.get("unsafe_candidate_answer")
            or result.get("answer")
            or ""
        ).strip()
        if not raw_text and not candidates:
            return {}
        repair_prompt = "\n\n".join(
            [
                "Repair a failed classroom attachment extraction into strict poll JSON.",
                "Return strict JSON only with keys: question, options, correct_option, timer_seconds, topic, difficulty.",
                "Use a zero-based index for correct_option.",
                "Create 4 concise options whenever possible.",
                "If OCR options are noisy, reconstruct the most plausible MCQ from the extracted question text instead of returning empty data.",
                f"Difficulty hint: {'hard' if adaptive else 'medium'}",
                f"OCR text:\n{raw_text}" if raw_text else "",
                (
                    "Structured question candidates:\n- " + "\n- ".join(candidates)
                    if candidates
                    else ""
                ),
                f"Answer hint: {answer_hint}" if answer_hint else "",
            ]
        )
        repaired = asyncio.run(
            lalacore_entry(
                input_data={"text": repair_prompt},
                input_type="text",
                user_context={
                    "surface": "maintenance_live_attachment_poll_repair",
                    "card": {"repair_mode": "attachment_poll"},
                },
                options={
                    "function": "live_poll_from_attachment",
                    "response_style": "structured_json",
                    "return_structured": True,
                    "return_markdown": False,
                    "strict_json_only": True,
                    "enable_web_retrieval": False,
                },
            )
        )
        return self._normalized_attachment_poll_payload(repaired)

    def _area_failure_signatures(
        self,
        area: str,
        summary: dict[str, Any],
    ) -> list[dict[str, Any]]:
        runs = summary.get("runs")
        if not isinstance(runs, list):
            return []
        signatures: list[dict[str, Any]] = []
        seen: set[str] = set()

        def _push(
            *,
            code: str,
            root_cause: str,
            atlas_fix: str,
            layer: str,
        ) -> None:
            if code in seen:
                return
            seen.add(code)
            signatures.append(
                {
                    "area": area,
                    "code": code,
                    "layer": layer,
                    "root_cause": root_cause,
                    "atlas_fix": atlas_fix,
                }
            )

        for row in runs:
            if not isinstance(row, dict):
                continue
            error = str(row.get("error") or row.get("evaluation_message") or "").lower()
            if (
                area == "auth_forgot_password"
                and str(row.get("request_status")) == "INVALID_EMAIL"
                and "+" in str(row.get("email") or "")
            ):
                _push(
                    code="forgot_password_plus_alias_rejected",
                    root_cause="Forgot-password validation rejected a valid plus-address email alias.",
                    atlas_fix="Use RFC-compatible email validation that accepts + aliases before SMTP delivery.",
                    layer="backend",
                )
            if (
                area == "auth_forgot_password"
                and str(row.get("request_status")) == "EMAIL_SEND_FAILED"
                and (
                    "nodename nor servname provided" in error
                    or "could not resolve host" in error
                )
            ):
                _push(
                    code="forgot_password_smtp_dns_resolution_failed",
                    root_cause="Forgot-password OTP delivery failed because SMTP host resolution was unavailable in the maintenance environment.",
                    atlas_fix="Retry the OTP transport check outside restricted sandbox networking and verify SMTP DNS reachability before blaming the auth flow.",
                    layer="network",
                )
            if (
                area == "create_quiz_pipeline"
                and "missing hidden key: _correct_option" in error
            ):
                _push(
                    code="quiz_grading_visible_answer_mismatch",
                    root_cause="Quiz grading could not map a visible correct answer back to the stored option label.",
                    atlas_fix="Normalize visible answer text against sanitized and LaTeX-wrapped option text before grading.",
                    layer="data",
                )
            if (
                area == "schedule_pipeline"
                and "'list' object has no attribute 'get'" in error
            ):
                _push(
                    code="schedule_probe_response_shape_mismatch",
                    root_cause="Maintenance expected a dict-shaped schedule payload where the route returned list-shaped schedule data.",
                    atlas_fix="Read schedule responses through compatibility keys like class, schedule, classes, or list instead of assuming one shape.",
                    layer="backend",
                )
            if (
                area == "live_class_ai"
                and bool(row.get("provider_error_detected"))
            ):
                _push(
                    code="live_class_provider_degraded",
                    root_cause="Live-class AI provider or retrieval resolution degraded during the maintenance sweep.",
                    atlas_fix="Prefer context-based fallback outputs and defer non-essential verification while keeping explain and quiz routes responsive.",
                    layer="ai",
                )
            if (
                area == "live_class_ai"
                and not bool(row.get("quiz_detected"))
                and str(row.get("quiz_payload_shape") or "") == "single_question"
            ):
                _push(
                    code="live_class_quiz_payload_shape_mismatch",
                    root_cause="Maintenance treated a valid single-question live-class quiz payload as empty.",
                    atlas_fix="Accept both single-question quiz payloads and list-based payloads during health verification.",
                    layer="backend",
                )
            if (
                area == "material_ai_pipeline"
                and isinstance(row.get("empty_modes"), list)
                and bool(row.get("empty_modes"))
            ):
                _push(
                    code="material_ai_mode_empty_output",
                    root_cause="One or more Study AI modes returned empty or weak content despite valid material context.",
                    atlas_fix="Keep per-mode fallback templates strong and verify every material mode returns grounded non-empty content.",
                    layer="ai",
                )
            if (
                area == "material_ai_query_pipeline"
                and (not bool(row.get("ok")) or int(row.get("answer_signal") or 0) < 24)
            ):
                _push(
                    code="material_ai_query_blank",
                    root_cause="Study Ask-AI mode returned blank or weak material-grounded output for a valid material question.",
                    atlas_fix="Preserve material-grounded QA fallback with direct answer plus explanation sections whenever provider output is thin.",
                    layer="ai",
                )
            if (
                area == "teacher_authoring_ai_pipeline"
                and int(row.get("weak_question_count") or 0) > 0
            ):
                _push(
                    code="teacher_authoring_ai_weak_questions",
                    root_cause="Teacher authoring AI produced draft questions with missing options, missing solutions, or weak statements.",
                    atlas_fix="Enforce teacher authoring quality gates so every generated question has a clear statement, valid options, and stepwise solution text before draft review.",
                    layer="ai",
                )
            if area == "app_ai_chat" and not bool(row.get("answer_signal")) and not bool(row.get("ok")):
                _push(
                    code="app_ai_chat_timeout_or_blank",
                    root_cause="A dashboard or analytics AI chat surface timed out or returned blank output.",
                    atlas_fix="Retry the chat surface with lighter prompts, preserve exam_coach fallback, and prefer surface-specific context cards over generic chat context.",
                    layer="ai",
                )
            if area == "app_atlas_planner" and not bool(row.get("plan_valid")):
                _push(
                    code="app_atlas_plan_shape_invalid",
                    root_cause="Atlas planner returned an invalid or incomplete plan shape for an app surface.",
                    atlas_fix="Normalize plans to single_action, multi_step_plan, or needs_more_info and prune steps above the 4-step budget.",
                    layer="backend",
                )
            if (
                area == "app_atlas_planner"
                and str(row.get("recovery_mode") or "") == "tool_mentions_from_reasoning"
            ):
                _push(
                    code="app_atlas_truncated_plan_recovered",
                    root_cause="Atlas app planner produced truncated JSON, but the reasoning still contained the intended tool sequence.",
                    atlas_fix="Recover the plan deterministically from tool mentions in the reasoning trace, then fill obvious args from instruction and dashboard context.",
                    layer="ai",
                )
            if (
                area == "app_atlas_planner"
                and int(row.get("disallowed_tool_count") or 0) > 0
            ):
                _push(
                    code="app_atlas_disallowed_tool_selected",
                    root_cause="Atlas planner selected tools outside the allowed role-specific app surface.",
                    atlas_fix="Intersect proposed tools with the role whitelist and replan before execution when any disallowed tool appears.",
                    layer="backend",
                )
            if area == "question_search_ai" and not bool(row.get("matches")):
                _push(
                    code="question_search_empty_for_known_query",
                    root_cause="Question search returned no matches for a known-maintenance query that should produce retrieval evidence.",
                    atlas_fix="Use normalized query variants and retrieval fallback instead of accepting empty search evidence.",
                    layer="ai",
                )
            if (
                area == "attachment_import_ai"
                and (not bool(row.get("ok")) or int(row.get("question_count") or 0) <= 0)
            ):
                _push(
                    code="attachment_import_ai_no_questions",
                    root_cause="Attachment-based quiz import returned no structured questions from a simple maintenance paper image.",
                    atlas_fix="Reinforce OCR-to-question extraction with a strict questions-array schema and keep a maintenance image path in the multimodal health sweep.",
                    layer="ai",
                )
            if (
                area == "live_class_ai"
                and str(row.get("variant")) == "concepts_flashcards"
                and not bool(row.get("concepts_signal"))
            ):
                _push(
                    code="live_class_concepts_empty",
                    root_cause="Live-class concept timeline returned no structured timeline items for a valid teaching context.",
                    atlas_fix="Keep concept timeline fallback tied to topic, transcript, and board cues so timeline output never collapses to empty.",
                    layer="ai",
                )
            if (
                area == "live_class_ai"
                and str(row.get("variant")) == "concepts_flashcards"
                and not bool(row.get("flashcards_signal"))
            ):
                _push(
                    code="live_class_flashcards_empty",
                    root_cause="Live-class flashcard generation returned no usable flashcards for a valid class context.",
                    atlas_fix="Require concise front/back flashcards and fall back to transcript-grounded recall cards when provider output is weak.",
                    layer="ai",
                )
            if (
                area == "live_class_support_ai"
                and (not bool(row.get("ok")) or int(row.get("citation_count") or 0) <= 0)
            ):
                _push(
                    code="live_class_support_missing_evidence",
                    root_cause="Deferred live-class explain support returned without usable evidence or citations for a known classroom prompt.",
                    atlas_fix="Keep explain-support evidence backfill alive even when support actions are empty so the hydrated classroom message still gains citations.",
                    layer="ai",
                )
            if (
                area == "live_attachment_poll_ai"
                and (not bool(row.get("ok")) or int(row.get("option_count") or 0) < 2)
            ):
                _push(
                    code="live_attachment_poll_invalid",
                    root_cause="Live-class attachment poll generation returned no usable question or not enough answer options for a simple maintenance attachment.",
                    atlas_fix="Keep the attachment-to-poll prompt on a strict question/options schema and fall back to concise option synthesis when OCR is thin.",
                    layer="ai",
                )
            if area == "live_class_agent_planner" and not bool(row.get("agent_plan_valid")):
                _push(
                    code="live_class_agent_plan_invalid",
                    root_cause="Live-class Atlas planning returned an invalid plan shape or an empty tool selection.",
                    atlas_fix="Normalize live-class plans to the strict single_action or multi_step_plan schema before execution.",
                    layer="backend",
                )
            if (
                area == "live_class_agent_planner"
                and str(row.get("recovery_mode") or "") == "tool_mentions_from_reasoning"
            ):
                _push(
                    code="live_class_agent_truncated_plan_recovered",
                    root_cause="Live-class Atlas planning lost structured JSON, but the reasoning still preserved the intended classroom tools.",
                    atlas_fix="Recover the live-class plan deterministically from tool mentions in the reasoning trace and rebuild the ordered steps.",
                    layer="ai",
                )
        return signatures

    def _support_recipient(self) -> str:
        raw = (
            os.getenv("ATLAS_MAINTENANCE_OTP_EMAIL", "").strip()
            or os.getenv("ATLAS_SUPPORT_EMAIL_RECIPIENT", "").strip()
        )
        for chunk in raw.replace(";", ",").replace("\n", ",").split(","):
            email = chunk.strip()
            if email:
                return email
        return ""

    def _post(self, client: TestClient, url: str, **kwargs):
        return client.post(url, timeout=self._request_timeout_seconds, **kwargs)

    def _get(self, client: TestClient, url: str, **kwargs):
        return client.get(url, timeout=self._request_timeout_seconds, **kwargs)

    def _otp_probe_email(self, idx: int) -> str:
        base = self._support_recipient()
        if "@" not in base:
            return base
        local, domain = base.split("@", 1)
        if "+" in local:
            local = local.split("+", 1)[0]
        if domain.lower() == "gmail.com":
            return f"{local}+atlas-maint-{idx + 1}@{domain}"
        return base

    def _probe_auth_login(self, client: TestClient, idx: int, *, adaptive: bool) -> dict[str, Any]:
        email = f"atlas.auth.{idx}@example.com"
        password = "MaintPass1234!" if not adaptive else "MaintPass2468!"
        register = self._post(
            client,
            "/auth/action",
            json={
                "action": "register_direct",
                "email": email,
                "password": password,
                "name": f"Atlas Maint {idx}",
                "username": f"atlas_maint_{idx}",
                "device_id": f"dev_maint_{idx}",
            },
        )
        login = self._post(
            client,
            "/auth/action",
            json={
                "action": "login_direct",
                "email": email,
                "password": password,
                "device_id": f"dev_maint_{idx}",
            },
        )
        register_body = register.json()
        login_body = login.json()
        ok = (
            register.status_code == 200
            and login.status_code == 200
            and str(register_body.get("status")) == "SUCCESS"
            and str(login_body.get("status")) == "SUCCESS"
        )
        return {
            "ok": ok,
            "register_status": register_body.get("status"),
            "login_status": login_body.get("status"),
        }

    def _probe_auth_forgot_password(
        self,
        client: TestClient,
        auth: LocalAuthService,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        email = self._otp_probe_email(idx)
        device_id = f"dev_maint_forgot_{idx}"
        start_password = f"StartMaint{idx + 1}!"
        next_password = f"ResetMaint{idx + 1}!"
        register = self._post(
            client,
            "/auth/action",
            json={
                "action": "register_direct",
                "email": email,
                "password": start_password,
                "name": "Atlas Maintenance OTP",
                "username": f"atlas_otp_{idx}",
                "device_id": device_id,
            },
        )
        request = self._post(
            client,
            "/auth/action",
            json={
                "action": "request_forgot_otp",
                "email": email,
                "device_id": device_id,
            },
        )
        request_body = request.json()
        otp = auth._otps.get(email, {}).get("otp")  # type: ignore[attr-defined]
        reset_status = None
        login_status = None
        if otp:
            reset = self._post(
                client,
                "/auth/action",
                json={
                    "action": "forgot_password_reset",
                    "email": email,
                    "otp": otp,
                    "new_password": next_password,
                    "device_id": device_id,
                },
            )
            reset_status = reset.json().get("status")
            login = self._post(
                client,
                "/auth/action",
                json={
                    "action": "login_direct",
                    "email": email,
                    "password": next_password,
                    "device_id": device_id,
                },
            )
            login_status = login.json().get("status")
        ok = (
            register.status_code == 200
            and str(register.json().get("status")) == "SUCCESS"
            and request.status_code == 200
            and str(request_body.get("status")) == "OTP_SENT"
            and str(reset_status) == "SUCCESS"
            and str(login_status) == "SUCCESS"
        )
        return {
            "ok": ok,
            "email": email,
            "register_status": register.json().get("status"),
            "request_status": request_body.get("status"),
            "request_message": request_body.get("message"),
            "delivery": request_body.get("delivery"),
            "reset_status": reset_status,
            "login_status": login_status,
        }

    def _probe_create_quiz_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        create = self._post(
            client,
            "/app/action",
            json={
                "action": "create_quiz",
                "title": f"Maintenance Quiz {idx + 1}",
                "type": "Exam",
                "duration": 20 if adaptive else 30,
                "role": "teacher",
                "questions": [
                    {
                        "text": "If x^2/16 - y^2/9 = 1, find e.",
                        "type": "MCQ",
                        "options": ["3/4", "4/3", "5/4", "5/3"],
                        "correct": "5/4",
                        "solution_explanation": "Use e = sqrt(1 + b^2/a^2).",
                    }
                ],
            },
        )
        create_body = create.json()
        quiz_id = str(create_body.get("id") or "")
        evaluate = self._post(
            client,
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {"0": ["C"]},
                "student_name": "Maintenance Bot",
                "student_id": "maint_student",
                "preview_only": True,
            },
        )
        eval_body = evaluate.json()
        ok = (
            create.status_code == 200
            and evaluate.status_code == 200
            and bool(create_body.get("ok"))
            and bool(eval_body.get("ok"))
            and quiz_id != ""
        )
        return {
            "ok": ok,
            "quiz_id": quiz_id,
            "create_status": create_body.get("status"),
            "evaluation_status": eval_body.get("status"),
            "evaluation_ok": eval_body.get("ok"),
            "evaluation_message": eval_body.get("message"),
        }

    def _probe_ai_quiz_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        response = self._post(
            client,
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Physics",
                "chapters": ["Kinematics"] if adaptive else ["Kinematics", "NLM"],
                "subtopics": ["Relative Velocity"] if adaptive else ["Relative Velocity", "Pseudo Force"],
                "difficulty": 3 if adaptive else 4,
                "question_count": 3 if adaptive else 5,
                "trap_intensity": "medium" if adaptive else "high",
                "weakness_mode": True,
                "cross_concept": not adaptive,
                "user_id": "maint_student",
                "role": "student",
                "self_practice_mode": True,
                "title": f"Maintenance AI Quiz {idx + 1}",
            },
        )
        body = response.json()
        questions = body.get("questions_json", [])
        ok = response.status_code == 200 and bool(body.get("ok")) and len(questions) >= 1
        degraded = not ok or len(questions) < 2
        return {
            "ok": ok,
            "degraded": degraded,
            "status": body.get("status"),
            "quiz_id": body.get("quiz_id"),
            "question_count": len(questions),
        }

    def _probe_study_material_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        add = self._post(
            client,
            "/app/action",
            json={
                "action": "add_material",
                "title": f"Maintenance Material {idx + 1}",
                "type": "pdf",
                "url": f"https://example.com/maintenance_{idx + 1}.pdf",
                "class": "Class 12",
                "subject": "Mathematics",
                "chapters": "Binomial Theorem" if adaptive else "Electrostatics",
                "notes": "Maintenance synthetic material.",
                "role": "teacher",
            },
        )
        listing = self._get(client, "/app/action", params={"action": "get_materials"})
        add_body = add.json()
        list_body = listing.json()
        ok = (
            add.status_code == 200
            and listing.status_code == 200
            and bool(add_body.get("ok"))
            and bool(list_body.get("ok"))
        )
        return {
            "ok": ok,
            "material_id": ((add_body.get("material") or {}).get("material_id")),
            "list_count": len(list_body.get("list", [])),
        }

    def _probe_material_ai_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        add = self._post(
            client,
            "/app/action",
            json={
                "action": "add_material",
                "title": f"Maintenance AI Material {idx + 1}",
                "type": "pdf",
                "url": f"https://example.com/maintenance_ai_{idx + 1}.pdf",
                "class": "Class 12",
                "subject": "Physics",
                "chapters": "Electrostatics",
                "notes": "Electric field, superposition, flux.",
                "role": "teacher",
            },
        )
        material_id = (((add.json().get("material") or {}).get("material_id")) if add.status_code == 200 else "")
        mode_groups = [
            ["summarize", "notes"],
            ["formula_sheet", "flashcards"],
            ["revision_plan", "quiz_drill"],
        ]
        modes = mode_groups[idx % len(mode_groups)]
        generate_responses = [
            self._post(
                client,
                "/app/action",
                json={
                    "action": "material_generate",
                    "material_id": material_id,
                    "mode": mode,
                },
            )
            for mode in modes
        ]
        generate_bodies = [response.json() for response in generate_responses]
        mode_outputs: dict[str, int] = {}
        empty_modes: list[str] = []
        for mode, body in zip(modes, generate_bodies):
            content = str(body.get("content") or "").strip()
            mode_outputs[mode] = len(content)
            if not bool(body.get("ok")) or len(content) < 24:
                empty_modes.append(mode)
        ok = (
            add.status_code == 200
            and all(response.status_code == 200 for response in generate_responses)
            and all(bool(body.get("ok")) for body in generate_bodies)
        )
        degraded = not ok or bool(empty_modes)
        return {
            "ok": ok,
            "degraded": degraded,
            "generate_status": [body.get("status") for body in generate_bodies],
            "checked_modes": modes,
            "mode_outputs": mode_outputs,
            "empty_modes": empty_modes,
        }

    def _probe_material_ai_query_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        add = self._post(
            client,
            "/app/action",
            json={
                "action": "add_material",
                "title": f"Maintenance QA Material {idx + 1}",
                "type": "pdf",
                "url": f"https://example.com/maintenance_qa_{idx + 1}.pdf",
                "class": "Class 12",
                "subject": "Physics",
                "chapters": "Thermodynamics",
                "notes": "Carnot engine, entropy, reversible process, common traps in efficiency questions.",
                "role": "teacher",
            },
        )
        material_id = (((add.json().get("material") or {}).get("material_id")) if add.status_code == 200 else "")
        response = self._post(
            client,
            "/app/action",
            json={
                "action": "material_query",
                "material_id": material_id,
                "question": (
                    "What is the main trap in reversible process and Carnot efficiency questions?"
                    if adaptive
                    else "Explain the biggest study trap in entropy and Carnot engine questions."
                ),
                "context_mode": "qa",
            },
        )
        body = response.json()
        content = str(body.get("content") or body.get("answer") or "").strip()
        ok = (
            add.status_code == 200
            and response.status_code == 200
            and bool(body.get("ok"))
            and len(content) >= 24
        )
        return {
            "ok": ok,
            "degraded": not ok,
            "material_id": material_id,
            "answer_signal": len(content),
            "status": body.get("status"),
        }

    def _probe_teacher_authoring_ai_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        response = self._post(
            client,
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "title": f"Maintenance Teacher Draft {idx + 1}",
                "subject": "Physics",
                "chapters": ["Thermodynamics"],
                "class_name": "Class 12",
                "type": "Exam",
                "difficulty": 4 if adaptive else 5,
                "question_count": 3 if adaptive else 4,
                "duration": 45,
                "role": "teacher",
                "authoring_mode": True,
                "self_practice_mode": False,
                "include_answer_key": True,
                "include_solutions": True,
            },
        )
        body = response.json()
        questions = body.get("questions_json")
        if not isinstance(questions, list):
            questions = []
        weak_indices: list[int] = []
        for index, row in enumerate(questions, start=1):
            if not isinstance(row, dict):
                weak_indices.append(index)
                continue
            statement = str(row.get("question_text") or row.get("text") or "").strip()
            options = row.get("options") if isinstance(row.get("options"), list) else []
            q_type = str(row.get("question_type") or row.get("type") or "MCQ").upper()
            solution = str(row.get("solution_explanation") or "").strip()
            has_valid_options = q_type == "NUMERICAL" or len(options) >= 2
            if not statement or not has_valid_options or len(solution) < 24:
                weak_indices.append(index)
        ok = response.status_code == 200 and bool(body.get("ok")) and bool(questions)
        degraded = not ok or bool(weak_indices)
        return {
            "ok": ok,
            "degraded": degraded,
            "question_count": len(questions),
            "weak_question_count": len(weak_indices),
            "weak_question_indices": weak_indices,
            "status": body.get("status"),
        }

    def _probe_app_ai_chat(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        variant = ["analytics_review", "study_support", "teacher_dashboard"][idx % 3]
        if variant == "analytics_review":
            payload = {
                "action": "ai_chat",
                "prompt": "Review this attempt and tell me the biggest weakness plus one next step.",
                "user_id": "student_analytics_maint",
                "chat_id": f"analytics_{idx}",
                "options": {
                    "function": "analytics_review",
                    "response_style": "exam_coach",
                    "app_surface": "analytics",
                    "enable_web_retrieval": False,
                },
                "card": {
                    "surface": "analytics",
                    "score": 61,
                    "percentage": 61.0,
                    "percentile": 84.2,
                    "rank": 128,
                    "weak_topics": ["Thermodynamics", "Waves"],
                    "top_rankers": [
                        {"name": "Aarav", "score": 92},
                        {"name": "Ishita", "score": 89},
                    ],
                },
            }
        elif variant == "study_support":
            payload = {
                "action": "ai_chat",
                "prompt": "Give me a crisp study summary from this material and tell me the main trap.",
                "user_id": "student_study_maint",
                "chat_id": f"study_{idx}",
                "options": {
                    "function": "study_material_chat",
                    "response_style": "exam_coach",
                    "app_surface": "study_material",
                    "enable_web_retrieval": False,
                    "prefer_material_grounding": True,
                },
                "card": {
                    "material_id": f"maint_material_{idx}",
                    "title": "Thermodynamics Audit Sheet",
                    "material_notes": "Entropy, Carnot engine, efficiency, reversible process.",
                    "subject": "Physics",
                    "chapter": "Thermodynamics",
                },
            }
        else:
            payload = {
                "action": "ai_chat",
                "prompt": (
                    "Give exactly two short bullets: "
                    "1) which student needs attention and why, "
                    "2) what next quiz to create and why."
                ),
                "user_id": "teacher_dashboard_maint",
                "chat_id": f"teacher_dashboard_{idx}",
                "options": {
                    "function": "teacher_dashboard_review",
                    "response_style": "exam_coach",
                    "app_surface": "teacher_dashboard",
                    "enable_web_retrieval": False,
                },
                "card": {
                    "surface": "teacher_dashboard",
                    "attention_students": [
                        {"name": "Aarav", "issue": "low accuracy in thermodynamics"},
                        {"name": "Mira", "issue": "missed last two homeworks"},
                    ],
                    "recommended_focus": "Thermodynamics",
                },
            }
        response = self._post(client, "/app/action", json=payload)
        body = response.json()
        answer_signal = len(
            str(
                body.get("answer")
                or body.get("content")
                or body.get("final_answer")
                or ""
            ).strip()
        )
        ok = response.status_code == 200 and bool(body.get("ok")) and answer_signal >= 24
        return {
            "ok": ok,
            "degraded": not ok,
            "variant": variant,
            "status": body.get("status"),
            "answer_signal": answer_signal,
        }

    def _probe_app_atlas_planner(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        if idx % 2 == 0:
            role = "student"
            response = self._post(
                client,
                "/ai/app/agent",
                json={
                    "instruction": "Show my weak topics and open study for the weakest one.",
                    "authority_level": "assist",
                    "context": {
                        "account_id": "student_plan_maint",
                        "student_id": "student_plan_maint",
                        "student_profile": {
                            "weak_topics": ["Thermodynamics", "Optics"],
                            "preferred_actions": ["summarize", "flashcards"],
                        },
                    },
                },
            )
        else:
            role = "teacher"
            response = self._post(
                client,
                "/ai/app/agent",
                json={
                    "instruction": "Create a class 12 thermodynamics quiz with 5 questions and then open student analytics.",
                    "authority_level": "semi_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "teacher_id": "teacher_plan_maint",
                        "selected_student": {"student_id": "student_7", "name": "Aarav"},
                    },
                },
            )
        body = response.json()
        plan_type = str(body.get("type") or "")
        steps = body.get("steps") if isinstance(body.get("steps"), list) else []
        step_tools = [step.get("tool") for step in steps if isinstance(step, dict)]
        single_tool = str(body.get("tool") or "").strip()
        step_count = len(steps)
        if plan_type == "single_action":
            plan_valid = bool(single_tool)
        elif plan_type == "multi_step_plan":
            plan_valid = 1 <= step_count <= 4 and all(
                isinstance(tool, str) and tool.strip() for tool in step_tools
            )
        elif plan_type == "needs_more_info":
            followups = body.get("follow_up_questions")
            plan_valid = isinstance(followups, list) and bool(followups)
        else:
            plan_valid = False
        disallowed_tool_count = 0
        if role == "student":
            disallowed_tool_count = sum(
                1
                for tool in ([single_tool] if single_tool else []) + step_tools
                if isinstance(tool, str)
                and tool
                and (
                    "teacher_" in tool
                    or tool
                    in {
                        "schedule_next_class",
                        "create_homework_assignment",
                        "create_exam_assignment",
                        "publish_teacher_quiz_draft",
                        "mute_all",
                        "approve_waiting_all",
                    }
                )
            )
        ok = response.status_code == 200 and plan_valid and disallowed_tool_count == 0
        return {
            "ok": ok,
            "degraded": not ok,
            "role_variant": role,
            "type": plan_type,
            "step_count": step_count,
            "plan_valid": plan_valid,
            "disallowed_tool_count": disallowed_tool_count,
            "recovery_mode": str(body.get("recovery_mode") or ""),
        }

    def _probe_chat_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        thread = f"student_{idx}|TEACHER"
        send = self._post(
            client,
            "/app/action",
            json={
                "action": "send_message",
                "is_peer": True,
                "chat_id": thread,
                "participants": f"student_{idx},TEACHER",
                "payload": {
                    "id": f"m_{idx}",
                    "sender": f"student_{idx}",
                    "senderName": f"Student {idx}",
                    "text": "maintenance hello teacher",
                    "type": "text",
                    "time": int(time.time() * 1000),
                },
            },
        )
        directory = self._post(
            client,
            "/app/action",
            json={
                "action": "list_chat_directory",
                "chat_id": f"student_{idx}",
                "role": "student",
            },
        )
        queue = self._post(
            client,
            "/app/action",
            json={
                "action": "queue_teacher_review",
                "quiz_id": f"quiz_{idx}",
                "question_id": "1",
                "student_answer": "A",
                "correct_answer": "B",
                "student_id": f"student_{idx}",
                "message": "Maintenance review request",
            },
        )
        ok = (
            send.status_code == 200
            and directory.status_code == 200
            and queue.status_code == 200
            and bool(send.json().get("ok"))
            and bool(directory.json().get("ok"))
            and bool(queue.json().get("ok"))
        )
        return {
            "ok": ok,
            "thread": thread,
            "directory_count": len(directory.json().get("list", [])),
        }

    def _probe_schedule_pipeline(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        scheduled = self._post(
            client,
            "/app/action",
            json={
                "action": "schedule_live_class",
                "role": "teacher",
                "teacher_id": "teacher_maint",
                "teacher_name": "Atlas Maintenance",
                "class_name": "Class 12",
                "title": f"Maintenance Scheduled Class {idx + 1}",
                "subject": "Mathematics",
                "topic": "Permutation and Combination" if adaptive else "Definite Integration",
                "start_time": f"2026-04-{10 + idx:02d}T10:00:00Z",
                "duration_minutes": 60,
                "description": "Maintenance synthetic schedule",
            },
        )
        listing = self._get(
            client,
            "/app/action",
            params={
                "action": "list_live_class_schedule",
                "role": "teacher",
                "viewer_id": "teacher_maint",
            },
        )
        ok = (
            scheduled.status_code == 200
            and listing.status_code == 200
            and bool(scheduled.json().get("ok"))
            and bool(listing.json().get("ok"))
        )
        scheduled_body = scheduled.json()
        scheduled_class = (
            scheduled_body.get("class")
            if isinstance(scheduled_body.get("class"), dict)
            else {}
        )
        listing_body = listing.json()
        list_rows = listing_body.get("schedule")
        if not isinstance(list_rows, list):
            list_rows = listing_body.get("classes")
        if not isinstance(list_rows, list):
            list_rows = listing_body.get("list")
        if not isinstance(list_rows, list):
            list_rows = []
        return {
            "ok": ok,
            "scheduled_id": scheduled_class.get("class_id") or scheduled_class.get("id"),
            "list_count": len(list_rows),
        }

    def _probe_question_search_ai(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        query = (
            "eccentricity of hyperbola x^2/16 - y^2/9 = 1"
            if idx % 2 == 0
            else "general term in (1+x)^n binomial theorem"
        )
        response = self._post(
            client,
            "/ai/question-search",
            json={"query": query, "max_matches": 5 if adaptive else 8},
        )
        body = response.json()
        matches = body.get("matches")
        query_variants = body.get("query_variants")
        match_count = len(matches) if isinstance(matches, list) else 0
        ok = (
            response.status_code == 200
            and bool(body.get("ok"))
            and match_count > 0
            and isinstance(query_variants, list)
            and bool(query_variants)
        )
        return {
            "ok": ok,
            "degraded": not ok,
            "matches": match_count,
            "query_variants": len(query_variants) if isinstance(query_variants, list) else 0,
            "cache_hit": bool(body.get("cache_hit")),
        }

    def _probe_attachment_import_ai(
        self,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        from core.api.entrypoint import lalacore_entry

        data_url = self._maintenance_attachment_image_data_url(
            lines=[
                "Q1. What is the SI unit of force?",
                "A. Newton",
                "B. Joule",
                "C. Pascal",
                "D. Watt",
                "Answer: A",
            ]
        )
        prompt = "\n".join(
            [
                "Extract quiz questions from this attachment for teacher review.",
                "Return strict JSON only with a questions array.",
                "Each question should include statement or question_text, options, correct_answer if visible, solution if visible, and type.",
                "Do not invent missing answers.",
                "Source type: Exam",
                f"Difficulty hint: {'hard' if adaptive else 'medium'}",
            ]
        )
        result = asyncio.run(
            lalacore_entry(
                input_data={"text": prompt, "image": data_url},
                input_type="mixed",
                user_context={
                    "surface": "teacher_dashboard_attachment_import",
                    "card": {
                        "task": "paper_to_quiz",
                        "assessment_type": "Exam",
                        "strict_ocr": True,
                        "ocr_multi_pass": 3,
                    },
                },
                options={
                    "function": "quiz_paper_ocr_extract",
                    "response_style": "structured_json",
                    "return_structured": True,
                    "return_markdown": False,
                    "enable_web_retrieval": False,
                    "strict_json_only": True,
                },
            )
        )
        payload = self._decode_jsonish_map(
            result.get("final_answer"),
            result.get("answer"),
            result.get("display_answer"),
            result.get("reasoning"),
            result.get("explanation"),
        )
        questions = payload.get("questions")
        if not isinstance(questions, list):
            questions = payload.get("items")
        if not isinstance(questions, list) and any(
            str(payload.get(key) or "").strip()
            for key in ("statement", "question_text", "question", "text")
        ):
            questions = [payload]
        if not isinstance(questions, list):
            questions = []
        usable_questions = [
            row
            for row in questions
            if isinstance(row, dict)
            and str(row.get("statement") or row.get("question_text") or row.get("question") or "").strip()
        ]
        if not usable_questions:
            usable_questions = self._recover_attachment_import_questions(result)
        ok = bool(usable_questions)
        return {
            "ok": ok,
            "degraded": not ok,
            "question_count": len(usable_questions),
            "winner_provider": str(result.get("winner_provider") or ""),
            "answer_signal": len(str(result.get("final_answer") or result.get("answer") or "").strip()),
        }

    def _probe_live_class_core(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        class_id = f"maint_live_{idx}"
        token = self._post(
            client,
            "/live/token",
            json={
                "class_id": class_id,
                "user_id": "teacher_maint",
                "display_name": "Atlas Teacher",
                "role": "teacher",
                "title": "Maintenance Live Class",
                "teacher_name": "Atlas Teacher",
                "subject": "Physics",
                "topic": "Newton Laws",
            },
        )
        join = self._post(
            client,
            "/class/join_request",
            json={
                "class_id": class_id,
                "user_id": f"student_{idx}",
                "user_name": f"Student {idx}",
                "role": "student",
            },
        )
        admit_all = self._post(client, "/class/admit_all", json={"class_id": class_id})
        state = self._get(
            client,
            "/class/state",
            params={"class_id": class_id, "user_id": f"student_{idx}"},
        )
        mute = self._post(
            client,
            "/class/mute",
            json={"class_id": class_id, "user_id": f"student_{idx}", "muted": True},
        )
        camera = self._post(
            client,
            "/class/camera",
            json={"class_id": class_id, "user_id": f"student_{idx}", "disabled": True},
        )
        recording = self._post(
            client,
            "/class/recording",
            json={"class_id": class_id, "enabled": True},
        )
        ok = all(
            response.status_code == 200
            for response in (token, join, admit_all, state, mute, camera, recording)
        ) and all(
            bool(response.json().get("ok", True))
            for response in (token, join, admit_all, state, mute, camera, recording)
        )
        return {
            "ok": ok,
            "approved_count": len((state.json().get("approved_users") or [])),
            "recording_enabled": (recording.json().get("session") or {}).get("is_recording"),
        }

    def _probe_live_class_ai(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        context = {
            "class_id": f"maint_live_ai_{idx}",
            "subject": "Mathematics",
            "topic": "Hyperbola",
            "teacher_name": "Atlas Teacher",
            "transcript": "We are solving the eccentricity of x^2/16 - y^2/9 = 1.",
        }
        variant_map = {
            0: "concepts_flashcards",
            1: "notes_analysis",
            2: "core_quiz",
        }
        variant = variant_map[idx % len(variant_map)]
        explain_body: dict[str, Any] = {}
        notes_body: dict[str, Any] = {}
        analysis_body: dict[str, Any] = {}
        quiz_body: dict[str, Any] = {}
        concepts_body: dict[str, Any] = {}
        flashcards_body: dict[str, Any] = {}
        explain_signal = False
        notes_signal = False
        analysis_signal = False
        concepts_signal = False
        flashcards_signal = False
        quiz_items: list[dict[str, Any]] = []
        provider_error_detected = False
        responses = []
        if variant == "core_quiz":
            explain = self._post(
                client,
                "/ai/class/explain",
                json={
                    "prompt": "Find the eccentricity of x^2/16 - y^2/9 = 1",
                    "context": context,
                },
            )
            quiz = self._post(
                client,
                "/ai/class/quiz",
                json={
                    "instruction": "Generate a short quiz",
                    "context": context,
                    "question_type": "MCQ",
                    "difficulty": "medium" if adaptive else "hard",
                },
            )
            responses = [explain, quiz]
            explain_body = explain.json()
            quiz_body = quiz.json()
            explain_signal = any(
                str(value).strip()
                for value in (
                    explain_body.get("answer"),
                    explain_body.get("explanation"),
                )
            )
            raw_quiz_items = quiz_body.get("items", []) or quiz_body.get("questions", []) or []
            if isinstance(raw_quiz_items, list):
                quiz_items = [item for item in raw_quiz_items if isinstance(item, dict)]
            if not quiz_items and str(quiz_body.get("question") or "").strip():
                quiz_items = [quiz_body]
            steps = explain_body.get("steps")
            if isinstance(steps, list) and any(
                "could not resolve host" in str(item).lower() for item in steps
            ):
                provider_error_detected = True
            ok = (
                all(response.status_code == 200 for response in responses)
                and explain_signal
                and bool(quiz_items)
            )
            degraded = not ok
        elif variant == "notes_analysis":
            notes = self._post(
                client,
                "/ai/class/notes",
                json={"instruction": "Generate notes", "context": context},
            )
            analysis = self._post(
                client,
                "/ai/class/analysis",
                json={"instruction": "Analyze class understanding", "context": context},
            )
            responses = [notes, analysis]
            notes_body = notes.json()
            analysis_body = analysis.json()
            notes_signal = any(
                isinstance(notes_body.get(key), list) and bool(notes_body.get(key))
                for key in ("key_concepts", "formulas", "shortcuts", "common_mistakes")
            )
            analysis_signal = any(
                isinstance(analysis_body.get(key), list) and bool(analysis_body.get(key))
                for key in ("insights", "doubt_clusters", "verification_notes")
            )
            for body in (notes_body, analysis_body):
                steps = body.get("steps")
                if not isinstance(steps, list):
                    continue
                if any("could not resolve host" in str(item).lower() for item in steps):
                    provider_error_detected = True
                    break
            ok = all(response.status_code == 200 for response in responses) and (
                notes_signal or analysis_signal
            )
            degraded = not ok
        else:
            concepts = self._post(
                client,
                "/ai/class/concepts",
                json={"instruction": "List concepts", "context": context},
            )
            flashcards = self._post(
                client,
                "/ai/class/flashcards",
                json={"instruction": "Create flashcards", "context": context},
            )
            responses = [concepts, flashcards]
            concepts_body = concepts.json()
            flashcards_body = flashcards.json()
            concepts_signal = isinstance(concepts_body.get("timeline"), list) and bool(
                concepts_body.get("timeline")
            )
            flashcards_signal = isinstance(flashcards_body.get("flashcards"), list) and bool(
                flashcards_body.get("flashcards")
            )
            for body in (concepts_body, flashcards_body):
                steps = body.get("steps")
                if not isinstance(steps, list):
                    continue
                if any("could not resolve host" in str(item).lower() for item in steps):
                    provider_error_detected = True
                    break
            ok = all(response.status_code == 200 for response in responses) and (
                concepts_signal or flashcards_signal
            )
            degraded = not ok
        return {
            "ok": ok,
            "degraded": degraded,
            "variant": variant,
            "explain_citations": len(explain_body.get("citations", []) or []),
            "quiz_items": len(quiz_items),
            "quiz_detected": len(quiz_items) > 0,
            "quiz_payload_shape": "single_question"
            if str(quiz_body.get("question") or "").strip()
            else "list",
            "provider_error_detected": provider_error_detected,
            "notes_signal": notes_signal,
            "analysis_signal": analysis_signal,
            "concepts_signal": concepts_signal,
            "flashcards_signal": flashcards_signal,
        }

    def _probe_live_class_support_ai(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        response = self._post(
            client,
            "/ai/class/explain/support",
            json={
                "prompt": (
                    "Find the eccentricity of x^2/16 - y^2/9 = 1 and explain the classroom takeaway."
                ),
                "context": {
                    "class_id": f"maint_live_support_{idx}",
                    "subject": "Mathematics",
                    "topic": "Hyperbola",
                    "teacher_name": "Atlas Teacher",
                    "transcript": "We are solving eccentricity of x^2/16 - y^2/9 = 1.",
                    "ocr_snippets": [
                        "x^2/16 - y^2/9 = 1",
                        "e = sqrt(1 + b^2/a^2)",
                    ],
                },
                "atlas_actions": {
                    "follow_up_hint": "support_hydration_probe",
                    "support_actions_pending": adaptive,
                },
            },
        )
        body = response.json()
        citations = body.get("citations") if isinstance(body.get("citations"), list) else []
        ok = response.status_code == 200 and bool(body.get("ok")) and len(citations) >= 1
        return {
            "ok": ok,
            "degraded": not ok,
            "citation_count": len(citations),
            "support_action_count": len(body.get("support_actions") or {}),
            "status": body.get("status"),
        }

    def _probe_live_attachment_poll_ai(
        self,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        from core.api.entrypoint import lalacore_entry

        data_url = self._maintenance_attachment_image_data_url(
            lines=[
                "Question 1: How many ways can 2 students be chosen from 4?",
                "A. Four",
                "B. Six",
                "C. Eight",
                "D. Twelve",
            ]
        )
        prompt = "\n".join(
            [
                "Analyze the uploaded classroom attachment and create one strong live poll for the current class.",
                "Return strict JSON only with keys: question, options, correct_option, timer_seconds, topic, difficulty.",
                "Use 2 to 4 concise options. Prefer 4 if the attachment supports it.",
                f"Difficulty hint: {'hard' if adaptive else 'medium'}",
                "Teacher instruction: Turn this into a live class poll.",
            ]
        )
        result = asyncio.run(
            lalacore_entry(
                input_data={"text": prompt, "image": data_url},
                input_type="mixed",
                user_context={
                    "surface": "live_class_ai_chat",
                    "card": {
                        "attachment_name": "maintenance_poll.png",
                        "attachment_mime_type": "image/png",
                        "class_context": {
                            "subject": "Mathematics",
                            "topic": "Permutation and Combination",
                        },
                    },
                },
                options={
                    "function": "live_poll_from_attachment",
                    "response_style": "structured_json",
                    "return_structured": True,
                    "return_markdown": False,
                    "strict_json_only": True,
                    "enable_web_retrieval": False,
                },
            )
        )
        payload = self._normalized_attachment_poll_payload(result)
        if not str(payload.get("question") or payload.get("prompt") or "").strip():
            payload = self._repair_attachment_poll_payload(result, adaptive=adaptive)
        question = str(payload.get("question") or payload.get("prompt") or "").strip()
        options = payload.get("options") if isinstance(payload.get("options"), list) else []
        usable_options = [str(item).strip() for item in options if str(item).strip()]
        ok = bool(question) and len(usable_options) >= 2
        return {
            "ok": ok,
            "degraded": not ok,
            "question_signal": len(question),
            "option_count": len(usable_options),
            "winner_provider": str(result.get("winner_provider") or ""),
        }

    def _probe_live_class_agent_planner(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        context = {
            "class_id": f"maint_live_agent_{idx}",
            "subject": "Mathematics",
            "topic": "Binomial Theorem",
            "teacher_name": "Atlas Teacher",
            "transcript": "We are expanding (1+x)^n and discussing general term.",
            "role": "teacher",
        }
        response = self._post(
            client,
            "/ai/class/agent",
            json={
                "instruction": "Admit all waiting students, mute all, and write Binomial Theorem on the board",
                "context": context,
            },
        )
        body = response.json()
        agent_type = str(body.get("type") or "")
        agent_steps = body.get("steps") if isinstance(body.get("steps"), list) else []
        agent_plan_valid = (
            (agent_type == "single_action" and bool(str(body.get("tool") or "").strip()))
            or (
                agent_type == "multi_step_plan"
                and 1 <= len(agent_steps) <= 4
                and all(
                    isinstance(step, dict) and str(step.get("tool") or "").strip()
                    for step in agent_steps
                )
            )
            or (
                agent_type == "needs_more_info"
                and isinstance(body.get("follow_up_questions"), list)
                and bool(body.get("follow_up_questions"))
            )
        )
        return {
            "ok": response.status_code == 200 and agent_plan_valid,
            "degraded": not agent_plan_valid,
            "type": agent_type,
            "step_count": len(agent_steps),
            "agent_plan_valid": agent_plan_valid,
            "recovery_mode": str(body.get("recovery_mode") or ""),
        }

    def _probe_live_class_transcription(
        self,
        client: TestClient,
        idx: int,
        *,
        adaptive: bool,
    ) -> dict[str, Any]:
        with client.websocket_connect("/transcription/stream") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "id": f"maint_transcript_{idx}",
                        "speaker_id": "teacher",
                        "speaker_name": "Atlas Teacher",
                        "text": "Today we discuss binomial theorem.",
                        "source": "maintenance",
                    }
                )
            )
            raw = ws.receive_text()
        payload = json.loads(raw)
        ok = str(payload.get("text") or "").strip() == "Today we discuss binomial theorem."
        return {
            "ok": ok,
            "source": payload.get("source"),
            "speaker_name": payload.get("speaker_name"),
        }


class AtlasMaintenanceScheduler:
    """Background poller that triggers the weekly Saturday maintenance window."""

    def __init__(
        self,
        *,
        service: AtlasMaintenanceService | None = None,
        interval_seconds: int | None = None,
    ) -> None:
        self._service = service or AtlasMaintenanceService()
        self._interval_seconds = max(
            60,
            int(os.getenv("ATLAS_MAINTENANCE_TICK_SECONDS", str(interval_seconds or 900))),
        )
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run_loop(self) -> None:
        while True:
            try:
                await self._service.run_if_due()
            except Exception:
                pass
            await asyncio.sleep(self._interval_seconds)
