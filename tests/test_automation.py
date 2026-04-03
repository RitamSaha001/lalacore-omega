from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.automation.feeder_engine import FeederEngine
from core.automation.orchestrator import AutomationOrchestrator
from core.automation.state_manager import AutomationStateManager
from core.lalacore_x.mini_evolution import MiniEvolutionEngine
from core.lalacore_x.token_budget import TokenBudgetGuardian
from app.auth.local_auth_service import LocalAuthService
from app.data.local_app_data_service import LocalAppDataService
from app.main import AtlasMaintenanceLockMiddleware
from app.storage.sqlite_json_store import SQLiteJsonBlobStore
from services.app_update_release_notifier import AppUpdateReleaseNotifierService
from services.atlas_maintenance_service import (
    AtlasMaintenanceService,
    _AtlasPipelineMaintenanceAuditor,
)


class AutomationTests(unittest.TestCase):
    def test_app_update_release_notifier_sends_mail_for_new_release_once(self):
        class _FakeEmailService:
            def __init__(self) -> None:
                self.confirmation_calls: list[dict] = []
                self.announcement_calls: list[dict] = []

            def send_release_confirmation(self, **kwargs):
                self.confirmation_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release confirmation sent",
                }

            def send_release_announcement(self, **kwargs):
                self.announcement_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release announcement sent",
                    "sent_count": len(list(kwargs.get("recipients") or [])),
                    "recipients": list(kwargs.get("recipients") or []),
                }

        csv_text = (
            "enabled,app_id,channel,audience,platform,version,build_number,apk_url,force,message,release_notes\n"
            "TRUE,lalacore_rebuild,stable,all,android,1.0.0,1,https://example.com/app.apk,FALSE,Update available,Added AI upgrades\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            state = AutomationStateManager(path=str(Path(tmp) / "LC9_AUTOMATION_STATE.json"))
            email = _FakeEmailService()
            service = AppUpdateReleaseNotifierService(
                state=state,
                email_service=email,
                fetcher=lambda url: csv_text,
                sheet_url="https://example.com/updates.csv",
            )

            first = asyncio.run(service.poll_for_new_releases(trigger="manual"))
            second = asyncio.run(service.poll_for_new_releases(trigger="manual"))

        self.assertTrue(first.get("ok"))
        self.assertEqual(first.get("new_release_count"), 1)
        self.assertEqual(second.get("status"), "NO_NEW_RELEASE")
        self.assertEqual(len(email.confirmation_calls), 1)
        self.assertEqual(len(email.announcement_calls), 1)
        self.assertEqual(
            email.confirmation_calls[0]["releases"][0]["release_key"],
            "lalacore_rebuild|stable|all|android|1.0.0|1",
        )

    def test_app_update_release_notifier_force_resend_ignores_seen_keys(self):
        class _FakeEmailService:
            def __init__(self) -> None:
                self.confirmation_calls: list[dict] = []
                self.announcement_calls: list[dict] = []

            def send_release_confirmation(self, **kwargs):
                self.confirmation_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release confirmation sent",
                }

            def send_release_announcement(self, **kwargs):
                self.announcement_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release announcement sent",
                    "sent_count": len(list(kwargs.get("recipients") or [])),
                    "recipients": list(kwargs.get("recipients") or []),
                }

        csv_text = (
            "enabled,app_id,channel,audience,platform,version,build_number,apk_url,force,message\n"
            "TRUE,lalacore_rebuild,stable,teacher,android,1.0.0,1,https://example.com/teacher.apk,FALSE,Teacher update available\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            state = AutomationStateManager(path=str(Path(tmp) / "LC9_AUTOMATION_STATE.json"))
            email = _FakeEmailService()
            service = AppUpdateReleaseNotifierService(
                state=state,
                email_service=email,
                fetcher=lambda url: csv_text,
                sheet_url="https://example.com/updates.csv",
            )

            asyncio.run(service.poll_for_new_releases(trigger="manual"))
            resent = asyncio.run(
                service.poll_for_new_releases(
                    trigger="manual",
                    force_resend=True,
                )
            )

        self.assertTrue(resent.get("ok"))
        self.assertEqual(resent.get("new_release_count"), 1)
        self.assertEqual(len(email.confirmation_calls), 2)
        self.assertEqual(len(email.announcement_calls), 2)

    def test_app_update_release_notifier_collects_signed_in_user_emails(self):
        class _FakeEmailService:
            def __init__(self) -> None:
                self.confirmation_calls: list[dict] = []
                self.announcement_calls: list[dict] = []

            def send_release_confirmation(self, **kwargs):
                self.confirmation_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release confirmation sent",
                }

            def send_release_announcement(self, **kwargs):
                self.announcement_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release announcement sent",
                    "sent_count": len(list(kwargs.get("recipients") or [])),
                    "recipients": list(kwargs.get("recipients") or []),
                }

        csv_text = (
            "enabled,app_id,channel,audience,platform,version,build_number,apk_url,force,message\n"
            "TRUE,lalacore_rebuild,stable,teacher,android,2.0.2,16,https://example.com/teacher.apk,TRUE,Teacher update available\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            auth_root = root / "auth"
            auth_root.mkdir(parents=True, exist_ok=True)
            (auth_root / "users.json").write_text(
                json.dumps(
                    {
                        "teacher@school.edu": {
                            "email": "teacher@school.edu",
                            "role": "teacher",
                            "name": "Teacher",
                        },
                        "student@school.edu": {
                            "email": "student@school.edu",
                            "role": "student",
                            "name": "Student",
                        },
                    }
                ),
                encoding="utf-8",
            )
            SQLiteJsonBlobStore(auth_root / "auth_store.sqlite3").write_json(
                "auth_users",
                {
                    "teacher2@school.edu": {
                        "email": "teacher2@school.edu",
                        "role": "teacher",
                        "name": "Teacher Two",
                    }
                },
            )

            state = AutomationStateManager(path=str(root / "LC9_AUTOMATION_STATE.json"))
            email = _FakeEmailService()
            service = AppUpdateReleaseNotifierService(
                state=state,
                email_service=email,
                fetcher=lambda url: csv_text,
                sheet_url="https://example.com/updates.csv",
                auth_users_file=auth_root / "users.json",
                auth_storage_db_file=auth_root / "auth_store.sqlite3",
            )

            result = asyncio.run(service.poll_for_new_releases(trigger="manual"))

        self.assertTrue(result.get("ok"))
        self.assertEqual(len(email.announcement_calls), 1)
        self.assertEqual(
            email.announcement_calls[0]["recipients"],
            ["teacher@school.edu", "teacher2@school.edu"],
        )

    def test_app_update_release_notifier_catches_up_latest_release_for_late_login(self):
        class _FakeEmailService:
            def __init__(self) -> None:
                self.announcement_calls: list[dict] = []

            def send_release_announcement(self, **kwargs):
                payload = dict(kwargs)
                recipients = list(payload.get("recipients") or [])
                self.announcement_calls.append(payload)
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release announcement sent",
                    "sent_count": len(recipients),
                    "recipients": recipients,
                    "sent_recipients": recipients,
                }

        csv_text = (
            "enabled,app_id,channel,audience,platform,version,build_number,apk_url,force,message\n"
            "TRUE,lalacore_rebuild,stable,all,android,2.0.1,15,https://example.com/all-old.apk,TRUE,Old update\n"
            "TRUE,lalacore_rebuild,stable,all,android,3.0.1,17,https://example.com/all-new.apk,TRUE,Latest all update\n"
            "TRUE,lalacore_rebuild,stable,teacher,android,2.0.2,16,https://example.com/teacher-old.apk,TRUE,Old teacher update\n"
            "TRUE,lalacore_rebuild,stable,teacher,android,3.0.2,18,https://example.com/teacher-new.apk,TRUE,Latest teacher update\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = AutomationStateManager(path=str(root / "LC9_AUTOMATION_STATE.json"))
            email = _FakeEmailService()
            service = AppUpdateReleaseNotifierService(
                state=state,
                email_service=email,
                fetcher=lambda url: csv_text,
                sheet_url="https://example.com/updates.csv",
                auth_users_file=root / "users.json",
                auth_storage_db_file=root / "auth_store.sqlite3",
            )

            first = service.notify_pending_releases_for_email(
                "teacher@school.edu",
                role="teacher",
            )
            second = service.notify_pending_releases_for_email(
                "teacher@school.edu",
                role="teacher",
            )

        self.assertTrue(first.get("ok"))
        self.assertEqual(first.get("sent_count"), 1)
        self.assertEqual(len(email.announcement_calls), 1)
        sent_releases = email.announcement_calls[0]["releases"]
        self.assertEqual(
            [release["version"] for release in sent_releases],
            ["3.0.1", "3.0.2"],
        )
        self.assertEqual(second.get("status"), "NO_PENDING_RELEASES")

    def test_app_update_release_notifier_marks_release_seen_when_no_users_are_deliverable(self):
        class _FakeEmailService:
            def __init__(self) -> None:
                self.confirmation_calls: list[dict] = []
                self.announcement_calls: list[dict] = []

            def send_release_confirmation(self, **kwargs):
                self.confirmation_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": True,
                    "message": "release confirmation sent",
                }

            def send_release_announcement(self, **kwargs):
                self.announcement_calls.append(dict(kwargs))
                return {
                    "ok": True,
                    "sent": False,
                    "message": "No signed-in user email recipients were available",
                    "recipients": [],
                    "sent_recipients": [],
                    "failed_recipients": [],
                    "sent_count": 0,
                    "failed_count": 0,
                    "no_deliverable_recipients": True,
                }

        csv_text = (
            "enabled,app_id,channel,audience,platform,version,build_number,apk_url,force,message\n"
            "TRUE,lalacore_rebuild,stable,all,android,3.0.1,17,https://example.com/app.apk,TRUE,Fresh release\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            auth_root = root / "auth"
            auth_root.mkdir(parents=True, exist_ok=True)
            (auth_root / "users.json").write_text(
                json.dumps(
                    {
                        "student@example.com": {
                            "email": "student@example.com",
                            "role": "student",
                        }
                    }
                ),
                encoding="utf-8",
            )
            state = AutomationStateManager(path=str(root / "LC9_AUTOMATION_STATE.json"))
            email = _FakeEmailService()
            service = AppUpdateReleaseNotifierService(
                state=state,
                email_service=email,
                fetcher=lambda url: csv_text,
                sheet_url="https://example.com/updates.csv",
                auth_users_file=auth_root / "users.json",
                auth_storage_db_file=auth_root / "auth_store.sqlite3",
            )

            first = asyncio.run(service.poll_for_new_releases(trigger="manual"))
            first_snapshot = service.status_snapshot()
            second = asyncio.run(service.poll_for_new_releases(trigger="manual"))
            snapshot = service.status_snapshot()

        self.assertTrue(first.get("ok"))
        self.assertEqual(first.get("status"), "NO_DELIVERABLE_RECIPIENTS")
        self.assertEqual(second.get("status"), "NO_NEW_RELEASE")
        self.assertEqual(first_snapshot.get("last_status"), "no_deliverable_recipients")
        self.assertEqual(snapshot.get("last_status"), "no_new_release")
        self.assertEqual(snapshot.get("seen_release_count"), 1)
        self.assertEqual(len(email.confirmation_calls), 0)
        self.assertEqual(len(email.announcement_calls), 1)

    def test_feeder_enqueue_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = AutomationStateManager(path=str(root / "LC9_AUTOMATION_STATE.json"))
            mini = MiniEvolutionEngine(
                state_path=str(root / "mini_state.json"),
                disagreement_path=str(root / "mini_disagreements.jsonl"),
                replay_queue_path=str(root / "mini_queue.jsonl"),
            )
            token_guard = TokenBudgetGuardian(path=str(root / "token_budget.json"))

            feeder = FeederEngine(
                queue_path=str(root / "LC9_FEEDER_QUEUE.jsonl"),
                training_cases_path=str(root / "LC9_FEEDER_CASES.jsonl"),
                replay_cases_path=str(root / "feeder_cases.jsonl"),
                state_manager=state,
                mini_evolution=mini,
                token_guardian=token_guard,
            )

            first = feeder.enqueue_question(
                question="What is 6*7?",
                subject="math",
                difficulty="easy",
                concept_cluster=["arithmetic"],
            )
            second = feeder.enqueue_question(
                question="What is 6*7?",
                subject="math",
                difficulty="easy",
                concept_cluster=["arithmetic"],
            )

            self.assertTrue(first["added"])
            self.assertFalse(first["duplicate"])
            self.assertFalse(second["added"])
            self.assertTrue(second["duplicate"])

            status = feeder.status(limit=5)
            self.assertEqual(status["total"], 1)
            self.assertEqual(status["counts"].get("Pending"), 1)

    def test_state_manager_recovers_stale_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "LC9_AUTOMATION_STATE.json")
            state = AutomationStateManager(path=state_path)
            state.start_job("weekly_automation", run_id="r1", trigger="manual", resume=False)

            # Force stale timestamp.
            state.state["jobs"]["weekly_automation"]["last_start_ts"] = (
                datetime.now(timezone.utc) - timedelta(hours=12)
            ).isoformat()
            state.state["jobs"]["weekly_automation"]["status"] = "running"
            state.state["jobs"]["weekly_automation"]["completed_stages"] = ["feeder_refresh"]
            state.path.write_text(json.dumps(state.state), encoding="utf-8")
            state = AutomationStateManager(path=state_path)

            changed = state.recover_stale_job("weekly_automation", stale_after_minutes=60)
            self.assertTrue(changed)
            row = state.get_job("weekly_automation")
            self.assertEqual(row.get("status"), "failed")
            self.assertIn("recovered_stale", str(row.get("last_error", "")))

    def test_orchestrator_run_if_due_skips_when_not_due(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = AutomationStateManager(path=str(Path(tmp) / "LC9_AUTOMATION_STATE.json"))
            state.start_job("weekly_automation", run_id="r2", trigger="manual", resume=False)
            state.mark_job_complete("weekly_automation", duration_s=0.1)

            orchestrator = AutomationOrchestrator(state_manager=state)
            out = asyncio.run(orchestrator.run_if_due(min_interval_days=7))
            self.assertTrue(out.get("ok"))
            self.assertTrue(out.get("skipped"))
            self.assertEqual(out.get("reason"), "not_due")

    def test_atlas_maintenance_runs_once_per_window(self):
        class _FakeAppData:
            def __init__(self) -> None:
                self.calls: list[dict] = []

            async def handle_action(self, payload: dict) -> dict:
                self.calls.append(payload)
                return {
                    "ok": True,
                    "status": "SUCCESS",
                    "incident_id": "atlas_incident_1",
                    "mail_sent": True,
                    "self_heal": {"applied_fix_count": 1},
                }

        class _FakeAuditor:
            def expected_duration_seconds(self) -> int:
                return 120

            async def run(self) -> dict:
                return {
                    "overall_status": "healthy",
                    "failing_areas": [],
                    "degraded_areas": [],
                    "artifacts_isolated": True,
                    "areas": {
                        "auth_login": {"status": "healthy", "iterations": 2},
                        "ai_quiz_pipeline": {"status": "healthy", "iterations": 2},
                    },
                }

        with tempfile.TemporaryDirectory() as tmp:
            previous_tz = os.environ.get("ATLAS_MAINTENANCE_TZ")
            os.environ["ATLAS_MAINTENANCE_TZ"] = "UTC"
            try:
                state = AutomationStateManager(path=str(Path(tmp) / "LC9_AUTOMATION_STATE.json"))
                fake = _FakeAppData()
                service = AtlasMaintenanceService(
                    app_data=fake,
                    state=state,
                    auditor=_FakeAuditor(),
                )
                saturday_window = datetime(2026, 3, 28, 1, 15, tzinfo=timezone.utc)

                first = asyncio.run(service.run_if_due(now=saturday_window))
                second = asyncio.run(service.run_if_due(now=saturday_window))

                self.assertTrue(first.get("ok"))
                self.assertFalse(first.get("skipped", False))
                self.assertTrue(second.get("skipped"))
                self.assertEqual(second.get("reason"), "already_ran_this_window")
                self.assertEqual(len(fake.calls), 1)
            finally:
                if previous_tz is None:
                    os.environ.pop("ATLAS_MAINTENANCE_TZ", None)
                else:
                    os.environ["ATLAS_MAINTENANCE_TZ"] = previous_tz

    def test_atlas_maintenance_lock_middleware_blocks_normal_routes(self):
        class _FakeService:
            def is_running(self) -> bool:
                return True

            def status_snapshot(self) -> dict:
                return {
                    "running": True,
                    "phase": "auditing",
                    "estimated_duration_minutes": 7.5,
                }

        app = FastAPI()
        app.add_middleware(AtlasMaintenanceLockMiddleware, service=_FakeService())

        @app.get("/health")
        async def health():
            return {"ok": True}

        @app.get("/hello")
        async def hello():
            return {"ok": True}

        @app.get("/ops/atlas-maintenance/status")
        async def maintenance_status():
            return {"ok": True}

        with TestClient(app) as client:
            blocked = client.get("/hello")
            allowed_health = client.get("/health")
            allowed_ops = client.get("/ops/atlas-maintenance/status")

        self.assertEqual(blocked.status_code, 503)
        self.assertEqual(blocked.json().get("status"), "MAINTENANCE")
        self.assertEqual(allowed_health.status_code, 200)
        self.assertEqual(allowed_ops.status_code, 200)

    def test_maintenance_schedule_probe_accepts_schedule_response_shape(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        app_routes, live_classes_api = auditor._runtime_modules()
        with tempfile.TemporaryDirectory() as tmp:
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
            with auditor._isolated_backend(
                app_data=app_data,
                auth=auth,
                app_routes=app_routes,
                live_classes_api=live_classes_api,
            ):
                app = FastAPI()
                app.include_router(app_routes.router)
                app.include_router(live_classes_api.router)
                with TestClient(app) as client:
                    result = auditor._probe_schedule_pipeline(
                        client,
                        0,
                        adaptive=False,
                    )
        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("list_count"), 1)

    def test_maintenance_live_class_signature_helper_ignores_valid_single_question_payload(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "live_class_ai",
            1,
            lambda idx, adaptive: {
                "ok": True,
                "degraded": False,
                "explain_citations": 0,
                "quiz_items": 1,
                "quiz_detected": True,
                "quiz_payload_shape": "single_question",
                "provider_error_detected": False,
            },
        )
        self.assertEqual(summary.get("status"), "healthy")
        self.assertEqual(auditor._area_failure_signatures("live_class_ai", summary), [])

    def test_maintenance_material_ai_signature_helper_flags_empty_modes(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "material_ai_pipeline",
            1,
            lambda idx, adaptive: {
                "ok": False,
                "degraded": True,
                "empty_modes": ["formula_sheet", "quiz_drill"],
            },
        )
        signatures = auditor._area_failure_signatures("material_ai_pipeline", summary)
        self.assertTrue(
            any(sig.get("code") == "material_ai_mode_empty_output" for sig in signatures)
        )

    def test_maintenance_material_ai_query_signature_helper_flags_blank_output(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "material_ai_query_pipeline",
            1,
            lambda idx, adaptive: {
                "ok": False,
                "degraded": True,
                "answer_signal": 0,
            },
        )
        signatures = auditor._area_failure_signatures(
            "material_ai_query_pipeline",
            summary,
        )
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("material_ai_query_blank", codes)

    def test_maintenance_teacher_authoring_signature_helper_flags_weak_questions(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "teacher_authoring_ai_pipeline",
            1,
            lambda idx, adaptive: {
                "ok": True,
                "degraded": True,
                "question_count": 3,
                "weak_question_count": 2,
                "weak_question_indices": [1, 3],
            },
        )
        signatures = auditor._area_failure_signatures(
            "teacher_authoring_ai_pipeline",
            summary,
        )
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("teacher_authoring_ai_weak_questions", codes)

    def test_maintenance_app_atlas_signature_helper_flags_invalid_plan(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "app_atlas_planner",
            1,
            lambda idx, adaptive: {
                "ok": False,
                "degraded": True,
                "plan_valid": False,
                "disallowed_tool_count": 1,
            },
        )
        signatures = auditor._area_failure_signatures("app_atlas_planner", summary)
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("app_atlas_plan_shape_invalid", codes)
        self.assertIn("app_atlas_disallowed_tool_selected", codes)

    def test_maintenance_app_atlas_signature_helper_tracks_truncated_plan_recovery(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "app_atlas_planner",
            1,
            lambda idx, adaptive: {
                "ok": True,
                "degraded": False,
                "plan_valid": True,
                "disallowed_tool_count": 0,
                "recovery_mode": "tool_mentions_from_reasoning",
            },
        )
        signatures = auditor._area_failure_signatures("app_atlas_planner", summary)
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("app_atlas_truncated_plan_recovered", codes)

    def test_maintenance_attachment_import_signature_helper_flags_missing_questions(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "attachment_import_ai",
            1,
            lambda idx, adaptive: {
                "ok": False,
                "degraded": True,
                "question_count": 0,
            },
        )
        signatures = auditor._area_failure_signatures("attachment_import_ai", summary)
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("attachment_import_ai_no_questions", codes)

    def test_maintenance_live_class_support_signature_helper_flags_missing_evidence(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "live_class_support_ai",
            1,
            lambda idx, adaptive: {
                "ok": False,
                "degraded": True,
                "citation_count": 0,
            },
        )
        signatures = auditor._area_failure_signatures("live_class_support_ai", summary)
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("live_class_support_missing_evidence", codes)

    def test_maintenance_live_attachment_poll_signature_helper_flags_invalid_output(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "live_attachment_poll_ai",
            1,
            lambda idx, adaptive: {
                "ok": False,
                "degraded": True,
                "option_count": 1,
            },
        )
        signatures = auditor._area_failure_signatures("live_attachment_poll_ai", summary)
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("live_attachment_poll_invalid", codes)

    def test_maintenance_live_agent_signature_helper_tracks_truncated_plan_recovery(self):
        auditor = _AtlasPipelineMaintenanceAuditor()
        summary = auditor._run_probe_series(
            "live_class_agent_planner",
            1,
            lambda idx, adaptive: {
                "ok": True,
                "degraded": False,
                "agent_plan_valid": True,
                "recovery_mode": "tool_mentions_from_reasoning",
            },
        )
        signatures = auditor._area_failure_signatures("live_class_agent_planner", summary)
        codes = {sig.get("code") for sig in signatures}
        self.assertIn("live_class_agent_truncated_plan_recovered", codes)


if __name__ == "__main__":
    unittest.main()
