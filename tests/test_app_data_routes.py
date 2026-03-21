import base64
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import urlparse

from fastapi.testclient import TestClient

os.environ.setdefault("OTP_EMAIL_ENABLED", "false")

import app.routes as routes  # noqa: E402
from app.data.local_app_data_service import LocalAppDataService  # noqa: E402
from app.main import app  # noqa: E402


class AppDataRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self._original = routes._APP_DATA
        routes._APP_DATA = LocalAppDataService(
            assessments_file=root / "assessments.json",
            materials_file=root / "materials.json",
            live_class_schedule_file=root / "live_class_schedule.json",
            uploads_file=root / "uploads.json",
            import_drafts_file=root / "import_drafts.json",
            import_question_bank_file=root / "import_question_bank.json",
        )
        self.client = TestClient(app)

    def tearDown(self) -> None:
        routes._APP_DATA = self._original
        self._tmp.cleanup()

    def test_create_quiz_and_read_csv(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Kinematics Drill",
                "type": "Exam",
                "duration": 30,
                "questions": [{"text": "v = u + at", "correct": "formula"}],
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        quiz_url = str(body.get("quiz_url") or body.get("url") or "")
        self.assertTrue(quiz_url)

        csv_path = urlparse(quiz_url).path
        csv_res = self.client.get(csv_path)
        self.assertEqual(csv_res.status_code, 200)
        self.assertIn("Question", csv_res.text)
        self.assertIn("v = u + at", csv_res.text)

        master = self.client.get("/app/action", params={"action": "get_master_csv"})
        self.assertEqual(master.status_code, 200)
        self.assertTrue(master.json().get("ok"))
        self.assertIn("Kinematics Drill", master.json().get("csv", ""))

    def test_add_material_and_list(self) -> None:
        add = self.client.post(
            "/app/action",
            json={
                "action": "add_material",
                "title": "Electrostatics Notes",
                "type": "pdf",
                "url": "https://example.com/electrostatics.pdf",
                "class": "Class 12",
                "subject": "Physics",
                "chapters": "Electrostatics",
                "role": "teacher",
            },
        )
        self.assertEqual(add.status_code, 200)
        self.assertTrue(add.json().get("ok"))

        out = self.client.get("/app/action", params={"action": "get_materials"})
        self.assertEqual(out.status_code, 200)
        self.assertTrue(out.json().get("ok"))
        items = out.json().get("list", [])
        self.assertTrue(any(x.get("title") == "Electrostatics Notes" for x in items))

    def test_add_material_forbidden_for_student_role(self) -> None:
        add = self.client.post(
            "/app/action",
            json={
                "action": "add_material",
                "title": "Student Attempted Upload",
                "type": "pdf",
                "url": "https://example.com/student.pdf",
                "class": "Class 11",
                "subject": "Physics",
                "chapters": "Kinematics",
                "role": "student",
            },
        )
        self.assertEqual(add.status_code, 200)
        body = add.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "FORBIDDEN")

    def test_schedule_live_class_teacher_and_student_flow(self) -> None:
        scheduled = self.client.post(
            "/app/action",
            json={
                "action": "schedule_live_class",
                "role": "teacher",
                "teacher_id": "teacher_1",
                "teacher_name": "A. Teacher",
                "class_name": "Class 12",
                "title": "Definite Integration Marathon",
                "subject": "Mathematics",
                "topic": "Definite Integration",
                "start_time": "2026-03-12T10:00:00Z",
                "duration_minutes": 90,
                "description": "PYQ-heavy revision",
            },
        )
        self.assertEqual(scheduled.status_code, 200)
        schedule_body = scheduled.json()
        self.assertTrue(schedule_body.get("ok"))
        class_item = schedule_body.get("class", {})
        self.assertEqual(class_item.get("status"), "upcoming")
        class_id = class_item.get("class_id")
        self.assertTrue(class_id)

        teacher_list = self.client.get(
            "/app/action",
            params={
                "action": "list_live_class_schedule",
                "viewer_role": "teacher",
                "viewer_id": "teacher_1",
            },
        )
        self.assertEqual(teacher_list.status_code, 200)
        teacher_rows = teacher_list.json().get("schedule", [])
        self.assertTrue(any(row.get("class_id") == class_id for row in teacher_rows))

        student_list = self.client.get(
            "/app/action",
            params={
                "action": "list_live_class_schedule",
                "viewer_role": "student",
                "viewer_id": "student_22",
            },
        )
        self.assertEqual(student_list.status_code, 200)
        student_rows = student_list.json().get("schedule", [])
        self.assertTrue(any(row.get("class_id") == class_id for row in student_rows))

        live = self.client.post(
            "/app/action",
            json={
                "action": "update_class_schedule_status",
                "class_id": class_id,
                "status": "live",
            },
        )
        self.assertEqual(live.status_code, 200)
        live_body = live.json()
        self.assertTrue(live_body.get("ok"))
        self.assertEqual(live_body.get("class", {}).get("status"), "live")

    def test_schedule_live_class_forbidden_for_student_role(self) -> None:
        scheduled = self.client.post(
            "/app/action",
            json={
                "action": "schedule_live_class",
                "role": "student",
                "teacher_id": "teacher_1",
                "teacher_name": "A. Teacher",
                "class_name": "Class 12",
                "title": "Bad Publish",
                "subject": "Mathematics",
                "topic": "Complex Numbers",
                "start_time": "2026-03-12T10:00:00Z",
            },
        )
        self.assertEqual(scheduled.status_code, 200)
        body = scheduled.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "FORBIDDEN")

    def test_live_class_schedule_events_websocket_emits_create_and_status(self) -> None:
        with self.client.websocket_connect("/app/live_class_schedule/events") as ws:
            connected = ws.receive_json()
            self.assertEqual(connected.get("type"), "connected")

            scheduled = self.client.post(
                "/app/action",
                json={
                    "action": "schedule_live_class",
                    "role": "teacher",
                    "teacher_id": "teacher_9",
                    "teacher_name": "A. Teacher",
                    "class_name": "Class 12",
                    "title": "Realtime Schedule Test",
                    "subject": "Mathematics",
                    "topic": "Complex Numbers",
                    "start_time": "2026-03-12T10:00:00Z",
                    "duration_minutes": 60,
                },
            )
            self.assertEqual(scheduled.status_code, 200)
            class_id = scheduled.json().get("class", {}).get("class_id")
            self.assertTrue(class_id)

            created = ws.receive_json()
            self.assertEqual(created.get("type"), "schedule_created")
            self.assertEqual(created.get("class_id"), class_id)
            self.assertEqual(created.get("class", {}).get("status"), "upcoming")

            live = self.client.post(
                "/app/action",
                json={
                    "action": "update_class_schedule_status",
                    "class_id": class_id,
                    "status": "live",
                },
            )
            self.assertEqual(live.status_code, 200)

            updated = ws.receive_json()
            self.assertEqual(updated.get("type"), "schedule_status_changed")
            self.assertEqual(updated.get("class_id"), class_id)
            self.assertEqual(updated.get("class", {}).get("status"), "live")

    def test_create_quiz_forbidden_for_student_role(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Student Publish Attempt",
                "type": "Exam",
                "duration": 20,
                "role": "student",
                "questions": [{"text": "x+y", "correct": "2"}],
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "FORBIDDEN")

    def test_upload_file_and_download(self) -> None:
        encoded = base64.b64encode(b"hello-file").decode("ascii")
        up = self.client.post(
            "/app/action",
            json={
                "action": "upload_file",
                "name": "sample.txt",
                "data": f"data:text/plain;base64,{encoded}",
            },
        )
        self.assertEqual(up.status_code, 200)
        body = up.json()
        self.assertTrue(body.get("ok"))
        file_url = str(body.get("url") or "")
        self.assertTrue(file_url)

        file_path = urlparse(file_url).path
        downloaded = self.client.get(file_path)
        self.assertEqual(downloaded.status_code, 200)
        self.assertEqual(downloaded.content, b"hello-file")

    def test_ai_generate_and_secure_evaluation_flow(self) -> None:
        generated = self.client.post(
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Physics",
                "chapters": ["Kinematics", "NLM"],
                "subtopics": ["Relative Velocity", "Pseudo Force"],
                "difficulty": 4,
                "question_count": 6,
                "trap_intensity": "high",
                "weakness_mode": True,
                "cross_concept": True,
                "user_id": "stu_11",
                "role": "student",
                "self_practice_mode": True,
                "title": "AI Physics Challenge",
            },
        )
        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertEqual(body.get("metadata", {}).get("engine_mode"), "ELITE_MODE")

        quiz_id = str(body.get("quiz_id") or "")
        self.assertTrue(quiz_id)
        questions = body.get("questions_json", [])
        self.assertEqual(len(questions), 6)
        for q in questions:
            self.assertNotIn("correct_option", q)
            self.assertNotIn("solution_explanation", q)
            self.assertTrue(q.get("question_text"))
            self.assertEqual(len(q.get("options", [])), 4)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {str(i): ["A"] for i in range(6)},
                "student_name": "Ritam",
                "student_id": "stu_11",
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        evaluated_body = evaluated.json()
        self.assertTrue(evaluated_body.get("ok"))
        answer_key = evaluated_body.get("answer_key", [])
        self.assertEqual(len(answer_key), 0)
        self.assertEqual(len(evaluated_body.get("per_question_result", [])), 6)

        results = self.client.get("/app/action", params={"action": "get_results"})
        self.assertEqual(results.status_code, 200)
        self.assertTrue(results.json().get("ok"))
        self.assertTrue(
            any(r.get("quiz_id") == quiz_id for r in results.json().get("list", []))
        )

        queued = self.client.post(
            "/app/action",
            json={
                "action": "queue_teacher_review",
                "quiz_id": quiz_id,
                "question_id": "1",
                "student_answer": "A) 10",
                "correct_answer": "B) 12",
                "student_id": "stu_11",
                "message": "Need review",
            },
        )
        self.assertEqual(queued.status_code, 200)
        self.assertTrue(queued.json().get("ok"))

        queue = self.client.get(
            "/app/action",
            params={"action": "get_teacher_review_queue"},
        )
        self.assertEqual(queue.status_code, 200)
        self.assertTrue(queue.json().get("ok"))
        self.assertTrue(any(x.get("quiz_id") == quiz_id for x in queue.json().get("list", [])))

    def test_ai_generate_pyq_strict_mode_uses_ranked_web_sources(self) -> None:
        web_row = {
            "title": "JEE Advanced Binomial PYQ",
            "url": "https://example.com/jee-binomial-pyq",
            "snippet": "Hard PYQ on coefficient extraction and greatest term",
            "query": "binomial theorem jee pyq hard",
            "scope_score": 0.92,
            "pyq_score": 1.0,
            "hardness_score": 0.88,
            "quality_score": 1.14,
            "question_text": "In the expansion of (1+x)^12, the coefficient of x^5 is:",
            "options": ["792", "462", "924", "950"],
            "correct_answer": "A",
            "question_stub": "In (1+x)^12, find the coefficient of x^5 ?",
            "answer_stub": "A",
            "solution_stub": "Use general term and match the power of x.",
            "has_answer": True,
            "has_solution": True,
        }
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
            side_effect=[[dict(web_row)], [dict(web_row)]],
        ):
            generated = self.client.post(
                "/app/action",
                json={
                    "action": "ai_generate_quiz",
                    "subject": "Mathematics",
                    "chapters": ["Binomial Theorem"],
                    "subtopics": ["Coefficient", "General Term"],
                    "difficulty": 5,
                    "question_count": 3,
                    "trap_intensity": "high",
                    "role": "teacher",
                    "authoring_mode": True,
                    "self_practice_mode": False,
                    "include_answer_key": True,
                    "pyq_focus": True,
                    "search_hard_pyq": True,
                    "pyq_mode": "strict_related_web",
                    "pyq_web_only_mode": True,
                },
            )

        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        policy = body.get("source_policy", {})
        self.assertEqual(policy.get("mode"), "pyq_related_web_only")
        self.assertGreaterEqual(policy.get("web_source_applied_count", 0), 1)
        questions = body.get("questions_json", [])
        self.assertEqual(len(questions), 3)
        self.assertTrue(
            all(str(q.get("source_origin", "")).startswith("web_pyq") for q in questions)
        )
        self.assertTrue(
            any(
                "example.com/jee-binomial-pyq" in str(q.get("source_url", ""))
                for q in questions
            )
        )

    def test_ai_generate_pyq_hybrid_mode_mixes_online_and_offline_sources(self) -> None:
        web_rows = [
            {
                "title": "Definite Integration PYQ Web",
                "url": "https://example.com/jee-definite-integral-1",
                "snippet": "Evaluate integral with parameter",
                "query": "definite integral jee pyq hard",
                "scope_score": 0.94,
                "pyq_score": 0.96,
                "hardness_score": 0.88,
                "quality_score": 1.08,
                "question_text": "If I = integral from 0 to 1 of x(1-x) dx, then 24I equals:",
                "options": ["4", "6", "8", "12"],
                "correct_answer": "A",
                "question_stub": "Compute I = int_0^1 x(1-x) dx and find 24I.",
                "answer_stub": "A",
                "solution_stub": "Integrate polynomial and simplify.",
                "has_answer": True,
                "has_solution": True,
            },
            {
                "title": "Definite Integration PYQ Offline",
                "url": "local://question_bank/def_int_2",
                "snippet": "Definite integral substitution",
                "query": "local_import_bank",
                "scope_score": 0.91,
                "pyq_score": 0.95,
                "hardness_score": 0.82,
                "quality_score": 1.01,
                "question_text": "Let J = integral from 0 to pi/2 of sin x dx. Then 2J equals:",
                "options": ["1", "2", "pi", "4"],
                "correct_answer": "B",
                "question_stub": "Compute 2 * int_0^(pi/2) sin x dx.",
                "answer_stub": "B",
                "solution_stub": "Use antiderivative of sin x.",
                "has_answer": True,
                "has_solution": True,
                "source_provider": "local_pyq_import_bank",
            },
        ]
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
            side_effect=[[dict(x) for x in web_rows], [dict(x) for x in web_rows]],
        ):
            generated = self.client.post(
                "/app/action",
                json={
                    "action": "ai_generate_quiz",
                    "subject": "Mathematics",
                    "chapters": ["Definite Integration"],
                    "subtopics": ["Definite Integration"],
                    "difficulty": 5,
                    "question_count": 2,
                    "trap_intensity": "high",
                    "role": "teacher",
                    "authoring_mode": True,
                    "self_practice_mode": False,
                    "include_answer_key": True,
                    "pyq_focus": True,
                    "search_hard_pyq": True,
                    "pyq_mode": "related_web",
                },
            )

        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        policy = body.get("source_policy", {})
        self.assertEqual(policy.get("mode"), "hybrid")
        self.assertTrue(policy.get("mixed_pyq_source_mode"))
        self.assertGreaterEqual(policy.get("web_source_online_applied_count", 0), 1)
        self.assertGreaterEqual(policy.get("web_source_offline_applied_count", 0), 1)
        self.assertFalse(policy.get("fallback_used"))
        source_urls = [str(q.get("source_url") or "") for q in body.get("questions_json", [])]
        self.assertTrue(any(url.startswith("https://") for url in source_urls))
        self.assertTrue(any(url.startswith("local://") for url in source_urls))

    def test_ai_generate_pyq_tries_backup_web_rows_before_synthesis(self) -> None:
        unusable_top_row = {
            "title": "High score but incomplete",
            "url": "https://example.com/unusable-top-row",
            "snippet": "No parsable answer/options",
            "query": "definite integral jee pyq",
            "scope_score": 0.99,
            "pyq_score": 0.97,
            "hardness_score": 0.89,
            "quality_score": 1.3,
            "question_text": "Evaluate integral from 0 to 1 of x^2 dx.",
            "options": [],
            "correct_answer": "",
            "question_stub": "Evaluate int_0^1 x^2 dx.",
            "answer_stub": "",
            "solution_stub": "",
            "has_answer": False,
            "has_solution": False,
        }
        backup_row = {
            "title": "Valid backup PYQ row",
            "url": "https://example.com/usable-backup-row",
            "snippet": "Has full options and answer",
            "query": "definite integral jee pyq",
            "scope_score": 0.94,
            "pyq_score": 0.95,
            "hardness_score": 0.86,
            "quality_score": 1.1,
            "question_text": "If K = integral from 0 to 1 of (2x+1) dx, then K is:",
            "options": ["2", "3", "4", "5"],
            "correct_answer": "A",
            "question_stub": "Compute int_0^1 (2x+1) dx.",
            "answer_stub": "A",
            "solution_stub": "Integrate termwise and evaluate.",
            "has_answer": True,
            "has_solution": True,
        }
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
            side_effect=[
                [dict(unusable_top_row), dict(backup_row)],
                [dict(unusable_top_row), dict(backup_row)],
            ],
        ):
            generated = self.client.post(
                "/app/action",
                json={
                    "action": "ai_generate_quiz",
                    "subject": "Mathematics",
                    "chapters": ["Definite Integration"],
                    "subtopics": ["Definite Integration"],
                    "difficulty": 5,
                    "question_count": 1,
                    "trap_intensity": "high",
                    "role": "teacher",
                    "authoring_mode": True,
                    "self_practice_mode": False,
                    "include_answer_key": True,
                    "pyq_focus": True,
                    "search_hard_pyq": True,
                    "pyq_mode": "related_web",
                },
            )

        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        questions = body.get("questions_json", [])
        self.assertEqual(len(questions), 1)
        self.assertEqual(questions[0].get("source_url"), "https://example.com/usable-backup-row")
        self.assertTrue(str(questions[0].get("source_origin", "")).startswith("web_pyq"))

    def test_ai_generate_pyq_recovers_solution_when_web_answer_missing(self) -> None:
        weak_web_row = {
            "title": "JEE PYQ Binomial",
            "url": "https://example.com/jee-binomial-no-answer",
            "snippet": "PYQ statement without answer key",
            "query": "binomial theorem jee pyq",
            "scope_score": 0.81,
            "pyq_score": 0.92,
            "hardness_score": 0.75,
            "quality_score": 0.86,
            "question_text": "The middle term in (1+x)^10 has coefficient:",
            "options": ["120", "252", "210", "126"],
            "correct_answer": "B",
            "question_stub": "Find the middle term in (1+x)^10 ?",
            "answer_stub": "B",
            "solution_stub": "",
            "has_answer": True,
            "has_solution": False,
        }
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
            side_effect=[[dict(weak_web_row)], [dict(weak_web_row)]],
        ), patch.object(
            routes._APP_DATA,
            "_recover_solution_via_ai_engine",
            new=AsyncMock(
                return_value={
                    "answer_token": "B",
                    "solution_explanation": "Step 1: Write general term. Step 2: Match exponent and simplify.",
                }
            ),
        ):
            generated = self.client.post(
                "/app/action",
                json={
                    "action": "ai_generate_quiz",
                    "subject": "Mathematics",
                    "chapters": ["Binomial Theorem"],
                    "subtopics": ["Middle Term"],
                    "difficulty": 5,
                    "question_count": 2,
                    "trap_intensity": "high",
                    "role": "teacher",
                    "authoring_mode": True,
                    "self_practice_mode": False,
                    "include_answer_key": True,
                    "pyq_focus": True,
                    "search_hard_pyq": True,
                    "pyq_mode": "strict_related_web",
                    "pyq_web_only_mode": True,
                    "pyq_answer_retrieval_required": True,
                },
            )

        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        policy = body.get("source_policy", {})
        self.assertGreaterEqual(policy.get("ai_solution_recovery_count", 0), 1)
        self.assertTrue(policy.get("answer_sources_verified"))
        questions = body.get("questions_json", [])
        self.assertEqual(len(questions), 2)
        for q in questions:
            self.assertTrue((q.get("solution_explanation") or "").strip())
            self.assertIn(
                q.get("source_origin"),
                {"web_pyq_ai_solution", "web_pyq_verified", "ai_synth_ultra_verified"},
            )

    def test_ai_generate_pyq_strict_mode_requires_verified_web_fetch(self) -> None:
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
            side_effect=[[], []],
        ):
            generated = self.client.post(
                "/app/action",
                json={
                    "action": "ai_generate_quiz",
                    "subject": "Mathematics",
                    "chapters": ["Integration"],
                    "subtopics": ["Definite Integral"],
                    "difficulty": 5,
                    "question_count": 1,
                    "trap_intensity": "high",
                    "role": "teacher",
                    "authoring_mode": True,
                    "self_practice_mode": False,
                    "include_answer_key": True,
                    "pyq_focus": True,
                    "search_hard_pyq": True,
                    "pyq_mode": "strict_related_web",
                    "pyq_web_only_mode": True,
                },
            )

        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "PARTIAL_SUCCESS")
        self.assertEqual(body.get("error_reason"), "insufficient_ultra_hard_verified_questions")
        self.assertEqual(body.get("web_source_applied_count"), 0)
        self.assertTrue(body.get("web_error_reason"))
        self.assertIn("web_provider_diagnostics", body)

    def test_teacher_can_request_answer_key_on_submission(self) -> None:
        generated = self.client.post(
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Mathematics",
                "chapters": ["Permutation and Combination"],
                "subtopics": ["Selections"],
                "difficulty": 4,
                "question_count": 3,
                "role": "teacher",
                "authoring_mode": True,
                "self_practice_mode": False,
                "include_answer_key": True,
                "user_id": "teacher_007",
            },
        )
        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        quiz_id = str(body.get("quiz_id") or "")
        self.assertTrue(quiz_id)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {str(i): ["A"] for i in range(3)},
                "role": "teacher",
                "include_answer_key": True,
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        evaluated_body = evaluated.json()
        self.assertTrue(evaluated_body.get("ok"))
        answer_key = evaluated_body.get("answer_key", [])
        self.assertEqual(len(answer_key), 3)
        self.assertTrue(any("correct_option" in row for row in answer_key))

    def test_student_can_request_answer_key_after_submission(self) -> None:
        generated = self.client.post(
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Mathematics",
                "chapters": ["Complex Numbers"],
                "subtopics": ["Argand Plane"],
                "difficulty": 4,
                "question_count": 3,
                "role": "student",
                "self_practice_mode": True,
                "user_id": "student_101",
            },
        )
        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        quiz_id = str(body.get("quiz_id") or "")
        self.assertTrue(quiz_id)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {str(i): ["A"] for i in range(3)},
                "role": "student",
                "include_answer_key": True,
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        evaluated_body = evaluated.json()
        self.assertTrue(evaluated_body.get("ok"))
        answer_key = evaluated_body.get("answer_key", [])
        self.assertEqual(len(answer_key), 3)
        self.assertTrue(
            all((row.get("question_text") or "").strip() for row in answer_key)
        )
        self.assertTrue(
            any((row.get("solution_explanation") or "").strip() for row in answer_key)
        )

    def test_ai_chat_action_uses_lalacore_entrypoint(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "x = 4",
            "reasoning": "Solve x + 1 = 5.",
            "winner_provider": "gemini",
            "engine": {"version": "research-grade-v2"},
        }
        with patch(
            "core.api.entrypoint.lalacore_entry",
            new=AsyncMock(return_value=fake_result),
        ) as mocked:
            res = self.client.post(
                "/app/action",
                json={
                    "action": "ai_chat",
                    "prompt": "Solve x + 1 = 5",
                    "user_id": "u1",
                    "chat_id": "c1",
                    "options": {"response_style": "exam_coach"},
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("provider"), "gemini")
        self.assertEqual(body.get("model"), "research-grade-v2")
        self.assertIn("x = 4", str(body.get("answer", "")))
        self.assertEqual(mocked.await_count, 1)

    def test_ai_chat_empty_engine_payload_returns_failed_empty_result(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "",
            "reasoning": "",
            "winner_provider": "gemini",
            "engine": {"version": "research-grade-v2"},
            "final_status": "Failed",
            "quality_gate": {"reasons": ["verification_failed_high_risk"]},
        }
        with patch(
            "core.api.entrypoint.lalacore_entry",
            new=AsyncMock(return_value=fake_result),
        ) as mocked:
            res = self.client.post(
                "/app/action",
                json={
                    "action": "ai_chat",
                    "prompt": "Solve x + 1 = 5",
                    "user_id": "u1",
                    "chat_id": "c1",
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertFalse(body.get("ok", True))
        self.assertEqual(body.get("status"), "FAILED_EMPTY_RESULT")
        self.assertIn(
            "verification_failed_high_risk",
            list(body.get("quality_reasons", [])),
        )
        self.assertIn("no usable answer", str(body.get("message", "")).lower())
        self.assertEqual(mocked.await_count, 1)

    def test_ai_question_search_endpoint(self) -> None:
        fake_matches = [
            {
                "url": "https://physics.stackexchange.com/q/123",
                "title": "Integral of x from 0 to 1",
                "similarity": 0.91,
                "snippet": "Compute integral from 0 to 1 of x dx",
                "source": "stackexchange",
            }
        ]
        with patch.object(
            routes._QUESTION_SEARCH_ENGINE,
            "search",
            new=AsyncMock(
                return_value={
                    "query": "integral from 0 to 1 of x dx",
                    "matches": fake_matches,
                    "cache_hit": True,
                    "query_variants": [{"kind": "exact", "query": "integral from 0 to 1 of x dx"}],
                }
            ),
        ):
            response = self.client.post(
                "/ai/question-search",
                json={"query": "Find value of ∫₀¹ x dx (A) 0 (B) 1/2", "max_matches": 6},
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertTrue(body.get("cache_hit"))
        self.assertEqual(len(body.get("matches", [])), 1)
        self.assertIn("normalized_query", body)

    def test_evaluate_route_supports_multi_and_numerical_with_hidden_keys(self) -> None:
        generated = self.client.post(
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Mathematics",
                "chapters": ["General"],
                "difficulty": 3,
                "question_count": 1,
                "role": "student",
                "self_practice_mode": True,
                "user_id": "student_mix",
            },
        )
        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        quiz_id = str(body.get("quiz_id") or "")
        self.assertTrue(quiz_id)

        quiz_row = routes._APP_DATA._find_ai_quiz(quiz_id)  # type: ignore[attr-defined]
        self.assertIsNotNone(quiz_row)
        quiz_row["questions_json"] = json.dumps(
            [
                {
                    "question_id": "m1",
                    "question_type": "MCQ_MULTI",
                    "question_text": "Select prime numbers",
                    "options": ["2", "4", "3", "6"],
                    "_correct_option": "A",
                    "_correct_answers": ["A", "C"],
                    "_numerical_answer": "",
                    "_solution_explanation": "2 and 3 are prime",
                    "partial_marking": True,
                },
                {
                    "question_id": "n1",
                    "question_type": "NUMERICAL",
                    "question_text": "Enter value of pi up to 2 decimals",
                    "options": [],
                    "_correct_option": "",
                    "_correct_answers": [],
                    "_numerical_answer": "3.14",
                    "_solution_explanation": "pi",
                    "numerical_tolerance": 0.02,
                },
            ],
            ensure_ascii=True,
        )

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {
                    "0": ["A", "C"],
                    "1": ["3.141"],
                },
                "role": "student",
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        evaluated_body = evaluated.json()
        self.assertTrue(evaluated_body.get("ok"))
        self.assertEqual(evaluated_body.get("correct"), 2)
        self.assertEqual(evaluated_body.get("answer_key"), [])
        per_question = evaluated_body.get("per_question_result", [])
        self.assertEqual(len(per_question), 2)
        self.assertEqual(
            per_question[0].get("grading_metadata", {}).get("correct_count"),
            2,
        )

    def test_ai_generate_permutation_combination_pipeline_for_teacher_and_student(self) -> None:
        teacher = self.client.post(
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Mathematics",
                "title": "Hard P&C",
                "chapters": ["Permutation and Combination"],
                "subtopics": [
                    "Restricted combinations",
                    "Circular permutations",
                    "Inclusion-Exclusion",
                ],
                "weak_concepts_json": ["Permutation", "Combination"],
                "difficulty": 5,
                "question_count": 5,
                "trap_intensity": "high",
                "weakness_mode": True,
                "cross_concept": True,
                "role": "teacher",
                "authoring_mode": True,
                "self_practice_mode": False,
                "include_answer_key": True,
                "user_id": "teacher_1",
            },
        )
        self.assertEqual(teacher.status_code, 200)
        teacher_body = teacher.json()
        self.assertTrue(teacher_body.get("ok"))
        self.assertEqual(teacher_body.get("status"), "SUCCESS")
        self.assertEqual(teacher_body.get("metadata", {}).get("engine_mode"), "ELITE_MODE")

        teacher_questions = teacher_body.get("questions_json", [])
        self.assertEqual(len(teacher_questions), 5)
        for q in teacher_questions:
            self.assertIn("correct_option", q)
            self.assertTrue((q.get("solution_explanation") or "").strip())
            self.assertEqual(len(q.get("options", [])), 4)
            self.assertIn("$", (q.get("question_text") or ""))
            self.assertIn("Permutation and Combination", q.get("concept_tags", []))

        student = self.client.post(
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Mathematics",
                "title": "Hard P&C Practice",
                "chapters": ["Permutation and Combination"],
                "subtopics": [
                    "Restricted combinations",
                    "Circular permutations",
                    "Inclusion-Exclusion",
                ],
                "weak_concepts_json": ["Permutation", "Combination"],
                "difficulty": 5,
                "question_count": 5,
                "trap_intensity": "high",
                "weakness_mode": True,
                "cross_concept": True,
                "role": "student",
                "authoring_mode": False,
                "self_practice_mode": True,
                "include_answer_key": True,
                "user_id": "student_1",
            },
        )
        self.assertEqual(student.status_code, 200)
        student_body = student.json()
        self.assertTrue(student_body.get("ok"))
        self.assertEqual(student_body.get("status"), "SUCCESS")
        self.assertEqual(student_body.get("metadata", {}).get("engine_mode"), "ELITE_MODE")

        student_questions = student_body.get("questions_json", [])
        self.assertEqual(len(student_questions), 5)
        for q in student_questions:
            self.assertNotIn("correct_option", q)
            self.assertNotIn("solution_explanation", q)
            self.assertNotIn("_correct_option", q)
            self.assertNotIn("_correct_answers", q)
            self.assertNotIn("_numerical_answer", q)
            self.assertEqual(len(q.get("options", [])), 4)
            self.assertIn("Permutation and Combination", q.get("concept_tags", []))

    def test_ai_generate_supports_chapter_picker_subjects_class11_12(self) -> None:
        cases = [
            ("Class 11", "Physics", "Kinematics"),
            ("Class 12", "Chemistry", "Electrochemistry"),
            ("Class 12", "Mathematics", "Integrals"),
            ("Class 12", "Biology", "Evolution"),
        ]
        for idx, (class_name, subject, chapter) in enumerate(cases, start=1):
            res = self.client.post(
                "/app/action",
                json={
                    "action": "ai_generate_quiz",
                    "subject": subject,
                    "title": f"{subject} hard chapter set",
                    "class": class_name,
                    "chapters": [chapter],
                    "subtopics": [chapter],
                    "difficulty": 5,
                    "question_count": 3,
                    "trap_intensity": "high",
                    "weakness_mode": True,
                    "cross_concept": True,
                    "role": "student",
                    "self_practice_mode": True,
                    "authoring_mode": False,
                    "user_id": f"stu_{idx}",
                },
            )
            self.assertEqual(res.status_code, 200)
            body = res.json()
            self.assertTrue(body.get("ok"))
            self.assertEqual(body.get("status"), "SUCCESS")
            questions = body.get("questions_json", [])
            self.assertEqual(len(questions), 3)
            for q in questions:
                self.assertTrue((q.get("question_text") or "").strip())
                self.assertEqual(len(q.get("options", [])), 4)
                self.assertIn(chapter, q.get("concept_tags", []))

    def test_ai_generate_forbidden_for_student_authoring(self) -> None:
        generated = self.client.post(
            "/app/action",
            json={
                "action": "ai_generate_quiz",
                "subject": "Physics",
                "chapters": ["Kinematics"],
                "difficulty": 3,
                "question_count": 3,
                "role": "student",
                "self_practice_mode": False,
                "authoring_mode": True,
            },
        )
        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "FORBIDDEN")

    def test_import_question_pipeline_parse_save_publish_success(self) -> None:
        raw_text = """
1. If x + y = 4 and x - y = 2, then x is:
(1) 1
(2) 2
(3) 3
(4) 4
Ans: 3

2. Integer type: value of 2 + 3 is ____.
Ans: 5
""".strip()

        parsed = self.client.post(
            "/app/action",
            json={
                "action": "lc9_parse_questions",
                "raw_text": raw_text,
                "meta": {
                    "teacher_id": "teacher_1",
                    "subject": "Mathematics",
                    "chapter": "Algebra",
                    "difficulty": "Hard",
                },
            },
        )
        self.assertEqual(parsed.status_code, 200)
        parsed_body = parsed.json()
        self.assertTrue(parsed_body.get("ok"))
        self.assertEqual(parsed_body.get("status"), "SUCCESS")
        self.assertIn("quality_dashboard", parsed_body)
        questions = parsed_body.get("questions", [])
        self.assertEqual(len(questions), 2)

        saved = self.client.post(
            "/app/action",
            json={
                "action": "lc9_save_import_drafts",
                "questions": questions,
                "meta": {
                    "teacher_id": "teacher_1",
                    "subject": "Mathematics",
                    "chapter": "Algebra",
                    "difficulty": "Hard",
                },
            },
        )
        self.assertEqual(saved.status_code, 200)
        saved_body = saved.json()
        self.assertTrue(saved_body.get("ok"))
        self.assertEqual(saved_body.get("status"), "SUCCESS")
        self.assertEqual(saved_body.get("saved_count"), 2)

        published = self.client.post(
            "/app/action",
            json={
                "action": "lc9_publish_questions",
                "questions": questions,
                "meta": {
                    "teacher_id": "teacher_1",
                    "subject": "Mathematics",
                    "chapter": "Algebra",
                    "difficulty": "Hard",
                },
            },
        )
        self.assertEqual(published.status_code, 200)
        published_body = published.json()
        self.assertTrue(published_body.get("ok"))
        self.assertEqual(published_body.get("status"), "SUCCESS")
        self.assertEqual(published_body.get("published_count"), 2)

    def test_import_question_pipeline_simultaneous_web_fusion_prefers_web(self) -> None:
        raw_text = """
1. If x + y = 4 and x - y = 2, then x is:
(1) 1
(2) 2
(3) 3
(4) 4
Ans: 3
""".strip()
        web_question = {
            "question_id": "imp_q_web_1",
            "type": "MCQ_SINGLE",
            "question_text": "If x + y = 4 and x - y = 2, then x is:",
            "options": [
                {"label": "A", "text": "1"},
                {"label": "B", "text": "2"},
                {"label": "C", "text": "3"},
                {"label": "D", "text": "4"},
            ],
            "correct_answer": {
                "single": "C",
                "multiple": ["C"],
                "numerical": None,
                "tolerance": None,
            },
            "subject": "Mathematics",
            "chapter": "Algebra",
            "difficulty": "Hard",
            "ai_confidence": 0.96,
            "validation_status": "valid",
            "validation_errors": [],
            "source_origin": "web_pre_ocr_match",
            "source_url": "https://example.com/algebra-q1",
            "web_match_similarity": 0.71,
        }
        with patch.object(
            routes._APP_DATA,
            "_import_pre_ocr_web_lookup",
            return_value={
                "enabled": True,
                "questions": [dict(web_question)],
                "diagnostics": {"web_error_reason": "", "query_attempts": 4},
                "web_error_reason": "",
                "seed_count": 1,
                "candidate_count": 3,
                "matched_count": 1,
            },
        ):
            parsed = self.client.post(
                "/app/action",
                json={
                    "action": "lc9_parse_question_paper",
                    "raw_text": raw_text,
                    "meta": {
                        "teacher_id": "teacher_1",
                        "subject": "Mathematics",
                        "chapter": "Algebra",
                        "difficulty": "Hard",
                    },
                    "web_ocr_fusion_mode": True,
                    "question_count": 1,
                },
            )
        self.assertEqual(parsed.status_code, 200)
        body = parsed.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        report = body.get("fusion_report", {})
        self.assertEqual(report.get("mode"), "simultaneous_web_ocr_fusion")
        self.assertGreaterEqual(report.get("web_count", 0), 1)
        questions = body.get("questions", [])
        self.assertEqual(len(questions), 1)
        self.assertIn(
            questions[0].get("source_origin"),
            {"web_verified", "fusion_verified"},
        )
        self.assertIn("confidence_score", questions[0])
        self.assertIn("semantic_similarity", questions[0])
        self.assertIn("verification_pass", questions[0])
        self.assertIn("conflict_detected", questions[0])

    def test_import_question_pipeline_web_only_requires_web_match(self) -> None:
        raw_text = """
1. Integer type: value of 2 + 3 is ____.
Ans: 5
""".strip()
        with patch.object(
            routes._APP_DATA,
            "_import_pre_ocr_web_lookup",
            return_value={
                "enabled": True,
                "questions": [],
                "diagnostics": {"web_error_reason": "dns_resolution_failed"},
                "web_error_reason": "dns_resolution_failed",
                "seed_count": 1,
                "candidate_count": 0,
                "matched_count": 0,
            },
        ):
            parsed = self.client.post(
                "/app/action",
                json={
                    "action": "lc9_parse_question_paper",
                    "raw_text": raw_text,
                    "meta": {"subject": "Mathematics", "chapter": "Algebra"},
                    "web_ocr_fusion_mode": True,
                    "web_ocr_fusion_web_only": True,
                },
            )
        self.assertEqual(parsed.status_code, 200)
        body = parsed.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "NO_WEB_MATCH_FOUND")
        self.assertEqual(body.get("web_error_reason"), "dns_resolution_failed")

    def test_import_question_parser_applies_answer_key_section(self) -> None:
        raw_text = """
1. For x^2 - 5x + 6 = 0, one root is:
(1) 1
(2) 2
(3) 3
(4) 4

2. If a+b=3, then a^2+b^2 is:
(1) 3
(2) 5
(3) 9
(4) 7

Answer Key:
1-C
2-B
""".strip()
        parsed = self.client.post(
            "/app/action",
            json={
                "action": "lc9_parse_questions",
                "raw_text": raw_text,
                "meta": {"subject": "Mathematics", "chapter": "Quadratic Equations"},
            },
        )
        self.assertEqual(parsed.status_code, 200)
        body = parsed.json()
        self.assertTrue(body.get("ok"))
        questions = body.get("questions", [])
        self.assertEqual(len(questions), 2)
        q1_ans = ((questions[0].get("correct_answer") or {}).get("single") or "").upper()
        q2_ans = ((questions[1].get("correct_answer") or {}).get("single") or "").upper()
        self.assertEqual(q1_ans, "C")
        self.assertEqual(q2_ans, "B")

    def test_import_question_parser_extracts_inline_solution_text(self) -> None:
        raw_text = """
1. If x + 1 = 3, then x is:
(1) 0
(2) 1
(3) 2
(4) 3
Ans: 3
Solution: Step 1 subtract 1 from both sides. Step 2 get x = 2.
        """.strip()
        parsed = self.client.post(
            "/app/action",
            json={
                "action": "lc9_parse_questions",
                "raw_text": raw_text,
                "meta": {"subject": "Mathematics", "chapter": "Algebra"},
            },
        )
        self.assertEqual(parsed.status_code, 200)
        body = parsed.json()
        self.assertTrue(body.get("ok"))
        questions = body.get("questions", [])
        self.assertEqual(len(questions), 1)
        solution = (questions[0].get("solution_explanation") or "").strip()
        self.assertTrue(solution)
        self.assertIn("Step 1", solution)

    def test_publish_duplicate_enriches_missing_solution(self) -> None:
        q_text = """
1. If x + 1 = 3, then x is:
(1) 0
(2) 1
(3) 2
(4) 3
Ans: 3
        """.strip()
        first_parse = self.client.post(
            "/app/action",
            json={
                "action": "lc9_parse_questions",
                "raw_text": q_text,
                "meta": {"teacher_id": "teacher_1", "subject": "Mathematics", "chapter": "Algebra"},
            },
        )
        self.assertEqual(first_parse.status_code, 200)
        first_questions = first_parse.json().get("questions", [])
        first_publish = self.client.post(
            "/app/action",
            json={
                "action": "lc9_publish_questions",
                "questions": first_questions,
                "meta": {"teacher_id": "teacher_1", "subject": "Mathematics", "chapter": "Algebra"},
            },
        )
        self.assertEqual(first_publish.status_code, 200)
        self.assertTrue(first_publish.json().get("ok"))

        q_text_with_solution = """
1. If x + 1 = 3, then x is:
(1) 0
(2) 1
(3) 2
(4) 3
Ans: 3
Solution: Step 1 subtract 1 from both sides. Step 2 get x = 2.
        """.strip()
        second_parse = self.client.post(
            "/app/action",
            json={
                "action": "lc9_parse_questions",
                "raw_text": q_text_with_solution,
                "meta": {"teacher_id": "teacher_1", "subject": "Mathematics", "chapter": "Algebra"},
            },
        )
        self.assertEqual(second_parse.status_code, 200)
        second_questions = second_parse.json().get("questions", [])
        second_publish = self.client.post(
            "/app/action",
            json={
                "action": "lc9_publish_questions",
                "questions": second_questions,
                "meta": {"teacher_id": "teacher_1", "subject": "Mathematics", "chapter": "Algebra"},
            },
        )
        self.assertEqual(second_publish.status_code, 200)
        second_body = second_publish.json()
        self.assertTrue(second_body.get("ok"))
        self.assertEqual(second_body.get("status"), "NO_NEW_QUESTIONS")
        self.assertEqual(int(second_body.get("solutions_enriched_count") or 0), 1)

        bank_rows = routes._APP_DATA._import_question_bank  # type: ignore[attr-defined]
        self.assertEqual(len(bank_rows), 1)
        self.assertTrue((bank_rows[0].get("solution_explanation") or "").strip())

    def test_import_question_parser_applies_tabular_answer_key_anywhere(self) -> None:
        raw_text = """
11. If x^2 - 3x + 2 = 0, then one root is:
(1) 1
(2) 2
(3) 3
(4) 4

12. If a+b=5 and ab=6, then a^2+b^2 is:
(1) 13
(2) 12
(3) 11
(4) 10

Random notes before key section.

Correct Options:
Q.No. 11 12
Ans. 2 1
        """.strip()
        parsed = self.client.post(
            "/app/action",
            json={
                "action": "lc9_parse_questions",
                "raw_text": raw_text,
                "meta": {"subject": "Mathematics", "chapter": "Algebra"},
            },
        )
        self.assertEqual(parsed.status_code, 200)
        body = parsed.json()
        self.assertTrue(body.get("ok"))
        questions = body.get("questions", [])
        self.assertEqual(len(questions), 2)
        q1_ans = ((questions[0].get("correct_answer") or {}).get("single") or "").upper()
        q2_ans = ((questions[1].get("correct_answer") or {}).get("single") or "").upper()
        self.assertEqual(q1_ans, "B")
        self.assertEqual(q2_ans, "A")

    def test_import_question_parser_applies_tabular_key_without_answer_heading(self) -> None:
        raw_text = """
1. Value of 2+2 is:
(1) 1
(2) 2
(3) 3
(4) 4

2. Value of 3+3 is:
(1) 5
(2) 6
(3) 7
(4) 8

Q.No. 1 2
Ans. 4 2
        """.strip()
        parsed = self.client.post(
            "/app/action",
            json={
                "action": "lc9_parse_questions",
                "raw_text": raw_text,
                "meta": {"subject": "Mathematics", "chapter": "Arithmetic"},
            },
        )
        self.assertEqual(parsed.status_code, 200)
        body = parsed.json()
        self.assertTrue(body.get("ok"))
        questions = body.get("questions", [])
        self.assertEqual(len(questions), 2)
        q1_ans = ((questions[0].get("correct_answer") or {}).get("single") or "").upper()
        q2_ans = ((questions[1].get("correct_answer") or {}).get("single") or "").upper()
        self.assertEqual(q1_ans, "D")
        self.assertEqual(q2_ans, "B")

    def test_import_question_pipeline_blocks_invalid_questions(self) -> None:
        saved = self.client.post(
            "/app/action",
            json={
                "action": "save_import_drafts",
                "questions": [
                    {
                        "question_id": "imp_q_1",
                        "type": "MCQ_SINGLE",
                        "question_text": "",
                        "options": [],
                        "correct_answer": {"single": None, "multiple": []},
                    }
                ],
                "meta": {"teacher_id": "teacher_2"},
            },
        )
        self.assertEqual(saved.status_code, 200)
        body = saved.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "INVALID_IMPORT_QUESTIONS")
        invalid = body.get("invalid", [])
        self.assertTrue(invalid)
        self.assertEqual(invalid[0].get("question_id"), "imp_q_1")

    def test_publish_gate_requires_one_tap_fix_for_review_items(self) -> None:
        questions = [
            {
                "question_id": "imp_q_1",
                "type": "MCQ_MULTI",
                "question_text": "Select all true statements.",
                "options": [
                    {"label": "A", "text": "Statement 1"},
                    {"label": "B", "text": "Statement 2"},
                    {"label": "C", "text": "Statement 3"},
                    {"label": "D", "text": "Statement 4"},
                ],
                "correct_answer": {
                    "single": "A",
                    "multiple": ["A"],
                    "numerical": None,
                    "tolerance": None,
                },
                "subject": "Mathematics",
                "chapter": "Logic",
                "difficulty": "Hard",
            }
        ]
        blocked = self.client.post(
            "/app/action",
            json={
                "action": "lc9_publish_questions",
                "questions": questions,
                "meta": {"teacher_id": "teacher_99"},
                "publish_gate_profile": "strict_critical_only",
                "fix_suggestions_applied": False,
            },
        )
        self.assertEqual(blocked.status_code, 200)
        blocked_body = blocked.json()
        self.assertFalse(blocked_body.get("ok"))
        self.assertEqual(
            blocked_body.get("status"),
            "PUBLISH_GATE_REVIEW_CONFIRMATION_REQUIRED",
        )

        published = self.client.post(
            "/app/action",
            json={
                "action": "lc9_publish_questions",
                "questions": questions,
                "meta": {"teacher_id": "teacher_99"},
                "publish_gate_profile": "strict_critical_only",
                "fix_suggestions_applied": True,
            },
        )
        self.assertEqual(published.status_code, 200)
        body = published.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertIn("publish_gate", body)

    def test_web_verify_query_action_returns_cached_endpoint_payload(self) -> None:
        with patch.object(
            routes._APP_DATA,
            "_search_rows_with_provider_fallback",
            return_value=(
                [{"title": "JEE QP", "url": "https://jeeadv.ac.in/past_qps/2022_1_English.pdf", "snippet": "paper"}],
                {"query": "jee", "providers": [], "result_count": 1, "error_reason": ""},
            ),
        ):
            response = self.client.post(
                "/app/action",
                json={
                    "action": "lc9_web_verify_query",
                    "query": "JEE advanced quadratic question",
                    "max_rows": 5,
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertEqual(body.get("count"), 1)

    def test_chat_thread_preserves_direct_id_and_flexible_user_search(self) -> None:
        save_result = self.client.post(
            "/app/action",
            json={
                "action": "save_result",
                "quiz_id": "quiz_42",
                "quiz_title": "Rotational Motion",
                "student_name": "Riya Sharma",
                "student_id": "stu_riya",
                "score": 76,
                "total": 100,
            },
        )
        self.assertEqual(save_result.status_code, 200)
        self.assertTrue(save_result.json().get("ok"))

        sent = self.client.post(
            "/app/action",
            json={
                "action": "send_message",
                "is_peer": True,
                "chat_id": "student_1|TEACHER",
                "participants": "student_1,TEACHER",
                "payload": {
                    "id": "m1",
                    "sender": "student_1",
                    "senderName": "Student One",
                    "text": "Hello teacher",
                    "type": "text",
                    "time": 1730000000000,
                },
            },
        )
        self.assertEqual(sent.status_code, 200)
        self.assertTrue(sent.json().get("ok"))
        self.assertEqual(sent.json().get("chat_id"), "student_1|TEACHER")

        directory = self.client.post(
            "/app/action",
            json={
                "action": "list_chat_directory",
                "chat_id": "student_1",
                "role": "student",
            },
        )
        self.assertEqual(directory.status_code, 200)
        self.assertTrue(directory.json().get("ok"))
        rows = directory.json().get("list", [])
        self.assertTrue(any(x.get("chat_id") == "student_1|TEACHER" for x in rows))

        marked = self.client.post(
            "/app/action",
            json={
                "action": "mark_chat_read",
                "chat_id": "student_1|TEACHER",
                "user_id": "student_1",
            },
        )
        self.assertEqual(marked.status_code, 200)
        self.assertTrue(marked.json().get("ok"))

        teacher_search = self.client.post(
            "/app/action",
            json={
                "action": "search_chat_users",
                "user_id": "student_1",
                "role": "student",
                "query": "teach",
            },
        )
        self.assertEqual(teacher_search.status_code, 200)
        self.assertTrue(teacher_search.json().get("ok"))
        teacher_users = teacher_search.json().get("list", [])
        self.assertTrue(any(x.get("user_id") == "TEACHER" for x in teacher_users))

        student_search = self.client.post(
            "/app/action",
            json={
                "action": "search_chat_users",
                "user_id": "student_1",
                "role": "teacher",
                "query": "riya",
            },
        )
        self.assertEqual(student_search.status_code, 200)
        self.assertTrue(student_search.json().get("ok"))
        student_users = student_search.json().get("list", [])
        self.assertTrue(any(x.get("user_id") == "stu_riya" for x in student_users))


if __name__ == "__main__":
    unittest.main()
