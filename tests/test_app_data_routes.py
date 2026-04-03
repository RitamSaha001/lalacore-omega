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
from app.storage.sqlite_json_store import SQLiteJsonBlobStore  # noqa: E402
from services.atlas_planner_engine import AtlasProviderSpec  # noqa: E402


class AppDataRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        auth_root = root / "auth"
        self._original = routes._APP_DATA
        routes._APP_DATA = LocalAppDataService(
            assessments_file=root / "assessments.json",
            materials_file=root / "materials.json",
            live_class_schedule_file=root / "live_class_schedule.json",
            uploads_file=root / "uploads.json",
            import_drafts_file=root / "import_drafts.json",
            import_question_bank_file=root / "import_question_bank.json",
            auth_users_file=auth_root / "users.json",
            auth_storage_db_file=auth_root / "auth_store.sqlite3",
        )
        self._auth_store = SQLiteJsonBlobStore(auth_root / "auth_store.sqlite3")
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
        self.assertEqual(body.get("status"), "SUCCESS")
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

    def test_create_quiz_uses_request_origin_for_published_urls(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Origin Sensitive Quiz",
                "type": "Exam",
                "duration": 25,
                "role": "teacher",
                "questions": [{"text": "1 + 1 = ?", "correct": "2"}],
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        quiz_url = str(body.get("quiz_url") or body.get("url") or "")
        parsed = urlparse(quiz_url)
        self.assertEqual(parsed.scheme, "http")
        self.assertEqual(parsed.netloc, "testserver")
        self.assertTrue(parsed.path.startswith("/app/quiz/"))

    def test_upload_file_uses_request_origin_for_download_url(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "upload_file",
                "name": "probe.txt",
                "data": "data:text/plain;base64,SGVsbG8=",
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        file_url = str(body.get("file_url") or body.get("url") or "")
        parsed = urlparse(file_url)
        self.assertEqual(parsed.scheme, "http")
        self.assertEqual(parsed.netloc, "testserver")
        self.assertTrue(parsed.path.startswith("/app/file/"))

    def test_get_assessments_normalizes_local_urls_to_request_origin(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Normalization Quiz",
                "type": "Exam",
                "duration": 20,
                "questions": [{"text": "2 + 2 = ?", "correct": "4"}],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        self.assertTrue(quiz_id)

        self.assertTrue(routes._APP_DATA._assessments)
        routes._APP_DATA._assessments[0]["url"] = (
            f"http://10.0.2.2:8000/app/quiz/{quiz_id}.csv"
        )

        listed = self.client.get("/app/action", params={"action": "get_assessments"})
        self.assertEqual(listed.status_code, 200)
        rows = listed.json().get("list", [])
        row = next(x for x in rows if x.get("id") == quiz_id)
        parsed = urlparse(str(row.get("url") or ""))
        self.assertEqual(parsed.scheme, "http")
        self.assertEqual(parsed.netloc, "testserver")
        self.assertEqual(parsed.path, f"/app/quiz/{quiz_id}.csv")

    def test_quiz_csv_rebuilds_when_runtime_file_is_missing(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "CSV Rebuild Quiz",
                "type": "Exam",
                "duration": 20,
                "questions": [
                    {
                        "text": "What is 1 + 3?",
                        "type": "MCQ",
                        "options": ["2", "3", "4", "5"],
                        "correct": "4",
                    }
                ],
            },
        )
        self.assertEqual(created.status_code, 200)
        body = created.json()
        quiz_id = str(body.get("id") or "")
        quiz_url = str(body.get("quiz_url") or body.get("url") or "")
        self.assertTrue(quiz_id)
        csv_path = urlparse(quiz_url).path

        local_csv = routes._APP_DATA._quizzes_dir / f"{quiz_id}.csv"
        self.assertTrue(local_csv.exists())
        local_csv.unlink()
        self.assertFalse(local_csv.exists())

        csv_res = self.client.get(csv_path)
        self.assertEqual(csv_res.status_code, 200)
        self.assertIn("What is 1 + 3?", csv_res.text)
        self.assertTrue(local_csv.exists())

    def test_upload_download_restores_file_from_db_when_local_copy_is_missing(self) -> None:
        uploaded = self.client.post(
            "/app/action",
            json={
                "action": "upload_file",
                "name": "probe.txt",
                "data": "data:text/plain;base64,SGVsbG8=",
            },
        )
        self.assertEqual(uploaded.status_code, 200)
        body = uploaded.json()
        file_url = str(body.get("file_url") or body.get("url") or "")
        file_id = str(body.get("id") or "")
        self.assertTrue(file_id)

        local_path = Path(routes._APP_DATA._uploads[file_id]["path"])
        self.assertTrue(local_path.exists())
        local_path.unlink()
        self.assertFalse(local_path.exists())

        with patch.object(
            routes._APP_DATA,
            "_read_upload_blob_from_db",
            new=AsyncMock(return_value=("probe.txt", "text/plain", b"Hello")),
        ):
            download = self.client.get(urlparse(file_url).path)

        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.text, "Hello")
        self.assertTrue(local_path.exists())

    def test_create_quiz_keeps_question_image_in_public_csv_and_answer_key(self) -> None:
        uploaded = self.client.post(
            "/app/action",
            json={
                "action": "upload_file",
                "name": "probe.txt",
                "data": "data:text/plain;base64,SGVsbG8=",
            },
        )
        self.assertEqual(uploaded.status_code, 200)
        file_url = str(uploaded.json().get("file_url") or "")
        self.assertTrue(file_url)

        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Image Quiz",
                "type": "Exam",
                "duration": 15,
                "questions": [
                    {
                        "text": "Pick the right answer",
                        "type": "MCQ",
                        "options": ["1", "2", "3", "4"],
                        "correct": "3",
                        "question_image": file_url,
                    }
                ],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        quiz_url = str(created.json().get("quiz_url") or created.json().get("url") or "")
        self.assertTrue(quiz_id)
        self.assertTrue(quiz_url)

        csv_res = self.client.get(urlparse(quiz_url).path)
        self.assertEqual(csv_res.status_code, 200)
        self.assertIn(file_url, csv_res.text)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {"0": ["3"]},
                "include_answer_key": True,
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        body = evaluated.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("evaluation_result", {}).get("score"), 4.0)
        answer_key = body.get("answer_key", [])
        self.assertEqual(len(answer_key), 1)
        self.assertEqual(answer_key[0].get("question_image"), file_url)

    def test_ping_probe_is_lightweight_and_does_not_send_support_email(self) -> None:
        with patch.object(
            routes._APP_DATA._atlas_incident_email,
            "send_incident_report",
        ) as mocked_send:
            response = self.client.post("/app/action", json={"action": "ping"})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("probe_action"), "ping")
        self.assertIn("diagnostics", body)
        mocked_send.assert_not_called()

    def test_publish_quiz_alias_creates_live_assessment(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "publish_quiz",
                "title": "Published Alias Quiz",
                "type": "Exam",
                "duration": 30,
                "role": "teacher",
                "questions": [{"text": "v = u + at", "correct": "formula"}],
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("assessment_id"), body.get("id"))
        self.assertIn("app/quiz/", str(body.get("quiz_url") or body.get("url") or ""))

    def test_create_quiz_republish_with_explicit_id_updates_in_place(self) -> None:
        first = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "id": "quiz_publish_fixed",
                "title": "First Draft",
                "type": "Exam",
                "duration": 30,
                "role": "teacher",
                "questions": [{"text": "1 + 1 = ?", "correct": "2"}],
            },
        )
        self.assertEqual(first.status_code, 200)
        self.assertTrue(first.json().get("ok"))

        second = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "id": "quiz_publish_fixed",
                "title": "Updated Draft",
                "type": "Exam",
                "duration": 35,
                "role": "teacher",
                "questions": [{"text": "2 + 2 = ?", "correct": "4"}],
            },
        )
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second.json().get("ok"))

        listed = self.client.get("/app/action", params={"action": "get_assessments"})
        self.assertEqual(listed.status_code, 200)
        rows = listed.json().get("list", [])
        matching = [row for row in rows if row.get("id") == "quiz_publish_fixed"]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].get("title"), "Updated Draft")

    def test_create_quiz_hides_answers_in_csv_but_keeps_backend_answer_key(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Teacher Mechanics Test",
                "type": "Exam",
                "duration": 45,
                "role": "teacher",
                "questions": [
                    {
                        "text": "What is the SI unit of force?",
                        "type": "MCQ",
                        "options": ["Joule", "Pascal", "Newton", "Watt"],
                        "correct": "Newton",
                        "solution_explanation": "Force is measured in Newton.",
                    }
                ],
                "ui_spec": {"layout": {"card_corner_radius": 28}},
                "student_adaptive_data": {"mastery_status": {"Mechanics": "watch"}},
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        quiz_id = str(body.get("id") or "")
        quiz_url = str(body.get("quiz_url") or body.get("url") or "")
        self.assertTrue(quiz_id)
        self.assertTrue(quiz_url)

        csv_res = self.client.get(urlparse(quiz_url).path)
        self.assertEqual(csv_res.status_code, 200)
        self.assertIn("What is the SI unit of force?", csv_res.text)
        self.assertNotIn("Force is measured in Newton.", csv_res.text)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {"0": ["C"]},
                "role": "teacher",
                "include_answer_key": True,
                "preview_only": True,
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        evaluated_body = evaluated.json()
        self.assertTrue(evaluated_body.get("ok"))
        answer_key = evaluated_body.get("answer_key", [])
        self.assertEqual(len(answer_key), 1)
        self.assertEqual(answer_key[0].get("correct_option"), "C")
        self.assertIn("Newton", answer_key[0].get("correct_answer", ""))

    def test_evaluate_quiz_submission_with_stable_id_is_idempotent(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Idempotent Submission Quiz",
                "type": "Exam",
                "duration": 30,
                "role": "teacher",
                "questions": [
                    {
                        "text": "3 + 3 = ?",
                        "type": "MCQ",
                        "options": ["5", "6", "7", "8"],
                        "correct": "6",
                    }
                ],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        self.assertTrue(quiz_id)

        payload = {
            "action": "evaluate_quiz_submission",
            "id": "res_stable_1",
            "submission_id": "res_stable_1",
            "quiz_id": quiz_id,
            "answers": {"0": ["B"]},
            "student_name": "Ritam",
            "student_id": "stu_11",
            "account_id": "acct_11",
            "submitted_at": "2026-04-03T09:00:00Z",
            "ts": 1712134800000,
        }
        first = self.client.post("/app/action", json=payload)
        second = self.client.post("/app/action", json=payload)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertTrue(first.json().get("ok"))
        self.assertTrue(second.json().get("ok"))

        results = self.client.get("/app/action", params={"action": "get_results"})
        self.assertEqual(results.status_code, 200)
        rows = [
            row
            for row in results.json().get("list", [])
            if row.get("id") == "res_stable_1"
        ]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("attempt_index"), 1)
        self.assertFalse(bool(rows[0].get("is_reattempt")))

    def test_publish_study_material_alias_adds_material(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "publish_study_material",
                "title": "Alias Material",
                "type": "pdf",
                "url": "https://example.com/notes.pdf",
                "subject": "Physics",
                "chapters": "Current Electricity",
                "role": "teacher",
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(str(body.get("material_id") or "").strip())
        self.assertEqual(body.get("url"), "https://example.com/notes.pdf")

    def test_create_quiz_preserves_ui_spec_and_student_adaptive_metadata(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Rich Publish Test",
                "type": "Exam",
                "duration": 35,
                "role": "teacher",
                "questions": [{"text": "x + 1 = 2", "correct": "1"}],
                "ui_spec": {"question_card": {"front": {"confidence_pill": False}}},
                "student_adaptive_data": {"recommendations": ["Revise linear equations"]},
                "is_ai_generated": True,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json().get("ok"))

        assessments = self.client.get(
            "/app/action",
            params={"action": "get_assessments"},
        )
        self.assertEqual(assessments.status_code, 200)
        rows = assessments.json().get("list", [])
        self.assertTrue(rows)
        row = next(x for x in rows if x.get("title") == "Rich Publish Test")
        self.assertEqual(
            row.get("ui_spec", {})
            .get("question_card", {})
            .get("front", {})
            .get("confidence_pill"),
            False,
        )
        self.assertEqual(
            row.get("student_adaptive_data", {}).get("recommendations"),
            ["Revise linear equations"],
        )
        self.assertTrue(row.get("ai_generated"))

    def test_evaluate_quiz_submission_answer_key_preserves_image_and_marks(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Image Mark Fidelity",
                "type": "Homework",
                "duration": 25,
                "role": "teacher",
                "questions": [
                    {
                        "text": "Identify the graph shown.",
                        "image": "https://example.com/graph.png",
                        "type": "MCQ",
                        "section": "Graphs",
                        "options": ["Parabola", "Line", "Circle", "Ellipse"],
                        "correct": "Circle",
                        "solution_explanation": "The image shows a circle centered at the origin.",
                        "posMark": 6,
                        "negMark": 2,
                    }
                ],
            },
        )
        self.assertEqual(response.status_code, 200)
        quiz_id = str(response.json().get("id") or "")
        self.assertTrue(quiz_id)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {"0": ["Circle"]},
                "role": "student",
                "include_answer_key": True,
                "preview_only": True,
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        body = evaluated.json()
        self.assertTrue(body.get("ok"))
        answer_key = body.get("answer_key", [])
        self.assertEqual(len(answer_key), 1)
        row = answer_key[0]
        self.assertEqual(row.get("question_image"), "https://example.com/graph.png")
        self.assertEqual(row.get("question_type"), "MCQ_SINGLE")
        self.assertEqual(row.get("marks_correct"), 6.0)
        self.assertEqual(row.get("marks_incorrect"), -2.0)
        self.assertEqual(row.get("section"), "Graphs")

    def test_teacher_quiz_negative_mark_penalty_is_applied_by_backend_grader(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Penalty Check",
                "type": "Exam",
                "duration": 20,
                "role": "teacher",
                "questions": [
                    {
                        "text": "2 + 2 = ?",
                        "type": "MCQ",
                        "options": ["3", "4", "5", "6"],
                        "correct": "4",
                        "posMark": 5,
                        "negMark": 2,
                    }
                ],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        self.assertTrue(quiz_id)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {"0": ["3"]},
                "role": "student",
                "include_answer_key": True,
                "preview_only": True,
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        body = evaluated.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("score"), -2.0)
        self.assertEqual(body.get("wrong"), 1)

    def test_create_quiz_evaluation_handles_latex_wrapped_correct_options(self) -> None:
        response = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Hyperbola Quiz",
                "type": "Exam",
                "duration": 20,
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
        self.assertEqual(response.status_code, 200)
        body = response.json()
        quiz_id = str(body.get("id") or "")
        self.assertTrue(quiz_id)

        evaluated = self.client.post(
            "/app/action",
            json={
                "action": "evaluate_quiz_submission",
                "quiz_id": quiz_id,
                "answers": {"0": ["C"]},
                "role": "teacher",
                "include_answer_key": True,
                "preview_only": True,
            },
        )
        self.assertEqual(evaluated.status_code, 200)
        evaluated_body = evaluated.json()
        self.assertTrue(evaluated_body.get("ok"))
        self.assertEqual(evaluated_body.get("correct"), 1)
        self.assertEqual(
            (evaluated_body.get("answer_key") or [{}])[0].get("correct_option"),
            "C",
        )

    def test_import_raw_text_parser_handles_common_ocr_label_confusions(self) -> None:
        rows = routes._APP_DATA._parse_import_raw_text(
            "G1. What is the SI unit of force?\n"
            "A Newton\n"
            "@ Joule\n"
            "C. Pascal\n"
            "O Watt\n"
            "Anewer. A",
            meta_defaults={},
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.get("question_text"), "What is the SI unit of force?")
        options = row.get("options") or []
        self.assertEqual(
            [(item.get("label"), item.get("text")) for item in options],
            [
                ("A", "Newton"),
                ("B", "Joule"),
                ("C", "Pascal"),
                ("D", "Watt"),
            ],
        )
        self.assertEqual((row.get("correct_answer") or {}).get("single"), "A")

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
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
        ) as mocked_fetch:
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
        self.assertEqual(policy.get("mode"), "synthesized_pyq_fallback")
        self.assertEqual(policy.get("web_source_applied_count", 0), 0)
        questions = body.get("questions_json", [])
        self.assertEqual(len(questions), 3)
        self.assertTrue(
            all(
                str(q.get("source_origin", "")).startswith("ai_synth")
                or str(q.get("source_origin", "")).startswith("synthesized")
                for q in questions
            )
        )
        self.assertTrue(all(not str(q.get("source_url", "")).strip() for q in questions))
        mocked_fetch.assert_not_called()

    def test_ai_generate_pyq_hybrid_mode_mixes_online_and_offline_sources(self) -> None:
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
        ) as mocked_fetch:
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
        self.assertEqual(policy.get("mode"), "synthesized_pyq_fallback")
        self.assertFalse(policy.get("mixed_pyq_source_mode"))
        self.assertEqual(policy.get("web_source_online_applied_count", 0), 0)
        self.assertEqual(policy.get("web_source_offline_applied_count", 0), 0)
        self.assertTrue(policy.get("fallback_used"))
        source_urls = [str(q.get("source_url") or "") for q in body.get("questions_json", [])]
        self.assertTrue(all(not url for url in source_urls))
        mocked_fetch.assert_not_called()

    def test_ai_generate_pyq_tries_backup_web_rows_before_synthesis(self) -> None:
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
        ) as mocked_fetch:
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
        self.assertFalse(str(questions[0].get("source_url", "")).strip())
        self.assertTrue(
            str(questions[0].get("source_origin", "")).startswith("ai_synth")
            or str(questions[0].get("source_origin", "")).startswith("synthesized")
        )
        mocked_fetch.assert_not_called()

    def test_ai_generate_pyq_recovers_solution_when_web_answer_missing(self) -> None:
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
        ) as mocked_fetch, patch.object(
            routes._APP_DATA,
            "_recover_solution_via_ai_engine",
            new_callable=AsyncMock,
        ) as mocked_recover:
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
        self.assertEqual(policy.get("ai_solution_recovery_count", 0), 0)
        self.assertFalse(policy.get("answer_sources_verified"))
        questions = body.get("questions_json", [])
        self.assertEqual(len(questions), 2)
        for q in questions:
            self.assertTrue((q.get("solution_explanation") or "").strip())
            self.assertIn(q.get("source_origin"), {"ai_synth_ultra_verified", "synthesized_pyq"})
        mocked_fetch.assert_not_called()
        mocked_recover.assert_not_called()

    def test_ai_generate_pyq_strict_mode_requires_verified_web_fetch(self) -> None:
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
        ) as mocked_fetch:
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
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertEqual(body.get("source_policy", {}).get("web_source_applied_count"), 0)
        self.assertEqual(
            body.get("source_policy", {}).get("mode"),
            "synthesized_pyq_fallback",
        )
        mocked_fetch.assert_not_called()

    def test_ai_generate_student_self_practice_softens_strict_web_requirement(self) -> None:
        with patch.object(
            routes._APP_DATA,
            "_fetch_pyq_web_snippets",
        ) as mocked_fetch:
            generated = self.client.post(
                "/app/action",
                json={
                    "action": "ai_generate_quiz",
                    "subject": "Physics",
                    "chapters": ["Alternating Current"],
                    "subtopics": ["Alternating Current"],
                    "difficulty": 5,
                    "question_count": 2,
                    "trap_intensity": "high",
                    "role": "student",
                    "authoring_mode": False,
                    "self_practice_mode": True,
                    "include_answer_key": True,
                    "pyq_focus": True,
                    "search_hard_pyq": True,
                    "pyq_mode": "strict_related_web",
                    "pyq_web_only_mode": True,
                    "pyq_answer_retrieval_required": True,
                    "user_id": "student_real_device",
                },
            )

        self.assertEqual(generated.status_code, 200)
        body = generated.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        questions = body.get("questions_json", [])
        self.assertEqual(len(questions), 2)
        source_policy = body.get("source_policy", {})
        self.assertEqual(source_policy.get("mode"), "synthesized_pyq_fallback")
        self.assertFalse(source_policy.get("strict_web_requirement_unmet"))
        self.assertFalse(source_policy.get("answer_sources_verified"))
        self.assertEqual(source_policy.get("web_source_applied_count"), 0)
        mocked_fetch.assert_not_called()

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

    def test_ai_chat_surfaces_selected_model_name_when_present(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "x = 4",
            "reasoning": "Solve x + 1 = 5.",
            "winner_provider": "gemini",
            "engine": {
                "version": "research-grade-v2",
                "model": "gemini-2.5-pro",
                "model_name": "gemini-2.5-pro",
            },
        }
        with patch(
            "core.api.entrypoint.lalacore_entry",
            new=AsyncMock(return_value=fake_result),
        ):
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
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("provider"), "gemini")
        self.assertEqual(body.get("model"), "gemini-2.5-pro")

    def test_material_generate_action_uses_dedicated_material_engine_with_content(self) -> None:
        fake_result = {
            "status": "ok",
            "content": "# Electrostatics Summary\n\n## Core Idea Map\n- Electric field lines and superposition.",
            "final_answer": "# Electrostatics Summary\n\n## Core Idea Map\n- Electric field lines and superposition.",
            "reasoning": "Use Coulomb law, field lines, and superposition for the revision sheet.",
            "winner_provider": "gemini",
            "engine": {"version": "research-grade-v2"},
        }
        add = self.client.post(
            "/app/action",
            json={
                "action": "add_material",
                "title": "Electrostatics Sheet",
                "type": "pdf",
                "url": "https://example.com/electrostatics.pdf",
                "class": "Class 12",
                "subject": "Physics",
                "chapters": "Electrostatics",
                "notes": "Electric field, superposition, and flux.",
                "role": "teacher",
            },
        )
        self.assertEqual(add.status_code, 200)
        material_id = add.json().get("material", {}).get("material_id")
        self.assertTrue(material_id)

        with patch(
            "app.data.local_app_data_service.material_generation_entry",
            new=AsyncMock(return_value=fake_result),
        ) as mocked:
            res = self.client.post(
                "/app/action",
                json={
                    "action": "material_generate",
                    "material_id": material_id,
                    "mode": "summarize",
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(body.get("ok"))
        self.assertIn("Electrostatics Summary", str(body.get("content", "")))
        self.assertEqual(mocked.await_count, 1)
        call_kwargs = mocked.await_args.kwargs
        self.assertIn(
            "Task: produce a material-grounded JEE study output",
            str(call_kwargs.get("prompt", "")),
        )
        options = dict(call_kwargs.get("options") or {})
        self.assertEqual(options.get("function"), "material_generate")
        self.assertFalse(bool(options.get("enable_pre_reasoning_context")))
        self.assertFalse(bool(options.get("enable_web_retrieval")))
        self.assertFalse(bool(options.get("enable_graph_of_thought")))
        self.assertFalse(bool(options.get("enable_mcts_reasoning")))
        self.assertFalse(bool(options.get("enable_verification_reevaluation")))
        self.assertFalse(bool(options.get("enable_meta_verification")))
        self.assertIn("Electrostatics", str(options.get("retrieval_query_override", "")))

    def test_material_generate_failure_does_not_inject_fake_fallback_content(self) -> None:
        add = self.client.post(
            "/app/action",
            json={
                "action": "add_material",
                "title": "Thermodynamics Sheet",
                "type": "pdf",
                "url": "https://example.com/thermo.pdf",
                "class": "Class 12",
                "subject": "Physics",
                "chapters": "Thermodynamics",
                "notes": "Entropy, Carnot, efficiency.",
                "role": "teacher",
            },
        )
        self.assertEqual(add.status_code, 200)
        material_id = add.json().get("material", {}).get("material_id")
        self.assertTrue(material_id)

        with patch(
            "app.data.local_app_data_service.material_generation_entry",
            new=AsyncMock(
                return_value={
                    "ok": False,
                    "status": "MATERIAL_ENGINE_EMPTY_OUTPUT",
                    "message": "Material AI did not return a usable study response.",
                }
            ),
        ):
            res = self.client.post(
                "/app/action",
                json={
                    "action": "material_generate",
                    "material_id": material_id,
                    "mode": "formula_sheet",
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "MATERIAL_ENGINE_EMPTY_OUTPUT")
        self.assertFalse(bool(body.get("content")))

    def test_material_generate_placeholder_output_is_not_marked_success(self) -> None:
        add = self.client.post(
            "/app/action",
            json={
                "action": "add_material",
                "title": "Electrostatics Sheet",
                "type": "pdf",
                "url": "https://example.com/electrostatics.pdf",
                "class": "Class 12",
                "subject": "Physics",
                "chapters": "Electrostatics",
                "notes": "Electric field, Gauss law, and capacitor basics.",
                "role": "teacher",
            },
        )
        self.assertEqual(add.status_code, 200)
        material_id = add.json().get("material", {}).get("material_id")
        self.assertTrue(material_id)

        with patch(
            "app.data.local_app_data_service.material_generation_entry",
            new=AsyncMock(
                return_value={
                    "status": "ok",
                    "final_answer": "[UNRESOLVED]",
                    "reasoning": "The actual question is missing.",
                }
            ),
        ):
            res = self.client.post(
                "/app/action",
                json={
                    "action": "material_generate",
                    "material_id": material_id,
                    "mode": "summarize",
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertFalse(body.get("ok"))
        self.assertEqual(body.get("status"), "MATERIAL_ENGINE_EMPTY_OUTPUT")
        self.assertFalse(bool(body.get("content")))

    def test_material_query_action_uses_material_context(self) -> None:
        fake_result = {
            "status": "ok",
            "content": "**Answer**\nEccentricity is 5/4.\n\n**Explanation**\nFor x^2/16 - y^2/9 = 1, use e = sqrt(1 + b^2/a^2).",
            "final_answer": "**Answer**\nEccentricity is 5/4.\n\n**Explanation**\nFor x^2/16 - y^2/9 = 1, use e = sqrt(1 + b^2/a^2).",
            "reasoning": "Use the standard form and asymptote relation.",
            "winner_provider": "gemini",
            "engine": {"version": "research-grade-v2"},
            "visualization": {
                "type": "desmos",
                "expressions": ["x^2/16-y^2/9=1"],
            },
        }
        add = self.client.post(
            "/app/action",
            json={
                "action": "add_material",
                "title": "Hyperbola Notes",
                "type": "pdf",
                "url": "https://example.com/hyperbola.pdf",
                "class": "Class 12",
                "subject": "Mathematics",
                "chapters": "Hyperbola",
                "notes": "Standard form x^2/a^2 - y^2/b^2 = 1",
                "role": "teacher",
            },
        )
        self.assertEqual(add.status_code, 200)
        material_id = add.json().get("material", {}).get("material_id")
        self.assertTrue(material_id)

        with patch(
            "app.data.local_app_data_service.material_generation_entry",
            new=AsyncMock(return_value=fake_result),
        ) as mocked:
            res = self.client.post(
                "/app/action",
                json={
                    "action": "material_query",
                    "material_id": material_id,
                    "question": "Find eccentricity and asymptotes.",
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(body.get("ok"))
        self.assertIn("Eccentricity is 5/4.", str(body.get("content", "")))
        self.assertTrue(body.get("visualization"))
        self.assertEqual(mocked.await_count, 1)
        call_kwargs = mocked.await_args.kwargs
        self.assertIn(
            "Task: answer the student's question",
            str(call_kwargs.get("prompt", "")),
        )
        options = dict(call_kwargs.get("options") or {})
        self.assertEqual(options.get("function"), "material_query")
        self.assertFalse(bool(options.get("enable_pre_reasoning_context")))
        self.assertFalse(bool(options.get("enable_web_retrieval")))
        self.assertFalse(bool(options.get("enable_graph_of_thought")))
        self.assertFalse(bool(options.get("enable_mcts_reasoning")))
        self.assertFalse(bool(options.get("enable_verification_reevaluation")))
        self.assertFalse(bool(options.get("enable_meta_verification")))
        self.assertIn("Find eccentricity", str(options.get("retrieval_query_override", "")))

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
        options = dict(mocked.await_args.kwargs.get("options") or {})
        self.assertFalse(bool(options.get("enable_web_retrieval")))
        self.assertEqual(options.get("require_citations"), "none")
        self.assertEqual(options.get("min_citation_count"), 0)

    def test_ai_app_agent_returns_structured_single_action_plan(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "single_action",
                    "goal": "Check pending homework",
                    "plan_id": "student_plan_1",
                    "summary": "Atlas can check the pending homework queue.",
                    "student_notice": "I will use your dashboard only.",
                    "tool": "list_pending_homeworks",
                    "title": "Check pending homework",
                    "detail": "Reading the current homework queue.",
                    "risk": "low",
                    "args": {},
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ) as mocked:
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Which homeworks are remaining?",
                    "context": {
                        "pending_homework_count": 2,
                        "study_materials": [],
                    },
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "single_action")
        self.assertEqual(body.get("tool"), "list_pending_homeworks")
        self.assertEqual(body.get("actions", [])[0].get("tool"), "list_pending_homeworks")
        self.assertEqual(mocked.await_count, 1)

    def test_ai_app_agent_parses_structured_plan_from_final_answer(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": json.dumps(
                {
                    "type": "single_action",
                    "goal": "Check pending homework",
                    "plan_id": "student_plan_final_1",
                    "summary": "Atlas can check the pending homework queue.",
                    "tool": "list_pending_homeworks",
                    "title": "Check pending homework",
                    "detail": "Reading the current homework queue.",
                    "risk": "low",
                    "args": {},
                }
            ),
            "reasoning": "Structured plan returned in final_answer.",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Which homeworks are remaining?",
                    "context": {
                        "pending_homework_count": 2,
                    },
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "single_action")
        self.assertEqual(body.get("tool"), "list_pending_homeworks")

    def test_ai_app_agent_recovers_plan_from_unsafe_candidate_answer(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "Uncertain answer: verification failed under high risk. Please retry with a stronger model.",
            "answer": "Uncertain answer: verification failed under high risk. Please retry with a stronger model.",
            "unsafe_candidate_answer": json.dumps(
                {
                    "type": "multi_step_plan",
                    "goal": "Create a quiz and open analytics",
                    "plan_id": "teacher_plan_unsafe_1",
                    "summary": "Create the quiz draft and then open analytics.",
                    "steps": [
                        {
                            "id": "step_1",
                            "tool": "generate_teacher_quiz_draft",
                            "title": "Generate quiz draft",
                            "detail": "Create a 5-question thermodynamics quiz draft.",
                            "risk": "low",
                            "args": {
                                "topic": "Thermodynamics",
                                "question_count": 5,
                            },
                        },
                        {
                            "id": "step_2",
                            "tool": "open_teacher_student_analytics",
                            "title": "Open analytics",
                            "detail": "Navigate to student analytics after the draft is ready.",
                            "risk": "low",
                            "args": {},
                            "depends_on": ["step_1"],
                        },
                    ],
                }
            ),
            "reasoning": "The candidate plan was suppressed by the solver quality gate.",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Create a class 12 thermodynamics quiz with 5 questions and then open student analytics.",
                    "context": {
                        "atlas_role": "teacher",
                        "teacher_id": "teacher_plan_maint",
                    },
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(body.get("steps", [])[0].get("tool"), "generate_teacher_quiz_draft")
        self.assertEqual(body.get("steps", [])[1].get("tool"), "open_teacher_student_analytics")

    def test_ai_app_agent_recovers_truncated_teacher_plan_from_reasoning_tool_mentions(self) -> None:
        fake_result = {
            "status": "ok",
            "final_status": "Completed",
            "answer": (
                '{"type":"multi_step_plan","goal":"Create a quiz and then open student analytics",'
                '"plan_id":"create_quiz_and_analytics","summary":"Create a quiz and then navigate to '
                'student analytics","teacher_notice":"Please review the quiz draft before publishing",'
                '"requires_confirmation":false,'
            ),
            "final_answer": (
                '{"type":"multi_step_plan","goal":"Create a quiz and then open student analytics",'
                '"plan_id":"create_quiz_and_analytics","summary":"Create a quiz and then navigate to '
                'student analytics","teacher_notice":"Please review the quiz draft before publishing",'
                '"requires_confirmation":false,'
            ),
            "reasoning": (
                "Reasoning: The teacher wants to create a class 12 thermodynamics quiz with 5 "
                "questions and then open student analytics. To achieve this, we need to use the "
                "`generate_teacher_quiz_draft` tool to create the quiz and then use the "
                "`open_teacher_student_analytics` tool to navigate to student analytics."
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Create a class 12 thermodynamics quiz with 5 questions and then open student analytics.",
                    "context": {
                        "atlas_role": "teacher",
                        "teacher_id": "teacher_plan_maint",
                        "selected_student": {
                            "student_id": "student_7",
                            "name": "Aarav",
                        },
                    },
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(body.get("recovery_mode"), "tool_mentions_from_reasoning")
        self.assertEqual(body.get("steps", [])[0].get("tool"), "generate_teacher_quiz_draft")
        self.assertEqual(body.get("steps", [])[0].get("args", {}).get("question_count"), 5)
        self.assertEqual(body.get("steps", [])[0].get("args", {}).get("class_name"), "Class 12")
        self.assertEqual(body.get("steps", [])[0].get("args", {}).get("topic"), "Thermodynamics")
        self.assertEqual(body.get("steps", [])[1].get("tool"), "open_teacher_student_analytics")
        self.assertEqual(body.get("steps", [])[1].get("args", {}).get("student_id"), "student_7")

    def test_ai_app_agent_asks_follow_up_for_ambiguous_material_request(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "needs_more_info",
                    "goal": "Download study material",
                    "summary": "Atlas needs to know which material you mean.",
                    "needs_more_info": True,
                    "follow_up_questions": [
                        "Which study material should I download?"
                    ],
                    "actions": [],
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ) as mocked:
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Download this material",
                    "context": {
                        "selected_material": {},
                        "study_materials": [],
                    },
                },
            )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(body.get("needs_more_info"))
        self.assertIn("follow_up_questions", body)
        self.assertTrue(body.get("follow_up_questions"))
        self.assertEqual(mocked.await_count, 1)

    def test_ai_app_agent_returns_adaptive_memory_stats_and_passive_events(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "single_action",
                    "goal": "Resume Study",
                    "plan_id": "student_plan_resume_1",
                    "summary": "Atlas will reopen the most relevant Study material.",
                    "student_notice": "Using the adaptive student memory layer.",
                    "tool": "open_material",
                    "title": "Resume Study material",
                    "detail": "Open the last material from memory.",
                    "risk": "low",
                    "args": {},
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            observed = self.client.post(
                "/ai/app/agent/observe",
                json={
                    "account_id": "student_22",
                    "tool_name": "open_material",
                    "category": "study",
                    "success": True,
                    "latency_ms": 220,
                    "context": {
                        "selected_material": {
                            "material_id": "mat_45",
                            "subject": "Physics",
                        }
                    },
                    "args": {"material_id": "mat_45"},
                },
            )
            self.assertEqual(observed.status_code, 200)
            observed_body = observed.json()
            self.assertTrue(observed_body.get("ok"))
            self.assertEqual(
                observed_body.get("student_memory", {}).get("last_material_id"),
                "mat_45",
            )

            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Resume physics",
                    "context": {
                        "account_id": "student_22",
                        "selected_material": {
                            "material_id": "mat_45",
                            "subject": "Physics",
                        },
                        "pending_homework_count": 2,
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "open_material")
        self.assertEqual(body.get("student_memory", {}).get("last_material_id"), "mat_45")
        self.assertIn("open_material", body.get("tool_stats_summary", {}).get("preferred_tools", []))
        self.assertTrue(body.get("passive_events"))

    def test_ai_app_agent_passive_endpoint_returns_student_nudges(self) -> None:
        observed = self.client.post(
            "/ai/app/agent/observe",
            json={
                "account_id": "student_11",
                "tool_name": "list_pending_homeworks",
                "category": "assessment",
                "success": True,
                "latency_ms": 180,
                "context": {
                    "pending_homework_count": 3,
                    "student_profile": {"weak_topics": ["Thermodynamics"]},
                },
            },
        )
        self.assertEqual(observed.status_code, 200)

        passive = self.client.post(
            "/ai/app/agent/passive",
            json={
                "account_id": "student_11",
                "context": {
                    "pending_homework_count": 3,
                    "student_profile": {"weak_topics": ["Thermodynamics"]},
                },
            },
        )
        self.assertEqual(passive.status_code, 200)
        body = passive.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("account_id"), "student_11")
        self.assertIn("preferred_tools", body.get("tool_stats_summary", {}))
        self.assertTrue(body.get("passive_events"))
        self.assertEqual(
            body.get("passive_events", [])[0].get("event"),
            "homework_due",
        )

    def test_ai_app_agent_respects_allowed_tool_subset(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "multi_step_plan",
                    "goal": "Resume work",
                    "plan_id": "student_plan_tools_1",
                    "summary": "Atlas will stay inside the limited tool subset.",
                    "steps": [
                        {
                            "id": "step_1",
                            "tool": "list_pending_homeworks",
                            "title": "Pending homework",
                            "detail": "Read homework queue.",
                            "args": {},
                        },
                        {
                            "id": "step_2",
                            "tool": "open_material",
                            "title": "Open material",
                            "detail": "This tool should be filtered out.",
                            "args": {},
                        },
                    ],
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Check my work",
                    "context": {
                        "account_id": "student_55",
                        "allowed_tools": ["list_pending_homeworks"],
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        actions = body.get("actions", [])
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].get("tool"), "list_pending_homeworks")

    def test_ai_app_agent_allows_new_study_material_formula_tool(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "single_action",
                    "goal": "Open a formula sheet for this material",
                    "plan_id": "student_plan_formula_1",
                    "summary": "Atlas will open a formula sheet for the selected material.",
                    "tool": "open_material_formula_sheet",
                    "title": "Open formula sheet",
                    "detail": "Use the selected Study material.",
                    "risk": "low",
                    "args": {},
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Make a formula sheet for this material",
                    "context": {
                        "account_id": "student_77",
                        "selected_material": {
                            "material_id": "mat_formula_1",
                            "title": "Binomial Theorem Notes",
                            "subject": "Mathematics",
                        },
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "open_material_formula_sheet")
        self.assertEqual(body.get("actions", [])[0].get("tool"), "open_material_formula_sheet")

    def test_ai_app_agent_teacher_mode_returns_teacher_dashboard_tool(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "single_action",
                    "goal": "Open teacher student analytics",
                    "plan_id": "teacher_plan_analytics_1",
                    "summary": "Atlas will open teacher student analytics.",
                    "teacher_notice": "Using teacher dashboard context only.",
                    "tool": "open_teacher_student_analytics",
                    "title": "Open student analytics",
                    "detail": "Navigate to the analytics surface.",
                    "risk": "low",
                    "args": {},
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ) as mocked:
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Open student analytics",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_1",
                        "teacher_students": [],
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("role"), "teacher")
        self.assertEqual(body.get("tool"), "open_teacher_student_analytics")
        self.assertEqual(
            body.get("actions", [])[0].get("tool"),
            "open_teacher_student_analytics",
        )
        self.assertEqual(mocked.await_count, 1)

    def test_ai_app_agent_teacher_mode_allows_class_performance_summary_tool(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "single_action",
                    "goal": "Summarize the whole class performance",
                    "plan_id": "teacher_plan_class_perf_1",
                    "summary": "Atlas will summarize the full class performance.",
                    "teacher_notice": "Using teacher analytics context.",
                    "tool": "get_teacher_class_performance_summary",
                    "title": "Class performance summary",
                    "detail": "Summarize the whole class using recent results.",
                    "risk": "low",
                    "args": {},
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Show me the whole class performance summary",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_1",
                        "teacher_students": [
                            {
                                "student_name": "Riya",
                                "average_pct": 72.5,
                                "attempts": 4,
                            }
                        ],
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "get_teacher_class_performance_summary")
        self.assertEqual(
            body.get("actions", [])[0].get("tool"),
            "get_teacher_class_performance_summary",
        )

    def test_ai_app_agent_teacher_mode_requests_follow_up_for_schedule(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "needs_more_info",
                    "goal": "Schedule next class",
                    "summary": "Atlas needs the class time.",
                    "teacher_notice": "I can schedule it once the time is clear.",
                    "needs_more_info": True,
                    "follow_up_questions": [
                        "What time should I schedule the next class for?"
                    ],
                    "actions": [],
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Schedule next week's class",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_1",
                        "teacher_scheduled_classes": [],
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("role"), "teacher")
        self.assertTrue(body.get("needs_more_info"))
        self.assertIn("follow_up_questions", body)
        self.assertTrue(body.get("follow_up_questions"))

    def test_ai_app_agent_student_mode_understands_natural_remaining_work_request(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Can you just show me what I still have left?",
                    "context": {
                        "account_id": "student_201",
                        "pending_homework_count": 3,
                        "pending_exam_count": 1,
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "single_action")
        self.assertEqual(body.get("tool"), "show_remaining_work")
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_student_mode_builds_authoritative_study_plan(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Create a quick study plan for binomial theorem",
                    "context": {
                        "account_id": "student_301",
                        "pending_homework_count": 2,
                        "pending_exam_count": 1,
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        steps = body.get("steps", [])
        self.assertEqual(
            [step.get("tool") for step in steps],
            [
                "find_study_material",
                "get_study_overview",
                "get_weak_topics",
                "suggest_next_best_task",
            ],
        )
        self.assertEqual(
            steps[0].get("args", {}).get("query"),
            "Binomial Theorem",
        )
        self.assertEqual(
            steps[-1].get("depends_on"),
            ["step_1", "step_2", "step_3"],
        )
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_teacher_mode_builds_topic_study_support_plan(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Create a short study plan for binomial theorem and suggest the next best action.",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_301",
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(
            [step.get("tool") for step in body.get("steps", [])],
            [
                "open_teacher_add_material",
                "generate_teacher_quiz_draft",
            ],
        )
        self.assertEqual(
            body.get("steps", [])[1].get("args", {}).get("topic"),
            "Binomial Theorem",
        )
        self.assertEqual(
            body.get("steps", [])[1].get("depends_on"),
            ["step_1"],
        )
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_teacher_mode_understands_attachment_quiz_review_publish_request(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Can you turn this PDF into a quiz, let me review it, and then publish it?",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_7",
                        "teacher_latest_attachment": {
                            "file_id": "file_pdf_1",
                            "name": "worksheet.pdf",
                            "mime": "application/pdf",
                        },
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "multi_step_plan")
        self.assertEqual(
            [step.get("tool") for step in body.get("steps", [])],
            [
                "import_teacher_quiz_from_attachment",
                "preview_teacher_quiz_draft",
                "publish_teacher_quiz_draft",
            ],
        )
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_runs_real_dedicated_planner_pipeline_for_student(self) -> None:
        planner_payload = json.dumps(
            {
                "type": "single_action",
                "goal": "Open unread notifications",
                "plan_id": "student_real_pipeline_1",
                "summary": "Atlas will open unread notifications.",
                "teacher_notice": None,
                "student_notice": "Using the app notification center only.",
                "requires_confirmation": False,
                "needs_more_info": False,
                "follow_up_questions": [],
                "proposed_tools": ["open_notifications_center_unread"],
                "actions": [],
                "steps": [],
                "tool": "open_notifications_center_unread",
                "title": "Open unread notifications",
                "detail": "Navigate to unread notifications.",
                "risk": "low",
                "args": {},
                "confidence": 0.92,
                "risk_score": 0.08,
                "needs_escalation": False,
                "recovery_mode": None,
            }
        )
        with patch(
            "services.atlas_planner_engine._atlas_provider_specs",
            return_value=[AtlasProviderSpec("openrouter", "openai/gpt-4o-mini")],
        ), patch(
            "services.atlas_planner_engine._call_provider",
            new=AsyncMock(return_value=planner_payload),
        ) as mocked:
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Open my unread notifications.",
                    "context": {"account_id": "student_real_1"},
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("type"), "single_action")
        self.assertEqual(body.get("tool"), "open_notifications_center_unread")
        self.assertEqual(mocked.await_count, 1)

    def test_ai_app_agent_teacher_request_injects_request_clock_into_real_planner_context(self) -> None:
        captured: dict[str, str] = {}

        async def _fake_provider(*args, **kwargs):
            captured["prompt"] = kwargs.get("prompt", "")
            return json.dumps(
                {
                    "type": "single_action",
                    "goal": "Schedule the next class.",
                    "plan_id": "teacher_real_pipeline_clock",
                    "summary": "Atlas will schedule the next class.",
                    "teacher_notice": "Using the dedicated planner with request clock context.",
                    "student_notice": None,
                    "requires_confirmation": False,
                    "needs_more_info": False,
                    "follow_up_questions": [],
                    "proposed_tools": ["schedule_next_class"],
                    "actions": [],
                    "steps": [],
                    "tool": "schedule_next_class",
                    "title": "Schedule next class",
                    "detail": "Schedule the next class.",
                    "risk": "low",
                    "args": {
                        "start_time": "2026-04-04T17:00:00+05:30",
                        "duration_minutes": 90,
                    },
                    "confidence": 0.89,
                    "risk_score": 0.12,
                    "needs_escalation": False,
                    "recovery_mode": None,
                }
            )

        with patch(
            "services.atlas_planner_engine._atlas_provider_specs",
            return_value=[AtlasProviderSpec("openrouter", "openai/gpt-4o-mini")],
        ), patch(
            "services.atlas_planner_engine._call_provider",
            new=AsyncMock(side_effect=_fake_provider),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Schedule the next physics class tomorrow at 5 PM for class 12.",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_real_1",
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        self.assertIn("request_clock", captured.get("prompt", ""))
        self.assertIn("timezone", captured.get("prompt", ""))

    def test_ai_app_agent_student_mode_understands_latest_flashcards_request(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Open the latest flashcards from the last class.",
                    "context": {"account_id": "student_flash_1"},
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "open_last_class_flashcards")
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_student_mode_understands_ai_chat_history_request(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Show me my AI chat history.",
                    "context": {"account_id": "student_ai_hist_1"},
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "open_ai_chat_history")
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_teacher_mode_understands_review_queue_request(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Show the review queue and pending reviews.",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_review_1",
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "get_teacher_review_queue_summary")
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_teacher_mode_infers_homework_assignment_args(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Create homework on thermodynamics for class 12 with 10 questions and 60 minutes duration.",
                    "authority_level": "teacher_full_auto",
                    "context": {
                        "atlas_role": "teacher",
                        "account_id": "teacher_hw_1",
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "create_homework_assignment")
        self.assertEqual(body.get("args", {}).get("class_name"), "Class 12")
        self.assertEqual(body.get("args", {}).get("question_count"), 10)
        self.assertEqual(body.get("args", {}).get("duration_minutes"), 60)
        self.assertEqual(body.get("args", {}).get("topic"), "Thermodynamics")

    def test_ai_app_agent_allows_report_system_issue_tool(self) -> None:
        fake_result = {
            "status": "ok",
            "answer": json.dumps(
                {
                    "type": "single_action",
                    "goal": "Diagnose the broken analytics screen",
                    "plan_id": "student_plan_issue_1",
                    "summary": "Atlas will diagnose and report the issue.",
                    "tool": "report_system_issue",
                    "title": "Report system issue",
                    "detail": "Investigate the failing analytics flow.",
                    "risk": "low",
                    "args": {
                        "issue_summary": "Analytics screen is lagging and AI is not working",
                        "surface": "student_analytics",
                    },
                }
            ),
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Analytics is lagging and AI is not working",
                    "context": {
                        "account_id": "student_91",
                        "surface": "student_analytics",
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "report_system_issue")
        self.assertEqual(body.get("actions", [])[0].get("tool"), "report_system_issue")

    def test_ai_app_agent_understands_live_media_quality_issue_language(self) -> None:
        fake_result = {
            "status": "ok",
            "final_answer": "{}",
            "reasoning": "",
        }
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(return_value=fake_result),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "My video is blurry and the sound quality is bad in the live class.",
                    "context": {
                        "account_id": "student_91",
                        "surface": "live_class_student_chat",
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "report_system_issue")
        self.assertEqual(
            body.get("args", {}).get("failing_feature"),
            "live_media_quality",
        )
        self.assertEqual(body.get("recovery_mode"), "instruction_signals")

    def test_ai_app_agent_falls_back_to_deterministic_issue_plan_when_planner_fails(self) -> None:
        with patch(
            "app.routes._run_app_atlas_planner",
            new=AsyncMock(side_effect=RuntimeError("planner backend unavailable")),
        ):
            res = self.client.post(
                "/ai/app/agent",
                json={
                    "instruction": "Atlas is not working and the app is stuck on offline fallback because the backend is unreachable.",
                    "context": {
                        "account_id": "student_91",
                        "surface": "student_ai_chat",
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertEqual(body.get("tool"), "report_system_issue")
        self.assertEqual(
            body.get("actions", [])[0].get("tool"),
            "report_system_issue",
        )
        self.assertEqual(
            body.get("planner_recovery_mode"),
            "deterministic_signal_fallback",
        )
        self.assertIn(
            "planner backend unavailable",
            str(body.get("planner_error", "")),
        )

    def test_report_system_issue_runs_analysis_and_sends_support_mail(self) -> None:
        fake_analysis = {
            "summary": "Atlas found a likely slow analytics data path and recent AI generation failures.",
            "severity": "high",
            "likely_root_causes": [
                "Analytics screen is waiting on a slow or unstable AI-dependent data path."
            ],
            "plausible_causes_by_layer": {
                "client": ["Analytics screen is surfacing a visible error state."],
                "backend": ["Recent AI-generation jobs failed inside the app backend state."],
                "ai": ["Recent retrieval or provider diagnostics already show failure reasons."],
            },
            "evidence": [
                "Last surfaced error: Timeout while loading analytics",
                "Recent AI or retrieval requests have failure signals in the backend diagnostics.",
            ],
            "next_steps": [
                "Inspect the analytics load path first.",
                "Review recent AI fallback failures.",
            ],
            "impact_assessment": "The teacher-side analytics experience is degraded and may block review work.",
            "engineer_checklist": [
                "Reproduce the analytics flow.",
                "Review attached diagnostics JSON.",
            ],
            "user_safe_reply": "I investigated the issue and sent a detailed report.",
            "engineer_report": "Detailed engineer-facing report.",
        }
        with patch.object(
            routes._APP_DATA,
            "_ai_chat_or_solve",
            new=AsyncMock(
                return_value={
                    "answer": json.dumps(fake_analysis),
                    "explanation": "",
                }
            ),
        ), patch.object(
            routes._APP_DATA._atlas_incident_email,
            "send_incident_report",
            return_value={"ok": True, "sent": True, "message": "Support email sent"},
        ) as mocked_mail:
            res = self.client.post(
                "/app/action",
                json={
                    "action": "report_system_issue",
                    "issue": "Teacher analytics screen is lagging and AI is not working",
                    "role": "teacher",
                    "account_id": "teacher_7",
                    "surface": "teacher_analytics",
                    "context": {
                        "surface": "teacher_analytics",
                        "last_error": "Timeout while loading analytics",
                        "pending_homework_count": 2,
                    },
                },
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("mail_sent"))
        self.assertEqual(body.get("severity"), "high")
        self.assertIn("self_heal", body)
        self.assertIn("diagnostics", body)
        self.assertIn("runtime_logs", body)
        self.assertIn("engineer_checklist", body)
        mocked_mail.assert_called_once()

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

    def test_ai_chat_prefers_rich_explanation_for_study_material_surface(self) -> None:
        with patch(
            "core.api.entrypoint.lalacore_entry",
            new=AsyncMock(
                return_value={
                    "status": "ok",
                    "final_answer": "Unit mismatch.",
                    "reasoning": (
                        "Rotational motion summary: revise angular velocity, torque, "
                        "moment of inertia, and rolling constraints first. "
                        "The main trap is mixing linear and angular units."
                    ),
                    "winner_provider": "groq",
                    "engine": {"version": "research-grade-v2"},
                }
            ),
        ):
            response = self.client.post(
                "/app/action",
                json={
                    "action": "ai_chat",
                    "prompt": "Give me a crisp study summary from this material and tell me the main trap.",
                    "options": {"function": "study_material_chat"},
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertIn("Rotational motion summary", str(body.get("answer")))
        self.assertNotEqual(str(body.get("answer")), "Unit mismatch.")

    def test_ai_chat_keeps_short_math_answer_for_general_chat(self) -> None:
        with patch(
            "core.api.entrypoint.lalacore_entry",
            new=AsyncMock(
                return_value={
                    "status": "ok",
                    "final_answer": "x = 2",
                    "reasoning": "Use the quadratic formula and simplify.",
                    "winner_provider": "openrouter",
                    "engine": {"version": "research-grade-v2"},
                }
            ),
        ):
            response = self.client.post(
                "/app/action",
                json={
                    "action": "ai_chat",
                    "prompt": "solve x^2 - 4 = 0",
                    "options": {"function": "general_chat"},
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("answer"), "x = 2")

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
        ) as mocked_search:
            response = self.client.post(
                "/app/action",
                json={
                    "action": "lc9_web_verify_query",
                    "query": "JEE advanced quadratic question",
                    "max_rows": 5,
                    "search_scope": "general_ai",
                    "timeout_s": 3.25,
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("status"), "SUCCESS")
        self.assertEqual(body.get("count"), 1)
        self.assertEqual(body.get("search_scope"), "general_ai")
        self.assertAlmostEqual(float(body.get("timeout_s") or 0.0), 3.25, places=2)
        self.assertEqual(
            mocked_search.call_args.kwargs.get("search_scope"),
            "general_ai",
        )
        self.assertAlmostEqual(
            float(mocked_search.call_args.kwargs.get("total_timeout_s") or 0.0),
            3.25,
            places=2,
        )

    def test_stackexchange_provider_rows_include_fetch_url(self) -> None:
        sample = {
            "items": [
                {
                    "question_id": 2626594,
                    "link": "https://math.stackexchange.com/questions/2626594/example",
                    "title": "Hyperbola eccentricity from asymptotes",
                    "tags": ["analytic-geometry", "conic-sections"],
                    "answer_count": 4,
                    "score": 2,
                }
            ]
        }
        rows = routes._APP_DATA._extract_search_rows_from_stackexchange_json(
            raw_json=json.dumps(sample),
            max_rows=5,
            site="math",
            search_scope="general_ai",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["url"], "https://math.stackexchange.com/questions/2626594/example")
        self.assertIn("stackprinter.appspot.com/export", rows[0]["fetch_url"])
        self.assertIn("answers: 4", rows[0]["snippet"])

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

    def test_save_result_preserves_rich_analytics_fields(self) -> None:
        save_result = self.client.post(
            "/app/action",
            json={
                "action": "save_result",
                "quiz_id": "quiz_rich_1",
                "quiz_title": "Thermodynamics",
                "student_name": "Aarav",
                "student_id": "stu_aarav",
                "account_id": "stu_aarav",
                "score": 68,
                "max_score": 100,
                "correct": 17,
                "wrong": 4,
                "skipped": 4,
                "total_time": 1320,
                "section_accuracy": {
                    "Thermodynamics": 52.0,
                    "Mechanics": 78.0,
                },
                "user_answers": {"1": ["A"], "2": ["B"]},
            },
        )
        self.assertEqual(save_result.status_code, 200)
        self.assertTrue(save_result.json().get("ok"))

        peer_result = self.client.post(
            "/app/action",
            json={
                "action": "save_result",
                "quiz_id": "quiz_peer_1",
                "quiz_title": "Rotational Motion",
                "student_name": "Riya Sharma",
                "student_id": "stu_riya",
                "account_id": "stu_riya",
                "score": 76,
                "max_score": 100,
            },
        )
        self.assertEqual(peer_result.status_code, 200)
        self.assertTrue(peer_result.json().get("ok"))

        results = self.client.get("/app/action", params={"action": "get_results"})
        self.assertEqual(results.status_code, 200)
        rows = results.json().get("list", [])
        rich = next(
            row for row in rows if row.get("quiz_id") == "quiz_rich_1"
        )
        self.assertEqual(rich.get("quiz_title"), "Thermodynamics")
        self.assertEqual(rich.get("max_score"), 100.0)
        self.assertEqual(rich.get("total_time"), 1320)
        self.assertEqual(
            rich.get("section_accuracy", {}).get("Thermodynamics"),
            52.0,
        )
        self.assertEqual(
            rich.get("user_answers", {}).get("1"),
            ["A"],
        )

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

    def test_save_result_marks_first_attempt_and_reattempt(self) -> None:
        for score in (62, 88):
            response = self.client.post(
                "/app/action",
                json={
                    "action": "save_result",
                    "quiz_id": "quiz_attempts_1",
                    "quiz_title": "Organic Chemistry",
                    "student_name": "Aarav",
                    "student_id": "stu_aarav",
                    "account_id": "stu_aarav",
                    "score": score,
                    "max_score": 100,
                },
            )
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json().get("ok"))

        results = self.client.get("/app/action", params={"action": "get_results"})
        self.assertEqual(results.status_code, 200)
        rows = [
            row
            for row in results.json().get("list", [])
            if row.get("quiz_id") == "quiz_attempts_1"
        ]
        self.assertEqual(len(rows), 2)
        latest = rows[0]
        first = rows[1]
        self.assertEqual(latest.get("attempt_index"), 2)
        self.assertTrue(latest.get("is_reattempt"))
        self.assertFalse(latest.get("counts_for_teacher_analytics"))
        self.assertEqual(first.get("attempt_index"), 1)
        self.assertFalse(first.get("is_reattempt"))
        self.assertTrue(first.get("counts_for_teacher_analytics"))
        self.assertEqual(latest.get("first_attempt_id"), first.get("id"))
        self.assertEqual(latest.get("total_attempts_for_quiz"), 2)

    def test_save_result_sends_submission_mail_for_first_attempt_and_reattempt(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Permutation Homework",
                "type": "Homework",
                "deadline": "2026-12-01T00:00:00Z",
                "duration": 25,
                "questions": [
                    {"text": "nPr formula?", "correct": "n!/(n-r)!"},
                    {"text": "2 + 2 = ?", "correct": "4"},
                ],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        self.assertTrue(quiz_id)

        with patch.object(
            routes._APP_DATA._atlas_incident_email,
            "send_assessment_submission_report",
            return_value={"ok": True, "sent": True, "message": "sent"},
        ) as mocked_mail:
            first = self.client.post(
                "/app/action",
                json={
                    "action": "save_result",
                    "quiz_id": quiz_id,
                    "quiz_title": "Permutation Homework",
                    "student_name": "Aarav",
                    "student_id": "stu_aarav",
                    "account_id": "stu_aarav",
                    "email": "aarav@example.com",
                    "score": 52,
                    "max_score": 100,
                    "correct": 8,
                    "wrong": 2,
                    "skipped": 0,
                    "total_time": 960,
                },
            )
            second = self.client.post(
                "/app/action",
                json={
                    "action": "save_result",
                    "quiz_id": quiz_id,
                    "quiz_title": "Permutation Homework",
                    "student_name": "Aarav",
                    "student_id": "stu_aarav",
                    "account_id": "stu_aarav",
                    "email": "aarav@example.com",
                    "score": 81,
                    "max_score": 100,
                    "correct": 14,
                    "wrong": 1,
                    "skipped": 0,
                    "total_time": 780,
                },
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(mocked_mail.call_count, 2)

        first_report = mocked_mail.call_args_list[0].kwargs["report"]
        second_report = mocked_mail.call_args_list[1].kwargs["report"]

        self.assertEqual(first_report.get("submission_kind"), "first_attempt")
        self.assertEqual(first_report.get("attempt_index"), 1)
        self.assertTrue(first_report.get("counts_for_teacher_analytics"))
        self.assertEqual(first_report.get("student_email"), "aarav@example.com")
        self.assertEqual(second_report.get("submission_kind"), "reattempt")
        self.assertEqual(second_report.get("attempt_index"), 2)
        self.assertFalse(second_report.get("counts_for_teacher_analytics"))
        self.assertEqual(
            second_report.get("first_attempt_baseline", {}).get("score_pct"),
            52.0,
        )
        self.assertEqual(mocked_mail.call_args_list[1].kwargs["recipient"], "")

    def test_save_result_defers_noncritical_reports_in_production(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Deferred Report Quiz",
                "type": "Homework",
                "deadline": "2026-12-01T00:00:00Z",
                "duration": 25,
                "questions": [
                    {"text": "nPr formula?", "correct": "n!/(n-r)!"},
                ],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        self.assertTrue(quiz_id)

        with patch.dict(os.environ, {"APP_ENV": "production"}, clear=False):
            with patch.object(
                routes._APP_DATA,
                "_schedule_background_task",
            ) as mocked_schedule:
                saved = self.client.post(
                    "/app/action",
                    json={
                        "action": "save_result",
                        "quiz_id": quiz_id,
                        "quiz_title": "Deferred Report Quiz",
                        "student_name": "Aarav",
                        "student_id": "stu_aarav",
                        "account_id": "stu_aarav",
                        "email": "aarav@example.com",
                        "score": 52,
                        "max_score": 100,
                    },
                )

        self.assertEqual(saved.status_code, 200)
        self.assertTrue(saved.json().get("ok"))
        self.assertEqual(mocked_schedule.call_count, 1)

    def test_save_result_does_not_merge_same_named_students_without_stable_identity(self) -> None:
        for score in (41, 77):
            response = self.client.post(
                "/app/action",
                json={
                    "action": "save_result",
                    "quiz_id": "quiz_same_name_no_identity",
                    "quiz_title": "Coordinate Geometry",
                    "student_name": "Aarav",
                    "score": score,
                    "max_score": 100,
                },
            )
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json().get("ok"))

        results = self.client.get("/app/action", params={"action": "get_results"})
        self.assertEqual(results.status_code, 200)
        rows = [
            row
            for row in results.json().get("list", [])
            if row.get("quiz_id") == "quiz_same_name_no_identity"
        ]
        self.assertEqual(len(rows), 2)
        self.assertEqual([row.get("attempt_index") for row in rows], [1, 1])
        self.assertEqual([row.get("is_reattempt") for row in rows], [False, False])
        self.assertEqual(
            [row.get("counts_for_teacher_analytics") for row in rows],
            [True, True],
        )

    def test_assessment_deadline_report_sends_once_after_deadline(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Past Deadline Quiz",
                "type": "Exam",
                "deadline": "2025-01-01T00:00:00Z",
                "duration": 20,
                "questions": [{"text": "2 + 2 = ?", "correct": "4"}],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        self.assertTrue(quiz_id)

        with patch.object(
            routes._APP_DATA._atlas_incident_email,
            "send_assessment_report",
            return_value={"ok": True, "sent": True, "message": "sent"},
        ) as mocked_mail:
            saved = self.client.post(
                "/app/action",
                json={
                    "action": "save_result",
                    "quiz_id": quiz_id,
                    "quiz_title": "Past Deadline Quiz",
                    "student_name": "Riya",
                    "student_id": "stu_riya",
                    "account_id": "stu_riya",
                    "score": 71,
                    "max_score": 100,
                },
            )
            self.assertEqual(saved.status_code, 200)
            self.assertTrue(saved.json().get("ok"))
            self.assertEqual(mocked_mail.call_count, 1)

            listed = self.client.get("/app/action", params={"action": "get_assessments"})
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(mocked_mail.call_count, 1)

        assessment = next(
            row for row in routes._APP_DATA._assessments if row.get("id") == quiz_id
        )
        metadata = assessment.get("metadata") or {}
        self.assertTrue(metadata.get("deadline_report_mail_sent"))
        self.assertTrue(str(metadata.get("deadline_report_sent_at") or "").strip())

    def test_get_results_defers_due_report_scan_in_production(self) -> None:
        created = self.client.post(
            "/app/action",
            json={
                "action": "create_quiz",
                "title": "Deferred Results Scan Quiz",
                "type": "Exam",
                "deadline": "2025-01-01T00:00:00Z",
                "duration": 20,
                "questions": [{"text": "2 + 2 = ?", "correct": "4"}],
            },
        )
        self.assertEqual(created.status_code, 200)
        quiz_id = str(created.json().get("id") or "")
        self.assertTrue(quiz_id)

        saved = self.client.post(
            "/app/action",
            json={
                "action": "save_result",
                "quiz_id": quiz_id,
                "quiz_title": "Deferred Results Scan Quiz",
                "student_name": "Riya",
                "student_id": "stu_riya",
                "account_id": "stu_riya",
                "score": 71,
                "max_score": 100,
            },
        )
        self.assertEqual(saved.status_code, 200)
        self.assertTrue(saved.json().get("ok"))

        with patch.dict(os.environ, {"APP_ENV": "production"}, clear=False):
            with patch.object(
                routes._APP_DATA,
                "_schedule_background_task",
            ) as mocked_schedule:
                listed = self.client.get("/app/action", params={"action": "get_results"})

        self.assertEqual(listed.status_code, 200)
        self.assertTrue(listed.json().get("ok"))
        self.assertEqual(mocked_schedule.call_count, 1)

    def test_ai_chat_history_is_account_scoped(self) -> None:
        saved = self.client.post(
            "/app/action",
            json={
                "action": "save_ai_chat_history",
                "account_id": "student_11",
                "chat_id": "AI_student_11_today",
                "title": "Kinematics doubts",
                "messages": [
                    {"role": "user", "content": "Explain projectile motion"},
                    {"role": "assistant", "content": "Start with x and y components"},
                ],
            },
        )
        self.assertEqual(saved.status_code, 200)
        self.assertTrue(saved.json().get("ok"))

        listing = self.client.post(
            "/app/action",
            json={
                "action": "list_ai_chat_sessions",
                "account_id": "student_11",
            },
        )
        self.assertEqual(listing.status_code, 200)
        rows = listing.json().get("list", [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("chat_id"), "AI_student_11_today")

        hidden = self.client.post(
            "/app/action",
            json={
                "action": "list_ai_chat_sessions",
                "account_id": "student_22",
            },
        )
        self.assertEqual(hidden.status_code, 200)
        self.assertEqual(hidden.json().get("list", []), [])

        history = self.client.post(
            "/app/action",
            json={
                "action": "get_ai_chat_history",
                "account_id": "student_11",
                "chat_id": "AI_student_11_today",
            },
        )
        self.assertEqual(history.status_code, 200)
        messages = history.json().get("messages", [])
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0].get("role"), "user")

    def test_chat_thread_canonicalizes_teacher_alias_and_dedupes_retry_by_message_id(self) -> None:
        first = self.client.post(
            "/app/action",
            json={
                "action": "send_message",
                "is_peer": True,
                "chat_id": "student_1|teacher",
                "participants": "student_1,TEACHER",
                "payload": {
                    "id": "m_same",
                    "sender": "student_1",
                    "senderName": "Student One",
                    "text": "hello teacher",
                    "type": "text",
                    "time": 1730000000000,
                },
            },
        )
        self.assertEqual(first.status_code, 200)
        self.assertTrue(first.json().get("ok"))
        self.assertEqual(first.json().get("chat_id"), "student_1|TEACHER")

        second = self.client.post(
            "/app/action",
            json={
                "action": "send_message",
                "is_peer": True,
                "chat_id": "student_1|TEACHER",
                "participants": "student_1,TEACHER",
                "payload": {
                    "id": "m_same",
                    "sender": "student_1",
                    "senderName": "Student One",
                    "text": "hello teacher",
                    "type": "text",
                    "time": 1730000000000,
                },
            },
        )
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second.json().get("ok"))
        self.assertEqual(second.json().get("chat_id"), "student_1|TEACHER")

        directory = self.client.post(
            "/app/action",
            json={
                "action": "list_chat_directory",
                "chat_id": "student_1",
                "role": "student",
            },
        )
        self.assertEqual(directory.status_code, 200)
        rows = directory.json().get("list", [])
        target = next(
            (row for row in rows if row.get("chat_id") == "student_1|TEACHER"),
            None,
        )
        self.assertIsNotNone(target)
        message_ids = [
            (entry.get("id") if isinstance(entry, dict) else None)
            for entry in (target or {}).get("messages", [])
        ]
        self.assertEqual(message_ids.count("m_same"), 1)

    def test_chat_search_and_directory_resolve_users_from_auth_sqlite_store(self) -> None:
        self._auth_store.write_json(
            "auth_users",
            {
                "me@example.com": {
                    "student_id": "ME123",
                    "name": "Aman Kumar",
                    "username": "aman",
                    "email": "me@example.com",
                },
                "friend@example.com": {
                    "student_id": "FRI123",
                    "name": "Riya Sharma",
                    "username": "riya",
                    "email": "friend@example.com",
                },
            },
        )

        search = self.client.post(
            "/app/action",
            json={
                "action": "search_chat_users",
                "user_id": "ME123",
                "role": "student",
                "query": "riya",
            },
        )
        self.assertEqual(search.status_code, 200)
        self.assertTrue(search.json().get("ok"))
        matches = search.json().get("list", [])
        self.assertTrue(
            any(
                row.get("user_id") == "FRI123"
                and row.get("name") == "Riya Sharma"
                for row in matches
            )
        )

        sent = self.client.post(
            "/app/action",
            json={
                "action": "send_message",
                "is_peer": True,
                "chat_id": "ME123|FRI123",
                "participants": "ME123,FRI123",
                "payload": {
                    "id": "m_sqlite_friend",
                    "sender": "ME123",
                    "senderName": "Aman Kumar",
                    "text": "Hey Riya",
                    "type": "text",
                    "time": 1730000000456,
                },
            },
        )
        self.assertEqual(sent.status_code, 200)
        self.assertTrue(sent.json().get("ok"))

        directory = self.client.post(
            "/app/action",
            json={
                "action": "list_chat_directory",
                "chat_id": "ME123",
                "role": "student",
            },
        )
        self.assertEqual(directory.status_code, 200)
        self.assertTrue(directory.json().get("ok"))
        target = next(
            (
                row
                for row in directory.json().get("list", [])
                if row.get("friend_id") == "FRI123"
            ),
            None,
        )
        self.assertIsNotNone(target)
        self.assertEqual((target or {}).get("friend_name"), "Riya Sharma")

    def test_teacher_review_queue_preserves_rich_doubt_context(self) -> None:
        queued = self.client.post(
            "/app/action",
            json={
                "action": "queue_teacher_review",
                "quiz_id": "quiz_ctx",
                "quiz_title": "Hyperbola Drill",
                "question_id": "3",
                "question_text": "Find the eccentricity of x^2/16 - y^2/9 = 1",
                "question_image": "https://example.com/q.png",
                "student_answer": "5/4",
                "correct_answer": "5/4",
                "student_id": "stu_riya",
                "student_name": "Riya Sharma",
                "message": "Please check if my asymptotes are correct too.",
                "subject": "Mathematics",
                "chapter": "Hyperbola",
                "source_surface": "answer_key_teacher_review",
                "answer_key_card": {
                    "question_text": "Find the eccentricity of x^2/16 - y^2/9 = 1",
                    "correct_answer": "5/4",
                },
            },
        )
        self.assertEqual(queued.status_code, 200)
        body = queued.json()
        self.assertTrue(body.get("ok"))
        queue_item = body.get("queue_item", {})
        self.assertEqual(queue_item.get("quiz_title"), "Hyperbola Drill")
        self.assertEqual(queue_item.get("student_name"), "Riya Sharma")
        self.assertEqual(queue_item.get("subject"), "Mathematics")
        self.assertEqual(queue_item.get("chapter"), "Hyperbola")
        self.assertEqual(queue_item.get("source_surface"), "answer_key_teacher_review")
        self.assertIsInstance(queue_item.get("answer_key_card"), dict)

    def test_ops_atlas_maintenance_run_uses_shared_maintenance_service(self) -> None:
        with patch.object(routes, "_ATLAS_MAINTENANCE") as maintenance:
            maintenance.run_weekly_maintenance = AsyncMock(
                return_value={
                    "ok": True,
                    "trigger": "manual",
                    "maintenance_report": {
                        "mail_sent": True,
                        "incident_id": "atlas_incident_123",
                    },
                }
            )
            res = self.client.post(
                "/ops/atlas-maintenance/run",
                json={"trigger": "manual"},
            )

        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("trigger"), "manual")
        self.assertTrue(body.get("maintenance_report", {}).get("mail_sent"))


if __name__ == "__main__":
    unittest.main()
