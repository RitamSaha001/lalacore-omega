import json
import tempfile
import unittest
from pathlib import Path

from app.storage.sqlite_json_store import SQLiteJsonBlobStore
from services.assessment_assignment_announcement_service import (
    AssessmentAssignmentAnnouncementService,
)


class _FakeEmailService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def send_assignment_announcement(self, *, report, recipient=None):
        self.calls.append({"report": dict(report), "recipient": recipient})
        return {
            "ok": True,
            "sent": True,
            "message": "assignment sent",
        }


class AssessmentAssignmentAnnouncementServiceTests(unittest.TestCase):
    def test_notify_assessment_assigned_sends_only_to_deliverable_students_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            auth_root = root / "auth"
            app_root = root / "app"
            auth_root.mkdir(parents=True, exist_ok=True)
            app_root.mkdir(parents=True, exist_ok=True)
            (auth_root / "users.json").write_text(
                json.dumps(
                    {
                        "student1@school.edu": {
                            "email": "student1@school.edu",
                            "role": "student",
                        },
                        "teacher@school.edu": {
                            "email": "teacher@school.edu",
                            "role": "teacher",
                        },
                        "student@example.com": {
                            "email": "student@example.com",
                            "role": "student",
                        },
                    }
                ),
                encoding="utf-8",
            )
            SQLiteJsonBlobStore(auth_root / "auth_store.sqlite3").write_json(
                "auth_users",
                {
                    "student2@school.edu": {
                        "email": "student2@school.edu",
                        "role": "student",
                    }
                },
            )
            fake_email = _FakeEmailService()
            service = AssessmentAssignmentAnnouncementService(
                email_service=fake_email,
                assessments_file=app_root / "assessments.json",
                auth_users_file=auth_root / "users.json",
                auth_storage_db_file=auth_root / "auth_store.sqlite3",
                app_storage_db_file=app_root / "app_data.sqlite3",
            )

            assessment = {
                "id": "quiz_1",
                "title": "Vectors Mock",
                "type": "Exam",
                "question_count": 20,
                "duration": 45,
                "deadline": "2026-04-02T10:00:00Z",
            }
            first = service.notify_assessment_assigned(assessment)
            second = service.notify_assessment_assigned(assessment)

        self.assertTrue(first.get("ok"))
        self.assertEqual(first.get("sent_count"), 2)
        self.assertEqual(second.get("sent_count"), 0)
        self.assertEqual(
            [call["recipient"] for call in fake_email.calls],
            ["student1@school.edu", "student2@school.edu"],
        )

    def test_notify_pending_assessments_for_email_catches_up_only_exam_and_homework(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            auth_root = root / "auth"
            app_root = root / "app"
            auth_root.mkdir(parents=True, exist_ok=True)
            app_root.mkdir(parents=True, exist_ok=True)
            (app_root / "assessments.json").write_text(
                json.dumps(
                    [
                        {"id": "quiz_exam", "title": "Mock 1", "type": "Exam"},
                        {
                            "id": "quiz_hw",
                            "title": "Homework 1",
                            "type": "Homework",
                        },
                        {"id": "quiz_other", "title": "Practice", "type": "Practice"},
                    ]
                ),
                encoding="utf-8",
            )
            fake_email = _FakeEmailService()
            service = AssessmentAssignmentAnnouncementService(
                email_service=fake_email,
                assessments_file=app_root / "assessments.json",
                auth_users_file=auth_root / "users.json",
                auth_storage_db_file=auth_root / "auth_store.sqlite3",
                app_storage_db_file=app_root / "app_data.sqlite3",
            )

            result = service.notify_pending_assessments_for_email("latejoiner@school.edu")

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("sent_count"), 2)
        self.assertEqual(
            [call["report"]["assessment_id"] for call in fake_email.calls],
            ["quiz_exam", "quiz_hw"],
        )

    def test_assignment_report_keeps_scheduled_start_time_for_mail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            auth_root = root / "auth"
            app_root = root / "app"
            auth_root.mkdir(parents=True, exist_ok=True)
            app_root.mkdir(parents=True, exist_ok=True)
            fake_email = _FakeEmailService()
            service = AssessmentAssignmentAnnouncementService(
                email_service=fake_email,
                assessments_file=app_root / "assessments.json",
                auth_users_file=auth_root / "users.json",
                auth_storage_db_file=auth_root / "auth_store.sqlite3",
                app_storage_db_file=app_root / "app_data.sqlite3",
            )

            result = service.notify_assessment_assigned(
                {
                    "id": "quiz_sched",
                    "title": "Scheduled Mock",
                    "type": "Exam",
                    "start_at": "2026-04-05T06:30:00Z",
                    "deadline": "2026-04-06T10:00:00Z",
                    "question_count": 30,
                }
            )

        self.assertTrue(result.get("ok"))
        self.assertEqual(fake_email.calls, [])
        report = service._build_assignment_report(
            assessment={
                "id": "quiz_sched",
                "title": "Scheduled Mock",
                "type": "Exam",
                "start_at": "2026-04-05T06:30:00Z",
                "deadline": "2026-04-06T10:00:00Z",
                "question_count": 30,
            },
            email="student@school.edu",
        )
        self.assertEqual(report["start_at"], "2026-04-05T06:30:00Z")
