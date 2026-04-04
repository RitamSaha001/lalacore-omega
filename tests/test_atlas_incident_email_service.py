import json
import os
import smtplib
import socket
import tempfile
import unittest
from unittest.mock import patch

from services.atlas_incident_email_service import AtlasIncidentEmailService


class AtlasIncidentEmailServiceTests(unittest.TestCase):
    def test_send_incident_report_without_smtp_returns_structured_failure(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ATLAS_SUPPORT_SENDER_EMAIL": "",
                "ATLAS_SUPPORT_SENDER_PASSWORD": "",
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                "OTP_SENDER_EMAIL": "",
                "OTP_SENDER_PASSWORD": "",
                "FORGOT_OTP_SENDER_EMAIL": "",
            },
            clear=False,
        ):
            service = AtlasIncidentEmailService()
            result = service.send_incident_report(
                report={
                    "incident_id": "atlas_incident_1",
                    "issue_summary": "Atlas AI failed to respond",
                    "severity": "high",
                    "reporter": {"email": "teacher@example.com"},
                }
            )

        self.assertIsInstance(result, dict)
        self.assertFalse(result.get("sent", False))
        self.assertIn("message", result)

    def test_release_confirmation_sends_to_all_configured_recipients(self) -> None:
        class _FakeSMTP:
            last_instance: "_FakeSMTP | None" = None

            def __init__(self, *args, **kwargs) -> None:
                self.sent_to = None
                self.message = None
                _FakeSMTP.last_instance = self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def ehlo(self) -> None:
                return None

            def starttls(self, context=None) -> None:
                return None

            def login(self, sender, password) -> None:
                return None

            def send_message(self, msg, to_addrs=None) -> None:
                self.message = msg
                self.sent_to = list(to_addrs or [])

        recipients = (
            "ops.primary@example.com,"
            "ops.secondary@example.com,"
            "ops.third@example.com"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                os.environ,
                {
                    "APP_ENV": "production",
                    "ATLAS_SUPPORT_EMAIL_RECIPIENT": recipients,
                    "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                    "ATLAS_RELEASE_CONFIRMATION_STATE_PATH": os.path.join(
                        tmpdir,
                        "state.json",
                    ),
                    "OTP_SENDER_EMAIL": "sender@example.com",
                    "OTP_SENDER_PASSWORD": "secret",
                    "OTP_SMTP_HOST": "smtp.example.com",
                    "OTP_SMTP_PORT": "587",
                    "OTP_SMTP_SECURITY": "tls",
                },
                clear=False,
            ), patch.object(smtplib, "SMTP", _FakeSMTP):
                service = AtlasIncidentEmailService()
                result = service.send_release_confirmation(
                    releases=[
                        {
                            "version": "1.0.11",
                            "build_number": "12",
                            "audience": "all",
                        }
                    ],
                    sheet_url="https://example.com/updates.csv",
                )

        self.assertTrue(result.get("sent", False))
        self.assertEqual(
            result.get("recipients"),
            [
                "ops.primary@example.com",
                "ops.secondary@example.com",
                "ops.third@example.com",
            ],
        )
        self.assertIsNotNone(_FakeSMTP.last_instance)
        self.assertEqual(
            _FakeSMTP.last_instance.sent_to,
            [
                "ops.primary@example.com",
                "ops.secondary@example.com",
                "ops.third@example.com",
            ],
        )
        self.assertEqual(
            _FakeSMTP.last_instance.message["From"],
            "God of Maths <sender@example.com>",
        )
        html_body = _FakeSMTP.last_instance.message.get_body(preferencelist=("html",))
        self.assertIsNotNone(html_body)
        self.assertIn("Atlas published release metadata", html_body.get_content())

    def test_release_confirmation_skips_when_support_recipient_is_cleared_after_init(
        self,
    ) -> None:
        class _UnexpectedSMTP:
            def __init__(self, *args, **kwargs) -> None:
                raise AssertionError("SMTP should not be invoked")

        with patch.dict(
            os.environ,
            {
                "APP_ENV": "production",
                "ATLAS_SUPPORT_EMAIL_RECIPIENT": "ops@example.com",
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                "OTP_SENDER_EMAIL": "sender@example.com",
                "OTP_SENDER_PASSWORD": "secret",
                "OTP_SMTP_HOST": "smtp.example.com",
                "OTP_SMTP_PORT": "587",
                "OTP_SMTP_SECURITY": "tls",
            },
            clear=False,
        ):
            service = AtlasIncidentEmailService()

        with patch.dict(
            os.environ,
            {
                "APP_ENV": "production",
                "ATLAS_SUPPORT_EMAIL_RECIPIENT": "",
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                "OTP_SENDER_EMAIL": "sender@example.com",
                "OTP_SENDER_PASSWORD": "secret",
                "OTP_SMTP_HOST": "smtp.example.com",
                "OTP_SMTP_PORT": "587",
                "OTP_SMTP_SECURITY": "tls",
            },
            clear=False,
        ), patch.object(smtplib, "SMTP", _UnexpectedSMTP):
            result = service.send_release_confirmation(
                releases=[
                    {
                        "version": "3.1.0",
                        "build_number": "18",
                        "audience": "all",
                    }
                ],
                sheet_url="https://example.com/updates.csv",
            )

        self.assertTrue(result.get("ok"))
        self.assertFalse(result.get("sent"))
        self.assertEqual(result.get("recipients"), [])
        self.assertIn("not configured", str(result.get("message") or ""))

    def test_release_announcement_sends_individual_messages_to_signed_in_users(self) -> None:
        class _FakeSMTP:
            sent_messages: list[tuple[list[str], object]] = []

            def __init__(self, *args, **kwargs) -> None:
                return None

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def ehlo(self) -> None:
                return None

            def starttls(self, context=None) -> None:
                return None

            def login(self, sender, password) -> None:
                return None

            def send_message(self, msg, to_addrs=None) -> None:
                _FakeSMTP.sent_messages.append((list(to_addrs or []), msg))

        with patch.dict(
            os.environ,
            {
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                "OTP_SENDER_EMAIL": "sender@example.com",
                "OTP_SENDER_PASSWORD": "secret",
                "OTP_SMTP_HOST": "smtp.example.com",
                "OTP_SMTP_PORT": "587",
                "OTP_SMTP_SECURITY": "tls",
            },
            clear=False,
        ), patch.object(smtplib, "SMTP", _FakeSMTP):
            service = AtlasIncidentEmailService()
            result = service.send_release_announcement(
                releases=[
                    {
                        "version": "2.0.2",
                        "build_number": "16",
                        "audience": "all",
                        "platform": "android",
                        "android_url": "https://example.com/app.apk",
                        "message": "A fresh update is ready.",
                        "release_notes": "Railway stability\nTeacher analytics polish",
                    }
                ],
                sheet_url="https://example.com/updates.csv",
                recipients=["student1@example.com", "student2@example.com"],
                trigger="publish_script",
                checked_at="2026-03-31T12:00:00Z",
            )

        self.assertTrue(result.get("sent", False))
        self.assertEqual(result.get("sent_count"), 2)
        self.assertEqual(len(_FakeSMTP.sent_messages), 2)
        self.assertEqual(
            _FakeSMTP.sent_messages[0][1]["From"],
            "God of Maths <sender@example.com>",
        )
        self.assertEqual(_FakeSMTP.sent_messages[0][0], ["student1@example.com"])
        plain_body = _FakeSMTP.sent_messages[0][1].get_body(
            preferencelist=("plain",)
        )
        self.assertIsNotNone(plain_body)
        self.assertIn("Railway stability", plain_body.get_content())
        html_body = _FakeSMTP.sent_messages[0][1].get_body(preferencelist=("html",))
        self.assertIsNotNone(html_body)
        self.assertIn("A polished LalaCore update is ready", html_body.get_content())
        self.assertIn("Download for Android", html_body.get_content())

    def test_assessment_submission_report_uses_god_of_maths_sender(self) -> None:
        class _FakeSMTP:
            last_instance: "_FakeSMTP | None" = None

            def __init__(self, *args, **kwargs) -> None:
                self.sent_to = None
                self.message = None
                _FakeSMTP.last_instance = self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def ehlo(self) -> None:
                return None

            def starttls(self, context=None) -> None:
                return None

            def login(self, sender, password) -> None:
                return None

            def send_message(self, msg, to_addrs=None) -> None:
                self.message = msg
                self.sent_to = list(to_addrs or [])

        with patch.dict(
            os.environ,
            {
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                "OTP_SENDER_EMAIL": "sender@example.com",
                "OTP_SENDER_PASSWORD": "secret",
                "OTP_SMTP_HOST": "smtp.example.com",
                "OTP_SMTP_PORT": "587",
                "OTP_SMTP_SECURITY": "tls",
            },
            clear=False,
        ), patch.object(smtplib, "SMTP", _FakeSMTP):
            service = AtlasIncidentEmailService()
            result = service.send_assessment_submission_report(
                report={
                    "assessment_title": "Vectors Homework",
                    "student_name": "Aarav",
                    "submission_kind": "reattempt",
                    "attempt_index": 2,
                    "total_attempts_for_quiz": 2,
                },
                recipient="sanny86@gmail.com",
            )

        self.assertTrue(result.get("sent", False))
        self.assertIsNotNone(_FakeSMTP.last_instance)
        self.assertEqual(_FakeSMTP.last_instance.sent_to, ["sanny86@gmail.com"])
        self.assertEqual(
            _FakeSMTP.last_instance.message["From"],
            "God of Maths <sender@example.com>",
        )
        html_body = _FakeSMTP.last_instance.message.get_body(preferencelist=("html",))
        self.assertIsNotNone(html_body)
        self.assertIn("Assessment submission detail", html_body.get_content())

    def test_assessment_submission_report_skips_when_recipient_not_configured(self) -> None:
        class _UnexpectedSMTP:
            def __init__(self, *args, **kwargs) -> None:
                raise AssertionError("SMTP should not be invoked")

        with patch.dict(
            os.environ,
            {
                "ATLAS_ASSESSMENT_SUBMISSION_EMAIL_RECIPIENT": "",
                "ATLAS_ASSESSMENT_REPORT_EMAIL_RECIPIENT": "",
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                "OTP_SENDER_EMAIL": "sender@example.com",
                "OTP_SENDER_PASSWORD": "secret",
                "OTP_SMTP_HOST": "smtp.example.com",
                "OTP_SMTP_PORT": "587",
                "OTP_SMTP_SECURITY": "tls",
            },
            clear=False,
        ), patch.object(smtplib, "SMTP", _UnexpectedSMTP):
            service = AtlasIncidentEmailService()
            result = service.send_assessment_submission_report(
                report={
                    "assessment_title": "Vectors Homework",
                    "student_name": "Aarav",
                }
            )

        self.assertTrue(result.get("ok"))
        self.assertFalse(result.get("sent"))
        self.assertEqual(result.get("recipients"), [])
        self.assertIn("not configured", str(result.get("message") or ""))

    def test_assignment_announcement_uses_god_of_maths_sender(self) -> None:
        class _FakeSMTP:
            last_instance: "_FakeSMTP | None" = None

            def __init__(self, *args, **kwargs) -> None:
                self.sent_to = None
                self.message = None
                _FakeSMTP.last_instance = self

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def ehlo(self) -> None:
                return None

            def starttls(self, context=None) -> None:
                return None

            def login(self, sender, password) -> None:
                return None

            def send_message(self, msg, to_addrs=None) -> None:
                self.message = msg
                self.sent_to = list(to_addrs or [])

        with patch.dict(
            os.environ,
            {
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "",
                "OTP_SENDER_EMAIL": "sender@example.com",
                "OTP_SENDER_PASSWORD": "secret",
                "OTP_SMTP_HOST": "smtp.example.com",
                "OTP_SMTP_PORT": "587",
                "OTP_SMTP_SECURITY": "tls",
            },
            clear=False,
        ), patch.object(smtplib, "SMTP", _FakeSMTP):
            service = AtlasIncidentEmailService()
            result = service.send_assignment_announcement(
                report={
                    "assessment_title": "Homework 1",
                    "assessment_type": "Homework",
                    "deadline": "2026-04-02T10:00:00Z",
                    "question_count": 15,
                    "total_marks": 60,
                },
                recipient="student@school.edu",
            )

        self.assertTrue(result.get("sent", False))
        self.assertIsNotNone(_FakeSMTP.last_instance)
        self.assertEqual(_FakeSMTP.last_instance.sent_to, ["student@school.edu"])
        self.assertEqual(
            _FakeSMTP.last_instance.message["From"],
            "God of Maths <sender@example.com>",
        )
        html_body = _FakeSMTP.last_instance.message.get_body(preferencelist=("html",))
        self.assertIsNotNone(html_body)
        self.assertIn("A new task is ready in LalaCore", html_body.get_content())
        self.assertIn("Homework 1", html_body.get_content())

    def test_apps_script_mailer_handles_automatic_mail_without_smtp(self) -> None:
        captured_payloads: list[dict[str, object]] = []

        class _FakeResponse:
            def __init__(self, body: str) -> None:
                self._body = body.encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self) -> bytes:
                return self._body

        def _fake_urlopen(request, timeout=None, context=None):
            del timeout, context
            captured_payloads.append(json.loads(request.data.decode("utf-8")))
            return _FakeResponse('{"ok": true, "status": "sent", "message": "Apps Script sent"}')

        with patch.dict(
            os.environ,
            {
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "https://script.google.com/macros/s/example/exec",
                "ATLAS_AUTOMAIL_SHARED_SECRET": "shared-secret",
                "OTP_SENDER_EMAIL": "",
                "OTP_SENDER_PASSWORD": "",
                "FORGOT_OTP_SENDER_EMAIL": "",
            },
            clear=False,
        ), patch("services.atlas_incident_email_service.urlopen", _fake_urlopen):
            service = AtlasIncidentEmailService()
            self.assertTrue(service.smtp_configured())
            result = service.send_assignment_announcement(
                report={
                    "assessment_title": "Homework 2",
                    "assessment_type": "Homework",
                    "deadline": "2026-04-02T10:00:00Z",
                    "question_count": 15,
                    "total_marks": 60,
                },
                recipient="student@school.edu",
            )

        self.assertTrue(result.get("sent", False))
        self.assertEqual(result.get("transport"), "apps_script")
        self.assertEqual(len(captured_payloads), 1)
        self.assertEqual(
            captured_payloads[0]["recipients"],
            ["student@school.edu"],
        )
        self.assertEqual(captured_payloads[0]["sender_name"], "God of Maths")
        self.assertEqual(captured_payloads[0]["secret"], "shared-secret")
        self.assertIn("Homework 2", str(captured_payloads[0]["text_body"]))
        self.assertIn("A new task is ready in LalaCore", str(captured_payloads[0]["html_body"]))

    def test_release_announcement_without_recipients_is_handled_without_failure(self) -> None:
        service = AtlasIncidentEmailService()
        result = service.send_release_announcement(
            releases=[
                {
                    "version": "3.0.1",
                    "build_number": "17",
                    "audience": "all",
                    "platform": "android",
                }
            ],
            sheet_url="https://example.com/updates.csv",
            recipients=[],
        )

        self.assertTrue(result.get("ok"))
        self.assertFalse(result.get("sent"))
        self.assertTrue(result.get("no_deliverable_recipients"))
        self.assertEqual(result.get("sent_count"), 0)

    def test_apps_script_mailer_retries_once_before_failing_over(self) -> None:
        attempts: list[int] = []

        class _FakeResponse:
            def __init__(self, body: str) -> None:
                self._body = body.encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self) -> bytes:
                return self._body

        def _fake_urlopen(request, timeout=None, context=None):
            del request, timeout, context
            attempts.append(1)
            if len(attempts) == 1:
                raise socket.timeout("timed out")
            return _FakeResponse('{"ok": true, "status": "sent", "message": "Apps Script sent"}')

        with patch.dict(
            os.environ,
            {
                "ATLAS_AUTOMAIL_WEBHOOK_URL": "https://script.google.com/macros/s/example/exec",
                "ATLAS_AUTOMAIL_RETRY_COUNT": "1",
                "ATLAS_AUTOMAIL_RETRY_BACKOFF_SECONDS": "0",
                "OTP_SENDER_EMAIL": "",
                "OTP_SENDER_PASSWORD": "",
                "FORGOT_OTP_SENDER_EMAIL": "",
            },
            clear=False,
        ), patch("services.atlas_incident_email_service.urlopen", _fake_urlopen), patch(
            "services.atlas_incident_email_service.time.sleep",
            lambda *_args, **_kwargs: None,
        ):
            service = AtlasIncidentEmailService()
            result = service.send_assignment_announcement(
                report={
                    "assessment_title": "Homework 3",
                    "assessment_type": "Homework",
                    "deadline": "2026-04-02T10:00:00Z",
                    "question_count": 15,
                    "total_marks": 60,
                },
                recipient="student@school.edu",
            )

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("attempts"), 2)
        self.assertEqual(len(attempts), 2)
