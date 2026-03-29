import os
import smtplib
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
            "saharitam171@gmail.com,"
            "sanny86@gmail.com,"
            "halder.saptajit2009@gmail.com"
        )
        with patch.dict(
            os.environ,
            {
                "ATLAS_SUPPORT_EMAIL_RECIPIENT": recipients,
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
                "saharitam171@gmail.com",
                "sanny86@gmail.com",
                "halder.saptajit2009@gmail.com",
            ],
        )
        self.assertIsNotNone(_FakeSMTP.last_instance)
        self.assertEqual(
            _FakeSMTP.last_instance.sent_to,
            [
                "saharitam171@gmail.com",
                "sanny86@gmail.com",
                "halder.saptajit2009@gmail.com",
            ],
        )
