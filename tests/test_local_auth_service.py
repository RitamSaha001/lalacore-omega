import os
import smtplib
import tempfile
import asyncio
import unittest
from pathlib import Path
from unittest.mock import patch

from app.auth.local_auth_service import LocalAuthService


class LocalAuthServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._prev_otp_email_enabled = os.environ.get("OTP_EMAIL_ENABLED")
        self._prev_reset_trusted_gate = os.environ.get(
            "OTP_REQUIRE_TRUSTED_DEVICE_FOR_RESET"
        )
        self._prev_reset_issuing_gate = os.environ.get(
            "OTP_REQUIRE_ISSUING_DEVICE_FOR_RESET"
        )
        self._prev_local_fallback = os.environ.get("OTP_ALLOW_LOCAL_FALLBACK")
        os.environ["OTP_EMAIL_ENABLED"] = "true"
        os.environ.pop("OTP_REQUIRE_TRUSTED_DEVICE_FOR_RESET", None)
        os.environ.pop("OTP_REQUIRE_ISSUING_DEVICE_FOR_RESET", None)
        os.environ.pop("OTP_ALLOW_LOCAL_FALLBACK", None)
        self.device_id = "dev_test_primary"
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.service = LocalAuthService(
            users_file=root / "users.json",
            otp_file=root / "otp.json",
        )

    async def asyncTearDown(self):
        if self._prev_otp_email_enabled is None:
            os.environ.pop("OTP_EMAIL_ENABLED", None)
        else:
            os.environ["OTP_EMAIL_ENABLED"] = self._prev_otp_email_enabled
        if self._prev_reset_trusted_gate is None:
            os.environ.pop("OTP_REQUIRE_TRUSTED_DEVICE_FOR_RESET", None)
        else:
            os.environ["OTP_REQUIRE_TRUSTED_DEVICE_FOR_RESET"] = (
                self._prev_reset_trusted_gate
            )
        if self._prev_reset_issuing_gate is None:
            os.environ.pop("OTP_REQUIRE_ISSUING_DEVICE_FOR_RESET", None)
        else:
            os.environ["OTP_REQUIRE_ISSUING_DEVICE_FOR_RESET"] = (
                self._prev_reset_issuing_gate
            )
        if self._prev_local_fallback is None:
            os.environ.pop("OTP_ALLOW_LOCAL_FALLBACK", None)
        else:
            os.environ["OTP_ALLOW_LOCAL_FALLBACK"] = self._prev_local_fallback
        self.tmp.cleanup()

    async def test_register_login_and_wrong_password(self):
        register = await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "student@example.com",
                "password": "abcd1234",
                "name": "Student One",
                "username": "student_one",
                "device_id": self.device_id,
            }
        )
        self.assertEqual(register.get("status"), "SUCCESS")
        self.assertTrue(register.get("student_id"))

        login_ok = await self.service.handle_action(
            {
                "action": "login_direct",
                "email": "student@example.com",
                "password": "abcd1234",
                "device_id": self.device_id,
            }
        )
        self.assertEqual(login_ok.get("status"), "SUCCESS")
        self.assertEqual(login_ok.get("name"), "Student One")

        login_bad = await self.service.handle_action(
            {
                "action": "login_direct",
                "email": "student@example.com",
                "password": "wrong-pass",
            }
        )
        self.assertEqual(login_bad.get("status"), "WRONG_PASSWORD")

    async def test_forgot_otp_and_password_reset(self):
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "learner@example.com",
                "password": "oldpass",
                "name": "Learner",
                "username": "learner",
                "device_id": self.device_id,
            }
        )

        with patch.object(
            LocalAuthService,
            "_send_otp_email",
            return_value=(True, "OTP sent"),
        ):
            req = await self.service.handle_action(
                {
                    "action": "request_forgot_otp",
                    "email": "learner@example.com",
                    "device_id": self.device_id,
                }
            )
        self.assertEqual(req.get("status"), "OTP_SENT")

        otp = self.service._otps["learner@example.com"]["otp"]
        reset = await self.service.handle_action(
            {
                "action": "forgot_password_reset",
                "email": "learner@example.com",
                "otp": otp,
                "new_password": "newpass123",
                "device_id": self.device_id,
            }
        )
        self.assertEqual(reset.get("status"), "SUCCESS")

        login = await self.service.handle_action(
            {
                "action": "login_direct",
                "email": "learner@example.com",
                "password": "newpass123",
            }
        )
        self.assertEqual(login.get("status"), "SUCCESS")

    async def test_otp_email_uses_god_of_maths_sender_name(self):
        class _FakeSMTP:
            last_instance: "_FakeSMTP | None" = None

            def __init__(self, *args, **kwargs) -> None:
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

            def send_message(self, msg) -> None:
                self.message = msg

        with patch.dict(
            os.environ,
            {
                "OTP_SENDER_EMAIL": "sender@example.com",
                "OTP_SENDER_PASSWORD": "secret",
                "OTP_SMTP_HOST": "smtp.example.com",
                "OTP_SMTP_PORT": "587",
                "OTP_SMTP_SECURITY": "tls",
            },
            clear=False,
        ), patch.object(smtplib, "SMTP", _FakeSMTP):
            sent, message = self.service._send_otp_email(
                "learner@example.com",
                "123456",
                600,
            )

        self.assertTrue(sent)
        self.assertEqual(message, "OTP sent")
        self.assertIsNotNone(_FakeSMTP.last_instance)
        self.assertEqual(
            _FakeSMTP.last_instance.message["From"],
            "God of Maths <sender@example.com>",
        )
        html_body = _FakeSMTP.last_instance.message.get_body(preferencelist=("html",))
        self.assertIsNotNone(html_body)
        self.assertIn("Your secure reset code", html_body.get_content())
        self.assertIn("123456", html_body.get_content())

    async def test_unknown_action(self):
        response = await self.service.handle_action({"action": "not_real"})
        self.assertEqual(response.get("status"), "UNKNOWN_ACTION")

    async def test_login_triggers_pending_assignment_notification_sync(self):
        class _FakeAssignmentAnnouncementService:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def notify_pending_assessments_for_email(self, email: str):
                self.calls.append(email)
                return {"ok": True, "sent_count": 0}

        fake_assignment = _FakeAssignmentAnnouncementService()
        service = LocalAuthService(
            users_file=Path(self.tmp.name) / "users_2.json",
            otp_file=Path(self.tmp.name) / "otp_2.json",
            assignment_announcement_service=fake_assignment,
        )
        await service.handle_action(
            {
                "action": "register_direct",
                "email": "student@school.edu",
                "password": "abcd1234",
                "name": "Student One",
                "username": "student_one",
                "device_id": self.device_id,
            }
        )
        await service.handle_action(
            {
                "action": "login_direct",
                "email": "student@school.edu",
                "password": "abcd1234",
                "device_id": self.device_id,
            }
        )
        await asyncio.sleep(0.05)
        self.assertEqual(
            fake_assignment.calls,
            ["student@school.edu", "student@school.edu"],
        )

    async def test_login_triggers_pending_release_notification_sync(self):
        class _FakeAssignmentAnnouncementService:
            def notify_pending_assessments_for_email(self, email: str):
                return {"ok": True, "sent_count": 0, "email": email}

        class _FakeReleaseNotifierService:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str, str]] = []

            async def notify_pending_releases_for_email_async(
                self,
                email: str,
                *,
                role: str,
                platform: str = "",
            ):
                return self.notify_pending_releases_for_email(
                    email,
                    role=role,
                    platform=platform,
                )

            def notify_pending_releases_for_email(
                self,
                email: str,
                *,
                role: str,
                platform: str = "",
            ):
                self.calls.append((email, role, platform))
                return {"ok": True, "sent_count": 0}

        fake_release = _FakeReleaseNotifierService()
        service = LocalAuthService(
            users_file=Path(self.tmp.name) / "users_3.json",
            otp_file=Path(self.tmp.name) / "otp_3.json",
            assignment_announcement_service=_FakeAssignmentAnnouncementService(),
            release_notifier_service=fake_release,
        )
        await service.handle_action(
            {
                "action": "register_direct",
                "email": "student@school.edu",
                "password": "abcd1234",
                "name": "Student One",
                "username": "student_one",
                "device_id": self.device_id,
                "platform": "android",
            }
        )
        await service.handle_action(
            {
                "action": "login_direct",
                "email": "student@school.edu",
                "password": "abcd1234",
                "device_id": self.device_id,
                "platform": "android",
            }
        )
        await asyncio.sleep(0.05)
        self.assertEqual(
            fake_release.calls,
            [
                ("student@school.edu", "student", "android"),
                ("student@school.edu", "student", "android"),
            ],
        )

    async def test_forgot_otp_send_failure_returns_email_failure_by_default(self):
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "nosend@example.com",
                "password": "abcd1234",
                "name": "No Send",
                "username": "nosend",
                "device_id": self.device_id,
            }
        )

        with patch.object(
            LocalAuthService,
            "_send_otp_email",
            return_value=(False, "smtp not configured"),
        ):
            req = await self.service.handle_action(
                {
                    "action": "request_forgot_otp",
                    "email": "nosend@example.com",
                    "device_id": self.device_id,
                }
            )
        self.assertEqual(req.get("status"), "EMAIL_SEND_FAILED")
        self.assertFalse(str(req.get("otp", "")).strip())
        self.assertNotIn("nosend@example.com", self.service._otps)

    async def test_upsert_user_updates_password(self):
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "sync@example.com",
                "password": "oldpass",
                "name": "Sync User",
                "username": "sync_user",
                "device_id": self.device_id,
            }
        )

        upsert = await self.service.handle_action(
            {
                "action": "upsert_user",
                "email": "sync@example.com",
                "password": "newpass",
                "name": "Sync User",
                "username": "sync_user",
                "device_id": self.device_id,
            }
        )
        self.assertEqual(upsert.get("status"), "SUCCESS")

        login = await self.service.handle_action(
            {
                "action": "login_direct",
                "email": "sync@example.com",
                "password": "newpass",
            }
        )
        self.assertEqual(login.get("status"), "SUCCESS")

    async def test_compat_otp_send_failure_returns_email_failure_by_default(self):
        with patch.object(
            LocalAuthService,
            "_send_otp_email",
            return_value=(False, "smtp not configured"),
        ):
            req = await self.service.handle_action(
                {
                    "action": "request_login_otp",
                    "email": "compat@example.com",
                }
            )
        self.assertEqual(req.get("status"), "EMAIL_SEND_FAILED")
        self.assertFalse(str(req.get("otp", "")).strip())
        self.assertNotIn("compat@example.com", self.service._otps)

    async def test_forgot_otp_cooldown(self):
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "cooldown@example.com",
                "password": "abcd1234",
                "name": "Cooldown User",
                "username": "cooldown",
                "device_id": self.device_id,
            }
        )
        with patch.object(
            LocalAuthService,
            "_send_otp_email",
            return_value=(True, "OTP sent"),
        ):
            first = await self.service.handle_action(
                {
                    "action": "request_forgot_otp",
                    "email": "cooldown@example.com",
                    "device_id": self.device_id,
                }
            )
            second = await self.service.handle_action(
                {
                    "action": "request_forgot_otp",
                    "email": "cooldown@example.com",
                    "device_id": self.device_id,
                }
            )
        self.assertEqual(first.get("status"), "OTP_SENT")
        self.assertEqual(second.get("status"), "OTP_COOLDOWN")

    async def test_forgot_otp_rejects_when_email_disabled_and_local_fallback_disallowed(self):
        os.environ["OTP_EMAIL_ENABLED"] = "false"
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "localotp@example.com",
                "password": "abcd1234",
                "name": "Local OTP",
                "username": "localotp",
                "device_id": self.device_id,
            }
        )
        req = await self.service.handle_action(
            {
                "action": "request_forgot_otp",
                "email": "localotp@example.com",
                "device_id": self.device_id,
            }
        )
        self.assertEqual(req.get("status"), "EMAIL_BACKEND_DISABLED")
        self.assertFalse(str(req.get("otp", "")).strip())

    async def test_forgot_otp_can_use_opt_in_local_fallback(self):
        os.environ["OTP_ALLOW_LOCAL_FALLBACK"] = "true"
        os.environ["OTP_EMAIL_ENABLED"] = "false"
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "localotp@example.com",
                "password": "abcd1234",
                "name": "Local OTP",
                "username": "localotp",
                "device_id": self.device_id,
            }
        )
        req = await self.service.handle_action(
            {
                "action": "request_forgot_otp",
                "email": "localotp@example.com",
                "device_id": self.device_id,
            }
        )
        self.assertEqual(req.get("status"), "OTP_SENT")
        self.assertEqual(req.get("delivery"), "local")
        self.assertTrue(str(req.get("otp", "")).strip())

    async def test_forgot_otp_rejects_untrusted_device(self):
        os.environ["OTP_REQUIRE_TRUSTED_DEVICE_FOR_RESET"] = "true"
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "trusted@example.com",
                "password": "abcd1234",
                "name": "Trusted User",
                "username": "trusted",
                "device_id": self.device_id,
            }
        )

        with patch.object(
            LocalAuthService,
            "_send_otp_email",
            return_value=(True, "OTP sent"),
        ):
            bad = await self.service.handle_action(
                {
                    "action": "request_forgot_otp",
                    "email": "trusted@example.com",
                    "device_id": "dev_other",
                }
            )
        self.assertEqual(bad.get("status"), "DEVICE_MISMATCH")

    async def test_forgot_otp_allows_new_device_when_email_reset_gate_disabled(self):
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "openreset@example.com",
                "password": "abcd1234",
                "name": "Open Reset",
                "username": "openreset",
                "device_id": self.device_id,
            }
        )

        with patch.object(
            LocalAuthService,
            "_send_otp_email",
            return_value=(True, "OTP sent"),
        ):
            req = await self.service.handle_action(
                {
                    "action": "request_forgot_otp",
                    "email": "openreset@example.com",
                    "device_id": "dev_new_phone",
                }
            )
        self.assertEqual(req.get("status"), "OTP_SENT")

    async def test_forgot_otp_accepts_plus_alias_email_addresses(self):
        await self.service.handle_action(
            {
                "action": "register_direct",
                "email": "atlas+teacher@example.com",
                "password": "abcd1234",
                "name": "Alias Reset",
                "username": "aliasreset",
                "device_id": self.device_id,
            }
        )

        with patch.object(
            LocalAuthService,
            "_send_otp_email",
            return_value=(True, "OTP sent"),
        ):
            req = await self.service.handle_action(
                {
                    "action": "request_forgot_otp",
                    "email": "atlas+teacher@example.com",
                    "device_id": self.device_id,
                }
            )
        self.assertEqual(req.get("status"), "OTP_SENT")


if __name__ == "__main__":
    unittest.main()
