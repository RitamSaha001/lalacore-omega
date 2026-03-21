import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.auth.local_auth_service import LocalAuthService


class LocalAuthServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._prev_otp_email_enabled = os.environ.get("OTP_EMAIL_ENABLED")
        os.environ["OTP_EMAIL_ENABLED"] = "true"
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

    async def test_unknown_action(self):
        response = await self.service.handle_action({"action": "not_real"})
        self.assertEqual(response.get("status"), "UNKNOWN_ACTION")

    async def test_forgot_otp_send_failure_falls_back_to_local_otp(self):
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
        self.assertEqual(req.get("status"), "OTP_SENT")
        self.assertEqual(req.get("delivery"), "local")
        self.assertTrue(str(req.get("otp", "")).strip())
        self.assertIn("nosend@example.com", self.service._otps)

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

    async def test_compat_otp_send_failure_falls_back_to_local_otp(self):
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
        self.assertEqual(req.get("status"), "OTP_SENT")
        self.assertEqual(req.get("delivery"), "local")
        self.assertTrue(str(req.get("otp", "")).strip())
        self.assertIn("compat@example.com", self.service._otps)

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

    async def test_forgot_otp_local_delivery_when_email_disabled(self):
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


if __name__ == "__main__":
    unittest.main()
