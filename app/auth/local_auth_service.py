from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import re
import smtplib
import ssl
import time
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from app.storage.sqlite_json_store import SQLiteJsonBlobStore


class LocalAuthService:
    """SQLite-backed auth + OTP service with JSON-file migration support."""

    def __init__(
        self,
        users_file: str | Path | None = None,
        otp_file: str | Path | None = None,
        storage_db_file: str | Path | None = None,
    ) -> None:
        root = Path(__file__).resolve().parents[2]
        auth_dir = root / "data" / "auth"
        auth_dir.mkdir(parents=True, exist_ok=True)
        self._users_file = Path(users_file) if users_file else auth_dir / "users.json"
        self._otp_file = Path(otp_file) if otp_file else auth_dir / "otp.json"
        default_storage_root = (
            self._users_file.parent
            if users_file is not None or otp_file is not None
            else auth_dir
        )
        self._storage = SQLiteJsonBlobStore(
            Path(storage_db_file)
            if storage_db_file
            else default_storage_root / "auth_store.sqlite3"
        )
        self._storage_keys = {
            self._users_file.resolve(): "auth_users",
            self._otp_file.resolve(): "auth_otps",
        }
        self._lock = asyncio.Lock()
        self._users: dict[str, dict[str, Any]] = {}
        self._otps: dict[str, dict[str, Any]] = {}
        self._loaded = False

    async def handle_action(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_loaded()
        action = self._str(payload.get("action")).lower()

        if action in {"login_direct", "login"}:
            return await self._login(payload)

        if action in {"register_direct", "register"}:
            return await self._register(payload)

        if action in {"upsert_user", "upsert_user_direct", "sync_user_direct"}:
            next_payload = dict(payload)
            next_payload["force_update"] = True
            return await self._register(next_payload)

        if action in {
            "request_forgot_otp",
            "forgot_password_request",
            "request_email_otp",
        }:
            return await self._request_forgot_otp(payload)

        if action in {"verify_forgot_otp"}:
            return await self._verify_forgot_otp(payload, update_password=False)

        if action in {
            "forgot_password_reset",
            "reset_password_with_otp",
            "reset_password",
        }:
            return await self._verify_forgot_otp(payload, update_password=True)

        # Optional compatibility shims for login/register OTP calls.
        if action in {"request_login_otp", "request_register_otp"}:
            return await self._request_compat_otp(payload)

        if action in {"verify_login_otp", "verify_register_otp", "verify_email_otp"}:
            return await self._verify_compat_otp(payload)

        return {
            "ok": False,
            "status": "UNKNOWN_ACTION",
            "message": f"Unknown Action: {action}",
        }

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            self._users = self._read_json_file(self._users_file)
            self._otps = self._read_json_file(self._otp_file)
            self._loaded = True

    def _read_json_file(self, path: Path) -> dict[str, dict[str, Any]]:
        storage_key = self._storage_keys.get(path.resolve())
        if storage_key:
            cached = self._storage.read_json(storage_key)
            normalized = self._normalize_json_map(cached)
            if cached is not None:
                return normalized
        try:
            if not path.exists():
                return {}
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                return {}
            decoded = json.loads(text)
            out = self._normalize_json_map(decoded)
            if storage_key and out:
                self._storage.write_json(storage_key, out)
            return out
        except Exception:
            return {}

    def _write_json_file(self, path: Path, data: dict[str, dict[str, Any]]) -> None:
        storage_key = self._storage_keys.get(path.resolve())
        if storage_key:
            self._storage.write_json(storage_key, data)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

    def _normalize_json_map(self, decoded: Any) -> dict[str, dict[str, Any]]:
        if not isinstance(decoded, dict):
            return {}
        out: dict[str, dict[str, Any]] = {}
        for k, v in decoded.items():
            if isinstance(v, dict):
                out[str(k).strip().lower()] = dict(v)
        return out

    def _str(self, value: Any) -> str:
        return str(value or "").strip()

    def _email_key(self, raw: Any) -> str:
        return self._str(raw).lower()

    def _bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        text = self._str(value).lower()
        return text in {"1", "true", "yes", "on"}

    def _env_flag(self, name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def _is_valid_email(self, email: str) -> bool:
        return bool(
            re.match(
                r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$",
                email,
            )
        )

    def _hash_password(self, email: str, password: str) -> str:
        salted = f"{email.lower()}::{password}".encode("utf-8")
        return hashlib.sha256(salted).hexdigest()

    def _safe_name(self, email: str, provided: str) -> str:
        if provided.strip():
            return provided.strip()
        return email.split("@")[0]

    def _safe_username(self, email: str, provided: str) -> str:
        if provided.strip():
            return provided.strip()
        return email.split("@")[0]

    def _stable_student_id(self, email: str, username: str) -> str:
        base = re.sub(r"[^A-Z0-9]", "", username.upper())
        if not base:
            base = "STUDENT"
        digest = hashlib.sha1(email.lower().encode("utf-8")).hexdigest()[:5].upper()
        return f"{base[:4]}{digest}"

    def _normalize_device_id(self, raw: Any) -> str:
        value = self._str(raw)
        if not value:
            return ""
        # Keep ID compact and deterministic for storage comparisons.
        return re.sub(r"[^a-zA-Z0-9:_-]", "", value)[:128]

    def _require_trusted_device_for_reset(self) -> bool:
        return self._env_flag("OTP_REQUIRE_TRUSTED_DEVICE_FOR_RESET", False)

    def _require_issuing_device_for_reset(self) -> bool:
        return self._env_flag("OTP_REQUIRE_ISSUING_DEVICE_FOR_RESET", False)

    def _allow_local_otp_fallback(self) -> bool:
        return self._env_flag("OTP_ALLOW_LOCAL_FALLBACK", False)

    def _trusted_devices_from_user(self, user: dict[str, Any]) -> list[str]:
        raw = user.get("trusted_device_ids")
        if isinstance(raw, list):
            out = [self._normalize_device_id(x) for x in raw]
            return [x for x in out if x]
        single = self._normalize_device_id(user.get("trusted_device_id"))
        return [single] if single else []

    def _attach_trusted_device(self, user: dict[str, Any], device_id: str) -> bool:
        normalized = self._normalize_device_id(device_id)
        if not normalized:
            return False
        trusted = self._trusted_devices_from_user(user)
        if normalized in trusted:
            return False
        trusted.append(normalized)
        user["trusted_device_ids"] = trusted[-8:]
        user["trusted_device_id"] = normalized
        return True

    async def _login(self, payload: dict[str, Any]) -> dict[str, Any]:
        email = self._email_key(payload.get("email"))
        password = self._str(payload.get("password"))
        device_id = self._normalize_device_id(payload.get("device_id"))
        if not self._is_valid_email(email):
            return {
                "ok": False,
                "status": "INVALID_EMAIL",
                "message": "Please enter a valid email",
            }
        if len(password) < 4:
            return {
                "ok": False,
                "status": "WEAK_PASSWORD",
                "message": "Password must be at least 4 characters",
            }

        user = self._users.get(email)
        if user is None:
            return {"ok": False, "status": "USER_NOT_FOUND"}

        expected = self._str(user.get("password_hash"))
        current = self._hash_password(email, password)
        if expected != current:
            return {"ok": False, "status": "WRONG_PASSWORD"}

        user_changed = self._attach_trusted_device(user, device_id)
        if user_changed:
            user["updated_at"] = int(time.time() * 1000)
            self._users[email] = user
            async with self._lock:
                self._write_json_file(self._users_file, self._users)

        return {
            "ok": True,
            "status": "SUCCESS",
            "student_id": self._str(user.get("student_id")),
            "name": self._str(user.get("name")),
            "email": email,
            "username": self._str(user.get("username")),
        }

    async def _register(self, payload: dict[str, Any]) -> dict[str, Any]:
        email = self._email_key(payload.get("email"))
        password = self._str(payload.get("password"))
        device_id = self._normalize_device_id(payload.get("device_id"))
        force_update = self._bool(payload.get("force_update"))
        existing = self._users.get(email)

        input_name = self._str(payload.get("name"))
        input_username = self._str(payload.get("username"))
        name = self._safe_name(
            email,
            input_name or self._str((existing or {}).get("name")),
        )
        username = self._safe_username(
            email,
            input_username or self._str((existing or {}).get("username")),
        )

        if not self._is_valid_email(email):
            return {
                "ok": False,
                "status": "INVALID_EMAIL",
                "message": "Please enter a valid email",
            }
        if not existing and len(password) < 4:
            return {
                "ok": False,
                "status": "WEAK_PASSWORD",
                "message": "Password must be at least 4 characters",
            }
        if existing is not None and not force_update:
            return {"ok": False, "status": "USER_EXISTS"}

        student_id = self._str(payload.get("student_id")) or self._str(
            (existing or {}).get("student_id"),
        )
        if not student_id:
            student_id = self._stable_student_id(email, username)
        now_ms = int(time.time() * 1000)

        if existing is None:
            user = {
                "student_id": student_id,
                "name": name,
                "username": username,
                "email": email,
                "created_at": now_ms,
            }
        else:
            user = dict(existing)
            user["student_id"] = student_id
            user["name"] = name
            user["username"] = username
            user["email"] = email
            user["created_at"] = int(user.get("created_at", now_ms))

        if password:
            if len(password) < 4:
                return {
                    "ok": False,
                    "status": "WEAK_PASSWORD",
                    "message": "Password must be at least 4 characters",
                }
            user["password_hash"] = self._hash_password(email, password)

        if not self._str(user.get("password_hash")):
            return {
                "ok": False,
                "status": "WEAK_PASSWORD",
                "message": "Password must be at least 4 characters",
            }

        self._attach_trusted_device(user, device_id)
        user["updated_at"] = now_ms
        self._users[email] = user

        async with self._lock:
            self._write_json_file(self._users_file, self._users)

        return {"ok": True, "status": "SUCCESS", "student_id": student_id, "name": name}

    async def _request_forgot_otp(self, payload: dict[str, Any]) -> dict[str, Any]:
        # request_email_otp is supported only for forgot-password flow here.
        action = self._str(payload.get("action")).lower()
        flow = self._str(payload.get("flow")).lower()
        if action == "request_email_otp" and flow not in {
            "",
            "forgot_password",
            "forgot",
            "reset_password",
        }:
            return {
                "ok": False,
                "status": "UNKNOWN_ACTION",
                "message": "Unknown Action",
            }

        email = self._email_key(payload.get("email"))
        device_id = self._normalize_device_id(payload.get("device_id"))
        if not self._is_valid_email(email):
            return {
                "ok": False,
                "status": "INVALID_EMAIL",
                "message": "Please enter a valid email",
            }
        user = self._users.get(email)
        if user is None:
            return {
                "ok": False,
                "status": "USER_NOT_FOUND",
                "message": "Account not found",
            }

        trusted_devices = self._trusted_devices_from_user(user)
        if self._require_trusted_device_for_reset():
            if not trusted_devices and not device_id:
                return {
                    "ok": False,
                    "status": "DEVICE_REQUIRED",
                    "message": "Reset requires a trusted device on this account",
                }
            if trusted_devices and (not device_id or device_id not in trusted_devices):
                return {
                    "ok": False,
                    "status": "DEVICE_MISMATCH",
                    "message": "Reset is allowed only on a previously used trusted device",
                }

        now = int(time.time())
        ttl = max(120, int(os.getenv("OTP_TTL_SECONDS", "600")))
        cooldown = max(0, int(os.getenv("OTP_RESEND_COOLDOWN_SECONDS", "30")))
        current = self._otps.get(email)
        if (
            current is not None
            and int(current.get("expires_at", 0)) >= now
            and int(current.get("sent_at", 0)) + cooldown > now
        ):
            wait_s = max(1, int(current.get("sent_at", 0)) + cooldown - now)
            return {
                "ok": False,
                "status": "OTP_COOLDOWN",
                "message": f"Please wait {wait_s}s before requesting another OTP",
            }
        otp = f"{random.randint(100000, 999999)}"
        email_enabled = self._bool(os.getenv("OTP_EMAIL_ENABLED", "true"))
        fallback_reason = ""
        if email_enabled:
            sent, send_msg = await asyncio.to_thread(
                self._send_otp_email,
                email=email,
                otp=otp,
                ttl_seconds=ttl,
            )
            if not sent:
                if self._allow_local_otp_fallback():
                    fallback_reason = send_msg
                    email_enabled = False
                    sent = True
                    send_msg = f"OTP generated locally ({send_msg})"
                else:
                    return {
                        "ok": False,
                        "status": "EMAIL_SEND_FAILED",
                        "message": send_msg,
                    }
        else:
            if self._allow_local_otp_fallback():
                sent, send_msg = True, "OTP generated locally"
            else:
                return {
                    "ok": False,
                    "status": "EMAIL_BACKEND_DISABLED",
                    "message": "OTP email delivery is disabled and local fallback is not allowed",
                }
        if not sent:
            return {
                "ok": False,
                "status": "EMAIL_SEND_FAILED",
                "message": send_msg,
            }

        self._otps[email] = {
            "otp": otp,
            "expires_at": now + ttl,
            "sent_at": now,
            "attempts": 0,
            "purpose": "forgot_password",
            "device_id": device_id,
        }
        async with self._lock:
            self._write_json_file(self._otp_file, self._otps)

        return {
            "ok": True,
            "status": "OTP_SENT",
            "message": (
                f"Reset code sent to {email}"
                if email_enabled
                else "Reset code generated for this trusted device"
            ),
            "delivery": "email" if email_enabled else "local",
            **({"otp": otp} if not email_enabled else {}),
            **({"fallback_reason": fallback_reason} if fallback_reason else {}),
        }

    def _send_otp_email(self, email: str, otp: str, ttl_seconds: int) -> tuple[bool, str]:
        sender = self._str(
            os.getenv("OTP_SENDER_EMAIL", "") or os.getenv("FORGOT_OTP_SENDER_EMAIL", ""),
        )
        sender_password = self._str(os.getenv("OTP_SENDER_PASSWORD", "")).replace(" ", "")
        smtp_host = self._str(os.getenv("OTP_SMTP_HOST", "smtp.gmail.com"))
        smtp_port_raw = self._str(os.getenv("OTP_SMTP_PORT", "587")) or "587"
        smtp_security = self._str(os.getenv("OTP_SMTP_SECURITY", "tls")).lower()
        try:
            smtp_port = int(smtp_port_raw)
        except ValueError:
            return False, f"Invalid OTP_SMTP_PORT: {smtp_port_raw}"
        from_name = self._str(os.getenv("OTP_FROM_NAME", "LalaCore"))

        if not sender or not sender_password:
            return (
                False,
                "OTP email backend not configured (missing OTP_SENDER_EMAIL / OTP_SENDER_PASSWORD)",
            )

        msg = EmailMessage()
        msg["Subject"] = "Your LalaCore password reset OTP"
        msg["From"] = f"{from_name} <{sender}>"
        msg["To"] = email
        msg.set_content(
            (
                "Hi,\n\n"
                f"Your LalaCore OTP is: {otp}\n"
                f"This OTP will expire in {ttl_seconds // 60} minutes.\n\n"
                "If you did not request this, you can ignore this email."
            )
        )

        # Use certifi bundle when available to avoid platform CA drift.
        try:
            import certifi  # type: ignore

            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        except Exception:
            ssl_ctx = ssl.create_default_context()
        try:
            if smtp_security in {"ssl", "smtps", "implicit_ssl"}:
                with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=20, context=ssl_ctx) as smtp:
                    smtp.ehlo()
                    smtp.login(sender, sender_password)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as smtp:
                    smtp.ehlo()
                    if smtp_security not in {"none", "plain"}:
                        smtp.starttls(context=ssl_ctx)
                        smtp.ehlo()
                    smtp.login(sender, sender_password)
                    smtp.send_message(msg)
            return True, "OTP sent"
        except Exception as exc:
            return False, f"OTP email send failed: {exc}"

    async def _verify_forgot_otp(
        self,
        payload: dict[str, Any],
        *,
        update_password: bool,
    ) -> dict[str, Any]:
        email = self._email_key(payload.get("email"))
        otp = self._str(payload.get("otp"))
        new_password = self._str(
            payload.get("new_password") or payload.get("password"),
        )
        request_device = self._normalize_device_id(payload.get("device_id"))

        if not self._is_valid_email(email):
            return {
                "ok": False,
                "status": "INVALID_EMAIL",
                "message": "Please enter a valid email",
            }

        current = self._otps.get(email)
        if current is None:
            return {"ok": False, "status": "INVALID_OTP", "message": "OTP not found"}

        user = self._users.get(email)
        if user is None:
            return {"ok": False, "status": "USER_NOT_FOUND", "message": "Account not found"}

        trusted_devices = self._trusted_devices_from_user(user)
        otp_device = self._normalize_device_id(current.get("device_id"))
        if (
            self._require_issuing_device_for_reset()
            and otp_device
            and request_device
            and otp_device != request_device
        ):
            return {
                "ok": False,
                "status": "DEVICE_MISMATCH",
                "message": "Reset code was issued to a different trusted device",
            }
        if (
            self._require_trusted_device_for_reset()
            and trusted_devices
            and (not request_device or request_device not in trusted_devices)
        ):
            return {
                "ok": False,
                "status": "DEVICE_MISMATCH",
                "message": "Reset is allowed only on a previously used trusted device",
            }

        now = int(time.time())
        if int(current.get("expires_at", 0)) < now:
            self._otps.pop(email, None)
            async with self._lock:
                self._write_json_file(self._otp_file, self._otps)
            return {"ok": False, "status": "OTP_EXPIRED", "message": "OTP expired"}

        if self._str(current.get("otp")) != otp:
            current["attempts"] = int(current.get("attempts", 0)) + 1
            if int(current.get("attempts", 0)) >= 5:
                self._otps.pop(email, None)
            async with self._lock:
                self._write_json_file(self._otp_file, self._otps)
            return {"ok": False, "status": "INVALID_OTP", "message": "Invalid OTP"}

        if not update_password:
            return {"ok": True, "status": "VERIFIED"}

        if len(new_password) < 4:
            return {
                "ok": False,
                "status": "WEAK_PASSWORD",
                "message": "Password must be at least 4 characters",
            }
        now_ms = int(time.time() * 1000)

        user["password_hash"] = self._hash_password(email, new_password)
        self._attach_trusted_device(user, request_device)
        user["updated_at"] = now_ms
        self._users[email] = user
        self._otps.pop(email, None)

        async with self._lock:
            self._write_json_file(self._users_file, self._users)
            self._write_json_file(self._otp_file, self._otps)

        return {"ok": True, "status": "SUCCESS", "message": "Password reset successful"}

    async def _request_compat_otp(self, payload: dict[str, Any]) -> dict[str, Any]:
        email = self._email_key(payload.get("email"))
        if not self._is_valid_email(email):
            return {"ok": False, "status": "INVALID_EMAIL"}
        otp = f"{random.randint(100000, 999999)}"
        ttl = max(120, int(os.getenv("OTP_TTL_SECONDS", "600")))
        email_enabled = self._bool(os.getenv("OTP_EMAIL_ENABLED", "true"))
        fallback_reason = ""
        if email_enabled:
            sent, send_msg = await asyncio.to_thread(
                self._send_otp_email,
                email=email,
                otp=otp,
                ttl_seconds=ttl,
            )
            if not sent:
                if self._allow_local_otp_fallback():
                    fallback_reason = send_msg
                    email_enabled = False
                    sent = True
                    send_msg = f"OTP generated locally ({send_msg})"
                else:
                    return {"ok": False, "status": "EMAIL_SEND_FAILED", "message": send_msg}
        else:
            if self._allow_local_otp_fallback():
                sent, send_msg = True, "OTP generated locally"
            else:
                return {
                    "ok": False,
                    "status": "EMAIL_BACKEND_DISABLED",
                    "message": "OTP email delivery is disabled and local fallback is not allowed",
                }
        if not sent:
            return {"ok": False, "status": "EMAIL_SEND_FAILED", "message": send_msg}
        now = int(time.time())
        self._otps[email] = {
            "otp": otp,
            "expires_at": now + ttl,
            "sent_at": now,
            "attempts": 0,
            "purpose": "compat_otp",
        }
        async with self._lock:
            self._write_json_file(self._otp_file, self._otps)
        return {
            "ok": True,
            "status": "OTP_SENT",
            "delivery": "email" if email_enabled else "local",
            **({"otp": otp} if not email_enabled else {}),
            **({"fallback_reason": fallback_reason} if fallback_reason else {}),
        }

    async def _verify_compat_otp(self, payload: dict[str, Any]) -> dict[str, Any]:
        email = self._email_key(payload.get("email"))
        otp = self._str(payload.get("otp"))
        current = self._otps.get(email)
        if current is None:
            return {"ok": False, "status": "INVALID_OTP"}
        if int(current.get("expires_at", 0)) < int(time.time()):
            self._otps.pop(email, None)
            async with self._lock:
                self._write_json_file(self._otp_file, self._otps)
            return {"ok": False, "status": "OTP_EXPIRED"}
        if self._str(current.get("otp")) != otp:
            current["attempts"] = int(current.get("attempts", 0)) + 1
            if int(current.get("attempts", 0)) >= 5:
                self._otps.pop(email, None)
            async with self._lock:
                self._write_json_file(self._otp_file, self._otps)
            return {"ok": False, "status": "INVALID_OTP"}
        self._otps.pop(email, None)
        async with self._lock:
            self._write_json_file(self._otp_file, self._otps)
        return {"ok": True, "status": "VERIFIED"}
