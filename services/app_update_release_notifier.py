from __future__ import annotations

import asyncio
import contextlib
import csv
import io
import json
import os
import ssl
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

from app.storage.sqlite_json_store import SQLiteJsonBlobStore
from core.automation.state_manager import AutomationStateManager
from services.atlas_incident_email_service import AtlasIncidentEmailService


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AppUpdateReleaseNotifierService:
    """Polls the published app-update sheet and emails on newly seen releases."""

    CHECKPOINT_SCOPE = "app_update_release_notifier"
    DEFAULT_SHEET_URL = (
        "https://docs.google.com/spreadsheets/d/"
        "1Il-ojLV1TecCPG43_a_ookL-Hb6EA46zS--a2xjVhng/"
        "export?format=csv&gid=1537205702"
    )

    def __init__(
        self,
        *,
        state: AutomationStateManager | None = None,
        email_service: AtlasIncidentEmailService | None = None,
        fetcher: Callable[[str], str] | None = None,
        sheet_url: str | None = None,
        release_recipient_provider: Callable[[list[dict[str, Any]], str | None], list[str]] | None = None,
        auth_users_file: str | Path | None = None,
        auth_storage_db_file: str | Path | None = None,
    ) -> None:
        root = Path(__file__).resolve().parents[1]
        auth_dir = root / "data" / "auth"
        self._state = state or AutomationStateManager()
        self._email = email_service or AtlasIncidentEmailService()
        self._fetcher = fetcher or self._fetch_sheet_csv
        self._sheet_url_override = (sheet_url or "").strip()
        self._release_recipient_provider = (
            release_recipient_provider or self._default_release_recipient_provider
        )
        self._auth_users_file = (
            Path(auth_users_file) if auth_users_file else auth_dir / "users.json"
        )
        self._auth_storage = SQLiteJsonBlobStore(
            Path(auth_storage_db_file)
            if auth_storage_db_file
            else auth_dir / "auth_store.sqlite3"
        )
        self._lock = asyncio.Lock()

    def enabled(self) -> bool:
        raw = os.getenv("APP_UPDATE_CONFIRMATION_ENABLED")
        if raw is None:
            return True
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def sheet_url(self) -> str:
        return (
            self._sheet_url_override
            or os.getenv("APP_UPDATE_SHEET_URL", "").strip()
            or self.DEFAULT_SHEET_URL
        )

    def status_snapshot(self) -> dict[str, Any]:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        seen_release_keys = checkpoint.get("seen_release_keys")
        if not isinstance(seen_release_keys, list):
            seen_release_keys = []
        return {
            "enabled": self.enabled(),
            "sheet_url": self.sheet_url(),
            "running": bool(checkpoint.get("running")),
            "last_checked_ts": checkpoint.get("last_checked_ts"),
            "last_status": str(checkpoint.get("last_status") or ""),
            "last_trigger": str(checkpoint.get("last_trigger") or ""),
            "last_error": str(checkpoint.get("last_error") or ""),
            "last_mail_sent": bool(checkpoint.get("last_mail_sent")),
            "last_mail_message": str(checkpoint.get("last_mail_message") or ""),
            "last_mail_sent_ts": checkpoint.get("last_mail_sent_ts"),
            "last_release_count": int(checkpoint.get("last_release_count") or 0),
            "last_new_release_count": int(checkpoint.get("last_new_release_count") or 0),
            "seen_release_count": len(seen_release_keys),
            "last_detected_release_keys": [
                str(item).strip()
                for item in list(checkpoint.get("last_detected_release_keys") or [])[:20]
                if str(item).strip()
            ],
        }

    async def poll_for_new_releases(
        self,
        *,
        trigger: str = "scheduled",
        recipient_email: str | None = None,
        force_resend: bool = False,
    ) -> dict[str, Any]:
        async with self._lock:
            checked_at = _utc_now().isoformat()
            if not self.enabled():
                self._state.checkpoint(
                    self.CHECKPOINT_SCOPE,
                    running=False,
                    last_checked_ts=checked_at,
                    last_status="disabled",
                    last_trigger=trigger,
                )
                return {
                    "ok": True,
                    "status": "DISABLED",
                    "checked_at": checked_at,
                    "sheet_url": self.sheet_url(),
                }

            sheet_url = self.sheet_url()
            if not sheet_url:
                self._state.checkpoint(
                    self.CHECKPOINT_SCOPE,
                    running=False,
                    last_checked_ts=checked_at,
                    last_status="misconfigured",
                    last_trigger=trigger,
                    last_error="APP_UPDATE_SHEET_URL missing",
                )
                return {
                    "ok": False,
                    "status": "MISCONFIGURED",
                    "checked_at": checked_at,
                    "message": "APP_UPDATE_SHEET_URL is not configured",
                }

            self._state.checkpoint(
                self.CHECKPOINT_SCOPE,
                running=True,
                last_trigger=trigger,
                last_checked_ts=checked_at,
                last_status="checking",
                last_error="",
            )

            try:
                csv_text = await asyncio.to_thread(self._fetcher, sheet_url)
                releases = self._parse_release_rows(csv_text)
                seen_release_keys = self._seen_release_keys()
                if force_resend:
                    new_releases = list(releases)
                else:
                    new_releases = [
                        release
                        for release in releases
                        if release["release_key"] not in seen_release_keys
                    ]
                if not new_releases:
                    self._state.checkpoint(
                        self.CHECKPOINT_SCOPE,
                        running=False,
                        last_status="no_new_release",
                        last_error="",
                        last_release_count=len(releases),
                        last_new_release_count=0,
                        last_mail_sent=False,
                        last_mail_message="No new release rows detected",
                        last_detected_release_keys=[r["release_key"] for r in releases[:20]],
                    )
                    return {
                        "ok": True,
                        "status": "NO_NEW_RELEASE",
                        "checked_at": checked_at,
                        "sheet_url": sheet_url,
                        "release_count": len(releases),
                        "new_release_count": 0,
                    }

                announcement_recipients = self._release_recipient_provider(
                    new_releases,
                    recipient_email,
                )
                announcement_result = await asyncio.to_thread(
                    self._email.send_release_announcement,
                    releases=new_releases,
                    sheet_url=sheet_url,
                    recipients=announcement_recipients,
                    trigger=trigger,
                    checked_at=checked_at,
                )
                sent_recipients = [
                    str(item).strip().lower()
                    for item in list(announcement_result.get("sent_recipients") or [])
                    if str(item).strip()
                ]
                if sent_recipients:
                    self._mark_release_sent_for_recipients(
                        releases=new_releases,
                        recipients=sent_recipients,
                    )
                no_deliverable_recipients = bool(
                    announcement_result.get("no_deliverable_recipients")
                )
                if no_deliverable_recipients:
                    confirmation_result = {
                        "ok": True,
                        "sent": False,
                        "message": "Skipped support confirmation because no deliverable recipients were available",
                    }
                else:
                    confirmation_result = await asyncio.to_thread(
                        self._email.send_release_confirmation,
                        releases=new_releases,
                        sheet_url=sheet_url,
                        recipient=recipient_email,
                        trigger=trigger,
                        checked_at=checked_at,
                    )
                mail_message = self._combined_mail_message(
                    announcement_result=announcement_result,
                    confirmation_result=confirmation_result,
                )
                if bool(announcement_result.get("ok")):
                    updated_seen = self._merged_seen_keys(
                        seen_release_keys,
                        [release["release_key"] for release in new_releases],
                    )
                    self._state.checkpoint(
                        self.CHECKPOINT_SCOPE,
                        running=False,
                        last_status=(
                            "no_deliverable_recipients"
                            if no_deliverable_recipients
                            else "mail_sent"
                        ),
                        last_error="",
                        last_mail_sent=not no_deliverable_recipients,
                        last_mail_message=mail_message,
                        **(
                            {"last_mail_sent_ts": checked_at}
                            if not no_deliverable_recipients
                            else {}
                        ),
                        last_release_count=len(releases),
                        last_new_release_count=len(new_releases),
                        last_detected_release_keys=[r["release_key"] for r in releases[:20]],
                        seen_release_keys=updated_seen,
                    )
                else:
                    self._state.checkpoint(
                        self.CHECKPOINT_SCOPE,
                        running=False,
                        last_status="mail_failed",
                        last_error=str(announcement_result.get("message") or "mail_failed"),
                        last_mail_sent=False,
                        last_mail_message=mail_message,
                        last_release_count=len(releases),
                        last_new_release_count=len(new_releases),
                        last_detected_release_keys=[r["release_key"] for r in releases[:20]],
                    )
                return {
                    "ok": bool(announcement_result.get("ok")),
                    "status": (
                        "NO_DELIVERABLE_RECIPIENTS"
                        if bool(announcement_result.get("ok")) and no_deliverable_recipients
                        else "SUCCESS"
                        if bool(announcement_result.get("ok"))
                        else "MAIL_FAILED"
                    ),
                    "checked_at": checked_at,
                    "sheet_url": sheet_url,
                    "release_count": len(releases),
                    "new_release_count": len(new_releases),
                    "new_releases": new_releases,
                    "announcement_recipients": announcement_recipients,
                    "mail": dict(announcement_result),
                    "confirmation": dict(confirmation_result),
                }
            except Exception as exc:
                self._state.checkpoint(
                    self.CHECKPOINT_SCOPE,
                    running=False,
                    last_status="failed",
                    last_error=str(exc)[:500],
                    last_mail_sent=False,
                    last_mail_message="",
                )
                return {
                    "ok": False,
                    "status": "FAILED",
                    "checked_at": checked_at,
                    "sheet_url": sheet_url,
                    "message": str(exc),
                }

    def notify_pending_releases_for_email(
        self,
        email: str,
        *,
        role: str | None = None,
        trigger: str = "auth_login",
    ) -> dict[str, Any]:
        normalized_email = str(email or "").strip().lower()
        if not self.enabled():
            return {
                "ok": True,
                "status": "DISABLED",
                "sent_count": 0,
                "releases": [],
            }
        if not self._looks_like_deliverable_email(normalized_email):
            return {
                "ok": False,
                "status": "INVALID_EMAIL",
                "sent_count": 0,
                "releases": [],
                "message": "No deliverable user email was available",
            }
        sheet_url = self.sheet_url()
        if not sheet_url:
            return {
                "ok": False,
                "status": "MISCONFIGURED",
                "sent_count": 0,
                "releases": [],
                "message": "APP_UPDATE_SHEET_URL is not configured",
            }
        try:
            csv_text = self._fetcher(sheet_url)
            releases = self._parse_release_rows(csv_text)
            relevant_releases = self._latest_releases_for_role(
                releases,
                role=role,
            )
            pending_releases = [
                release
                for release in relevant_releases
                if not self._release_already_sent_to_recipient(
                    normalized_email,
                    str(release.get("release_key") or ""),
                )
            ]
            if not pending_releases:
                return {
                    "ok": True,
                    "status": "NO_PENDING_RELEASES",
                    "sent_count": 0,
                    "releases": [],
                }
            checked_at = _utc_now().isoformat()
            announcement_result = self._email.send_release_announcement(
                releases=pending_releases,
                sheet_url=sheet_url,
                recipients=[normalized_email],
                trigger=trigger,
                checked_at=checked_at,
            )
            sent_recipients = [
                str(item).strip().lower()
                for item in list(announcement_result.get("sent_recipients") or [])
                if str(item).strip()
            ]
            if sent_recipients:
                self._mark_release_sent_for_recipients(
                    releases=pending_releases,
                    recipients=sent_recipients,
                )
            return {
                "ok": bool(announcement_result.get("ok")),
                "status": (
                    "SUCCESS"
                    if bool(announcement_result.get("ok"))
                    else "MAIL_FAILED"
                ),
                "sent_count": int(announcement_result.get("sent_count") or 0),
                "releases": pending_releases,
                "mail": dict(announcement_result),
            }
        except Exception as exc:
            return {
                "ok": False,
                "status": "FAILED",
                "sent_count": 0,
                "releases": [],
                "message": str(exc),
            }

    def _seen_release_keys(self) -> list[str]:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        raw = checkpoint.get("seen_release_keys")
        if not isinstance(raw, list):
            return []
        return [
            str(item).strip()
            for item in raw
            if str(item).strip()
        ]

    def _merged_seen_keys(
        self,
        seen_release_keys: list[str],
        new_release_keys: list[str],
    ) -> list[str]:
        merged: list[str] = []
        for item in seen_release_keys + new_release_keys:
            key = str(item).strip()
            if key and key not in merged:
                merged.append(key)
        if len(merged) > 500:
            merged = merged[-500:]
        return merged

    def _release_recipient_sent_map(self) -> dict[str, dict[str, str]]:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        raw = checkpoint.get("release_recipient_sent_map")
        if not isinstance(raw, dict):
            return {}
        cleaned: dict[str, dict[str, str]] = {}
        for raw_release_key, raw_recipient_map in raw.items():
            release_key = str(raw_release_key or "").strip()
            if not release_key or not isinstance(raw_recipient_map, dict):
                continue
            recipient_map: dict[str, str] = {}
            for raw_email, raw_ts in raw_recipient_map.items():
                email = str(raw_email or "").strip().lower()
                ts = str(raw_ts or "").strip()
                if email and ts:
                    recipient_map[email] = ts
            if recipient_map:
                cleaned[release_key] = recipient_map
        return cleaned

    def _release_already_sent_to_recipient(self, email: str, release_key: str) -> bool:
        recipient_map = self._release_recipient_sent_map()
        normalized_email = str(email or "").strip().lower()
        normalized_release_key = str(release_key or "").strip()
        if not normalized_email or not normalized_release_key:
            return False
        return bool(
            str(
                (recipient_map.get(normalized_release_key) or {}).get(
                    normalized_email
                )
                or ""
            ).strip()
        )

    def _mark_release_sent_for_recipients(
        self,
        *,
        releases: list[dict[str, Any]],
        recipients: list[str],
    ) -> None:
        recipient_map = self._release_recipient_sent_map()
        sent_at = _utc_now().isoformat()
        normalized_recipients = [
            str(item).strip().lower()
            for item in recipients
            if self._looks_like_deliverable_email(str(item).strip().lower())
        ]
        for release in releases:
            if not isinstance(release, dict):
                continue
            release_key = str(release.get("release_key") or "").strip()
            if not release_key:
                continue
            current = dict(recipient_map.get(release_key) or {})
            for email in normalized_recipients:
                current[email] = sent_at
            if len(current) > 2000:
                current = dict(list(current.items())[-2000:])
            recipient_map[release_key] = current
        if len(recipient_map) > 250:
            recipient_map = dict(list(recipient_map.items())[-250:])
        self._state.checkpoint(
            self.CHECKPOINT_SCOPE,
            release_recipient_sent_map=recipient_map,
        )

    def _fetch_sheet_csv(self, url: str) -> str:
        request = Request(
            url,
            headers={
                "User-Agent": "AtlasReleaseNotifier/1.0",
                "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.1",
            },
        )
        with urlopen(request, timeout=12, context=self._ssl_context()) as response:  # noqa: S310
            payload = response.read()
        return payload.decode("utf-8-sig", errors="replace")

    def _ssl_context(self) -> ssl.SSLContext:
        try:
            import certifi  # type: ignore

            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            return ssl.create_default_context()

    def _parse_release_rows(self, csv_text: str) -> list[dict[str, Any]]:
        if not csv_text.strip():
            return []
        reader = csv.DictReader(io.StringIO(csv_text))
        releases: list[dict[str, Any]] = []
        for row in reader:
            if not isinstance(row, dict):
                continue
            normalized = {str(key or "").strip().lower(): str(value or "").strip() for key, value in row.items()}
            if not self._row_enabled(normalized):
                continue
            version = self._first_value(normalized, "version", "latest_version")
            build_number = self._first_value(normalized, "build_number", "latest_build", "build")
            android_url = self._first_value(
                normalized,
                "android_url",
                "android_download_url",
                "android_apk_url",
                "apk_url",
                "apk_link",
            )
            ios_url = self._first_value(
                normalized,
                "ios_url",
                "ios_download_url",
                "ios_ipa_url",
                "ipa_url",
                "app_store_url",
                "testflight_url",
            )
            download_url = self._first_value(
                normalized,
                "download_url",
                "download_link",
                "url",
            )
            if not version and not build_number and not android_url and not ios_url and not download_url:
                continue
            app_id = self._first_value(normalized, "app_id") or "lalacore_rebuild"
            channel = self._first_value(normalized, "channel") or "stable"
            audience = self._first_value(normalized, "audience", "role") or "all"
            platform = self._first_value(normalized, "platform") or "android"
            release = {
                "app_id": app_id,
                "channel": channel,
                "audience": audience,
                "platform": platform,
                "version": version,
                "build_number": build_number,
                "android_url": android_url,
                "ios_url": ios_url,
                "download_url": download_url,
                "force": self._bool_value(
                    self._first_value(
                        normalized,
                        "force",
                        "required",
                        "mandatory_update",
                    )
                ),
                "message": self._first_value(normalized, "message"),
                "release_notes": self._first_value(normalized, "release_notes"),
                "min_supported_version": self._first_value(normalized, "min_supported_version"),
                "min_supported_build": self._first_value(normalized, "min_supported_build"),
            }
            release["release_key"] = "|".join(
                [
                    str(release["app_id"]).strip().lower(),
                    str(release["channel"]).strip().lower(),
                    str(release["audience"]).strip().lower(),
                    str(release["platform"]).strip().lower(),
                    str(release["version"]).strip(),
                    str(release["build_number"]).strip(),
                ]
            )
            releases.append(release)
        return releases

    def _latest_releases_for_role(
        self,
        releases: list[dict[str, Any]],
        *,
        role: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_role = str(role or "").strip().lower()
        latest: dict[str, dict[str, Any]] = {}
        for release in releases:
            if not isinstance(release, dict):
                continue
            audience = str(release.get("audience") or "").strip().lower() or "all"
            if audience != "all" and normalized_role and audience != normalized_role:
                continue
            if audience != "all" and not normalized_role:
                continue
            group_key = "|".join(
                [
                    str(release.get("app_id") or "").strip().lower(),
                    str(release.get("channel") or "").strip().lower(),
                    str(release.get("platform") or "").strip().lower(),
                    audience,
                ]
            )
            latest[group_key] = dict(release)
        return list(latest.values())

    def _row_enabled(self, row: dict[str, str]) -> bool:
        enabled_raw = self._first_value(row, "enabled")
        if not enabled_raw:
            return True
        return self._bool_value(enabled_raw)

    def _first_value(self, row: dict[str, str], *names: str) -> str:
        for name in names:
            value = str(row.get(name, "") or "").strip()
            if value:
                return value
        return ""

    def _bool_value(self, value: str) -> bool:
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    def _combined_mail_message(
        self,
        *,
        announcement_result: dict[str, Any],
        confirmation_result: dict[str, Any],
    ) -> str:
        announcement_message = str(announcement_result.get("message") or "").strip()
        confirmation_message = str(confirmation_result.get("message") or "").strip()
        sent_count = int(announcement_result.get("sent_count") or 0)
        recipient_count = len(list(announcement_result.get("recipients") or []))
        summary = f"release announcement {sent_count}/{recipient_count} recipients"
        if announcement_message:
            summary = f"{summary}; {announcement_message}"
        if confirmation_message:
            summary = f"{summary}; support confirmation: {confirmation_message}"
        return summary

    def _default_release_recipient_provider(
        self,
        releases: list[dict[str, Any]],
        recipient_email: str | None,
    ) -> list[str]:
        override = self._recipient_list(recipient_email)
        if override:
            return override
        target_audiences = {
            str(item.get("audience") or "").strip().lower() or "all"
            for item in releases
            if isinstance(item, dict)
        }
        if not target_audiences:
            target_audiences = {"all"}
        recipients: list[str] = []
        seen: set[str] = set()
        for user in self._iter_auth_users():
            email = str(user.get("email") or "").strip().lower()
            if not self._looks_like_deliverable_email(email):
                continue
            role = str(user.get("role") or "").strip().lower()
            if (
                "all" not in target_audiences
                and role
                and role not in target_audiences
            ):
                continue
            if email in seen:
                continue
            seen.add(email)
            recipients.append(email)
        return recipients

    def _iter_auth_users(self) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for row in self._auth_users_from_json_file() + self._auth_users_from_sqlite_store():
            email = str(row.get("email") or "").strip().lower()
            if not email:
                continue
            current = merged.get(email, {})
            next_row = dict(current)
            next_row.update(dict(row))
            next_row["email"] = email
            merged[email] = next_row
        return list(merged.values())

    def _auth_users_from_json_file(self) -> list[dict[str, Any]]:
        try:
            if not self._auth_users_file.exists():
                return []
            text = self._auth_users_file.read_text(encoding="utf-8").strip()
        except Exception:
            return []
        try:
            parsed = json.loads(text) if text else {}
            if not isinstance(parsed, dict):
                return []
            return [dict(value) for value in parsed.values() if isinstance(value, dict)]
        except Exception:
            return []

    def _auth_users_from_sqlite_store(self) -> list[dict[str, Any]]:
        try:
            decoded = self._auth_storage.read_json("auth_users")
            if not isinstance(decoded, dict):
                return []
            out: list[dict[str, Any]] = []
            for key, value in decoded.items():
                if not isinstance(value, dict):
                    continue
                row = dict(value)
                if str(row.get("email") or "").strip() == "" and str(key).strip():
                    row["email"] = str(key).strip()
                out.append(row)
            return out
        except Exception:
            return []

    def _looks_like_email(self, value: str) -> bool:
        text = str(value or "").strip()
        return "@" in text and "." in text.rsplit("@", 1)[-1]

    def _looks_like_deliverable_email(self, value: str) -> bool:
        text = str(value or "").strip().lower()
        if not self._looks_like_email(text):
            return False
        domain = text.rsplit("@", 1)[-1]
        if domain in {
            "example.com",
            "example.org",
            "example.net",
            "localhost",
            "invalid",
            "test",
        }:
            return False
        if domain.endswith(".invalid") or domain.endswith(".test"):
            return False
        return True

    def _recipient_list(self, recipient: str | None) -> list[str]:
        source = str(recipient or "").strip()
        if not source:
            return []
        recipients: list[str] = []
        for chunk in source.replace(";", ",").replace("\n", ",").split(","):
            email = chunk.strip().lower()
            if email and email not in recipients:
                recipients.append(email)
        return recipients


class AppUpdateReleaseNotifierScheduler:
    """Background poller for release confirmation emails."""

    def __init__(
        self,
        *,
        service: AppUpdateReleaseNotifierService | None = None,
        interval_seconds: int | None = None,
    ) -> None:
        self._service = service or AppUpdateReleaseNotifierService()
        self._interval_seconds = max(
            60,
            int(
                os.getenv(
                    "APP_UPDATE_CONFIRMATION_TICK_SECONDS",
                    str(interval_seconds or 300),
                )
            ),
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
                await self._service.poll_for_new_releases(trigger="scheduled")
            except Exception:
                pass
            await asyncio.sleep(self._interval_seconds)
