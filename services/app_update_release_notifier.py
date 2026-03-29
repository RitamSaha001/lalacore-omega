from __future__ import annotations

import asyncio
import contextlib
import csv
import io
import os
import ssl
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.request import Request, urlopen

from core.automation.state_manager import AutomationStateManager
from services.atlas_incident_email_service import AtlasIncidentEmailService


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AppUpdateReleaseNotifierService:
    """Polls the published app-update sheet and emails on newly seen releases."""

    CHECKPOINT_SCOPE = "app_update_release_notifier"
    DEFAULT_SHEET_URL = (
        "https://docs.google.com/spreadsheets/d/e/"
        "2PACX-1vRbG3TbovNdmce0l6UP3DAeyb4CMdKkPXXau3hKCXmZnPWjlCzkDiy8VGPnVF6xhS_iypTjqLVvdrqU/"
        "pub?output=csv"
    )

    def __init__(
        self,
        *,
        state: AutomationStateManager | None = None,
        email_service: AtlasIncidentEmailService | None = None,
        fetcher: Callable[[str], str] | None = None,
        sheet_url: str | None = None,
    ) -> None:
        self._state = state or AutomationStateManager()
        self._email = email_service or AtlasIncidentEmailService()
        self._fetcher = fetcher or self._fetch_sheet_csv
        self._sheet_url_override = (sheet_url or "").strip()
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

                mail_result = await asyncio.to_thread(
                    self._email.send_release_confirmation,
                    releases=new_releases,
                    sheet_url=sheet_url,
                    recipient=recipient_email,
                    trigger=trigger,
                    checked_at=checked_at,
                )
                if bool(mail_result.get("ok")):
                    updated_seen = self._merged_seen_keys(
                        seen_release_keys,
                        [release["release_key"] for release in new_releases],
                    )
                    self._state.checkpoint(
                        self.CHECKPOINT_SCOPE,
                        running=False,
                        last_status="mail_sent",
                        last_error="",
                        last_mail_sent=True,
                        last_mail_message=str(mail_result.get("message") or ""),
                        last_mail_sent_ts=checked_at,
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
                        last_error=str(mail_result.get("message") or "mail_failed"),
                        last_mail_sent=False,
                        last_mail_message=str(mail_result.get("message") or ""),
                        last_release_count=len(releases),
                        last_new_release_count=len(new_releases),
                        last_detected_release_keys=[r["release_key"] for r in releases[:20]],
                    )
                return {
                    "ok": bool(mail_result.get("ok")),
                    "status": "SUCCESS" if bool(mail_result.get("ok")) else "MAIL_FAILED",
                    "checked_at": checked_at,
                    "sheet_url": sheet_url,
                    "release_count": len(releases),
                    "new_release_count": len(new_releases),
                    "new_releases": new_releases,
                    "mail": dict(mail_result),
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
