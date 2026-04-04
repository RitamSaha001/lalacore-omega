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
from core.db.connection import Database
from core.automation.state_manager import AutomationStateManager
from services.atlas_incident_email_service import AtlasIncidentEmailService


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AppUpdateReleaseNotifierService:
    """Polls the published app-update sheet and emails on newly seen releases."""

    CHECKPOINT_SCOPE = "app_update_release_notifier"
    SHARED_STATE_KEY = "app_update_release_notifier_shared_state"
    CONFIRMATION_CLAIM_KEY_PREFIX = (
        "app_update_release_notifier_confirmation_claim::"
    )
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

    async def _runtime_db_pool(self):
        try:
            return await Database.get_pool()
        except Exception:
            return None

    async def _read_shared_state(self) -> dict[str, Any]:
        pool = await self._runtime_db_pool()
        if pool is None:
            return {}
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT json_value
                    FROM app_runtime_json_store
                    WHERE blob_key = $1
                    """,
                    self.SHARED_STATE_KEY,
                )
        except Exception:
            return {}
        if not row:
            return {}
        raw_value = row.get("json_value") if hasattr(row, "get") else row["json_value"]
        if raw_value is None:
            return {}
        try:
            decoded = json.loads(str(raw_value))
        except Exception:
            return {}
        return decoded if isinstance(decoded, dict) else {}

    async def _write_shared_state(self, state: dict[str, Any]) -> None:
        pool = await self._runtime_db_pool()
        if pool is None:
            return
        payload = json.dumps(state, ensure_ascii=True, indent=2)
        updated_at = int(_utc_now().timestamp() * 1000)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO app_runtime_json_store (blob_key, json_value, updated_at)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (blob_key) DO UPDATE
                    SET json_value = EXCLUDED.json_value,
                        updated_at = EXCLUDED.updated_at
                    """,
                    self.SHARED_STATE_KEY,
                    payload,
                    updated_at,
                )
        except Exception:
            return

    def _confirmation_claim_blob_key(self, confirmation_key: str) -> str:
        return f"{self.CONFIRMATION_CLAIM_KEY_PREFIX}{confirmation_key}"

    async def _try_claim_confirmation_key(
        self,
        confirmation_key: str,
        *,
        checked_at: str,
    ) -> bool:
        key = str(confirmation_key or "").strip()
        if not key:
            return True
        pool = await self._runtime_db_pool()
        if pool is None:
            return True
        payload = json.dumps(
            {
                "status": "claimed",
                "checked_at": checked_at,
            },
            ensure_ascii=True,
        )
        updated_at = int(_utc_now().timestamp() * 1000)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO app_runtime_json_store (blob_key, json_value, updated_at)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (blob_key) DO NOTHING
                    RETURNING blob_key
                    """,
                    self._confirmation_claim_blob_key(key),
                    payload,
                    updated_at,
                )
        except Exception:
            return True
        return bool(row)

    async def _clear_confirmation_claim(
        self,
        confirmation_key: str,
    ) -> None:
        key = str(confirmation_key or "").strip()
        if not key:
            return
        pool = await self._runtime_db_pool()
        if pool is None:
            return
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM app_runtime_json_store
                    WHERE blob_key = $1
                    """,
                    self._confirmation_claim_blob_key(key),
                )
        except Exception:
            return

    def _shared_state_snapshot(self) -> dict[str, Any]:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        return {
            "seen_release_keys": self._seen_release_keys_from_row(checkpoint),
            "release_recipient_sent_map": self._release_recipient_sent_map_from_row(
                checkpoint
            ),
            "release_confirmation_sent_map": self._release_confirmation_sent_map_from_row(
                checkpoint
            ),
        }

    def _merge_release_recipient_maps(
        self,
        *maps: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, str]]:
        merged: dict[str, dict[str, str]] = {}
        for raw_map in maps:
            if not isinstance(raw_map, dict):
                continue
            for raw_release_key, raw_recipient_map in raw_map.items():
                release_key = str(raw_release_key or "").strip()
                if not release_key or not isinstance(raw_recipient_map, dict):
                    continue
                current = dict(merged.get(release_key) or {})
                for raw_email, raw_ts in raw_recipient_map.items():
                    email = str(raw_email or "").strip().lower()
                    ts = str(raw_ts or "").strip()
                    if not email or not ts:
                        continue
                    existing = str(current.get(email) or "").strip()
                    current[email] = max(existing, ts) if existing else ts
                if current:
                    if len(current) > 2000:
                        current = dict(list(current.items())[-2000:])
                    merged[release_key] = current
        if len(merged) > 250:
            merged = dict(list(merged.items())[-250:])
        return merged

    def _merge_confirmation_maps(
        self,
        *maps: dict[str, str],
    ) -> dict[str, str]:
        merged: dict[str, str] = {}
        for raw_map in maps:
            if not isinstance(raw_map, dict):
                continue
            for raw_key, raw_ts in raw_map.items():
                key = str(raw_key or "").strip()
                ts = str(raw_ts or "").strip()
                if not key or not ts:
                    continue
                existing = str(merged.get(key) or "").strip()
                merged[key] = max(existing, ts) if existing else ts
        if len(merged) > 500:
            merged = dict(list(merged.items())[-500:])
        return merged

    async def _effective_shared_state(self) -> dict[str, Any]:
        local = self._shared_state_snapshot()
        remote = await self._read_shared_state()
        merged = {
            "seen_release_keys": self._merged_seen_keys(
                self._seen_release_keys_from_row(local),
                self._seen_release_keys_from_row(remote),
            ),
            "release_recipient_sent_map": self._merge_release_recipient_maps(
                self._release_recipient_sent_map_from_row(local),
                self._release_recipient_sent_map_from_row(remote),
            ),
            "release_confirmation_sent_map": self._merge_confirmation_maps(
                self._release_confirmation_sent_map_from_row(local),
                self._release_confirmation_sent_map_from_row(remote),
            ),
        }
        checkpoint_updates = {
            "seen_release_keys": merged["seen_release_keys"],
            "release_recipient_sent_map": merged["release_recipient_sent_map"],
            "release_confirmation_sent_map": merged["release_confirmation_sent_map"],
        }
        self._state.checkpoint(self.CHECKPOINT_SCOPE, **checkpoint_updates)
        if merged != remote:
            await self._write_shared_state(merged)
        return merged

    async def _write_effective_shared_state(
        self,
        *,
        seen_release_keys: list[str] | None = None,
        release_recipient_sent_map: dict[str, dict[str, str]] | None = None,
        release_confirmation_sent_map: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        current = await self._effective_shared_state()
        next_state = {
            "seen_release_keys": self._merged_seen_keys(
                current.get("seen_release_keys") or [],
                seen_release_keys or [],
            ),
            "release_recipient_sent_map": self._merge_release_recipient_maps(
                current.get("release_recipient_sent_map") or {},
                release_recipient_sent_map or {},
            ),
            "release_confirmation_sent_map": self._merge_confirmation_maps(
                current.get("release_confirmation_sent_map") or {},
                release_confirmation_sent_map or {},
            ),
        }
        self._state.checkpoint(
            self.CHECKPOINT_SCOPE,
            seen_release_keys=next_state["seen_release_keys"],
            release_recipient_sent_map=next_state["release_recipient_sent_map"],
            release_confirmation_sent_map=next_state["release_confirmation_sent_map"],
        )
        await self._write_shared_state(next_state)
        return next_state

    def enabled(self) -> bool:
        raw = os.getenv("APP_UPDATE_CONFIRMATION_ENABLED")
        if raw is None:
            return True
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def _runtime_is_production_like(self) -> bool:
        candidates = (
            os.getenv("APP_ENV", ""),
            os.getenv("NODE_ENV", ""),
            os.getenv("RAILWAY_ENVIRONMENT", ""),
        )
        return any(str(value or "").strip().lower() == "production" for value in candidates)

    def _background_trigger_allowed(self, trigger: str) -> bool:
        normalized = str(trigger or "").strip().lower()
        if normalized not in {"scheduled", "tick"}:
            return True
        if self._runtime_is_production_like():
            return True
        raw = os.getenv("APP_UPDATE_CONFIRMATION_ALLOW_NON_PRODUCTION", "")
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
            if not self._background_trigger_allowed(trigger):
                self._state.checkpoint(
                    self.CHECKPOINT_SCOPE,
                    running=False,
                    last_checked_ts=checked_at,
                    last_status="disabled_non_production",
                    last_trigger=trigger,
                )
                return {
                    "ok": True,
                    "status": "DISABLED_NON_PRODUCTION",
                    "checked_at": checked_at,
                    "sheet_url": self.sheet_url(),
                    "message": (
                        "Background release polling is disabled outside production-like "
                        "runtime unless APP_UPDATE_CONFIRMATION_ALLOW_NON_PRODUCTION=true."
                    ),
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
                shared_state = await self._effective_shared_state()
                seen_release_keys = self._merged_seen_keys(
                    self._seen_release_keys(),
                    self._seen_release_keys_from_row(shared_state),
                )
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

                target_recipients = self._release_recipient_provider(
                    new_releases,
                    recipient_email,
                )
                announcement_result = await self._send_release_announcements_to_pending_recipients(
                    releases=new_releases,
                    sheet_url=sheet_url,
                    recipients=target_recipients,
                    trigger=trigger,
                    checked_at=checked_at,
                    shared_state=shared_state,
                )
                announcement_ok = bool(announcement_result.get("ok"))
                no_deliverable_recipients = bool(
                    announcement_result.get("no_deliverable_recipients")
                )
                if (
                    no_deliverable_recipients
                    and not self._trigger_requires_publish_confirmation(trigger)
                ):
                    confirmation_result = {
                        "ok": True,
                        "sent": False,
                        "message": "",
                    }
                else:
                    confirmation_result = await self._send_release_confirmation_if_needed(
                        releases=new_releases,
                        sheet_url=sheet_url,
                        recipient=recipient_email,
                        trigger=trigger,
                        checked_at=checked_at,
                        shared_state=shared_state,
                    )
                mail_message = self._combined_mail_message(
                    announcement_result=announcement_result,
                    confirmation_result=confirmation_result,
                )
                updated_seen = self._merged_seen_keys(
                    seen_release_keys,
                    [release["release_key"] for release in new_releases],
                )
                if announcement_ok and no_deliverable_recipients:
                    await self._write_effective_shared_state(
                        seen_release_keys=updated_seen,
                    )
                    self._state.checkpoint(
                        self.CHECKPOINT_SCOPE,
                        running=False,
                        last_status="no_deliverable_recipients",
                        last_error="",
                        last_mail_sent=False,
                        last_mail_message=mail_message,
                        last_release_count=len(releases),
                        last_new_release_count=len(new_releases),
                        last_detected_release_keys=[r["release_key"] for r in releases[:20]],
                        seen_release_keys=updated_seen,
                    )
                elif announcement_ok:
                    await self._write_effective_shared_state(
                        seen_release_keys=updated_seen,
                    )
                    self._state.checkpoint(
                        self.CHECKPOINT_SCOPE,
                        running=False,
                        last_status="mail_sent",
                        last_error="",
                        last_mail_sent=True,
                        last_mail_message=mail_message,
                        last_mail_sent_ts=checked_at,
                        last_release_count=len(releases),
                        last_new_release_count=len(new_releases),
                        last_detected_release_keys=[r["release_key"] for r in releases[:20]],
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
                    "ok": announcement_ok,
                    "status": (
                        "NO_DELIVERABLE_RECIPIENTS"
                        if announcement_ok and no_deliverable_recipients
                        else "SUCCESS"
                        if announcement_ok
                        else "MAIL_FAILED"
                    ),
                    "checked_at": checked_at,
                    "sheet_url": sheet_url,
                    "release_count": len(releases),
                    "new_release_count": len(new_releases),
                    "new_releases": new_releases,
                    "announcement_recipients": target_recipients,
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

    async def notify_pending_releases_for_email_async(
        self,
        email: str,
        *,
        role: str | None = None,
        platform: str | None = None,
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
                platform=platform,
            )
            shared_state = await self._effective_shared_state()
            recipient_map = self._merge_release_recipient_maps(
                self._release_recipient_sent_map(),
                self._release_recipient_sent_map_from_row(shared_state),
            )
            pending_releases = [
                release
                for release in relevant_releases
                if not self._release_already_sent_to_recipient_from_map(
                    normalized_email,
                    str(release.get("release_key") or ""),
                    recipient_map,
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
                await self._mark_release_sent_for_recipients_async(
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

    def notify_pending_releases_for_email(
        self,
        email: str,
        *,
        role: str | None = None,
        platform: str | None = None,
        trigger: str = "auth_login",
    ) -> dict[str, Any]:
        try:
            return asyncio.run(
                self.notify_pending_releases_for_email_async(
                    email,
                    role=role,
                    platform=platform,
                    trigger=trigger,
                )
            )
        except RuntimeError:
            # Fall back to the async-safe path when already inside a running loop.
            return {
                "ok": False,
                "status": "FAILED",
                "sent_count": 0,
                "releases": [],
                "message": "notify_pending_releases_for_email_async must be awaited from async contexts",
            }

    def _seen_release_keys_from_row(self, row: dict[str, Any]) -> list[str]:
        raw = row.get("seen_release_keys")
        if not isinstance(raw, list):
            return []
        return [
            str(item).strip()
            for item in raw
            if str(item).strip()
        ]

    def _seen_release_keys(self) -> list[str]:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        return self._seen_release_keys_from_row(checkpoint)

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

    def _release_recipient_sent_map_from_row(
        self,
        row: dict[str, Any],
    ) -> dict[str, dict[str, str]]:
        raw = row.get("release_recipient_sent_map")
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

    def _release_recipient_sent_map(self) -> dict[str, dict[str, str]]:
        checkpoint = self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
        return self._release_recipient_sent_map_from_row(checkpoint)

    def _release_confirmation_sent_map_from_row(
        self,
        row: dict[str, Any],
    ) -> dict[str, str]:
        raw = row.get("release_confirmation_sent_map")
        if not isinstance(raw, dict):
            return {}
        cleaned: dict[str, str] = {}
        for raw_key, raw_ts in raw.items():
            key = str(raw_key or "").strip()
            ts = str(raw_ts or "").strip()
            if key and ts:
                cleaned[key] = ts
        return cleaned

    def _release_confirmation_key(
        self,
        releases: list[dict[str, Any]],
        recipient: str | None,
    ) -> str:
        release_keys = sorted(
            str(item.get("release_key") or "").strip()
            for item in releases
            if isinstance(item, dict) and str(item.get("release_key") or "").strip()
        )
        recipient_key = "|".join(self._recipient_list(recipient))
        return "::".join([recipient_key or "default", *release_keys])

    def _trigger_allows_release_confirmation(self, trigger: str) -> bool:
        normalized = str(trigger or "").strip().lower()
        return normalized in {
            "manual",
            "publish_script",
            "release_publish",
            "sheet_publish",
        }

    def _trigger_requires_publish_confirmation(self, trigger: str) -> bool:
        normalized = str(trigger or "").strip().lower()
        return normalized in {
            "publish_script",
            "release_publish",
            "sheet_publish",
        }

    def _release_already_sent_to_recipient_from_map(
        self,
        email: str,
        release_key: str,
        recipient_map: dict[str, dict[str, str]],
    ) -> bool:
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

    async def _mark_release_sent_for_recipients_async(
        self,
        *,
        releases: list[dict[str, Any]],
        recipients: list[str],
    ) -> None:
        shared_state = await self._effective_shared_state()
        recipient_map = self._merge_release_recipient_maps(
            self._release_recipient_sent_map(),
            self._release_recipient_sent_map_from_row(shared_state),
        )
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
        await self._write_effective_shared_state(
            release_recipient_sent_map=recipient_map,
        )

    async def _mark_release_confirmation_sent_async(
        self,
        *,
        releases: list[dict[str, Any]],
        recipient: str | None,
    ) -> None:
        key = self._release_confirmation_key(releases, recipient)
        if not key:
            return
        await self._write_effective_shared_state(
            release_confirmation_sent_map={key: _utc_now().isoformat()},
        )

    async def _send_release_announcements_to_pending_recipients(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        recipients: list[str],
        trigger: str,
        checked_at: str,
        shared_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        recipient_list = self._recipient_list(",".join(recipients))
        if not recipient_list:
            return await asyncio.to_thread(
                self._email.send_release_announcement,
                releases=releases,
                sheet_url=sheet_url,
                recipients=[],
                trigger=trigger,
                checked_at=checked_at,
            )
        recipient_map = self._merge_release_recipient_maps(
            self._release_recipient_sent_map(),
            self._release_recipient_sent_map_from_row(shared_state or {}),
        )
        grouped_recipients: dict[tuple[str, ...], list[str]] = {}
        grouped_releases: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for recipient in recipient_list:
            pending_releases = [
                dict(release)
                for release in releases
                if isinstance(release, dict)
                and not self._release_already_sent_to_recipient_from_map(
                    recipient,
                    str(release.get("release_key") or ""),
                    recipient_map,
                )
            ]
            if pending_releases:
                signature = tuple(
                    str(item.get("release_key") or "").strip()
                    for item in pending_releases
                    if str(item.get("release_key") or "").strip()
                )
                if not signature:
                    continue
                grouped_recipients.setdefault(signature, []).append(recipient)
                grouped_releases[signature] = pending_releases
        if not grouped_recipients:
            return {
                "ok": True,
                "sent": False,
                "message": "No signed-in user email recipients were pending for this release",
                "recipients": recipient_list,
                "sent_recipients": [],
                "failed_recipients": [],
                "sent_count": 0,
                "failed_count": 0,
                "no_deliverable_recipients": True,
            }

        sent_recipients: list[str] = []
        failed_recipients: list[str] = []
        last_message = ""
        for signature, group_recipients in grouped_recipients.items():
            pending_releases = grouped_releases.get(signature) or []
            result = await asyncio.to_thread(
                self._email.send_release_announcement,
                releases=pending_releases,
                sheet_url=sheet_url,
                recipients=group_recipients,
                trigger=trigger,
                checked_at=checked_at,
            )
            last_message = str(result.get("message") or "").strip() or last_message
            if bool(result.get("ok")):
                sent_recipients.extend(group_recipients)
                await self._mark_release_sent_for_recipients_async(
                    releases=pending_releases,
                    recipients=group_recipients,
                )
            else:
                failed_recipients.extend(group_recipients)
        return {
            "ok": not failed_recipients and bool(sent_recipients),
            "sent": not failed_recipients and bool(sent_recipients),
            "message": last_message or "Release announcement completed",
            "recipients": recipient_list,
            "sent_recipients": sent_recipients,
            "failed_recipients": failed_recipients,
            "sent_count": len(sent_recipients),
            "failed_count": len(failed_recipients),
        }

    async def _send_release_confirmation_if_needed(
        self,
        *,
        releases: list[dict[str, Any]],
        sheet_url: str,
        recipient: str | None,
        trigger: str,
        checked_at: str,
        shared_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self._trigger_allows_release_confirmation(trigger):
            return {
                "ok": True,
                "sent": False,
                "message": "",
            }
        confirmation_map = self._merge_confirmation_maps(
            self._release_confirmation_sent_map_from_row(
                self._state.checkpoint_row(self.CHECKPOINT_SCOPE)
            ),
            self._release_confirmation_sent_map_from_row(shared_state or {}),
        )
        confirmation_key = self._release_confirmation_key(releases, recipient)
        if trigger == "scheduled" and confirmation_key and confirmation_key in confirmation_map:
            return {
                "ok": True,
                "sent": False,
                "message": "Release confirmation already sent for this release batch",
            }
        claimed = False
        if trigger == "scheduled" and confirmation_key:
            claimed = await self._try_claim_confirmation_key(
                confirmation_key,
                checked_at=checked_at,
            )
            if not claimed:
                return {
                    "ok": True,
                    "sent": False,
                    "message": (
                        "Release confirmation is already being processed for "
                        "this release batch"
                    ),
                }
        result = await asyncio.to_thread(
            self._email.send_release_confirmation,
            releases=releases,
            sheet_url=sheet_url,
            recipient=recipient,
            trigger=trigger,
            checked_at=checked_at,
        )
        if bool(result.get("ok")):
            await self._mark_release_confirmation_sent_async(
                releases=releases,
                recipient=recipient,
            )
            if claimed:
                await self._clear_confirmation_claim(confirmation_key)
        elif claimed:
            await self._clear_confirmation_claim(confirmation_key)
        return result

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
        platform: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_role = str(role or "").strip().lower()
        normalized_platform = self._normalize_platform(platform)
        latest: dict[str, dict[str, Any]] = {}
        for release in releases:
            if not isinstance(release, dict):
                continue
            if not self._release_targets_identity(
                release,
                role=normalized_role,
                platform=normalized_platform,
            ):
                continue
            audience = str(release.get("audience") or "").strip().lower() or "all"
            group_key = "|".join(
                [
                    str(release.get("app_id") or "").strip().lower(),
                    str(release.get("channel") or "").strip().lower(),
                    str(release.get("platform") or "").strip().lower(),
                    audience,
                ]
            )
            current = latest.get(group_key)
            if current is None or self._release_sort_key(release) >= self._release_sort_key(current):
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
        target_platforms = {
            self._normalize_platform(item.get("platform")) or "all"
            for item in releases
            if isinstance(item, dict)
        }
        if not target_audiences:
            target_audiences = {"all"}
        if not target_platforms:
            target_platforms = {"all"}
        recipients: list[str] = []
        seen: set[str] = set()
        for user in self._iter_auth_users():
            email = str(user.get("email") or "").strip().lower()
            if not self._looks_like_deliverable_email(email):
                continue
            role = str(user.get("role") or "").strip().lower()
            platform = self._user_platform(user)
            if "all" not in target_audiences and role not in target_audiences:
                continue
            if "all" not in target_platforms and platform not in target_platforms:
                continue
            if email in seen:
                continue
            seen.add(email)
            recipients.append(email)
        return recipients

    def _release_targets_identity(
        self,
        release: dict[str, Any],
        *,
        role: str,
        platform: str,
    ) -> bool:
        audience = str(release.get("audience") or "").strip().lower() or "all"
        target_platform = self._normalize_platform(release.get("platform"))
        if audience != "all" and audience != role:
            return False
        if target_platform != "all" and target_platform != platform:
            return False
        return True

    def _release_sort_key(self, release: dict[str, Any]) -> tuple[int, tuple[int | str, ...], str]:
        build_number = self._to_int(release.get("build_number"), 0)
        version = str(release.get("version") or "").strip()
        version_parts: list[int | str] = []
        for part in version.replace("-", ".").split("."):
            token = part.strip()
            if not token:
                continue
            if token.isdigit():
                version_parts.append(int(token))
            else:
                version_parts.append(token.lower())
        return build_number, tuple(version_parts), str(release.get("release_key") or "")

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

    def _user_platform(self, user: dict[str, Any]) -> str:
        device_info = user.get("device_info")
        if isinstance(device_info, dict):
            raw = (
                device_info.get("platform")
                or device_info.get("os")
                or device_info.get("device_platform")
            )
            normalized = self._normalize_platform(raw)
            if normalized:
                return normalized
        return self._normalize_platform(
            user.get("platform")
            or user.get("last_platform")
            or user.get("device_platform")
        )

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

    def _normalize_platform(self, value: Any) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        if text in {"all", "*"}:
            return "all"
        if "android" in text:
            return "android"
        if any(token in text for token in ("ios", "iphone", "ipad")):
            return "ios"
        if "mac" in text:
            return "macos"
        if any(token in text for token in ("web", "chrome", "browser", "safari")):
            return "web"
        return text

    def _to_int(self, value: Any, fallback: int = 0) -> int:
        try:
            return int(float(str(value).strip()))
        except Exception:
            return fallback

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
