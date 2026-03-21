from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
import requests

from app.services.bilingual_stt_service import BilingualSttService

router = APIRouter()
_STT = BilingualSttService()


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _json_compact(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


@dataclass
class JoinRequestRecord:
    request_id: str
    class_id: str
    user_id: str
    user_name: str
    role: str
    requested_at: str
    device_info: dict[str, Any] = field(default_factory=dict)
    camera_enabled: bool = True
    mic_enabled: bool = True


@dataclass
class LiveClassRecord:
    class_id: str
    title: str
    teacher_name: str
    subject: str
    topic: str
    is_recording: bool = False
    meeting_locked: bool = False
    chat_enabled: bool = True
    waiting_room_enabled: bool = True
    join_requests: dict[str, JoinRequestRecord] = field(default_factory=dict)
    approved_users: set[str] = field(default_factory=set)
    breakout_room_by_user: dict[str, str] = field(default_factory=dict)
    whiteboard_access_users: set[str] = field(default_factory=set)
    active_whiteboard_user_id: str | None = None
    whiteboard_strokes: list[dict[str, Any]] = field(default_factory=list)
    muted_users: set[str] = field(default_factory=set)
    camera_disabled_users: set[str] = field(default_factory=set)


class LiveTokenRequest(BaseModel):
    class_id: str
    user_id: str
    display_name: str
    role: str = "student"
    title: str | None = None
    teacher_name: str | None = None
    subject: str | None = None
    topic: str | None = None


class JoinRequestPayload(BaseModel):
    class_id: str
    user_id: str
    user_name: str
    role: str = "student"
    device_info: dict[str, Any] = Field(default_factory=dict)
    session_token: str = ""
    camera_enabled: bool = True
    mic_enabled: bool = True


class JoinCancelPayload(BaseModel):
    class_id: str
    user_id: str
    request_id: str
    session_token: str = ""


class AdmitPayload(BaseModel):
    class_id: str
    user_id: str


class RejectPayload(BaseModel):
    class_id: str
    user_id: str
    reason: str | None = None


class AdmitAllPayload(BaseModel):
    class_id: str


class FallbackTokenPayload(BaseModel):
    class_id: str
    user_id: str


class SetMeetingLockPayload(BaseModel):
    class_id: str
    locked: bool


class SetChatEnabledPayload(BaseModel):
    class_id: str
    enabled: bool


class SetWaitingRoomEnabledPayload(BaseModel):
    class_id: str
    enabled: bool


class SetRecordingPayload(BaseModel):
    class_id: str
    enabled: bool


class MuteUserPayload(BaseModel):
    class_id: str
    user_id: str
    muted: bool = True


class RemoveUserPayload(BaseModel):
    class_id: str
    user_id: str


class CameraDisablePayload(BaseModel):
    class_id: str
    user_id: str
    disabled: bool = True


class BreakoutMovePayload(BaseModel):
    class_id: str
    user_id: str
    room_id: str | None = None


class BreakoutBroadcastPayload(BaseModel):
    class_id: str
    message: str


class WhiteboardAccessPayload(BaseModel):
    class_id: str
    user_id: str
    enabled: bool


class LiveClassHub:
    _MAX_WHITEBOARD_STROKES = 400

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._classes: dict[str, LiveClassRecord] = {}
        self._event_sockets: dict[str, set[WebSocket]] = {}
        self._sync_sockets: dict[str, set[WebSocket]] = {}
        self._signal_sockets: dict[str, dict[WebSocket, str]] = {}

    def _token_secret(self) -> str:
        return (
            os.getenv("LIVE_CLASSES_TOKEN_SECRET", "").strip()
            or os.getenv("REQUEST_SIGNING_SECRET", "").strip()
            or "lalacore-live-class-dev-secret"
        )

    def _rtc_provider(self) -> str:
        provider = os.getenv("LIVE_CLASSES_RTC_PROVIDER", "").strip().lower()
        return provider or "native_bridge"

    def _livekit_api_url(self) -> str:
        explicit = os.getenv("LIVEKIT_API_URL", "").strip()
        if explicit:
            return explicit.rstrip("/")
        ws_url = os.getenv("LIVEKIT_WS_URL", "").strip() or "ws://localhost:7880"
        if ws_url.startswith("wss://"):
            return "https://" + ws_url[len("wss://") :].rstrip("/")
        if ws_url.startswith("ws://"):
            return "http://" + ws_url[len("ws://") :].rstrip("/")
        return ws_url.rstrip("/")

    def _livekit_admin_token(self, room: str) -> str:
        api_key = os.getenv("LIVEKIT_API_KEY", "").strip() or "devkey"
        api_secret = os.getenv("LIVEKIT_API_SECRET", "").strip() or "secret"
        issued_at = int(time.time())
        expires_at = issued_at + 300
        claims = {
            "iss": api_key,
            "sub": "lalacore_admin",
            "nbf": issued_at,
            "exp": expires_at,
            "video": {"room": room, "roomAdmin": True},
        }
        header = {"alg": "HS256", "typ": "JWT"}
        header_b64 = _b64url(_json_compact(header))
        payload_b64 = _b64url(_json_compact(claims))
        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
        signature = hmac.new(
            api_secret.encode("utf-8"), signing_input, hashlib.sha256
        ).digest()
        signature_b64 = _b64url(signature)
        return f"{header_b64}.{payload_b64}.{signature_b64}"

    def _livekit_request(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._rtc_provider() != "livekit":
            return {"_ok": False, "_skipped": True}
        api_url = self._livekit_api_url()
        token = self._livekit_admin_token(payload.get("room") or payload.get("name") or "")
        url = f"{api_url}/twirp/livekit.RoomService/{method}"
        try:
            response = requests.post(
                url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=6,
            )
            if response.status_code >= 400:
                return {"_ok": False, "status": response.status_code}
            if response.content:
                data = response.json()
                if isinstance(data, dict):
                    data["_ok"] = True
                    return data
            return {"_ok": True}
        except Exception:
            return {"_ok": False}

    def _livekit_list_participants(self, room: str) -> list[dict[str, Any]]:
        payload = {"room": room}
        response = self._livekit_request("ListParticipants", payload)
        if response.get("_ok") is False:
            return []
        raw = response.get("participants")
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
        return []

    def _livekit_find_participant(
        self, room: str, identity: str
    ) -> dict[str, Any] | None:
        for participant in self._livekit_list_participants(room):
            if str(participant.get("identity") or "") == identity:
                return participant
        return None

    def _livekit_find_track_sid(
        self, participant: dict[str, Any], *, kind: str
    ) -> str | None:
        tracks = participant.get("tracks")
        if not isinstance(tracks, list):
            return None
        target = kind.lower()
        for track in tracks:
            if not isinstance(track, dict):
                continue
            source = str(track.get("source") or "").lower()
            if target in {"microphone", "audio"} and "microphone" in source:
                return str(track.get("sid") or "")
            if target in {"camera", "video"} and "camera" in source:
                return str(track.get("sid") or "")
        return None

    def _livekit_mute_track(
        self, room: str, identity: str, *, kind: str, muted: bool
    ) -> bool:
        participant = self._livekit_find_participant(room, identity)
        if not participant:
            return False
        track_sid = self._livekit_find_track_sid(participant, kind=kind)
        if not track_sid:
            return False
        response = self._livekit_request(
            "MutePublishedTrack",
            {"room": room, "identity": identity, "track_sid": track_sid, "muted": muted},
        )
        return response.get("_ok") is True

    def _livekit_remove_participant(self, room: str, identity: str) -> bool:
        response = self._livekit_request(
            "RemoveParticipant",
            {"room": room, "identity": identity},
        )
        return response.get("_ok") is True

    async def issue_live_token(self, req: LiveTokenRequest) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(
                req.class_id,
                title=req.title or "JEE Live Class",
                teacher_name=req.teacher_name or "Dr. A. Sharma",
                subject=req.subject or "General",
                topic=req.topic or "Lecture",
            )

        provider = self._rtc_provider()
        if provider == "livekit":
            return self._issue_livekit_token(room, req)

        issued_at = int(time.time())
        expires_at = issued_at + 600
        claims = {
            "sub": req.user_id,
            "name": req.display_name,
            "role": req.role,
            "room": room.class_id,
            "title": room.title,
            "teacher_name": room.teacher_name,
            "subject": room.subject,
            "topic": room.topic,
            "iat": issued_at,
            "exp": expires_at,
            "nonce": secrets.token_hex(8),
        }
        payload_bytes = _json_compact(claims)
        digest = hmac.new(
            self._token_secret().encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).digest()
        token = f"{_b64url(payload_bytes)}.{_b64url(digest)}"
        return {
            "ok": True,
            "status": "SUCCESS",
            "provider": provider,
            "session_id": room.class_id,
            "room_id": room.class_id,
            "token": token,
            "expires_at": expires_at,
        }

    def _issue_livekit_token(
        self, room: LiveClassRecord, req: LiveTokenRequest
    ) -> dict[str, Any]:
        api_key = os.getenv("LIVEKIT_API_KEY", "").strip() or "devkey"
        api_secret = os.getenv("LIVEKIT_API_SECRET", "").strip() or "secret"
        ws_url = os.getenv("LIVEKIT_WS_URL", "").strip() or "ws://localhost:7880"
        issued_at = int(time.time())
        expires_at = issued_at + 3600
        is_teacher = req.role.strip().lower() in {"teacher", "host", "cohost", "co_host"}
        claims = {
            "iss": api_key,
            "sub": req.user_id,
            "name": req.display_name,
            "nbf": issued_at,
            "exp": expires_at,
            "video": {
                "room": room.class_id,
                "roomJoin": True,
                "roomAdmin": is_teacher,
                "roomRecord": is_teacher,
                "canPublish": True,
                "canPublishData": True,
                "canSubscribe": True,
                "canUpdateOwnMetadata": True,
            },
            "metadata": json.dumps(
                {
                    "role": req.role,
                    "subject": room.subject,
                    "topic": room.topic,
                    "teacher_name": room.teacher_name,
                    "title": room.title,
                },
                separators=(",", ":"),
            ),
        }
        header = {"alg": "HS256", "typ": "JWT"}
        header_b64 = _b64url(_json_compact(header))
        claims_b64 = _b64url(_json_compact(claims))
        signature = hmac.new(
            api_secret.encode("utf-8"),
            f"{header_b64}.{claims_b64}".encode("utf-8"),
            hashlib.sha256,
        ).digest()
        token = f"{header_b64}.{claims_b64}.{_b64url(signature)}"
        return {
            "ok": True,
            "status": "SUCCESS",
            "provider": "livekit",
            "session_id": room.class_id,
            "room_id": room.class_id,
            "room": room.class_id,
            "token": token,
            "expires_at": expires_at,
            "ws_url": ws_url,
        }

    async def session_payload(
        self,
        class_id: str,
        *,
        title: str | None = None,
        teacher_name: str | None = None,
        subject: str | None = None,
        topic: str | None = None,
    ) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(
                class_id,
                title=title or "JEE Live Class",
                teacher_name=teacher_name or "Dr. A. Sharma",
                subject=subject or "General",
                topic=topic or "Lecture",
            )
            return self._session_payload(room)

    async def request_join(self, payload: JoinRequestPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            existing = room.join_requests.get(payload.user_id)
            if existing is not None:
                return {
                    "ok": True,
                    "status": "DUPLICATE",
                    "request_id": existing.request_id,
                }
            request_id = f"join_{int(time.time() * 1000)}_{secrets.token_hex(3)}"
            request = JoinRequestRecord(
                request_id=request_id,
                class_id=payload.class_id,
                user_id=payload.user_id,
                user_name=payload.user_name,
                role=payload.role,
                requested_at=_utc_now_iso(),
                device_info=dict(payload.device_info),
                camera_enabled=payload.camera_enabled,
                mic_enabled=payload.mic_enabled,
            )
            room.join_requests[payload.user_id] = request
            waiting_snapshot = self._waiting_room_snapshot(room)
            event_payload = {
                "type": "join_request_received",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "user_name": payload.user_name,
                "requested_at": request.requested_at,
                "request_id": request_id,
            }
        await self._broadcast_events(payload.class_id, event_payload)
        await self._broadcast_events(payload.class_id, waiting_snapshot)
        return {"ok": True, "status": "PENDING", "request_id": request_id}

    async def cancel_join_request(self, payload: JoinCancelPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            removed = room.join_requests.pop(payload.user_id, None)
            waiting_snapshot = self._waiting_room_snapshot(room)
        if removed is not None:
            await self._broadcast_events(
                payload.class_id,
                {
                    "type": "join_request_removed",
                    "class_id": payload.class_id,
                    "user_id": payload.user_id,
                    "request_id": removed.request_id,
                },
            )
            await self._broadcast_events(payload.class_id, waiting_snapshot)
        return {"ok": True, "status": "CANCELED"}

    async def approve_join(self, payload: AdmitPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            request = room.join_requests.pop(payload.user_id, None)
            if request is None:
                return {"ok": True, "status": "MISSING"}
            room.approved_users.add(payload.user_id)
            waiting_snapshot = self._waiting_room_snapshot(room)
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "join_approved",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "request_id": request.request_id,
            },
        )
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "join_request_removed",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "request_id": request.request_id,
            },
        )
        await self._broadcast_events(payload.class_id, waiting_snapshot)
        return {"ok": True, "status": "APPROVED"}

    async def reject_join(self, payload: RejectPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            request = room.join_requests.pop(payload.user_id, None)
            if request is None:
                return {"ok": True, "status": "MISSING"}
            waiting_snapshot = self._waiting_room_snapshot(room)
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "join_rejected",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "request_id": request.request_id,
                "message": payload.reason or "Teacher declined the join request.",
            },
        )
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "join_request_removed",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "request_id": request.request_id,
            },
        )
        await self._broadcast_events(payload.class_id, waiting_snapshot)
        return {"ok": True, "status": "REJECTED"}

    async def approve_all(self, payload: AdmitAllPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            requests = list(room.join_requests.values())
            room.join_requests.clear()
            room.approved_users.update(item.user_id for item in requests)
            waiting_snapshot = self._waiting_room_snapshot(room)
        for request in requests:
            await self._broadcast_events(
                payload.class_id,
                {
                    "type": "join_approved",
                    "class_id": payload.class_id,
                    "user_id": request.user_id,
                    "request_id": request.request_id,
                },
            )
        await self._broadcast_events(payload.class_id, waiting_snapshot)
        return {"ok": True, "status": "APPROVED_ALL", "count": len(requests)}

    async def fallback_token(self, payload: FallbackTokenPayload, request: Request) -> dict[str, Any]:
        token_response = await self.issue_live_token(
            LiveTokenRequest(
                class_id=payload.class_id,
                user_id=payload.user_id,
                display_name=payload.user_id,
                role="student",
            )
        )
        scheme = "wss" if request.url.scheme == "https" else "ws"
        ws_url = f"{scheme}://{request.url.netloc}/class/fallback_signal"
        return {
            "provider": "webrtc",
            "room": payload.class_id,
            "token": token_response["token"],
            "url": ws_url,
        }

    async def class_state_payload(self, class_id: str, user_id: str) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(class_id)
            return {
                "ok": True,
                "status": "SUCCESS",
                "class_id": class_id,
                "user_id": user_id,
                "active_breakout_room_id": room.breakout_room_by_user.get(user_id),
                "active_whiteboard_user_id": room.active_whiteboard_user_id,
                "whiteboard_strokes": list(room.whiteboard_strokes),
                "whiteboard_access": user_id in room.whiteboard_access_users,
                "muted": user_id in room.muted_users,
                "camera_disabled": user_id in room.camera_disabled_users,
                "meeting_locked": room.meeting_locked,
                "chat_enabled": room.chat_enabled,
                "waiting_room_enabled": room.waiting_room_enabled,
                "is_recording": room.is_recording,
            }

    async def set_meeting_lock(self, payload: SetMeetingLockPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            room.meeting_locked = payload.locked
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "meeting_lock_changed",
                "class_id": payload.class_id,
                "locked": payload.locked,
            },
        )
        return {"ok": True, "status": "SUCCESS", "meeting_locked": payload.locked}

    async def set_chat_enabled(self, payload: SetChatEnabledPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            room.chat_enabled = payload.enabled
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "chat_enabled_changed",
                "class_id": payload.class_id,
                "enabled": payload.enabled,
            },
        )
        return {"ok": True, "status": "SUCCESS", "chat_enabled": payload.enabled}

    async def set_waiting_room_enabled(
        self, payload: SetWaitingRoomEnabledPayload
    ) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            room.waiting_room_enabled = payload.enabled
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "waiting_room_changed",
                "class_id": payload.class_id,
                "enabled": payload.enabled,
            },
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "waiting_room_enabled": payload.enabled,
        }

    async def set_recording_enabled(self, payload: SetRecordingPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            room.is_recording = payload.enabled
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "recording_changed",
                "class_id": payload.class_id,
                "enabled": payload.enabled,
            },
        )
        return {"ok": True, "status": "SUCCESS", "is_recording": payload.enabled}

    async def set_user_muted(self, payload: MuteUserPayload) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            if payload.muted:
                room.muted_users.add(payload.user_id)
            else:
                room.muted_users.discard(payload.user_id)
        livekit_enforced = None
        if self._rtc_provider() == "livekit":
            livekit_enforced = self._livekit_mute_track(
                payload.class_id,
                payload.user_id,
                kind="microphone",
                muted=payload.muted,
            )
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "user_muted",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "muted": payload.muted,
            },
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "muted": payload.muted,
            "livekit_enforced": livekit_enforced,
        }

    async def set_user_camera_disabled(
        self, payload: CameraDisablePayload
    ) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            if payload.disabled:
                room.camera_disabled_users.add(payload.user_id)
            else:
                room.camera_disabled_users.discard(payload.user_id)
        livekit_enforced = None
        if self._rtc_provider() == "livekit":
            livekit_enforced = self._livekit_mute_track(
                payload.class_id,
                payload.user_id,
                kind="camera",
                muted=payload.disabled,
            )
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "user_camera_disabled",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "disabled": payload.disabled,
            },
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "disabled": payload.disabled,
            "livekit_enforced": livekit_enforced,
        }

    async def remove_user(self, payload: RemoveUserPayload) -> dict[str, Any]:
        livekit_enforced = None
        if self._rtc_provider() == "livekit":
            livekit_enforced = self._livekit_remove_participant(
                payload.class_id, payload.user_id
            )
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "user_removed",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
            },
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "livekit_enforced": livekit_enforced,
        }

    async def move_breakout_user(self, payload: BreakoutMovePayload) -> dict[str, Any]:
        normalized_room = (payload.room_id or "").strip()
        async with self._lock:
            room = self._class_for(payload.class_id)
            if normalized_room:
                room.breakout_room_by_user[payload.user_id] = normalized_room
            else:
                room.breakout_room_by_user.pop(payload.user_id, None)
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "room_changed",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "room_id": normalized_room or None,
            },
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "room_id": normalized_room or None,
        }

    async def broadcast_breakout_message(
        self, payload: BreakoutBroadcastPayload
    ) -> dict[str, Any]:
        message = payload.message.strip()
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "breakout_broadcast",
                "class_id": payload.class_id,
                "message": message,
            },
        )
        return {"ok": True, "status": "SUCCESS", "message": message}

    async def set_whiteboard_access(
        self, payload: WhiteboardAccessPayload
    ) -> dict[str, Any]:
        async with self._lock:
            room = self._class_for(payload.class_id)
            if payload.enabled:
                room.whiteboard_access_users.add(payload.user_id)
                room.active_whiteboard_user_id = payload.user_id
            else:
                room.whiteboard_access_users.discard(payload.user_id)
                if room.active_whiteboard_user_id == payload.user_id:
                    room.active_whiteboard_user_id = None
        await self._broadcast_events(
            payload.class_id,
            {
                "type": "whiteboard_access_changed",
                "class_id": payload.class_id,
                "user_id": payload.user_id,
                "enabled": payload.enabled,
            },
        )
        return {"ok": True, "status": "SUCCESS", "enabled": payload.enabled}

    async def connect_events(self, websocket: WebSocket, class_id: str) -> None:
        await websocket.accept()
        async with self._lock:
            self._event_sockets.setdefault(class_id, set()).add(websocket)
            snapshot = self._waiting_room_snapshot(self._class_for(class_id))
        await websocket.send_text(json.dumps(snapshot))

    async def disconnect_events(self, websocket: WebSocket, class_id: str) -> None:
        async with self._lock:
            self._event_sockets.get(class_id, set()).discard(websocket)

    async def connect_sync(self, websocket: WebSocket, class_id: str) -> None:
        await websocket.accept()
        async with self._lock:
            self._sync_sockets.setdefault(class_id, set()).add(websocket)

    async def disconnect_sync(self, websocket: WebSocket, class_id: str) -> None:
        async with self._lock:
            self._sync_sockets.get(class_id, set()).discard(websocket)

    async def publish_sync(self, class_id: str, raw_text: str) -> None:
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        payload["class_id"] = payload.get("class_id") or class_id
        sync_payload = json.dumps(payload)
        async with self._lock:
            sockets = list(self._sync_sockets.get(class_id, set()))
            room = self._class_for(class_id)
            event_type = str(payload.get("type") or "")
            target_user = str(payload.get("target_user_id") or "")
            if event_type == "whiteboard_grant" and target_user:
                room.whiteboard_access_users.add(target_user)
                room.active_whiteboard_user_id = target_user
            elif event_type == "whiteboard_revoke" and target_user:
                room.whiteboard_access_users.discard(target_user)
                if room.active_whiteboard_user_id == target_user:
                    room.active_whiteboard_user_id = None
            elif event_type == "whiteboard_clear":
                room.whiteboard_strokes.clear()
            elif event_type == "whiteboard_stroke":
                stroke = payload.get("metadata")
                if isinstance(stroke, dict) and self._is_valid_whiteboard_stroke(stroke):
                    room.whiteboard_strokes.append(stroke)
                    if len(room.whiteboard_strokes) > self._MAX_WHITEBOARD_STROKES:
                        room.whiteboard_strokes = room.whiteboard_strokes[
                            -self._MAX_WHITEBOARD_STROKES :
                        ]
            elif event_type == "approve_mic" and target_user:
                room.muted_users.discard(target_user)
            elif event_type == "participant_camera_disabled" and target_user:
                room.camera_disabled_users.add(target_user)
        await self._broadcast_raw(sockets, sync_payload)

    async def connect_signal(self, websocket: WebSocket) -> None:
        await websocket.accept()

    async def disconnect_signal(self, websocket: WebSocket) -> None:
        async with self._lock:
            for room_id, sockets in self._signal_sockets.items():
                if websocket in sockets:
                    sockets.pop(websocket, None)
                    if not sockets:
                        self._signal_sockets.pop(room_id, None)
                    break

    async def handle_signal_message(self, websocket: WebSocket, raw_text: str) -> None:
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        event_type = str(payload.get("type") or "")
        room = str(payload.get("room") or "")
        user_id = str(payload.get("user_id") or "")
        if event_type == "join" and room and user_id:
            async with self._lock:
                peers = self._signal_sockets.setdefault(room, {})
                peers[websocket] = user_id
                peer_count = len(peers)
                others = [peer for peer in peers if peer is not websocket]
            await websocket.send_text(json.dumps({"type": "ready"}))
            if peer_count > 1:
                await self._broadcast_raw(
                    others,
                    json.dumps(
                        {
                            "type": "peer_joined",
                            "room": room,
                            "user_id": user_id,
                        }
                    ),
                )
            return
        if not room:
            return
        async with self._lock:
            sockets = [
                peer
                for peer in self._signal_sockets.get(room, {})
                if peer is not websocket
            ]
        await self._broadcast_raw(sockets, raw_text)

    async def _broadcast_events(self, class_id: str, payload: dict[str, Any]) -> None:
        message = json.dumps(payload)
        async with self._lock:
            sockets = list(self._event_sockets.get(class_id, set()))
        await self._broadcast_raw(sockets, message)

    async def _broadcast_raw(self, sockets: list[WebSocket], message: str) -> None:
        stale: list[WebSocket] = []
        for socket in sockets:
            try:
                await socket.send_text(message)
            except Exception:
                stale.append(socket)
        if not stale:
            return
        async with self._lock:
            for stale_socket in stale:
                for mapping in (
                    self._event_sockets,
                    self._sync_sockets,
                ):
                    for key, peers in list(mapping.items()):
                        peers.discard(stale_socket)
                        if not peers:
                            mapping.pop(key, None)
                for room_id, peers in list(self._signal_sockets.items()):
                    peers.pop(stale_socket, None)
                    if not peers:
                        self._signal_sockets.pop(room_id, None)

    def _class_for(
        self,
        class_id: str,
        *,
        title: str = "JEE Live Class",
        teacher_name: str = "Dr. A. Sharma",
        subject: str = "General",
        topic: str = "Lecture",
    ) -> LiveClassRecord:
        room = self._classes.get(class_id)
        if room is None:
            room = LiveClassRecord(
                class_id=class_id,
                title=title,
                teacher_name=teacher_name,
                subject=subject,
                topic=topic,
            )
            self._classes[class_id] = room
            return room
        if title and room.title == "JEE Live Class":
            room.title = title
        if teacher_name and room.teacher_name == "Dr. A. Sharma":
            room.teacher_name = teacher_name
        if subject and room.subject == "General":
            room.subject = subject
        if topic and room.topic == "Lecture":
            room.topic = topic
        return room

    def _session_payload(self, room: LiveClassRecord) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "SUCCESS",
            "class_id": room.class_id,
            "title": room.title,
            "teacher_name": room.teacher_name,
            "subject": room.subject,
            "topic": room.topic,
            "is_recording": room.is_recording,
            "meeting_locked": room.meeting_locked,
            "chat_enabled": room.chat_enabled,
            "waiting_room_enabled": room.waiting_room_enabled,
        }

    def _waiting_room_snapshot(self, room: LiveClassRecord) -> dict[str, Any]:
        requests = sorted(
            room.join_requests.values(),
            key=lambda item: item.requested_at,
        )
        return {
            "type": "waiting_room_snapshot",
            "class_id": room.class_id,
            "requests": [
                {
                    "user_id": item.user_id,
                    "user_name": item.user_name,
                    "role": item.role,
                    "request_id": item.request_id,
                    "requested_at": item.requested_at,
                    "camera_enabled": item.camera_enabled,
                    "mic_enabled": item.mic_enabled,
                }
                for item in requests
            ],
        }

    def _is_valid_whiteboard_stroke(self, stroke: dict[str, Any]) -> bool:
        points = stroke.get("points")
        if not isinstance(points, list) or len(points) < 2:
            return False
        for point in points:
            if not isinstance(point, dict):
                return False
            if not isinstance(point.get("x"), (int, float)):
                return False
            if not isinstance(point.get("y"), (int, float)):
                return False
        return isinstance(stroke.get("color"), int) and isinstance(
            stroke.get("width"), (int, float)
        )


_LIVE_HUB = LiveClassHub()


@router.get("/health/ping")
async def live_health_ping() -> dict[str, Any]:
    return {"ok": True, "status": "LIVE_READY", "ts": _utc_now_iso()}


@router.post("/live/token")
async def live_token(req: LiveTokenRequest) -> dict[str, Any]:
    if not req.class_id.strip() or not req.user_id.strip():
        raise HTTPException(status_code=400, detail="class_id and user_id are required")
    return await _LIVE_HUB.issue_live_token(req)


@router.get("/class/session")
async def class_session(
    class_id: str,
    title: str | None = None,
    teacher_name: str | None = None,
    subject: str | None = None,
    topic: str | None = None,
) -> dict[str, Any]:
    return await _LIVE_HUB.session_payload(
        class_id,
        title=title,
        teacher_name=teacher_name,
        subject=subject,
        topic=topic,
    )


@router.get("/class/state")
async def class_state(class_id: str, user_id: str) -> dict[str, Any]:
    return await _LIVE_HUB.class_state_payload(class_id, user_id)


@router.post("/class/join_request")
async def class_join_request(payload: JoinRequestPayload) -> dict[str, Any]:
    return await _LIVE_HUB.request_join(payload)


@router.post("/class/join_cancel")
async def class_join_cancel(payload: JoinCancelPayload) -> dict[str, Any]:
    return await _LIVE_HUB.cancel_join_request(payload)


@router.post("/class/admit")
async def class_admit(payload: AdmitPayload) -> dict[str, Any]:
    return await _LIVE_HUB.approve_join(payload)


@router.post("/class/reject")
async def class_reject(payload: RejectPayload) -> dict[str, Any]:
    return await _LIVE_HUB.reject_join(payload)


@router.post("/class/admit_all")
async def class_admit_all(payload: AdmitAllPayload) -> dict[str, Any]:
    return await _LIVE_HUB.approve_all(payload)


@router.post("/class/fallback_token")
async def class_fallback_token(
    payload: FallbackTokenPayload, request: Request
) -> dict[str, Any]:
    return await _LIVE_HUB.fallback_token(payload, request)


@router.post("/class/lock")
async def class_lock(payload: SetMeetingLockPayload) -> dict[str, Any]:
    return await _LIVE_HUB.set_meeting_lock(payload)


@router.post("/class/chat")
async def class_chat(payload: SetChatEnabledPayload) -> dict[str, Any]:
    return await _LIVE_HUB.set_chat_enabled(payload)


@router.post("/class/waiting_room")
async def class_waiting_room(
    payload: SetWaitingRoomEnabledPayload,
) -> dict[str, Any]:
    return await _LIVE_HUB.set_waiting_room_enabled(payload)


@router.post("/class/recording")
async def class_recording(payload: SetRecordingPayload) -> dict[str, Any]:
    return await _LIVE_HUB.set_recording_enabled(payload)


@router.post("/class/mute")
async def class_mute(payload: MuteUserPayload) -> dict[str, Any]:
    return await _LIVE_HUB.set_user_muted(payload)


@router.post("/class/camera")
async def class_camera(payload: CameraDisablePayload) -> dict[str, Any]:
    return await _LIVE_HUB.set_user_camera_disabled(payload)


@router.post("/class/remove")
async def class_remove(payload: RemoveUserPayload) -> dict[str, Any]:
    return await _LIVE_HUB.remove_user(payload)


@router.post("/class/breakout/move")
async def class_breakout_move(payload: BreakoutMovePayload) -> dict[str, Any]:
    return await _LIVE_HUB.move_breakout_user(payload)


@router.post("/class/breakout/broadcast")
async def class_breakout_broadcast(
    payload: BreakoutBroadcastPayload,
) -> dict[str, Any]:
    return await _LIVE_HUB.broadcast_breakout_message(payload)


@router.post("/class/whiteboard/access")
async def class_whiteboard_access(
    payload: WhiteboardAccessPayload,
) -> dict[str, Any]:
    return await _LIVE_HUB.set_whiteboard_access(payload)


@router.websocket("/class/events")
async def class_events_socket(websocket: WebSocket) -> None:
    class_id = websocket.query_params.get("class_id", "").strip()
    if not class_id:
        await websocket.close(code=1008)
        return
    await _LIVE_HUB.connect_events(websocket, class_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await _LIVE_HUB.disconnect_events(websocket, class_id)


@router.websocket("/class/sync")
async def class_sync_socket(websocket: WebSocket) -> None:
    class_id = websocket.query_params.get("class_id", "").strip()
    if not class_id:
        await websocket.close(code=1008)
        return
    await _LIVE_HUB.connect_sync(websocket, class_id)
    try:
        while True:
            raw = await websocket.receive_text()
            await _LIVE_HUB.publish_sync(class_id, raw)
    except WebSocketDisconnect:
        await _LIVE_HUB.disconnect_sync(websocket, class_id)


@router.websocket("/class/fallback_signal")
async def class_fallback_signal_socket(websocket: WebSocket) -> None:
    await _LIVE_HUB.connect_signal(websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            await _LIVE_HUB.handle_signal_message(websocket, raw)
    except WebSocketDisconnect:
        await _LIVE_HUB.disconnect_signal(websocket)


@router.websocket("/transcription/stream")
async def transcription_stream_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue

            transcript_text = str(payload.get("text") or "").strip()
            if not transcript_text:
                audio_b64 = (
                    payload.get("audio_base64")
                    or payload.get("audio")
                    or payload.get("audio_chunk")
                )
                if audio_b64 and _STT.enabled:
                    content_type = str(
                        payload.get("content_type") or payload.get("mime") or "audio/wav"
                    )
                    try:
                        sample_rate = int(payload.get("sample_rate") or 16000)
                    except (TypeError, ValueError):
                        sample_rate = 16000
                    try:
                        channels = int(payload.get("channels") or 1)
                    except (TypeError, ValueError):
                        channels = 1
                    language_hint = str(
                        payload.get("language_hint")
                        or payload.get("language")
                        or "bn,en"
                    )
                    result = await asyncio.to_thread(
                        _STT.transcribe_base64,
                        str(audio_b64),
                        content_type=content_type,
                        language_hint=language_hint,
                        sample_rate=sample_rate,
                        channels=channels,
                    )
                    transcript_text = str(result.get("text") or "").strip()
                    confidence = result.get("confidence") or 0.0
                else:
                    # No audio or STT is disabled; keep socket open.
                    continue
            else:
                confidence = payload.get("confidence") or 0.9

            await websocket.send_text(
                json.dumps(
                    {
                        "id": payload.get("id") or secrets.token_hex(6),
                        "speaker_id": payload.get("speaker_id") or "speaker",
                        "speaker_name": payload.get("speaker_name") or "Speaker",
                        "text": transcript_text,
                        "timestamp": payload.get("timestamp") or _utc_now_iso(),
                        "confidence": confidence,
                        "source": payload.get("source") or "stt",
                    }
                )
            )
    except WebSocketDisconnect:
        return
