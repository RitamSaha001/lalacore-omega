from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import re
import secrets
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from fastapi.responses import StreamingResponse
import requests

from app.services.bilingual_stt_service import BilingualSttService
from core.network.resilient_http import request_sync

router = APIRouter()
_STT = BilingualSttService()
logger = logging.getLogger(__name__)


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
    whiteboard_surface_style: str = "classic"
    whiteboard_strokes: list[dict[str, Any]] = field(default_factory=list)
    whiteboard_document_pages: list[dict[str, Any]] = field(default_factory=list)
    active_whiteboard_page_id: str | None = None
    whiteboard_lamport_clock: int = 0
    whiteboard_clear_clock: int = 0
    whiteboard_deleted_strokes: dict[str, int] = field(default_factory=dict)
    whiteboard_op_log: list[dict[str, Any]] = field(default_factory=list)
    whiteboard_seen_ops: list[str] = field(default_factory=list)
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


class LiveClassAiRequest(BaseModel):
    prompt: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    instruction: str | None = None
    stream: bool = False


class LiveClassAiSupportRequest(BaseModel):
    prompt: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    atlas_actions: dict[str, Any] = Field(default_factory=dict)


class LiveClassAnalysisRequest(BaseModel):
    context: dict[str, Any] = Field(default_factory=dict)
    instruction: str | None = None
    web_verification: bool = False


class LiveClassQuizRequest(BaseModel):
    context: dict[str, Any] = Field(default_factory=dict)
    instruction: str | None = None
    topic: str | None = None
    difficulty: str | None = None
    question_type: str | None = None
    live_mode: bool = False


class LiveClassAgentRequest(BaseModel):
    instruction: str
    context: dict[str, Any] = Field(default_factory=dict)
    authority_level: str = "assist"


class RecordingTranscribeRequest(BaseModel):
    recording_path: str
    content_type: str | None = None
    language_hint: str = "bn,en"
    sample_rate: int = 16000
    channels: int = 1


class TranscriptWorkerRequest(BaseModel):
    transcript: Any = Field(default_factory=list)


def _s(value: Any) -> str:
    return str(value or "").strip()


def _to_list_str(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_s(item) for item in value if _s(item)]
    text = _s(value)
    if not text:
        return []
    return [item.strip() for item in text.splitlines() if item.strip()]


def _json_candidate(text: str) -> str:
    raw = text.strip()
    if not raw:
        return ""
    fenced = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", raw, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    start_obj = raw.find("{")
    end_obj = raw.rfind("}")
    if start_obj != -1 and end_obj > start_obj:
        return raw[start_obj : end_obj + 1]
    start_arr = raw.find("[")
    end_arr = raw.rfind("]")
    if start_arr != -1 and end_arr > start_arr:
        return raw[start_arr : end_arr + 1]
    return raw


def _decode_json_payload(text: str) -> dict[str, Any] | list[Any] | None:
    candidate = _json_candidate(text)
    if not candidate:
        return None
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, (dict, list)):
        return parsed
    return None


def _extract_live_answer(result: dict[str, Any]) -> tuple[str, str]:
    answer = _s(
        result.get("unsafe_candidate_answer")
        or result.get("final_answer")
        or result.get("answer")
        or result.get("display_answer")
    )
    explanation = _s(result.get("reasoning") or result.get("explanation"))
    return answer, explanation


def _extract_confidence(result: dict[str, Any]) -> float | None:
    calibration = result.get("calibration_metrics")
    raw = None
    if isinstance(calibration, dict):
        raw = calibration.get("confidence_score")
    if raw is None:
        raw = result.get("confidence")
    if isinstance(raw, (int, float)):
        return round(float(raw), 6)
    text = _s(raw)
    if not text:
        return None
    try:
        return round(float(text), 6)
    except ValueError:
        return None


def _extract_concept(result: dict[str, Any]) -> str:
    profile = result.get("profile")
    if isinstance(profile, dict):
        return _s(profile.get("subject") or profile.get("concept"))
    return ""


def _extract_bullets(text: str) -> list[str]:
    bullets: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r"^[\-\*\u2022]+\s*", "", line)
        line = re.sub(r"^\d+[\.\)]\s*", "", line)
        if line and line not in bullets:
            bullets.append(line)
    if bullets:
        return bullets
    sentences = [
        part.strip()
        for part in re.split(r"(?<=[\.\!\?])\s+", text.strip())
        if part.strip()
    ]
    return sentences[:6]


def _build_live_context_sections(context: dict[str, Any]) -> dict[str, list[str]]:
    transcript_items = context.get("transcript")
    transcript_lines: list[str] = []
    timestamps: list[int] = []
    if isinstance(transcript_items, str):
        for raw_line in transcript_items.splitlines():
            line = _s(raw_line)
            if line:
                transcript_lines.append(line)
        if not transcript_lines:
            single_line = _s(transcript_items)
            if single_line:
                transcript_lines.append(single_line)
    elif isinstance(transcript_items, list):
        for row in transcript_items:
            if not isinstance(row, dict):
                continue
            speaker = _s(row.get("speaker")) or "Speaker"
            message = _s(row.get("message"))
            if message:
                transcript_lines.append(f"{speaker}: {message}")
            timestamp = row.get("timestamp_seconds")
            if isinstance(timestamp, int):
                timestamps.append(timestamp)
    if not timestamps:
        timestamps = [
            int(item)
            for item in (context.get("timestamps") or [])
            if isinstance(item, int)
        ]
    class_metadata = (
        context.get("class_metadata")
        if isinstance(context.get("class_metadata"), dict)
        else {}
    )
    if not class_metadata:
        class_metadata = {
            "class_title": _s(context.get("class_title") or context.get("class_name")),
            "subject": _s(context.get("subject")),
            "topic": _s(context.get("topic")),
            "teacher_name": _s(context.get("teacher_name")),
            "current_user_role": _s(context.get("current_user_role") or context.get("role")),
        }
    session_flags = (
        context.get("session_flags")
        if isinstance(context.get("session_flags"), dict)
        else {}
    )
    participant_snapshot = (
        context.get("participant_snapshot")
        if isinstance(context.get("participant_snapshot"), dict)
        else {}
    )
    student_profiles_raw = (
        context.get("student_profiles")
        if isinstance(context.get("student_profiles"), list)
        else []
    )
    autonomy_signals = (
        context.get("autonomy_signals")
        if isinstance(context.get("autonomy_signals"), dict)
        else {}
    )
    class_timeline_raw = (
        context.get("class_timeline")
        if isinstance(context.get("class_timeline"), list)
        else []
    )
    recent_doubts_raw = (
        context.get("recent_doubts") if isinstance(context.get("recent_doubts"), list) else []
    )
    active_poll = (
        context.get("active_poll") if isinstance(context.get("active_poll"), dict) else {}
    )
    active_quiz = (
        context.get("active_quiz") if isinstance(context.get("active_quiz"), dict) else {}
    )
    mastery_snapshot = (
        context.get("mastery_snapshot")
        if isinstance(context.get("mastery_snapshot"), dict)
        else {}
    )
    search_state = (
        context.get("search_state") if isinstance(context.get("search_state"), dict) else {}
    )
    homework_snapshot = (
        context.get("homework_snapshot")
        if isinstance(context.get("homework_snapshot"), dict)
        else {}
    )
    lecture_notes_snapshot = (
        context.get("lecture_notes_snapshot")
        if isinstance(context.get("lecture_notes_snapshot"), dict)
        else {}
    )
    recording_context = (
        context.get("recording_context")
        if isinstance(context.get("recording_context"), dict)
        else {}
    )

    class_summary = [
        line
        for line in [
            f"Class: {_s(class_metadata.get('class_title'))}"
            if _s(class_metadata.get("class_title"))
            else "",
            f"Subject: {_s(class_metadata.get('subject'))}"
            if _s(class_metadata.get("subject"))
            else "",
            f"Topic: {_s(class_metadata.get('topic'))}"
            if _s(class_metadata.get("topic"))
            else "",
            f"Teacher: {_s(class_metadata.get('teacher_name'))}"
            if _s(class_metadata.get("teacher_name"))
            else "",
            f"Current user role: {_s(class_metadata.get('current_user_role'))}"
            if _s(class_metadata.get("current_user_role"))
            else "",
        ]
        if line
    ]
    session_summary = [
        line
        for line in [
            f"Connection lifecycle: {_s(session_flags.get('connection_lifecycle'))}"
            if _s(session_flags.get("connection_lifecycle"))
            else "",
            f"Meeting locked: {_s(session_flags.get('meeting_locked'))}"
            if "meeting_locked" in session_flags
            else "",
            f"Chat enabled: {_s(session_flags.get('chat_enabled'))}"
            if "chat_enabled" in session_flags
            else "",
            f"Waiting room enabled: {_s(session_flags.get('waiting_room_enabled'))}"
            if "waiting_room_enabled" in session_flags
            else "",
            f"Focus mode enabled: {_s(session_flags.get('focus_mode_enabled'))}"
            if "focus_mode_enabled" in session_flags
            else "",
            f"Recording active: {_s(session_flags.get('is_recording'))}"
            if "is_recording" in session_flags
            else "",
            f"Active breakout room: {_s(session_flags.get('active_breakout_room_id'))}"
            if _s(session_flags.get("active_breakout_room_id"))
            else "",
            f"Active panel: {_s(session_flags.get('active_panel'))}"
            if _s(session_flags.get("active_panel"))
            else "",
        ]
        if line
    ]
    participant_summary = [
        line
        for line in [
            f"Participants: {_s(participant_snapshot.get('participant_count'))}"
            if "participant_count" in participant_snapshot
            else "",
            f"Raised hands: {_s(participant_snapshot.get('raised_hands_count'))}"
            if "raised_hands_count" in participant_snapshot
            else "",
            f"Raised hand names: {', '.join(_to_list_str(participant_snapshot.get('raised_hands'))[:5])}"
            if _to_list_str(participant_snapshot.get("raised_hands"))
            else "",
            f"Shared content: {_s(participant_snapshot.get('shared_content_source'))}"
            if _s(participant_snapshot.get("shared_content_source"))
            else "",
            f"Network quality: {_s(participant_snapshot.get('network_quality'))}"
            if _s(participant_snapshot.get("network_quality"))
            else "",
            f"Audio-only mode: {_s(participant_snapshot.get('audio_only_mode_active'))}"
            if "audio_only_mode_active" in participant_snapshot
            else "",
        ]
        if line
    ]
    student_profile_lines: list[str] = []
    for row in student_profiles_raw[:8]:
        if not isinstance(row, dict):
            continue
        name = _s(row.get("name")) or _s(row.get("student_name")) or "Student"
        weak = ", ".join(_to_list_str(row.get("weak_concepts"))[:3])
        flags = ", ".join(_to_list_str(row.get("risk_flags"))[:4])
        engagement = row.get("engagement_score")
        signal = row.get("session_signal_score")
        line_parts = [name]
        if isinstance(engagement, (int, float)):
            line_parts.append(f"engagement={float(engagement):.2f}")
        if isinstance(signal, (int, float)):
            line_parts.append(f"signal={float(signal):.2f}")
        if weak:
            line_parts.append(f"weak={weak}")
        if flags:
            line_parts.append(f"flags={flags}")
        if len(line_parts) > 1:
            student_profile_lines.append(" | ".join(line_parts))
    autonomy_lines = [
        line
        for line in [
            f"Dominant signal: {_s(autonomy_signals.get('dominant_signal'))}"
            if _s(autonomy_signals.get("dominant_signal"))
            else "",
            f"Confusion score: {_s(autonomy_signals.get('confusion_score'))}"
            if "confusion_score" in autonomy_signals
            else "",
            f"Engagement score: {_s(autonomy_signals.get('engagement_score'))}"
            if "engagement_score" in autonomy_signals
            else "",
            f"Queued doubts: {_s(autonomy_signals.get('queued_doubt_count'))}"
            if "queued_doubt_count" in autonomy_signals
            else "",
            f"Waiting room backlog: {_s(autonomy_signals.get('waiting_room_backlog'))}"
            if "waiting_room_backlog" in autonomy_signals
            else "",
            f"At-risk students: {', '.join(_to_list_str(autonomy_signals.get('at_risk_students'))[:4])}"
            if _to_list_str(autonomy_signals.get("at_risk_students"))
            else "",
        ]
        if line
    ]
    timeline_lines: list[str] = []
    for row in class_timeline_raw[:10]:
        if not isinstance(row, dict):
            continue
        label = _s(row.get("label")) or _s(row.get("type")) or "Event"
        detail = _s(row.get("detail")) or _s(row.get("summary"))
        ts = _s(row.get("timestamp")) or _s(row.get("at_seconds"))
        if not (label or detail):
            continue
        if ts:
            timeline_lines.append(f"{ts} • {label}: {detail}".strip(": "))
        else:
            timeline_lines.append(f"{label}: {detail}".strip(": "))
    doubt_lines: list[str] = []
    for row in recent_doubts_raw[:6]:
        if not isinstance(row, dict):
            continue
        question = _s(row.get("question"))
        if not question:
            continue
        status = _s(row.get("status")) or "queued"
        student = _s(row.get("student_name")) or "Student"
        resolution = _s(row.get("teacher_resolution"))
        line = f"{student} [{status}]: {question}"
        if resolution:
            line += f" | resolution: {resolution}"
        doubt_lines.append(line)
    poll_lines = [
        line
        for line in [
            f"Question: {_s(active_poll.get('question'))}"
            if _s(active_poll.get("question"))
            else "",
            f"Options: {', '.join(_to_list_str(active_poll.get('options'))[:4])}"
            if _to_list_str(active_poll.get("options"))
            else "",
            f"Results: {', '.join(_to_list_str(active_poll.get('results'))[:4])}"
            if _to_list_str(active_poll.get("results"))
            else "",
            f"Silent concept check: {_s(active_poll.get('silent_mode'))}"
            if "silent_mode" in active_poll
            else "",
        ]
        if line
    ]
    quiz_lines = [
        line
        for line in [
            f"Question: {_s(active_quiz.get('question'))}"
            if _s(active_quiz.get("question"))
            else "",
            f"Options: {', '.join(_to_list_str(active_quiz.get('options'))[:4])}"
            if _to_list_str(active_quiz.get("options"))
            else "",
            f"Responses: {_s(active_quiz.get('total_responses'))}"
            if "total_responses" in active_quiz
            else "",
            f"Correct responses: {_s(active_quiz.get('correct_responses'))}"
            if "correct_responses" in active_quiz
            else "",
        ]
        if line
    ]
    mastery_lines = [
        line
        for line in [
            f"Weakest concepts: {', '.join(_to_list_str(mastery_snapshot.get('weakest_concepts'))[:4])}"
            if _to_list_str(mastery_snapshot.get("weakest_concepts"))
            else "",
            f"Strongest concepts: {', '.join(_to_list_str(mastery_snapshot.get('strongest_concepts'))[:3])}"
            if _to_list_str(mastery_snapshot.get("strongest_concepts"))
            else "",
            f"Concept summaries: {' | '.join(_to_list_str(mastery_snapshot.get('concept_summaries'))[:4])}"
            if _to_list_str(mastery_snapshot.get("concept_summaries"))
            else "",
            f"Important points: {' | '.join(_to_list_str(mastery_snapshot.get('important_points'))[:4])}"
            if _to_list_str(mastery_snapshot.get("important_points"))
            else "",
            f"Formulas: {' | '.join(_to_list_str(mastery_snapshot.get('formulas'))[:5])}"
            if _to_list_str(mastery_snapshot.get("formulas"))
            else "",
        ]
        if line
    ]
    teacher_signals = _to_list_str(context.get("teacher_insights"))[:8]
    revision_lines: list[str] = []
    revision_raw = context.get("revision_recommendations")
    if isinstance(revision_raw, dict):
        for concept, items in list(revision_raw.items())[:6]:
            recommended = ", ".join(_to_list_str(items)[:3])
            if _s(concept) and recommended:
                revision_lines.append(f"{_s(concept)} -> {recommended}")
    search_lines: list[str] = []
    results_raw = search_state.get("results")
    if isinstance(results_raw, list):
        for row in results_raw[:4]:
            if not isinstance(row, dict):
                continue
            concept = _s(row.get("concept")) or "Result"
            note = _s(row.get("note")) or _s(row.get("formula"))
            if concept or note:
                search_lines.append(f"{concept}: {note}".strip(": "))
    homework_lines: list[str] = []
    for label in ("easy", "medium", "hard"):
        items = _to_list_str(homework_snapshot.get(label))
        if items:
            homework_lines.append(f"{label.title()}: {' | '.join(items[:3])}")
    notes_lines = [
        line
        for line in [
            f"Source summary: {_s(lecture_notes_snapshot.get('source_summary'))}"
            if _s(lecture_notes_snapshot.get("source_summary"))
            else "",
            f"Section topics: {', '.join(_to_list_str(lecture_notes_snapshot.get('section_topics'))[:6])}"
            if _to_list_str(lecture_notes_snapshot.get("section_topics"))
            else "",
            f"Verification: {' | '.join(_to_list_str(lecture_notes_snapshot.get('verification_notes'))[:4])}"
            if _to_list_str(lecture_notes_snapshot.get("verification_notes"))
            else "",
        ]
        if line
    ]
    recording_lines = [
        line
        for line in [
            f"Recording notes: {_s(recording_context.get('recording_notes'))[:280]}"
            if _s(recording_context.get("recording_notes"))
            else "",
            f"Recording job status: {_s(recording_context.get('recording_job_status'))}"
            if _s(recording_context.get("recording_job_status"))
            else "",
            f"AI teaching suggestion: {_s(recording_context.get('ai_teaching_suggestion'))}"
            if _s(recording_context.get("ai_teaching_suggestion"))
            else "",
        ]
        if line
    ]
    lecture_materials = _to_list_str(context.get("lecture_materials"))[:8]
    if not lecture_materials:
        subject_hint = _s(class_metadata.get("subject")) or _s(context.get("subject"))
        if subject_hint:
            lecture_materials = [subject_hint]
    concepts = _to_list_str(context.get("lecture_concepts"))[:12]
    if not concepts:
        topic_hint = _s(class_metadata.get("topic")) or _s(context.get("topic"))
        if topic_hint:
            concepts = [topic_hint]

    return {
        "transcript_lines": transcript_lines[:20],
        "chat_messages": _to_list_str(context.get("chat_messages"))[:12],
        "ocr_snippets": _to_list_str(context.get("ocr_snippets"))[:12],
        "lecture_materials": lecture_materials,
        "concepts": concepts,
        "timestamps": [str(item) for item in timestamps[:12]],
        "class_summary": class_summary[:8],
        "session_summary": session_summary[:10],
        "participant_summary": participant_summary[:8],
        "student_profiles": student_profile_lines[:8],
        "autonomy": autonomy_lines[:6],
        "timeline": timeline_lines[:10],
        "doubts": doubt_lines[:6],
        "poll": poll_lines[:6],
        "quiz": quiz_lines[:6],
        "mastery": mastery_lines[:8],
        "teacher_signals": teacher_signals[:8],
        "revision": revision_lines[:8],
        "search": search_lines[:6],
        "homework": homework_lines[:6],
        "notes": notes_lines[:6],
        "recording": recording_lines[:4],
    }


def _live_context_prompt(context: dict[str, Any], *, compact: bool = False) -> str:
    sections = _build_live_context_sections(context)
    if compact:
        compact_parts: list[str] = ["Live class context:"]
        if sections["class_summary"]:
            compact_parts.append(f"- {sections['class_summary'][0]}")
        if sections["concepts"]:
            compact_parts.append(f"- Concepts: {', '.join(sections['concepts'][:4])}")
        if sections["mastery"]:
            compact_parts.append(f"- Mastery: {sections['mastery'][0]}")
        if sections["student_profiles"]:
            compact_parts.append(f"- Student signal: {sections['student_profiles'][0]}")
        if sections["autonomy"]:
            compact_parts.append(f"- Autonomy: {sections['autonomy'][0]}")
        if sections["doubts"]:
            compact_parts.append(f"- Recent doubt: {sections['doubts'][0]}")
        if sections["poll"]:
            compact_parts.append(f"- Active poll: {sections['poll'][0]}")
        if sections["quiz"]:
            compact_parts.append(f"- Active quiz: {sections['quiz'][0]}")
        if sections["ocr_snippets"]:
            compact_parts.append(f"- Board OCR: {sections['ocr_snippets'][0]}")
        if sections["lecture_materials"]:
            compact_parts.append(
                f"- Lecture material: {sections['lecture_materials'][0]}"
            )
        if sections["transcript_lines"]:
            compact_parts.append(
                f"- Latest teacher line: {sections['transcript_lines'][0]}"
            )
        if sections["teacher_signals"]:
            compact_parts.append(f"- Teacher signal: {sections['teacher_signals'][0]}")
        return "\n".join(compact_parts)
    parts = [
        "Live class context:",
        "Class identity:",
        *(
            [f"- {line}" for line in sections["class_summary"]]
            if sections["class_summary"]
            else ["- none"]
        ),
        "Live session state:",
        *(
            [f"- {line}" for line in sections["session_summary"]]
            if sections["session_summary"]
            else ["- none"]
        ),
        "Participation signals:",
        *(
            [f"- {line}" for line in sections["participant_summary"]]
            if sections["participant_summary"]
            else ["- none"]
        ),
        "Student intelligence signals:",
        *(
            [f"- {line}" for line in sections["student_profiles"]]
            if sections["student_profiles"]
            else ["- none"]
        ),
        "Autonomy signals:",
        *(
            [f"- {line}" for line in sections["autonomy"]]
            if sections["autonomy"]
            else ["- none"]
        ),
        "Transcript:",
        *(
            [f"- {line}" for line in sections["transcript_lines"]]
            if sections["transcript_lines"]
            else ["- none"]
        ),
        "Chat messages:",
        *(
            [f"- {line}" for line in sections["chat_messages"]]
            if sections["chat_messages"]
            else ["- none"]
        ),
        "Board OCR:",
        *(
            [f"- {line}" for line in sections["ocr_snippets"]]
            if sections["ocr_snippets"]
            else ["- none"]
        ),
        "Lecture materials:",
        *(
            [f"- {line}" for line in sections["lecture_materials"]]
            if sections["lecture_materials"]
            else ["- none"]
        ),
        "Detected concepts:",
        *(
            [f"- {line}" for line in sections["concepts"]]
            if sections["concepts"]
            else ["- none"]
        ),
        "Recent doubts:",
        *([f"- {line}" for line in sections["doubts"]] if sections["doubts"] else ["- none"]),
        "Active poll:",
        *([f"- {line}" for line in sections["poll"]] if sections["poll"] else ["- none"]),
        "Active quiz:",
        *([f"- {line}" for line in sections["quiz"]] if sections["quiz"] else ["- none"]),
        "Mastery and concept signals:",
        *(
            [f"- {line}" for line in sections["mastery"]]
            if sections["mastery"]
            else ["- none"]
        ),
        "Teacher insights:",
        *(
            [f"- {line}" for line in sections["teacher_signals"]]
            if sections["teacher_signals"]
            else ["- none"]
        ),
        "Revision recommendations:",
        *(
            [f"- {line}" for line in sections["revision"]]
            if sections["revision"]
            else ["- none"]
        ),
        "Lecture search results:",
        *([f"- {line}" for line in sections["search"]] if sections["search"] else ["- none"]),
        "Study outputs:",
        *(
            [f"- {line}" for line in sections["homework"] + sections["notes"]]
            if sections["homework"] or sections["notes"]
            else ["- none"]
        ),
        "Recording and teaching signals:",
        *(
            [f"- {line}" for line in sections["recording"]]
            if sections["recording"]
            else ["- none"]
        ),
        "Recent class timeline:",
        *(
            [f"- {line}" for line in sections["timeline"]]
            if sections["timeline"]
            else ["- none"]
        ),
    ]
    if sections["timestamps"]:
        parts.extend(
            [
                "Relevant class timestamps (seconds):",
                f"- {', '.join(sections['timestamps'])}",
            ]
        )
    return "\n".join(parts)


def _derive_live_retrieval_prompt(
    *,
    prompt: str,
    context: dict[str, Any],
) -> str:
    prompt_text = _s(prompt)
    if prompt_text:
        normalized = re.sub(r"\s+", " ", prompt_text).strip()
        if len(normalized) <= 320 and not any(
            token in normalized.lower()
            for token in (
                "answer as a live-class",
                "classroom task directive",
                "live class context:",
            )
        ):
            return normalized

    metadata = context.get("class_metadata")
    metadata_map = metadata if isinstance(metadata, dict) else {}
    subject = _s(metadata_map.get("subject"))
    topic = _s(metadata_map.get("topic"))
    class_title = _s(metadata_map.get("class_title"))
    concepts = _to_list_str(context.get("lecture_concepts"))[:4]
    ocr_snippets = _to_list_str(context.get("ocr_snippets"))[:2]

    doubt_prompt = ""
    recent_doubts = context.get("recent_doubts")
    if isinstance(recent_doubts, list):
        for item in recent_doubts:
            if not isinstance(item, dict):
                continue
            doubt_prompt = _s(item.get("question"))
            if doubt_prompt:
                break

    parts = [
        value
        for value in (
            subject,
            topic,
            class_title,
            ", ".join(concepts) if concepts else "",
            doubt_prompt,
            ocr_snippets[0] if ocr_snippets else "",
        )
        if value
    ]
    return " ".join(parts)[:320]


def _result_has_live_evidence(result: dict[str, Any]) -> bool:
    if not isinstance(result.get("citations"), list):
        citations = []
    else:
        citations = result.get("citations") or []
    if citations:
        return True
    if isinstance(result.get("sources_consulted"), list) and result.get("sources_consulted"):
        return True
    web = result.get("web_retrieval")
    if isinstance(web, dict) and isinstance(web.get("matches"), list) and web.get("matches"):
        return True
    return False


def _citation_source_from_url(url: str) -> str:
    host = urlparse(_s(url)).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or "web"


async def _backfill_live_explain_evidence(
    *,
    result: dict[str, Any],
    prompt: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    if _result_has_live_evidence(result):
        logger.info(
            "live_explain_evidence skip_existing duration_s=%.3f",
            time.perf_counter() - started_at,
        )
        return result

    from core.api.entrypoint import _build_citation_map, _build_source_groups
    from services.question_normalizer import QuestionNormalizer
    from services.question_search_engine import QuestionSearchEngine

    subject = _s(
        (
            context.get("class_metadata")
            if isinstance(context.get("class_metadata"), dict)
            else {}
        ).get("subject")
    ).lower()
    scopes = ["general_ai"]
    if subject in {"math", "mathematics", "physics", "chemistry"}:
        scopes.append("pyq")

    primary_query = _derive_live_retrieval_prompt(prompt=prompt, context=context)
    secondary_query = _derive_live_retrieval_prompt(prompt="", context=context)
    query_candidates: list[str] = []
    for query in (primary_query, secondary_query):
        normalized_query = re.sub(r"\s+", " ", _s(query)).strip()
        if not normalized_query:
            continue
        if normalized_query.lower() in {item.lower() for item in query_candidates}:
            continue
        query_candidates.append(normalized_query)

    if not query_candidates:
        return result

    normalizer = QuestionNormalizer()
    search_engine = QuestionSearchEngine()
    warm_started = time.perf_counter()
    await search_engine.warm()
    logger.info(
        "live_explain_evidence warm_done duration_s=%.3f",
        time.perf_counter() - warm_started,
    )

    best: dict[str, Any] | None = None
    best_key: tuple[int, float] = (0, 0.0)
    search_tasks: list[tuple[str, asyncio.Task[dict[str, Any]]]] = []
    for query in query_candidates:
        normalized_question = normalizer.normalize(query)
        search_query = _s(normalized_question.get("search_query"))
        if not search_query:
            continue
        for scope in scopes:
            search_tasks.append(
                (
                    scope,
                    asyncio.create_task(
                        search_engine.search(
                            normalized_question,
                            max_matches=6 if scope == "general_ai" else 5,
                            query_timeout_s=4.8 if scope == "general_ai" else 5.4,
                            search_scope=scope,
                        )
                    ),
                )
            )

    if not search_tasks:
        return result

    try:
        logger.info(
            "live_explain_evidence search_start queries=%s scopes=%s",
            len(query_candidates),
            ",".join(scopes),
        )
        resolved = await asyncio.wait_for(
            asyncio.gather(
                *(task for _, task in search_tasks),
                return_exceptions=True,
            ),
            timeout=8.5,
        )
    except asyncio.TimeoutError:
        for _, task in search_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for _, task in search_tasks),
            return_exceptions=True,
        )
        logger.warning(
            "live_explain_evidence search_timeout total_duration_s=%.3f",
            time.perf_counter() - started_at,
        )
        return result

    for (scope, _task), candidate in zip(search_tasks, resolved):
        if not isinstance(candidate, dict):
            continue
        matches = (
            candidate.get("matches")
            if isinstance(candidate.get("matches"), list)
            else []
        )
        top_similarity = max(
            (
                float(item.get("similarity", 0.0) or 0.0)
                for item in matches
                if isinstance(item, dict)
            ),
            default=0.0,
        )
        ranking_key = (len(matches), top_similarity)
        if ranking_key > best_key:
            best = dict(candidate)
            best["search_scope"] = scope
            best_key = ranking_key

    if not isinstance(best, dict):
        logger.info(
            "live_explain_evidence no_best_candidate total_duration_s=%.3f",
            time.perf_counter() - started_at,
        )
        return result

    matches = best.get("matches") if isinstance(best.get("matches"), list) else []
    if not matches:
        logger.info(
            "live_explain_evidence best_without_matches total_duration_s=%.3f",
            time.perf_counter() - started_at,
        )
        return result

    citations: list[dict[str, Any]] = []
    sources_consulted: list[str] = []
    for row in matches[:5]:
        if not isinstance(row, dict):
            continue
        url = _s(row.get("url"))
        title = _s(row.get("title"))
        if not url or not title:
            continue
        source = _s(row.get("source")) or _citation_source_from_url(url)
        citations.append(
            {
                "title": title,
                "url": url,
                "source": source,
                "snippet": _s(row.get("snippet")),
                "similarity": float(row.get("similarity", 0.0) or 0.0),
            }
        )
        if source and source not in sources_consulted:
            sources_consulted.append(source)
    if not citations:
        logger.info(
            "live_explain_evidence citations_empty total_duration_s=%.3f",
            time.perf_counter() - started_at,
        )
        return result

    merged = dict(result)
    answer, explanation = _extract_live_answer(merged)
    web_retrieval = (
        dict(merged.get("web_retrieval"))
        if isinstance(merged.get("web_retrieval"), dict)
        else {}
    )
    web_retrieval.update(
        {
            "enabled": True,
            "context_injected": bool(web_retrieval.get("context_injected")),
            "query": _s(best.get("query")) or primary_query or secondary_query,
            "query_variants": best.get("query_variants")
            if isinstance(best.get("query_variants"), list)
            else [],
            "matches": matches[:5],
            "cache_hit": bool(best.get("cache_hit")),
            "sources_consulted": sources_consulted,
            "evidence_backfilled": True,
            "search_scope": _s(best.get("search_scope")) or "general_ai",
        }
    )
    merged["web_retrieval"] = web_retrieval
    merged["citations"] = citations
    merged["sources_consulted"] = sources_consulted
    existing_score = merged.get("retrieval_score")
    top_similarity = max(
        (float(row.get("similarity", 0.0) or 0.0) for row in citations),
        default=0.0,
    )
    if not isinstance(existing_score, (int, float)) or float(existing_score) <= 0.0:
        merged["retrieval_score"] = round(top_similarity, 6)
    merged["citation_map"] = _build_citation_map(
        answer_text=answer,
        explanation_text=explanation,
        citations=citations,
    )
    merged["source_groups"] = _build_source_groups(
        citations=citations,
        formulas=[],
        hint="",
        solution_excerpt=explanation,
    )
    logger.info(
        "live_explain_evidence complete citations=%s total_duration_s=%.3f",
        len(citations),
        time.perf_counter() - started_at,
    )
    return merged


def _build_live_reasoning_blocks(
    *,
    task_prompt: str,
    context: dict[str, Any],
    compact_context: bool,
    retrieval_prompt: str,
) -> list[str]:
    blocks: list[str] = []
    live_context = _live_context_prompt(context, compact=compact_context)
    if live_context:
        blocks.append(live_context)
    task_text = _s(task_prompt)
    retrieval_text = _s(retrieval_prompt)
    if task_text and task_text != retrieval_text:
        blocks.append(f"CLASSROOM TASK DIRECTIVE:\n{task_text}")
    return blocks


def _derive_live_student_profile(context: dict[str, Any]) -> dict[str, Any]:
    class_metadata = (
        context.get("class_metadata")
        if isinstance(context.get("class_metadata"), dict)
        else {}
    )
    mastery_snapshot = (
        context.get("mastery_snapshot")
        if isinstance(context.get("mastery_snapshot"), dict)
        else {}
    )
    recent_doubts = (
        context.get("recent_doubts") if isinstance(context.get("recent_doubts"), list) else []
    )
    weak_concepts = _to_list_str(mastery_snapshot.get("weakest_concepts"))[:5]
    strong_concepts = _to_list_str(mastery_snapshot.get("strongest_concepts"))[:4]
    recent_doubt_topics = [
        _s(row.get("question"))
        for row in recent_doubts[:6]
        if isinstance(row, dict) and _s(row.get("question"))
    ]
    role = _s(class_metadata.get("current_user_role")).lower()
    preferred_style = _s(mastery_snapshot.get("preferred_style"))
    if not preferred_style:
        preferred_style = (
            "teacher_copilot"
            if role == "teacher"
            else ("example_driven_teaching" if weak_concepts else "concise_exam_focused")
        )
    return {
        "account_id": _s(class_metadata.get("current_user_id") or class_metadata.get("user_id")),
        "weak_concepts": weak_concepts,
        "strong_concepts": strong_concepts,
        "recent_doubt_topics": recent_doubt_topics[:5],
        "preferred_style": preferred_style,
        "explanation_depth": "deep" if weak_concepts else "medium",
    }


def _atlas_route_metadata(result: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key in (
        "student_profile",
        "atlas_actions",
        "source_groups",
        "verification",
    ):
        value = result.get(key)
        if isinstance(value, dict):
            payload[key] = value
    for key in ("steps", "concepts", "citations"):
        value = result.get(key)
        if isinstance(value, list):
            payload[key] = value
    retrieval_score = result.get("retrieval_score")
    if isinstance(retrieval_score, (int, float)):
        payload["retrieval_score"] = float(retrieval_score)
    risk = _s(result.get("risk"))
    if risk:
        payload["risk"] = risk
    return payload


def _atlas_actions_triggered(value: Any) -> bool:
    return isinstance(value, dict) and bool(value.get("triggered"))


def _should_enable_live_agent_web(instruction: str) -> bool:
    lowered = instruction.lower()
    return any(
        token in lowered
        for token in (
            "pyq",
            "previous year",
            "web",
            "search",
            "question",
            "poll",
            "difficult",
            "insert",
            "example",
            "citation",
            "source",
            "resource pack",
            "handout",
            "worksheet",
        )
    )


def _agent_authority_requires_confirmation(level: str) -> bool:
    normalized = level.strip().lower()
    return normalized in {"assist", "semi_auto", "semi-auto", "semi auto"}


def _coerce_agent_actions(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    actions: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        tool = _s(item.get("tool")).lower()
        if not tool:
            continue
        args = item.get("args") if isinstance(item.get("args"), dict) else {}
        depends_on = (
            [entry for entry in (_s(value) for value in item.get("depends_on", []))]
            if isinstance(item.get("depends_on"), list)
            else []
        )
        on_failure = (
            dict(item.get("on_failure")) if isinstance(item.get("on_failure"), dict) else {}
        )
        actions.append(
            {
                "id": _s(item.get("id")) or f"action_{index + 1}",
                "tool": tool,
                "title": _s(item.get("title")) or tool.replace("_", " ").title(),
                "detail": _s(item.get("detail")),
                "risk": _s(item.get("risk")).lower() or "low",
                "requires_confirmation": bool(item.get("requires_confirmation")),
                "args": dict(args),
                "depends_on": [entry for entry in depends_on if entry],
                "on_failure": {
                    "strategy": _s(on_failure.get("strategy")).lower(),
                    **(
                        {"fallback_tool": _s(on_failure.get("fallback_tool")).lower()}
                        if _s(on_failure.get("fallback_tool"))
                        else {}
                    ),
                },
            }
        )
    return actions


def _agent_plan_type(payload: dict[str, Any], *, follow_up_questions: list[str]) -> str:
    raw_type = _s(payload.get("type")).lower()
    if raw_type in {"single_action", "multi_step_plan", "needs_more_info", "clarification_request"}:
        return raw_type
    if follow_up_questions or bool(payload.get("needs_more_info")):
        return "needs_more_info"
    if _s(payload.get("tool")):
        return "single_action"
    return "multi_step_plan"


def _agent_plan_id(payload: dict[str, Any], *, plan_type: str) -> str:
    existing = _s(payload.get("plan_id"))
    if existing:
        return existing
    prefix = "atlas_single" if plan_type == "single_action" else "atlas_plan"
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _agent_plan_goal(payload: dict[str, Any], *, instruction: str) -> str:
    return _s(payload.get("goal")) or instruction


def _normalize_agent_text_list(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [item.strip() for item in (str(value) for value in raw) if item.strip()]


_LIVE_AGENT_EXPLANATION_TOOL_PHRASES: dict[str, tuple[str, ...]] = {
    "approve_waiting_all": (
        "admit all waiting students",
        "admit all waiting",
        "admit everyone in the waiting room",
    ),
    "mute_all": ("mute all",),
    "draw_text_on_whiteboard": (
        "write on the board",
        "write on the whiteboard",
        "write binomial theorem",
    ),
}

_LIVE_AGENT_TOOL_SIGNAL_PHRASES: dict[str, tuple[str, ...]] = {
    "approve_waiting_all": (
        "admit everyone",
        "let everyone in",
        "bring everyone in",
        "let them all in",
        "allow everyone in",
    ),
    "mute_all": (
        "mute everyone",
        "quiet the room",
        "silence the room",
        "make everyone quiet",
    ),
    "draw_text_on_whiteboard": (
        "open whiteboard and write",
        "put this on the board",
        "put it on the board",
        "give heading",
        "write heading",
        "put a heading",
        "write on the board",
    ),
    "start_screen_share": (
        "share my screen",
        "start screen share",
        "show my screen",
    ),
    "set_reminder": (
        "remind me",
        "set a reminder",
        "ping me",
    ),
    "schedule_next_class": (
        "schedule the next class",
        "set up the next class",
        "line up the next class",
        "lock in the next class",
        "put the next class on the calendar",
    ),
    "schedule_class_reminder": (
        "remind students before class",
        "send a reminder before class",
        "ping students before class",
        "remind me before the class",
    ),
    "announce_to_class": (
        "announce it to students",
        "tell the class",
        "inform the students",
        "announce it",
    ),
    "create_poll": (
        "give them a poll",
        "run a poll",
        "make a poll",
    ),
    "create_homework_assignment": (
        "give homework",
        "assign homework",
        "set homework",
    ),
    "create_exam_assignment": (
        "make an exam",
        "set an exam",
        "assign a test",
        "give an exam",
    ),
    "create_revision_pack": (
        "revision pack",
        "revise this topic",
    ),
    "report_system_issue": (
        "not working",
        "lagging",
        "broken",
        "crashing",
        "ai is not working",
        "replay is not working",
        "video is blurry",
        "video blurry",
        "sound quality is bad",
        "audio quality is bad",
        "voice is breaking",
        "cannot hear properly",
        "can't hear properly",
        "video is freezing",
        "network issue",
    ),
}


def _normalize_live_instruction_text(text: str) -> str:
    lowered = _s(text).lower()
    lowered = re.sub(
        r"\b(?:atlas|please|can you|could you|would you|will you|just|kindly|help me|help us)\b",
        " ",
        lowered,
        flags=re.IGNORECASE,
    )
    lowered = lowered.replace("what's", "what is")
    lowered = re.sub(r"[\"`]+", "", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip(" .,!?\n\t")


def _live_instruction_has_time_hint(text: str) -> bool:
    lowered = _normalize_live_instruction_text(text)
    return any(
        token in lowered
        for token in (
            "today",
            "tomorrow",
            "next week",
            "same time",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "am",
            "pm",
            ":",
        )
    )


def _extract_live_whiteboard_heading(instruction: str) -> str:
    raw = _s(instruction).strip()
    patterns = (
        r"give\s+(?:the\s+)?heading\s+(.+?)(?:[,.;]|$)",
        r"write\s+(.+?)\s+on\s+the\s+(?:white)?board",
        r"put\s+(.+?)\s+on\s+the\s+(?:white)?board",
    )
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        text = re.sub(r"\s+", " ", match.group(1)).strip(" .")
        if text:
            return text
    if "binomial theorem" in raw.lower():
        return "Binomial Theorem"
    return "Class Heading"


def _extract_live_reminder_delay(instruction: str) -> dict[str, Any]:
    lowered = _normalize_live_instruction_text(instruction)
    hour_match = re.search(r"\b(\d+)\s+hours?\b", lowered)
    minute_match = re.search(r"\b(\d+)\s+minutes?\b", lowered)
    if hour_match:
        try:
            minutes = int(hour_match.group(1)) * 60
            return {"delay_minutes": minutes}
        except ValueError:
            return {}
    if minute_match:
        try:
            minutes = int(minute_match.group(1))
            return {"delay_minutes": minutes}
        except ValueError:
            return {}
    return {}


def _build_live_instruction_signals(
    instruction: str,
    *,
    context: dict[str, Any],
) -> dict[str, Any]:
    normalized = _normalize_live_instruction_text(instruction)
    sequence_cues = [
        cue
        for cue in ("first", "then", "after", "before", "next", "finally")
        if cue in normalized
    ]
    action_modes: list[str] = []
    if any(token in normalized for token in ("admit", "let", "bring", "mute", "quiet", "screen", "whiteboard", "board")):
        action_modes.append("classroom_control")
    if any(token in normalized for token in ("schedule", "class", "calendar", "next week", "recurring")):
        action_modes.append("schedule")
    if any(token in normalized for token in ("remind", "ping")):
        action_modes.append("reminder")
    if any(token in normalized for token in ("poll", "quiz", "rapid fire")):
        action_modes.append("assessment")
    if any(token in normalized for token in ("study", "resource", "homework", "exam")):
        action_modes.append("followup")
    if any(token in normalized for token in ("not working", "broken", "lagging", "crashing", "slow")):
        action_modes.append("diagnose")
    score_map: dict[str, int] = {}
    first_index: dict[str, int] = {}
    for tool, phrases in _LIVE_AGENT_TOOL_SIGNAL_PHRASES.items():
        for phrase in phrases:
            idx = normalized.find(phrase)
            if idx == -1:
                continue
            score_map[tool] = score_map.get(tool, 0) + max(2, len(phrase.split()))
            first_index[tool] = min(first_index.get(tool, idx), idx)
    if any(token in normalized for token in (" on the board", " on the whiteboard", "give heading", "write heading")):
        idx = normalized.find(" on the board")
        if idx == -1:
            idx = normalized.find(" on the whiteboard")
        if idx == -1:
            idx = normalized.find("heading")
        score_map["draw_text_on_whiteboard"] = score_map.get("draw_text_on_whiteboard", 0) + 5
        first_index["draw_text_on_whiteboard"] = min(
            first_index.get("draw_text_on_whiteboard", idx if idx != -1 else 10**9),
            idx if idx != -1 else 10**9,
        )
    if "report_system_issue" in score_map or "diagnose" in action_modes:
        score_map["report_system_issue"] = score_map.get("report_system_issue", 0) + 5
        first_index.setdefault("report_system_issue", 0)
    candidate_tools = [
        tool
        for tool, _score in sorted(
            score_map.items(),
            key=lambda item: (-item[1], first_index.get(item[0], 10**9), item[0]),
        )
    ][:8]
    candidate_tools_in_order = [
        tool
        for tool, _idx in sorted(
            first_index.items(),
            key=lambda item: (item[1], -score_map.get(item[0], 0), item[0]),
        )
    ][:8]
    missing_detail_hints: list[str] = []
    if "schedule" in action_modes and not _live_instruction_has_time_hint(instruction):
        missing_detail_hints.append("time_reference")
    if "draw_text_on_whiteboard" in candidate_tools and not _extract_live_whiteboard_heading(instruction):
        missing_detail_hints.append("board_heading")
    return {
        "normalized_instruction": normalized,
        "action_modes": action_modes,
        "sequence_cues": sequence_cues,
        "candidate_tools": candidate_tools,
        "candidate_tools_in_order": candidate_tools_in_order,
        "whiteboard_heading": _extract_live_whiteboard_heading(instruction),
        "reminder_delay": _extract_live_reminder_delay(instruction),
        "missing_detail_hints": missing_detail_hints[:4],
    }


def _live_instruction_signal_prompt(signals: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"- Normalized intent: {_s(signals.get('normalized_instruction')) or 'unknown'}",
            f"- Candidate tools by human-language fit: {', '.join(signals.get('candidate_tools') or []) or 'none'}",
            f"- Action modes: {', '.join(signals.get('action_modes') or []) or 'none'}",
            f"- Sequence cues: {', '.join(signals.get('sequence_cues') or []) or 'none'}",
            f"- Whiteboard heading hint: {_s(signals.get('whiteboard_heading')) or 'none'}",
            f"- Reminder delay hint: {json.dumps(signals.get('reminder_delay') or {}, ensure_ascii=True)}",
            f"- Missing detail hints: {', '.join(signals.get('missing_detail_hints') or []) or 'none'}",
        ]
    )


def _extract_agent_tool_mentions(text: str) -> list[str]:
    lowered = _s(text).lower()
    if not lowered:
        return []
    matches: list[tuple[int, int, str]] = []
    phrase_map = {
        **_LIVE_AGENT_TOOL_SIGNAL_PHRASES,
        **_LIVE_AGENT_EXPLANATION_TOOL_PHRASES,
    }
    for tool, phrases in phrase_map.items():
        tool_index = lowered.find(f"`{tool}`")
        if tool_index == -1:
            tool_index = lowered.find(tool)
        if tool_index == -1:
            for phrase in phrases:
                phrase_index = lowered.find(phrase)
                if phrase_index != -1:
                    tool_index = phrase_index
                    break
        if tool_index == -1:
            continue
        matches.append((tool_index, -len(tool), tool))
    matches.sort()
    ordered: list[str] = []
    seen: set[str] = set()
    for _, _, tool in matches:
        if tool in seen:
            continue
        seen.add(tool)
        ordered.append(tool)
    return ordered[:4]


def _recover_agent_actions_from_explanation(
    *,
    explanation: str,
    instruction: str,
) -> list[dict[str, Any]]:
    tools = _extract_agent_tool_mentions(explanation)
    if not tools:
        return []
    lowered_instruction = _s(instruction).lower()
    actions: list[dict[str, Any]] = []
    previous_id: str | None = None
    for index, tool in enumerate(tools, start=1):
        args: dict[str, Any] = {}
        if tool == "draw_text_on_whiteboard":
            text_match = re.search(
                r"write\s+(.+?)\s+on\s+the\s+(?:white)?board",
                lowered_instruction,
            )
            if text_match:
                args["text"] = text_match.group(1).strip().title()
            else:
                args["text"] = _extract_live_whiteboard_heading(instruction)
        action_id = f"step_{index}"
        actions.append(
            {
                "id": action_id,
                "tool": tool,
                "title": tool.replace("_", " ").title(),
                "detail": _s(explanation),
                "risk": "low",
                "requires_confirmation": False,
                "args": args,
                "depends_on": [previous_id] if previous_id else [],
                "on_failure": {"strategy": "retry"},
            }
        )
        previous_id = action_id
    return actions


def _synthesized_live_action(
    tool: str,
    *,
    instruction: str,
    step_id: str,
    depends_on: list[str] | None = None,
    signals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    args: dict[str, Any] = {}
    if tool == "draw_text_on_whiteboard":
        args["text"] = _s((signals or {}).get("whiteboard_heading")) or _extract_live_whiteboard_heading(instruction)
    elif tool == "set_reminder":
        args = {
            "note": "End class reminder",
            **dict((signals or {}).get("reminder_delay") or {}),
        }
    elif tool == "report_system_issue":
        args = {"issue_summary": instruction}
    return {
        "id": step_id,
        "tool": tool,
        "title": tool.replace("_", " ").title(),
        "detail": _extract_bullets(instruction)[0] if _extract_bullets(instruction) else instruction,
        "risk": "low",
        "requires_confirmation": False,
        "args": args,
        "depends_on": list(depends_on or []),
        "on_failure": {"strategy": "retry"},
    }


def _synthesize_live_plan_from_signals(
    *,
    instruction: str,
    signals: dict[str, Any],
) -> dict[str, Any] | None:
    candidate_tools = list(signals.get("candidate_tools") or [])
    if not candidate_tools:
        return None
    normalized = _s(signals.get("normalized_instruction"))
    top_tool = candidate_tools[0]
    if top_tool == "report_system_issue":
        return {
            "type": "single_action",
            "goal": instruction,
            "summary": "Atlas inferred this is a live-class troubleshooting request.",
            "teacher_notice": "Atlas matched the issue-reporting intent from your natural wording.",
            "tool": "report_system_issue",
            "title": "Report system issue",
            "detail": instruction,
            "risk": "low",
            "args": {"issue_summary": instruction},
            "recovery_mode": "instruction_signals",
        }
    ordered_tools = [
        tool
        for tool in (signals.get("candidate_tools_in_order") or candidate_tools)
        if tool in {
            "approve_waiting_all",
            "mute_all",
            "start_screen_share",
            "draw_text_on_whiteboard",
            "set_reminder",
        }
    ]
    if ordered_tools:
        actions: list[dict[str, Any]] = []
        previous_id: str | None = None
        for index, tool in enumerate(ordered_tools[:4], start=1):
            if tool == "set_reminder" and not dict(signals.get("reminder_delay") or {}):
                continue
            if tool == "draw_text_on_whiteboard" and not _s(signals.get("whiteboard_heading")):
                continue
            action = _synthesized_live_action(
                tool,
                instruction=instruction,
                step_id=f"step_{index}",
                depends_on=[previous_id] if previous_id else [],
                signals=signals,
            )
            actions.append(action)
            previous_id = action["id"]
        if actions:
            return {
                "type": "single_action" if len(actions) == 1 else "multi_step_plan",
                "goal": instruction,
                "summary": "Atlas inferred the live-class sequence from the teacher's natural wording.",
                "teacher_notice": "Atlas matched the classroom-control flow from your phrasing.",
                "actions": actions,
                "steps": actions,
                "proposed_tools": [action["tool"] for action in actions],
                "recovery_mode": "instruction_signals",
            }
    if "schedule_next_class" in candidate_tools and "time_reference" in (signals.get("missing_detail_hints") or []):
        return {
            "type": "needs_more_info",
            "goal": instruction,
            "summary": "Atlas understood this as a scheduling request but still needs the timing details.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "When should I schedule the class? You can say something like next Tuesday 6 PM.",
            ],
            "proposed_tools": ["schedule_next_class"],
            "actions": [],
            "recovery_mode": "instruction_signals",
        }
    if "create_homework_assignment" in candidate_tools:
        return {
            "type": "needs_more_info",
            "goal": instruction,
            "summary": "Atlas understood that you want a homework workflow, but it still needs the chapter or topic.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which chapter or topic should the homework cover?",
            ],
            "proposed_tools": ["create_homework_assignment"],
            "actions": [],
            "recovery_mode": "instruction_signals",
        }
    return None


def _fallback_agent_follow_up_plan(
    instruction: str,
    *,
    signals: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    lowered = _s((signals or {}).get("normalized_instruction")) or instruction.lower()
    if "free slot" in lowered or "free time" in lowered or "when am i free" in lowered:
        return {
            "summary": "Atlas can scan the live-class calendar and rank the best free slots, but it still needs a search window.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which date range should I scan for free slots?",
                "How long should the class be?",
            ],
            "proposed_tools": ["get_free_slots"],
            "actions": [],
        }
    if ("edit" in lowered or "update" in lowered) and ("recurring" in lowered or "series" in lowered):
        return {
            "summary": "Atlas can edit the recurring plan once you identify the series and the change you want.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which recurring plan should I edit?",
                "What should change: title, time, duration, reminders, or topic?",
            ],
            "proposed_tools": ["edit_recurring_plan"],
            "actions": [],
        }
    if ("shift" in lowered or "move all" in lowered) and ("series" in lowered or "recurring" in lowered or "classes" in lowered):
        return {
            "summary": "Atlas can shift the recurring series once it knows which plan to move and the new target slot.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which recurring plan should I shift?",
                "What new time or weekday should future classes move to?",
            ],
            "proposed_tools": ["shift_series"],
            "actions": [],
        }
    if ("pause" in lowered or "hold" in lowered) and ("series" in lowered or "recurring" in lowered or "classes" in lowered):
        return {
            "summary": "Atlas can pause a recurring plan once it knows which series and how long the pause should last.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which recurring plan should I pause?",
                "How long should the pause last: days, weeks, or until a specific date?",
            ],
            "proposed_tools": ["pause_series"],
            "actions": [],
        }
    if ("cancel next class" in lowered or "delete occurrence" in lowered or "cancel one class" in lowered):
        return {
            "summary": "Atlas can cancel a single occurrence once it knows which scheduled class to remove.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which single scheduled class or occurrence should I cancel?",
            ],
            "proposed_tools": ["delete_occurrence"],
            "actions": [],
        }
    if (
        "recurring" in lowered
        or "repeat" in lowered
        or "every " in lowered
        or "same time every week" in lowered
    ) and "class" in lowered:
        return {
            "summary": "Atlas can build a recurring class plan, but it still needs the first class time and repeat pattern.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "When should the first class happen? Please share a date and time.",
                "How should it repeat: daily, weekly, or on specific weekdays?",
                "How many sessions should I schedule in this recurring plan?",
            ],
            "proposed_tools": ["create_recurring_class_plan"],
            "actions": [],
        }
    if ("remind" in lowered or "reminder" in lowered or "ping" in lowered) and ("class" in lowered or "session" in lowered):
        return {
            "summary": "Atlas can attach schedule reminders, but it still needs the class target and reminder timing.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which scheduled class should get the reminder?",
                "How long before class should I remind everyone? You can give one or more reminder windows.",
            ],
            "proposed_tools": ["schedule_class_reminder"],
            "actions": [],
        }
    if ("conflict" in lowered or "overlap" in lowered or "calendar" in lowered) and (
        "class" in lowered or "schedule" in lowered
    ):
        return {
            "summary": "Atlas can check the live-class calendar for conflicts before it schedules anything.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which date and time should I check for a class conflict?",
                "How long will that class run?",
            ],
            "proposed_tools": ["check_schedule_conflicts"],
            "actions": [],
        }
    if (
        ("schedule" in lowered and "class" in lowered)
        or "line up the next class" in lowered
        or "lock in the next class" in lowered
        or "put the next class on the calendar" in lowered
    ):
        return {
            "summary": "Atlas can schedule the next class, but it still needs a few details before it acts.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "When should I schedule the class? Please share a date and time.",
                "What title should students see? I can reuse the current class topic if you prefer.",
            ],
            "proposed_tools": ["schedule_next_class"],
            "actions": [],
        }
    if "reschedule" in lowered and "class" in lowered:
        return {
            "summary": "Atlas can reschedule a class once you identify which class and the new time.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which scheduled class should I move?",
                "What is the new date and time?",
            ],
            "proposed_tools": ["reschedule_scheduled_class"],
            "actions": [],
        }
    if ("cancel" in lowered or "drop" in lowered) and "class" in lowered:
        return {
            "summary": "Atlas can cancel a scheduled class after you identify which one to cancel.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which scheduled class should I cancel?",
            ],
            "proposed_tools": ["cancel_scheduled_class"],
            "actions": [],
        }
    if ("resource pack" in lowered or "follow-up resources" in lowered) and (
        "study" in lowered or "students" in lowered or "class" in lowered
    ):
        return {
            "summary": "Atlas can publish a follow-up resource pack after class once it has the key links or the source topic.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Should I use specific links, or should I build the pack from the current topic and PYQ context?",
                "Do you want Atlas to announce the pack to the class after publishing it?",
            ],
            "proposed_tools": ["publish_followup_resource_pack"],
            "actions": [],
        }
    if ("resource" in lowered or "link" in lowered) and "study" in lowered:
        return {
            "summary": "Atlas can publish the resource into Study once it has the link.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which URL or resource link should I publish to Study?",
            ],
            "proposed_tools": ["publish_resource_link_to_study"],
            "actions": [],
        }
    if "bundle" in lowered and ("homework" in lowered or "exam" in lowered):
        bundle_tool = (
            "create_post_class_homework_bundle"
            if "homework" in lowered
            else "create_post_class_exam_bundle"
        )
        return {
            "summary": "Atlas can build the post-class bundle, but a deadline or publish preference may still be helpful.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "What deadline should I use for this bundle?",
                "Should Atlas announce it to the class after publishing?",
            ],
            "proposed_tools": [bundle_tool],
            "actions": [],
        }
    if "revision pack" in lowered:
        return {
            "summary": "Atlas can build a revision pack, but it still needs the chapter focus and whether to include an auto-scheduled test.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which chapter or topic should the revision pack cover?",
                "Should Atlas also schedule a test or homework follow-up after the pack is published?",
            ],
            "proposed_tools": ["create_revision_pack"],
            "actions": [],
        }
    if "test pack" in lowered:
        return {
            "summary": "Atlas can build a structured test pack, but it still needs the topic scope and assessment style.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which chapter or topic should the test pack cover?",
                "Should the pack end with a homework or an exam-style assessment?",
            ],
            "proposed_tools": ["create_test_pack"],
            "actions": [],
        }
    if "crash course pack" in lowered:
        return {
            "summary": "Atlas can build a crash-course pack once it knows the topic scope and timeline.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which topic should the crash-course pack cover?",
                "How quickly should students finish it?",
            ],
            "proposed_tools": ["create_crash_course_pack"],
            "actions": [],
        }
    if ("weak topic" in lowered or "recovery pack" in lowered):
        return {
            "summary": "Atlas can build a weak-topic recovery pack, but it still needs the target weakness or student group.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which weak topic or student weakness should the recovery pack focus on?",
                "Do you want homework, flashcards, or both inside the pack?",
            ],
            "proposed_tools": ["create_weak_topic_recovery_pack"],
            "actions": [],
        }
    if "homework" in lowered and "bundle" not in lowered:
        return {
            "summary": "Atlas can create the homework and publish it straight to the student dashboard, but it still needs a few teaching choices.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which chapter or topic should the homework cover?",
                "What deadline should I use?",
                "How difficult should it be, and roughly how many marks or questions do you want?",
            ],
            "proposed_tools": ["create_homework_assignment"],
            "actions": [],
        }
    if "exam" in lowered and "bundle" not in lowered:
        return {
            "summary": "Atlas can create the exam and publish it straight to the student dashboard, but it still needs the assessment settings.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue the plan.",
            "requires_confirmation": False,
            "needs_more_info": True,
            "follow_up_questions": [
                "Which chapter or topic should the exam cover?",
                "When should students take it or when is the deadline?",
                "How difficult should it be, and roughly how many marks or questions do you want?",
            ],
            "proposed_tools": ["create_exam_assignment"],
            "actions": [],
        }
    return None


def _agent_plan_from_result(
    *,
    result: dict[str, Any],
    instruction: str,
    authority_level: str,
    instruction_signals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    answer, explanation = _extract_live_answer(result)
    payload = _decode_json_payload(answer) or _decode_json_payload(explanation)
    if not isinstance(payload, dict):
        payload = {}
    follow_up_questions = _normalize_agent_text_list(payload.get("follow_up_questions"))
    proposed_tools = _normalize_agent_text_list(payload.get("proposed_tools"))
    plan_type = _agent_plan_type(payload, follow_up_questions=follow_up_questions)
    if plan_type == "single_action":
        raw_steps: Any = [payload]
    elif plan_type == "multi_step_plan":
        raw_steps = payload.get("steps", payload.get("actions"))
    else:
        raw_steps = []
    actions = _coerce_agent_actions(raw_steps)
    requires_confirmation = bool(payload.get("requires_confirmation"))
    needs_more_info = plan_type in {"needs_more_info", "clarification_request"} or bool(
        payload.get("needs_more_info")
    )
    if not actions and explanation:
        recovered_actions = _recover_agent_actions_from_explanation(
            explanation=explanation,
            instruction=instruction,
        )
        if recovered_actions:
            actions = recovered_actions
            payload.setdefault(
                "type",
                "single_action" if len(actions) == 1 else "multi_step_plan",
            )
            payload.setdefault("goal", instruction)
            payload.setdefault("summary", _extract_bullets(explanation)[0] if _extract_bullets(explanation) else instruction)
            payload["recovery_mode"] = "tool_mentions_from_reasoning"
            plan_type = _agent_plan_type(payload, follow_up_questions=follow_up_questions)
    if not actions and not (needs_more_info or follow_up_questions):
        synthesized = _synthesize_live_plan_from_signals(
            instruction=instruction,
            signals=instruction_signals or {},
        )
        if isinstance(synthesized, dict):
            payload = synthesized
            follow_up_questions = _normalize_agent_text_list(payload.get("follow_up_questions"))
            proposed_tools = _normalize_agent_text_list(payload.get("proposed_tools"))
            plan_type = _agent_plan_type(payload, follow_up_questions=follow_up_questions)
            if plan_type == "single_action":
                raw_steps = [payload]
            elif plan_type == "multi_step_plan":
                raw_steps = payload.get("steps", payload.get("actions"))
            else:
                raw_steps = []
            actions = _coerce_agent_actions(raw_steps)
            needs_more_info = plan_type in {"needs_more_info", "clarification_request"} or bool(
                payload.get("needs_more_info")
            )
    if not actions and not (needs_more_info or follow_up_questions):
        fallback = _fallback_agent_follow_up_plan(
            instruction,
            signals=instruction_signals,
        )
        if fallback is not None:
            payload = fallback
            plan_type = "needs_more_info"
            actions = []
            needs_more_info = True
            follow_up_questions = _normalize_agent_text_list(
                fallback.get("follow_up_questions")
            )
            proposed_tools = _normalize_agent_text_list(fallback.get("proposed_tools"))
        else:
            raise HTTPException(
                status_code=502,
                detail="Agent planner returned no actions",
            )
    if _agent_authority_requires_confirmation(authority_level):
        requires_confirmation = True
    for action in actions:
        if action["risk"] == "high":
            action["requires_confirmation"] = True
            requires_confirmation = True
    if not proposed_tools and actions:
        proposed_tools = list(dict.fromkeys(action["tool"] for action in actions if action["tool"]))
    goal = _agent_plan_goal(payload, instruction=instruction)
    plan_id = _agent_plan_id(payload, plan_type=plan_type)
    response: dict[str, Any] = {
        "type": plan_type,
        "goal": goal,
        "plan_id": plan_id,
        "instruction": instruction,
        "summary": _s(payload.get("summary")) or goal or answer or explanation,
        "teacher_notice": _s(payload.get("teacher_notice")),
        "requires_confirmation": requires_confirmation,
        "needs_more_info": needs_more_info or bool(follow_up_questions),
        "follow_up_questions": follow_up_questions,
        "proposed_tools": proposed_tools,
        "actions": actions,
        "recovery_mode": _s(payload.get("recovery_mode")),
    }
    if plan_type == "single_action" and actions:
        response["tool"] = actions[0]["tool"]
        response["args"] = actions[0]["args"]
    elif plan_type == "multi_step_plan":
        response["steps"] = actions
    if isinstance(result.get("web_retrieval"), dict):
        response["web_retrieval"] = result.get("web_retrieval")
    if isinstance(result.get("citations"), list):
        response["citations"] = result.get("citations")
    if isinstance(result.get("sources_consulted"), list):
        response["sources_consulted"] = result.get("sources_consulted")
    return response


async def _run_live_class_pipeline(
    *,
    task_prompt: str,
    context: dict[str, Any],
    enable_web_retrieval: bool,
    min_citation_count: int = 1,
    compact_context: bool = False,
    retrieval_prompt: str | None = None,
    function_hint: str = "live_class",
    app_surface: str = "live_class",
    pipeline_timeout_s: float = 46.0,
    solve_stage_timeout_s: float = 28.0,
    solve_reevaluation_timeout_s: float = 14.0,
    provider_timeout_overrides: dict[str, float] | None = None,
    meta_timeout_s: float = 5.0,
) -> dict[str, Any]:
    from core.api.entrypoint import lalacore_entry

    clean_retrieval_prompt = _derive_live_retrieval_prompt(
        prompt=_s(retrieval_prompt) or _s(task_prompt),
        context=context,
    )
    planner_like = "agent" in _s(function_hint).lower()
    reasoning_blocks = _build_live_reasoning_blocks(
        task_prompt=task_prompt,
        context=context,
        compact_context=compact_context,
        retrieval_prompt=clean_retrieval_prompt,
    )
    result = await lalacore_entry(
        input_data=clean_retrieval_prompt,
        input_type="text",
        user_context={
            "app_surface": app_surface,
            "student_profile": _derive_live_student_profile(context),
            "mastery_snapshot": context.get("mastery_snapshot")
            if isinstance(context.get("mastery_snapshot"), dict)
            else {},
            "classroom_focus_summary": context.get("classroom_focus_summary")
            if isinstance(context.get("classroom_focus_summary"), list)
            else [],
        },
        options={
            "enable_persona": False,
            "function": function_hint,
            "app_surface": app_surface,
            # Live classes need classroom-safe latency without removing any
            # Omega stages, so only the live-class routes use tighter per-request
            # budgets here.
            "pipeline_timeout_s": float(max(1.0, pipeline_timeout_s)),
            "solve_stage_timeout_s": float(max(1.0, solve_stage_timeout_s)),
            "solve_reevaluation_timeout_s": float(
                max(1.0, solve_reevaluation_timeout_s)
            ),
            "provider_timeout_overrides": provider_timeout_overrides
            or {
                "mini": 20.0,
                "symbolic_guard": 6.0,
                "openrouter": 14.0,
                "groq": 14.0,
                "gemini": 16.0,
                "hf": 18.0,
                "huggingface": 18.0,
            },
            "enable_pre_reasoning_context": True,
            "enable_graph_of_thought": True,
            "enable_mcts_reasoning": True,
            "enable_web_retrieval": enable_web_retrieval,
            "search_max_matches": 18,
            "web_search_timeout_s": 4.8,
            "web_fetch_timeout_s": 2.8,
            "web_similarity_threshold": 0.62,
            "require_citations": False if planner_like else "auto",
            "evidence_mode": "auto",
            "min_citation_count": min_citation_count,
            "min_evidence_score": 0.58,
            "meta_timeout_s": float(max(1.0, meta_timeout_s)),
            "enable_verification_reevaluation": False if planner_like else True,
            "meta_override_min_confidence": 0.0 if planner_like else 0.60,
            "meta_override_max_risk": 1.0 if planner_like else 0.40,
            "meta_override_max_disagreement": 1.0 if planner_like else 0.65,
            "auxiliary_reasoning_blocks": reasoning_blocks,
            "include_pipeline_debug": False,
        },
    )
    if not isinstance(result, dict):
        raise HTTPException(status_code=502, detail="AI engine returned invalid response")
    return result


def _looks_polluted_expression(expr: str) -> bool:
    lowered = expr.lower()
    return any(
        token in lowered
        for token in ("transcript", "chat", "lecture", "before ", "use ", "source")
    )


def _sanitize_visualization(
    *,
    visualization: dict[str, Any] | None,
    prompt: str,
    context: dict[str, Any],
) -> dict[str, Any] | None:
    current = dict(visualization or {}) if isinstance(visualization, dict) else None
    expressions = current.get("expressions") if isinstance(current, dict) else None
    if isinstance(expressions, list):
        rows = [
            str(item.get("latex") or "").strip()
            for item in expressions
            if isinstance(item, dict)
        ]
        if rows and not any(_looks_polluted_expression(row) for row in rows):
            return current
    from core.visualization import DesmosGraphBuilder

    sections = _build_live_context_sections(context)
    seed_parts = [prompt]
    if sections["concepts"]:
        seed_parts.append("Concepts: " + ", ".join(sections["concepts"][:4]))
    if sections["ocr_snippets"]:
        seed_parts.append(sections["ocr_snippets"][0])
    rebuilt = DesmosGraphBuilder().build(
        question="\n".join(part for part in seed_parts if part.strip()),
        profile={"subject": "math"},
    )
    if isinstance(rebuilt, dict):
        return rebuilt
    return current


def _class_explain_response(
    result: dict[str, Any],
    *,
    prompt: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    answer, explanation = _extract_live_answer(result)
    payload: dict[str, Any] = {
        "answer": answer or explanation or "No response from AI backend.",
    }
    if explanation:
        payload["explanation"] = explanation
    confidence = _extract_confidence(result)
    if confidence is not None:
        payload["confidence"] = confidence
    concept = _extract_concept(result)
    if concept:
        payload["concept"] = concept
    for key in (
        "web_retrieval",
        "mcts_search",
        "reasoning_graph",
        "input_analysis",
    ):
        value = result.get(key)
        if isinstance(value, dict):
            payload[key] = value
    visualization = _sanitize_visualization(
        visualization=result.get("visualization")
        if isinstance(result.get("visualization"), dict)
        else None,
        prompt=prompt,
        context=context,
    )
    if isinstance(visualization, dict):
        payload["visualization"] = visualization
    if isinstance(result.get("citations"), list):
        payload["citations"] = result.get("citations")
    if isinstance(result.get("sources_consulted"), list):
        payload["sources_consulted"] = result.get("sources_consulted")
    if isinstance(result.get("citation_map"), list):
        payload["citation_map"] = result.get("citation_map")
    payload.update(_atlas_route_metadata(result))
    return payload


async def _maybe_generate_live_support_actions(
    *,
    result: dict[str, Any],
    prompt: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    atlas = result.get("atlas_actions")
    if not isinstance(atlas, dict) or not bool(atlas.get("triggered")):
        return {}

    async def _bounded(coro: Any, *, timeout_s: float) -> dict[str, Any]:
        try:
            value = await asyncio.wait_for(coro, timeout=max(1.0, float(timeout_s)))
        except Exception:
            return {}
        return value if isinstance(value, dict) else {}

    async def _simplified() -> dict[str, Any]:
        rerun = await _run_live_class_pipeline(
            task_prompt=(
                "Explain the answer again in a simpler, calmer classroom style. "
                "Keep the math rigorous, but reduce cognitive load and focus on one clean path."
                f"\n\n{prompt}"
            ),
            context=context,
            enable_web_retrieval=True,
            min_citation_count=1,
            compact_context=True,
            retrieval_prompt=prompt,
            function_hint="ai_chat",
            app_surface="ai_chat",
            pipeline_timeout_s=12.0,
            solve_stage_timeout_s=8.0,
            solve_reevaluation_timeout_s=4.0,
            provider_timeout_overrides={
                "mini": 10.0,
                "symbolic_guard": 5.0,
                "openrouter": 6.0,
                "groq": 6.0,
                "gemini": 8.0,
                "hf": 9.0,
                "huggingface": 9.0,
            },
            meta_timeout_s=3.0,
        )
        answer, explanation = _extract_live_answer(rerun)
        return {
            "answer": answer or explanation,
            "explanation": explanation or answer,
        }

    async def _worked_example() -> dict[str, Any]:
        rerun = await _run_live_class_pipeline(
            task_prompt=(
                "Generate one fully worked classroom example that teaches the same idea as the user's question. "
                "Keep it JEE-level, but slightly simpler than the original question, and end with the final answer."
                f"\n\nOriginal question:\n{prompt}"
            ),
            context=context,
            enable_web_retrieval=True,
            min_citation_count=1,
            compact_context=True,
            retrieval_prompt=prompt,
            function_hint="ai_chat",
            app_surface="ai_chat",
            pipeline_timeout_s=12.0,
            solve_stage_timeout_s=8.0,
            solve_reevaluation_timeout_s=4.0,
            provider_timeout_overrides={
                "mini": 10.0,
                "symbolic_guard": 5.0,
                "openrouter": 6.0,
                "groq": 6.0,
                "gemini": 8.0,
                "hf": 9.0,
                "huggingface": 9.0,
            },
            meta_timeout_s=3.0,
        )
        answer, explanation = _extract_live_answer(rerun)
        return {
            "answer": answer or explanation,
            "explanation": explanation or answer,
        }

    async def _flashcards() -> dict[str, Any]:
        rerun = await _run_live_class_pipeline(
            task_prompt=(
                "Generate revision flashcards for the same difficulty pocket exposed by this question. "
                "Return strict JSON as {\"flashcards\":[{\"front\":\"...\",\"back\":\"...\"}]}."
            ),
            context=context,
            enable_web_retrieval=True,
            min_citation_count=1,
            compact_context=True,
            retrieval_prompt=prompt,
            function_hint="live_class",
            app_surface="live_class",
            pipeline_timeout_s=10.0,
            solve_stage_timeout_s=7.0,
            solve_reevaluation_timeout_s=4.0,
            provider_timeout_overrides={
                "mini": 9.0,
                "symbolic_guard": 5.0,
                "openrouter": 6.0,
                "groq": 6.0,
                "gemini": 7.0,
                "hf": 8.0,
                "huggingface": 8.0,
            },
            meta_timeout_s=3.0,
        )
        return _flashcards_payload_from_result(rerun, context=context)

    async def _mini_quiz() -> dict[str, Any]:
        topic = _s(prompt)
        generated = await _generate_quiz_via_app_backend(
            topic=topic[:180],
            difficulty="medium",
            context=context,
        )
        return generated or {}

    simplified, worked_example, flashcards, mini_quiz = await asyncio.gather(
        _bounded(_simplified(), timeout_s=10.0),
        _bounded(_worked_example(), timeout_s=10.0),
        _bounded(_flashcards(), timeout_s=8.0),
        _bounded(_mini_quiz(), timeout_s=5.0),
    )

    payload: dict[str, Any] = {}
    if simplified:
        payload["simplified_explanation"] = simplified
    if worked_example:
        payload["worked_example"] = worked_example
    if flashcards:
        payload["flashcards_preview"] = flashcards.get("flashcards", [])
    if mini_quiz:
        payload["mini_quiz"] = mini_quiz
    return payload


async def _deferred_live_support_actions(
    *,
    prompt: str,
    context: dict[str, Any],
    atlas_actions: dict[str, Any] | None,
) -> dict[str, Any]:
    return await _maybe_generate_live_support_actions(
        result={"atlas_actions": dict(atlas_actions or {})},
        prompt=prompt,
        context=context,
    )


def _ensure_string_list(value: Any, *, fallback_text: str = "") -> list[str]:
    if isinstance(value, list):
        out = [_s(item) for item in value if _s(item)]
        if out:
            return out
    fallback = _extract_bullets(fallback_text)
    return fallback


_DEGRADED_GENERATION_MARKERS = (
    "uncertain answer:",
    "provider error:",
    "could not resolve host:",
    "all_provider_answers_empty",
    "verification failed under high risk",
    "providers returned no usable output",
)


def _is_degraded_generation_text(text: str) -> bool:
    normalized = _s(text).lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in _DEGRADED_GENERATION_MARKERS)


def _result_is_degraded(result: dict[str, Any]) -> bool:
    answer, explanation = _extract_live_answer(result)
    if _is_degraded_generation_text(answer) or _is_degraded_generation_text(explanation):
        return True
    steps = result.get("steps")
    if isinstance(steps, list):
        for item in steps:
            if _is_degraded_generation_text(_s(item)):
                return True
    verification = result.get("verification")
    if isinstance(verification, dict):
        if not bool(verification.get("verified")) and _s(verification.get("reason")) in {
            "all_provider_answers_empty",
            "missing_ground_truth",
        }:
            return True
    return False


def _context_formula_candidates(context: dict[str, Any]) -> list[str]:
    sections = _build_live_context_sections(context)
    candidates: list[str] = []
    for line in [*sections["ocr_snippets"], *sections["transcript_lines"], *sections["notes"]]:
        text = _s(line)
        if not text:
            continue
        if any(token in text for token in ("=", "sqrt", "∫", "pi", "^", "/", "→")):
            if text not in candidates:
                candidates.append(text)
    return candidates[:6]


def _context_fallback_notes(context: dict[str, Any]) -> dict[str, list[str]]:
    sections = _build_live_context_sections(context)
    concepts = sections["concepts"][:6]
    doubts = sections["doubts"][:6]
    formulas = _context_formula_candidates(context)
    shortcuts = sections["teacher_signals"][:4] or sections["revision"][:4]
    return {
        "key_concepts": concepts,
        "formulas": formulas,
        "shortcuts": shortcuts,
        "common_mistakes": doubts,
    }


def _context_fallback_analysis(context: dict[str, Any]) -> dict[str, list[str]]:
    sections = _build_live_context_sections(context)
    return {
        "insights": [*sections["mastery"][:4], *sections["autonomy"][:2]],
        "doubt_clusters": sections["doubts"][:6],
        "verification_notes": [*sections["search"][:3], *sections["notes"][:3]],
    }


def _context_fallback_flashcards(context: dict[str, Any]) -> list[dict[str, str]]:
    sections = _build_live_context_sections(context)
    cards: list[dict[str, str]] = []
    for concept in sections["concepts"][:6]:
        cards.append(
            {
                "front": f"Key idea: {concept}",
                "back": f"Review the live-class explanation for {concept}.",
            }
        )
    if cards:
        return cards
    for line in sections["doubts"][:4]:
        cards.append(
            {
                "front": "Queued doubt",
                "back": line,
            }
        )
    return cards


def _chunk_text(text: str, *, chunk_size: int = 96) -> list[str]:
    raw = text.strip()
    if not raw:
        return []
    return [raw[index : index + chunk_size] for index in range(0, len(raw), chunk_size)]


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
            response = request_sync(
                "POST",
                url,
                json_body=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout_s=6.0,
            )
            if response.status_code >= 400:
                return {"_ok": False, "status": response.status_code}
            if response.text:
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
                "active_whiteboard_page_id": room.active_whiteboard_page_id,
                "whiteboard_surface_style": room.whiteboard_surface_style,
                "whiteboard_strokes": list(room.whiteboard_strokes),
                "whiteboard_document_pages": list(room.whiteboard_document_pages),
                "whiteboard_clock": room.whiteboard_lamport_clock,
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
            elif event_type == "whiteboard_surface_changed":
                surface = str((payload.get("metadata") or {}).get("surface") or "").strip().lower()
                if self._is_valid_whiteboard_surface_style(surface):
                    room.whiteboard_surface_style = surface
            elif event_type == "whiteboard_stroke":
                stroke = payload.get("metadata")
                if isinstance(stroke, dict) and self._is_valid_whiteboard_stroke(stroke):
                    stroke_id = str(stroke.get("id") or "").strip()
                    revision = int(stroke.get("revision") or 0)
                    replaced = False
                    if stroke_id:
                        updated: list[dict[str, Any]] = []
                        for existing in room.whiteboard_strokes:
                            existing_id = str(existing.get("id") or "").strip()
                            if existing_id != stroke_id:
                                updated.append(existing)
                                continue
                            existing_revision = int(existing.get("revision") or 0)
                            updated.append(stroke if revision >= existing_revision else existing)
                            replaced = True
                        room.whiteboard_strokes = updated
                    if not replaced:
                        room.whiteboard_strokes.append(stroke)
                    if len(room.whiteboard_strokes) > self._MAX_WHITEBOARD_STROKES:
                        room.whiteboard_strokes = room.whiteboard_strokes[
                            -self._MAX_WHITEBOARD_STROKES :
                        ]
            elif event_type == "whiteboard_operation":
                operation = (payload.get("metadata") or {}).get("operation")
                if isinstance(operation, dict):
                    self._apply_whiteboard_operation(room, dict(operation))
            elif event_type == "whiteboard_snapshot":
                metadata = payload.get("metadata")
                if not isinstance(metadata, dict):
                    metadata = {}
                surface = str(metadata.get("surface") or "").strip().lower()
                raw_strokes = metadata.get("strokes")
                raw_pages = metadata.get("document_pages")
                if self._is_valid_whiteboard_surface_style(surface):
                    room.whiteboard_surface_style = surface
                if isinstance(raw_strokes, list):
                    valid = [
                        dict(item)
                        for item in raw_strokes
                        if isinstance(item, dict)
                        and self._is_valid_whiteboard_stroke(item)
                    ]
                    room.whiteboard_strokes = valid[-self._MAX_WHITEBOARD_STROKES :]
                if isinstance(raw_pages, list):
                    room.whiteboard_document_pages = [
                        dict(item)
                        for item in raw_pages
                        if isinstance(item, dict)
                        and self._is_valid_whiteboard_document_page(item)
                    ][:12]
                active_page_id = str(metadata.get("active_page_id") or "").strip()
                room.active_whiteboard_page_id = active_page_id or None
                room.whiteboard_lamport_clock = int(metadata.get("lamport_clock") or 0)
                room.whiteboard_clear_clock = int(metadata.get("clear_clock") or 0)
                room.whiteboard_deleted_strokes = (
                    {
                        str(key): int(value)
                        for key, value in metadata.get("deleted_strokes", {}).items()
                    }
                    if isinstance(metadata.get("deleted_strokes"), dict)
                    else {}
                )
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
        if not isinstance(points, list) or not points:
            return False
        for point in points:
            if not isinstance(point, dict):
                return False
            if not isinstance(point.get("x"), (int, float)):
                return False
            if not isinstance(point.get("y"), (int, float)):
                return False
        tool = str(stroke.get("tool") or "pen").strip().lower()
        if tool not in {"pen", "eraser", "line", "rectangle", "shape", "text", "image"}:
            return False
        if tool == "text":
            if not str(stroke.get("text") or "").strip():
                return False
        elif tool == "image":
            if not str(stroke.get("asset_data_url") or "").strip().startswith("data:image/"):
                return False
            if len(points) < 2:
                return False
        elif len(points) < 2:
            return False
        if "fill_color" in stroke and not isinstance(stroke.get("fill_color"), int):
            return False
        if "rotation_radians" in stroke and not isinstance(
            stroke.get("rotation_radians"), (int, float)
        ):
            return False
        return isinstance(stroke.get("color"), int) and isinstance(
            stroke.get("width"), (int, float)
        )

    def _is_valid_whiteboard_document_page(self, page: dict[str, Any]) -> bool:
        if not str(page.get("id") or "").strip():
            return False
        if not str(page.get("document_id") or "").strip():
            return False
        if not str(page.get("title") or "").strip():
            return False
        if not isinstance(page.get("page_number"), int):
            return False
        data_url = str(page.get("background_data_url") or "").strip()
        return data_url.startswith("data:image/png;base64,")

    def _is_valid_whiteboard_operation(self, operation: dict[str, Any]) -> bool:
        if not str(operation.get("id") or "").strip():
            return False
        if not str(operation.get("actor_id") or "").strip():
            return False
        if not isinstance(operation.get("lamport"), int):
            return False
        kind = str(operation.get("kind") or "").strip().lower()
        payload = operation.get("payload")
        if not isinstance(payload, dict):
            return False
        if kind == "upsert_stroke":
            return self._is_valid_whiteboard_stroke(payload)
        if kind == "delete_stroke":
            return bool(str(operation.get("object_id") or "").strip())
        if kind == "clear_board":
            return True
        if kind == "set_surface":
            return self._is_valid_whiteboard_surface_style(
                str(payload.get("surface") or "").strip().lower()
            )
        if kind == "set_active_page":
            return bool(str(payload.get("active_page_id") or "").strip())
        if kind == "import_document":
            pages = payload.get("pages")
            if not isinstance(pages, list) or not pages:
                return False
            return all(
                isinstance(page, dict) and self._is_valid_whiteboard_document_page(page)
                for page in pages
            )
        return False

    def _remember_whiteboard_op(self, room: LiveClassRecord, op_id: str) -> bool:
        if op_id in room.whiteboard_seen_ops:
            return False
        room.whiteboard_seen_ops.append(op_id)
        if len(room.whiteboard_seen_ops) > 4000:
            room.whiteboard_seen_ops = room.whiteboard_seen_ops[-4000:]
        return True

    def _apply_whiteboard_operation(
        self, room: LiveClassRecord, operation: dict[str, Any]
    ) -> None:
        if not self._is_valid_whiteboard_operation(operation):
            return
        op_id = str(operation.get("id") or "").strip()
        if not self._remember_whiteboard_op(room, op_id):
            return
        lamport = int(operation.get("lamport") or 0)
        room.whiteboard_lamport_clock = max(room.whiteboard_lamport_clock, lamport)
        room.whiteboard_op_log.append(dict(operation))
        if len(room.whiteboard_op_log) > 4000:
            room.whiteboard_op_log = room.whiteboard_op_log[-4000:]
        kind = str(operation.get("kind") or "").strip().lower()
        object_id = str(operation.get("object_id") or "").strip()
        payload = dict(operation.get("payload") or {})
        if kind == "upsert_stroke":
            stroke = dict(payload)
            stroke_id = str(stroke.get("id") or "").strip()
            if not stroke_id:
                return
            tombstone = room.whiteboard_deleted_strokes.get(stroke_id, 0)
            stroke_clock = int(stroke.get("clock") or lamport)
            if stroke_clock <= tombstone or stroke_clock <= room.whiteboard_clear_clock:
                return
            updated: list[dict[str, Any]] = []
            replaced = False
            for existing in room.whiteboard_strokes:
                existing_id = str(existing.get("id") or "").strip()
                if existing_id != stroke_id:
                    updated.append(existing)
                    continue
                existing_clock = int(existing.get("clock") or 0)
                existing_revision = int(existing.get("revision") or 0)
                incoming_revision = int(stroke.get("revision") or 0)
                if stroke_clock > existing_clock or (
                    stroke_clock == existing_clock
                    and incoming_revision >= existing_revision
                ):
                    updated.append(stroke)
                else:
                    updated.append(existing)
                replaced = True
            if not replaced:
                updated.append(stroke)
            room.whiteboard_strokes = updated[-self._MAX_WHITEBOARD_STROKES :]
            return
        if kind == "delete_stroke" and object_id:
            room.whiteboard_deleted_strokes[object_id] = max(
                room.whiteboard_deleted_strokes.get(object_id, 0), lamport
            )
            room.whiteboard_strokes = [
                stroke
                for stroke in room.whiteboard_strokes
                if str(stroke.get("id") or "").strip() != object_id
            ]
            return
        if kind == "clear_board":
            room.whiteboard_clear_clock = max(room.whiteboard_clear_clock, lamport)
            room.whiteboard_deleted_strokes = {}
            room.whiteboard_strokes = []
            return
        if kind == "set_surface":
            surface = str(payload.get("surface") or "").strip().lower()
            if self._is_valid_whiteboard_surface_style(surface):
                room.whiteboard_surface_style = surface
            return
        if kind == "set_active_page":
            active_page_id = str(payload.get("active_page_id") or object_id).strip()
            room.active_whiteboard_page_id = active_page_id or None
            room.whiteboard_surface_style = "document"
            return
        if kind == "import_document":
            pages = [
                dict(page)
                for page in payload.get("pages", [])
                if isinstance(page, dict)
                and self._is_valid_whiteboard_document_page(page)
            ]
            if not pages:
                return
            room.whiteboard_document_pages = pages[:12]
            room.active_whiteboard_page_id = str(
                payload.get("active_page_id") or pages[0].get("id") or ""
            ).strip() or None
            room.whiteboard_surface_style = "document"
            return

    def _is_valid_whiteboard_surface_style(self, surface: str) -> bool:
        return surface in {"classic", "document"}


_LIVE_HUB = LiveClassHub()


@router.get("/health/ping")
async def live_health_ping() -> dict[str, Any]:
    return {"ok": True, "status": "LIVE_READY", "ts": _utc_now_iso()}


@router.post("/transcribe")
async def worker_transcribe(payload: RecordingTranscribeRequest) -> dict[str, Any]:
    audio_bytes, detected_content_type = _read_recording_bytes(
        payload.recording_path,
        content_type=payload.content_type,
    )
    result = _STT.transcribe_bytes(
        audio_bytes,
        content_type=detected_content_type,
        language_hint=payload.language_hint,
        sample_rate=payload.sample_rate,
        channels=payload.channels,
    )
    transcript_text = _s(result.get("text"))
    confidence = float(result.get("confidence") or 0.0)
    transcript = (
        [
            {
                "speaker": "Speaker",
                "message": transcript_text,
                "timestamp": _utc_now_iso(),
                "confidence": confidence,
            }
        ]
        if transcript_text
        else []
    )
    return {
        "transcript": transcript,
        "text": transcript_text,
        "confidence": confidence,
    }


@router.post("/notes")
async def worker_notes(payload: TranscriptWorkerRequest) -> dict[str, Any]:
    context = _worker_context_from_transcript(payload.transcript)
    if not context["transcript"]:
        return {
            "key_concepts": [],
            "formulas": [],
            "shortcuts": [],
            "common_mistakes": [],
        }
    result = await _run_live_class_pipeline(
        task_prompt=(
            "Generate post-class lecture notes from this recording transcript. "
            "Return strict JSON with keys: key_concepts, formulas, shortcuts, common_mistakes."
        ),
        context=context,
        compact_context=True,
        function_hint="recording_notes",
        app_surface="recording_worker",
    )
    return _notes_payload_from_result(result, context=context)


@router.post("/flashcards")
async def worker_flashcards(payload: TranscriptWorkerRequest) -> dict[str, Any]:
    context = _worker_context_from_transcript(payload.transcript)
    if not context["transcript"]:
        return {"flashcards": []}
    result = await _run_live_class_pipeline(
        task_prompt=(
            "Generate revision flashcards from this class recording transcript. "
            "Return strict JSON as {\"flashcards\":[{\"front\":\"...\",\"back\":\"...\"}]}."
        ),
        context=context,
        compact_context=True,
        function_hint="recording_flashcards",
        app_surface="recording_worker",
    )
    return _flashcards_payload_from_result(result, context=context)


@router.post("/summary")
async def worker_summary(payload: TranscriptWorkerRequest) -> dict[str, Any]:
    context = _worker_context_from_transcript(payload.transcript)
    if not context["transcript"]:
        return {"summary": "", "highlights": [], "action_items": []}
    result = await _run_live_class_pipeline(
        task_prompt=(
            "Generate a concise post-class recap from this transcript. "
            "Return strict JSON with keys: summary, highlights, action_items."
        ),
        context=context,
        compact_context=True,
        function_hint="recording_summary",
        app_surface="recording_worker",
    )
    return _summary_payload_from_result(result, transcript=payload.transcript)


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


def _notes_payload_from_result(
    result: dict[str, Any], *, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    answer, explanation = _extract_live_answer(result)
    decoded = _decode_json_payload(answer) or _decode_json_payload(explanation)
    notes = decoded if isinstance(decoded, dict) else {}
    combined = "\n".join(part for part in (answer, explanation) if part)
    fallback_notes = _context_fallback_notes(context or {}) if _result_is_degraded(result) else {}
    payload = {
        "key_concepts": _ensure_string_list(
            notes.get("key_concepts"),
            fallback_text="" if fallback_notes else combined,
        )
        or fallback_notes.get("key_concepts", []),
        "formulas": _ensure_string_list(
            notes.get("formulas"),
            fallback_text="" if fallback_notes else combined,
        )
        or fallback_notes.get("formulas", []),
        "shortcuts": _ensure_string_list(
            notes.get("shortcuts"),
            fallback_text="" if fallback_notes else combined,
        )
        or fallback_notes.get("shortcuts", []),
        "common_mistakes": _ensure_string_list(
            notes.get("common_mistakes"),
            fallback_text="" if fallback_notes else combined,
        )
        or fallback_notes.get("common_mistakes", []),
    }
    for key, value in list(payload.items()):
        payload[key] = value[:8]
    payload.update(_atlas_route_metadata(result))
    return payload


def _analysis_payload_from_result(
    result: dict[str, Any], *, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    answer, explanation = _extract_live_answer(result)
    decoded = _decode_json_payload(answer) or _decode_json_payload(explanation)
    analysis = decoded if isinstance(decoded, dict) else {}
    combined = "\n".join(part for part in (answer, explanation) if part)
    fallback_analysis = (
        _context_fallback_analysis(context or {}) if _result_is_degraded(result) else {}
    )
    payload = {
        "insights": _ensure_string_list(
            analysis.get("insights"),
            fallback_text="" if fallback_analysis else combined,
        )
        or fallback_analysis.get("insights", []),
        "doubt_clusters": _ensure_string_list(
            analysis.get("doubt_clusters"),
            fallback_text="" if fallback_analysis else combined,
        )
        or fallback_analysis.get("doubt_clusters", []),
        "verification_notes": _ensure_string_list(
            analysis.get("verification_notes"),
            fallback_text="" if fallback_analysis else combined,
        )
        or fallback_analysis.get("verification_notes", []),
    }
    for key, value in list(payload.items()):
        payload[key] = value[:8]
    payload.update(_atlas_route_metadata(result))
    return payload


def _flashcards_payload_from_result(
    result: dict[str, Any], *, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    answer, explanation = _extract_live_answer(result)
    decoded = _decode_json_payload(answer) or _decode_json_payload(explanation)
    cards: list[dict[str, str]] = []
    if isinstance(decoded, dict) and isinstance(decoded.get("flashcards"), list):
        raw_cards = decoded.get("flashcards")
    elif isinstance(decoded, list):
        raw_cards = decoded
    else:
        raw_cards = []
    if isinstance(raw_cards, list):
        for row in raw_cards:
            if not isinstance(row, dict):
                continue
            front = _s(row.get("front"))
            back = _s(row.get("back"))
            if front and back:
                cards.append({"front": front, "back": back})
    if cards:
        payload = {"flashcards": cards[:12]}
        payload.update(_atlas_route_metadata(result))
        return payload
    degraded_cards = (
        _context_fallback_flashcards(context or {}) if _result_is_degraded(result) else []
    )
    if degraded_cards:
        payload = {"flashcards": degraded_cards[:12]}
        payload.update(_atlas_route_metadata(result))
        return payload
    bullets = _extract_bullets("\n".join(part for part in (answer, explanation) if part))
    generated: list[dict[str, str]] = []
    for line in bullets[:6]:
        if ":" in line:
            left, right = line.split(":", 1)
            front = left.strip()
            back = right.strip()
        else:
            front = line.strip()
            back = "Review this checkpoint from the live lecture."
        if front and back:
            generated.append({"front": front, "back": back})
    payload = {"flashcards": generated}
    payload.update(_atlas_route_metadata(result))
    return payload


def _concepts_payload_from_result(result: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    answer, explanation = _extract_live_answer(result)
    decoded = _decode_json_payload(answer) or _decode_json_payload(explanation)
    if isinstance(decoded, dict):
        timeline = decoded.get("timeline")
    else:
        timeline = decoded
    items: list[dict[str, Any]] = []
    if isinstance(timeline, list):
        for row in timeline:
            if not isinstance(row, dict):
                continue
            topic = _s(row.get("topic"))
            summary = _s(row.get("summary"))
            if not topic and not summary:
                continue
            timestamp_value = row.get("timestamp_seconds")
            try:
                timestamp_seconds = int(timestamp_value or 0)
            except (TypeError, ValueError):
                timestamp_seconds = 0
            items.append(
                {
                    "timestamp_seconds": timestamp_seconds,
                    "topic": topic or "Topic",
                    "summary": summary,
                }
            )
    if items:
        payload = {"timeline": items[:8]}
        payload.update(_atlas_route_metadata(result))
        return payload
    sections = _build_live_context_sections(context)
    concepts = sections["concepts"] or ["Current Topic"]
    timestamps = [int(item) for item in sections["timestamps"] if item.isdigit()]
    fallback: list[dict[str, Any]] = []
    for index, concept in enumerate(concepts[:4]):
        timestamp = timestamps[index] if index < len(timestamps) else index * 300
        fallback.append(
            {
                "timestamp_seconds": timestamp,
                "topic": concept,
                "summary": concept,
            }
        )
    payload = {"timeline": fallback}
    payload.update(_atlas_route_metadata(result))
    return payload


def _normalize_worker_transcript(transcript: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(transcript, str):
        for index, raw_line in enumerate(transcript.splitlines()):
            message = raw_line.strip()
            if not message:
                continue
            rows.append(
                {
                    "speaker": "Speaker",
                    "message": message,
                    "timestamp": f"T+{index * 30}s",
                    "confidence": 0.0,
                }
            )
        return rows
    if not isinstance(transcript, list):
        return rows
    for index, item in enumerate(transcript):
        if isinstance(item, dict):
            message = _s(item.get("message") or item.get("text"))
            if not message:
                continue
            try:
                confidence = float(item.get("confidence") or 0.0)
            except (TypeError, ValueError):
                confidence = 0.0
            rows.append(
                {
                    "speaker": _s(item.get("speaker") or item.get("speaker_name"))
                    or "Speaker",
                    "message": message,
                    "timestamp": _s(item.get("timestamp"))
                    or f"T+{index * 30}s",
                    "confidence": confidence,
                }
            )
            continue
        message = _s(item)
        if not message:
            continue
        rows.append(
            {
                "speaker": "Speaker",
                "message": message,
                "timestamp": f"T+{index * 30}s",
                "confidence": 0.0,
            }
        )
    return rows


def _worker_context_from_transcript(transcript: Any) -> dict[str, Any]:
    normalized = _normalize_worker_transcript(transcript)
    return {
        "transcript": normalized,
        "lecture_materials": ["Live class recording"],
        "recording_context": {"source": "recording_worker"},
        "class_metadata": {"surface": "recording_worker"},
    }


def _read_recording_bytes(
    recording_path: str, *, content_type: str | None = None
) -> tuple[bytes, str]:
    target = recording_path.strip()
    if not target:
        raise HTTPException(status_code=400, detail="recording_path is required")
    parsed = urlparse(target)
    guessed_content_type = content_type or ""
    if parsed.scheme in {"http", "https"}:
        try:
            response = requests.get(target, timeout=45.0)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Unable to fetch recording from URL: {exc}",
            ) from exc
        final_content_type = (
            guessed_content_type
            or str(response.headers.get("content-type") or "").split(";", 1)[0].strip()
            or "application/octet-stream"
        )
        return response.content, final_content_type
    file_path = Path(target).expanduser()
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Recording file not found")
    final_content_type = (
        guessed_content_type
        or mimetypes.guess_type(file_path.name)[0]
        or "application/octet-stream"
    )
    try:
        return file_path.read_bytes(), final_content_type
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to read recording file: {exc}",
        ) from exc


def _summary_payload_from_result(
    result: dict[str, Any], *, transcript: Any
) -> dict[str, Any]:
    answer, explanation = _extract_live_answer(result)
    decoded = _decode_json_payload(answer) or _decode_json_payload(explanation)
    payload = decoded if isinstance(decoded, dict) else {}
    normalized = _normalize_worker_transcript(transcript)
    transcript_lines = [
        _s(row.get("message"))
        for row in normalized
        if isinstance(row, dict) and _s(row.get("message"))
    ]
    combined = "\n".join(part for part in (answer, explanation) if part)
    bullets = _extract_bullets(combined)
    summary = _s(payload.get("summary")) or _s(payload.get("overview"))
    if not summary and bullets:
        summary = bullets[0]
    if not summary:
        summary = " ".join(transcript_lines[:3]).strip()
    highlights = _ensure_string_list(
        payload.get("highlights"),
        fallback_text="" if payload.get("highlights") is not None else combined,
    )
    if not highlights:
        highlights = transcript_lines[:4]
    action_items = _ensure_string_list(
        payload.get("action_items"),
        fallback_text="",
    )
    response = {
        "summary": summary[:600],
        "highlights": highlights[:6],
        "action_items": action_items[:6],
    }
    response.update(_atlas_route_metadata(result))
    return response


def _quiz_payload_from_result(
    result: dict[str, Any],
    *,
    topic: str,
    difficulty: str,
    live_mode: bool,
) -> dict[str, Any] | None:
    if _result_is_degraded(result):
        return None
    answer, explanation = _extract_live_answer(result)
    decoded = _decode_json_payload(answer) or _decode_json_payload(explanation)
    if not isinstance(decoded, dict):
        return None
    quiz = decoded
    question = _s(quiz.get("question")) or answer or explanation
    if not question or _is_degraded_generation_text(question):
        return None
    options = [
        option
        for option in _to_list_str(quiz.get("options"))
        if option and not _is_degraded_generation_text(option)
    ]
    if len(options) < 4:
        return None
    options = options[:4]
    if all(re.fullmatch(r"Option\s+[A-D]", option, flags=re.IGNORECASE) for option in options):
        return None
    correct_index_raw = quiz.get("correct_index")
    try:
        correct_index = int(correct_index_raw if correct_index_raw is not None else 0)
    except (TypeError, ValueError):
        correct_index = 0
    if correct_index < 0 or correct_index >= len(options):
        correct_index = 0
    timer_value = quiz.get("timer_seconds")
    try:
        timer_seconds = int(timer_value if timer_value is not None else 20)
    except (TypeError, ValueError):
        timer_seconds = 20
    payload = {
        "question": question,
        "options": options,
        "correct_index": correct_index,
    }
    if live_mode:
        payload["timer_seconds"] = max(10, timer_seconds)
        payload["correct_option"] = correct_index
        payload["topic"] = topic
        payload["difficulty"] = difficulty
    payload.update(_atlas_route_metadata(result))
    return payload


def _live_quiz_payload_with_compat_aliases(
    quiz_payload: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(quiz_payload)
    try:
        correct_index = int(payload.get("correct_index") or 0)
    except (TypeError, ValueError):
        correct_index = 0
    single_question = {
        "question": _s(payload.get("question")),
        "options": _to_list_str(payload.get("options"))[:4],
        "correct_index": correct_index,
    }
    timer_value = payload.get("timer_seconds")
    if timer_value is not None:
        single_question["timer_seconds"] = timer_value
    payload.setdefault(
        "questions",
        [single_question] if single_question["question"] else [],
    )
    payload.setdefault("items", list(payload.get("questions") or []))
    payload.setdefault("question_count", len(payload.get("questions") or []))
    return payload


async def _generate_quiz_via_app_backend(
    *,
    topic: str,
    difficulty: str,
    context: dict[str, Any],
) -> dict[str, Any] | None:
    from app.routes import _APP_DATA

    difficulty_map = {
        "easy": 2,
        "medium": 3,
        "hard": 4,
        "very hard": 5,
    }
    class_metadata = (
        context.get("class_metadata")
        if isinstance(context.get("class_metadata"), dict)
        else {}
    )
    mastery_snapshot = (
        context.get("mastery_snapshot")
        if isinstance(context.get("mastery_snapshot"), dict)
        else {}
    )
    search_state = (
        context.get("search_state")
        if isinstance(context.get("search_state"), dict)
        else {}
    )
    recent_doubts = (
        context.get("recent_doubts") if isinstance(context.get("recent_doubts"), list) else []
    )
    subject_candidates = [
        _s(class_metadata.get("subject")),
        *_to_list_str(context.get("lecture_materials")),
        topic,
    ]
    subject_text = next((item for item in subject_candidates if item), topic)
    chapter_candidates = [
        *_to_list_str(context.get("lecture_concepts")),
        _s(class_metadata.get("topic")),
        topic,
    ]
    chapters = list(dict.fromkeys(item for item in chapter_candidates if item))
    weak_concepts = _to_list_str(mastery_snapshot.get("weakest_concepts"))[:4]
    doubt_focus = [
        _s(row.get("question"))
        for row in recent_doubts[:4]
        if isinstance(row, dict) and _s(row.get("question"))
    ]
    search_focus: list[str] = []
    if isinstance(search_state.get("results"), list):
        for row in search_state.get("results")[:4]:
            if not isinstance(row, dict):
                continue
            concept = _s(row.get("concept"))
            note = _s(row.get("note"))
            if concept:
                search_focus.append(concept)
            if note:
                search_focus.append(note)
    subtopics = list(
        dict.fromkeys(
            item
            for item in [
                *chapters,
                *weak_concepts,
                *doubt_focus,
                *search_focus,
                *_to_list_str(mastery_snapshot.get("concept_summaries"))[:4],
            ]
            if item
        )
    )
    generated = await _APP_DATA.handle_action(
        {
            "action": "ai_generate_quiz",
            "role": "teacher",
            "authoring_mode": True,
            "subject": subject_text,
            "title": f"Live Class Quiz • {_s(class_metadata.get('class_title')) or topic}",
            "chapters": chapters,
            "subtopics": subtopics or chapters,
            "difficulty": difficulty_map.get(difficulty.lower(), 3),
            "question_count": 1,
            "forced_question_type": "MCQ_SINGLE",
            "require_type_variety": False,
            "weak_concepts_json": weak_concepts,
            "allow_web_search": True,
            "pyq_focus": True,
            "trap_intensity": "medium" if difficulty.lower() == "easy" else "high",
            "minimum_reasoning_steps": 3
            if difficulty.lower() in {"hard", "very hard"}
            else 2,
        }
    )
    if not isinstance(generated, dict) or not generated.get("ok"):
        return None
    questions = generated.get("questions")
    if not isinstance(questions, list) or not questions:
        return None
    first = questions[0]
    if not isinstance(first, dict):
        return None
    options = _to_list_str(first.get("options"))
    if len(options) < 4:
        return None
    correct_option = _s(first.get("correct_option")).upper()
    correct_index = {"A": 0, "B": 1, "C": 2, "D": 3}.get(correct_option, 0)
    return {
        "question": _s(first.get("question_text")) or _s(first.get("question")),
        "options": options[:4],
        "correct_index": correct_index,
        "timer_seconds": 20,
    }


@router.post("/ai/class/explain")
async def ai_class_explain(payload: LiveClassAiRequest):
    route_started = time.perf_counter()
    prompt = _s(payload.prompt)
    if not prompt:
        raise HTTPException(status_code=400, detail="Missing prompt")
    explain_directive = (
        "Answer as a live-class teaching copilot. Use the current lecture "
        "trajectory, board OCR, recent doubts, active poll or quiz, and "
        "mastery signals so the response fits what is happening in class right now."
    )
    task_prompt = (
        f"{_s(payload.instruction)}\n\n{explain_directive}\n\n{prompt}".strip()
        if _s(payload.instruction)
        else f"{explain_directive}\n\n{prompt}".strip()
    )
    result = await _run_live_class_pipeline(
        task_prompt=task_prompt,
        context=payload.context,
        enable_web_retrieval=True,
        min_citation_count=1,
        compact_context=True,
        retrieval_prompt=prompt,
        function_hint="ai_chat",
        app_surface="ai_chat",
    )
    logger.info(
        "live_class_explain pipeline_done duration_s=%.3f",
        time.perf_counter() - route_started,
    )
    # Live classes prioritize getting the main classroom answer onto the screen
    # immediately. Evidence hydration and Atlas support bundles already have a
    # dedicated follow-up route, so we do not inline them here. This avoids
    # route-tail latency from cancellation-unfriendly warm/search/secondary
    # solve work while preserving the same features in the hydrated message.
    evidence_pending = not _result_has_live_evidence(result)
    response = _class_explain_response(
        result,
        prompt=prompt,
        context=payload.context,
    )
    logger.info(
        "live_class_explain response_built duration_s=%.3f",
        time.perf_counter() - route_started,
    )
    support_bundle: dict[str, Any] = {}
    support_actions_pending = _atlas_actions_triggered(result.get("atlas_actions"))
    logger.info(
        "live_class_explain support_phase_done duration_s=%.3f pending=%s inline=%s",
        time.perf_counter() - route_started,
        support_actions_pending,
        bool(support_bundle),
    )
    if support_bundle:
        response["support_actions"] = support_bundle
    elif support_actions_pending:
        response["support_actions_pending"] = True
    if evidence_pending:
        response["evidence_pending"] = True
    if not payload.stream:
        logger.info(
            "live_class_explain returning_json duration_s=%.3f",
            time.perf_counter() - route_started,
        )
        return response

    async def _stream() -> Any:
        for chunk in _chunk_text(_s(response.get("answer"))):
            yield chunk
            await asyncio.sleep(0)

    return StreamingResponse(_stream(), media_type="text/plain; charset=utf-8")


@router.post("/ai/class/explain/support")
async def ai_class_explain_support(
    payload: LiveClassAiSupportRequest,
) -> dict[str, Any]:
    prompt = _s(payload.prompt)
    if not prompt:
        raise HTTPException(status_code=400, detail="Missing prompt")
    enriched_result = await _backfill_live_explain_evidence(
        result={"atlas_actions": dict(payload.atlas_actions)},
        prompt=prompt,
        context=payload.context,
    )
    support_bundle = await asyncio.wait_for(
        _deferred_live_support_actions(
            prompt=prompt,
            context=payload.context,
            atlas_actions=payload.atlas_actions,
        ),
        timeout=16.0,
    )
    return {
        "ok": True,
        "support_actions": support_bundle,
        "web_retrieval": enriched_result.get("web_retrieval"),
        "citations": enriched_result.get("citations") or [],
        "sources_consulted": enriched_result.get("sources_consulted") or [],
        "retrieval_score": enriched_result.get("retrieval_score"),
    }


@router.post("/ai/class/agent")
async def ai_class_agent(payload: LiveClassAgentRequest) -> dict[str, Any]:
    instruction = _s(payload.instruction)
    if not instruction:
        raise HTTPException(status_code=400, detail="Missing instruction")
    authority_level = _s(payload.authority_level).lower() or "assist"
    instruction_signals = _build_live_instruction_signals(
        instruction,
        context=payload.context,
    )
    planning_directive = (
        "You are the Atlas live-class classroom agent planner for a JEE teacher. "
        "Do not chat casually. Produce a strict JSON action plan only, with no prose outside JSON. "
        "Use the full live-class context: transcript, OCR, doubts, poll/quiz state, student mastery, "
        "participants, student profiles, autonomy signals, class topic, and recent classroom activity. "
        "Prefer real classroom tools over generic advice.\n\n"
        "Human-language understanding hints for this request:\n"
        f"{_live_instruction_signal_prompt(instruction_signals)}\n\n"
        "Allowed tools: mute_all, mute_student, unmute_student, approve_student_mic, "
        "lower_student_hand, disable_student_camera, remove_student, kick_student, "
        "mute_all_except_teacher, mute_noisy_students_auto, promote_to_cohost, "
        "spotlight_student, pin_participant, clear_spotlight, clear_pinned_participant, "
        "lock_meeting, unlock_meeting, freeze_all_interactions, restore_normal_mode, "
        "enable_focus_mode, disable_focus_mode, enable_chat, disable_chat, "
        "enable_waiting_room, disable_waiting_room, approve_waiting_all, "
        "approve_waiting_student, admit_from_waiting_room, auto_admit_students, "
        "reject_waiting_student, create_poll, search_pyq_and_make_poll, attention_check, "
        "confidence_poll, speed_check_poll, reaction_poll, emoji_reaction_poll, "
        "start_attention_tracker, detect_class_confusion, start_silent_concept_check, "
        "launch_class_quiz, create_live_quiz, launch_mini_quiz, "
        "end_live_poll, reveal_poll_results, clear_live_poll, set_reminder, "
        "solve_doubt_batch, enable_auto_doubt_solver, disable_auto_doubt_solver, "
        "enable_class_autonomy, disable_class_autonomy, select_doubt, select_next_doubt, "
        "clear_active_doubt, resolve_selected_doubt, random_student_pick, random_question_ping, "
        "suggest_student_to_answer, auto_call_student_by_name, get_student_profile, "
        "detect_weak_students, suggest_intervention, group_students_by_level, "
        "rank_students_by_understanding, identify_top_performer, "
        "track_student_improvement_curve, detect_guessing_behavior, "
        "flag_at_risk_students, show_leaderboard, streak_tracker, gamify_leaderboard, "
        "start_recording, stop_recording, end_class, extend_class, "
        "schedule_next_class, reschedule_scheduled_class, cancel_scheduled_class, "
        "list_scheduled_classes, schedule_class_reminder, create_recurring_class_plan, "
        "check_schedule_conflicts, get_free_slots, edit_recurring_plan, delete_occurrence, "
        "shift_series, pause_series, "
        "start_screen_share, stop_screen_share, create_breakout_room, "
        "join_breakout_room, leave_breakout_room, assign_breakout_room, "
        "broadcast_breakout_message, send_class_chat, announce_to_class, generate_notes, "
        "generate_flashcards, generate_adaptive_practice, generate_teacher_report, "
        "generate_revision_summary, generate_dpp, auto_generate_examples, "
        "break_topic_into_steps, re_explain_in_simpler_way, switch_teaching_style, "
        "explain_mistake_pattern, predict_next_doubt, search_and_insert_pyq, "
        "summarize_live_transcript, "
        "sync_notes_to_study, sync_flashcards_to_study, sync_adaptive_practice_to_study, "
        "sync_teacher_report_to_study, sync_class_activity_digest_to_study, "
        "sync_transcript_digest_to_study, sync_whiteboard_digest_to_study, "
        "sync_replay_to_study, sync_recording_bundle_to_study, sync_full_class_bundle_to_study, "
        "publish_class_recap_to_study, "
        "publish_followup_note_to_study, publish_resource_link_to_study, "
        "publish_followup_resource_pack, create_homework_assignment, create_exam_assignment, "
        "create_post_class_homework_bundle, create_post_class_exam_bundle, "
        "create_revision_pack, create_test_pack, create_crash_course_pack, "
        "create_weak_topic_recovery_pack, "
        "report_system_issue, "
        "list_teacher_assessments, list_teacher_homeworks, list_teacher_exams, "
        "list_teacher_study_materials, get_teacher_study_overview, "
        "list_teacher_scheduled_classes, get_teacher_schedule_overview, "
        "get_teacher_chat_summary, list_teacher_open_doubts, "
        "list_teacher_recent_results, get_teacher_review_queue_summary, "
        "list_teacher_pending_reviews, "
        "suggest_next_step, approve_whiteboard_access, grant_whiteboard_access, "
        "dismiss_whiteboard_request, revoke_whiteboard_access, enable_laser_pointer, "
        "disable_laser_pointer, move_laser_pointer, draw_text_on_whiteboard, "
        "draw_line_on_whiteboard, draw_rectangle_on_whiteboard, draw_diagram, "
        "draw_free_body_diagram, draw_graph, highlight_area, segment_board_into_sections, "
        "clean_up_board_layout, convert_board_to_notes, explain_drawing, "
        "solve_what_teacher_wrote, convert_handwriting_to_clean_text, clear_whiteboard.\n\n"
        "You are a planner-executor agent, not a simple tool caller. "
        "Always decide whether the teacher request needs a single action or a multi-step plan. "
        "Use PLAN -> EXECUTE -> OBSERVE -> ADAPT reasoning internally, but output JSON only.\n\n"
        "If one tool call is enough, return strict JSON with this shape: "
        '{"type":"single_action","goal":"<teacher_goal>","plan_id":"<unique_id>","summary":"<short summary>","teacher_notice":"<optional note>","requires_confirmation":<bool>,"tool":"<tool_name>","title":"<short title>","detail":"<short detail>","risk":"low|medium|high","requires_confirmation":<bool>,"args":{...}}. '
        "If multiple actions are needed, return strict JSON with this shape: "
        '{"type":"multi_step_plan","goal":"<teacher_goal>","plan_id":"<unique_id>","summary":"<short summary>","teacher_notice":"<optional note>","requires_confirmation":<bool>,"steps":[{"id":"step_1","tool":"<tool_name>","title":"<short title>","detail":"<short detail>","risk":"low|medium|high","requires_confirmation":<bool>,"args":{...},"depends_on":["<optional step ids>"],"on_failure":{"strategy":"retry|replan|fallback","fallback_tool":"<optional_tool>"}}]}. '
        "If essential information is missing, do not guess and do not emit actions. "
        'Return strict JSON with this shape: {"type":"needs_more_info","goal":"<teacher_goal>","summary":"<short summary>","teacher_notice":"<optional note>","requires_confirmation":false,"needs_more_info":true,"follow_up_questions":["..."],"proposed_tools":["..."],"actions":[]}. '
        "Keep multi-step plans minimal, logically ordered, and usually between 3 and 7 steps. "
        "Use depends_on only when a later step relies on an earlier step. "
        "Avoid redundant steps and never hallucinate tools.\n\n"
        "For create_poll, search_pyq_and_make_poll, attention_check, confidence_poll, speed_check_poll, "
        "reaction_poll, and emoji_reaction_poll, args should include question, options, "
        "timer_seconds, topic, and difficulty when helpful. If the teacher asks for a tough PYQ "
        "or web-backed question, use web retrieval and citations before proposing the poll.\n\n"
        "For student moderation tools, args must include participant_name or participant_id. "
        "For student-intelligence tools, include participant_name or participant_id when the teacher targets a specific student. "
        "For waiting-room approval tools, args must include participant_name or participant_id when needed. "
        "For set_reminder, args must include note and delay_minutes or delay_seconds. "
        "For solve_doubt_batch, args should include max_items. "
        "For extend_class, args may include minutes. "
        "For schedule_next_class, prefer current class defaults for class_name, subject, topic, and title, "
        "but args must include start_time once enough detail is available. Use ISO-8601 time when possible. "
        "For reschedule_scheduled_class, include class_id or enough identifying detail plus the new start_time. "
        "For cancel_scheduled_class, include class_id or enough identifying detail. "
        "For list_scheduled_classes, args can stay empty. "
        "For schedule_class_reminder, include class_id or enough identifying detail plus reminder_offsets_minutes or delay_minutes. "
        "For create_recurring_class_plan, include start_time, occurrences, and either interval_days or weekdays/frequency. "
        "For check_schedule_conflicts, include start_time and duration_minutes when possible. "
        "For get_free_slots, include range_start, range_end, and duration_minutes when possible. "
        "For edit_recurring_plan, identify the recurring plan and include only the fields that should change. "
        "For delete_occurrence, identify the single class occurrence by class_id or clear title/time detail. "
        "For shift_series, include the recurring plan plus the new time or weekday constraints. "
        "For pause_series, include the recurring plan plus pause_days, pause_weeks, or pause_until. "
        "For publish_followup_note_to_study, include title and body when the teacher provides them; otherwise ask only if the current class context is not enough. "
        "For publish_resource_link_to_study, args must include url or source_url. "
        "For publish_followup_resource_pack, args should include resources as a list of url/title/type items when available; if not, Atlas may build a summary pack from the current topic and cited web sources. "
        "For create_homework_assignment and create_exam_assignment, prefer current class defaults for subject, chapter, and class_name; "
        "include title, question_count, difficulty, duration_minutes, deadline, total_marks, marks_per_question, and pyq_focus when the teacher clearly asks for them. "
        "For create_post_class_homework_bundle and create_post_class_exam_bundle, bundle the generated assessment with automatic Study linking and include announce=true when the teacher wants the class notified. "
        "For create_revision_pack, create_test_pack, create_crash_course_pack, and create_weak_topic_recovery_pack, combine the already-supported bundle actions, Study publishing, and optional scheduling into one structured teacher workflow. "
        "For report_system_issue, use it when the teacher reports lag, crashes, blurry video, broken audio, participant network issues, broken media, AI failures, replay issues, or anything in the live classroom not working correctly. Include issue_summary, the failing feature, surface, symptom details, and visible error text when possible. Prefer this tool over generic advice when the teacher clearly wants troubleshooting or escalation. "
        "Teacher app-overview tools are also allowed: list_teacher_assessments, list_teacher_homeworks, list_teacher_exams, list_teacher_study_materials, get_teacher_study_overview, list_teacher_scheduled_classes, get_teacher_schedule_overview, get_teacher_chat_summary, list_teacher_open_doubts, list_teacher_recent_results, get_teacher_review_queue_summary, list_teacher_pending_reviews. "
        "For summarize_live_transcript, keep args empty unless the teacher requests a tighter focus. "
        "For sync_transcript_digest_to_study, sync_recording_bundle_to_study, sync_full_class_bundle_to_study, and publish_class_recap_to_study, Atlas can rely on current live-class context and does not need complex args unless the teacher requests a specific emphasis. "
        "Respect execution order. Schedule creation must happen before reminders. Content generation must happen before publishing. Creation must happen before class announcements. "
        "If scheduling may conflict, insert check_schedule_conflicts before creating or shifting a class. "
        "For revision-style flows, prefer: create_recurring_class_plan -> schedule_class_reminder -> create_revision_pack -> create_homework_assignment -> announce_to_class. "
        "For recovery flows, prefer: detect_weak_students -> create_weak_topic_recovery_pack -> create_recurring_class_plan or schedule_next_class when an extra class is needed. "
        "For switch_teaching_style, args may include style. "
        "Study-sync tools do not need complex args unless a note or target is explicitly requested. "
        "For resolve_selected_doubt, args must include answer. "
        "For laser movement, args must include x and y in normalized 0..1 space. "
        "For breakout assignment, include room_id. For breakout creation, include name. "
        "For draw_text_on_whiteboard, args must include text and optional color_hex. "
        "For draw_line_on_whiteboard, draw_rectangle_on_whiteboard, draw_diagram, draw_free_body_diagram, "
        "draw_graph, highlight_area, segment_board_into_sections, and clean_up_board_layout, "
        "args may include text, title, color_hex, and hint.\n\n"
        f"Teacher authority level: {authority_level}.\n"
        "High-risk actions should require confirmation unless the teacher is in full_auto mode."
    )
    task_prompt = f"{planning_directive}\n\nTeacher instruction:\n{instruction}"
    enable_web = _should_enable_live_agent_web(instruction)
    result = await _run_live_class_pipeline(
        task_prompt=task_prompt,
        context={**payload.context, "instruction_signals": instruction_signals},
        enable_web_retrieval=enable_web,
        min_citation_count=1 if enable_web else 0,
        compact_context=True,
        retrieval_prompt=instruction,
        function_hint="live_class_agent",
        app_surface="live_class",
    )
    return _agent_plan_from_result(
        result=result,
        instruction=instruction,
        authority_level=authority_level,
        instruction_signals=instruction_signals,
    )


@router.post("/ai/class/notes")
async def ai_class_notes(payload: LiveClassAiRequest) -> dict[str, Any]:
    result = await _run_live_class_pipeline(
        task_prompt=(
            f"{_s(payload.instruction)}\n\n"
            "Generate clean lecture notes from the live-class context. Use transcript, "
            "board OCR, recent doubts, active poll or quiz, mastery gaps, and teacher "
            "signals so the notes capture what mattered in this exact class. "
            "Return strict JSON with keys: key_concepts, formulas, shortcuts, common_mistakes."
        ).strip(),
        context=payload.context,
        enable_web_retrieval=True,
        min_citation_count=1,
        retrieval_prompt=_derive_live_retrieval_prompt(
            prompt="",
            context=payload.context,
        ),
    )
    return _notes_payload_from_result(result, context=payload.context)


@router.post("/ai/class/concepts")
async def ai_class_concepts(payload: LiveClassAiRequest) -> dict[str, Any]:
    result = await _run_live_class_pipeline(
        task_prompt=(
            f"{_s(payload.instruction)}\n\n"
            "Build a lecture concept timeline from the live-class context. Use the actual "
            "teaching progression, not a generic chapter order, and pay attention to the "
            "board, doubts, polls, and mastery signals. "
            "Return strict JSON as {\"timeline\":[{\"timestamp_seconds\":180,\"topic\":\"...\",\"summary\":\"...\"}]}. "
            "Keep 3 to 6 items."
        ).strip(),
        context=payload.context,
        enable_web_retrieval=True,
        min_citation_count=1,
        retrieval_prompt=_derive_live_retrieval_prompt(
            prompt="",
            context=payload.context,
        ),
    )
    return _concepts_payload_from_result(result, payload.context)


@router.post("/ai/class/flashcards")
async def ai_class_flashcards(payload: LiveClassAiRequest) -> dict[str, Any]:
    result = await _run_live_class_pipeline(
        task_prompt=(
            f"{_s(payload.instruction)}\n\n"
            "Generate revision flashcards from the live-class context. Prioritize weak concepts, "
            "doubt-heavy checkpoints, active poll confusion, and what was emphasized on the board. "
            "Return strict JSON as {\"flashcards\":[{\"front\":\"...\",\"back\":\"...\"}]}. "
            "Keep the cards JEE-focused and concise."
        ).strip(),
        context=payload.context,
        enable_web_retrieval=True,
        min_citation_count=1,
        retrieval_prompt=_derive_live_retrieval_prompt(
            prompt="",
            context=payload.context,
        ),
    )
    return _flashcards_payload_from_result(result, context=payload.context)


@router.post("/ai/class/analysis")
async def ai_class_analysis(payload: LiveClassAnalysisRequest) -> dict[str, Any]:
    directive = (
        "Analyze the live-class context for teacher intelligence. Use transcript, OCR, "
        "participation, recent doubts, active poll or quiz state, mastery gaps, and "
        "revision recommendations to produce classroom-aware guidance. "
        "Return strict JSON with keys: insights, doubt_clusters, verification_notes."
    )
    if payload.web_verification:
        directive += " Use web retrieval to cross-check standard formulations when useful."
    result = await _run_live_class_pipeline(
        task_prompt=f"{_s(payload.instruction)}\n\n{directive}".strip(),
        context=payload.context,
        enable_web_retrieval=bool(payload.web_verification),
        min_citation_count=1 if payload.web_verification else 0,
        retrieval_prompt=_derive_live_retrieval_prompt(
            prompt="",
            context=payload.context,
        ),
    )
    return _analysis_payload_from_result(result, context=payload.context)


@router.post("/ai/class/quiz")
async def ai_class_quiz(payload: LiveClassQuizRequest) -> dict[str, Any]:
    topic = _s(payload.topic) or "the current live-class topic"
    difficulty = _s(payload.difficulty) or "medium"
    mode = "live poll" if payload.live_mode else "mini quiz"
    question_type = _s(payload.question_type) or "MCQ"
    generated_quiz = await _generate_quiz_via_app_backend(
        topic=topic,
        difficulty=difficulty,
        context=payload.context,
    )
    if generated_quiz is not None:
        quiz_payload = dict(generated_quiz)
        if payload.live_mode:
            quiz_payload["correct_option"] = quiz_payload.get("correct_index", 0)
            quiz_payload["topic"] = topic
            quiz_payload["difficulty"] = difficulty
        return _live_quiz_payload_with_compat_aliases(quiz_payload)
    quiz_context = {
        "lecture_concepts": payload.context.get("lecture_concepts")
        if isinstance(payload.context, dict)
        else [topic],
        "lecture_materials": payload.context.get("lecture_materials")
        if isinstance(payload.context, dict)
        else [],
    }
    if not _to_list_str(quiz_context.get("lecture_concepts")):
        quiz_context["lecture_concepts"] = [topic]
    result = await _run_live_class_pipeline(
        task_prompt=(
            f"{_s(payload.instruction)}\n\n"
            f"Generate one {difficulty} JEE {mode} on {topic}. "
            f"Use {question_type}. Ground it in the current live class context, unresolved doubts, "
            "board OCR, mastery gaps, and any active assessment confusion so it feels like the next "
            "best classroom question, not a generic chapter prompt. "
            "This is a content-generation task, not a request to solve the current lecture example. "
            "Return strict JSON with keys: question, options, correct_index, timer_seconds."
        ).strip(),
        context=quiz_context,
        enable_web_retrieval=True,
        min_citation_count=1,
        compact_context=True,
        retrieval_prompt=topic,
    )
    quiz_payload = _quiz_payload_from_result(
        result,
        topic=topic,
        difficulty=difficulty,
        live_mode=payload.live_mode,
    )
    if quiz_payload is None:
        raise HTTPException(status_code=503, detail="Quiz generation unavailable")
    return _live_quiz_payload_with_compat_aliases(quiz_payload)


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
