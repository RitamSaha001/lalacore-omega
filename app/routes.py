import asyncio
from typing import Any
import os
import json
import uuid
import hashlib
import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from app.auth.local_auth_service import LocalAuthService
from app.data.local_app_data_service import LocalAppDataService
from app.training.dataset_builder import ZaggleDatasetBuilder
from core.api.entrypoint import lalacore_entry
from core.automation.feeder_engine import FeederEngine
from core.automation.orchestrator import AutomationOrchestrator
from core.lalacore_x.weekly import WeeklyEvolutionJob
from services.app_update_release_notifier import AppUpdateReleaseNotifierService
from services.atlas_maintenance_service import AtlasMaintenanceService
from services.question_normalizer import QuestionNormalizer
from services.question_search_engine import QuestionSearchEngine

router = APIRouter()
_FEEDER = FeederEngine()
_AUTOMATION = AutomationOrchestrator(feeder=_FEEDER)
_AUTH = LocalAuthService()
_APP_DATA = LocalAppDataService()
_ATLAS_MAINTENANCE = AtlasMaintenanceService(app_data=_APP_DATA)
_APP_UPDATE_RELEASE_NOTIFIER = AppUpdateReleaseNotifierService()
_QUESTION_NORMALIZER = QuestionNormalizer()
_QUESTION_SEARCH_ENGINE = QuestionSearchEngine()


# ==============================
# REQUEST MODEL
# ==============================

class SolveRequest(BaseModel):
    question: str | None = None
    input_data: str | dict | None = None
    input_type: str = "auto"
    user_context: dict | None = None
    options: dict | None = None


# ==============================
# RESPONSE MODEL (Optional but clean)
# ==============================

class SolveResponse(BaseModel):
    status: str | None = None
    error: str | None = None
    message: str | None = None
    question: str | None = None
    reasoning: str | None = None
    final_answer: str | None = None
    answer: str | None = None
    confidence: float | None = None
    visualization: dict | None = None
    verification: dict | None = None
    plausibility: dict | None = None
    routing_decision: str | None = None
    escalate: bool | None = None
    winner_provider: str | None = None
    profile: dict | None = None
    arena: dict | None = None
    retrieval: dict | None = None
    engine: dict | None = None
    input_metadata: dict | None = None
    ocr_data: dict | None = None
    pdf_data: dict | None = None
    vision_analysis: dict | None = None
    input_analysis: dict | None = None
    web_retrieval: dict | None = None
    mcts_search: dict | None = None
    reasoning_graph: dict | None = None
    citations: list[dict] | None = None
    sources_consulted: list[str] | None = None
    provider_diagnostics: dict | None = None
    research_verification: dict | None = None
    calibration_metrics: dict | None = None
    meta_verification: dict | None = None
    entropy: float | None = None
    disagreement: float | None = None
    latency_metrics: dict | None = None
    display_answer: str | None = None


class WeeklyEvolutionResponse(BaseModel):
    weekly: dict
    datasets: dict


class FeederAddRequest(BaseModel):
    question: str
    subject: str = "general"
    difficulty: str = "unknown"
    concept_cluster: list[str] = Field(default_factory=list)
    source_tag: str = "manual"


class FeederAddResponse(BaseModel):
    added: bool
    duplicate: bool
    queue_item: dict


class FeederProcessRequest(BaseModel):
    max_items: int = 10


class FeederStatusResponse(BaseModel):
    total: int
    counts: dict
    recent: list[dict]
    daily_cap: int
    processed_today: int


class AutomationRunRequest(BaseModel):
    trigger: str = "manual"
    resume: bool = True
    feeder_batch: int = 12
    replay_batch: int | None = None
    execute_replay_pipeline: bool = True


class AtlasMaintenanceRunRequest(BaseModel):
    trigger: str = "manual"
    recipient_email: str | None = None


class AppUpdateConfirmationRunRequest(BaseModel):
    trigger: str = "manual"
    recipient_email: str | None = None
    force_resend: bool = False


class AuthActionRequest(BaseModel):
    action: str
    email: str | None = None
    password: str | None = None
    new_password: str | None = None
    otp: str | None = None
    name: str | None = None
    username: str | None = None
    flow: str | None = None
    purpose: str | None = None
    device_id: str | None = None


class QuestionSearchRequest(BaseModel):
    query: str
    max_matches: int = 10


class AppAtlasPlanRequest(BaseModel):
    instruction: str | None = None
    context: dict[str, Any] | None = None
    authority_level: str = "student_full_auto"


class AppAtlasObserveRequest(BaseModel):
    account_id: str | None = None
    tool_name: str
    category: str | None = None
    success: bool = True
    latency_ms: int = 0
    context: dict[str, Any] | None = None
    args: dict[str, Any] | None = None
    observation: str | None = None


class AppAtlasPassiveRequest(BaseModel):
    account_id: str | None = None
    context: dict[str, Any] | None = None


_APP_ATLAS_ALLOWED_TOOLS: tuple[str, ...] = (
    "report_system_issue",
    "list_pending_homeworks",
    "list_pending_exams",
    "show_remaining_work",
    "get_homework_details",
    "get_exam_details",
    "open_homework",
    "open_exam",
    "list_study_materials",
    "find_study_material",
    "get_material_details",
    "open_material",
    "download_material",
    "open_material_formula_sheet",
    "open_material_flashcards",
    "open_material_revision_plan",
    "quiz_me_on_material",
    "open_related_materials",
    "open_latest_notes",
    "open_latest_flashcards",
    "open_latest_practice",
    "open_latest_transcript_digest",
    "open_latest_whiteboard_digest",
    "list_live_class_artifacts",
    "open_last_class_bundle",
    "play_last_class_recording",
    "open_latest_replay",
    "get_next_scheduled_class",
    "list_upcoming_classes",
    "get_notifications_summary",
    "get_recent_scores",
    "get_results_history",
    "get_weak_topics",
    "suggest_next_best_task",
    "summarize_material_with_ai",
    "make_notes_from_material",
    "ask_material_ai",
    "open_classes_hub",
    "open_study_library",
    "open_notifications_center",
    "join_next_class",
    "open_last_class_notes",
    "open_last_class_flashcards",
    "open_last_class_practice",
    "open_last_class_transcript",
    "open_last_class_whiteboard",
    "get_last_class_bundle_details",
    "get_material_download_link",
    "open_material_notes",
    "open_material_summary",
    "open_latest_material_formula_sheet",
    "open_latest_material_flashcards",
    "open_latest_material_revision_plan",
    "quiz_me_on_latest_material",
    "open_recording_bundle",
    "show_deadline_pressure",
    "show_score_trend",
    "show_recent_activity",
    "list_due_today",
    "list_due_this_week",
    "show_completion_summary",
    "show_attempted_vs_pending",
    "show_subject_breakdown",
    "get_study_overview",
    "get_live_class_overview",
    "list_subject_materials",
    "list_chapter_materials",
    "open_homework_dashboard",
    "open_exam_dashboard",
    "open_latest_teacher_report",
    "open_last_class_teacher_report",
    "open_latest_class_activity_digest",
    "open_last_class_activity_digest",
    "open_chats_inbox",
    "open_chat_thread",
    "open_doubt_thread",
    "get_chat_directory_summary",
    "show_recent_chat_threads",
    "show_unread_chat_threads",
    "search_chat_messages",
    "summarize_chat_thread",
    "summarize_chat_last_day",
    "send_chat_message",
    "send_chat_attachment",
    "create_chat_poll",
    "reply_to_doubt_thread",
    "react_to_chat_message",
    "vote_in_chat_poll",
    "pin_chat_message",
    "unpin_chat_message",
    "get_open_doubts",
    "get_resolved_doubts",
    "show_recent_doubt_updates",
    "show_doubt_status_summary",
    "open_study_ai_hub",
    "open_ai_chat_history",
    "open_latest_ai_chat",
    "open_new_ai_chat",
    "show_ai_chat_history_summary",
    "show_pinned_ai_chats",
    "get_unread_notification_count",
    "show_high_priority_notifications",
    "show_ai_notifications",
    "mark_all_notifications_seen",
    "refresh_notifications",
    "open_latest_notification",
    "open_notifications_center_unread",
    "open_notifications_center_high_priority",
    "open_notifications_center_ai",
    "open_latest_result_analytics",
    "open_result_analytics_for_assessment",
    "open_latest_result_review",
    "open_result_review_for_assessment",
    "open_latest_answer_key",
    "open_answer_key_for_assessment",
    "open_latest_mistake_review",
    "open_mistake_review_for_assessment",
    "open_latest_result_attempt_history",
    "open_result_attempt_history_for_assessment",
    "open_latest_completed_homework",
    "open_latest_completed_exam",
    "open_latest_pending_homework",
    "open_latest_pending_exam",
    "open_homework_dashboard_pending",
    "open_homework_dashboard_completed",
    "open_exam_dashboard_pending",
    "open_exam_dashboard_completed",
    "show_assessment_attempt_history",
    "open_self_practice_quiz_builder",
    "list_ai_practice_quizzes",
    "open_latest_ai_practice_quiz",
    "open_latest_ai_practice_result",
    "open_ai_practice_history",
    "reattempt_latest_ai_practice",
    "get_classes_today",
    "get_live_now_classes",
    "open_classes_hub_today",
    "open_classes_hub_live",
    "open_classes_hub_upcoming",
    "open_join_options_for_next_class",
    "open_study_for_next_class",
    "open_study_for_weak_topic",
    "open_latest_material",
    "open_latest_non_live_material",
    "open_latest_live_class_material",
    "download_latest_material",
    "open_latest_material_notes",
    "open_latest_material_summary",
)

_APP_ATLAS_TEACHER_ALLOWED_TOOLS: tuple[str, ...] = (
    "report_system_issue",
    "open_teacher_dashboard",
    "open_teacher_exams_tab",
    "open_teacher_homeworks_tab",
    "open_teacher_classes_hub",
    "open_teacher_study_library",
    "open_teacher_student_analytics",
    "open_teacher_create_quiz",
    "open_teacher_add_material",
    "open_teacher_chats_inbox",
    "open_chat_thread",
    "open_doubt_thread",
    "open_teacher_live_classes",
    "open_teacher_schedule_sheet",
    "open_teacher_class_ai_overview",
    "open_teacher_student_profile",
    "open_teacher_result_detail",
    "list_teacher_assessments",
    "list_teacher_homeworks",
    "list_teacher_exams",
    "list_teacher_study_materials",
    "get_teacher_study_overview",
    "list_teacher_scheduled_classes",
    "get_teacher_schedule_overview",
    "get_teacher_chat_summary",
    "search_chat_messages",
    "summarize_chat_thread",
    "summarize_chat_last_day",
    "send_chat_message",
    "send_chat_attachment",
    "create_chat_poll",
    "reply_to_doubt_thread",
    "react_to_chat_message",
    "vote_in_chat_poll",
    "pin_chat_message",
    "unpin_chat_message",
    "list_teacher_open_doubts",
    "list_teacher_recent_results",
    "get_teacher_review_queue_summary",
    "list_teacher_pending_reviews",
    "list_teacher_students",
    "get_teacher_student_profile_summary",
    "get_teacher_student_history_detail",
    "get_teacher_class_performance_summary",
    "identify_teacher_attention_students",
    "create_homework_assignment",
    "create_exam_assignment",
    "schedule_next_class",
    "create_recurring_class_plan",
    "schedule_class_reminder",
    "check_schedule_conflicts",
    "publish_followup_note_to_study",
    "publish_resource_link_to_study",
    "generate_teacher_quiz_draft",
    "import_teacher_quiz_from_attachment",
    "revise_teacher_quiz_draft",
    "preview_teacher_quiz_draft",
    "publish_teacher_quiz_draft",
    "create_teacher_material_draft_from_attachment",
    "publish_teacher_material_draft",
)


def _atlas_s(value: Any) -> str:
    return (value or "").strip() if isinstance(value, str) else str(value or "").strip()


def _atlas_decode_json_payload(text: str) -> dict[str, Any] | list[Any] | None:
    raw = text.strip()
    if not raw:
        return None
    candidates: list[str] = [raw]
    fenced = re.findall(r"```(?:json)?\s*([\s\S]*?)```", raw, flags=re.IGNORECASE)
    candidates.extend(fragment.strip() for fragment in fenced if fragment.strip())
    for open_char, close_char in (("{", "}"), ("[", "]")):
        start = raw.find(open_char)
        end = raw.rfind(close_char)
        if start != -1 and end != -1 and end > start:
            snippet = raw[start : end + 1].strip()
            if snippet and snippet not in candidates:
                candidates.append(snippet)
    for candidate in candidates:
        try:
            decoded = json.loads(candidate)
        except Exception:
            continue
        if isinstance(decoded, (dict, list)):
            return decoded
    return None


def _atlas_extract_answer(result: dict[str, Any]) -> tuple[str, str]:
    answer = (
        _atlas_s(result.get("unsafe_candidate_answer"))
        or _atlas_s(result.get("answer"))
        or _atlas_s(result.get("final_answer"))
    )
    if answer:
        return answer, _atlas_s(result.get("explanation") or result.get("reasoning"))
    raw = result.get("raw")
    if isinstance(raw, dict):
        nested_answer = (
            _atlas_s(raw.get("unsafe_candidate_answer"))
            or _atlas_s(raw.get("answer"))
            or _atlas_s(raw.get("final_answer"))
        )
        nested_explanation = _atlas_s(raw.get("explanation") or raw.get("reasoning"))
        if nested_answer or nested_explanation:
            return nested_answer, nested_explanation
    return "", _atlas_s(result.get("explanation") or result.get("reasoning"))


def _atlas_normalize_text_list(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [item.strip() for item in (str(value) for value in raw) if item.strip()]


def _atlas_plan_type(payload: dict[str, Any], *, follow_up_questions: list[str]) -> str:
    raw_type = _atlas_s(payload.get("type")).lower()
    if raw_type in {
        "single_action",
        "multi_step_plan",
        "needs_more_info",
        "clarification_request",
    }:
        return raw_type
    if follow_up_questions or bool(payload.get("needs_more_info")):
        return "needs_more_info"
    if _atlas_s(payload.get("tool")):
        return "single_action"
    return "multi_step_plan"


def _atlas_plan_id(payload: dict[str, Any], *, plan_type: str) -> str:
    existing = _atlas_s(payload.get("plan_id"))
    if existing:
        return existing
    prefix = "student_single" if plan_type == "single_action" else "student_plan"
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _atlas_summary_from_explanation(text: str) -> str:
    cleaned = re.sub(r"^\s*reasoning:\s*", "", _atlas_s(text), flags=re.IGNORECASE).strip()
    if not cleaned:
        return ""
    sentence = re.split(r"(?<=[\.\!\?])\s+", cleaned, maxsplit=1)[0].strip()
    return sentence[:220]


_ATLAS_EXPLANATION_TOOL_PHRASES: dict[str, tuple[str, ...]] = {
    "find_study_material": (
        "find study material",
        "find materials",
        "study material for",
        "notes for",
        "resources for",
    ),
    "get_study_overview": (
        "study overview",
        "overview of my study",
        "understand my study load",
        "where should i focus",
    ),
    "generate_teacher_quiz_draft": (
        "create a quiz",
        "create quiz",
        "generate a quiz",
        "generate quiz",
        "quiz draft",
    ),
    "open_teacher_student_analytics": (
        "open student analytics",
        "student analytics",
        "analytics page",
    ),
    "get_teacher_student_history_detail": (
        "student performance history",
        "full performance history",
        "detailed student history",
    ),
    "get_teacher_class_performance_summary": (
        "class performance summary",
        "whole class performance",
        "class performance history",
    ),
    "get_weak_topics": (
        "weak topics",
        "weakest topic",
    ),
    "suggest_next_best_task": (
        "next best task",
        "what should i do next",
        "what should i study next",
        "study plan",
        "revision roadmap",
    ),
    "open_study_for_weak_topic": (
        "open study for the weakest topic",
        "open study for the weakest one",
        "study for the weakest topic",
        "study the weakest topic",
    ),
}

_ATLAS_HUMAN_FILLER_RE = re.compile(
    r"\b(?:atlas|please|can you|could you|would you|will you|just|kindly|i want you to|i need you to|help me|help us)\b",
    flags=re.IGNORECASE,
)

_ATLAS_TOPIC_HINTS: tuple[str, ...] = (
    "binomial theorem",
    "permutation and combination",
    "permutation",
    "combination",
    "thermodynamics",
    "electrostatics",
    "newton's laws of motion",
    "newtons laws of motion",
    "hyperbola",
    "physics",
    "chemistry",
    "mathematics",
    "maths",
)

_ATLAS_TOOL_SIGNAL_PHRASES: dict[str, tuple[str, ...]] = {
    "find_study_material": (
        "find study material for",
        "find material for",
        "study material for",
        "notes for",
        "resources for",
    ),
    "get_study_overview": (
        "study overview",
        "overview of my study",
        "show my study overview",
        "understand my study load",
        "where should i focus",
    ),
    "show_remaining_work": (
        "what's left",
        "what is left",
        "what do i still have left",
        "what i still have left",
        "what remains",
        "what is remaining",
        "what is still pending",
        "see what's left",
        "show what is left",
    ),
    "list_due_today": (
        "due today",
        "for today",
        "today's work",
    ),
    "list_due_this_week": (
        "due this week",
        "for this week",
        "this week's work",
    ),
    "open_material": (
        "open this material",
        "open this note",
        "open the material",
        "open the notes",
        "show this material",
    ),
    "download_material": (
        "download this material",
        "download the material",
        "save this material",
        "download the notes",
    ),
    "open_material_formula_sheet": (
        "formula sheet",
        "formula summary",
        "give me the formulas",
        "show the formulas",
    ),
    "open_material_flashcards": (
        "flashcards",
        "make flashcards",
        "revise with flashcards",
    ),
    "open_material_revision_plan": (
        "revision plan",
        "study plan for this",
        "how should i revise this",
    ),
    "suggest_next_best_task": (
        "what should i do next",
        "what should i study next",
        "next best task",
        "what should i focus on",
        "help me plan my study",
        "plan my study",
        "plan my revision",
        "create a study plan",
        "make me a study plan",
        "quick study plan",
        "revision roadmap",
    ),
    "quiz_me_on_material": (
        "quiz me on this",
        "test me on this",
        "ask me questions from this",
    ),
    "open_study_for_weak_topic": (
        "study my weak topic",
        "open study for my weak topic",
        "help me revise my weak topic",
        "study plan for my weak topic",
        "revise my weakest topic",
    ),
    "get_weak_topics": (
        "my weak topics",
        "weak topics",
        "where am i weak",
        "which topic am i weak in",
    ),
    "open_latest_result_analytics": (
        "how did i do",
        "show me how i did",
        "show my analytics",
        "my result analytics",
        "my performance",
    ),
    "open_latest_result_review": (
        "review my result",
        "result review",
        "go through my result",
    ),
    "open_latest_answer_key": (
        "answer key",
        "show the answer key",
        "open answer key",
    ),
    "open_latest_mistake_review": (
        "mistake review",
        "show my mistakes",
        "wrong answers",
        "where did i go wrong",
    ),
    "get_next_scheduled_class": (
        "next class",
        "next scheduled class",
        "when is my next class",
    ),
    "open_latest_replay": (
        "play last class",
        "last class recording",
        "latest replay",
    ),
    "open_last_class_transcript": (
        "last class transcript",
        "transcript from last class",
    ),
    "summarize_chat_last_day": (
        "what happened in the last day",
        "summarise what happened in the last day",
        "summarize what happened in the last day",
        "what happened yesterday in this group",
        "summarize yesterday in this group",
    ),
    "report_system_issue": (
        "not working",
        "is broken",
        "lagging",
        "crashing",
        "stuck",
        "slow",
        "ai is not working",
        "something is wrong",
        "video is blurry",
        "video blurry",
        "sound quality is bad",
        "audio quality is bad",
        "voice is breaking",
        "cannot hear properly",
        "can't hear properly",
        "video is freezing",
        "call quality is bad",
        "network issue",
    ),
    "open_teacher_student_analytics": (
        "open student analytics",
        "student analytics",
        "class analytics",
        "how is the class doing",
        "how are students doing",
        "show performance overview",
    ),
    "open_teacher_student_profile": (
        "open student profile",
        "student profile",
        "show this student",
    ),
    "get_teacher_student_history_detail": (
        "full performance history",
        "student performance history",
        "detailed performance history",
        "show this student's history",
        "show student history",
    ),
    "get_teacher_class_performance_summary": (
        "whole class performance",
        "class performance summary",
        "show class performance",
        "how is the whole class doing",
        "all students performance",
    ),
    "generate_teacher_quiz_draft": (
        "make a quiz",
        "create a quiz",
        "generate a quiz",
        "prepare a quiz",
        "draft a quiz",
        "make a test",
        "create a test",
    ),
    "import_teacher_quiz_from_attachment": (
        "make a quiz from this",
        "turn this pdf into a quiz",
        "turn this image into a quiz",
        "import questions from this",
        "extract questions from this pdf",
        "use this pdf for the quiz",
        "use this image for the quiz",
    ),
    "revise_teacher_quiz_draft": (
        "change question",
        "edit question",
        "remove question",
        "add question",
        "change marks",
        "edit options",
        "reorder questions",
    ),
    "preview_teacher_quiz_draft": (
        "show me the quiz",
        "show the draft",
        "let me review it",
        "review it first",
        "preview the quiz",
        "preview it first",
    ),
    "publish_teacher_quiz_draft": (
        "publish the quiz",
        "go ahead and publish",
        "send it to students",
        "publish it",
    ),
    "create_teacher_material_draft_from_attachment": (
        "put this in study",
        "add this to study",
        "upload this to study",
        "turn this into study material",
        "make study material from this",
    ),
    "publish_teacher_material_draft": (
        "publish this material",
        "publish the material",
        "send this material",
    ),
    "schedule_next_class": (
        "schedule the next class",
        "set up the next class",
        "line up the next class",
        "put the next class on the calendar",
    ),
    "create_recurring_class_plan": (
        "set this every week",
        "make it recurring",
        "repeat this class",
        "run this every",
    ),
    "schedule_class_reminder": (
        "remind me before the class",
        "remind students before class",
        "send a reminder before the class",
        "ping them before class",
    ),
    "check_schedule_conflicts": (
        "check for clashes",
        "check if it overlaps",
        "see if the timing conflicts",
        "find schedule conflicts",
    ),
    "create_homework_assignment": (
        "give homework",
        "assign homework",
        "set homework",
    ),
    "create_exam_assignment": (
        "create an exam",
        "make an exam",
        "assign a test",
        "set an exam",
    ),
    "open_teacher_add_material": (
        "open add material",
        "add study material",
        "study upload screen",
    ),
}


def _atlas_has_attachment_context(context: dict[str, Any]) -> bool:
    for key in (
        "atlas_latest_attachment",
        "latest_attachment",
        "teacher_latest_attachment",
        "student_latest_attachment",
    ):
        raw = context.get(key)
        if isinstance(raw, dict) and any(
            _atlas_s(raw.get(field))
            for field in ("file_id", "id", "name", "filename", "mime", "url", "path")
        ):
            return True
    return False


def _atlas_normalize_instruction_text(text: str) -> str:
    lowered = _atlas_s(text).lower()
    lowered = _ATLAS_HUMAN_FILLER_RE.sub(" ", lowered)
    lowered = lowered.replace("what's", "what is")
    lowered = lowered.replace("can't", "cannot").replace("won't", "will not")
    lowered = re.sub(r"[\"'`]+", "", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip(" .,!?\n\t")


def _atlas_has_time_hint(text: str) -> bool:
    lowered = _atlas_normalize_instruction_text(text)
    return any(
        token in lowered
        for token in (
            "today",
            "tomorrow",
            "next week",
            "this week",
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
            "same time",
        )
    )


def _atlas_extract_instruction_topics(text: str) -> list[str]:
    lowered = _atlas_normalize_instruction_text(text)
    topics: list[str] = []
    for hint in _ATLAS_TOPIC_HINTS:
        if hint in lowered and hint not in topics:
            topics.append(hint)
    return topics[:5]


def _atlas_primary_study_query(
    instruction: str,
    *,
    signals: dict[str, Any] | None = None,
) -> str:
    topics = _atlas_extract_instruction_topics(instruction)
    if topics:
        return topics[0].title()
    normalized = _atlas_s(
        (signals or {}).get("normalized_instruction")
    ) or _atlas_normalize_instruction_text(instruction)
    query_match = re.search(
        r"(?:study|revise|revision|plan|roadmap|material|notes|resources)\s+(?:for|on|about)\s+([a-z0-9][a-z0-9\s'&+\-/]+)",
        normalized,
    )
    if query_match:
        query = re.sub(r"\s+", " ", query_match.group(1)).strip(" .,!?\n\t")
        if query:
            return query.title()
    return ""


def _atlas_build_instruction_signals(
    instruction: str,
    *,
    context: dict[str, Any],
    allowed_tools: set[str],
    role: str,
) -> dict[str, Any]:
    normalized = _atlas_normalize_instruction_text(instruction)
    sequence_cues = [
        cue
        for cue in ("first", "then", "after", "before", "next", "finally")
        if cue in normalized
    ]
    selected_material = _atlas_selected_material(context)
    selected_student = (
        dict(context.get("selected_student"))
        if isinstance(context.get("selected_student"), dict)
        else {}
    )
    attachment_referenced = _atlas_has_attachment_context(context) or any(
        phrase in normalized
        for phrase in ("this pdf", "that pdf", "this image", "that image", "attachment", "uploaded")
    )
    action_modes: list[str] = []
    if any(token in normalized for token in ("open ", "show ", "go to ", "take me to", "navigate")):
        action_modes.append("open")
    if any(token in normalized for token in ("create", "make", "generate", "prepare", "build", "turn this into")):
        action_modes.append("create")
    if any(token in normalized for token in ("edit", "change", "remove", "add", "update", "reorder")):
        action_modes.append("edit")
    if any(token in normalized for token in ("publish", "send it", "share it", "post it")):
        action_modes.append("publish")
    if any(token in normalized for token in ("review", "preview", "show me the draft", "let me review")):
        action_modes.append("review")
    if any(
        token in normalized
        for token in (
            "study plan",
            "plan my study",
            "plan my revision",
            "revision roadmap",
            "what should i do next",
            "what should i study next",
            "what should i focus on",
            "quick study plan",
            "revise",
            "revision",
        )
    ):
        action_modes.append("plan")
    if any(token in normalized for token in ("summarize", "summarise", "what happened", "recap")):
        action_modes.append("summarize")
    if any(
        token in normalized
        for token in (
            "not working",
            "broken",
            "lagging",
            "crashing",
            "slow",
            "stuck",
            "backend",
            "server",
            "offline fallback",
            "unreachable",
            "connection refused",
            "cannot connect",
            "cant connect",
            "not reaching backend",
            "not reaching the backend",
            "ai is down",
            "atlas is down",
            "atlas not working",
            "ai not working",
        )
    ):
        action_modes.append("diagnose")
    if any(token in normalized for token in ("schedule", "class", "remind", "calendar", "recurring")):
        action_modes.append("schedule")
    if any(token in normalized for token in ("send", "reply", "message", "react", "poll")):
        action_modes.append("chat")
    score_map: dict[str, int] = {}
    first_index: dict[str, int] = {}
    for tool, phrases in _ATLAS_TOOL_SIGNAL_PHRASES.items():
        if tool not in allowed_tools:
            continue
        for phrase in phrases:
            idx = normalized.find(phrase)
            if idx == -1:
                continue
            score_map[tool] = score_map.get(tool, 0) + max(2, len(phrase.split()))
            first_index[tool] = min(first_index.get(tool, idx), idx)
    if role == "student":
        if "show_remaining_work" in allowed_tools and any(
            phrase in normalized for phrase in ("what is left", "what remains", "still pending")
        ):
            score_map["show_remaining_work"] = score_map.get("show_remaining_work", 0) + 4
            first_index.setdefault("show_remaining_work", 0)
        if "plan" in action_modes:
            plan_tools = (
                "find_study_material",
                "get_study_overview",
                "get_weak_topics",
                "suggest_next_best_task",
                "open_study_for_weak_topic",
                "open_material_revision_plan",
                "list_due_this_week",
                "show_remaining_work",
            )
            for tool in plan_tools:
                if tool in allowed_tools:
                    score_map[tool] = score_map.get(tool, 0) + 4
                    first_index.setdefault(
                        tool,
                        normalized.find("plan") if "plan" in normalized else 0,
                    )
            if bool(_atlas_primary_study_query(instruction, signals={"normalized_instruction": normalized})):
                if "find_study_material" in allowed_tools:
                    score_map["find_study_material"] = score_map.get("find_study_material", 0) + 5
                    first_index.setdefault("find_study_material", 0)
            if selected_material and "open_material_revision_plan" in allowed_tools:
                score_map["open_material_revision_plan"] = (
                    score_map.get("open_material_revision_plan", 0) + 5
                )
                first_index.setdefault("open_material_revision_plan", 0)
        if selected_material and any(
            phrase in normalized for phrase in ("formula", "flashcards", "revision plan", "quiz me", "this material")
        ):
            tool_boosts = {
                "formula": "open_material_formula_sheet",
                "flashcards": "open_material_flashcards",
                "revision plan": "open_material_revision_plan",
                "quiz me": "quiz_me_on_material",
                "this material": "open_material",
            }
            for phrase, tool in tool_boosts.items():
                if phrase in normalized and tool in allowed_tools:
                    score_map[tool] = score_map.get(tool, 0) + 4
                    first_index.setdefault(tool, normalized.find(phrase))
        if "report_system_issue" in allowed_tools and "diagnose" in action_modes:
            score_map["report_system_issue"] = score_map.get("report_system_issue", 0) + 5
            first_index.setdefault("report_system_issue", 0)
    else:
        if attachment_referenced:
            if "import_teacher_quiz_from_attachment" in allowed_tools and any(
                phrase in normalized
                for phrase in (
                    "quiz from this",
                    "test from this",
                    "import questions",
                    "extract questions",
                    "turn this pdf into a quiz",
                    "turn this image into a quiz",
                )
            ):
                score_map["import_teacher_quiz_from_attachment"] = (
                    score_map.get("import_teacher_quiz_from_attachment", 0) + 6
                )
                first_index.setdefault(
                    "import_teacher_quiz_from_attachment",
                    normalized.find("this"),
                )
            if "create_teacher_material_draft_from_attachment" in allowed_tools and any(
                phrase in normalized
                for phrase in (
                    "put this in study",
                    "add this to study",
                    "upload this to study",
                    "turn this into study material",
                )
            ):
                score_map["create_teacher_material_draft_from_attachment"] = (
                    score_map.get("create_teacher_material_draft_from_attachment", 0) + 6
                )
                first_index.setdefault(
                    "create_teacher_material_draft_from_attachment",
                    normalized.find("study"),
                )
        if "preview_teacher_quiz_draft" in allowed_tools and "review" in action_modes:
            score_map["preview_teacher_quiz_draft"] = (
                score_map.get("preview_teacher_quiz_draft", 0) + 3
            )
            first_index.setdefault("preview_teacher_quiz_draft", normalized.find("review"))
        if "publish_teacher_quiz_draft" in allowed_tools and "publish" in action_modes:
            score_map["publish_teacher_quiz_draft"] = (
                score_map.get("publish_teacher_quiz_draft", 0) + 3
            )
            first_index.setdefault("publish_teacher_quiz_draft", normalized.find("publish"))
        if "publish_teacher_material_draft" in allowed_tools and "publish" in action_modes:
            score_map["publish_teacher_material_draft"] = (
                score_map.get("publish_teacher_material_draft", 0) + 3
            )
            first_index.setdefault("publish_teacher_material_draft", normalized.find("publish"))
        if "report_system_issue" in allowed_tools and "diagnose" in action_modes:
            score_map["report_system_issue"] = score_map.get("report_system_issue", 0) + 5
            first_index.setdefault("report_system_issue", 0)
    candidate_tools = [
        tool
        for tool, _score in sorted(
            score_map.items(),
            key=lambda item: (
                -item[1],
                first_index.get(item[0], 10**9),
                item[0],
            ),
        )
    ][:8]
    missing_detail_hints: list[str] = []
    if role == "student" and any(
        phrase in normalized for phrase in ("this material", "the material", "download it", "open it")
    ) and not bool(_atlas_s(selected_material.get("material_id")) or _atlas_s(selected_material.get("title"))):
        missing_detail_hints.append("material_reference")
    if role == "teacher" and any(
        phrase in normalized for phrase in ("student profile", "student analytics", "this student")
    ) and not bool(
        _atlas_s(selected_student.get("student_name"))
        or _atlas_s(selected_student.get("name"))
        or _atlas_s(selected_student.get("student_id"))
    ):
        missing_detail_hints.append("student_reference")
    if "schedule" in action_modes and not _atlas_has_time_hint(instruction):
        missing_detail_hints.append("time_reference")
    return {
        "normalized_instruction": normalized,
        "action_modes": action_modes,
        "sequence_cues": sequence_cues,
        "topic_hints": _atlas_extract_instruction_topics(instruction),
        "candidate_tools": candidate_tools,
        "attachment_referenced": attachment_referenced,
        "has_selected_material": bool(
            _atlas_s(selected_material.get("material_id"))
            or _atlas_s(selected_material.get("title"))
        ),
        "has_selected_student": bool(
            _atlas_s(selected_student.get("student_name"))
            or _atlas_s(selected_student.get("name"))
            or _atlas_s(selected_student.get("student_id"))
        ),
        "missing_detail_hints": missing_detail_hints[:4],
    }


def _atlas_instruction_signal_prompt(signals: dict[str, Any]) -> str:
    lines = [
        f"- Normalized intent: {_atlas_s(signals.get('normalized_instruction')) or 'unknown'}",
        f"- Candidate tools by human-language fit: {', '.join(signals.get('candidate_tools') or []) or 'none'}",
        f"- Action modes: {', '.join(signals.get('action_modes') or []) or 'none'}",
        f"- Sequence cues: {', '.join(signals.get('sequence_cues') or []) or 'none'}",
        f"- Topic hints: {', '.join(signals.get('topic_hints') or []) or 'none'}",
        f"- Attachment referenced: {'yes' if signals.get('attachment_referenced') else 'no'}",
        f"- Missing detail hints: {', '.join(signals.get('missing_detail_hints') or []) or 'none'}",
    ]
    return "\n".join(lines)


def _atlas_synthesized_action(
    tool: str,
    *,
    instruction: str,
    context: dict[str, Any],
    step_id: str = "step_1",
    depends_on: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": step_id,
        "tool": tool,
        "title": tool.replace("_", " ").title(),
        "detail": _atlas_summary_from_explanation(instruction) or instruction,
        "risk": "low",
        "requires_confirmation": False,
        "args": _atlas_infer_action_args(tool, instruction=instruction, context=context),
        "depends_on": list(depends_on or []),
        "on_failure": {"strategy": "retry"},
    }


def _atlas_synthesize_plan_from_signals(
    *,
    instruction: str,
    context: dict[str, Any],
    allowed_tools: set[str],
    role: str,
    signals: dict[str, Any],
) -> dict[str, Any] | None:
    normalized = _atlas_s(signals.get("normalized_instruction"))
    candidate_tools = [
        tool
        for tool in signals.get("candidate_tools", [])
        if tool in allowed_tools
    ]
    attachment_referenced = bool(signals.get("attachment_referenced"))
    if role == "student":
        planning_intent = "plan" in (signals.get("action_modes") or []) or any(
            phrase in normalized
            for phrase in (
                "study plan",
                "plan my study",
                "plan my revision",
                "revision roadmap",
                "what should i do next",
                "what should i study next",
                "what should i focus on",
                "quick study plan",
            )
        )
        if planning_intent:
            selected_material = _atlas_selected_material(context)
            if (
                bool(_atlas_s(selected_material.get("material_id")) or _atlas_s(selected_material.get("title")))
                and "open_material_revision_plan" in allowed_tools
                and any(
                    phrase in normalized
                    for phrase in ("this material", "selected material", "this note", "these notes")
                )
            ):
                return {
                    "type": "single_action",
                    "goal": instruction,
                    "summary": "Atlas is using the selected Study material to open the best revision flow.",
                    "student_notice": "Atlas matched your revision-planning request to the current Study material.",
                    "tool": "open_material_revision_plan",
                    "title": "Open revision plan",
                    "detail": instruction,
                    "risk": "low",
                    "args": {},
                    "recovery_mode": "instruction_signals",
                }
            actions: list[dict[str, Any]] = []
            evidence_steps: list[str] = []
            primary_query = _atlas_primary_study_query(instruction, signals=signals)
            if primary_query and "find_study_material" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "find_study_material",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                    )
                )
                evidence_steps.append(actions[-1]["id"])
            if "get_study_overview" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "get_study_overview",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                    )
                )
                evidence_steps.append(actions[-1]["id"])
            if "get_weak_topics" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "get_weak_topics",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                    )
                )
                evidence_steps.append(actions[-1]["id"])
            if "today" in normalized and "list_due_today" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "list_due_today",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                    )
                )
                evidence_steps.append(actions[-1]["id"])
            elif "this week" in normalized and "list_due_this_week" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "list_due_this_week",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                    )
                )
                evidence_steps.append(actions[-1]["id"])
            elif not primary_query and "show_remaining_work" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "show_remaining_work",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                    )
                )
                evidence_steps.append(actions[-1]["id"])
            if "suggest_next_best_task" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "suggest_next_best_task",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                        depends_on=evidence_steps,
                    )
                )
            elif "open_study_for_weak_topic" in allowed_tools:
                actions.append(
                    _atlas_synthesized_action(
                        "open_study_for_weak_topic",
                        instruction=instruction,
                        context=context,
                        step_id=f"step_{len(actions) + 1}",
                        depends_on=evidence_steps,
                    )
                )
            actions = actions[:4]
            if actions:
                return {
                    "type": "single_action" if len(actions) == 1 else "multi_step_plan",
                    "goal": instruction,
                    "summary": "Atlas composed a study-planning workflow from your request using real Study and progress tools.",
                    "student_notice": "Atlas is planning with the actual study, weakness, and next-best-task tools instead of generic advice.",
                    "actions": actions,
                    "steps": actions,
                    "proposed_tools": [action["tool"] for action in actions],
                    "recovery_mode": "instruction_signals",
                }
        if not candidate_tools:
            return None
        top_tool = candidate_tools[0]
        if top_tool in {
            "show_remaining_work",
            "list_due_today",
            "list_due_this_week",
            "get_weak_topics",
            "open_latest_result_analytics",
            "open_latest_result_review",
            "open_latest_answer_key",
            "open_latest_mistake_review",
            "get_next_scheduled_class",
            "open_latest_replay",
            "open_last_class_transcript",
            "report_system_issue",
        }:
            payload = {
                "type": "single_action",
                "goal": instruction,
                "summary": _atlas_summary_from_explanation(instruction) or instruction,
                "student_notice": "Atlas inferred the likely student action from your natural request.",
                "tool": top_tool,
                "title": top_tool.replace("_", " ").title(),
                "detail": instruction,
                "risk": "low",
                "args": _atlas_infer_action_args(top_tool, instruction=instruction, context=context),
                "recovery_mode": "instruction_signals",
            }
            if top_tool == "report_system_issue":
                payload["args"] = _atlas_fill_action_args(
                    {"tool": top_tool, "args": {"issue_summary": instruction}},
                    context=context,
                )["args"]
            return payload
        if top_tool in {
            "open_material",
            "download_material",
            "open_material_formula_sheet",
            "open_material_flashcards",
            "open_material_revision_plan",
            "quiz_me_on_material",
        } and bool(signals.get("has_selected_material")):
            return {
                "type": "single_action",
                "goal": instruction,
                "summary": _atlas_summary_from_explanation(instruction) or instruction,
                "student_notice": "Atlas matched the material-scoped request from your natural wording.",
                "tool": top_tool,
                "title": top_tool.replace("_", " ").title(),
                "detail": instruction,
                "risk": "low",
                "args": {},
                "recovery_mode": "instruction_signals",
            }
        return None
    if not candidate_tools:
        return None
    top_tool = candidate_tools[0]
    if top_tool == "report_system_issue":
        return {
            "type": "single_action",
            "goal": instruction,
            "summary": _atlas_summary_from_explanation(instruction) or instruction,
            "teacher_notice": "Atlas inferred this is a troubleshooting request and will diagnose it directly.",
            "tool": "report_system_issue",
            "title": "Report system issue",
            "detail": instruction,
            "risk": "low",
            "args": {"issue_summary": instruction},
            "recovery_mode": "instruction_signals",
        }
    if (
        "get_teacher_class_performance_summary" in allowed_tools
        and any(
            phrase in normalized
            for phrase in (
                "whole class",
                "entire class",
                "all students",
                "class performance",
                "class history",
            )
        )
    ):
        return {
            "type": "single_action",
            "goal": instruction,
            "summary": _atlas_summary_from_explanation(instruction) or instruction,
            "teacher_notice": "Atlas inferred a whole-class performance summary request from your wording.",
            "tool": "get_teacher_class_performance_summary",
            "title": "Get teacher class performance summary",
            "detail": instruction,
            "risk": "low",
            "args": {},
            "recovery_mode": "instruction_signals",
        }
    if (
        "get_teacher_student_history_detail" in allowed_tools
        and any(
            phrase in normalized
            for phrase in (
                "performance history",
                "full history",
                "detailed history",
                "attempt history",
            )
        )
    ):
        return {
            "type": "single_action",
            "goal": instruction,
            "summary": _atlas_summary_from_explanation(instruction) or instruction,
            "teacher_notice": "Atlas inferred a detailed student history request from your wording.",
            "tool": "get_teacher_student_history_detail",
            "title": "Get teacher student history detail",
            "detail": instruction,
            "risk": "low",
            "args": _atlas_infer_action_args(
                "get_teacher_student_history_detail",
                instruction=instruction,
                context=context,
            ),
            "recovery_mode": "instruction_signals",
        }
    if top_tool == "open_teacher_student_analytics":
        return {
            "type": "single_action",
            "goal": instruction,
            "summary": _atlas_summary_from_explanation(instruction) or instruction,
            "teacher_notice": "Atlas inferred the analytics navigation request from your natural wording.",
            "tool": "open_teacher_student_analytics",
            "title": "Open student analytics",
            "detail": instruction,
            "risk": "low",
            "args": _atlas_infer_action_args(
                "open_teacher_student_analytics",
                instruction=instruction,
                context=context,
            ),
            "recovery_mode": "instruction_signals",
        }
    if top_tool in {
        "get_teacher_student_profile_summary",
        "get_teacher_student_history_detail",
        "get_teacher_class_performance_summary",
        "identify_teacher_attention_students",
    }:
        return {
            "type": "single_action",
            "goal": instruction,
            "summary": _atlas_summary_from_explanation(instruction) or instruction,
            "teacher_notice": "Atlas inferred the analytics summary request from your natural wording.",
            "tool": top_tool,
            "title": top_tool.replace("_", " ").title(),
            "detail": instruction,
            "risk": "low",
            "args": _atlas_infer_action_args(
                top_tool,
                instruction=instruction,
                context=context,
            ),
            "recovery_mode": "instruction_signals",
        }
    if attachment_referenced and top_tool in {
        "create_teacher_material_draft_from_attachment",
        "publish_teacher_material_draft",
    }:
        actions = [
            _atlas_synthesized_action(
                "create_teacher_material_draft_from_attachment",
                instruction=instruction,
                context=context,
                step_id="step_1",
            )
        ]
        if "publish" in normalized and "publish_teacher_material_draft" in allowed_tools:
            actions.append(
                _atlas_synthesized_action(
                    "publish_teacher_material_draft",
                    instruction=instruction,
                    context=context,
                    step_id="step_2",
                    depends_on=["step_1"],
                )
            )
        return {
            "type": "single_action" if len(actions) == 1 else "multi_step_plan",
            "goal": instruction,
            "summary": "Atlas inferred a Study-material authoring flow from the natural teacher request.",
            "teacher_notice": "Atlas matched the attachment-to-Study workflow from your wording.",
            "actions": actions,
            "steps": actions,
            "proposed_tools": [action["tool"] for action in actions],
            "recovery_mode": "instruction_signals",
        }
    if top_tool in {"generate_teacher_quiz_draft", "import_teacher_quiz_from_attachment"}:
        draft_tool = (
            "import_teacher_quiz_from_attachment"
            if attachment_referenced and "import_teacher_quiz_from_attachment" in allowed_tools
            else "generate_teacher_quiz_draft"
        )
        actions = [
            _atlas_synthesized_action(
                draft_tool,
                instruction=instruction,
                context=context,
                step_id="step_1",
            )
        ]
        if (
            any(token in normalized for token in ("review", "preview", "show me the draft"))
            and "preview_teacher_quiz_draft" in allowed_tools
        ):
            actions.append(
                _atlas_synthesized_action(
                    "preview_teacher_quiz_draft",
                    instruction=instruction,
                    context=context,
                    step_id="step_2",
                    depends_on=["step_1"],
                )
            )
        if (
            "publish" in normalized
            and "publish_teacher_quiz_draft" in allowed_tools
        ):
            depends_on = [actions[-1]["id"]] if actions else []
            actions.append(
                _atlas_synthesized_action(
                    "publish_teacher_quiz_draft",
                    instruction=instruction,
                    context=context,
                    step_id=f"step_{len(actions) + 1}",
                    depends_on=depends_on,
                )
            )
        return {
            "type": "single_action" if len(actions) == 1 else "multi_step_plan",
            "goal": instruction,
            "summary": "Atlas inferred the teacher authoring workflow from the natural request.",
            "teacher_notice": "Atlas matched your create-review-publish intent from natural language.",
            "actions": actions,
            "steps": actions,
            "proposed_tools": [action["tool"] for action in actions],
            "recovery_mode": "instruction_signals",
        }
    return None


def _atlas_extract_tool_mentions(
    text: str,
    *,
    allowed_tools: set[str],
) -> list[str]:
    lowered = _atlas_s(text).lower()
    if not lowered:
        return []
    matches: list[tuple[int, int, str]] = []
    for tool in allowed_tools:
        tool_key = tool.lower()
        start = lowered.find(f"`{tool_key}`")
        if start == -1:
            start = lowered.find(tool_key)
        if start == -1:
            for phrase in _ATLAS_EXPLANATION_TOOL_PHRASES.get(tool, ()):
                phrase_index = lowered.find(phrase)
                if phrase_index != -1:
                    start = phrase_index
                    break
        if start == -1:
            for phrase in _ATLAS_TOOL_SIGNAL_PHRASES.get(tool, ()):
                phrase_index = lowered.find(phrase)
                if phrase_index != -1:
                    start = phrase_index
                    break
        if start == -1:
            continue
        matches.append((start, -len(tool_key), tool))
    matches.sort()
    ordered: list[str] = []
    seen: set[str] = set()
    for _, _, tool in matches:
        if tool in seen:
            continue
        seen.add(tool)
        ordered.append(tool)
    return ordered[:4]


def _atlas_infer_action_args(
    tool_name: str,
    *,
    instruction: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    tool = _atlas_s(tool_name).lower()
    lowered = _atlas_s(instruction).lower()
    args: dict[str, Any] = {}
    primary_study_query = _atlas_primary_study_query(instruction)
    selected_student = (
        dict(context.get("selected_student"))
        if isinstance(context.get("selected_student"), dict)
        else {}
    )
    if tool in {"find_study_material", "list_subject_materials", "list_chapter_materials"}:
        if primary_study_query:
            args["query"] = primary_study_query
    elif tool == "open_study_for_weak_topic":
        if primary_study_query:
            args["query"] = primary_study_query
    elif tool == "generate_teacher_quiz_draft":
        class_match = re.search(r"\bclass\s+(\d+)\b", lowered)
        if class_match:
            args["class_name"] = f"Class {class_match.group(1)}"
        question_match = re.search(r"\b(\d+)\s+questions?\b", lowered)
        if question_match:
            try:
                args["question_count"] = int(question_match.group(1))
            except ValueError:
                pass
        topic_match = re.search(
            r"(?:create|make|generate)\s+(?:a|an)\s+(?:class\s+\d+\s+)?(.+?)\s+quiz\b",
            lowered,
        )
        if topic_match:
            topic = re.sub(r"\s+", " ", topic_match.group(1)).strip(" .")
            if topic:
                pretty_topic = topic.title()
                args["topic"] = pretty_topic
                args.setdefault("title", f"{pretty_topic} Quiz")
        if "thermodynamics" in lowered:
            args.setdefault("subject", "Physics")
            args.setdefault("topic", "Thermodynamics")
            args.setdefault("title", "Thermodynamics Quiz")
    elif tool in {
        "open_teacher_student_analytics",
        "get_teacher_student_profile_summary",
        "get_teacher_student_history_detail",
    }:
        if _atlas_s(selected_student.get("student_id")):
            args["student_id"] = _atlas_s(selected_student.get("student_id"))
        if _atlas_s(selected_student.get("student_name") or selected_student.get("name")):
            args["student_name"] = _atlas_s(
                selected_student.get("student_name") or selected_student.get("name")
            )
    return args


def _atlas_recover_actions_from_explanation(
    *,
    explanation: str,
    instruction: str,
    context: dict[str, Any],
    allowed_tools: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    tools = _atlas_extract_tool_mentions(explanation, allowed_tools=allowed_tools)
    if not tools:
        return [], []
    actions: list[dict[str, Any]] = []
    previous_id: str | None = None
    for index, tool in enumerate(tools, start=1):
        step_id = f"step_{index}"
        action: dict[str, Any] = {
            "id": step_id,
            "tool": tool,
            "title": tool.replace("_", " ").title(),
            "detail": _atlas_summary_from_explanation(explanation),
            "risk": "low",
            "requires_confirmation": False,
            "args": _atlas_infer_action_args(tool, instruction=instruction, context=context),
            "depends_on": [previous_id] if previous_id else [],
            "on_failure": {"strategy": "retry"},
        }
        actions.append(action)
        previous_id = step_id
    return actions, tools


def _atlas_coerce_actions(
    raw: Any,
    *,
    allowed_tools: set[str],
) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    actions: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        tool = _atlas_s(item.get("tool")).lower()
        if not tool or tool not in allowed_tools:
            continue
        args = item.get("args") if isinstance(item.get("args"), dict) else {}
        depends_on = (
            [entry for entry in (_atlas_s(value) for value in item.get("depends_on", []))]
            if isinstance(item.get("depends_on"), list)
            else []
        )
        on_failure = (
            dict(item.get("on_failure"))
            if isinstance(item.get("on_failure"), dict)
            else {}
        )
        fallback_tool = _atlas_s(on_failure.get("fallback_tool")).lower()
        actions.append(
            {
                "id": _atlas_s(item.get("id")) or f"action_{index + 1}",
                "tool": tool,
                "title": _atlas_s(item.get("title")) or tool.replace("_", " ").title(),
                "detail": _atlas_s(item.get("detail")),
                "risk": _atlas_s(item.get("risk")).lower() or "low",
                "requires_confirmation": bool(item.get("requires_confirmation")),
                "args": dict(args),
                "depends_on": [entry for entry in depends_on if entry],
                "on_failure": {
                    "strategy": _atlas_s(on_failure.get("strategy")).lower(),
                    **({"fallback_tool": fallback_tool} if fallback_tool and fallback_tool in allowed_tools else {}),
                },
            }
        )
    return actions


def _atlas_tool_category(tool_name: str) -> str:
    tool = _atlas_s(tool_name).lower()
    if tool in {
        "list_study_materials",
        "find_study_material",
        "get_material_details",
        "open_material",
        "download_material",
        "open_material_formula_sheet",
        "open_material_flashcards",
        "open_material_revision_plan",
        "quiz_me_on_material",
        "open_related_materials",
        "open_material_notes",
        "open_material_summary",
        "summarize_material_with_ai",
        "make_notes_from_material",
        "ask_material_ai",
        "open_study_library",
        "list_subject_materials",
        "list_chapter_materials",
        "get_study_overview",
        "open_study_ai_hub",
        "open_study_for_next_class",
        "open_study_for_weak_topic",
        "open_latest_material",
        "open_latest_non_live_material",
        "open_latest_live_class_material",
        "download_latest_material",
        "open_latest_material_notes",
        "open_latest_material_summary",
        "open_latest_material_formula_sheet",
        "open_latest_material_flashcards",
        "open_latest_material_revision_plan",
        "quiz_me_on_latest_material",
    }:
        return "study"
    if tool in {
        "list_pending_homeworks",
        "list_pending_exams",
        "show_remaining_work",
        "get_homework_details",
        "get_exam_details",
        "open_homework",
        "open_exam",
        "open_homework_dashboard",
        "open_exam_dashboard",
        "list_due_today",
        "list_due_this_week",
        "show_completion_summary",
        "show_attempted_vs_pending",
        "open_latest_result_analytics",
        "open_result_analytics_for_assessment",
        "open_latest_completed_homework",
        "open_latest_completed_exam",
        "open_latest_pending_homework",
        "open_latest_pending_exam",
        "show_assessment_attempt_history",
        "open_self_practice_quiz_builder",
        "list_ai_practice_quizzes",
        "open_latest_ai_practice_quiz",
        "open_latest_ai_practice_result",
        "open_ai_practice_history",
        "reattempt_latest_ai_practice",
    }:
        return "assessment"
    if tool in {
        "get_next_scheduled_class",
        "list_upcoming_classes",
        "join_next_class",
        "open_classes_hub",
        "get_live_class_overview",
        "get_classes_today",
        "get_live_now_classes",
        "open_join_options_for_next_class",
    }:
        return "schedule"
    if tool in {
        "play_last_class_recording",
        "open_latest_replay",
        "open_last_class_bundle",
        "open_recording_bundle",
        "get_last_class_bundle_details",
        "open_last_class_notes",
        "open_last_class_flashcards",
        "open_last_class_practice",
        "open_last_class_transcript",
        "open_last_class_whiteboard",
        "open_latest_teacher_report",
        "open_last_class_teacher_report",
        "open_latest_class_activity_digest",
        "open_last_class_activity_digest",
        "list_live_class_artifacts",
    }:
        return "live_class"
    if tool in {"report_system_issue"}:
        return "support"
    if tool in {
        "get_notifications_summary",
        "open_notifications_center",
        "show_recent_activity",
        "get_recent_scores",
        "get_results_history",
        "show_score_trend",
        "get_weak_topics",
        "suggest_next_best_task",
        "show_subject_breakdown",
        "get_unread_notification_count",
        "show_high_priority_notifications",
        "show_ai_notifications",
        "mark_all_notifications_seen",
        "refresh_notifications",
        "open_latest_notification",
        "get_chat_directory_summary",
        "show_recent_chat_threads",
        "show_unread_chat_threads",
        "get_open_doubts",
        "get_resolved_doubts",
        "show_recent_doubt_updates",
        "show_doubt_status_summary",
        "open_chats_inbox",
        "open_ai_chat_history",
        "open_latest_ai_chat",
        "open_new_ai_chat",
        "show_ai_chat_history_summary",
        "show_pinned_ai_chats",
    }:
        return "intelligence"
    return "general"


def _atlas_role(context: dict[str, Any], *, authority_level: str = "") -> str:
    role = _atlas_s(context.get("role") or context.get("atlas_role")).lower()
    if role in {"teacher", "student"}:
        return role
    authority = _atlas_s(authority_level).lower()
    if authority.startswith("teacher"):
        return "teacher"
    return "student"


def _atlas_allowed_tools(context: dict[str, Any], *, role: str = "student") -> set[str]:
    raw = context.get("allowed_tools")
    default_tools = (
        _APP_ATLAS_TEACHER_ALLOWED_TOOLS
        if role == "teacher"
        else _APP_ATLAS_ALLOWED_TOOLS
    )
    if not isinstance(raw, list):
        return set(default_tools)
    requested = {
        _atlas_s(item).lower()
        for item in raw
        if _atlas_s(item)
    }
    allowed = requested.intersection(default_tools)
    return allowed or set(default_tools)


def _atlas_should_explore(*, account_id: str, instruction: str) -> bool:
    identity = _atlas_s(account_id)
    if not identity:
        return False
    day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    seed = f"{identity}|{instruction.strip().lower()}|{day_key}"
    bucket = int(hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8], 16) % 10
    return bucket == 0


def _atlas_selected_material(context: dict[str, Any]) -> dict[str, Any]:
    raw = context.get("selected_material")
    return dict(raw) if isinstance(raw, dict) else {}


def _atlas_student_memory(context: dict[str, Any]) -> dict[str, Any]:
    raw = context.get("student_memory")
    return dict(raw) if isinstance(raw, dict) else {}


def _atlas_fill_action_args(
    action: dict[str, Any],
    *,
    context: dict[str, Any],
) -> dict[str, Any]:
    out = dict(action)
    args = dict(out.get("args") or {})
    selected_material = _atlas_selected_material(context)
    student_memory = _atlas_student_memory(context)
    if out.get("tool") in {
        "open_material",
        "download_material",
        "open_material_formula_sheet",
        "open_material_flashcards",
        "open_material_revision_plan",
        "quiz_me_on_material",
        "open_related_materials",
        "summarize_material_with_ai",
        "make_notes_from_material",
        "ask_material_ai",
        "open_material_notes",
        "open_material_summary",
    }:
        if not _atlas_s(args.get("material_id")):
            args["material_id"] = _atlas_s(
                selected_material.get("material_id")
                or student_memory.get("last_material_id")
            )
        if not _atlas_s(args.get("title")) and _atlas_s(selected_material.get("title")):
            args["title"] = _atlas_s(selected_material.get("title"))
        if not _atlas_s(args.get("subject")) and _atlas_s(selected_material.get("subject")):
            args["subject"] = _atlas_s(selected_material.get("subject"))
    if out.get("tool") in {"list_subject_materials", "list_chapter_materials"}:
        if not _atlas_s(args.get("query")):
            args["query"] = _atlas_s(
                args.get("subject")
                or args.get("chapter")
                or student_memory.get("last_subject")
                or selected_material.get("subject")
                or selected_material.get("chapter")
            )
    if out.get("tool") == "report_system_issue":
        if not _atlas_s(args.get("issue_summary")):
            args["issue_summary"] = _atlas_s(
                args.get("issue")
                or context.get("last_error")
                or context.get("surface_issue")
                or context.get("problem_summary")
            )
        if not _atlas_s(args.get("surface")):
            args["surface"] = _atlas_s(context.get("surface"))
        issue_summary = _atlas_s(args.get("issue_summary")).lower()
        if (
            not _atlas_s(args.get("failing_feature"))
            and any(
                token in issue_summary
                for token in (
                    "blurry",
                    "video",
                    "audio",
                    "sound",
                    "voice",
                    "hear",
                    "network",
                    "lag",
                    "freeze",
                )
            )
        ):
            args["failing_feature"] = "live_media_quality"
        if not _atlas_s(args.get("symptom")) and issue_summary:
            args["symptom"] = issue_summary
    if out.get("tool") == "open_study_for_weak_topic" and not _atlas_s(args.get("query")):
        weak_topics = student_memory.get("weak_topics")
        if isinstance(weak_topics, list):
            args["query"] = _atlas_s(weak_topics[0] if weak_topics else "")
    out["args"] = args
    return out


def _atlas_should_skip_action(
    action: dict[str, Any],
    *,
    index: int,
    actions: list[dict[str, Any]],
    context: dict[str, Any],
) -> bool:
    tool = _atlas_s(action.get("tool")).lower()
    later_tools = {
        _atlas_s(item.get("tool")).lower()
        for item in actions[index + 1 :]
        if isinstance(item, dict)
    }
    selected_material = _atlas_selected_material(context)
    if tool in {"find_study_material", "list_study_materials"} and later_tools.intersection(
        {
            "open_material",
            "download_material",
            "open_material_formula_sheet",
            "open_material_flashcards",
            "open_material_revision_plan",
            "quiz_me_on_material",
            "open_related_materials",
            "summarize_material_with_ai",
            "make_notes_from_material",
            "ask_material_ai",
            "open_material_notes",
            "open_material_summary",
        }
    ):
        return bool(_atlas_s(selected_material.get("material_id")) or _atlas_s(action.get("args", {}).get("query")))
    if tool in {"list_pending_homeworks", "show_remaining_work"} and later_tools.intersection(
        {"open_homework", "open_homework_dashboard"}
    ):
        return True
    if tool in {"list_pending_exams", "show_remaining_work"} and later_tools.intersection(
        {"open_exam", "open_exam_dashboard"}
    ):
        return True
    if tool == "get_next_scheduled_class" and later_tools.intersection(
        {"join_next_class", "open_classes_hub"}
    ):
        return True
    return False


def _atlas_optimize_actions(
    actions: list[dict[str, Any]],
    *,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    optimized: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    removed_ids: set[str] = set()
    for index, raw_action in enumerate(actions):
        action = _atlas_fill_action_args(raw_action, context=context)
        if _atlas_should_skip_action(action, index=index, actions=actions, context=context):
            removed_ids.add(_atlas_s(action.get("id")))
            continue
        key = json.dumps(
            {
                "tool": _atlas_s(action.get("tool")).lower(),
                "args": action.get("args") if isinstance(action.get("args"), dict) else {},
            },
            sort_keys=True,
        )
        if key in seen_keys:
            removed_ids.add(_atlas_s(action.get("id")))
            continue
        seen_keys.add(key)
        optimized.append(action)
    for action in optimized:
        depends_on = action.get("depends_on")
        if isinstance(depends_on, list):
            action["depends_on"] = [
                _atlas_s(item)
                for item in depends_on
                if _atlas_s(item) and _atlas_s(item) not in removed_ids
            ]
    return optimized[:4]


def _student_atlas_follow_up_plan(
    instruction: str,
    *,
    context: dict[str, Any],
    allowed_tools: set[str],
    signals: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    lowered = _atlas_s(
        (signals or {}).get("normalized_instruction")
    ) or instruction.lower()
    selected_material = context.get("selected_material")
    has_selected_material = isinstance(selected_material, dict) and bool(
        _atlas_s(selected_material.get("material_id"))
        or _atlas_s(selected_material.get("title"))
    )
    if ("this material" in lowered or "selected material" in lowered) and not has_selected_material:
        return {
            "summary": "Atlas can use the Study library, but it still needs to know which material you mean.",
            "student_notice": "Reply in the same Atlas chat and it will continue.",
            "needs_more_info": True,
            "follow_up_questions": [
                "Which study material should I use? You can give the title, chapter, or subject.",
            ],
            "proposed_tools": [
                tool
                for tool in (
                    "find_study_material",
                    "open_material",
                    "download_material",
                    "open_material_formula_sheet",
                    "open_material_revision_plan",
                    "quiz_me_on_material",
                )
                if tool in allowed_tools
            ],
            "actions": [],
        }
    if ("download" in lowered or "open" in lowered) and "material" in lowered and not has_selected_material:
        return {
            "summary": "Atlas can open or download study material once it knows which item you want.",
            "student_notice": "Reply in the same Atlas chat and it will continue.",
            "needs_more_info": True,
            "follow_up_questions": [
                "Which material should I open or download? You can give the title, chapter, or subject.",
            ],
            "proposed_tools": [
                tool
                for tool in ("find_study_material", "open_material", "download_material")
                if tool in allowed_tools
            ],
            "actions": [],
        }
    if ("homework" in lowered or "exam" in lowered) and (
        "detail" in lowered or "open" in lowered
    ):
        return {
            "summary": "Atlas can fetch the right homework or exam once it knows which one you mean.",
            "student_notice": "Reply in the same Atlas chat and it will continue.",
            "needs_more_info": True,
            "follow_up_questions": [
                "Which homework or exam should I use? You can give the title or topic.",
            ],
            "proposed_tools": [
                tool
                for tool in (
                    "get_homework_details",
                    "get_exam_details",
                    "open_homework",
                    "open_exam",
                )
                if tool in allowed_tools
            ],
            "actions": [],
        }
    return None


def _teacher_atlas_follow_up_plan(
    instruction: str,
    *,
    context: dict[str, Any],
    allowed_tools: set[str],
    signals: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    lowered = _atlas_s(
        (signals or {}).get("normalized_instruction")
    ) or instruction.lower()
    selected_student = (
        dict(context.get("selected_student"))
        if isinstance(context.get("selected_student"), dict)
        else {}
    )
    has_student = bool(
        _atlas_s(selected_student.get("student_name"))
        or _atlas_s(selected_student.get("name"))
        or _atlas_s(selected_student.get("student_id"))
    )
    if "student" in lowered and (
        "profile" in lowered or "analytics" in lowered or "performance" in lowered
    ) and not has_student:
        return {
            "summary": "Atlas can open or summarize a student profile once it knows which student you mean.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue.",
            "needs_more_info": True,
            "follow_up_questions": [
                "Which student should I open or summarize? You can give the student's name.",
            ],
            "proposed_tools": [
                tool
                for tool in (
                    "open_teacher_student_profile",
                    "get_teacher_student_profile_summary",
                    "get_teacher_student_history_detail",
                )
                if tool in allowed_tools
            ],
            "actions": [],
        }
    if (
        "schedule" in lowered
        or "next class" in lowered
        or "recurring" in lowered
        or "repeat" in lowered
    ):
        needs_time = "time_reference" in ((signals or {}).get("missing_detail_hints") or []) or not _atlas_has_time_hint(instruction)
        if needs_time:
            return {
                "summary": "Atlas can schedule the class once the time is clear.",
                "teacher_notice": "Reply in the same Atlas chat and it will continue.",
                "needs_more_info": True,
                "follow_up_questions": [
                    "When should the class start? You can say a date and time like tomorrow 6 PM.",
                ],
                "proposed_tools": [
                    tool
                    for tool in (
                        "schedule_next_class",
                        "create_recurring_class_plan",
                        "check_schedule_conflicts",
                    )
                    if tool in allowed_tools
                ],
                "actions": [],
            }
    if (
        "quiz" in lowered
        or "exam" in lowered
        or "homework" in lowered
        or "material" in lowered
    ) and not any(
        token in lowered
        for token in (
            "binomial",
            "permutation",
            "combination",
            "physics",
            "chemistry",
            "mathematics",
            "electrostatics",
            "thermodynamics",
        )
    ):
        return {
            "summary": "Atlas can prepare the right draft once it knows the topic or chapter.",
            "teacher_notice": "Reply in the same Atlas chat and it will continue.",
            "needs_more_info": True,
            "follow_up_questions": [
                "What topic, chapter, or subject should I use for this teacher action?",
            ],
            "proposed_tools": [
                tool
                for tool in (
                    "open_teacher_create_quiz",
                    "create_exam_assignment",
                    "create_homework_assignment",
                    "open_teacher_add_material",
                )
                if tool in allowed_tools
            ],
            "actions": [],
        }
    planning_intent = "plan" in ((signals or {}).get("action_modes") or []) or any(
        phrase in lowered
        for phrase in (
            "study plan",
            "revision plan",
            "revision roadmap",
            "roadmap",
            "next best action",
            "what should i do next",
            "what should we do next",
            "plan for",
        )
    )
    if planning_intent:
        primary_query = _atlas_primary_study_query(instruction, signals=signals)
        if not primary_query:
            return {
                "summary": "Atlas can build a stronger teacher-side study plan once the topic is clear.",
                "teacher_notice": "Reply in the same Atlas chat with the topic or chapter and Atlas will continue.",
                "needs_more_info": True,
                "follow_up_questions": [
                    "Which topic, chapter, or subject should I use for the teacher study plan?",
                ],
                "proposed_tools": [
                    tool
                    for tool in (
                        "open_teacher_add_material",
                        "generate_teacher_quiz_draft",
                        "get_teacher_class_performance_summary",
                    )
                    if tool in allowed_tools
                ],
                "actions": [],
            }
        actions: list[dict[str, Any]] = []
        if "open_teacher_add_material" in allowed_tools:
            material_action = _atlas_synthesized_action(
                "open_teacher_add_material",
                instruction=instruction,
                context=context,
                step_id="step_1",
            )
            material_action["title"] = "Open study material composer"
            material_action["detail"] = (
                f"Prepare study material support for {primary_query}."
            )
            material_action.setdefault("args", {})
            material_action["args"].setdefault("topic", primary_query)
            actions.append(material_action)
        if "generate_teacher_quiz_draft" in allowed_tools:
            quiz_action = _atlas_synthesized_action(
                "generate_teacher_quiz_draft",
                instruction=instruction,
                context=context,
                step_id=f"step_{len(actions) + 1}",
                depends_on=[actions[-1]["id"]] if actions else [],
            )
            quiz_action["title"] = "Generate teacher quiz draft"
            quiz_action["detail"] = f"Draft a teacher quiz for {primary_query}."
            quiz_action.setdefault("args", {})
            quiz_action["args"].setdefault("topic", primary_query)
            quiz_action["args"].setdefault("title", f"{primary_query} Quiz")
            actions.append(quiz_action)
        if actions:
            return {
                "type": "single_action" if len(actions) == 1 else "multi_step_plan",
                "goal": instruction,
                "summary": "Atlas composed a teacher-side study-support plan from your topic request.",
                "teacher_notice": "Atlas is using the real teacher authoring tools instead of returning a generic answer.",
                "actions": actions,
                "steps": actions,
                "proposed_tools": [action["tool"] for action in actions],
                "recovery_mode": "instruction_signals",
            }
    return None


def _student_atlas_plan_from_result(
    *,
    result: dict[str, Any],
    instruction: str,
    allowed_tools: set[str],
) -> dict[str, Any]:
    answer, explanation = _atlas_extract_answer(result)
    payload = _atlas_decode_json_payload(answer) or _atlas_decode_json_payload(explanation)
    if not isinstance(payload, dict):
        payload = {}
    follow_up_questions = _atlas_normalize_text_list(payload.get("follow_up_questions"))
    proposed_tools = [
        tool
        for tool in _atlas_normalize_text_list(payload.get("proposed_tools"))
        if tool in allowed_tools
    ]
    plan_type = _atlas_plan_type(payload, follow_up_questions=follow_up_questions)
    if plan_type == "single_action":
        raw_steps: Any = [payload]
    elif plan_type == "multi_step_plan":
        raw_steps = payload.get("steps", payload.get("actions"))
    else:
        raw_steps = []
    actions = _atlas_coerce_actions(raw_steps, allowed_tools=allowed_tools)
    planner_context = result.get("planner_context") if isinstance(result.get("planner_context"), dict) else {}
    instruction_signals = (
        dict(planner_context.get("instruction_signals"))
        if isinstance(planner_context.get("instruction_signals"), dict)
        else _atlas_build_instruction_signals(
            instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
            role="student",
        )
    )
    actions = _atlas_optimize_actions(actions, context=planner_context)
    needs_more_info = plan_type in {"needs_more_info", "clarification_request"} or bool(
        payload.get("needs_more_info")
    )
    if not actions and explanation:
        recovered_actions, recovered_tools = _atlas_recover_actions_from_explanation(
            explanation=explanation,
            instruction=instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
        )
        if recovered_actions:
            actions = _atlas_optimize_actions(recovered_actions, context=planner_context)
            if actions:
                payload.setdefault(
                    "type",
                    "single_action" if len(actions) == 1 else "multi_step_plan",
                )
                payload.setdefault("goal", instruction)
                payload.setdefault(
                    "summary",
                    _atlas_summary_from_explanation(explanation) or instruction,
                )
                payload["recovery_mode"] = "tool_mentions_from_reasoning"
                proposed_tools = recovered_tools
                plan_type = _atlas_plan_type(payload, follow_up_questions=follow_up_questions)
    if not actions and not (needs_more_info or follow_up_questions):
        synthesized = _atlas_synthesize_plan_from_signals(
            instruction=instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
            role="student",
            signals=instruction_signals,
        )
        if isinstance(synthesized, dict):
            payload = synthesized
            follow_up_questions = _atlas_normalize_text_list(payload.get("follow_up_questions"))
            proposed_tools = [
                tool
                for tool in _atlas_normalize_text_list(payload.get("proposed_tools"))
                if tool in allowed_tools
            ]
            plan_type = _atlas_plan_type(payload, follow_up_questions=follow_up_questions)
            if plan_type == "single_action":
                raw_steps = [payload]
            elif plan_type == "multi_step_plan":
                raw_steps = payload.get("steps", payload.get("actions"))
            else:
                raw_steps = []
            actions = _atlas_coerce_actions(raw_steps, allowed_tools=allowed_tools)
            actions = _atlas_optimize_actions(actions, context=planner_context)
            needs_more_info = plan_type in {"needs_more_info", "clarification_request"} or bool(
                payload.get("needs_more_info")
            )
    if not actions and not (needs_more_info or follow_up_questions):
        fallback = _student_atlas_follow_up_plan(
            instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
            signals=instruction_signals,
        )
        if fallback is not None:
            payload = fallback
            plan_type = "needs_more_info"
            needs_more_info = True
            follow_up_questions = _atlas_normalize_text_list(
                fallback.get("follow_up_questions")
            )
            proposed_tools = _atlas_normalize_text_list(fallback.get("proposed_tools"))
            actions = []
        else:
            raise HTTPException(status_code=502, detail="Student Atlas returned no executable actions")
    if not proposed_tools and actions:
        proposed_tools = list(
            dict.fromkeys(
                action["tool"]
                for action in actions
                if _atlas_s(action.get("tool"))
            )
        )
    response: dict[str, Any] = {
        "type": plan_type,
        "goal": _atlas_s(payload.get("goal")) or instruction,
        "plan_id": _atlas_plan_id(payload, plan_type=plan_type),
        "instruction": instruction,
        "summary": _atlas_s(payload.get("summary")) or _atlas_s(payload.get("goal")) or answer or explanation,
        "student_notice": _atlas_s(payload.get("student_notice"))
        or _atlas_s(payload.get("teacher_notice")),
        "requires_confirmation": bool(payload.get("requires_confirmation")),
        "needs_more_info": needs_more_info or bool(follow_up_questions),
        "follow_up_questions": follow_up_questions,
        "proposed_tools": proposed_tools,
        "actions": actions,
        "recovery_mode": _atlas_s(payload.get("recovery_mode")),
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


def _teacher_atlas_plan_from_result(
    *,
    result: dict[str, Any],
    instruction: str,
    allowed_tools: set[str],
) -> dict[str, Any]:
    answer, explanation = _atlas_extract_answer(result)
    payload = _atlas_decode_json_payload(answer) or _atlas_decode_json_payload(explanation)
    if not isinstance(payload, dict):
        payload = {}
    follow_up_questions = _atlas_normalize_text_list(payload.get("follow_up_questions"))
    proposed_tools = [
        tool
        for tool in _atlas_normalize_text_list(payload.get("proposed_tools"))
        if tool in allowed_tools
    ]
    plan_type = _atlas_plan_type(payload, follow_up_questions=follow_up_questions)
    if plan_type == "single_action":
        raw_steps: Any = [payload]
    elif plan_type == "multi_step_plan":
        raw_steps = payload.get("steps", payload.get("actions"))
    else:
        raw_steps = []
    actions = _atlas_coerce_actions(raw_steps, allowed_tools=allowed_tools)
    planner_context = result.get("planner_context") if isinstance(result.get("planner_context"), dict) else {}
    instruction_signals = (
        dict(planner_context.get("instruction_signals"))
        if isinstance(planner_context.get("instruction_signals"), dict)
        else _atlas_build_instruction_signals(
            instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
            role="teacher",
        )
    )
    actions = _atlas_optimize_actions(actions, context=planner_context)
    needs_more_info = plan_type in {"needs_more_info", "clarification_request"} or bool(
        payload.get("needs_more_info")
    )
    if not actions and explanation:
        recovered_actions, recovered_tools = _atlas_recover_actions_from_explanation(
            explanation=explanation,
            instruction=instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
        )
        if recovered_actions:
            actions = _atlas_optimize_actions(recovered_actions, context=planner_context)
            if actions:
                payload.setdefault(
                    "type",
                    "single_action" if len(actions) == 1 else "multi_step_plan",
                )
                payload.setdefault("goal", instruction)
                payload.setdefault(
                    "summary",
                    _atlas_summary_from_explanation(explanation) or instruction,
                )
                payload["recovery_mode"] = "tool_mentions_from_reasoning"
                proposed_tools = recovered_tools
                plan_type = _atlas_plan_type(payload, follow_up_questions=follow_up_questions)
    if not actions and not (needs_more_info or follow_up_questions):
        synthesized = _atlas_synthesize_plan_from_signals(
            instruction=instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
            role="teacher",
            signals=instruction_signals,
        )
        if isinstance(synthesized, dict):
            payload = synthesized
            follow_up_questions = _atlas_normalize_text_list(payload.get("follow_up_questions"))
            proposed_tools = [
                tool
                for tool in _atlas_normalize_text_list(payload.get("proposed_tools"))
                if tool in allowed_tools
            ]
            plan_type = _atlas_plan_type(payload, follow_up_questions=follow_up_questions)
            if plan_type == "single_action":
                raw_steps = [payload]
            elif plan_type == "multi_step_plan":
                raw_steps = payload.get("steps", payload.get("actions"))
            else:
                raw_steps = []
            actions = _atlas_coerce_actions(raw_steps, allowed_tools=allowed_tools)
            actions = _atlas_optimize_actions(actions, context=planner_context)
            needs_more_info = plan_type in {"needs_more_info", "clarification_request"} or bool(
                payload.get("needs_more_info")
            )
    if not actions and not (needs_more_info or follow_up_questions):
        fallback = _teacher_atlas_follow_up_plan(
            instruction,
            context=planner_context,
            allowed_tools=allowed_tools,
            signals=instruction_signals,
        )
        if fallback is not None:
            payload = fallback
            follow_up_questions = _atlas_normalize_text_list(
                fallback.get("follow_up_questions")
            )
            proposed_tools = [
                tool
                for tool in _atlas_normalize_text_list(fallback.get("proposed_tools"))
                if tool in allowed_tools
            ]
            plan_type = _atlas_plan_type(
                payload,
                follow_up_questions=follow_up_questions,
            )
            if plan_type == "single_action":
                raw_steps = [payload]
            elif plan_type == "multi_step_plan":
                raw_steps = payload.get("steps", payload.get("actions"))
            else:
                raw_steps = []
            actions = _atlas_coerce_actions(raw_steps, allowed_tools=allowed_tools)
            actions = _atlas_optimize_actions(actions, context=planner_context)
            needs_more_info = plan_type in {"needs_more_info", "clarification_request"} or bool(
                payload.get("needs_more_info")
            )
            if not actions and plan_type not in {"needs_more_info", "clarification_request"}:
                plan_type = "needs_more_info"
                needs_more_info = True
        else:
            raise HTTPException(status_code=502, detail="Teacher Atlas returned no executable actions")
    if not proposed_tools and actions:
        proposed_tools = list(
            dict.fromkeys(
                action["tool"]
                for action in actions
                if _atlas_s(action.get("tool"))
            )
        )
    response: dict[str, Any] = {
        "type": plan_type,
        "goal": _atlas_s(payload.get("goal")) or instruction,
        "plan_id": _atlas_plan_id(payload, plan_type=plan_type),
        "instruction": instruction,
        "summary": _atlas_s(payload.get("summary")) or _atlas_s(payload.get("goal")) or answer or explanation,
        "teacher_notice": _atlas_s(payload.get("teacher_notice"))
        or _atlas_s(payload.get("student_notice")),
        "requires_confirmation": bool(payload.get("requires_confirmation")),
        "needs_more_info": needs_more_info or bool(follow_up_questions),
        "follow_up_questions": follow_up_questions,
        "proposed_tools": proposed_tools,
        "actions": actions,
        "recovery_mode": _atlas_s(payload.get("recovery_mode")),
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


@router.post("/app/action")
async def app_action(request: Request, req: dict[str, Any]):
    payload = dict(req)
    payload["_request_base_url"] = str(request.base_url).rstrip("/")
    action = str(payload.get("action") or "").strip()
    if not action:
        raise HTTPException(status_code=400, detail="Missing action")
    try:
        return await _APP_DATA.handle_action(payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"App action error: {exc}")


@router.get("/app/action")
async def app_action_get(request: Request):
    payload: dict[str, Any] = {
        key: value for key, value in request.query_params.multi_items()
    }
    payload["_request_base_url"] = str(request.base_url).rstrip("/")
    action = str(payload.get("action") or "").strip()
    if not action:
        raise HTTPException(status_code=400, detail="Missing action")
    try:
        return await _APP_DATA.handle_action(payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"App action error: {exc}")


@router.websocket("/app/live_class_schedule/events")
async def app_live_class_schedule_events(websocket: WebSocket):
    await websocket.accept()
    queue = _APP_DATA.subscribe_live_class_schedule_events()
    try:
        await websocket.send_json(
            {
                "type": "connected",
                "timestamp": asyncio.get_running_loop().time(),
            }
        )
        while True:
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=25.0)
                await websocket.send_json(payload)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "heartbeat"})
    except WebSocketDisconnect:
        pass
    finally:
        _APP_DATA.unsubscribe_live_class_schedule_events(queue)


@router.get("/app/file/{file_id}")
async def app_file(file_id: str):
    meta = await _APP_DATA.get_uploaded_file(file_id)
    if not meta:
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        path=meta["path"],
        media_type=meta.get("mime") or "application/octet-stream",
        filename=meta.get("name") or f"{file_id}.bin",
    )


@router.get("/app/quiz/{quiz_id}.csv")
async def app_quiz_csv(quiz_id: str):
    path = await _APP_DATA.get_quiz_csv_file(quiz_id)
    if not path:
        raise HTTPException(status_code=404, detail="Quiz not found")
    return FileResponse(
        path=path,
        media_type="text/csv",
        filename=f"{quiz_id}.csv",
    )


@router.get("/auth/health")
async def auth_health():
    smtp_sender = bool(
        (os.getenv("OTP_SENDER_EMAIL", "") or os.getenv("FORGOT_OTP_SENDER_EMAIL", "")).strip()
    )
    smtp_password = bool(os.getenv("OTP_SENDER_PASSWORD", "").strip())
    return {
        "ok": True,
        "status": "AUTH_BACKEND_READY",
        "smtp_configured": smtp_sender and smtp_password,
    }


@router.post("/auth/action")
async def auth_action(req: AuthActionRequest):
    payload: dict[str, Any] = req.model_dump(exclude_none=True)
    try:
        return await _AUTH.handle_action(payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Auth action error: {exc}")


@router.post("/ai/app/agent")
async def ai_app_agent(req: AppAtlasPlanRequest):
    instruction = _atlas_s(req.instruction)
    if not instruction:
        raise HTTPException(status_code=400, detail="Instruction cannot be empty")

    context = dict(req.context or {})
    atlas_role = _atlas_role(context, authority_level=req.authority_level)
    allowed_tools = _atlas_allowed_tools(context, role=atlas_role)
    account_id = _atlas_s(
        context.get("account_id")
        or context.get("student_id")
        or context.get("user_id")
    )
    tool_stats_summary = _APP_DATA.atlas_tool_stats_summary(limit=12)
    instruction_signals = _atlas_build_instruction_signals(
        instruction,
        context=context,
        allowed_tools=allowed_tools,
        role=atlas_role,
    )
    context["instruction_signals"] = instruction_signals
    if atlas_role == "student":
        fallback_profile = (
            dict(context.get("student_profile"))
            if isinstance(context.get("student_profile"), dict)
            else {}
        )
        student_memory = _APP_DATA.build_atlas_student_memory(
            account_id=account_id,
            fallback_profile=fallback_profile,
            recent_context=context,
        ) if account_id else fallback_profile
        exploration_mode = _atlas_should_explore(
            account_id=account_id,
            instruction=instruction,
        )
        tool_stats_summary = {
            **tool_stats_summary,
            "exploration_mode": exploration_mode,
        }
        passive_events = _APP_DATA.atlas_passive_events(
            account_id=account_id,
            context={**context, "student_profile": student_memory},
        ) if account_id else []
        context["student_memory"] = student_memory
        context["tool_stats_summary"] = tool_stats_summary
        context["recent_actions"] = (
            student_memory.get("recent_actions")
            if isinstance(student_memory.get("recent_actions"), list)
            else []
        )
        context["passive_events"] = passive_events
        signal_prompt = _atlas_instruction_signal_prompt(instruction_signals)
        planning_directive = (
            "You are Atlas, an advanced student-side classroom copilot built on LalaCore. "
        "You are a planner-executor agent, not a simple tool caller. "
        "Understand the student's intent, decide whether it needs a single action or a multi-step plan, "
        "and output strict JSON only.\n\n"
        "You must use PLAN -> EXECUTE -> OBSERVE -> ADAPT reasoning internally. "
        "Break complex goals into atomic tool steps, add dependencies only when needed, "
        "avoid redundant steps, and never hallucinate tools.\n\n"
        "Human-language understanding hints for this request:\n"
        f"{signal_prompt}\n\n"
        "Student role rules:\n"
        "- You are helping a student, not a teacher.\n"
        "- Never propose teacher moderation, schedule-editing, or class-control tools.\n"
        "- Only use the student's own dashboard, Study library, class schedule, results, recordings, and notifications.\n\n"
        f"Allowed tools: {', '.join(sorted(allowed_tools))}.\n\n"
        "Adaptive planning rules:\n"
        "- Prefer tools with higher confidence_score and lower avg_latency_ms from tool_stats_summary.\n"
        "- Avoid tools with recent failures unless no reasonable alternative exists.\n"
        "- If exploration_mode is true, you may use one exploration_candidate for a low-risk action to avoid overfitting to the same tool every time.\n"
        "- Never use exploration mode for risky or destructive actions.\n"
        "- Bias toward the student's preferred_actions and recent study history when choosing Study actions.\n"
        "- If student_memory shows last_material_id or last_subject, use that to reduce unnecessary follow-up.\n"
        "- If snapshot data already contains what you need, skip redundant fetch/list steps.\n"
        "- Keep plans minimal, compressed, and logically ordered.\n\n"
        'If one action is enough, return strict JSON: {"type":"single_action","goal":"<student_goal>","plan_id":"<unique_id>","summary":"<short summary>","student_notice":"<optional note>","requires_confirmation":false,"tool":"<tool_name>","title":"<short title>","detail":"<short detail>","risk":"low|medium|high","args":{...}}. '
        'If multiple actions are needed, return strict JSON: {"type":"multi_step_plan","goal":"<student_goal>","plan_id":"<unique_id>","summary":"<short summary>","student_notice":"<optional note>","requires_confirmation":false,"steps":[{"id":"step_1","tool":"<tool_name>","title":"<short title>","detail":"<short detail>","risk":"low|medium|high","args":{...},"depends_on":["<optional step ids>"],"on_failure":{"strategy":"retry|replan|fallback","fallback_tool":"<optional_tool>"}}]}. '
        'If essential information is missing, return strict JSON: {"type":"needs_more_info","goal":"<student_goal>","summary":"<short summary>","student_notice":"<optional note>","requires_confirmation":false,"needs_more_info":true,"follow_up_questions":["..."],"proposed_tools":["..."],"actions":[]}. '
        "Keep multi-step plans minimal and logically ordered, usually 2 to 4 steps, and never exceed 4 steps. "
        "If you conceptually parallelize independent work, keep it to at most 2 parallel groups. "
        "Creation or retrieval must happen before opening or downloading. "
        "Listing must happen before asking the student to choose a specific item. "
        "If the student says 'this material', use selected_material from context when available; otherwise ask a follow-up question.\n\n"
        "Use the app snapshot in context: selected material, study materials, live-class artifacts, scheduled classes, pending work, notifications, recent results, and student profile. "
        "Prefer concrete app actions over generic advice. "
        "For broad study-planning, revision, or 'what should I do next' requests, compose a real plan using overview, weak-topic, due-work, study-library, and next-best-task tools instead of refusing the request because there is no tool literally named 'study plan'.\n\n"
        "For list_pending_homeworks, list_pending_exams, show_remaining_work, list_due_today, list_due_this_week, show_completion_summary, show_attempted_vs_pending, show_subject_breakdown, get_study_overview, get_live_class_overview, get_notifications_summary, get_recent_scores, get_results_history, get_weak_topics, and suggest_next_best_task, args may stay empty. "
        "For get_homework_details, get_exam_details, open_homework, open_exam, find_study_material, get_material_details, open_material, download_material, open_material_formula_sheet, open_material_flashcards, open_material_revision_plan, quiz_me_on_material, open_related_materials, summarize_material_with_ai, make_notes_from_material, and ask_material_ai, include material_id, quiz_id, title, or query when known. "
        "For open_latest_material, open_latest_non_live_material, open_latest_live_class_material, download_latest_material, open_latest_material_notes, open_latest_material_summary, open_latest_material_formula_sheet, open_latest_material_flashcards, open_latest_material_revision_plan, quiz_me_on_latest_material, open_study_for_next_class, and open_study_for_weak_topic, Atlas may rely on the latest material, next class, or weak-topic context when the student does not name a specific item. "
        "For play_last_class_recording, open_latest_replay, open_latest_notes, open_latest_flashcards, open_latest_practice, open_latest_transcript_digest, open_latest_whiteboard_digest, open_last_class_notes, open_last_class_flashcards, open_last_class_practice, open_last_class_transcript, open_last_class_whiteboard, open_recording_bundle, open_latest_teacher_report, open_last_class_teacher_report, open_latest_class_activity_digest, open_last_class_activity_digest, and open_last_class_bundle, Atlas may rely on the latest live-class artifact context if present. "
        "For get_next_scheduled_class, list_upcoming_classes, join_next_class, open_classes_hub, get_classes_today, get_live_now_classes, open_join_options_for_next_class, open_homework_dashboard, open_exam_dashboard, and get_last_class_bundle_details, use the schedule and dashboard context already provided. "
        "For list_subject_materials and list_chapter_materials, include a subject, chapter, or query string whenever possible. "
        "For open_chats_inbox, open_chat_thread, open_doubt_thread, get_chat_directory_summary, show_recent_chat_threads, show_unread_chat_threads, search_chat_messages, summarize_chat_thread, summarize_chat_last_day, send_chat_message, send_chat_attachment, create_chat_poll, reply_to_doubt_thread, react_to_chat_message, vote_in_chat_poll, pin_chat_message, unpin_chat_message, get_open_doubts, get_resolved_doubts, show_recent_doubt_updates, and show_doubt_status_summary, use chat_directory and doubts context from the app snapshot when present. "
        "For send_chat_message and reply_to_doubt_thread, include target thread, peer, or query plus the message text whenever possible. "
        "For send_chat_attachment, only use it when atlas_latest_attachment is present in context; include the target thread, peer, or query whenever possible. "
        "For create_chat_poll, include the poll question and at least two options; Atlas may use the latest matching thread when the target chat is obvious. "
        "For react_to_chat_message, include an emoji and a thread, peer, or query; if the teacher or student does not identify a specific message, Atlas may react to the latest visible message in that thread. "
        "For vote_in_chat_poll, include a thread and either option_index, option_label, or answer text when possible; if there is only one recent open poll in the target thread, Atlas may use it. "
        "For pin_chat_message and unpin_chat_message, include the target thread and message hint when possible; Atlas may use the latest visible message if the intent is clear. "
        "For summarize_chat_thread and summarize_chat_last_day, include attachment-aware summaries so images, PDFs, voice notes, polls, and cards are reflected in the result. "
        "For open_study_ai_hub, open_ai_chat_history, open_latest_ai_chat, open_new_ai_chat, show_ai_chat_history_summary, and show_pinned_ai_chats, use ai_chat_sessions from the snapshot. "
        "For report_system_issue, use it when the student is clearly describing lag, crashes, broken screens, failed downloads, blurry live video, poor audio, media quality issues, missing AI output, or any feature not working properly. Include issue_summary, failing feature, surface, and any visible error or symptom text when possible. Prefer this tool over generic advice when the user wants troubleshooting or escalation. "
        "For get_unread_notification_count, show_high_priority_notifications, show_ai_notifications, mark_all_notifications_seen, refresh_notifications, open_latest_notification, open_notifications_center_unread, open_notifications_center_high_priority, and open_notifications_center_ai, use notifications from the snapshot and prefer lightweight actions before navigation. "
        "For open_latest_result_analytics, open_result_analytics_for_assessment, open_latest_result_review, open_result_review_for_assessment, open_latest_answer_key, open_answer_key_for_assessment, open_latest_mistake_review, open_mistake_review_for_assessment, open_latest_result_attempt_history, open_result_attempt_history_for_assessment, open_latest_completed_homework, open_latest_completed_exam, open_latest_pending_homework, open_latest_pending_exam, open_homework_dashboard_pending, open_homework_dashboard_completed, open_exam_dashboard_pending, open_exam_dashboard_completed, show_assessment_attempt_history, list_ai_practice_quizzes, open_latest_ai_practice_quiz, open_latest_ai_practice_result, open_ai_practice_history, and reattempt_latest_ai_practice, use assessments and recent_results together and prefer the latest matching assessment when the student does not specify one exactly. "
        "For open_classes_hub_today, open_classes_hub_live, and open_classes_hub_upcoming, use schedule context and prefer direct class-hub filters over generic summaries. "
        "For open_self_practice_quiz_builder, args can stay empty. "
        "Do not guess item ids when there are multiple close matches; ask for clarification instead."
        )
    else:
        student_memory = {}
        exploration_mode = False
        passive_events = []
        context["tool_stats_summary"] = tool_stats_summary
        signal_prompt = _atlas_instruction_signal_prompt(instruction_signals)
        planning_directive = (
            "You are Atlas, an advanced teacher-side dashboard copilot built on LalaCore. "
            "You are a planner-executor agent, not a simple tool caller. "
            "Understand the teacher's intent, decide whether it needs a single action or a multi-step plan, "
            "and output strict JSON only.\n\n"
            "You must use PLAN -> EXECUTE -> OBSERVE -> ADAPT reasoning internally. "
            "Break complex goals into atomic tool steps, add dependencies only when needed, "
            "avoid redundant steps, and never hallucinate tools.\n\n"
            "Human-language understanding hints for this request:\n"
            f"{signal_prompt}\n\n"
            "Teacher role rules:\n"
            "- You are helping a teacher inside the teacher dashboard.\n"
            "- You may navigate quizzes, study material, classes, student analytics, chats, and live-class entry flows.\n"
            "- You may prepare schedule, homework, exam, and Study publishing actions when the data is clear.\n"
            "- Prefer opening prefilled screens for quiz/material authoring when exact final content still needs teacher review.\n\n"
            f"Allowed tools: {', '.join(sorted(allowed_tools))}.\n\n"
            "Planning rules:\n"
            "- Prefer the fewest steps that complete the teacher's workflow.\n"
            "- If the request mixes navigation and creation, prepare or create first, then navigate or announce.\n"
            "- Reuse teacher dashboard context: assessments, study materials, schedule, students, chats, doubts, notifications, and selected student when present.\n"
            "- For broad workflow intents, compose the plan from the closest real teacher tools instead of failing because there is no exact tool-name match.\n"
            "- Use tool_stats_summary to prefer reliable tools.\n\n"
            'If one action is enough, return strict JSON: {"type":"single_action","goal":"<teacher_goal>","plan_id":"<unique_id>","summary":"<short summary>","teacher_notice":"<optional note>","requires_confirmation":false,"tool":"<tool_name>","title":"<short title>","detail":"<short detail>","risk":"low|medium|high","args":{...}}. '
            'If multiple actions are needed, return strict JSON: {"type":"multi_step_plan","goal":"<teacher_goal>","plan_id":"<unique_id>","summary":"<short summary>","teacher_notice":"<optional note>","requires_confirmation":false,"steps":[{"id":"step_1","tool":"<tool_name>","title":"<short title>","detail":"<short detail>","risk":"low|medium|high","args":{...},"depends_on":["<optional step ids>"],"on_failure":{"strategy":"retry|replan|fallback","fallback_tool":"<optional_tool>"}}]}. '
            'If essential information is missing, return strict JSON: {"type":"needs_more_info","goal":"<teacher_goal>","summary":"<short summary>","teacher_notice":"<optional note>","requires_confirmation":false,"needs_more_info":true,"follow_up_questions":["..."],"proposed_tools":["..."],"actions":[]}. '
            "Keep multi-step plans minimal and logically ordered, usually 2 to 4 steps, and never exceed 4 steps. "
            "For open_teacher_student_profile, get_teacher_student_profile_summary, get_teacher_student_history_detail, and identify_teacher_attention_students, prefer selected_student from context when available. "
            "For get_teacher_class_performance_summary, use teacher_student_histories and recent_results together to summarize the whole class without requiring the teacher to navigate first. "
            "For open_teacher_result_detail, include title, query, quiz_id, or assessment when possible. "
            "For schedule_next_class, include title, subject, topic, class_name, start_time, and duration_minutes when they are known; otherwise ask a follow-up question. "
            "For create_recurring_class_plan, include start_time, occurrences, and interval_days or weekdays when known. "
            "For schedule_class_reminder, include class_id or enough identifying detail plus reminder_offsets_minutes. "
            "For check_schedule_conflicts, include start_time and duration_minutes when possible. "
            "For create_homework_assignment and create_exam_assignment, include title, subject, chapter or topic, class_name, duration_minutes, and deadline when possible. "
            "For generate_teacher_quiz_draft, use the existing teacher AI quiz pipeline semantics: include title, type, subject, chapters or topic, class_name, difficulty, question_count, duration_minutes, marks_per_question, total_marks, deadline, and pyq_focus when known. "
            "For import_teacher_quiz_from_attachment, use it when the teacher mentions an uploaded PDF or image and wants to extract/import questions into a reviewable draft. "
            "For revise_teacher_quiz_draft, use it when the teacher wants to modify a current draft: add/remove questions, change marks, edit statement/options/answers/solutions, or attach an uploaded image to a question. "
            "For preview_teacher_quiz_draft, use it when the teacher explicitly asks to review the current draft in chat before publishing. "
            "For publish_teacher_quiz_draft, only use it after the teacher clearly approves final publishing. "
            "For create_teacher_material_draft_from_attachment, use it when an uploaded PDF or image should become Study material through the normal teacher add-material flow. "
            "For publish_teacher_material_draft, only use it after the teacher clearly confirms publishing the prepared Study material draft. "
            "For open_chat_thread and open_doubt_thread, use teacher chats or doubt context already present in the snapshot and prefer the latest matching thread when the teacher names a student or group. "
            "For search_chat_messages, summarize_chat_thread, summarize_chat_last_day, send_chat_message, send_chat_attachment, create_chat_poll, reply_to_doubt_thread, react_to_chat_message, vote_in_chat_poll, pin_chat_message, and unpin_chat_message, reuse teacher chat and doubt context directly. "
            "For send_chat_message and reply_to_doubt_thread, include the intended recipient or thread plus the exact message text whenever possible. "
            "For send_chat_attachment, only use it when atlas_latest_attachment or teacher_latest_attachment is present in context; include the destination chat clearly. "
            "For create_chat_poll, include the question plus concrete options; Atlas may use the latest matching teacher chat when the target is obvious. "
            "For react_to_chat_message, include an emoji and a target thread; Atlas may use the latest visible message when the teacher does not identify a specific one. "
            "For vote_in_chat_poll, include the intended option when possible and prefer the latest open poll in the chosen chat. "
            "For pin_chat_message and unpin_chat_message, include the target thread and a message hint whenever possible. "
            "For report_system_issue, use it when the teacher describes lag, crashes, analytics failures, publish failures, blurry live video, broken audio, participant media issues, broken AI behavior, or any feature not working correctly. Include issue_summary, failing feature, surface, visible symptom, and any last error text when possible. Prefer this tool over generic troubleshooting advice when the teacher clearly wants diagnosis or escalation. "
            "For publish_followup_note_to_study, include title and body when the teacher provides them, otherwise prefer a short teacher review draft. "
            "For publish_resource_link_to_study, include url or source_url. "
            "Do not guess a student name or schedule time when multiple options are plausible; ask a follow-up question instead."
        )

    try:
        result = await lalacore_entry(
            input_data=f"{planning_directive}\n\n{atlas_role.title()} instruction:\n{instruction}",
            input_type="text",
            user_context={
                "app_surface": f"{atlas_role}_app",
                "role": atlas_role,
                "student_profile": student_memory,
                "student_memory": student_memory,
                "tool_stats_summary": tool_stats_summary,
                "instruction_signals": instruction_signals,
                "allowed_tools": sorted(allowed_tools),
                "exploration_mode": exploration_mode,
                "recent_actions": context.get("recent_actions")
                if isinstance(context.get("recent_actions"), list)
                else [],
                "passive_events": passive_events,
                "selected_material": context.get("selected_material")
                if isinstance(context.get("selected_material"), dict)
                else {},
                "snapshot_summary": context.get("snapshot_summary")
                if isinstance(context.get("snapshot_summary"), list)
                else [],
                "student_dashboard_context": context,
                "teacher_dashboard_context": context,
            },
            options={
                "function": f"{atlas_role}_app_agent",
                "app_surface": f"{atlas_role}_app",
                "return_structured": True,
                "return_markdown": False,
                "require_citations": False,
                "enable_verification_reevaluation": False,
                "meta_override_min_confidence": 0.0,
                "meta_override_max_risk": 1.0,
                "meta_override_max_disagreement": 1.0,
                "enable_pre_reasoning_context": True,
                "enable_graph_of_thought": True,
                "enable_mcts_reasoning": True,
                "enable_web_retrieval": False,
            },
        )
        if isinstance(result, dict):
            result["planner_context"] = context
        response = (
            _teacher_atlas_plan_from_result(
                result=result,
                instruction=instruction,
                allowed_tools=allowed_tools,
            )
            if atlas_role == "teacher"
            else _student_atlas_plan_from_result(
                result=result,
                instruction=instruction,
                allowed_tools=allowed_tools,
            )
        )
    except Exception as exc:
        synthesized = _atlas_synthesize_plan_from_signals(
            instruction=instruction,
            context=context,
            allowed_tools=allowed_tools,
            role=atlas_role,
            signals=instruction_signals,
        )
        if not isinstance(synthesized, dict):
            if "report_system_issue" in allowed_tools and "diagnose" in (
                instruction_signals.get("action_modes") or []
            ):
                synthesized = {
                    "type": "single_action",
                    "goal": instruction,
                    "summary": "Atlas planner backend is unavailable, so Atlas is falling back to direct issue diagnosis.",
                    "student_notice": (
                        "Atlas will try to repair the app/backend connection first."
                        if atlas_role == "student"
                        else ""
                    ),
                    "teacher_notice": (
                        "Atlas will try to repair the app/backend connection first."
                        if atlas_role == "teacher"
                        else ""
                    ),
                    "tool": "report_system_issue",
                    "title": "Report system issue",
                    "detail": instruction,
                    "risk": "low",
                    "args": {"issue_summary": instruction},
                    "recovery_mode": "planner_exception_fallback",
                }
        if not isinstance(synthesized, dict):
            raise HTTPException(
                status_code=503,
                detail=f"Atlas planner unavailable: {exc}",
            )
        fallback_result = {
            "answer": json.dumps(synthesized, ensure_ascii=False),
            "planner_context": context,
        }
        response = (
            _teacher_atlas_plan_from_result(
                result=fallback_result,
                instruction=instruction,
                allowed_tools=allowed_tools,
            )
            if atlas_role == "teacher"
            else _student_atlas_plan_from_result(
                result=fallback_result,
                instruction=instruction,
                allowed_tools=allowed_tools,
            )
        )
        response["planner_error"] = str(exc)
        response["planner_recovery_mode"] = "deterministic_signal_fallback"
    if atlas_role == "student":
        response["student_memory"] = student_memory
    response["tool_stats_summary"] = tool_stats_summary
    if atlas_role == "student":
        response["passive_events"] = passive_events
        response["exploration_mode"] = exploration_mode
    response["role"] = atlas_role
    return response


@router.post("/ai/app/agent/observe")
async def ai_app_agent_observe(req: AppAtlasObserveRequest):
    account_id = _atlas_s(req.account_id)
    context = dict(req.context or {})
    if not account_id:
        account_id = _atlas_s(
            context.get("account_id")
            or context.get("student_id")
            or context.get("user_id")
        )
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id is required")
    tool_name = _atlas_s(req.tool_name)
    if not tool_name:
        raise HTTPException(status_code=400, detail="tool_name is required")
    category = _atlas_s(req.category) or _atlas_tool_category(tool_name)
    return _APP_DATA.record_atlas_tool_execution(
        account_id=account_id,
        tool_name=tool_name,
        category=category,
        success=bool(req.success),
        latency_ms=max(0, int(req.latency_ms or 0)),
        context=context,
        args=dict(req.args or {}),
        observation=_atlas_s(req.observation),
    )


@router.post("/ai/app/agent/passive")
async def ai_app_agent_passive(req: AppAtlasPassiveRequest):
    context = dict(req.context or {})
    account_id = _atlas_s(req.account_id)
    if not account_id:
        account_id = _atlas_s(
            context.get("account_id")
            or context.get("student_id")
            or context.get("user_id")
        )
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id is required")
    fallback_profile = (
        dict(context.get("student_profile"))
        if isinstance(context.get("student_profile"), dict)
        else {}
    )
    student_memory = _APP_DATA.build_atlas_student_memory(
        account_id=account_id,
        fallback_profile=fallback_profile,
        recent_context=context,
    )
    tool_stats_summary = _APP_DATA.atlas_tool_stats_summary(limit=12)
    passive_events = _APP_DATA.atlas_passive_events(
        account_id=account_id,
        context={**context, "student_profile": student_memory},
    )
    return {
        "ok": True,
        "account_id": account_id,
        "student_memory": student_memory,
        "tool_stats_summary": tool_stats_summary,
        "passive_events": passive_events,
    }


# ==============================
# SOLVE ENDPOINT
# ==============================

@router.post("/solve", response_model=SolveResponse)
async def solve(req: SolveRequest):
    """
    Main Omega solve endpoint.
    """

    payload = req.input_data if req.input_data is not None else req.question
    if not payload or (isinstance(payload, str) and payload.strip() == ""):
        raise HTTPException(
            status_code=400,
            detail="Input cannot be empty"
        )

    try:
        used_type = req.input_type if req.input_data is not None else "text"
        result = await lalacore_entry(
            input_data=payload,
            input_type=used_type,
            user_context=req.user_context,
            options=req.options,
        )

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Solver error: {str(e)}"
        )


@router.post("/ai/question-search")
async def ai_question_search(req: QuestionSearchRequest):
    query = str(req.query or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    try:
        normalized = _QUESTION_NORMALIZER.normalize(query)
        results = await _QUESTION_SEARCH_ENGINE.search(
            normalized,
            max_matches=max(1, min(20, int(req.max_matches))),
        )
        return {
            "ok": True,
            "status": "SUCCESS",
            "query": query,
            "normalized_query": normalized,
            "matches": results.get("matches", []),
            "cache_hit": bool(results.get("cache_hit", False)),
            "query_variants": results.get("query_variants", []),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"question-search error: {exc}")


@router.post("/ops/weekly-evolution", response_model=WeeklyEvolutionResponse)
async def weekly_evolution():
    try:
        weekly = WeeklyEvolutionJob().run()
        datasets = ZaggleDatasetBuilder().build_all()
        return {"weekly": weekly, "datasets": datasets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Weekly evolution error: {str(e)}")


@router.post("/ops/feeder/add", response_model=FeederAddResponse)
async def feeder_add(req: FeederAddRequest):
    if not req.question or not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    try:
        return _FEEDER.enqueue_question(
            question=req.question,
            subject=req.subject,
            difficulty=req.difficulty,
            concept_cluster=req.concept_cluster,
            source_tag=req.source_tag,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feeder add error: {str(e)}")


@router.post("/ops/feeder/process")
async def feeder_process(req: FeederProcessRequest):
    try:
        return await _FEEDER.process_pending(max_items=req.max_items, trigger="manual")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feeder process error: {str(e)}")


@router.get("/ops/feeder/status", response_model=FeederStatusResponse)
async def feeder_status(limit: int = 20):
    try:
        return _FEEDER.status(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Feeder status error: {str(e)}")


@router.post("/ops/automation/run-weekly")
async def automation_run_weekly(req: AutomationRunRequest):
    try:
        return await _AUTOMATION.run_weekly(
            trigger=req.trigger,
            resume=req.resume,
            feeder_batch=req.feeder_batch,
            replay_batch=req.replay_batch,
            execute_replay_pipeline=req.execute_replay_pipeline,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Automation run error: {str(e)}")


@router.post("/ops/automation/tick")
async def automation_tick():
    try:
        return await _AUTOMATION.run_if_due()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Automation tick error: {str(e)}")


@router.post("/ops/atlas-maintenance/run")
async def atlas_maintenance_run(req: AtlasMaintenanceRunRequest):
    try:
        return await _ATLAS_MAINTENANCE.run_weekly_maintenance(
            trigger=req.trigger,
            recipient_email=req.recipient_email,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Atlas maintenance run error: {str(e)}")


@router.post("/ops/atlas-maintenance/tick")
async def atlas_maintenance_tick():
    try:
        return await _ATLAS_MAINTENANCE.run_if_due()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Atlas maintenance tick error: {str(e)}")


@router.get("/ops/atlas-maintenance/status")
async def atlas_maintenance_status():
    try:
        return {"ok": True, "status": "SUCCESS", **_ATLAS_MAINTENANCE.status_snapshot()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Atlas maintenance status error: {str(e)}")


@router.post("/ops/app-update-confirmation/run")
async def app_update_confirmation_run(req: AppUpdateConfirmationRunRequest):
    try:
        return await _APP_UPDATE_RELEASE_NOTIFIER.poll_for_new_releases(
            trigger=req.trigger,
            recipient_email=req.recipient_email,
            force_resend=req.force_resend,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"App update confirmation run error: {str(e)}",
        )


@router.post("/ops/app-update-confirmation/tick")
async def app_update_confirmation_tick():
    try:
        return await _APP_UPDATE_RELEASE_NOTIFIER.poll_for_new_releases(
            trigger="tick",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"App update confirmation tick error: {str(e)}",
        )


@router.get("/ops/app-update-confirmation/status")
async def app_update_confirmation_status():
    try:
        return {
            "ok": True,
            "status": "SUCCESS",
            **_APP_UPDATE_RELEASE_NOTIFIER.status_snapshot(),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"App update confirmation status error: {str(e)}",
        )
