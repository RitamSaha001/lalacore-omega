import asyncio
from typing import Any
import os

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
from services.question_normalizer import QuestionNormalizer
from services.question_search_engine import QuestionSearchEngine

router = APIRouter()
_FEEDER = FeederEngine()
_AUTOMATION = AutomationOrchestrator(feeder=_FEEDER)
_AUTH = LocalAuthService()
_APP_DATA = LocalAppDataService()
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


@router.post("/app/action")
async def app_action(req: dict[str, Any]):
    action = str(req.get("action") or "").strip()
    if not action:
        raise HTTPException(status_code=400, detail="Missing action")
    try:
        return await _APP_DATA.handle_action(req)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"App action error: {exc}")


@router.get("/app/action")
async def app_action_get(request: Request):
    payload: dict[str, Any] = {
        key: value for key, value in request.query_params.multi_items()
    }
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
    smtp_sender = bool(os.getenv("OTP_SENDER_EMAIL", "").strip())
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
