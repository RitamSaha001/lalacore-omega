from __future__ import annotations

import asyncio
import gc
import json
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Sequence

from engine.got_engine import GraphOfThoughtEngine
from engine.mcts_reasoner import MCTSSearch
from core.api.persona_layer import apply_persona
from core.lalacore_x.classifier import ProblemClassifier
from core.lalacore_x.plausibility_checker import check_answer_plausibility
from core.lalacore_x.providers import ProviderFabric
from core.lalacore_x.retrieval import ConceptVault
from core.lalacore_x.research_calibration import BayesianConfidenceAdjuster
from core.lalacore_x.research_verifier import ResearchMetaVerifier
from core.multimodal.intake import IntakePayload, MultimodalIntake
from core.multimodal.diagram_parser import DiagramParser as OCRDiagramParser
from core.multimodal.ocr_engine import OCREngine
from core.multimodal.pdf_processor import PDFProcessor
from core.multimodal.telemetry import DEFAULT_MULTIMODAL_TELEMETRY
from core.multimodal.vision_router import VisionRouter
from core.solver import solve_question
from core.visualization import DesmosGraphBuilder
from services.context_builder import RetrievalContextBuilder
from services.input_analyzer import InputAnalyzer
from services.mcts_logger import MCTSLogger
from services.question_normalizer import QuestionNormalizer
from services.reasoning_graph_logger import ReasoningGraphLogger
from services.question_search_engine import QuestionSearchEngine
from services.search_cache import SearchCacheStore
from services.solution_fetcher import SolutionFetcher
from verification.verifier import verify_solution


_INTAKE = MultimodalIntake()
_OCR = OCREngine()
_PDF = PDFProcessor(ocr_engine=_OCR)
_VISION = VisionRouter()
_OCR_DIAGRAM = OCRDiagramParser()
_CLASSIFIER = ProblemClassifier()
_RESEARCH_VERIFIER = ResearchMetaVerifier()
_BAYESIAN = BayesianConfidenceAdjuster()
_TELEMETRY = DEFAULT_MULTIMODAL_TELEMETRY
_DESMOS = DesmosGraphBuilder()
_PROVIDER_FABRIC: ProviderFabric | None = None
_INPUT_ANALYZER = InputAnalyzer()
_QUESTION_NORMALIZER = QuestionNormalizer()
_SEARCH_CACHE = SearchCacheStore(ttl_days=7)
_QUESTION_SEARCH_ENGINE = QuestionSearchEngine(cache_store=_SEARCH_CACHE)
_SOLUTION_FETCHER = SolutionFetcher()
_CONTEXT_BUILDER = RetrievalContextBuilder()
_GOT_ENGINE = GraphOfThoughtEngine(max_nodes=20)
_MCTS_ENGINE = MCTSSearch(got_engine=_GOT_ENGINE, max_depth=8, max_nodes=200, max_iterations=50)
_REASONING_GRAPH_LOGGER = ReasoningGraphLogger()
_MCTS_LOGGER = MCTSLogger()
_CONCEPT_VAULT = ConceptVault()
_PIPELINE_STAGE_LOG_PATH = Path("data/lc9/LC9_PIPELINE_STAGE_LOG.jsonl")
_PIPELINE_STAGE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_PIPELINE_STAGE_LOG_LOCK = threading.Lock()
_PIPELINE_MAX_CONCURRENCY = max(
    1, int(os.getenv("LC9_PIPELINE_MAX_CONCURRENCY", "2") or 2)
)
_PIPELINE_TIMEOUT_S = max(
    10.0, float(os.getenv("LC9_PIPELINE_TIMEOUT_S", "55.0") or 55.0)
)
_PIPELINE_SEMAPHORE = asyncio.Semaphore(_PIPELINE_MAX_CONCURRENCY)

_DEFAULT_MAX_INPUT_CHARS = 18_000
_DEFAULT_MULTIMODAL_MAX_INPUT_CHARS = 120_000
_DEFAULT_MAX_IMAGE_BYTES = 10_000_000
_DEFAULT_META_TIMEOUT_S = 8.0
_DEFAULT_WEB_SEARCH_TIMEOUT_S = 1.60
_DEFAULT_WEB_FETCH_TIMEOUT_S = 1.30
_DEFAULT_SEARCH_MAX_MATCHES = 14
_DEFAULT_GOT_TIMEOUT_S = 1.20
_DEFAULT_GOT_MAX_NODES = 20
_DEFAULT_MCTS_TIMEOUT_S = 3.20
_DEFAULT_MCTS_MAX_ITERATIONS = 50
_DEFAULT_MCTS_MAX_DEPTH = 8
_DEFAULT_MCTS_MAX_NODES = 200
_DEFAULT_MIN_CITATION_COUNT = 1
_DEFAULT_MIN_EVIDENCE_SCORE = 0.58
_DEFAULT_STAGE_LOG_PREVIEW_CHARS = 480


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_pipeline_stage_log(row: Dict[str, Any]) -> None:
    try:
        with _PIPELINE_STAGE_LOG_LOCK:
            with _PIPELINE_STAGE_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(_json_sanitize(row), ensure_ascii=True) + "\n")
    except Exception:
        pass


def _compute_evidence_metrics(
    search_payload: Dict[str, Any],
    fetched_solution: Dict[str, Any],
) -> Dict[str, Any]:
    matches = [
        dict(row)
        for row in (search_payload.get("matches") or [])
        if isinstance(row, dict)
    ]
    citations = matches[:6]
    similarity_scores = [
        float(row.get("similarity", 0.0) or 0.0) for row in citations
    ]
    top_similarity = max(similarity_scores) if similarity_scores else 0.0
    avg_similarity = (
        sum(similarity_scores[:3]) / max(1, min(3, len(similarity_scores)))
        if similarity_scores
        else 0.0
    )
    source_labels = [
        str(row.get("source") or "").strip().lower() for row in citations
    ]
    source_labels = [label for label in source_labels if label]
    source_diversity = len(set(source_labels))
    solution_conf = float(fetched_solution.get("confidence", 0.0) or 0.0)

    citation_count = len(citations)
    diversity_score = min(1.0, source_diversity / 3.0)
    citation_score = min(1.0, citation_count / 3.0)

    score = (
        (0.35 * top_similarity)
        + (0.20 * avg_similarity)
        + (0.15 * solution_conf)
        + (0.20 * citation_score)
        + (0.10 * diversity_score)
    )
    score = float(max(0.0, min(1.0, score)))

    if score >= 0.78 and citation_count >= 2:
        strength = "strong"
    elif score >= 0.58 and citation_count >= 1:
        strength = "ok"
    else:
        strength = "weak"

    return {
        "score": round(score, 6),
        "strength": strength,
        "citation_count": citation_count,
        "source_diversity": source_diversity,
        "top_similarity": round(top_similarity, 6),
        "avg_similarity": round(avg_similarity, 6),
        "solution_confidence": round(solution_conf, 6),
    }


def _tokenize_for_overlap(text: str) -> set[str]:
    tokens = re.findall(r"[a-z0-9]+", str(text or "").lower())
    return {t for t in tokens if len(t) > 3}


def _build_citation_map(
    *,
    answer_text: str,
    explanation_text: str,
    citations: Sequence[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    citation_rows = [dict(row) for row in citations if isinstance(row, dict)]
    if not citation_rows:
        return []

    segments: list[tuple[str, str]] = []
    if str(answer_text or "").strip():
        segments.append(("Answer", str(answer_text).strip()))
    if str(explanation_text or "").strip():
        segments.append(("Explanation", str(explanation_text).strip()))
    if not segments:
        return []

    for row in citation_rows:
        title = str(row.get("title") or "")
        snippet = str(row.get("snippet") or row.get("description") or "")
        source = str(row.get("source") or "")
        row["_tokens"] = _tokenize_for_overlap(f"{title} {snippet} {source}")

    mapped: list[Dict[str, Any]] = []
    for label, segment in segments:
        seg_tokens = _tokenize_for_overlap(segment)
        scored: list[tuple[float, Dict[str, Any]]] = []
        for row in citation_rows:
            base_similarity = float(row.get("similarity", 0.0) or 0.0)
            overlap = 0.0
            if seg_tokens and row["_tokens"]:
                overlap = len(seg_tokens.intersection(row["_tokens"])) / max(1, len(seg_tokens))
            score = (0.55 * base_similarity) + (0.45 * overlap)
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        top_sources: list[Dict[str, Any]] = []
        for score, row in scored[:3]:
            top_sources.append(
                {
                    "title": str(row.get("title") or row.get("source") or "").strip(),
                    "url": str(row.get("url") or "").strip(),
                    "source": str(row.get("source") or "web").strip(),
                    "similarity": float(row.get("similarity", 0.0) or 0.0),
                    "score": round(float(score), 6),
                }
            )
        mapped.append(
            {
                "label": label,
                "segment": segment[:3600],
                "sources": top_sources,
            }
        )

    return mapped


def _json_sanitize(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_sanitize(v) for v in value]
    if hasattr(value, "item"):
        try:
            return _json_sanitize(value.item())
        except Exception:
            pass
    if hasattr(value, "tolist"):
        try:
            return _json_sanitize(value.tolist())
        except Exception:
            pass
    return str(value)


def _should_require_citations(
    *,
    options: Dict[str, Any],
    profile: Any,
    verified: bool,
) -> bool:
    raw = options.get("require_citations", "auto")
    raw_token = str(raw).strip().lower()
    if raw_token in {"", "auto", "default"}:
        evidence_mode = str(options.get("evidence_mode", "auto") or "").strip().lower()
        if evidence_mode == "strict":
            return True
        function_hint = str(options.get("function") or "").strip().lower()
        response_style = str(options.get("response_style") or "").strip().lower()
        app_surface = str(options.get("app_surface") or "").strip().lower()
        subject = str(getattr(profile, "subject", "") or "").strip().lower()
        citation_first_surfaces = {
            "ai_chat",
            "chat_ai",
            "general_chat",
            "question_search",
            "research",
            "web_research",
        }
        if (
            function_hint in citation_first_surfaces
            or response_style in {"casual_chat", "companion_chat"}
            or app_surface in citation_first_surfaces
        ):
            return True
        if subject in {"mathematics", "math"} and verified:
            return False
        return False
    return bool(raw)


def _default_web_search_payload() -> Dict[str, Any]:
    return {"query": "", "matches": [], "cache_hit": False}


def _default_fetched_solution() -> Dict[str, Any]:
    return {
        "solution_text": "",
        "answer": "",
        "hint": "",
        "confidence": 0.0,
        "source_url": "",
        "source": "",
        "consulted": [],
        "formulas": [],
    }


def _default_reasoning_graph(*, enabled: bool) -> Dict[str, Any]:
    return {
        "status": "pending" if enabled else "disabled",
        "context_block": "",
        "nodes": [],
        "edges": [],
        "telemetry": {
            "node_count": 0,
            "tool_calls": 0,
            "retrieval_nodes": 0,
            "verification_pass": False,
            "final_confidence": 0.0,
            "stop_reason": "pending" if enabled else "disabled",
        },
        "diagram": {},
        "concepts": [],
        "early_verified": False,
    }


def _default_mcts_search(*, enabled: bool, developer_mode: bool) -> Dict[str, Any]:
    return {
        "status": "pending" if enabled else "disabled",
        "context_block": "",
        "best_path": [],
        "tree": {"nodes": [], "edges": []},
        "developer_mode": bool(developer_mode),
        "telemetry": {
            "iterations": 0,
            "nodes_explored": 0,
            "tool_calls": 0,
            "retrieval_calls": 0,
            "verification_pass": False,
            "final_confidence": 0.0,
            "stop_reason": "pending" if enabled else "disabled",
        },
    }


def _build_vault_context_payload(
    blocks: Sequence[Any],
    *,
    max_blocks: int = 6,
) -> Dict[str, Any]:
    rows = []
    for block in blocks[: max(1, int(max_blocks))]:
        title = str(getattr(block, "title", "") or "")
        text = str(getattr(block, "text", "") or "")
        score = float(getattr(block, "score", 0.0) or 0.0)
        tags = [str(tag) for tag in (getattr(block, "tags", []) or []) if str(tag).strip()]
        rows.append(
            {
                "block_id": str(getattr(block, "block_id", "") or ""),
                "title": title[:180],
                "text": text[:600],
                "score": round(score, 6),
                "tags": tags[:10],
            }
        )
    if not rows:
        return {
            "context_block": "",
            "blocks": [],
            "sources_consulted": [],
        }

    lines = ["CONCEPT VAULT CONTEXT:"]
    for row in rows:
        label = row["title"] or row["block_id"] or "concept_block"
        tag_suffix = f" [{' / '.join(row['tags'][:4])}]" if row["tags"] else ""
        lines.append(f"- {label}{tag_suffix}: {row['text']}")
    lines.append("")
    lines.append(
        "Instruction: treat Concept Vault retrieval as high-signal background knowledge, but still verify deterministically."
    )

    return {
        "context_block": "\n".join(lines).strip(),
        "blocks": rows,
        "sources_consulted": ["concept_vault"],
    }


def _verification_rank(result: Dict[str, Any], verification: Dict[str, Any]) -> tuple[int, float, float]:
    verified = 1 if bool(verification.get("verified", False)) else 0
    risk = float(verification.get("risk_score", 1.0) or 1.0)
    confidence = float(
        verification.get("confidence_score", 0.0)
        or (result.get("confidence") if isinstance(result, dict) else 0.0)
        or 0.0
    )
    return (verified, -risk, confidence)


def _build_reevaluation_prompt(
    *,
    question_text: str,
    final_prompt: str,
    solve_result: Dict[str, Any],
    explicit_verification: Dict[str, Any],
) -> str:
    reasons = [
        str(explicit_verification.get("failure_reason") or "").strip(),
        str(explicit_verification.get("reason") or "").strip(),
    ]
    reasons = [reason for reason in reasons if reason]
    checks = []
    for key, value in (explicit_verification.get("stage_results") or {}).items():
        checks.append(f"- {key}: {bool(value)}")
    verification_feedback = "\n".join(
        [
            "DETERMINISTIC VERIFICATION FEEDBACK:",
            *(f"- issue: {reason}" for reason in reasons[:3]),
            *checks[:8],
            "",
            "Instruction: re-evaluate the answer, preserve deep reasoning, and repair any domain, algebraic, unit, or boundary mistakes.",
            f"Previous candidate answer: {str(solve_result.get('final_answer', '')).strip()}",
            f"Original user question: {question_text}",
        ]
    ).strip()
    return f"{verification_feedback}\n\n{final_prompt}".strip()


@dataclass(slots=True)
class PipelineConfig:
    max_input_chars: int
    max_image_bytes: int
    meta_timeout_s: float
    function_hint: str
    pre_reasoning_enabled: bool
    web_retrieval_enabled: bool
    web_search_timeout_s: float
    web_fetch_timeout_s: float
    search_max_matches: int
    got_enabled: bool
    got_timeout_s: float
    got_max_nodes: int
    got_provider_reasoning: bool
    mcts_enabled: bool
    mcts_timeout_s: float
    mcts_iterations: int
    mcts_max_depth: int
    mcts_max_nodes: int
    mcts_provider_reasoning: bool
    mcts_developer_mode: bool
    optional_web_snippets: list[Any]
    similarity_threshold: float
    enable_verification_reevaluation: bool

    @classmethod
    def from_options(cls, options: Dict[str, Any]) -> "PipelineConfig":
        max_input_chars_opt = options.get("max_input_chars")
        max_input_chars = int(
            max(
                128,
                max_input_chars_opt
                if max_input_chars_opt is not None
                else _DEFAULT_MAX_INPUT_CHARS,
            )
        )
        max_image_bytes = int(
            max(1024, options.get("max_image_bytes", _DEFAULT_MAX_IMAGE_BYTES))
        )
        meta_timeout_s = float(
            max(1.0, options.get("meta_timeout_s", _DEFAULT_META_TIMEOUT_S))
        )
        function_hint = str(options.get("function") or "").strip().lower()
        default_pre_reasoning = function_hint in {
            "ai_chat",
            "chat_ai",
            "general_chat",
            "ai_solve",
            "chat",
        }
        pre_reasoning_enabled = bool(
            options.get("enable_pre_reasoning_context", default_pre_reasoning)
        )
        web_retrieval_enabled = bool(
            options.get("enable_web_retrieval", pre_reasoning_enabled)
        )
        web_search_timeout_s = float(
            max(
                0.25,
                options.get("web_search_timeout_s", _DEFAULT_WEB_SEARCH_TIMEOUT_S),
            )
        )
        web_fetch_timeout_s = float(
            max(
                0.25,
                options.get("web_fetch_timeout_s", _DEFAULT_WEB_FETCH_TIMEOUT_S),
            )
        )
        search_max_matches = int(
            max(
                1,
                min(
                    20,
                    options.get("search_max_matches", _DEFAULT_SEARCH_MAX_MATCHES),
                ),
            )
        )
        got_enabled = bool(
            options.get("enable_graph_of_thought", pre_reasoning_enabled)
        )
        got_timeout_s = float(
            max(0.35, options.get("got_timeout_s", _DEFAULT_GOT_TIMEOUT_S))
        )
        got_max_nodes = int(
            max(8, min(40, options.get("got_max_nodes", _DEFAULT_GOT_MAX_NODES)))
        )
        got_provider_reasoning = bool(options.get("got_provider_reasoning", True))
        mcts_enabled = bool(options.get("enable_mcts_reasoning", pre_reasoning_enabled))
        mcts_timeout_s = float(
            max(0.8, options.get("mcts_timeout_s", _DEFAULT_MCTS_TIMEOUT_S))
        )
        mcts_iterations = int(
            max(
                5,
                min(
                    80,
                    options.get(
                        "mcts_max_iterations", _DEFAULT_MCTS_MAX_ITERATIONS
                    ),
                ),
            )
        )
        mcts_max_depth = int(
            max(2, min(12, options.get("mcts_max_depth", _DEFAULT_MCTS_MAX_DEPTH)))
        )
        mcts_max_nodes = int(
            max(20, min(300, options.get("mcts_max_nodes", _DEFAULT_MCTS_MAX_NODES)))
        )
        mcts_provider_reasoning = bool(
            options.get("mcts_provider_reasoning", got_provider_reasoning)
        )
        mcts_developer_mode = bool(options.get("mcts_developer_mode", False))
        optional_web_snippets = options.get("optional_web_snippets")
        if not isinstance(optional_web_snippets, Sequence) or isinstance(
            optional_web_snippets, (str, bytes)
        ):
            optional_web_snippets = []
        similarity_threshold_raw = options.get("web_similarity_threshold", 0.75)
        try:
            similarity_threshold = float(similarity_threshold_raw)
        except (TypeError, ValueError):
            similarity_threshold = 0.75
        similarity_threshold = float(max(0.4, min(0.95, similarity_threshold)))

        return cls(
            max_input_chars=max_input_chars,
            max_image_bytes=max_image_bytes,
            meta_timeout_s=meta_timeout_s,
            function_hint=function_hint,
            pre_reasoning_enabled=pre_reasoning_enabled,
            web_retrieval_enabled=web_retrieval_enabled,
            web_search_timeout_s=web_search_timeout_s,
            web_fetch_timeout_s=web_fetch_timeout_s,
            search_max_matches=search_max_matches,
            got_enabled=got_enabled,
            got_timeout_s=got_timeout_s,
            got_max_nodes=got_max_nodes,
            got_provider_reasoning=got_provider_reasoning,
            mcts_enabled=mcts_enabled,
            mcts_timeout_s=mcts_timeout_s,
            mcts_iterations=mcts_iterations,
            mcts_max_depth=mcts_max_depth,
            mcts_max_nodes=mcts_max_nodes,
            mcts_provider_reasoning=mcts_provider_reasoning,
            mcts_developer_mode=mcts_developer_mode,
            optional_web_snippets=list(optional_web_snippets),
            similarity_threshold=similarity_threshold,
            enable_verification_reevaluation=bool(
                options.get("enable_verification_reevaluation", True)
            ),
        )


@dataclass(slots=True)
class PipelineState:
    input_data: Any
    input_type: str
    user_context: Dict[str, Any]
    options: Dict[str, Any]
    config: PipelineConfig
    stage_timing: Dict[str, float] = field(default_factory=dict)
    stage_records: list[Dict[str, Any]] = field(default_factory=list)
    stage_failures: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    intake: IntakePayload | None = None
    question_text: str = ""
    input_analysis: Dict[str, Any] = field(default_factory=dict)
    normalized_question: Dict[str, Any] = field(default_factory=dict)
    ocr_data: Dict[str, Any] | None = None
    pdf_data: Dict[str, Any] | None = None
    pdf_primary_ocr: Dict[str, Any] | None = None
    vision_analysis: Dict[str, Any] | None = None
    profile: Any = None
    profile_dict: Dict[str, Any] = field(default_factory=dict)
    vault_blocks: list[Any] = field(default_factory=list)
    vault_context_payload: Dict[str, Any] = field(default_factory=dict)
    web_search_payload: Dict[str, Any] = field(default_factory=_default_web_search_payload)
    fetched_solution: Dict[str, Any] = field(default_factory=_default_fetched_solution)
    evidence_metrics: Dict[str, Any] = field(default_factory=dict)
    context_payload: Dict[str, Any] = field(default_factory=dict)
    final_prompt: str = ""
    reasoning_graph: Dict[str, Any] = field(default_factory=dict)
    mcts_search: Dict[str, Any] = field(default_factory=dict)
    provider_warm_task: asyncio.Task[Dict[str, Any]] | None = None
    provider_warm_report: Dict[str, Any] = field(default_factory=dict)
    solve_result: Dict[str, Any] = field(default_factory=dict)
    explicit_verification: Dict[str, Any] = field(default_factory=dict)
    reevaluation: Dict[str, Any] = field(default_factory=dict)
    research_verification: Dict[str, Any] = field(default_factory=dict)
    calibration_metrics: Dict[str, Any] = field(default_factory=dict)
    meta_verification: Dict[str, Any] = field(default_factory=dict)
    final_output: Dict[str, Any] = field(default_factory=dict)
    terminal_response: Dict[str, Any] | None = None


class LalaCorePipelineController:
    def __init__(self) -> None:
        self.stage_order = [
            ("stage1_intake_normalization", self._stage_intake_and_normalization),
            ("stage2_retrieval_context", self._stage_retrieval_and_context),
            ("stage3_multi_reasoning", self._stage_multi_reasoning_generation),
            ("stage4_mcts_expansion", self._stage_mcts_expansion),
            ("stage5_provider_arena", self._stage_provider_arena),
            ("stage6_deterministic_verification", self._stage_deterministic_verification),
            ("stage7_critic_calibration", self._stage_critic_and_calibration),
            ("stage8_final_synthesis", self._stage_final_synthesis),
        ]

    async def execute(
        self,
        *,
        input_data: Any,
        input_type: str,
        user_context: Dict[str, Any] | None,
        options: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        state = PipelineState(
            input_data=input_data,
            input_type=input_type,
            user_context=dict(user_context or {}),
            options=dict(options or {}),
            config=PipelineConfig.from_options(dict(options or {})),
        )
        state.reasoning_graph = _default_reasoning_graph(
            enabled=state.config.got_enabled
        )
        state.mcts_search = _default_mcts_search(
            enabled=state.config.mcts_enabled,
            developer_mode=state.config.mcts_developer_mode,
        )

        try:
            for stage_name, stage_fn in self.stage_order:
                await self._run_stage(stage_name, stage_fn, state)
                if state.terminal_response is not None:
                    break
            if state.terminal_response is not None:
                return state.terminal_response
            if state.final_output:
                return state.final_output
            return {
                "status": "error",
                "error": "pipeline_failed",
                "message": "Pipeline finished without producing an output.",
                "latency_metrics": dict(state.stage_timing),
            }
        finally:
            await self._cleanup(state)

    async def _run_stage(
        self,
        stage_name: str,
        stage_fn,
        state: PipelineState,
    ) -> None:
        t0 = perf_counter()
        status = "ok"
        summary: Dict[str, Any] = {}
        try:
            summary = await stage_fn(state) or {}
        except Exception as exc:
            status = "failed"
            summary = self._handle_stage_failure(stage_name, state, exc)
        duration_s = float(max(0.0, perf_counter() - t0))
        state.stage_timing[f"{stage_name}_s"] = duration_s
        record = {
            "ts": _utc_now(),
            "stage": stage_name,
            "status": status,
            "duration_s": duration_s,
            "question_preview": str(state.question_text or "")[
                :_DEFAULT_STAGE_LOG_PREVIEW_CHARS
            ],
            "input_type": (
                str(state.intake.input_type)
                if state.intake is not None
                else str(state.input_type)
            ),
            "summary": _json_sanitize(summary),
        }
        state.stage_records.append(record)
        _write_pipeline_stage_log(record)
        self._release_stage_memory(stage_name, state)

    def _handle_stage_failure(
        self,
        stage_name: str,
        state: PipelineState,
        exc: Exception,
    ) -> Dict[str, Any]:
        error_payload = {
            "error_type": type(exc).__name__,
            "message": str(exc)[:320],
        }
        state.stage_failures[stage_name] = error_payload
        _TELEMETRY.log_event(
            "pipeline_stage_failure",
            {"stage": stage_name, **error_payload},
        )

        if stage_name == "stage1_intake_normalization":
            state.terminal_response = {
                "status": "error",
                "error": "stage1_failure",
                "message": str(exc)[:320],
                "latency_metrics": dict(state.stage_timing),
            }
            return error_payload

        if stage_name == "stage2_retrieval_context":
            state.web_search_payload = _default_web_search_payload()
            state.fetched_solution = _default_fetched_solution()
            state.context_payload = {}
            state.evidence_metrics = _compute_evidence_metrics(
                state.web_search_payload, state.fetched_solution
            )
            state.final_prompt = state.question_text
            return error_payload

        if stage_name == "stage3_multi_reasoning":
            state.reasoning_graph = {
                **_default_reasoning_graph(enabled=state.config.got_enabled),
                "status": "failed",
                "telemetry": {
                    **(
                        _default_reasoning_graph(enabled=True).get("telemetry") or {}
                    ),
                    "stop_reason": "exception",
                    "error": str(exc)[:240],
                },
            }
            return error_payload

        if stage_name == "stage4_mcts_expansion":
            state.mcts_search = {
                **_default_mcts_search(
                    enabled=state.config.mcts_enabled,
                    developer_mode=state.config.mcts_developer_mode,
                ),
                "status": "failed",
                "telemetry": {
                    **(
                        _default_mcts_search(
                            enabled=True,
                            developer_mode=state.config.mcts_developer_mode,
                        ).get("telemetry")
                        or {}
                    ),
                    "stop_reason": "exception",
                    "error": str(exc)[:240],
                },
            }
            return error_payload

        if stage_name == "stage5_provider_arena":
            state.terminal_response = {
                "status": "error",
                "error": "solve_result_invalid",
                "message": str(exc)[:320],
                "question": state.question_text,
                "latency_metrics": dict(state.stage_timing),
            }
            return error_payload

        if stage_name == "stage6_deterministic_verification":
            state.explicit_verification = {
                "verified": False,
                "confidence_score": 0.0,
                "risk_score": 1.0,
                "escalate": True,
                "failure_reason": "stage6_exception",
                "reason": str(exc)[:240],
            }
            return error_payload

        if stage_name == "stage7_critic_calibration":
            state.research_verification = {
                "score": 0.0,
                "issues": ["stage7_exception"],
                "error": str(exc)[:240],
            }
            state.calibration_metrics = {
                "risk_score": 1.0,
                "confidence_score": 0.0,
                "verified": False,
                "entropy": 1.0,
                "disagreement": 1.0,
            }
            return error_payload

        if stage_name == "stage8_final_synthesis":
            state.final_output = {
                **dict(state.solve_result or {}),
                "status": "error",
                "error": "stage8_failure",
                "message": str(exc)[:320],
                "latency_metrics": dict(state.stage_timing),
            }
            return error_payload

        return error_payload

    async def _stage_intake_and_normalization(
        self, state: PipelineState
    ) -> Dict[str, Any]:
        config = state.config
        if (
            isinstance(state.input_data, str)
            and state.input_type in {"auto", "text"}
            and len(state.input_data) > config.max_input_chars
        ):
            state.terminal_response = {
                "status": "error",
                "error": "input_too_long",
                "message": f"Input exceeds {config.max_input_chars} characters.",
                "input_metadata": {
                    "detected_type": "text",
                    "max_input_chars": config.max_input_chars,
                },
            }
            return {"terminal": True, "reason": "input_too_long"}

        intake = _INTAKE.normalize(
            state.input_data,
            input_type=(
                state.input_type
                if state.input_type in {"auto", "text", "image", "pdf", "mixed"}
                else "auto"
            ),
        )
        state.intake = intake

        if intake.image_bytes is not None and len(intake.image_bytes) > config.max_image_bytes:
            state.terminal_response = {
                "status": "error",
                "error": "image_too_large",
                "message": f"Image input exceeds {config.max_image_bytes} bytes.",
                "input_metadata": {
                    "detected_type": intake.input_type,
                    "max_image_bytes": config.max_image_bytes,
                    "files": intake.files,
                },
            }
            return {"terminal": True, "reason": "image_too_large"}

        if config.pre_reasoning_enabled and state.provider_warm_task is None:
            state.provider_warm_task = asyncio.create_task(_warm_provider_availability())

        composed_text_parts = []
        if intake.text.strip():
            composed_text_parts.append(intake.text.strip())

        async def _run_ocr_stage() -> Dict[str, Any] | None:
            if intake.input_type not in {"image", "mixed"} or not intake.image_bytes:
                return None
            t0 = perf_counter()
            ocr_payload = await _OCR.extract_async(
                intake.image_bytes,
                page_number=1,
                math_aware=True,
                optional_web_snippets=config.optional_web_snippets,
            )
            state.stage_timing["ocr_s"] = float(max(0.0, perf_counter() - t0))
            _TELEMETRY.log_timing(
                stage="ocr",
                duration_s=state.stage_timing["ocr_s"],
                slow_threshold_s=2.5,
                extra={"input_type": intake.input_type},
            )
            return ocr_payload

        async def _run_pdf_stage() -> Dict[str, Any] | None:
            if intake.input_type not in {"pdf", "mixed"} or not intake.pdf_bytes:
                return None
            t0 = perf_counter()
            pdf_payload = await _PDF.process(
                intake.pdf_bytes,
                optional_web_snippets=config.optional_web_snippets,
            )
            state.stage_timing["pdf_s"] = float(max(0.0, perf_counter() - t0))
            _TELEMETRY.log_timing(
                stage="pdf_preprocess",
                duration_s=state.stage_timing["pdf_s"],
                slow_threshold_s=6.0,
                extra={"input_type": intake.input_type},
            )
            return pdf_payload

        ocr_payload, pdf_payload = await asyncio.gather(
            _run_ocr_stage(),
            _run_pdf_stage(),
            return_exceptions=True,
        )
        if isinstance(ocr_payload, Exception):
            _TELEMETRY.log_event(
                "ocr_stage_error",
                {"error_type": type(ocr_payload).__name__, "reason": str(ocr_payload)[:240]},
            )
            ocr_payload = None
        if isinstance(pdf_payload, Exception):
            _TELEMETRY.log_event(
                "pdf_stage_error",
                {"error_type": type(pdf_payload).__name__, "reason": str(pdf_payload)[:240]},
            )
            pdf_payload = None
        state.ocr_data = ocr_payload
        state.pdf_data = pdf_payload

        if isinstance(ocr_payload, dict):
            ocr_primary_text = str(
                ocr_payload.get("clean_text")
                or ocr_payload.get("math_normalized_text")
                or ocr_payload.get("raw_text", "")
            ).strip()
            if ocr_primary_text:
                composed_text_parts.append(ocr_primary_text)
            lc_iie_rows = [
                dict(x)
                for x in (ocr_payload.get("lc_iie_questions") or [])
                if isinstance(x, dict)
            ]
            extracted_statements = [
                str(row.get("statement", "")).strip()
                for row in lc_iie_rows[:12]
                if str(row.get("statement", "")).strip()
            ]
            if extracted_statements:
                composed_text_parts.append("\n".join(extracted_statements))

        if isinstance(pdf_payload, dict):
            merged = str((pdf_payload or {}).get("merged_text", "")).strip()
            if merged:
                composed_text_parts.append(merged)
            if intake.image_bytes is None and (pdf_payload or {}).get("pages"):
                first_page = pdf_payload["pages"][0]
                if isinstance(first_page, dict):
                    state.pdf_primary_ocr = dict(first_page)

        question_text = _compose_question(composed_text_parts, fallback="")
        if not question_text:
            state.terminal_response = {
                "status": "error",
                "error": "empty_input_after_preprocessing",
                "input_metadata": {
                    "detected_type": intake.input_type,
                    "files": intake.files,
                },
            }
            return {"terminal": True, "reason": "empty_input_after_preprocessing"}

        max_preprocessed_chars = config.max_input_chars
        if (
            state.options.get("max_input_chars") is None
            and intake.input_type in {"image", "mixed", "pdf"}
        ):
            max_preprocessed_chars = _DEFAULT_MULTIMODAL_MAX_INPUT_CHARS
        if len(question_text) > max_preprocessed_chars:
            state.terminal_response = {
                "status": "error",
                "error": "preprocessed_input_too_long",
                "message": f"Preprocessed input exceeds {max_preprocessed_chars} characters.",
                "input_metadata": {
                    "detected_type": intake.input_type,
                    "max_input_chars": max_preprocessed_chars,
                    "files": intake.files,
                },
            }
            return {"terminal": True, "reason": "preprocessed_input_too_long"}

        state.question_text = question_text
        state.input_analysis = _INPUT_ANALYZER.build(
            detected_type=intake.input_type,
            question_text=question_text,
            user_text=intake.text,
            ocr_data=state.ocr_data,
            pdf_data=state.pdf_data,
        )
        state.normalized_question = _QUESTION_NORMALIZER.normalize(question_text)
        state.profile = _CLASSIFIER.classify(question_text)
        state.profile_dict = {
            "subject": state.profile.subject,
            "difficulty": state.profile.difficulty,
            "numeric": state.profile.numeric,
            "multi_concept": state.profile.multi_concept,
            "trap_probability": state.profile.trap_probability,
        }

        if intake.image_bytes:
            tv = perf_counter()
            try:
                state.vision_analysis = await _VISION.analyze(
                    intake.image_bytes,
                    problem_profile=state.profile_dict,
                )
            except Exception as exc:
                state.vision_analysis = None
                _TELEMETRY.log_event(
                    "vision_stage_error",
                    {"error_type": type(exc).__name__, "reason": str(exc)[:240]},
                )
            state.stage_timing["vision_s"] = float(max(0.0, perf_counter() - tv))
            _TELEMETRY.log_timing(
                stage="vision",
                duration_s=state.stage_timing["vision_s"],
                slow_threshold_s=4.0,
                extra={"input_type": intake.input_type},
            )
        elif isinstance(state.pdf_primary_ocr, dict):
            tv = perf_counter()
            state.vision_analysis = _build_vision_from_ocr(
                state.pdf_primary_ocr,
                profile_dict=state.profile_dict,
            )
            state.stage_timing["vision_s"] = float(max(0.0, perf_counter() - tv))
            _TELEMETRY.log_timing(
                stage="vision_ocr_fusion",
                duration_s=state.stage_timing["vision_s"],
                slow_threshold_s=2.5,
                extra={"input_type": intake.input_type},
            )

        if isinstance(state.vision_analysis, dict) and isinstance(state.ocr_data, dict):
            state.vision_analysis = _merge_vision_with_ocr(
                state.vision_analysis, state.ocr_data
            )

        state.intake = IntakePayload(
            input_type=intake.input_type,
            text=intake.text,
            files=[dict(row) for row in intake.files],
            metadata=dict(intake.metadata),
        )

        return {
            "detected_type": intake.input_type,
            "question_length": len(state.question_text),
            "normalized_search_query": str(
                state.normalized_question.get("search_query") or ""
            )[:180],
            "profile": dict(state.profile_dict),
            "ocr_available": state.ocr_data is not None,
            "pdf_available": state.pdf_data is not None,
            "vision_available": state.vision_analysis is not None,
        }

    async def _stage_retrieval_and_context(self, state: PipelineState) -> Dict[str, Any]:
        async def _vault_task():
            t0 = perf_counter()
            try:
                return await asyncio.to_thread(
                    _CONCEPT_VAULT.retrieve,
                    state.question_text,
                    str(getattr(state.profile, "subject", "general")),
                    5,
                )
            finally:
                state.stage_timing["vault_retrieval_s"] = float(
                    max(0.0, perf_counter() - t0)
                )

        async def _web_task():
            if not state.config.web_retrieval_enabled:
                state.stage_timing["web_search_s"] = 0.0
                return _default_web_search_payload()
            t0 = perf_counter()
            try:
                return await _QUESTION_SEARCH_ENGINE.search(
                    state.normalized_question,
                    max_matches=state.config.search_max_matches,
                    query_timeout_s=state.config.web_search_timeout_s,
                )
            finally:
                state.stage_timing["web_search_s"] = float(
                    max(0.0, perf_counter() - t0)
                )

        t_retrieval = perf_counter()
        vault_blocks, web_search_payload = await asyncio.gather(
            _vault_task(),
            _web_task(),
            return_exceptions=True,
        )
        state.stage_timing["retrieval_parallel_s"] = float(
            max(0.0, perf_counter() - t_retrieval)
        )
        _TELEMETRY.log_timing(
            stage="web_search",
            duration_s=float(state.stage_timing.get("web_search_s", 0.0)),
            slow_threshold_s=1.5,
            extra={
                "input_type": state.intake.input_type if state.intake else state.input_type
            },
        )

        if isinstance(vault_blocks, Exception):
            vault_blocks = []
        if isinstance(web_search_payload, Exception):
            web_search_payload = {
                **_default_web_search_payload(),
                "error": str(web_search_payload)[:240],
            }

        state.vault_blocks = list(vault_blocks or [])
        state.web_search_payload = dict(web_search_payload or {})

        if state.config.web_retrieval_enabled:
            t_fetch = perf_counter()
            try:
                state.fetched_solution = await asyncio.wait_for(
                    _SOLUTION_FETCHER.fetch(
                        state.web_search_payload,
                        similarity_threshold=state.config.similarity_threshold,
                        timeout_s=state.config.web_fetch_timeout_s,
                    ),
                    timeout=max(0.35, state.config.web_fetch_timeout_s + 0.25),
                )
            except Exception as exc:
                state.fetched_solution = {
                    **_default_fetched_solution(),
                    "error": str(exc)[:240],
                }
            state.stage_timing["web_fetch_s"] = float(
                max(0.0, perf_counter() - t_fetch)
            )
            _TELEMETRY.log_timing(
                stage="web_fetch",
                duration_s=state.stage_timing["web_fetch_s"],
                slow_threshold_s=1.5,
                extra={"input_type": state.intake.input_type if state.intake else state.input_type},
            )
        else:
            state.fetched_solution = _default_fetched_solution()

        state.evidence_metrics = _compute_evidence_metrics(
            state.web_search_payload, state.fetched_solution
        )
        state.vault_context_payload = _build_vault_context_payload(state.vault_blocks)
        state.context_payload = _CONTEXT_BUILDER.build(
            original_question=state.question_text,
            search_payload=state.web_search_payload,
            fetched_solution=state.fetched_solution,
        )

        prompt_sections = []
        if state.vault_context_payload.get("context_block"):
            prompt_sections.append(str(state.vault_context_payload["context_block"]))
        if state.context_payload.get("context_block"):
            prompt_sections.append(str(state.context_payload["context_block"]))
        prompt_sections.append(f"User Question:\n{state.question_text}")
        state.final_prompt = "\n\n".join(
            section.strip() for section in prompt_sections if str(section).strip()
        ).strip()
        if not state.final_prompt:
            state.final_prompt = state.question_text

        if "web_search_s" not in state.stage_timing:
            state.stage_timing["web_search_s"] = float(
                state.stage_timing.get("retrieval_parallel_s", 0.0)
            )

        return {
            "vault_blocks": len(state.vault_blocks),
            "web_matches": len(state.web_search_payload.get("matches", []) or []),
            "context_injected": bool(state.context_payload.get("context_block"))
            or bool(state.vault_context_payload.get("context_block")),
            "evidence_score": float(state.evidence_metrics.get("score", 0.0) or 0.0),
        }

    async def _stage_multi_reasoning_generation(
        self, state: PipelineState
    ) -> Dict[str, Any]:
        if not state.config.got_enabled:
            return {"enabled": False, "status": "disabled"}

        tg = perf_counter()
        try:
            state.reasoning_graph = await asyncio.wait_for(
                _GOT_ENGINE.run(
                    question=state.question_text,
                    profile=state.profile_dict,
                    web_retrieval={
                        "matches": state.web_search_payload.get("matches", []),
                        "solution": {
                            "hint": state.fetched_solution.get("hint", ""),
                            "answer": state.fetched_solution.get("answer", ""),
                            "solution_text": state.fetched_solution.get(
                                "solution_text", ""
                            ),
                            "confidence": state.fetched_solution.get(
                                "confidence", 0.0
                            ),
                            "source_url": state.fetched_solution.get(
                                "source_url", ""
                            ),
                        },
                    },
                    input_analysis=state.input_analysis,
                    ocr_data=state.ocr_data,
                    vision_analysis=state.vision_analysis,
                    max_nodes=state.config.got_max_nodes,
                    allow_provider_reasoning=bool(
                        state.config.got_provider_reasoning
                    ),
                    timeout_s=state.config.got_timeout_s,
                ),
                timeout=max(0.5, state.config.got_timeout_s + 0.4),
            )
        except Exception as exc:
            state.reasoning_graph = {
                **_default_reasoning_graph(enabled=True),
                "status": "failed",
                "telemetry": {
                    **(
                        _default_reasoning_graph(enabled=True).get("telemetry") or {}
                    ),
                    "stop_reason": "exception",
                    "error": str(exc)[:240],
                },
            }
        state.stage_timing["got_s"] = float(max(0.0, perf_counter() - tg))
        _TELEMETRY.log_timing(
            stage="got_pre_reasoning",
            duration_s=state.stage_timing["got_s"],
            slow_threshold_s=1.8,
            extra={
                "input_type": state.intake.input_type if state.intake else state.input_type
            },
        )
        got_context = str(state.reasoning_graph.get("context_block") or "").strip()
        if got_context:
            state.final_prompt = f"{got_context}\n\n{state.final_prompt}".strip()
        return {
            "enabled": True,
            "status": str(state.reasoning_graph.get("status", "")),
            "node_count": int(
                ((state.reasoning_graph.get("telemetry") or {}).get("node_count", 0))
                or 0
            ),
            "provider_reasoning": bool(state.config.got_provider_reasoning),
        }

    async def _stage_mcts_expansion(self, state: PipelineState) -> Dict[str, Any]:
        if not state.config.mcts_enabled:
            return {"enabled": False, "status": "disabled"}

        tmcts = perf_counter()
        try:
            state.mcts_search = await asyncio.wait_for(
                _MCTS_ENGINE.search(
                    question=state.question_text,
                    profile=state.profile_dict,
                    web_retrieval={
                        "matches": state.web_search_payload.get("matches", []),
                        "solution": {
                            "hint": state.fetched_solution.get("hint", ""),
                            "answer": state.fetched_solution.get("answer", ""),
                            "solution_text": state.fetched_solution.get(
                                "solution_text", ""
                            ),
                            "confidence": state.fetched_solution.get(
                                "confidence", 0.0
                            ),
                            "source_url": state.fetched_solution.get(
                                "source_url", ""
                            ),
                        },
                    },
                    input_analysis=state.input_analysis,
                    ocr_data=state.ocr_data or {},
                    vision_analysis=state.vision_analysis or {},
                    max_iterations=state.config.mcts_iterations,
                    max_depth=state.config.mcts_max_depth,
                    max_nodes=state.config.mcts_max_nodes,
                    allow_provider_reasoning=bool(
                        state.config.mcts_provider_reasoning
                    ),
                    developer_mode=bool(state.config.mcts_developer_mode),
                    timeout_s=state.config.mcts_timeout_s,
                ),
                timeout=max(1.0, state.config.mcts_timeout_s + 0.5),
            )
        except Exception as exc:
            state.mcts_search = {
                **_default_mcts_search(
                    enabled=True,
                    developer_mode=state.config.mcts_developer_mode,
                ),
                "status": "failed",
                "telemetry": {
                    **(
                        _default_mcts_search(
                            enabled=True,
                            developer_mode=state.config.mcts_developer_mode,
                        ).get("telemetry")
                        or {}
                    ),
                    "stop_reason": "exception",
                    "error": str(exc)[:240],
                },
            }
        state.stage_timing["mcts_s"] = float(max(0.0, perf_counter() - tmcts))
        _TELEMETRY.log_timing(
            stage="mcts_pre_reasoning",
            duration_s=state.stage_timing["mcts_s"],
            slow_threshold_s=3.5,
            extra={
                "input_type": state.intake.input_type if state.intake else state.input_type
            },
        )
        mcts_context = str(state.mcts_search.get("context_block") or "").strip()
        if mcts_context and str(state.mcts_search.get("status", "")).lower() == "ok":
            state.final_prompt = f"{mcts_context}\n\n{state.final_prompt}".strip()
        return {
            "enabled": True,
            "status": str(state.mcts_search.get("status", "")),
            "iterations": int(
                ((state.mcts_search.get("telemetry") or {}).get("iterations", 0))
                or 0
            ),
            "nodes_explored": int(
                (
                    (state.mcts_search.get("telemetry") or {}).get(
                        "nodes_explored", 0
                    )
                )
                or 0
            ),
        }

    async def _stage_provider_arena(self, state: PipelineState) -> Dict[str, Any]:
        ts = perf_counter()
        solve_result = await solve_question(state.final_prompt)
        state.stage_timing["solver_s"] = float(max(0.0, perf_counter() - ts))
        _TELEMETRY.log_timing(
            stage="solver",
            duration_s=state.stage_timing["solver_s"],
            slow_threshold_s=12.0,
            extra={
                "input_type": state.intake.input_type if state.intake else state.input_type
            },
        )

        if not isinstance(solve_result, dict):
            raise RuntimeError("solve_result_invalid")
        state.solve_result = dict(solve_result)

        if state.provider_warm_task is not None:
            try:
                state.provider_warm_report = await asyncio.wait_for(
                    state.provider_warm_task, timeout=0.35
                )
            except Exception:
                state.provider_warm_report = {}
            finally:
                state.provider_warm_task = None

        return {
            "winner_provider": str(state.solve_result.get("winner_provider", "")),
            "arena_entropy": float(
                ((state.solve_result.get("arena") or {}).get("entropy", 0.0)) or 0.0
            ),
            "arena_disagreement": float(
                (
                    (state.solve_result.get("arena") or {}).get(
                        "disagreement", 0.0
                    )
                )
                or 0.0
            ),
        }

    async def _stage_deterministic_verification(
        self, state: PipelineState
    ) -> Dict[str, Any]:
        difficulty = str(getattr(state.profile, "difficulty", "") or "")
        state.explicit_verification = await asyncio.to_thread(
            verify_solution,
            state.question_text,
            str(state.solve_result.get("final_answer", "")),
            difficulty or None,
            None,
        )

        base_verification = dict(state.solve_result.get("verification", {}) or {})
        base_verified = bool(base_verification.get("verified", False))
        explicit_verified = bool(state.explicit_verification.get("verified", False))
        if explicit_verified and not base_verified:
            base_verification["verified"] = True
            base_verification["confidence_score"] = float(
                max(
                    base_verification.get("confidence_score", 0.0) or 0.0,
                    state.explicit_verification.get("confidence_score", 0.0) or 0.0,
                )
            )
            base_verification["risk_score"] = float(
                min(
                    base_verification.get("risk_score", 1.0) or 1.0,
                    state.explicit_verification.get("risk_score", 1.0) or 1.0,
                )
            )
        base_verification["pipeline_stage6"] = dict(state.explicit_verification)

        reevaluated = False
        if (
            state.config.enable_verification_reevaluation
            and not bool(base_verification.get("verified", False))
            and not explicit_verified
            and str(state.solve_result.get("final_answer", "")).strip()
        ):
            reevaluation_prompt = _build_reevaluation_prompt(
                question_text=state.question_text,
                final_prompt=state.final_prompt,
                solve_result=state.solve_result,
                explicit_verification=state.explicit_verification,
            )
            reevaluated_result = await solve_question(reevaluation_prompt)
            if isinstance(reevaluated_result, dict):
                reevaluated_verification = await asyncio.to_thread(
                    verify_solution,
                    state.question_text,
                    str(reevaluated_result.get("final_answer", "")),
                    difficulty or None,
                    None,
                )
                state.reevaluation = {
                    "attempted": True,
                    "question_preview": state.question_text[
                        :_DEFAULT_STAGE_LOG_PREVIEW_CHARS
                    ],
                    "verification": dict(reevaluated_verification),
                    "winner_provider": str(
                        reevaluated_result.get("winner_provider", "")
                    ),
                }
                if _verification_rank(
                    reevaluated_result, reevaluated_verification
                ) > _verification_rank(state.solve_result, state.explicit_verification):
                    state.solve_result = dict(reevaluated_result)
                    state.explicit_verification = dict(reevaluated_verification)
                    base_verification = dict(
                        state.solve_result.get("verification", {}) or {}
                    )
                    base_verification["pipeline_stage6"] = dict(
                        state.explicit_verification
                    )
                    base_verification["pipeline_reevaluation_applied"] = True
                    reevaluated = True
            elif not state.reevaluation:
                state.reevaluation = {"attempted": True, "accepted": False}

        state.solve_result["verification"] = base_verification
        return {
            "verified": bool(base_verification.get("verified", False)),
            "explicit_verified": bool(
                state.explicit_verification.get("verified", False)
            ),
            "reevaluated": bool(reevaluated),
            "risk_score": float(base_verification.get("risk_score", 1.0) or 1.0),
        }

    async def _stage_critic_and_calibration(
        self, state: PipelineState
    ) -> Dict[str, Any]:
        base_verification = dict(state.solve_result.get("verification", {}) or {})
        arena = dict(state.solve_result.get("arena", {}) or {})
        raw_risk = float(base_verification.get("risk_score", 1.0))

        state.research_verification = _RESEARCH_VERIFIER.evaluate(
            question=state.question_text,
            final_answer=str(state.solve_result.get("final_answer", "")),
            reasoning=str(state.solve_result.get("reasoning", "")),
            profile=state.solve_result.get("profile", state.profile_dict)
            or state.profile_dict,
            base_verification=base_verification,
            ocr_data=state.ocr_data,
            vision_analysis=state.vision_analysis,
        )

        prior_conf = 1.0 - raw_risk
        entropy = float(arena.get("entropy", 0.0))
        disagreement = float(arena.get("disagreement", 0.0))
        winner_margin = float(arena.get("winner_margin", 0.0))
        ranked = list(arena.get("ranked_providers", []) or [])
        single_provider_mode = len(ranked) <= 1

        drift_state = _TELEMETRY.update_calibration_drift(
            expected=prior_conf,
            observed=1.0 if bool(base_verification.get("verified", False)) else 0.0,
        )

        effective_entropy = float(entropy)
        confidence_multiplier = 1.0
        if single_provider_mode:
            effective_entropy = max(effective_entropy, 0.38)
            confidence_multiplier = 0.82

        bayesian_confidence = _BAYESIAN.adjust(
            prior_confidence=prior_conf,
            agreement=max(0.0, min(1.0, 1.0 - disagreement)),
            entropy=effective_entropy,
            deterministic_verified=bool(base_verification.get("verified", False)),
            calibration_ema=float(drift_state.get("ema_abs_error", 0.0)),
            answer_type_match=bool(
                (state.research_verification.get("answer_type") or {}).get(
                    "match", False
                )
            ),
            winner_margin=winner_margin,
        )
        if confidence_multiplier < 0.999:
            posterior = float(
                bayesian_confidence.get("posterior_confidence", 0.0)
            ) * confidence_multiplier
            posterior = max(0.0, min(1.0, posterior))
            bayesian_confidence["posterior_confidence"] = posterior
            bayesian_confidence["posterior_risk"] = float(
                max(0.0, min(1.0, 1.0 - posterior))
            )
            bayesian_confidence["single_provider_multiplier"] = float(
                confidence_multiplier
            )

        cross_modal = dict(
            state.research_verification.get("cross_modal_consistency", {}) or {}
        )
        cross_modal_score = float(cross_modal.get("score", 0.0))
        cross_modal_applicable = bool(
            (state.intake and state.intake.input_type in {"image", "pdf", "mixed"})
            or state.vision_analysis is not None
            or state.ocr_data is not None
            or state.pdf_data is not None
        )
        cross_modal_penalty = (
            max(0.0, min(0.30, (0.30 - cross_modal_score) * 0.8))
            if (cross_modal_applicable and cross_modal_score < 0.30)
            else 0.0
        )

        effective_risk = max(
            raw_risk,
            float(bayesian_confidence.get("posterior_risk", raw_risk)),
        )
        effective_risk = float(
            max(0.0, min(1.0, effective_risk + cross_modal_penalty))
        )
        effective_confidence = float(max(0.0, min(1.0, 1.0 - effective_risk)))

        adaptive_state = _BAYESIAN.update_adaptive(
            features={
                "agreement": max(0.0, min(1.0, 1.0 - disagreement)),
                "entropy": effective_entropy,
                "deterministic": 1.0
                if bool(base_verification.get("verified", False))
                else 0.0,
                "calibration_ema": float(drift_state.get("ema_abs_error", 0.0)),
                "answer_type_match": 1.0
                if bool(
                    (state.research_verification.get("answer_type") or {}).get(
                        "match", False
                    )
                )
                else 0.0,
                "winner_margin": winner_margin,
            },
            predicted_confidence=float(
                bayesian_confidence.get("posterior_confidence", prior_conf)
            ),
            observed_success=bool(base_verification.get("verified", False)),
        )

        tm = perf_counter()
        state.meta_verification = await _run_meta_verification(
            question=state.question_text,
            solve_result=state.solve_result,
            profile=state.profile,
            enabled=bool(state.options.get("enable_meta_verification", True)),
            timeout_s=state.config.meta_timeout_s,
        )
        state.stage_timing["meta_verification_s"] = float(
            max(0.0, perf_counter() - tm)
        )
        _TELEMETRY.log_timing(
            stage="meta_verification",
            duration_s=state.stage_timing["meta_verification_s"],
            slow_threshold_s=5.0,
            extra={
                "input_type": state.intake.input_type if state.intake else state.input_type
            },
        )

        state.calibration_metrics = {
            "risk_score": effective_risk,
            "raw_risk_score": raw_risk,
            "confidence_score": effective_confidence,
            "verified": bool(base_verification.get("verified", False)),
            "entropy": effective_entropy,
            "raw_entropy": entropy,
            "disagreement": disagreement,
            "winner_margin": winner_margin,
            "single_provider_mode": bool(single_provider_mode),
            "cross_modal_penalty": float(cross_modal_penalty),
            "cross_modal_score": float(cross_modal_score),
            "bayesian": bayesian_confidence,
            "adaptive": adaptive_state,
            "drift": drift_state,
        }

        return {
            "research_score": float(state.research_verification.get("score", 0.0) or 0.0),
            "confidence_score": float(
                state.calibration_metrics.get("confidence_score", 0.0) or 0.0
            ),
            "meta_attempted": bool(
                state.meta_verification.get("attempted", False)
            ),
        }

    async def _stage_final_synthesis(self, state: PipelineState) -> Dict[str, Any]:
        solve_result = dict(state.solve_result or {})
        base_verification = dict(solve_result.get("verification", {}) or {})
        arena = dict(solve_result.get("arena", {}) or {})
        effective_risk = float(
            state.calibration_metrics.get(
                "risk_score", base_verification.get("risk_score", 1.0)
            )
            or 1.0
        )
        effective_confidence = float(
            state.calibration_metrics.get("confidence_score", 0.0) or 0.0
        )
        disagreement = float(state.calibration_metrics.get("disagreement", 0.0) or 0.0)
        entropy = float(state.calibration_metrics.get("entropy", 0.0) or 0.0)
        raw_risk = float(base_verification.get("raw_risk_score", base_verification.get("risk_score", 1.0)) or 1.0)

        quality_gate = dict(solve_result.get("quality_gate") or {})
        evidence_required = _should_require_citations(
            options=state.options,
            profile=state.profile,
            verified=bool(base_verification.get("verified", False)),
        )
        min_citations = int(
            max(
                0,
                min(
                    6,
                    int(
                        state.options.get(
                            "min_citation_count", _DEFAULT_MIN_CITATION_COUNT
                        )
                    ),
                ),
            )
        )
        min_evidence_score = float(
            max(
                0.2,
                min(
                    0.95,
                    float(
                        state.options.get(
                            "min_evidence_score", _DEFAULT_MIN_EVIDENCE_SCORE
                        )
                    ),
                ),
            )
        )
        evidence_mode = str(
            state.options.get("evidence_mode", "auto")
        ).strip().lower()
        if evidence_mode == "strict":
            strict_evidence = True
        elif evidence_mode == "soft":
            strict_evidence = False
        else:
            strict_evidence = evidence_required and not bool(
                base_verification.get("verified", False)
            )
        evidence_ok = (
            state.evidence_metrics.get("citation_count", 0) >= min_citations
            and float(state.evidence_metrics.get("score", 0.0) or 0.0)
            >= min_evidence_score
        )
        if evidence_required and not evidence_ok:
            reasons = [
                str(item)
                for item in (quality_gate.get("reasons") or [])
                if str(item).strip()
            ]
            if "insufficient_evidence" not in reasons:
                reasons.append("insufficient_evidence")
            quality_gate.update(
                {
                    "completion_ok": False
                    if strict_evidence
                    else quality_gate.get("completion_ok", True),
                    "final_status": "Failed"
                    if strict_evidence
                    else quality_gate.get("final_status", "Warning"),
                    "force_escalate": True,
                    "reasons": reasons,
                    "evidence": {
                        "required": True,
                        "ok": bool(evidence_ok),
                        "score": float(
                            state.evidence_metrics.get("score", 0.0) or 0.0
                        ),
                        "min_score": float(min_evidence_score),
                        "citations": int(
                            state.evidence_metrics.get("citation_count", 0) or 0
                        ),
                        "min_citations": int(min_citations),
                        "mode": evidence_mode,
                    },
                }
            )
            solve_result["quality_gate"] = quality_gate

        quality_failed = (
            str(quality_gate.get("final_status", "")).strip().lower() == "failed"
            or not bool(quality_gate.get("completion_ok", True))
        )
        meta_min_confidence = float(
            max(
                0.0,
                min(
                    1.0,
                    state.options.get("meta_override_min_confidence", 0.60),
                ),
            )
        )
        meta_max_risk = float(
            max(
                0.0,
                min(1.0, state.options.get("meta_override_max_risk", 0.40)),
            )
        )
        meta_max_disagreement = float(
            max(
                0.0,
                min(
                    1.0,
                    state.options.get("meta_override_max_disagreement", 0.65),
                ),
            )
        )
        verification_failed = not bool(base_verification.get("verified", False))
        low_confidence_guard = bool(
            verification_failed and effective_confidence < meta_min_confidence
        )
        high_risk_guard = bool(
            verification_failed and effective_risk > meta_max_risk
        )
        disagreement_guard = bool(float(disagreement) > meta_max_disagreement)

        if bool(base_verification.get("verified", False)):
            state.meta_verification["override_allowed"] = False
        if quality_failed or low_confidence_guard or high_risk_guard or disagreement_guard:
            state.meta_verification["override_allowed"] = False
            block_reasons = []
            if quality_failed:
                block_reasons.append("quality_gate_failed")
            if low_confidence_guard:
                block_reasons.append("low_confidence")
            if high_risk_guard:
                block_reasons.append("high_risk")
            if disagreement_guard:
                block_reasons.append("high_disagreement")
            state.meta_verification["override_block_reason"] = ",".join(
                block_reasons
            )

        suggested_correction = _normalize_meta_correction(
            state.meta_verification.get("suggested_correction")
        )
        correction_plausibility = {}
        if suggested_correction:
            correction_plausibility = check_answer_plausibility(
                question_text=state.question_text,
                final_answer=suggested_correction,
                metadata={
                    "numeric_expected": bool(getattr(state.profile, "numeric", False)),
                    "observed_type": "numeric"
                    if re.search(r"\d", suggested_correction)
                    else "text",
                },
            )
            state.meta_verification["suggested_correction_plausibility"] = (
                correction_plausibility
            )
        if (
            bool(state.meta_verification.get("override_allowed", False))
            and suggested_correction
            and bool(correction_plausibility.get("plausible", False))
            and not bool(state.meta_verification.get("timed_out", False))
            and not list(state.meta_verification.get("flags", []))
        ):
            original_answer = str(solve_result.get("final_answer", "")).strip()
            if suggested_correction != original_answer:
                solve_result["final_answer"] = suggested_correction
                state.meta_verification["applied_correction"] = True
                state.meta_verification["original_final_answer"] = original_answer
            else:
                state.meta_verification["applied_correction"] = False
        elif suggested_correction:
            state.meta_verification["applied_correction"] = False
            if (
                bool(state.meta_verification.get("override_allowed", False))
                and not bool(state.meta_verification.get("timed_out", False))
                and not list(state.meta_verification.get("flags", []))
                and not bool(correction_plausibility.get("plausible", False))
            ):
                state.meta_verification["correction_rejected"] = (
                    "implausible_suggested_correction"
                )

        provider_diagnostics = {
            "winner_provider": solve_result.get("winner_provider"),
            "provider_availability": (
                (solve_result.get("engine") or {}).get("provider_availability", {})
            ),
            "ranked_providers": arena.get("ranked_providers", []),
            "provider_comparison_vision": (
                state.vision_analysis or {}
            ).get("provider_comparison", []),
            "pre_reasoning_provider_warmup": state.provider_warm_report,
            "mcts_tree_arena_winner": str(
                (state.mcts_search.get("telemetry") or {}).get(
                    "arena_tree_winner", ""
                )
            ),
            "mcts_provider_tree_scores": dict(
                (state.mcts_search.get("telemetry") or {}).get(
                    "provider_tree_scores", {}
                )
            )
            if isinstance(
                (state.mcts_search.get("telemetry") or {}).get(
                    "provider_tree_scores"
                ),
                dict,
            )
            else {},
        }

        web_matches = [
            _json_sanitize(dict(row))
            for row in (state.web_search_payload.get("matches") or [])
            if isinstance(row, dict)
        ]
        web_results_found = len(web_matches)
        solution_used = bool(
            str(state.fetched_solution.get("solution_text", "")).strip()
            or str(state.fetched_solution.get("hint", "")).strip()
            or str(state.fetched_solution.get("answer", "")).strip()
        )
        solver_answer_norm = _normalize_answer_token(solve_result.get("final_answer"))
        retrieved_answer_norm = _normalize_answer_token(
            state.context_payload.get("possible_answer")
            or state.fetched_solution.get("answer")
        )
        mismatch_with_verified_answer = bool(
            retrieved_answer_norm
            and solver_answer_norm
            and (retrieved_answer_norm != solver_answer_norm)
            and bool(base_verification.get("verified", False))
        )
        web_retrieval = {
            "enabled": bool(state.config.web_retrieval_enabled),
            "query": str(
                state.web_search_payload.get("query")
                or state.normalized_question.get("search_query")
                or ""
            ),
            "cache_hit": bool(state.web_search_payload.get("cache_hit", False)),
            "input_analysis": _json_sanitize(dict(state.input_analysis)),
            "normalized_question": _json_sanitize(dict(state.normalized_question)),
            "matches": web_matches[:8],
            "solution": {
                "hint": str(state.fetched_solution.get("hint") or ""),
                "answer": str(state.fetched_solution.get("answer") or ""),
                "solution_text": str(
                    state.fetched_solution.get("solution_text") or ""
                ),
                "confidence": float(
                    state.fetched_solution.get("confidence", 0.0) or 0.0
                ),
                "source_url": str(
                    state.fetched_solution.get("source_url") or ""
                ),
                "source": str(state.fetched_solution.get("source") or ""),
                "formulas": [
                    str(x)
                    for x in (state.fetched_solution.get("formulas") or [])
                    if str(x).strip()
                ][:8],
            },
            "context_block": str(state.context_payload.get("context_block") or ""),
            "context_injected": bool(state.context_payload.get("context_block")),
            "citations": [
                _json_sanitize(dict(row))
                for row in (state.context_payload.get("citations") or [])
                if isinstance(row, dict)
            ],
            "sources_consulted": [
                str(x)
                for x in (state.context_payload.get("sources_consulted") or [])
                if str(x).strip()
            ],
            "mismatch_with_verified_answer": bool(mismatch_with_verified_answer),
            "evidence": _json_sanitize(dict(state.evidence_metrics)),
            "latency_s": {
                "search": float(state.stage_timing.get("web_search_s", 0.0)),
                "fetch": float(state.stage_timing.get("web_fetch_s", 0.0)),
                "parallel": float(
                    state.stage_timing.get("retrieval_parallel_s", 0.0)
                ),
            },
            "vault_blocks": _json_sanitize(
                state.vault_context_payload.get("blocks", [])
            ),
            "vault_context_block": str(
                state.vault_context_payload.get("context_block") or ""
            ),
        }

        multimodal = {
            "input_detected_type": state.intake.input_type if state.intake else state.input_type,
            "files": state.intake.files if state.intake else [],
            "ocr_available": state.ocr_data is not None,
            "pdf_available": state.pdf_data is not None,
            "vision_available": state.vision_analysis is not None,
            "ocr_question_count": len(
                (state.ocr_data or {}).get("lc_iie_questions", []) or []
            ),
            "pdf_question_count": len(
                (state.pdf_data or {}).get("lc_iie_questions", []) or []
            ),
        }
        effective_verification = dict(base_verification)
        effective_verification["raw_risk_score"] = raw_risk
        effective_verification["risk_score"] = effective_risk

        suppress_failed_answer = bool(
            quality_failed
            and not bool(effective_verification.get("verified", False))
            and (low_confidence_guard or high_risk_guard or disagreement_guard)
        )
        empty_final_answer_guard = bool(
            (not str(solve_result.get("final_answer", "")).strip())
            and not bool(effective_verification.get("verified", False))
        )
        if suppress_failed_answer:
            unsafe_candidate = str(solve_result.get("final_answer", "")).strip()
            if unsafe_candidate:
                solve_result["unsafe_candidate_answer"] = unsafe_candidate
            solve_result["final_answer"] = str(
                state.options.get("uncertain_answer_message")
                or "Uncertain answer: verification failed under high risk. Please retry with a stronger model."
            )
            solve_result["escalate"] = True
            solve_result["final_status"] = "Failed"
            quality_gate["output_suppressed"] = True
            solve_result["quality_gate"] = quality_gate
            if suggested_correction and not bool(
                state.meta_verification.get("applied_correction", False)
            ):
                state.meta_verification.setdefault(
                    "correction_rejected", "guarded_failure_state"
                )
        elif empty_final_answer_guard:
            solve_result["final_answer"] = str(
                state.options.get("empty_answer_message")
                or "Uncertain answer: providers returned no usable output. Please retry with stronger settings."
            )
            solve_result["escalate"] = True
            solve_result["final_status"] = "Failed"
            quality_gate = dict(solve_result.get("quality_gate") or {})
            reasons = [
                str(item)
                for item in (quality_gate.get("reasons") or [])
                if str(item).strip()
            ]
            if "empty_final_answer" not in reasons:
                reasons.append("empty_final_answer")
            quality_gate.update(
                {
                    "completion_ok": False,
                    "final_status": "Failed",
                    "force_escalate": True,
                    "reasons": reasons,
                    "output_suppressed": True,
                }
            )
            solve_result["quality_gate"] = quality_gate
        elif evidence_required and not evidence_ok and strict_evidence:
            solve_result["final_answer"] = str(
                state.options.get("citation_missing_message")
                or "Insufficient evidence to answer with citations. Please refine the question or allow non-cited mode."
            )
            solve_result["escalate"] = True
            solve_result["final_status"] = "Failed"
            quality_gate = dict(solve_result.get("quality_gate") or {})
            reasons = [
                str(item)
                for item in (quality_gate.get("reasons") or [])
                if str(item).strip()
            ]
            if "insufficient_evidence" not in reasons:
                reasons.append("insufficient_evidence")
            quality_gate.update(
                {
                    "completion_ok": False,
                    "final_status": "Failed",
                    "force_escalate": True,
                    "reasons": reasons,
                    "output_suppressed": True,
                }
            )
            solve_result["quality_gate"] = quality_gate

        enable_citation_map = bool(
            state.options.get(
                "enable_citation_map",
                state.config.function_hint
                in {"ai_chat", "chat_ai", "general_chat", "ai_solve", "chat"},
            )
        )
        citation_map = (
            _build_citation_map(
                answer_text=str(solve_result.get("final_answer", "")),
                explanation_text=str(
                    solve_result.get("explanation")
                    or solve_result.get("reasoning")
                    or solve_result.get("solution")
                    or ""
                ),
                citations=web_retrieval.get("citations", []),
            )
            if enable_citation_map
            else []
        )

        out = {
            **solve_result,
            "status": "uncertain"
            if (suppress_failed_answer or empty_final_answer_guard)
            else "ok",
            "question": state.question_text,
            "verification": effective_verification,
            "input_metadata": multimodal,
            "input_analysis": state.input_analysis,
            "ocr_data": state.ocr_data,
            "pdf_data": state.pdf_data,
            "vision_analysis": state.vision_analysis,
            "web_retrieval": web_retrieval,
            "mcts_search": state.mcts_search,
            "reasoning_graph": state.reasoning_graph,
            "citations": web_retrieval.get("citations", []),
            "citation_map": citation_map,
            "sources_consulted": web_retrieval.get("sources_consulted", []),
            "provider_diagnostics": provider_diagnostics,
            "research_verification": state.research_verification,
            "calibration_metrics": state.calibration_metrics,
            "meta_verification": state.meta_verification,
            "evidence": {
                **dict(state.evidence_metrics),
                "required": bool(evidence_required),
                "ok": bool(evidence_ok),
                "mode": evidence_mode,
                "min_score": float(min_evidence_score),
                "min_citations": int(min_citations),
            },
            "entropy": entropy,
            "disagreement": disagreement,
            "latency_metrics": dict(state.stage_timing),
            "user_context": state.user_context,
        }
        if bool(state.options.get("include_pipeline_debug", False)):
            out["pipeline"] = {
                "stage_records": _json_sanitize(state.stage_records),
                "stage_failures": _json_sanitize(state.stage_failures),
                "vault_retrieval": _json_sanitize(
                    state.vault_context_payload.get("blocks", [])
                ),
                "reevaluation": _json_sanitize(state.reevaluation),
            }
        out = _json_sanitize(out)
        visualization = _DESMOS.build(
            question=state.question_text, profile=state.profile_dict
        )
        if visualization is not None:
            out["answer"] = str(out.get("final_answer", ""))
            out["confidence"] = round(float(effective_confidence), 6)
            out["visualization"] = visualization

        response_style = str(state.options.get("response_style", "") or "").strip().lower()
        persona_display_allowed = response_style in {
            "casual_chat",
            "companion_chat",
        }
        if bool(state.options.get("enable_persona", True)) and persona_display_allowed:
            try:
                out["display_answer"] = apply_persona(
                    str(out.get("final_answer", ""))
                )
            except Exception as exc:
                out["display_answer"] = str(out.get("final_answer", ""))
                _TELEMETRY.log_event(
                    "persona_error",
                    {
                        "error_type": type(exc).__name__,
                        "reason": str(exc)[:240],
                    },
                )
        else:
            out["display_answer"] = str(out.get("final_answer", ""))

        if (
            state.intake
            and state.intake.input_type in {"image", "pdf", "mixed"}
            and not bool(base_verification.get("verified", False))
        ):
            _TELEMETRY.cluster_failure(
                failure_type="multimodal_unverified",
                modality=state.intake.input_type,
                profile={
                    "subject": str(
                        (solve_result.get("profile") or {}).get(
                            "subject", getattr(state.profile, "subject", "")
                        )
                    ),
                    "difficulty": str(
                        (solve_result.get("profile") or {}).get(
                            "difficulty", getattr(state.profile, "difficulty", "")
                        )
                    ),
                },
            )

        if state.config.mcts_enabled:
            try:
                mcts_meta = dict(state.mcts_search.get("telemetry") or {})
                await _MCTS_LOGGER.log_event(
                    question=state.question_text,
                    iterations=int(mcts_meta.get("iterations", 0) or 0),
                    nodes_explored=int(mcts_meta.get("nodes_explored", 0) or 0),
                    tool_calls=int(mcts_meta.get("tool_calls", 0) or 0),
                    retrieval_calls=int(mcts_meta.get("retrieval_calls", 0) or 0),
                    verification_pass=bool(
                        mcts_meta.get("verification_pass", False)
                    ),
                    final_confidence=float(effective_confidence),
                    metadata={
                        "mcts_status": str(state.mcts_search.get("status", "")),
                        "mcts_stop_reason": str(mcts_meta.get("stop_reason", "")),
                        "input_type": state.intake.input_type
                        if state.intake
                        else state.input_type,
                        "mcts_developer_mode": bool(
                            state.mcts_search.get("developer_mode", False)
                        ),
                        "latency_s": float(
                            mcts_meta.get(
                                "latency_s", state.stage_timing.get("mcts_s", 0.0)
                            )
                        ),
                        "arena_tree_winner": str(
                            mcts_meta.get("arena_tree_winner", "")
                        ),
                        "provider_tree_scores": dict(
                            mcts_meta.get("provider_tree_scores", {})
                        )
                        if isinstance(
                            mcts_meta.get("provider_tree_scores"), dict
                        )
                        else {},
                    },
                )
            except Exception:
                pass

        if state.config.got_enabled:
            try:
                got_meta = dict(state.reasoning_graph.get("telemetry") or {})
                await _REASONING_GRAPH_LOGGER.log_event(
                    question=state.question_text,
                    node_count=int(got_meta.get("node_count", 0) or 0),
                    tool_calls=int(got_meta.get("tool_calls", 0) or 0),
                    retrieval_nodes=int(got_meta.get("retrieval_nodes", 0) or 0),
                    verification_pass=bool(
                        got_meta.get("verification_pass", False)
                    ),
                    final_confidence=float(effective_confidence),
                    metadata={
                        "stop_reason": str(got_meta.get("stop_reason", "")),
                        "got_enabled": bool(state.config.got_enabled),
                        "got_status": str(state.reasoning_graph.get("status", "")),
                        "early_verified": bool(
                            state.reasoning_graph.get("early_verified", False)
                        ),
                        "graph_arena_winner": str(
                            got_meta.get("arena_graph_winner", "")
                        ),
                        "input_type": state.intake.input_type
                        if state.intake
                        else state.input_type,
                        "latency_s": float(
                            got_meta.get(
                                "latency_s", state.stage_timing.get("got_s", 0.0)
                            )
                        ),
                        "provider_scores": dict(
                            got_meta.get("provider_graph_scores", {})
                        )
                        if isinstance(
                            got_meta.get("provider_graph_scores"), dict
                        )
                        else {},
                    },
                )
            except Exception:
                pass

        if state.config.pre_reasoning_enabled:
            try:
                await _SEARCH_CACHE.log_search_event(
                    question=state.question_text,
                    ocr_used=bool(state.input_analysis.get("ocr_used", False)),
                    web_results_found=web_results_found,
                    solution_used=solution_used,
                    lalacore_provider=str(solve_result.get("winner_provider", "")),
                    arena_triggered=bool(
                        len(arena.get("ranked_providers", []) or []) > 1
                        or float(arena.get("disagreement", 0.0) or 0.0) > 0.0
                    ),
                    verification_passed=bool(base_verification.get("verified", False)),
                    mismatch_detected=bool(mismatch_with_verified_answer),
                    metadata={
                        "cache_hit": bool(web_retrieval.get("cache_hit", False)),
                        "query": str(web_retrieval.get("query", "")),
                        "sources_consulted": list(
                            web_retrieval.get("sources_consulted", [])
                        ),
                        "citation_count": len(web_retrieval.get("citations", [])),
                        "context_injected": bool(
                            web_retrieval.get("context_injected", False)
                        ),
                        "web_search_enabled": bool(
                            state.config.web_retrieval_enabled
                        ),
                        "vault_blocks": len(
                            state.vault_context_payload.get("blocks", [])
                        ),
                    },
                )
            except Exception:
                pass

        state.final_output = out
        return {
            "status": str(out.get("status", "")),
            "final_answer_preview": str(out.get("final_answer", ""))[
                :_DEFAULT_STAGE_LOG_PREVIEW_CHARS
            ],
            "confidence": float(effective_confidence),
        }

    def _release_stage_memory(self, stage_name: str, state: PipelineState) -> None:
        if stage_name == "stage1_intake_normalization":
            state.pdf_primary_ocr = None
            gc.collect()

    async def _cleanup(self, state: PipelineState) -> None:
        if state.provider_warm_task is not None and not state.provider_warm_task.done():
            state.provider_warm_task.cancel()
            try:
                await state.provider_warm_task
            except BaseException:
                pass


_PIPELINE_CONTROLLER = LalaCorePipelineController()


async def lalacore_entry(
    input_data: Any,
    input_type: str = "auto",  # text | image | pdf | auto
    user_context: Dict[str, Any] | None = None,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Unified public entrypoint for app integration.
    """

    async def _run_pipeline() -> Dict[str, Any]:
        async with _PIPELINE_SEMAPHORE:
            return await _PIPELINE_CONTROLLER.execute(
                input_data=input_data,
                input_type=input_type,
                user_context=user_context,
                options=options,
            )

    try:
        return await asyncio.wait_for(_run_pipeline(), timeout=_PIPELINE_TIMEOUT_S)
    except asyncio.TimeoutError:
        _TELEMETRY.log_event(
            "pipeline_timeout",
            {
                "timeout_s": float(_PIPELINE_TIMEOUT_S),
                "input_type": str(input_type or "auto"),
            },
        )
        return {
            "status": "error",
            "error": "pipeline_timeout",
            "message": f"Pipeline exceeded {_PIPELINE_TIMEOUT_S:.1f}s timeout.",
            "input_metadata": {"detected_type": str(input_type or "auto")},
            "latency_metrics": {"timeout_s": float(_PIPELINE_TIMEOUT_S)},
        }
    except Exception as exc:
        _TELEMETRY.log_event(
            "pipeline_runtime_failure",
            {
                "error_type": type(exc).__name__,
                "reason": str(exc)[:240],
                "input_type": str(input_type or "auto"),
            },
        )
        return {
            "status": "error",
            "error": "pipeline_runtime_failure",
            "message": str(exc)[:240],
            "input_metadata": {"detected_type": str(input_type or "auto")},
        }


class _DeferredASGIApp:
    def __init__(self) -> None:
        self._app = None

    def _load(self):
        if self._app is None:
            from app.main import app as main_app

            self._app = main_app
        return self._app

    async def __call__(self, scope, receive, send) -> None:
        app = self._load()
        await app(scope, receive, send)

    def __getattr__(self, name: str):
        return getattr(self._load(), name)


app = _DeferredASGIApp()


async def _run_meta_verification(
    *,
    question: str,
    solve_result: Dict[str, Any],
    profile,
    enabled: bool,
    timeout_s: float,
) -> Dict[str, Any]:
    if not enabled:
        return {"attempted": False, "reason": "disabled"}

    winner = str(solve_result.get("winner_provider", "")).strip() or "mini"
    answer = str(solve_result.get("final_answer", "")).strip()
    original_reasoning = str(solve_result.get("reasoning", "")).strip()
    if not answer:
        return {"attempted": False, "reason": "empty_final_answer"}
    if not question.strip():
        return {"attempted": False, "reason": "empty_question"}

    question_trimmed = _truncate_words(question, 320)
    answer_trimmed = str(answer)[:600]
    prompt = (
        "Meta-verification task:\n"
        "Given the original question and candidate final answer, check logical consistency.\n"
        "If inconsistent, provide a corrected final answer.\n\n"
        f"Question: {question_trimmed}\n"
        f"Candidate Final Answer: {answer_trimmed}\n\n"
        "Return format:\n"
        "Reasoning: <brief consistency check>\n"
        "Final Answer: <same as candidate if consistent, else corrected answer>"
    )

    global _PROVIDER_FABRIC
    if _PROVIDER_FABRIC is None:
        _PROVIDER_FABRIC = ProviderFabric()

    reviewer = winner
    try:
        available = [str(p).strip() for p in _PROVIDER_FABRIC.available_providers() if str(p).strip()]
    except Exception:
        available = []
    if reviewer == "mini":
        for preferred in ("openrouter", "gemini", "groq", "hf"):
            if preferred in available:
                reviewer = preferred
                break
    if reviewer not in available and available:
        reviewer = available[0]

    try:
        review = await asyncio.wait_for(
            _PROVIDER_FABRIC.generate(reviewer, prompt, profile, []),
            timeout=float(max(1.0, timeout_s)),
        )
    except asyncio.TimeoutError:
        return {
            "attempted": True,
            "provider": reviewer,
            "consistent": False,
            "timed_out": True,
            "reason": "meta_verification_timeout",
            "override_allowed": not bool((solve_result.get("verification") or {}).get("verified", False)),
        }
    except Exception as exc:
        return {
            "attempted": True,
            "provider": reviewer,
            "consistent": False,
            "error": str(exc)[:240],
            "timed_out": False,
            "reason": "meta_verification_error",
            "override_allowed": not bool((solve_result.get("verification") or {}).get("verified", False)),
        }

    review_reasoning = _truncate_words(str(review.reasoning or ""), 180)
    review_answer = str(review.final_answer or "").strip()
    consistent = review_answer == answer
    contradiction = _RESEARCH_VERIFIER.detect_self_contradiction(original_reasoning, review_reasoning)
    circular_reasoning = False
    if review_reasoning and original_reasoning:
        similarity = SequenceMatcher(a=original_reasoning[:1600], b=review_reasoning[:1600]).ratio()
        if similarity >= 0.92 and review_answer and review_answer != answer:
            circular_reasoning = True
    flags = []
    if contradiction.get("contradiction"):
        flags.append("self_contradiction")
    if circular_reasoning:
        flags.append("circular_reasoning")

    return {
        "attempted": True,
        "provider": reviewer,
        "consistent": bool(consistent),
        "review_reasoning": review_reasoning,
        "review_final_answer": review_answer,
        "suggested_correction": None if consistent or flags else review_answer,
        "self_contradiction": contradiction,
        "circular_reasoning": bool(circular_reasoning),
        "flags": flags,
        "timed_out": False,
        "override_allowed": not bool((solve_result.get("verification") or {}).get("verified", False)),
    }


def _compose_question(parts: Sequence[str], *, fallback: str) -> str:
    cleaned = [str(part).strip() for part in parts if str(part).strip()]
    if not cleaned:
        return str(fallback or "").strip()
    if len(cleaned) == 1:
        return cleaned[0]

    # Preserve context sources while keeping the prompt bounded.
    capped = []
    total = 0
    for part in cleaned:
        if total > 14_000:
            break
        capped.append(part)
        total += len(part)

    return "\n\n".join(capped)


def _build_vision_from_ocr(ocr_payload: Dict[str, Any], *, profile_dict: Dict[str, Any] | None = None) -> Dict[str, Any]:
    text = str(
        (ocr_payload or {}).get("clean_text")
        or (ocr_payload or {}).get("math_normalized_text")
        or (ocr_payload or {}).get("raw_text", "")
    ).strip()
    layout_blocks = [row for row in ((ocr_payload or {}).get("layout_blocks") or []) if isinstance(row, dict)]
    parsed = _OCR_DIAGRAM.parse(text, layout_blocks)
    points = len(parsed.get("points", []) or [])
    segments = len(parsed.get("segments", []) or [])
    angles = len(parsed.get("angles", []) or [])
    geometry_detected = bool(parsed.get("is_geometry", False))
    subject = str((profile_dict or {}).get("subject", "general"))
    confidence = min(
        1.0,
        0.50 * float((ocr_payload or {}).get("confidence", 0.0) or 0.0)
        + (0.30 if geometry_detected else 0.12)
        + 0.12,
    )
    return {
        "winner_provider": "ocr_diagram_fusion",
        "provider": "ocr_diagram_fusion",
        "detected_text": text,
        "detected_diagrams": {
            "geometry": geometry_detected,
            "points": points,
            "segments": segments,
            "angles": angles,
        },
        "structured_math_expressions": [],
        "figure_interpretation": (
            f"Diagram cues detected from OCR ({points} points, {segments} segments, {angles} angles)."
            if geometry_detected
            else f"No explicit diagram cues from OCR; treated as {subject} textual prompt."
        ),
        "geometry_objects": parsed.get("abstraction", {}),
        "confidence": float(max(0.0, min(1.0, confidence))),
        "provider_comparison": [],
        "entropy": 0.42,
        "disagreement": 0.55,
        "single_provider_uncertainty": True,
        "ocr": dict(ocr_payload or {}),
        "ocr_model": str((ocr_payload or {}).get("ocr_model", "")),
    }


def _merge_vision_with_ocr(vision_analysis: Dict[str, Any], ocr_payload: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(vision_analysis or {})
    ocr_clean = str(ocr_payload.get("clean_text") or ocr_payload.get("math_normalized_text") or ocr_payload.get("raw_text", "")).strip()
    if not str(merged.get("detected_text", "")).strip() and ocr_clean:
        merged["detected_text"] = ocr_clean

    merged_ocr = dict(merged.get("ocr") or {})
    for key in ("clean_text", "math_normalized_text", "raw_text", "layout_blocks", "confidence", "ocr_model"):
        if key in ocr_payload and (key not in merged_ocr or not merged_ocr.get(key)):
            merged_ocr[key] = ocr_payload.get(key)
    merged["ocr"] = merged_ocr

    if not isinstance(merged.get("geometry_objects"), dict) or not merged.get("geometry_objects"):
        parsed = _OCR_DIAGRAM.parse(
            ocr_clean,
            [row for row in (ocr_payload.get("layout_blocks") or []) if isinstance(row, dict)],
        )
        merged["geometry_objects"] = parsed.get("abstraction", {})
        diag = dict(merged.get("detected_diagrams") or {})
        diag.setdefault("geometry", bool(parsed.get("is_geometry", False)))
        diag.setdefault("points", len(parsed.get("points", []) or []))
        diag.setdefault("segments", len(parsed.get("segments", []) or []))
        diag.setdefault("angles", len(parsed.get("angles", []) or []))
        merged["detected_diagrams"] = diag
    return merged


def _truncate_words(text: str, max_words: int) -> str:
    words = [w for w in str(text or "").split() if w]
    if len(words) <= int(max_words):
        return " ".join(words)
    return " ".join(words[: int(max_words)])


def _normalize_meta_correction(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("```") and text.endswith("```"):
        text = text.strip("`").strip()
    boxed = text
    if boxed.startswith("\\boxed{") and boxed.endswith("}"):
        boxed = boxed[len("\\boxed{") : -1].strip()
    return boxed or text


async def _warm_provider_availability() -> Dict[str, Any]:
    global _PROVIDER_FABRIC
    if _PROVIDER_FABRIC is None:
        _PROVIDER_FABRIC = ProviderFabric()
    try:
        providers = await asyncio.to_thread(_PROVIDER_FABRIC.available_providers)
    except Exception:
        providers = []
    return {"providers": [str(p) for p in providers if str(p).strip()], "ok": bool(providers)}


def _normalize_answer_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = text.replace("\\boxed{", "").replace("}", "")
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^a-z0-9+\-*/.=()]", "", text)
    return text
