from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any, Dict, List, Sequence

from core.lalacore_x.providers import (
    ProviderFabric,
    provider_model_priority_plan,
    provider_runtime_budget,
)
from core.lalacore_x.schemas import ProblemProfile
from core.math.contextual_math_solver import solve_contextual_math_question


def _clamp(value: Any, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = lo
    return max(lo, min(hi, numeric))


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _dedupe_strings(items: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = _as_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _normalize_answer_token(text: str) -> str:
    normalized = str(text or "").strip().lower()
    normalized = re.sub(r"\\boxed\{([^{}]+)\}", r"\1", normalized)
    normalized = normalized.replace("therefore", " ").replace("hence", " ")
    normalized = normalized.replace(" or ", ",")
    normalized = normalized.replace(" and ", ",")
    normalized = normalized.replace(";", ",")
    normalized = re.sub(r"\b[a-z]\s*=\s*", "", normalized)
    normalized = re.sub(r"[^a-z0-9+\-*/^=(),.√\\]", "", normalized)
    normalized = re.sub(r",+", ",", normalized).strip(",")
    return normalized


def _extract_json_object(text: str) -> Dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
    decoder = json.JSONDecoder()
    for idx, char in enumerate(raw):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(raw[idx:])
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _contextual_correction_text(contextual: Dict[str, Any]) -> str:
    expressions = [
        _as_text(item)
        for item in (contextual.get("expected_expressions") or [])
        if _as_text(item)
    ]
    if expressions:
        return ", ".join(expressions)
    equations = [
        _as_text(item)
        for item in (contextual.get("expected_equations") or [])
        if _as_text(item)
    ]
    if equations:
        return "; ".join(equations)
    solution_text = _as_text(contextual.get("expected_solution_text"))
    if solution_text:
        return solution_text
    answer = _as_text(contextual.get("answer"))
    if answer:
        return answer
    expected_expr = _as_text(contextual.get("expected_expr"))
    return expected_expr


def _split_answer_parts(text: str) -> List[str]:
    cleaned = _as_text(text)
    if not cleaned:
        return []
    parts = re.split(r"(?:,|;|\band\b|\bor\b)", cleaned, flags=re.IGNORECASE)
    out: List[str] = []
    for part in parts:
        token = _normalize_answer_token(part)
        if token:
            out.append(token)
    return sorted(set(out))


def _answers_roughly_match(lhs: str, rhs: str) -> bool:
    left = _normalize_answer_token(lhs)
    right = _normalize_answer_token(rhs)
    if left and right and left == right:
        return True
    left_parts = _split_answer_parts(lhs)
    right_parts = _split_answer_parts(rhs)
    return bool(left_parts and right_parts and left_parts == right_parts)


def _verification_missing_ground_truth(base_verification: Dict[str, Any]) -> bool:
    reason = _as_text(
        base_verification.get("failure_reason") or base_verification.get("reason")
    ).lower()
    return "missing_ground_truth" in reason or "no expected answer" in reason


def _heuristic_review(
    *,
    question: str,
    candidate_answer: str,
    candidate_reasoning: str,
    base_verification: Dict[str, Any],
    research_verification: Dict[str, Any],
) -> Dict[str, Any]:
    answer = _as_text(candidate_answer)
    reasoning = _as_text(candidate_reasoning)
    base_verified = bool(base_verification.get("verified", False))
    base_risk = _clamp(base_verification.get("risk_score", 1.0), 0.0, 1.0)
    base_confidence = _clamp(
        base_verification.get("confidence_score", 1.0 - base_risk),
        0.0,
        1.0,
    )
    quality = _clamp(1.0 - base_risk, 0.05, 0.98)
    consistent = bool(base_verified)
    should_block = False
    suggested_correction = ""
    review_final_answer = answer
    issues: List[str] = []
    notes: List[str] = []
    hard_block = False

    if not answer:
        issues.append("empty_candidate_answer")
        notes.append("Candidate answer is empty.")
        should_block = True
        hard_block = True
        consistent = False
        quality = 0.04
        base_risk = max(base_risk, 0.98)

    failure_reason = _as_text(
        base_verification.get("failure_reason") or base_verification.get("reason")
    ).lower()
    missing_ground_truth = _verification_missing_ground_truth(base_verification)
    if not base_verified and base_risk >= 0.88 and not missing_ground_truth:
        issues.append("verification_failed_high_risk")
        notes.append(
            "Deterministic verification marked the answer high risk, but a fast verifier review is still allowed."
        )
        consistent = False
        quality = min(quality, 0.22)
    elif not base_verified:
        issues.append("verification_failed")
        notes.append("Deterministic verification did not certify the answer.")

    contextual = solve_contextual_math_question(question)
    contextual_correction = (
        _contextual_correction_text(contextual)
        if isinstance(contextual, dict) and bool(contextual.get("handled"))
        else ""
    )
    if contextual_correction and not base_verified and not _answers_roughly_match(
        answer,
        contextual_correction,
    ):
        suggested_correction = contextual_correction
        review_final_answer = contextual_correction
        issues.append("deterministic_contextual_available")
        notes.append("A deterministic contextual solve path found a better grounded answer.")
        should_block = True
        hard_block = True
        consistent = False
        base_risk = max(base_risk, 0.94)
        quality = min(quality, 0.08)
    elif contextual_correction:
        review_final_answer = contextual_correction
        quality = max(quality, 0.74 if not base_verified else 0.92)
        base_risk = min(base_risk, 0.28 if not base_verified else 0.08)
        consistent = True
        if not base_verified:
            notes.append(
                "The candidate answer matches the deterministic contextual solve after normalization."
            )

    answer_type = dict(research_verification.get("answer_type") or {})
    if not bool(answer_type.get("match", True)) and not base_verified:
        issues.append("answer_type_mismatch")
        notes.append("Research verification found an answer-type mismatch.")
        base_risk = max(base_risk, 0.82)
        quality = min(quality, 0.24)

    if missing_ground_truth:
        notes.append("Ground truth was unavailable, so heuristic scoring leans on plausibility and context.")
        if not should_block:
            base_risk = min(base_risk, 0.55)
            quality = max(quality, 0.46)
            consistent = bool(answer)

    if reasoning and answer:
        normalized_reasoning = _normalize_answer_token(reasoning)
        normalized_answer = _normalize_answer_token(answer)
        if normalized_reasoning == normalized_answer and len(reasoning.split()) < 12:
            issues.append("reasoning_thin")
            notes.append("Reasoning is too thin to justify the answer confidently.")
            if not base_verified:
                base_risk = max(base_risk, 0.78)
                quality = min(quality, 0.28)

    verdict = "safe"
    if should_block or (hard_block and base_risk >= 0.9):
        verdict = "unsafe"
    elif not consistent:
        verdict = "uncertain"

    confidence_score = _clamp(
        min(base_confidence if base_confidence > 0.0 else (1.0 - base_risk), 1.0 - base_risk),
        0.0,
        1.0,
    )
    return {
        "attempted": False,
        "provider": "heuristic_guard",
        "model": "heuristic_guard_v2",
        "method": "heuristic_only",
        "consistent": bool(consistent),
        "review_reasoning": " ".join(notes).strip(),
        "review_final_answer": review_final_answer,
        "suggested_correction": suggested_correction or None,
        "flags": _dedupe_strings(issues),
        "issues": _dedupe_strings(issues),
        "timed_out": False,
        "override_allowed": not bool(base_verified),
        "risk_score": _clamp(base_risk, 0.0, 1.0),
        "confidence_score": confidence_score,
        "answer_quality_score": _clamp(quality, 0.0, 1.0),
        "should_block_response": bool(should_block),
        "verdict": verdict,
        "_base_verified": bool(base_verified),
        "_deterministic_correction": bool(suggested_correction),
        "_hard_block": bool(hard_block),
        "_missing_ground_truth": bool(missing_ground_truth),
    }


def _review_candidates(fabric: ProviderFabric) -> List[tuple[str, str]]:
    configured = [
        (
            _as_text(os.getenv("LC9_FAST_VERIFIER_PROVIDER") or "gemini").lower(),
            _as_text(os.getenv("LC9_FAST_VERIFIER_MODEL") or "gemini-2.5-flash-lite"),
        ),
        (
            _as_text(os.getenv("LC9_FAST_VERIFIER_FALLBACK_1_PROVIDER") or "openrouter").lower(),
            _as_text(os.getenv("LC9_FAST_VERIFIER_FALLBACK_1_MODEL") or "openai/gpt-4o-mini"),
        ),
        (
            _as_text(os.getenv("LC9_FAST_VERIFIER_FALLBACK_2_PROVIDER") or "groq").lower(),
            _as_text(os.getenv("LC9_FAST_VERIFIER_FALLBACK_2_MODEL") or "llama-3.1-8b-instant"),
        ),
    ]
    try:
        available = {
            str(provider).strip().lower()
            for provider in fabric.available_providers()
            if str(provider).strip()
        }
    except Exception:
        available = set()
    candidates: List[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for provider, model in configured:
        if not provider or not model:
            continue
        if available and provider not in available:
            continue
        key = (provider, model)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(key)
    if candidates or not available:
        return candidates
    for provider, model in (
        ("gemini", "gemini-2.5-flash-lite"),
        ("openrouter", "openai/gpt-4o-mini"),
        ("groq", "llama-3.1-8b-instant"),
        ("hf", "meta-llama/Llama-3.1-8B-Instruct"),
    ):
        if provider in available:
            candidates.append((provider, model))
    return candidates


def _build_review_prompt(
    *,
    question: str,
    candidate_answer: str,
    candidate_reasoning: str,
    base_verification: Dict[str, Any],
    research_verification: Dict[str, Any],
) -> str:
    question_trimmed = _as_text(question)[:1600]
    answer_trimmed = _as_text(candidate_answer)[:600]
    reasoning_trimmed = _as_text(candidate_reasoning)[:1200]
    verification_summary = json.dumps(
        {
            "verified": bool(base_verification.get("verified", False)),
            "risk_score": _clamp(base_verification.get("risk_score", 1.0)),
            "confidence_score": _clamp(
                base_verification.get("confidence_score", 0.0)
            ),
            "failure_reason": _as_text(
                base_verification.get("failure_reason")
                or base_verification.get("reason")
            )[:160],
            "stage_results": dict(base_verification.get("stage_results") or {}),
            "research_answer_type": dict(research_verification.get("answer_type") or {}),
            "research_score": _clamp(research_verification.get("score", 0.0)),
        },
        ensure_ascii=True,
    )
    return (
        "You are a fast answer-quality verifier for an educational AI system.\n"
        "Judge whether the candidate answer is safe and useful to show as the final answer.\n"
        "Use the question, candidate answer, candidate reasoning, and verification summary.\n"
        "Be realistic, not over-conservative: do not block a good answer just because verification is incomplete.\n"
        "Use should_block_response=true only when the answer is likely wrong, misleading, or unusable.\n"
        "Always provide the best usable answer you can in review_final_answer.\n"
        "Use suggested_correction only when you can confidently improve or replace the candidate answer.\n\n"
        f"Question: {question_trimmed}\n"
        f"Candidate Answer: {answer_trimmed}\n"
        f"Candidate Reasoning: {reasoning_trimmed}\n"
        f"Verification Summary: {verification_summary}\n\n"
        "Return exactly this format:\n"
        "Reasoning: <one short line>\n"
        'Final Answer: {"consistent":true,"confidence_score":0.0,"risk_score":0.0,"answer_quality_score":0.0,"should_block_response":false,"verdict":"safe","suggested_correction":"","review_final_answer":"","review_reasoning":"","issues":[]}\n'
    )


def _parse_model_review(raw_text: str, *, candidate_answer: str, reasoning: str) -> Dict[str, Any] | None:
    payload = _extract_json_object(raw_text)
    if not isinstance(payload, dict):
        return None
    issues = _dedupe_strings(payload.get("issues") or [])
    review_final_answer = _as_text(
        payload.get("review_final_answer")
        or payload.get("suggested_correction")
        or (candidate_answer if bool(payload.get("consistent", False)) else "")
    )
    suggested_correction = _as_text(payload.get("suggested_correction"))
    consistent = bool(payload.get("consistent", False))
    risk_score = _clamp(payload.get("risk_score", 1.0), 0.0, 1.0)
    confidence_score = _clamp(payload.get("confidence_score", 1.0 - risk_score), 0.0, 1.0)
    answer_quality_score = _clamp(
        payload.get("answer_quality_score", payload.get("quality_score", 1.0 - risk_score)),
        0.0,
        1.0,
    )
    verdict = _as_text(payload.get("verdict")).lower() or (
        "unsafe" if bool(payload.get("should_block_response", False)) else "uncertain"
    )
    return {
        "consistent": consistent,
        "review_reasoning": _as_text(payload.get("review_reasoning") or reasoning),
        "review_final_answer": review_final_answer or candidate_answer,
        "suggested_correction": suggested_correction or None,
        "flags": issues,
        "issues": issues,
        "risk_score": risk_score,
        "confidence_score": confidence_score,
        "answer_quality_score": answer_quality_score,
        "should_block_response": bool(payload.get("should_block_response", False)),
        "verdict": verdict,
    }


def _merge_reviews(heuristic: Dict[str, Any], model_review: Dict[str, Any]) -> Dict[str, Any]:
    base_verified = bool(heuristic.get("_base_verified", False))
    deterministic_correction = bool(heuristic.get("_deterministic_correction", False))
    hard_block = bool(heuristic.get("_hard_block", False))
    missing_ground_truth = bool(heuristic.get("_missing_ground_truth", False))
    heuristic_risk = _clamp(heuristic.get("risk_score", 1.0), 0.0, 1.0)
    model_risk = _clamp(model_review.get("risk_score", heuristic_risk), 0.0, 1.0)
    heuristic_quality = _clamp(heuristic.get("answer_quality_score", 0.0), 0.0, 1.0)
    model_quality = _clamp(model_review.get("answer_quality_score", heuristic_quality), 0.0, 1.0)
    heuristic_conf = _clamp(heuristic.get("confidence_score", 0.0), 0.0, 1.0)
    model_conf = _clamp(model_review.get("confidence_score", heuristic_conf), 0.0, 1.0)
    model_blocks = bool(model_review.get("should_block_response", False))
    model_supports = bool(model_review.get("consistent", False)) and (
        model_conf >= 0.42 or model_quality >= 0.48
    )
    heuristic_supports = bool(heuristic.get("consistent", False)) and (
        heuristic_conf >= 0.38 or heuristic_quality >= 0.45
    )

    if base_verified:
        risk_score = min(heuristic_risk, model_risk)
        confidence_score = max(heuristic_conf, model_conf, 0.9)
        quality_score = max(heuristic_quality, model_quality, 0.9)
        consistent = True
        should_block = False
    elif hard_block or deterministic_correction:
        consistent = False
        should_block = True
        risk_score = max(heuristic_risk, model_risk, 0.93)
        confidence_score = min(
            heuristic_conf if heuristic_conf > 0.0 else 0.12,
            model_conf if model_conf > 0.0 else 0.12,
            0.18,
        )
        quality_score = min(heuristic_quality, model_quality, 0.16)
    else:
        consistent = bool(model_supports or (heuristic_supports and model_risk <= 0.70))
        should_block = False
        if model_supports:
            risk_score = min(max(model_risk, 0.08), heuristic_risk, 0.72)
            confidence_score = max(model_conf, heuristic_conf, 0.52)
            quality_score = max(model_quality, heuristic_quality, 0.52)
        else:
            risk_score = max(heuristic_risk, model_risk)
            confidence_score = min(
                heuristic_conf if heuristic_conf > 0.0 else 1.0 - heuristic_risk,
                model_conf if model_conf > 0.0 else 1.0 - model_risk,
            )
            quality_score = min(heuristic_quality, model_quality)
        if model_blocks and not missing_ground_truth:
            should_block = True
        elif not consistent and risk_score >= 0.94 and quality_score <= 0.18:
            should_block = True
        elif (
            not consistent
            and risk_score >= 0.86
            and heuristic_quality <= 0.24
            and model_quality <= 0.26
            and not missing_ground_truth
        ):
            should_block = True
        if should_block:
            confidence_score = min(confidence_score, 0.14)
            quality_score = min(quality_score, 0.18)

    suggested_correction = _as_text(
        heuristic.get("suggested_correction")
        or model_review.get("suggested_correction")
    )
    review_final_answer = _as_text(
        suggested_correction
        or model_review.get("review_final_answer")
        or heuristic.get("review_final_answer")
    )
    verdict = "safe"
    if should_block or risk_score >= 0.9:
        verdict = "unsafe"
    elif not consistent:
        verdict = "uncertain"

    return {
        "attempted": True,
        "provider": _as_text(model_review.get("provider")),
        "model": _as_text(model_review.get("model")),
        "method": "fast_model_plus_heuristic",
        "consistent": bool(consistent),
        "review_reasoning": " ".join(
            _dedupe_strings(
                [
                    heuristic.get("review_reasoning"),
                    model_review.get("review_reasoning"),
                ]
            )
        ).strip(),
        "review_final_answer": review_final_answer,
        "suggested_correction": suggested_correction or None,
        "flags": _dedupe_strings(
            [*(heuristic.get("flags") or []), *(model_review.get("flags") or [])]
        ),
        "issues": _dedupe_strings(
            [*(heuristic.get("issues") or []), *(model_review.get("issues") or [])]
        ),
        "timed_out": False,
        "override_allowed": bool(heuristic.get("override_allowed", False)),
        "risk_score": _clamp(risk_score, 0.0, 1.0),
        "confidence_score": _clamp(confidence_score, 0.0, 1.0),
        "answer_quality_score": _clamp(quality_score, 0.0, 1.0),
        "should_block_response": bool(should_block),
        "verdict": verdict,
    }


async def run_answer_quality_verifier(
    *,
    fabric: ProviderFabric,
    question: str,
    candidate_answer: str,
    candidate_reasoning: str,
    profile: ProblemProfile,
    base_verification: Dict[str, Any],
    research_verification: Dict[str, Any],
    enabled: bool = True,
) -> Dict[str, Any]:
    heuristic = _heuristic_review(
        question=question,
        candidate_answer=candidate_answer,
        candidate_reasoning=candidate_reasoning,
        base_verification=base_verification,
        research_verification=research_verification,
    )
    if not enabled:
        heuristic["reason"] = "disabled"
        return {key: value for key, value in heuristic.items() if not key.startswith("_")}

    if bool(heuristic.get("_base_verified", False)):
        heuristic["reason"] = "deterministic_verified"
        return {key: value for key, value in heuristic.items() if not key.startswith("_")}

    candidates = _review_candidates(fabric)
    if not candidates:
        heuristic["reason"] = "no_fast_verifier_provider_available"
        return {key: value for key, value in heuristic.items() if not key.startswith("_")}

    timeout_s = max(
        1.0,
        float(os.getenv("LC9_FAST_VERIFIER_TIMEOUT_S", "4.5") or 4.5),
    )
    retry_count = max(
        0,
        int(float(os.getenv("LC9_FAST_VERIFIER_RETRY_COUNT", "1") or 1)),
    )
    prompt = _build_review_prompt(
        question=question,
        candidate_answer=candidate_answer,
        candidate_reasoning=candidate_reasoning,
        base_verification=base_verification,
        research_verification=research_verification,
    )

    for provider, model in candidates:
        for attempt in range(retry_count + 1):
            try:
                with provider_runtime_budget(
                    timeout_overrides={provider: timeout_s},
                    request_policy={
                        "preferred_provider": provider,
                        "preferred_model": model,
                    },
                ), provider_model_priority_plan({provider: [model]}):
                    review = await fabric.generate(provider, prompt, profile, [])
            except Exception as exc:
                heuristic["reason"] = f"fast_verifier_error:{type(exc).__name__}"
                if attempt < retry_count:
                    await asyncio.sleep(min(0.25 * (attempt + 1), 0.5))
                    continue
                break

            raw_output = _as_text(
                review.final_answer
                or (review.raw or {}).get("raw_output_text")
                or review.reasoning
            )
            parsed = _parse_model_review(
                raw_output,
                candidate_answer=candidate_answer,
                reasoning=_as_text(review.reasoning),
            )
            if not parsed:
                heuristic["reason"] = "fast_verifier_invalid_payload"
                if attempt < retry_count:
                    await asyncio.sleep(min(0.18 * (attempt + 1), 0.4))
                    continue
                break
            merged = _merge_reviews(
                heuristic,
                {
                    **parsed,
                    "provider": provider,
                    "model": model,
                },
            )
            return merged

    return {key: value for key, value in heuristic.items() if not key.startswith("_")}
