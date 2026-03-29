from __future__ import annotations

import asyncio
import hashlib
import math
import os
import random
import re
from collections import OrderedDict
from difflib import SequenceMatcher
from typing import Any, Dict, List, Sequence

from app.arena.entropy import compute_entropy
from core.lalacore_x.answer_fusion import ProviderIndependentAnswerResolver, normalize_answer
from core.lalacore_x.arena import AdvancedArenaLayer, ArenaJudge
from core.lalacore_x.calibration import ConfidenceCalibrator
from core.lalacore_x.classifier import ProblemClassifier
from core.lalacore_x.crash_replay import CrashReplayRecorder
from core.lalacore_x.deterministic_guard import DeterministicDominanceGuard
from core.lalacore_x.meta_verification import MetaVerificationLayer
from core.lalacore_x.mini_distillation import LC9DistillationHub
from core.lalacore_x.mini_evolution import MiniEvolutionEngine
from core.lalacore_x.logging_debug import SolverDebugLogger
from core.lalacore_x.plausibility_checker import check_answer_plausibility
from core.lalacore_x.provider_orchestrator import ProviderOrchestrator
from core.lalacore_x.providers import ProviderFabric
from core.lalacore_x.recovery import retry_async
from core.lalacore_x.reasoning import DAGReasoner
from core.lalacore_x.replay import FailureReplayMemory
from core.lalacore_x.retrieval import ConceptVault
from core.lalacore_x.runtime_telemetry import RuntimeTelemetry
from core.lalacore_x.routing import ProviderStatsMemory, StatisticalRouter
from core.lalacore_x.schemas import ProviderAnswer, SolveArtifacts
from core.lalacore_x.solve_pipeline import SolvePipelinePolicy
from core.lalacore_x.statistical_sanity import StatisticalSanityValidator
from core.lalacore_x.telemetry import DEFAULT_TELEMETRY
from core.lalacore_x.token_budget import TokenBudgetGuardian
from core.math.contextual_math_solver import solve_contextual_math_question
from core.math.problem_parser import parse_structured_problem
from verification.verifier import verify_solution


class LalaCoreXEngine:
    """
    Research-grade orchestration core with backward-compatible output shape.
    """

    def __init__(self):
        self.classifier = ProblemClassifier()
        self.vault = ConceptVault()
        self.providers = ProviderFabric()

        self.stats = ProviderStatsMemory()
        self.router = StatisticalRouter(self.stats)

        self.calibrator = ConfidenceCalibrator()
        self.judge = ArenaJudge(calibrator=self.calibrator)
        self.advanced_arena = AdvancedArenaLayer()

        self.reasoner = DAGReasoner()
        self.replay = FailureReplayMemory()
        self.mini_evolution = MiniEvolutionEngine()
        self.distillation = LC9DistillationHub()
        self.meta_verification = MetaVerificationLayer()
        self.telemetry = DEFAULT_TELEMETRY
        self.runtime_telemetry = RuntimeTelemetry()
        self.crash_replay = CrashReplayRecorder()
        self.sanity_validator = StatisticalSanityValidator()
        self.dominance_guard = DeterministicDominanceGuard()
        self.token_guardian = TokenBudgetGuardian()
        self.debug_logger = SolverDebugLogger()
        self.provider_orchestrator = ProviderOrchestrator(min_provider_count=2, strong_gap=0.08)
        self.solve_policy = SolvePipelinePolicy()
        self.answer_resolver = ProviderIndependentAnswerResolver()
        self._debate_cache: OrderedDict[str, Dict] = OrderedDict()
        self._debate_cache_limit = 128
        self.shadow_arena_enabled = str(os.getenv("LC9_SHADOW_ARENA_MODE", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.shadow_exploration_rate = max(0.0, min(1.0, float(os.getenv("LC9_SHADOW_EXPLORATION_RATE", "0.07") or 0.07)))
        self.shadow_entropy_band_min = max(0.0, min(1.0, float(os.getenv("LC9_SHADOW_ENTROPY_MIN", "0.25") or 0.25)))
        self.shadow_entropy_band_max = max(0.0, min(1.0, float(os.getenv("LC9_SHADOW_ENTROPY_MAX", "0.65") or 0.65)))
        self.shadow_confidence_floor = max(0.0, min(1.0, float(os.getenv("LC9_SHADOW_CONFIDENCE_FLOOR", "0.985") or 0.985)))
        self.shadow_max_extra_providers = max(0, int(os.getenv("LC9_SHADOW_MAX_EXTRA_PROVIDERS", "2") or 2))
        self.shadow_probe_timeout_s = max(0.5, float(os.getenv("LC9_SHADOW_PROBE_TIMEOUT_S", "2.5") or 2.5))

    async def solve(self, question: str) -> Dict:
        authority_question = self._authority_question(question)
        profile = self.classifier.classify(authority_question)
        symbolic_heavy = self._is_symbolic_heavy(authority_question, profile)
        retrieved = self.vault.retrieve(authority_question, subject=profile.subject, top_k=5)
        concept_clusters = self._concept_clusters(retrieved)
        reinforced_clusters = self.vault.expand_concept_clusters(concept_clusters, depth=2)
        question_tokens = int(profile.features.get("token_count", 0))
        entropy_hint = min(1.0, 0.18 + 0.48 * float(profile.trap_probability) + (0.24 if profile.multi_concept else 0.0))
        crash_snapshot = {
            "question_hash": self._sha1(question),
            "active_providers": [],
            "provider_health": {},
            "responses": [],
            "entropy": 0.0,
            "matches": [],
            "bt_thetas": {},
            "mini_eligible": False,
        }

        try:
            await self.providers.ensure_startup_warmup()
        except Exception as exc:
            self.runtime_telemetry.log_exception(
                exception_type=type(exc).__name__,
                module="engine",
                function="solve.provider_warmup",
                input_size=len(question or ""),
                entropy=None,
                active_providers=[],
                mini_eligible=None,
                token_usage=None,
                extra={"warmup": "failed_non_blocking"},
            )

        provider_health = self.providers.availability_report()
        crash_snapshot["provider_health"] = provider_health

        try:
            available = await retry_async(
                self._safe_available_providers,
                component="engine",
                operation="available_providers",
                telemetry=self.runtime_telemetry,
                max_attempts=3,
                base_delay_s=0.2,
            )
        except Exception as exc:
            self.runtime_telemetry.log_exception(
                exception_type=type(exc).__name__,
                module="engine",
                function="solve.available_providers",
                input_size=len(question or ""),
                entropy=None,
                active_providers=[],
                mini_eligible=False,
                token_usage=None,
                extra={"fallback": "mini_only"},
            )
            available = ["mini"]
        if not available:
            available = ["mini"]
        crash_snapshot["active_providers"] = list(available)
        if available == ["mini"]:
            self.runtime_telemetry.log_incident(
                "single_provider_mode",
                {
                    "active": list(available),
                    "provider_health": provider_health,
                },
            )
        requested_arena_size = 3 if profile.difficulty in {"easy", "medium"} else 4

        decision = self.router.choose(
            available_providers=available,
            profile=profile,
            arena_size=requested_arena_size,
            concept_clusters=concept_clusters,
            question_tokens=question_tokens,
            entropy_hint=entropy_hint,
        )

        selected = self.provider_orchestrator.select_for_run(
            available_providers=available,
            ranked=decision.ranked,
            initial_selected=decision.selected,
            require_non_mini=symbolic_heavy and any(provider != "mini" for provider in available),
            target_provider_count=(3 if symbolic_heavy else None),
        )
        self.debug_logger.log_routing_decision(
            {
                "question_hash": crash_snapshot["question_hash"],
                "available": list(available),
                "provider_health": provider_health,
                "ranked": [{"provider": p, "score": float(s)} for p, s in decision.ranked],
                "router_selected": list(decision.selected),
                "orchestrator_selected": list(selected),
                "rationale": decision.rationale,
            }
        )
        candidates = await self.providers.generate_many(selected, question, profile, retrieved)
        crash_snapshot["responses"] = [self._snapshot_candidate(c) for c in candidates]

        if symbolic_heavy:
            symbolic_guard = await self.providers.generate("symbolic_guard", authority_question, profile, retrieved)
            if symbolic_guard.final_answer.strip():
                replaced = False
                updated_candidates: List[ProviderAnswer] = []
                for candidate in candidates:
                    if candidate.provider == "symbolic_guard":
                        updated_candidates.append(symbolic_guard)
                        replaced = True
                    else:
                        updated_candidates.append(candidate)
                if not replaced:
                    updated_candidates.append(symbolic_guard)
                candidates = updated_candidates
                crash_snapshot["responses"] = [self._snapshot_candidate(c) for c in candidates]

        mini_shadow = False
        if "mini" in available and not any(c.provider == "mini" for c in candidates):
            # Shadow inference only, does not increase paid API usage.
            mini_candidate = await self.providers.generate("mini", question, profile, retrieved)
            candidates.append(mini_candidate)
            mini_shadow = True
            crash_snapshot["responses"] = [self._snapshot_candidate(c) for c in candidates]

        if self._all_empty(candidates):
            additional = [provider for provider in available if provider not in selected and provider != "mini"]
            if additional:
                extra = await self.providers.generate_many(additional[:1], question, profile, retrieved)
                candidates.extend(extra)

        # Quality floor: enforce minimum provider pool when possible.
        if len(candidates) < self.provider_orchestrator.min_provider_count:
            existing = {c.provider for c in candidates}
            extra_pool = [provider for provider, _ in decision.ranked if provider not in existing]
            needed = max(0, self.provider_orchestrator.min_provider_count - len(candidates))
            if needed > 0 and extra_pool:
                extra = await self.providers.generate_many(extra_pool[:needed], question, profile, retrieved)
                candidates.extend(extra)

        if self._all_empty(candidates):
            rescue_candidates = await self._rescue_from_empty_pool(
                question=question,
                profile=profile,
                retrieved=retrieved,
                candidates=candidates,
            )
            if rescue_candidates:
                for rescue in rescue_candidates:
                    candidates = self._replace_candidate(candidates, rescue.provider, rescue)
                crash_snapshot["responses"] = [self._snapshot_candidate(c) for c in candidates]

        if not candidates:
            degraded = self._degraded_result(
                question=question,
                profile=profile,
                retrieved=retrieved,
                candidates=[],
                verification_by_provider={},
                reason="no_provider_candidates",
            )
            self.runtime_telemetry.log_incident(
                "degraded_mode",
                {"reason": "no_provider_candidates", "active_providers": available},
            )
            return degraded
        if self._all_empty(candidates):
            degraded = self._degraded_result(
                question=question,
                profile=profile,
                retrieved=retrieved,
                candidates=candidates,
                verification_by_provider={},
                reason="all_provider_answers_empty",
            )
            self.runtime_telemetry.log_incident(
                "degraded_mode",
                {"reason": "all_provider_answers_empty", "active_providers": [c.provider for c in candidates]},
            )
            return degraded

        for candidate in candidates:
            if candidate.provider == "mini":
                candidate.confidence = self.mini_evolution.scale_shadow_confidence(profile.subject, candidate.confidence)

        reasoning_graph = self.reasoner.build_graph(candidates)
        provider_graphs = reasoning_graph.get("provider_graphs", {})
        coherence_by_provider = reasoning_graph.get("coherence", {})
        structure_by_provider = reasoning_graph.get("structure_metrics", {})
        process_reward_by_provider = reasoning_graph.get("process_reward", {})

        claims = self.reasoner.extract_claims(reasoning_graph, limit=6)
        claim_support = self.vault.check_claims(claims[:3], top_k=2)

        substitution_hooks = self.reasoner.numeric_substitution_hooks(authority_question)

        verification_by_provider: Dict[str, Dict] = {}
        plausibility_by_provider: Dict[str, Dict] = {}
        for candidate in candidates:
            report = verify_solution(
                question=authority_question,
                predicted_answer=candidate.final_answer,
                difficulty=profile.difficulty,
                substitution_hooks=substitution_hooks,
            )
            if candidate.provider == "symbolic_guard" and str(candidate.final_answer or "").strip():
                report.setdefault("stage_results", {})
                if bool(report.get("verified")):
                    report["stage_results"]["symbolic_guard"] = "deterministic_pass"
                    report["reason"] = report.get("reason") or "symbolic_guard_deterministic"
                    report["failure_reason"] = ""
                    report["confidence_score"] = max(0.99, float(report.get("confidence_score", 0.0)))
                    report["risk_score"] = min(0.02, float(report.get("risk_score", 1.0)))
                else:
                    report["stage_results"]["symbolic_guard"] = "deterministic_mismatch"
            report["trap_probability"] = profile.trap_probability

            plausibility = check_answer_plausibility(
                question_text=authority_question,
                final_answer=candidate.final_answer,
                metadata={
                    "numeric_expected": bool(profile.numeric),
                    "observed_type": "numeric" if re.search(r"\d", str(candidate.final_answer or "")) else "text",
                },
            )
            plausibility_by_provider[candidate.provider] = plausibility
            report["plausibility"] = plausibility
            report["plausibility_score"] = float(plausibility.get("score", 0.0))
            report["plausibility_failed"] = not bool(plausibility.get("plausible", False))

            if not bool(plausibility.get("plausible", False)):
                report["verified"] = False
                report["risk_score"] = max(0.95, float(report.get("risk_score", 1.0)))
                report["escalate"] = True
                reason = str(report.get("failure_reason", "")).strip()
                report["failure_reason"] = (
                    f"{reason},plausibility:{'|'.join(plausibility.get('issues', []))}".strip(",")
                    if reason
                    else f"plausibility:{'|'.join(plausibility.get('issues', []))}"
                )
                # Confidence decay for implausible outputs prevents arena dominance by bad echoes.
                candidate.confidence = max(0.01, float(candidate.confidence) * max(0.2, float(plausibility.get("score", 0.0))))

            self.debug_logger.log_plausibility(
                provider=candidate.provider,
                question=authority_question,
                answer=candidate.final_answer,
                report=plausibility,
            )
            verification_by_provider[candidate.provider] = report

        provider_reliability = {
            candidate.provider: max(
                0.0,
                min(
                    1.0,
                    self.stats.provider_score(candidate.provider, profile, concept_clusters=concept_clusters) + 0.5,
                ),
            )
            for candidate in candidates
        }

        retrieval_strength = self._retrieval_strength(retrieved)

        judge_results = self.judge.evaluate(
            candidates=candidates,
            verification_by_provider=verification_by_provider,
            provider_reliability=provider_reliability,
            retrieval_strength=retrieval_strength,
            coherence_by_provider=coherence_by_provider,
            structure_by_provider=structure_by_provider,
            process_reward_by_provider=process_reward_by_provider,
        )
        judge_by_provider = {result.provider: result for result in judge_results}

        arena_inputs = []
        for candidate in candidates:
            arena_inputs.append(
                {
                    "provider": candidate.provider,
                    "final_answer": candidate.final_answer,
                    "critic_score": judge_by_provider.get(candidate.provider).critic_score if candidate.provider in judge_by_provider else 0.5,
                    "deterministic_pass": bool(verification_by_provider.get(candidate.provider, {}).get("verified"))
                    and bool(plausibility_by_provider.get(candidate.provider, {}).get("plausible", False)),
                    "confidence": float(candidate.confidence),
                    "skill": float(provider_reliability.get(candidate.provider, 0.5)),
                    "reasoning": candidate.reasoning,
                    "graph": provider_graphs.get(candidate.provider),
                    "structural_coherence": float(coherence_by_provider.get(candidate.provider, 0.5)),
                    "process_reward": float(process_reward_by_provider.get(candidate.provider, 0.5)),
                }
            )

        entropy = compute_entropy(arena_inputs)
        crash_snapshot["entropy"] = float(entropy)
        for provider in verification_by_provider:
            verification_by_provider[provider]["entropy"] = entropy
        for candidate in candidates:
            usage = self._candidate_token_usage(candidate)
            total_tokens = int(usage.get("total_tokens", 0.0))
            ver = verification_by_provider.get(candidate.provider, {})
            self.debug_logger.log_provider_output(
                provider=candidate.provider,
                question=authority_question,
                raw_output=str(candidate.raw.get("raw_output_text", "") if isinstance(candidate.raw, dict) else candidate.reasoning),
                extracted_answer=str(candidate.final_answer),
                tokens_used=total_tokens,
                extraction_matched=bool((candidate.raw or {}).get("extraction", {}).get("matched", False)),
                extraction_pattern=str((candidate.raw or {}).get("extraction", {}).get("pattern", "")),
                verification=bool(ver.get("verified")) if ver else None,
                risk=float(ver.get("risk_score")) if isinstance(ver, dict) and ver.get("risk_score") is not None else None,
                entropy=float(entropy),
            )

        iterations_override = None
        iteration_scale = float(self.token_guardian.arena_iteration_scale())
        reduced_iterations = None
        if iteration_scale < 0.999 and hasattr(self.advanced_arena.bt_engine, "schedule_iterations"):
            base_iterations = self.advanced_arena.bt_engine.schedule_iterations(
                entropy=max(0.0, min(1.0, float(entropy))),
                provider_count=max(1, len(arena_inputs)),
            )
            iterations_override = max(1, int(round(base_iterations * iteration_scale)))
        if hasattr(self.advanced_arena.bt_engine, "schedule_iterations"):
            base_iterations = self.advanced_arena.bt_engine.schedule_iterations(
                entropy=max(0.0, min(1.0, float(entropy))),
                provider_count=max(1, len(arena_inputs)),
            )
            reduced_iterations = max(1, int(round(base_iterations * 0.5)))
        elif iterations_override is not None:
            reduced_iterations = max(1, int(round(iterations_override * 0.5)))

        try:
            arena_outcome = self.advanced_arena.run(
                arena_inputs,
                entropy=entropy,
                iterations_override=iterations_override,
            )
        except Exception as exc:
            self.runtime_telemetry.log_exception(
                exception_type=type(exc).__name__,
                module="engine",
                function="solve.arena_run",
                input_size=len(question or ""),
                entropy=entropy,
                active_providers=[c.provider for c in candidates],
                mini_eligible=self.mini_evolution.can_promote(profile.subject, profile.difficulty, concept_clusters=concept_clusters),
                token_usage=self._total_token_usage(candidates),
                extra={"reason": "arena_run_exception"},
            )
            self.crash_replay.record(
                exception_type=type(exc).__name__,
                message=str(exc),
                snapshot={
                    **crash_snapshot,
                    "responses": [self._snapshot_candidate(c) for c in candidates],
                },
            )
            arena_outcome = self._arena_fallback_outcome(arena_inputs, verification_by_provider)

        arena_outcome = self.sanity_validator.auto_correct(
            arena_outcome,
            recompute_fn=lambda: self.advanced_arena.run(
                arena_inputs,
                entropy=min(0.20, max(0.0, float(entropy))),
                iterations_override=reduced_iterations,
            ),
        )
        if arena_outcome.get("auto_corrected"):
            self.runtime_telemetry.log_incident(
                "statistical_auto_correction",
                {
                    "issues": arena_outcome.get("sanity_issues", []),
                    "provider_count": len(arena_inputs),
                },
            )

        crash_snapshot["matches"] = list(arena_outcome.get("matches", []))[:64]
        crash_snapshot["bt_thetas"] = dict(arena_outcome.get("thetas", {}))
        crash_snapshot["mini_eligible"] = self.mini_evolution.can_promote(
            profile.subject,
            profile.difficulty,
            concept_clusters=concept_clusters,
        )

        if not arena_outcome.get("posteriors"):
            degraded = self._degraded_result(
                question=question,
                profile=profile,
                retrieved=retrieved,
                candidates=candidates,
                verification_by_provider=verification_by_provider,
                reason="arena_posteriors_empty",
                arena_outcome=arena_outcome,
            )
            self.runtime_telemetry.log_incident("degraded_mode", {"reason": "arena_posteriors_empty"})
            return degraded

        selected_provider = self._select_winner_with_constraints(
            arena_outcome=arena_outcome,
            verification_by_provider=verification_by_provider,
            plausibility_by_provider=plausibility_by_provider,
            profile=profile,
            concept_clusters=concept_clusters,
        )
        selected_provider, dominance_event = self.dominance_guard.enforce(
            winner=selected_provider,
            posteriors=arena_outcome.get("posteriors", {}),
            verification_by_provider=verification_by_provider,
            structure_by_provider=structure_by_provider,
        )
        if dominance_event.get("enforced"):
            self.runtime_telemetry.log_incident("deterministic_dominance_enforced", dominance_event)
        if not selected_provider:
            selected_provider = self._fallback_provider_selection(
                candidates=candidates,
                verification_by_provider=verification_by_provider,
                arena_outcome=arena_outcome,
            )
        degraded_reason = None

        selected_candidate = self._candidate_by_provider(candidates, selected_provider)
        selected_verification = verification_by_provider.get(selected_provider, {"verified": False, "risk_score": 1.0})
        selected_plausibility = plausibility_by_provider.get(
            selected_provider,
            {"plausible": False, "issues": ["missing_plausibility"], "score": 0.0},
        )
        selected_judge = judge_by_provider.get(selected_provider)
        debate_outcome = {"triggered": False, "accepted": False}
        if not selected_candidate.final_answer.strip():
            selected_provider = self._fallback_provider_selection(
                candidates=candidates,
                verification_by_provider=verification_by_provider,
                arena_outcome=arena_outcome,
            )
            selected_candidate = self._candidate_by_provider(candidates, selected_provider)
            selected_verification = verification_by_provider.get(selected_provider, {"verified": False, "risk_score": 1.0})
            selected_plausibility = plausibility_by_provider.get(
                selected_provider,
                {"plausible": False, "issues": ["missing_plausibility"], "score": 0.0},
            )
            selected_judge = judge_by_provider.get(selected_provider)
            degraded_reason = "winner_answer_empty"

        if not bool(selected_plausibility.get("plausible", False)):
            ranked_posteriors = sorted(
                arena_outcome.get("posteriors", {}).items(),
                key=lambda row: float(row[1]),
                reverse=True,
            )
            fallback_provider = None
            for provider, _ in ranked_posteriors:
                if provider == selected_provider:
                    continue
                if bool(plausibility_by_provider.get(provider, {}).get("plausible", False)):
                    fallback_provider = provider
                    break
            if fallback_provider:
                selected_provider = str(fallback_provider)
                selected_candidate = self._candidate_by_provider(candidates, selected_provider)
                selected_verification = verification_by_provider.get(selected_provider, {"verified": False, "risk_score": 1.0})
                selected_plausibility = plausibility_by_provider.get(
                    selected_provider,
                    {"plausible": False, "issues": ["missing_plausibility"], "score": 0.0},
                )
                selected_judge = judge_by_provider.get(selected_provider)
            else:
                degraded_reason = degraded_reason or "winner_plausibility_failed"

        if self._should_run_debate(
            entropy=entropy,
            candidate_count=len(candidates),
            selected_verification=selected_verification,
        ):
            debate_outcome = await self._run_self_debate_lite(
                question=question,
                profile=profile,
                retrieved=retrieved,
                arena_outcome=arena_outcome,
                candidates=candidates,
                verification_by_provider=verification_by_provider,
                provider_reliability=provider_reliability,
                retrieval_strength=retrieval_strength,
                coherence_by_provider=coherence_by_provider,
                structure_by_provider=structure_by_provider,
                process_reward_by_provider=process_reward_by_provider,
                current_provider=selected_provider,
                current_judge=selected_judge,
                current_verification=selected_verification,
            )
            if debate_outcome.get("accepted"):
                updated_provider = str(debate_outcome.get("provider", selected_provider))
                updated_candidate = debate_outcome.get("candidate")
                updated_verification = debate_outcome.get("verification")
                updated_judge = debate_outcome.get("judge")
                updated_structure = debate_outcome.get("structure", {})
                updated_coherence = float(updated_structure.get("structural_coherence_score", 0.5))
                updated_process = float(updated_structure.get("process_reward_score", 0.5))

                if updated_candidate is not None:
                    candidates = self._replace_candidate(candidates, updated_provider, updated_candidate)
                    selected_candidate = updated_candidate
                selected_provider = updated_provider
                if isinstance(updated_verification, dict):
                    verification_by_provider[updated_provider] = updated_verification
                    selected_verification = updated_verification
                if updated_candidate is not None:
                    selected_plausibility = check_answer_plausibility(
                        question_text=authority_question,
                        final_answer=updated_candidate.final_answer,
                        metadata={"numeric_expected": bool(profile.numeric)},
                    )
                    plausibility_by_provider[updated_provider] = selected_plausibility
                if updated_judge is not None:
                    selected_judge = updated_judge
                    judge_by_provider[updated_provider] = updated_judge
                    judge_results = [row for row in judge_results if row.provider != updated_provider]
                    judge_results.append(updated_judge)
                    judge_results.sort(key=lambda row: row.score, reverse=True)
                if updated_structure:
                    structure_by_provider[updated_provider] = updated_structure
                    coherence_by_provider[updated_provider] = updated_coherence
                    process_reward_by_provider[updated_provider] = updated_process

        answer_resolution = self.answer_resolver.resolve(
            candidates=candidates,
            current_provider=selected_provider,
            posteriors=arena_outcome.get("posteriors", {}),
            verification_by_provider=verification_by_provider,
            plausibility_by_provider=plausibility_by_provider,
            judge_by_provider=judge_by_provider,
        )
        if bool(answer_resolution.get("switched", False)):
            resolved_provider = str(answer_resolution.get("provider", "")).strip()
            if resolved_provider:
                selected_provider = resolved_provider
                selected_candidate = self._candidate_by_provider(candidates, selected_provider)
                selected_verification = verification_by_provider.get(selected_provider, {"verified": False, "risk_score": 1.0})
                selected_plausibility = plausibility_by_provider.get(
                    selected_provider,
                    {"plausible": False, "issues": ["missing_plausibility"], "score": 0.0},
                )
                selected_judge = judge_by_provider.get(selected_provider)

        disagreement = self._disagreement(candidates, question_text=authority_question)
        claim_support_score = self._claim_support_score(claim_support)

        winner_margin = float(arena_outcome.get("winner_margin", 0.0))
        uncertainty_adjusted_margin = float(arena_outcome.get("pairwise", {}).get("uncertainty_adjusted_margin", 0.0))
        disagreement_cases = arena_outcome.get("pairwise", {}).get("disagreement_cases", [])

        deterministic_dominance = any(bool(v.get("verified")) for v in verification_by_provider.values())
        verification_failure_reason = str(selected_verification.get("failure_reason", "")).lower()
        verification_supported = "missing_ground_truth" not in verification_failure_reason

        policy_gate = self.solve_policy.evaluate(
            verified=bool(selected_verification.get("verified", False)),
            risk=float(selected_verification.get("risk_score", 1.0)),
            plausibility=selected_plausibility,
            disagreement=float(disagreement),
            arena_winner_found=bool(selected_provider),
            entropy=float(entropy),
            verification_supported=verification_supported,
        )

        escalate = bool(selected_verification.get("escalate"))
        if (
            not verification_supported
            and bool(selected_plausibility.get("plausible", False))
            and bool(str(selected_candidate.final_answer or "").strip())
        ):
            escalate = False
        if (selected_judge.risk if selected_judge else 1.0) > 0.55:
            escalate = True
        if claim_support_score < 0.2:
            escalate = True
        if (
            deterministic_dominance
            and verification_supported
            and not bool(selected_verification.get("verified"))
        ):
            escalate = True
        if winner_margin < 0.04 or uncertainty_adjusted_margin < 0.03:
            escalate = True
        if self.provider_orchestrator.should_force_escalation(
            entropy=float(entropy),
            disagreement=float(disagreement),
            plausibility_failed=not bool(selected_plausibility.get("plausible", False)),
            verification_failed=not bool(selected_verification.get("verified", False)),
            provider_count=len(candidates),
        ):
            escalate = True
        if bool(policy_gate.get("force_escalate", False)):
            escalate = True

        shadow_policy = self._shadow_diversity_policy(
            question=question,
            profile=profile,
            concept_clusters=concept_clusters,
            entropy=float(entropy),
            selected_verification=selected_verification,
            deterministic_dominance=deterministic_dominance,
        )
        shadow_bundle = await self._collect_shadow_candidates(
            question=question,
            profile=profile,
            retrieved=retrieved,
            base_candidates=candidates,
            policy=shadow_policy,
        )

        artifacts = SolveArtifacts(
            profile=profile,
            retrieved=retrieved,
            candidates=candidates,
            judge_results=judge_results,
            reasoning_graph=reasoning_graph,
            mcts_trace=[],
            selected_provider=selected_provider,
        )

        result = {
            "question": question,
            "reasoning": selected_candidate.reasoning,
            "final_answer": selected_candidate.final_answer,
            "verification": selected_verification,
            "plausibility": selected_plausibility,
            "routing_decision": decision.rationale,
            "escalate": escalate,
            "winner_provider": selected_provider,
            "answer_resolution": {
                "provider_independent": True,
                "switched": bool(answer_resolution.get("switched", False)),
                "reason": str(answer_resolution.get("reason", "")),
                "resolved_provider": str(answer_resolution.get("provider", selected_provider)),
            },
            "profile": {
                "subject": profile.subject,
                "difficulty": profile.difficulty,
                "numeric": profile.numeric,
                "multiConcept": profile.multi_concept,
                "trapProbability": profile.trap_probability,
            },
            "arena": {
                "ranked_providers": [{"provider": provider, "score": score} for provider, score in decision.ranked],
                "judge_results": [
                    {
                        "provider": row.provider,
                        "score": row.score,
                        "risk": row.risk,
                        "rule_score": row.rule_score,
                        "critic_score": row.critic_score,
                        "verified": row.verified,
                        "notes": row.notes,
                    }
                    for row in judge_results
                ],
                "bt_thetas": arena_outcome.get("thetas", {}),
                "posteriors": arena_outcome.get("posteriors", {}),
                "winner_margin": winner_margin,
                "arena_confidence": float(arena_outcome.get("arena_confidence", 0.0)),
                "pairwise_confidence_margin": float(arena_outcome.get("pairwise", {}).get("confidence_margin", 0.0)),
                "uncertainty_adjusted_margin": uncertainty_adjusted_margin,
                "disagreement": disagreement,
                "disagreement_case_count": len(disagreement_cases),
                "deterministic_dominance": deterministic_dominance,
                "entropy": entropy,
                "plausibility_by_provider": {
                    provider: {
                        "plausible": bool(report.get("plausible", False)),
                        "score": float(report.get("score", 0.0)),
                        "issues": list(report.get("issues", [])),
                    }
                    for provider, report in plausibility_by_provider.items()
                },
                "winner_structure": structure_by_provider.get(selected_provider, {}),
                "winner_process_reward": float(process_reward_by_provider.get(selected_provider, 0.0)),
                "debate_lite": {
                    "triggered": bool(debate_outcome.get("triggered")),
                    "accepted": bool(debate_outcome.get("accepted")),
                    "provider": debate_outcome.get("provider"),
                },
                "answer_pool": list(answer_resolution.get("groups", [])),
                "dominance_enforced": bool(dominance_event.get("enforced")),
                "auto_corrected": bool(arena_outcome.get("auto_corrected")),
                "sanity_issues": list(arena_outcome.get("sanity_issues", [])),
            },
            "retrieval": {
                "top_blocks": [
                    {
                        "id": block.block_id,
                        "title": block.title,
                        "score": block.score,
                        "source": block.source,
                    }
                    for block in retrieved
                ],
                "claim_support_score": claim_support_score,
            },
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "mini_shadow": mini_shadow,
                "shadow_diversity": {
                    "enabled": bool(shadow_bundle.get("enabled", False)),
                    "attempted_extra": list(shadow_bundle.get("attempted", [])),
                    "collected_extra": int(len(shadow_bundle.get("candidates", []))),
                    "policy": dict(shadow_policy),
                },
                "degraded_mode": degraded_reason is not None,
                "degraded_reason": degraded_reason,
                "provider_availability": provider_health,
            },
            "quality_gate": policy_gate,
            "final_status": policy_gate.get("final_status", "Failed"),
        }
        if degraded_reason is not None:
            self.runtime_telemetry.log_incident(
                "degraded_mode",
                {"reason": degraded_reason, "winner_provider": selected_provider},
            )
        self.debug_logger.log_final_status(
            {
                "question_hash": crash_snapshot["question_hash"],
                "winner_provider": selected_provider,
                "verified": bool(selected_verification.get("verified", False)),
                "risk": float(selected_verification.get("risk_score", 1.0)),
                "plausible": bool(selected_plausibility.get("plausible", False)),
                "plausibility_score": float(selected_plausibility.get("score", 0.0)),
                "disagreement": float(disagreement),
                "entropy": float(entropy),
                "escalate": bool(escalate),
                "final_status": str(result.get("final_status", "Failed")),
                "quality_reasons": list(policy_gate.get("reasons", [])),
            }
        )

        try:
            self._post_solve_updates(
                result=result,
                artifacts=artifacts,
                selected_judge=selected_judge,
                verification=selected_verification,
                verification_by_provider=verification_by_provider,
                disagreement=disagreement,
                retrieval_strength=retrieval_strength,
                entropy=entropy,
                winner_margin=winner_margin,
                uncertainty=arena_outcome.get("pairwise", {}).get("uncertainties", {}),
                disagreement_cases=disagreement_cases,
                deterministic_dominance=deterministic_dominance,
                concept_clusters=concept_clusters,
                reinforced_clusters=reinforced_clusters,
                structure_by_provider=structure_by_provider,
                process_reward_by_provider=process_reward_by_provider,
                debate_outcome=debate_outcome,
                question_tokens=question_tokens,
                shadow_bundle=shadow_bundle,
            )
        except Exception as exc:
            self.runtime_telemetry.log_exception(
                exception_type=type(exc).__name__,
                module="engine",
                function="_post_solve_updates",
                input_size=len(question or ""),
                entropy=entropy,
                active_providers=[c.provider for c in candidates],
                mini_eligible=self.mini_evolution.can_promote(profile.subject, profile.difficulty, concept_clusters=concept_clusters),
                token_usage=self._total_token_usage(candidates),
                extra={"recoverable": True},
            )

        return result

    def _shadow_diversity_policy(
        self,
        *,
        question: str,
        profile,
        concept_clusters: Sequence[str],
        entropy: float,
        selected_verification: Dict,
        deterministic_dominance: bool,
    ) -> Dict:
        difficulty = str(getattr(profile, "difficulty", "unknown")).lower().strip()
        confidence = 1.0 - float(selected_verification.get("risk_score", 1.0))
        entropy_band = float(self.shadow_entropy_band_min) <= float(entropy) <= float(self.shadow_entropy_band_max)
        rare_cluster = any(self._is_rare_cluster_label(cluster) for cluster in concept_clusters)
        medium_plus = difficulty in {"medium", "hard"}
        multi_concept = bool(getattr(profile, "multi_concept", False))
        low_deterministic_conf = confidence < float(self.shadow_confidence_floor)

        seed = int(self._sha1(f"shadow|{question}")[:8], 16)
        exploration_sample = random.Random(seed).random() < float(self.shadow_exploration_rate)
        trigger_flags = {
            "difficulty_medium_plus": bool(medium_plus),
            "multi_concept": bool(multi_concept),
            "rare_cluster": bool(rare_cluster),
            "entropy_band": bool(entropy_band),
            "deterministic_confidence_low": bool(low_deterministic_conf),
            "exploration_sample": bool(exploration_sample),
        }
        trigger_count = sum(1 for value in trigger_flags.values() if value)
        collect_extra = bool(
            self.shadow_arena_enabled
            and self.shadow_max_extra_providers > 0
            and (deterministic_dominance or bool(selected_verification.get("verified", False)))
            and (trigger_count > 0)
        )
        return {
            **trigger_flags,
            "trigger_count": int(trigger_count),
            "collect_extra": bool(collect_extra),
            "enabled": bool(self.shadow_arena_enabled),
        }

    async def _collect_shadow_candidates(
        self,
        *,
        question: str,
        profile,
        retrieved,
        base_candidates: Sequence[ProviderAnswer],
        policy: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not bool(policy.get("collect_extra", False)):
            return {"enabled": bool(self.shadow_arena_enabled), "attempted": [], "candidates": [], "policy": dict(policy)}

        available = [provider for provider in self.providers.available_providers() if provider not in {"mini", "symbolic_guard"}]
        existing = {candidate.provider for candidate in base_candidates}
        missing = [provider for provider in available if provider not in existing]
        target = min(len(missing), int(self.shadow_max_extra_providers))
        if target <= 0:
            return {"enabled": bool(self.shadow_arena_enabled), "attempted": [], "candidates": [], "policy": dict(policy)}

        attempted = missing[:target]
        try:
            extra = await asyncio.wait_for(
                self.providers.generate_many(attempted, question, profile, retrieved),
                timeout=float(self.shadow_probe_timeout_s),
            )
        except Exception as exc:
            self.runtime_telemetry.log_exception(
                exception_type=type(exc).__name__,
                module="engine",
                function="_collect_shadow_candidates",
                input_size=len(question or ""),
                entropy=None,
                active_providers=list(attempted),
                mini_eligible=None,
                token_usage=None,
                extra={"shadow_probe": True},
            )
            extra = []
        return {
            "enabled": bool(self.shadow_arena_enabled),
            "attempted": list(attempted),
            "candidates": [candidate for candidate in extra if isinstance(candidate, ProviderAnswer)],
            "policy": dict(policy),
        }

    def _is_rare_cluster_label(self, cluster: str) -> bool:
        key = str(cluster or "").lower().strip()
        if not key:
            return False
        stats = self.mini_evolution.state.get("cluster_stats", {})
        row = stats.get(key, {})
        total = int(row.get("total", 0))
        # Rare-by-data fallback for unseen/low-frequency concepts.
        return total <= 5

    def _select_winner_with_constraints(
        self,
        arena_outcome: Dict,
        verification_by_provider: Dict[str, Dict],
        plausibility_by_provider: Dict[str, Dict],
        profile,
        concept_clusters: Sequence[str] | None = None,
    ) -> str:
        posteriors = dict(arena_outcome.get("posteriors", {}))
        if not posteriors:
            return ""

        def _plausible(provider: str) -> bool:
            row = plausibility_by_provider.get(provider, {})
            return bool(row.get("plausible", False))

        # Deterministic verification supremacy.
        verified = [provider for provider, report in verification_by_provider.items() if report.get("verified") and _plausible(provider)]
        if verified:
            verified_post = {provider: posteriors.get(provider, 0.0) for provider in verified}
            if verified_post:
                winner = max(verified_post, key=verified_post.get)
            else:
                winner = max(posteriors, key=posteriors.get)
        else:
            plausible_post = {provider: score for provider, score in posteriors.items() if _plausible(provider)}
            winner = max(plausible_post, key=plausible_post.get) if plausible_post else max(posteriors, key=posteriors.get)

        # Mini promotion gate: mini remains shadow unless gated metrics are healthy.
        mini_unhealthy = winner == "mini" and (
            (not bool(verification_by_provider.get("mini", {}).get("verified")))
            or (not _plausible("mini"))
            or float(plausibility_by_provider.get("mini", {}).get("score", 0.0)) < 0.60
        )
        if mini_unhealthy or (
            winner == "mini"
            and not self.mini_evolution.can_promote(
                profile.subject,
                profile.difficulty,
                concept_clusters=concept_clusters or [],
            )
        ):
            alternatives = [(provider, score) for provider, score in posteriors.items() if provider != "mini" and _plausible(provider)]
            if not alternatives:
                alternatives = [(provider, score) for provider, score in posteriors.items() if provider != "mini"]
            if alternatives:
                alternatives.sort(key=lambda x: x[1], reverse=True)
                winner = alternatives[0][0]
        elif winner == "mini":
            self.mini_evolution.note_promotion()

        return winner

    def _post_solve_updates(
        self,
        result: Dict,
        artifacts: SolveArtifacts,
        selected_judge,
        verification: Dict,
        verification_by_provider: Dict[str, Dict],
        disagreement: float,
        retrieval_strength: float,
        entropy: float,
        winner_margin: float,
        uncertainty: Dict[str, float],
        disagreement_cases: List[Dict],
        deterministic_dominance: bool,
        concept_clusters: Sequence[str],
        reinforced_clusters: Sequence[str],
        structure_by_provider: Dict[str, Dict],
        process_reward_by_provider: Dict[str, float],
        debate_outcome: Dict,
        question_tokens: int,
        shadow_bundle: Dict | None = None,
    ) -> None:
        profile = artifacts.profile
        provider_graphs = artifacts.reasoning_graph.get("provider_graphs", {}) if isinstance(artifacts.reasoning_graph, dict) else {}

        provider_reports = {row.provider: row for row in artifacts.judge_results}
        provider_candidates = {candidate.provider: candidate for candidate in artifacts.candidates}
        shadow_candidates = list((shadow_bundle or {}).get("candidates", [])) if isinstance(shadow_bundle, dict) else []
        all_candidates = list(artifacts.candidates)
        existing_providers = {candidate.provider for candidate in all_candidates}
        for candidate in shadow_candidates:
            if candidate.provider in existing_providers:
                continue
            all_candidates.append(candidate)
            existing_providers.add(candidate.provider)
            provider_candidates[candidate.provider] = candidate
        arena_thetas = result.get("arena", {}).get("bt_thetas", {})

        for candidate in artifacts.candidates:
            provider_report = provider_reports.get(candidate.provider)
            provider_verification = verification_by_provider.get(candidate.provider, {"verified": False})
            confidence = provider_report.score if provider_report else candidate.confidence

            self.stats.record_outcome(
                provider=candidate.provider,
                subject=profile.subject,
                difficulty=profile.difficulty,
                predicted_confidence=confidence,
                verified=bool(provider_verification.get("verified")),
                calibration_risk=(provider_report.risk if provider_report else None),
                concept_clusters=concept_clusters,
                token_usage=self._candidate_token_usage(candidate),
                question_tokens=question_tokens,
            )

        provider_rel = self.stats.provider_score(
            result["winner_provider"],
            profile,
            concept_clusters=concept_clusters,
        ) + 0.5

        uncertainty_values = list(uncertainty.values()) if isinstance(uncertainty, dict) else []
        mean_uncertainty = sum(uncertainty_values) / len(uncertainty_values) if uncertainty_values else 1.0

        event = {
            "event_type": "solve_result",
            "question": result["question"],
            "final_answer": result.get("final_answer"),
            "subject": profile.subject,
            "difficulty": profile.difficulty,
            "provider": result["winner_provider"],
            "verified": bool(verification.get("verified")),
            "failure_reason": verification.get("failure_reason"),
            "risk": float(verification.get("risk_score", 1.0)),
            "trap_probability": profile.trap_probability,
            "retrieval_strength": retrieval_strength,
            "critic_score": selected_judge.critic_score if selected_judge else 0.5,
            "provider_reliability": max(0.0, min(1.0, provider_rel)),
            "disagreement": disagreement,
            "entropy": entropy,
            "bt_margin": winner_margin,
            "disagreement_cluster_size": len(disagreement_cases),
            "deterministic_dominance": deterministic_dominance,
            "uncertainty": mean_uncertainty,
            "structural_coherence": float(structure_by_provider.get(result["winner_provider"], {}).get("structural_coherence_score", 0.0)),
            "process_reward": float(process_reward_by_provider.get(result["winner_provider"], 0.0)),
            "debate_triggered": bool(debate_outcome.get("triggered")),
            "debate_accepted": bool(debate_outcome.get("accepted")),
            "calibration_features": {
                "entropy": entropy,
                "bt_margin": winner_margin,
                "disagreement_cluster_size": len(disagreement_cases),
                "deterministic_dominance": deterministic_dominance,
                "uncertainty": mean_uncertainty,
                "structural_coherence": float(structure_by_provider.get(result["winner_provider"], {}).get("structural_coherence_score", 0.0)),
                "process_reward": float(process_reward_by_provider.get(result["winner_provider"], 0.0)),
            },
        }
        self.telemetry.append_event(event)

        # Prompt archive logging (analysis-only, no runtime prompt mutation).
        for candidate in artifacts.candidates:
            prompt_meta = {}
            if isinstance(candidate.raw, dict):
                prompt_meta = candidate.raw.get("prompt_meta", {}) or {}
            if not prompt_meta:
                continue

            prompt_text = str(prompt_meta.get("prompt", ""))
            prompt_hash = self._sha1(prompt_text)

            self.distillation.log_prompt_record(
                {
                    "question": result["question"],
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "provider": candidate.provider,
                    "model_name": prompt_meta.get("model_name", "unknown"),
                    "template_version": prompt_meta.get("template_version", "unknown"),
                    "system_instructions": prompt_meta.get("system_instructions", ""),
                    "prompt_hash": prompt_hash,
                    "prompt": prompt_text,
                    "is_winner": candidate.provider == result["winner_provider"],
                    "winner_verified": bool(verification.get("verified")),
                    "winner_margin": winner_margin,
                    "bt_theta": float(arena_thetas.get(candidate.provider, 0.0)),
                }
            )

        # Disagreement memory capture with full context for LC9 distillation.
        for case in disagreement_cases:
            a = str(case.get("provider_a"))
            b = str(case.get("provider_b"))

            ca = provider_candidates.get(a)
            cb = provider_candidates.get(b)

            va = verification_by_provider.get(a, {})
            vb = verification_by_provider.get(b, {})

            ja = provider_reports.get(a)
            jb = provider_reports.get(b)

            winner_provider = result["winner_provider"]
            winner_candidate = provider_candidates.get(winner_provider)
            winner_verification = verification_by_provider.get(winner_provider, {})
            winner_judge = provider_reports.get(winner_provider)

            self.distillation.log_disagreement_case(
                {
                    "question": result["question"],
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "concept_cluster": list(concept_clusters),
                    "winner_provider": winner_provider,
                    "winner_final_answer": (winner_candidate.final_answer if winner_candidate else result.get("final_answer", "")),
                    "winner_verified": bool(winner_verification.get("verified")),
                    "winner_judge_score": (winner_judge.score if winner_judge else 0.0),
                    "winner_margin": winner_margin,
                    "entropy": entropy,
                    "uncertainty": mean_uncertainty,
                    "bt_margin": winner_margin,
                    "deterministic_dominance": deterministic_dominance,
                    "case": case,
                    "providers": {
                        a: {
                            "final_answer": (ca.final_answer if ca else ""),
                            "reasoning": (ca.reasoning if ca else ""),
                            "reasoning_graph": provider_graphs.get(a, {}),
                            "judge": {
                                "score": (ja.score if ja else 0.0),
                                "risk": (ja.risk if ja else 1.0),
                                "rule_score": (ja.rule_score if ja else 0.0),
                                "critic_score": (ja.critic_score if ja else 0.0),
                            },
                            "verification": va,
                        },
                        b: {
                            "final_answer": (cb.final_answer if cb else ""),
                            "reasoning": (cb.reasoning if cb else ""),
                            "reasoning_graph": provider_graphs.get(b, {}),
                            "judge": {
                                "score": (jb.score if jb else 0.0),
                                "risk": (jb.risk if jb else 1.0),
                                "rule_score": (jb.rule_score if jb else 0.0),
                                "critic_score": (jb.critic_score if jb else 0.0),
                            },
                            "verification": vb,
                        },
                    },
                    "winner_reasoning_graph": provider_graphs.get(winner_provider, {}),
                    "winner_structure": structure_by_provider.get(winner_provider, {}),
                }
            )

        winner_provider = str(result.get("winner_provider", "")).strip()
        winner_answer = str(result.get("final_answer", "")).strip()
        winner_norm = normalize_answer(winner_answer)
        shadow_provider_set = {candidate.provider for candidate in shadow_candidates}
        answers_payload = []
        by_answer_cluster: Dict[str, Dict] = {}

        for candidate in all_candidates:
            provider = str(candidate.provider or "").strip()
            if not provider:
                continue
            answer = str(candidate.final_answer or "").strip()
            answer_norm = normalize_answer(answer)
            provider_ver = verification_by_provider.get(provider, {})
            report = provider_reports.get(provider)
            plausible = bool((result.get("arena", {}).get("plausibility_by_provider", {}).get(provider, {}) or {}).get("plausible", False))
            risk = float(provider_ver.get("risk_score", 1.0))
            verified = bool(provider_ver.get("verified", False))
            score = float(getattr(report, "score", 0.0)) if report is not None else 0.0
            posterior = float(result.get("arena", {}).get("posteriors", {}).get(provider, 0.0))

            answers_payload.append(
                {
                    "provider": provider,
                    "answer": answer,
                    "normalized_answer": answer_norm,
                    "verified": verified,
                    "plausible": plausible,
                    "risk": risk,
                    "score": score,
                    "posterior": posterior,
                    "is_shadow_provider": provider in shadow_provider_set,
                    "matches_winner_answer": bool(winner_norm and answer_norm and answer_norm == winner_norm),
                }
            )

            key = answer_norm or f"__empty__:{provider}"
            cluster = by_answer_cluster.setdefault(
                key,
                {"providers": [], "count": 0, "verified": 0, "plausible": 0},
            )
            cluster["providers"].append(provider)
            cluster["count"] += 1
            cluster["verified"] += int(verified)
            cluster["plausible"] += int(plausible)

        self.distillation.log_arena_shadow_disagreement(
            {
                "question": result.get("question", ""),
                "subject": profile.subject,
                "difficulty": profile.difficulty,
                "concept_cluster": list(concept_clusters),
                "winner_provider": winner_provider,
                "winner_answer": winner_answer,
                "winner_verified": bool(verification.get("verified", False)),
                "winner_margin": float(winner_margin),
                "entropy": float(entropy),
                "disagreement": float(disagreement),
                "provider_count": int(len(all_candidates)),
                "shadow_provider_count": int(len(shadow_candidates)),
                "answers": answers_payload,
                "policy": dict((shadow_bundle or {}).get("policy", {})) if isinstance(shadow_bundle, dict) else {},
            }
        )

        for provider_row in answers_payload:
            if str(provider_row.get("provider", "")) == winner_provider:
                continue
            mismatch = not bool(provider_row.get("matches_winner_answer", False))
            if not mismatch and bool(provider_row.get("verified", False)):
                continue
            self.distillation.log_deterministic_vs_provider_gap(
                {
                    "question": result.get("question", ""),
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "concept_cluster": list(concept_clusters),
                    "winner_provider": winner_provider,
                    "winner_answer": winner_answer,
                    "winner_verified": bool(verification.get("verified", False)),
                    "provider": provider_row.get("provider"),
                    "provider_answer": provider_row.get("answer"),
                    "provider_verified": bool(provider_row.get("verified", False)),
                    "provider_plausible": bool(provider_row.get("plausible", False)),
                    "provider_risk": float(provider_row.get("risk", 1.0)),
                    "provider_score": float(provider_row.get("score", 0.0)),
                    "provider_posterior": float(provider_row.get("posterior", 0.0)),
                    "answer_mismatch": bool(mismatch),
                    "is_shadow_provider": bool(provider_row.get("is_shadow_provider", False)),
                    "entropy": float(entropy),
                    "disagreement": float(disagreement),
                }
            )

        for answer_key, cluster in by_answer_cluster.items():
            self.distillation.log_reasoning_divergence_cluster(
                {
                    "question": result.get("question", ""),
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "concept_cluster": list(concept_clusters),
                    "normalized_answer": answer_key,
                    "providers": list(cluster.get("providers", [])),
                    "count": int(cluster.get("count", 0)),
                    "verified_count": int(cluster.get("verified", 0)),
                    "plausible_count": int(cluster.get("plausible", 0)),
                    "winner_provider": winner_provider,
                    "winner_answer": winner_answer,
                    "entropy": float(entropy),
                    "disagreement": float(disagreement),
                }
            )

        rare_clusters = [cluster for cluster in concept_clusters if self._is_rare_cluster_label(cluster)]
        if rare_clusters:
            self.distillation.log_rare_cluster_cross_provider(
                {
                    "question": result.get("question", ""),
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "rare_clusters": rare_clusters,
                    "winner_provider": winner_provider,
                    "winner_answer": winner_answer,
                    "winner_verified": bool(verification.get("verified", False)),
                    "entropy": float(entropy),
                    "disagreement": float(disagreement),
                    "providers": answers_payload,
                }
            )

        if debate_outcome.get("triggered"):
            self.distillation.log_debate_outcome(
                {
                    "question": result["question"],
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "concept_cluster": list(concept_clusters),
                    "winner_provider": result["winner_provider"],
                    "winner_final_answer": result.get("final_answer", ""),
                    "winner_verified": bool(verification.get("verified")),
                    "winner_margin": winner_margin,
                    "entropy": entropy,
                    "uncertainty": mean_uncertainty,
                    "debate": {
                        "accepted": bool(debate_outcome.get("accepted")),
                        "provider": debate_outcome.get("provider"),
                        "top2": debate_outcome.get("top2", []),
                    },
                }
            )

        if any(c.provider == "mini" for c in artifacts.candidates):
            mini_candidate = next(c for c in artifacts.candidates if c.provider == "mini")
            mini_verification = verification_by_provider.get("mini", {"verified": False})
            mini_judge = provider_reports.get("mini")

            winner_answer = str(result.get("final_answer", "")).strip().lower()
            mini_answer = str(mini_candidate.final_answer).strip().lower()
            agreement_with_winner = bool(winner_answer and mini_answer and winner_answer == mini_answer)

            mini_target = 1.0 if agreement_with_winner and bool(verification.get("verified")) else 0.0
            mini_pred = float(mini_judge.score if mini_judge else mini_candidate.confidence)
            mini_brier = (mini_pred - mini_target) ** 2

            self.mini_evolution.record_shadow_outcome(
                subject=profile.subject,
                difficulty=profile.difficulty,
                predicted_confidence=(mini_judge.score if mini_judge else mini_candidate.confidence),
                verified=bool(mini_verification.get("verified")),
                calibration_risk=(mini_judge.risk if mini_judge else 1.0),
                disagreement_size=len(disagreement_cases),
                concept_clusters=concept_clusters,
            )

            self.distillation.log_shadow_eval(
                {
                    "question": result["question"],
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "concept_cluster": list(concept_clusters),
                    "mini_answer": mini_candidate.final_answer,
                    "mini_reasoning": mini_candidate.reasoning,
                    "mini_confidence": mini_pred,
                    "mini_verified": bool(mini_verification.get("verified")),
                    "arena_winner_provider": result["winner_provider"],
                    "arena_winner_answer": result.get("final_answer"),
                    "agreement_with_winner": agreement_with_winner,
                    "winner_verified": bool(verification.get("verified")),
                    "brier_subject": mini_brier,
                    "calibration_drift": self.mini_evolution.drift_score(),
                    "entropy": entropy,
                    "winner_margin": winner_margin,
                    "uncertainty": mean_uncertainty,
                }
            )

            for case in disagreement_cases:
                if case.get("provider_a") == "mini" or case.get("provider_b") == "mini":
                    self.mini_evolution.log_disagreement_case(
                        {
                            "question": result["question"],
                            "subject": profile.subject,
                            "difficulty": profile.difficulty,
                            "case": case,
                        }
                    )

            # Distillation entry from finalized winner session.
            winner_provider = result["winner_provider"]
            winner_candidate = provider_candidates.get(winner_provider)
            winner_graph = provider_graphs.get(winner_provider, {})
            winner_judge = provider_reports.get(winner_provider)

            self.distillation.try_add_training_entry(
                {
                    "question": result["question"],
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "concept_cluster": list(concept_clusters),
                    "verified_answer": result.get("final_answer"),
                    "best_provider": winner_provider,
                    "best_reasoning_graph": winner_graph,
                    "judge_score": (winner_judge.score if winner_judge else 0.0),
                    "deterministic_pass": bool(verification.get("verified")),
                    "winner_margin": winner_margin,
                    "uncertainty": mean_uncertainty,
                    "structural_coherence": float(structure_by_provider.get(winner_provider, {}).get("structural_coherence_score", 0.0)),
                    "process_reward": float(process_reward_by_provider.get(winner_provider, 0.0)),
                    "curriculum_level": self._curriculum_level(profile.difficulty, concept_clusters, entropy, disagreement),
                }
            )

            if not mini_verification.get("verified"):
                mini_error = self.meta_verification.classify(
                    question=result["question"],
                    subject=profile.subject,
                    difficulty=profile.difficulty,
                    concept_clusters=concept_clusters,
                    predicted_answer=mini_candidate.final_answer,
                    predicted_confidence=mini_pred,
                    verification=mini_verification,
                    structure=structure_by_provider.get("mini", {}),
                )
                mini_error_weight = self.meta_verification.error_weight(
                    mini_error.get("error_type", "unknown"),
                    concept_clusters=concept_clusters,
                )
                self.meta_verification.log(
                    {
                        **mini_error,
                        "question": result["question"],
                        "provider": "mini",
                        "winner_provider": result["winner_provider"],
                    }
                )

                self.mini_evolution.enqueue_failure(
                    {
                        "question": result["question"],
                        "subject": profile.subject,
                        "difficulty": profile.difficulty,
                        "provider": "mini",
                        "risk": float(mini_verification.get("risk_score", 1.0)),
                        "calibration_risk": (mini_judge.risk if mini_judge else 1.0),
                        "deterministic_fail": True,
                        "entropy": entropy,
                        "mini_disagreement": 1.0 - (1.0 if agreement_with_winner else 0.0),
                        "disagreement": disagreement,
                        "reason": mini_verification.get("failure_reason") or mini_verification.get("reason") or "unknown",
                        "final_answer": mini_candidate.final_answer,
                        "concept_clusters": list(concept_clusters),
                        "reinforced_clusters": list(reinforced_clusters),
                        "error_type": mini_error.get("error_type", "unknown"),
                        "error_weight": mini_error_weight,
                        "curriculum_level": self._curriculum_level(profile.difficulty, concept_clusters, entropy, disagreement),
                    }
                )

        if not verification.get("verified"):
            winner_structure = structure_by_provider.get(result["winner_provider"], {})
            winner_confidence = float(selected_judge.score if selected_judge else 0.5)
            error_record = self.meta_verification.classify(
                question=result["question"],
                subject=profile.subject,
                difficulty=profile.difficulty,
                concept_clusters=concept_clusters,
                predicted_answer=str(result.get("final_answer", "")),
                predicted_confidence=winner_confidence,
                verification=verification,
                structure=winner_structure,
            )
            error_weight = self.meta_verification.error_weight(
                error_record.get("error_type", "unknown"),
                concept_clusters=concept_clusters,
            )
            self.meta_verification.log(
                {
                    **error_record,
                    "question": result["question"],
                    "provider": result["winner_provider"],
                }
            )

            payload = {
                "question": result["question"],
                "subject": profile.subject,
                "difficulty": profile.difficulty,
                "provider": result["winner_provider"],
                "risk": float(verification.get("risk_score", 1.0)),
                "calibration_risk": (selected_judge.risk if selected_judge else 1.0),
                "deterministic_fail": True,
                "entropy": entropy,
                "mini_disagreement": disagreement,
                "reason": verification.get("failure_reason") or verification.get("reason") or "unknown",
                "final_answer": result.get("final_answer"),
                "disagreement": disagreement,
                "concept_clusters": list(concept_clusters),
                "reinforced_clusters": list(reinforced_clusters),
                "error_type": error_record.get("error_type", "unknown"),
                "error_weight": error_weight,
                "curriculum_level": self._curriculum_level(profile.difficulty, concept_clusters, entropy, disagreement),
            }
            self.replay.log_failure(payload)
            self.mini_evolution.enqueue_failure(payload)

        token_usage_by_provider = {}
        for candidate in artifacts.candidates:
            usage = self._candidate_token_usage(candidate)
            if usage:
                token_usage_by_provider[candidate.provider] = usage
        self.token_guardian.record_session(
            token_usage_by_provider=token_usage_by_provider,
            debate_triggered=bool(debate_outcome.get("triggered")),
        )

        # Non-blocking automation hooks (no provider calls, no scoring impact).
        try:
            from core.automation.hooks import dispatch_post_arena_hooks

            dispatch_post_arena_hooks(
                {
                    "subject": profile.subject,
                    "difficulty": profile.difficulty,
                    "winner_provider": result.get("winner_provider"),
                    "winner_verified": bool(verification.get("verified")),
                    "winner_margin": float(winner_margin),
                    "entropy": float(entropy),
                    "disagreement_case_count": int(len(disagreement_cases)),
                    "mini_shadow": any(c.provider == "mini" for c in artifacts.candidates),
                    "hook_signals": {
                        "disagreement_logging": bool(disagreement_cases),
                        "shadow_logging": any(c.provider == "mini" for c in artifacts.candidates),
                        "calibration_feature_logging": True,
                        "provider_stats_update": True,
                        "concept_reinforcement_update": bool(reinforced_clusters),
                    },
                }
            )
        except Exception:
            pass

    def _all_empty(self, candidates: Sequence[ProviderAnswer]) -> bool:
        if not candidates:
            return True
        return all(not candidate.final_answer.strip() for candidate in candidates)

    def _candidate_by_provider(self, candidates: Sequence[ProviderAnswer], provider: str) -> ProviderAnswer:
        for candidate in candidates:
            if candidate.provider == provider:
                return candidate
        return max(candidates, key=lambda candidate: candidate.confidence)

    def _disagreement(self, candidates: Sequence[ProviderAnswer], question_text: str = "") -> float:
        answers = [candidate.final_answer.strip().lower() for candidate in candidates if candidate.final_answer.strip()]
        if len(answers) <= 1:
            return 0.0

        unique_ratio = (len(set(answers)) - 1) / max(len(answers), 1)

        texts = []
        for candidate in candidates:
            final_answer = str(candidate.final_answer or "").strip().lower()
            reasoning = str(candidate.reasoning or "").strip().lower()[:220]
            if not final_answer and not reasoning:
                continue
            texts.append(f"{final_answer} || {reasoning}")

        pair_dist = 0.0
        pair_count = 0
        for i in range(len(texts)):
            for j in range(i + 1, len(texts)):
                ratio = float(SequenceMatcher(a=texts[i], b=texts[j]).ratio())
                pair_dist += max(0.0, 1.0 - ratio)
                pair_count += 1
        structural_disagreement = (pair_dist / pair_count) if pair_count else 0.0

        disagreement = 0.55 * unique_ratio + 0.45 * structural_disagreement

        # If all providers converge on a fragment copied from the question, keep disagreement non-zero.
        q_norm = " ".join(str(question_text or "").strip().lower().split())
        if q_norm and len(set(answers)) == 1 and answers and answers[0] in q_norm and len(answers[0]) >= 8:
            disagreement = max(disagreement, 0.22)

        return max(0.0, min(1.0, disagreement))

    def _retrieval_strength(self, retrieved) -> float:
        if not retrieved:
            return 0.0

        top = retrieved[:5]
        score = sum(float(block.score) for block in top) / max(1, len(top))
        return max(0.0, min(1.0, score))

    def _claim_support_score(self, claim_support: Dict[str, List]) -> float:
        if not claim_support:
            return 0.0

        score = 0.0
        count = 0
        for blocks in claim_support.values():
            if not blocks:
                continue
            score += max(float(block.score) for block in blocks)
            count += 1

        if count == 0:
            return 0.0

        return max(0.0, min(1.0, score / count))

    def _concept_clusters(self, retrieved) -> List[str]:
        clusters = []
        for block in retrieved:
            for tag in block.tags:
                tag = str(tag).lower().strip()
                if tag and tag not in clusters:
                    clusters.append(tag)

        if not clusters:
            clusters.append("general")

        return clusters[:8]

    def _curriculum_level(self, difficulty: str, concept_clusters: Sequence[str], entropy: float, disagreement: float) -> int:
        cluster_count = len([c for c in concept_clusters if str(c).strip()])
        if entropy >= 0.70 or disagreement >= 0.70:
            return 5
        if difficulty == "hard" and cluster_count >= 2:
            return 4
        if cluster_count >= 2:
            return 3
        if difficulty in {"medium", "hard"}:
            return 2
        return 1

    def _authority_question(self, question: str) -> str:
        text = str(question or "")
        lowered = text.lower()
        for marker in ("user question:\n", "user question:", "original question:\n", "original question:"):
            idx = lowered.rfind(marker)
            if idx == -1:
                continue
            extracted = text[idx + len(marker) :].strip()
            if extracted:
                return extracted
        return text

    def _is_symbolic_heavy(self, question: str, profile) -> bool:
        text = str(question or "").lower()
        deterministic_case = solve_contextual_math_question(question)
        if deterministic_case and bool(deterministic_case.get("handled")):
            return True
        # Treat structured/discrete math as deterministic-friendly so symbolic_guard
        # is always injected before arena arbitration.
        if parse_structured_problem(text) is not None:
            return True

        hard_markers = (
            "integral",
            "∫",
            "differentiate",
            "d/dx",
            "derivative",
            "from ",
            " to ",
            "sin^(-1)",
            "cos^(-1)",
            "tan^(-1)",
            "asin(",
            "acos(",
            "atan(",
        )
        discrete_markers = (
            "permutation",
            "combination",
            "arrangement",
            "arrangements",
            "without repetition",
            "with repetition",
            "divisible by",
            "how many",
            "digit number",
            "letters",
            "word ",
            "no two",
            "factorial",
            "ncr",
        )
        marker_hits = sum(1 for marker in hard_markers if marker in text)
        discrete_hits = sum(1 for marker in discrete_markers if marker in text)
        if marker_hits >= 2:
            return True
        if profile.difficulty == "hard" and marker_hits >= 1:
            return True
        if discrete_hits >= 2:
            return True
        return False

    def _candidate_token_usage(self, candidate: ProviderAnswer) -> Dict:
        if isinstance(candidate.raw, dict):
            usage = candidate.raw.get("token_usage")
            if isinstance(usage, dict):
                return {
                    "prompt_tokens": float(usage.get("prompt_tokens", 0.0) or 0.0),
                    "completion_tokens": float(usage.get("completion_tokens", 0.0) or 0.0),
                    "total_tokens": float(usage.get("total_tokens", 0.0) or 0.0),
                }
        return {}

    def _replace_candidate(
        self,
        candidates: Sequence[ProviderAnswer],
        provider: str,
        updated: ProviderAnswer,
    ) -> List[ProviderAnswer]:
        out = []
        replaced = False
        for candidate in candidates:
            if candidate.provider == provider and not replaced:
                out.append(updated)
                replaced = True
            else:
                out.append(candidate)
        if not replaced:
            out.append(updated)
        return out

    def _should_run_debate(self, entropy: float, candidate_count: int, selected_verification: Dict) -> bool:
        if candidate_count < 2:
            return False
        if bool(selected_verification.get("verified")):
            return False
        max_entropy = math.log(max(2, candidate_count))
        norm_entropy = 0.0 if max_entropy <= 0 else max(0.0, min(1.0, entropy / max_entropy))
        return norm_entropy >= 0.68

    async def _run_self_debate_lite(
        self,
        question: str,
        profile,
        retrieved,
        arena_outcome: Dict,
        candidates: Sequence[ProviderAnswer],
        verification_by_provider: Dict[str, Dict],
        provider_reliability: Dict[str, float],
        retrieval_strength: float,
        coherence_by_provider: Dict[str, float],
        structure_by_provider: Dict[str, Dict],
        process_reward_by_provider: Dict[str, float],
        current_provider: str,
        current_judge,
        current_verification: Dict,
    ) -> Dict:
        posteriors = arena_outcome.get("posteriors", {})
        ranked = sorted(posteriors.items(), key=lambda x: x[1], reverse=True)
        if len(ranked) < 2:
            return {"triggered": False, "accepted": False}
        if not self.token_guardian.allow_debate():
            return {"triggered": False, "accepted": False, "budget_guarded": True}

        top2 = [ranked[0][0], ranked[1][0]]
        provider_candidates = {candidate.provider: candidate for candidate in candidates}
        if top2[0] not in provider_candidates or top2[1] not in provider_candidates:
            return {"triggered": False, "accepted": False}

        # Prefer non-mini provider for the single extra API call.
        debate_provider = top2[0]
        if debate_provider == "mini" and top2[1] != "mini":
            debate_provider = top2[1]

        a = provider_candidates[top2[0]]
        b = provider_candidates[top2[1]]
        summary_a = self._summary_from_graph(a.provider, provider_candidates[a.provider], structure_by_provider)
        summary_b = self._summary_from_graph(b.provider, provider_candidates[b.provider], structure_by_provider)

        cache_key = self._sha1(f"{question}|{debate_provider}|{a.final_answer}|{b.final_answer}")
        debate_candidate = None
        if self._valid_hash(cache_key) and cache_key in self._debate_cache:
            cached = self._debate_cache[cache_key]
            self._debate_cache.move_to_end(cache_key)
            debate_candidate = ProviderAnswer(
                provider=debate_provider,
                reasoning=str(cached.get("reasoning", "")),
                final_answer=str(cached.get("final_answer", "")),
                confidence=float(cached.get("confidence", 0.5)),
                self_critique="debate_cache",
                latency_s=0.0,
                raw={"debate_lite": True, "cached": True, "token_usage": cached.get("token_usage", {})},
            )
        else:
            debate_prompt = self._debate_prompt(question, a, b, summary_a, summary_b)
            debate_candidate = await self.providers.generate(
                debate_provider,
                debate_prompt,
                profile,
                retrieved[:2],
            )
            debate_candidate.raw = {
                **(debate_candidate.raw or {}),
                "debate_lite": True,
                "debate_prompt_version": "lc9_debate_lite_v1",
            }
            if self._valid_hash(cache_key):
                self._debate_cache[cache_key] = {
                    "reasoning": debate_candidate.reasoning,
                    "final_answer": debate_candidate.final_answer,
                    "confidence": debate_candidate.confidence,
                    "token_usage": debate_candidate.raw.get("token_usage", {}),
                }
                self._debate_cache.move_to_end(cache_key)
                while len(self._debate_cache) > self._debate_cache_limit:
                    self._debate_cache.popitem(last=False)

        if not debate_candidate.final_answer.strip():
            return {
                "triggered": True,
                "accepted": False,
                "provider": debate_provider,
                "top2": top2,
            }

        substitution_hooks = self.reasoner.numeric_substitution_hooks(question)
        debate_verification = verify_solution(
            question=question,
            predicted_answer=debate_candidate.final_answer,
            difficulty=profile.difficulty,
            substitution_hooks=substitution_hooks,
        )
        debate_verification["trap_probability"] = profile.trap_probability

        debate_graph_full = self.reasoner.build_graph([debate_candidate])
        debate_structure = debate_graph_full.get("structure_metrics", {}).get(debate_provider, {})
        debate_coherence = float(debate_graph_full.get("coherence", {}).get(debate_provider, 0.5))
        debate_process = float(debate_graph_full.get("process_reward", {}).get(debate_provider, 0.5))

        debate_judge_rows = self.judge.evaluate(
            candidates=[debate_candidate],
            verification_by_provider={debate_provider: debate_verification},
            provider_reliability={debate_provider: float(provider_reliability.get(debate_provider, 0.5))},
            retrieval_strength=retrieval_strength,
            coherence_by_provider={debate_provider: debate_coherence},
            structure_by_provider={debate_provider: debate_structure},
            process_reward_by_provider={debate_provider: debate_process},
        )
        debate_judge = debate_judge_rows[0] if debate_judge_rows else None

        current_score = float(current_judge.score if current_judge else 0.0)
        current_risk = float(current_verification.get("risk_score", 1.0))
        debate_score = float(debate_judge.score if debate_judge else 0.0)
        debate_risk = float(debate_verification.get("risk_score", 1.0))

        accepted = False
        if bool(debate_verification.get("verified")) and not bool(current_verification.get("verified")):
            accepted = True
        elif debate_score >= (current_score + 0.07) and debate_risk <= current_risk:
            accepted = True

        return {
            "triggered": True,
            "accepted": accepted,
            "provider": debate_provider if accepted else current_provider,
            "candidate": debate_candidate if accepted else None,
            "verification": debate_verification if accepted else None,
            "judge": debate_judge if accepted else None,
            "structure": debate_structure if accepted else None,
            "top2": top2,
        }

    def _summary_from_graph(self, provider: str, candidate: ProviderAnswer, structure_by_provider: Dict[str, Dict]) -> str:
        structure = structure_by_provider.get(provider, {}) or {}
        base = [
            f"answer={candidate.final_answer}",
            f"coherence={float(structure.get('structural_coherence_score', 0.0)):.3f}",
            f"process={float(structure.get('process_reward_score', 0.0)):.3f}",
        ]
        reasoning = str(candidate.reasoning or "")
        lines = [line.strip() for line in reasoning.splitlines() if line.strip()]
        snippet = " | ".join(lines[:3]) if lines else reasoning[:160]
        base.append(f"steps={snippet[:220]}")
        return "; ".join(base)

    def _debate_prompt(
        self,
        question: str,
        a: ProviderAnswer,
        b: ProviderAnswer,
        summary_a: str,
        summary_b: str,
    ) -> str:
        return (
            "Resolve disagreement with minimal tokens.\n"
            f"Original question: {question}\n"
            f"Candidate A ({a.provider}): {summary_a}\n"
            f"Candidate B ({b.provider}): {summary_b}\n"
            "Return:\nReasoning: <max 4 concise steps>\nFinal Answer: <exact answer>"
        )

    async def _safe_available_providers(self) -> List[str]:
        providers = list(self.providers.available_providers())
        if not providers:
            raise RuntimeError("provider_registry_empty")
        return providers

    def _snapshot_candidate(self, candidate: ProviderAnswer) -> Dict:
        return {
            "provider": candidate.provider,
            "final_answer": str(candidate.final_answer),
            "deterministic_pass": False,
            "confidence": float(candidate.confidence),
        }

    def _total_token_usage(self, candidates: Sequence[ProviderAnswer]) -> Dict[str, float]:
        total = {"prompt_tokens": 0.0, "completion_tokens": 0.0, "total_tokens": 0.0}
        for candidate in candidates:
            usage = self._candidate_token_usage(candidate)
            for key in total:
                total[key] += float(usage.get(key, 0.0))
        return total

    def _arena_fallback_outcome(self, responses: Sequence[Dict], verification_by_provider: Dict[str, Dict]) -> Dict:
        posteriors = {}
        thetas = {}
        for row in responses:
            provider = str(row.get("provider", ""))
            verified = bool(verification_by_provider.get(provider, {}).get("verified"))
            score = 1.0 if verified else float(row.get("confidence", 0.0))
            thetas[provider] = score
            posteriors[provider] = score
        total = sum(posteriors.values())
        if total <= 0.0:
            n = max(1, len(posteriors))
            posteriors = {p: 1.0 / n for p in posteriors}
        else:
            posteriors = {p: v / total for p, v in posteriors.items()}
        winner = max(posteriors, key=posteriors.get) if posteriors else ""
        return {
            "entropy": 0.0,
            "thetas": thetas,
            "matches": [],
            "pairwise": {"uncertainties": {}, "confidence_margin": 0.0, "uncertainty_adjusted_margin": 0.0, "disagreement_cases": []},
            "posteriors": posteriors,
            "winner": winner,
            "winner_margin": 0.0,
            "arena_confidence": 0.0,
            "bayesian": {"posteriors": posteriors},
            "guard_fallback": True,
        }

    def _fallback_provider_selection(
        self,
        *,
        candidates: Sequence[ProviderAnswer],
        verification_by_provider: Dict[str, Dict],
        arena_outcome: Dict,
    ) -> str:
        non_empty = [c for c in candidates if str(c.final_answer or "").strip()]
        deterministic = [
            c.provider
            for c in non_empty
            if bool(verification_by_provider.get(c.provider, {}).get("verified"))
        ]
        if deterministic:
            return deterministic[0]

        posteriors = arena_outcome.get("posteriors", {}) if isinstance(arena_outcome, dict) else {}
        if non_empty and isinstance(posteriors, dict) and posteriors:
            ranked_non_empty = sorted(
                non_empty,
                key=lambda c: float(posteriors.get(c.provider, 0.0)),
                reverse=True,
            )
            if ranked_non_empty:
                return str(ranked_non_empty[0].provider)

        if non_empty:
            ranked_by_confidence = sorted(non_empty, key=lambda c: float(c.confidence), reverse=True)
            if ranked_by_confidence:
                return str(ranked_by_confidence[0].provider)

        uncertainties = arena_outcome.get("pairwise", {}).get("uncertainties", {}) if isinstance(arena_outcome, dict) else {}
        if uncertainties:
            ranked = sorted(uncertainties.items(), key=lambda x: float(x[1]))
            if ranked:
                return str(ranked[0][0])
        if candidates:
            return candidates[0].provider
        return ""

    def _degraded_result(
        self,
        *,
        question: str,
        profile,
        retrieved,
        candidates: Sequence[ProviderAnswer],
        verification_by_provider: Dict[str, Dict],
        reason: str,
        arena_outcome: Dict | None = None,
    ) -> Dict:
        arena_outcome = arena_outcome or {"posteriors": {}, "thetas": {}, "pairwise": {"uncertainties": {}}, "winner_margin": 0.0}
        provider = self._fallback_provider_selection(
            candidates=candidates,
            verification_by_provider=verification_by_provider,
            arena_outcome=arena_outcome,
        )
        selected = self._candidate_by_provider(candidates, provider) if candidates else ProviderAnswer(provider="none", reasoning="", final_answer="", confidence=0.0)
        verification = verification_by_provider.get(provider, {"verified": False, "risk_score": 1.0, "reason": reason})
        fallback_answer = str(selected.final_answer or "").strip()
        if not fallback_answer:
            fallback_answer = "Uncertain answer: providers returned no usable output for this prompt. Please retry."

        return {
            "question": question,
            "reasoning": selected.reasoning,
            "final_answer": fallback_answer,
            "verification": verification,
            "routing_decision": "degraded_mode",
            "escalate": True,
            "winner_provider": provider,
            "profile": {
                "subject": profile.subject,
                "difficulty": profile.difficulty,
                "numeric": profile.numeric,
                "multiConcept": profile.multi_concept,
                "trapProbability": profile.trap_probability,
            },
            "arena": {
                "ranked_providers": [],
                "judge_results": [],
                "bt_thetas": arena_outcome.get("thetas", {}),
                "posteriors": arena_outcome.get("posteriors", {}),
                "winner_margin": float(arena_outcome.get("winner_margin", 0.0)),
                "arena_confidence": float(arena_outcome.get("arena_confidence", 0.0)),
                "pairwise_confidence_margin": float(arena_outcome.get("pairwise", {}).get("confidence_margin", 0.0)),
                "uncertainty_adjusted_margin": float(arena_outcome.get("pairwise", {}).get("uncertainty_adjusted_margin", 0.0)),
                "disagreement": 0.0,
                "disagreement_case_count": 0,
                "deterministic_dominance": any(bool(r.get("verified")) for r in verification_by_provider.values()),
                "entropy": 0.0,
            },
            "retrieval": {
                "top_blocks": [
                    {"id": block.block_id, "title": block.title, "score": block.score, "source": block.source}
                    for block in (retrieved or [])
                ],
                "claim_support_score": 0.0,
            },
            "engine": {
                "name": "LALACORE_X",
                "version": "research-grade-v2",
                "backward_compatible": True,
                "mini_shadow": any(c.provider == "mini" for c in candidates),
                "degraded_mode": True,
                "degraded_reason": str(reason),
                "provider_availability": self.providers.availability_report(),
            },
            "quality_gate": {
                "completion_ok": False,
                "final_status": "Failed",
                "force_escalate": True,
                "reasons": [str(reason)],
            },
            "final_status": "Failed",
        }

    async def _rescue_from_empty_pool(
        self,
        *,
        question: str,
        profile,
        retrieved,
        candidates: Sequence[ProviderAnswer],
    ) -> List[ProviderAnswer]:
        rescued: List[ProviderAnswer] = []
        existing_with_answers = {
            candidate.provider
            for candidate in candidates
            if str(candidate.final_answer or "").strip()
        }
        for provider in ("symbolic_guard", "mini"):
            if provider in existing_with_answers:
                continue
            try:
                candidate = await self.providers.generate(provider, question, profile, retrieved)
            except Exception:
                continue
            if str(candidate.final_answer or "").strip():
                rescued.append(candidate)
            if rescued and provider == "symbolic_guard":
                break
        return rescued

    def _valid_hash(self, value: str) -> bool:
        return isinstance(value, str) and len(value) == 40 and all(c in "0123456789abcdef" for c in value.lower())

    def _sha1(self, text: str) -> str:
        return hashlib.sha1(str(text).encode("utf-8")).hexdigest()
