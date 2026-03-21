from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from app.arena.bayesian_aggregator import BayesianAggregator
from app.arena.bradley_terry import BradleyTerryEngine
from app.arena.entropy import compute_entropy
from app.arena.pairwise_engine import PairwiseEngine
from core.lalacore_x.calibration import ConfidenceCalibrator
from core.lalacore_x.schemas import JudgeResult, ProviderAnswer
from core.safe_math import clipped_division, safe_log


@dataclass(slots=True)
class _Node:
    provider: str
    visits: int = 0
    value: float = 0.0


class ArenaJudge:
    """
    Deterministic-first judge with calibration-aware scoring.
    """

    def __init__(self, calibrator: ConfidenceCalibrator | None = None):
        self.calibrator = calibrator or ConfidenceCalibrator()

    def evaluate(
        self,
        candidates: Sequence[ProviderAnswer],
        verification_by_provider: Dict[str, Dict],
        provider_reliability: Dict[str, float],
        retrieval_strength: float,
        coherence_by_provider: Dict[str, float] | None = None,
        structure_by_provider: Dict[str, Dict] | None = None,
        process_reward_by_provider: Dict[str, float] | None = None,
    ) -> List[JudgeResult]:
        coherence_by_provider = coherence_by_provider or {}
        structure_by_provider = structure_by_provider or {}
        process_reward_by_provider = process_reward_by_provider or {}
        disagreement = self._disagreement(candidates)

        # Deterministic supremacy reference.
        any_verified = any(bool(verification_by_provider.get(c.provider, {}).get("verified")) for c in candidates)

        results: List[JudgeResult] = []
        for candidate in candidates:
            verification = verification_by_provider.get(candidate.provider, {})
            verified = bool(verification.get("verified"))
            coherence = float(coherence_by_provider.get(candidate.provider, 0.5))
            structure = structure_by_provider.get(candidate.provider, {}) or {}
            process_reward = float(
                process_reward_by_provider.get(
                    candidate.provider,
                    structure.get("process_reward_score", 0.5),
                )
            )

            stage_results = verification.get("stage_results", {})
            stage_total = max(1, len(stage_results))
            stage_pass_ratio = sum(1 for v in stage_results.values() if v) / stage_total

            # Rule score is feature-driven, not lexical.
            rule_score = (
                0.42 * stage_pass_ratio
                + 0.18 * coherence
                + 0.16 * process_reward
                + 0.14 * float(candidate.confidence)
                + 0.10 * float(provider_reliability.get(candidate.provider, 0.5))
            )
            rule_score -= (
                0.12 * float(structure.get("circular_reasoning", 0.0))
                + 0.08 * float(structure.get("missing_inference_rate", 0.0))
                + 0.06 * float(structure.get("step_redundancy_rate", 0.0))
            )
            rule_score = max(0.0, min(1.0, rule_score))

            if verified:
                rule_score = max(rule_score, 0.80)

            # If at least one verified answer exists, cap non-verified candidates.
            if any_verified and not verified:
                rule_score = min(rule_score, 0.45)

            critic_score = self._critic_proxy(candidate, coherence, process_reward, verification, structure)

            features = {
                "verification_fail": 0.0 if verified else 1.0,
                "disagreement": disagreement,
                "retrieval_strength": retrieval_strength,
                "critic_score": critic_score,
                "provider_reliability": provider_reliability.get(candidate.provider, 0.5),
                "trap_probability": float(verification.get("trap_probability", 0.0)),
                "entropy": float(verification.get("entropy", 0.0)),
                "structural_coherence": coherence,
                "process_reward": process_reward,
                "graph_missing_inference": float(structure.get("missing_inference_rate", 0.0)),
                "graph_redundancy": float(structure.get("step_redundancy_rate", 0.0)),
            }
            calibration_risk = self.calibrator.predict_risk(features)

            score = max(0.0, min(1.0, (0.62 * rule_score + 0.38 * critic_score) * (1.0 - calibration_risk)))

            notes = []
            if verified:
                notes.append("deterministic_verified")
            if coherence < 0.45:
                notes.append("low_graph_coherence")
            if process_reward < 0.4:
                notes.append("weak_process_reward")

            results.append(
                JudgeResult(
                    provider=candidate.provider,
                    score=score,
                    risk=calibration_risk,
                    rule_score=rule_score,
                    critic_score=critic_score,
                    calibration_risk=calibration_risk,
                    verified=verified,
                    notes=notes,
                )
            )

        return sorted(results, key=lambda row: row.score, reverse=True)

    def _critic_proxy(
        self,
        candidate: ProviderAnswer,
        coherence: float,
        process_reward: float,
        verification: Dict,
        structure: Dict,
    ) -> float:
        risk = float(verification.get("risk_score", 1.0))
        stage_results = verification.get("stage_results", {})
        stage_total = max(1, len(stage_results))
        stage_pass = sum(1 for v in stage_results.values() if v) / stage_total

        proxy = (
            0.38 * stage_pass
            + 0.20 * coherence
            + 0.18 * process_reward
            + 0.16 * (1.0 - risk)
            + 0.08 * float(candidate.confidence)
        )
        proxy -= 0.08 * float(structure.get("circular_reasoning", 0.0))
        return max(0.0, min(1.0, proxy))

    def _disagreement(self, candidates: Sequence[ProviderAnswer]) -> float:
        answers = [c.final_answer.strip().lower() for c in candidates if c.final_answer.strip()]
        if len(answers) <= 1:
            return 0.0

        unique = len(set(answers))
        return min(1.0, (unique - 1) / max(len(answers), 1))


class MCTSReasoner:
    def __init__(self, exploration: float = 1.2, simulations: int = 24):
        self.exploration = exploration
        self.simulations = simulations

    def select(self, utility_by_provider: Dict[str, float]) -> Tuple[str, List[Dict]]:
        nodes = {provider: _Node(provider=provider) for provider in utility_by_provider}
        trace: List[Dict] = []

        if not nodes:
            return "", trace

        for _ in range(self.simulations):
            selected = self._uct_select(nodes)
            reward = self._simulate_reward(utility_by_provider[selected.provider])
            selected.visits += 1
            selected.value += reward

            trace.append(
                {
                    "provider": selected.provider,
                    "reward": round(reward, 6),
                    "visits": selected.visits,
                    "mean": round(selected.value / selected.visits, 6),
                }
            )

        best = max(nodes.values(), key=lambda n: (n.value / max(n.visits, 1), n.visits))
        return best.provider, trace

    def _uct_select(self, nodes: Dict[str, _Node]) -> _Node:
        total_visits = sum(node.visits for node in nodes.values()) + 1

        for node in nodes.values():
            if node.visits == 0:
                return node

        def uct(node: _Node) -> float:
            mean = clipped_division(node.value, node.visits, fallback=0.0)
            explore = self.exploration * math.sqrt(clipped_division(safe_log(total_visits, fallback=0.0), node.visits, fallback=0.0))
            return mean + explore

        return max(nodes.values(), key=uct)

    def _simulate_reward(self, base_utility: float) -> float:
        noise = random.uniform(-0.02, 0.02)
        return max(0.0, min(1.0, base_utility + noise))


class AdvancedArenaLayer:
    """
    BT + log-space Bayesian arena with deterministic dominance.
    """

    def __init__(self, similarity_engine=None):
        self.bt_engine = BradleyTerryEngine()
        self.pairwise_engine = PairwiseEngine(self.bt_engine, similarity_engine=similarity_engine)
        self.aggregator = BayesianAggregator()

    def run(
        self,
        responses: List[Dict],
        entropy: float | None = None,
        iterations_override: int | None = None,
    ) -> Dict:
        entropy = float(entropy if entropy is not None else compute_entropy(responses))

        thetas, matches, pairwise_details = self.pairwise_engine.run(
            responses=responses,
            entropy=entropy,
            return_details=True,
            iterations_override=iterations_override,
        )

        aggregation = self.aggregator.compute(
            responses=responses,
            thetas=thetas,
            uncertainties=pairwise_details.get("uncertainties", {}),
            entropy=entropy,
            return_details=True,
        )

        posteriors = aggregation["posteriors"]
        winner = max(posteriors, key=posteriors.get) if posteriors else ""

        return {
            "entropy": entropy,
            "thetas": thetas,
            "matches": matches,
            "pairwise": pairwise_details,
            "posteriors": posteriors,
            "winner": winner,
            "winner_margin": aggregation.get("winner_margin", 0.0),
            "arena_confidence": aggregation.get("confidence", 0.0),
            "bayesian": aggregation,
        }
