"""Promotion gating policy for Mini checkpoint eligibility."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Mapping


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


@dataclass(slots=True)
class PromotionThresholds:
    """Thresholds required before Mini can be considered for runtime eligibility gates."""

    validation_accuracy: float = 0.72
    hard_case_accuracy: float = 0.58
    calibration_brier_max: float = 0.18
    overconfidence_rate_max: float = 0.22
    concept_cluster_coverage: float = 0.60
    hard_case_regression_tolerance: float = 0.01
    rare_concept_regression_tolerance: float = 0.02
    calibration_drift_max: float = 0.03
    ece_drift_max: float = 0.015
    disagreement_drop_tolerance: float = 0.01
    overconfidence_increase_tolerance: float = 0.0
    self_disagreement_max: float = 0.35
    stability_variance_max: float = 0.12
    traffic_regression_risk_max: float = 0.35
    stress_resilience_min: float = 0.55
    promotion_risk_factor_max: float = 0.45


class MiniPromotionPolicy:
    """Evaluates checkpoint quality without auto-replacing incumbent Mini."""

    def __init__(self, thresholds: PromotionThresholds | None = None) -> None:
        self.thresholds = thresholds or PromotionThresholds()

    def evaluate(
        self,
        metrics: Mapping[str, Any],
        *,
        candidate_model_id: str = "mini_candidate",
        incumbent_model_id: str = "mini_current",
        baseline_metrics: Mapping[str, Any] | None = None,
        traffic_simulation: Mapping[str, Any] | None = None,
        self_disagreement: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Return additive promotion decision metadata and explicit block reasons."""
        exact_match = _clamp(float(metrics.get("exact_match", 0.0)))
        hard_case_accuracy = _clamp(float(metrics.get("hard_case_accuracy", 0.0)))
        rare_concept_accuracy = _clamp(float(metrics.get("rare_concept_accuracy", 0.0)))
        brier = _clamp(float(metrics.get("calibration_brier", 1.0)))
        ece = _clamp(float(metrics.get("ece", metrics.get("adaptive_ece", 1.0))))
        disagreement_accuracy = _clamp(float(metrics.get("disagreement_slice_accuracy", 0.0)))
        overconfidence = _clamp(float(metrics.get("overconfidence_rate", 1.0)))
        coverage = self._concept_coverage(metrics.get("concept_cluster_accuracy", {}))

        blocked_reasons = []
        if exact_match < float(self.thresholds.validation_accuracy):
            blocked_reasons.append(
                f"validation_accuracy_below_threshold:{exact_match:.4f}<{self.thresholds.validation_accuracy:.4f}"
            )
        if hard_case_accuracy < float(self.thresholds.hard_case_accuracy):
            blocked_reasons.append(
                f"hard_case_accuracy_below_threshold:{hard_case_accuracy:.4f}<{self.thresholds.hard_case_accuracy:.4f}"
            )
        if brier > float(self.thresholds.calibration_brier_max):
            blocked_reasons.append(
                f"calibration_brier_above_threshold:{brier:.4f}>{self.thresholds.calibration_brier_max:.4f}"
            )
        if overconfidence > float(self.thresholds.overconfidence_rate_max):
            blocked_reasons.append(
                f"overconfidence_rate_above_threshold:{overconfidence:.4f}>{self.thresholds.overconfidence_rate_max:.4f}"
            )
        if coverage < float(self.thresholds.concept_cluster_coverage):
            blocked_reasons.append(
                f"concept_coverage_below_threshold:{coverage:.4f}<{self.thresholds.concept_cluster_coverage:.4f}"
            )

        hard_case_regression = 0.0
        rare_concept_regression = 0.0
        calibration_drift = 0.0
        ece_drift = 0.0
        disagreement_drop = 0.0
        overconfidence_increase = 0.0
        if isinstance(baseline_metrics, Mapping):
            baseline_hard = _clamp(float(baseline_metrics.get("hard_case_accuracy", 0.0)))
            baseline_rare = _clamp(float(baseline_metrics.get("rare_concept_accuracy", 0.0)))
            baseline_brier = _clamp(float(baseline_metrics.get("calibration_brier", 1.0)))
            baseline_ece = _clamp(float(baseline_metrics.get("ece", baseline_metrics.get("adaptive_ece", 1.0))))
            baseline_disagreement = _clamp(float(baseline_metrics.get("disagreement_slice_accuracy", 0.0)))
            baseline_overconfidence = _clamp(float(baseline_metrics.get("overconfidence_rate", 1.0)))
            hard_case_regression = max(0.0, baseline_hard - hard_case_accuracy)
            rare_concept_regression = max(0.0, baseline_rare - rare_concept_accuracy)
            calibration_drift = max(0.0, brier - baseline_brier)
            ece_drift = max(0.0, ece - baseline_ece)
            disagreement_drop = max(0.0, baseline_disagreement - disagreement_accuracy)
            overconfidence_increase = max(0.0, overconfidence - baseline_overconfidence)
            if hard_case_regression > float(self.thresholds.hard_case_regression_tolerance):
                blocked_reasons.append(f"hard_case_regression:{hard_case_regression:.4f}")
            if rare_concept_regression > float(self.thresholds.rare_concept_regression_tolerance):
                blocked_reasons.append(f"rare_concept_regression:{rare_concept_regression:.4f}")
            if calibration_drift > float(self.thresholds.calibration_drift_max):
                blocked_reasons.append(f"calibration_drift:{calibration_drift:.4f}")
            if ece_drift > float(self.thresholds.ece_drift_max):
                blocked_reasons.append(f"ece_drift:{ece_drift:.4f}")
            if disagreement_drop > float(self.thresholds.disagreement_drop_tolerance):
                blocked_reasons.append(f"disagreement_performance_drop:{disagreement_drop:.4f}")
            if overconfidence_increase > float(self.thresholds.overconfidence_increase_tolerance):
                blocked_reasons.append(f"overconfidence_rate_increased:{overconfidence_increase:.4f}")

        self_disagreement_rate = 0.0
        stability_variance = 0.0
        if isinstance(self_disagreement, Mapping):
            self_disagreement_rate = _clamp(float(self_disagreement.get("self_disagreement_rate", 0.0)))
            answer_variance = _clamp(float(self_disagreement.get("answer_variance", 0.0)))
            confidence_variance = _clamp(float(self_disagreement.get("confidence_variance", 0.0)))
            stability_variance = max(answer_variance, confidence_variance)
            if self_disagreement_rate > float(self.thresholds.self_disagreement_max):
                blocked_reasons.append(f"self_disagreement_too_high:{self_disagreement_rate:.4f}")
            if stability_variance > float(self.thresholds.stability_variance_max):
                blocked_reasons.append(f"stability_variance_too_high:{stability_variance:.4f}")

        traffic_risk = 0.0
        traffic_safe = True
        stress_resilience_score = 1.0
        promotion_risk_factor = 0.0
        if isinstance(traffic_simulation, Mapping):
            traffic_risk = _clamp(float(traffic_simulation.get("regression_risk_score", 0.0)))
            traffic_safe = bool(traffic_simulation.get("safe_to_promote", True))
            stress_resilience_score = _clamp(float(traffic_simulation.get("stress_resilience_score", 1.0)))
            promotion_risk_factor = _clamp(float(traffic_simulation.get("promotion_risk_factor", traffic_risk)))
            if not traffic_safe:
                blocked_reasons.append("traffic_simulation_unsafe")
            if traffic_risk > float(self.thresholds.traffic_regression_risk_max):
                blocked_reasons.append(f"traffic_regression_risk_high:{traffic_risk:.4f}")
            if stress_resilience_score < float(self.thresholds.stress_resilience_min):
                blocked_reasons.append(f"stress_resilience_low:{stress_resilience_score:.4f}")
            if promotion_risk_factor > float(self.thresholds.promotion_risk_factor_max):
                blocked_reasons.append(f"promotion_risk_factor_high:{promotion_risk_factor:.4f}")

        eligible = len(blocked_reasons) == 0
        production_risk = _clamp(
            0.30 * brier
            + 0.25 * overconfidence
            + 0.20 * hard_case_regression
            + 0.10 * rare_concept_regression
            + 0.10 * traffic_risk
            + 0.05 * self_disagreement_rate
            + 0.10 * ece_drift
            + 0.10 * disagreement_drop
            + 0.10 * overconfidence_increase
            + 0.10 * (1.0 - stress_resilience_score)
            + 0.10 * stability_variance
        )
        kaggle_projection = _clamp(
            0.45 * exact_match
            + 0.30 * hard_case_accuracy
            + 0.10 * rare_concept_accuracy
            + 0.10 * (1.0 - overconfidence)
            + 0.05 * coverage
        )

        return {
            "eligible": bool(eligible),
            "candidate_model_id": str(candidate_model_id),
            "incumbent_model_id": str(incumbent_model_id),
            "auto_replace_allowed": False,
            "fallback_model_id": str(incumbent_model_id),
            "next_action": "pass_to_runtime_eligibility_gating" if eligible else "continue_shadow_training",
            "blocked_reasons": blocked_reasons,
            "promote": bool(eligible),
            "block_reasons": blocked_reasons,
            "risk_score": float(production_risk),
            "rollback_required": bool(not eligible),
            "kaggle_projection": float(kaggle_projection),
            "kaggle_score_projection": float(kaggle_projection),
            "production_risk_score": float(production_risk),
            "thresholds": asdict(self.thresholds),
            "observed": {
                "validation_accuracy": exact_match,
                "hard_case_accuracy": hard_case_accuracy,
                "rare_concept_accuracy": rare_concept_accuracy,
                "calibration_brier": brier,
                "overconfidence_rate": overconfidence,
                "concept_cluster_coverage": coverage,
                "hard_case_regression": hard_case_regression,
                "rare_concept_regression": rare_concept_regression,
                "calibration_drift": calibration_drift,
                "ece_drift": ece_drift,
                "disagreement_drop": disagreement_drop,
                "overconfidence_increase": overconfidence_increase,
                "self_disagreement_rate": self_disagreement_rate,
                "stability_variance": stability_variance,
                "traffic_regression_risk": traffic_risk,
                "traffic_safe_to_promote": bool(traffic_safe),
                "stress_resilience_score": stress_resilience_score,
                "promotion_risk_factor": promotion_risk_factor,
            },
        }

    def _concept_coverage(self, concept_cluster_accuracy: Any) -> float:
        if not isinstance(concept_cluster_accuracy, Mapping) or not concept_cluster_accuracy:
            return 0.0
        total = 0
        covered = 0
        for _, value in concept_cluster_accuracy.items():
            total += 1
            if float(value) > 0.0:
                covered += 1
        if total <= 0:
            return 0.0
        return float(covered / total)
