"""Dynamic curriculum scheduler for staged offline Mini training."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


@dataclass(slots=True)
class CurriculumThresholds:
    """Thresholds controlling curriculum stage progression."""

    min_hard_case_accuracy: float = 0.55
    max_calibration_brier: float = 0.22
    max_overconfidence_rate: float = 0.28
    max_ece_regression: float = 0.01


@dataclass(slots=True)
class CurriculumConfig:
    """Configuration for curriculum weighting and diagnostics."""

    thresholds: CurriculumThresholds = field(default_factory=CurriculumThresholds)
    replay_frequency_gain: float = 0.08
    entropy_gain: float = 0.35
    concept_priority_gain: float = 0.20
    output_filename: str = "curriculum_diagnostics.json"
    progression_output_filename: str = "curriculum_progression_diagnostics.json"


class CurriculumScheduler:
    """Builds ordered training batches across progressive hard-case stages."""

    def __init__(self, *, output_dir: str = "data/mini_training/curriculum") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def schedule(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        metrics_history: Sequence[Mapping[str, Any]] | None = None,
        replay_frequency: Mapping[str, int] | None = None,
        concept_priority: Mapping[str, float] | None = None,
        config: CurriculumConfig | None = None,
    ) -> Dict[str, Any]:
        cfg = config or CurriculumConfig()
        history = list(metrics_history or [])
        replay_frequency = dict(replay_frequency or {})
        concept_priority = dict(concept_priority or {})

        stages = [
            ("stage_1_low_entropy_verified_easy", self._is_stage1),
            ("stage_2_medium_entropy", self._is_stage2),
            ("stage_3_disagreement_focus", self._is_stage3),
            ("stage_4_hard_negatives", self._is_stage4),
            ("stage_5_high_concept_energy_failures", self._is_stage5),
        ]

        stage_transition_log: List[Dict[str, Any]] = []
        ordered_batches: List[Dict[str, Any]] = []
        progression_diagnostics: List[Dict[str, Any]] = []

        unlocked = True
        for idx, (stage_name, predicate) in enumerate(stages):
            source_rows = [dict(row) for row in rows if predicate(row)]
            weighted_rows = [
                self._with_curriculum_weight(
                    row,
                    replay_frequency=replay_frequency,
                    concept_priority=concept_priority,
                    cfg=cfg,
                )
                for row in source_rows
            ]
            weighted_rows.sort(key=lambda item: float(item.get("curriculum_weight", 0.0)), reverse=True)

            gate: Dict[str, Any] = {"passed": True, "reasons": [], "metrics": {}}
            if idx > 0:
                unlocked, gate = self._can_unlock_next_stage(history, cfg.thresholds)
            stage_transition_log.append(
                {
                    "stage": stage_name,
                    "unlocked": bool(unlocked),
                    "candidate_rows": len(weighted_rows),
                    "thresholds": asdict(cfg.thresholds),
                    "gate": gate,
                }
            )
            progression_diagnostics.append(
                {
                    "stage": stage_name,
                    "unlocked": bool(unlocked),
                    "gate": gate,
                    "candidate_rows": int(len(weighted_rows)),
                }
            )
            if not unlocked:
                break

            ordered_batches.append(
                {
                    "stage": stage_name,
                    "rows": weighted_rows,
                    "size": len(weighted_rows),
                }
            )

        diagnostics = {
            "ordered_training_batches": ordered_batches,
            "stage_transition_log": stage_transition_log,
            "metrics_history_used": list(history),
            "config": {
                "thresholds": asdict(cfg.thresholds),
                "replay_frequency_gain": float(cfg.replay_frequency_gain),
                "entropy_gain": float(cfg.entropy_gain),
                "concept_priority_gain": float(cfg.concept_priority_gain),
            },
            "curriculum_progression_diagnostics": progression_diagnostics,
        }
        path = self.output_dir / str(cfg.output_filename)
        progression_path = self.output_dir / str(cfg.progression_output_filename)
        path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
        progression_path.write_text(json.dumps(progression_diagnostics, indent=2, sort_keys=True), encoding="utf-8")
        diagnostics["diagnostics_path"] = str(path)
        diagnostics["curriculum_progression_diagnostics_path"] = str(progression_path)
        return diagnostics

    def _can_unlock_next_stage(
        self,
        metrics_history: Sequence[Mapping[str, Any]],
        thresholds: CurriculumThresholds,
    ) -> tuple[bool, Dict[str, Any]]:
        reasons: List[str] = []
        if not metrics_history:
            return True, {"passed": True, "reasons": [], "metrics": {}}
        latest = metrics_history[-1]
        hard_acc = _clamp(float(latest.get("hard_case_accuracy", 0.0)))
        brier = _clamp(float(latest.get("calibration_brier", 1.0)))
        overconf = _clamp(float(latest.get("overconfidence_rate", 1.0)))
        rare_acc = _clamp(float(latest.get("rare_concept_accuracy", 0.0)))
        ece = _clamp(float(latest.get("ece", latest.get("adaptive_ece", 1.0))))
        bootstrap_lb = self._hard_case_bootstrap_lower_bound(latest)
        if not (
            hard_acc >= float(thresholds.min_hard_case_accuracy)
            and brier <= float(thresholds.max_calibration_brier)
            and overconf <= float(thresholds.max_overconfidence_rate)
        ):
            reasons.append("threshold_gate_failed")

        if len(metrics_history) < 2:
            passed = len(reasons) == 0
            return passed, {
                "passed": bool(passed),
                "reasons": reasons,
                "metrics": {
                    "hard_case_accuracy": float(hard_acc),
                    "rare_concept_accuracy": float(rare_acc),
                    "ece": float(ece),
                    "hard_case_bootstrap_lower_bound": float(bootstrap_lb),
                },
            }

        prev = metrics_history[-2]
        prev_hard = _clamp(float(prev.get("hard_case_accuracy", 0.0)))
        prev_rare = _clamp(float(prev.get("rare_concept_accuracy", 0.0)))
        prev_ece = _clamp(float(prev.get("ece", prev.get("adaptive_ece", 1.0))))
        prev_lb = self._hard_case_bootstrap_lower_bound(prev)
        # Strict progression guard for hard-case replay curriculum.
        if hard_acc < prev_hard:
            reasons.append("hard_case_accuracy_regressed")
        if rare_acc < prev_rare:
            reasons.append("rare_concept_accuracy_regressed")
        if (ece - prev_ece) > float(thresholds.max_ece_regression):
            reasons.append("ece_regressed")
        if bootstrap_lb < prev_lb:
            reasons.append("bootstrap_ci_lower_bound_not_improved")

        passed = len(reasons) == 0
        return passed, {
            "passed": bool(passed),
            "reasons": reasons,
            "metrics": {
                "hard_case_accuracy": float(hard_acc),
                "prev_hard_case_accuracy": float(prev_hard),
                "rare_concept_accuracy": float(rare_acc),
                "prev_rare_concept_accuracy": float(prev_rare),
                "ece": float(ece),
                "prev_ece": float(prev_ece),
                "hard_case_bootstrap_lower_bound": float(bootstrap_lb),
                "prev_hard_case_bootstrap_lower_bound": float(prev_lb),
                "calibration_brier": float(brier),
                "overconfidence_rate": float(overconf),
            },
        }

    def _with_curriculum_weight(
        self,
        row: Dict[str, Any],
        *,
        replay_frequency: Mapping[str, int],
        concept_priority: Mapping[str, float],
        cfg: CurriculumConfig,
    ) -> Dict[str, Any]:
        question = str(row.get("question", "")).strip().lower()
        entropy = _clamp(float(row.get("entropy", 0.0)))
        replay_count = int(replay_frequency.get(question, 0))
        replay_weight = 1.0 + float(cfg.replay_frequency_gain) * max(0, replay_count)
        entropy_weight = 1.0 + float(cfg.entropy_gain) * entropy

        clusters = row.get("concept_cluster", [])
        if isinstance(clusters, list) and clusters:
            concept_values = [float(concept_priority.get(str(cluster).strip().lower(), 1.0)) for cluster in clusters]
            concept_score = sum(concept_values) / len(concept_values)
        else:
            concept_score = 1.0
        concept_weight = 1.0 + float(cfg.concept_priority_gain) * max(0.0, concept_score - 1.0)
        weight = replay_weight * entropy_weight * concept_weight

        out = dict(row)
        out["curriculum_weight"] = float(weight)
        out["replay_weight"] = float(replay_weight)
        out["entropy_weight"] = float(entropy_weight)
        out["concept_weight"] = float(concept_weight)
        return out

    def _is_stage1(self, row: Mapping[str, Any]) -> bool:
        entropy = _clamp(float(row.get("entropy", 0.0)))
        verified = bool(row.get("verified", False))
        difficulty = str(row.get("difficulty", "unknown")).lower().strip()
        return entropy <= 0.25 and verified and difficulty in {"easy", "unknown"}

    def _is_stage2(self, row: Mapping[str, Any]) -> bool:
        entropy = _clamp(float(row.get("entropy", 0.0)))
        return 0.25 < entropy <= 0.55

    def _is_stage3(self, row: Mapping[str, Any]) -> bool:
        entropy = _clamp(float(row.get("entropy", 0.0)))
        disagreement = _clamp(float(row.get("disagreement", 0.0)))
        return entropy > 0.55 or disagreement > 0.0

    def _is_stage4(self, row: Mapping[str, Any]) -> bool:
        return bool(row.get("hard_negative", False)) or (not bool(row.get("verified", False))) or float(row.get("risk", 0.0)) >= 0.70

    def _is_stage5(self, row: Mapping[str, Any]) -> bool:
        clusters = row.get("concept_cluster", [])
        cluster_count = len(clusters) if isinstance(clusters, list) else 0
        text = str(row.get("question", "")).lower()
        trap = "trap" in text or "none of the above" in text or any(str(c).lower() == "trap" for c in clusters if isinstance(clusters, list))
        concept_energy = _clamp(float(row.get("concept_energy", 0.0)))
        return (cluster_count >= 2 and trap) or concept_energy >= 0.75

    def _hard_case_bootstrap_lower_bound(self, metrics: Mapping[str, Any]) -> float:
        bootstrap = metrics.get("bootstrap_ci", {})
        if isinstance(bootstrap, Mapping):
            ci = bootstrap.get("hard_case_accuracy_ci95", [])
            if isinstance(ci, Sequence) and len(ci) >= 1:
                try:
                    return float(ci[0])
                except Exception:
                    return 0.0
        return 0.0
