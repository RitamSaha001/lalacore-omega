"""Kaggle hard-case specialist curriculum built on top of the generic scheduler."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from core.mini_training.curriculum_scheduler import CurriculumConfig, CurriculumScheduler, CurriculumThresholds


class KaggleHardCaseCurriculum:
    """Builds a hard-case-first curriculum while preserving offline-only constraints."""

    def __init__(self, *, output_dir: str = "data/mini_training/kaggle_curriculum") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.scheduler = CurriculumScheduler(output_dir=str(self.output_dir))

    def build(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        metrics_history: Sequence[Mapping[str, Any]] | None = None,
        replay_frequency: Mapping[str, int] | None = None,
        concept_priority: Mapping[str, float] | None = None,
    ) -> Dict[str, Any]:
        """Build stage-ordered rows and export Kaggle-specific diagnostics."""
        cfg = CurriculumConfig(
            thresholds=CurriculumThresholds(
                min_hard_case_accuracy=0.52,
                max_calibration_brier=0.25,
                max_overconfidence_rate=0.30,
            ),
            output_filename="curriculum_diagnostics.json",
        )
        scheduled = self.scheduler.schedule(
            rows,
            metrics_history=metrics_history,
            replay_frequency=replay_frequency,
            concept_priority=concept_priority,
            config=cfg,
        )

        stage_metrics: List[Dict[str, Any]] = []
        failure_cluster_evolution: Dict[str, int] = {}
        for batch in scheduled.get("ordered_training_batches", []):
            batch_rows = list(batch.get("rows", []))
            size = len(batch_rows)
            if size <= 0:
                continue
            hard = sum(1 for row in batch_rows if (not bool(row.get("verified", False))) or float(row.get("risk", 0.0)) >= 0.65)
            avg_entropy = sum(float(row.get("entropy", 0.0)) for row in batch_rows) / size
            stage_metrics.append(
                {
                    "stage": str(batch.get("stage", "")),
                    "size": int(size),
                    "hard_ratio": float(hard / size),
                    "avg_entropy": float(avg_entropy),
                }
            )

            for row in batch_rows:
                clusters = row.get("concept_cluster", [])
                if not isinstance(clusters, list):
                    continue
                for cluster in clusters:
                    key = str(cluster).strip().lower()
                    if not key:
                        continue
                    failure_cluster_evolution[key] = failure_cluster_evolution.get(key, 0) + int(not bool(row.get("verified", False)))

        progression_path = self.output_dir / "curriculum_progression_log.json"
        stage_perf_path = self.output_dir / "stage_performance_metrics.json"
        cluster_path = self.output_dir / "failure_cluster_evolution.json"
        progression_path.write_text(
            json.dumps(scheduled.get("stage_transition_log", []), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        stage_perf_path.write_text(json.dumps(stage_metrics, indent=2, sort_keys=True), encoding="utf-8")
        cluster_path.write_text(json.dumps(failure_cluster_evolution, indent=2, sort_keys=True), encoding="utf-8")

        return {
            **scheduled,
            "curriculum_progression_log": str(progression_path),
            "stage_performance_metrics": str(stage_perf_path),
            "failure_cluster_evolution": str(cluster_path),
        }
