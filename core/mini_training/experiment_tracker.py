"""Local experiment tracker for offline reproducible Mini training research."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MiniExperimentTracker:
    """Records hashes, metrics, and stage transitions without external services."""

    def __init__(self, *, output_dir: str = "data/mini_training/experiments") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def dataset_checksum(self, rows: Sequence[Mapping[str, Any]]) -> str:
        normalized = [self._canonical_json(dict(row)) for row in rows]
        payload = "\n".join(sorted(normalized))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def config_checksum(self, config: Mapping[str, Any]) -> str:
        return hashlib.sha256(self._canonical_json(dict(config)).encode("utf-8")).hexdigest()

    def write_experiment(
        self,
        *,
        experiment_name: str,
        hyperparameters: Mapping[str, Any],
        dataset_rows: Sequence[Mapping[str, Any]],
        curriculum_transitions: Sequence[Mapping[str, Any]] | None = None,
        evaluation_metrics: Mapping[str, Any] | None = None,
        replay_distribution: Mapping[str, Any] | None = None,
        calibration_state: Mapping[str, Any] | None = None,
        shadow_results: Mapping[str, Any] | None = None,
        promotion_decision: Mapping[str, Any] | None = None,
    ) -> Dict[str, str]:
        dataset_hash = self.dataset_checksum(dataset_rows)
        config_hash = self.config_checksum(hyperparameters)

        summary = {
            "ts": _utc_now(),
            "experiment_name": str(experiment_name),
            "dataset_hash": dataset_hash,
            "config_hash": config_hash,
            "curriculum_transitions": list(curriculum_transitions or []),
            "evaluation_metrics": dict(evaluation_metrics or {}),
            "replay_distribution": dict(replay_distribution or {}),
            "calibration_state": dict(calibration_state or {}),
            "shadow_results": dict(shadow_results or {}),
            "promotion_decision": dict(promotion_decision or {}),
        }
        manifest = {
            "ts": _utc_now(),
            "experiment_name": str(experiment_name),
            "dataset_hash": dataset_hash,
            "config_hash": config_hash,
            "row_count": int(len(dataset_rows)),
            "hyperparameters": dict(hyperparameters),
        }
        reproducibility_hash = hashlib.sha256(
            (dataset_hash + ":" + config_hash + ":" + str(experiment_name)).encode("utf-8")
        ).hexdigest()

        summary_path = self.output_dir / "experiment_summary.json"
        manifest_path = self.output_dir / "experiment_manifest.json"
        reproducibility_manifest_path = self.output_dir / "reproducibility_manifest.json"
        hash_path = self.output_dir / "reproducibility_hash.txt"

        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        reproducibility_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        hash_path.write_text(reproducibility_hash + "\n", encoding="utf-8")

        return {
            "experiment_summary": str(summary_path),
            "experiment_manifest": str(manifest_path),
            "reproducibility_manifest": str(reproducibility_manifest_path),
            "reproducibility_hash": str(hash_path),
        }

    def _canonical_json(self, payload: Mapping[str, Any]) -> str:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
