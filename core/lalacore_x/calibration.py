from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List

from core.safe_math import safe_sigmoid


class ConfidenceCalibrator:
    """
    Lightweight logistic-risk calibrator.

    Predicts P(answer_is_wrong) from structured features.
    """

    DEFAULT_WEIGHTS = {
        "bias": -0.2,
        "verification_fail": 1.6,
        "disagreement": 1.2,
        "retrieval_strength": -0.8,
        "critic_score": -1.0,
        "provider_reliability": -1.1,
        "trap_probability": 1.0,
        "entropy": 0.6,
        "bt_margin": -0.9,
        "disagreement_cluster_size": 0.45,
        "deterministic_dominance": -0.7,
        "uncertainty": 0.9,
        "structural_coherence": -0.65,
        "process_reward": -0.50,
        "graph_missing_inference": 0.45,
        "graph_redundancy": 0.30,
    }

    def __init__(self, path: str = "data/metrics/calibration_model.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.weights = self._load()

    def _load(self) -> Dict[str, float]:
        if not self.path.exists():
            return dict(self.DEFAULT_WEIGHTS)
        try:
            with self.path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            return {**self.DEFAULT_WEIGHTS, **{k: float(v) for k, v in payload.items()}}
        except Exception:
            return dict(self.DEFAULT_WEIGHTS)

    def _save(self) -> None:
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(self.weights, f, indent=2, sort_keys=True)

    def predict_risk(self, features: Dict[str, float]) -> float:
        z = self.weights.get("bias", 0.0)
        for key, value in features.items():
            z += self.weights.get(key, 0.0) * float(value)
        return safe_sigmoid(z)

    def fit_from_rows(self, rows: Iterable[Dict], epochs: int = 10, lr: float = 0.05) -> None:
        """
        Each row must include feature keys + "target_wrong" in {0,1}.
        """
        rows = list(rows)
        if not rows:
            return

        keys = sorted({k for row in rows for k in row.keys() if k != "target_wrong"})
        for key in keys:
            self.weights.setdefault(key, 0.0)

        for _ in range(epochs):
            for row in rows:
                target = float(row.get("target_wrong", 0.0))
                features = {k: float(row.get(k, 0.0)) for k in keys}

                pred = self.predict_risk(features)
                error = pred - target

                self.weights["bias"] -= lr * error
                for key, value in features.items():
                    self.weights[key] -= lr * error * value

        self._save()
