from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Dict


class BayesianConfidenceAdjuster:
    """
    Bayesian-style confidence adjustment from multiple uncertainty signals.
    """

    def __init__(self, *, adaptive_state_path: str = "data/metrics/bayesian_adaptive_state.json") -> None:
        self.weights = {
            "agreement": 1.25,
            "entropy": -1.10,
            "deterministic": 1.55,
            "calibration_ema": -0.90,
            "answer_type_match": 0.85,
            "winner_margin": 0.55,
        }
        self.state_path = Path(adaptive_state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.adaptive_state = self._load_state()

    def adjust(
        self,
        *,
        prior_confidence: float,
        agreement: float,
        entropy: float,
        deterministic_verified: bool,
        calibration_ema: float,
        answer_type_match: bool,
        winner_margin: float,
    ) -> Dict[str, float]:
        prior = self._clamp(prior_confidence)
        odds = prior / max(1e-9, 1.0 - prior)

        features = {
            "agreement": self._clamp(agreement),
            "entropy": self._clamp(entropy),
            "deterministic": 1.0 if deterministic_verified else 0.0,
            "calibration_ema": self._clamp(calibration_ema),
            "answer_type_match": 1.0 if answer_type_match else 0.0,
            "winner_margin": self._clamp(winner_margin),
        }

        log_lr = 0.0
        for key, value in features.items():
            centered = 2.0 * value - 1.0  # [-1, 1]
            effective_weight = self._effective_weight(key)
            log_lr += effective_weight * centered

        adjusted_odds = odds * math.exp(log_lr)
        posterior = adjusted_odds / (1.0 + adjusted_odds)
        posterior = self._clamp(posterior)

        return {
            "prior_confidence": prior,
            "posterior_confidence": posterior,
            "posterior_risk": float(1.0 - posterior),
            "evidence_log_lr": float(log_lr),
            "agreement": features["agreement"],
            "entropy": features["entropy"],
            "deterministic": features["deterministic"],
            "calibration_ema": features["calibration_ema"],
            "answer_type_match": features["answer_type_match"],
            "winner_margin": features["winner_margin"],
            "adaptive_ema_error": float(self.adaptive_state.get("ema_error", 0.0)),
        }

    def update_adaptive(
        self,
        *,
        features: Dict[str, float],
        predicted_confidence: float,
        observed_success: bool,
        alpha: float = 0.02,
    ) -> Dict[str, float]:
        alpha = float(max(0.001, min(0.2, alpha)))
        predicted = self._clamp(predicted_confidence)
        observed = 1.0 if bool(observed_success) else 0.0
        error = predicted - observed

        prev_ema = float(self.adaptive_state.get("ema_error", 0.0))
        ema_error = (1.0 - alpha) * prev_ema + alpha * abs(error)
        self.adaptive_state["ema_error"] = float(self._clamp(ema_error))
        self.adaptive_state["count"] = int(self.adaptive_state.get("count", 0)) + 1

        offsets = self.adaptive_state.setdefault("offsets", {})
        for key, value in features.items():
            if key not in self.weights:
                continue
            centered = 2.0 * self._clamp(value) - 1.0
            prev = float(offsets.get(key, 0.0))
            # Slow adaptation layer on top of fixed research priors.
            delta = -alpha * 0.06 * error * centered
            offsets[key] = float(max(-0.35, min(0.35, prev + delta)))

        self._save_state()
        return {
            "ema_error": float(self.adaptive_state.get("ema_error", 0.0)),
            "count": float(self.adaptive_state.get("count", 0)),
            "offsets": {k: float(v) for k, v in offsets.items()},
        }

    def _clamp(self, value: float) -> float:
        return float(max(0.0, min(1.0, value)))

    def _effective_weight(self, key: str) -> float:
        base = float(self.weights.get(key, 0.0))
        offsets = self.adaptive_state.get("offsets", {})
        return base + float(offsets.get(key, 0.0))

    def _load_state(self) -> Dict:
        default = {"offsets": {}, "ema_error": 0.0, "count": 0}
        if not self.state_path.exists():
            return default
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return {
                    "offsets": dict(payload.get("offsets", {})),
                    "ema_error": float(payload.get("ema_error", 0.0)),
                    "count": int(payload.get("count", 0)),
                }
        except Exception:
            return default
        return default

    def _save_state(self) -> None:
        payload = {
            "offsets": {k: float(v) for k, v in self.adaptive_state.get("offsets", {}).items()},
            "ema_error": float(self.adaptive_state.get("ema_error", 0.0)),
            "count": int(self.adaptive_state.get("count", 0)),
        }
        self.state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
