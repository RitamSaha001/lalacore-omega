from __future__ import annotations

from typing import Dict

from core.lalacore_x.calibration import ConfidenceCalibrator


class ConfidenceModel:
    """
    Backward-compatible adapter over LalaCore X calibrator.
    """

    def __init__(self):
        self._calibrator = ConfidenceCalibrator()

    def predict_risk(self, features: Dict[str, float]) -> float:
        return self._calibrator.predict_risk(features)

    def fit(self, rows):
        self._calibrator.fit_from_rows(rows)


DEFAULT_CONFIDENCE_MODEL = ConfidenceModel()
