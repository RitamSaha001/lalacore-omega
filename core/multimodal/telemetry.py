from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict



def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MultimodalTelemetry:
    """
    Additive LC9-compatible telemetry for multimodal preprocessing and vision analysis.
    """

    def __init__(
        self,
        *,
        debug_path: str = "data/lc9/LC9_MULTIMODAL_DEBUG.jsonl",
        drift_path: str = "data/metrics/calibration_drift.json",
        failure_cluster_path: str = "data/metrics/multimodal_failure_clusters.json",
    ) -> None:
        self.debug_path = Path(debug_path)
        self.debug_path.parent.mkdir(parents=True, exist_ok=True)

        self.drift_path = Path(drift_path)
        self.drift_path.parent.mkdir(parents=True, exist_ok=True)

        self.failure_cluster_path = Path(failure_cluster_path)
        self.failure_cluster_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def log_event(self, event_type: str, payload: Dict[str, Any] | None = None) -> None:
        row = {
            "ts": _utc_now(),
            "event_type": str(event_type),
            **(payload or {}),
        }
        with self._lock:
            with self.debug_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def log_ocr_metrics(
        self,
        *,
        source: str,
        confidence: float,
        block_count: int,
        math_normalized: bool,
    ) -> None:
        self.log_event(
            "ocr_metrics",
            {
                "source": str(source),
                "confidence": float(max(0.0, min(1.0, confidence))),
                "block_count": int(max(0, block_count)),
                "math_normalized": bool(math_normalized),
            },
        )

    def log_vision_metrics(
        self,
        *,
        provider: str,
        detection_confidence: float,
        diagram_detected: bool,
        geometry_count: int,
    ) -> None:
        self.log_event(
            "vision_metrics",
            {
                "provider": str(provider),
                "detection_confidence": float(max(0.0, min(1.0, detection_confidence))),
                "diagram_detected": bool(diagram_detected),
                "geometry_count": int(max(0, geometry_count)),
            },
        )

    def log_provider_comparison(self, comparison: Dict[str, Any]) -> None:
        self.log_event("vision_provider_comparison", comparison)

    def log_timing(self, *, stage: str, duration_s: float, slow_threshold_s: float | None = None, extra: Dict[str, Any] | None = None) -> None:
        duration = float(max(0.0, duration_s))
        payload = {"stage": str(stage), "duration_s": duration, **(extra or {})}
        self.log_event("latency_timing", payload)
        if slow_threshold_s is not None and duration >= float(slow_threshold_s):
            self.log_event(
                "slow_path_warning",
                {
                    "stage": str(stage),
                    "duration_s": duration,
                    "slow_threshold_s": float(slow_threshold_s),
                    **(extra or {}),
                },
            )

    def update_calibration_drift(self, *, expected: float, observed: float, alpha: float = 0.15) -> Dict[str, Any]:
        alpha = float(max(0.01, min(0.95, alpha)))
        with self._lock:
            state = self._load_json(self.drift_path, default={})
            prev_ema = float(state.get("ema_abs_error", 0.0))
            abs_err = abs(float(expected) - float(observed))
            ema_abs_error = (1.0 - alpha) * prev_ema + alpha * abs_err

            count = int(state.get("count", 0)) + 1
            out = {
                "updated_at": _utc_now(),
                "count": count,
                "ema_abs_error": float(max(0.0, min(1.0, ema_abs_error))),
                "last_expected": float(max(0.0, min(1.0, expected))),
                "last_observed": float(max(0.0, min(1.0, observed))),
            }
            self.drift_path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
        self.log_event("calibration_drift", out)
        return out

    def cluster_failure(self, *, failure_type: str, modality: str, profile: Dict[str, Any] | None = None) -> Dict[str, Any]:
        with self._lock:
            payload = self._load_json(self.failure_cluster_path, default={"clusters": {}})
            clusters = payload.setdefault("clusters", {})
            profile = profile or {}
            subject = str(profile.get("subject", "general")).lower()
            difficulty = str(profile.get("difficulty", "unknown")).lower()
            key = f"{str(modality).lower()}|{str(failure_type).lower()}|{subject}|{difficulty}"

            row = clusters.setdefault(key, {"count": 0, "modality": str(modality), "failure_type": str(failure_type)})
            row["count"] = int(row.get("count", 0)) + 1
            payload["updated_at"] = _utc_now()

            self.failure_cluster_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        self.log_event("failure_cluster", {"cluster_key": key, "count": row["count"]})
        return {"cluster_key": key, "count": row["count"]}

    def _load_json(self, path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
        if not path.exists():
            return dict(default)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        return dict(default)


DEFAULT_MULTIMODAL_TELEMETRY = MultimodalTelemetry()
