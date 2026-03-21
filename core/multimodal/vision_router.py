from __future__ import annotations

import asyncio
import math
import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Protocol, Sequence

from core.lalacore_x.provider_circuit import ProviderCircuitBreaker
from core.multimodal.diagram_parser import DiagramParser
from core.multimodal.ocr_engine import OCREngine
from core.multimodal.telemetry import DEFAULT_MULTIMODAL_TELEMETRY, MultimodalTelemetry


class VisionProvider(Protocol):
    async def analyze(self, image_bytes: bytes, problem_profile: Dict[str, Any] | None = None) -> Dict[str, Any]:
        ...


class HeuristicVisionProvider:
    """
    Built-in local vision analysis provider using OCR and geometry parsing.
    """

    def __init__(self, *, ocr_engine: OCREngine | None = None, diagram_parser: DiagramParser | None = None) -> None:
        self.ocr_engine = ocr_engine or OCREngine()
        self.diagram_parser = diagram_parser or DiagramParser()

    async def analyze(self, image_bytes: bytes, problem_profile: Dict[str, Any] | None = None) -> Dict[str, Any]:
        ocr = self.ocr_engine.extract(image_bytes, page_number=1, math_aware=True)
        ocr_text = str(ocr.get("clean_text") or ocr.get("math_normalized_text") or ocr.get("raw_text", ""))
        parsed = self.diagram_parser.parse(ocr_text, ocr.get("layout_blocks", []))
        expressions = self._extract_math_expressions(ocr_text)

        subject = str((problem_profile or {}).get("subject", "general"))
        figure_interpretation = self._figure_interpretation(parsed, subject)
        confidence = min(1.0, 0.55 * float(ocr.get("confidence", 0.0)) + (0.25 if parsed.get("is_geometry") else 0.0) + 0.15)

        return {
            "provider": "heuristic_vision",
            "detected_text": ocr_text,
            "detected_diagrams": {
                "geometry": bool(parsed.get("is_geometry", False)),
                "points": len(parsed.get("points", [])),
                "segments": len(parsed.get("segments", [])),
                "angles": len(parsed.get("angles", [])),
            },
            "structured_math_expressions": expressions,
            "figure_interpretation": figure_interpretation,
            "geometry_objects": parsed.get("abstraction", {}),
            "confidence": float(max(0.0, min(1.0, confidence))),
            "ocr": ocr,
            "ocr_model": str(ocr.get("ocr_model", "")),
        }

    def _extract_math_expressions(self, text: str) -> List[str]:
        text = str(text or "")
        if not text:
            return []
        candidates = re.findall(r"[A-Za-z0-9_\+\-\*/\^\(\)=<>]{4,}", text)
        out = []
        seen = set()
        for candidate in candidates:
            c = candidate.strip()
            if c in seen:
                continue
            if not re.search(r"[\d\+\-\*/\^=]", c):
                continue
            seen.add(c)
            out.append(c)
        return out[:24]

    def _figure_interpretation(self, parsed: Dict[str, Any], subject: str) -> str:
        if parsed.get("is_geometry"):
            points = len(parsed.get("points", []))
            segments = len(parsed.get("segments", []))
            angles = len(parsed.get("angles", []))
            return f"Geometry diagram detected with {points} points, {segments} segments and {angles} angles."
        return f"No explicit geometry diagram detected; treating image as {subject} text/math prompt."


class VisionRouter:
    """
    Vision provider orchestration with timeout guards, circuit breaker and arena-style comparison.
    """

    def __init__(
        self,
        *,
        providers: Dict[str, VisionProvider] | None = None,
        timeout_s: float = 18.0,
        max_image_bytes: int = 10_000_000,
        telemetry: MultimodalTelemetry | None = None,
    ) -> None:
        self.providers: Dict[str, VisionProvider] = dict(providers or {})
        if "heuristic_vision" not in self.providers:
            self.providers["heuristic_vision"] = HeuristicVisionProvider()
        self.timeout_s = float(max(1.0, timeout_s))
        self.max_image_bytes = int(max(1024, max_image_bytes))
        self.telemetry = telemetry or DEFAULT_MULTIMODAL_TELEMETRY
        self.circuit = ProviderCircuitBreaker(path="data/metrics/vision_provider_circuit.json", failure_threshold=3, cooldown_s=90.0)
        self._circuit_lock = asyncio.Lock()
        self._provider_confidence_scale = {
            "heuristic_vision": 0.78,
            "openai_vision": 1.00,
            "gemini_vision": 1.00,
            "claude_vision": 1.00,
            "local_vision": 0.92,
        }

    def register_provider(self, name: str, provider: VisionProvider) -> None:
        self.providers[str(name)] = provider

    async def analyze(
        self,
        image_bytes: bytes,
        problem_profile: Dict[str, Any] | None = None,
        *,
        provider_names: Sequence[str] | None = None,
    ) -> Dict[str, Any]:
        if not image_bytes:
            return self._empty_response("empty_image")
        if len(image_bytes) > self.max_image_bytes:
            return self._empty_response("image_size_limit_exceeded")

        requested = [str(name) for name in (provider_names or self.providers.keys()) if str(name) in self.providers]
        if not requested:
            requested = ["heuristic_vision"]

        active = [name for name in requested if self.circuit.can_request(name)]
        if not active:
            active = ["heuristic_vision"] if "heuristic_vision" in self.providers else requested[:1]

        tasks = [asyncio.create_task(self._analyze_one(name, image_bytes, problem_profile)) for name in active]
        try:
            rows = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

        analyses: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict) and row:
                analyses.append(row)

        if not analyses:
            return self._empty_response("all_vision_providers_failed")

        comparison = self._arena_compare(analyses)
        winner = comparison[0] if comparison else analyses[0]

        geometry_objects = winner.get("geometry_objects", {}) or {}
        self.telemetry.log_vision_metrics(
            provider=str(winner.get("provider", "unknown")),
            detection_confidence=float(winner.get("confidence", 0.0)),
            diagram_detected=bool((winner.get("detected_diagrams") or {}).get("geometry", False)),
            geometry_count=len(geometry_objects.get("points", [])) + len(geometry_objects.get("segments", [])),
        )
        self.telemetry.log_provider_comparison(
            {
                "winner_provider": winner.get("provider"),
                "provider_count": len(analyses),
                "scores": [
                    {
                        "provider": row.get("provider"),
                        "score": float(row.get("_score", 0.0)),
                        "normalized_confidence": float(row.get("_normalized_confidence", 0.0)),
                        "agreement": float(row.get("_agreement", 0.0)),
                        "confidence": float(row.get("confidence", 0.0)),
                        "probability": float(row.get("_prob", 0.0)),
                    }
                    for row in comparison
                ],
            }
        )

        probs = [float(row.get("_prob", 0.0)) for row in comparison]
        entropy = self._entropy_from_probs(probs)
        output_confidence = float(winner.get("confidence", 0.0))
        single_provider_uncertainty = False
        if len(comparison) <= 1:
            # Avoid false certainty in single-provider mode.
            entropy = max(float(entropy), 0.42)
            output_confidence = max(0.0, min(1.0, output_confidence * 0.82))
            single_provider_uncertainty = True

        disagreement = max(0.0, min(1.0, 1.0 - float(winner.get("_agreement", 0.0))))

        return {
            "winner_provider": winner.get("provider"),
            "detected_text": winner.get("detected_text", ""),
            "detected_diagrams": winner.get("detected_diagrams", {}),
            "structured_math_expressions": winner.get("structured_math_expressions", []),
            "figure_interpretation": winner.get("figure_interpretation", ""),
            "geometry_objects": geometry_objects,
            "confidence": float(output_confidence),
            "provider_comparison": [
                {
                    "provider": row.get("provider"),
                    "score": float(row.get("_score", 0.0)),
                    "normalized_confidence": float(row.get("_normalized_confidence", 0.0)),
                    "agreement": float(row.get("_agreement", 0.0)),
                    "confidence": float(row.get("confidence", 0.0)),
                    "probability": float(row.get("_prob", 0.0)),
                }
                for row in comparison
            ],
            "entropy": float(entropy),
            "disagreement": float(disagreement),
            "single_provider_uncertainty": bool(single_provider_uncertainty),
        }

    async def _analyze_one(self, provider_name: str, image_bytes: bytes, profile: Dict[str, Any] | None) -> Dict[str, Any] | None:
        provider = self.providers.get(provider_name)
        if provider is None:
            return None

        try:
            out = await asyncio.wait_for(provider.analyze(image_bytes, profile), timeout=self.timeout_s)
            if not isinstance(out, dict):
                raise RuntimeError("invalid_response")
            async with self._circuit_lock:
                self.circuit.record_success(provider_name)
            out.setdefault("provider", provider_name)
            return out
        except Exception as exc:
            async with self._circuit_lock:
                self.circuit.record_failure(provider_name, "timeout")
            self.telemetry.log_event(
                "vision_provider_error",
                {"provider": str(provider_name), "error_type": type(exc).__name__, "reason": str(exc)[:300]},
            )
            return None

    def _arena_compare(self, analyses: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not analyses:
            return []

        calibrated_conf = []
        for row in analyses:
            provider = str(row.get("provider", "")).strip().lower()
            scale = float(self._provider_confidence_scale.get(provider, 0.94))
            base_conf = float(max(0.0, min(1.0, row.get("confidence", 0.0))))
            # Heuristic providers are intentionally conservative to avoid dominance inflation.
            adjusted = max(0.0, min(1.0, base_conf * scale))
            if "heuristic" in provider:
                adjusted = min(adjusted, 0.72)
            calibrated_conf.append(adjusted)

        norm_conf = self._normalize_confidences(calibrated_conf)
        scored = []
        for idx, row in enumerate(analyses):
            agreement = self._average_agreement(idx, analyses)
            normalized_confidence = float(norm_conf[idx])
            geometry_signal = 1.0 if bool((row.get("detected_diagrams") or {}).get("geometry", False)) else 0.0
            score = 0.48 * normalized_confidence + 0.34 * agreement + 0.08 * geometry_signal
            enriched = dict(row)
            enriched["_agreement"] = float(agreement)
            enriched["_normalized_confidence"] = float(normalized_confidence)
            enriched["_score"] = float(max(0.0, min(1.0, score)))
            scored.append(enriched)

        probs = self._softmax([float(row.get("_score", 0.0)) for row in scored], temperature=0.65)
        for row, prob in zip(scored, probs):
            row["_prob"] = float(prob)

        scored.sort(key=lambda r: float(r.get("_score", 0.0)), reverse=True)
        return scored

    def _average_agreement(self, idx: int, analyses: Sequence[Dict[str, Any]]) -> float:
        if len(analyses) <= 1:
            return 1.0
        base = self._signature(analyses[idx])
        sims = []
        for j, other in enumerate(analyses):
            if j == idx:
                continue
            sims.append(SequenceMatcher(a=base, b=self._signature(other)).ratio())
        return float(sum(sims) / max(1, len(sims)))

    def _signature(self, row: Dict[str, Any]) -> str:
        text = str(row.get("detected_text", "")).lower().strip()
        text = re.sub(r"\s+", " ", text)
        exprs = ",".join(str(x) for x in row.get("structured_math_expressions", [])[:8])
        diag = row.get("detected_diagrams", {})
        geom = f"g:{int(bool(diag.get('geometry', False)))}|p:{int(diag.get('points', 0))}|s:{int(diag.get('segments', 0))}|a:{int(diag.get('angles', 0))}"
        return f"{text}|{exprs}|{geom}"

    def _entropy_from_probs(self, probs: Sequence[float]) -> float:
        values = [max(1e-9, float(p)) for p in probs if float(p) > 0.0]
        if not values:
            return 0.0
        total = sum(values)
        normalized = [v / total for v in values]
        entropy = -sum(p * math.log(p, 2) for p in normalized)
        max_entropy = math.log(len(normalized), 2) if len(normalized) > 1 else 1.0
        if max_entropy <= 0.0:
            return 0.0
        return float(max(0.0, min(1.0, entropy / max_entropy)))

    def _normalize_confidences(self, values: Sequence[float]) -> List[float]:
        vals = [float(max(0.0, min(1.0, v))) for v in values]
        if not vals:
            return []
        if len(vals) == 1:
            return [max(0.0, min(1.0, vals[0] * 0.78))]

        mean = sum(vals) / len(vals)
        variance = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = max(1e-6, math.sqrt(variance))
        z_scores = [(v - mean) / std for v in vals]
        probs = self._softmax(z_scores, temperature=0.9)
        return [float(max(0.0, min(1.0, p))) for p in probs]

    def _softmax(self, values: Sequence[float], *, temperature: float = 1.0) -> List[float]:
        if not values:
            return []
        t = max(1e-3, float(temperature))
        scaled = [float(v) / t for v in values]
        max_v = max(scaled)
        exps = [math.exp(v - max_v) for v in scaled]
        denom = sum(exps)
        if denom <= 0.0:
            return [1.0 / len(values)] * len(values)
        return [v / denom for v in exps]

    def _empty_response(self, reason: str) -> Dict[str, Any]:
        return {
            "winner_provider": "",
            "detected_text": "",
            "detected_diagrams": {},
            "structured_math_expressions": [],
            "figure_interpretation": "",
            "geometry_objects": {},
            "confidence": 0.0,
            "provider_comparison": [],
            "entropy": 0.0,
            "disagreement": 1.0,
            "reason": str(reason),
        }
