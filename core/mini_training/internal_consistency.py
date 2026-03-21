"""Self-disagreement and internal consistency analysis for Mini research diagnostics."""

from __future__ import annotations

import json
import math
import random
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Sequence


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


class MiniInternalConsistencyAnalyzer:
    """Runs multi-sample consistency analysis without touching runtime inference paths."""

    def __init__(self, *, output_dir: str = "data/mini_training/internal_consistency") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def analyze_samples(
        self,
        samples: Sequence[Mapping[str, Any]],
        *,
        concept_clusters: Sequence[str] | None = None,
        variance_threshold: float = 0.12,
    ) -> Dict[str, Any]:
        """Analyze already-collected N-run samples from Mini."""
        rows = list(samples)
        n = len(rows)
        if n <= 0:
            report = {
                "runs": 0,
                "answer_variance": 0.0,
                "confidence_variance": 0.0,
                "entropy_shift": 0.0,
                "reasoning_divergence": 0.0,
                "self_disagreement_rate": 0.0,
                "disagreement_self_rate": 0.0,
                "stability_score": 1.0,
                "variance_threshold": float(variance_threshold),
                "variance_blocked": False,
                "instability_clusters": [],
            }
            self.write_report(report)
            return report

        answers = [str(row.get("final_answer", "")).strip() for row in rows]
        reasonings = [str(row.get("reasoning_summary", row.get("reasoning", ""))).strip() for row in rows]
        confidences = [_clamp(float(row.get("confidence", 0.5))) for row in rows]
        entropies = [_clamp(float(row.get("entropy", 0.0))) for row in rows]
        counts = Counter(answers)
        dominant_answer, dominant_count = counts.most_common(1)[0]
        disagreement_rate = 1.0 - (dominant_count / n)
        answer_variance = len(counts) / n
        confidence_variance = self._std(confidences)
        entropy_shift = self._std(entropies)
        reasoning_div = self._reasoning_divergence(reasonings)

        instability_clusters = self._instability_clusters(
            concept_clusters=concept_clusters,
            disagreement_rate=disagreement_rate,
            reasoning_divergence=reasoning_div,
        )
        stability_score = _clamp(1.0 - (0.45 * disagreement_rate + 0.30 * reasoning_div + 0.25 * entropy_shift))
        variance_blocked = bool(max(answer_variance, confidence_variance) > float(variance_threshold))

        report = {
            "runs": int(n),
            "dominant_answer": dominant_answer,
            "answer_variance": float(answer_variance),
            "confidence_variance": float(confidence_variance),
            "entropy_shift": float(entropy_shift),
            "reasoning_divergence": float(reasoning_div),
            "self_disagreement_rate": float(disagreement_rate),
            "disagreement_self_rate": float(disagreement_rate),
            "stability_score": float(stability_score),
            "variance_threshold": float(variance_threshold),
            "variance_blocked": bool(variance_blocked),
            "instability_clusters": instability_clusters,
        }
        self.write_report(report)
        return report

    def analyze_with_sampler(
        self,
        question: str,
        *,
        sampler: Callable[[str, int], Mapping[str, Any]],
        runs: int = 5,
        seed: int = 42,
        concept_clusters: Sequence[str] | None = None,
        variance_threshold: float = 0.12,
    ) -> Dict[str, Any]:
        """Collect N stochastic samples via callback and analyze consistency."""
        rng = random.Random(int(seed))
        outputs = []
        for run_idx in range(max(1, int(runs))):
            out = dict(sampler(str(question), int(rng.randrange(0, 2**31 - 1))))
            outputs.append(out)
        return self.analyze_samples(
            outputs,
            concept_clusters=concept_clusters,
            variance_threshold=variance_threshold,
        )

    def analyze_model_state(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        model_state: Mapping[str, Any],
        runs: int = 5,
        seed: int = 42,
        confidence_noise: float = 0.08,
        answer_flip_rate: float = 0.08,
        variance_threshold: float = 0.12,
    ) -> Dict[str, Any]:
        """Run N stochastic forward-pass simulations over rows using noisy confidence."""
        memory = dict(model_state.get("memory", {}))
        default_answer = str(model_state.get("default_answer", ""))
        default_conf = _clamp(float(model_state.get("default_confidence", 0.5)))
        questions = [" ".join(str(r.get("question", "")).strip().lower().split()) for r in rows]
        questions = [q for q in questions if q]
        if not questions:
            return self.analyze_samples([], variance_threshold=variance_threshold)

        rng = random.Random(int(seed))
        samples: List[Dict[str, Any]] = []
        for _ in range(max(1, int(runs))):
            idx = rng.randrange(0, len(questions))
            question = questions[idx]
            slot = memory.get(question, {})
            answer = str(slot.get("final_answer", default_answer)).strip()
            base_conf = _clamp(float(slot.get("confidence", default_conf)))
            noisy_conf = _clamp(base_conf + rng.uniform(-confidence_noise, confidence_noise))
            if rng.random() < float(max(0.0, min(1.0, answer_flip_rate))):
                answer = default_answer if answer != default_answer else str(rows[idx].get("final_answer", answer))
            entropy_proxy = _clamp(float(rows[idx].get("entropy", 1.0 - noisy_conf)))
            samples.append(
                {
                    "final_answer": answer,
                    "confidence": noisy_conf,
                    "entropy": entropy_proxy,
                    "reasoning_summary": str(rows[idx].get("reasoning_summary", "")),
                }
            )

        clusters = rows[0].get("concept_cluster", []) if rows else []
        if not isinstance(clusters, list):
            clusters = []
        return self.analyze_samples(
            samples,
            concept_clusters=clusters,
            variance_threshold=variance_threshold,
        )

    def write_report(self, report: Mapping[str, Any], filename: str = "stability_diagnostics.json") -> Path:
        path = self.output_dir / filename
        path.write_text(json.dumps(dict(report), indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _reasoning_divergence(self, reasonings: Sequence[str]) -> float:
        if len(reasonings) <= 1:
            return 0.0
        pair_scores: List[float] = []
        for i in range(len(reasonings)):
            for j in range(i + 1, len(reasonings)):
                pair_scores.append(1.0 - self._jaccard(reasonings[i], reasonings[j]))
        if not pair_scores:
            return 0.0
        return float(sum(pair_scores) / len(pair_scores))

    def _jaccard(self, a: str, b: str) -> float:
        a_tokens = set(str(a).lower().split())
        b_tokens = set(str(b).lower().split())
        if not a_tokens and not b_tokens:
            return 1.0
        union = a_tokens | b_tokens
        if not union:
            return 1.0
        return float(len(a_tokens & b_tokens) / len(union))

    def _std(self, values: Sequence[float]) -> float:
        vals = [float(v) for v in values]
        if len(vals) <= 1:
            return 0.0
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        return float(math.sqrt(var))

    def _instability_clusters(
        self,
        *,
        concept_clusters: Sequence[str] | None,
        disagreement_rate: float,
        reasoning_divergence: float,
    ) -> List[str]:
        if concept_clusters is None:
            return ["general"] if (disagreement_rate > 0.35 or reasoning_divergence > 0.35) else []
        clusters = [str(cluster).strip().lower() for cluster in concept_clusters if str(cluster).strip()]
        if disagreement_rate > 0.35 or reasoning_divergence > 0.35:
            return sorted(set(clusters))
        return []
