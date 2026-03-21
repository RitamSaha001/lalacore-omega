from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Dict, List, Sequence, Tuple

from core.lalacore_x.schemas import ProblemProfile


def _clamp(value, lo=-1.0, hi=1.0):
    return max(lo, min(hi, value))


@dataclass(slots=True)
class RoutingDecision:
    ranked: List[Tuple[str, float]]
    selected: List[str]
    rationale: str


class ProviderStatsMemory:
    """
    Statistical provider memory for routing.

    Features:
    - subject-specific EMA accuracy
    - difficulty-weighted historical performance
    - cluster weakness penalties
    - Brier and calibration drift integration
    - weekly ranking recompute and threshold autotuning
    - cached provider score priors
    """

    def __init__(self, path: str = "data/metrics/provider_stats.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()
        self._score_cache: Dict[str, float] = {}

    def _load(self) -> Dict:
        if not self.path.exists():
            return {
                "providers": {},
                "routing_thresholds": {
                    "high_confidence_score": 0.18,
                    "gap_for_two_provider_mode": 0.10,
                    "default_arena_size": 3,
                    "token_penalty_scale": 0.14,
                    "token_baseline_ema": 240.0,
                    "efficiency_baseline": 2.2,
                    "question_token_ema": 48.0,
                },
                "weekly_rankings": [],
            }

        try:
            with self.path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            payload = {}

        payload.setdefault("providers", {})
        payload.setdefault(
            "routing_thresholds",
            {
                "high_confidence_score": 0.18,
                "gap_for_two_provider_mode": 0.10,
                "default_arena_size": 3,
                "token_penalty_scale": 0.14,
                "token_baseline_ema": 240.0,
                "efficiency_baseline": 2.2,
                "question_token_ema": 48.0,
            },
        )
        payload.setdefault("weekly_rankings", [])
        return payload

    def _save(self) -> None:
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, sort_keys=True)

    def _provider_bucket(self, provider: str) -> Dict:
        providers = self.data.setdefault("providers", {})

        if provider not in providers:
            providers[provider] = {
                "total": 0,
                "verified_pass": 0,
                "ema_reliability": 0.5,
                "calibration_error": 0.5,
                "brier_score": 0.5,
                "by_subject": {},
                "by_difficulty": {},
                "by_subject_difficulty": {},
                "cluster_weakness": {},
                "token_stats": {
                    "total_tokens": 0.0,
                    "calls": 0,
                    "avg_tokens_ema": 240.0,
                    "gain_per_1k_tokens_ema": 2.2,
                    "by_subject": {},
                },
            }

        bucket = providers[provider]
        bucket.setdefault("total", 0)
        bucket.setdefault("verified_pass", 0)
        bucket.setdefault("ema_reliability", 0.5)
        bucket.setdefault("calibration_error", 0.5)
        bucket.setdefault("brier_score", 0.5)
        bucket.setdefault("by_subject", {})
        bucket.setdefault("by_difficulty", {})
        bucket.setdefault("by_subject_difficulty", {})
        bucket.setdefault("cluster_weakness", {})
        bucket.setdefault(
            "token_stats",
            {
                "total_tokens": 0.0,
                "calls": 0,
                "avg_tokens_ema": 240.0,
                "gain_per_1k_tokens_ema": 2.2,
                "by_subject": {},
            },
        )

        return bucket

    def record_outcome(
        self,
        provider: str,
        subject: str,
        difficulty: str,
        predicted_confidence: float,
        verified: bool,
        calibration_risk: float | None = None,
        concept_clusters: Sequence[str] | None = None,
        token_usage: Dict | None = None,
        question_tokens: int | None = None,
    ) -> None:
        bucket = self._provider_bucket(provider)

        target = 1.0 if bool(verified) else 0.0
        pred = max(0.0, min(1.0, float(predicted_confidence)))
        err = abs(pred - target)
        brier = (pred - target) ** 2

        bucket["total"] += 1
        bucket["verified_pass"] += int(bool(verified))

        alpha = 0.10
        bucket["ema_reliability"] = self._ema(bucket["ema_reliability"], target, alpha)
        bucket["calibration_error"] = self._ema(bucket["calibration_error"], err, alpha)
        bucket["brier_score"] = self._ema(bucket["brier_score"], brier, alpha)

        if calibration_risk is not None:
            bucket["calibration_error"] = self._ema(bucket["calibration_error"], float(calibration_risk), alpha=0.05)

        self._update_nested(bucket["by_subject"], subject.lower(), target, err, brier)
        self._update_nested(bucket["by_difficulty"], difficulty.lower(), target, err, brier)
        self._update_nested(bucket["by_subject_difficulty"], f"{subject.lower()}:{difficulty.lower()}", target, err, brier)

        for cluster in concept_clusters or []:
            c = str(cluster).lower().strip()
            if not c:
                continue
            cw = bucket["cluster_weakness"].setdefault(c, {"failure_ema": 0.5, "total": 0})
            cw["total"] += 1
            cw["failure_ema"] = self._ema(cw["failure_ema"], 1.0 - target, alpha=0.12)

        self._update_token_stats(
            bucket=bucket,
            subject=subject.lower().strip(),
            target=target,
            token_usage=token_usage or {},
            question_tokens=question_tokens,
        )

        self._score_cache.clear()
        self.auto_tune_thresholds()
        self._save()

    def provider_score(
        self,
        provider: str,
        profile: ProblemProfile,
        concept_clusters: Sequence[str] | None = None,
        question_tokens: int | None = None,
        entropy: float | None = None,
    ) -> float:
        cache_key = self._cache_key(provider, profile, concept_clusters, question_tokens=question_tokens, entropy=entropy)
        if cache_key in self._score_cache:
            return self._score_cache[cache_key]

        bucket = self._provider_bucket(provider)

        subject = self._nested(bucket["by_subject"], profile.subject.lower())
        difficulty = self._nested(bucket["by_difficulty"], profile.difficulty.lower())
        sd = self._nested(bucket["by_subject_difficulty"], f"{profile.subject.lower()}:{profile.difficulty.lower()}")

        # Difficulty-weighted reliability emphasis.
        diff_weight = self._difficulty_weight(profile.difficulty)

        reliability = (
            0.30 * float(bucket["ema_reliability"])
            + 0.20 * float(subject["ema_reliability"])
            + 0.20 * float(difficulty["ema_reliability"])
            + 0.30 * float(sd["ema_reliability"])
        )

        risk_penalty = (
            0.45 * float(bucket["calibration_error"])
            + 0.35 * float(bucket["brier_score"])
            + 0.10 * float(sd["calibration_error"])
            + 0.10 * float(difficulty["calibration_error"])
        )

        weakness_penalty = self._cluster_weakness_penalty(bucket, concept_clusters or [])
        token_penalty = self._token_penalty(
            provider_bucket=bucket,
            subject=profile.subject.lower(),
            question_tokens=question_tokens,
            entropy=entropy,
        )

        score = diff_weight * reliability - risk_penalty - weakness_penalty - token_penalty
        score = _clamp(score, -1.0, 1.0)

        self._score_cache[cache_key] = score
        return score

    def rank(
        self,
        available_providers: Sequence[str],
        profile: ProblemProfile,
        concept_clusters: Sequence[str] | None = None,
        question_tokens: int | None = None,
        entropy: float | None = None,
    ) -> List[Tuple[str, float]]:
        ranked = [
            (
                provider,
                self.provider_score(
                    provider,
                    profile,
                    concept_clusters=concept_clusters,
                    question_tokens=question_tokens,
                    entropy=entropy,
                ),
            )
            for provider in available_providers
        ]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked

    def auto_tune_thresholds(self) -> Dict:
        providers = self.data.get("providers", {})
        scores = []
        token_emas = []
        efficiencies = []
        for provider, bucket in providers.items():
            rel = float(bucket.get("ema_reliability", 0.5))
            risk = 0.5 * float(bucket.get("calibration_error", 0.5)) + 0.5 * float(bucket.get("brier_score", 0.5))
            scores.append(rel - risk)
            token_stats = bucket.get("token_stats", {})
            token_emas.append(float(token_stats.get("avg_tokens_ema", 240.0)))
            efficiencies.append(float(token_stats.get("gain_per_1k_tokens_ema", 2.2)))

        if not scores:
            return self.data["routing_thresholds"]

        high = max(0.05, min(0.40, mean(scores) + 0.10))
        gap = max(0.05, min(0.20, 0.06 + 0.20 * max(scores) - 0.10 * min(scores)))

        self.data["routing_thresholds"]["high_confidence_score"] = high
        self.data["routing_thresholds"]["gap_for_two_provider_mode"] = gap
        if token_emas:
            self.data["routing_thresholds"]["token_baseline_ema"] = max(80.0, min(1200.0, mean(token_emas)))
        if efficiencies:
            self.data["routing_thresholds"]["efficiency_baseline"] = max(0.5, min(8.0, mean(efficiencies)))
        return self.data["routing_thresholds"]

    def weekly_recompute_rankings(self) -> List[Dict]:
        rows = []
        for provider, bucket in self.data.get("providers", {}).items():
            composite = (
                float(bucket.get("ema_reliability", 0.5))
                - float(bucket.get("calibration_error", 0.5))
                - float(bucket.get("brier_score", 0.5))
            )
            rows.append(
                {
                    "provider": provider,
                    "composite": round(composite, 6),
                    "ema_reliability": round(float(bucket.get("ema_reliability", 0.5)), 6),
                    "calibration_error": round(float(bucket.get("calibration_error", 0.5)), 6),
                    "brier_score": round(float(bucket.get("brier_score", 0.5)), 6),
                    "avg_tokens_ema": round(float(bucket.get("token_stats", {}).get("avg_tokens_ema", 240.0)), 3),
                    "gain_per_1k_tokens_ema": round(float(bucket.get("token_stats", {}).get("gain_per_1k_tokens_ema", 2.2)), 6),
                    "total": int(bucket.get("total", 0)),
                }
            )

        rows.sort(key=lambda r: r["composite"], reverse=True)
        self.data["weekly_rankings"] = rows
        self._save()
        return rows

    def _update_nested(self, bucket: Dict, key: str, target: float, err: float, brier: float) -> None:
        row = bucket.setdefault(
            key,
            {
                "total": 0,
                "verified_pass": 0,
                "ema_reliability": 0.5,
                "calibration_error": 0.5,
                "brier": 0.5,
            },
        )
        row.setdefault("total", 0)
        row.setdefault("verified_pass", 0)
        row.setdefault("ema_reliability", 0.5)
        row.setdefault("calibration_error", 0.5)
        row.setdefault("brier", 0.5)
        row["total"] += 1
        row["verified_pass"] += int(target)
        row["ema_reliability"] = self._ema(row["ema_reliability"], target, alpha=0.12)
        row["calibration_error"] = self._ema(row["calibration_error"], err, alpha=0.12)
        row["brier"] = self._ema(row["brier"], brier, alpha=0.12)

    def _nested(self, bucket: Dict, key: str) -> Dict:
        row = bucket.setdefault(
            key,
            {
                "ema_reliability": 0.5,
                "calibration_error": 0.5,
                "brier": 0.5,
            },
        )
        row.setdefault("ema_reliability", 0.5)
        row.setdefault("calibration_error", 0.5)
        row.setdefault("brier", 0.5)
        return row

    def _cluster_weakness_penalty(self, provider_bucket: Dict, concept_clusters: Sequence[str]) -> float:
        if not concept_clusters:
            return 0.0

        weakness = provider_bucket.get("cluster_weakness", {})
        penalties = []
        for cluster in concept_clusters:
            row = weakness.get(str(cluster).lower().strip())
            if not row:
                continue
            penalties.append(float(row.get("failure_ema", 0.5)))

        if not penalties:
            return 0.0

        return 0.20 * sum(penalties) / len(penalties)

    def _update_token_stats(
        self,
        bucket: Dict,
        subject: str,
        target: float,
        token_usage: Dict,
        question_tokens: int | None,
    ) -> None:
        token_stats = bucket.setdefault(
            "token_stats",
            {
                "total_tokens": 0.0,
                "calls": 0,
                "avg_tokens_ema": 240.0,
                "gain_per_1k_tokens_ema": 2.2,
                "by_subject": {},
            },
        )
        token_stats.setdefault("by_subject", {})

        total_tokens = float(token_usage.get("total_tokens", 0.0) or 0.0)
        if total_tokens <= 0.0:
            prompt_tokens = float(token_usage.get("prompt_tokens", 0.0) or 0.0)
            completion_tokens = float(token_usage.get("completion_tokens", 0.0) or 0.0)
            total_tokens = prompt_tokens + completion_tokens
        if total_tokens <= 0.0:
            # Deterministic fallback estimate when providers omit usage.
            baseline_question = max(6.0, float(question_tokens or 24))
            total_tokens = baseline_question + 72.0

        token_stats["total_tokens"] = float(token_stats.get("total_tokens", 0.0)) + total_tokens
        token_stats["calls"] = int(token_stats.get("calls", 0)) + 1
        token_stats["avg_tokens_ema"] = self._ema(token_stats.get("avg_tokens_ema", total_tokens), total_tokens, alpha=0.12)

        gain = target / max(total_tokens / 1000.0, 0.05)
        token_stats["gain_per_1k_tokens_ema"] = self._ema(token_stats.get("gain_per_1k_tokens_ema", gain), gain, alpha=0.12)

        srow = token_stats["by_subject"].setdefault(
            subject,
            {
                "avg_tokens_ema": total_tokens,
                "gain_per_1k_tokens_ema": gain,
                "calls": 0,
            },
        )
        srow["calls"] = int(srow.get("calls", 0)) + 1
        srow["avg_tokens_ema"] = self._ema(srow.get("avg_tokens_ema", total_tokens), total_tokens, alpha=0.14)
        srow["gain_per_1k_tokens_ema"] = self._ema(srow.get("gain_per_1k_tokens_ema", gain), gain, alpha=0.14)

        if question_tokens is not None:
            qt = max(1.0, float(question_tokens))
            thresholds = self.data.setdefault("routing_thresholds", {})
            prev = float(thresholds.get("question_token_ema", qt))
            thresholds["question_token_ema"] = self._ema(prev, qt, alpha=0.08)

    def _token_penalty(
        self,
        provider_bucket: Dict,
        subject: str,
        question_tokens: int | None = None,
        entropy: float | None = None,
    ) -> float:
        token_stats = provider_bucket.get("token_stats", {})
        by_subject = token_stats.get("by_subject", {})
        srow = by_subject.get(subject, {})

        avg_tokens = (
            0.60 * float(srow.get("avg_tokens_ema", token_stats.get("avg_tokens_ema", 240.0)))
            + 0.40 * float(token_stats.get("avg_tokens_ema", 240.0))
        )
        gain = (
            0.55 * float(srow.get("gain_per_1k_tokens_ema", token_stats.get("gain_per_1k_tokens_ema", 2.2)))
            + 0.45 * float(token_stats.get("gain_per_1k_tokens_ema", 2.2))
        )

        thresholds = self.data.get("routing_thresholds", {})
        baseline_tokens = max(40.0, float(thresholds.get("token_baseline_ema", 240.0)))
        baseline_eff = max(0.5, float(thresholds.get("efficiency_baseline", 2.2)))
        penalty_scale = max(0.02, min(0.40, float(thresholds.get("token_penalty_scale", 0.14))))
        q_baseline = max(4.0, float(thresholds.get("question_token_ema", 48.0)))

        verbosity_ratio = avg_tokens / baseline_tokens
        effective_eff = gain / max(verbosity_ratio, 0.2)

        deficit = max(0.0, baseline_eff - effective_eff)
        short_question = 0.0
        if question_tokens is not None:
            short_question = max(0.0, min(1.0, 1.0 - float(question_tokens) / q_baseline))

        entropy_pressure = 0.0
        if entropy is not None:
            entropy_pressure = max(0.0, min(1.0, float(entropy)))

        penalty = penalty_scale * deficit * (1.0 + 0.45 * short_question - 0.25 * entropy_pressure)
        return max(0.0, penalty)

    def _difficulty_weight(self, difficulty: str) -> float:
        d = difficulty.lower().strip()
        if d == "hard":
            return 1.15
        if d == "medium":
            return 1.00
        return 0.90

    def _cache_key(
        self,
        provider: str,
        profile: ProblemProfile,
        concept_clusters: Sequence[str] | None,
        question_tokens: int | None = None,
        entropy: float | None = None,
    ) -> str:
        clusters = ",".join(sorted({str(c).lower().strip() for c in (concept_clusters or [])}))
        q = int(question_tokens) if question_tokens is not None else -1
        e = round(float(entropy), 3) if entropy is not None else -1.0
        return f"{provider}|{profile.subject.lower()}|{profile.difficulty.lower()}|{clusters}|q{q}|e{e}"

    def _ema(self, current, new, alpha):
        return (1.0 - alpha) * float(current) + alpha * float(new)


class StatisticalRouter:
    def __init__(self, stats: ProviderStatsMemory | None = None):
        self.stats = stats or ProviderStatsMemory()

    def choose(
        self,
        available_providers: Sequence[str],
        profile: ProblemProfile,
        arena_size: int = 3,
        concept_clusters: Sequence[str] | None = None,
        question_tokens: int | None = None,
        entropy_hint: float | None = None,
    ) -> RoutingDecision:
        ranked = self.stats.rank(
            available_providers,
            profile,
            concept_clusters=concept_clusters,
            question_tokens=question_tokens,
            entropy=entropy_hint,
        )

        # Mini runs in shadow mode and should not force paid API expansion.
        non_mini_ranked = [(p, s) for p, s in ranked if p != "mini"]
        mini_present = any(p == "mini" for p, _ in ranked)

        thresholds = self.stats.data.get("routing_thresholds", {})
        high_score = float(thresholds.get("high_confidence_score", 0.18))
        gap_threshold = float(thresholds.get("gap_for_two_provider_mode", 0.10))

        target_size = max(2, int(thresholds.get("default_arena_size", arena_size)))
        target_size = min(max(2, arena_size), max(2, target_size))

        if len(non_mini_ranked) >= 2:
            top_score = non_mini_ranked[0][1]
            top_gap = non_mini_ranked[0][1] - non_mini_ranked[1][1]
            if top_score >= high_score and top_gap >= gap_threshold:
                target_size = 2

        selected = [provider for provider, _ in non_mini_ranked[: max(1, target_size)]]
        if not selected and mini_present:
            selected = ["mini"]

        rationale = (
            f"Statistical routing with subject/difficulty EMA + cluster weakness + token efficiency penalties; "
            f"selected={selected}, high_score={high_score:.3f}, gap={gap_threshold:.3f}, "
            f"shadow_mini={mini_present}"
        )

        return RoutingDecision(ranked=ranked, selected=selected, rationale=rationale)
