"""Arena-aware distillation and calibration supervision utilities."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence



def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))



def _sigmoid(value: float) -> float:
    if value >= 0:
        exp_neg = math.exp(-value)
        return 1.0 / (1.0 + exp_neg)
    exp_pos = math.exp(value)
    return exp_pos / (1.0 + exp_pos)


@dataclass(slots=True)
class DistillationConfig:
    """Weights and thresholds used by the distillation logic."""

    low_entropy_weight: float = 1.20
    high_entropy_weight: float = 0.60
    deterministic_bonus: float = 0.35
    near_miss_risk_threshold: float = 0.45
    # Kaggle hard-case specialist replay weights.
    replay_disagreement_weight: float = 1.8
    replay_entropy_weight: float = 1.7
    replay_concept_energy_weight: float = 1.5
    replay_risk_weight: float = 1.2
    replay_overconfidence_weight: float = 0.8
    replay_near_miss_weight: float = 0.6
    replay_disagreement_bonus_weight: float = 0.8
    temporal_decay_half_life_days: float = 14.0
    synthetic_entropy_floor: float = 0.12
    rare_cluster_amplification: float = 0.65
    disagreement_entropy_amplifier: float = 0.50
    disagreement_entropy_threshold: float = 0.45
    enable_disagreement_augmentation: bool = True
    rare_weight_floor: float = 0.15
    concept_energy_alpha: float = 0.45
    concept_energy_beta: float = 0.30
    concept_energy_gamma: float = 0.25
    concept_energy_ema_decay: float = 0.80
    concept_energy_rising_amplification: float = 0.75


@dataclass(slots=True)
class CalibrationHeadState:
    """Simple calibration head parameters and training diagnostics."""

    bias: float
    weight_confidence: float
    weight_entropy: float
    losses: List[float]


class MiniDistillationEngine:
    """Produces distillation targets, hard negatives, and calibration supervision."""

    def __init__(self, config: DistillationConfig | None = None) -> None:
        self.config = config or DistillationConfig()
        self._concept_energy_state: Dict[str, Dict[str, float]] = {}

    def teacher_distribution(self, row: Dict[str, Any]) -> Dict[str, float]:
        """Builds multi-teacher soft labels using arena posterior/margin/entropy signals."""
        synthetic = row.get("synthetic_teacher_distribution")
        if isinstance(synthetic, dict) and synthetic:
            cleaned = {str(k): max(1e-9, float(v)) for k, v in synthetic.items() if str(k).strip()}
            if cleaned:
                total = sum(cleaned.values())
                if total > 0:
                    return {k: float(v / total) for k, v in cleaned.items()}

        margin = _clamp(float(row.get("winner_margin", 0.0)))
        temperature = max(0.35, 1.05 - 0.55 * margin)

        posteriors = row.get("arena_posteriors")
        if isinstance(posteriors, dict) and posteriors:
            values = {str(k): max(1e-9, float(v)) for k, v in posteriors.items()}
            if len(values) == 1:
                key = next(iter(values.keys()))
                return {key: 1.0}
            return self._softmax_dict(values, temperature=temperature)

        ranked = row.get("ranked_providers")
        if isinstance(ranked, list) and ranked:
            raw_scores: Dict[str, float] = {}
            for item in ranked:
                if not isinstance(item, dict):
                    continue
                provider = str(item.get("provider", "")).strip()
                if not provider:
                    continue
                raw_scores[provider] = float(item.get("score", 0.0))
            if raw_scores:
                return self._softmax_dict(raw_scores, temperature=temperature)

        winner = str(row.get("winner_provider", "mini")).strip() or "mini"
        return {winner: 1.0}

    def confidence_weight(self, row: Dict[str, Any]) -> float:
        """Confidence-weighted loss multiplier favoring deterministic, low-entropy samples."""
        entropy = _clamp(float(row.get("entropy", 0.0)))
        verified = bool(row.get("verified", False))
        deterministic = bool(row.get("deterministic_verified", verified))

        base = self.config.low_entropy_weight if entropy < 0.25 else self.config.high_entropy_weight
        if deterministic:
            base += self.config.deterministic_bonus
        if not verified:
            base *= 0.8
        return float(max(0.1, min(2.5, base)))

    def build_distillation_rows(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Creates weighted distillation records with soft teacher distributions."""
        out: List[Dict[str, Any]] = []
        base_rows = [dict(row) for row in rows]
        synthetic_rows: List[Dict[str, Any]] = []
        if bool(self.config.enable_disagreement_augmentation):
            synthetic_rows = self.synthesize_disagreement_rows(base_rows)
        working_rows = base_rows + synthetic_rows

        frequency = self.concept_cluster_frequency(working_rows)
        concept_energy_state = self.compute_concept_energy_evolution(working_rows)

        for row in working_rows:
            distribution = self.teacher_distribution(row)
            weight = self.confidence_weight(row)
            entropy = _clamp(float(row.get("entropy", 0.0)))
            margin = _clamp(float(row.get("winner_margin", 0.0)))
            synthetic_entropy_floor_applied = len(distribution) <= 1
            teacher_entropy = max(
                entropy,
                float(self.config.synthetic_entropy_floor) if synthetic_entropy_floor_applied else entropy,
            )
            replay_priority = self.replay_priority_score(
                row,
                cluster_frequency=frequency,
                concept_energy_state=concept_energy_state,
            )
            weighted_loss = float(weight)
            if bool(row.get("calibration_guard_mode", False)):
                weighted_loss *= 1.0

            candidate = dict(row)
            candidate["teacher_distribution"] = distribution
            candidate["teacher_entropy"] = teacher_entropy
            candidate["teacher_entropy_floor_applied"] = bool(synthetic_entropy_floor_applied)
            candidate["margin_weight"] = float(max(0.2, 0.4 + margin))
            candidate["loss_weight"] = weighted_loss
            candidate["replay_priority_score"] = float(replay_priority)
            candidate["concept_cluster_frequency"] = self._row_cluster_frequency(candidate, frequency)
            candidate["rare_cluster_weight"] = float(self.rare_cluster_weight(candidate, frequency))
            candidate["concept_energy_trend"] = float(self._row_concept_energy_trend(candidate, concept_energy_state))
            candidate["disagreement_augmented"] = bool(row.get("disagreement_augmented", False))
            out.append(candidate)

        return out

    def synthesize_disagreement_rows(self, rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        """Amplifies disagreement density by creating top-2 teacher swap variants."""
        out: List[Dict[str, Any]] = []
        threshold = float(self.config.disagreement_entropy_threshold)

        for row in rows:
            entropy = _clamp(float(row.get("entropy", 0.0)))
            disagreement = _clamp(float(row.get("disagreement", 0.0)))
            disagreement_candidate = (
                disagreement > 0.0
                or bool(row.get("agreement_with_winner") is False)
                or entropy > threshold
            )
            if not disagreement_candidate:
                continue

            teacher = self.teacher_distribution(dict(row))
            ranked = sorted(teacher.items(), key=lambda item: item[1], reverse=True)
            if len(ranked) < 2:
                primary = ranked[0][0] if ranked else (str(row.get("winner_provider", "mini")).strip() or "mini")
                secondary = self._fallback_secondary_provider(row=row, primary=primary)
                ranked = [(primary, 0.55), (secondary, 0.45)]
            else:
                ranked = ranked[:2]
                total = max(1e-9, ranked[0][1] + ranked[1][1])
                ranked = [(ranked[0][0], ranked[0][1] / total), (ranked[1][0], ranked[1][1] / total)]

            swapped = [ranked[1], ranked[0]]
            synthetic_dist = {provider: float(score) for provider, score in swapped}
            ranked_payload = [
                {"provider": provider, "score": float(score)}
                for provider, score in swapped
            ]

            synthetic = dict(row)
            synthetic["source"] = f"{str(row.get('source', 'unknown')).strip() or 'unknown'}_synthetic_disagreement"
            synthetic["synthetic_disagreement"] = True
            synthetic["synthetic_variant"] = "top2_provider_swap"
            synthetic["disagreement"] = float(
                _clamp(max(disagreement, 1.0 if entropy > threshold else 0.60))
            )
            synthetic["disagreement_augmented"] = True
            synthetic["disagreement_entropy_flag"] = bool(entropy > threshold)
            synthetic["synthetic_teacher_distribution"] = synthetic_dist
            synthetic["arena_posteriors"] = synthetic_dist
            synthetic["ranked_providers"] = ranked_payload
            out.append(synthetic)

        return out

    def concept_cluster_frequency(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        """Counts concept-cluster frequency to amplify rare-cluster replay."""
        frequency: Dict[str, int] = {}
        for row in rows:
            clusters = row.get("concept_cluster", [])
            if not isinstance(clusters, list):
                continue
            for cluster in clusters:
                key = str(cluster).strip().lower()
                if not key:
                    continue
                frequency[key] = frequency.get(key, 0) + 1
        return frequency

    def replay_priority_score(
        self,
        row: Mapping[str, Any],
        *,
        cluster_frequency: Mapping[str, int] | None = None,
        now_ts: datetime | None = None,
        concept_energy_state: Mapping[str, Mapping[str, float]] | None = None,
    ) -> float:
        """Computes hard-case replay priority with rare-cluster and temporal weighting."""
        cfg = self.config
        risk = _clamp(float(row.get("risk", 1.0)))
        entropy = _clamp(float(row.get("entropy", 0.0)))
        disagreement = _clamp(float(row.get("disagreement", 0.0)))
        confidence = _clamp(float(row.get("confidence", 1.0 - risk)))
        verified = bool(row.get("verified", False))
        concept_energy = self._concept_energy(dict(row), concept_energy_state=concept_energy_state)
        near_miss = bool((not verified) and (risk < cfg.near_miss_risk_threshold) and (confidence > 0.65))
        overconfident = bool((not verified) and (confidence >= 0.85))

        temporal_decay_weight = self._temporal_decay_weight(row=row, now_ts=now_ts)
        rare_cluster_boost = self._rare_cluster_amplification(row=row, cluster_frequency=cluster_frequency)
        rare_weight = self.rare_cluster_weight(row, cluster_frequency)
        energy_rise_amp = self._energy_rise_amplification(row, concept_energy_state=concept_energy_state)
        disagreement_entropy_amp = 1.0 + cfg.disagreement_entropy_amplifier * ((disagreement + entropy) / 2.0)

        raw_score = (
            cfg.replay_disagreement_weight * disagreement
            + cfg.replay_disagreement_bonus_weight * disagreement
            + cfg.replay_entropy_weight * entropy
            + cfg.replay_concept_energy_weight * concept_energy
            + cfg.replay_risk_weight * risk
            + cfg.replay_overconfidence_weight * (1.0 if overconfident else 0.0)
            + cfg.replay_near_miss_weight * (1.0 if near_miss else 0.0)
            + temporal_decay_weight
        )
        return float(raw_score * rare_cluster_boost * disagreement_entropy_amp * rare_weight * energy_rise_amp)

    def mine_hard_negatives(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        top_k: int = 256,
        use_advanced_scoring: bool = False,
        export_dir: str | None = None,
    ) -> List[Dict[str, Any]]:
        """Prioritizes replay failures using concept energy, near-miss, and overconfidence signals."""
        scored: List[tuple[float, Dict[str, Any]]] = []
        frequency = self.concept_cluster_frequency(rows)
        concept_energy_state = self.compute_concept_energy_evolution(rows)
        for row in rows:
            verified = bool(row.get("verified", False))
            risk = _clamp(float(row.get("risk", 1.0)))
            entropy = _clamp(float(row.get("entropy", 0.0)))
            disagreement = _clamp(float(row.get("disagreement", 0.0)))
            confidence = _clamp(float(row.get("confidence", 0.0)))
            concept_energy = self._concept_energy(row)
            near_miss = bool((not verified) and (risk < self.config.near_miss_risk_threshold) and (confidence > 0.65))
            overconfident = bool((not verified) and (confidence >= 0.80))

            if use_advanced_scoring:
                score = self.replay_priority_score(
                    row,
                    cluster_frequency=frequency,
                    concept_energy_state=concept_energy_state,
                )
            else:
                score = (
                    1.20 * (0.0 if verified else 1.0)
                    + 0.90 * risk
                    + 0.55 * entropy
                    + 0.50 * disagreement
                    + 0.75 * concept_energy
                    + (0.45 if near_miss else 0.0)
                    + (0.40 if overconfident else 0.0)
                )
            candidate = dict(row)
            candidate["near_miss"] = near_miss
            candidate["overconfident"] = overconfident
            candidate["concept_energy"] = concept_energy
            candidate["replay_priority_score"] = float(score)
            scored.append((score, candidate))

        scored.sort(key=lambda item: item[0], reverse=True)
        selected = [row for _, row in scored[: max(1, int(top_k))]]
        if export_dir is not None:
            self.export_replay_diagnostics(selected, output_dir=export_dir)
        return selected

    def export_replay_diagnostics(self, rows: Sequence[Dict[str, Any]], *, output_dir: str) -> Dict[str, str]:
        """Exports replay diagnostics for explainable hard-case sampling."""
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        frequency = self.concept_cluster_frequency(rows)
        concept_energy_state = self.compute_concept_energy_evolution(rows)
        weighted_rows: List[Dict[str, Any]] = []
        total_score = 0.0
        for row in rows:
            score = self.replay_priority_score(
                row,
                cluster_frequency=frequency,
                concept_energy_state=concept_energy_state,
            )
            total_score += max(0.0, score)
            weighted_rows.append(
                {
                    "question": str(row.get("question", "")),
                    "score": float(score),
                    "risk": float(_clamp(float(row.get("risk", 1.0)))),
                    "entropy": float(_clamp(float(row.get("entropy", 0.0)))),
                    "disagreement": float(_clamp(float(row.get("disagreement", 0.0)))),
                    "concept_cluster": list(row.get("concept_cluster", [])) if isinstance(row.get("concept_cluster"), list) else [],
                    "rare_cluster_weight": float(self.rare_cluster_weight(row, frequency)),
                    "concept_energy_trend": float(self._row_concept_energy_trend(row, concept_energy_state)),
                }
            )

        distribution = []
        for item in weighted_rows:
            prob = (item["score"] / total_score) if total_score > 0 else 0.0
            distribution.append(
                {
                    "question": item["question"],
                    "sampling_probability": float(prob),
                    "score": item["score"],
                }
            )
        distribution.sort(key=lambda item: item["sampling_probability"], reverse=True)

        weight_analysis = {
            "rows": len(weighted_rows),
            "concept_cluster_frequency": frequency,
            "top_rows": sorted(weighted_rows, key=lambda item: item["score"], reverse=True)[:50],
        }
        rare_cluster_weighting = self._rare_cluster_weighting_payload(rows, frequency)
        disagreement_distribution = self._disagreement_distribution_payload(rows)
        concept_energy_evolution = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "alpha": float(self.config.concept_energy_alpha),
            "beta": float(self.config.concept_energy_beta),
            "gamma": float(self.config.concept_energy_gamma),
            "ema_decay": float(self.config.concept_energy_ema_decay),
            "clusters": concept_energy_state,
        }
        explainability = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "formula": (
                "1.8*disagreement + 1.7*entropy + 1.5*concept_energy + 1.2*risk + "
                "0.8*overconfidence + 0.6*near_miss + 0.8*disagreement_bonus + temporal_decay_weight"
            ),
            "amplifiers": {
                "rare_cluster_amplification": float(self.config.rare_cluster_amplification),
                "disagreement_entropy_amplifier": float(self.config.disagreement_entropy_amplifier),
                "concept_energy_rising_amplification": float(self.config.concept_energy_rising_amplification),
            },
            "distribution_preview": distribution[:30],
        }

        distribution_path = out_dir / "replay_sampling_distribution.json"
        analysis_path = out_dir / "hard_case_weight_analysis.json"
        explainability_path = out_dir / "replay_explainability_log.json"
        rare_weight_path = out_dir / "rare_cluster_weighting.json"
        disagreement_distribution_path = out_dir / "disagreement_distribution.json"
        concept_energy_path = out_dir / "concept_energy_evolution.json"
        distribution_path.write_text(json.dumps(distribution, indent=2, sort_keys=True), encoding="utf-8")
        analysis_path.write_text(json.dumps(weight_analysis, indent=2, sort_keys=True), encoding="utf-8")
        explainability_path.write_text(json.dumps(explainability, indent=2, sort_keys=True), encoding="utf-8")
        rare_weight_path.write_text(json.dumps(rare_cluster_weighting, indent=2, sort_keys=True), encoding="utf-8")
        disagreement_distribution_path.write_text(
            json.dumps(disagreement_distribution, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        concept_energy_path.write_text(json.dumps(concept_energy_evolution, indent=2, sort_keys=True), encoding="utf-8")
        return {
            "replay_sampling_distribution": str(distribution_path),
            "hard_case_weight_analysis": str(analysis_path),
            "replay_explainability_log": str(explainability_path),
            "rare_cluster_weighting": str(rare_weight_path),
            "disagreement_distribution": str(disagreement_distribution_path),
            "concept_energy_evolution": str(concept_energy_path),
        }

    def train_calibration_head(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        epochs: int = 30,
        lr: float = 0.15,
    ) -> CalibrationHeadState:
        """Fits a lightweight calibration head by minimizing Brier score with SGD."""
        samples = []
        for row in rows:
            conf = _clamp(float(row.get("confidence", 1.0 - float(row.get("risk", 1.0)))))
            ent = _clamp(float(row.get("entropy", 0.0)))
            target = 1.0 if bool(row.get("verified", False)) else 0.0
            samples.append((conf, ent, target))

        if not samples:
            return CalibrationHeadState(bias=0.0, weight_confidence=1.0, weight_entropy=-0.5, losses=[])

        b = 0.0
        w_conf = 1.0
        w_ent = -0.8
        losses: List[float] = []

        for _ in range(max(1, int(epochs))):
            db = 0.0
            dw_conf = 0.0
            dw_ent = 0.0
            loss = 0.0

            for conf, ent, target in samples:
                pred = _sigmoid(b + w_conf * conf + w_ent * ent)
                err = pred - target
                loss += err * err
                grad_common = 2.0 * err * pred * (1.0 - pred)
                db += grad_common
                dw_conf += grad_common * conf
                dw_ent += grad_common * ent

            n = float(len(samples))
            b -= float(lr) * (db / n)
            w_conf -= float(lr) * (dw_conf / n)
            w_ent -= float(lr) * (dw_ent / n)
            losses.append(loss / n)

        return CalibrationHeadState(
            bias=float(b),
            weight_confidence=float(w_conf),
            weight_entropy=float(w_ent),
            losses=losses,
        )

    def predict_calibrated_confidence(self, confidence: float, entropy: float, state: CalibrationHeadState) -> float:
        return _clamp(
            _sigmoid(
                float(state.bias)
                + float(state.weight_confidence) * _clamp(confidence)
                + float(state.weight_entropy) * _clamp(entropy)
            )
        )

    def calibration_brier_score(self, predictions: Sequence[float], targets: Sequence[float]) -> float:
        if len(predictions) != len(targets):
            raise ValueError("predictions and targets must have identical lengths")
        if not predictions:
            return 0.0
        err = 0.0
        for prediction, target in zip(predictions, targets):
            p = _clamp(float(prediction))
            t = _clamp(float(target))
            err += (p - t) ** 2
        return err / float(len(predictions))

    def _concept_energy(
        self,
        row: Dict[str, Any],
        *,
        concept_energy_state: Mapping[str, Mapping[str, float]] | None = None,
    ) -> float:
        clusters = row.get("concept_cluster", [])
        if isinstance(clusters, list):
            cluster_count = len([c for c in clusters if str(c).strip()])
        else:
            cluster_count = 1

        difficulty = str(row.get("difficulty", "unknown")).lower().strip()
        diff_bonus = {"easy": 0.2, "medium": 0.45, "hard": 0.75}.get(difficulty, 0.35)
        base = _clamp(0.15 * min(cluster_count, 6) + diff_bonus)
        state = concept_energy_state or self._concept_energy_state
        if not isinstance(state, Mapping):
            return base
        cluster_energy = self._row_cluster_energy(row, state)
        return _clamp(0.60 * base + 0.40 * cluster_energy)

    def compute_concept_energy_evolution(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Dict[str, float]]:
        """Track concept energy via EMA to prioritize clusters with rising failures."""
        bucket: Dict[str, Dict[str, float]] = {}
        for row in rows:
            clusters = row.get("concept_cluster", [])
            if not isinstance(clusters, list):
                continue
            failure = 0.0 if bool(row.get("verified", False)) else 1.0
            disagreement = _clamp(float(row.get("disagreement", 0.0)))
            entropy = _clamp(float(row.get("entropy", 0.0)))
            for cluster in clusters:
                key = str(cluster).strip().lower()
                if not key:
                    continue
                slot = bucket.setdefault(
                    key,
                    {"count": 0.0, "failure_sum": 0.0, "disagreement_sum": 0.0, "entropy_sum": 0.0},
                )
                slot["count"] += 1.0
                slot["failure_sum"] += failure
                slot["disagreement_sum"] += disagreement
                slot["entropy_sum"] += entropy

        updated: Dict[str, Dict[str, float]] = {}
        decay = _clamp(float(self.config.concept_energy_ema_decay))
        alpha = _clamp(float(self.config.concept_energy_alpha))
        beta = _clamp(float(self.config.concept_energy_beta))
        gamma = _clamp(float(self.config.concept_energy_gamma))
        for cluster, slot in bucket.items():
            count = max(1.0, float(slot["count"]))
            failure_rate = _clamp(float(slot["failure_sum"]) / count)
            disagreement_mean = _clamp(float(slot["disagreement_sum"]) / count)
            entropy_mean = _clamp(float(slot["entropy_sum"]) / count)
            instant_energy = _clamp(alpha * failure_rate + beta * disagreement_mean + gamma * entropy_mean)

            prev = float(self._concept_energy_state.get(cluster, {}).get("ema_energy", instant_energy))
            ema = _clamp((decay * prev) + ((1.0 - decay) * instant_energy))
            trend = float(ema - prev)
            updated[cluster] = {
                "frequency": float(count),
                "failure_rate": float(failure_rate),
                "disagreement_mean": float(disagreement_mean),
                "entropy_mean": float(entropy_mean),
                "instant_energy": float(instant_energy),
                "ema_energy": float(ema),
                "trend": float(trend),
                "rising": float(1.0 if trend > 0.0 else 0.0),
            }

        self._concept_energy_state = dict(updated)
        return updated

    def _softmax_dict(self, values: Dict[str, float], *, temperature: float = 1.0) -> Dict[str, float]:
        if not values:
            return {}
        t = max(1e-3, float(temperature))
        max_v = max(values.values())
        exp_values = {k: math.exp((v - max_v) / t) for k, v in values.items()}
        total = sum(exp_values.values())
        if total <= 0.0:
            uniform = 1.0 / max(1, len(values))
            return {k: uniform for k in values}
        return {k: v / total for k, v in exp_values.items()}

    def _temporal_decay_weight(self, *, row: Mapping[str, Any], now_ts: datetime | None = None) -> float:
        ts_raw = row.get("ts")
        if ts_raw is None:
            return 1.0

        parsed = self._parse_ts(str(ts_raw))
        if parsed is None:
            return 1.0
        now = now_ts or datetime.now(timezone.utc)
        delta_days = max(0.0, (now - parsed).total_seconds() / 86400.0)
        half_life = max(1e-6, float(self.config.temporal_decay_half_life_days))
        decay = math.exp((-math.log(2.0) / half_life) * delta_days)
        return float(max(0.05, decay))

    def _rare_cluster_amplification(
        self,
        *,
        row: Mapping[str, Any],
        cluster_frequency: Mapping[str, int] | None,
    ) -> float:
        if not cluster_frequency:
            return 1.0
        clusters = row.get("concept_cluster", [])
        if not isinstance(clusters, list) or not clusters:
            return 1.0
        valid = [str(c).strip().lower() for c in clusters if str(c).strip()]
        if not valid:
            return 1.0
        freqs = [max(1, int(cluster_frequency.get(cluster, 1))) for cluster in valid]
        inv = sum(1.0 / f for f in freqs) / len(freqs)
        return float(1.0 + self.config.rare_cluster_amplification * inv)

    def rare_cluster_weight(
        self,
        row: Mapping[str, Any],
        cluster_frequency: Mapping[str, int] | None,
    ) -> float:
        """Inverse-frequency weighting used to amplify rare concept replay."""
        if not cluster_frequency:
            return 1.0
        clusters = row.get("concept_cluster", [])
        if not isinstance(clusters, list) or not clusters:
            return 1.0
        values: List[float] = []
        for cluster in clusters:
            key = str(cluster).strip().lower()
            if not key:
                continue
            freq = max(1, int(cluster_frequency.get(key, 1)))
            values.append(1.0 / math.sqrt(float(freq)))
        if not values:
            return 1.0
        mean = sum(values) / len(values)
        return float(max(float(self.config.rare_weight_floor), mean))

    def _row_cluster_frequency(self, row: Mapping[str, Any], frequency: Mapping[str, int]) -> Dict[str, int]:
        clusters = row.get("concept_cluster", [])
        if not isinstance(clusters, list):
            return {}
        out: Dict[str, int] = {}
        for cluster in clusters:
            key = str(cluster).strip().lower()
            if not key:
                continue
            out[key] = int(frequency.get(key, 0))
        return out

    def _row_cluster_energy(
        self,
        row: Mapping[str, Any],
        concept_energy_state: Mapping[str, Mapping[str, float]],
    ) -> float:
        clusters = row.get("concept_cluster", [])
        if not isinstance(clusters, list) or not clusters:
            return 0.0
        values: List[float] = []
        for cluster in clusters:
            key = str(cluster).strip().lower()
            if not key:
                continue
            values.append(float(concept_energy_state.get(key, {}).get("ema_energy", 0.0)))
        if not values:
            return 0.0
        return float(sum(values) / len(values))

    def _row_concept_energy_trend(
        self,
        row: Mapping[str, Any],
        concept_energy_state: Mapping[str, Mapping[str, float]],
    ) -> float:
        clusters = row.get("concept_cluster", [])
        if not isinstance(clusters, list) or not clusters:
            return 0.0
        values: List[float] = []
        for cluster in clusters:
            key = str(cluster).strip().lower()
            if not key:
                continue
            values.append(float(concept_energy_state.get(key, {}).get("trend", 0.0)))
        if not values:
            return 0.0
        return float(sum(values) / len(values))

    def _energy_rise_amplification(
        self,
        row: Mapping[str, Any],
        *,
        concept_energy_state: Mapping[str, Mapping[str, float]] | None,
    ) -> float:
        state = concept_energy_state or self._concept_energy_state
        if not state:
            return 1.0
        trend = max(0.0, self._row_concept_energy_trend(row, state))
        return float(1.0 + float(self.config.concept_energy_rising_amplification) * trend)

    def _fallback_secondary_provider(self, *, row: Mapping[str, Any], primary: str) -> str:
        ranked = row.get("ranked_providers")
        if isinstance(ranked, list):
            for item in ranked:
                if not isinstance(item, Mapping):
                    continue
                provider = str(item.get("provider", "")).strip()
                if provider and provider != primary:
                    return provider
        for candidate in ("mini", "openai", "openrouter", "groq", "anthropic"):
            if candidate != primary:
                return candidate
        return "mini"

    def _rare_cluster_weighting_payload(
        self,
        rows: Sequence[Mapping[str, Any]],
        cluster_frequency: Mapping[str, int],
    ) -> Dict[str, Any]:
        per_cluster = {
            cluster: float(1.0 / math.sqrt(max(1, int(freq))))
            for cluster, freq in sorted(cluster_frequency.items())
        }
        row_preview = []
        for row in rows[:200]:
            row_preview.append(
                {
                    "question": str(row.get("question", "")),
                    "rare_weight": float(self.rare_cluster_weight(row, cluster_frequency)),
                    "concept_cluster": (
                        list(row.get("concept_cluster", []))
                        if isinstance(row.get("concept_cluster"), list)
                        else []
                    ),
                }
            )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cluster_frequency": {k: int(v) for k, v in cluster_frequency.items()},
            "cluster_weight": per_cluster,
            "rows": row_preview,
        }

    def _disagreement_distribution_payload(self, rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        threshold = float(self.config.disagreement_entropy_threshold)
        entropy_gt = 0
        disagreement_rows = 0
        synthetic_rows = 0
        bins = {"low": 0, "medium": 0, "high": 0}
        for row in rows:
            entropy = _clamp(float(row.get("entropy", 0.0)))
            disagreement = _clamp(float(row.get("disagreement", 0.0)))
            if entropy > threshold:
                entropy_gt += 1
            if disagreement > 0.0:
                disagreement_rows += 1
            if bool(row.get("synthetic_disagreement", False)):
                synthetic_rows += 1
            if entropy < 0.33:
                bins["low"] += 1
            elif entropy < 0.66:
                bins["medium"] += 1
            else:
                bins["high"] += 1

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rows": int(len(rows)),
            "disagreement_rows": int(disagreement_rows),
            "synthetic_disagreement_rows": int(synthetic_rows),
            "entropy_gt_0_45_rows": int(entropy_gt),
            "entropy_bucket_counts": bins,
        }

    def _parse_ts(self, value: str) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
