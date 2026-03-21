from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Sequence


def _clamp(value, lo=0.0, hi=1.0):
    return max(lo, min(hi, value))


def _parse_ts(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


class MiniEvolutionEngine:
    """
    Mini shadow evolution control plane.

    Guarantees:
    - Shadow-only default behavior
    - Subject+difficulty-specific promotion gates
    - Replay prioritization with aging decay and cluster weakness weighting
    - Brier and calibration drift monitoring
    - No online heavy training loops
    """

    def __init__(
        self,
        state_path: str = "data/metrics/mini_evolution_state.json",
        disagreement_path: str = "data/replay/mini_disagreements.jsonl",
        replay_queue_path: str = "data/replay/mini_failure_queue.jsonl",
        replay_queue_cap: int = 5000,
    ):
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

        self.disagreement_path = Path(disagreement_path)
        self.disagreement_path.parent.mkdir(parents=True, exist_ok=True)

        self.replay_queue_path = Path(replay_queue_path)
        self.replay_queue_path.parent.mkdir(parents=True, exist_ok=True)
        self.replay_queue_cap = max(200, int(replay_queue_cap))

        self.state = self._load_state()

    # -----------------------------
    # Public API (backward-compatible)
    # -----------------------------

    def record_shadow_outcome(
        self,
        subject: str,
        difficulty: str,
        predicted_confidence: float,
        verified: bool,
        calibration_risk: float,
        disagreement_size: int,
        concept_clusters: Sequence[str],
    ) -> None:
        subject = str(subject or "general").lower().strip()
        difficulty = str(difficulty or "unknown").lower().strip()
        concept_clusters = [str(c).lower().strip() for c in concept_clusters if str(c).strip()]

        target = 1.0 if verified else 0.0
        pred = self.scale_shadow_confidence(subject, float(predicted_confidence))
        brier = (pred - target) ** 2
        calib_error = abs(pred - target)
        blended_calibration = 0.5 * calib_error + 0.5 * _clamp(float(calibration_risk))

        global_bucket = self.state["global"]
        global_bucket["total"] += 1
        global_bucket["ema_reliability"] = self._ema(global_bucket["ema_reliability"], target, alpha=0.08)
        global_bucket["ema_calibration_error"] = self._ema(global_bucket["ema_calibration_error"], blended_calibration, alpha=0.08)
        global_bucket["brier_short"] = self._ema(global_bucket["brier_short"], brier, alpha=0.20)
        global_bucket["brier_long"] = self._ema(global_bucket["brier_long"], brier, alpha=0.05)
        global_bucket["drift_score"] = _clamp(abs(global_bucket["brier_short"] - global_bucket["brier_long"]))

        instability = 0.0
        if not verified:
            instability += 0.4
        instability += 0.4 * _clamp(float(calibration_risk))
        instability += 0.2 * _clamp(disagreement_size / 6.0)
        global_bucket["instability_ema"] = self._ema(global_bucket["instability_ema"], instability, alpha=0.15)
        global_bucket["calibration_pressure"] = self._ema(
            global_bucket.get("calibration_pressure", 0.0),
            self._calibration_pressure_signal(pred=pred, target=target),
            alpha=0.18,
        )
        global_bucket["confidence_multiplier"] = self._confidence_multiplier_update(
            current=float(global_bucket.get("confidence_multiplier", 1.0)),
            pressure=float(global_bucket.get("calibration_pressure", 0.0)),
        )
        if instability >= 0.80:
            self._register_instability_rollback()

        sd_key = f"{subject}:{difficulty}"
        sd_bucket = self.state["subject_difficulty"].setdefault(sd_key, self._default_sd_bucket())
        self._update_bucket(sd_bucket, target, blended_calibration, brier, instability)

        d_bucket = self.state["difficulty"].setdefault(difficulty, self._default_sd_bucket())
        self._update_bucket(d_bucket, target, blended_calibration, brier, instability)

        self._update_cluster_stats(concept_clusters, verified)
        self._update_subject_calibration(subject, pred=pred, target=target)
        self._update_curriculum(sd_key=sd_key, concept_clusters=concept_clusters, difficulty=difficulty, disagreement_size=disagreement_size, target=target)
        self._adjust_replay_intensity(subject=subject, difficulty=difficulty, disagreement_size=disagreement_size, verified=verified)
        self._auto_tune_thresholds()
        self._save_state()

    def can_promote(self, subject: str, difficulty: str, concept_clusters: Sequence[str] | None = None) -> bool:
        subject = str(subject or "general").lower().strip()
        difficulty = str(difficulty or "unknown").lower().strip()
        concept_clusters = list(concept_clusters or [])

        sd_key = f"{subject}:{difficulty}"
        sd_bucket = self.state["subject_difficulty"].get(sd_key)
        if not sd_bucket:
            return False

        min_samples = self._min_samples_for_difficulty(difficulty)
        if int(sd_bucket.get("total", 0)) < min_samples:
            return False

        difficulty_bucket = self.state["difficulty"].get(difficulty, self._default_sd_bucket())

        threshold = self.promotion_threshold(subject, difficulty)
        brier_threshold = self._brier_threshold(difficulty)
        calibration_threshold = self._calibration_threshold(difficulty)

        reliability_ok = float(sd_bucket.get("ema_reliability", 0.0)) >= threshold
        difficulty_ok = float(difficulty_bucket.get("ema_reliability", 0.0)) >= max(0.50, threshold - 0.05)
        brier_ok = float(sd_bucket.get("brier", 1.0)) <= brier_threshold
        calibration_ok = float(sd_bucket.get("ema_calibration_error", 1.0)) <= calibration_threshold

        global_bucket = self.state["global"]
        drift_ok = float(global_bucket.get("drift_score", 1.0)) <= 0.22
        instability_ok = float(global_bucket.get("instability_ema", 1.0)) <= 0.35
        subject_instability_ok = float(sd_bucket.get("instability_ema", 1.0)) <= 0.38

        coverage_ok = self._cluster_coverage_ok(concept_clusters)
        calibration_stability_ok = self._subject_calibration_stable(subject)
        curriculum_ok = self._curriculum_gate(subject=subject, difficulty=difficulty)
        cooldown_ok = self._promotion_cooldown_ok()
        rollback_ok = self._rollback_window_ok()

        return bool(
            reliability_ok
            and difficulty_ok
            and brier_ok
            and calibration_ok
            and drift_ok
            and instability_ok
            and subject_instability_ok
            and coverage_ok
            and calibration_stability_ok
            and curriculum_ok
            and cooldown_ok
            and rollback_ok
        )

    def promotion_threshold(self, subject: str, difficulty: str) -> float:
        subject = str(subject or "general").lower().strip()
        difficulty = str(difficulty or "unknown").lower().strip()

        base = float(self.state["promotion_thresholds"].get(difficulty, 0.72))
        sd_key = f"{subject}:{difficulty}"
        sd_bucket = self.state["subject_difficulty"].get(sd_key, self._default_sd_bucket())
        global_bucket = self.state["global"]

        drift = float(global_bucket.get("drift_score", 0.0))
        calib = float(sd_bucket.get("ema_calibration_error", global_bucket.get("ema_calibration_error", 0.5)))
        instability = float(sd_bucket.get("instability_ema", global_bucket.get("instability_ema", 0.0)))

        threshold = base + 0.22 * drift + 0.12 * max(0.0, calib - 0.25) + 0.10 * instability
        return _clamp(threshold, 0.55, 0.96)

    def scale_shadow_confidence(self, subject: str, confidence: float) -> float:
        subject = str(subject or "general").lower().strip()
        global_multiplier = float(self.state["global"].get("confidence_multiplier", 1.0))
        subject_bias = float(self.state.get("subject_calibration", {}).get(subject, {}).get("slope", 0.0))
        adjusted = float(confidence) * global_multiplier + 0.15 * subject_bias
        return _clamp(adjusted, 0.01, 0.99)

    def weekly_adjustments(self) -> Dict:
        # Decay concept reinforcement and advance curriculum using recent stability.
        now = datetime.now(timezone.utc)
        reinforced = self.state.get("cluster_reinforcement", {})
        for cluster, row in reinforced.items():
            value = float(row.get("weight", 1.0))
            age_days = 0.0
            ts = _parse_ts(row.get("updated_ts"))
            if ts is not None:
                age_days = max(0.0, (now - ts).total_seconds() / 86400.0)
            decay = math.exp(-0.08 * age_days)
            row["weight"] = _clamp(value * decay, 0.7, 2.8)
            row["updated_ts"] = now.isoformat()

        self._recompute_curriculum_levels()
        self._auto_tune_thresholds()
        self._save_state()
        return {
            "replay_intensity": float(self.state["global"].get("replay_intensity", 0.15)),
            "confidence_multiplier": float(self.state["global"].get("confidence_multiplier", 1.0)),
            "calibration_pressure": float(self.state["global"].get("calibration_pressure", 0.0)),
            "curriculum_tracks": len(self.state.get("curriculum", {})),
        }

    def log_disagreement_case(self, payload: Dict) -> None:
        row = {"ts": datetime.now(timezone.utc).isoformat(), **payload}
        with self.disagreement_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def enqueue_failure(self, payload: Dict) -> None:
        clusters = [str(c).lower().strip() for c in payload.get("concept_clusters", []) if str(c).strip()]
        reinforced = [str(c).lower().strip() for c in payload.get("reinforced_clusters", []) if str(c).strip()]
        if reinforced:
            for c in reinforced:
                if c not in clusters:
                    clusters.append(c)

        calibration_risk = float(payload.get("calibration_risk", payload.get("risk", 1.0)))
        deterministic_fail = 1.0 if bool(payload.get("deterministic_fail", True)) else 0.0
        entropy = _clamp(float(payload.get("entropy", 0.0)))
        mini_disagreement = _clamp(float(payload.get("mini_disagreement", payload.get("disagreement", 0.0))))
        error_type = str(payload.get("error_type", "unknown")).lower().strip()
        error_weight = float(payload.get("error_weight", self._error_weight(error_type)))

        cluster_weight = self._cluster_weight(clusters)

        subject = str(payload.get("subject", "general")).lower().strip()
        difficulty = str(payload.get("difficulty", "unknown")).lower().strip()
        curriculum_level = int(payload.get("curriculum_level", self._infer_curriculum_level(difficulty, clusters, entropy, mini_disagreement)))

        subject_need = self._subject_need(subject, difficulty)
        difficulty_weight = self._difficulty_weight(difficulty)

        base_priority = (
            1.40 * deterministic_fail
            + 1.10 * _clamp(calibration_risk)
            + 0.85 * entropy
            + 0.65 * mini_disagreement
        ) * cluster_weight * subject_need * difficulty_weight * max(0.75, error_weight)

        row = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "base_priority": base_priority,
            "priority": base_priority,
            **payload,
            "concept_clusters": clusters,
            "subject": subject,
            "difficulty": difficulty,
            "error_type": error_type,
            "error_weight": error_weight,
            "curriculum_level": max(1, min(5, curriculum_level)),
        }

        with self.replay_queue_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
        self._trim_replay_queue()

        for cluster in clusters:
            bucket = self.state["cluster_stats"].setdefault(cluster, self._default_cluster_bucket())
            bucket["replay_coverage"] += 1
            self._reinforce_cluster(cluster, strength=0.16)

        self._save_state()

    def sample_replay_batch(self, max_items: int = 32, subject: str | None = None, difficulty: str | None = None) -> List[Dict]:
        rows = self._read_jsonl(self.replay_queue_path)
        if not rows:
            return []

        if subject and difficulty:
            unlocked = self._current_curriculum_level(subject=str(subject).lower().strip(), difficulty=str(difficulty).lower().strip())
            filtered = [r for r in rows if int(r.get("curriculum_level", 1)) <= unlocked]
            if filtered:
                rows = filtered

        intensity = self.replay_intensity(subject=subject, difficulty=difficulty)
        budget = max(1, int(max_items * intensity))

        scored = []
        now = datetime.now(timezone.utc)
        for row in rows:
            scored.append((self._effective_priority(row, now=now), row))

        scored.sort(key=lambda x: x[0], reverse=True)

        top = [row for _, row in scored[:budget]]
        tail = [row for _, row in scored[budget: budget + max_items]]

        if tail:
            weights = [max(1e-9, self._effective_priority(row, now=now)) for row in tail]
            sample_size = min(len(tail), max(1, budget // 2))
            sampled_tail = random.choices(tail, weights=weights, k=sample_size)
            return top + sampled_tail

        return top

    def replay_intensity(self, subject: str | None = None, difficulty: str | None = None) -> float:
        base = float(self.state["global"].get("replay_intensity", 0.15))
        base *= 1.0 + float(self.state["global"].get("calibration_pressure", 0.0))

        if not subject or not difficulty:
            return _clamp(base, 0.05, 0.92)

        subject = str(subject).lower().strip()
        difficulty = str(difficulty).lower().strip()

        sd_key = f"{subject}:{difficulty}"
        bucket = self.state["subject_difficulty"].get(sd_key)
        if not bucket:
            return _clamp(base, 0.05, 0.90)

        reliability = float(bucket.get("ema_reliability", 0.5))
        adjusted = base * (1.0 + (1.0 - reliability)) * self._difficulty_weight(difficulty)
        return _clamp(adjusted, 0.05, 0.90)

    def drift_score(self) -> float:
        return float(self.state["global"].get("drift_score", 0.0))

    def note_promotion(self) -> None:
        row = self.state.setdefault("promotion_state", self._default_promotion_state())
        row["last_promotion_ts"] = datetime.now(timezone.utc).isoformat()
        row["promotion_count"] = int(row.get("promotion_count", 0)) + 1
        self._save_state()

    # -----------------------------
    # Internal updates
    # -----------------------------

    def _update_bucket(self, bucket: Dict, target: float, blended_calibration: float, brier: float, instability: float) -> None:
        bucket["total"] += 1
        bucket["verified"] += int(target)
        bucket["ema_reliability"] = self._ema(bucket["ema_reliability"], target, alpha=0.10)
        bucket["ema_calibration_error"] = self._ema(bucket["ema_calibration_error"], blended_calibration, alpha=0.10)
        bucket["brier"] = self._ema(bucket["brier"], brier, alpha=0.10)
        bucket["instability_ema"] = self._ema(bucket["instability_ema"], instability, alpha=0.14)

    def _update_cluster_stats(self, concept_clusters: Sequence[str], verified: bool) -> None:
        target = 1.0 if verified else 0.0
        for cluster in concept_clusters:
            key = str(cluster).lower().strip()
            if not key:
                continue
            bucket = self.state["cluster_stats"].setdefault(key, self._default_cluster_bucket())
            bucket["total"] += 1
            bucket["ema_reliability"] = self._ema(bucket["ema_reliability"], target, alpha=0.12)
            if verified:
                bucket["last_success_ts"] = datetime.now(timezone.utc).isoformat()

    def _adjust_replay_intensity(self, subject: str, difficulty: str, disagreement_size: int, verified: bool) -> None:
        global_bucket = self.state["global"]
        drift = float(global_bucket.get("drift_score", 0.0))

        sd_key = f"{subject}:{difficulty}"
        sd_bucket = self.state["subject_difficulty"].get(sd_key, self._default_sd_bucket())
        reliability = float(sd_bucket.get("ema_reliability", 0.5))

        failure_signal = 0.0 if verified else 1.0
        disagreement_signal = _clamp(float(disagreement_size) / 6.0)

        target = (
            0.08
            + 0.45 * failure_signal
            + 0.30 * (1.0 - reliability)
            + 0.20 * disagreement_signal
            + 0.25 * drift
        )
        target *= self._difficulty_weight(difficulty)

        global_bucket["replay_intensity"] = self._ema(global_bucket.get("replay_intensity", 0.15), _clamp(target, 0.05, 0.90), alpha=0.18)

    def _auto_tune_thresholds(self) -> None:
        global_bucket = self.state["global"]
        drift = float(global_bucket.get("drift_score", 0.0))
        calib = float(global_bucket.get("ema_calibration_error", 0.5))
        instability = float(global_bucket.get("instability_ema", 0.0))

        adjustment = 0.14 * drift + 0.10 * max(0.0, calib - 0.25) + 0.08 * instability

        base = {"easy": 0.62, "medium": 0.70, "hard": 0.80}
        for diff, threshold in base.items():
            self.state["promotion_thresholds"][diff] = _clamp(threshold + adjustment, 0.55, 0.96)

    def _effective_priority(self, row: Dict, now: datetime) -> float:
        base = float(row.get("base_priority", row.get("priority", 0.0)))

        ts = _parse_ts(row.get("ts"))
        age_days = 0.0
        if ts is not None:
            age_days = max(0.0, (now - ts).total_seconds() / 86400.0)
        age_days = min(age_days, 3650.0)

        difficulty = str(row.get("difficulty", "unknown")).lower().strip()
        difficulty_weight = self._difficulty_weight(difficulty)

        subject = str(row.get("subject", "general")).lower().strip()
        subject_need = self._subject_need(subject, difficulty)

        clusters = [str(c).lower().strip() for c in row.get("concept_clusters", []) if str(c).strip()]
        cluster_need = self._cluster_weight(clusters)

        decay = math.exp(-self._decay_lambda(difficulty) * age_days)

        effective = base * decay * difficulty_weight * subject_need * cluster_need
        return max(0.0, effective)

    def _cluster_weight(self, concept_clusters: Sequence[str]) -> float:
        if not concept_clusters:
            return 1.0

        weights = []
        for cluster in concept_clusters:
            bucket = self.state["cluster_stats"].get(str(cluster).lower().strip())
            if not bucket:
                weights.append(1.0)
                continue

            reliability = float(bucket.get("ema_reliability", 0.5))
            replay_cov = float(bucket.get("replay_coverage", 0.0))
            reinforce_row = self.state.get("cluster_reinforcement", {}).get(str(cluster).lower().strip(), {})
            reinforce = float(reinforce_row.get("weight", 1.0))

            # More replay for weak + under-covered clusters.
            coverage_factor = 1.0 + 1.0 / (1.0 + replay_cov)
            weights.append((1.0 + (1.0 - reliability)) * coverage_factor * reinforce)

        return sum(weights) / len(weights)

    def _cluster_coverage_ok(self, concept_clusters: Sequence[str]) -> bool:
        if not concept_clusters:
            return True

        required = float(self.state["gates"].get("min_cluster_replay_coverage", 3))
        cov = []
        for cluster in concept_clusters:
            bucket = self.state["cluster_stats"].get(str(cluster).lower().strip(), self._default_cluster_bucket())
            cov.append(float(bucket.get("replay_coverage", 0.0)))

        if not cov:
            return False

        return (sum(cov) / len(cov)) >= required

    def _subject_calibration_stable(self, subject: str) -> bool:
        row = self.state.get("subject_calibration", {}).get(subject, {})
        slope = float(row.get("slope", 0.0))
        return abs(slope) <= 0.30

    def _update_subject_calibration(self, subject: str, pred: float, target: float) -> None:
        rows = self.state.setdefault("subject_calibration", {})
        row = rows.setdefault(
            subject,
            {
                "slope": 0.0,
                "intercept": 0.0,
                "total": 0,
            },
        )
        residual = target - pred
        row["slope"] = self._ema(row.get("slope", 0.0), residual, alpha=0.10)
        row["intercept"] = self._ema(row.get("intercept", 0.0), target - 0.5, alpha=0.08)
        row["total"] = int(row.get("total", 0)) + 1

    def _calibration_pressure_signal(self, pred: float, target: float) -> float:
        if target < 0.5 and pred >= 0.72:
            # overconfident and wrong -> raise pressure
            return 0.85
        if target >= 0.5 and pred <= 0.35:
            # underconfident and correct -> reduce pressure to relax scaling
            return 0.15
        return 0.40

    def _confidence_multiplier_update(self, current: float, pressure: float) -> float:
        if pressure >= 0.65:
            target = 0.92
        elif pressure <= 0.25:
            target = 1.05
        else:
            target = 1.0
        return _clamp(self._ema(current, target, alpha=0.12), 0.80, 1.20)

    def _reinforce_cluster(self, cluster: str, strength: float = 0.12) -> None:
        rows = self.state.setdefault("cluster_reinforcement", {})
        row = rows.setdefault(
            cluster,
            {
                "weight": 1.0,
                "updated_ts": datetime.now(timezone.utc).isoformat(),
            },
        )
        cur = float(row.get("weight", 1.0))
        row["weight"] = _clamp(cur + strength, 0.7, 2.8)
        row["updated_ts"] = datetime.now(timezone.utc).isoformat()

    def _error_weight(self, error_type: str) -> float:
        error_type = str(error_type or "unknown").lower().strip()
        if error_type == "unit_mismatch":
            return 1.35
        if error_type == "boundary_condition_error":
            return 1.30
        if error_type == "logical_inconsistency":
            return 1.28
        if error_type == "overconfidence_hallucination":
            return 1.20
        if error_type == "algebraic_simplification_error":
            return 1.16
        return 1.0

    def _subject_need(self, subject: str, difficulty: str) -> float:
        sd_key = f"{subject}:{difficulty}"
        bucket = self.state["subject_difficulty"].get(sd_key)
        if not bucket:
            return 1.2

        reliability = float(bucket.get("ema_reliability", 0.5))
        return 1.0 + (1.0 - reliability)

    def _difficulty_weight(self, difficulty: str) -> float:
        if difficulty == "hard":
            return 1.35
        if difficulty == "medium":
            return 1.10
        if difficulty == "easy":
            return 0.85
        return 1.00

    def _decay_lambda(self, difficulty: str) -> float:
        # Hard cases decay slower to stay longer in replay queue.
        if difficulty == "hard":
            return 0.015
        if difficulty == "medium":
            return 0.030
        return 0.055

    def _brier_threshold(self, difficulty: str) -> float:
        if difficulty == "hard":
            return 0.26
        if difficulty == "medium":
            return 0.22
        return 0.18

    def _calibration_threshold(self, difficulty: str) -> float:
        if difficulty == "hard":
            return 0.34
        if difficulty == "medium":
            return 0.30
        return 0.26

    def _min_samples_for_difficulty(self, difficulty: str) -> int:
        if difficulty == "easy":
            return 24
        if difficulty == "medium":
            return 36
        if difficulty == "hard":
            return 52
        return 40

    def _infer_curriculum_level(
        self,
        difficulty: str,
        concept_clusters: Sequence[str],
        entropy: float,
        disagreement: float,
    ) -> int:
        cluster_count = len(concept_clusters)
        if entropy >= 0.70 or disagreement >= 0.75:
            return 5
        if difficulty == "hard" and cluster_count >= 2:
            return 4
        if cluster_count >= 2:
            return 3
        if difficulty in {"medium", "hard"}:
            return 2
        return 1

    def _update_curriculum(
        self,
        sd_key: str,
        concept_clusters: Sequence[str],
        difficulty: str,
        disagreement_size: int,
        target: float,
    ) -> None:
        track = self.state.setdefault("curriculum", {}).setdefault(sd_key, self._default_curriculum_bucket())
        entropy_proxy = _clamp(disagreement_size / 5.0)
        level = self._infer_curriculum_level(
            difficulty=difficulty,
            concept_clusters=concept_clusters,
            entropy=entropy_proxy,
            disagreement=entropy_proxy,
        )
        self._update_curriculum_level_stats(track, level, target)
        self._advance_curriculum_if_stable(track)

    def _update_curriculum_level_stats(self, track: Dict, level: int, target: float) -> None:
        level = max(1, min(5, int(level)))
        levels = track.setdefault("levels", {str(i): self._default_curriculum_level_bucket() for i in range(1, 6)})
        # Solving a higher level case implies exposure to lower-level skills.
        for lvl in range(1, level + 1):
            row = levels.setdefault(str(lvl), self._default_curriculum_level_bucket())
            row["total"] += 1
            alpha = 0.12 if lvl == level else 0.08
            row["ema_reliability"] = self._ema(row["ema_reliability"], target, alpha=alpha)

    def _advance_curriculum_if_stable(self, track: Dict) -> None:
        unlocked = int(track.get("unlocked_level", 1))
        if unlocked >= 5:
            return
        levels = track.get("levels", {})
        for lvl in range(1, unlocked + 1):
            row = levels.get(str(lvl), self._default_curriculum_level_bucket())
            if int(row.get("total", 0)) < 16:
                return
            if float(row.get("ema_reliability", 0.5)) < 0.72:
                return
        track["unlocked_level"] = unlocked + 1

    def _recompute_curriculum_levels(self) -> None:
        for sd_key, track in self.state.get("curriculum", {}).items():
            self._advance_curriculum_if_stable(track)

    def _current_curriculum_level(self, subject: str, difficulty: str) -> int:
        sd_key = f"{subject}:{difficulty}"
        track = self.state.get("curriculum", {}).get(sd_key, self._default_curriculum_bucket())
        return max(1, min(5, int(track.get("unlocked_level", 1))))

    def _curriculum_gate(self, subject: str, difficulty: str) -> bool:
        required = 3 if difficulty == "hard" else 2
        return self._current_curriculum_level(subject, difficulty) >= required

    def _promotion_cooldown_ok(self) -> bool:
        row = self.state.get("promotion_state", self._default_promotion_state())
        last = _parse_ts(row.get("last_promotion_ts"))
        if last is None:
            return True
        cooldown_h = float(row.get("cooldown_hours", 24.0))
        elapsed_h = max(0.0, (datetime.now(timezone.utc) - last).total_seconds() / 3600.0)
        return elapsed_h >= cooldown_h

    def _rollback_window_ok(self) -> bool:
        row = self.state.get("promotion_state", self._default_promotion_state())
        rollback_until = _parse_ts(row.get("rollback_until_ts"))
        if rollback_until is None:
            return True
        return datetime.now(timezone.utc) >= rollback_until

    def _register_instability_rollback(self) -> None:
        row = self.state.setdefault("promotion_state", self._default_promotion_state())
        hours = float(row.get("rollback_hours", 48.0))
        until = datetime.now(timezone.utc) + timedelta(hours=max(1.0, hours))
        row["rollback_until_ts"] = until.isoformat()
        row["rollback_count"] = int(row.get("rollback_count", 0)) + 1

    def _read_jsonl(self, path: Path) -> List[Dict]:
        if not path.exists():
            return []

        rows = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        return rows

    def _load_state(self) -> Dict:
        if not self.state_path.exists():
            return self._default_state()

        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            data = self._default_state()

        return self._normalize_state(data)

    def _default_state(self) -> Dict:
        return {
            "global": {
                "ema_reliability": 0.5,
                "ema_calibration_error": 0.5,
                "brier_short": 0.5,
                "brier_long": 0.5,
                "drift_score": 0.0,
                "instability_ema": 0.0,
                "replay_intensity": 0.15,
                "calibration_pressure": 0.0,
                "confidence_multiplier": 1.0,
                "total": 0,
            },
            "subject_difficulty": {},
            "difficulty": {},
            "cluster_stats": {},
            "subject_calibration": {},
            "cluster_reinforcement": {},
            "curriculum": {},
            "promotion_state": self._default_promotion_state(),
            "promotion_thresholds": {
                "easy": 0.62,
                "medium": 0.70,
                "hard": 0.80,
            },
            "gates": {
                "min_cluster_replay_coverage": 3,
            },
        }

    def _normalize_state(self, state: Dict) -> Dict:
        base = self._default_state()

        for key, value in state.items():
            if key not in base:
                continue
            if isinstance(base[key], dict) and isinstance(value, dict):
                merged = dict(base[key])
                merged.update(value)
                base[key] = merged
            else:
                base[key] = value

        for key, bucket in list(base.get("subject_difficulty", {}).items()):
            merged = self._default_sd_bucket()
            merged.update(bucket)
            base["subject_difficulty"][key] = merged

        for key, bucket in list(base.get("difficulty", {}).items()):
            merged = self._default_sd_bucket()
            merged.update(bucket)
            base["difficulty"][key] = merged

        for key, bucket in list(base.get("cluster_stats", {}).items()):
            merged = self._default_cluster_bucket()
            merged.update(bucket)
            base["cluster_stats"][key] = merged

        for subject, row in list(base.get("subject_calibration", {}).items()):
            merged = {"slope": 0.0, "intercept": 0.0, "total": 0}
            if isinstance(row, dict):
                merged.update(row)
            base["subject_calibration"][subject] = merged

        for cluster, row in list(base.get("cluster_reinforcement", {}).items()):
            merged = {"weight": 1.0, "updated_ts": None}
            if isinstance(row, dict):
                merged.update(row)
            base["cluster_reinforcement"][cluster] = merged

        for sd_key, track in list(base.get("curriculum", {}).items()):
            merged = self._default_curriculum_bucket()
            if isinstance(track, dict):
                merged.update(track)
            levels = merged.get("levels", {})
            for i in range(1, 6):
                row = levels.get(str(i), {})
                d = self._default_curriculum_level_bucket()
                if isinstance(row, dict):
                    d.update(row)
                levels[str(i)] = d
            merged["levels"] = levels
            base["curriculum"][sd_key] = merged

        pstate = base.get("promotion_state", self._default_promotion_state())
        merged = self._default_promotion_state()
        if isinstance(pstate, dict):
            merged.update(pstate)
        base["promotion_state"] = merged

        return base

    def _save_state(self) -> None:
        self.state_path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")

    def _default_sd_bucket(self) -> Dict:
        return {
            "ema_reliability": 0.5,
            "ema_calibration_error": 0.5,
            "brier": 0.5,
            "instability_ema": 0.0,
            "total": 0,
            "verified": 0,
        }

    def _default_cluster_bucket(self) -> Dict:
        return {
            "ema_reliability": 0.5,
            "total": 0,
            "replay_coverage": 0,
            "last_success_ts": None,
        }

    def _default_curriculum_level_bucket(self) -> Dict:
        return {
            "ema_reliability": 0.5,
            "total": 0,
        }

    def _default_curriculum_bucket(self) -> Dict:
        return {
            "unlocked_level": 1,
            "levels": {str(i): self._default_curriculum_level_bucket() for i in range(1, 6)},
        }

    def _default_promotion_state(self) -> Dict:
        return {
            "last_promotion_ts": None,
            "rollback_until_ts": None,
            "cooldown_hours": 24.0,
            "rollback_hours": 48.0,
            "promotion_count": 0,
            "rollback_count": 0,
        }

    def _trim_replay_queue(self) -> None:
        rows = self._read_jsonl(self.replay_queue_path)
        if len(rows) <= self.replay_queue_cap:
            return
        keep = rows[-self.replay_queue_cap :]
        with self.replay_queue_path.open("w", encoding="utf-8") as f:
            for row in keep:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")

    def _ema(self, current, new, alpha):
        try:
            value = (1.0 - float(alpha)) * float(current) + float(alpha) * float(new)
        except Exception:
            value = float(new)
        if not math.isfinite(value):
            value = 0.0
        return max(-1_000_000.0, min(1_000_000.0, value))
