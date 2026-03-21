"""Shadow evaluation for trained Mini checkpoints against live traffic logs."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, List, Mapping


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _norm_question(question: str) -> str:
    return " ".join(str(question or "").strip().lower().split())


class MiniShadowEvaluator:
    """Evaluates candidate checkpoints in shadow mode without changing runtime model selection."""

    def __init__(
        self,
        *,
        shadow_log_path: str = "data/lc9/LC9_MINI_SHADOW_LOGS.jsonl",
        output_dir: str = "data/mini_training/shadow_eval",
    ) -> None:
        self.shadow_log_path = Path(shadow_log_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def evaluate_checkpoint(self, checkpoint_path: str, *, max_rows: int | None = None) -> Dict[str, Any]:
        """Score candidate checkpoint against shadow logs and return promotion readiness diagnostics."""
        model_state = self._load_model_state(Path(checkpoint_path))
        rows = self._read_jsonl(self.shadow_log_path)
        if max_rows is not None:
            rows = rows[: max(0, int(max_rows))]

        memory = dict(model_state.get("memory", {}))
        default_answer = str(model_state.get("default_answer", ""))
        default_conf = _clamp(float(model_state.get("default_confidence", 0.50)))

        total = len(rows)
        if total == 0:
            report = {
                "samples": 0,
                "mini_vs_winner_agreement": 0.0,
                "candidate_vs_winner_agreement": 0.0,
                "candidate_brier": 1.0,
                "confidence_correctness_correlation": 0.0,
                "upgrade_candidate_score": 0.0,
                "promotion_readiness_score": 0.0,
            }
            self.write_report(report)
            return report

        mini_match = 0
        candidate_match = 0
        brier_sum = 0.0
        candidate_confidences: List[float] = []
        candidate_correctness: List[float] = []

        for row in rows:
            question = _norm_question(str(row.get("question", "")))
            winner_answer = str(row.get("arena_winner_answer", "")).strip()
            mini_answer = str(row.get("mini_answer", "")).strip()
            winner_verified = bool(row.get("winner_verified", False))

            if mini_answer == winner_answer:
                mini_match += 1

            slot = memory.get(question, {})
            candidate_answer = str(slot.get("final_answer", default_answer)).strip()
            candidate_conf = _clamp(float(slot.get("confidence", default_conf)))
            correct = 1.0 if (winner_verified and candidate_answer == winner_answer) else 0.0

            if candidate_answer == winner_answer:
                candidate_match += 1

            brier_sum += (candidate_conf - correct) ** 2
            candidate_confidences.append(candidate_conf)
            candidate_correctness.append(correct)

        mini_agreement = mini_match / float(total)
        candidate_agreement = candidate_match / float(total)
        candidate_brier = brier_sum / float(total)
        corr = self._pearson(candidate_confidences, candidate_correctness)

        # Aggregate to a bounded readiness score; deterministic and transparent.
        upgrade_candidate_score = _clamp(
            (0.45 * candidate_agreement)
            + (0.25 * mini_agreement)
            + (0.20 * (1.0 - candidate_brier))
            + (0.10 * max(0.0, corr))
        )
        promotion_readiness_score = _clamp(
            (0.60 * upgrade_candidate_score)
            + (0.25 * (1.0 - candidate_brier))
            + (0.15 * candidate_agreement)
        )

        report = {
            "samples": int(total),
            "mini_vs_winner_agreement": float(mini_agreement),
            "candidate_vs_winner_agreement": float(candidate_agreement),
            "candidate_brier": float(candidate_brier),
            "confidence_correctness_correlation": float(corr),
            "upgrade_candidate_score": float(upgrade_candidate_score),
            "promotion_readiness_score": float(promotion_readiness_score),
        }
        self.write_report(report)
        return report

    def write_report(self, report: Mapping[str, Any], filename: str = "shadow_report.json") -> Path:
        path = self.output_dir / filename
        path.write_text(json.dumps(dict(report), indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _load_model_state(self, checkpoint_path: Path) -> Dict[str, Any]:
        if not checkpoint_path.exists():
            raise FileNotFoundError(f"checkpoint not found: {checkpoint_path}")

        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        model_state = payload.get("model_state", payload)
        if not isinstance(model_state, Mapping):
            raise ValueError("checkpoint payload does not contain a valid model_state mapping")
        return {
            "memory": dict(model_state.get("memory", {})),
            "default_answer": str(model_state.get("default_answer", "")),
            "default_confidence": _clamp(float(model_state.get("default_confidence", 0.50))),
        }

    def _read_jsonl(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if isinstance(payload, dict):
                    rows.append(payload)
        return rows

    def _pearson(self, x: List[float], y: List[float]) -> float:
        if len(x) != len(y) or not x:
            return 0.0
        n = float(len(x))
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
        den_x = math.sqrt(sum((a - mean_x) ** 2 for a in x))
        den_y = math.sqrt(sum((b - mean_y) ** 2 for b in y))
        denom = den_x * den_y
        if denom <= 1e-12:
            return 0.0
        return float(num / denom)
