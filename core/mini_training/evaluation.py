"""Evaluation harness for offline Mini training and Kaggle-style reporting."""

from __future__ import annotations

import csv
import json
import math
import random
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence


try:  # pragma: no cover - optional dependency
    import sympy as sp
except Exception:  # pragma: no cover - optional dependency
    sp = None



def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


class MiniEvaluationHarness:
    """Computes exactness, calibration, and segmentation metrics for Mini checkpoints."""

    def __init__(self, *, output_dir: str = "data/mini_training/evaluation") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def evaluate(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        predictions: Sequence[Dict[str, Any]] | None = None,
        tag: str = "heldout",
        previous_metrics: Mapping[str, Any] | None = None,
        bootstrap_samples: int = 1000,
        bootstrap_seed: int = 42,
    ) -> Dict[str, Any]:
        if predictions is None:
            predictions = [
                {
                    "final_answer": str(row.get("final_answer", "")),
                    "confidence": float(max(0.0, min(1.0, row.get("confidence", 1.0 - float(row.get("risk", 1.0)))))),
                }
                for row in rows
            ]

        if len(predictions) != len(rows):
            raise ValueError("predictions length must match rows length")

        total = len(rows)
        correct = 0
        symbolic_correct = 0
        brier_sum = 0.0
        overconfident_wrong = 0
        hard_total = 0
        hard_correct = 0
        disagreement_total = 0
        disagreement_correct = 0
        multi_cluster_total = 0
        multi_cluster_correct = 0

        by_subject: Dict[str, List[int]] = {}
        by_difficulty: Dict[str, List[int]] = {}
        by_cluster: Dict[str, List[int]] = {}
        entropy_values: List[float] = []
        confidences: List[float] = []
        correctness: List[float] = []
        risk_values: List[float] = []
        hard_slice_confidences: List[float] = []
        hard_slice_correctness: List[float] = []
        rare_slice_confidences: List[float] = []
        rare_slice_correctness: List[float] = []
        class_pos_confidences: List[float] = []
        class_pos_correctness: List[float] = []
        class_neg_confidences: List[float] = []
        class_neg_correctness: List[float] = []

        cluster_frequency = self._cluster_frequency(rows)
        rare_clusters = self._rare_clusters(cluster_frequency)
        rare_total = 0
        rare_correct = 0

        entropy_bucket_counts: Dict[str, List[int]] = {
            "low": [0, 0],
            "medium": [0, 0],
            "high": [0, 0],
        }
        risk_bucket_scores: Dict[str, List[float]] = {"low": [], "medium": [], "high": []}

        for row, pred in zip(rows, predictions):
            truth = str(row.get("final_answer", "")).strip()
            guess = str(pred.get("final_answer", "")).strip()
            confidence = _clamp(float(pred.get("confidence", 0.0)))
            entropy = _clamp(float(row.get("entropy", 0.0)))
            risk = _clamp(float(row.get("risk", 0.0)))
            is_correct = self._exact_match(guess, truth)
            is_symbolic = self._symbolic_equivalent(guess, truth)

            if is_correct:
                correct += 1
            if is_symbolic:
                symbolic_correct += 1

            target = 1.0 if is_correct else 0.0
            brier_sum += (confidence - target) ** 2

            if confidence >= 0.8 and not is_correct:
                overconfident_wrong += 1

            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = row.get("concept_cluster", [])
            primary_cluster = str(clusters[0]).lower().strip() if isinstance(clusters, list) and clusters else "general"

            by_subject.setdefault(subject, [0, 0])[0] += int(is_correct)
            by_subject[subject][1] += 1
            by_difficulty.setdefault(difficulty, [0, 0])[0] += int(is_correct)
            by_difficulty[difficulty][1] += 1
            by_cluster.setdefault(primary_cluster, [0, 0])[0] += int(is_correct)
            by_cluster[primary_cluster][1] += 1

            hard_case = bool(row.get("hard_case", False)) or difficulty == "hard" or float(row.get("risk", 0.0)) >= 0.65
            if hard_case:
                hard_total += 1
                hard_correct += int(is_correct)
                hard_slice_confidences.append(confidence)
                hard_slice_correctness.append(float(is_correct))

            disagreement_case = float(row.get("disagreement", 0.0)) > 0.0 or bool(row.get("agreement_with_winner") is False)
            if disagreement_case:
                disagreement_total += 1
                disagreement_correct += int(is_correct)

            clusters = row.get("concept_cluster", [])
            if isinstance(clusters, list) and len(clusters) >= 2:
                multi_cluster_total += 1
                multi_cluster_correct += int(is_correct)
            if isinstance(clusters, list):
                if any(str(c).strip().lower() in rare_clusters for c in clusters):
                    rare_total += 1
                    rare_correct += int(is_correct)
                    rare_slice_confidences.append(confidence)
                    rare_slice_correctness.append(float(is_correct))

            entropy_bucket = self._entropy_bucket(entropy)
            entropy_bucket_counts[entropy_bucket][0] += int(is_correct)
            entropy_bucket_counts[entropy_bucket][1] += 1

            risk_bucket = self._risk_bucket(risk)
            risk_bucket_scores[risk_bucket].append((confidence - target) ** 2)

            entropy_values.append(entropy)
            confidences.append(confidence)
            correctness.append(target)
            risk_values.append(risk)
            if target >= 0.5:
                class_pos_confidences.append(confidence)
                class_pos_correctness.append(target)
            else:
                class_neg_confidences.append(confidence)
                class_neg_correctness.append(target)

        accuracy = (correct / total) if total else 0.0
        symbolic_accuracy = (symbolic_correct / total) if total else 0.0
        brier = (brier_sum / total) if total else 0.0
        overconfidence_rate = (overconfident_wrong / total) if total else 0.0
        hard_accuracy = (hard_correct / hard_total) if hard_total else 0.0
        disagreement_accuracy = (disagreement_correct / disagreement_total) if disagreement_total else 0.0
        rare_concept_accuracy = (rare_correct / rare_total) if rare_total else 0.0
        multi_cluster_accuracy = (multi_cluster_correct / multi_cluster_total) if multi_cluster_total else 0.0

        calibration_curve = self._calibration_curve(confidences, correctness, bins=10)
        reliability = self._reliability_diagram(confidences, correctness, bins=10)
        ece = self._ece_from_curve(calibration_curve, total)
        mce = self._mce_from_curve(calibration_curve)
        adaptive_curve = self._adaptive_calibration_curve(confidences, correctness, bins=10)
        adaptive_ece = self._ece_from_curve(adaptive_curve, total)
        adaptive_mce = self._mce_from_curve(adaptive_curve)
        hard_slice_ece = self._ece_from_lists(hard_slice_confidences, hard_slice_correctness)
        rare_cluster_ece = self._ece_from_lists(rare_slice_confidences, rare_slice_correctness)
        class_conditional_ece = {
            "positive_class_ece": float(self._ece_from_lists(class_pos_confidences, class_pos_correctness)),
            "negative_class_ece": float(self._ece_from_lists(class_neg_confidences, class_neg_correctness)),
        }
        temp_scaling = self._temperature_scaling_search(confidences, correctness)

        drift = self._calibration_drift(current_brier=brier, previous_metrics=previous_metrics)
        risk_stratified_brier = {
            key: float(sum(values) / len(values)) if values else 0.0 for key, values in risk_bucket_scores.items()
        }
        entropy_stratified_accuracy = self._as_accuracy_map(entropy_bucket_counts)

        bootstrap = self.bootstrap_confidence_intervals(
            rows,
            predictions,
            samples=max(50, int(bootstrap_samples)),
            seed=int(bootstrap_seed),
        )

        metrics = {
            "tag": str(tag),
            "samples": int(total),
            "exact_match": float(accuracy),
            "symbolic_equivalence": float(symbolic_accuracy),
            "calibration_brier": float(brier),
            "overconfidence_rate": float(overconfidence_rate),
            "hard_case_accuracy": float(hard_accuracy),
            "hard_slice_accuracy": float(hard_accuracy),
            "disagreement_slice_accuracy": float(disagreement_accuracy),
            "rare_concept_accuracy": float(rare_concept_accuracy),
            "multi_cluster_accuracy": float(multi_cluster_accuracy),
            "accuracy_by_subject": self._as_accuracy_map(by_subject),
            "accuracy_by_difficulty": self._as_accuracy_map(by_difficulty),
            "concept_cluster_accuracy": self._as_accuracy_map(by_cluster),
            "entropy_stratified_accuracy": entropy_stratified_accuracy,
            "calibration_curve": calibration_curve,
            "reliability_diagram": reliability,
            "calibration_adaptive_curve": adaptive_curve,
            "ece": float(ece),
            "mce": float(mce),
            "adaptive_ece": float(adaptive_ece),
            "adaptive_mce": float(adaptive_mce),
            "class_conditional_ece": class_conditional_ece,
            "rare_cluster_ece": float(rare_cluster_ece),
            "hard_slice_ece": float(hard_slice_ece),
            "temperature_scaling": temp_scaling,
            "risk_stratified_brier": risk_stratified_brier,
            "calibration_drift": drift,
            "entropy_distribution": self._distribution(entropy_values),
            "risk_distribution": self._distribution(risk_values),
            "confidence_correctness_correlation": float(self._pearson(confidences, correctness)),
            "bootstrap_ci": bootstrap,
            "calibration_stratified": {
                "adaptive_ece": float(adaptive_ece),
                "class_conditional_ece": class_conditional_ece,
                "rare_cluster_ece": float(rare_cluster_ece),
                "hard_slice_ece": float(hard_slice_ece),
                "temperature_scaling": temp_scaling,
            },
            "instability_flag": bool(bootstrap.get("instability_flag", False)),
        }
        return metrics

    def predict_with_memory_model(self, rows: Sequence[Dict[str, Any]], model_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        memory = dict(model_state.get("memory", {}))
        default_answer = str(model_state.get("default_answer", ""))
        default_conf = float(_clamp(float(model_state.get("default_confidence", 0.5))))

        predictions: List[Dict[str, Any]] = []
        for row in rows:
            question = self._norm_question(str(row.get("question", "")))
            slot = memory.get(question, {})
            predictions.append(
                {
                    "final_answer": str(slot.get("final_answer", default_answer)),
                    "confidence": float(_clamp(float(slot.get("confidence", default_conf)))),
                }
            )
        return predictions

    def write_metrics_json(self, metrics: Dict[str, Any], filename: str = "metrics.json") -> Path:
        path = self.output_dir / filename
        path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def write_calibration_diagnostics(self, metrics: Mapping[str, Any], filename: str = "calibration_diagnostics.json") -> Path:
        payload = {
            "ece": float(metrics.get("ece", 0.0)),
            "mce": float(metrics.get("mce", 0.0)),
            "adaptive_ece": float(metrics.get("adaptive_ece", 0.0)),
            "adaptive_mce": float(metrics.get("adaptive_mce", 0.0)),
            "class_conditional_ece": dict(metrics.get("class_conditional_ece", {}))
            if isinstance(metrics.get("class_conditional_ece"), Mapping)
            else {},
            "rare_cluster_ece": float(metrics.get("rare_cluster_ece", 0.0)),
            "hard_slice_ece": float(metrics.get("hard_slice_ece", 0.0)),
            "temperature_scaling": dict(metrics.get("temperature_scaling", {}))
            if isinstance(metrics.get("temperature_scaling"), Mapping)
            else {},
            "calibration_brier": float(metrics.get("calibration_brier", 0.0)),
            "risk_stratified_brier": dict(metrics.get("risk_stratified_brier", {})),
            "calibration_drift": dict(metrics.get("calibration_drift", {})) if isinstance(metrics.get("calibration_drift"), Mapping) else {},
            "calibration_curve": list(metrics.get("calibration_curve", [])),
            "calibration_adaptive_curve": list(metrics.get("calibration_adaptive_curve", [])),
        }
        path = self.output_dir / filename
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def write_calibration_stratified(
        self,
        metrics: Mapping[str, Any],
        filename: str = "calibration_stratified.json",
    ) -> Path:
        payload = metrics.get("calibration_stratified", {})
        if not isinstance(payload, Mapping):
            payload = {}
        path = self.output_dir / filename
        path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
        return path

    def write_kaggle_diagnostics(self, metrics: Mapping[str, Any], filename: str = "kaggle_diagnostics.json") -> Path:
        payload = {
            "exact_match": float(metrics.get("exact_match", 0.0)),
            "hard_slice_accuracy": float(metrics.get("hard_slice_accuracy", metrics.get("hard_case_accuracy", 0.0))),
            "disagreement_slice_accuracy": float(metrics.get("disagreement_slice_accuracy", 0.0)),
            "rare_concept_accuracy": float(metrics.get("rare_concept_accuracy", 0.0)),
            "multi_cluster_accuracy": float(metrics.get("multi_cluster_accuracy", 0.0)),
            "entropy_stratified_accuracy": dict(metrics.get("entropy_stratified_accuracy", {})),
            "bootstrap_ci": dict(metrics.get("bootstrap_ci", {})) if isinstance(metrics.get("bootstrap_ci"), Mapping) else {},
            "instability_flag": bool(metrics.get("instability_flag", False)),
        }
        path = self.output_dir / filename
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def write_leaderboard_csv(self, metrics: Dict[str, Any], filename: str = "leaderboard.csv") -> Path:
        path = self.output_dir / filename
        rows = [
            ["metric", "value"],
            ["samples", metrics.get("samples", 0)],
            ["exact_match", metrics.get("exact_match", 0.0)],
            ["symbolic_equivalence", metrics.get("symbolic_equivalence", 0.0)],
            ["calibration_brier", metrics.get("calibration_brier", 0.0)],
            ["overconfidence_rate", metrics.get("overconfidence_rate", 0.0)],
            ["hard_case_accuracy", metrics.get("hard_case_accuracy", 0.0)],
            ["confidence_correctness_correlation", metrics.get("confidence_correctness_correlation", 0.0)],
        ]
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        return path

    def write_leaderboard_simulation_csv(self, metrics: Mapping[str, Any], filename: str = "leaderboard_simulation.csv") -> Path:
        path = self.output_dir / filename
        rows = [
            ["projection_metric", "value"],
            ["exact_match", float(metrics.get("exact_match", 0.0))],
            ["hard_slice_accuracy", float(metrics.get("hard_slice_accuracy", metrics.get("hard_case_accuracy", 0.0)))],
            ["disagreement_slice_accuracy", float(metrics.get("disagreement_slice_accuracy", 0.0))],
            ["rare_concept_accuracy", float(metrics.get("rare_concept_accuracy", 0.0))],
            ["multi_cluster_accuracy", float(metrics.get("multi_cluster_accuracy", 0.0))],
            ["ece", float(metrics.get("ece", 0.0))],
            ["mce", float(metrics.get("mce", 0.0))],
        ]
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        return path

    def write_kaggle_submission(
        self,
        rows: Sequence[Dict[str, Any]],
        predictions: Sequence[Dict[str, Any]],
        filename: str = "kaggle_submission.csv",
    ) -> Path:
        if len(rows) != len(predictions):
            raise ValueError("rows and predictions must have identical lengths")

        path = self.output_dir / filename
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "prediction"])
            for idx, pred in enumerate(predictions):
                row_id = rows[idx].get("id", idx)
                writer.writerow([row_id, str(pred.get("final_answer", ""))])
        return path

    def bootstrap_confidence_intervals(
        self,
        rows: Sequence[Dict[str, Any]],
        predictions: Sequence[Dict[str, Any]],
        *,
        samples: int = 1000,
        seed: int = 42,
    ) -> Dict[str, Any]:
        """Bootstrap 95% confidence intervals for exact match, hard-case accuracy, ECE, and MCE."""
        if len(rows) != len(predictions):
            raise ValueError("rows and predictions must have identical lengths")
        if not rows:
            return {
                "exact_match_ci95": [0.0, 0.0],
                "hard_case_accuracy_ci95": [0.0, 0.0],
                "ece_ci95": [0.0, 0.0],
                "mce_ci95": [0.0, 0.0],
                "exact_match_mean": 0.0,
                "exact_match_std": 0.0,
                "hard_case_accuracy_mean": 0.0,
                "hard_case_accuracy_std": 0.0,
                "ece_mean": 0.0,
                "ece_std": 0.0,
                "mce_mean": 0.0,
                "mce_std": 0.0,
                "instability_flag": False,
            }

        rng = random.Random(int(seed))
        exact_vals: List[float] = []
        hard_vals: List[float] = []
        ece_vals: List[float] = []
        mce_vals: List[float] = []
        n = len(rows)

        for _ in range(max(1, int(samples))):
            idxs = [rng.randrange(0, n) for _ in range(n)]
            resampled_rows = [rows[i] for i in idxs]
            resampled_preds = [predictions[i] for i in idxs]
            sample_metrics = self._evaluate_sample_metrics(resampled_rows, resampled_preds)
            exact_vals.append(sample_metrics["exact_match"])
            hard_vals.append(sample_metrics["hard_case_accuracy"])
            ece_vals.append(sample_metrics["ece"])
            mce_vals.append(sample_metrics["mce"])

        exact_ci = self._quantile_ci(exact_vals)
        hard_ci = self._quantile_ci(hard_vals)
        ece_ci = self._quantile_ci(ece_vals)
        mce_ci = self._quantile_ci(mce_vals)

        instability_flag = (exact_ci[1] - exact_ci[0]) > 0.15 or (hard_ci[1] - hard_ci[0]) > 0.20

        exact_mean, exact_std = self._mean_std(exact_vals)
        hard_mean, hard_std = self._mean_std(hard_vals)
        ece_mean, ece_std = self._mean_std(ece_vals)
        mce_mean, mce_std = self._mean_std(mce_vals)
        return {
            "exact_match_ci95": [float(exact_ci[0]), float(exact_ci[1])],
            "hard_case_accuracy_ci95": [float(hard_ci[0]), float(hard_ci[1])],
            "ece_ci95": [float(ece_ci[0]), float(ece_ci[1])],
            "mce_ci95": [float(mce_ci[0]), float(mce_ci[1])],
            "exact_match_mean": float(exact_mean),
            "exact_match_std": float(exact_std),
            "hard_case_accuracy_mean": float(hard_mean),
            "hard_case_accuracy_std": float(hard_std),
            "ece_mean": float(ece_mean),
            "ece_std": float(ece_std),
            "mce_mean": float(mce_mean),
            "mce_std": float(mce_std),
            "instability_flag": bool(instability_flag),
            "samples": int(max(1, int(samples))),
        }

    def _exact_match(self, predicted: str, expected: str) -> bool:
        return predicted.strip() == expected.strip()

    def _symbolic_equivalent(self, predicted: str, expected: str) -> bool:
        if self._exact_match(predicted, expected):
            return True
        if sp is None:
            return False
        try:
            left = sp.simplify(sp.sympify(predicted))
            right = sp.simplify(sp.sympify(expected))
            return bool(sp.simplify(left - right) == 0)
        except Exception:
            return False

    def _as_accuracy_map(self, buckets: Dict[str, List[int]]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for key, value in buckets.items():
            correct, total = int(value[0]), int(value[1])
            out[key] = float(correct / total) if total else 0.0
        return out

    def _calibration_curve(self, confidences: Sequence[float], correctness: Sequence[float], *, bins: int = 10) -> List[Dict[str, float]]:
        out: List[Dict[str, float]] = []
        for idx in range(max(1, int(bins))):
            lo = idx / float(bins)
            hi = (idx + 1) / float(bins)
            selected = [i for i, conf in enumerate(confidences) if (conf >= lo and (conf < hi or (idx == bins - 1 and conf <= hi)))]
            if not selected:
                out.append({"bin": float(idx), "avg_confidence": 0.0, "accuracy": 0.0, "count": 0.0})
                continue
            avg_conf = sum(float(confidences[i]) for i in selected) / len(selected)
            avg_acc = sum(float(correctness[i]) for i in selected) / len(selected)
            out.append(
                {
                    "bin": float(idx),
                    "avg_confidence": float(avg_conf),
                    "accuracy": float(avg_acc),
                    "count": float(len(selected)),
                }
            )
        return out

    def _reliability_diagram(self, confidences: Sequence[float], correctness: Sequence[float], *, bins: int = 10) -> List[Dict[str, float]]:
        curve = self._calibration_curve(confidences, correctness, bins=bins)
        out: List[Dict[str, float]] = []
        for row in curve:
            out.append(
                {
                    "bin": row["bin"],
                    "gap": float(abs(row["avg_confidence"] - row["accuracy"])),
                    "count": row["count"],
                }
            )
        return out

    def _distribution(self, values: Sequence[float]) -> Dict[str, float]:
        vals = [float(v) for v in values]
        if not vals:
            return {"min": 0.0, "max": 0.0, "mean": 0.0, "std": 0.0}
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        return {
            "min": float(min(vals)),
            "max": float(max(vals)),
            "mean": float(mean),
            "std": float(math.sqrt(var)),
        }

    def _pearson(self, x: Sequence[float], y: Sequence[float]) -> float:
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

    def _evaluate_sample_metrics(
        self,
        rows: Sequence[Dict[str, Any]],
        predictions: Sequence[Dict[str, Any]],
    ) -> Dict[str, float]:
        total = len(rows)
        if total <= 0:
            return {"exact_match": 0.0, "hard_case_accuracy": 0.0, "ece": 0.0, "mce": 0.0}

        correct = 0
        hard_total = 0
        hard_correct = 0
        confidences: List[float] = []
        correctness: List[float] = []

        for row, pred in zip(rows, predictions):
            truth = str(row.get("final_answer", "")).strip()
            guess = str(pred.get("final_answer", "")).strip()
            conf = _clamp(float(pred.get("confidence", 0.0)))
            ok = 1.0 if self._exact_match(guess, truth) else 0.0
            correct += int(ok)
            confidences.append(conf)
            correctness.append(ok)

            difficulty = str(row.get("difficulty", "unknown")).lower().strip()
            hard_case = bool(row.get("hard_case", False)) or difficulty == "hard" or float(row.get("risk", 0.0)) >= 0.65
            if hard_case:
                hard_total += 1
                hard_correct += int(ok)

        curve = self._calibration_curve(confidences, correctness, bins=10)
        return {
            "exact_match": float(correct / total),
            "hard_case_accuracy": float(hard_correct / hard_total) if hard_total else 0.0,
            "ece": float(self._ece_from_curve(curve, total)),
            "mce": float(self._mce_from_curve(curve)),
        }

    def _quantile_ci(self, values: Sequence[float], lo_q: float = 0.025, hi_q: float = 0.975) -> tuple[float, float]:
        vals = sorted(float(v) for v in values)
        if not vals:
            return (0.0, 0.0)
        lo_idx = int(max(0, min(len(vals) - 1, round((len(vals) - 1) * lo_q))))
        hi_idx = int(max(0, min(len(vals) - 1, round((len(vals) - 1) * hi_q))))
        return float(vals[lo_idx]), float(vals[hi_idx])

    def _ece_from_curve(self, curve: Sequence[Mapping[str, float]], total: int) -> float:
        if total <= 0:
            return 0.0
        ece = 0.0
        for row in curve:
            count = float(row.get("count", 0.0))
            if count <= 0:
                continue
            gap = abs(float(row.get("avg_confidence", 0.0)) - float(row.get("accuracy", 0.0)))
            ece += (count / float(total)) * gap
        return float(ece)

    def _mce_from_curve(self, curve: Sequence[Mapping[str, float]]) -> float:
        gaps = [abs(float(row.get("avg_confidence", 0.0)) - float(row.get("accuracy", 0.0))) for row in curve if float(row.get("count", 0.0)) > 0]
        if not gaps:
            return 0.0
        return float(max(gaps))

    def _adaptive_calibration_curve(
        self,
        confidences: Sequence[float],
        correctness: Sequence[float],
        *,
        bins: int = 10,
    ) -> List[Dict[str, float]]:
        n = len(confidences)
        if n == 0:
            return []
        paired = sorted((float(c), float(y)) for c, y in zip(confidences, correctness))
        b = max(1, int(bins))
        step = max(1, n // b)
        out: List[Dict[str, float]] = []
        for idx in range(0, n, step):
            chunk = paired[idx : min(n, idx + step)]
            if not chunk:
                continue
            conf_avg = sum(item[0] for item in chunk) / len(chunk)
            acc_avg = sum(item[1] for item in chunk) / len(chunk)
            out.append(
                {
                    "bin": float(len(out)),
                    "avg_confidence": float(conf_avg),
                    "accuracy": float(acc_avg),
                    "count": float(len(chunk)),
                }
            )
        return out

    def _ece_from_lists(self, confidences: Sequence[float], correctness: Sequence[float], *, bins: int = 10) -> float:
        if len(confidences) != len(correctness) or not confidences:
            return 0.0
        curve = self._calibration_curve(confidences, correctness, bins=bins)
        return float(self._ece_from_curve(curve, len(confidences)))

    def _temperature_scaling_search(
        self,
        confidences: Sequence[float],
        correctness: Sequence[float],
    ) -> Dict[str, Any]:
        if len(confidences) != len(correctness) or not confidences:
            return {
                "best_temperature": 1.0,
                "raw_brier": 0.0,
                "calibrated_brier": 0.0,
                "improvement": 0.0,
            }

        raw_brier = self._brier(confidences, correctness)
        best_temp = 1.0
        best_brier = raw_brier
        for step in range(5, 61):
            temperature = step / 20.0  # 0.25 .. 3.0
            calibrated = [self._apply_temperature(conf, temperature) for conf in confidences]
            brier = self._brier(calibrated, correctness)
            if brier < best_brier:
                best_brier = brier
                best_temp = temperature

        return {
            "best_temperature": float(best_temp),
            "raw_brier": float(raw_brier),
            "calibrated_brier": float(best_brier),
            "improvement": float(raw_brier - best_brier),
        }

    def _apply_temperature(self, confidence: float, temperature: float) -> float:
        conf = _clamp(float(confidence), 1e-6, 1.0 - 1e-6)
        temp = max(1e-6, float(temperature))
        logit = math.log(conf / (1.0 - conf))
        scaled = logit / temp
        return _clamp(1.0 / (1.0 + math.exp(-scaled)))

    def _brier(self, confidences: Sequence[float], correctness: Sequence[float]) -> float:
        if len(confidences) != len(correctness) or not confidences:
            return 0.0
        err = 0.0
        for conf, corr in zip(confidences, correctness):
            err += (_clamp(float(conf)) - _clamp(float(corr))) ** 2
        return float(err / len(confidences))

    def _mean_std(self, values: Sequence[float]) -> tuple[float, float]:
        vals = [float(v) for v in values]
        if not vals:
            return (0.0, 0.0)
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        return (float(mean), float(math.sqrt(max(0.0, var))))

    def select_checkpoint_by_bootstrap(
        self,
        candidates: Sequence[Mapping[str, Any]],
        *,
        filename: str = "bootstrap_selection.json",
    ) -> Dict[str, Any]:
        """Variance-aware selection: maximize hard-case mean - 1.5 * std."""
        rows: List[Dict[str, Any]] = []
        best_idx = -1
        best_score = float("-inf")

        for idx, candidate in enumerate(candidates):
            bootstrap = candidate.get("bootstrap_ci", {})
            if not isinstance(bootstrap, Mapping):
                bootstrap = {}
            hard_mean = float(
                bootstrap.get(
                    "hard_case_accuracy_mean",
                    candidate.get("hard_case_accuracy", 0.0),
                )
            )
            hard_std = float(bootstrap.get("hard_case_accuracy_std", 0.0))
            score = hard_mean - 1.5 * hard_std
            row = {
                "epoch": int(candidate.get("epoch", idx + 1)),
                "hard_case_accuracy_mean": float(hard_mean),
                "hard_case_accuracy_std": float(hard_std),
                "selection_score": float(score),
            }
            rows.append(row)
            if score > best_score:
                best_score = score
                best_idx = idx

        selected = rows[best_idx] if 0 <= best_idx < len(rows) else {}
        payload = {
            "rows": rows,
            "selected": selected,
            "selection_formula": "hard_case_accuracy_mean - 1.5 * hard_case_accuracy_std",
        }
        path = self.output_dir / filename
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        payload["path"] = str(path)
        return payload

    def _calibration_drift(self, *, current_brier: float, previous_metrics: Mapping[str, Any] | None) -> Dict[str, Any]:
        prev = 0.0
        if isinstance(previous_metrics, Mapping):
            prev = float(previous_metrics.get("calibration_brier", 0.0))
        delta = float(current_brier - prev)
        return {
            "previous_brier": float(prev),
            "current_brier": float(current_brier),
            "delta": float(delta),
            "drift_flag": bool(delta > 0.02),
        }

    def _cluster_frequency(self, rows: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
        freq: Dict[str, int] = {}
        for row in rows:
            clusters = row.get("concept_cluster", [])
            if not isinstance(clusters, list):
                continue
            for cluster in clusters:
                key = str(cluster).strip().lower()
                if not key:
                    continue
                freq[key] = freq.get(key, 0) + 1
        return freq

    def _rare_clusters(self, frequency: Mapping[str, int]) -> set[str]:
        if not frequency:
            return set()
        values = sorted(int(v) for v in frequency.values())
        cutoff = values[max(0, int(len(values) * 0.25) - 1)] if values else 0
        return {k for k, v in frequency.items() if int(v) <= int(max(1, cutoff))}

    def _entropy_bucket(self, entropy: float) -> str:
        value = _clamp(entropy)
        if value < 0.33:
            return "low"
        if value < 0.66:
            return "medium"
        return "high"

    def _risk_bucket(self, risk: float) -> str:
        value = _clamp(risk)
        if value < 0.33:
            return "low"
        if value < 0.66:
            return "medium"
        return "high"

    def _norm_question(self, question: str) -> str:
        return " ".join(str(question or "").strip().lower().split())
