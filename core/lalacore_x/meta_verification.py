from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Sequence


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


class MetaVerificationLayer:
    """
    Deterministic error typing and memory logging.

    Guarantees:
    - No provider/API calls.
    - Structured error taxonomy for replay prioritization.
    - Concept-cluster weighted error frequency tracking.
    """

    PRIORITY = (
        "unit_mismatch",
        "boundary_condition_error",
        "algebraic_simplification_error",
        "logical_inconsistency",
        "concept_misclassification",
        "overconfidence_hallucination",
        "deterministic_failure",
        "incorrect_final_answer",
    )

    def __init__(self, root: str = "data/lc9"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

        self.memory_path = self.root / "LC9_ERROR_MEMORY.jsonl"
        self.stats_path = self.root / "LC9_ERROR_MEMORY_STATS.json"
        self.stats = self._load_stats()

    def classify(
        self,
        *,
        question: str,
        subject: str,
        difficulty: str,
        concept_clusters: Sequence[str],
        predicted_answer: str,
        predicted_confidence: float,
        verification: Dict,
        structure: Dict | None = None,
    ) -> Dict:
        structure = structure or {}
        stage = verification.get("stage_results", {}) or {}
        failure_reason = str(verification.get("failure_reason") or verification.get("reason") or "").lower()

        verified = bool(verification.get("verified"))
        confidence = _clamp(predicted_confidence)

        flags = set()
        if not verified:
            flags.add("incorrect_final_answer")
            flags.add("deterministic_failure")

        if ("unit" in failure_reason) or (stage.get("unit") is False):
            flags.add("unit_mismatch")

        if any(stage.get(key) is False for key in ("boundary", "extraneous_root", "graph_monotonicity", "optimization_sanity")):
            flags.add("boundary_condition_error")
        if "boundary" in failure_reason or "extraneous" in failure_reason or "domain" in failure_reason:
            flags.add("boundary_condition_error")

        symbolic_failed = stage.get("symbolic") is False
        numeric_passed = stage.get("numeric") is True
        if symbolic_failed and numeric_passed:
            flags.add("algebraic_simplification_error")
        if "simplif" in failure_reason or "algebra" in failure_reason:
            flags.add("algebraic_simplification_error")

        if float(structure.get("circular_reasoning", 0.0)) > 0.0:
            flags.add("logical_inconsistency")
        if float(structure.get("missing_inference_rate", 0.0)) >= 0.34:
            flags.add("logical_inconsistency")
        if float(structure.get("structural_coherence_score", 0.5)) < 0.33:
            flags.add("logical_inconsistency")

        subject_l = str(subject or "general").lower().strip()
        clusters = [str(c).lower().strip() for c in concept_clusters if str(c).strip()]
        if clusters and subject_l != "general":
            if not any(subject_l in c for c in clusters):
                # Question was routed to a subject but supporting clusters disagree.
                flags.add("concept_misclassification")

        if not verified and confidence >= 0.72:
            flags.add("overconfidence_hallucination")

        if not flags:
            flags.add("deterministic_failure" if not verified else "none")

        error_type = "none"
        for label in self.PRIORITY:
            if label in flags:
                error_type = label
                break

        return {
            "error_type": error_type,
            "error_flags": sorted(flags),
            "verified": verified,
            "predicted_confidence": confidence,
            "subject": subject_l,
            "difficulty": str(difficulty or "unknown").lower().strip(),
            "concept_clusters": clusters,
            "predicted_answer": str(predicted_answer or ""),
            "failure_reason": failure_reason,
            "structure": {
                "coherence": float(structure.get("structural_coherence_score", 0.0)),
                "missing_inference_rate": float(structure.get("missing_inference_rate", 0.0)),
                "redundancy_rate": float(structure.get("step_redundancy_rate", 0.0)),
                "circular_reasoning": float(structure.get("circular_reasoning", 0.0)),
            },
        }

    def log(self, payload: Dict) -> None:
        row = {"ts": _now(), **payload}
        with self.memory_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

        error_type = str(row.get("error_type", "unknown")).lower().strip() or "unknown"
        subject = str(row.get("subject", "general")).lower().strip()
        clusters = [str(c).lower().strip() for c in row.get("concept_clusters", []) if str(c).strip()]
        if not clusters:
            clusters = ["general"]

        self.stats["total"] = int(self.stats.get("total", 0)) + 1
        self.stats["error_counts"][error_type] = int(self.stats["error_counts"].get(error_type, 0)) + 1
        self.stats["by_subject"][subject] = int(self.stats["by_subject"].get(subject, 0)) + 1

        for cluster in clusters:
            c_row = self.stats["by_cluster"].setdefault(
                cluster,
                {
                    "total": 0,
                    "errors": {},
                },
            )
            c_row["total"] = int(c_row.get("total", 0)) + 1
            c_errors = c_row.setdefault("errors", {})
            c_errors[error_type] = int(c_errors.get(error_type, 0)) + 1

            sc_key = f"{subject}|{cluster}"
            sc_row = self.stats["by_subject_cluster"].setdefault(sc_key, {"total": 0, "errors": {}})
            sc_row["total"] = int(sc_row.get("total", 0)) + 1
            sc_errors = sc_row.setdefault("errors", {})
            sc_errors[error_type] = int(sc_errors.get(error_type, 0)) + 1

        self._save_stats()

    def error_weight(self, error_type: str, concept_clusters: Sequence[str] | None = None) -> float:
        """
        Cluster-weighted replay multiplier for a typed failure.
        """
        error_type = str(error_type or "unknown").lower().strip()
        concept_clusters = [str(c).lower().strip() for c in (concept_clusters or []) if str(c).strip()]

        global_total = max(1, int(self.stats.get("total", 0)))
        global_count = int(self.stats.get("error_counts", {}).get(error_type, 0))
        global_rate = global_count / global_total

        cluster_rates = []
        for cluster in concept_clusters:
            c_row = self.stats.get("by_cluster", {}).get(cluster, {})
            c_total = max(1, int(c_row.get("total", 0)))
            c_count = int(c_row.get("errors", {}).get(error_type, 0))
            cluster_rates.append(c_count / c_total)

        cluster_rate = sum(cluster_rates) / len(cluster_rates) if cluster_rates else global_rate
        combined = 0.45 * global_rate + 0.55 * cluster_rate
        # Higher-frequency typed errors get higher replay pressure, bounded.
        return _clamp(1.0 + 1.4 * combined, 0.75, 2.35)

    def summarize(self) -> Dict:
        return {
            "total": int(self.stats.get("total", 0)),
            "error_counts": dict(self.stats.get("error_counts", {})),
            "clusters": len(self.stats.get("by_cluster", {})),
        }

    def _load_stats(self) -> Dict:
        base = {
            "total": 0,
            "error_counts": {},
            "by_subject": {},
            "by_cluster": {},
            "by_subject_cluster": {},
        }
        if not self.stats_path.exists():
            return base
        try:
            payload = json.loads(self.stats_path.read_text(encoding="utf-8"))
        except Exception:
            return base
        for key, value in payload.items():
            if key in base and isinstance(value, dict):
                base[key].update(value)
            elif key in base:
                base[key] = value
        return base

    def _save_stats(self) -> None:
        self.stats_path.write_text(json.dumps(self.stats, indent=2, sort_keys=True), encoding="utf-8")

    def read_memory(self) -> List[Dict]:
        if not self.memory_path.exists():
            return []
        rows = []
        with self.memory_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows
