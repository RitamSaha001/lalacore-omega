"""Mini training dataset extraction from LC9 logs for offline research workflows."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple



def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()



def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None



def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


@dataclass(slots=True)
class DatasetBuildConfig:
    """Filtering and extraction settings for dataset construction."""

    require_verified: bool = True
    risk_threshold: float = 0.35
    entropy_threshold: float = 0.60
    hard_negative_risk_threshold: float = 0.70
    hard_negative_entropy_threshold: float = 0.60
    disagreement_entropy_threshold: float = 0.45
    include_synthetic_disagreement_rows: bool = True
    strict: bool = False


@dataclass(slots=True)
class DatasetBuildResult:
    """In-memory dataset and derived subsets for training workflows."""

    rows: List[Dict[str, Any]]
    hard_negatives: List[Dict[str, Any]]
    disagreement_cases: List[Dict[str, Any]]
    trap_cases: List[Dict[str, Any]]
    stats: Dict[str, Any]


class MiniTrainingDatasetBuilder:
    """Builds SFT/distillation-ready records from LC9 telemetry logs."""

    def __init__(
        self,
        *,
        solver_debug_path: str = "data/lc9/LC9_SOLVER_DEBUG.jsonl",
        automation_hooks_path: str = "data/lc9/LC9_AUTOMATION_HOOK_EVENTS.jsonl",
        mini_shadow_logs_path: str = "data/lc9/LC9_MINI_SHADOW_LOGS.jsonl",
        arena_shadow_disagreements_path: str = "data/lc9/LC9_ARENA_SHADOW_DISAGREEMENTS.jsonl",
        deterministic_vs_provider_gap_path: str = "data/lc9/LC9_DETERMINISTIC_VS_PROVIDER_GAP.jsonl",
        reasoning_divergence_clusters_path: str = "data/lc9/LC9_REASONING_DIVERGENCE_CLUSTERS.jsonl",
        rare_cluster_cross_provider_path: str = "data/lc9/LC9_RARE_CLUSTER_CROSS_PROVIDER.jsonl",
        output_dir: str = "data/mini_training/datasets",
    ) -> None:
        self.solver_debug_path = Path(solver_debug_path)
        self.automation_hooks_path = Path(automation_hooks_path)
        self.mini_shadow_logs_path = Path(mini_shadow_logs_path)
        self.arena_shadow_disagreements_path = Path(arena_shadow_disagreements_path)
        self.deterministic_vs_provider_gap_path = Path(deterministic_vs_provider_gap_path)
        self.reasoning_divergence_clusters_path = Path(reasoning_divergence_clusters_path)
        self.rare_cluster_cross_provider_path = Path(rare_cluster_cross_provider_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def build_dataset(self, config: DatasetBuildConfig | None = None) -> DatasetBuildResult:
        cfg = config or DatasetBuildConfig()

        solver_rows, solver_errors = self._read_jsonl(self.solver_debug_path)
        hook_rows, hook_errors = self._read_jsonl(self.automation_hooks_path)
        shadow_rows, shadow_errors = self._read_jsonl(self.mini_shadow_logs_path)
        arena_shadow_rows, arena_shadow_errors = self._read_jsonl(self.arena_shadow_disagreements_path)
        provider_gap_rows, provider_gap_errors = self._read_jsonl(self.deterministic_vs_provider_gap_path)
        divergence_rows, divergence_errors = self._read_jsonl(self.reasoning_divergence_clusters_path)
        rare_cross_rows, rare_cross_errors = self._read_jsonl(self.rare_cluster_cross_provider_path)

        automation_index = self._build_automation_index(hook_rows)
        solver_index = self._build_solver_index(solver_rows)

        rows: List[Dict[str, Any]] = []
        rows.extend(self._build_from_shadow(shadow_rows, automation_index, solver_index))
        rows.extend(self._build_from_provider_gap(provider_gap_rows))
        rows.extend(self._build_from_arena_shadow(arena_shadow_rows))
        rows.extend(self._build_from_reasoning_divergence(divergence_rows))
        rows.extend(self._build_from_rare_cross_provider(rare_cross_rows))
        rows.extend(self._build_from_solver(solver_rows))

        deduped = self._dedupe_rows(rows)

        if cfg.require_verified:
            filtered = [row for row in deduped if bool(row.get("verified", False))]
        else:
            filtered = list(deduped)

        filtered = [
            row
            for row in filtered
            if float(row.get("risk", 1.0)) < float(cfg.risk_threshold)
            and float(row.get("entropy", 1.0)) < float(cfg.entropy_threshold)
        ]

        synthetic_disagreement_rows: List[Dict[str, Any]] = []
        if bool(cfg.include_synthetic_disagreement_rows):
            synthetic_disagreement_rows = self._synthesize_disagreement_rows(
                filtered,
                entropy_threshold=cfg.disagreement_entropy_threshold,
            )
            filtered = self._dedupe_rows(list(filtered) + synthetic_disagreement_rows)

        hard_source = list(deduped) + list(synthetic_disagreement_rows)
        hard_negatives = self.extract_hard_negatives(
            hard_source,
            risk_threshold=cfg.hard_negative_risk_threshold,
            entropy_threshold=cfg.hard_negative_entropy_threshold,
        )
        disagreement_cases = self.extract_disagreement_cases(
            list(deduped) + list(synthetic_disagreement_rows),
            entropy_threshold=cfg.disagreement_entropy_threshold,
        )
        trap_cases = self.extract_trap_cases(deduped)

        parse_errors = int(
            solver_errors
            + hook_errors
            + shadow_errors
            + arena_shadow_errors
            + provider_gap_errors
            + divergence_errors
            + rare_cross_errors
        )
        if cfg.strict and parse_errors > 0:
            raise ValueError(f"Dataset parsing encountered {parse_errors} malformed JSONL lines")

        stats = {
            "ts": _utc_now(),
            "total_raw": len(deduped),
            "total_filtered": len(filtered),
            "hard_negatives": len(hard_negatives),
            "disagreement_cases": len(disagreement_cases),
            "synthetic_disagreement_rows": len(synthetic_disagreement_rows),
            "trap_cases": len(trap_cases),
            "parse_errors": parse_errors,
            "sources": {
                "solver_debug_rows": len(solver_rows),
                "automation_hook_rows": len(hook_rows),
                "mini_shadow_rows": len(shadow_rows),
                "arena_shadow_rows": len(arena_shadow_rows),
                "provider_gap_rows": len(provider_gap_rows),
                "reasoning_divergence_rows": len(divergence_rows),
                "rare_cross_provider_rows": len(rare_cross_rows),
            },
        }

        return DatasetBuildResult(
            rows=filtered,
            hard_negatives=hard_negatives,
            disagreement_cases=disagreement_cases,
            trap_cases=trap_cases,
            stats=stats,
        )

    def extract_hard_negatives(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        risk_threshold: float = 0.70,
        entropy_threshold: float = 0.60,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            verified = bool(row.get("verified", False))
            risk = float(row.get("risk", 1.0))
            entropy = float(row.get("entropy", 0.0))
            disagreement = float(row.get("disagreement", 0.0))
            near_miss = bool(not verified and risk < 0.45 and float(row.get("confidence", 0.0)) > 0.65)
            if (not verified) or risk >= risk_threshold or entropy >= entropy_threshold or disagreement > 0.0 or near_miss:
                copy_row = dict(row)
                copy_row["hard_negative"] = True
                copy_row["near_miss"] = bool(near_miss)
                out.append(copy_row)
        return out

    def extract_disagreement_cases(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        entropy_threshold: float = 0.45,
    ) -> List[Dict[str, Any]]:
        return [
            dict(row)
            for row in rows
            if (
                float(row.get("disagreement", 0.0)) > 0.0
                or bool(row.get("agreement_with_winner") is False)
                or float(row.get("entropy", 0.0)) > float(entropy_threshold)
            )
        ]

    def extract_trap_cases(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        trap_tokens = ("trap", "except", "not correct", "all of the above", "none of the above")
        for row in rows:
            question = str(row.get("question", "")).lower()
            clusters = [str(c).lower() for c in row.get("concept_cluster", [])]
            if "trap" in clusters or any(tok in question for tok in trap_tokens):
                out.append(dict(row))
        return out

    def write_jsonl(self, rows: Sequence[Dict[str, Any]], filename: str) -> Path:
        path = self.output_dir / filename
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(dict(row), ensure_ascii=True) + "\n")
        return path

    def write_kaggle_csv(self, rows: Sequence[Dict[str, Any]], filename: str = "mini_training_dataset.csv") -> Path:
        path = self.output_dir / filename
        fieldnames = [
            "question",
            "final_answer",
            "reasoning_summary",
            "winner_provider",
            "verified",
            "entropy",
            "disagreement",
            "risk",
            "concept_cluster",
            "difficulty",
            "subject",
            "source",
        ]
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "question": str(row.get("question", "")),
                        "final_answer": str(row.get("final_answer", "")),
                        "reasoning_summary": str(row.get("reasoning_summary", "")),
                        "winner_provider": str(row.get("winner_provider", "")),
                        "verified": bool(row.get("verified", False)),
                        "entropy": float(row.get("entropy", 0.0)),
                        "disagreement": float(row.get("disagreement", 0.0)),
                        "risk": float(row.get("risk", 1.0)),
                        "concept_cluster": "|".join(str(c) for c in row.get("concept_cluster", [])),
                        "difficulty": str(row.get("difficulty", "unknown")),
                        "subject": str(row.get("subject", "general")),
                        "source": str(row.get("source", "unknown")),
                    }
                )
        return path

    def _build_from_shadow(
        self,
        shadow_rows: Sequence[Dict[str, Any]],
        automation_index: Dict[str, List[Dict[str, Any]]],
        solver_index: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in shadow_rows:
            question = str(row.get("question", "")).strip()
            if not question:
                continue

            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = self._normalized_clusters(row.get("concept_cluster"))
            if not clusters:
                clusters = self._infer_concept_clusters(question)

            ts = _parse_ts(row.get("ts"))
            automation = self._match_automation_event(
                subject=subject,
                difficulty=difficulty,
                event_ts=ts,
                automation_index=automation_index,
            )
            solver_meta = solver_index.get(question)

            entropy = self._coalesce_float(
                row.get("entropy"),
                (automation or {}).get("entropy"),
                (solver_meta or {}).get("entropy"),
                default=1.0,
            )
            disagreement = self._coalesce_float(
                (automation or {}).get("disagreement_case_count"),
                row.get("disagreement", 0.0),
                default=0.0,
            )
            risk = self._coalesce_float(
                row.get("risk"),
                row.get("uncertainty"),
                (solver_meta or {}).get("risk"),
                default=(0.05 if bool(row.get("winner_verified", False)) else 1.0),
            )

            winner_verified = bool(
                self._coalesce_bool(
                    row.get("winner_verified"),
                    row.get("mini_verified"),
                    (automation or {}).get("winner_verified"),
                    (solver_meta or {}).get("verified"),
                    default=False,
                )
            )

            final_answer = str(
                row.get("arena_winner_answer")
                or row.get("mini_answer")
                or (solver_meta or {}).get("final_answer")
                or ""
            ).strip()
            reasoning_summary = str(
                row.get("mini_reasoning")
                or (solver_meta or {}).get("reasoning_summary")
                or ""
            ).strip()

            if not final_answer:
                continue

            out.append(
                {
                    "question": question,
                    "final_answer": final_answer,
                    "reasoning_summary": self._trim(reasoning_summary, 320),
                    "winner_provider": str(row.get("arena_winner_provider") or row.get("winner_provider") or "mini"),
                    "verified": winner_verified,
                    "entropy": float(max(0.0, min(1.0, entropy))),
                    "disagreement": float(max(0.0, min(1.0, disagreement))),
                    "risk": float(max(0.0, min(1.0, risk))),
                    "concept_cluster": clusters,
                    "difficulty": difficulty,
                    "subject": subject,
                    "source": "mini_shadow_logs",
                    "confidence": float(max(0.0, min(1.0, _safe_float(row.get("mini_confidence"), 0.0)))),
                    "winner_margin": float(max(0.0, min(1.0, _safe_float(row.get("winner_margin"), 0.0)))),
                    "agreement_with_winner": bool(row.get("agreement_with_winner", True)),
                    "deterministic_verified": bool((automation or {}).get("winner_verified", winner_verified)),
                    "arena_posteriors": dict(row.get("arena_posteriors", {}))
                    if isinstance(row.get("arena_posteriors"), dict)
                    else {},
                    "ranked_providers": list(row.get("ranked_providers", []))
                    if isinstance(row.get("ranked_providers"), list)
                    else [],
                }
            )
        return out

    def _build_from_solver(self, solver_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in solver_rows:
            if str(row.get("event_type", "")) != "provider_output":
                continue
            question = str(row.get("question", "")).strip()
            final_answer = str(row.get("extracted_answer", "")).strip()
            if not question or not final_answer:
                continue

            verified_val = row.get("verification")
            risk = row.get("risk")
            entropy = row.get("entropy")
            subject = self._infer_subject(question)
            difficulty = self._infer_difficulty(question)

            out.append(
                {
                    "question": question,
                    "final_answer": final_answer,
                    "reasoning_summary": self._trim(str(row.get("raw_output", "")), 320),
                    "winner_provider": str(row.get("provider", "unknown")),
                    "verified": bool(verified_val) if verified_val is not None else False,
                    "entropy": float(max(0.0, min(1.0, _safe_float(entropy, 1.0)))),
                    "disagreement": 0.0,
                    "risk": float(max(0.0, min(1.0, _safe_float(risk, 1.0)))),
                    "concept_cluster": self._infer_concept_clusters(question),
                    "difficulty": difficulty,
                    "subject": subject,
                    "source": "solver_debug",
                    "confidence": 1.0 - float(max(0.0, min(1.0, _safe_float(risk, 1.0)))),
                    "winner_margin": 0.0,
                    "agreement_with_winner": True,
                    "deterministic_verified": bool(verified_val),
                    "arena_posteriors": {},
                    "ranked_providers": [],
                }
            )
        return out

    def _build_from_provider_gap(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            question = str(row.get("question", "")).strip()
            final_answer = str(row.get("winner_answer", "")).strip()
            winner_provider = str(row.get("winner_provider", "")).strip() or "unknown"
            if not question or not final_answer:
                continue

            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = self._normalized_clusters(row.get("concept_cluster"))
            if not clusters:
                clusters = self._infer_concept_clusters(question)

            answer_mismatch = bool(row.get("answer_mismatch", False))
            disagreement = float(max(0.0, min(1.0, _safe_float(row.get("disagreement"), 1.0 if answer_mismatch else 0.4))))
            risk = float(max(0.0, min(1.0, _safe_float(row.get("provider_risk"), 1.0 if answer_mismatch else 0.35))))
            verified = bool(row.get("winner_verified", False))

            out.append(
                {
                    "question": question,
                    "final_answer": final_answer,
                    "reasoning_summary": self._trim(
                        f"gap:{row.get('provider','unknown')} ans={row.get('provider_answer','')}",
                        320,
                    ),
                    "winner_provider": winner_provider,
                    "verified": verified,
                    "entropy": float(max(0.0, min(1.0, _safe_float(row.get("entropy"), 0.6)))),
                    "disagreement": disagreement,
                    "risk": risk,
                    "concept_cluster": clusters,
                    "difficulty": difficulty,
                    "subject": subject,
                    "source": "deterministic_vs_provider_gap",
                    "confidence": float(max(0.0, min(1.0, 1.0 - risk))),
                    "winner_margin": float(max(0.0, min(1.0, _safe_float(row.get("winner_margin"), 0.0)))),
                    "agreement_with_winner": not answer_mismatch,
                    "deterministic_verified": verified,
                    "arena_posteriors": {
                        winner_provider: float(max(0.0, min(1.0, _safe_float(row.get("winner_posterior"), 0.55)))),
                        str(row.get("provider", "peer")): float(max(0.0, min(1.0, _safe_float(row.get("provider_posterior"), 0.45)))),
                    },
                    "ranked_providers": [
                        {"provider": winner_provider, "score": float(max(0.0, min(1.0, _safe_float(row.get("winner_posterior"), 0.55))))},
                        {"provider": str(row.get("provider", "peer")), "score": float(max(0.0, min(1.0, _safe_float(row.get("provider_posterior"), 0.45))))},
                    ],
                    "contrast_provider": str(row.get("provider", "")),
                    "contrast_answer": str(row.get("provider_answer", "")),
                }
            )
        return out

    def _build_from_arena_shadow(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            question = str(row.get("question", "")).strip()
            final_answer = str(row.get("winner_answer", "")).strip()
            if not question or not final_answer:
                continue
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = self._normalized_clusters(row.get("concept_cluster"))
            if not clusters:
                clusters = self._infer_concept_clusters(question)

            verified = bool(row.get("winner_verified", False))
            entropy = float(max(0.0, min(1.0, _safe_float(row.get("entropy"), 0.6))))
            disagreement = float(max(0.0, min(1.0, _safe_float(row.get("disagreement"), 0.0))))
            risk = 0.15 if verified else 0.95
            out.append(
                {
                    "question": question,
                    "final_answer": final_answer,
                    "reasoning_summary": self._trim(f"shadow providers={int(row.get('provider_count', 0))}", 320),
                    "winner_provider": str(row.get("winner_provider", "unknown")).strip() or "unknown",
                    "verified": verified,
                    "entropy": entropy,
                    "disagreement": disagreement,
                    "risk": risk,
                    "concept_cluster": clusters,
                    "difficulty": difficulty,
                    "subject": subject,
                    "source": "arena_shadow_disagreement",
                    "confidence": float(max(0.0, min(1.0, 1.0 - risk))),
                    "winner_margin": float(max(0.0, min(1.0, _safe_float(row.get("winner_margin"), 0.0)))),
                    "agreement_with_winner": True,
                    "deterministic_verified": verified,
                    "arena_posteriors": {},
                    "ranked_providers": [],
                }
            )
        return out

    def _build_from_reasoning_divergence(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            question = str(row.get("question", "")).strip()
            final_answer = str(row.get("winner_answer", "")).strip()
            if not question or not final_answer:
                continue
            provider_count = int(_safe_float(row.get("count"), 0))
            if provider_count < 2:
                continue
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            clusters = self._normalized_clusters(row.get("concept_cluster"))
            if not clusters:
                clusters = self._infer_concept_clusters(question)
            entropy = float(max(0.0, min(1.0, _safe_float(row.get("entropy"), 0.6))))
            disagreement = float(max(0.0, min(1.0, _safe_float(row.get("disagreement"), 0.6))))
            verified = bool(int(_safe_float(row.get("verified_count"), 0)) > 0)
            risk = 0.20 if verified else 0.90
            out.append(
                {
                    "question": question,
                    "final_answer": final_answer,
                    "reasoning_summary": self._trim(f"divergence answer={row.get('normalized_answer', '')}", 320),
                    "winner_provider": str(row.get("winner_provider", "unknown")).strip() or "unknown",
                    "verified": verified,
                    "entropy": entropy,
                    "disagreement": disagreement,
                    "risk": risk,
                    "concept_cluster": clusters,
                    "difficulty": difficulty,
                    "subject": subject,
                    "source": "reasoning_divergence_cluster",
                    "confidence": float(max(0.0, min(1.0, 1.0 - risk))),
                    "winner_margin": 0.0,
                    "agreement_with_winner": True,
                    "deterministic_verified": verified,
                    "arena_posteriors": {},
                    "ranked_providers": [],
                }
            )
        return out

    def _build_from_rare_cross_provider(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            question = str(row.get("question", "")).strip()
            final_answer = str(row.get("winner_answer", "")).strip()
            if not question or not final_answer:
                continue
            rare_clusters = self._normalized_clusters(row.get("rare_clusters"))
            if not rare_clusters:
                continue
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            verified = bool(row.get("winner_verified", False))
            entropy = float(max(0.0, min(1.0, _safe_float(row.get("entropy"), 0.7))))
            disagreement = float(max(0.0, min(1.0, _safe_float(row.get("disagreement"), 0.6))))
            risk = 0.15 if verified else 0.95
            out.append(
                {
                    "question": question,
                    "final_answer": final_answer,
                    "reasoning_summary": self._trim("rare_cluster_cross_provider", 320),
                    "winner_provider": str(row.get("winner_provider", "unknown")).strip() or "unknown",
                    "verified": verified,
                    "entropy": entropy,
                    "disagreement": disagreement,
                    "risk": risk,
                    "concept_cluster": rare_clusters,
                    "difficulty": difficulty,
                    "subject": subject,
                    "source": "rare_cluster_cross_provider",
                    "confidence": float(max(0.0, min(1.0, 1.0 - risk))),
                    "winner_margin": 0.0,
                    "agreement_with_winner": True,
                    "deterministic_verified": verified,
                    "arena_posteriors": {},
                    "ranked_providers": [],
                }
            )
        return out

    def _build_automation_index(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        index: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            subject = str(row.get("subject", "general")).lower().strip() or "general"
            difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
            key = f"{subject}|{difficulty}"
            index.setdefault(key, []).append(row)

        for key in index:
            index[key].sort(key=lambda row: _parse_ts(row.get("ts")) or datetime.min.replace(tzinfo=timezone.utc))
        return index

    def _build_solver_index(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        index: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if str(row.get("event_type", "")) != "provider_output":
                continue
            question = str(row.get("question", "")).strip()
            if not question:
                continue
            existing = index.get(question, {})
            candidate = {
                "final_answer": str(row.get("extracted_answer", "")).strip(),
                "reasoning_summary": self._trim(str(row.get("raw_output", "")), 300),
                "verified": row.get("verification"),
                "risk": row.get("risk"),
                "entropy": row.get("entropy"),
            }
            index[question] = {**existing, **{k: v for k, v in candidate.items() if v is not None and v != ""}}
        return index

    def _match_automation_event(
        self,
        *,
        subject: str,
        difficulty: str,
        event_ts: datetime | None,
        automation_index: Dict[str, List[Dict[str, Any]]],
        max_gap_s: float = 480.0,
    ) -> Dict[str, Any] | None:
        key = f"{subject}|{difficulty}"
        rows = automation_index.get(key, [])
        if not rows:
            return None
        if event_ts is None:
            return rows[-1]

        nearest: Dict[str, Any] | None = None
        nearest_gap = float("inf")
        for row in rows:
            ts = _parse_ts(row.get("ts"))
            if ts is None:
                continue
            gap = abs((event_ts - ts).total_seconds())
            if gap < nearest_gap:
                nearest = row
                nearest_gap = gap
        if nearest is None:
            return None
        if nearest_gap > float(max_gap_s):
            return None
        return nearest

    def _dedupe_rows(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            key = (
                str(row.get("question", "")).strip().lower(),
                str(row.get("final_answer", "")).strip().lower(),
                str(row.get("winner_provider", "")).strip().lower(),
                str(row.get("source", "")).strip().lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(dict(row))
        return out

    def _read_jsonl(self, path: Path) -> Tuple[List[Dict[str, Any]], int]:
        rows: List[Dict[str, Any]] = []
        errors = 0
        if not path.exists():
            return rows, errors

        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    errors += 1
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
        return rows, errors

    def _infer_subject(self, question: str) -> str:
        q = str(question or "").lower()
        if any(tok in q for tok in ("integral", "derivative", "matrix", "equation", "sin", "cos", "tan")):
            return "math"
        if any(tok in q for tok in ("force", "velocity", "acceleration", "current", "voltage")):
            return "physics"
        if any(tok in q for tok in ("mole", "acid", "base", "equilibrium", "organic")):
            return "chemistry"
        return "general"

    def _infer_difficulty(self, question: str) -> str:
        q = str(question or "").lower()
        score = 0
        score += len(re.findall(r"[\+\-\*/\^=]", q))
        if any(tok in q for tok in ("prove", "derive", "greatest integer", "limit", "differential")):
            score += 4
        if len(q.split()) > 25:
            score += 3
        if score >= 12:
            return "hard"
        if score >= 6:
            return "medium"
        return "easy"

    def _infer_concept_clusters(self, question: str) -> List[str]:
        q = str(question or "").lower()
        clusters = []
        table = {
            "algebra": ("equation", "polynomial", "roots", "factor"),
            "calculus": ("integral", "derivative", "limit", "differential"),
            "trigonometry": ("sin", "cos", "tan", "asin", "acos", "atan"),
            "geometry": ("triangle", "circle", "angle", "chord"),
            "mechanics": ("force", "velocity", "acceleration"),
            "electro": ("current", "voltage", "field", "charge"),
            "equilibrium": ("equilibrium", "acid", "base", "reaction"),
            "trap": ("except", "not correct", "all of the above", "none of the above", "trap"),
        }
        for label, needles in table.items():
            if any(needle in q for needle in needles):
                clusters.append(label)
        if not clusters:
            clusters.append("general")
        return clusters

    def _normalized_clusters(self, value: Any) -> List[str]:
        if isinstance(value, list):
            out = [str(v).lower().strip() for v in value if str(v).strip()]
        elif isinstance(value, str):
            out = [str(value).lower().strip()] if value.strip() else []
        else:
            out = []
        dedup: List[str] = []
        seen = set()
        for cluster in out:
            if cluster in seen:
                continue
            seen.add(cluster)
            dedup.append(cluster)
        return dedup

    def _coalesce_float(self, *values: Any, default: float) -> float:
        for value in values:
            if value is None:
                continue
            try:
                return float(value)
            except Exception:
                continue
        return float(default)

    def _coalesce_bool(self, *values: Any, default: bool) -> bool:
        for value in values:
            if value is None:
                continue
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"true", "1", "yes"}:
                return True
            if text in {"false", "0", "no"}:
                return False
        return bool(default)

    def _trim(self, text: str, limit: int) -> str:
        value = str(text or "").strip()
        if len(value) <= int(limit):
            return value
        return value[: int(limit)]

    def _synthesize_disagreement_rows(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        entropy_threshold: float,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            entropy = float(max(0.0, min(1.0, _safe_float(row.get("entropy"), 0.0))))
            disagreement = float(max(0.0, min(1.0, _safe_float(row.get("disagreement"), 0.0))))
            disagreement_candidate = (
                disagreement > 0.0
                or bool(row.get("agreement_with_winner") is False)
                or entropy > float(entropy_threshold)
            )
            if not disagreement_candidate:
                continue

            provider = str(row.get("winner_provider", "mini")).strip() or "mini"
            ranked = row.get("ranked_providers", [])
            alt_provider = "mini" if provider != "mini" else "peer"
            if isinstance(ranked, list):
                for item in ranked:
                    if isinstance(item, dict):
                        p = str(item.get("provider", "")).strip()
                        if p and p != provider:
                            alt_provider = p
                            break

            synthetic = dict(row)
            synthetic["source"] = f"{str(row.get('source', 'unknown')).strip() or 'unknown'}_synthetic_disagreement"
            synthetic["synthetic_disagreement"] = True
            synthetic["synthetic_variant"] = "top2_provider_swap"
            synthetic["disagreement"] = float(max(disagreement, 1.0 if entropy > float(entropy_threshold) else 0.6))
            synthetic["disagreement_augmented"] = True
            synthetic["disagreement_entropy_flag"] = bool(entropy > float(entropy_threshold))
            synthetic["arena_posteriors"] = {
                alt_provider: 0.55,
                provider: 0.45,
            }
            synthetic["ranked_providers"] = [
                {"provider": alt_provider, "score": 0.55},
                {"provider": provider, "score": 0.45},
            ]
            out.append(synthetic)
        return out
