"""Stratified dataset splitting for Mini training experiments."""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence


@dataclass(slots=True)
class SplitConfig:
    """Split ratios and reproducibility configuration."""

    train_ratio: float = 0.8
    val_ratio: float = 0.1
    test_ratio: float = 0.1
    seed: int = 42


class StratifiedDatasetSplitter:
    """Performs stratified train/val/test splitting by key metadata."""

    def __init__(self, *, output_dir: str = "data/mini_training/splits") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def split(self, rows: Sequence[Dict[str, Any]], config: SplitConfig | None = None) -> Dict[str, List[Dict[str, Any]]]:
        cfg = config or SplitConfig()
        self._validate_ratios(cfg)

        rng = random.Random(int(cfg.seed))
        strata: Dict[str, List[Dict[str, Any]]] = {}

        for row in rows:
            key = self._strata_key(row)
            strata.setdefault(key, []).append(dict(row))

        train: List[Dict[str, Any]] = []
        val: List[Dict[str, Any]] = []
        test: List[Dict[str, Any]] = []

        for key, bucket in strata.items():
            rng.shuffle(bucket)
            n = len(bucket)
            if n == 1:
                train.extend(bucket)
                continue

            n_train = int(round(n * cfg.train_ratio))
            n_val = int(round(n * cfg.val_ratio))
            n_test = n - n_train - n_val

            if n_train <= 0:
                n_train = 1
            if n_test < 0:
                n_test = 0
            if n_train + n_val + n_test > n:
                n_train = max(1, n - n_val - n_test)

            head = 0
            train.extend(bucket[head : head + n_train])
            head += n_train
            val.extend(bucket[head : head + n_val])
            head += n_val
            test.extend(bucket[head : head + n_test])
            head += n_test
            if head < n:
                test.extend(bucket[head:])

        # Mild balancing pass: keep hard/easy in each split when possible.
        train, val, test = self._balance_hard_easy(train, val, test)
        return {"train": train, "val": val, "test": test}

    def write_jsonl_splits(self, splits: Dict[str, Sequence[Dict[str, Any]]], *, prefix: str = "mini") -> Dict[str, Path]:
        out: Dict[str, Path] = {}
        for split_name, rows in splits.items():
            path = self.output_dir / f"{prefix}_{split_name}.jsonl"
            with path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(dict(row), ensure_ascii=True) + "\n")
            out[split_name] = path
        return out

    def write_csv_splits(self, splits: Dict[str, Sequence[Dict[str, Any]]], *, prefix: str = "mini") -> Dict[str, Path]:
        out: Dict[str, Path] = {}
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
            "hard_case",
        ]
        for split_name, rows in splits.items():
            path = self.output_dir / f"{prefix}_{split_name}.csv"
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
                            "hard_case": bool(row.get("hard_case", False)),
                        }
                    )
            out[split_name] = path
        return out

    def _strata_key(self, row: Dict[str, Any]) -> str:
        subject = str(row.get("subject", "general")).lower().strip() or "general"
        difficulty = str(row.get("difficulty", "unknown")).lower().strip() or "unknown"
        clusters = row.get("concept_cluster", [])
        primary_cluster = str(clusters[0]).lower().strip() if isinstance(clusters, list) and clusters else "general"
        hard_case = self._hard_case(row)
        return f"{subject}|{difficulty}|{primary_cluster}|hard={int(hard_case)}"

    def _hard_case(self, row: Dict[str, Any]) -> bool:
        if bool(row.get("hard_case", False)):
            return True
        if str(row.get("difficulty", "")).lower().strip() == "hard":
            return True
        if float(row.get("risk", 0.0)) >= 0.65:
            return True
        if float(row.get("entropy", 0.0)) >= 0.60:
            return True
        return False

    def _balance_hard_easy(
        self,
        train: List[Dict[str, Any]],
        val: List[Dict[str, Any]],
        test: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        # Keep implementation simple and deterministic; rebalance only when a split has zero hard cases.
        def has_hard(rows: Sequence[Dict[str, Any]]) -> bool:
            return any(self._hard_case(row) for row in rows)

        if not has_hard(val):
            donor_idx = next((idx for idx, row in enumerate(train) if self._hard_case(row)), None)
            if donor_idx is not None:
                val.append(train.pop(donor_idx))

        if not has_hard(test):
            donor_idx = next((idx for idx, row in enumerate(train) if self._hard_case(row)), None)
            if donor_idx is not None:
                test.append(train.pop(donor_idx))

        return train, val, test

    def _validate_ratios(self, cfg: SplitConfig) -> None:
        total = float(cfg.train_ratio + cfg.val_ratio + cfg.test_ratio)
        if abs(total - 1.0) > 1e-6:
            raise ValueError("train/val/test ratios must sum to 1.0")
        if cfg.train_ratio <= 0.0 or cfg.val_ratio < 0.0 or cfg.test_ratio < 0.0:
            raise ValueError("split ratios must be non-negative and train_ratio must be > 0")
