"""Synthetic augmentation utilities for Mini training sets (train split only)."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Sequence


@dataclass(slots=True)
class AugmentationConfig:
    """Controls augmentation behavior and reproducibility."""

    max_aug_per_row: int = 1
    noise_probability: float = 0.20
    difficulty_scaling: bool = True
    seed: int = 42


class SyntheticAugmentor:
    """Creates optional synthetic training variants without touching val/test rows."""

    def __init__(self, paraphraser: Callable[[str], str] | None = None) -> None:
        self.paraphraser = paraphraser

    def augment_training_rows(
        self,
        rows: Sequence[Dict[str, Any]],
        config: AugmentationConfig | None = None,
    ) -> List[Dict[str, Any]]:
        cfg = config or AugmentationConfig()
        rng = random.Random(int(cfg.seed))

        out: List[Dict[str, Any]] = [dict(row) for row in rows]
        for row in rows:
            question = str(row.get("question", "")).strip()
            if not question:
                continue

            variants = self._candidate_variants(question, rng=rng, cfg=cfg)
            variants = variants[: int(max(0, cfg.max_aug_per_row))]
            for idx, variant in enumerate(variants, start=1):
                aug = dict(row)
                aug["question"] = variant
                aug["source"] = f"{row.get('source', 'unknown')}+synthetic"
                aug["augmentation_type"] = f"variant_{idx}"
                aug["synthetic"] = True
                if cfg.difficulty_scaling:
                    aug["difficulty"] = self._scaled_difficulty(str(row.get("difficulty", "unknown")))
                out.append(aug)

        return out

    def _candidate_variants(self, question: str, *, rng: random.Random, cfg: AugmentationConfig) -> List[str]:
        variants: List[str] = []

        # Optional provider paraphrase (offline-capable by graceful fallback).
        if self.paraphraser is not None:
            try:
                paraphrased = str(self.paraphraser(question)).strip()
                if paraphrased and paraphrased != question:
                    variants.append(paraphrased)
            except Exception:
                # Do not fail the pipeline if paraphraser is unavailable.
                pass

        variants.extend(self._local_paraphrases(question))

        if rng.random() <= float(max(0.0, min(1.0, cfg.noise_probability))):
            noisy = self._inject_noise(question, rng=rng)
            if noisy and noisy != question:
                variants.append(noisy)

        dedup: List[str] = []
        seen = set()
        for variant in variants:
            key = variant.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            dedup.append(variant.strip())
        return dedup

    def _local_paraphrases(self, question: str) -> List[str]:
        q = str(question).strip()
        if not q:
            return []

        templates = [
            f"Solve carefully: {q}",
            f"Compute the final value for this problem: {q}",
            f"Find the exact answer. {q}",
        ]
        return templates

    def _inject_noise(self, question: str, *, rng: random.Random) -> str:
        tokens = [tok for tok in question.split(" ") if tok]
        if len(tokens) < 4:
            return question

        i = rng.randint(0, len(tokens) - 2)
        tokens[i], tokens[i + 1] = tokens[i + 1], tokens[i]
        if rng.random() < 0.30:
            # Prefix with instruction noise often seen in scraped datasets.
            tokens.insert(0, "[verify]")
        return " ".join(tokens)

    def _scaled_difficulty(self, difficulty: str) -> str:
        normalized = str(difficulty or "unknown").lower().strip()
        table = {
            "easy": "medium",
            "medium": "hard",
            "hard": "hard",
            "unknown": "medium",
        }
        return table.get(normalized, "medium")
