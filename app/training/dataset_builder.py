from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

from core.lalacore_x.mini_distillation import LC9DistillationHub
from core.lalacore_x.replay import FailureReplayMemory
from core.lalacore_x.telemetry import DEFAULT_TELEMETRY


class ZaggleDatasetBuilder:
    """
    Builds research datasets for PRM, DPO, and RLAIF training loops.
    """

    def __init__(self, out_dir: str = "data/zaggle"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.replay = FailureReplayMemory()
        self.distillation = LC9DistillationHub(export_dir=str(self.out_dir))

    def build_all(self) -> Dict[str, str]:
        events = DEFAULT_TELEMETRY.read_events()
        failures = self.replay.read_failures()

        prm = self._build_prm(events, failures)
        dpo = self._build_dpo(events, failures)
        rlaif = self._build_rlaif(events, failures)

        prm_path = self._write_jsonl("prm_dataset.jsonl", prm)
        dpo_path = self._write_jsonl("dpo_pairs.jsonl", dpo)
        rlaif_path = self._write_jsonl("rlaif_feedback.jsonl", rlaif)

        manifest = {
            "prm": str(prm_path),
            "dpo": str(dpo_path),
            "rlaif": str(rlaif_path),
            "lc9_weekly": str(self.out_dir / "LC9_MINI_WEEKLY_DATASET.jsonl"),
            "lc9_synthetic": str(Path("data/lc9/LC9_SYNTHETIC_EXPANSION.jsonl")),
            "lc9_error_memory": str(Path("data/lc9/LC9_ERROR_MEMORY.jsonl")),
            "samples": {
                "prm": len(prm),
                "dpo": len(dpo),
                "rlaif": len(rlaif),
            },
        }

        manifest_path = self.out_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest

    def _build_prm(self, events: List[Dict], failures: List[Dict]) -> List[Dict]:
        rows: List[Dict] = []

        for event in events:
            if event.get("event_type") != "solve_result":
                continue

            prompt = str(event.get("question", "")).strip()
            if not prompt:
                continue

            reward = 1.0 if event.get("verified") else 0.0
            rows.append(
                {
                    "prompt": prompt,
                    "provider": event.get("provider", "unknown"),
                    "subject": event.get("subject", "general"),
                    "difficulty": event.get("difficulty", "unknown"),
                    "reward": reward,
                    "meta": {
                        "risk": event.get("risk", 1.0),
                        "trap_probability": event.get("trap_probability", 0.0),
                    },
                }
            )

        # Add hard negatives from replay.
        for row in failures:
            question = str(row.get("question", "")).strip()
            if not question:
                continue
            rows.append(
                {
                    "prompt": question,
                    "provider": row.get("provider", "unknown"),
                    "subject": row.get("subject", "general"),
                    "difficulty": row.get("difficulty", "unknown"),
                    "reward": 0.0,
                    "meta": {"reason": row.get("reason", "failure")},
                }
            )

        return rows

    def _build_dpo(self, events: List[Dict], failures: List[Dict]) -> List[Dict]:
        positives_by_key = defaultdict(list)
        negatives_by_key = defaultdict(list)

        for event in events:
            if event.get("event_type") != "solve_result":
                continue

            key = self._key(event.get("subject"), event.get("difficulty"))
            sample = {
                "question": event.get("question", ""),
                "answer": event.get("final_answer", ""),
                "provider": event.get("provider", "unknown"),
            }

            if event.get("verified"):
                positives_by_key[key].append(sample)
            else:
                negatives_by_key[key].append(sample)

        for row in failures:
            key = self._key(row.get("subject"), row.get("difficulty"))
            negatives_by_key[key].append(
                {
                    "question": row.get("question", ""),
                    "answer": row.get("final_answer", ""),
                    "provider": row.get("provider", "unknown"),
                }
            )

        pairs = []
        for key, positives in positives_by_key.items():
            negatives = negatives_by_key.get(key)
            if not negatives:
                continue
            for pos in positives:
                neg = negatives[0]
                if not pos["question"]:
                    continue
                pairs.append(
                    {
                        "prompt": pos["question"],
                        "chosen": pos["answer"],
                        "rejected": neg["answer"],
                        "meta": {
                            "subject_difficulty": key,
                            "chosen_provider": pos["provider"],
                            "rejected_provider": neg["provider"],
                        },
                    }
                )

        return pairs

    def _build_rlaif(self, events: List[Dict], failures: List[Dict]) -> List[Dict]:
        rows = []

        for event in events:
            if event.get("event_type") != "solve_result":
                continue
            prompt = str(event.get("question", "")).strip()
            if not prompt:
                continue

            reward = 1.0 - float(event.get("risk", 1.0))
            rows.append(
                {
                    "prompt": prompt,
                    "response": event.get("final_answer", ""),
                    "reward": max(-1.0, min(1.0, 2 * reward - 1)),
                    "source": "arena_verification",
                }
            )

        for failure in failures:
            prompt = str(failure.get("question", "")).strip()
            if not prompt:
                continue
            rows.append(
                {
                    "prompt": prompt,
                    "response": failure.get("final_answer", ""),
                    "reward": -1.0,
                    "source": "failure_replay",
                }
            )

        return rows

    def _write_jsonl(self, filename: str, rows: Iterable[Dict]) -> Path:
        path = self.out_dir / filename
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")
        return path

    def _key(self, subject, difficulty) -> str:
        return f"{str(subject or 'general').lower()}:{str(difficulty or 'unknown').lower()}"
