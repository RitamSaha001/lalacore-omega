from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


class FailureReplayMemory:
    def __init__(self, path: str = "data/replay/failures.jsonl", max_rows: int = 20000):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_rows = max(500, int(max_rows))

    def log_failure(self, payload: Dict) -> None:
        payload = dict(payload)
        payload.setdefault("ts", datetime.now(timezone.utc).isoformat())
        payload.setdefault("cluster", self._cluster_key(payload))

        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
        self._trim()

    def read_failures(self) -> List[Dict]:
        if not self.path.exists():
            return []

        rows: List[Dict] = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows

    def build_weekly_replay(self, out_path: str = "data/replay/weekly_replay.jsonl", top_fraction: float = 0.05) -> Dict:
        failures = self.read_failures()
        if not failures:
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            Path(out_path).write_text("", encoding="utf-8")
            return {"total_failures": 0, "selected": 0, "clusters": 0, "output": out_path}

        by_cluster: Dict[str, List[Dict]] = defaultdict(list)
        for row in failures:
            by_cluster[row.get("cluster", "unknown")].append(row)

        cluster_freq = Counter()
        for row in failures:
            for concept_cluster in row.get("concept_clusters", []):
                cluster_freq[str(concept_cluster).lower().strip()] += 1

        cluster_scores = []
        for cluster, rows in by_cluster.items():
            avg_risk = sum(float(r.get("risk", 1.0)) for r in rows) / len(rows)
            avg_disagreement = sum(float(r.get("disagreement", 0.0)) for r in rows) / len(rows)

            concept_weight = 1.0
            weights = []
            for row in rows:
                local_clusters = [str(c).lower().strip() for c in row.get("concept_clusters", []) if str(c).strip()]
                if not local_clusters:
                    continue
                inv = [1.0 / max(1, cluster_freq.get(c, 1)) for c in local_clusters]
                weights.append(sum(inv) / len(inv))
            if weights:
                concept_weight = 1.0 + (sum(weights) / len(weights))

            cluster_scores.append((cluster, len(rows) * avg_risk * (1.0 + avg_disagreement) * concept_weight))

        cluster_scores.sort(key=lambda x: x[1], reverse=True)

        k = max(1, int(len(failures) * top_fraction))
        selected: List[Dict] = []

        for cluster, _ in cluster_scores:
            for row in by_cluster[cluster]:
                selected.append(row)
                if len(selected) >= k:
                    break
            if len(selected) >= k:
                break

        out = Path(out_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        with out.open("w", encoding="utf-8") as f:
            for row in selected:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")

        return {
            "total_failures": len(failures),
            "selected": len(selected),
            "clusters": len(by_cluster),
            "output": str(out),
        }

    def _cluster_key(self, payload: Dict) -> str:
        question = str(payload.get("question", "")).lower().strip()
        subject = str(payload.get("subject", "general")).lower().strip()
        difficulty = str(payload.get("difficulty", "unknown")).lower().strip()

        signature = " ".join(question.split())[:180]
        base = f"{subject}|{difficulty}|{signature}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

    def _trim(self) -> None:
        rows = self.read_failures()
        if len(rows) <= self.max_rows:
            return
        keep = rows[-self.max_rows :]
        with self.path.open("w", encoding="utf-8") as f:
            for row in keep:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")
