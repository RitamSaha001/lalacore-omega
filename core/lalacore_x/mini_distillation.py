from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


class LC9DistillationHub:
    """
    LC9 Mini evolution memory + distillation manager.

    Stores:
    - LC9_MINI_DISAGREEMENT_MEMORY
    - LC9_MINI_TRAINING_DATASET
    - LC9_PROVIDER_PROMPT_ARCHIVE
    - LC9_MINI_SHADOW_LOGS

    All stores are append-only JSONL for backward compatibility and simple auditing.
    """

    def __init__(self, root: str = "data/lc9", export_dir: str = "data/zaggle"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

        self.export_dir = Path(export_dir)
        self.export_dir.mkdir(parents=True, exist_ok=True)

        self.disagreement_path = self.root / "LC9_MINI_DISAGREEMENT_MEMORY.jsonl"
        self.disagreement_index_path = self.root / "LC9_MINI_DISAGREEMENT_MEMORY.index.json"

        self.training_path = self.root / "LC9_MINI_TRAINING_DATASET.jsonl"
        self.training_hash_path = self.root / "LC9_MINI_TRAINING_DATASET.graph_hashes.json"

        self.prompt_archive_path = self.root / "LC9_PROVIDER_PROMPT_ARCHIVE.jsonl"
        self.prompt_effectiveness_path = self.root / "LC9_PROVIDER_PROMPT_EFFECTIVENESS.json"

        self.shadow_logs_path = self.root / "LC9_MINI_SHADOW_LOGS.jsonl"
        self.synthetic_path = self.root / "LC9_SYNTHETIC_EXPANSION.jsonl"
        self.arena_shadow_disagreements_path = self.root / "LC9_ARENA_SHADOW_DISAGREEMENTS.jsonl"
        self.deterministic_vs_provider_gap_path = self.root / "LC9_DETERMINISTIC_VS_PROVIDER_GAP.jsonl"
        self.reasoning_divergence_clusters_path = self.root / "LC9_REASONING_DIVERGENCE_CLUSTERS.jsonl"
        self.rare_cluster_cross_provider_path = self.root / "LC9_RARE_CLUSTER_CROSS_PROVIDER.jsonl"

        self._graph_hash_cache = self._load_hash_cache()
        self._disagreement_index = self._load_disagreement_index()

    # -----------------------------
    # Disagreement Memory
    # -----------------------------

    def log_disagreement_case(self, payload: Dict) -> None:
        row = {"ts": _utc_now(), **payload}
        row_id = int(self._disagreement_index.get("total", 0))
        self._append_jsonl(self.disagreement_path, row)

        subject = str(row.get("subject", "general")).lower().strip()
        concept_clusters = [str(c).lower().strip() for c in row.get("concept_cluster", []) if str(c).strip()]

        self._disagreement_index["total"] += 1
        self._disagreement_index["by_subject"].setdefault(subject, []).append(row_id)

        for cluster in concept_clusters:
            self._disagreement_index["by_concept_cluster"].setdefault(cluster, []).append(row_id)
            key = f"{subject}|{cluster}"
            self._disagreement_index["by_subject_cluster"].setdefault(key, []).append(row_id)

        self._save_disagreement_index()

    def log_debate_outcome(self, payload: Dict) -> None:
        self.log_disagreement_case({"debate_lite": True, **payload})

    # -----------------------------
    # Prompt Archive
    # -----------------------------

    def log_prompt_record(self, payload: Dict) -> None:
        row = {"ts": _utc_now(), **payload}
        self._append_jsonl(self.prompt_archive_path, row)

    def analyze_prompt_effectiveness(self) -> Dict:
        rows = self._read_jsonl(self.prompt_archive_path)

        by_subject_prompt = defaultdict(list)
        for row in rows:
            subject = str(row.get("subject", "general")).lower().strip()
            template_version = str(row.get("template_version", "unknown")).strip()
            prompt_hash = str(row.get("prompt_hash", "unknown")).strip()
            model = str(row.get("model_name", "unknown")).strip()
            key = f"{subject}|{template_version}|{model}|{prompt_hash}"
            by_subject_prompt[key].append(row)

        scored = []
        for key, group in by_subject_prompt.items():
            wins = sum(1 for row in group if row.get("is_winner"))
            verified = sum(1 for row in group if row.get("winner_verified"))

            avg_theta = sum(float(row.get("bt_theta", 0.0)) for row in group) / len(group)
            avg_margin = sum(float(row.get("winner_margin", 0.0)) for row in group) / len(group)

            win_rate = wins / len(group)
            ver_rate = verified / len(group)
            theta_sigmoid = self._sigmoid(avg_theta)

            effectiveness = 0.40 * win_rate + 0.30 * ver_rate + 0.20 * theta_sigmoid + 0.10 * min(1.0, max(0.0, avg_margin))

            subject, template_version, model_name, prompt_hash = key.split("|", 3)
            scored.append(
                {
                    "subject": subject,
                    "template_version": template_version,
                    "model_name": model_name,
                    "prompt_hash": prompt_hash,
                    "samples": len(group),
                    "win_rate": round(win_rate, 6),
                    "verified_rate": round(ver_rate, 6),
                    "avg_theta": round(avg_theta, 6),
                    "avg_margin": round(avg_margin, 6),
                    "effectiveness": round(effectiveness, 6),
                }
            )

        scored.sort(key=lambda row: row["effectiveness"], reverse=True)
        output = {"ts": _utc_now(), "rows": scored}
        self.prompt_effectiveness_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
        return output

    # -----------------------------
    # Shadow Logs
    # -----------------------------

    def log_shadow_eval(self, payload: Dict) -> None:
        row = {"ts": _utc_now(), **payload}
        self._append_jsonl(self.shadow_logs_path, row)

    def log_arena_shadow_disagreement(self, payload: Dict) -> None:
        row = {"ts": _utc_now(), **payload}
        self._append_jsonl(self.arena_shadow_disagreements_path, row)

    def log_deterministic_vs_provider_gap(self, payload: Dict) -> None:
        row = {"ts": _utc_now(), **payload}
        self._append_jsonl(self.deterministic_vs_provider_gap_path, row)

    def log_reasoning_divergence_cluster(self, payload: Dict) -> None:
        row = {"ts": _utc_now(), **payload}
        self._append_jsonl(self.reasoning_divergence_clusters_path, row)

    def log_rare_cluster_cross_provider(self, payload: Dict) -> None:
        row = {"ts": _utc_now(), **payload}
        self._append_jsonl(self.rare_cluster_cross_provider_path, row)

    # -----------------------------
    # Distillation Dataset
    # -----------------------------

    def try_add_training_entry(self, session: Dict) -> bool:
        margin = float(session.get("winner_margin", 0.0))
        uncertainty = float(session.get("uncertainty", 1.0))
        verified = bool(session.get("deterministic_pass", False))
        judge_score = float(session.get("judge_score", 0.0))
        structural_coherence = float(session.get("structural_coherence", 0.5))
        process_reward = float(session.get("process_reward", 0.5))

        if not verified:
            return False
        if margin < float(session.get("min_margin_threshold", 0.06)):
            return False
        if uncertainty > float(session.get("max_uncertainty_threshold", 0.72)):
            return False
        if judge_score < float(session.get("min_judge_score", 0.55)):
            return False
        if structural_coherence < float(session.get("min_structural_coherence", 0.42)):
            return False
        if process_reward < float(session.get("min_process_reward", 0.40)):
            return False

        graph = session.get("best_reasoning_graph") or {}
        graph_hash = self._hash_graph(graph)

        if graph_hash in self._graph_hash_cache:
            return False

        compressed_steps = self.compress_graph(graph)

        entry = {
            "ts": _utc_now(),
            "question": session.get("question"),
            "subject": session.get("subject"),
            "difficulty": session.get("difficulty"),
            "concept_cluster": list(session.get("concept_cluster", [])),
            "verified_answer": session.get("verified_answer"),
            "best_provider": session.get("best_provider"),
            "best_reasoning_graph": graph,
            "compressed_reasoning_steps": compressed_steps,
            "judge_score": judge_score,
            "deterministic_pass": bool(session.get("deterministic_pass", False)),
            "winner_margin": margin,
            "uncertainty": uncertainty,
            "structural_coherence": structural_coherence,
            "process_reward": process_reward,
            "curriculum_level": int(session.get("curriculum_level", self._curriculum_level(session))),
            "graph_hash": graph_hash,
        }

        self._append_jsonl(self.training_path, entry)
        self._graph_hash_cache.add(graph_hash)
        self._save_hash_cache()
        return True

    def finalize_weekly_dataset(
        self,
        replay_rows: Sequence[Dict],
        min_margin: float = 0.06,
        max_uncertainty: float = 0.72,
    ) -> Dict:
        winners = self._read_jsonl(self.training_path)
        disagreements = self._read_jsonl(self.disagreement_path)

        merged: List[Dict] = []
        dedupe = set()

        # High-confidence winners.
        for row in winners:
            if float(row.get("winner_margin", 0.0)) < min_margin:
                continue
            if float(row.get("uncertainty", 1.0)) > max_uncertainty:
                continue
            if float(row.get("structural_coherence", 0.0)) < 0.40:
                continue
            if float(row.get("process_reward", 0.0)) < 0.38:
                continue

            key = self._entry_key(row)
            if key in dedupe:
                continue
            dedupe.add(key)
            merged.append({"source": "winner", **row})

        # Disagreement cases with deterministic winner.
        for row in disagreements:
            if not bool(row.get("winner_verified", False)):
                continue
            if float(row.get("winner_margin", 0.0)) < min_margin:
                continue
            if float(row.get("uncertainty", 1.0)) > max_uncertainty:
                continue

            graph = row.get("winner_reasoning_graph") or {}
            graph_hash = self._hash_graph(graph)

            entry = {
                "ts": row.get("ts", _utc_now()),
                "question": row.get("question"),
                "subject": row.get("subject"),
                "difficulty": row.get("difficulty"),
                "concept_cluster": list(row.get("concept_cluster", [])),
                "verified_answer": row.get("winner_final_answer"),
                "best_provider": row.get("winner_provider"),
                "best_reasoning_graph": graph,
                "compressed_reasoning_steps": self.compress_graph(graph),
                "judge_score": float(row.get("winner_judge_score", 0.0)),
                "deterministic_pass": bool(row.get("winner_verified", False)),
                "winner_margin": float(row.get("winner_margin", 0.0)),
                "uncertainty": float(row.get("uncertainty", 1.0)),
                "structural_coherence": float(row.get("winner_structure", {}).get("structural_coherence_score", row.get("structural_coherence", 0.0))),
                "process_reward": float(row.get("winner_structure", {}).get("process_reward_score", row.get("process_reward", 0.0))),
                "curriculum_level": int(row.get("curriculum_level", self._curriculum_level(row))),
                "graph_hash": graph_hash,
                "source": "disagreement",
            }

            key = self._entry_key(entry)
            if key in dedupe:
                continue
            dedupe.add(key)
            merged.append(entry)

        # Replay focus rows for corrective batches.
        for row in replay_rows:
            subject = str(row.get("subject", "general")).lower().strip()
            difficulty = str(row.get("difficulty", "unknown")).lower().strip()
            concept_cluster = list(row.get("concept_clusters", []))

            entry = {
                "ts": row.get("ts", _utc_now()),
                "question": row.get("question"),
                "subject": subject,
                "difficulty": difficulty,
                "concept_cluster": concept_cluster,
                "verified_answer": row.get("final_answer", ""),
                "best_provider": row.get("provider", "unknown"),
                "best_reasoning_graph": {},
                "compressed_reasoning_steps": [],
                "judge_score": 0.0,
                "deterministic_pass": False,
                "winner_margin": 0.0,
                "uncertainty": float(row.get("risk", 1.0)),
                "structural_coherence": 0.0,
                "process_reward": 0.0,
                "curriculum_level": int(row.get("curriculum_level", self._curriculum_level(row))),
                "graph_hash": self._hash_text(f"replay|{row.get('question', '')}|{row.get('final_answer', '')}"),
                "source": "replay",
            }

            key = self._entry_key(entry)
            if key in dedupe:
                continue
            dedupe.add(key)
            merged.append(entry)

        stratified = self._stratify(merged)

        out_path = self.export_dir / "LC9_MINI_WEEKLY_DATASET.jsonl"
        with out_path.open("w", encoding="utf-8") as f:
            for row in stratified:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")

        summary = {
            "path": str(out_path),
            "total": len(stratified),
            "by_source": self._count_by(stratified, "source"),
            "by_subject": self._count_by(stratified, "subject"),
            "by_difficulty": self._count_by(stratified, "difficulty"),
        }
        return summary

    def generate_synthetic_expansion(
        self,
        source_rows: Sequence[Dict],
        reliable_clusters: Sequence[str],
        max_per_cluster: int = 20,
    ) -> Dict:
        reliable = {str(c).lower().strip() for c in reliable_clusters if str(c).strip()}
        if not reliable:
            return {"generated": 0, "clusters": 0}

        existing = {str(r.get("synthetic_hash", "")) for r in self._read_jsonl(self.synthetic_path)}
        generated = 0
        by_cluster = defaultdict(int)

        for row in source_rows:
            question = str(row.get("question", "")).strip()
            if not question:
                continue

            clusters = [str(c).lower().strip() for c in row.get("concept_cluster", row.get("concept_clusters", [])) if str(c).strip()]
            if not clusters:
                clusters = ["general"]
            hit_clusters = [c for c in clusters if c in reliable]
            if not hit_clusters:
                continue

            variants = self._synthetic_variants(question)
            for cluster in hit_clusters:
                if by_cluster[cluster] >= max_per_cluster:
                    continue
                for variant_type, variant_question in variants:
                    h = self._hash_text(f"{cluster}|{variant_type}|{variant_question}")
                    if h in existing:
                        continue
                    out = {
                        "ts": _utc_now(),
                        "source_question": question,
                        "synthetic_question": variant_question,
                        "variant_type": variant_type,
                        "subject": str(row.get("subject", "general")).lower().strip(),
                        "difficulty": str(row.get("difficulty", "unknown")).lower().strip(),
                        "concept_cluster": cluster,
                        "synthetic_hash": h,
                    }
                    self._append_jsonl(self.synthetic_path, out)
                    existing.add(h)
                    generated += 1
                    by_cluster[cluster] += 1
                    break

        return {"generated": generated, "clusters": len(by_cluster), "by_cluster": dict(by_cluster)}

    # -----------------------------
    # Compression
    # -----------------------------

    def compress_graph(self, graph: Dict) -> List[Dict]:
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        if not nodes:
            return []

        node_map = {int(n.get("id")): n for n in nodes if "id" in n}
        adjacency = defaultdict(list)
        indegree = defaultdict(int)
        predecessors = defaultdict(list)

        for edge in edges:
            src = int(edge.get("from", -1))
            dst = int(edge.get("to", -1))
            if src in node_map and dst in node_map:
                adjacency[src].append(dst)
                predecessors[dst].append(src)
                indegree[dst] += 1

        queue = deque(sorted([nid for nid in node_map if indegree[nid] == 0]))
        topo = []

        while queue:
            nid = queue.popleft()
            topo.append(nid)
            for nxt in sorted(adjacency.get(nid, [])):
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)

        # Cycle fallback: append unseen nodes in stable order.
        unseen = [nid for nid in sorted(node_map) if nid not in set(topo)]
        topo.extend(unseen)

        compressed = []
        seen_signatures = set()
        map_old_to_new = {}

        preserve_types = {"numeric_evaluation", "verification", "logical_inference"}

        for nid in topo:
            node = node_map[nid]
            node_type = str(node.get("type", "algebra_step"))
            summary = str(node.get("summary", "")).strip()
            signature = f"{node_type}|{_norm_text(summary)}"

            if signature in seen_signatures and node_type not in preserve_types:
                continue

            seen_signatures.add(signature)
            new_idx = len(compressed) + 1
            map_old_to_new[nid] = new_idx

            deps = []
            for prev in predecessors.get(nid, []):
                mapped = map_old_to_new.get(prev)
                if mapped is not None:
                    deps.append(mapped)

            compressed.append(
                {
                    "step": new_idx,
                    "type": node_type,
                    "summary": summary,
                    "depends_on": sorted(set(deps)),
                }
            )

        return compressed

    # -----------------------------
    # Internal helpers
    # -----------------------------

    def _load_hash_cache(self) -> set:
        if not self.training_hash_path.exists():
            return set()
        try:
            data = json.loads(self.training_hash_path.read_text(encoding="utf-8"))
            return set(str(x) for x in data.get("graph_hashes", []))
        except Exception:
            return set()

    def _save_hash_cache(self) -> None:
        payload = {"graph_hashes": sorted(self._graph_hash_cache)}
        self.training_hash_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _load_disagreement_index(self) -> Dict:
        if not self.disagreement_index_path.exists():
            return {
                "total": 0,
                "by_subject": {},
                "by_concept_cluster": {},
                "by_subject_cluster": {},
            }
        try:
            return json.loads(self.disagreement_index_path.read_text(encoding="utf-8"))
        except Exception:
            return {
                "total": 0,
                "by_subject": {},
                "by_concept_cluster": {},
                "by_subject_cluster": {},
            }

    def _save_disagreement_index(self) -> None:
        self.disagreement_index_path.write_text(json.dumps(self._disagreement_index, indent=2, sort_keys=True), encoding="utf-8")

    def _append_jsonl(self, path: Path, row: Dict) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")

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

    def _entry_key(self, row: Dict) -> str:
        graph_hash = str(row.get("graph_hash", "")).strip()
        if graph_hash:
            return f"gh:{graph_hash}"

        q = _norm_text(str(row.get("question", "")))
        a = _norm_text(str(row.get("verified_answer", "")))
        return f"qa:{self._hash_text(q + '|' + a)}"

    def _hash_graph(self, graph: Dict) -> str:
        canonical = self._canonical_graph(graph)
        return self._hash_text(canonical)

    def _canonical_graph(self, graph: Dict) -> str:
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        node_rows = []
        for node in nodes:
            node_rows.append(
                {
                    "id": int(node.get("id", 0)),
                    "type": str(node.get("type", "")),
                    "summary": _norm_text(str(node.get("summary", ""))),
                }
            )

        edge_rows = []
        for edge in edges:
            edge_rows.append(
                {
                    "from": int(edge.get("from", 0)),
                    "to": int(edge.get("to", 0)),
                }
            )

        payload = {
            "nodes": sorted(node_rows, key=lambda r: (r["id"], r["type"], r["summary"])),
            "edges": sorted(edge_rows, key=lambda r: (r["from"], r["to"])),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _hash_text(self, text: str) -> str:
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    def _synthetic_variants(self, question: str) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        q = str(question).strip()

        # Numeric jitter variant (deterministic + small perturbation).
        nums = [int(m.group(0)) for m in re.finditer(r"\b\d+\b", q)]
        if nums:
            first = nums[0]
            out.append(("numeric_jitter_plus1", re.sub(rf"\b{first}\b", str(first + 1), q, count=1)))
            out.append(("numeric_jitter_minus1", re.sub(rf"\b{first}\b", str(max(0, first - 1)), q, count=1)))

        # Boundary-condition variant.
        if "<=" in q:
            out.append(("boundary_flip", q.replace("<=", "<", 1)))
        elif ">=" in q:
            out.append(("boundary_flip", q.replace(">=", ">", 1)))
        elif "<" in q:
            out.append(("boundary_flip", q.replace("<", "<=", 1)))
        elif ">" in q:
            out.append(("boundary_flip", q.replace(">", ">=", 1)))

        # Domain-check variant.
        out.append(("domain_check", f"{q} Also verify boundary and domain constraints explicitly."))
        return out[:4]

    def _curriculum_level(self, row: Dict) -> int:
        clusters = row.get("concept_cluster", row.get("concept_clusters", []))
        cluster_count = len([c for c in clusters if str(c).strip()])
        entropy = float(row.get("entropy", 0.0))
        disagreement = float(row.get("disagreement", row.get("mini_disagreement", 0.0)))
        difficulty = str(row.get("difficulty", "unknown")).lower().strip()

        if entropy >= 0.70 or disagreement >= 0.70:
            return 5
        if difficulty == "hard" and cluster_count >= 2:
            return 4
        if cluster_count >= 2:
            return 3
        if difficulty in {"medium", "hard"}:
            return 2
        return 1

    def _stratify(self, rows: List[Dict]) -> List[Dict]:
        buckets = defaultdict(list)
        for row in rows:
            key = f"{str(row.get('subject', 'general')).lower()}:{str(row.get('difficulty', 'unknown')).lower()}"
            buckets[key].append(row)

        for key in buckets:
            buckets[key].sort(key=lambda r: (str(r.get("source", "")), str(r.get("ts", ""))), reverse=True)

        keys = sorted(buckets.keys())
        out = []
        idx = 0

        while True:
            progressed = False
            for key in keys:
                if idx < len(buckets[key]):
                    out.append(buckets[key][idx])
                    progressed = True
            if not progressed:
                break
            idx += 1

        return out

    def _count_by(self, rows: List[Dict], key: str) -> Dict[str, int]:
        out = defaultdict(int)
        for row in rows:
            out[str(row.get(key, "unknown"))] += 1
        return dict(out)

    def _sigmoid(self, x: float) -> float:
        if x >= 0:
            z = math.exp(-x)
            return 1.0 / (1.0 + z)
        z = math.exp(x)
        return z / (1.0 + z)
