from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

from core.lalacore_x.schemas import ProviderAnswer


class DAGReasoner:
    """
    Builds provider-wise reasoning DAGs and structural coherence metrics.
    """

    def build_graph(self, candidates: Sequence[ProviderAnswer]) -> Dict:
        nodes = []
        edges = []
        node_id = 1

        provider_node_ids: Dict[str, List[int]] = defaultdict(list)

        for candidate in candidates:
            steps = self._split_steps(candidate.reasoning)
            prev = None

            for step in steps:
                nodes.append(
                    {
                        "id": node_id,
                        "provider": candidate.provider,
                        "type": self._step_type(step),
                        "summary": step[:180],
                    }
                )
                provider_node_ids[candidate.provider].append(node_id)

                if prev is not None:
                    edges.append({"from": prev, "to": node_id, "provider": candidate.provider})
                prev = node_id
                node_id += 1

        provider_graphs = {}
        coherence = {}
        structure_metrics = {}
        process_rewards = {}
        for provider, ids in provider_node_ids.items():
            provider_nodes = [node for node in nodes if node["provider"] == provider]
            provider_edges = [edge for edge in edges if edge["provider"] == provider]
            graph = {"nodes": provider_nodes, "edges": provider_edges}
            provider_graphs[provider] = graph
            metrics = self.graph_structure_metrics(graph)
            structure_metrics[provider] = metrics
            coherence[provider] = float(metrics["structural_coherence_score"])
            process_rewards[provider] = float(metrics["process_reward_score"])

        return {
            "nodes": nodes,
            "edges": edges,
            "provider_graphs": provider_graphs,
            "coherence": coherence,
            "structure_metrics": structure_metrics,
            "process_reward": process_rewards,
        }

    def extract_claims(self, reasoning_graph: Dict, limit: int = 6) -> List[str]:
        claims = []
        for node in reasoning_graph.get("nodes", []):
            summary = str(node.get("summary", "")).strip()
            if summary:
                claims.append(summary)
        return claims[:limit]

    def numeric_substitution_hooks(self, question: str, max_hooks: int = 3) -> List[Dict[str, float]]:
        """
        Builds deterministic substitution test points for verifier hooks.
        """
        vars_ = sorted(set(re.findall(r"\b[a-zA-Z]\b", question)))
        if not vars_:
            return []

        hooks = []
        seeds = [1.0, 2.0, 3.0, 5.0]
        for seed in seeds:
            hook = {v: seed + idx for idx, v in enumerate(vars_[:4])}
            hooks.append(hook)
            if len(hooks) >= max_hooks:
                break

        return hooks

    def graph_coherence_score(self, graph: Dict) -> float:
        return float(self.graph_structure_metrics(graph)["structural_coherence_score"])

    def graph_structure_metrics(self, graph: Dict) -> Dict[str, float]:
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        n = len(nodes)
        if n == 0:
            return {
                "graph_depth": 0.0,
                "branching_factor": 0.0,
                "dependency_chain_length": 0.0,
                "circular_reasoning": 0.0,
                "step_redundancy_rate": 0.0,
                "missing_inference_rate": 1.0,
                "structural_coherence_score": 0.0,
                "process_reward_score": 0.0,
            }

        adjacency = defaultdict(list)
        indegree = defaultdict(int)
        outdegree = defaultdict(int)
        node_ids = {node["id"] for node in nodes}

        edge_consistency_hits = 0
        edge_total = 0
        jump_count = 0
        for edge in edges:
            edge_total += 1
            src = edge.get("from")
            dst = edge.get("to")
            if src not in node_ids or dst not in node_ids:
                continue
            edge_consistency_hits += 1
            adjacency[src].append(dst)
            indegree[dst] += 1
            outdegree[src] += 1
            if abs(int(dst) - int(src)) > 2:
                jump_count += 1

        edge_consistency = float(edge_consistency_hits) / max(1, edge_total)

        acyclic, topo = self._topological_order(node_ids, adjacency, indegree)
        graph_depth = self._longest_path_depth(topo, adjacency) if acyclic else float(min(n, 10))

        branching_nodes = [outdegree[nid] for nid in node_ids if outdegree[nid] > 0]
        branching_factor = sum(branching_nodes) / len(branching_nodes) if branching_nodes else 0.0

        dependency_chain = graph_depth
        circular_reasoning = 0.0 if acyclic else 1.0

        redundancy = self._redundancy_rate(nodes)
        missing_inference = self._missing_inference_rate(node_ids, adjacency)

        # Structural coherence in [0, 1].
        depth_score = min(1.0, graph_depth / max(2.0, min(float(n), 8.0)))
        branch_balance = math.exp(-abs(branching_factor - 1.0))
        chain_score = min(1.0, dependency_chain / max(2.0, float(n)))

        coherence = (
            0.22 * (1.0 - circular_reasoning)
            + 0.17 * edge_consistency
            + 0.16 * (1.0 - missing_inference)
            + 0.15 * (1.0 - redundancy)
            + 0.12 * depth_score
            + 0.10 * chain_score
            + 0.08 * branch_balance
        )
        coherence = max(0.0, min(1.0, coherence))

        process_reward = self._process_reward_score(
            nodes=nodes,
            jump_ratio=(jump_count / max(1, edge_consistency_hits)),
            missing_inference=missing_inference,
            coherence=coherence,
        )

        return {
            "graph_depth": round(float(graph_depth), 6),
            "branching_factor": round(float(branching_factor), 6),
            "dependency_chain_length": round(float(dependency_chain), 6),
            "circular_reasoning": round(float(circular_reasoning), 6),
            "step_redundancy_rate": round(float(redundancy), 6),
            "missing_inference_rate": round(float(missing_inference), 6),
            "structural_coherence_score": round(float(coherence), 6),
            "process_reward_score": round(float(process_reward), 6),
        }

    def _split_steps(self, reasoning: str) -> List[str]:
        if not reasoning:
            return ["No reasoning provided"]

        lines = [line.strip() for line in reasoning.splitlines() if line.strip()]
        if len(lines) >= 2:
            return lines[:14]

        fragments = [s.strip() for s in re.split(r"[.;]\s+", reasoning) if s.strip()]
        return fragments[:14] if fragments else [reasoning[:240]]

    def _step_type(self, step: str) -> str:
        s = step.lower()
        if any(k in s for k in ("assume", "given", "let")):
            return "assumption"
        if any(k in s for k in ("substitute", "plug", "insert")):
            return "substitution"
        if any(k in s for k in ("therefore", "hence", "thus", "implies")):
            return "logical_inference"
        if any(k in s for k in ("verify", "check", "constraint")):
            return "verification"
        if re.search(r"\d", s):
            return "numeric_evaluation"
        return "algebra_step"

    def _topological_order(self, node_ids, adjacency, indegree):
        indeg = {nid: int(indegree.get(nid, 0)) for nid in node_ids}
        queue = [nid for nid in node_ids if indeg[nid] == 0]
        queue.sort()

        topo = []
        idx = 0
        while idx < len(queue):
            nid = queue[idx]
            idx += 1
            topo.append(nid)
            for nxt in adjacency.get(nid, []):
                indeg[nxt] = max(0, indeg[nxt] - 1)
                if indeg[nxt] == 0:
                    queue.append(nxt)

        return (len(topo) == len(node_ids)), topo

    def _longest_path_depth(self, topo, adjacency) -> float:
        if not topo:
            return 0.0

        depth = {nid: 1 for nid in topo}
        for nid in topo:
            for nxt in adjacency.get(nid, []):
                depth[nxt] = max(depth.get(nxt, 1), depth[nid] + 1)
        return float(max(depth.values()))

    def _redundancy_rate(self, nodes) -> float:
        if not nodes:
            return 0.0

        seen = set()
        redundant = 0
        for node in nodes:
            node_type = str(node.get("type", "")).strip().lower()
            summary = re.sub(r"\s+", " ", str(node.get("summary", "")).strip().lower())
            sig = f"{node_type}|{summary}"
            if sig in seen:
                redundant += 1
            else:
                seen.add(sig)
        return redundant / len(nodes)

    def _missing_inference_rate(self, node_ids, adjacency) -> float:
        if not node_ids:
            return 1.0
        if len(node_ids) == 1:
            return 0.0

        # Deterministic weak-link estimate: disconnected components in undirected view.
        undirected = defaultdict(set)
        for src, outs in adjacency.items():
            for dst in outs:
                undirected[src].add(dst)
                undirected[dst].add(src)

        unseen = set(node_ids)
        components = 0
        while unseen:
            components += 1
            root = unseen.pop()
            stack = [root]
            while stack:
                cur = stack.pop()
                for nxt in undirected.get(cur, set()):
                    if nxt in unseen:
                        unseen.remove(nxt)
                        stack.append(nxt)

        return max(0.0, min(1.0, (components - 1) / max(1, len(node_ids) - 1)))

    def _process_reward_score(self, nodes, jump_ratio: float, missing_inference: float, coherence: float) -> float:
        if not nodes:
            return 0.0

        valid_types = {
            "algebra_step",
            "numeric_evaluation",
            "substitution",
            "verification",
            "logical_inference",
        }
        valid_steps = 0
        substitution_checks = 0
        for node in nodes:
            node_type = str(node.get("type", "")).strip().lower()
            summary = str(node.get("summary", "")).lower()
            if node_type in valid_types:
                valid_steps += 1
            if node_type in {"substitution", "verification"} or "substitut" in summary or "check" in summary:
                substitution_checks += 1

        valid_ratio = valid_steps / len(nodes)
        subst_ratio = substitution_checks / len(nodes)

        process = (
            0.36 * valid_ratio
            + 0.24 * subst_ratio
            + 0.20 * (1.0 - max(0.0, min(1.0, jump_ratio)))
            + 0.10 * (1.0 - max(0.0, min(1.0, missing_inference)))
            + 0.10 * coherence
        )
        return max(0.0, min(1.0, process))
