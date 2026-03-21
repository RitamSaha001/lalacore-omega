import math
from collections import defaultdict

from app.arena.stability_guard import ArenaStabilityGuard
from core.safe_math import clipped_division, safe_log


class _UnionFind:
    def __init__(self, elements):
        self.parent = {e: e for e in elements}
        self.size = {e: 1 for e in elements}

    def find(self, x):
        p = self.parent[x]
        if p != x:
            self.parent[x] = self.find(p)
        return self.parent[x]

    def union(self, a, b):
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]

    def cluster_size(self, x):
        return self.size[self.find(x)]


class PairwiseEngine:
    """
    Research-grade O(N^2) pairwise tournament.

    Guarantees:
    - No API calls in tournament loop
    - Similarity and clustering precomputed once
    - Entropy-normalized adaptive iteration scheduling
    """

    def __init__(
        self,
        bt_engine,
        similarity_engine=None,
        clone_threshold=0.95,
        disagreement_margin=0.08,
    ):
        self.bt = bt_engine
        self.similarity_engine = similarity_engine
        self.clone_threshold = clone_threshold
        self.disagreement_margin = disagreement_margin
        self.guard = ArenaStabilityGuard()

    def run(self, responses, entropy=0.0, return_details=False, iterations_override: int | None = None):
        responses, response_issues = self.guard.validate_responses(responses)
        entropy = self.guard.sanitize_entropy(entropy)

        providers = [r["provider"] for r in responses]
        if not providers:
            details = {
                "iterations": 0,
                "weights": {},
                "cluster_sizes": {},
                "uncertainties": {},
                "confidence_margin": 0.0,
                "uncertainty_adjusted_margin": 0.0,
                "disagreement_cases": [],
                "pair_count": 0,
                "guard_issues": response_issues + ["empty_responses"],
                "guard_fallback": True,
            }
            if return_details:
                return {}, [], details
            return {}, []

        self.bt.initialize(providers)

        # 1) Precompute similarities and clone clusters (O(N^2)).
        sim_matrix = self._build_similarity_matrix(responses)
        cluster_sizes = self._cluster_sizes(providers, sim_matrix)

        # 2) Dynamic statistical weights.
        weights = self._generate_dynamic_weights(responses, entropy)

        # 3) Build pairwise matches.
        raw_matches = []
        match_records = []
        disagreement_cases = []

        n = len(responses)
        for i in range(n):
            for j in range(i + 1, n):
                ri = responses[i]
                rj = responses[j]
                similarity = sim_matrix.get((i, j), 0.0)

                score, margin, components = self._score_pair(
                    ri=ri,
                    rj=rj,
                    weights=weights,
                    similarity=similarity,
                    cluster_sizes=cluster_sizes,
                )

                if score >= 0:
                    winner = ri["provider"]
                    loser = rj["provider"]
                else:
                    winner = rj["provider"]
                    loser = ri["provider"]

                raw_matches.append((winner, loser, margin))

                record = {
                    "provider_a": ri["provider"],
                    "provider_b": rj["provider"],
                    "winner": winner,
                    "score_diff": margin,
                    "signed_score": score,
                    "similarity": similarity,
                    "cluster_size_a": cluster_sizes.get(ri["provider"], 1),
                    "cluster_size_b": cluster_sizes.get(rj["provider"], 1),
                    "components": components,
                }
                match_records.append(record)

                if self._is_disagreement_case(ri, rj, score):
                    disagreement_cases.append(
                        {
                            "provider_a": ri["provider"],
                            "provider_b": rj["provider"],
                            "answer_a": ri.get("final_answer", ""),
                            "answer_b": rj.get("final_answer", ""),
                            "similarity": similarity,
                            "margin": margin,
                            "deterministic_pair": (
                                bool(ri.get("deterministic_pass")),
                                bool(rj.get("deterministic_pass")),
                            ),
                        }
                    )

        # 4) Validate matches and fit BT with entropy-adaptive schedule.
        raw_matches, match_issues = self.guard.validate_matches(raw_matches)
        guard_fallback = False
        guard_issues = response_issues + match_issues

        if not raw_matches:
            guard_fallback = True
            self.bt.theta = self.guard.single_pass_thetas(responses)
            iterations = 0
        else:
            if iterations_override is not None:
                iterations = max(1, int(iterations_override))
            elif hasattr(self.bt, "schedule_iterations"):
                iterations = self.bt.schedule_iterations(entropy=entropy, provider_count=max(len(providers), 1))
            else:
                iterations = int(10 + 30 * entropy)
            self.bt.fit(raw_matches, iterations=iterations)

            # Additional finite-check guard.
            if any((not math.isfinite(float(v))) for v in self.bt.theta.values()):
                guard_fallback = True
                guard_issues.append("nonfinite_theta")
                self.bt.theta = self.guard.single_pass_thetas(responses)
                iterations = 0

        # 5) Confidence diagnostics.
        uncertainties = self.bt.uncertainty_map() if hasattr(self.bt, "uncertainty_map") else {}
        confidence_margin, uncertainty_adjusted_margin = self._confidence_margins(self.bt.theta, uncertainties)

        details = {
            "iterations": iterations,
            "weights": weights,
            "cluster_sizes": cluster_sizes,
            "uncertainties": uncertainties,
            "confidence_margin": confidence_margin,
            "uncertainty_adjusted_margin": uncertainty_adjusted_margin,
            "disagreement_cases": disagreement_cases,
            "pair_count": len(match_records),
            "guard_issues": guard_issues,
            "guard_fallback": guard_fallback,
        }

        if return_details:
            return self.bt.theta, match_records, details

        return self.bt.theta, match_records

    def _generate_dynamic_weights(self, responses, entropy):
        if not responses:
            return {
                "critic": 0.25,
                "deterministic": 0.35,
                "skill": 0.25,
                "confidence": 0.15,
            }

        det_pass_rate = sum(1 for r in responses if r.get("deterministic_pass")) / len(responses)
        confidence_var = self._variance([float(r.get("confidence", 0.5)) for r in responses])

        # Entropy-normalized adaptation: higher entropy => slightly increase deterministic influence.
        denom = max(safe_log(max(len(responses), 2), fallback=0.0), 1e-9)
        norm_entropy = min(1.0, max(0.0, clipped_division(float(entropy), denom, fallback=0.0)))

        weights = {
            "critic": 0.24 + 0.06 * (1 - det_pass_rate),
            "deterministic": 0.33 + 0.08 * norm_entropy,
            "skill": 0.27 + 0.05 * confidence_var,
            "confidence": 0.16 - 0.04 * norm_entropy,
        }

        # normalize safely.
        total = sum(max(0.0, v) for v in weights.values())
        if total <= 0:
            return {
                "critic": 0.25,
                "deterministic": 0.35,
                "skill": 0.25,
                "confidence": 0.15,
            }

        return {k: max(0.0, v) / total for k, v in weights.items()}

    def _score_pair(self, ri, rj, weights, similarity, cluster_sizes):
        p_i = ri["provider"]
        p_j = rj["provider"]

        # Union-Find cluster correction.
        c_i = max(1, cluster_sizes.get(p_i, 1))
        c_j = max(1, cluster_sizes.get(p_j, 1))

        skill_i = float(ri.get("skill", 0.5)) / c_i
        skill_j = float(rj.get("skill", 0.5)) / c_j

        critic_diff = float(ri.get("critic_score", 0.5)) - float(rj.get("critic_score", 0.5))
        det_diff = int(bool(ri.get("deterministic_pass"))) - int(bool(rj.get("deterministic_pass")))
        skill_diff = skill_i - skill_j
        conf_diff = float(ri.get("confidence", 0.5)) - float(rj.get("confidence", 0.5))

        similarity_penalty = 0.25 * max(0.0, min(1.0, similarity))

        raw_score = (
            weights["critic"] * critic_diff
            + weights["deterministic"] * det_diff
            + weights["skill"] * skill_diff
            + weights["confidence"] * conf_diff
            - similarity_penalty
        )

        # Margin bounded update signal for BT stability.
        margin = abs(math.tanh(raw_score))

        components = {
            "critic_diff": critic_diff,
            "det_diff": det_diff,
            "skill_diff": skill_diff,
            "conf_diff": conf_diff,
            "similarity_penalty": similarity_penalty,
        }

        return raw_score, margin, components

    def _build_similarity_matrix(self, responses):
        matrix = {}
        n = len(responses)

        if n <= 1:
            return matrix

        if not self.similarity_engine:
            for i in range(n):
                for j in range(i + 1, n):
                    matrix[(i, j)] = 0.0
            return matrix

        embeddings = [None] * n
        for i, response in enumerate(responses):
            graph = response.get("graph")
            if graph:
                embeddings[i] = self.similarity_engine.graph_embedding(graph)

        for i in range(n):
            for j in range(i + 1, n):
                if embeddings[i] is None or embeddings[j] is None:
                    matrix[(i, j)] = 0.0
                else:
                    matrix[(i, j)] = self.similarity_engine.similarity(embeddings[i], embeddings[j])

        return matrix

    def _cluster_sizes(self, providers, sim_matrix):
        uf = _UnionFind(providers)

        # Index map for pair lookup.
        idx = {provider: i for i, provider in enumerate(providers)}

        for i, p_i in enumerate(providers):
            for j in range(i + 1, len(providers)):
                p_j = providers[j]
                sim = sim_matrix.get((i, j), 0.0)
                if sim >= self.clone_threshold:
                    uf.union(p_i, p_j)

        return {provider: uf.cluster_size(provider) for provider in providers}

    def _confidence_margins(self, theta, uncertainties):
        if not theta:
            return 0.0, 0.0

        ranked = sorted(theta.items(), key=lambda x: x[1], reverse=True)
        if len(ranked) == 1:
            return 1.0, 1.0

        (p1, t1), (p2, t2) = ranked[0], ranked[1]
        margin = max(0.0, t1 - t2)

        u1 = uncertainties.get(p1, 1.0)
        u2 = uncertainties.get(p2, 1.0)
        adjusted = margin / (1.0 + u1 + u2)

        return margin, adjusted

    def _is_disagreement_case(self, ri, rj, score):
        answer_a = str(ri.get("final_answer", "")).strip().lower()
        answer_b = str(rj.get("final_answer", "")).strip().lower()

        if not answer_a or not answer_b:
            return False

        if answer_a != answer_b and abs(score) <= self.disagreement_margin:
            return True

        det_a = bool(ri.get("deterministic_pass"))
        det_b = bool(rj.get("deterministic_pass"))
        return det_a != det_b and abs(score) <= max(self.disagreement_margin, 0.12)

    def _variance(self, values):
        if not values:
            return 0.0
        mu = sum(values) / len(values)
        return sum((v - mu) ** 2 for v in values) / len(values)
