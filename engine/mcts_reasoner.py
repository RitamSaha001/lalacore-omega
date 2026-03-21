from __future__ import annotations

import asyncio
import json
import math
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from concept_graph_engine import ConceptGraphEngine
from engine.got_engine import GraphOfThoughtEngine
from engine.thought_generators import generate_hypotheses, generate_solution_paths
from tools.tool_router import ToolRouter


@dataclass
class ReasoningAction:
    type: str
    content: str
    prior: float
    provider: str = "heuristic"
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "content": self.content,
            "prior": float(max(0.0, min(1.0, self.prior))),
            "provider": self.provider,
            "payload": dict(self.payload),
        }


@dataclass
class ReasoningState:
    question: str
    partial_solution: List[str] = field(default_factory=list)
    tool_results: List[Dict[str, Any]] = field(default_factory=list)
    retrieval_context: List[Dict[str, Any]] = field(default_factory=list)
    confidence: float = 0.35
    depth: int = 0
    verification_pass: bool = False
    provider_trace: List[str] = field(default_factory=list)
    got_context: str = ""

    def clone(self) -> "ReasoningState":
        return ReasoningState(
            question=self.question,
            partial_solution=list(self.partial_solution),
            tool_results=[dict(row) for row in self.tool_results],
            retrieval_context=[dict(row) for row in self.retrieval_context],
            confidence=float(self.confidence),
            depth=int(self.depth),
            verification_pass=bool(self.verification_pass),
            provider_trace=list(self.provider_trace),
            got_context=str(self.got_context),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "question": self.question,
            "partial_solution": list(self.partial_solution),
            "tool_results": [dict(row) for row in self.tool_results],
            "retrieval_context": [dict(row) for row in self.retrieval_context],
            "confidence": float(max(0.0, min(1.0, self.confidence))),
            "depth": int(self.depth),
            "verification_pass": bool(self.verification_pass),
            "provider_trace": list(self.provider_trace),
            "got_context": str(self.got_context),
        }


@dataclass
class TreeNode:
    state: ReasoningState
    parent: Optional["TreeNode"] = None
    action: Optional[ReasoningAction] = None
    prior: float = 1.0
    children: List["TreeNode"] = field(default_factory=list)
    visits: int = 0
    value: float = 0.0
    node_id: int = 0

    @property
    def q(self) -> float:
        return float(self.value / self.visits) if self.visits > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": int(self.node_id),
            "parent_id": int(self.parent.node_id) if self.parent is not None else None,
            "action": self.action.to_dict() if self.action else None,
            "visits": int(self.visits),
            "value": float(round(self.value, 6)),
            "q": float(round(self.q, 6)),
            "prior": float(round(self.prior, 6)),
            "state": self.state.to_dict(),
        }


class SimulationPolicy:
    def __init__(self, *, top_k: int = 3) -> None:
        self.top_k = int(max(1, min(6, top_k)))

    async def propose_actions(
        self,
        *,
        state: ReasoningState,
        profile: Dict[str, Any] | None,
        allow_provider_reasoning: bool,
    ) -> List[ReasoningAction]:
        seed = state.partial_solution[-1] if state.partial_solution else state.question
        provider_fabric = None if allow_provider_reasoning else _NullProviderFabric()

        hypotheses = await generate_hypotheses(
            seed,
            profile=profile,
            provider_fabric=provider_fabric,
            max_items=self.top_k,
            timeout_s=0.50,
        )
        paths = await generate_solution_paths(
            seed,
            profile=profile,
            provider_fabric=provider_fabric,
            max_items=self.top_k,
            timeout_s=0.50,
        )

        actions: List[ReasoningAction] = []
        for row in hypotheses[: self.top_k]:
            content = str(row.get("content", "")).strip()
            if not content:
                continue
            actions.append(
                ReasoningAction(
                    type="generate_hypothesis",
                    content=content,
                    prior=float(row.get("confidence", 0.55) or 0.55),
                    provider=str(row.get("provider", "heuristic")),
                )
            )
        for row in paths[: self.top_k]:
            content = str(row.get("content", "")).strip()
            if not content:
                continue
            actions.append(
                ReasoningAction(
                    type="expand_reasoning_step",
                    content=content,
                    prior=float(row.get("confidence", 0.52) or 0.52),
                    provider=str(row.get("provider", "heuristic")),
                )
            )

        # Additional structured actions.
        low = seed.lower()
        if any(tok in low for tok in ("solve", "equation", "=")):
            expr = _extract_expression(seed)
            actions.append(
                ReasoningAction(
                    type="call_solver_tool",
                    content=f"Solve equation: {expr}",
                    prior=0.67,
                    provider="tool_agent",
                    payload={"tool": "symbolic_solver", "expression": expr},
                )
            )
        if any(tok in low for tok in ("integral", "∫")):
            actions.append(
                ReasoningAction(
                    type="call_solver_tool",
                    content=f"Evaluate integral: {seed[:160]}",
                    prior=0.66,
                    provider="tool_agent",
                    payload={"tool": "integral_solver", "expression": seed[:200]},
                )
            )
        if any(tok in low for tok in ("unit", "dimension", "velocity", "force", "energy", "power")):
            actions.append(
                ReasoningAction(
                    type="verify_intermediate_result",
                    content="Run unit consistency check.",
                    prior=0.60,
                    provider="verifier_agent",
                    payload={"tool": "unit_analysis", "expression": seed[:200]},
                )
            )
        actions.append(
            ReasoningAction(
                type="retrieve_concept",
                content="Retrieve top related concept blocks.",
                prior=0.58,
                provider="retrieval_agent",
            )
        )

        dedupe: List[ReasoningAction] = []
        seen = set()
        for row in actions:
            sig = f"{row.type}|{row.content.lower().strip()}"
            if sig in seen:
                continue
            seen.add(sig)
            dedupe.append(row)

        dedupe.sort(key=lambda row: float(row.prior), reverse=True)
        return dedupe[: self.top_k]


class EvaluationFunction:
    def evaluate(self, state: ReasoningState) -> Dict[str, float]:
        verification = 1.0 if bool(state.verification_pass) else 0.0
        tool_ok = 0.0
        if state.tool_results:
            ok_count = sum(1 for row in state.tool_results if bool(row.get("ok", False)))
            tool_ok = float(ok_count / max(1, len(state.tool_results)))
        concept_alignment = _concept_alignment(state)
        model_conf = float(max(0.0, min(1.0, state.confidence)))
        score = 0.35 * verification + 0.25 * tool_ok + 0.20 * concept_alignment + 0.20 * model_conf
        return {
            "verification": float(round(verification, 6)),
            "tool_support": float(round(tool_ok, 6)),
            "concept_alignment": float(round(concept_alignment, 6)),
            "model_confidence": float(round(model_conf, 6)),
            "score": float(round(max(0.0, min(1.0, score)), 6)),
        }


class MCTSSearch:
    def __init__(
        self,
        *,
        got_engine: GraphOfThoughtEngine | None = None,
        tool_router: ToolRouter | None = None,
        concept_engine: ConceptGraphEngine | None = None,
        simulation_policy: SimulationPolicy | None = None,
        evaluator: EvaluationFunction | None = None,
        c: float = 1.4,
        max_depth: int = 8,
        max_nodes: int = 200,
        max_iterations: int = 50,
        rollout_timeout_s: float = 0.55,
    ) -> None:
        self.c = float(max(0.1, c))
        self.max_depth = int(max(2, min(20, max_depth)))
        self.max_nodes = int(max(20, min(400, max_nodes)))
        self.max_iterations = int(max(5, min(200, max_iterations)))
        self.rollout_timeout_s = float(max(0.25, rollout_timeout_s))
        self.got_engine = got_engine or GraphOfThoughtEngine(max_nodes=12)
        self.tool_router = tool_router or ToolRouter()
        self.concept_engine = concept_engine or ConceptGraphEngine()
        self.policy = simulation_policy or SimulationPolicy(top_k=3)
        self.evaluator = evaluator or EvaluationFunction()
        self.shadow_signal_path = Path("data/replay/mcts_shadow_signals.jsonl")
        self.shadow_signal_path.parent.mkdir(parents=True, exist_ok=True)

    async def search(
        self,
        *,
        question: str,
        profile: Dict[str, Any] | None = None,
        web_retrieval: Dict[str, Any] | None = None,
        input_analysis: Dict[str, Any] | None = None,
        ocr_data: Dict[str, Any] | None = None,
        vision_analysis: Dict[str, Any] | None = None,
        max_iterations: int | None = None,
        max_depth: int | None = None,
        max_nodes: int | None = None,
        allow_provider_reasoning: bool = True,
        developer_mode: bool = False,
        timeout_s: float = 3.2,
    ) -> Dict[str, Any]:
        q = str(question or "").strip()
        if not q:
            return self._empty(reason="empty_question", developer_mode=developer_mode)

        iterations_cap = int(max(1, min(self.max_iterations, max_iterations if max_iterations is not None else self.max_iterations)))
        depth_cap = int(max(1, min(self.max_depth, max_depth if max_depth is not None else self.max_depth)))
        nodes_cap = int(max(5, min(self.max_nodes, max_nodes if max_nodes is not None else self.max_nodes)))
        started = time.perf_counter()

        retrieval_context = self._initial_retrieval_context(
            question=q,
            profile=profile or {},
            web_retrieval=web_retrieval or {},
            ocr_data=ocr_data or {},
            input_analysis=input_analysis or {},
        )
        root_state = ReasoningState(
            question=q,
            partial_solution=[],
            tool_results=[],
            retrieval_context=retrieval_context,
            confidence=0.36,
            depth=0,
            verification_pass=False,
        )
        root = TreeNode(state=root_state, node_id=1, prior=1.0)
        tree_nodes: List[TreeNode] = [root]

        tool_calls = 0
        retrieval_calls = len([row for row in retrieval_context if row.get("source") != "question"])
        provider_scores: Dict[str, float] = {}
        provider_counts: Dict[str, int] = {}
        best_verified_node: TreeNode | None = None
        best_node: TreeNode = root
        stop_reason = "iteration_budget"
        iterations_run = 0

        for iteration in range(iterations_cap):
            iterations_run = iteration + 1
            if time.perf_counter() - started > float(timeout_s):
                stop_reason = "timeout"
                break
            if len(tree_nodes) >= nodes_cap:
                stop_reason = "max_nodes_reached"
                break
            node = self._select(root)
            if node.state.depth >= depth_cap:
                eval_row = self.evaluator.evaluate(node.state)
                self._backpropagate(node, eval_row["score"])
                continue

            expanded = await self._expand(
                node=node,
                profile=profile or {},
                allow_provider_reasoning=allow_provider_reasoning,
                tree_nodes=tree_nodes,
                nodes_cap=nodes_cap,
                depth_cap=depth_cap,
            )
            if not expanded:
                eval_row = self.evaluator.evaluate(node.state)
                self._backpropagate(node, eval_row["score"])
                continue

            selected_child = expanded[0]
            for child in expanded:
                provider = str((child.action.provider if child.action else "") or "heuristic")
                provider_counts[provider] = provider_counts.get(provider, 0) + 1
                provider_scores[provider] = provider_scores.get(provider, 0.0) + float(child.prior)
                if child.state.tool_results:
                    tool_calls += 1
                if child.action and child.action.type == "retrieve_concept":
                    retrieval_calls += 1

            rollout_state = await self._simulate_rollout(
                state=selected_child.state,
                profile=profile or {},
                web_retrieval=web_retrieval or {},
                input_analysis=input_analysis or {},
                ocr_data=ocr_data or {},
                vision_analysis=vision_analysis or {},
                max_depth=depth_cap,
            )
            eval_row = self.evaluator.evaluate(rollout_state)
            self._backpropagate(selected_child, eval_row["score"])

            if rollout_state.verification_pass and (best_verified_node is None or selected_child.q > best_verified_node.q):
                best_verified_node = selected_child
                if eval_row["score"] >= 0.92:
                    stop_reason = "verified_solution_found"
                    break
            if selected_child.q >= best_node.q:
                best_node = selected_child

        if best_verified_node is not None:
            best_node = best_verified_node
        elif root.children:
            best_node = sorted(root.children, key=lambda row: (row.visits, row.q), reverse=True)[0]
            if not best_node.state.verification_pass:
                stop_reason = "unverified_best_path"

        best_path = self._path_from_root(best_node)
        verification_pass = bool(best_node.state.verification_pass)
        final_eval = self.evaluator.evaluate(best_node.state)
        final_confidence = float(final_eval["score"])
        if not verification_pass and best_verified_node is None:
            final_confidence = min(final_confidence, 0.62)

        tree_payload = self._serialize_tree(tree_nodes, include_states=bool(developer_mode))
        arena_scores = self._arena_tree_scores(provider_scores, provider_counts)
        arena_winner = ""
        if arena_scores:
            arena_winner = sorted(arena_scores.items(), key=lambda kv: float(kv[1].get("score", 0.0)), reverse=True)[0][0]

        self._emit_shadow_signal(
            question=q,
            best_path=best_path,
            verification_pass=verification_pass,
            final_confidence=final_confidence,
            provider_scores=arena_scores,
        )

        context_block = self._build_context_block(best_path=best_path, verification_pass=verification_pass, final_confidence=final_confidence)
        elapsed = float(max(0.0, time.perf_counter() - started))

        status = "ok" if verification_pass else "unverified"
        if not best_path:
            status = "failed"
            context_block = ""

        return {
            "status": status,
            "context_block": context_block,
            "best_path": best_path,
            "tree": tree_payload,
            "developer_mode": bool(developer_mode),
            "telemetry": {
                "iterations": int(max(0, iterations_run)),
                "nodes_explored": len(tree_nodes),
                "tool_calls": int(max(0, tool_calls)),
                "retrieval_calls": int(max(0, retrieval_calls)),
                "verification_pass": verification_pass,
                "final_confidence": float(round(final_confidence, 6)),
                "stop_reason": stop_reason,
                "latency_s": float(round(elapsed, 6)),
                "max_nodes": int(nodes_cap),
                "max_depth": int(depth_cap),
                "max_iterations": int(iterations_cap),
                "arena_tree_winner": arena_winner,
                "provider_tree_scores": arena_scores,
            },
        }

    def _select(self, root: TreeNode) -> TreeNode:
        node = root
        while node.children:
            total_visits = max(1, node.visits)
            scored = []
            for child in node.children:
                ucb = child.q + self.c * math.sqrt(math.log(total_visits + 1.0) / (child.visits + 1.0))
                scored.append((ucb, child))
            scored.sort(key=lambda row: row[0], reverse=True)
            node = scored[0][1]
        return node

    async def _expand(
        self,
        *,
        node: TreeNode,
        profile: Dict[str, Any],
        allow_provider_reasoning: bool,
        tree_nodes: List[TreeNode],
        nodes_cap: int,
        depth_cap: int,
    ) -> List[TreeNode]:
        if node.state.depth >= depth_cap:
            return []
        if len(tree_nodes) >= nodes_cap:
            return []

        actions = await self.policy.propose_actions(
            state=node.state,
            profile=profile,
            allow_provider_reasoning=allow_provider_reasoning,
        )
        if not actions:
            return []

        children: List[TreeNode] = []
        for action in actions[:3]:
            if len(tree_nodes) >= nodes_cap:
                break
            child_state = await self._apply_action(node.state, action, profile=profile)
            child = TreeNode(
                state=child_state,
                parent=node,
                action=action,
                prior=float(max(0.01, min(1.0, action.prior))),
                children=[],
                visits=0,
                value=0.0,
                node_id=len(tree_nodes) + 1,
            )
            node.children.append(child)
            tree_nodes.append(child)
            children.append(child)
        return children

    async def _apply_action(
        self,
        state: ReasoningState,
        action: ReasoningAction,
        *,
        profile: Dict[str, Any],
    ) -> ReasoningState:
        nxt = state.clone()
        nxt.depth = int(state.depth + 1)
        nxt.provider_trace.append(str(action.provider))

        if action.type in {"generate_hypothesis", "expand_reasoning_step", "apply_formula"}:
            nxt.partial_solution.append(str(action.content).strip())
            nxt.confidence = min(1.0, nxt.confidence + 0.05)
            return nxt

        if action.type == "call_solver_tool":
            expression = str(action.payload.get("expression", action.content))
            tool_name = str(action.payload.get("tool", "")).strip().lower()
            if tool_name:
                tool_out = self.tool_router.run(tool_name, expression)
            else:
                tool_out = self.tool_router.route_from_text(expression)
            nxt.tool_results.append(dict(tool_out))
            nxt.partial_solution.append(f"Tool[{tool_out.get('tool', 'unknown')}]: {tool_out.get('output', tool_out.get('summary', ''))}")
            nxt.confidence = min(1.0, nxt.confidence + (0.08 if bool(tool_out.get("ok", False)) else -0.04))
            if bool(tool_out.get("ok", False)):
                nxt.verification_pass = nxt.verification_pass or self._quick_verify_tool_result(tool_out)
            return nxt

        if action.type == "verify_intermediate_result":
            expr = str(action.payload.get("expression", action.content))
            verify = self.tool_router.unit_analysis(expr)
            nxt.tool_results.append(dict(verify))
            nxt.partial_solution.append("Verification: " + ("pass" if verify.get("unit_consistent", False) else "needs review"))
            nxt.verification_pass = nxt.verification_pass or bool(verify.get("unit_consistent", False))
            nxt.confidence = min(1.0, nxt.confidence + (0.05 if nxt.verification_pass else -0.02))
            return nxt

        if action.type == "retrieve_concept":
            try:
                concept_rows = self.concept_engine.traverse(
                    state.question,
                    subject=str(profile.get("subject", "general")),
                    top_k=2,
                )
            except Exception:
                concept_rows = []
            for row in concept_rows:
                nxt.retrieval_context.append(
                    {
                        "source": "concept_vault",
                        "title": row.get("title", ""),
                        "text": row.get("text", ""),
                        "score": row.get("score", 0.0),
                    }
                )
            nxt.partial_solution.append("Retrieved concept hints from concept vault.")
            nxt.confidence = min(1.0, nxt.confidence + 0.04)
            return nxt

        nxt.partial_solution.append(str(action.content).strip())
        return nxt

    async def _simulate_rollout(
        self,
        *,
        state: ReasoningState,
        profile: Dict[str, Any],
        web_retrieval: Dict[str, Any],
        input_analysis: Dict[str, Any],
        ocr_data: Dict[str, Any],
        vision_analysis: Dict[str, Any],
        max_depth: int,
    ) -> ReasoningState:
        rollout = state.clone()
        if rollout.depth >= max_depth:
            return rollout

        prompt = rollout.question
        if rollout.partial_solution:
            prompt = f"{rollout.question}\nCurrent path:\n" + "\n".join(f"- {x}" for x in rollout.partial_solution[-4:])

        try:
            got = await asyncio.wait_for(
                self.got_engine.run(
                    question=prompt,
                    profile=profile,
                    web_retrieval=web_retrieval,
                    input_analysis=input_analysis,
                    ocr_data=ocr_data,
                    vision_analysis=vision_analysis,
                    max_nodes=12,
                    allow_provider_reasoning=False,
                    timeout_s=min(0.7, self.rollout_timeout_s),
                ),
                timeout=self.rollout_timeout_s + 0.20,
            )
        except Exception:
            got = {}

        if isinstance(got, dict):
            context_block = str(got.get("context_block", "")).strip()
            if context_block:
                rollout.got_context = context_block
                rollout.partial_solution.append("GoT simulation: " + context_block.splitlines()[0][:180])
            telemetry = dict(got.get("telemetry") or {})
            rollout.confidence = min(1.0, max(0.0, rollout.confidence * 0.65 + float(telemetry.get("final_confidence", 0.0)) * 0.35))
            rollout.verification_pass = rollout.verification_pass or bool(telemetry.get("verification_pass", False))
            rollout.depth = min(max_depth, rollout.depth + 1)

        if not rollout.verification_pass and rollout.tool_results:
            rollout.verification_pass = any(self._quick_verify_tool_result(row) for row in rollout.tool_results)

        return rollout

    def _backpropagate(self, node: TreeNode, score: float) -> None:
        cur: TreeNode | None = node
        val = float(max(0.0, min(1.0, score)))
        while cur is not None:
            cur.visits += 1
            cur.value += val
            cur = cur.parent

    def _quick_verify_tool_result(self, row: Dict[str, Any]) -> bool:
        tool = str(row.get("tool", "")).strip().lower()
        if not bool(row.get("ok", False)):
            return False
        if tool == "unit_analysis":
            return bool(row.get("unit_consistent", False))
        if tool in {"symbolic_solver", "equation_system_solver"}:
            output = row.get("output")
            return bool(output)
        if tool == "integral_solver":
            text = str(row.get("output", "")).lower()
            return text and not any(tok in text for tok in ("nan", "oo", "zoo", "inf"))
        return True

    def _initial_retrieval_context(
        self,
        *,
        question: str,
        profile: Dict[str, Any],
        web_retrieval: Dict[str, Any],
        ocr_data: Dict[str, Any],
        input_analysis: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = [{"source": "question", "text": question[:500]}]

        solution = dict(web_retrieval.get("solution") or {})
        hint = str(solution.get("hint", "")).strip()
        if hint:
            rows.append({"source": "web", "text": hint[:280], "url": str(solution.get("source_url", ""))})
        for match in [row for row in (web_retrieval.get("matches") or []) if isinstance(row, dict)][:2]:
            snippet = str(match.get("snippet", "")).strip()
            if snippet:
                rows.append({"source": str(match.get("source", "web")), "text": snippet[:280], "url": str(match.get("url", ""))})

        if bool(input_analysis.get("ocr_used", False)):
            ocr_text = str(ocr_data.get("clean_text") or ocr_data.get("math_normalized_text") or "").strip()
            if ocr_text:
                rows.append({"source": "ocr", "text": ocr_text[:280]})

        try:
            concept_rows = self.concept_engine.traverse(
                question,
                subject=str(profile.get("subject", "general")),
                top_k=3,
            )
        except Exception:
            concept_rows = []
        for row in concept_rows:
            rows.append({"source": "concept_vault", "title": row.get("title", ""), "text": str(row.get("text", ""))[:240], "score": row.get("score", 0.0)})

        return rows[:10]

    def _path_from_root(self, node: TreeNode) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        cur: TreeNode | None = node
        while cur is not None and cur.parent is not None:
            out.append(
                {
                    "node_id": int(cur.node_id),
                    "action": cur.action.to_dict() if cur.action else None,
                    "q": float(round(cur.q, 6)),
                    "visits": int(cur.visits),
                    "state_confidence": float(round(cur.state.confidence, 6)),
                    "verification_pass": bool(cur.state.verification_pass),
                }
            )
            cur = cur.parent
        out.reverse()
        return out

    def _serialize_tree(self, nodes: Sequence[TreeNode], *, include_states: bool) -> Dict[str, Any]:
        out_nodes = []
        out_edges = []
        for node in nodes:
            payload = {
                "id": int(node.node_id),
                "parent_id": int(node.parent.node_id) if node.parent is not None else None,
                "visits": int(node.visits),
                "q": float(round(node.q, 6)),
                "prior": float(round(node.prior, 6)),
                "action_type": str(node.action.type) if node.action else "root",
                "action_content": str(node.action.content)[:180] if node.action else "root",
            }
            if include_states:
                payload["state"] = node.state.to_dict()
            out_nodes.append(payload)
            if node.parent is not None:
                out_edges.append({"from": int(node.parent.node_id), "to": int(node.node_id)})
        return {"nodes": out_nodes, "edges": out_edges}

    def _arena_tree_scores(self, provider_scores: Dict[str, float], provider_counts: Dict[str, int]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for provider, score in provider_scores.items():
            count = int(provider_counts.get(provider, 0))
            avg = float(score / max(1, count))
            out[provider] = {
                "score": float(round(avg + min(1.0, count / 12.0) * 0.1, 6)),
                "count": count,
                "raw_score": float(round(score, 6)),
            }
        return out

    def _build_context_block(self, *, best_path: Sequence[Dict[str, Any]], verification_pass: bool, final_confidence: float) -> str:
        if not best_path:
            return ""
        lines = [
            "MCTS SEARCH CONTEXT",
            "Use this explored path as a hint, not as forced output.",
            "",
            f"Path confidence: {final_confidence:.3f}",
            f"Verification status: {'pass' if verification_pass else 'unverified'}",
            "",
            "Chosen reasoning path:",
        ]
        for idx, row in enumerate(best_path, start=1):
            action = row.get("action") if isinstance(row.get("action"), dict) else {}
            content = str((action or {}).get("content", "")).strip()
            action_type = str((action or {}).get("type", "")).strip() or "step"
            if content:
                lines.append(f"{idx}. [{action_type}] {content}")
        return "\n".join(lines).strip()

    def _emit_shadow_signal(
        self,
        *,
        question: str,
        best_path: Sequence[Dict[str, Any]],
        verification_pass: bool,
        final_confidence: float,
        provider_scores: Dict[str, Dict[str, Any]],
    ) -> None:
        row = {
            "ts": int(time.time()),
            "question": str(question)[:500],
            "verification_pass": bool(verification_pass),
            "final_confidence": float(round(final_confidence, 6)),
            "path_length": len(best_path),
            "provider_scores": provider_scores,
            "path": list(best_path)[:8],
        }
        try:
            with self.shadow_signal_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=True) + "\n")
        except Exception:
            pass

    def _empty(self, *, reason: str, developer_mode: bool) -> Dict[str, Any]:
        return {
            "status": "failed",
            "context_block": "",
            "best_path": [],
            "tree": {"nodes": [], "edges": []},
            "developer_mode": bool(developer_mode),
            "telemetry": {
                "iterations": 0,
                "nodes_explored": 0,
                "tool_calls": 0,
                "retrieval_calls": 0,
                "verification_pass": False,
                "final_confidence": 0.0,
                "stop_reason": reason,
            },
        }


class _NullProviderFabric:
    def available_providers(self) -> List[str]:
        return []

    async def generate(self, *args: Any, **kwargs: Any):  # pragma: no cover - expected fallback
        raise RuntimeError("provider_reasoning_disabled")


def _concept_alignment(state: ReasoningState) -> float:
    q_tokens = set(re.findall(r"[a-zA-Z][a-zA-Z0-9_]{1,}", state.question.lower()))
    if not q_tokens:
        return 0.0
    aligned = 0
    total = 0
    for row in state.retrieval_context:
        text = str(row.get("text", "")).lower()
        if not text:
            continue
        total += 1
        tokens = set(re.findall(r"[a-zA-Z][a-zA-Z0-9_]{1,}", text))
        if q_tokens.intersection(tokens):
            aligned += 1
    if total == 0:
        return 0.0
    return float(aligned / total)


def _extract_expression(text: str) -> str:
    raw = str(text or "").strip()
    match = re.search(r"([a-zA-Z0-9\^\*\+\-\/\(\)\s]+=[a-zA-Z0-9\^\*\+\-\/\(\)\s]+)", raw)
    if match:
        return match.group(1).strip()
    compact = re.sub(r"\s+", " ", raw)
    return compact[:180]
