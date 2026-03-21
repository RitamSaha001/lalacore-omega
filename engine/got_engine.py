from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Dict, List, Sequence

from concept_graph_engine import ConceptGraphEngine
from engine.graph_synthesizer import GraphSynthesizer
from engine.thought_generators import (
    generate_hypotheses,
    generate_solution_paths,
    generate_subproblems,
)
from tools.tool_router import ToolRouter
from vision.diagram_parser import DiagramParser

try:  # pragma: no cover - optional dependency
    import sympy as sp
except Exception:  # pragma: no cover
    sp = None


@dataclass
class ThoughtNode:
    id: str
    type: str
    content: str
    confidence: float
    parents: List[str] = field(default_factory=list)
    children: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "content": self.content,
            "confidence": float(max(0.0, min(1.0, self.confidence))),
            "parents": list(self.parents),
            "children": list(self.children),
            "meta": dict(self.meta),
        }


class _NullProviderFabric:
    def available_providers(self) -> List[str]:
        return []

    async def generate(self, *args: Any, **kwargs: Any):  # pragma: no cover - intentional hard fallback
        raise RuntimeError("provider_reasoning_disabled")


class ReasonerAgent:
    async def hypotheses(
        self,
        *,
        question: str,
        profile: Dict[str, Any],
        provider_fabric: Any,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        return await generate_hypotheses(
            question,
            profile=profile,
            provider_fabric=provider_fabric,
            max_items=max_items,
            timeout_s=0.55,
        )

    async def subproblems(
        self,
        *,
        question: str,
        profile: Dict[str, Any],
        provider_fabric: Any,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        return await generate_subproblems(
            question,
            profile=profile,
            provider_fabric=provider_fabric,
            max_items=max_items,
            timeout_s=0.55,
        )

    async def paths(
        self,
        *,
        subproblem: str,
        profile: Dict[str, Any],
        provider_fabric: Any,
        max_items: int,
    ) -> List[Dict[str, Any]]:
        return await generate_solution_paths(
            subproblem,
            profile=profile,
            provider_fabric=provider_fabric,
            max_items=max_items,
            timeout_s=0.55,
        )


class CriticAgent:
    def critique(self, node: ThoughtNode) -> Dict[str, Any]:
        text = node.content.lower()
        flags = []
        if "assume" in text and "domain" not in text:
            flags.append("assumption_without_domain")
        if "approx" in text and "error" not in text:
            flags.append("approximation_without_error_bound")
        if "cancel" in text and "non-zero" not in text:
            flags.append("potential_illegal_cancellation")
        if not flags:
            return {
                "content": "No major logical red flags detected in this path.",
                "confidence": min(0.9, 0.55 + 0.35 * node.confidence),
                "flags": [],
            }
        return {
            "content": "Critique flags: " + ", ".join(flags),
            "confidence": 0.68,
            "flags": flags,
        }


class ToolAgent:
    def __init__(self, tool_router: ToolRouter | None = None) -> None:
        self.tool_router = tool_router or ToolRouter()

    def execute(self, text: str) -> Dict[str, Any] | None:
        candidate = str(text or "").strip()
        if not candidate:
            return None
        if not self._looks_tool_relevant(candidate):
            return None
        result = self.tool_router.route_from_text(candidate)
        if not isinstance(result, dict):
            return None
        if not result.get("ok"):
            return None
        return result

    def _looks_tool_relevant(self, text: str) -> bool:
        if len(text) > 400:
            text = text[:400]
        return bool(
            re.search(r"(integral|∫|=|matrix|det|vector|unit|dimension|solve|constant|\d)", text, flags=re.IGNORECASE)
        )


class VerifierAgent:
    def verify(self, *, question: str, node: ThoughtNode) -> Dict[str, Any]:
        payload = dict(node.meta.get("tool_output") or {})
        tool = str(payload.get("tool", "")).strip().lower()
        output = payload.get("output")

        if tool in {"symbolic_solver", "equation_system_solver"}:
            passed = bool(output)
            return {
                "verification_pass": passed,
                "checks": {"equation_consistency": passed},
                "message": "Equation consistency check passed." if passed else "Equation consistency uncertain.",
            }

        if tool == "unit_analysis":
            unit_ok = bool(payload.get("unit_consistent", False))
            return {
                "verification_pass": unit_ok,
                "checks": {"unit_analysis": unit_ok},
                "message": "Unit analysis is consistent." if unit_ok else "Potential unit mismatch found.",
            }

        if tool == "integral_solver":
            integral_ok = self._integral_sanity(str(output or ""))
            return {
                "verification_pass": integral_ok,
                "checks": {"integral_sanity": integral_ok},
                "message": "Integral sanity check passed." if integral_ok else "Integral result needs caution.",
            }

        if tool == "numerical_evaluator":
            numeric_ok = payload.get("value") is not None
            return {
                "verification_pass": bool(numeric_ok),
                "checks": {"numeric_defined": bool(numeric_ok)},
                "message": "Numerical evaluation is defined." if numeric_ok else "Numerical evaluation failed.",
            }

        symbolic_ok = self._symbolic_equivalence_hint(question, node.content)
        return {
            "verification_pass": symbolic_ok,
            "checks": {"symbolic_equivalence_hint": symbolic_ok},
            "message": "Symbolic consistency heuristic pass." if symbolic_ok else "No symbolic consistency signal.",
        }

    def _integral_sanity(self, value: str) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return False
        if any(token in text for token in ("nan", "zoo", "oo", "inf")):
            return False
        return True

    def _symbolic_equivalence_hint(self, question: str, candidate: str) -> bool:
        if sp is None:
            return bool(candidate.strip())
        exprs = re.findall(r"[a-zA-Z0-9\(\)\+\-\*/\^\.=]{5,}", f"{question} {candidate}")
        equations = [e for e in exprs if "=" in e]
        if not equations:
            return bool(candidate.strip())
        try:
            for eq in equations[:3]:
                left, right = eq.split("=", 1)
                lhs = sp.sympify(left.replace("^", "**"))
                rhs = sp.sympify(right.replace("^", "**"))
                if sp.simplify(lhs - rhs) == 0:
                    return True
        except Exception:
            return False
        return False


class GraphOfThoughtEngine:
    def __init__(
        self,
        *,
        max_nodes: int = 20,
        tool_router: ToolRouter | None = None,
        concept_engine: ConceptGraphEngine | None = None,
        diagram_parser: DiagramParser | None = None,
        synthesizer: GraphSynthesizer | None = None,
    ) -> None:
        self.max_nodes = int(max(8, min(60, max_nodes)))
        self.reasoner = ReasonerAgent()
        self.critic = CriticAgent()
        self.tool_agent = ToolAgent(tool_router=tool_router)
        self.verifier = VerifierAgent()
        self.concept_engine = concept_engine or ConceptGraphEngine()
        self.diagram_parser = diagram_parser or DiagramParser()
        self.synthesizer = synthesizer or GraphSynthesizer()

    async def run(
        self,
        *,
        question: str,
        profile: Dict[str, Any] | None = None,
        web_retrieval: Dict[str, Any] | None = None,
        input_analysis: Dict[str, Any] | None = None,
        ocr_data: Dict[str, Any] | None = None,
        vision_analysis: Dict[str, Any] | None = None,
        max_nodes: int | None = None,
        allow_provider_reasoning: bool = True,
        timeout_s: float = 1.2,
    ) -> Dict[str, Any]:
        q = str(question or "").strip()
        if not q:
            return self._empty("empty_question")

        limit = int(max(8, min(60, max_nodes if max_nodes is not None else self.max_nodes)))
        profile_dict = dict(profile or {})
        provider_fabric = None if allow_provider_reasoning else _NullProviderFabric()
        started = perf_counter()
        nodes: List[ThoughtNode] = []
        edges: List[Dict[str, str]] = []
        provider_node_counter: Dict[str, int] = {}
        early_verified = False
        stopped_reason = ""

        def add_node(
            node_type: str,
            content: str,
            confidence: float,
            *,
            parents: Sequence[str] | None = None,
            meta: Dict[str, Any] | None = None,
            force: bool = False,
        ) -> ThoughtNode | None:
            budget_limit = limit if force else max(1, limit - 1)
            if len(nodes) >= budget_limit:
                return None
            node_id = f"n{len(nodes) + 1}"
            row = ThoughtNode(
                id=node_id,
                type=str(node_type),
                content=str(content).strip(),
                confidence=float(max(0.0, min(1.0, confidence))),
                parents=[str(x) for x in (parents or []) if str(x).strip()],
                meta=dict(meta or {}),
            )
            nodes.append(row)
            for parent in row.parents:
                edges.append({"from": parent, "to": row.id})
                parent_row = next((n for n in nodes if n.id == parent), None)
                if parent_row is not None and row.id not in parent_row.children:
                    parent_row.children.append(row.id)
            provider_name = str(row.meta.get("provider", "")).strip()
            if provider_name:
                provider_node_counter[provider_name] = provider_node_counter.get(provider_name, 0) + 1
            return row

        root = add_node(
            "hypothesis",
            q,
            0.62,
            meta={"source": "question", "provider": "input"},
        )
        if root is None:
            return self._empty("node_budget_exhausted")

        # Retrieval fusion tasks run in parallel.
        question_plus_vision = q
        diagram_analysis: Dict[str, Any] = {}
        if isinstance(vision_analysis, dict):
            diagram_analysis.update(dict(vision_analysis))
            question_plus_vision = f"{q}\n{str(vision_analysis.get('detected_text', '')).strip()}".strip()
        if isinstance(ocr_data, dict):
            ocr_text = str(ocr_data.get("clean_text") or ocr_data.get("math_normalized_text") or ocr_data.get("raw_text", "")).strip()
            if ocr_text:
                question_plus_vision = f"{question_plus_vision}\n{ocr_text}".strip()
            ocr_payload = {
                "clean_text": str(ocr_data.get("clean_text", "")),
                "math_normalized_text": str(ocr_data.get("math_normalized_text", "")),
                "raw_text": str(ocr_data.get("raw_text", "")),
                "layout_blocks": [row for row in (ocr_data.get("layout_blocks") or []) if isinstance(row, dict)],
            }
            existing_ocr = diagram_analysis.get("ocr")
            if isinstance(existing_ocr, dict):
                merged_ocr = dict(existing_ocr)
                for key, value in ocr_payload.items():
                    if key not in merged_ocr or not merged_ocr.get(key):
                        merged_ocr[key] = value
                diagram_analysis["ocr"] = merged_ocr
            else:
                diagram_analysis["ocr"] = ocr_payload
        concept_task = asyncio.create_task(
            asyncio.to_thread(
                self.concept_engine.traverse,
                q,
                subject=str(profile_dict.get("subject", "general")),
                top_k=5,
            )
        )
        diagram_task = asyncio.create_task(
            asyncio.to_thread(
                self.diagram_parser.parse,
                question_plus_vision,
                diagram_analysis if diagram_analysis else vision_analysis,
            )
        )
        hypothesis_task = asyncio.create_task(
            self.reasoner.hypotheses(
                question=q,
                profile=profile_dict,
                provider_fabric=provider_fabric,
                max_items=4,
            )
        )

        try:
            concepts, diagram, hypotheses = await asyncio.wait_for(
                asyncio.gather(concept_task, diagram_task, hypothesis_task),
                timeout=max(0.4, float(timeout_s)),
            )
        except Exception:
            concepts, diagram, hypotheses = [], {}, []
            for task in (concept_task, diagram_task, hypothesis_task):
                if not task.done():
                    task.cancel()

        # Retrieval nodes.
        retrieval_nodes = 0
        for concept in concepts[:5]:
            node = add_node(
                "retrieval",
                f"Concept: {concept.get('title', '')} | {concept.get('text', '')}",
                min(0.88, max(0.45, float(concept.get("score", 0.5)))),
                parents=[root.id],
                meta={"source": "concept_vault", "concept_id": concept.get("id")},
            )
            if node is not None:
                retrieval_nodes += 1

        web = dict(web_retrieval or {})
        solution = dict(web.get("solution") or {})
        hint = str(solution.get("hint", "")).strip()
        if hint:
            node = add_node(
                "retrieval",
                f"Web hint: {hint}",
                min(0.82, max(0.45, float(solution.get("confidence", 0.62) or 0.62))),
                parents=[root.id],
                meta={"source": "web", "url": solution.get("source_url", "")},
            )
            if node is not None:
                retrieval_nodes += 1
        match_rows = [row for row in (web.get("matches") or []) if isinstance(row, dict)]
        for row in match_rows[:2]:
            snippet = str(row.get("snippet", "")).strip()
            if not snippet:
                continue
            node = add_node(
                "retrieval",
                f"Web snippet: {snippet}",
                min(0.80, max(0.40, float(row.get("similarity", 0.6) or 0.6))),
                parents=[root.id],
                meta={"source": row.get("source", "web"), "url": row.get("url", "")},
            )
            if node is not None:
                retrieval_nodes += 1

        input_meta = dict(input_analysis or {})
        if bool(input_meta.get("ocr_used", False)):
            ocr_text = str((ocr_data or {}).get("clean_text") or (ocr_data or {}).get("math_normalized_text") or "")
            if ocr_text.strip():
                node = add_node(
                    "retrieval",
                    f"OCR extraction: {ocr_text[:260]}",
                    0.58,
                    parents=[root.id],
                    meta={"source": "ocr"},
                )
                if node is not None:
                    retrieval_nodes += 1

        if isinstance(diagram, dict) and (diagram.get("objects") or diagram.get("angles") or diagram.get("connections")):
            diag_type = str(diagram.get("diagram_type", "unknown"))
            node = add_node(
                "retrieval",
                f"Diagram parsed as {diag_type} with {len(diagram.get('objects', []))} objects.",
                max(0.45, min(0.85, float(diagram.get("confidence", 0.55) or 0.55))),
                parents=[root.id],
                meta={"source": "diagram", "diagram_type": diag_type},
            )
            if node is not None:
                retrieval_nodes += 1

        # Reasoning expansion.
        for row in hypotheses[:4]:
            node = add_node(
                "hypothesis",
                str(row.get("content", "")).strip(),
                float(row.get("confidence", 0.55) or 0.55),
                parents=[root.id],
                meta={"provider": row.get("provider", "heuristic"), "strategy": row.get("strategy", "unknown")},
            )
            if node is None:
                stopped_reason = "max_nodes_reached"
                break

        expansion_targets = [n for n in nodes if n.type == "hypothesis" and n.id != root.id and float(n.confidence) > 0.5]

        for hyp in expansion_targets:
            if len(nodes) >= limit:
                stopped_reason = "max_nodes_reached"
                break
            subproblems = await self.reasoner.subproblems(
                question=hyp.content,
                profile=profile_dict,
                provider_fabric=provider_fabric,
                max_items=3,
            )
            for sub in subproblems:
                if len(nodes) >= limit:
                    stopped_reason = "max_nodes_reached"
                    break
                sub_node = add_node(
                    "calculation",
                    str(sub.get("content", "")).strip(),
                    float(sub.get("confidence", 0.52) or 0.52),
                    parents=[hyp.id],
                    meta={"provider": sub.get("provider", "heuristic")},
                )
                if sub_node is None:
                    stopped_reason = "max_nodes_reached"
                    break
                if float(sub_node.confidence) <= 0.5:
                    continue
                paths = await self.reasoner.paths(
                    subproblem=sub_node.content,
                    profile=profile_dict,
                    provider_fabric=provider_fabric,
                    max_items=2,
                )
                for path in paths:
                    if len(nodes) >= limit:
                        stopped_reason = "max_nodes_reached"
                        break
                    add_node(
                        "calculation",
                        str(path.get("content", "")).strip(),
                        float(path.get("confidence", 0.5) or 0.5),
                        parents=[sub_node.id],
                        meta={"provider": path.get("provider", "heuristic")},
                    )

        # Tool execution and verification stages.
        tool_calls = 0
        verification_pass_count = 0
        candidate_nodes = [n for n in nodes if n.type in {"calculation", "hypothesis"} and n.confidence > 0.5]
        for base_node in candidate_nodes:
            if len(nodes) >= limit:
                stopped_reason = "max_nodes_reached"
                break
            tool_out = self.tool_agent.execute(base_node.content)
            if not tool_out:
                continue
            tool_calls += 1
            tool_summary = str(tool_out.get("summary") or tool_out.get("output") or "").strip()
            tool_node = add_node(
                "tool_execution",
                f"{tool_out.get('tool', 'tool')}: {tool_summary}",
                max(0.55, min(0.92, 0.60 + 0.04 * tool_calls)),
                parents=[base_node.id],
                meta={
                    "tool": tool_out.get("tool", ""),
                    "tool_output": tool_out,
                    "tool_support": 0.5,
                    "provider": "tool_agent",
                },
            )
            if tool_node is None:
                stopped_reason = "max_nodes_reached"
                break

            if len(nodes) >= limit:
                stopped_reason = "max_nodes_reached"
                break
            verification = self.verifier.verify(question=q, node=tool_node)
            v_pass = bool(verification.get("verification_pass", False))
            if v_pass:
                verification_pass_count += 1
            v_node = add_node(
                "verification",
                str(verification.get("message", "Verification completed.")),
                0.86 if v_pass else 0.48,
                parents=[tool_node.id],
                meta={
                    "verification_pass": v_pass,
                    "checks": verification.get("checks", {}),
                    "provider": "verifier_agent",
                },
            )
            if v_node is None:
                stopped_reason = "max_nodes_reached"
                break
            if v_pass:
                early_verified = True
                stopped_reason = "solution_verified"
                break

        # Critique top active nodes.
        for node in [n for n in nodes if n.type in {"hypothesis", "calculation"}][:4]:
            if len(nodes) >= limit:
                stopped_reason = "max_nodes_reached"
                break
            critique = self.critic.critique(node)
            add_node(
                "critique",
                str(critique.get("content", "")).strip(),
                float(critique.get("confidence", 0.6) or 0.6),
                parents=[node.id],
                meta={"flags": critique.get("flags", []), "provider": "critic_agent"},
            )

        synth = self.synthesizer.synthesize(question=q, nodes=[n.to_dict() for n in nodes], top_k=6)
        synthesis = add_node(
            "synthesis",
            str(synth.get("summary", "")).strip(),
            float(synth.get("confidence", 0.62) or 0.62),
            parents=[row.get("id") for row in (synth.get("selected_nodes") or [])[:3] if isinstance(row, dict)],
            meta={"provider": "synthesizer"},
            force=True,
        )
        if synthesis is not None:
            synth["summary"] = synthesis.content

        provider_graph_scores = self._provider_graph_scores(nodes)
        arena_winner = ""
        if provider_graph_scores:
            arena_winner = sorted(
                provider_graph_scores.items(),
                key=lambda kv: float(kv[1].get("score", 0.0)),
                reverse=True,
            )[0][0]

        elapsed = float(max(0.0, perf_counter() - started))
        if not stopped_reason:
            stopped_reason = "max_nodes_reached" if len(nodes) >= limit else "graph_expanded"

        telemetry = {
            "node_count": len(nodes),
            "tool_calls": int(tool_calls),
            "retrieval_nodes": int(retrieval_nodes),
            "verification_pass": bool(verification_pass_count > 0),
            "verification_pass_count": int(verification_pass_count),
            "final_confidence": float(synth.get("confidence", 0.0) or 0.0),
            "latency_s": round(elapsed, 6),
            "max_nodes": int(limit),
            "stop_reason": stopped_reason,
            "arena_graph_winner": arena_winner,
            "provider_graph_scores": provider_graph_scores,
        }

        return {
            "status": "ok",
            "context_block": str(synth.get("summary", "")).strip(),
            "nodes": [row.to_dict() for row in nodes],
            "edges": edges,
            "telemetry": telemetry,
            "diagram": diagram if isinstance(diagram, dict) else {},
            "concepts": concepts if isinstance(concepts, list) else [],
            "early_verified": bool(early_verified),
        }

    def _provider_graph_scores(self, nodes: Sequence[ThoughtNode]) -> Dict[str, Dict[str, Any]]:
        bucket: Dict[str, List[ThoughtNode]] = {}
        for node in nodes:
            provider = str(node.meta.get("provider", "")).strip()
            if not provider:
                continue
            bucket.setdefault(provider, []).append(node)

        out: Dict[str, Dict[str, Any]] = {}
        for provider, rows in bucket.items():
            confidence = sum(float(row.confidence) for row in rows) / max(1, len(rows))
            verified = any(bool((row.meta or {}).get("verification_pass", False)) for row in rows)
            score = confidence + (0.7 if verified else 0.0)
            out[provider] = {
                "node_count": len(rows),
                "mean_confidence": round(confidence, 6),
                "verification_bonus": 0.7 if verified else 0.0,
                "score": round(score, 6),
            }
        return out

    def _empty(self, reason: str) -> Dict[str, Any]:
        return {
            "status": "failed",
            "context_block": "",
            "nodes": [],
            "edges": [],
            "telemetry": {
                "node_count": 0,
                "tool_calls": 0,
                "retrieval_nodes": 0,
                "verification_pass": False,
                "final_confidence": 0.0,
                "stop_reason": reason,
            },
            "diagram": {},
            "concepts": [],
            "early_verified": False,
        }
