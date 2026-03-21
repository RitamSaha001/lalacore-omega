from __future__ import annotations

from typing import Any, Dict, List, Sequence


class GraphSynthesizer:
    """
    Selects top reasoning nodes and generates a concise synthesis block.
    """

    def synthesize(self, *, question: str, nodes: Sequence[Dict[str, Any]], top_k: int = 6) -> Dict[str, Any]:
        enriched: List[Dict[str, Any]] = []
        for node in nodes:
            conf = float(node.get("confidence", 0.0) or 0.0)
            verification_pass = 1.0 if bool((node.get("meta") or {}).get("verification_pass", False)) else 0.0
            tool_support = float((node.get("meta") or {}).get("tool_support", 0.0) or 0.0)
            score = conf + verification_pass + tool_support
            row = dict(node)
            row["_score"] = round(float(score), 6)
            row["_verification_pass"] = verification_pass
            row["_tool_support"] = tool_support
            enriched.append(row)

        ranked = sorted(enriched, key=lambda row: float(row.get("_score", 0.0)), reverse=True)
        selected = ranked[: max(1, min(12, int(top_k)))]

        hypothesis = [r for r in selected if str(r.get("type")) == "hypothesis"][:2]
        tools = [r for r in selected if str(r.get("type")) == "tool_execution"][:2]
        verification = [r for r in selected if str(r.get("type")) == "verification"][:2]
        retrieval = [r for r in selected if str(r.get("type")) == "retrieval"][:2]
        critique = [r for r in selected if str(r.get("type")) == "critique"][:1]

        lines: List[str] = []
        lines.append("GRAPH-OF-THOUGHT CONTEXT")
        lines.append("Use the following hints as guidance only; verify independently.")
        lines.append("")
        lines.append(f"Original Question: {str(question or '').strip()[:1000]}")
        lines.append("")
        if hypothesis:
            lines.append("Hypotheses:")
            for row in hypothesis:
                lines.append(f"- {self._clean(row.get('content'))}")
        if retrieval:
            lines.append("Retrieval Signals:")
            for row in retrieval:
                src = str((row.get("meta") or {}).get("source", "")).strip()
                prefix = f"[{src}] " if src else ""
                lines.append(f"- {prefix}{self._clean(row.get('content'))}")
        if tools:
            lines.append("Tool Findings:")
            for row in tools:
                tool = str((row.get("meta") or {}).get("tool", "")).strip()
                prefix = f"[{tool}] " if tool else ""
                lines.append(f"- {prefix}{self._clean(row.get('content'))}")
        if verification:
            lines.append("Verification Notes:")
            for row in verification:
                lines.append(f"- {self._clean(row.get('content'))}")
        if critique:
            lines.append("Critique:")
            for row in critique:
                lines.append(f"- {self._clean(row.get('content'))}")

        summary = "\n".join(lines).strip()
        confidence = 0.0
        if selected:
            confidence = sum(float(row.get("confidence", 0.0) or 0.0) for row in selected) / len(selected)
        return {
            "selected_nodes": selected,
            "summary": summary,
            "confidence": round(float(max(0.0, min(1.0, confidence))), 6),
        }

    def _clean(self, value: Any) -> str:
        return " ".join(str(value or "").strip().split())
