from __future__ import annotations

from typing import Any, Dict, List


class RetrievalContextBuilder:
    """
    Builds prompt-safe retrieval context for LalaCore pre-reasoning injection.
    """

    def build(
        self,
        *,
        original_question: str,
        search_payload: Dict[str, Any] | None,
        fetched_solution: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        search_payload = dict(search_payload or {})
        fetched_solution = dict(fetched_solution or {})
        matches = [
            dict(row)
            for row in (search_payload.get("matches") or [])
            if isinstance(row, dict)
        ]
        top_sources = [
            {
                "source": str(row.get("source") or ""),
                "url": str(row.get("url") or ""),
                "title": str(row.get("title") or ""),
                "similarity": float(row.get("similarity", 0.0) or 0.0),
            }
            for row in matches[:5]
        ]
        hint = str(fetched_solution.get("hint") or "").strip()
        possible_answer = str(fetched_solution.get("answer") or "").strip()
        solution_excerpt = str(fetched_solution.get("solution_text") or "").strip()
        formulas = [str(x).strip() for x in (fetched_solution.get("formulas") or []) if str(x).strip()]
        source_label = str(fetched_solution.get("source") or (top_sources[0]["source"] if top_sources else "web"))
        source_url = str(fetched_solution.get("source_url") or (top_sources[0]["url"] if top_sources else ""))

        sections: List[str] = []
        if top_sources or hint or solution_excerpt or formulas:
            sections.append("CONTEXT BLOCK:")
            sections.append("Known similar problem found online.")
            if source_label:
                sections.append(f"Source: {source_label}")
            if top_sources:
                sections.append("")
                sections.append("Top sources:")
                for row in top_sources[:3]:
                    title = str(row.get("title") or "").strip()
                    url = str(row.get("url") or "").strip()
                    label = title if title else url
                    if label:
                        sections.append(f"- {label[:180]}")
            if hint:
                sections.append("")
                sections.append("Hint:")
                sections.append(hint[:1200])
            if possible_answer:
                sections.append("")
                sections.append("Possible Answer:")
                sections.append(possible_answer[:220])
            if formulas:
                sections.append("")
                sections.append("Relevant Formulas:")
                sections.extend(f"- {formula[:180]}" for formula in formulas[:6])
            if solution_excerpt:
                sections.append("")
                sections.append("Explanation excerpt:")
                sections.append(solution_excerpt[:2800])
            if source_url:
                sections.append("")
                sections.append(f"Primary source URL: {source_url}")
            sections.append("")
            sections.append(
                "Instruction: Use this only as auxiliary hints. Do not copy blindly."
            )
            sections.append(
                "Prefer deterministic verification and your normal reasoning pipeline."
            )
            sections.append(
                "If you cite these sources in the final response, explicitly mention the source label or URL."
            )
            sections.append("")
            sections.append("Original user question:")
            sections.append(str(original_question or "").strip())

        context_block = "\n".join(sections).strip()
        final_prompt = (
            f"{context_block}\n\nUser Question:\n{str(original_question or '').strip()}"
            if context_block
            else str(original_question or "").strip()
        )
        citations = [
            {
                "source": row.get("source", ""),
                "url": row.get("url", ""),
                "title": row.get("title", ""),
                "similarity": row.get("similarity", 0.0),
            }
            for row in top_sources
        ]
        return {
            "context_block": context_block,
            "final_prompt": final_prompt,
            "citations": citations,
            "sources_consulted": [row.get("source", "") for row in citations if str(row.get("source", "")).strip()],
            "source_url": source_url,
            "source_label": source_label,
            "hint": hint,
            "possible_answer": possible_answer,
            "solution_excerpt": solution_excerpt,
            "formulas": formulas[:8],
        }
