from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Dict, List


_INLINE_EQUATION_RE = re.compile(
    r"([A-Za-z0-9()^+\-*/\s]{2,120}=[A-Za-z0-9()^+\-*/\s]{1,120})"
)


@dataclass(slots=True)
class AtlasUnifiedInput:
    clean_question: str
    retrieval_question: str
    context_blocks: List[str]
    concept_hints: List[str]
    equation_hints: List[str]
    question_boundaries: List[str]
    source_metadata: Dict[str, Any]


class AtlasInputHandler:
    """
    Builds structured Atlas-era context on top of the existing intake/OCR pipeline.
    This never replaces OCR/PDF/Vision processing; it only shapes their outputs.
    """

    def build(
        self,
        *,
        question_text: str,
        normalized_question: Dict[str, Any],
        ocr_data: Dict[str, Any] | None = None,
        pdf_data: Dict[str, Any] | None = None,
        vision_analysis: Dict[str, Any] | None = None,
        user_context: Dict[str, Any] | None = None,
    ) -> AtlasUnifiedInput:
        ocr_data = dict(ocr_data or {})
        pdf_data = dict(pdf_data or {})
        vision_analysis = dict(vision_analysis or {})
        user_context = dict(user_context or {})
        normalized_question = dict(normalized_question or {})

        extracted_questions = self._extract_question_candidates(ocr_data, pdf_data)
        question_boundaries = [row["statement"] for row in extracted_questions]
        clean_question = (
            question_boundaries[0]
            if question_boundaries
            else str(normalized_question.get("stem") or question_text or "").strip()
        )
        retrieval_question = clean_question or str(question_text or "").strip()

        equation_hints = self._collect_equations(
            retrieval_question=retrieval_question,
            normalized_question=normalized_question,
            ocr_data=ocr_data,
            pdf_data=pdf_data,
            vision_analysis=vision_analysis,
        )
        concept_hints = self._collect_concepts(
            retrieval_question=retrieval_question,
            normalized_question=normalized_question,
            vision_analysis=vision_analysis,
            user_context=user_context,
        )
        context_blocks = self._build_context_blocks(
            retrieval_question=retrieval_question,
            question_boundaries=question_boundaries,
            concept_hints=concept_hints,
            equation_hints=equation_hints,
            ocr_data=ocr_data,
            pdf_data=pdf_data,
            vision_analysis=vision_analysis,
            user_context=user_context,
        )
        source_metadata = {
            "ocr_confidence": float(ocr_data.get("confidence", 0.0) or 0.0),
            "pdf_confidence": float(pdf_data.get("overall_confidence", 0.0) or 0.0),
            "question_boundaries_detected": len(question_boundaries),
            "equation_count": len(equation_hints),
            "diagram_detected": bool(
                vision_analysis.get("figure_interpretation")
                or vision_analysis.get("is_geometry")
            ),
            "material_id": self._safe_text(
                user_context.get("material_id")
                or (user_context.get("card") or {}).get("material_id")
            ),
        }
        return AtlasUnifiedInput(
            clean_question=clean_question,
            retrieval_question=retrieval_question,
            context_blocks=context_blocks,
            concept_hints=concept_hints,
            equation_hints=equation_hints,
            question_boundaries=question_boundaries,
            source_metadata=source_metadata,
        )

    def _extract_question_candidates(
        self,
        ocr_data: Dict[str, Any],
        pdf_data: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        for payload in (ocr_data, pdf_data):
            for row in (payload.get("lc_iie_questions") or []):
                if not isinstance(row, dict):
                    continue
                statement = self._safe_text(row.get("statement"))
                if not statement:
                    continue
                rows.append(
                    {
                        "statement": statement,
                        "source": self._safe_text(row.get("source") or "ocr"),
                    }
                )
        dedup: List[Dict[str, str]] = []
        seen: set[str] = set()
        for row in rows:
            key = row["statement"].lower()
            if key in seen:
                continue
            seen.add(key)
            dedup.append(row)
            if len(dedup) >= 6:
                break
        return dedup

    def _collect_equations(
        self,
        *,
        retrieval_question: str,
        normalized_question: Dict[str, Any],
        ocr_data: Dict[str, Any],
        pdf_data: Dict[str, Any],
        vision_analysis: Dict[str, Any],
    ) -> List[str]:
        raw: List[str] = []
        for key in ("equation_query", "math_only_query"):
            text = self._safe_text(normalized_question.get(key))
            if text:
                raw.append(text)
        for payload in (ocr_data, pdf_data):
            for key in ("math_normalized_text", "clean_text", "raw_text", "merged_text"):
                text = self._safe_text(payload.get(key))
                if text:
                    raw.extend(match.strip() for match in _INLINE_EQUATION_RE.findall(text))
        for row in (vision_analysis.get("structured_math_expressions") or []):
            text = self._safe_text(row)
            if text:
                raw.append(text)
        for row in (vision_analysis.get("expressions") or []):
            if isinstance(row, dict):
                text = self._safe_text(row.get("latex") or row.get("text"))
            else:
                text = self._safe_text(row)
            if text:
                raw.append(text)
        raw.extend(match.strip() for match in _INLINE_EQUATION_RE.findall(retrieval_question))
        return self._dedupe(raw, cap=8)

    def _collect_concepts(
        self,
        *,
        retrieval_question: str,
        normalized_question: Dict[str, Any],
        vision_analysis: Dict[str, Any],
        user_context: Dict[str, Any],
    ) -> List[str]:
        raw: List[str] = []
        semantic = self._safe_text(normalized_question.get("semantic_query"))
        if semantic:
            raw.extend(part.strip() for part in semantic.split() if part.strip())
        for key in ("classroom_focus_summary", "teacher_insights", "concept_tags"):
            value = user_context.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                raw.extend(self._safe_text(item) for item in value)
        live_profile = user_context.get("student_profile")
        if isinstance(live_profile, dict):
            raw.extend(
                self._safe_text(item)
                for item in (live_profile.get("weak_concepts") or [])
            )
        interpretation = self._safe_text(vision_analysis.get("figure_interpretation"))
        if interpretation:
            raw.append(interpretation)
        raw.extend(re.findall(r"[A-Za-z]{4,}", retrieval_question)[:10])
        return self._dedupe(raw, cap=10)

    def _build_context_blocks(
        self,
        *,
        retrieval_question: str,
        question_boundaries: Sequence[str],
        concept_hints: Sequence[str],
        equation_hints: Sequence[str],
        ocr_data: Dict[str, Any],
        pdf_data: Dict[str, Any],
        vision_analysis: Dict[str, Any],
        user_context: Dict[str, Any],
    ) -> List[str]:
        blocks: List[str] = []
        ocr_conf = float(ocr_data.get("confidence", 0.0) or 0.0)
        if question_boundaries:
            lines = ["STRUCTURED OCR QUESTION BOUNDARIES:"]
            lines.extend(f"- {row}" for row in question_boundaries[:4])
            blocks.append("\n".join(lines))
        if equation_hints:
            lines = ["OCR / VISION EQUATION HINTS:"]
            lines.extend(f"- {row}" for row in equation_hints[:6])
            blocks.append("\n".join(lines))
        if concept_hints:
            blocks.append(
                "ATLAS CONCEPT FOCUS:\n- "
                + "\n- ".join(item for item in concept_hints[:6] if item)
            )
        diagram_text = self._safe_text(vision_analysis.get("figure_interpretation"))
        if diagram_text or vision_analysis.get("is_geometry"):
            descriptor = diagram_text or "Diagram detected by the vision router."
            blocks.append(f"DIAGRAM CONTEXT:\n- {descriptor}")
        if ocr_conf and ocr_conf < 0.60:
            blocks.append(
                "OCR CONFIDENCE WARNING:\n"
                "- OCR confidence is low, so preserve symbol uncertainty and verify algebra carefully."
            )
        card = user_context.get("card")
        if isinstance(card, dict):
            card_lines = [
                self._safe_text(card.get("title")),
                self._safe_text(card.get("subject")),
                self._safe_text(card.get("chapter")),
                self._safe_text(card.get("description")),
            ]
            card_lines = [line for line in card_lines if line]
            if card_lines:
                blocks.append(
                    "STUDY MATERIAL CONTEXT:\n- " + "\n- ".join(card_lines[:4])
                )
        if pdf_data:
            page_count = int(pdf_data.get("page_count", 0) or 0)
            if page_count > 0:
                blocks.append(
                    f"DOCUMENT CONTEXT:\n- Material spans {page_count} page(s) and should be interpreted as a structured study document."
                )
        if retrieval_question and len(retrieval_question.split()) > 30:
            blocks.append(
                "QUESTION NORMALIZATION NOTE:\n- Retrieval uses the cleaned question stem while reasoning keeps the broader context."
            )
        return self._dedupe(blocks, cap=8)

    def _dedupe(self, rows: Sequence[str], *, cap: int) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for row in rows:
            text = self._safe_text(row)
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
            if len(out) >= cap:
                break
        return out

    def _safe_text(self, value: Any) -> str:
        return str(value or "").strip()
