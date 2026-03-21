from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple


_UNIT_PATTERN = re.compile(
    r"\b(cm|mm|m|km|kg|g|mg|s|sec|ms|min|h|hr|N|J|W|V|A|mol|K|Pa|bar|degree|deg|rad|%)\b",
    flags=re.IGNORECASE,
)


class ResearchMetaVerifier:
    """
    Context-aware post-verification layer that augments deterministic verification.
    """

    def evaluate(
        self,
        *,
        question: str,
        final_answer: str,
        reasoning: str,
        profile: Dict[str, Any] | None = None,
        base_verification: Dict[str, Any] | None = None,
        ocr_data: Dict[str, Any] | None = None,
        vision_analysis: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        profile = profile or {}
        base_verification = base_verification or {}

        expected_type = self._expected_answer_type(question=question, profile=profile)
        observed_type = self._observed_answer_type(final_answer)
        answer_type_match = expected_type in {"unknown", observed_type}

        expected_units, observed_units = self._units(question, final_answer)
        unit_match = True if not expected_units else bool(expected_units.intersection(observed_units))

        context_text = self._context_text(ocr_data=ocr_data, vision_analysis=vision_analysis)
        context_alignment = self._context_overlap(question, context_text)
        cross_modal = self.cross_modal_consistency(
            ocr_text=str(
                (ocr_data or {}).get("clean_text")
                or (ocr_data or {}).get("math_normalized_text")
                or (ocr_data or {}).get("raw_text", "")
            ),
            vision_analysis=vision_analysis or {},
            reasoning_summary=reasoning,
        )

        deterministic_verified = bool(base_verification.get("verified", False))
        risk = float(base_verification.get("risk_score", 1.0))

        reasoning_quality = self._reasoning_quality(reasoning)

        score = 0.0
        score += 0.30 if deterministic_verified else 0.0
        score += 0.20 if answer_type_match else 0.0
        score += 0.15 if unit_match else 0.0
        score += 0.20 * context_alignment
        score += 0.15 * reasoning_quality
        score += 0.15 * float(cross_modal.get("score", 0.0))
        score *= max(0.2, 1.0 - 0.65 * risk)

        issues: List[str] = []
        if not deterministic_verified:
            issues.append("deterministic_not_verified")
        if not answer_type_match:
            issues.append(f"answer_type_mismatch:{expected_type}->{observed_type}")
        if not unit_match:
            issues.append("unit_mismatch")
        if context_alignment < 0.12:
            issues.append("low_context_alignment")
        if reasoning_quality < 0.18:
            issues.append("weak_reasoning_summary")
        if float(cross_modal.get("score", 0.0)) < 0.22:
            issues.append("cross_modal_mismatch")

        return {
            "score": float(max(0.0, min(1.0, score))),
            "answer_type": {
                "expected": expected_type,
                "observed": observed_type,
                "match": bool(answer_type_match),
            },
            "units": {
                "expected": sorted(expected_units),
                "observed": sorted(observed_units),
                "match": bool(unit_match),
            },
            "context_alignment": float(max(0.0, min(1.0, context_alignment))),
            "cross_modal_consistency": cross_modal,
            "reasoning_quality": float(max(0.0, min(1.0, reasoning_quality))),
            "deterministic_verified": bool(deterministic_verified),
            "issues": issues,
        }

    def cross_modal_consistency(
        self,
        *,
        ocr_text: str,
        vision_analysis: Dict[str, Any],
        reasoning_summary: str,
    ) -> Dict[str, Any]:
        ocr_tokens = self._content_tokens(ocr_text)
        vision_text = " ".join(
            [
                str(vision_analysis.get("detected_text", "")),
                str(vision_analysis.get("figure_interpretation", "")),
                " ".join(str(x) for x in vision_analysis.get("structured_math_expressions", [])[:12]),
            ]
        )
        vision_tokens = self._content_tokens(vision_text)
        reasoning_tokens = self._content_tokens(reasoning_summary)

        def _pair_overlap(a: set[str], b: set[str]) -> float:
            if not a or not b:
                return 0.0
            return len(a.intersection(b)) / max(1, min(len(a), len(b)))

        ocr_vs_vision = _pair_overlap(ocr_tokens, vision_tokens)
        ocr_vs_reasoning = _pair_overlap(ocr_tokens, reasoning_tokens)
        vision_vs_reasoning = _pair_overlap(vision_tokens, reasoning_tokens)

        score = 0.40 * ocr_vs_vision + 0.35 * ocr_vs_reasoning + 0.25 * vision_vs_reasoning
        issues: List[str] = []
        if ocr_vs_vision < 0.10:
            issues.append("ocr_vision_mismatch")
        if ocr_vs_reasoning < 0.10:
            issues.append("ocr_reasoning_mismatch")
        if vision_vs_reasoning < 0.10:
            issues.append("vision_reasoning_mismatch")

        return {
            "score": float(max(0.0, min(1.0, score))),
            "ocr_vs_vision": float(max(0.0, min(1.0, ocr_vs_vision))),
            "ocr_vs_reasoning": float(max(0.0, min(1.0, ocr_vs_reasoning))),
            "vision_vs_reasoning": float(max(0.0, min(1.0, vision_vs_reasoning))),
            "issues": issues,
        }

    def detect_self_contradiction(self, original_reasoning: str, review_reasoning: str) -> Dict[str, Any]:
        original = str(original_reasoning or "").lower()
        review = str(review_reasoning or "").lower()
        if not original or not review:
            return {"contradiction": False, "signals": []}

        opposite_pairs = (
            ("increasing", "decreasing"),
            ("positive", "negative"),
            ("valid", "invalid"),
            ("true", "false"),
            ("converges", "diverges"),
            ("possible", "impossible"),
        )
        signals: List[str] = []
        for left, right in opposite_pairs:
            if left in original and right in review:
                signals.append(f"{left}->{right}")
            if right in original and left in review:
                signals.append(f"{right}->{left}")

        # Contradiction often appears as explicit negation against the original conclusion.
        if "therefore" in original and "not" in review and "therefore" in review:
            signals.append("therefore_negation")
        if "correct" in original and "incorrect" in review:
            signals.append("correct_to_incorrect")
        if "incorrect" in original and "correct" in review:
            signals.append("incorrect_to_correct")

        return {"contradiction": bool(signals), "signals": signals[:8]}

    def _expected_answer_type(self, *, question: str, profile: Dict[str, Any]) -> str:
        q = str(question or "").lower()
        if any(token in q for token in ("true or false", "is it true", "boolean")):
            return "boolean"
        if any(token in q for token in ("simplify", "expression", "in terms of", "prove")):
            return "expression"
        if bool(profile.get("numeric", False)) or any(token in q for token in ("value", "evaluate", "compute", "number")):
            return "numeric"
        return "unknown"

    def _observed_answer_type(self, answer: str) -> str:
        text = str(answer or "").strip().lower()
        if not text:
            return "unknown"
        if text in {"true", "false", "yes", "no"}:
            return "boolean"
        if re.fullmatch(r"[-+]?\d+(?:\.\d+)?(?:/\d+)?", text):
            return "numeric"
        if re.search(r"[a-z]\s*[\+\-\*/\^=]", text):
            return "expression"
        if re.search(r"[=\+\-\*/\^]", text):
            return "expression"
        return "symbolic"

    def _units(self, question: str, answer: str) -> Tuple[set[str], set[str]]:
        expected_units = {m.group(1).lower() for m in _UNIT_PATTERN.finditer(str(question or ""))}
        observed_units = {m.group(1).lower() for m in _UNIT_PATTERN.finditer(str(answer or ""))}
        return expected_units, observed_units

    def _context_text(self, *, ocr_data: Dict[str, Any] | None, vision_analysis: Dict[str, Any] | None) -> str:
        parts: List[str] = []
        if isinstance(ocr_data, dict):
            parts.append(str(ocr_data.get("raw_text", "")))
            parts.append(str(ocr_data.get("math_normalized_text", "")))
            parts.append(str(ocr_data.get("clean_text", "")))
        if isinstance(vision_analysis, dict):
            parts.append(str(vision_analysis.get("detected_text", "")))
            parts.append(str(vision_analysis.get("figure_interpretation", "")))
        return "\n".join(part for part in parts if part).strip()

    def _context_overlap(self, question: str, context_text: str) -> float:
        q_tokens = self._content_tokens(question)
        c_tokens = self._content_tokens(context_text)
        if not q_tokens or not c_tokens:
            return 0.0
        overlap = len(q_tokens.intersection(c_tokens)) / max(1, len(q_tokens))
        return float(max(0.0, min(1.0, overlap)))

    def _reasoning_quality(self, reasoning: str) -> float:
        text = str(reasoning or "").strip()
        if not text:
            return 0.0
        length_score = min(1.0, len(text) / 200.0)
        structure_score = 0.2 + 0.4 * bool(re.search(r"\b(therefore|hence|so|because|thus)\b", text, flags=re.IGNORECASE))
        math_score = 0.2 + 0.4 * bool(re.search(r"[=\+\-\*/\^]", text))
        return float(max(0.0, min(1.0, 0.45 * length_score + 0.35 * structure_score + 0.20 * math_score)))

    def _content_tokens(self, text: str) -> set[str]:
        tokens = re.findall(r"[a-zA-Z0-9_\+\-\*/\^=]+", str(text or "").lower())
        return {tok for tok in tokens if len(tok) >= 2}
