from __future__ import annotations

import re
from typing import Any, Dict


_IMAGE_HINT_RE = re.compile(
    r"\b(image|screenshot|camera|photo|scan|snap|uploaded)\b",
    flags=re.IGNORECASE,
)


class InputAnalyzer:
    """
    Lightweight pre-reasoning input analyzer.
    Uses already-produced multimodal artifacts and returns a stable shape.
    """

    def detect_kind(self, detected_type: str) -> str:
        token = str(detected_type or "").strip().lower()
        if token in {"image", "pdf", "text"}:
            return token
        if token == "mixed":
            return "image"
        return "text"

    def should_route_to_ocr(self, *, detected_type: str, user_text: str) -> bool:
        dtype = str(detected_type or "").strip().lower()
        if dtype in {"image", "pdf", "mixed"}:
            return True
        return bool(_IMAGE_HINT_RE.search(str(user_text or "")))

    def build(
        self,
        *,
        detected_type: str,
        question_text: str,
        user_text: str = "",
        ocr_data: Dict[str, Any] | None = None,
        pdf_data: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        ocr_confidence = 0.0
        if isinstance(ocr_data, dict):
            ocr_confidence = float(ocr_data.get("confidence", 0.0) or 0.0)
        elif isinstance(pdf_data, dict):
            ocr_confidence = float(pdf_data.get("overall_confidence", 0.0) or 0.0)
        ocr_confidence = max(0.0, min(1.0, ocr_confidence))

        kind = self.detect_kind(detected_type)
        ocr_used = bool(
            self.should_route_to_ocr(detected_type=detected_type, user_text=user_text)
            and (isinstance(ocr_data, dict) or isinstance(pdf_data, dict))
        )
        return {
            "type": kind,
            "question_text": str(question_text or "").strip(),
            "ocr_used": bool(ocr_used),
            "ocr_confidence": float(ocr_confidence),
        }
