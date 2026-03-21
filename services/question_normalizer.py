from __future__ import annotations

import re
from typing import Dict, List

from latex_sanitizer import sanitize_latex


_UNICODE_TO_LATEX = {
    "∫": r"\int",
    "∑": r"\sum",
    "√": r"\sqrt",
    "π": r"\pi",
    "∞": r"\infty",
    "≤": r"\leq",
    "≥": r"\geq",
    "≠": r"\neq",
    "≈": r"\approx",
    "×": r"\times",
    "÷": r"\div",
    "−": "-",
    "→": r"\to",
}

_SUBSCRIPT_MAP = str.maketrans(
    {
        "₀": "0",
        "₁": "1",
        "₂": "2",
        "₃": "3",
        "₄": "4",
        "₅": "5",
        "₆": "6",
        "₇": "7",
        "₈": "8",
        "₉": "9",
    }
)
_SUPERSCRIPT_MAP = str.maketrans(
    {
        "⁰": "0",
        "¹": "1",
        "²": "2",
        "³": "3",
        "⁴": "4",
        "⁵": "5",
        "⁶": "6",
        "⁷": "7",
        "⁸": "8",
        "⁹": "9",
        "⁻": "-",
    }
)

_OPTION_LINE_RE = re.compile(
    r"^\s*(?:\(?\s*[A-D]\s*\)?|(?:option\s*)?[A-D]|[1-4])\s*[\).:\-]\s+.+$",
    flags=re.IGNORECASE,
)
_ANSWER_LINE_RE = re.compile(
    r"^\s*(?:ans(?:wer)?|correct\s*option)\s*[:\-].*$",
    flags=re.IGNORECASE,
)


class QuestionNormalizer:
    """
    Canonical normalization for web retrieval matching.
    """

    def normalize(self, question_text: str) -> Dict[str, str | bool]:
        original = str(question_text or "").strip()
        if not original:
            return {
                "original": "",
                "latex_normalized": "",
                "stem": "",
                "search_query": "",
                "partial_query": "",
                "math_only_query": "",
                "options_removed": False,
            }

        unicode_fixed = self._unicode_math_to_latex(original)
        latex_normalized = sanitize_latex(unicode_fixed)
        stem, options_removed = self._extract_stem(latex_normalized)
        search_query = self._to_search_query(stem)
        partial_query = self._truncate_words(search_query, 18)
        math_only_query = self._extract_math_query(stem)
        return {
            "original": original,
            "latex_normalized": latex_normalized,
            "stem": stem,
            "search_query": search_query,
            "partial_query": partial_query,
            "math_only_query": math_only_query,
            "options_removed": options_removed,
        }

    def _unicode_math_to_latex(self, text: str) -> str:
        out = str(text or "").translate(_SUBSCRIPT_MAP).translate(_SUPERSCRIPT_MAP)
        for source, target in _UNICODE_TO_LATEX.items():
            out = out.replace(source, target)

        # Convert common compact integral notation after subscript/superscript translation.
        out = re.sub(
            r"\\int\s*([\-+]?\d+)\s*([\-+]?\d+)",
            r"\\int_{\1}^{\2}",
            out,
        )
        out = re.sub(r"\s+", " ", out).strip()
        return out

    def _extract_stem(self, text: str) -> tuple[str, bool]:
        inline_option = re.search(
            r"(?is)\s(?:\(|\b)(?:A|1)\)?\s*[\).:\-]\s+.*(?:\(|\b)(?:B|2)\)?\s*[\).:\-]\s+",
            str(text or ""),
        )
        if inline_option:
            stem_inline = str(text or "")[: inline_option.start()].strip()
            if stem_inline:
                return re.sub(r"\s+", " ", stem_inline).strip(), True

        lines = [ln.strip() for ln in str(text or "").splitlines() if ln.strip()]
        if not lines:
            return str(text or "").strip(), False

        option_idx = None
        for idx, line in enumerate(lines):
            if _OPTION_LINE_RE.match(line) or _ANSWER_LINE_RE.match(line):
                option_idx = idx
                break
        if option_idx is None:
            stem = " ".join(lines)
            return re.sub(r"\s+", " ", stem).strip(), False

        stem_lines = lines[:option_idx]
        stem = " ".join(stem_lines).strip()
        if not stem:
            stem = lines[0]
        stem = re.sub(r"\s+", " ", stem).strip()
        return stem, True

    def _extract_math_query(self, text: str) -> str:
        raw = str(text or "")
        inline = re.findall(r"\$([^$]{1,200})\$", raw)
        if inline:
            return re.sub(r"\s+", " ", " ; ".join(inline[:3])).strip()
        latex_cmds = re.findall(r"(\\[a-zA-Z]+(?:_[^\s{}]+|\{[^}]+\})?(?:\^[^\s{}]+|\{[^}]+\})?)", raw)
        if latex_cmds:
            return re.sub(r"\s+", " ", " ".join(latex_cmds[:8])).strip()
        return ""

    def _to_search_query(self, stem: str) -> str:
        text = str(stem or "")

        replacements = {
            r"\\int_?\{?([^\s{}]+)\}?\^?\{?([^\s{}]+)\}?": r"integral from \1 to \2",
            r"\\sum_?\{?([^\s{}]+)\}?\^?\{?([^\s{}]+)\}?": r"summation from \1 to \2",
            r"\\sqrt\{([^}]+)\}": r"square root of \1",
            r"\\frac\{([^}]+)\}\{([^}]+)\}": r"\1 over \2",
            r"\\pi": "pi",
            r"\\theta": "theta",
            r"\\infty": "infinity",
            r"\\leq": "less than or equal to",
            r"\\geq": "greater than or equal to",
            r"\\neq": "not equal to",
            r"\\times": "times",
            r"\\div": "divided by",
            r"\\to": "to",
            r"\\left": "",
            r"\\right": "",
        }
        for pattern, repl in replacements.items():
            text = re.sub(pattern, repl, text)

        text = re.sub(r"\$+", " ", text)
        text = re.sub(r"[{}\\]", " ", text)
        text = re.sub(r"[_^]", " ", text)
        text = re.sub(r"(?i)\b(find|evaluate|determine|solve|calculate|compute)\b", " ", text)
        text = re.sub(r"(?i)\bvalue of\b", " ", text)
        text = re.sub(r"[^a-zA-Z0-9+\-*/=().,\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()

        # Keep query compact for external search providers.
        words: List[str] = [w for w in text.split(" ") if w]
        if len(words) > 28:
            words = words[:28]
        return " ".join(words)

    def _truncate_words(self, text: str, max_words: int) -> str:
        words = [w for w in str(text or "").split() if w]
        return " ".join(words[: max(1, int(max_words))])
