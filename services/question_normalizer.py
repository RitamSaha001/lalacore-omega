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
                "semantic_query": "",
                "equation_query": "",
                "formula_query": "",
                "options_removed": False,
            }

        unicode_fixed = self._unicode_math_to_latex(original)
        latex_normalized = sanitize_latex(unicode_fixed)
        stem, options_removed = self._extract_stem(latex_normalized)
        search_query = self._to_search_query(stem)
        partial_query = self._truncate_words(search_query, 18)
        math_only_query = self._extract_math_query(stem)
        semantic_query = self._build_semantic_query(stem)
        equation_query = self._extract_equation_query(stem)
        formula_query = self._build_formula_query(
            stem=stem,
            semantic_query=semantic_query,
            equation_query=equation_query,
        )
        return {
            "original": original,
            "latex_normalized": latex_normalized,
            "stem": stem,
            "search_query": search_query,
            "partial_query": partial_query,
            "math_only_query": math_only_query,
            "semantic_query": semantic_query,
            "equation_query": equation_query,
            "formula_query": formula_query,
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
        equation = self._extract_equation_query(raw)
        if equation:
            return equation
        inline = re.findall(r"\$([^$]{1,200})\$", raw)
        if inline:
            return re.sub(r"\s+", " ", " ; ".join(inline[:3])).strip()
        latex_cmds = re.findall(r"(\\[a-zA-Z]+(?:_[^\s{}]+|\{[^}]+\})?(?:\^[^\s{}]+|\{[^}]+\})?)", raw)
        if latex_cmds:
            return re.sub(r"\s+", " ", " ".join(latex_cmds[:8])).strip()
        return ""

    def _extract_equation_query(self, text: str) -> str:
        raw = str(text or "")
        if not raw:
            return ""
        cleaned = raw.replace("$", " ")
        cleaned = re.sub(r"\\frac\{([^}]+)\}\{([^}]+)\}", r"(\1)/(\2)", cleaned)
        cleaned = re.sub(r"\\left|\\right", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        patterns = (
            r"([a-zA-Z0-9()^+\-*/\s]{3,120}=[a-zA-Z0-9()^+\-*/\s]{1,80})",
            r"([a-zA-Z]\^2\s*/\s*\d+\s*[+\-]\s*[a-zA-Z]\^2\s*/\s*\d+\s*=\s*\d+)",
        )
        for pattern in patterns:
            match = re.search(pattern, cleaned)
            if not match:
                continue
            chunk = re.sub(r"\s+", " ", str(match.group(1))).strip(" ,.;:")
            chunk = re.sub(r"(?i)^.*?\b([a-z]\^2.*=.*)$", r"\1", chunk)
            chunk = re.sub(r"\s*/\s*", "/", chunk)
            chunk = re.sub(r"\s*([=+\-])\s*", r" \1 ", chunk)
            chunk = re.sub(r"\s+", " ", chunk).strip()
            if len(chunk) >= 5:
                return chunk[:160]
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

        text = re.sub(r"([a-zA-Z])\s*\^\s*2\b", r"\1 squared", text)
        text = re.sub(r"([a-zA-Z])\s*\^\s*3\b", r"\1 cubed", text)
        text = re.sub(r"([a-zA-Z0-9)\]])\s*/\s*([a-zA-Z0-9(])", r"\1 over \2", text)
        text = re.sub(r"\$+", " ", text)
        text = re.sub(r"[{}\\]", " ", text)
        text = re.sub(r"[_^]", " ", text)
        text = re.sub(
            r"(?i)\b(include|with|show|add)\s+cited\s+sources?(?:\s+if\s+available)?\b",
            " ",
            text,
        )
        text = re.sub(r"(?i)\bif available\b", " ", text)
        text = re.sub(r"(?i)\bstep by step\b", " ", text)
        text = re.sub(r"(?i)\bstep-by-step\b", " ", text)
        text = re.sub(r"(?i)\bfull solution\b", " ", text)
        text = re.sub(
            r"(?i)\b(?:jee(?:\s+advanced)?|advanced|mains?|level|question|give|write)\b",
            " ",
            text,
        )
        text = re.sub(r"(?i)\bfor the\b", " ", text)
        text = re.sub(r"(?i)\b(find|evaluate|determine|solve|calculate|compute)\b", " ", text)
        text = re.sub(r"(?i)\bvalue of\b", " ", text)
        text = re.sub(r"(?i)\bits\b", " ", text)
        text = re.sub(r"[^a-zA-Z0-9+\-*/=().,\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()

        # Drop low-signal leading scaffolding that hurts external retrieval quality.
        text = re.sub(
            r"^(?:for\s+)?(?:the\s+)?(?:hyperbola|ellipse|parabola|circle)\s+",
            lambda m: m.group(0).strip() + " ",
            text,
        )
        text = re.sub(
            r"^(?:jee\s+advanced\s+)?(?:level\s+)?(?:question\s+)?",
            "",
            text,
        ).strip()

        # Keep query compact for external search providers.
        words: List[str] = [w for w in text.split(" ") if w]
        if len(words) > 28:
            words = words[:28]
        return " ".join(words)

    def _build_semantic_query(self, stem: str) -> str:
        low = str(stem or "").lower()
        if not low:
            return ""
        low = re.sub(r"\$[^$]{1,220}\$", " ", low)
        low = re.sub(r"\\[a-z]+(?:\{[^}]+\})*", " ", low)
        keywords = [
            "hyperbola",
            "ellipse",
            "parabola",
            "circle",
            "conic",
            "eccentricity",
            "asymptote",
            "asymptotes",
            "focus",
            "directrix",
            "latus rectum",
            "tangent",
            "normal",
            "chord",
            "integral",
            "derivative",
            "limit",
            "series",
            "permutation",
            "combination",
            "probability",
            "binomial",
            "matrix",
            "determinant",
            "vector",
            "complex",
            "modulus",
            "argand",
        ]
        found: List[str] = []
        for keyword in keywords:
            if keyword in low and keyword not in found:
                found.append(keyword)
        if found:
            return " ".join(found[:6])

        stopwords = {
            "for",
            "the",
            "and",
            "with",
            "from",
            "that",
            "this",
            "find",
            "show",
            "include",
            "cited",
            "sources",
            "available",
            "question",
            "what",
            "which",
            "there",
            "their",
            "then",
            "into",
            "its",
        }
        tokens = [
            tok
            for tok in re.findall(r"[a-z]{3,}", low)
            if tok not in stopwords
        ]
        dedup: List[str] = []
        for token in tokens:
            if token not in dedup:
                dedup.append(token)
            if len(dedup) >= 6:
                break
        return " ".join(dedup)

    def _build_formula_query(
        self,
        *,
        stem: str,
        semantic_query: str,
        equation_query: str,
    ) -> str:
        semantic = str(semantic_query or "").strip()
        equation = str(equation_query or "").strip()
        tokens = [token for token in semantic.split() if token]
        if equation and tokens:
            head = " ".join(tokens[:4])
            return f"{head} {equation} formula".strip()[:220]
        if tokens:
            return f"{' '.join(tokens[:4])} formula".strip()[:220]
        if equation:
            return f"{equation} formula".strip()[:220]
        fallback = re.findall(r"[a-z]{4,}", str(stem or "").lower())
        if not fallback:
            return ""
        return f"{' '.join(fallback[:4])} formula".strip()[:220]

    def _truncate_words(self, text: str, max_words: int) -> str:
        words = [w for w in str(text or "").split() if w]
        return " ".join(words[: max(1, int(max_words))])
