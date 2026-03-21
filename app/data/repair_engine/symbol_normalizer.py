from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any


@dataclass
class SymbolNormalizationResult:
    text: str
    actions: list[str]


class SymbolNormalizer:
    """Deterministic OCR cleanup and symbol normalization for JEE math text."""

    _ESCAPED_UNICODE_MAP = {
        "\\ud835\\udc65": "x",
        "\\ud835\\udc53": "f",
        "\\ud835\\udc54": "g",
        "\\ud835\\udc45": "R",
    }
    _SYMBOL_MAP = {
        "−": "-",
        "—": "-",
        "–": "-",
        "×": "*",
        "·": "*",
        "⋅": "*",
        "÷": "/",
        "≤": "<=",
        "≥": ">=",
        "≠": "!=",
        "→": "->",
        "∞": "infinity",
        "π": "pi",
        "√": "sqrt",
    }
    _NOISE_LINE_RE = re.compile(
        r"(?i)^(?:www\.[\w.-]+|exercise(?:\s*-\s*jee.*)?|answer\s*key|"
        r"solutions?\s*key|if\d{2,4}-\d+|sr\d{2,4}[-_]\d+|fn\d{2,4}[-_]\d+|"
        r"lt\d{2,4}[-_]\d+|cd\d{2,4}[-_]\d+|page\s+\d+(?:\s+of\s+\d+)?)$"
    )
    _FUNC_FIX_RE = re.compile(
        r"(?i)\b(sin|cos|tan|cot|sec|cosec|log|ln|exp|sqrt|sgn)\s*([A-Za-z](?:\^\d+)?|\d+)\b"
    )

    def normalize_text(self, raw: Any) -> SymbolNormalizationResult:
        text = self._to_str(raw)
        actions: list[str] = []
        if not text:
            return SymbolNormalizationResult(text="", actions=actions)

        out = text
        out2 = self._replace_escaped_unicode(out)
        if out2 != out:
            actions.append("unicode_fix")
            out = out2

        out2 = self._normalize_math_alphanumeric(out)
        if out2 != out:
            if "unicode_fix" not in actions:
                actions.append("unicode_fix")
            out = out2

        out2 = self._replace_symbols(out)
        if out2 != out:
            actions.append("symbol_normalized")
            out = out2

        out2 = self._strip_noise_lines(out)
        if out2 != out:
            actions.append("noise_removed")
            out = out2

        out2 = self._normalize_common_ocr_patterns(out)
        if out2 != out:
            actions.append("equation_reconstructed")
            out = out2

        out = re.sub(r"\s+", " ", out).strip()
        return SymbolNormalizationResult(text=out, actions=actions)

    def normalize_options(self, options: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[str]]:
        out: list[dict[str, str]] = []
        actions: list[str] = []
        for idx, option in enumerate(options):
            if not isinstance(option, dict):
                continue
            label = self._to_str(option.get("label")).upper() or chr(65 + min(idx, 25))
            normalized = self.normalize_text(option.get("text"))
            for action in normalized.actions:
                if action not in actions:
                    actions.append(action)
            if normalized.text:
                out.append({"label": label, "text": normalized.text})
        return out, actions

    def _replace_escaped_unicode(self, text: str) -> str:
        out = text
        for src, dst in self._ESCAPED_UNICODE_MAP.items():
            pattern = re.compile(re.escape(src), re.IGNORECASE)
            out = pattern.sub(dst, out)
        return out

    def _normalize_math_alphanumeric(self, text: str) -> str:
        chars: list[str] = []
        for ch in text:
            code = ord(ch)
            if 0xE000 <= code <= 0xF8FF:
                chars.append(" ")
                continue
            if ch in {"□", "■", "▢", "▣", "◻", "◼", "⬜", "⬛", "⧈", "�"}:
                chars.append(" ")
                continue
            if 0x1D400 <= code <= 0x1D7FF:
                chars.append(self._map_math_unicode_char(ch))
                continue
            chars.append(ch)
        return "".join(chars)

    def _map_math_unicode_char(self, ch: str) -> str:
        name = unicodedata.name(ch, "")
        if "DIGIT" in name:
            m = re.search(r"\bDIGIT\s+(\d)\b", name)
            if m:
                return m.group(1)
        if "SMALL" in name:
            m = re.search(r"\bSMALL\s+([A-Z])\b", name)
            if m:
                return m.group(1).lower()
        if "CAPITAL" in name:
            m = re.search(r"\bCAPITAL\s+([A-Z])\b", name)
            if m:
                return m.group(1)
        return ch

    def _replace_symbols(self, text: str) -> str:
        out = text
        for src, dst in self._SYMBOL_MAP.items():
            out = out.replace(src, dst)
        return out

    def _strip_noise_lines(self, text: str) -> str:
        lines = re.split(r"\r?\n", text)
        if len(lines) <= 1:
            line = self._to_str(text).strip()
            return "" if self._NOISE_LINE_RE.match(line) else line
        kept: list[str] = []
        for raw in lines:
            line = self._to_str(raw).strip()
            if not line:
                continue
            if self._NOISE_LINE_RE.match(line):
                continue
            if re.fullmatch(r"\d{1,3}\s*/\s*\d{1,3}", line):
                continue
            kept.append(line)
        return " ".join(kept).strip()

    def _normalize_common_ocr_patterns(self, text: str) -> str:
        out = text
        out = re.sub(r"\bf\*\s*\(", "f(", out, flags=re.IGNORECASE)
        out = re.sub(r"\bg\*\s*\(", "g(", out, flags=re.IGNORECASE)
        out = re.sub(r"\b([A-Za-z])\s*([2-9])\b", r"\1^\2", out)
        out = re.sub(r"\b([A-Za-z])\s+\1\b", r"\1^2", out)
        out = re.sub(r"\b([A-Za-z])\s+\1\s+\1\b", r"\1^3", out)
        out = self._FUNC_FIX_RE.sub(lambda m: f"{m.group(1).lower()}({m.group(2)})", out)
        out = re.sub(r"\bsgn\(\s*([A-Za-z])([2-9])\s*\)", r"sgn(\1^\2)", out, flags=re.IGNORECASE)
        out = re.sub(r"\blim\s+([A-Za-z])\s*->\s*(?=[^\w]|$)", r"lim_{\1->?}", out, flags=re.IGNORECASE)
        return out

    def _to_str(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return str(value)
