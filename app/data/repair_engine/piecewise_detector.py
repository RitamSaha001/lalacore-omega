from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class PiecewiseResult:
    text: str
    detected: bool
    cases: list[dict[str, str]]
    actions: list[str]


class PiecewiseDetector:
    """Detect and reconstruct OCR-flattened piecewise function questions."""

    _CASE_RE = re.compile(
        r"(?is)(.+?)\s+for\s+([xX][^,;:.]*?(?:<=|>=|<|>|=)\s*[^,;:.]+)"
    )

    def reconstruct(self, text: str) -> PiecewiseResult:
        src = (text or "").strip()
        if not src:
            return PiecewiseResult(text="", detected=False, cases=[], actions=[])

        cases = self._extract_cases(src)
        if len(cases) < 2:
            return PiecewiseResult(text=src, detected=False, cases=cases, actions=[])

        label = "f(x)"
        fn_match = re.search(r"(?i)\b([a-z])\s*\(\s*x\s*\)", src)
        if fn_match:
            label = f"{fn_match.group(1)}(x)"
        pieces = [f"{row['expr']}, {row['cond']}" for row in cases]
        rebuilt = f"{label} = {{ " + " ; ".join(pieces) + " }"
        return PiecewiseResult(
            text=rebuilt,
            detected=True,
            cases=cases,
            actions=["piecewise_reconstructed"],
        )

    def _extract_cases(self, text: str) -> list[dict[str, str]]:
        matches = list(self._CASE_RE.finditer(text))
        if len(matches) < 2:
            return []
        out: list[dict[str, str]] = []
        for idx, match in enumerate(matches):
            expr = re.sub(r"\s+", " ", match.group(1)).strip(" ,;:")
            cond = re.sub(r"\s+", " ", match.group(2)).strip(" ,;:")
            if idx == 0:
                # Strip leading function declaration noise from first case.
                expr = re.sub(r"(?is)^.*?(?:=|:)\s*", "", expr).strip() or expr
            if expr and cond:
                out.append({"expr": expr, "cond": cond})
        return out
