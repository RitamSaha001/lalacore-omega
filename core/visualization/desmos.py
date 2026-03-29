from __future__ import annotations

import math
import re
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Dict, List, Sequence

_GRAPH_KEYWORDS = (
    "sketch",
    "plot",
    "graph",
    "locus",
    "area between",
    "area bounded",
    "region bounded",
    "intersection",
    "intersections",
)

_CALCULUS_GRAPH_HINTS = (
    "asymptote",
    "asymptotes",
    "increasing",
    "decreasing",
    "maxima",
    "minima",
    "stationary point",
)

_CONIC_HINTS = (
    "hyperbola",
    "ellipse",
    "parabola",
    "circle",
    "conic",
    "conics",
)

_COORDINATE_HINTS = (
    "x=",
    "y=",
    "x^",
    "y^",
    "f(x)",
    "(x, y)",
    "theta",
    "r=",
)

_ALLOWED_LATEX = re.compile(r"^[a-zA-Z0-9\s\+\-\*\/\^\=\(\)\[\]\{\}\|<>,\._\\:]+$")
_EQUATION_EXTRACTOR = re.compile(
    r"([a-zA-Z0-9\(\)\[\]\{\}\|\\\+\-\*\/\^\s\.,]+(?:=|<=|>=|<|>)[a-zA-Z0-9\(\)\[\]\{\}\|\\\+\-\*\/\^\s\.,]+)"
)

_PALETTE = (
    "#2D70B3",
    "#C74440",
    "#388C46",
    "#6042A6",
    "#FA7E19",
    "#000000",
    "#2B8FB8",
    "#7A3E9D",
)

_SAFE_EVAL_ENV = {
    "sin": math.sin,
    "cos": math.cos,
    "tan": math.tan,
    "asin": math.asin,
    "acos": math.acos,
    "atan": math.atan,
    "log": math.log,
    "ln": math.log,
    "sqrt": math.sqrt,
    "exp": math.exp,
    "abs": abs,
    "pi": math.pi,
    "e": math.e,
}


@dataclass
class _Viewport:
    xmin: float
    xmax: float
    ymin: float
    ymax: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "xmin": round(self.xmin, 6),
            "xmax": round(self.xmax, 6),
            "ymin": round(self.ymin, 6),
            "ymax": round(self.ymax, 6),
        }


class DesmosGraphBuilder:
    """
    Builds safe Desmos-ready visualization payloads from graph-oriented questions.
    """

    def __init__(self, *, max_expressions: int = 20, timeout_s: float = 0.5) -> None:
        self.max_expressions = int(max(1, max_expressions))
        self.timeout_s = float(max(0.05, timeout_s))

    def build(
        self,
        *,
        question: str,
        profile: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        start = perf_counter()
        if not self._is_graph_intent(question=question, profile=profile or {}):
            return None

        expressions = self._extract_expressions(question)
        if not expressions:
            return None
        if perf_counter() - start > self.timeout_s:
            return None

        expressions = self._optimize_expressions(expressions)[: self.max_expressions]
        if not expressions:
            return None

        viewport = self._auto_viewport(expressions)
        span_x = abs(viewport.xmax - viewport.xmin)
        span_y = abs(viewport.ymax - viewport.ymin)
        has_implicit = any("=" in e and not e.strip().startswith(("x=", "y=")) for e in expressions)
        heavy = len(expressions) > 5 or span_x > 200 or span_y > 200 or has_implicit

        payload = {
            "type": "desmos",
            "expressions": [
                {
                    "id": f"eq{i + 1}",
                    "latex": expr,
                    "color": _PALETTE[i % len(_PALETTE)],
                    "lineStyle": "solid",
                }
                for i, expr in enumerate(expressions)
            ],
            "viewport": viewport.to_dict(),
            "options": {
                "showGrid": True,
                "showAxes": True,
                "lockViewport": False,
                "projectorMode": False,
                "disableAnimation": heavy,
                "reducedPrecision": heavy,
                "disableShadingGradients": heavy,
            },
        }
        return payload

    def _is_graph_intent(self, *, question: str, profile: Dict[str, Any]) -> bool:
        text = (question or "").strip().lower()
        if not text:
            return False

        keyword_hit = any(k in text for k in _GRAPH_KEYWORDS)
        profile_subject = str(profile.get("subject", "")).lower()
        graph_like = bool(profile.get("graph_like")) or "graph" in str(profile.get("difficulty", "")).lower()
        has_coordinate_context = any(k in text for k in _COORDINATE_HINTS) or bool(
            re.search(r"\bx\b", text) and re.search(r"\by\b", text)
        )
        calculus_hint = any(k in text for k in _CALCULUS_GRAPH_HINTS)
        conic_hint = any(k in text for k in _CONIC_HINTS)

        if keyword_hit:
            return has_coordinate_context or calculus_hint or profile_subject in {"math", "calculus", "coordinate geometry"}
        if profile_subject in {"math", "calculus", "coordinate geometry"} and graph_like and has_coordinate_context:
            return True
        if conic_hint and has_coordinate_context and profile_subject in {"math", "coordinate geometry", "general"}:
            return True
        if calculus_hint and has_coordinate_context:
            return True
        return False

    def _extract_expressions(self, question: str) -> List[str]:
        raw = str(question or "")
        candidates: List[str] = []

        segments = re.split(r"[\n;]", raw)
        for segment in segments:
            if not segment.strip():
                continue
            parts = re.split(r"\band\b", segment, flags=re.IGNORECASE)
            for part in parts:
                expr = self._normalize_expr(part)
                if not expr:
                    continue
                regex_matches = [
                    self._normalize_expr(match)
                    for match in _EQUATION_EXTRACTOR.findall(expr)
                ]
                regex_matches = [match for match in regex_matches if match]
                if regex_matches:
                    candidates.extend(regex_matches)
                    continue
                if not re.search(r"(=|<=|>=|<|>)", expr):
                    continue
                marker = re.search(r"(x|y|r|\\theta)", expr.lower())
                if marker is None:
                    if "x^2+y^2=" in re.sub(r"\s+", "", expr.lower()):
                        candidates.append(expr)
                    continue
                candidates.append(expr[marker.start() :].strip())

        if not candidates:
            for part in re.split(r"[\n;]", raw):
                part = self._normalize_expr(part)
                if part and ("x" in part.lower() or "y" in part.lower() or "r=" in part.lower()):
                    candidates.append(part)

        out: List[str] = []
        for expr in candidates:
            if self._is_safe_latex(expr):
                out.append(expr)
        return out

    def _normalize_expr(self, expr: str) -> str:
        text = str(expr or "").strip()
        if not text:
            return ""
        text = text.replace("−", "-").replace("—", "-").replace("×", "*").replace("÷", "/")
        text = text.replace("**", "^")
        text = text.replace("theta", r"\theta").replace("Theta", r"\theta")
        text = re.sub(r"\s+", " ", text)
        text = text.replace(" = ", "=").replace(" <= ", "<=").replace(" >= ", ">=").replace(" < ", "<").replace(" > ", ">")
        text = re.sub(
            r"^(?:(?:jee|advanced|level|question|plot|graph|sketch|draw|for|the|hyperbola|ellipse|parabola|circle|conic|same)\b[:\s]*)+",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            r"\s+\b(on|where|when|mark|find|same)\b.*$",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = text.strip(" ,.")
        return text

    def _is_safe_latex(self, expr: str) -> bool:
        lowered = expr.lower()
        if any(token in lowered for token in ("javascript:", "<script", "</script", "onerror=", "onclick=")):
            return False
        if not _ALLOWED_LATEX.match(expr):
            return False
        if expr.count("{") != expr.count("}") or expr.count("(") != expr.count(")"):
            return False
        if len(expr) > 220:
            return False
        if "=" not in expr and "<" not in expr and ">" not in expr:
            return False
        return True

    def _optimize_expressions(self, expressions: Sequence[str]) -> List[str]:
        seen: set[str] = set()
        out: List[str] = []
        for expr in expressions:
            compact = re.sub(r"\s+", "", expr)
            if compact in seen:
                continue
            seen.add(compact)
            out.append(expr.strip())
        return out

    def _auto_viewport(self, expressions: Sequence[str]) -> _Viewport:
        x_points: List[float] = []
        y_points: List[float] = []

        for expr in expressions:
            x_points.extend(self._extract_numeric_literals(expr))

            if expr.lower().startswith("x="):
                value = self._safe_number(expr.split("=", 1)[1])
                if value is not None:
                    x_points.append(value)
                continue

            if expr.lower().startswith("y="):
                rhs = expr.split("=", 1)[1]
                ys = self._sample_function(rhs)
                y_points.extend(ys)
                continue

            if "x^2+y^2=" in re.sub(r"\s+", "", expr.lower()):
                rhs = expr.split("=", 1)[1]
                radius_sq = self._safe_number(rhs)
                if radius_sq is not None and radius_sq > 0:
                    r = math.sqrt(radius_sq)
                    x_points.extend([-r, r])
                    y_points.extend([-r, r])

        if not x_points:
            x_points = [-10.0, 10.0]
        if not y_points:
            y_points = [-10.0, 10.0]

        xmin, xmax = min(x_points), max(x_points)
        ymin, ymax = min(y_points), max(y_points)
        xmin, xmax = self._inflate(xmin, xmax)
        ymin, ymax = self._inflate(ymin, ymax)
        return _Viewport(
            xmin=max(-1000.0, xmin),
            xmax=min(1000.0, xmax),
            ymin=max(-1000.0, ymin),
            ymax=min(1000.0, ymax),
        )

    def _extract_numeric_literals(self, expr: str) -> List[float]:
        out: List[float] = []
        for token in re.findall(r"-?\d+(?:\.\d+)?", expr):
            value = self._safe_number(token)
            if value is None:
                continue
            out.append(value)
        return out

    def _sample_function(self, rhs: str) -> List[float]:
        cleaned = rhs.replace("^", "**").replace(r"\theta", "theta")
        y_values: List[float] = []

        sample_x = [-10, -6, -3, -2, -1, -0.5, 0, 0.5, 1, 2, 3, 6, 10]
        for value in sample_x:
            try:
                y_f = self._safe_eval_math(cleaned, value)
                if math.isfinite(y_f):
                    y_values.append(max(-1000.0, min(1000.0, y_f)))
            except Exception:
                continue
        return y_values

    def _inflate(self, low: float, high: float) -> tuple[float, float]:
        if not math.isfinite(low) or not math.isfinite(high):
            return -10.0, 10.0
        if low > high:
            low, high = high, low
        if abs(high - low) < 1e-6:
            low -= 5.0
            high += 5.0
        span = abs(high - low)
        pad = max(2.0, span * 0.2)
        low -= pad
        high += pad
        low = max(-1000.0, low)
        high = min(1000.0, high)
        if high - low > 2000:
            return -1000.0, 1000.0
        return low, high

    def _safe_number(self, raw: str) -> float | None:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            value = float(self._safe_eval_math(text.replace("^", "**"), 0.0))
        except Exception:
            return None
        if not math.isfinite(value):
            return None
        return max(-1000.0, min(1000.0, value))

    def _safe_eval_math(self, expr: str, x_value: float) -> float:
        cleaned = expr.strip().replace("^", "**")
        cleaned = cleaned.replace("{", "(").replace("}", ")")
        cleaned = cleaned.replace("[", "(").replace("]", ")")
        cleaned = cleaned.replace("|x|", "abs(x)")
        env = dict(_SAFE_EVAL_ENV)
        env["x"] = x_value
        value = eval(cleaned, {"__builtins__": {}}, env)
        return float(value)
