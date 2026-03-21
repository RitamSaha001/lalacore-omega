from __future__ import annotations

import ast
import math
import re
from typing import Any, Dict, Iterable, List

try:  # pragma: no cover - optional dependency
    import numpy as np
except Exception:  # pragma: no cover
    np = None

try:  # pragma: no cover - optional dependency
    import sympy as sp
except Exception:  # pragma: no cover
    sp = None


class ToolRouter:
    """
    Lightweight symbolic/numeric tool dispatcher used by GoT tool nodes.
    """

    _PHYSICS_CONSTANTS: Dict[str, float] = {
        "c": 299792458.0,
        "g": 9.80665,
        "h": 6.62607015e-34,
        "hbar": 1.054571817e-34,
        "k": 1.380649e-23,
        "na": 6.02214076e23,
        "r": 8.314462618,
        "e": 1.602176634e-19,
        "epsilon0": 8.8541878128e-12,
        "mu0": 1.25663706212e-6,
    }

    def run(self, tool: str, payload: Any) -> Dict[str, Any]:
        name = str(tool or "").strip().lower()
        if name in {"symbolic_solver", "sympy.solve", "equation_solver"}:
            return self.solve_equation(payload)
        if name in {"numerical_evaluator", "numeric_eval", "evaluate"}:
            return self.numerical_evaluator(payload)
        if name in {"unit_analysis", "unit_checker"}:
            return self.unit_analysis(payload)
        if name in {"integral_solver", "sympy.integrate"}:
            return self.integral_solver(payload)
        if name in {"equation_system_solver", "system_solver"}:
            return self.equation_system_solver(payload)
        if name in {"physics_constants_lookup", "constants"}:
            return self.physics_constants_lookup(payload)
        if name in {"matrix_solver", "linear_algebra"}:
            return self.matrix_solver(payload)
        return {"ok": False, "tool": name or "unknown", "error": "unsupported_tool"}

    def route_from_text(self, text: str) -> Dict[str, Any]:
        raw = str(text or "").strip()
        if not raw:
            return {"ok": False, "error": "empty_tool_text"}

        low = raw.lower()
        if any(k in low for k in ("constant", "speed of light", "boltzmann", "planck")):
            return self.physics_constants_lookup(raw)
        if any(k in low for k in ("matrix", "det(", "[[", "linear system", "vector form")):
            return self.matrix_solver(raw)
        if "integral" in low or "∫" in raw:
            return self.integral_solver(raw)
        if any(k in low for k in ("unit", "dimension", "dimensional")):
            return self.unit_analysis(raw)
        if ("=" in raw and re.search(r"[a-zA-Z]", raw)) or low.startswith("solve"):
            return self.solve_equation(raw)
        return self.numerical_evaluator(raw)

    def solve_equation(self, payload: Any) -> Dict[str, Any]:
        equation = self._extract_text(payload)
        expression = self._strip_solve_prefix(equation)
        if not expression:
            return {"ok": False, "tool": "symbolic_solver", "error": "empty_equation"}
        if sp is None:
            return {"ok": False, "tool": "symbolic_solver", "error": "sympy_unavailable"}

        try:
            left, right = self._split_equation(expression)
            target = self._guess_primary_symbol(f"{left} {right}")
            expr = sp.sympify(left) - sp.sympify(right)
            sols = sp.solve(sp.Eq(expr, 0), target, dict=True)
            values: List[str] = []
            for row in sols:
                if isinstance(row, dict):
                    values.append(str(sp.simplify(row.get(target))))
                else:
                    values.append(str(sp.simplify(row)))
            return {
                "ok": True,
                "tool": "symbolic_solver",
                "input": expression,
                "symbol": str(target),
                "output": values,
                "summary": f"{target} = {', '.join(values)}" if values else "No closed-form roots found.",
            }
        except Exception as exc:
            return {"ok": False, "tool": "symbolic_solver", "error": type(exc).__name__, "message": str(exc)[:240]}

    def numerical_evaluator(self, payload: Any) -> Dict[str, Any]:
        text = self._extract_text(payload)
        expression = self._sanitize_numeric_expression(text)
        if not expression:
            return {"ok": False, "tool": "numerical_evaluator", "error": "empty_expression"}
        if sp is not None:
            try:
                value = sp.N(sp.sympify(expression))
                return {
                    "ok": True,
                    "tool": "numerical_evaluator",
                    "input": expression,
                    "output": str(value),
                    "value": float(value) if value.is_real else None,
                }
            except Exception:
                pass

        try:
            code = ast.parse(expression, mode="eval")
            safe_names = {"pi": math.pi, "e": math.e, "sqrt": math.sqrt, "sin": math.sin, "cos": math.cos, "tan": math.tan, "log": math.log}
            value = eval(compile(code, "<expr>", "eval"), {"__builtins__": {}}, safe_names)
            return {
                "ok": True,
                "tool": "numerical_evaluator",
                "input": expression,
                "output": str(value),
                "value": float(value),
            }
        except Exception as exc:
            return {"ok": False, "tool": "numerical_evaluator", "error": type(exc).__name__, "message": str(exc)[:240]}

    def unit_analysis(self, payload: Any) -> Dict[str, Any]:
        text = self._extract_text(payload)
        units = re.findall(r"\b(?:m|cm|mm|km|s|ms|min|h|kg|g|N|J|W|Pa|V|A|mol|K|C)\b", text)
        freq: Dict[str, int] = {}
        for unit in units:
            freq[unit] = freq.get(unit, 0) + 1

        suspicious = []
        if "N" in freq and "kg" in freq and not any(k in text.lower() for k in ("acceleration", "m/s", "newton", "force")):
            suspicious.append("force_mass_relation_not_explicit")
        if "J" in freq and "W" in freq and not any(k in text.lower() for k in ("time", "second", "s")):
            suspicious.append("power_energy_time_incomplete")

        return {
            "ok": True,
            "tool": "unit_analysis",
            "input": text[:500],
            "output": {"units": freq, "issues": suspicious},
            "unit_consistent": len(suspicious) == 0,
        }

    def integral_solver(self, payload: Any) -> Dict[str, Any]:
        text = self._extract_text(payload)
        if sp is None:
            return {"ok": False, "tool": "integral_solver", "error": "sympy_unavailable"}

        parsed = self._parse_integral(text)
        if parsed is None:
            return {"ok": False, "tool": "integral_solver", "error": "integral_parse_failed", "input": text[:300]}
        expr_text, var_name, lower, upper = parsed
        try:
            var = sp.symbols(var_name)
            expr = sp.sympify(expr_text)
            if lower is not None and upper is not None:
                out = sp.integrate(expr, (var, sp.sympify(lower), sp.sympify(upper)))
                kind = "definite"
            else:
                out = sp.integrate(expr, var)
                kind = "indefinite"
            return {
                "ok": True,
                "tool": "integral_solver",
                "input": text[:500],
                "integral_type": kind,
                "output": str(sp.simplify(out)),
            }
        except Exception as exc:
            return {"ok": False, "tool": "integral_solver", "error": type(exc).__name__, "message": str(exc)[:240]}

    def equation_system_solver(self, payload: Any) -> Dict[str, Any]:
        if sp is None:
            return {"ok": False, "tool": "equation_system_solver", "error": "sympy_unavailable"}

        equations: List[str] = []
        symbols: List[str] = []
        if isinstance(payload, dict):
            equations = [str(x).strip() for x in (payload.get("equations") or []) if str(x).strip()]
            symbols = [str(x).strip() for x in (payload.get("symbols") or []) if str(x).strip()]
        else:
            raw = self._extract_text(payload)
            equations = [seg.strip() for seg in re.split(r"[;\n,]", raw) if "=" in seg]

        if not equations:
            return {"ok": False, "tool": "equation_system_solver", "error": "no_equations"}
        try:
            eq_objs = []
            discovered = set(symbols)
            for row in equations:
                left, right = self._split_equation(row)
                eq_objs.append(sp.Eq(sp.sympify(left), sp.sympify(right)))
                for token in re.findall(r"\b[a-zA-Z]\w*\b", row):
                    if token.lower() not in {"sin", "cos", "tan", "log", "exp", "sqrt"}:
                        discovered.add(token)
            sym_list = [sp.symbols(sym) for sym in sorted(discovered)] or [sp.symbols("x")]
            solved = sp.solve(eq_objs, sym_list, dict=True)
            return {
                "ok": True,
                "tool": "equation_system_solver",
                "input": equations,
                "symbols": [str(s) for s in sym_list],
                "output": [{str(k): str(v) for k, v in row.items()} for row in solved],
            }
        except Exception as exc:
            return {"ok": False, "tool": "equation_system_solver", "error": type(exc).__name__, "message": str(exc)[:240]}

    def physics_constants_lookup(self, payload: Any) -> Dict[str, Any]:
        text = self._extract_text(payload).lower()
        hits: Dict[str, float] = {}
        for key, value in self._PHYSICS_CONSTANTS.items():
            if key in text:
                hits[key] = value
        if not hits:
            aliases = {
                "speed of light": "c",
                "gravitational acceleration": "g",
                "planck": "h",
                "boltzmann": "k",
                "avogadro": "na",
                "gas constant": "r",
                "permittivity": "epsilon0",
                "permeability": "mu0",
            }
            for phrase, symbol in aliases.items():
                if phrase in text:
                    hits[symbol] = self._PHYSICS_CONSTANTS[symbol]
        return {
            "ok": bool(hits),
            "tool": "physics_constants_lookup",
            "input": text[:300],
            "output": hits,
            "error": "" if hits else "no_matching_constant",
        }

    def matrix_solver(self, payload: Any) -> Dict[str, Any]:
        if np is None:
            return {"ok": False, "tool": "matrix_solver", "error": "numpy_unavailable"}

        matrix: List[List[float]] = []
        rhs: List[float] = []
        if isinstance(payload, dict):
            matrix = self._to_float_matrix(payload.get("matrix"))
            rhs = self._to_float_vector(payload.get("rhs"))
        else:
            text = self._extract_text(payload)
            matrix = self._parse_matrix_from_text(text)

        if not matrix:
            return {"ok": False, "tool": "matrix_solver", "error": "matrix_parse_failed"}

        try:
            a = np.array(matrix, dtype=float)
            if rhs:
                b = np.array(rhs, dtype=float)
                if a.shape[0] != b.shape[0]:
                    return {"ok": False, "tool": "matrix_solver", "error": "rhs_dimension_mismatch"}
                sol = np.linalg.solve(a, b)
                return {
                    "ok": True,
                    "tool": "matrix_solver",
                    "operation": "solve_linear_system",
                    "output": [float(x) for x in sol.tolist()],
                }
            det = float(np.linalg.det(a))
            rank = int(np.linalg.matrix_rank(a))
            return {
                "ok": True,
                "tool": "matrix_solver",
                "operation": "matrix_stats",
                "output": {"determinant": det, "rank": rank, "shape": [int(a.shape[0]), int(a.shape[1])]},
            }
        except Exception as exc:
            return {"ok": False, "tool": "matrix_solver", "error": type(exc).__name__, "message": str(exc)[:240]}

    def _extract_text(self, payload: Any) -> str:
        if payload is None:
            return ""
        if isinstance(payload, str):
            return payload.strip()
        if isinstance(payload, dict):
            for key in ("expression", "equation", "input", "text", "query"):
                if key in payload:
                    return str(payload.get(key) or "").strip()
            return str(payload).strip()
        if isinstance(payload, (list, tuple)):
            return "; ".join(str(x) for x in payload)
        return str(payload).strip()

    def _strip_solve_prefix(self, text: str) -> str:
        out = str(text or "").strip()
        out = re.sub(r"^\s*solve\s*(for\s*[a-zA-Z]\w*)?\s*:?\s*", "", out, flags=re.IGNORECASE)
        return out.strip()

    def _split_equation(self, text: str) -> tuple[str, str]:
        normalized = str(text or "").replace("^", "**").strip()
        if "=" in normalized:
            left, right = normalized.split("=", 1)
            return left.strip(), right.strip()
        return normalized, "0"

    def _guess_primary_symbol(self, text: str):
        if sp is None:
            return "x"
        for token in re.findall(r"\b[a-zA-Z]\w*\b", text):
            if token.lower() in {"sin", "cos", "tan", "log", "exp", "sqrt"}:
                continue
            return sp.symbols(token)
        return sp.symbols("x")

    def _sanitize_numeric_expression(self, text: str) -> str:
        expr = str(text or "").strip().replace("^", "**")
        expr = re.sub(r"[^0-9a-zA-Z\.\+\-\*/\(\), _]", "", expr)
        expr = expr.replace(",", "")
        return expr

    def _parse_integral(self, text: str) -> tuple[str, str, str | None, str | None] | None:
        raw = str(text or "").replace("^", "**")

        definite = re.search(
            r"(?:integral|∫)\s*(?:from)?\s*([\-+]?[0-9a-zA-Z\./]+)\s*(?:to|->)\s*([\-+]?[0-9a-zA-Z\./]+)\s*(?:of)?\s*(.*?)\s*d([a-zA-Z]\w*)",
            raw,
            flags=re.IGNORECASE,
        )
        if definite:
            lower, upper, expr, var = definite.groups()
            return expr.strip(), var.strip(), lower.strip(), upper.strip()

        simple = re.search(
            r"(?:integral|∫)\s*(?:of)?\s*(.*?)\s*d([a-zA-Z]\w*)",
            raw,
            flags=re.IGNORECASE,
        )
        if simple:
            expr, var = simple.groups()
            return expr.strip(), var.strip(), None, None

        sympy_like = re.search(
            r"integrate\((.*?),(?:\s*([a-zA-Z]\w*)\s*)(?:,\s*([\-+]?[0-9a-zA-Z\./]+)\s*,\s*([\-+]?[0-9a-zA-Z\./]+)\s*)?\)",
            raw,
            flags=re.IGNORECASE,
        )
        if sympy_like:
            expr, var, lower, upper = sympy_like.groups()
            return expr.strip(), (var or "x").strip(), (lower or None), (upper or None)
        return None

    def _to_float_matrix(self, payload: Any) -> List[List[float]]:
        if not isinstance(payload, list):
            return []
        out: List[List[float]] = []
        for row in payload:
            if not isinstance(row, list):
                return []
            vals = self._to_float_vector(row)
            if not vals:
                return []
            out.append(vals)
        return out

    def _to_float_vector(self, payload: Any) -> List[float]:
        if not isinstance(payload, list):
            return []
        out: List[float] = []
        for val in payload:
            try:
                out.append(float(val))
            except Exception:
                return []
        return out

    def _parse_matrix_from_text(self, text: str) -> List[List[float]]:
        raw = str(text or "").strip()
        if not raw:
            return []
        rows: List[List[float]] = []
        bracket_rows = re.findall(r"\[([^\[\]]+)\]", raw)
        if bracket_rows:
            for row_text in bracket_rows:
                row_vals = self._extract_numbers(row_text)
                if row_vals:
                    rows.append(row_vals)
            return self._normalize_rectangular(rows)

        split_rows = [seg.strip() for seg in re.split(r"[;\n]", raw) if seg.strip()]
        for row_text in split_rows:
            row_vals = self._extract_numbers(row_text)
            if row_vals:
                rows.append(row_vals)
        return self._normalize_rectangular(rows)

    def _extract_numbers(self, text: str) -> List[float]:
        return [float(x) for x in re.findall(r"[-+]?\d+(?:\.\d+)?", text)]

    def _normalize_rectangular(self, rows: Iterable[List[float]]) -> List[List[float]]:
        collected = [list(r) for r in rows if r]
        if not collected:
            return []
        width = len(collected[0])
        if width == 0:
            return []
        for row in collected:
            if len(row) != width:
                return []
        return collected
