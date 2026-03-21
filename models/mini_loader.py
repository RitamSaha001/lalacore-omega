from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Dict

from core.math.contextual_math_solver import solve_contextual_math_question
from app.training.mini_signal_logger import DEFAULT_MINI_SIGNAL_LOGGER
from core.math.inverse_trig_solver import solve_inverse_trig_question


class MiniEvolutionMemory:
    def __init__(self, path: str = "data/metrics/mini_memory.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()

    def _load(self) -> Dict:
        if not self.path.exists():
            return {"cases": {}, "stats": {"calls": 0, "hits": 0}}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {"cases": {}, "stats": {"calls": 0, "hits": 0}}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")

    def lookup(self, question: str):
        self.data["stats"]["calls"] = self.data["stats"].get("calls", 0) + 1
        key = self._key(question)
        hit = self.data["cases"].get(key)
        if hit:
            self.data["stats"]["hits"] = self.data["stats"].get("hits", 0) + 1
        self._save()
        return hit

    def remember(self, question: str, answer: str, confidence: float, reasoning: str) -> None:
        key = self._key(question)
        self.data["cases"][key] = {
            "answer": answer,
            "confidence": float(confidence),
            "reasoning": reasoning[:400],
        }
        self._save()

    def _key(self, question: str) -> str:
        return " ".join((question or "").lower().split())[:220]


_MEMORY = MiniEvolutionMemory()


def run_mini(question: str, context: str):
    deterministic = solve_inverse_trig_question(question)
    if deterministic and bool(deterministic.get("handled")):
        answer = str(deterministic.get("answer", "")).strip() or "0.0000"
        reasoning = str(deterministic.get("reasoning", "Deterministic inverse-trig solve."))
        confidence = 0.985
        _MEMORY.remember(question=question, answer=answer, confidence=confidence, reasoning=reasoning)
        DEFAULT_MINI_SIGNAL_LOGGER.log(
            {
                "event": "mini_deterministic_inverse_trig",
                "question": question,
                "answer": answer,
                "confidence": confidence,
            }
        )
        return {
            "reasoning": reasoning,
            "final_answer": answer,
            "confidence": confidence,
            "mode": "deterministic_inverse_trig",
        }

    contextual = solve_contextual_math_question(question)
    if contextual and bool(contextual.get("handled")):
        answer = str(contextual.get("answer", "")).strip() or "0.0000"
        reasoning = str(contextual.get("reasoning", "Deterministic contextual math solve."))
        confidence = 0.97
        _MEMORY.remember(question=question, answer=answer, confidence=confidence, reasoning=reasoning)
        DEFAULT_MINI_SIGNAL_LOGGER.log(
            {
                "event": "mini_deterministic_contextual_math",
                "question": question,
                "answer": answer,
                "confidence": confidence,
            }
        )
        return {
            "reasoning": reasoning,
            "final_answer": answer,
            "confidence": confidence,
            "mode": "deterministic_contextual_math",
        }

    cached = _MEMORY.lookup(question) if _should_use_memory(question) else None
    if cached and _is_degenerate_memory_hit(cached):
        cached = None
    if cached:
        DEFAULT_MINI_SIGNAL_LOGGER.log(
            {
                "event": "mini_cache_hit",
                "question": question,
                "confidence": cached.get("confidence", 0.6),
            }
        )
        return {
            "reasoning": f"Memory replay: {cached.get('reasoning', '')}",
            "final_answer": cached.get("answer", "0"),
            "confidence": cached.get("confidence", 0.6),
            "mode": "memory",
        }

    reasoning, answer, confidence = _solve_local(question, context)
    if answer.strip() and confidence >= 0.20:
        _MEMORY.remember(question=question, answer=answer, confidence=confidence, reasoning=reasoning)

    DEFAULT_MINI_SIGNAL_LOGGER.log(
        {
            "event": "mini_infer",
            "question": question,
            "answer": answer,
            "confidence": confidence,
            "mode": "fresh",
        }
    )

    return {
        "reasoning": reasoning,
        "final_answer": answer,
        "confidence": confidence,
        "mode": "fresh",
    }


def _solve_local(question: str, context: str) -> tuple[str, str, float]:
    expr = _extract_expression(question)
    if expr:
        try:
            value = eval(expr, {"__builtins__": {}}, {})
            if isinstance(value, (int, float)):
                answer = str(int(value) if float(value).is_integer() else float(value))
                reasoning = f"Parsed arithmetic expression `{expr}` and evaluated directly."
                return reasoning, answer, 0.72
        except Exception:
            pass

    rhs = _extract_equation_rhs(question)
    if rhs is not None:
        reasoning = "Used equation RHS fallback from prompt structure."
        return reasoning, rhs, 0.58

    # Retrieval-grounded fallback from context snippets.
    hint = _best_hint(context)
    reasoning = "Local mini fallback could not solve this reliably."
    if hint:
        reasoning += f" Retrieved hint: {hint}"

    return reasoning, "", 0.08


def _extract_expression(question: str) -> str | None:
    q = str(question or "").strip().lower()
    m = re.match(r"^\s*(?:what is|compute|calculate|evaluate)\s+([0-9\.\s\+\-\*/\(\)]+)\??\s*$", q)
    if not m:
        return None
    expr = m.group(1).strip()
    if not expr or re.search(r"[^0-9\s\+\-\*/\(\)\.]", expr):
        return None
    return expr


def _extract_equation_rhs(question: str) -> str | None:
    q = str(question or "").lower()
    if "=" not in q:
        return None
    if re.search(r"\b(differentiate|derivative|integrate|integral|evaluate)\b", q):
        return None
    if re.search(r"\bat\s+[a-z]\s*=", q):
        return None
    if re.search(r"\bfrom\b.+\bto\b", q):
        return None
    if not re.search(r"\b(solve|find)\b", q):
        return None
    rhs = question.split("=")[-1].strip()
    if rhs:
        return rhs
    return None


def _should_use_memory(question: str) -> bool:
    enabled = str(os.getenv("LC9_MINI_MEMORY_REPLAY", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return False
    q = str(question or "").lower()
    blocked_markers = (
        "integral",
        "∫",
        "differentiate",
        "d/dx",
        "from ",
        " to ",
        "sin^(-1)",
        "cos^(-1)",
        "tan^(-1)",
        "asin(",
        "acos(",
        "atan(",
    )
    return not any(marker in q for marker in blocked_markers)


def _is_degenerate_memory_hit(cached: Dict) -> bool:
    answer = str(cached.get("answer", "")).strip().lower()
    reasoning = str(cached.get("reasoning", "")).strip().lower()
    if not answer:
        return True
    if answer in {"0", "0.0", "0.00", "0.000", "0.0000"} and "retrieval-guided heuristic reasoning" in reasoning:
        return True
    if "memory replay" in reasoning and answer in {"0", "0.0", "0.00", "0.000", "0.0000"}:
        return True
    return False


def _best_hint(context: str) -> str:
    for line in context.splitlines():
        line = line.strip(" -")
        if line:
            return line[:120]
    return ""
