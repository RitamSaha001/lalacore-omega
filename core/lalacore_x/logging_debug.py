from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate(value: Any, limit: int = 4000) -> str:
    text = str(value or "")
    return text if len(text) <= limit else text[:limit]


class SolverDebugLogger:
    """
    Persistent structured debug sink for solver/provider quality diagnostics.
    """

    def __init__(self, path: str = "data/lc9/LC9_SOLVER_DEBUG.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log_provider_output(
        self,
        *,
        provider: str,
        question: str,
        raw_output: str,
        extracted_answer: str,
        tokens_used: int,
        extraction_matched: bool,
        extraction_pattern: str,
        verification: bool | None = None,
        risk: float | None = None,
        entropy: float | None = None,
    ) -> None:
        self._append(
            {
                "event_type": "provider_output",
                "provider": str(provider),
                "question": _truncate(question, 600),
                "raw_output": _truncate(raw_output),
                "extracted_answer": _truncate(extracted_answer, 500),
                "tokens_used": int(max(0, tokens_used)),
                "extraction_matched": bool(extraction_matched),
                "extraction_pattern": str(extraction_pattern or ""),
                "verification": verification,
                "risk": float(risk) if risk is not None else None,
                "entropy": float(entropy) if entropy is not None else None,
            }
        )

    def log_extraction_failure(self, *, provider: str, question: str, raw_output: str, reason: str) -> None:
        self._append(
            {
                "event_type": "extraction_failure",
                "provider": str(provider),
                "question": _truncate(question, 600),
                "raw_output": _truncate(raw_output),
                "reason": str(reason)[:500],
            }
        )

    def log_plausibility(self, *, provider: str, question: str, answer: str, report: Dict[str, Any]) -> None:
        self._append(
            {
                "event_type": "plausibility_check",
                "provider": str(provider),
                "question": _truncate(question, 600),
                "answer": _truncate(answer, 500),
                "report": report,
            }
        )

    def log_routing_decision(self, payload: Dict[str, Any]) -> None:
        self._append({"event_type": "routing_decision", **(payload or {})})

    def log_final_status(self, payload: Dict[str, Any]) -> None:
        self._append({"event_type": "final_status", **(payload or {})})

    def _append(self, row: Dict[str, Any]) -> None:
        line = {"ts": _utc_now(), **row}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=True) + "\n")

