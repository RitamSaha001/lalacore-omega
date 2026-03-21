from __future__ import annotations

from typing import Any, Dict, List


class SolvePipelinePolicy:
    """
    Final quality gate for solver outcomes.
    Keeps output interface stable while enforcing completion correctness.
    """

    def evaluate(
        self,
        *,
        verified: bool,
        risk: float,
        plausibility: Dict[str, Any] | None,
        disagreement: float,
        arena_winner_found: bool,
        entropy: float = 0.0,
        verification_supported: bool = True,
    ) -> Dict[str, Any]:
        plausibility = plausibility or {"plausible": False, "issues": ["missing_plausibility"], "score": 0.0}
        reasons: List[str] = []

        if not bool(plausibility.get("plausible", False)):
            reasons.append("plausibility_failed")

        if not bool(verification_supported):
            reasons.append("verification_unavailable")
        elif (not bool(verified)) and float(risk) > 0.8:
            reasons.append("verification_failed_high_risk")
        elif not bool(verified):
            reasons.append("verification_failed")

        if float(disagreement) > 0.0:
            reasons.append("cross_provider_disagreement")

        if float(entropy) > 0.55:
            reasons.append("high_entropy")

        verification_gate_ok = bool(verified) if bool(verification_supported) else True
        completion_ok = verification_gate_ok and bool(plausibility.get("plausible", False)) and (
            float(disagreement) == 0.0 or bool(arena_winner_found)
        )
        final_status = "Completed" if completion_ok else "Failed"
        force_escalate = not completion_ok

        return {
            "completion_ok": completion_ok,
            "final_status": final_status,
            "force_escalate": force_escalate,
            "reasons": reasons,
        }


def should_mark_completed(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate if feeder should mark queue item as Completed.
    """
    verification = result.get("verification", {}) if isinstance(result, dict) else {}
    arena = result.get("arena", {}) if isinstance(result, dict) else {}
    plausibility = result.get("plausibility", {}) if isinstance(result, dict) else {}
    winner = str(result.get("winner_provider", "") or "")

    policy = SolvePipelinePolicy()
    gate = policy.evaluate(
        verified=bool(verification.get("verified", False)),
        risk=float(verification.get("risk_score", verification.get("risk", 1.0)) or 1.0),
        plausibility=plausibility,
        disagreement=float(arena.get("disagreement", 0.0) or 0.0),
        arena_winner_found=bool(winner),
        entropy=float(arena.get("entropy", 0.0) or 0.0),
    )

    return {
        "complete": bool(gate["completion_ok"]),
        "status": str(gate["final_status"]),
        "reasons": list(gate.get("reasons", [])),
    }
