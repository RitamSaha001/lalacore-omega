from __future__ import annotations

from typing import Dict, Tuple


class DeterministicDominanceGuard:
    """
    Post-hoc deterministic dominance enforcement.
    """

    def enforce(
        self,
        *,
        winner: str,
        posteriors: Dict[str, float],
        verification_by_provider: Dict[str, Dict],
        structure_by_provider: Dict[str, Dict],
    ) -> Tuple[str, Dict]:
        winner_verified = bool(verification_by_provider.get(winner, {}).get("verified"))
        deterministic_pass = [p for p, report in verification_by_provider.items() if bool(report.get("verified"))]

        event = {"enforced": False, "from": winner, "to": winner, "reason": None}
        if winner_verified:
            return winner, event
        if not deterministic_pass:
            return winner, event

        valid_pass = []
        for provider in deterministic_pass:
            structure = structure_by_provider.get(provider, {}) or {}
            circular = float(structure.get("circular_reasoning", 0.0))
            missing = float(structure.get("missing_inference_rate", 0.0))
            if circular > 0.0:
                continue
            if missing > 0.55:
                continue
            valid_pass.append(provider)

        if not valid_pass:
            return winner, event

        selected = max(valid_pass, key=lambda p: float(posteriors.get(p, 0.0)))
        if selected != winner:
            event = {
                "enforced": True,
                "from": winner,
                "to": selected,
                "reason": "deterministic_dominance",
            }
        return selected, event

