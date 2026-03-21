from __future__ import annotations

import math
from typing import Dict, List, Sequence, Tuple


class ArenaStabilityGuard:
    """
    Validates arena state and provides deterministic fallback scoring.
    """

    def sanitize_entropy(self, entropy: float | None) -> float:
        try:
            value = float(entropy if entropy is not None else 0.0)
        except Exception:
            value = 0.0
        if not math.isfinite(value):
            value = 0.0
        return max(0.0, value)

    def validate_responses(self, responses: Sequence[Dict]) -> Tuple[List[Dict], List[str]]:
        issues = []
        cleaned: List[Dict] = []
        seen = set()
        for row in responses:
            provider = str(row.get("provider", "")).strip()
            if not provider:
                issues.append("missing_provider")
                continue
            if provider in seen:
                issues.append("duplicate_provider")
                continue
            seen.add(provider)
            cleaned.append(row)
        return cleaned, issues

    def validate_matches(self, matches: Sequence[Tuple[str, str, float]]) -> Tuple[List[Tuple[str, str, float]], List[str]]:
        issues = []
        out = []
        for winner, loser, margin in matches:
            if winner == loser:
                issues.append("self_match")
                continue
            try:
                m = float(margin)
            except Exception:
                m = 0.0
            if not math.isfinite(m):
                m = 0.0
                issues.append("nonfinite_margin")
            if m < 0.0:
                m = abs(m)
                issues.append("negative_margin")
            out.append((winner, loser, m))
        return out, issues

    def single_pass_thetas(self, responses: Sequence[Dict]) -> Dict[str, float]:
        thetas = {}
        for row in responses:
            provider = str(row.get("provider", ""))
            det = 1.0 if bool(row.get("deterministic_pass")) else 0.0
            critic = float(row.get("critic_score", 0.5))
            conf = float(row.get("confidence", 0.5))
            thetas[provider] = 0.50 * det + 0.30 * critic + 0.20 * conf
        return thetas
