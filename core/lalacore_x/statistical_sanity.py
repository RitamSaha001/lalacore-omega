from __future__ import annotations

import math
from typing import Callable, Dict, List, Tuple


class StatisticalSanityValidator:
    """
    Post-arena statistical validation with auto-correction hook.
    """

    def validate(self, outcome: Dict) -> Tuple[bool, List[str]]:
        issues: List[str] = []

        posteriors = outcome.get("posteriors", {}) or {}
        thetas = outcome.get("thetas", {}) or {}
        pairwise = outcome.get("pairwise", {}) or {}
        uncertainties = pairwise.get("uncertainties", {}) or {}
        entropy = outcome.get("entropy")

        if posteriors:
            s = sum(float(v) for v in posteriors.values())
            if not math.isfinite(s) or abs(s - 1.0) > 1e-3:
                issues.append("posterior_sum_invalid")
            if any((not math.isfinite(float(v)) or float(v) < 0.0) for v in posteriors.values()):
                issues.append("posterior_nonfinite")

        for provider, theta in thetas.items():
            if not math.isfinite(float(theta)):
                issues.append("theta_nonfinite")
                break

        if entropy is None or (not math.isfinite(float(entropy))):
            issues.append("entropy_nonfinite")
        elif float(entropy) < 0.0:
            issues.append("entropy_negative")

        for provider, value in uncertainties.items():
            if not math.isfinite(float(value)):
                issues.append("uncertainty_nonfinite")
                break

        return (len(issues) == 0), issues

    def auto_correct(
        self,
        outcome: Dict,
        recompute_fn: Callable[[], Dict],
    ) -> Dict:
        valid, issues = self.validate(outcome)
        if valid:
            outcome["auto_corrected"] = False
            outcome["sanity_issues"] = []
            return outcome

        try:
            corrected = recompute_fn()
        except Exception:
            outcome["auto_corrected"] = True
            outcome["sanity_issues"] = issues + ["recompute_failed"]
            return outcome
        c_valid, c_issues = self.validate(corrected)
        corrected["auto_corrected"] = True
        corrected["sanity_issues"] = issues if c_valid else issues + c_issues
        return corrected
