from __future__ import annotations

from typing import Dict, List, Sequence, Tuple


def _uniq(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        key = str(value or "").strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


class ProviderOrchestrator:
    """
    Lightweight orchestration helper that preserves existing routing contracts.
    """

    def __init__(self, min_provider_count: int = 2, strong_gap: float = 0.08):
        self.min_provider_count = max(1, int(min_provider_count))
        self.strong_gap = max(0.0, float(strong_gap))

    def select_for_run(
        self,
        *,
        available_providers: Sequence[str],
        ranked: Sequence[Tuple[str, float]],
        initial_selected: Sequence[str],
        require_non_mini: bool = False,
        target_provider_count: int | None = None,
    ) -> List[str]:
        selected = _uniq(initial_selected)
        available = _uniq(available_providers)
        ranked_rows = [(str(provider), float(score)) for provider, score in ranked]
        target = max(1, int(target_provider_count)) if target_provider_count is not None else self.min_provider_count

        # Include all strong non-mini contenders close to top score.
        non_mini_ranked = [(provider, score) for provider, score in ranked_rows if provider != "mini"]
        if non_mini_ranked:
            top_score = non_mini_ranked[0][1]
            for provider, score in non_mini_ranked:
                if score >= top_score - self.strong_gap and provider not in selected:
                    selected.append(provider)

        # Enforce minimum provider count when possible.
        if len(selected) < target:
            for provider, _ in ranked_rows:
                if provider not in selected:
                    selected.append(provider)
                if len(selected) >= target:
                    break

        if len(selected) < target:
            for provider in available:
                if provider not in selected:
                    selected.append(provider)
                if len(selected) >= target:
                    break

        if require_non_mini and not any(provider != "mini" for provider in selected):
            for provider, _ in non_mini_ranked:
                if provider not in selected:
                    selected.append(provider)
                    break
            if not any(provider != "mini" for provider in selected):
                for provider in available:
                    if provider != "mini" and provider not in selected:
                        selected.append(provider)
                        break

        return _uniq(selected)

    def should_force_escalation(
        self,
        *,
        entropy: float,
        disagreement: float,
        plausibility_failed: bool,
        verification_failed: bool,
        entropy_threshold: float = 0.55,
        provider_count: int | None = None,
    ) -> bool:
        if provider_count is not None and int(provider_count) <= 1:
            return True
        if bool(plausibility_failed):
            return True
        if bool(verification_failed):
            return True
        if float(disagreement) > 0.0:
            return True
        if float(entropy) > float(entropy_threshold):
            return True
        return False
