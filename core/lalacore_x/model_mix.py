from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from core.lalacore_x.schemas import ProblemProfile


@dataclass(slots=True, frozen=True)
class RankedProviderModel:
    provider: str
    model: str
    score: float
    base_provider_score: float
    model_bonus: float
    policy_bonus: float
    cost_penalty: float
    rationale: str


class ProviderModelMixLayer:
    """
    Thin compatibility layer that sits above the legacy provider router.

    It does not replace provider-level ranking. Instead, it refines the
    existing provider scores with model-specific heuristics so harder prompts
    can prefer stronger model variants while easy prompts keep the lighter,
    cheaper defaults.
    """

    def rank(
        self,
        *,
        provider_ranked: Sequence[Tuple[str, float]],
        profile: ProblemProfile,
        candidate_models_by_provider: Mapping[str, Sequence[str]],
        request_policy: Mapping[str, Any] | None = None,
    ) -> List[RankedProviderModel]:
        ranked_rows: List[RankedProviderModel] = []
        request_policy = dict(request_policy or {})
        base_scores = {
            str(provider): float(score) for provider, score in provider_ranked
        }

        for provider, base_score in provider_ranked:
            models = [
                str(model).strip()
                for model in candidate_models_by_provider.get(str(provider), ())
                if str(model).strip()
            ]
            if not models:
                continue
            for model in models:
                model_bonus = self._model_bonus(
                    provider=str(provider),
                    model=model,
                    profile=profile,
                )
                policy_bonus = self._policy_bonus(
                    provider=str(provider),
                    model=model,
                    request_policy=request_policy,
                )
                cost_penalty = self._cost_penalty(
                    provider=str(provider),
                    model=model,
                    profile=profile,
                    request_policy=request_policy,
                )
                final_score = float(base_score) + model_bonus + policy_bonus - cost_penalty
                rationale = (
                    f"base={base_score:.3f}, model_bonus={model_bonus:.3f}, "
                    f"policy_bonus={policy_bonus:.3f}, cost_penalty={cost_penalty:.3f}"
                )
                ranked_rows.append(
                    RankedProviderModel(
                        provider=str(provider),
                        model=model,
                        score=final_score,
                        base_provider_score=float(base_score),
                        model_bonus=float(model_bonus),
                        policy_bonus=float(policy_bonus),
                        cost_penalty=float(cost_penalty),
                        rationale=rationale,
                    )
                )

        ranked_rows.sort(key=lambda row: row.score, reverse=True)
        return ranked_rows

    def collapse_provider_scores(
        self,
        ranked_rows: Sequence[RankedProviderModel],
        *,
        fallback_ranked: Sequence[Tuple[str, float]],
    ) -> List[Tuple[str, float]]:
        best_by_provider: Dict[str, float] = {}
        for row in ranked_rows:
            current = best_by_provider.get(row.provider)
            if current is None or float(row.score) > current:
                best_by_provider[row.provider] = float(row.score)

        collapsed = [(provider, score) for provider, score in best_by_provider.items()]
        if not collapsed:
            return [(str(provider), float(score)) for provider, score in fallback_ranked]
        collapsed.sort(key=lambda item: item[1], reverse=True)
        return collapsed

    def initial_selected_providers(
        self,
        ranked_rows: Sequence[RankedProviderModel],
        *,
        fallback_selected: Sequence[str],
        limit: int,
    ) -> List[str]:
        selected: List[str] = []
        seen = set()
        for row in ranked_rows:
            if row.provider in seen:
                continue
            seen.add(row.provider)
            selected.append(row.provider)
            if len(selected) >= max(1, int(limit)):
                return selected
        for provider in fallback_selected:
            token = str(provider).strip()
            if not token or token in seen:
                continue
            seen.add(token)
            selected.append(token)
            if len(selected) >= max(1, int(limit)):
                break
        return selected

    def model_plan_by_provider(
        self,
        ranked_rows: Sequence[RankedProviderModel],
    ) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        seen: Dict[str, set[str]] = {}
        for row in ranked_rows:
            bucket = out.setdefault(row.provider, [])
            seen_bucket = seen.setdefault(row.provider, set())
            if row.model in seen_bucket:
                continue
            seen_bucket.add(row.model)
            bucket.append(row.model)
        return out

    def _model_bonus(
        self,
        *,
        provider: str,
        model: str,
        profile: ProblemProfile,
    ) -> float:
        tier = self._difficulty_tier(profile)
        text = model.lower().strip()

        easy_bias = 0.0
        hard_bias = 0.0

        if provider == "gemini":
            if "2.5-pro" in text:
                hard_bias += 0.34
                easy_bias -= 0.18
            elif "2.5-flash" in text and "lite" not in text:
                hard_bias += 0.18
                easy_bias -= 0.05
            elif "flash-lite" in text:
                hard_bias -= 0.03
                easy_bias += 0.08
            elif "2.0-flash" in text:
                hard_bias += 0.08
                easy_bias += 0.02
        elif provider == "openrouter":
            if "deepseek-r1" in text:
                hard_bias += 0.28
                easy_bias -= 0.10
            elif text == "openrouter/free":
                hard_bias += 0.16
                easy_bias -= 0.04
            elif "gpt-4o" in text and "mini" not in text:
                hard_bias += 0.32
                easy_bias -= 0.22
            elif "claude-3.7-sonnet" in text:
                hard_bias += 0.35
                easy_bias -= 0.22
            elif "gpt-4o-mini" in text:
                hard_bias += 0.12
                easy_bias -= 0.02
            elif "llama-3.1-8b" in text:
                hard_bias -= 0.04
                easy_bias += 0.05
        elif provider == "groq":
            if "deepseek-r1-distill-llama-70b" in text:
                hard_bias += 0.27
                easy_bias -= 0.12
            elif "70b" in text:
                hard_bias += 0.14
                easy_bias -= 0.06
            elif "8b" in text:
                hard_bias -= 0.05
                easy_bias += 0.06
        elif provider in {"hf", "huggingface"}:
            if "70b" in text:
                hard_bias += 0.10
                easy_bias -= 0.03
            elif "8b" in text:
                hard_bias -= 0.03
                easy_bias += 0.03

        return easy_bias if tier == "easy" else hard_bias

    def _policy_bonus(
        self,
        *,
        provider: str,
        model: str,
        request_policy: Mapping[str, Any],
    ) -> float:
        bonus = 0.0
        preferred_provider = str(
            request_policy.get("preferred_provider") or ""
        ).strip().lower()
        preferred_model = str(request_policy.get("preferred_model") or "").strip().lower()
        provider_priority = request_policy.get("provider_priority") or []

        if preferred_provider and preferred_provider == provider.lower():
            bonus += 0.10
        if preferred_model and preferred_model == model.lower():
            bonus += 0.18

        if isinstance(provider_priority, Sequence) and not isinstance(provider_priority, (str, bytes)):
            for idx, item in enumerate(provider_priority):
                token = str(item or "").strip().lower()
                if not token:
                    continue
                weight = max(0.0, 0.08 - (0.015 * idx))
                if token == provider.lower() or token == model.lower():
                    bonus += weight

        if bool(request_policy.get("quality_retry_force_max")):
            bonus += 0.10
        elif bool(request_policy.get("quality_retry")):
            bonus += 0.04
        return bonus

    def _cost_penalty(
        self,
        *,
        provider: str,
        model: str,
        profile: ProblemProfile,
        request_policy: Mapping[str, Any],
    ) -> float:
        if bool(request_policy.get("quality_retry_force_max")):
            return 0.0
        tier = self._difficulty_tier(profile)
        text = model.lower().strip()
        if tier != "easy":
            return 0.0
        if any(
            marker in text
            for marker in (
                "gpt-4o",
                "claude-3.7-sonnet",
                "2.5-pro",
                "deepseek-r1-distill-llama-70b",
                "deepseek-r1:free",
            )
        ):
            return 0.18
        return 0.0

    def _difficulty_tier(self, profile: ProblemProfile) -> str:
        difficulty = str(getattr(profile, "difficulty", "easy") or "easy").lower().strip()
        if difficulty in {"medium", "hard"}:
            return "advanced"
        return "easy"
