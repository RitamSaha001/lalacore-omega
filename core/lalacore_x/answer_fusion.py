from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Mapping, Sequence

from core.lalacore_x.schemas import ProviderAnswer


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def normalize_answer(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    compact = re.sub(r"\s+", "", text).replace(",", "")
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?", compact):
        try:
            num = float(compact)
            if abs(num - round(num)) < 1e-10:
                return str(int(round(num)))
            return f"{num:.12g}"
        except Exception:
            return compact
    return compact


class ProviderIndependentAnswerResolver:
    """
    Groups equivalent answers and selects an answer-cluster winner independent of provider identity.
    """

    def resolve(
        self,
        *,
        candidates: Sequence[ProviderAnswer],
        current_provider: str,
        posteriors: Mapping[str, Any] | None,
        verification_by_provider: Mapping[str, Mapping[str, Any]],
        plausibility_by_provider: Mapping[str, Mapping[str, Any]],
        judge_by_provider: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        groups = self._build_groups(
            candidates=candidates,
            posteriors=posteriors or {},
            verification_by_provider=verification_by_provider,
            plausibility_by_provider=plausibility_by_provider,
            judge_by_provider=judge_by_provider or {},
        )
        if not groups:
            return {
                "switched": False,
                "provider": str(current_provider or ""),
                "reason": "no_nonempty_answer_groups",
                "groups": [],
            }

        best_key, best_row = max(
            groups.items(),
            key=lambda item: (float(item[1]["score"]), float(item[1]["verified_count"]), float(item[1]["posterior_mass"])),
        )

        current_provider = str(current_provider or "")
        current_key = ""
        for key, row in groups.items():
            if current_provider in row["providers"]:
                current_key = key
                break

        switched = False
        reason = "keep_current"
        target_provider = current_provider

        if current_key != best_key:
            current_row = groups.get(current_key, None)
            current_verified = bool((verification_by_provider.get(current_provider) or {}).get("verified", False))
            current_plausible = bool((plausibility_by_provider.get(current_provider) or {}).get("plausible", False))
            best_verified = bool(best_row["verified_count"] > 0)
            best_plausible = bool(best_row["plausible_count"] > 0)
            current_score = float(current_row["score"]) if isinstance(current_row, dict) else 0.0
            delta = float(best_row["score"]) - current_score

            should_switch = False
            if current_plausible and not best_plausible:
                should_switch = False
                reason = "current_more_plausible"
            elif best_verified and not current_verified:
                should_switch = True
                reason = "verified_answer_cluster_superior"
            elif best_plausible and not current_plausible:
                should_switch = True
                reason = "plausible_answer_cluster_superior"
            elif (not current_verified) and best_plausible and delta >= 0.12:
                should_switch = True
                reason = "higher_cluster_score_unverified_current"
            elif best_plausible and delta >= 0.25:
                should_switch = True
                reason = "significant_cluster_score_delta"

            if should_switch:
                target_provider = str(best_row["representative_provider"])
                switched = bool(target_provider and target_provider != current_provider)

        groups_payload = [
            {
                "normalized_answer": key,
                "providers": list(value["providers"]),
                "score": float(value["score"]),
                "posterior_mass": float(value["posterior_mass"]),
                "verified_count": int(value["verified_count"]),
                "plausible_count": int(value["plausible_count"]),
                "judge_mean": float(value["judge_mean"]),
                "support": int(value["support"]),
                "representative_provider": str(value["representative_provider"]),
            }
            for key, value in sorted(groups.items(), key=lambda item: float(item[1]["score"]), reverse=True)
        ]

        return {
            "switched": switched,
            "provider": str(target_provider or current_provider),
            "reason": reason,
            "best_answer_norm": best_key,
            "groups": groups_payload,
        }

    def _build_groups(
        self,
        *,
        candidates: Sequence[ProviderAnswer],
        posteriors: Mapping[str, Any],
        verification_by_provider: Mapping[str, Mapping[str, Any]],
        plausibility_by_provider: Mapping[str, Mapping[str, Any]],
        judge_by_provider: Mapping[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        rows: Dict[str, Dict[str, Any]] = {}
        for candidate in candidates:
            provider = str(candidate.provider or "").strip()
            if not provider:
                continue
            norm = normalize_answer(candidate.final_answer)
            if not norm:
                continue

            row = rows.setdefault(
                norm,
                {
                    "providers": [],
                    "posterior_mass": 0.0,
                    "verified_count": 0,
                    "plausible_count": 0,
                    "judge_total": 0.0,
                    "judge_count": 0,
                    "support": 0,
                },
            )
            row["providers"].append(provider)
            row["support"] += 1
            row["posterior_mass"] += float(_clamp(float(posteriors.get(provider, 0.0))))
            if bool((verification_by_provider.get(provider) or {}).get("verified", False)):
                row["verified_count"] += 1
            if bool((plausibility_by_provider.get(provider) or {}).get("plausible", False)):
                row["plausible_count"] += 1

            judge_row = judge_by_provider.get(provider)
            judge_score = 0.0
            if judge_row is not None:
                judge_score = float(getattr(judge_row, "score", 0.0))
            row["judge_total"] += judge_score
            row["judge_count"] += 1

        out: Dict[str, Dict[str, Any]] = {}
        for key, row in rows.items():
            support = max(1, int(row["support"]))
            verified_frac = float(row["verified_count"]) / support
            plausible_frac = float(row["plausible_count"]) / support
            judge_mean = float(row["judge_total"]) / max(1, int(row["judge_count"]))
            posterior_mass = float(row["posterior_mass"])
            if posterior_mass <= 0.0:
                posterior_mass = min(1.0, 0.20 * support)

            score = (
                0.45 * _clamp(posterior_mass)
                + 0.25 * _clamp(verified_frac)
                + 0.15 * _clamp(plausible_frac)
                + 0.10 * _clamp(judge_mean)
                + 0.05 * _clamp(support / max(2.0, float(len(candidates))))
            )
            # Guard against posterior-only dominance by implausible, unverified singleton echoes.
            if int(row["verified_count"]) == 0 and int(row["plausible_count"]) == 0:
                score *= 0.35

            representative = self._representative_provider(
                providers=row["providers"],
                posteriors=posteriors,
                judge_by_provider=judge_by_provider,
            )
            out[key] = {
                "providers": row["providers"],
                "posterior_mass": posterior_mass,
                "verified_count": int(row["verified_count"]),
                "plausible_count": int(row["plausible_count"]),
                "judge_mean": judge_mean,
                "support": support,
                "score": float(score),
                "representative_provider": representative,
            }
        return out

    def _representative_provider(
        self,
        *,
        providers: Iterable[str],
        posteriors: Mapping[str, Any],
        judge_by_provider: Mapping[str, Any],
    ) -> str:
        ranked = []
        for provider in providers:
            judge_row = judge_by_provider.get(provider)
            judge_score = float(getattr(judge_row, "score", 0.0)) if judge_row is not None else 0.0
            posterior = float(_clamp(float(posteriors.get(provider, 0.0))))
            ranked.append((judge_score, posterior, str(provider)))
        if not ranked:
            return ""
        ranked.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
        return ranked[0][2]
