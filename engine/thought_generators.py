from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Sequence

from core.lalacore_x.providers import ProviderFabric
from core.lalacore_x.schemas import ProblemProfile

_PROVIDER_FABRIC: ProviderFabric | None = None


def _default_profile() -> ProblemProfile:
    return ProblemProfile(
        subject="general",
        difficulty="unknown",
        numeric=False,
        multi_concept=False,
        trap_probability=0.0,
        symbolic=True,
        graph_like=False,
    )


def _to_profile(profile: ProblemProfile | Dict[str, Any] | None) -> ProblemProfile:
    if isinstance(profile, ProblemProfile):
        return profile
    if isinstance(profile, dict):
        return ProblemProfile(
            subject=str(profile.get("subject", "general")),
            difficulty=str(profile.get("difficulty", "unknown")),
            numeric=bool(profile.get("numeric", False)),
            multi_concept=bool(profile.get("multi_concept", profile.get("multiConcept", False))),
            trap_probability=float(profile.get("trap_probability", profile.get("trapProbability", 0.0)) or 0.0),
            symbolic=bool(profile.get("symbolic", True)),
            graph_like=bool(profile.get("graph_like", False)),
            features=dict(profile.get("features", {}) if isinstance(profile.get("features"), dict) else {}),
        )
    return _default_profile()


async def generate_hypotheses(
    question: str,
    *,
    profile: ProblemProfile | Dict[str, Any] | None = None,
    provider_fabric: ProviderFabric | None = None,
    max_items: int = 4,
    timeout_s: float = 0.65,
) -> List[Dict[str, Any]]:
    prompt = (
        "Generate short math/physics/chemistry solving hypotheses.\n"
        "Return each hypothesis on a new line, no numbering, max 12 words each.\n\n"
        f"Question: {str(question or '').strip()[:900]}"
    )
    provider_lines, provider_name = await _provider_lines(
        prompt=prompt,
        profile=profile,
        provider_fabric=provider_fabric,
        timeout_s=timeout_s,
        max_lines=max_items,
    )
    heuristics = _heuristic_hypotheses(question, max_items=max_items)
    merged = _merge_unique(provider_lines, heuristics, max_items=max_items)
    out: List[Dict[str, Any]] = []
    for idx, line in enumerate(merged):
        conf = max(0.45, min(0.88, 0.72 - 0.06 * idx))
        if line in provider_lines:
            conf += 0.04
        out.append(
            {
                "content": line,
                "confidence": round(conf, 4),
                "provider": provider_name,
                "strategy": "provider_assisted" if line in provider_lines else "heuristic",
            }
        )
    return out


async def generate_subproblems(
    question: str,
    *,
    profile: ProblemProfile | Dict[str, Any] | None = None,
    provider_fabric: ProviderFabric | None = None,
    max_items: int = 4,
    timeout_s: float = 0.6,
) -> List[Dict[str, Any]]:
    prompt = (
        "Decompose the question into concise subproblems.\n"
        "Each line should be one subproblem, no numbering.\n\n"
        f"Question: {str(question or '').strip()[:900]}"
    )
    provider_lines, provider_name = await _provider_lines(
        prompt=prompt,
        profile=profile,
        provider_fabric=provider_fabric,
        timeout_s=timeout_s,
        max_lines=max_items,
    )
    heuristics = _heuristic_subproblems(question, max_items=max_items)
    merged = _merge_unique(provider_lines, heuristics, max_items=max_items)
    out: List[Dict[str, Any]] = []
    for idx, line in enumerate(merged):
        out.append(
            {
                "content": line,
                "confidence": round(max(0.42, 0.70 - 0.06 * idx), 4),
                "provider": provider_name,
            }
        )
    return out


async def generate_solution_paths(
    subproblem: str,
    *,
    profile: ProblemProfile | Dict[str, Any] | None = None,
    provider_fabric: ProviderFabric | None = None,
    max_items: int = 3,
    timeout_s: float = 0.6,
) -> List[Dict[str, Any]]:
    prompt = (
        "Propose short solution paths for the given subproblem.\n"
        "Return each path in one line.\n\n"
        f"Subproblem: {str(subproblem or '').strip()[:700]}"
    )
    provider_lines, provider_name = await _provider_lines(
        prompt=prompt,
        profile=profile,
        provider_fabric=provider_fabric,
        timeout_s=timeout_s,
        max_lines=max_items,
    )
    heuristics = _heuristic_paths(subproblem, max_items=max_items)
    merged = _merge_unique(provider_lines, heuristics, max_items=max_items)
    out: List[Dict[str, Any]] = []
    for idx, line in enumerate(merged):
        out.append(
            {
                "content": line,
                "confidence": round(max(0.40, 0.66 - 0.05 * idx), 4),
                "provider": provider_name,
            }
        )
    return out


async def _provider_lines(
    *,
    prompt: str,
    profile: ProblemProfile | Dict[str, Any] | None,
    provider_fabric: ProviderFabric | None,
    timeout_s: float,
    max_lines: int,
) -> tuple[List[str], str]:
    try:
        lines, provider = await asyncio.wait_for(
            _provider_lines_inner(
                prompt=prompt,
                profile=_to_profile(profile),
                provider_fabric=provider_fabric,
                max_lines=max_lines,
            ),
            timeout=max(0.2, float(timeout_s)),
        )
        return lines, provider
    except Exception:
        return [], "heuristic"


async def _provider_lines_inner(
    *,
    prompt: str,
    profile: ProblemProfile,
    provider_fabric: ProviderFabric | None,
    max_lines: int,
) -> tuple[List[str], str]:
    fabric = provider_fabric or _get_provider_fabric()
    provider = "mini"
    try:
        available = [str(x).strip() for x in fabric.available_providers() if str(x).strip()]
        if available:
            # Keep pre-reasoning fast and local-first.
            provider = "mini" if "mini" in available else available[0]
    except Exception:
        provider = "mini"

    ans = await fabric.generate(provider, prompt, profile, [])
    blob = "\n".join(
        part for part in (str(ans.reasoning or "").strip(), str(ans.final_answer or "").strip()) if part
    )
    lines = _extract_lines(blob, limit=max_lines)
    return lines, provider


def _get_provider_fabric() -> ProviderFabric:
    global _PROVIDER_FABRIC
    if _PROVIDER_FABRIC is None:
        _PROVIDER_FABRIC = ProviderFabric()
    return _PROVIDER_FABRIC


def _extract_lines(text: str, *, limit: int) -> List[str]:
    rows = []
    for line in str(text or "").splitlines():
        line = re.sub(r"^[\s\-*\d\.\)\(]+", "", line).strip()
        if len(line) < 6:
            continue
        if re.match(r"^(answer|final answer|reasoning)\s*[:\-]", line, flags=re.IGNORECASE):
            line = re.sub(r"^(answer|final answer|reasoning)\s*[:\-]\s*", "", line, flags=re.IGNORECASE).strip()
        if not line:
            continue
        rows.append(line)

    if not rows:
        chunks = re.split(r"[.;]\s+", str(text or ""))
        rows = [chunk.strip() for chunk in chunks if len(chunk.strip()) >= 8]

    out = []
    seen = set()
    for row in rows:
        key = row.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
        if len(out) >= max(1, int(limit)):
            break
    return out


def _merge_unique(first: Sequence[str], second: Sequence[str], *, max_items: int) -> List[str]:
    out = []
    seen = set()
    for row in list(first) + list(second):
        token = str(row or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
        if len(out) >= max(1, int(max_items)):
            break
    return out


def _heuristic_hypotheses(question: str, *, max_items: int) -> List[str]:
    q = str(question or "").lower()
    out: List[str] = []
    if any(k in q for k in ("integral", "∫", "differentiate", "derivative")):
        out.extend(
            [
                "Translate symbols into canonical calculus expression.",
                "Apply standard integration/differentiation identity first.",
                "Check boundary/domain constraints before simplification.",
            ]
        )
    if any(k in q for k in ("equation", "solve", "=")):
        out.extend(
            [
                "Reduce to standard equation form and isolate variable.",
                "Check for extraneous roots after solving.",
            ]
        )
    if any(k in q for k in ("probability", "arrange", "combination", "permutation")):
        out.extend(
            [
                "Define sample space and favorable outcomes explicitly.",
                "Use complementary counting for constrained cases.",
            ]
        )
    if any(k in q for k in ("force", "velocity", "acceleration", "electric", "circuit")):
        out.extend(
            [
                "Write governing physical law and resolve known variables.",
                "Check unit consistency across each step.",
            ]
        )
    if not out:
        out = [
            "Identify unknown quantity and governing relations.",
            "Break question into independent solvable chunks.",
            "Verify final expression against constraints.",
        ]
    return out[: max(1, int(max_items))]


def _heuristic_subproblems(question: str, *, max_items: int) -> List[str]:
    text = re.sub(r"\s+", " ", str(question or "")).strip()
    if not text:
        return ["Extract core target variable and constraints."]

    splits = re.split(r"\b(?:and|then|subject to|given that|such that|where)\b|[?;:]", text, flags=re.IGNORECASE)
    chunks = [chunk.strip(" ,.") for chunk in splits if len(chunk.strip(" ,.")) >= 12]
    if not chunks:
        chunks = [text]

    out = []
    for chunk in chunks[: max_items]:
        out.append(f"Solve subproblem: {chunk}")
    if out:
        out.append("Validate the assembled result against original conditions.")
    return out[: max(1, int(max_items))]


def _heuristic_paths(subproblem: str, *, max_items: int) -> List[str]:
    q = str(subproblem or "").lower()
    out = [
        "Derive symbolic expression and simplify before substitution.",
        "Use an alternative form to cross-check the same target quantity.",
        "Run sanity checks on sign, magnitude, and constraints.",
    ]
    if "integral" in q or "∫" in subproblem:
        out[0] = "Transform integrand to standard form, then integrate."
    if any(k in q for k in ("matrix", "determinant", "eigen")):
        out[1] = "Use linear algebra identities and verify determinant/rank."
    if any(k in q for k in ("probability", "combination", "permutation")):
        out[2] = "Cross-check using complementary or symmetry argument."
    return out[: max(1, int(max_items))]
