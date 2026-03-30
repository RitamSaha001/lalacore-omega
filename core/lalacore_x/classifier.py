from __future__ import annotations

import re
from collections import Counter
from typing import Dict

from core.lalacore_x.schemas import ProblemProfile


_SUBJECT_PATTERNS: Dict[str, tuple[str, ...]] = {
    "math": (
        "integral",
        "derivative",
        "algebra",
        "probability",
        "matrix",
        "equation",
        "limit",
        "permutation",
        "combination",
        "binomial",
        "factorial",
        "circle",
        "parabola",
        "ellipse",
        "hyperbola",
        "tangent",
        "normal",
        "chord",
        "locus",
        "asymptote",
        "directrix",
        "focus",
        "envelope",
        "intercept",
        "coordinate geometry",
    ),
    "physics": (
        "force",
        "velocity",
        "acceleration",
        "joule",
        "newton",
        "electro",
        "electric",
        "magnetic",
        "wave",
        "field",
        "flux",
        "gauss",
        "coulomb",
        "charge density",
        "potential",
        "permittivity",
        "dielectric",
        "spherical cavity",
    ),
    "chemistry": ("mole", "reaction", "equilibrium", "acid", "base", "orbital", "stoichiometry"),
}

_ADVANCED_MATH_PATTERNS = (
    "circle",
    "parabola",
    "ellipse",
    "hyperbola",
    "tangent",
    "tangents",
    "normal",
    "chord",
    "locus",
    "asymptote",
    "directrix",
    "focus",
    "envelope",
    "intercept",
    "axes",
    "axis",
    "midpoint",
    "intersection",
    "pair of perpendicular lines",
    "chord of contact",
    "fixed point",
)

_TRAP_PATTERNS = (
    r"except",
    r"not\s+correct",
    r"incorrect",
    r"all\s+of\s+the\s+above",
    r"none\s+of\s+the\s+above",
    r"closest",
    r"approx",
    r"assume",
)


class ProblemClassifier:
    """
    Lightweight classifier for routing decisions.
    Kept dependency-free so it can run in constrained environments.
    """

    def classify(self, question: str) -> ProblemProfile:
        text = (question or "").strip().lower()
        tokens = re.findall(r"[a-z0-9_\-\+\*/\^=]+", text)

        subject_scores = Counter()
        for subject, patterns in _SUBJECT_PATTERNS.items():
            for pattern in patterns:
                if pattern in text:
                    subject_scores[subject] += 1

        physics_signal = int(subject_scores.get("physics", 0))
        chemistry_signal = int(subject_scores.get("chemistry", 0))
        operator_heavy = bool(re.search(r"[\+\-\*/\^=]", text))
        math_operator_context = bool(
            re.search(
                r"\b(integral|derivative|algebra|probability|matrix|equation|limit|permutation|combination|binomial|factorial)\b",
                text,
            )
        )

        # Operator-heavy text is ambiguous: only default to math when there
        # is no stronger physics/chemistry evidence.
        if operator_heavy:
            if physics_signal > 0 and not math_operator_context:
                subject_scores["physics"] += 1
            elif chemistry_signal > 0 and not math_operator_context:
                subject_scores["chemistry"] += 1
            else:
                subject_scores["math"] += 1

        subject = subject_scores.most_common(1)[0][0] if subject_scores else "general"

        numeric = bool(re.search(r"\d", text))
        symbolic = bool(re.search(r"[a-z]\s*[\+\-\*/\^=]", text))
        graph_like = any(k in text for k in ("graph", "plot", "monotonic", "increasing", "decreasing"))

        multi_concept = self._is_multi_concept(text)
        trap_probability = self._trap_probability(text)
        advanced_math_hits = sum(1 for pattern in _ADVANCED_MATH_PATTERNS if pattern in text)
        difficulty = self._difficulty_label(
            text,
            multi_concept,
            trap_probability,
            advanced_math_hits=advanced_math_hits,
            graph_like=graph_like,
        )

        return ProblemProfile(
            subject=subject,
            difficulty=difficulty,
            numeric=numeric,
            multi_concept=multi_concept,
            trap_probability=trap_probability,
            symbolic=symbolic,
            graph_like=graph_like,
            features={
                "token_count": len(tokens),
                "equation_count": text.count("="),
                "operator_count": len(re.findall(r"[\+\-\*/\^]", text)),
                "advanced_math_hits": advanced_math_hits,
                "trap_pattern_hits": [p for p in _TRAP_PATTERNS if re.search(p, text)],
            },
        )

    def _is_multi_concept(self, text: str) -> bool:
        multi_markers = (
            " and ",
            " then ",
            " using ",
            " prove ",
            " derive ",
            " mechanism",
            "include",
            "step",
        )
        marker_hits = sum(1 for marker in multi_markers if marker in text)
        return marker_hits >= 2

    def _trap_probability(self, text: str) -> float:
        hits = sum(1 for pattern in _TRAP_PATTERNS if re.search(pattern, text))
        base = min(0.15 * hits, 0.75)

        if "assertion" in text and "reason" in text:
            base += 0.15
        if "integer" in text and "closest" in text:
            base += 0.1

        return round(min(base, 0.95), 4)

    def _difficulty_label(
        self,
        text: str,
        multi_concept: bool,
        trap_probability: float,
        *,
        advanced_math_hits: int = 0,
        graph_like: bool = False,
    ) -> str:
        score = 0

        score += len(re.findall(r"[\+\-\*/\^=]", text))
        score += 3 if multi_concept else 0
        score += int(trap_probability * 10)
        score += min(8, advanced_math_hits * 2)

        if any(k in text for k in ("prove", "derive", "optimization", "mechanism", "eigen", "differential")):
            score += 4
        if graph_like:
            score += 2
        if "locus" in text and advanced_math_hits > 0:
            score += 4
        if any(
            k in text
            for k in (
                "tangent",
                "tangents",
                "normal",
                "chord",
                "envelope",
                "asymptote",
                "directrix",
                "fixed point",
                "pair of perpendicular lines",
            )
        ):
            score += 2

        if score >= 12:
            return "hard"
        if score >= 6:
            return "medium"
        return "easy"
