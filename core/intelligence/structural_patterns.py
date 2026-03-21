from __future__ import annotations

import re
from typing import Dict, Iterable, List


def _uniq(values: Iterable[str]) -> List[str]:
    out = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


REASONING_ARCHETYPE_RULES: Dict[str, List[str]] = {
    "transformation_based_reasoning": [
        r"\btransform\b",
        r"\brewrite\b",
        r"\bconvert\b",
        r"\bexpress in terms\b",
    ],
    "extremal_argument": [
        r"\bmaximum\b",
        r"\bminimum\b",
        r"\bextreme\b",
        r"\boptimal\b",
    ],
    "symmetry_exploitation": [
        r"\bsymmetric\b",
        r"\bsymmetry\b",
        r"\bequal terms\b",
        r"\bmirror\b",
    ],
    "substitution_strategy": [
        r"\bsubstitute\b",
        r"\blet\b",
        r"\bchange variable\b",
    ],
    "invariant_method": [
        r"\binvariant\b",
        r"\bconserved\b",
        r"\bremains constant\b",
    ],
    "generating_function_strategy": [
        r"\bgenerating function\b",
        r"\bcoefficient extraction\b",
        r"\bseries coefficient\b",
    ],
    "geometric_locus_construction": [
        r"\blocus\b",
        r"\bset of points\b",
        r"\bgeometric place\b",
    ],
    "energy_method_substitution": [
        r"\benergy\b",
        r"\bwork done\b",
        r"\bpotential\b",
        r"\bconservation of energy\b",
    ],
}


STRUCTURAL_PATTERN_RULES: Dict[str, List[str]] = {
    "exclusion_principle": [r"\bexclude\b", r"\bnot allowed\b", r"\bwithout\b"],
    "complementary_counting": [r"\bcomplement\b", r"\bat least\b", r"\bstrictly greater\b"],
    "arrangement_under_restriction": [r"\barrangement\b", r"\bformed using\b", r"\brestriction\b"],
    "series_expansion_transform": [r"\bexpansion\b", r"\bcoefficient\b", r"\bbinomial\b"],
    "asymptotic_comparison": [r"\blimit\b", r"\bdominant\b", r"\bas x tends\b"],
    "gaussian_surface_selection": [r"\bgauss\b", r"\bsymmetric charge\b", r"\bflux\b"],
    "state_variable_transformation": [r"\bthermodynamic\b", r"\bstate function\b", r"\bprocess\b"],
    "mechanism_flow_tracking": [r"\bmechanism\b", r"\bintermediate\b", r"\breaction step\b"],
    "titration_logic": [r"\btitration\b", r"\bend point\b", r"\bindicator\b"],
    "error_analysis": [r"\berror\b", r"\buncertainty\b", r"\bsignificant\b"],
}


TRAP_SIGNAL_RULES: Dict[str, List[str]] = {
    "overcounting": [r"\bnumber of\b", r"\bformed using\b", r"\barrangements?\b"],
    "double_counting": [r"\bcount\b", r"\btwice\b", r"\bduplicate\b"],
    "adjacency_confusion": [r"\badjacent\b", r"\bnext to\b", r"\btogether\b"],
    "index_shift_error": [r"\bcoefficient\b", r"x\^\d+", r"\bterm\b"],
    "middle_term_index_error": [r"\bmiddle term\b", r"\bn\+1\b", r"\bequal coefficients\b"],
    "domain_loss": [r"\broot\b", r"\blog\b", r"\bdenominator\b"],
    "sign_convention_error": [r"\bnegative\b", r"\bsign\b", r"\bdirection\b"],
    "unit_mismatch": [r"\bunit\b", r"\bdimension\b", r"\bmeasurement\b"],
    "endpoint_overshoot": [r"\bend point\b", r"\btitration\b"],
}


PRACTICAL_TAG_RULES: Dict[str, List[str]] = {
    "experimental_setup": [r"\bexperiment\b", r"\bsetup\b", r"\bapparatus\b", r"\bcircuit diagram\b"],
    "instrument_calibration": [r"\bcalibration\b", r"\bzero error\b", r"\bleast count\b", r"\bstandardization\b"],
    "titration_logic": [r"\btitration\b", r"\bindicator\b", r"\bend point\b", r"\bburette\b"],
    "error_analysis": [r"\buncertainty\b", r"\berror\b", r"\bsignificant figure\b", r"\bprecision\b"],
}


class StructuralPatternDetector:
    def __init__(self):
        self.archetype_rules = REASONING_ARCHETYPE_RULES
        self.pattern_rules = STRUCTURAL_PATTERN_RULES
        self.trap_rules = TRAP_SIGNAL_RULES
        self.practical_rules = PRACTICAL_TAG_RULES

    def analyze(
        self,
        question: str,
        *,
        unit_structural_patterns: List[str] | None = None,
        unit_common_traps: List[str] | None = None,
        unit_archetypes: List[str] | None = None,
    ) -> Dict:
        text = str(question or "").strip().lower()

        detected_patterns = self._detect_by_rules(text, self.pattern_rules)
        detected_archetypes = self._detect_by_rules(text, self.archetype_rules)
        trap_signals = self._detect_by_rules(text, self.trap_rules)
        practical_tags = self._detect_by_rules(text, self.practical_rules)

        if unit_structural_patterns:
            for pattern in unit_structural_patterns:
                normalized = str(pattern).strip().lower()
                if normalized and normalized in text and pattern not in detected_patterns:
                    detected_patterns.append(pattern)

        if unit_common_traps:
            for trap in unit_common_traps:
                normalized = str(trap).strip().lower().replace("_", " ")
                if normalized and normalized in text and trap not in trap_signals:
                    trap_signals.append(trap)

        if unit_archetypes:
            for archetype in unit_archetypes:
                if archetype not in detected_archetypes:
                    fallback = str(archetype).replace("_", " ")
                    if fallback and fallback in text:
                        detected_archetypes.append(archetype)

        structural_complexity = self._structural_complexity_score(
            text=text,
            patterns=detected_patterns,
            archetypes=detected_archetypes,
            practical=practical_tags,
        )
        trap_density = self._trap_density_score(text=text, traps=trap_signals)

        return {
            "structural_patterns": _uniq(detected_patterns),
            "reasoning_archetypes": _uniq(detected_archetypes),
            "trap_signals": _uniq(trap_signals),
            "practical_tags": _uniq(practical_tags),
            "structural_complexity_score": structural_complexity,
            "trap_density_score": trap_density,
        }

    def _detect_by_rules(self, text: str, rules: Dict[str, List[str]]) -> List[str]:
        out = []
        for label, patterns in rules.items():
            for pattern in patterns:
                if re.search(pattern, text):
                    out.append(label)
                    break
        return out

    def _structural_complexity_score(self, *, text: str, patterns: List[str], archetypes: List[str], practical: List[str]) -> float:
        clause_count = len([s for s in re.split(r"[,.:;]", text) if s.strip()])
        score = 0.10
        score += 0.12 * len(patterns)
        score += 0.13 * len(archetypes)
        score += 0.10 * len(practical)
        score += 0.02 * min(clause_count, 10)
        return max(0.0, min(1.0, score))

    def _trap_density_score(self, *, text: str, traps: List[str]) -> float:
        score = 0.05 * len(traps)
        score += 0.01 * len(re.findall(r"\bnot\b|\bexcept\b|\bstrictly\b|\bat least\b|\bat most\b", text))
        return max(0.0, min(1.0, score))
