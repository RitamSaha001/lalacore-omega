from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "app" / "JEE_BANK_X.json"
OUTPUT = ROOT / "data" / "app" / "JEE_BANK_X.manual_curated.math.json"
REJECTED = ROOT / "data" / "app" / "JEE_BANK_X.manual_curated.math.rejected.json"
REPORT = ROOT / "data" / "app" / "JEE_BANK_X.manual_curated.math.report.json"


CHAPTER_KEYWORDS: dict[str, list[str]] = {
    "Sequences and Series": [
        "a.p",
        "g.p",
        "h.p",
        "sequence",
        "series",
        "progression",
        "sum of first",
        "terms",
        "nth term",
        "arithmetic progression",
        "geometric progression",
    ],
    "Coordinate Geometry": [
        "circle",
        "parabola",
        "ellipse",
        "hyperbola",
        "directrix",
        "focus",
        "foci",
        "chord",
        "tangent",
        "normal",
        "slope",
        "locus",
        "origin",
        "distance",
        "line",
        "axes",
    ],
    "Three Dimensional Geometry": [
        "plane",
        "line",
        "vector",
        "position vector",
        "tetrahedron",
        "coplanar",
        "direction",
        "distance",
        "point",
        "normal",
        "parallel",
        "perpendicular",
    ],
    "Relations and Functions": [
        "function",
        "relation",
        "range",
        "domain",
        "inverse",
        "bijection",
        "one-one",
        "onto",
        "composite",
        "greatest integer",
        "equivalence",
        "symmetric",
        "transitive",
        "reflexive",
    ],
    "Sets, Relations and Functions": [
        "function",
        "relation",
        "range",
        "domain",
        "inverse",
        "bijection",
        "one-one",
        "onto",
        "composite",
        "greatest integer",
        "set",
        "equivalence",
        "symmetric",
        "transitive",
        "reflexive",
    ],
    "Application of Derivatives": [
        "tangent",
        "normal",
        "maximum",
        "minimum",
        "increasing",
        "decreasing",
        "local maxima",
        "local minima",
        "rate",
        "derivative",
        "slope",
    ],
    "Limits, Continuity and Differentiability": [
        "lim",
        "limit",
        "continuous",
        "continuity",
        "differentiable",
        "derivative",
        "signum",
        "sgn",
        "discontinuity",
    ],
    "Integral Calculus": [
        "integral",
        "dx",
        "area",
        "antiderivative",
        "definite",
        "indefinite",
    ],
    "Trigonometric Functions": [
        "sin",
        "cos",
        "tan",
        "cot",
        "sec",
        "cosec",
        "trigonometric",
        "arcsin",
        "arccos",
        "arctan",
    ],
    "Binomial Theorem": [
        "binomial",
        "coefficient",
        "expansion",
        "term",
        "middle term",
        "general term",
    ],
    "Permutations and Combinations": [
        "ways",
        "arrangements",
        "permutations",
        "combinations",
        "select",
        "choose",
        "committee",
        "digit",
        "word",
    ],
    "Probability and Statistics": [
        "probability",
        "mean",
        "variance",
        "standard deviation",
        "observations",
        "bag",
        "coin",
        "die",
        "random",
        "distribution",
        "median",
    ],
    "Matrices and Determinants": [
        "matrix",
        "determinant",
        "adjoint",
        "inverse",
        "trace",
        "diagonal",
        "singular",
        "non-singular",
    ],
    "Complex Numbers and Quadratic Equations": [
        "complex",
        "argand",
        "modulus",
        "conjugate",
        "real part",
        "imaginary",
        "quadratic",
    ],
    "Differential Equations": [
        "differential equation",
        "dy/dx",
        "general solution",
        "particular solution",
        "solution curve",
        "order",
        "degree",
    ],
    "Inverse Trigonometric Functions": [
        "sin-1",
        "cos-1",
        "tan-1",
        "inverse trigonometric",
        "arcsin",
        "arccos",
        "arctan",
    ],
    "Vector Algebra": [
        "vector",
        "dot product",
        "cross product",
        "position vector",
        "scalar product",
        "vector product",
    ],
}

SCIENCE_PAT = re.compile(
    r"(molar|molar mass|mol-1|g mol|ksp|kc\b|acid|base|salt|reaction sequence|major product|minor product|"
    r"alkane|alkene|benzene|organic|anode|cathode|electrode|electrolyte|enthalpy|photoelectric|wavelength|"
    r"current|voltage|resistance|capacitor|transistor|galvanometer|battery|coil|hydrogen atom|lyman|balmer|"
    r"paschen|nuclear|radioactive|fructose|glucose|tollen|ozonolysis|bromine water|azo dye|hybridised carbon|"
    r"compound|molecule|ionisation enthalpy|electron|proton|neutron|photon|interference experiment|young's "
    r"double slit|de-broglie|diffraction|slit|transformer|magnetic field|electric field|pendulum|eutrophication|"
    r"fly ash|sulphur|nitric|oxalic acid|benzoic acid|buffer|entropy|equilibrium constant|cell potential|"
    r"rubidium atom|quantum numbers)",
    re.I,
)
ARTIFACT_PAT = re.compile(
    r"(question paper previous year paper|join the most relevant test series|https?://|answer keys|"
    r"mathongo|allen|resonance|fiitjee|exercise\s*\(?s\)?|jee\s*\((main|advanced)\)|iit jee|"
    r"cn\d{2}-\d+|di\d{2}-\d+|mt\d{2}-\d+|dt\d{2}-\d+|official ans\.? by nta)",
    re.I,
)
ANSWER_TABLE_PAT = re.compile(r"^\s*(\(?\d+\)?\.?\s*){4,}")
TRUNC_PAT = re.compile(r"\b(then|find|is|are|be|if|let|which|the|value|number|locus|statement)\s*$", re.I)
FIGURE_PAT = re.compile(r"(shown in the figure|as shown in figure|shown below|graph|figure)", re.I)
PLACEHOLDER_PAT = re.compile(
    r"(\blet be\b|\blet and\b|\bif is\b|\bwhere \.\b|\bthen the value of is\b|"
    r"\bthe range of is\b|\bsuch that equals\b|\bif is the\b|\bthe number of complex numbers such that equals\b)",
    re.I,
)
OPTION_PAT = re.compile(r"\(1\)|\(2\)|\(3\)|\(4\)")


def _cleanup(text: Any) -> str:
    value = str(text or "").replace("\x00", " ")
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(ARTIFACT_PAT, " ", value)
    value = re.sub(r"\s+", " ", value).strip(" -:;,.")
    return value


def _ratio(a: str, b: str) -> float:
    from difflib import SequenceMatcher

    return SequenceMatcher(
        None,
        re.sub(r"[^a-z0-9]+", "", a.lower())[:600],
        re.sub(r"[^a-z0-9]+", "", b.lower())[:600],
    ).ratio()


def curate_row(row: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    chapter = str(row.get("chapter") or "").strip()
    subject = str(row.get("subject") or "").strip()
    question = _cleanup(
        row.get("clean_question_text")
        or row.get("repaired_question_text")
        or row.get("question_text")
        or ""
    )
    solution = _cleanup(row.get("solution_explanation") or "")
    reasons: list[str] = []

    if subject == "Mathematics" and chapter not in CHAPTER_KEYWORDS:
        reasons.append("unsupported_chapter")
    if str(row.get("repair_status") or "").lower() in {"manual_review", "reject", "unrecoverable"}:
        reasons.append("unsafe_status")
    if row.get("requires_human_review"):
        reasons.append("requires_human_review")
    if len(question) < 45:
        reasons.append("short_question")
    if len(solution) < 60:
        reasons.append("short_solution")
    if TRUNC_PAT.search(question):
        reasons.append("truncated_question")
    if ANSWER_TABLE_PAT.search(question):
        reasons.append("answer_table_question")
    if ANSWER_TABLE_PAT.search(solution):
        reasons.append("answer_table_solution")
    if FIGURE_PAT.search(question) or FIGURE_PAT.search(solution):
        reasons.append("figure_dependency")
    if PLACEHOLDER_PAT.search(question):
        reasons.append("placeholder_question")
    if SCIENCE_PAT.search(question):
        reasons.append("non_math_question")
    if SCIENCE_PAT.search(solution):
        reasons.append("non_math_solution")
    if question.count("*") >= 5:
        reasons.append("heavy_ocr_question")
    if solution.count("*") >= 5:
        reasons.append("heavy_ocr_solution")
    if OPTION_PAT.search(solution) and "=" not in solution and "therefore" not in solution.lower() and "hence" not in solution.lower():
        reasons.append("solution_looks_like_option_dump")
    if chapter in CHAPTER_KEYWORDS:
        q_lower = question.lower()
        if not any(token in q_lower for token in CHAPTER_KEYWORDS[chapter]):
            reasons.append("chapter_keyword_mismatch")
    if question and solution and _ratio(question, solution) > 0.88:
        reasons.append("solution_repeats_question")

    accepted = not reasons
    payload = dict(row)
    payload["manual_curation"] = {
        "accepted": accepted,
        "reasons": reasons,
        "clean_question_text": question,
        "clean_solution_explanation": solution,
        "source_bank": "JEE_BANK_X",
    }
    if accepted:
        payload["question_text"] = question
        payload["solution_explanation"] = solution
        payload["manual_curation_status"] = "accepted_candidate"
    else:
        payload["manual_curation_status"] = "rejected"
    return accepted, payload


def main() -> None:
    rows = json.loads(INPUT.read_text())
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for row in rows:
        chapter = str(row.get("chapter") or "").strip()
        subject = str(row.get("subject") or "").strip()
        in_math_scope = subject == "Mathematics" or chapter in CHAPTER_KEYWORDS
        if not in_math_scope:
            continue
        ok, curated = curate_row(row)
        if ok:
            accepted.append(curated)
        else:
            rejected.append(curated)

    OUTPUT.write_text(json.dumps(accepted, ensure_ascii=False, indent=2))
    REJECTED.write_text(json.dumps(rejected, ensure_ascii=False, indent=2))

    report = {
        "input_rows": len(rows),
        "accepted_rows": len(accepted),
        "rejected_rows": len(rejected),
        "accepted_by_chapter": dict(
            sorted(
                Counter(str(r.get("chapter") or "").strip() for r in accepted).items(),
                key=lambda item: (-item[1], item[0]),
            )
        ),
        "rejection_reasons": dict(
            sorted(
                Counter(
                    reason
                    for row in rejected
                    for reason in row.get("manual_curation", {}).get("reasons", [])
                ).items(),
                key=lambda item: (-item[1], item[0]),
            )
        ),
        "output_file": str(OUTPUT),
        "rejected_file": str(REJECTED),
    }
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
