from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .expression_graph import ExpressionGraphBuilder
from .graph_repair import GraphRepairEngine
from .deterministic_rule_engine import DeterministicOcrRepairEngine
from .math_validator import MathematicalSanityValidator
from .piecewise_detector import PiecewiseDetector
from .question_type_classifier import QuestionTypeClassifier
from .similarity_repair import SimilarityRepairEngine
from .solver_engine import DeterministicSolverEngine
from .symbol_normalizer import SymbolNormalizer

try:
    from latex_sanitizer import sanitize_latex
except Exception:  # pragma: no cover - fallback when sanitizer import is unavailable
    def sanitize_latex(text: str) -> str:
        return str(text or "")


@dataclass
class MathRepairOutput:
    question_id: str
    repaired_question_text: str
    repaired_latex: str
    options: list[dict[str, str]]
    correct_answer: dict[str, Any]
    repair_actions: list[str]
    repair_confidence: float
    repair_status: str
    detected_question_type: str
    validation_issues: list[str]
    solver_notes: list[str]
    options_latex: dict[str, str]
    topic: str
    difficulty: str
    detected_repairs: list[str]
    verification: dict[str, Any]
    requires_human_review: bool
    clean_question_text: str
    clean_question_latex: str


class MathRepairEngine:
    """Production-safe deterministic reconstruction pipeline for JEE questions."""

    _OPTION_MARKER_RE = re.compile(r"(?:^|[\s;])\(?([A-D])\)?[\).:\-]\s*", re.IGNORECASE)

    def __init__(self) -> None:
        self.symbol_normalizer = SymbolNormalizer()
        self.graph_builder = ExpressionGraphBuilder()
        self.graph_repair = GraphRepairEngine()
        self.deterministic_rule_engine = DeterministicOcrRepairEngine(
            symbol_normalizer=self.symbol_normalizer,
            graph_builder=self.graph_builder,
        )
        self.piecewise_detector = PiecewiseDetector()
        self.type_classifier = QuestionTypeClassifier()
        self.math_validator = MathematicalSanityValidator()
        self.solver_engine = DeterministicSolverEngine()
        self.similarity_engine = SimilarityRepairEngine()

    def repair_question(
        self,
        question: dict[str, Any],
        *,
        corpus: list[dict[str, Any]] | None = None,
    ) -> MathRepairOutput:
        qid = str(question.get("question_id") or question.get("id") or "")
        text = str(question.get("question_text") or question.get("question") or question.get("text") or "")
        options = self._normalize_options(question.get("options"))
        correct_answer = self._normalize_correct_answer(question)
        declared_type = str(question.get("type") or question.get("question_type") or "")
        subject = str(question.get("subject") or "").strip()
        chapter = str(question.get("chapter") or "").strip()
        declared_difficulty = str(question.get("difficulty") or "").strip()

        actions: list[str] = []
        issues: list[str] = []
        confidence = 0.25

        # Stage 1-10: Deterministic OCR repair layers (pre-AST).
        deterministic = self.deterministic_rule_engine.repair(
            question_text=text,
            options=options,
            question_type=declared_type,
        )
        text = deterministic.question_text
        options = deterministic.options
        for action in deterministic.actions:
            if action not in actions:
                actions.append(action)
        for token in deterministic.issues:
            if token not in issues:
                issues.append(token)
        if deterministic.actions:
            confidence += 0.14

        # Stage 11/12: Graph build + repair.
        graph_before = self.graph_builder.build(text)
        repair = self.graph_repair.repair(text)
        text = repair.text
        for action in repair.actions:
            if action not in actions:
                actions.append(action)
        graph_issue_allow = {"unbalanced_parenthesis", "dangling_operator", "empty_expression", "graph_root_missing"}
        for token in graph_before.issues:
            if token in graph_issue_allow or token.startswith("missing_operand:"):
                if token not in issues:
                    issues.append(token)
        for token in repair.issues:
            if token in graph_issue_allow or token.startswith("missing_operand:"):
                if token not in issues:
                    issues.append(token)
        if not graph_before.issues and not repair.issues:
            confidence += 0.2
        else:
            confidence += 0.08

        # Stage 13: Piecewise reconstruction.
        piecewise = self.piecewise_detector.reconstruct(text)
        if piecewise.detected:
            text = piecewise.text
            for action in piecewise.actions:
                if action not in actions:
                    actions.append(action)
            confidence += 0.1

        # Stage 14: Structure/type reconstruction.
        text, extracted = self._extract_inline_options(text)
        if extracted and len(options) <= 1:
            options = extracted
            if "options_split" not in actions:
                actions.append("options_split")
        q_type = self.type_classifier.classify(
            question_text=text,
            options=options,
            declared_type=declared_type,
        )
        if q_type == "NUMERICAL":
            options = []
            correct_answer["single"] = None
            correct_answer["multiple"] = []
        elif q_type == "MCQ_SINGLE" and options and not correct_answer.get("single"):
            # Minimal deterministic fallback: if exactly one label is stored in multiple, promote it.
            multiple = [str(x).upper() for x in (correct_answer.get("multiple") or []) if str(x).strip()]
            if len(multiple) == 1:
                correct_answer["single"] = multiple[0]
        if q_type == "LIST_MATCH" and "list_match_detected" not in actions:
            actions.append("list_match_detected")
        if q_type != "NUMERICAL" and len(options) >= 2:
            confidence += 0.12
        if q_type == "NUMERICAL" and str(correct_answer.get("numerical") or "").strip():
            confidence += 0.12
        answer_single = str(correct_answer.get("single") or "").upper()
        if answer_single:
            labels = {str(opt.get("label") or "").upper() for opt in options}
            if not labels or answer_single in labels:
                confidence += 0.1

        # Stage 15: Validate sanity/grammar.
        validation = self.math_validator.validate(
            question_text=text,
            options=options,
            question_type=q_type,
        )
        for issue in validation.issues:
            if issue not in issues:
                issues.append(issue)
        if validation.grammar_valid:
            confidence += 0.2
        if validation.sanity_valid:
            confidence += 0.1
        if validation.grammar_valid and len(text.strip()) >= 24:
            confidence += 0.08
        if validation.grammar_valid and validation.sanity_valid and not validation.issues:
            confidence += 0.1

        # Stage 16: Deterministic solver verification.
        solver = self.solver_engine.verify(
            question_text=text,
            options=options,
            correct_answer=correct_answer,
            question_type=q_type,
        )
        solver_notes = list(solver.notes)
        if solver.attempted and solver.verified:
            confidence += 0.3
            if "solver_verified" not in actions:
                actions.append("solver_verified")
        elif solver.attempted and solver.answer_mismatch:
            issues.append("answer_mismatch")

        # Stage 17: Similarity repair.
        if len(text.strip()) < 18 or "empty_question" in issues or "dangling_operator" in issues:
            sim = self.similarity_engine.repair_with_corpus(
                query_text=text,
                corpus=corpus,
                min_score=0.9,
            )
            if sim.matched and sim.replacement_text:
                text = sim.replacement_text
                if "similarity_repaired" not in actions:
                    actions.append("similarity_repaired")
                confidence += 0.1
                if "empty_question" in issues:
                    issues.remove("empty_question")
                solver_notes.append(f"similarity_match:{sim.matched_question_id}:{sim.score:.3f}")

        # Stage 18: AI fallback marker only (LLM is last resort, disabled here).
        if confidence < 0.5 and "ai_fallback_required" not in actions:
            actions.append("ai_fallback_required")

        # Confidence penalties.
        penalty = 0.0
        for issue in issues:
            if issue in {"empty_question", "unbalanced_brackets", "equation_rhs_missing"}:
                penalty += 0.2
            elif issue in {"expression_parse_failure", "dangling_operator", "missing_options"}:
                penalty += 0.12
            elif issue == "answer_mismatch":
                penalty += 0.15
            elif issue.startswith("missing_operand:"):
                penalty += 0.04
            else:
                penalty += 0.02
        confidence = max(0.0, min(1.0, confidence - min(0.7, penalty)))

        clean_question_text = self._cleanup_render_text(text.strip())
        cleaned_options: list[dict[str, str]] = []
        for opt in options:
            cleaned_options.append(
                {
                    "label": str(opt.get("label") or "").upper(),
                    "text": self._cleanup_render_text(str(opt.get("text") or "").strip()),
                }
            )
        options = [opt for opt in cleaned_options if opt["label"] and opt["text"]]
        clean_question_latex = sanitize_latex(clean_question_text)
        options_latex = {
            str(opt.get("label") or "").upper(): sanitize_latex(str(opt.get("text") or "").strip())
            for opt in options
            if str(opt.get("label") or "").strip() and str(opt.get("text") or "").strip()
        }
        topic = self._infer_topic(
            question_text=clean_question_text,
            option_texts=[str(opt.get("text") or "") for opt in options],
            chapter=chapter,
            subject=subject,
        )
        difficulty = self._infer_difficulty(
            declared_difficulty=declared_difficulty,
            chapter=chapter,
            topic=topic,
            confidence=confidence,
        )

        if confidence >= 0.85 and "answer_mismatch" not in issues:
            status = "safe"
        elif confidence >= 0.5:
            status = "review"
        else:
            status = "manual_review"

        ambiguity_detected = bool(
            status != "safe"
            or any(
                token in {
                    "expression_parse_failure",
                    "equation_rhs_missing",
                    "empty_question",
                    "piecewise_case_incomplete",
                    "list_match_structure_missing",
                    "dangling_operator",
                }
                for token in issues
            )
        )
        requires_human_review = bool(confidence < 0.9 or ambiguity_detected)
        verification = {
            "mathematical_consistency": bool(
                validation.grammar_valid
                and validation.sanity_valid
                and not any(
                    token
                    in {
                        "expression_parse_failure",
                        "equation_rhs_missing",
                        "empty_question",
                        "dangling_operator",
                    }
                    for token in issues
                )
            ),
            "answer_key_verified": bool(not solver.answer_mismatch),
            "answer_key_check_performed": bool(solver.attempted),
            "ambiguity_detected": ambiguity_detected,
            "computed_answer": solver.computed_answer,
            "computed_option_label": getattr(solver, "computed_label", None),
            "suggested_correct_answer": getattr(solver, "suggested_correct_answer", None),
        }

        return MathRepairOutput(
            question_id=qid,
            repaired_question_text=clean_question_text,
            repaired_latex=clean_question_latex,
            options=options,
            correct_answer=correct_answer,
            repair_actions=list(dict.fromkeys(actions)),
            repair_confidence=round(confidence, 4),
            repair_status=status,
            detected_question_type=q_type,
            validation_issues=list(dict.fromkeys(issues)),
            solver_notes=solver_notes,
            options_latex=options_latex,
            topic=topic,
            difficulty=difficulty,
            detected_repairs=list(dict.fromkeys(actions)),
            verification=verification,
            requires_human_review=requires_human_review,
            clean_question_text=clean_question_text,
            clean_question_latex=clean_question_latex,
        )

    def _infer_topic(
        self,
        *,
        question_text: str,
        option_texts: list[str],
        chapter: str,
        subject: str,
    ) -> str:
        blob = f"{question_text} {' '.join(option_texts)} {chapter} {subject}".lower()
        rules = [
            ("limits", ("lim", "approach", "x->", "x->", "\\to")),
            ("continuity", ("continuity", "continuous")),
            ("differentiability", ("differentiable", "derivative", "f'(", "f'_")),
            ("signum function", ("sgn", "signum")),
            ("greatest integer", ("floor(", "[x]", "greatest integer")),
            ("piecewise functions", ("cases", "for x", "piecewise", "{", "}")),
            ("sequences and series", ("sum", "series", "sequence", "progression", "sigma")),
            ("algebraic equations", ("equation", "roots", "polynomial", "quadratic")),
            ("vector calculus", ("vector", "dot product", "cross product", "plane", "line")),
            ("matrices", ("matrix", "det(", "adj(", "rank", "eigen")),
            ("inequalities", ("inequality", "<=", ">=", "≤", "≥")),
        ]
        for label, probes in rules:
            if any(tok in blob for tok in probes):
                return label
        if chapter:
            return chapter
        if subject:
            return subject
        return "general mathematics"

    def _cleanup_render_text(self, text: str) -> str:
        out = str(text or "")
        # Remove stray OCR multiplication markers that survive near option boundaries.
        out = re.sub(r"(?<![A-Za-z0-9])\*\s*", "", out)
        out = re.sub(r"\s*\*(?=\s*(?:$|[.,;:!?]))", "", out)
        out = re.sub(r"(?<=\b\d)\*(?=\s*(?:need\b|none\b|exists\b|does\b))", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\*(?=\s*need\b)", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\)\s*need\b", ") need", out, flags=re.IGNORECASE)
        out = re.sub(r"([A-Za-z0-9])need\b", r"\1 need", out, flags=re.IGNORECASE)
        out = re.sub(r"\s{2,}", " ", out).strip()
        return out

    def _infer_difficulty(
        self,
        *,
        declared_difficulty: str,
        chapter: str,
        topic: str,
        confidence: float,
    ) -> str:
        explicit = declared_difficulty.strip()
        if explicit:
            low = explicit.lower()
            if "advanced" in low:
                return "JEE Advanced"
            if "main" in low:
                return "JEE Main"
            if "hard" in low:
                return "JEE Advanced"
        blob = f"{chapter} {topic}".lower()
        if any(tok in blob for tok in ("advanced", "subjective", "matrix match")):
            return "JEE Advanced"
        return "JEE Advanced" if confidence >= 0.75 else "JEE Main"

    def _normalize_options(self, raw: Any) -> list[dict[str, str]]:
        if isinstance(raw, dict):
            out: list[dict[str, str]] = []
            for idx, key in enumerate(sorted(raw.keys())):
                value = str(raw.get(key) or "").strip()
                if value:
                    out.append({"label": chr(65 + min(idx, 25)), "text": value})
            return out
        out: list[dict[str, str]] = []
        if isinstance(raw, list):
            for idx, item in enumerate(raw):
                if isinstance(item, dict):
                    label = str(item.get("label") or "").upper() or chr(65 + min(idx, 25))
                    text = str(item.get("text") or item.get("option") or item.get("value") or "").strip()
                else:
                    label = chr(65 + min(idx, 25))
                    text = str(item or "").strip()
                if text:
                    out.append({"label": label, "text": text})
        return out

    def _normalize_correct_answer(self, row: dict[str, Any]) -> dict[str, Any]:
        ans = row.get("correct_answer")
        if isinstance(ans, dict):
            multiple = ans.get("multiple")
            if not isinstance(multiple, list):
                multiple = []
            return {
                "single": (str(ans.get("single") or "").upper() or None),
                "multiple": [str(x).upper() for x in multiple if str(x).strip()],
                "numerical": (str(ans.get("numerical") or "").strip() or None),
                "tolerance": ans.get("tolerance"),
            }
        single = str(row.get("_correct_option") or row.get("correct_option") or "").upper() or None
        multiple_raw = row.get("_correct_answers") or row.get("correct_answers") or []
        multiple = [str(x).upper() for x in multiple_raw] if isinstance(multiple_raw, list) else []
        numerical = str(
            row.get("_numerical_answer")
            or row.get("numerical_answer")
            or row.get("answer")
            or ""
        ).strip() or None
        if single and not multiple:
            multiple = [single]
        return {
            "single": single,
            "multiple": [x for x in multiple if x],
            "numerical": numerical,
            "tolerance": row.get("numerical_tolerance") or row.get("tolerance"),
        }

    def _extract_inline_options(self, question_text: str) -> tuple[str, list[dict[str, str]]]:
        matches = list(self._OPTION_MARKER_RE.finditer(question_text or ""))
        if len(matches) < 2:
            return question_text, []
        prefix = (question_text[: matches[0].start()] or "").strip()
        if len(prefix) < 8:
            return question_text, []
        options: list[dict[str, str]] = []
        for idx, match in enumerate(matches):
            start = match.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(question_text)
            text = (question_text[start:end] or "").strip(" ;")
            if text:
                options.append({"label": match.group(1).upper(), "text": text})
        if len(options) < 2:
            return question_text, []
        return prefix, options
