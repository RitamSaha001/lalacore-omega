from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any


HIDDEN_GRADING_KEYS = (
    "_correct_option",
    "_correct_answers",
    "_numerical_answer",
    "_solution_explanation",
)

_NUMERIC_TOKEN_RE = re.compile(
    r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
)
_DIV_BY_ZERO_RE = re.compile(
    r"^\s*[-+]?(?:\d+(?:\.\d*)?|\.\d+)\s*/\s*0+(?:\.0*)?\s*$"
)


class GradingError(ValueError):
    """Base grading exception."""


class GradingValidationError(GradingError):
    """Raised when question payload is malformed."""


class GradingSecurityError(GradingError):
    """Raised when anti-cheat checks fail."""


def evaluate_attempt(question: dict, student_answer: dict) -> dict:
    """
    Deterministic grading entrypoint.

    Required output:
    {
      "is_correct": bool,
      "score_awarded": float,
      "max_score": float,
      "penalty_applied": float,
      "confidence": float,
      "grading_metadata": dict
    }
    """
    if not isinstance(question, dict):
        raise GradingValidationError("question must be a dict")
    if not isinstance(student_answer, dict):
        raise GradingValidationError("student_answer must be a dict")

    q_type = _normalize_question_type(question.get("question_type") or question.get("type"))
    marking = _extract_marking(question)

    _validate_anti_cheat(question=question, student_answer=student_answer)
    _assert_hidden_fields(question=question, q_type=q_type)

    if q_type == "MCQ_SINGLE":
        result = _grade_mcq_single(
            question=question, student_answer=student_answer, marking=marking
        )
    elif q_type == "MCQ_MULTI":
        result = _grade_mcq_multi(
            question=question, student_answer=student_answer, marking=marking
        )
    else:
        result = _grade_numerical(
            question=question, student_answer=student_answer, marking=marking
        )

    return {
        "is_correct": bool(result["is_correct"]),
        "score_awarded": float(result["score_awarded"]),
        "max_score": float(marking["marks_correct"]),
        "penalty_applied": float(max(0.0, result["penalty_applied"])),
        "confidence": float(result["confidence"]),
        "grading_metadata": dict(result["grading_metadata"]),
    }


def compute_structure_hash(question: dict[str, Any]) -> str:
    """
    Stable checksum for public question structure only.
    """
    payload = {
        "question_id": _first_text(question.get("question_id"), question.get("id")),
        "question_type": _normalize_question_type(
            question.get("question_type") or question.get("type")
        ),
        "question_text": _first_text(
            question.get("question_text"),
            question.get("question"),
            question.get("text"),
        ),
        "options": _extract_options(question),
    }
    return _sha256(payload)


def compute_grading_hash(question: dict[str, Any]) -> str:
    """
    Stable checksum for grading-critical hidden fields and marking config.
    """
    payload = {
        "structure_hash": compute_structure_hash(question),
        "hidden": {
            "_correct_option": _first_text(question.get("_correct_option")).upper(),
            "_correct_answers": _string_list(question.get("_correct_answers")),
            "_numerical_answer": _first_text(question.get("_numerical_answer")),
            "_solution_explanation": _first_text(question.get("_solution_explanation")),
        },
        "marking": _extract_marking(question),
    }
    return _sha256(payload)


def _grade_mcq_single(
    *,
    question: dict[str, Any],
    student_answer: dict[str, Any],
    marking: dict[str, float | bool],
) -> dict[str, Any]:
    options = _extract_options(question)
    labels = _labels_for_options(options)
    correct_label = _normalize_option_token(
        _first_text(question.get("_correct_option")),
        options=options,
        labels=labels,
    )
    if correct_label not in set(labels):
        raise GradingValidationError("Invalid _correct_option for MCQ_SINGLE")

    raw_answers = _extract_student_tokens(student_answer)
    first = raw_answers[0] if raw_answers else ""
    student_label = _normalize_option_token(first, options=options, labels=labels)
    answered = bool(_first_text(first))
    is_correct = answered and student_label == correct_label

    if not answered:
        score = float(marking["marks_unattempted"])
    elif is_correct:
        score = float(marking["marks_correct"])
    else:
        score = float(marking["marks_incorrect"])

    penalty = abs(score) if answered and not is_correct and score < 0 else 0.0
    return {
        "is_correct": is_correct,
        "score_awarded": score,
        "penalty_applied": penalty,
        "confidence": 1.0 if is_correct else 0.0,
        "grading_metadata": {
            "question_type": "MCQ_SINGLE",
            "answered": answered,
            "student_choice": student_label,
            "correct_choice": correct_label,
            "valid_choice": student_label in set(labels),
        },
    }


def _grade_mcq_multi(
    *,
    question: dict[str, Any],
    student_answer: dict[str, Any],
    marking: dict[str, float | bool],
) -> dict[str, Any]:
    options = _extract_options(question)
    labels = _labels_for_options(options)
    correct_set = _normalize_correct_set(question=question, options=options, labels=labels)
    if not correct_set:
        raise GradingValidationError("Invalid _correct_answers for MCQ_MULTI")

    raw_answers = _extract_student_tokens(student_answer)
    student_set, invalid_count = _normalize_student_set(
        raw_answers, options=options, labels=labels
    )
    answered = bool(raw_answers)

    intersection = student_set & correct_set
    missing = correct_set - student_set
    wrong = student_set - correct_set
    incorrect_count = len(wrong) + invalid_count
    exact_match = answered and (not missing) and incorrect_count == 0

    marks_correct = float(marking["marks_correct"])
    marks_incorrect = float(marking["marks_incorrect"])
    marks_unattempted = float(marking["marks_unattempted"])
    partial_marking = bool(marking["partial_marking"])
    strict_mode = (
        _to_bool(question.get("strict_multi_mode"))
        or _to_bool(question.get("strict_mode"))
        or (not partial_marking)
    )

    penalty = 0.0
    partial = False
    if not answered:
        score = marks_unattempted
    elif exact_match:
        score = marks_correct
    elif partial_marking:
        fraction = len(intersection) / max(1, len(correct_set))
        base_score = fraction * marks_correct
        if incorrect_count > 0:
            if strict_mode:
                score = marks_incorrect
                penalty = abs(min(0.0, marks_incorrect))
            else:
                per_wrong_penalty = abs(marks_incorrect) / max(1, len(correct_set))
                penalty = incorrect_count * per_wrong_penalty
                score = base_score - penalty
                if score < marks_incorrect:
                    score = marks_incorrect
            partial = score > 0.0 and len(intersection) > 0
        else:
            score = base_score
            partial = score > 0.0 and len(intersection) > 0
    else:
        score = marks_incorrect
        penalty = abs(min(0.0, marks_incorrect))

    if score < marks_incorrect:
        score = marks_incorrect

    confidence = 1.0 if exact_match else (0.5 if partial else 0.0)
    return {
        "is_correct": exact_match,
        "score_awarded": float(score),
        "penalty_applied": float(penalty),
        "confidence": float(confidence),
        "grading_metadata": {
            "question_type": "MCQ_MULTI",
            "answered": answered,
            "student_choices": sorted(student_set),
            "correct_choices": sorted(correct_set),
            "correct_count": len(intersection),
            "incorrect_count": incorrect_count,
            "missing_count": len(missing),
            "strict_mode": strict_mode,
            "partial_awarded": partial,
        },
    }


def _grade_numerical(
    *,
    question: dict[str, Any],
    student_answer: dict[str, Any],
    marking: dict[str, float | bool],
) -> dict[str, Any]:
    correct_value = _safe_float(question.get("_numerical_answer"))
    if correct_value is None:
        raise GradingValidationError("Invalid _numerical_answer for NUMERICAL")

    raw_answers = _extract_student_tokens(student_answer)
    first = raw_answers[0] if raw_answers else ""
    answered = bool(_first_text(first))
    tolerance = max(0.0, _to_float(question.get("numerical_tolerance"), 0.001))

    student_value = _safe_float(first) if answered else None
    valid_numeric = student_value is not None
    exact = False
    abs_error = None

    if not answered:
        score = float(marking["marks_unattempted"])
    elif not valid_numeric:
        score = float(marking["marks_incorrect"])
    else:
        abs_error = abs(student_value - correct_value)
        exact = abs_error <= tolerance
        score = float(marking["marks_correct"] if exact else marking["marks_incorrect"])

    penalty = abs(score) if answered and not exact and score < 0 else 0.0
    return {
        "is_correct": exact,
        "score_awarded": float(score),
        "penalty_applied": float(penalty),
        "confidence": 1.0 if exact else 0.0,
        "grading_metadata": {
            "question_type": "NUMERICAL",
            "answered": answered,
            "valid_numeric": valid_numeric,
            "student_value": student_value,
            "correct_value": correct_value,
            "tolerance": tolerance,
            "abs_error": abs_error,
        },
    }


def _validate_anti_cheat(
    *,
    question: dict[str, Any],
    student_answer: dict[str, Any],
) -> None:
    question_id = _first_text(question.get("question_id"), question.get("id"))
    answer_qid = _first_text(student_answer.get("question_id"), student_answer.get("id"))
    if question_id and answer_qid and question_id != answer_qid:
        raise GradingSecurityError("question_id mismatch in attempt payload")

    for key in HIDDEN_GRADING_KEYS:
        if key in student_answer and not _json_equal(
            student_answer.get(key), question.get(key)
        ):
            raise GradingSecurityError(f"Student attempt modified protected key: {key}")

    structure_hash = _first_text(question.get("structure_hash"))
    if structure_hash:
        expected = compute_structure_hash(question)
        if structure_hash != expected:
            raise GradingSecurityError("Question structure_hash mismatch")
        incoming = _first_text(student_answer.get("structure_hash"))
        if incoming and incoming != structure_hash:
            raise GradingSecurityError("Attempt structure hash mismatch")

    grading_hash = _first_text(question.get("grading_hash"))
    if grading_hash:
        expected = compute_grading_hash(question)
        if grading_hash != expected:
            raise GradingSecurityError("Question grading_hash mismatch")
        incoming = _first_text(student_answer.get("grading_hash"))
        if incoming and incoming != grading_hash:
            raise GradingSecurityError("Attempt grading hash mismatch")


def _assert_hidden_fields(*, question: dict[str, Any], q_type: str) -> None:
    for key in HIDDEN_GRADING_KEYS:
        if key not in question:
            raise GradingValidationError(f"Missing required hidden key: {key}")

    if q_type == "MCQ_SINGLE":
        if not _first_text(question.get("_correct_option")):
            raise GradingValidationError("Missing required hidden key: _correct_option")
    elif q_type == "MCQ_MULTI":
        answers = _string_list(question.get("_correct_answers"))
        if not answers:
            raise GradingValidationError("Missing required hidden key: _correct_answers")
    elif q_type == "NUMERICAL":
        if not _first_text(question.get("_numerical_answer")):
            raise GradingValidationError("Missing required hidden key: _numerical_answer")
    else:
        raise GradingValidationError(f"Unsupported question_type: {q_type}")


def _normalize_correct_set(
    *,
    question: dict[str, Any],
    options: list[str],
    labels: list[str],
) -> set[str]:
    raw_answers = _string_list(question.get("_correct_answers"))
    out: set[str] = set()
    for raw in raw_answers:
        normalized = _normalize_option_token(raw, options=options, labels=labels)
        if normalized:
            out.add(normalized)
    if not out:
        fallback = _normalize_option_token(
            _first_text(question.get("_correct_option")),
            options=options,
            labels=labels,
        )
        if fallback:
            out.add(fallback)
    return out


def _normalize_student_set(
    values: list[str],
    *,
    options: list[str],
    labels: list[str],
) -> tuple[set[str], int]:
    out: set[str] = set()
    invalid = 0
    for raw in values:
        value = _first_text(raw)
        if not value:
            continue
        normalized = _normalize_option_token(value, options=options, labels=labels)
        if normalized:
            out.add(normalized)
        else:
            invalid += 1
    return out, invalid


def _normalize_option_token(value: Any, *, options: list[str], labels: list[str]) -> str:
    token = _first_text(value).strip()
    if not token:
        return ""

    upper = token.upper()
    label_set = set(labels)

    if upper in label_set:
        return upper
    if len(upper) >= 2 and upper[0] in label_set and upper[1] in {")", ".", ":"}:
        return upper[0]
    if upper.isdigit():
        idx = int(upper) - 1
        if 0 <= idx < len(labels):
            return labels[idx]

    for idx, option in enumerate(options):
        if option.lower() == token.lower():
            return labels[idx]
    return ""


def _labels_for_options(options: list[str]) -> list[str]:
    if not options:
        return ["A", "B", "C", "D"]
    labels: list[str] = []
    for idx in range(len(options)):
        if idx < 26:
            labels.append(chr(65 + idx))
        else:
            labels.append(f"O{idx + 1}")
    return labels


def _extract_options(question: dict[str, Any]) -> list[str]:
    raw = question.get("options")
    if isinstance(raw, list):
        out: list[str] = []
        for item in raw:
            if isinstance(item, dict):
                out.append(_first_text(item.get("text"), item.get("value")))
            else:
                out.append(_first_text(item))
        return [x for x in out if x]
    return []


def _extract_student_tokens(student_answer: dict[str, Any]) -> list[str]:
    for key in (
        "selected_options",
        "answers",
        "answer",
        "selected",
        "response",
        "value",
        "student_answer",
    ):
        if key not in student_answer:
            continue
        raw = student_answer.get(key)
        if isinstance(raw, list):
            return [_first_text(item) for item in raw if _first_text(item)]
        if raw is None:
            return []
        text = _first_text(raw)
        if not text:
            return []
        if "," in text:
            return [chunk.strip() for chunk in text.split(",") if chunk.strip()]
        return [text]
    return []


def _extract_marking(question: dict[str, Any]) -> dict[str, float | bool]:
    return {
        "marks_correct": _to_float(question.get("marks_correct"), 4.0),
        "marks_incorrect": _to_float(question.get("marks_incorrect"), -1.0),
        "marks_unattempted": _to_float(question.get("marks_unattempted"), 0.0),
        "partial_marking": _to_bool(question.get("partial_marking")),
        "numerical_tolerance": max(
            0.0, _to_float(question.get("numerical_tolerance"), 0.001)
        ),
    }


def _normalize_question_type(raw: Any) -> str:
    token = _first_text(raw).upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "MCQ": "MCQ_SINGLE",
        "MCQ_SINGLE": "MCQ_SINGLE",
        "SINGLE": "MCQ_SINGLE",
        "MCQ_MULTI": "MCQ_MULTI",
        "MULTI": "MCQ_MULTI",
        "MULTIPLE": "MCQ_MULTI",
        "NUMERICAL": "NUMERICAL",
        "NUMERIC": "NUMERICAL",
        "INTEGER": "NUMERICAL",
    }
    resolved = aliases.get(token)
    if resolved is None:
        raise GradingValidationError(f"Unsupported question_type: {token or 'UNKNOWN'}")
    return resolved


def _safe_float(raw: Any) -> float | None:
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
        if math.isfinite(value):
            return value
        return None

    text = _first_text(raw)
    if not text:
        return None
    compact = text.replace(",", "").replace("−", "-").strip()
    if _DIV_BY_ZERO_RE.match(compact):
        return None
    if compact.lower() in {"nan", "+nan", "-nan", "inf", "+inf", "-inf", "infinity"}:
        return None
    match = _NUMERIC_TOKEN_RE.search(compact)
    if not match:
        return None

    token = match.group(0)
    try:
        value = float(token)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return value


def _string_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [_first_text(item) for item in raw if _first_text(item)]
    text = _first_text(raw)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            decoded = json.loads(text)
            if isinstance(decoded, list):
                return [_first_text(item) for item in decoded if _first_text(item)]
        except Exception:
            pass
    if "," in text:
        return [chunk.strip() for chunk in text.split(",") if chunk.strip()]
    return [text]


def _to_float(raw: Any, fallback: float) -> float:
    try:
        if isinstance(raw, bool):
            return 1.0 if raw else 0.0
        if isinstance(raw, (int, float)):
            return float(raw)
        text = _first_text(raw)
        return float(text) if text else float(fallback)
    except Exception:
        return float(fallback)


def _to_bool(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    return _first_text(raw).lower() in {"1", "true", "yes", "y", "on"}


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _json_equal(left: Any, right: Any) -> bool:
    try:
        return json.dumps(left, sort_keys=True, ensure_ascii=True) == json.dumps(
            right, sort_keys=True, ensure_ascii=True
        )
    except Exception:
        return left == right
