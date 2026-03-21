from __future__ import annotations

import copy
import json
import re
from typing import Any


FORBIDDEN_COMMANDS = (
    "write",
    "input",
    "include",
    "catcode",
)

_FORBIDDEN_RE = re.compile(
    r"\\+(?:write\d*|input|include|catcode)(?:\s*\{[^{}]*\})?",
    re.IGNORECASE,
)
_MATH_BLOCK_RE = re.compile(r"(\$\$.*?\$\$|\$.*?\$)", re.DOTALL)
_INLINE_FRACTION_RE = re.compile(
    r"(?<!\\frac\{)\b([A-Za-z0-9]+)\s*/\s*([A-Za-z0-9]+)\b"
)
_BARE_EXP_RE = re.compile(
    r"(?<![$\\])\b([A-Za-z][A-Za-z0-9]*)\s*\^\s*([\-+]?\d+|[A-Za-z])\b"
)
_MATHISH_RE = re.compile(
    r"^[\s0-9A-Za-z\\\^\+\-\*/=\(\)\[\]\{\}\.,:<>|]+$"
)
_HAS_OPERATOR_RE = re.compile(r"[\^\+\-\*/=<>]|\\(?:frac|times|div|sqrt|sum|int)")


class QuestionStructureError(ValueError):
    """Raised when question payload fails structural validation."""


def sanitize_latex(text: str) -> str:
    """
    Sanitize and normalize LaTeX for flutter_math_fork safety.
    """
    if text is None:
        return ""
    cleaned = str(text)

    cleaned = _normalize_ocr_symbols(cleaned)
    cleaned = _remove_forbidden_commands(cleaned)
    cleaned = _fix_odd_dollar_signs(cleaned)
    cleaned = _sanitize_math_segments(cleaned)
    cleaned = _wrap_bare_exponents(cleaned)
    cleaned = _normalize_inline_vs_block_math(cleaned)
    cleaned = _balance_braces(cleaned)
    cleaned = cleaned.strip()

    if not validate_latex(cleaned):
        return _fallback_plain_text(cleaned)
    return cleaned


def validate_latex(text: str) -> bool:
    """
    Lightweight safety/consistency checks for LaTeX text.
    """
    if text is None:
        return True
    value = str(text)
    if _FORBIDDEN_RE.search(value):
        return False
    if not _is_balanced_braces(value):
        return False
    if not _has_even_unescaped_dollars(value):
        return False
    if _has_nested_math_delimiters(value):
        return False
    return True


def validate_question_structure(q: dict, *, student_mode: bool = False) -> None:
    """
    Validate generated/imported question structure before persistence/return.
    """
    if not isinstance(q, dict):
        raise QuestionStructureError("Question must be a dict")

    question_id = _as_text(q.get("question_id"))
    if not question_id:
        raise QuestionStructureError("Missing required field: question_id")

    q_type = _normalize_question_type(q.get("question_type") or q.get("type"))
    question_text = _as_text(q.get("question_text"))
    if not question_text:
        raise QuestionStructureError("question_text cannot be empty")
    if not validate_latex(question_text):
        raise QuestionStructureError("question_text contains malformed LaTeX")

    if "_solution_explanation" not in q:
        raise QuestionStructureError("Missing hidden key: _solution_explanation")

    options = q.get("options")
    normalized_options = _normalize_options(options)

    if q_type == "MCQ_SINGLE":
        if len(normalized_options) < 2:
            raise QuestionStructureError("MCQ_SINGLE requires at least 2 options")
        if not _as_text(q.get("_correct_option")):
            raise QuestionStructureError("Missing hidden key: _correct_option")
    elif q_type == "MCQ_MULTI":
        if len(normalized_options) < 2:
            raise QuestionStructureError("MCQ_MULTI requires at least 2 options")
        raw_multi = _coerce_str_list(q.get("_correct_answers"))
        if not raw_multi:
            raise QuestionStructureError("Missing hidden key: _correct_answers")
    else:
        if normalized_options:
            raise QuestionStructureError("NUMERICAL must not contain options")
        if not _as_text(q.get("_numerical_answer")):
            raise QuestionStructureError("Missing hidden key: _numerical_answer")

    if student_mode:
        visible_keys = (
            "correct_option",
            "correct_answers",
            "correct_answer",
            "numerical_answer",
            "answer",
            "solution_explanation",
        )
        for key in visible_keys:
            value = q.get(key)
            if value is None:
                continue
            if isinstance(value, list) and not value:
                continue
            if _as_text(value):
                raise QuestionStructureError(
                    f"Visible answer key field not allowed in student mode: {key}"
                )


def sanitize_question_payload(
    question: dict[str, Any], *, student_mode: bool = False
) -> dict[str, Any]:
    """
    Sanitize common LaTeX-bearing fields and validate structure.
    """
    if not isinstance(question, dict):
        raise QuestionStructureError("Question payload must be a dict")
    out = copy.deepcopy(question)

    out["question_text"] = sanitize_latex(_as_text(out.get("question_text")))

    opts = out.get("options")
    if isinstance(opts, list):
        sanitized_options: list[Any] = []
        for idx, opt in enumerate(opts):
            if isinstance(opt, dict):
                text = sanitize_latex(_as_text(opt.get("text")))
                label = _as_text(opt.get("label")) or _label_for_index(idx)
                sanitized_options.append({"label": label, "text": text})
            else:
                sanitized_options.append(sanitize_latex(_as_text(opt)))
        out["options"] = sanitized_options
    else:
        out["options"] = []

    out["_solution_explanation"] = sanitize_latex(
        _as_text(out.get("_solution_explanation"))
    )
    if out.get("solution_explanation") is not None:
        out["solution_explanation"] = sanitize_latex(
            _as_text(out.get("solution_explanation"))
        )

    validate_question_structure(out, student_mode=student_mode)
    return out


def _normalize_ocr_symbols(text: str) -> str:
    return (
        text.replace("−", "-")
        .replace("×", r"\times")
        .replace("÷", r"\div")
        .replace("\u00A0", " ")
    )


def _remove_forbidden_commands(text: str) -> str:
    return _FORBIDDEN_RE.sub("", text)


def _fix_odd_dollar_signs(text: str) -> str:
    if _count_unescaped_char(text, "$") % 2 == 0:
        return text
    return f"{text}$"


def _sanitize_math_segments(text: str) -> str:
    if "$" not in text:
        return text

    def repl(match: re.Match[str]) -> str:
        segment = match.group(0)
        if segment.startswith("$$"):
            inner = segment[2:-2]
            inner = _convert_fraction_tokens(inner)
            inner = _balance_braces(inner)
            return f"$${inner}$$"
        inner = segment[1:-1]
        inner = _convert_fraction_tokens(inner)
        inner = _balance_braces(inner)
        return f"${inner}$"

    return _MATH_BLOCK_RE.sub(repl, text)


def _convert_fraction_tokens(text: str) -> str:
    return _INLINE_FRACTION_RE.sub(r"\\frac{\1}{\2}", text)


def _wrap_bare_exponents(text: str) -> str:
    """
    Wrap simple exponent tokens in inline math when they are outside math blocks.
    """
    if "$" not in text:
        return _BARE_EXP_RE.sub(r"$\1^\2$", text)

    pieces: list[str] = []
    last = 0
    for match in _MATH_BLOCK_RE.finditer(text):
        if match.start() > last:
            plain = text[last:match.start()]
            plain = _BARE_EXP_RE.sub(r"$\1^\2$", plain)
            pieces.append(plain)
        pieces.append(match.group(0))
        last = match.end()
    if last < len(text):
        tail = _BARE_EXP_RE.sub(r"$\1^\2$", text[last:])
        pieces.append(tail)
    return "".join(pieces)


def _normalize_inline_vs_block_math(text: str) -> str:
    lines = text.splitlines()
    if not lines:
        return text
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            out.append(line)
            continue
        if "$" in stripped:
            out.append(line)
            continue
        if _looks_like_full_math_line(stripped):
            out.append(f"$${stripped}$$")
        else:
            out.append(line)
    return "\n".join(out)


def _looks_like_full_math_line(line: str) -> bool:
    if len(line) < 3:
        return False
    if not _MATHISH_RE.match(line):
        return False
    if not _HAS_OPERATOR_RE.search(line):
        return False
    words = re.findall(r"[A-Za-z]+", line)
    return len(words) <= 4


def _balance_braces(text: str) -> str:
    out: list[str] = []
    depth = 0
    escaped = False
    for ch in text:
        if escaped:
            out.append(ch)
            escaped = False
            continue
        if ch == "\\":
            out.append(ch)
            escaped = True
            continue
        if ch == "{":
            depth += 1
            out.append(ch)
            continue
        if ch == "}":
            if depth == 0:
                out.append(r"\}")
            else:
                depth -= 1
                out.append(ch)
            continue
        out.append(ch)
    if depth > 0:
        out.extend("}" * depth)
    return "".join(out)


def _fallback_plain_text(text: str) -> str:
    stripped = _FORBIDDEN_RE.sub("", text)
    stripped = stripped.replace("$", "")
    stripped = re.sub(r"\\+", "", stripped)
    stripped = re.sub(r"\s{2,}", " ", stripped)
    return stripped.strip()


def _is_balanced_braces(text: str) -> bool:
    depth = 0
    escaped = False
    for ch in text:
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def _has_even_unescaped_dollars(text: str) -> bool:
    return _count_unescaped_char(text, "$") % 2 == 0


def _count_unescaped_char(text: str, char: str) -> int:
    count = 0
    escaped = False
    for ch in text:
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == char:
            count += 1
    return count


def _has_nested_math_delimiters(text: str) -> bool:
    """
    Reject obvious delimiter nesting patterns like '$...$$...$'.
    """
    in_inline = False
    in_block = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\\":
            i += 2
            continue
        if ch != "$":
            i += 1
            continue
        is_block = i + 1 < len(text) and text[i + 1] == "$"
        if is_block:
            if in_inline:
                return True
            in_block = not in_block
            i += 2
            continue
        if in_block:
            return True
        in_inline = not in_inline
        i += 1
    return in_inline or in_block


def _normalize_question_type(raw: Any) -> str:
    token = _as_text(raw).upper().replace("-", "_").replace(" ", "_")
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
        raise QuestionStructureError(f"Unsupported question type: {token or 'UNKNOWN'}")
    return resolved


def _normalize_options(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    seen_labels: set[str] = set()
    for idx, item in enumerate(raw):
        if isinstance(item, dict):
            label = _as_text(item.get("label")) or _label_for_index(idx)
            text = _as_text(item.get("text"))
        else:
            label = _label_for_index(idx)
            text = _as_text(item)
        label = label.upper()
        if not text:
            raise QuestionStructureError(f"Option {label} cannot be empty")
        if label in seen_labels:
            raise QuestionStructureError(f"Duplicate option label: {label}")
        if not validate_latex(text):
            raise QuestionStructureError(f"Malformed LaTeX in option {label}")
        seen_labels.add(label)
        out.append({"label": label, "text": text})
    return out


def _coerce_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_as_text(x) for x in value if _as_text(x)]
    text = _as_text(value)
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            decoded = json.loads(text)
            if isinstance(decoded, list):
                return [_as_text(x) for x in decoded if _as_text(x)]
        except Exception:
            pass
    return [x.strip() for x in text.split(",") if x.strip()]


def _label_for_index(index: int) -> str:
    if 0 <= index < 26:
        return chr(65 + index)
    return f"O{index + 1}"


def _as_text(value: Any) -> str:
    return str(value or "").strip()
