#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from sympy import sympify
from sympy.core.sympify import SympifyError

from app.data.local_app_data_service import LocalAppDataService
from app.data.question_repair_engine import QuestionRepairEngine


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _to_float(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(fallback)


def _to_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(fallback)


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError(f"input is not a JSON list: {path}")
    return [row for row in rows if isinstance(row, dict)]


def _normalize_option_list(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                label = _to_str(item.get("label")).upper() or chr(65 + min(idx, 25))
                text = _to_str(item.get("text") or item.get("value") or item.get("option")).strip()
            else:
                label = chr(65 + min(idx, 25))
                text = _to_str(item).strip()
            if text:
                out.append({"label": label, "text": text})
    elif isinstance(raw, dict):
        items = sorted(raw.items(), key=lambda kv: _to_str(kv[0]).upper())
        for idx, (_, value) in enumerate(items):
            text = _to_str(value).strip()
            if not text:
                continue
            out.append({"label": chr(65 + min(idx, 25)), "text": text})
    return out


def _normalize_correct_answer(row: dict[str, Any]) -> dict[str, Any]:
    ans = row.get("correct_answer")
    if isinstance(ans, dict):
        multiple = ans.get("multiple")
        if not isinstance(multiple, list):
            multiple = []
        return {
            "single": (_to_str(ans.get("single")).upper() or None),
            "multiple": [_to_str(x).upper() for x in multiple if _to_str(x).strip()],
            "numerical": (_to_str(ans.get("numerical")).strip() or None),
            "tolerance": ans.get("tolerance"),
        }
    return {
        "single": (_to_str(row.get("_correct_option") or row.get("correct_option")).upper() or None),
        "multiple": [],
        "numerical": (_to_str(row.get("_numerical_answer") or row.get("numerical_answer")).strip() or None),
        "tolerance": row.get("numerical_tolerance") or row.get("tolerance"),
    }


def _looks_heavily_corrupted(text: str) -> bool:
    low = _to_str(text).lower()
    if not low.strip():
        return True
    if len(low.strip()) < 14:
        return True
    corruption_tokens = (
        "xxfx",
        "()'rf",
        "()'lf",
        " +<== +>",
        "www.allen.in",
        "answer key",
        " cd0",
    )
    if any(tok in low for tok in corruption_tokens):
        return True
    if re.search(r"\b\d+\s+\d+\s+for\s+\d+\b", low):
        return True
    if re.search(r"[=+\-*/^]\s*$", low):
        return True
    return False


def _score_integrity(
    *,
    repair_confidence: float,
    question_text: str,
    options: list[dict[str, str]],
    issues: list[str],
) -> float:
    score = max(0.0, min(1.0, float(repair_confidence)))
    penalties = {
        "empty_question": 0.35,
        "unbalanced_brackets": 0.22,
        "dangling_operator": 0.16,
        "expression_parse_failure": 0.14,
        "missing_options": 0.12,
        "answer_mismatch": 0.18,
        "invalid_token_sequences": 0.10,
    }
    for issue in issues:
        score -= penalties.get(issue, 0.03)
    if len(_to_str(question_text)) < 16:
        score -= 0.25
    if options and len(options) < 2:
        score -= 0.12
    if _looks_heavily_corrupted(question_text):
        score -= 0.20
    return round(max(0.0, min(1.0, score)), 4)


def _status_from_integrity(integrity: float) -> str:
    if integrity >= 0.9:
        return "safe"
    if integrity >= 0.75:
        return "review"
    if integrity >= 0.5:
        return "reject"
    return "unrecoverable"


def _extract_first_json_object(raw: str) -> dict[str, Any] | None:
    text = _to_str(raw).strip()
    if not text:
        return None
    if text.startswith("{"):
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        return None
    try:
        obj = json.loads(match.group(0))
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _coerce_repair_payload(obj: dict[str, Any]) -> dict[str, Any] | None:
    required = (
        "question_text",
        "options",
        "correct_answer",
        "question_text_clean",
        "options_clean",
        "question_text_latex",
        "repair_confidence",
        "unrecoverable_flag",
    )
    if any(key in obj for key in required):
        return obj
    for key in ("data", "result", "output", "response", "repaired"):
        nested = obj.get(key)
        if isinstance(nested, dict) and any(field in nested for field in required):
            return nested
    return None


def _has_balanced_brackets(text: str) -> bool:
    pairs = {")": "(", "]": "[", "}": "{"}
    stack: list[str] = []
    for ch in _to_str(text):
        if ch in "([{":
            stack.append(ch)
        elif ch in ")]}":
            if not stack or stack[-1] != pairs[ch]:
                return False
            stack.pop()
    return not stack


def _sanitize_ocr_junk(text: str) -> str:
    out = _to_str(text)
    if not out:
        return ""
    out = re.sub(r"\b(?:CD|IF|FN|SR|LT)\d{2}-\d{4}\b", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\bwww\.[^\s]+\b", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\b(?:EXERCISE|ANSWER\s*KEY)\b", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s{2,}", " ", out)
    return out.strip()


_TOKEN_REPAIR_RULES: list[tuple[str, str]] = [
    (r"\bxxfx\b", "f(x)"),
    (r"\bfx\b", "f(x)"),
    (r"\bsgn\s*x\b", "sgn(x)"),
    (r"\bsgnx\b", "sgn(x)"),
    (r"\bxa\b", "x→a"),
    (r"<==|<=<|-><|-<=>|-\s*<=>", "≤"),
    (r"=>|>=<|=><|-\s*=>", "≥"),
    (r"\s*-\s*>\s*", "→"),
]


def _token_repair_text(text: str) -> tuple[str, list[str]]:
    out = _sanitize_ocr_junk(text)
    actions: list[str] = []
    for pattern, repl in _TOKEN_REPAIR_RULES:
        nxt = re.sub(pattern, repl, out, flags=re.IGNORECASE)
        if nxt != out:
            out = nxt
            actions.append("layer4_l1_token_repair")
    # Compact repeated delimiters and whitespace damage.
    out2 = re.sub(r"\s{2,}", " ", out).strip()
    if out2 != out:
        out = out2
        actions.append("layer4_l1_whitespace_normalize")
    return out, list(dict.fromkeys(actions))


def _repair_option_tokens(options: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[str]]:
    repaired: list[dict[str, str]] = []
    actions: list[str] = []
    for opt in options:
        label = _to_str(opt.get("label")).upper() or "A"
        text, local_actions = _token_repair_text(_to_str(opt.get("text")))
        if local_actions:
            actions.extend(local_actions)
        if text:
            repaired.append({"label": label, "text": text})
    return repaired, list(dict.fromkeys(actions))


def _extract_math_fragments(text: str, *, max_items: int = 8) -> list[str]:
    src = _to_str(text)
    if not src:
        return []
    patterns = (
        r"lim[^.;\n]{4,160}",
        r"[a-zA-Z]\s*\([^)]+\)\s*=\s*[^.;\n]{2,160}",
        r"\|[^|]{1,100}\|",
        r"\[[^\]]{1,100}\]",
        r"\{[^{}]{1,120}\}",
        r"[a-zA-Z0-9\)\]]\s*[\+\-\*/\^]\s*[a-zA-Z0-9\(\[]",
    )
    out: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, src, flags=re.IGNORECASE):
            frag = _to_str(match.group(0))
            if frag and frag not in out:
                out.append(frag[:180])
            if len(out) >= max_items:
                return out
    return out


def _sympy_fragment_parse_ok(fragment: str) -> bool:
    frag = _to_str(fragment)
    if not frag:
        return False
    low = frag.lower()
    if "lim" in low:
        # Keep limit checks deterministic; sympy parsing of textual limit OCR is noisy.
        return bool(re.search(r"\blim\b", low) and ("→" in frag or "->" in frag or r"\to" in low))
    expr = frag
    expr = expr.replace("^", "**")
    expr = expr.replace("≤", "<=").replace("≥", ">=").replace("→", "->")
    expr = re.sub(r"\bsgn\s*\(", "sign(", expr, flags=re.IGNORECASE)
    expr = re.sub(r"(?<=\d)(?=[A-Za-z])", "*", expr)
    expr = re.sub(r"(?<=[A-Za-z])(?=\d)", "*", expr)
    expr = expr.strip()
    if "=" in expr:
        left, right = expr.split("=", 1)
        try:
            sympify(left.strip())
            sympify(right.strip())
            return True
        except (SympifyError, SyntaxError, TypeError, ValueError):
            return False
    try:
        sympify(expr)
        return True
    except (SympifyError, SyntaxError, TypeError, ValueError):
        return False


def _ast_grammar_validation(text: str, options: list[dict[str, str]]) -> dict[str, Any]:
    issues: list[str] = []
    syntax_score = 1.0
    structure_score = 1.0
    src = _to_str(text)
    low = src.lower()

    if not _has_balanced_brackets(src):
        issues.append("unbalanced_brackets")
        syntax_score -= 0.30
    if re.search(r"^[=+\-*/^,:;]|[=+\-*/^,:;]\s*$", src.strip()):
        issues.append("dangling_operator")
        syntax_score -= 0.24
    if re.search(r"\b(?:if|then|let|hence)\s*$", low):
        issues.append("truncated_question")
        structure_score -= 0.35
    if re.search(r"\blim\b", low) and not ("→" in src or "->" in src or r"\to" in low):
        issues.append("broken_limit_expression")
        structure_score -= 0.22

    piecewise_like = src.count("{") > 0 or bool(re.search(r"\bfor\b", low))
    if piecewise_like:
        branch_hits = len(re.findall(r"(?:≤|>=|>=|<=|<|>)", src))
        if branch_hits < 2:
            issues.append("corrupted_piecewise")
            structure_score -= 0.26

    frags = _extract_math_fragments(src)
    ast_ok = 0
    for frag in frags:
        if _sympy_fragment_parse_ok(frag):
            ast_ok += 1
    if frags:
        ratio = ast_ok / max(1, len(frags))
        if ratio < 0.5:
            issues.append("expression_parse_failure")
            syntax_score -= 0.24
        elif ratio < 0.8:
            syntax_score -= 0.12

    if options and len(options) < 2:
        issues.append("missing_options")
        structure_score -= 0.18

    return {
        "issues": list(dict.fromkeys(issues)),
        "syntax_score": round(max(0.0, min(1.0, syntax_score)), 4),
        "structure_score": round(max(0.0, min(1.0, structure_score)), 4),
        "ast_fragment_count": len(frags),
        "ast_valid_count": ast_ok,
    }


def _mathematical_validator(text: str, options: list[dict[str, str]]) -> dict[str, Any]:
    src = _to_str(text)
    low = src.lower()
    issues: list[str] = []
    score = 1.0
    hard_reject = False

    if re.search(r"\bif\s*$", low) or re.search(r"\bfor all[^.?!]*\bif\s*$", low):
        issues.append("truncated_question")
        score -= 0.45
        hard_reject = True
    digits_only = re.sub(r"\s+", " ", src).strip()
    if digits_only and re.fullmatch(r"[0-9 .,\-]+", digits_only):
        issues.append("garbage_numeric_blob")
        score -= 0.55
        hard_reject = True
    if "sgn" in low and re.search(r"f'?[\s_]*\(?0\)?\s*=\s*1", low):
        issues.append("impossible_math_claim")
        score -= 0.35

    normalized_opts = [re.sub(r"\s+", " ", _to_str(o.get("text")).lower()).strip() for o in options]
    normalized_opts = [x for x in normalized_opts if x]
    if len(set(normalized_opts)) < len(normalized_opts):
        issues.append("duplicate_options")
        score -= 0.12

    return {
        "issues": list(dict.fromkeys(issues)),
        "math_validity_score": round(max(0.0, min(1.0, score)), 4),
        "hard_reject": hard_reject,
    }


def _confidence_engine(
    *,
    syntax_score: float,
    structure_score: float,
    math_validity_score: float,
    ai_certainty: float,
) -> float:
    confidence = (
        0.4 * max(0.0, min(1.0, syntax_score))
        + 0.3 * max(0.0, min(1.0, structure_score))
        + 0.2 * max(0.0, min(1.0, math_validity_score))
        + 0.1 * max(0.0, min(1.0, ai_certainty))
    )
    return round(max(0.0, min(1.0, confidence)), 4)


def _detected_structural_issues(text: str, options: list[dict[str, str]]) -> list[str]:
    issues: list[str] = []
    low = _to_str(text).lower()
    if not _to_str(text).strip():
        issues.append("empty_question")
    if not _has_balanced_brackets(text):
        issues.append("unbalanced_brackets")
    if re.search(r"[=+\-*/^,:;]\s*$", _to_str(text)):
        issues.append("dangling_operator")
    if re.search(r"\b(?:xxfx|x\s*x\s*f\s*\(|\+<==\+>|=><|<==)\b", low):
        issues.append("invalid_token_sequences")
    if re.search(r"\blim\b", low) and ("→" not in low and "->" not in low and "to" not in low):
        issues.append("broken_limit_expression")
    if options and len(options) < 2:
        issues.append("missing_options")
    return list(dict.fromkeys(issues))


def _normalize_ai_payload_fields(
    *,
    ai_obj: dict[str, Any],
    fallback_question_text: str,
    fallback_options: list[dict[str, str]],
    fallback_correct_answer: dict[str, Any],
) -> dict[str, Any]:
    qtext = _to_str(
        ai_obj.get("question_text")
        or ai_obj.get("question_text_clean")
        or fallback_question_text
    ).strip()
    qtext, _ = _token_repair_text(qtext)
    qtext_latex = _to_str(ai_obj.get("question_text_latex")).strip()

    raw_options = ai_obj.get("options")
    if raw_options is None:
        raw_options = ai_obj.get("options_clean")
    opts = _normalize_option_list(raw_options)
    if not opts:
        opts = list(fallback_options)
    opts, _ = _repair_option_tokens(opts)

    ans_raw = ai_obj.get("correct_answer")
    if not isinstance(ans_raw, dict):
        ans_raw = {}
    single = _to_str(ans_raw.get("single")).upper() or None
    multiple = [_to_str(x).upper() for x in (ans_raw.get("multiple") or []) if _to_str(x).strip()]
    numerical = _to_str(ans_raw.get("numerical")).strip() or None
    correct_answer = {
        "single": single or _to_str(fallback_correct_answer.get("single")).upper() or None,
        "multiple": multiple or list(fallback_correct_answer.get("multiple") or []),
        "numerical": numerical or _to_str(fallback_correct_answer.get("numerical")).strip() or None,
        "tolerance": ans_raw.get("tolerance", fallback_correct_answer.get("tolerance")),
    }

    detected_errors = ai_obj.get("detected_errors")
    if isinstance(detected_errors, list):
        detected = [_to_str(x).strip() for x in detected_errors if _to_str(x).strip()]
    else:
        detected = []
    detected.extend(_detected_structural_issues(qtext, opts))
    detected = list(dict.fromkeys(detected))

    ai_conf = _to_float(ai_obj.get("repair_confidence"), -1.0)
    if ai_conf < 0.0:
        ai_conf = None
    else:
        ai_conf = max(0.0, min(1.0, ai_conf))
    unrecoverable_flag = bool(ai_obj.get("unrecoverable_flag"))

    return {
        "question_text": qtext,
        "question_text_latex": qtext_latex,
        "options": opts,
        "options_latex": ai_obj.get("options_latex"),
        "correct_answer": correct_answer,
        "detected_errors": detected,
        "repair_confidence": ai_conf,
        "unrecoverable_flag": unrecoverable_flag,
    }


def _extract_ai_repair_object(result: dict[str, Any]) -> dict[str, Any] | None:
    raw_payload = result.get("raw")
    if isinstance(raw_payload, dict):
        structured = raw_payload.get("structured_output")
        if isinstance(structured, dict):
            coerced = _coerce_repair_payload(structured)
            if isinstance(coerced, dict):
                return coerced

    candidate_texts: list[str] = []
    for key in ("answer", "explanation"):
        token = _to_str(result.get(key))
        if token:
            candidate_texts.append(token)

    if isinstance(raw_payload, dict):
        for key in ("final_answer", "answer", "display_answer", "reasoning", "explanation"):
            token = _to_str(raw_payload.get(key))
            if token:
                candidate_texts.append(token)

    for candidate in candidate_texts:
        if not candidate:
            continue
        obj = _extract_first_json_object(candidate)
        if isinstance(obj, dict):
            coerced = _coerce_repair_payload(obj)
            if isinstance(coerced, dict):
                return coerced
        if "Uncertain answer" in candidate and "{" not in candidate:
            continue
    return None


def _split_env_keys(*names: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for name in names:
        raw = _to_str(os.getenv(name))
        if not raw:
            continue
        for chunk in raw.split(","):
            token = _to_str(chunk)
            if not token or token in seen:
                continue
            seen.add(token)
            out.append(token)
    return out


def _coerce_json_candidate(raw: str) -> dict[str, Any] | None:
    obj = _extract_first_json_object(raw)
    if isinstance(obj, dict):
        return _coerce_repair_payload(obj)
    return None


async def _direct_openrouter_json_repair(
    *,
    prompt: str,
    timeout_s: float,
) -> dict[str, Any] | None:
    keys = _split_env_keys("OPENROUTER_API_KEY", "OPENROUTER_KEYS")
    if not keys:
        return None
    configured = [m.strip() for m in _to_str(os.getenv("OPENROUTER_FALLBACK_MODELS")).split(",") if m.strip()]
    models = [
        _to_str(os.getenv("OPENROUTER_MODEL")) or "openai/gpt-4o-mini",
        "google/gemini-2.0-flash-001",
        "meta-llama/llama-3.1-70b-instruct",
        *configured,
    ]
    seen_models: set[str] = set()
    ordered_models: list[str] = []
    for model in models:
        if not model or model in seen_models:
            continue
        seen_models.add(model)
        ordered_models.append(model)
    for key in keys:
        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost",
            "X-Title": "LalaCore-Omega-Layer4",
        }
        for model in ordered_models:
            body = {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a MATHEMATICAL DATASET REPAIR ENGINE for OCR-damaged JEE questions. "
                            "Do deterministic reconstruction, not solving. "
                            "Return only one JSON object with keys: "
                            "question_text_clean, question_text_latex, options_clean, options_latex, "
                            "detected_errors, repair_confidence, unrecoverable_flag, correct_answer. "
                            "Never hallucinate new data; preserve mathematical meaning and answers unless OCR clearly corrupted."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.0,
                "max_tokens": 1400,
                "response_format": {"type": "json_object"},
            }
            try:
                async with httpx.AsyncClient(timeout=max(8.0, float(timeout_s))) as client:
                    resp = await client.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers=headers,
                        json=body,
                    )
                if resp.status_code != 200:
                    continue
                payload = resp.json()
                content = _to_str(
                    (((payload.get("choices") or [{}])[0].get("message") or {}).get("content"))
                )
                obj = _coerce_json_candidate(content)
                if isinstance(obj, dict):
                    obj["_layer4_ai_source"] = "direct_openrouter"
                    obj["_layer4_ai_model"] = model
                    return obj
            except Exception:
                continue
    return None


async def _direct_groq_json_repair(
    *,
    prompt: str,
    timeout_s: float,
) -> dict[str, Any] | None:
    keys = _split_env_keys("GROQ_KEY", "GROQ_KEYS")
    if not keys:
        return None
    model = _to_str(os.getenv("GROQ_MODEL")) or "llama-3.3-70b-versatile"
    for key in keys:
        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You repair OCR-damaged JEE math text. Return only strict JSON object with keys "
                        "question_text_clean, question_text_latex, options_clean, options_latex, "
                        "detected_errors, repair_confidence, unrecoverable_flag, correct_answer."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
            "max_tokens": 1400,
            "response_format": {"type": "json_object"},
        }
        try:
            async with httpx.AsyncClient(timeout=max(8.0, float(timeout_s))) as client:
                resp = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers=headers,
                    json=body,
                )
            if resp.status_code != 200:
                continue
            payload = resp.json()
            content = _to_str(
                (((payload.get("choices") or [{}])[0].get("message") or {}).get("content"))
            )
            obj = _coerce_json_candidate(content)
            if isinstance(obj, dict):
                obj["_layer4_ai_source"] = "direct_groq"
                obj["_layer4_ai_model"] = model
                return obj
        except Exception:
            continue
    return None


async def _direct_gemini_json_repair(
    *,
    prompt: str,
    timeout_s: float,
) -> dict[str, Any] | None:
    keys = _split_env_keys("GEMINI_KEYS", "GEMINI_KEY")
    if not keys:
        return None
    model = (_to_str(os.getenv("GEMINI_MODEL")) or "gemini-2.5-flash-lite").replace("models/", "")
    for key in keys:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
        body = {
            "system_instruction": {
                "parts": [
                    {
                        "text": (
                            "You repair OCR-damaged JEE math text. Return only strict JSON object with keys "
                            "question_text_clean, question_text_latex, options_clean, options_latex, "
                            "detected_errors, repair_confidence, unrecoverable_flag, correct_answer."
                        )
                    }
                ]
            },
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": 1400,
                "responseMimeType": "application/json",
            },
        }
        try:
            async with httpx.AsyncClient(timeout=max(8.0, float(timeout_s))) as client:
                resp = await client.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json=body,
                )
            if resp.status_code != 200:
                continue
            payload = resp.json()
            content = _to_str(
                ((((payload.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [{}])[0].get("text"))
            )
            obj = _coerce_json_candidate(content)
            if isinstance(obj, dict):
                obj["_layer4_ai_source"] = "direct_gemini"
                obj["_layer4_ai_model"] = model
                return obj
        except Exception:
            continue
    return None


async def _direct_provider_json_repair(
    *,
    prompt: str,
    timeout_s: float,
) -> dict[str, Any] | None:
    for fn in (
        _direct_openrouter_json_repair,
        _direct_groq_json_repair,
        _direct_gemini_json_repair,
    ):
        obj = await fn(prompt=prompt, timeout_s=timeout_s)
        if isinstance(obj, dict):
            return obj
    return None


def _salvage_severity(row: dict[str, Any]) -> float:
    status = _to_str(row.get("repair_status")).lower()
    conf = _to_float(row.get("repair_confidence"), 0.0)
    integrity = _to_float(row.get("structural_integrity_score"), 0.0)
    qtext = _to_str(row.get("question_text") or row.get("question"))
    sev = (1.0 - conf) + (1.0 - integrity)
    if status == "unrecoverable":
        sev += 1.2
    elif status == "reject":
        sev += 0.8
    elif status == "review":
        sev += 0.35
    if _looks_heavily_corrupted(qtext):
        sev += 0.5
    return sev


def _norm_for_similarity(text: str) -> str:
    out = _to_str(text).lower()
    out = re.sub(r"\s+", " ", out)
    return out.strip()


def _char_trigram_cosine(a: str, b: str) -> float:
    def _vec(text: str) -> dict[str, int]:
        t = re.sub(r"[^a-z0-9]+", "", text.lower())
        if len(t) < 3:
            return {}
        out: dict[str, int] = {}
        for i in range(len(t) - 2):
            tri = t[i : i + 3]
            out[tri] = out.get(tri, 0) + 1
        return out

    va = _vec(a)
    vb = _vec(b)
    if not va or not vb:
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for k, v in va.items():
        na += float(v * v)
        dot += float(v * vb.get(k, 0))
    for v in vb.values():
        nb += float(v * v)
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return float(dot / ((na ** 0.5) * (nb ** 0.5)))


def _estimate_ai_tokens(
    *,
    question_text: str,
    options: list[dict[str, str]],
    correct_answer: dict[str, Any],
) -> int:
    option_blob = " ".join(_to_str(o.get("text")) for o in options[:6])
    answer_blob = json.dumps(
        {
            "single": _to_str(correct_answer.get("single")),
            "multiple": [_to_str(x) for x in (correct_answer.get("multiple") or [])],
            "numerical": _to_str(correct_answer.get("numerical")),
        },
        ensure_ascii=False,
    )
    chars = len(_to_str(question_text)) + len(option_blob) + len(answer_blob) + 520
    return max(120, int(chars / 4))


def _build_super_repair_prompt(
    *,
    row: dict[str, Any],
    question_text: str,
    options: list[dict[str, str]],
    correct_answer: dict[str, Any],
) -> str:
    opts_min = [{"label": o.get("label"), "text": o.get("text")} for o in options[:6]]
    ca = {
        "single": _to_str(correct_answer.get("single")).upper() or None,
        "multiple": [_to_str(x).upper() for x in (correct_answer.get("multiple") or []) if _to_str(x)],
        "numerical": _to_str(correct_answer.get("numerical")).strip() or None,
    }
    return (
        "You are JEE_MATH_DATASET_REPAIR_ENGINE_V5.\n"
        "Your job is OCR repair and dataset integrity, not solving.\n"
        "First do an internal plain-English structural analysis (do not output it), "
        "then repair conservatively.\n\n"
        "LAYER 1 DAMAGE DETECTION: detect OCR_TOKEN_FUSION, BROKEN_INEQUALITIES, "
        "MISSING_PARENTHESES, FRAGMENTED_LIMITS, GARBAGE_TOKENS, UNBALANCED_DELIMITERS.\n"
        "LAYER 2 TOKEN NORMALIZATION: normalize xx, fx, xa, ->, <=, >=, sgn x and remove publisher garbage tokens.\n"
        "LAYER 3 STRUCTURAL RECONSTRUCTION: rebuild piecewise, limits, derivative notation, abs/signum/GIF/fractional-part forms.\n"
        "LAYER 4 VALIDATION: balanced delimiters, valid operators, non-dangling expressions, consistent options.\n"
        "If unrecoverable set unrecoverable_flag=true.\n"
        "LAYER 5 LATEX: provide clean LaTeX for question and options.\n"
        "CRITICAL RULES: never solve; never invent unless strongly implied; preserve meaning.\n\n"
        "Return ONLY valid JSON with keys exactly:\n"
        "{\n"
        '  "question_text_clean": "",\n'
        '  "question_text_latex": "",\n'
        '  "options_clean": {"A":"","B":"","C":"","D":""},\n'
        '  "options_latex": {"A":"","B":"","C":"","D":""},\n'
        '  "detected_errors": [],\n'
        '  "repair_confidence": 0.0,\n'
        '  "unrecoverable_flag": false,\n'
        '  "correct_answer": {"single": null, "multiple": [], "numerical": null}\n'
        "}\n\n"
        f"subject={_to_str(row.get('subject'))}\n"
        f"chapter={_to_str(row.get('chapter'))}\n"
        f"type={_to_str(row.get('type') or row.get('question_type'))}\n"
        f"question_text={question_text}\n"
        f"options={json.dumps(opts_min, ensure_ascii=False)}\n"
        f"correct_answer={json.dumps(ca, ensure_ascii=False)}\n"
    )


async def _ai_repair_row(
    *,
    svc: LocalAppDataService,
    row: dict[str, Any],
    question_text: str,
    options: list[dict[str, str]],
    correct_answer: dict[str, Any],
    direct_timeout_s: float,
) -> dict[str, Any] | None:
    prompt = _build_super_repair_prompt(
        row=row,
        question_text=question_text,
        options=options,
        correct_answer=correct_answer,
    )
    payload: dict[str, Any] = {
        "action": "ai_chat",
        "prompt": prompt,
        "options": {
            "enable_web_retrieval": False,
            "enable_graph_of_thought": False,
            "enable_mcts_reasoning": False,
            "enable_meta_verification": False,
            "return_structured": True,
            "response_style": "json_only",
            "arena_mode": False,
            "provider_count": 1,
            "meta_override_min_confidence": 0.0,
            "meta_override_max_risk": 1.0,
            "meta_override_max_disagreement": 1.0,
            "retry_on_rate_limit": True,
            "prefer_key_rotator": True,
        },
    }
    result = await svc.handle_action(payload)
    if isinstance(result, dict):
        obj = _extract_ai_repair_object(result)
        if isinstance(obj, dict):
            obj.setdefault("_layer4_ai_source", "lalacore_ai_chat")
            return obj

    direct_enabled = _to_str(os.getenv("LAYER4_DIRECT_PROVIDER_FALLBACK", "1")).lower() not in {"0", "false", "no", "off"}
    if direct_enabled:
        direct_obj = await _direct_provider_json_repair(
            prompt=prompt,
            timeout_s=max(8.0, float(direct_timeout_s)),
        )
        if isinstance(direct_obj, dict):
            return direct_obj
    return None


async def _ai_repair_row_with_retries(
    *,
    svc: LocalAppDataService,
    row: dict[str, Any],
    question_text: str,
    options: list[dict[str, str]],
    correct_answer: dict[str, Any],
    max_retries: int,
    retry_delay_s: float,
    timeout_s: float,
) -> tuple[dict[str, Any] | None, int]:
    attempts = 0
    per_attempt_timeout = max(6.0, float(timeout_s) * 2.5)
    for attempt in range(max(1, int(max_retries))):
        attempts += 1
        try:
            out = await asyncio.wait_for(
                _ai_repair_row(
                    svc=svc,
                    row=row,
                    question_text=question_text,
                    options=options,
                    correct_answer=correct_answer,
                    direct_timeout_s=max(8.0, float(timeout_s)),
                ),
                timeout=per_attempt_timeout,
            )
        except asyncio.TimeoutError:
            out = None
        if isinstance(out, dict):
            return out, attempts
        if attempt + 1 < max(1, int(max_retries)):
            wait_s = max(0.0, float(retry_delay_s)) * float(attempt + 1)
            if wait_s > 0:
                await asyncio.sleep(wait_s)
    return None, attempts


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Layer-4 AI salvage: repair low-confidence rows, retag chapters, and build final usable bank"
    )
    parser.add_argument(
        "--input",
        default="data/app/import_question_bank_layer3_verified.live.json",
        help="Layer-3 input JSON list",
    )
    parser.add_argument(
        "--output",
        default="data/app/import_question_bank_final.live.json",
        help="Final merged/usable bank output JSON list",
    )
    parser.add_argument(
        "--dropped-output",
        default="data/app/import_question_bank_layer4_dropped.live.json",
        help="Dropped unusable rows output JSON list",
    )
    parser.add_argument(
        "--report",
        default="data/app/repair_report_layer4.live.json",
        help="Layer-4 report JSON path",
    )
    parser.add_argument(
        "--progress-file",
        default="data/app/repair_report_layer4.progress.live.json",
        help="Live progress JSON path",
    )
    parser.add_argument(
        "--row-log-file",
        default="data/app/repair_report_layer4.rows.live.jsonl",
        help="Per-question JSONL log path",
    )
    parser.add_argument(
        "--row-log-every",
        type=int,
        default=1,
        help="Write one row-log JSON entry every N questions",
    )
    parser.add_argument(
        "--print-row-json",
        action="store_true",
        help="Also print per-question JSON to stdout",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.9,
        help="Rows below this confidence are salvage candidates",
    )
    parser.add_argument(
        "--min-integrity",
        type=float,
        default=0.8,
        help="Rows below this integrity are salvage candidates",
    )
    parser.add_argument(
        "--max-ai-rows",
        type=int,
        default=2500,
        help="Max rows to send to AI salvage (0 = all candidates)",
    )
    parser.add_argument(
        "--ai-review-all",
        action="store_true",
        help="Allow AI to review all rows (not only low-confidence rows)",
    )
    parser.add_argument(
        "--require-ai-check-all",
        action="store_true",
        help="Force AI attempt for every row (with retries/provider rotation)",
    )
    parser.add_argument(
        "--token-budget",
        type=int,
        default=1_800_000,
        help="Estimated token budget for AI salvage (0 = unlimited)",
    )
    parser.add_argument(
        "--avg-response-tokens",
        type=int,
        default=220,
        help="Estimated response tokens per AI salvage call for budget control",
    )
    parser.add_argument(
        "--min-question-similarity",
        type=float,
        default=0.78,
        help="Reject AI rewrite if repaired question diverges below this trigram cosine threshold",
    )
    parser.add_argument(
        "--ai-failure-breaker",
        type=int,
        default=25,
        help="Disable further AI calls after this many consecutive unusable AI responses",
    )
    parser.add_argument(
        "--ai-max-retries",
        type=int,
        default=3,
        help="Max retry attempts per row when AI output is unusable",
    )
    parser.add_argument(
        "--ai-retry-delay-s",
        type=float,
        default=3.0,
        help="Base delay between retries (linear backoff)",
    )
    parser.add_argument(
        "--ai-timeout-s",
        type=float,
        default=20.0,
        help="Timeout per AI call attempt",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=400,
        help="Emit progress every N rows",
    )
    parser.add_argument(
        "--delete-unusable",
        action="store_true",
        help="Drop rows with final status reject/unrecoverable from final output",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    dropped_path = Path(args.dropped_output)
    report_path = Path(args.report)
    progress_path = Path(args.progress_file)
    row_log_path = Path(args.row_log_file)

    rows = _load_json_list(input_path)
    total = len(rows)
    repair_corpus: list[dict[str, Any]] | None = None

    svc = LocalAppDataService()
    await svc._ensure_loaded()
    repair_engine = QuestionRepairEngine()

    force_all_ai = bool(args.require_ai_check_all)
    salvage_candidates: list[tuple[int, float]] = []
    for idx, row in enumerate(rows):
        conf = _to_float(row.get("repair_confidence"), 0.0)
        integrity = _to_float(row.get("structural_integrity_score"), 0.0)
        status = _to_str(row.get("repair_status")).lower()
        qtext = _to_str(row.get("question_text") or row.get("question"))
        should_include = (
            force_all_ai
            or bool(args.ai_review_all)
            or status in {"review", "reject", "unrecoverable"}
            or conf < float(args.min_confidence)
            or integrity < float(args.min_integrity)
            or _looks_heavily_corrupted(qtext)
        )
        if should_include:
            salvage_candidates.append((idx, _salvage_severity(row)))
    salvage_candidates.sort(key=lambda x: x[1], reverse=True)
    max_ai_rows = int(args.max_ai_rows)
    if force_all_ai:
        ai_index_set = set(range(total))
    elif max_ai_rows > 0:
        ai_index_set = {idx for idx, _ in salvage_candidates[:max_ai_rows]}
    else:
        ai_index_set = {idx for idx, _ in salvage_candidates}

    finalized: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    ai_attempted = 0
    ai_calls_total = 0
    ai_applied = 0
    ai_skipped_budget = 0
    ai_tokens_estimated = 0
    ai_consecutive_failures = 0
    ai_disabled = False
    ai_disabled_reason = ""
    deterministic_repaired = 0

    started_at = _now_iso()
    t0 = time.time()
    row_log_path.parent.mkdir(parents=True, exist_ok=True)
    row_log_path.write_text("", encoding="utf-8")
    _atomic_write_json(
        progress_path,
        {
            "stage": "layer4_ai_salvage",
            "status": "running",
            "started_at": started_at,
            "done": 0,
            "total": total,
            "progress_pct": 0.0,
            "salvage_candidates": len(salvage_candidates),
            "ai_budget": len(ai_index_set),
            "ai_attempted": 0,
            "ai_calls_total": 0,
            "ai_applied": 0,
            "ai_skipped_budget": 0,
            "ai_tokens_estimated": 0,
            "ai_consecutive_failures": 0,
            "ai_disabled": False,
            "dropped": 0,
            "kept": 0,
        },
    )
    # Ensure live output files exist from the start of the run.
    _atomic_write_json(output_path, finalized)
    _atomic_write_json(dropped_path, dropped)

    for idx, raw in enumerate(rows, start=1):
        row = dict(raw)
        qid = _to_str(row.get("question_id") or row.get("id") or f"row_{idx}")
        question_text = _to_str(row.get("question_text") or row.get("question")).strip()
        question_text, seed_token_actions = _token_repair_text(question_text)
        question_text_latex = _to_str(row.get("question_text_latex")).strip()
        options = _normalize_option_list(row.get("options"))
        options, option_token_actions = _repair_option_tokens(options)
        options_latex: dict[str, Any] | None = (
            dict(row.get("options_latex"))
            if isinstance(row.get("options_latex"), dict)
            else None
        )
        correct_answer = _normalize_correct_answer(row)
        subject = _to_str(row.get("subject") or "Mathematics").strip() or "Mathematics"
        qtype = _to_str(row.get("type") or row.get("question_type")).strip()

        chapter = svc._resolve_import_row_chapter(row, subject_override=subject)
        chapter_tags = svc._resolve_import_row_chapter_tags(
            row,
            subject_override=subject,
            chapter_override=chapter,
            max_tags=3,
        )
        if chapter:
            row["chapter"] = chapter
        if chapter_tags:
            row["chapter_tags"] = chapter_tags

        pre_status = _to_str(row.get("repair_status")).lower()
        pre_conf = _to_float(row.get("repair_confidence"), 0.0)
        pre_integrity = _to_float(row.get("structural_integrity_score"), 0.0)
        needs_salvage = (
            pre_status in {"review", "reject", "unrecoverable"}
            or pre_conf < float(args.min_confidence)
            or pre_integrity < float(args.min_integrity)
            or _looks_heavily_corrupted(question_text)
        )

        actions = [_to_str(x).strip() for x in (row.get("repair_actions") or []) if _to_str(x).strip()]
        actions.extend(seed_token_actions)
        actions.extend(option_token_actions)
        actions = list(dict.fromkeys([x for x in actions if _to_str(x).strip()]))
        issues = [_to_str(x).strip() for x in (row.get("detected_issues") or []) if _to_str(x).strip()]
        post_det_conf = pre_conf if pre_conf > 0 else 0.9
        post_det_integrity = _score_integrity(
            repair_confidence=post_det_conf,
            question_text=question_text,
            options=options,
            issues=issues,
        )
        post_det_status = _status_from_integrity(post_det_integrity)
        still_bad = (
            post_det_status in {"reject", "unrecoverable"}
            or post_det_conf < float(args.min_confidence)
            or post_det_integrity < float(args.min_integrity)
            or _looks_heavily_corrupted(question_text)
        )
        ai_attempted_this_row = False
        ai_calls_for_row = 0
        ai_applied_this_row = False
        ai_similarity_score: float | None = None
        ai_source: str | None = None
        ai_conf_override_value: float | None = None
        syntax_score = 1.0
        structure_score = 1.0
        math_validity_score = 1.0

        if needs_salvage:
            repaired = repair_engine.repair_question(
                question_text=question_text,
                options=options,
                correct_answer=correct_answer,
                question_type=qtype,
                question_id=qid,
                corpus=repair_corpus,
            )
            if _to_str(repaired.question_text).strip():
                question_text = _to_str(repaired.question_text).strip()
            if repaired.options:
                options = list(repaired.options)
            if repaired.correct_answer:
                correct_answer = dict(repaired.correct_answer)
            actions = list(dict.fromkeys([*actions, *list(repaired.repair_actions or []), "layer4_deterministic_repair"]))
            issues = list(dict.fromkeys(list(repaired.repair_issues or [])))
            deterministic_repaired += 1

            post_det_conf = float(repaired.repair_confidence or pre_conf)
            post_det_integrity = _score_integrity(
                repair_confidence=post_det_conf,
                question_text=question_text,
                options=options,
                issues=issues,
            )
            post_det_status = _status_from_integrity(post_det_integrity)
            still_bad = (
                post_det_status in {"reject", "unrecoverable"}
                or post_det_conf < float(args.min_confidence)
                or post_det_integrity < float(args.min_integrity)
                or _looks_heavily_corrupted(question_text)
            )
        ai_eligible = ((idx - 1) in ai_index_set) and (
            still_bad or bool(args.ai_review_all) or force_all_ai
        )
        if ai_eligible and (not ai_disabled or force_all_ai):
            original_for_similarity = question_text
            est_prompt = _estimate_ai_tokens(
                question_text=question_text,
                options=options,
                correct_answer=correct_answer,
            )
            est_total = est_prompt + max(0, int(args.avg_response_tokens))
            token_budget = max(0, int(args.token_budget))
            if token_budget > 0 and (ai_tokens_estimated + est_total > token_budget):
                ai_skipped_budget += 1
            else:
                ai_tokens_estimated += est_total
                ai_attempted += 1
                ai_attempted_this_row = True
                ai_obj, ai_call_count = await _ai_repair_row_with_retries(
                    svc=svc,
                    row=row,
                    question_text=question_text,
                    options=options,
                    correct_answer=correct_answer,
                    max_retries=max(1, int(args.ai_max_retries)),
                    retry_delay_s=max(0.0, float(args.ai_retry_delay_s)),
                    timeout_s=max(3.0, float(args.ai_timeout_s)),
                )
                ai_calls_total += int(ai_call_count)
                ai_calls_for_row += int(ai_call_count)
                if isinstance(ai_obj, dict):
                    ai_source = _to_str(ai_obj.get("_layer4_ai_source")) or None
                    normalized_ai = _normalize_ai_payload_fields(
                        ai_obj=ai_obj,
                        fallback_question_text=question_text,
                        fallback_options=options,
                        fallback_correct_answer=correct_answer,
                    )
                    ai_qtext = _to_str(normalized_ai.get("question_text")).strip()
                    ai_opts = list(normalized_ai.get("options") or [])
                    ai_ans = dict(normalized_ai.get("correct_answer") or correct_answer)
                    ai_detected_errors = [
                        _to_str(x).strip()
                        for x in (normalized_ai.get("detected_errors") or [])
                        if _to_str(x).strip()
                    ]
                    ai_qtext_latex = _to_str(normalized_ai.get("question_text_latex")).strip()
                    ai_options_latex = normalized_ai.get("options_latex")
                    ai_unrecoverable = bool(normalized_ai.get("unrecoverable_flag"))
                    ai_conf_override = normalized_ai.get("repair_confidence")
                    similarity_score = 0.0
                    if ai_qtext:
                        similarity_score = _char_trigram_cosine(
                            _norm_for_similarity(original_for_similarity),
                            _norm_for_similarity(ai_qtext),
                        )
                        ai_similarity_score = float(similarity_score)
                    if ai_qtext and similarity_score < float(args.min_question_similarity):
                        actions = list(
                            dict.fromkeys(
                                [*actions, "layer4_ai_rejected_low_similarity"]
                            )
                        )
                        ai_qtext = ""
                        ai_opts = []
                    issues = list(dict.fromkeys([*issues, *ai_detected_errors]))
                    if ai_unrecoverable:
                        issues = list(dict.fromkeys([*issues, "ai_unrecoverable_flag"]))
                        actions = list(dict.fromkeys([*actions, "layer4_ai_marked_unrecoverable"]))
                    if isinstance(ai_conf_override, float):
                        ai_conf_override_value = max(0.0, min(1.0, float(ai_conf_override)))
                        post_det_conf = max(0.0, min(1.0, float(ai_conf_override)))
                    if ai_qtext:
                        question_text = ai_qtext
                    if ai_qtext_latex:
                        question_text_latex = ai_qtext_latex
                    if ai_opts:
                        options = ai_opts
                    if isinstance(ai_options_latex, dict):
                        options_latex = dict(ai_options_latex)
                    correct_answer = ai_ans
                    if ai_qtext or ai_opts:
                        actions = list(dict.fromkeys([*actions, "layer4_ai_salvage_applied"]))
                        ai_applied += 1
                        ai_applied_this_row = True
                        ai_consecutive_failures = 0

                    repaired2 = repair_engine.repair_question(
                        question_text=question_text,
                        options=options,
                        correct_answer=correct_answer,
                        question_type=qtype,
                        question_id=qid,
                        corpus=repair_corpus,
                    )
                    if _to_str(repaired2.question_text).strip():
                        question_text = _to_str(repaired2.question_text).strip()
                    if repaired2.options:
                        options = list(repaired2.options)
                        options, post_ai_option_actions = _repair_option_tokens(options)
                        if post_ai_option_actions:
                            actions = list(dict.fromkeys([*actions, *post_ai_option_actions]))
                    if repaired2.correct_answer:
                        correct_answer = dict(repaired2.correct_answer)
                    actions = list(
                        dict.fromkeys(
                            [*actions, *list(repaired2.repair_actions or []), "layer4_post_ai_verify"]
                        )
                    )
                    issues = list(dict.fromkeys(list(repaired2.repair_issues or [])))
                    post_det_conf = max(post_det_conf, float(repaired2.repair_confidence or post_det_conf))
                else:
                    ai_consecutive_failures += 1
                    breaker = max(0, int(args.ai_failure_breaker))
                    if (not force_all_ai) and breaker > 0 and ai_consecutive_failures >= breaker:
                        ai_disabled = True
                        ai_disabled_reason = (
                            f"ai_failure_breaker_{breaker}"
                        )

        grammar_eval = _ast_grammar_validation(question_text, options)
        syntax_score = _to_float(grammar_eval.get("syntax_score"), 0.0)
        structure_score = _to_float(grammar_eval.get("structure_score"), 0.0)
        grammar_issues = [
            _to_str(x).strip()
            for x in (grammar_eval.get("issues") or [])
            if _to_str(x).strip()
        ]
        if grammar_issues:
            issues = list(dict.fromkeys([*issues, *grammar_issues]))
            actions = list(dict.fromkeys([*actions, "layer4_l2_ast_grammar"]))

        validator_eval = _mathematical_validator(question_text, options)
        math_validity_score = _to_float(validator_eval.get("math_validity_score"), 0.0)
        validator_issues = [
            _to_str(x).strip()
            for x in (validator_eval.get("issues") or [])
            if _to_str(x).strip()
        ]
        if validator_issues:
            issues = list(dict.fromkeys([*issues, *validator_issues]))
            actions = list(dict.fromkeys([*actions, "layer4_l5_math_validator"]))

        ai_certainty = 0.35
        if ai_attempted_this_row:
            ai_certainty = 0.70
        if ai_applied_this_row:
            ai_certainty = 0.92
        if isinstance(ai_conf_override_value, float):
            ai_certainty = max(ai_certainty, ai_conf_override_value)

        engine_conf = _confidence_engine(
            syntax_score=syntax_score,
            structure_score=structure_score,
            math_validity_score=math_validity_score,
            ai_certainty=ai_certainty,
        )
        final_conf = round(max(0.0, min(1.0, (0.35 * post_det_conf) + (0.65 * engine_conf))), 4)
        if bool(validator_eval.get("hard_reject")):
            final_conf = min(final_conf, 0.35)
            actions = list(dict.fromkeys([*actions, "layer4_l5_hard_reject"]))

        integrity = _score_integrity(
            repair_confidence=final_conf,
            question_text=question_text,
            options=options,
            issues=issues,
        )
        status = _status_from_integrity(integrity)
        publish_risk = round(max(0.0, min(1.0, 1.0 - integrity)), 4)

        row["question_text"] = question_text
        row["question_text_latex"] = _to_str(question_text_latex or question_text)
        row["options"] = options
        if isinstance(options_latex, dict) and options_latex:
            row["options_latex"] = options_latex
        row["correct_answer"] = correct_answer
        row["repair_actions"] = list(dict.fromkeys(actions))
        row["detected_issues"] = issues
        row["repair_confidence"] = round(float(final_conf), 4)
        row["structural_integrity_score"] = integrity
        row["repair_status"] = status
        row["publish_risk_score"] = publish_risk
        row["layer4_salvage"] = {
            "applied": bool(needs_salvage),
            "ai_attempted": bool(ai_attempted_this_row),
            "ai_source": ai_source,
            "l2_ast_syntax_score": round(float(syntax_score), 4),
            "l3_structure_score": round(float(structure_score), 4),
            "l5_math_validity_score": round(float(math_validity_score), 4),
            "l6_engine_confidence": engine_conf,
            "processed_at": _now_iso(),
        }

        should_drop = bool(args.delete_unusable) and status in {"reject", "unrecoverable"}
        if should_drop:
            dropped.append(row)
        else:
            finalized.append(row)
        status_counts[status] += 1
        for tok in row.get("repair_actions") or []:
            action_counts[_to_str(tok)] += 1

        row_log_payload = {
            "stage": "layer4_ai_salvage_row",
            "updated_at": _now_iso(),
            "idx": idx,
            "total": total,
            "question_id": qid,
            "subject": subject,
            "chapter": _to_str(row.get("chapter")),
            "pre_status": pre_status,
            "pre_confidence": round(float(pre_conf), 4),
            "pre_integrity": round(float(pre_integrity), 4),
            "needs_salvage": bool(needs_salvage),
            "ai_eligible": bool(ai_eligible),
            "ai_attempted": bool(ai_attempted_this_row),
            "ai_calls_for_row": int(ai_calls_for_row),
            "ai_applied": bool(ai_applied_this_row),
            "ai_similarity": (
                round(float(ai_similarity_score), 6)
                if ai_similarity_score is not None
                else None
            ),
            "ai_source": ai_source,
            "l2_ast_syntax_score": round(float(syntax_score), 4),
            "l3_structure_score": round(float(structure_score), 4),
            "l5_math_validity_score": round(float(math_validity_score), 4),
            "l6_engine_confidence": engine_conf,
            "final_status": status,
            "final_confidence": round(float(final_conf), 4),
            "final_integrity": integrity,
            "dropped": bool(should_drop),
            "repair_actions_count": len(row.get("repair_actions") or []),
        }
        row_log_every = max(1, int(args.row_log_every))
        if idx % row_log_every == 0 or idx == total:
            _append_jsonl(row_log_path, row_log_payload)
            if bool(args.print_row_json):
                print(json.dumps(row_log_payload, ensure_ascii=False))

        if args.progress_every > 0 and (idx % int(args.progress_every) == 0 or idx == total):
            elapsed = max(0.001, time.time() - t0)
            rate = idx / elapsed
            eta_s = int((max(0, total - idx) / max(0.01, rate)))
            payload = {
                "stage": "layer4_ai_salvage",
                "status": "running",
                "updated_at": _now_iso(),
                "done": idx,
                "total": total,
                "progress_pct": round((idx / max(1, total)) * 100.0, 2),
                "rows_per_s": round(rate, 2),
                "eta_s": eta_s,
                "salvage_candidates": len(salvage_candidates),
                "ai_budget": len(ai_index_set),
                "ai_attempted": ai_attempted,
                "ai_calls_total": ai_calls_total,
                "ai_applied": ai_applied,
                "ai_skipped_budget": ai_skipped_budget,
                "ai_tokens_estimated": ai_tokens_estimated,
                "ai_consecutive_failures": ai_consecutive_failures,
                "ai_disabled": ai_disabled,
                "ai_disabled_reason": ai_disabled_reason,
                "kept": len(finalized),
                "dropped": len(dropped),
                "status_counts": dict(status_counts),
            }
            _atomic_write_json(progress_path, payload)
            # Checkpoint live bank files so UI/CLI can read partial results.
            _atomic_write_json(output_path, finalized)
            _atomic_write_json(dropped_path, dropped)
            print(json.dumps(payload, ensure_ascii=False))

    _atomic_write_json(output_path, finalized)
    _atomic_write_json(dropped_path, dropped)

    seen = max(1, len(finalized) + len(dropped))
    avg_conf = sum(_to_float(r.get("repair_confidence"), 0.0) for r in [*finalized, *dropped]) / seen
    avg_integrity = sum(_to_float(r.get("structural_integrity_score"), 0.0) for r in [*finalized, *dropped]) / seen
    report = {
        "started_at": started_at,
        "finished_at": _now_iso(),
        "input": str(input_path),
        "output": str(output_path),
        "dropped_output": str(dropped_path),
        "rows_seen": len(rows),
        "rows_kept": len(finalized),
        "rows_dropped": len(dropped),
        "salvage_candidates": len(salvage_candidates),
        "deterministic_repaired": deterministic_repaired,
        "ai_attempted": ai_attempted,
        "ai_calls_total": ai_calls_total,
        "ai_applied": ai_applied,
        "ai_skipped_budget": ai_skipped_budget,
        "ai_tokens_estimated": ai_tokens_estimated,
        "ai_consecutive_failures": ai_consecutive_failures,
        "ai_disabled": ai_disabled,
        "ai_disabled_reason": ai_disabled_reason,
        "status_counts": dict(status_counts),
        "avg_repair_confidence": round(avg_conf, 4),
        "avg_structural_integrity_score": round(avg_integrity, 4),
        "top_repair_actions": dict(action_counts.most_common(30)),
        "config": {
            "min_confidence": float(args.min_confidence),
            "min_integrity": float(args.min_integrity),
            "max_ai_rows": int(args.max_ai_rows),
            "ai_review_all": bool(args.ai_review_all),
            "require_ai_check_all": bool(args.require_ai_check_all),
            "token_budget": int(args.token_budget),
            "avg_response_tokens": int(args.avg_response_tokens),
            "min_question_similarity": float(args.min_question_similarity),
            "ai_failure_breaker": int(args.ai_failure_breaker),
            "ai_max_retries": int(args.ai_max_retries),
            "ai_retry_delay_s": float(args.ai_retry_delay_s),
            "ai_timeout_s": float(args.ai_timeout_s),
            "delete_unusable": bool(args.delete_unusable),
        },
    }
    _atomic_write_json(report_path, report)
    _atomic_write_json(
        progress_path,
        {
            "stage": "layer4_ai_salvage",
            "status": "done",
            "updated_at": _now_iso(),
            "done": len(rows),
            "total": len(rows),
            "progress_pct": 100.0,
            "kept": len(finalized),
            "dropped": len(dropped),
            "ai_attempted": ai_attempted,
            "ai_calls_total": ai_calls_total,
            "ai_applied": ai_applied,
            "ai_skipped_budget": ai_skipped_budget,
            "ai_tokens_estimated": ai_tokens_estimated,
            "ai_consecutive_failures": ai_consecutive_failures,
            "ai_disabled": ai_disabled,
            "ai_disabled_reason": ai_disabled_reason,
            "output": str(output_path),
            "report": str(report_path),
        },
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
