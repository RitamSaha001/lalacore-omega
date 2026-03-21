#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sympy import sympify
from sympy.core.sympify import SympifyError


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


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError(f"input is not a JSON list: {path}")
    return [dict(row) for row in rows if isinstance(row, dict)]


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", _to_str(text).lower()).strip()


def _char_trigram_cosine(a: str, b: str) -> float:
    def _vec(text: str) -> dict[str, int]:
        t = re.sub(r"[^a-z0-9]+", "", text.lower())
        if len(t) < 3:
            return {}
        out: dict[str, int] = {}
        for i in range(len(t) - 2):
            token = t[i : i + 3]
            out[token] = out.get(token, 0) + 1
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
            if text:
                out.append({"label": chr(65 + min(idx, 25)), "text": text})
    return out


def _apply_regex_rules(
    text: str,
    rules: list[tuple[str, str]],
    *,
    flags: int = re.IGNORECASE,
    max_passes: int = 2,
) -> tuple[str, int]:
    out = _to_str(text)
    changed = 0
    for _ in range(max(1, max_passes)):
        pass_changed = 0
        for pat, repl in rules:
            nxt, n = re.subn(pat, repl, out, flags=flags)
            if n > 0:
                out = nxt
                pass_changed += n
        if pass_changed <= 0:
            break
        changed += pass_changed
    return out, changed


_LATEX_COMMAND_RULES: list[tuple[str, str]] = [
    (r"\b(?:extfrac|textfrac|texfrac|fracfrac)\b", r"\\frac"),
    (r"\bmathbbfrac\b", r"\\mathbb"),
    (r"\bmathbbf\b", r"\\mathbb"),
    (r"\bmathbbR\b", r"\\mathbb{R}"),
    (r"\bmathbbZ\b", r"\\mathbb{Z}"),
    (r"\bmathbbN\b", r"\\mathbb{N}"),
    (r"\bmathbbQ\b", r"\\mathbb{Q}"),
    (r"\bmathbbC\b", r"\\mathbb{C}"),
    (r"\b(?:sqrtfrac|sqrf|sgrt|sqrtf|sqroot)\b", r"\\sqrt"),
    (r"\barcsinx\b", r"\\arcsin x"),
    (r"\barccosx\b", r"\\arccos x"),
    (r"\barctanx\b", r"\\arctan x"),
    (r"\bsinx\b", r"\\sin x"),
    (r"\bcosx\b", r"\\cos x"),
    (r"\btanx\b", r"\\tan x"),
    (r"\blogx\b", r"\\log x"),
    (r"\blnx\b", r"\\ln x"),
    (r"\bsecx\b", r"\\sec x"),
    (r"\bcosecx\b", r"\\csc x"),
    (r"\bcotx\b", r"\\cot x"),
    (r"\b1imx\b", r"\\lim x"),
    (r"\b1im\b", r"\\lim"),
    (r"\blimx\b", r"\\lim x"),
    (r"\bsum\b", r"\\sum"),
    (r"\bprod\b", r"\\prod"),
    (r"\boint\b", r"\\oint"),
    (r"\bint\b", r"\\int"),
    (r"\binfty\b", r"\\infty"),
    (r"\boo\b", r"\\infty"),
    (r"\binf\b", r"\\infty"),
]

_SET_RULES: list[tuple[str, str]] = [
    (r"(?<![A-Za-z])R(?![A-Za-z])", r"\\mathbb{R}"),
    (r"(?<![A-Za-z])Z(?![A-Za-z])", r"\\mathbb{Z}"),
    (r"(?<![A-Za-z])N(?![A-Za-z])", r"\\mathbb{N}"),
    (r"(?<![A-Za-z])Q(?![A-Za-z])", r"\\mathbb{Q}"),
    (r"(?<![A-Za-z])C(?![A-Za-z])", r"\\mathbb{C}"),
    (r"\bsubseteq\b", r"\\subseteq"),
    (r"\bsubset\b", r"\\subset"),
    (r"\bsupseteq\b", r"\\supseteq"),
    (r"\bsupset\b", r"\\supset"),
    (r"\bunion\b", r"\\cup"),
    (r"\bintersection\b", r"\\cap"),
    (r"\bbelongs\b", r"\\in"),
    (r"\bnotin\b", r"\\notin"),
    (r"\bempty\b", r"\\emptyset"),
    (r"\bphi\b", r"\\varphi"),
    (r"\b([A-Z])x([A-Z])\b", r"\1 \\times \2"),
]

_INEQUALITY_RULES: list[tuple[str, str]] = [
    (r"=<\s*=", "≤"),
    (r">\s*=", "≥"),
    (r"<\s*=", "≤"),
    (r"=<", "≤"),
    (r"=>", "≥"),
    (r"<=", "≤"),
    (r">=", "≥"),
    (r"<<", "≪"),
    (r">>", "≫"),
    (r"\|x\|\s*>=\s*0", r"|x| ≥ 0"),
    (r"\|x\|\s*<=\s*1", r"|x| ≤ 1"),
]

_MULT_RULES: list[tuple[str, str]] = [
    (r"\b([2-9]\d*)\s*([xyzntijkm])\b", r"\1*\2"),
    (r"\b([xyzntijkm])\s*([2-9])\b", r"\1^\2"),
    (r"\b([2-9]\d*)\s*([xyzntijkm])\s*\^\s*([2-9])\b", r"\1*\2^\3"),
    (r"\b([2-9]\d*)\s*([xyz])\s*([2-9])\b", r"\1*\2^\3"),
    (r"\b([xyz])\s*\*\s*\1\b", r"\1^2"),
    (r"\b([xyz])\s*([xyz])\b", r"\1*\2"),
]

_LIMIT_RULES: list[tuple[str, str]] = [
    (r"\blim\s*x\s*[-=]*>\s*0\b", r"\\lim_{x\\to0}"),
    (r"\blim\s*x\s*[-=]*>\s*\\infty\b", r"\\lim_{x\\to\\infty}"),
    (r"\blimx\s*[-=]*>\s*0\b", r"\\lim_{x\\to0}"),
    (r"\blimx\s*→\s*0\b", r"\\lim_{x\\to0}"),
    (r"\bn\s*[-=]*>\s*\\infty\b", r"n\\to\\infty"),
]

_SUM_INT_RULES: list[tuple[str, str]] = [
    (r"\\sum\s*([irkn])\s*=\s*1\s*n\b", r"\\sum_{\1=1}^{n}"),
    (r"\\sum\s*([irkn])\s*=\s*1\b", r"\\sum_{\1=1}"),
    (r"∑\s*([irkn])\s*=\s*1\s*n", r"\\sum_{\1=1}^{n}"),
    (r"\\int\s*0\s*\^\s*1", r"\\int_0^1"),
    (r"\\int\s*0\s*\^\s*\\infty", r"\\int_0^\\infty"),
    (r"\bd\s+([xyt])\b", r"d\1"),
]

_TRIG_LOG_RULES: list[tuple[str, str]] = [
    (r"\bsin([2-9][a-z])\b", r"sin(\1)"),
    (r"\bcos([2-9][a-z])\b", r"cos(\1)"),
    (r"\btan([2-9][a-z])\b", r"tan(\1)"),
    (r"\bsin\^2\s*x\b", r"\\sin^2 x"),
    (r"\bcos\^2\s*x\b", r"\\cos^2 x"),
    (r"\btan\^2\s*x\b", r"\\tan^2 x"),
    (r"\bsin-1x\b", r"\\sin^{-1}x"),
    (r"\bcos-1x\b", r"\\cos^{-1}x"),
    (r"\btan-1x\b", r"\\tan^{-1}x"),
    (r"\blog([2-9]|10)x\b", r"log_\1(x)"),
    (r"\blogx\b", r"log(x)"),
    (r"\blnx\b", r"ln(x)"),
]

_DOMAIN_RULES: list[tuple[str, str]] = [
    (r"\\mathbb\{R\}\s*[-=]*>\s*\\mathbb\{R\}", r"\\mathbb{R}\\to\\mathbb{R}"),
    (r"\bR\s*[-=]*>\s*R\b", r"\\mathbb{R}\\to\\mathbb{R}"),
]

_ARTIFACT_RULES: list[tuple[str, str]] = [
    (r"\b(?:JEE\s*Main|JEE\s*Advanced|PYQ|Allen|Resonance|FIITJEE|Arihant|Cengage)\b", " "),
    (r"\b(?:Ans\.?|Que\.?|Solution)\b", " "),
    (r"\bPage\s*\d+\b", " "),
]

_NUMERIC_RULES: list[tuple[str, str]] = [
    (r"\b00\b", "0"),
    (r"\b01\b", "1"),
    (r"\b02\b", "2"),
    (r"\b1O\b", "10"),
    (r"(?<![A-Za-z0-9])O(?![A-Za-z0-9])", "0"),
    (r"(?<![A-Za-z0-9])[lI](?![A-Za-z0-9])", "1"),
]

def _clean_artifacts(text: str) -> tuple[str, bool]:
    src = _to_str(text)
    out = src
    out = re.sub(r"\b(?:CD|IF|FN|SR|LT)\d{2}-\d{4}\b", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\b(?:JEE\s*Advanced|JEE\s*Main|Allen|Aakash|Page)\b[^\n]{0,40}", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\[[ ]*\d+[ ]*\]$", " ", out)
    out = re.sub(r"\bwww\.[^\s]+\b", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out, out != src


def _normalize_floor_frac(text: str) -> tuple[str, bool]:
    src = _to_str(text)
    out = src
    out = re.sub(r"\[\s*([A-Za-z][A-Za-z0-9_+\-*/^ ]{0,24})\s*\]", r"floor(\1)", out)
    out = re.sub(r"\{\s*([A-Za-z][A-Za-z0-9_+\-*/^ ]{0,24})\s*\}", r"frac(\1)", out)
    out = out.replace("⌊", "floor(").replace("⌋", ")")
    out = out.replace("⌈", "ceil(").replace("⌉", ")")
    return out, out != src


def _apply_layer7_rule_catalog(text: str) -> tuple[str, list[str]]:
    out = _to_str(text)
    actions: list[str] = []

    out, changed = _apply_regex_rules(out, _ARTIFACT_RULES)
    if changed:
        actions.append("layer7_artifact_rules")
    out, changed = _apply_regex_rules(out, _NUMERIC_RULES)
    if changed:
        actions.append("layer7_numeric_rules")
    out, changed = _apply_regex_rules(out, _LATEX_COMMAND_RULES)
    if changed:
        actions.append("layer7_latex_rules")
    out, changed = _apply_regex_rules(out, _SET_RULES)
    if changed:
        actions.append("layer7_set_rules")
    out, changed = _apply_regex_rules(out, _INEQUALITY_RULES)
    if changed:
        actions.append("layer7_inequality_rules")
    out, changed = _apply_regex_rules(out, _MULT_RULES)
    if changed:
        actions.append("layer7_multiplication_rules")
    out, changed = _apply_regex_rules(out, _LIMIT_RULES)
    if changed:
        actions.append("layer7_limit_rules")
    out, changed = _apply_regex_rules(out, _SUM_INT_RULES)
    if changed:
        actions.append("layer7_sum_int_rules")
    out, changed = _apply_regex_rules(out, _TRIG_LOG_RULES)
    if changed:
        actions.append("layer7_trig_log_rules")
    out, changed = _apply_regex_rules(out, _DOMAIN_RULES)
    if changed:
        actions.append("layer7_domain_rules")
    out, floor_changed = _normalize_floor_frac(out)
    if floor_changed:
        actions.append("layer7_floor_frac_normalize")
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out, list(dict.fromkeys(actions))


def _extract_fragments(text: str, *, max_items: int = 8) -> list[str]:
    src = _to_str(text)
    if not src:
        return []
    pats = (
        r"lim[^.;\n]{4,160}",
        r"[a-zA-Z]\s*\([^)]+\)\s*=\s*[^.;\n]{2,160}",
        r"\|[^|]{1,100}\|",
        r"sum[^.;\n]{1,120}",
        r"[a-zA-Z0-9\)\]]\s*[\+\-\*/\^]\s*[a-zA-Z0-9\(\[]",
    )
    out: list[str] = []
    for pat in pats:
        for m in re.finditer(pat, src, flags=re.IGNORECASE):
            frag = _to_str(m.group(0))
            if frag and frag not in out:
                out.append(frag[:180])
            if len(out) >= max_items:
                return out
    return out


def _sympy_ok(fragment: str) -> bool:
    frag = _to_str(fragment)
    if not frag:
        return False
    if "lim" in frag.lower():
        return bool("->" in frag or "→" in frag or r"\to" in frag.lower())
    expr = frag.replace("^", "**").replace("≤", "<=").replace("≥", ">=").replace("→", "->")
    expr = expr.replace(r"\sin", "sin").replace(r"\cos", "cos").replace(r"\tan", "tan")
    expr = expr.replace(r"\log", "log").replace(r"\ln", "log").replace(r"\sqrt", "sqrt")
    expr = expr.replace(r"\infty", "oo")
    expr = expr.replace(r"\times", "*")
    expr = expr.replace(r"\mathbb{R}", "R").replace(r"\mathbb{Z}", "Z")
    expr = expr.replace(r"\mathbb{N}", "N").replace(r"\mathbb{Q}", "Q").replace(r"\mathbb{C}", "C")
    expr = re.sub(r"\bsgn\s*\(", "sign(", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bfloor\s*\(", "floor(", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bfrac\s*\(", "frac(", expr, flags=re.IGNORECASE)
    expr = re.sub(r"(?<=\d)(?=[A-Za-z])", "*", expr)
    expr = re.sub(r"(?<=[A-Za-z])(?=\d)", "*", expr)
    if "=" in expr:
        left, right = expr.split("=", 1)
        try:
            sympify(left.strip())
            sympify(right.strip())
            return True
        except (SympifyError, SyntaxError, TypeError, ValueError):
            return False
    try:
        sympify(expr.strip())
        return True
    except (SympifyError, SyntaxError, TypeError, ValueError):
        return False


def _ocr_number_merge_suspect(text: str) -> bool:
    src = _to_str(text)
    has_piecewise_hint = bool(re.search(r"(x\s*[<>≤≥]=?\s*\d+)|\bfor\b", src, flags=re.IGNORECASE))
    has_linear = bool(re.search(r"\b\d+\s*x\b", src))
    constant_big = bool(re.search(r"(^|[\s,{;(])\d{2,}(?=$|[\s,};)])", src))
    return bool(has_piecewise_hint and has_linear and constant_big)


def _attempt_ocr_merge_linear_reconstruct(text: str) -> tuple[str, bool]:
    src = _to_str(text)
    low = src.lower()
    if not _ocr_number_merge_suspect(src):
        return src, False
    if "x" not in low:
        return src, False

    out = src
    changed = False
    for m in re.finditer(r"(^|[\s,{;(])(\d{2})(?=$|[\s,};)])", src):
        token = m.group(2)
        if not (token.isdigit() and len(token) == 2):
            continue
        a = token[0]
        b = token[1]
        out = out.replace(m.group(0), f"{m.group(1)}{a}x+{b}", 1)
        changed = True
        break
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out, changed


def _semantic_sanity_checks(text: str, options: list[dict[str, str]]) -> tuple[list[str], float, bool]:
    src = _to_str(text)
    low = src.lower()
    issues: list[str] = []
    score = 1.0
    hard_reject = False

    if re.search(r"\b(?:if|let|then|is|find)\s*$", low):
        issues.append("truncated_question")
        score -= 0.45
        hard_reject = True

    if re.fullmatch(r"[0-9 .,\-]+", src.strip() or " "):
        issues.append("garbage_numeric_blob")
        score -= 0.55
        hard_reject = True

    if not re.search(r"(=|\\lim|\\int|\\sum|\\prod|\+|\-|\*|/|\^|≤|≥|<|>)", src) and len(src.split()) <= 5:
        issues.append("missing_equation_structure")
        score -= 0.25

    # Summation sanity: upper bound should not be lower than lower bound.
    sum_match = re.search(r"(?:n|k)\s*=\s*(-?\d+).{0,12}(?:\^|to)\s*(-?\d+)", low)
    if sum_match:
        lo = int(sum_match.group(1))
        hi = int(sum_match.group(2))
        if hi < lo:
            issues.append("invalid_summation_bounds")
            score -= 0.45
            hard_reject = True

    opts_norm = [_norm(_to_str(o.get("text"))) for o in options if _to_str(o.get("text")).strip()]
    if len(set(opts_norm)) < len(opts_norm):
        issues.append("duplicate_options")
        score -= 0.12

    return list(dict.fromkeys(issues)), max(0.0, min(1.0, score)), hard_reject


def _symbolic_verification_score(text: str) -> tuple[float, list[str]]:
    frags = _extract_fragments(text, max_items=10)
    if not frags:
        return 0.75, []
    valid = sum(1 for frag in frags if _sympy_ok(frag))
    ratio = valid / max(1, len(frags))
    issues: list[str] = []
    if ratio < 0.4:
        issues.append("symbolic_verification_failed")
    elif ratio < 0.7:
        issues.append("symbolic_verification_partial")
    return max(0.0, min(1.0, ratio)), issues


def _status_from_conf(conf: float) -> str:
    if conf >= 0.85:
        return "safe"
    if conf >= 0.60:
        return "review"
    if conf >= 0.40:
        return "reject"
    return "unrecoverable"


def main() -> None:
    parser = argparse.ArgumentParser(description="Layer-7 post-validation and confidence hardening on top of Layer-4 final bank")
    parser.add_argument("--input", default="data/app/import_question_bank_final.live.json")
    parser.add_argument("--output", default="data/app/import_question_bank_layer7_final.live.json")
    parser.add_argument("--dropped-output", default="data/app/import_question_bank_layer7_dropped.live.json")
    parser.add_argument("--report", default="data/app/repair_report_layer7.live.json")
    parser.add_argument("--progress-file", default="data/app/repair_report_layer7.progress.live.json")
    parser.add_argument("--progress-every", type=int, default=400)
    parser.add_argument("--min-similarity", type=float, default=0.90)
    parser.add_argument("--delete-unusable", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    dropped_path = Path(args.dropped_output)
    report_path = Path(args.report)
    progress_path = Path(args.progress_file)

    rows = _load_json_list(input_path)
    total = len(rows)
    started_at = _now_iso()
    t0 = time.time()

    # High-trust references for similarity recovery.
    refs: list[dict[str, Any]] = []
    for row in rows:
        if _to_str(row.get("repair_status")).lower() != "safe":
            continue
        if _to_float(row.get("repair_confidence"), 0.0) < 0.90:
            continue
        q = _to_str(row.get("question_text"))
        if q and q not in {x.get("question_text") for x in refs}:
            refs.append(
                {
                    "question_text": q,
                    "chapter": _to_str(row.get("chapter")),
                    "subject": _to_str(row.get("subject")),
                    "options": _normalize_option_list(row.get("options")),
                }
            )

    finalized: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    similarity_recovered = 0

    _atomic_write_json(
        progress_path,
        {
            "stage": "layer7_postvalidate",
            "status": "running",
            "started_at": started_at,
            "done": 0,
            "total": total,
            "progress_pct": 0.0,
            "kept": 0,
            "dropped": 0,
            "similarity_recovered": 0,
        },
    )
    _atomic_write_json(output_path, finalized)
    _atomic_write_json(dropped_path, dropped)

    for idx, raw in enumerate(rows, start=1):
        row = dict(raw)
        qid = _to_str(row.get("question_id") or row.get("id") or f"row_{idx}")
        qtext = _to_str(row.get("question_text"))
        options = _normalize_option_list(row.get("options"))
        issues = [_to_str(x).strip() for x in (row.get("detected_issues") or []) if _to_str(x).strip()]
        actions = [_to_str(x).strip() for x in (row.get("repair_actions") or []) if _to_str(x).strip()]

        qtext, q_actions = _apply_layer7_rule_catalog(qtext)
        if q_actions:
            actions.extend(q_actions)

        repaired_opts: list[dict[str, str]] = []
        opt_rule_applied = False
        for opt in options:
            text = _to_str(opt.get("text"))
            t2, opt_actions = _apply_layer7_rule_catalog(text)
            if opt_actions:
                opt_rule_applied = True
                for tok in opt_actions:
                    actions.append(f"layer7_option_{tok}")
            repaired_opts.append({"label": _to_str(opt.get("label")).upper() or "A", "text": t2})
        if opt_rule_applied:
            actions.append("layer7_option_rule_catalog")
        options = repaired_opts

        qlow = qtext.lower()
        is_list_match = "list-i" in qlow and bool(re.search(r"\([pqrs]\)", qlow))
        if is_list_match:
            row["type"] = "LIST_MATCH"
            actions.append("layer7_list_match_classified")
        elif "list-i" in qlow and not is_list_match:
            issues.append("truncated_list_match")

        ast_frags = _extract_fragments(qtext)
        ast_valid = sum(1 for frag in ast_frags if _sympy_ok(frag))
        ast_score = 1.0 if not ast_frags else max(0.0, min(1.0, ast_valid / max(1, len(ast_frags))))
        structure_score = 1.0
        if re.search(r"^[=+\-*/^,:;]|[=+\-*/^,:;]\s*$", qtext.strip()):
            issues.append("dangling_operator")
            structure_score -= 0.25
        if qtext.count("(") != qtext.count(")") or qtext.count("[") != qtext.count("]") or qtext.count("{") != qtext.count("}"):
            issues.append("unbalanced_brackets")
            structure_score -= 0.28
        if len(options) < 2 and _to_str(row.get("type")).upper().startswith("MCQ"):
            issues.append("missing_options")
            structure_score -= 0.20
        structure_score = max(0.0, min(1.0, structure_score))

        semantic_issues, math_score, hard_reject = _semantic_sanity_checks(qtext, options)
        if semantic_issues:
            issues.extend(semantic_issues)
            actions.append("layer7_semantic_sanity")
        symbolic_score, symbolic_issues = _symbolic_verification_score(qtext)
        if symbolic_issues:
            issues.extend(symbolic_issues)
            actions.append("layer7_symbolic_verification")

        token_quality = 1.0
        if re.search(r"\b(?:CD|IF|FN|SR|LT)\d{2}-\d{4}\b", qtext, flags=re.IGNORECASE):
            token_quality -= 0.30
        if re.search(r"\bwww\.[^\s]+\b", qtext, flags=re.IGNORECASE):
            token_quality -= 0.25
        if _ocr_number_merge_suspect(qtext):
            issues.append("ocr_merged_coefficient_suspect")
            token_quality -= 0.20
            # Similarity recovery on suspicious OCR merges.
            best_score = 0.0
            best_ref: dict[str, Any] | None = None
            for ref in refs:
                if ref.get("chapter") and _to_str(ref.get("chapter")) != _to_str(row.get("chapter")):
                    continue
                score = _char_trigram_cosine(_norm(qtext), _norm(_to_str(ref.get("question_text"))))
                if score > best_score:
                    best_score = score
                    best_ref = ref
            if best_ref is not None and best_score >= float(args.min_similarity):
                qtext = _to_str(best_ref.get("question_text"))
                if best_ref.get("options"):
                    options = list(best_ref.get("options"))
                actions.append("layer7_similarity_recovered")
                similarity_recovered += 1
                token_quality = min(1.0, token_quality + 0.15)
            else:
                rebuilt, rebuilt_ok = _attempt_ocr_merge_linear_reconstruct(qtext)
                if rebuilt_ok and rebuilt != qtext:
                    qtext = rebuilt
                    actions.append("layer7_ocr_merge_linearized")
                    token_quality = min(1.0, token_quality + 0.08)
        token_quality = max(0.0, min(1.0, token_quality))

        ai_meta = row.get("layer4_salvage")
        ai_applied = bool((ai_meta or {}).get("ai_attempted")) and bool((ai_meta or {}).get("ai_source"))
        ai_score = 0.90 if ai_applied else (0.75 if bool((ai_meta or {}).get("ai_attempted")) else 0.60)

        mult_conf = round(
            max(0.0, min(1.0, ast_score * structure_score * math_score * symbolic_score * ai_score * token_quality)),
            4,
        )
        if hard_reject:
            mult_conf = min(mult_conf, 0.35)
        status = _status_from_conf(mult_conf)
        integrity = round(max(0.0, min(1.0, mult_conf)), 4)
        publish_risk = round(max(0.0, min(1.0, 1.0 - integrity)), 4)

        row["question_text"] = qtext
        row["options"] = options
        row["repair_actions"] = list(dict.fromkeys([x for x in actions if _to_str(x).strip()]))
        row["detected_issues"] = list(dict.fromkeys([x for x in issues if _to_str(x).strip()]))
        row["repair_confidence"] = mult_conf
        row["structural_integrity_score"] = integrity
        row["repair_status"] = status
        row["publish_risk_score"] = publish_risk
        row["layer7_postvalidate"] = {
            "applied": True,
            "ast_score": round(ast_score, 4),
            "structure_score": round(structure_score, 4),
            "math_score": round(math_score, 4),
            "symbolic_score": round(symbolic_score, 4),
            "ai_score": round(ai_score, 4),
            "token_quality": round(token_quality, 4),
            "confidence_multiplicative": mult_conf,
            "hard_reject": bool(hard_reject),
            "updated_at": _now_iso(),
        }
        row["layer_results"] = {
            "layer1_text_normalization": {"applied": bool(q_actions)},
            "layer2_ocr_corruption_repair": {"issues": [x for x in issues if "ocr" in x.lower()]},
            "layer3_math_tokenizer": {"applied": True},
            "layer4_ast_parser": {"ast_score": round(ast_score, 4), "fragments": len(ast_frags), "valid": ast_valid},
            "layer5_structural_validator": {"structure_score": round(structure_score, 4)},
            "layer6_symbolic_verification": {"symbolic_score": round(symbolic_score, 4)},
            "layer7_similarity_recovery": {"recovered": "layer7_similarity_recovered" in actions},
            "layer8_ai_reconstruction": {"layer4_ai_used": bool((row.get("layer4_salvage") or {}).get("ai_attempted"))},
            "layer9_confidence_engine": {"confidence_multiplicative": mult_conf},
        }

        should_drop = bool(args.delete_unusable) and status in {"reject", "unrecoverable"}
        if should_drop:
            dropped.append(row)
        else:
            finalized.append(row)

        status_counts[status] += 1
        for tok in row.get("repair_actions") or []:
            action_counts[_to_str(tok)] += 1

        if int(args.progress_every) > 0 and (idx % int(args.progress_every) == 0 or idx == total):
            elapsed = max(0.001, time.time() - t0)
            rate = idx / elapsed
            eta_s = int((max(0, total - idx) / max(0.01, rate)))
            payload = {
                "stage": "layer7_postvalidate",
                "status": "running",
                "updated_at": _now_iso(),
                "done": idx,
                "total": total,
                "progress_pct": round((idx / max(1, total)) * 100.0, 2),
                "rows_per_s": round(rate, 2),
                "eta_s": eta_s,
                "kept": len(finalized),
                "dropped": len(dropped),
                "similarity_recovered": similarity_recovered,
                "status_counts": dict(status_counts),
            }
            _atomic_write_json(progress_path, payload)
            _atomic_write_json(output_path, finalized)
            _atomic_write_json(dropped_path, dropped)
            print(json.dumps(payload, ensure_ascii=False))

    _atomic_write_json(output_path, finalized)
    _atomic_write_json(dropped_path, dropped)

    seen = max(1, len(finalized) + len(dropped))
    avg_conf = (
        sum(_to_float(r.get("repair_confidence"), 0.0) for r in [*finalized, *dropped]) / seen
    )
    avg_integrity = (
        sum(_to_float(r.get("structural_integrity_score"), 0.0) for r in [*finalized, *dropped]) / seen
    )
    report = {
        "started_at": started_at,
        "finished_at": _now_iso(),
        "input": str(input_path),
        "output": str(output_path),
        "dropped_output": str(dropped_path),
        "rows_seen": len(rows),
        "rows_kept": len(finalized),
        "rows_dropped": len(dropped),
        "similarity_recovered": similarity_recovered,
        "status_counts": dict(status_counts),
        "avg_repair_confidence": round(avg_conf, 4),
        "avg_structural_integrity_score": round(avg_integrity, 4),
        "top_repair_actions": dict(action_counts.most_common(30)),
    }
    _atomic_write_json(report_path, report)
    _atomic_write_json(
        progress_path,
        {
            "stage": "layer7_postvalidate",
            "status": "done",
            "updated_at": _now_iso(),
            "done": len(rows),
            "total": len(rows),
            "progress_pct": 100.0,
            "kept": len(finalized),
            "dropped": len(dropped),
            "similarity_recovered": similarity_recovered,
            "output": str(output_path),
            "report": str(report_path),
        },
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
