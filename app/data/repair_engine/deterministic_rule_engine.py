from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from .expression_graph import ExpressionGraphBuilder
from .symbol_normalizer import SymbolNormalizer


@dataclass
class DeterministicRuleResult:
    question_text: str
    options: list[dict[str, str]]
    actions: list[str]
    issues: list[str]
    layer_actions: dict[str, list[str]]


class DeterministicOcrRepairEngine:
    """Deterministic, pre-AST OCR repair engine for math questions."""

    _DANGLING_RE = re.compile(r"[=+\-*/^,:;]\s*$")
    _SPLIT_OPTION_RE = re.compile(r"\(?([A-D])\)?[\).:\-]\s*", re.IGNORECASE)

    _SYMBOL_TRANSLATIONS = str.maketrans(
        {
            "≤": "<=",
            "≥": ">=",
            "≠": "!=",
            "≈": "~=",
            "∈": " in ",
            "∉": " notin ",
            "⊂": " subset ",
            "⊆": " subseteq ",
            "⊃": " supset ",
            "⊇": " supseteq ",
            "∪": " union ",
            "∩": " intersection ",
            "→": "->",
            "∞": "infinity",
            "×": "*",
            "⋅": "*",
            "·": "*",
        }
    )

    _CATEGORY_2_MATHBB_RULES: list[tuple[str, str]] = [
        (r"\\mathbb\{\s*\\mathbbfrac\((R|N|Z|Q|C)\)\s*\}", r"\\mathbb{\1}"),
        (r"\\mathbb\{\\mathbbfrac\((R|N|Z|Q|C)\)\}", r"\\mathbb{\1}"),
        (r"\\mathbb\{([RNZQC])\)", r"\\mathbb{\1}"),
        (r"\\mathbbfrac\((R|N|Z|Q|C)\)", r"\\mathbb{\1}"),
        (r"\\mathbbfrac\{([RNZQC])\}", r"\\mathbb{\1}"),
        (r"\\mathbb\{\\mathbb\{([RNZQC])\}\}", r"\\mathbb{\1}"),
        (r"ℝ", r"\\mathbb{R}"),
        (r"ℕ", r"\\mathbb{N}"),
        (r"ℤ", r"\\mathbb{Z}"),
        (r"ℚ", r"\\mathbb{Q}"),
        (r"ℂ", r"\\mathbb{C}"),
        (r"\bmathbbR\b", r"\\mathbb{R}"),
        (r"\bmathbbN\b", r"\\mathbb{N}"),
        (r"\bmathbbZ\b", r"\\mathbb{Z}"),
        (r"\bmathbbQ\b", r"\\mathbb{Q}"),
        (r"\bmathbbC\b", r"\\mathbb{C}"),
    ]

    _CATEGORY_3_FRACTION_RULES: list[tuple[str, str]] = [
        (r"\bmathbbfrac\b", "frac"),
        (r"\btextfrac\b", "frac"),
        (r"\btexfrac\b", "frac"),
        (r"\bfracfrac\b", "frac"),
        (r"\\frac\s+([A-Za-z0-9+\-*/^()]+)\s+([A-Za-z0-9+\-*/^()]+)", r"\\frac{\1}{\2}"),
        (r"\bfrac\s+([A-Za-z0-9+\-*/^()]+)\s+([A-Za-z0-9+\-*/^()]+)", r"(\1)/(\2)"),
    ]

    _CATEGORY_4_MATRIX_RULES: list[tuple[str, str]] = [
        (r"\\beginfrac\(pmatrix\)", r"\\begin{pmatrix}"),
        (r"\\endfrac\(pmatrix\)", r"\\end{pmatrix}"),
        (r"\bbegin\(pmatrix\)", r"\\begin{pmatrix}"),
        (r"\bend\(pmatrix\)", r"\\end{pmatrix}"),
        (r"(?<!\\begin\{)pmatrix\)", r"\\end{pmatrix}"),
        (r"\badj\s+([A-Za-z])\b", r"adj(\1)"),
        (r"\bdet\s+([A-Za-z])\b", r"det(\1)"),
        (r"\btr\s+([A-Za-z])\b", r"tr(\1)"),
    ]

    _CATEGORY_5_LIMIT_RULES: list[tuple[str, str]] = [
        (r"\b1im\b", "lim"),
        (r"\b1imx\b", "lim x"),
        (r"\blimx\b", "lim x"),
        (r"\blim\s+([A-Za-z])\s*[-=]*>\s*([A-Za-z0-9+\-]+)", r"lim_{\1->\2}"),
        (r"\blim\s+([A-Za-z])\s*->\s*([0-9]+)\s*\+\b", r"lim_{\1->\2+}"),
        (r"\blim\s+([A-Za-z])\s*->\s*([0-9]+)\s*-\b", r"lim_{\1->\2-}"),
        (r"\b([A-Za-z])\s*->\s*infinity\b", r"\1->infinity"),
        (r"->\s*∞", "->infinity"),
    ]

    _CATEGORY_6_MULT_RULES: list[tuple[str, str]] = [
        (r"\b([A-Za-z])\s+\1\b", r"\1^2"),
        (r"\b([A-Za-z])\s+\1\s+\1\b", r"\1^3"),
        (r"\b([A-Za-z])([2-9])\b", r"\1^\2"),
        (r"(?<![A-Za-z0-9_])(\d+)\s*([A-Za-z])\b", r"\1*\2"),
        (r"\b([A-Za-z])\s*\(\s*([^)]+)\s*\)", r"\1(\2)"),
        (r"(?<![A-Za-z0-9_])(\d+)\s*\(", r"\1*("),
        (r"(?<!\([A-Da-d])\)\s*([xyzntijkm0-9])", r")*\1"),
        (r"\b([A-Za-z])\s+([A-Za-z])\b", r"\1*\2"),
    ]

    _CATEGORY_7_SUM_PROD_RULES: list[tuple[str, str]] = [
        (r"∑", "sum"),
        (r"∏", "prod"),
        (r"\bsummation\b", "sum"),
        (r"\bsum\s+([irknj])\s*=\s*([0-9]+)\s+([A-Za-z0-9]+)\b", r"sum_{\1=\2}^{\3}"),
        (r"\bprod\s+([irknj])\s*=\s*([0-9]+)\s+([A-Za-z0-9]+)\b", r"prod_{\1=\2}^{\3}"),
        (r"\bmin\s*,\s*([ij])\b", r"min(\1)"),
        (r"\bmax\s*,\s*([ij])\b", r"max(\1)"),
    ]

    _CATEGORY_8_SET_RULES: list[tuple[str, str]] = [
        (r"\bx\s*R\s*y\b", "x R y"),
        (r"\(\s*([A-Za-z]),\s*([A-Za-z])\s*\)\s*in\s*R\b", r"(\1,\2) in \\mathbb{R}"),
        (r"\bbelongs\b", "in"),
        (r"\bnotin\b", "notin"),
        (r"\bunion\b", "union"),
        (r"\bintersection\b", "intersection"),
        (r"\bsubseteq\b", "subseteq"),
        (r"\bsubset\b", "subset"),
        (r"\bsupseteq\b", "supseteq"),
        (r"\bsupset\b", "supset"),
    ]

    _CATEGORY_9_LINEAR_ALG_RULES: list[tuple[str, str]] = [
        (r"\badj\s+([A-Za-z])\b", r"adj(\1)"),
        (r"\bdet\s+([A-Za-z])\b", r"det(\1)"),
        (r"\btrace\s+([A-Za-z])\b", r"tr(\1)"),
        (r"\btr\s+([A-Za-z])\b", r"tr(\1)"),
    ]

    _STRUCTURAL_ARTIFACTS = re.compile(
        r"(?i)\b(?:www\.[\w.-]+|jee\s*main|jee\s*advanced|allen|resonance|fiitjee|aakash|arihant|cengage|"
        r"ans\.?|que\.?|solution|page\s*\d+|cd\d{2,4}-\d+|if\d{2,4}-\d+|fn\d{2,4}-\d+|sr\d{2,4}-\d+|lt\d{2,4}-\d+)\b"
    )

    _SET_CONTEXT_RE = re.compile(r"(?i)\b(?:domain|codomain|range|set|belongs|in)\s*[:=]?\s*([RNZQC])\b")

    def __init__(
        self,
        *,
        symbol_normalizer: SymbolNormalizer | None = None,
        graph_builder: ExpressionGraphBuilder | None = None,
    ) -> None:
        self._symbol_normalizer = symbol_normalizer or SymbolNormalizer()
        self._graph_builder = graph_builder or ExpressionGraphBuilder()

    def repair(
        self,
        *,
        question_text: str,
        options: list[dict[str, Any]],
        question_type: str = "",
    ) -> DeterministicRuleResult:
        q_text = self._to_str(question_text)
        norm_options = self._normalize_options(options)
        actions: list[str] = []
        issues: list[str] = []
        layer_actions: dict[str, list[str]] = {}

        q_text, norm_options, step_actions = self._layer_1_symbol_normalization(q_text, norm_options)
        self._merge_layer_actions("layer1_symbol_normalization", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_2_MATHBB_RULES, "mathbb_set_repair"
        )
        q_text = self._normalize_set_context_symbols(q_text)
        self._merge_layer_actions("layer2_mathbb_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_3_FRACTION_RULES, "fraction_repair"
        )
        self._merge_layer_actions("layer3_fraction_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_4_MATRIX_RULES, "matrix_repair"
        )
        q_text, matrix_actions = self._repair_numeric_matrix_layout(q_text)
        step_actions.extend(matrix_actions)
        self._merge_layer_actions("layer4_matrix_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_5_LIMIT_RULES, "limit_repair"
        )
        self._merge_layer_actions("layer5_limit_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_6_MULT_RULES, "multiplication_repair"
        )
        q_text, trig_actions = self._repair_trig_compaction(q_text)
        step_actions.extend(trig_actions)
        self._merge_layer_actions("layer6_multiplication_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_7_SUM_PROD_RULES, "sum_product_repair"
        )
        self._merge_layer_actions("layer7_sum_product_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_8_SET_RULES, "set_notation_repair"
        )
        q_text, brace_actions = self._balance_set_braces(q_text)
        step_actions.extend(brace_actions)
        self._merge_layer_actions("layer8_set_notation_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions = self._apply_text_layer(
            q_text, norm_options, self._CATEGORY_9_LINEAR_ALG_RULES, "linear_algebra_repair"
        )
        self._merge_layer_actions("layer9_linear_algebra_repair", step_actions, actions, layer_actions)

        q_text, norm_options, step_actions, structural_issues = self._layer_10_structural_repair(
            q_text, norm_options, question_type
        )
        self._merge_layer_actions("layer10_structural_repair", step_actions, actions, layer_actions)
        issues.extend(structural_issues)

        validation_issues = self._post_rule_validation(q_text, norm_options, question_type)
        for token in validation_issues:
            if token not in issues:
                issues.append(token)

        return DeterministicRuleResult(
            question_text=re.sub(r"\s+", " ", q_text).strip(),
            options=norm_options,
            actions=list(dict.fromkeys(actions)),
            issues=list(dict.fromkeys(issues)),
            layer_actions=layer_actions,
        )

    def _layer_1_symbol_normalization(
        self,
        question_text: str,
        options: list[dict[str, str]],
    ) -> tuple[str, list[dict[str, str]], list[str]]:
        actions: list[str] = []
        norm_q = self._symbol_normalizer.normalize_text(question_text)
        out_q = norm_q.text.translate(self._SYMBOL_TRANSLATIONS)
        if out_q != question_text or norm_q.actions:
            actions.extend(norm_q.actions)
            actions.append("symbol_translation_normalized")

        out_options: list[dict[str, str]] = []
        for opt in options:
            norm_opt = self._symbol_normalizer.normalize_text(opt.get("text"))
            opt_text = norm_opt.text.translate(self._SYMBOL_TRANSLATIONS)
            out_options.append({"label": opt.get("label", "A"), "text": opt_text})
            if norm_opt.actions:
                actions.append("option_symbol_normalized")
        return out_q, out_options, list(dict.fromkeys(actions))

    def _apply_text_layer(
        self,
        question_text: str,
        options: list[dict[str, str]],
        rules: list[tuple[str, str]],
        action_prefix: str,
    ) -> tuple[str, list[dict[str, str]], list[str]]:
        q_text = question_text
        actions: list[str] = []
        for idx, (pat, repl) in enumerate(rules, start=1):
            nxt, n = re.subn(pat, repl, q_text, flags=re.IGNORECASE)
            if n > 0:
                q_text = nxt
                actions.append(f"{action_prefix}_{idx}")

        out_options: list[dict[str, str]] = []
        option_action_applied = False
        for opt in options:
            text = opt.get("text", "")
            out_text = text
            for pat, repl in rules:
                out_text = re.sub(pat, repl, out_text, flags=re.IGNORECASE)
            if out_text != text:
                option_action_applied = True
            out_options.append({"label": opt.get("label", "A"), "text": out_text})
        if option_action_applied:
            actions.append(f"{action_prefix}_options")

        q_text = re.sub(r"\s{2,}", " ", q_text).strip()
        out_options = [
            {"label": opt.get("label", "A"), "text": re.sub(r"\s{2,}", " ", opt.get("text", "")).strip()}
            for opt in out_options
        ]
        return q_text, out_options, list(dict.fromkeys(actions))

    def _repair_numeric_matrix_layout(self, text: str) -> tuple[str, list[str]]:
        src = self._to_str(text)
        actions: list[str] = []
        if "\\begin{pmatrix}" in src and "\\end{pmatrix}" not in src:
            src += " \\end{pmatrix}"
            actions.append("matrix_missing_end_fixed")
        if "\\end{pmatrix}" in src and "\\begin{pmatrix}" not in src:
            src = "\\begin{pmatrix} " + src
            actions.append("matrix_missing_begin_fixed")

        lines = [ln.strip() for ln in re.split(r"\r?\n", src) if ln.strip()]
        if len(lines) < 2:
            return src, actions

        numeric_rows: list[list[str]] = []
        for ln in lines:
            if re.search(r"[A-Za-z]", ln):
                numeric_rows = []
                break
            tokens = re.findall(r"-?\d+(?:\.\d+)?", ln)
            if len(tokens) >= 2:
                numeric_rows.append(tokens)

        if len(numeric_rows) >= 2:
            col_sizes = {len(row) for row in numeric_rows}
            if len(col_sizes) == 1:
                cols = next(iter(col_sizes))
                if cols >= 2:
                    matrix_body = " \\\\ ".join(" & ".join(row) for row in numeric_rows)
                    src = re.sub(
                        r"(?:\s*\d+(?:\s+\d+)+\s*){2,}",
                        f"\\begin{{pmatrix}} {matrix_body} \\end{{pmatrix}}",
                        src,
                        count=1,
                    )
                    actions.append("matrix_grid_to_pmatrix")
                    if len(numeric_rows) == cols:
                        # Square numeric matrix.
                        n = cols
                        if n > 0 and all(
                            int(float(numeric_rows[r][c])) == (1 if r == c else 0)
                            for r in range(n)
                            for c in range(n)
                        ):
                            actions.append("matrix_identity_detected")
        return src, actions

    def _repair_trig_compaction(self, text: str) -> tuple[str, list[str]]:
        out = self._to_str(text)
        actions: list[str] = []
        rules = [
            (r"\b(sin|cos|tan|cot|sec|cosec|log|ln)\s+([A-Za-z0-9])\b", r"\1(\2)"),
            (r"\b([2-9]\d*)\s*(sin|cos|tan|cot|sec|cosec|log|ln)\s*\(\s*([^)]+)\s*\)", r"\1*\2(\3)"),
            (r"\b(sin|cos|tan|cot|sec|cosec)\s*([2-9])([A-Za-z])\b", r"\1(\2*\3)"),
        ]
        for idx, (pat, repl) in enumerate(rules, start=1):
            nxt, n = re.subn(pat, repl, out, flags=re.IGNORECASE)
            if n > 0:
                out = nxt
                actions.append(f"trig_normalized_{idx}")
        return out, actions

    def _balance_set_braces(self, text: str) -> tuple[str, list[str]]:
        out = self._to_str(text)
        actions: list[str] = []
        open_curly = out.count("{")
        close_curly = out.count("}")
        if open_curly > close_curly:
            out += "}" * (open_curly - close_curly)
            actions.append("set_brace_closed")
        elif close_curly > open_curly:
            out = ("{" * (close_curly - open_curly)) + out
            actions.append("set_brace_opened")
        return out, actions

    def _layer_10_structural_repair(
        self,
        question_text: str,
        options: list[dict[str, str]],
        question_type: str,
    ) -> tuple[str, list[dict[str, str]], list[str], list[str]]:
        out_q = self._to_str(question_text)
        out_options = list(options)
        actions: list[str] = []
        issues: list[str] = []

        cleaned = self._STRUCTURAL_ARTIFACTS.sub(" ", out_q)
        cleaned = re.sub(r"\[\s*\d+\s*\]$", " ", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
        if cleaned != out_q:
            out_q = cleaned
            actions.append("structural_artifact_removed")

        if self._DANGLING_RE.search(out_q):
            out_q = re.sub(self._DANGLING_RE, "", out_q).strip()
            actions.append("structural_trailing_operator_removed")
            issues.append("dangling_operator")

        out_q, paren_actions = self._balance_parentheses(out_q)
        actions.extend(paren_actions)
        if paren_actions:
            issues.append("parenthesis_autobalanced")

        split_from_options = self._split_merged_options(out_options)
        if split_from_options is not None:
            out_options = split_from_options
            actions.append("structural_merged_options_split")

        if (not out_options or len(out_options) <= 1) and self._looks_like_inline_options(out_q):
            stem, extracted = self._extract_inline_options(out_q)
            if len(extracted) >= 2:
                out_q = stem
                out_options = extracted
                actions.append("structural_inline_options_extracted")

        out_options = self._normalize_option_labels(out_options)
        if len(out_options) >= 2:
            actions.append("structural_option_labels_normalized")

        qtype = self._to_str(question_type).upper().strip()
        if qtype.startswith("MCQ") and len(out_options) < 4:
            issues.append("mcq_options_incomplete")
        if qtype == "NUMERICAL" and out_options:
            out_options = []
            actions.append("structural_numerical_options_removed")
        if qtype == "LIST_MATCH":
            blob = out_q + " " + " ".join(opt.get("text", "") for opt in out_options)
            if not re.search(r"(?i)list[-\s]*i", blob):
                issues.append("list_match_list_i_missing")
            if not re.search(r"(?i)list[-\s]*ii", blob):
                issues.append("list_match_list_ii_missing")

        if re.search(r"(?i)\b(?:then|is|find|if)\s*$", out_q):
            issues.append("truncated_question")

        return out_q, out_options, list(dict.fromkeys(actions)), list(dict.fromkeys(issues))

    def _post_rule_validation(
        self,
        question_text: str,
        options: list[dict[str, str]],
        question_type: str,
    ) -> list[str]:
        issues: list[str] = []
        q = self._to_str(question_text)
        qtype = self._to_str(question_type).upper().strip()

        if not self._latex_braces_valid(q):
            issues.append("latex_syntax_invalid")

        ast_issues = self._ast_validation_issues(q)
        issues.extend(ast_issues)

        if self._DANGLING_RE.search(q):
            issues.append("dangling_token")

        if qtype.startswith("MCQ") and len(options) < 2:
            issues.append("question_structure_invalid")
        if qtype == "LIST_MATCH":
            blob = q + " " + " ".join(opt.get("text", "") for opt in options)
            if not re.search(r"\([PQRS]\)", blob, flags=re.IGNORECASE):
                issues.append("list_match_pairs_missing")
        return list(dict.fromkeys(issues))

    def _latex_braces_valid(self, text: str) -> bool:
        stack: list[str] = []
        pairs = {")": "(", "]": "[", "}": "{"}
        for ch in text:
            if ch in "([{":
                stack.append(ch)
            elif ch in ")]}":
                if not stack or stack[-1] != pairs[ch]:
                    return False
                stack.pop()

        begin_count = len(re.findall(r"\\begin\{", text))
        end_count = len(re.findall(r"\\end\{", text))
        return not stack and begin_count == end_count

    def _ast_validation_issues(self, text: str) -> list[str]:
        fragments = self._extract_fragments(text)
        if not fragments:
            return []
        invalid = 0
        for frag in fragments:
            graph = self._graph_builder.build(frag)
            if graph.root_id is None:
                invalid += 1
                continue
            hard = {"empty_expression", "graph_root_missing"}
            if any(token in hard for token in graph.issues):
                invalid += 1
        if invalid == 0:
            return []
        if invalid >= max(2, math.ceil(len(fragments) * 0.6)):
            return ["ast_validation_failed"]
        return ["ast_validation_partial"]

    def _extract_fragments(self, text: str, *, max_items: int = 6) -> list[str]:
        src = self._to_str(text)
        if not src:
            return []
        patterns = (
            r"lim[^.;\n]{4,160}",
            r"[A-Za-z]\s*\([^)]+\)\s*=\s*[^.;\n]{2,180}",
            r"\b(?:sum|prod)\b[^.;\n]{2,160}",
            r"\|[^|]{1,100}\|",
            r"[A-Za-z0-9\)\]]\s*[\+\-\*/\^]\s*[A-Za-z0-9\(\[]",
        )
        out: list[str] = []
        for pat in patterns:
            for m in re.finditer(pat, src, flags=re.IGNORECASE):
                frag = self._to_str(m.group(0)).strip()
                if frag and frag not in out:
                    out.append(frag[:180])
                if len(out) >= max_items:
                    return out
        return out

    def _normalize_set_context_symbols(self, text: str) -> str:
        def repl(match: re.Match[str]) -> str:
            sym = match.group(1).upper()
            return match.group(0).replace(sym, f"\\mathbb{{{sym}}}")

        out = self._SET_CONTEXT_RE.sub(repl, text)
        out = re.sub(r"\bin\s+([RNZQC])\b", lambda m: f"in \\mathbb{{{m.group(1).upper()}}}", out)
        out = re.sub(r"\bto\s+([RNZQC])\b", lambda m: f"to \\mathbb{{{m.group(1).upper()}}}", out)
        return out

    def _split_merged_options(self, options: list[dict[str, str]]) -> list[dict[str, str]] | None:
        if len(options) != 1:
            return None
        text = self._to_str(options[0].get("text"))
        matches = list(self._SPLIT_OPTION_RE.finditer(text))
        if len(matches) < 2:
            return None
        out: list[dict[str, str]] = []
        for idx, match in enumerate(matches):
            start = match.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            body = text[start:end].strip(" ;,")
            if body:
                out.append({"label": match.group(1).upper(), "text": body})
        return out if len(out) >= 2 else None

    def _looks_like_inline_options(self, question_text: str) -> bool:
        return bool(len(self._SPLIT_OPTION_RE.findall(question_text)) >= 2)

    def _extract_inline_options(self, question_text: str) -> tuple[str, list[dict[str, str]]]:
        matches = list(self._SPLIT_OPTION_RE.finditer(question_text or ""))
        if len(matches) < 2:
            return question_text, []
        stem = (question_text[: matches[0].start()] or "").strip(" ;,")
        options: list[dict[str, str]] = []
        for idx, match in enumerate(matches):
            start = match.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(question_text)
            text = (question_text[start:end] or "").strip(" ;,")
            if text:
                options.append({"label": match.group(1).upper(), "text": text})
        return stem, options

    def _balance_parentheses(self, text: str) -> tuple[str, list[str]]:
        out = self._to_str(text)
        actions: list[str] = []

        pairs = {"(": ")", "[": "]", "{": "}"}
        opens = {k: out.count(k) for k in pairs}
        closes = {v: out.count(v) for v in pairs.values()}
        for open_br, close_br in pairs.items():
            if opens[open_br] > closes[close_br]:
                out += close_br * (opens[open_br] - closes[close_br])
                actions.append(f"autobalance_added_{close_br}")
            elif closes[close_br] > opens[open_br]:
                out = (open_br * (closes[close_br] - opens[open_br])) + out
                actions.append(f"autobalance_added_{open_br}")
        return out, actions

    def _normalize_option_labels(self, options: list[dict[str, str]]) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for idx, opt in enumerate(options):
            label = self._to_str(opt.get("label")).upper()
            if not label or len(label) != 1 or not label.isalpha() or label in seen:
                label = chr(65 + min(idx, 25))
            seen.add(label)
            text = self._to_str(opt.get("text")).strip()
            if text:
                out.append({"label": label, "text": text})
        return out

    def _normalize_options(self, raw: list[dict[str, Any]] | Any) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        if isinstance(raw, list):
            for idx, item in enumerate(raw):
                if isinstance(item, dict):
                    label = self._to_str(item.get("label")).upper() or chr(65 + min(idx, 25))
                    text = self._to_str(item.get("text") or item.get("value") or item.get("option")).strip()
                else:
                    label = chr(65 + min(idx, 25))
                    text = self._to_str(item).strip()
                if text:
                    out.append({"label": label, "text": text})
        elif isinstance(raw, dict):
            items = sorted(raw.items(), key=lambda kv: self._to_str(kv[0]))
            for idx, (_, value) in enumerate(items):
                text = self._to_str(value).strip()
                if text:
                    out.append({"label": chr(65 + min(idx, 25)), "text": text})
        return out

    def _merge_layer_actions(
        self,
        layer_name: str,
        layer_step_actions: list[str],
        flat_actions: list[str],
        layer_actions: dict[str, list[str]],
    ) -> None:
        uniq = [token for token in dict.fromkeys(layer_step_actions) if token]
        if not uniq:
            return
        layer_actions[layer_name] = uniq
        flat_actions.extend([f"{layer_name}:{token}" for token in uniq])

    def _to_str(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return str(value)
