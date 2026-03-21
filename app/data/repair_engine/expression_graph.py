from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


TOKEN_RE = re.compile(
    r"lim|sqrt|sin|cos|tan|cot|sec|cosec|log|ln|sgn|->|<=|>=|!=|[A-Za-z_]+|\d+(?:\.\d+)?|[()+\-*/^=,{}\[\]]",
    re.IGNORECASE,
)


@dataclass
class GraphNode:
    node_id: int
    kind: str
    value: str
    children: list[int] = field(default_factory=list)


@dataclass
class ExpressionGraph:
    nodes: dict[int, GraphNode]
    root_id: int | None
    tokens: list[str]
    issues: list[str]


class ExpressionGraphBuilder:
    """Build lightweight expression graphs from OCR-repaired tokens."""

    _FUNCTIONS = {"sin", "cos", "tan", "cot", "sec", "cosec", "log", "ln", "sqrt", "sgn"}
    _OP_PRECEDENCE = {"+": 1, "-": 1, "*": 2, "/": 2, "^": 3}
    _RIGHT_ASSOC = {"^"}

    def tokenize(self, text: str) -> list[str]:
        if not text:
            return []
        tokens = TOKEN_RE.findall(text)
        return [tok for tok in tokens if tok and tok.strip()]

    def build(self, text: str) -> ExpressionGraph:
        tokens = self.tokenize(text)
        if not tokens:
            return ExpressionGraph(nodes={}, root_id=None, tokens=[], issues=["empty_expression"])
        if not any(op in tokens for op in self._OP_PRECEDENCE.keys()):
            # For paragraph-style text, keep a flat graph so downstream stages still get structure.
            nodes: dict[int, GraphNode] = {}
            parent_id = 1
            nodes[parent_id] = GraphNode(node_id=parent_id, kind="sequence", value="sequence", children=[])
            for idx, tok in enumerate(tokens, start=2):
                nodes[idx] = GraphNode(node_id=idx, kind="token", value=tok, children=[])
                nodes[parent_id].children.append(idx)
            return ExpressionGraph(nodes=nodes, root_id=parent_id, tokens=tokens, issues=[])

        rpn, issues = self._to_rpn(tokens)
        if not rpn:
            return ExpressionGraph(nodes={}, root_id=None, tokens=tokens, issues=issues or ["rpn_empty"])
        nodes, root_id, build_issues = self._rpn_to_graph(rpn)
        all_issues = [*issues, *build_issues]
        return ExpressionGraph(nodes=nodes, root_id=root_id, tokens=tokens, issues=all_issues)

    def _to_rpn(self, tokens: list[str]) -> tuple[list[str], list[str]]:
        out: list[str] = []
        stack: list[str] = []
        issues: list[str] = []

        for tok in tokens:
            low = tok.lower()
            if self._is_number(tok) or self._is_variable(tok):
                out.append(tok)
                continue
            if low in self._FUNCTIONS:
                stack.append(low)
                continue
            if tok == ",":
                while stack and stack[-1] != "(":
                    out.append(stack.pop())
                continue
            if tok in self._OP_PRECEDENCE:
                while stack and stack[-1] in self._OP_PRECEDENCE:
                    top = stack[-1]
                    if (
                        self._OP_PRECEDENCE[top] > self._OP_PRECEDENCE[tok]
                        or (
                            self._OP_PRECEDENCE[top] == self._OP_PRECEDENCE[tok]
                            and tok not in self._RIGHT_ASSOC
                        )
                    ):
                        out.append(stack.pop())
                    else:
                        break
                stack.append(tok)
                continue
            if tok in {"(", "[", "{"}:
                stack.append(tok)
                continue
            if tok in {")", "]", "}"}:
                open_tok = {"}": "{", "]": "[", ")": "("}[tok]
                while stack and stack[-1] != open_tok:
                    out.append(stack.pop())
                if not stack:
                    issues.append("unbalanced_parenthesis")
                    continue
                stack.pop()
                if stack and stack[-1] in self._FUNCTIONS:
                    out.append(stack.pop())
                continue
            if tok in {"=", "->", "<=", ">=", "!="}:
                out.append(tok)
                continue
            issues.append(f"unknown_token:{tok}")

        while stack:
            top = stack.pop()
            if top in {"(", ")"}:
                issues.append("unbalanced_parenthesis")
                continue
            out.append(top)
        return out, issues

    def _rpn_to_graph(self, rpn: list[str]) -> tuple[dict[int, GraphNode], int | None, list[str]]:
        nodes: dict[int, GraphNode] = {}
        stack: list[int] = []
        issues: list[str] = []
        next_id = 1

        def push_node(kind: str, value: str, children: list[int]) -> int:
            nonlocal next_id
            node_id = next_id
            next_id += 1
            nodes[node_id] = GraphNode(node_id=node_id, kind=kind, value=value, children=children)
            return node_id

        for tok in rpn:
            low = tok.lower()
            if self._is_number(tok) or self._is_variable(tok):
                stack.append(push_node("atom", tok, []))
                continue
            if low in self._FUNCTIONS:
                if not stack:
                    issues.append(f"missing_operand:{tok}")
                    continue
                child = stack.pop()
                stack.append(push_node("function", low, [child]))
                continue
            if tok in {"+", "-", "*", "/", "^"}:
                if len(stack) < 2:
                    issues.append(f"missing_operand:{tok}")
                    continue
                right = stack.pop()
                left = stack.pop()
                stack.append(push_node("operator", tok, [left, right]))
                continue
            if tok in {"=", "->", "<=", ">=", "!="}:
                if len(stack) >= 2:
                    right = stack.pop()
                    left = stack.pop()
                    stack.append(push_node("relation", tok, [left, right]))
                elif len(stack) == 1:
                    left = stack.pop()
                    stack.append(push_node("relation", tok, [left]))
                else:
                    stack.append(push_node("relation", tok, []))
                continue
            issues.append(f"unknown_rpn_token:{tok}")

        if not stack:
            return nodes, None, [*issues, "graph_root_missing"]
        if len(stack) > 1:
            # Keep the last node as root and attach residual nodes.
            root = stack[-1]
            leftovers = stack[:-1]
            seq = push_node("sequence", "sequence", [*leftovers, root])
            return nodes, seq, [*issues, "expression_fragmented"]
        return nodes, stack[0], issues

    def _is_number(self, token: str) -> bool:
        return bool(re.fullmatch(r"\d+(?:\.\d+)?", token))

    def _is_variable(self, token: str) -> bool:
        return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token))

    def to_dict(self, graph: ExpressionGraph) -> dict[str, Any]:
        return {
            "root_id": graph.root_id,
            "tokens": list(graph.tokens),
            "issues": list(graph.issues),
            "nodes": [
                {
                    "node_id": node.node_id,
                    "kind": node.kind,
                    "value": node.value,
                    "children": list(node.children),
                }
                for node in graph.nodes.values()
            ],
        }
