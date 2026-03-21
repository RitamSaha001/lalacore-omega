from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class ProblemProfile:
    subject: str
    difficulty: str
    numeric: bool
    multi_concept: bool
    trap_probability: float
    symbolic: bool = False
    graph_like: bool = False
    features: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RetrievedBlock:
    block_id: str
    title: str
    text: str
    score: float
    source: str = "vault"
    tags: List[str] = field(default_factory=list)


@dataclass(slots=True)
class ProviderAnswer:
    provider: str
    reasoning: str
    final_answer: str
    confidence: float
    self_critique: str = ""
    latency_s: float = 0.0
    answer_contract: Dict[str, Any] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JudgeResult:
    provider: str
    score: float
    risk: float
    rule_score: float
    critic_score: float
    calibration_risk: float
    verified: bool
    notes: List[str] = field(default_factory=list)


@dataclass(slots=True)
class SolveArtifacts:
    profile: ProblemProfile
    retrieved: List[RetrievedBlock]
    candidates: List[ProviderAnswer]
    judge_results: List[JudgeResult]
    reasoning_graph: Dict[str, Any]
    mcts_trace: List[Dict[str, Any]]
    selected_provider: str
