from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return float(fallback)


def _norm_text(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


_SUBJECT_CONCEPT_PATTERNS: Dict[str, Dict[str, Sequence[str]]] = {
    "math": {
        "algebra": (
            r"\bcoefficient[s]?\b",
            r"\bexpansion\b",
            r"\bbinomial\b",
            r"\bpolynomial\b",
            r"\broot[s]?\b",
            r"\bequation[s]?\b",
            r"\bx\^\d+\b",
        ),
        "combinatorics": (
            r"\bnumber of\b",
            r"\bfour[- ]digit\b",
            r"\bformed using digits\b",
            r"\barrangement[s]?\b",
            r"\bpermutation[s]?\b",
            r"\bcombination[s]?\b",
            r"\bncr\b",
            r"\bnpr\b",
            r"\bstrictly greater\b",
            r"\bdistinct\b",
        ),
        "probability": (
            r"\bprobability\b",
            r"\brandom\b",
            r"\bexpected value\b",
            r"\bchance\b",
        ),
        "calculus": (
            r"\bderivative\b",
            r"\bintegral\b",
            r"\blimit\b",
            r"\bcontinuity\b",
            r"\bdifferentiable\b",
        ),
        "number_theory": (
            r"\bprime\b",
            r"\bgcd\b",
            r"\blcm\b",
            r"\bmod\b",
            r"\bdivisible\b",
            r"\binteger[s]?\b",
        ),
        "geometry": (
            r"\btriangle\b",
            r"\bcircle\b",
            r"\bangle\b",
            r"\bradius\b",
            r"\barea\b",
            r"\bvolume\b",
        ),
    },
    "physics": {
        "mechanics": (
            r"\bvelocity\b",
            r"\bacceleration\b",
            r"\bforce\b",
            r"\bnewton\b",
            r"\bkinematic[s]?\b",
        ),
        "electricity": (
            r"\bcurrent\b",
            r"\bvoltage\b",
            r"\bresistance\b",
            r"\bcircuit\b",
            r"\bcapacit",
        ),
        "thermo": (
            r"\bentropy\b",
            r"\benthalpy\b",
            r"\bheat\b",
            r"\btemperature\b",
        ),
        "waves": (
            r"\bfrequency\b",
            r"\bwavelength\b",
            r"\binterference\b",
        ),
    },
    "chemistry": {
        "organic": (
            r"\bmechanism\b",
            r"\be1\b",
            r"\be2\b",
            r"\bsn1\b",
            r"\bsn2\b",
            r"\balkene\b",
            r"\bcarbocation\b",
        ),
        "physical": (
            r"\bequilibrium\b",
            r"\bkp\b",
            r"\bkc\b",
            r"\bpka\b",
            r"\bthermodynamic\b",
        ),
        "inorganic": (
            r"\bcoordination\b",
            r"\bcomplex\b",
            r"\bligand\b",
            r"\boxidation\b",
        ),
    },
    "biology": {
        "genetics": (
            r"\bgenotype\b",
            r"\bphenotype\b",
            r"\ballele\b",
            r"\bdominant\b",
        ),
        "cell_biology": (
            r"\bmitosis\b",
            r"\bmeiosis\b",
            r"\bcell\b",
            r"\borganelle\b",
        ),
    },
}


_GENERIC_COMPLEXITY_PATTERNS: Sequence[Tuple[str, float]] = (
    (r"\bif\b", 0.40),
    (r"\bstrictly\b", 0.55),
    (r"\bat least\b", 0.60),
    (r"\bat most\b", 0.60),
    (r"\bexactly\b", 0.55),
    (r"\bdistinct\b", 0.55),
    (r"\bnon[- ]zero\b", 0.40),
    (r"\bpositive integer\b", 0.50),
    (r"\bprove\b", 0.75),
    (r"\bderive\b", 0.75),
)


_CONCEPT_DIFFICULTY_PRIOR: Dict[str, float] = {
    "combinatorics": 1.40,
    "probability": 1.30,
    "number_theory": 1.20,
    "calculus": 1.40,
    "geometry": 1.05,
    "algebra": 1.15,
    "organic": 1.20,
    "physical": 1.20,
}


@dataclass
class ClassificationOutcome:
    question: str
    subject: str
    difficulty: str
    concept_cluster: List[str]
    confidence: float
    metadata: Dict[str, Any]

    def to_feeder_payload(self, source_tag: str = "raw_auto") -> Dict[str, Any]:
        return {
            "question": self.question,
            "subject": self.subject,
            "difficulty": self.difficulty,
            "concept_cluster": list(self.concept_cluster),
            "source_tag": str(source_tag),
        }


class AdaptiveQuestionClassifier:
    """
    Dynamic/adaptive classifier for raw questions.

    Adaptation signal is learned from existing feeder/replay outcomes.
    This is additive only: no changes to solver, feeder, or arena logic.
    """

    def __init__(
        self,
        *,
        feeder_cases_path: str = "data/lc9/LC9_FEEDER_CASES.jsonl",
        replay_cases_path: str = "data/replay/feeder_cases.jsonl",
        queue_path: str = "data/lc9/LC9_FEEDER_QUEUE.jsonl",
        state_path: str = "data/lc9/LC9_ADAPTIVE_CLASSIFIER_STATE.json",
        max_history_rows: int = 25000,
    ):
        self.feeder_cases_path = Path(feeder_cases_path)
        self.replay_cases_path = Path(replay_cases_path)
        self.queue_path = Path(queue_path)
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_history_rows = max(500, int(max_history_rows))

        self.state = self._load_state()
        self.refresh()

    def refresh(self) -> None:
        profile = self._build_adaptive_profile()
        self.state["profile"] = profile
        self.state["updated_ts"] = _utc_now()
        self._save_state()

    def classify(self, question: str) -> ClassificationOutcome:
        q = str(question or "").strip()
        if not q:
            raise ValueError("question cannot be empty")

        text = _norm_text(q)
        tokens = re.findall(r"[a-z0-9\^\+\-\*/=]+", text)
        token_count = len(tokens)
        number_count = len(re.findall(r"\d+", text))
        op_count = len(re.findall(r"[\+\-\*/\^=]", text))

        subject_scores: Dict[str, float] = {k: 0.0 for k in _SUBJECT_CONCEPT_PATTERNS}
        concept_scores: Dict[str, float] = {}
        match_signals: List[str] = []

        for subject, concept_map in _SUBJECT_CONCEPT_PATTERNS.items():
            for concept, patterns in concept_map.items():
                hits = 0
                for pattern in patterns:
                    if re.search(pattern, text):
                        hits += 1
                if hits <= 0:
                    continue
                concept_scores[concept] = concept_scores.get(concept, 0.0) + float(hits)
                subject_scores[subject] += 1.10 * float(hits)
                match_signals.append(f"{subject}:{concept}:{hits}")

        if op_count >= 2:
            subject_scores["math"] = subject_scores.get("math", 0.0) + 0.8
        if re.search(r"\b(m\/s|kg|newton|joule|volt|ampere)\b", text):
            subject_scores["physics"] = subject_scores.get("physics", 0.0) + 1.0
        if re.search(r"\b(mole|ph|orbital|reaction)\b", text):
            subject_scores["chemistry"] = subject_scores.get("chemistry", 0.0) + 1.0

        profile = self.state.get("profile", {})
        subject_pressure = profile.get("subject_pressure", {})
        concept_pressure = profile.get("concept_pressure", {})
        adaptive_global = _safe_float(profile.get("global_pressure", 0.0), 0.0)

        for subject in subject_scores:
            pressure = _safe_float(subject_pressure.get(subject, 0.0), 0.0)
            subject_scores[subject] += 0.45 * pressure

        if not any(value > 0 for value in subject_scores.values()):
            from core.lalacore_x.classifier import ProblemClassifier

            fallback = ProblemClassifier().classify(q)
            concept_cluster = ["unclassified"]
            difficulty = fallback.difficulty
            return ClassificationOutcome(
                question=q,
                subject=fallback.subject,
                difficulty=difficulty,
                concept_cluster=concept_cluster,
                confidence=0.40,
                metadata={
                    "mode": "fallback_problem_classifier",
                    "subject_scores": subject_scores,
                    "adaptive_profile_samples": int(profile.get("samples", 0)),
                },
            )

        ranked_subjects = sorted(subject_scores.items(), key=lambda item: item[1], reverse=True)
        subject = ranked_subjects[0][0]
        subject_gap = ranked_subjects[0][1] - (ranked_subjects[1][1] if len(ranked_subjects) > 1 else 0.0)

        ranked_concepts = sorted(concept_scores.items(), key=lambda item: item[1], reverse=True)
        concept_cluster: List[str] = []
        for concept, score in ranked_concepts:
            if score < 1.0:
                continue
            concept_cluster.append(concept)
            if len(concept_cluster) >= 6:
                break

        if not concept_cluster:
            concept_cluster = ["general_reasoning"]

        complexity_score = 0.0
        complexity_score += 0.015 * min(token_count, 200)
        complexity_score += 0.10 * min(number_count, 12)
        complexity_score += 0.08 * min(op_count, 12)
        complexity_score += 0.55 * max(0.0, subject_scores.get(subject, 0.0))
        complexity_score += 0.35 * adaptive_global

        for pattern, weight in _GENERIC_COMPLEXITY_PATTERNS:
            if re.search(pattern, text):
                complexity_score += float(weight)

        for concept in concept_cluster:
            complexity_score += float(_CONCEPT_DIFFICULTY_PRIOR.get(concept, 0.65))
            complexity_score += 0.60 * _safe_float(concept_pressure.get(concept, 0.0), 0.0)

        if token_count > 48:
            complexity_score += 0.60
        if token_count > 80:
            complexity_score += 0.70
        if re.search(r"\b(therefore|hence|show that|determine)\b", text):
            complexity_score += 0.45

        if complexity_score >= 8.2:
            difficulty = "hard"
        elif complexity_score >= 4.8:
            difficulty = "medium"
        else:
            difficulty = "easy"

        confidence = _clamp(0.35 + 0.08 * subject_gap + 0.05 * len(concept_cluster), 0.30, 0.98)

        return ClassificationOutcome(
            question=q,
            subject=subject,
            difficulty=difficulty,
            concept_cluster=concept_cluster,
            confidence=confidence,
            metadata={
                "mode": "adaptive",
                "subject_scores": {k: round(v, 6) for k, v in ranked_subjects},
                "concept_scores": {k: round(v, 6) for k, v in ranked_concepts},
                "complexity_score": round(complexity_score, 6),
                "token_count": token_count,
                "number_count": number_count,
                "operator_count": op_count,
                "signals": match_signals[:20],
                "adaptive_profile_samples": int(profile.get("samples", 0)),
                "adaptive_global_pressure": round(adaptive_global, 6),
            },
        )

    def classify_many(self, questions: Sequence[str]) -> List[ClassificationOutcome]:
        out = []
        for question in questions:
            q = str(question or "").strip()
            if not q:
                continue
            out.append(self.classify(q))
        return out

    def _load_state(self) -> Dict[str, Any]:
        base: Dict[str, Any] = {
            "version": 1,
            "updated_ts": None,
            "profile": {
                "samples": 0,
                "global_pressure": 0.0,
                "subject_pressure": {},
                "concept_pressure": {},
            },
        }
        if not self.state_path.exists():
            return base
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                base.update(payload)
        except Exception:
            pass
        return base

    def _save_state(self) -> None:
        self.state_path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")

    def _read_jsonl(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
        if len(rows) > self.max_history_rows:
            rows = rows[-self.max_history_rows :]
        return rows

    def _build_adaptive_profile(self) -> Dict[str, Any]:
        feeder_rows = self._read_jsonl(self.feeder_cases_path)
        replay_rows = self._read_jsonl(self.replay_cases_path)
        queue_rows = self._read_jsonl(self.queue_path)

        samples = 0
        global_sum = 0.0
        subject_stat: Dict[str, Dict[str, float]] = {}
        concept_stat: Dict[str, Dict[str, float]] = {}

        def update_stats(
            *,
            subject: str,
            concepts: Iterable[str],
            risk: float,
            verified: bool,
            weight: float,
        ) -> None:
            nonlocal samples, global_sum
            subject = str(subject or "general").strip().lower() or "general"
            challenge = _clamp(0.65 * float(risk) + 0.35 * (0.0 if bool(verified) else 1.0), 0.0, 1.0)
            challenge *= float(weight)
            samples += 1
            global_sum += challenge

            srow = subject_stat.setdefault(subject, {"count": 0.0, "sum": 0.0})
            srow["count"] += 1.0
            srow["sum"] += challenge

            for concept in concepts:
                c = str(concept or "").strip().lower()
                if not c:
                    continue
                crow = concept_stat.setdefault(c, {"count": 0.0, "sum": 0.0})
                crow["count"] += 1.0
                crow["sum"] += challenge

        for row in feeder_rows:
            summary = row
            subject = str(summary.get("subject", "general"))
            concepts = list(summary.get("concept_cluster", []))
            risk = _safe_float(summary.get("risk", 1.0), 1.0)
            verified = bool(summary.get("verified", False))
            update_stats(subject=subject, concepts=concepts, risk=risk, verified=verified, weight=1.0)

        for row in replay_rows:
            subject = str(row.get("subject", "general"))
            concepts = list(row.get("concept_clusters", []))
            risk = _safe_float(row.get("risk", 1.0), 1.0)
            verified = bool(row.get("verified", False))
            update_stats(subject=subject, concepts=concepts, risk=risk, verified=verified, weight=0.9)

        for row in queue_rows:
            if str(row.get("status", "")) != "Completed":
                continue
            summary = row.get("result_summary", {}) if isinstance(row.get("result_summary"), dict) else {}
            subject = str(row.get("subject", "general"))
            concepts = list(row.get("concept_cluster", []))
            risk = _safe_float(summary.get("risk", 1.0), 1.0)
            verified = bool(summary.get("verified", False))
            update_stats(subject=subject, concepts=concepts, risk=risk, verified=verified, weight=0.7)

        subject_pressure = {}
        for subject, stat in subject_stat.items():
            count = max(1.0, stat["count"])
            pressure = stat["sum"] / count
            subject_pressure[subject] = round(_clamp(pressure, 0.0, 1.0), 6)

        concept_pressure = {}
        for concept, stat in concept_stat.items():
            count = max(1.0, stat["count"])
            pressure = stat["sum"] / count
            concept_pressure[concept] = round(_clamp(pressure, 0.0, 1.0), 6)

        global_pressure = round(_clamp(global_sum / max(1, samples), 0.0, 1.0), 6)
        return {
            "samples": int(samples),
            "global_pressure": global_pressure,
            "subject_pressure": subject_pressure,
            "concept_pressure": concept_pressure,
        }
