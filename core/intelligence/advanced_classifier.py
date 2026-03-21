from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from core.intelligence.bfs_engine import ConceptBFSEngine
from core.intelligence.concept_graph_generator import ConceptGraphGenerator
from core.intelligence.dynamic_edge_updater import DynamicEdgeUpdater
from core.intelligence.edge_builder import EdgeBuilder
from core.intelligence.structural_patterns import StructuralPatternDetector
from core.intelligence.syllabus_graph import build_syllabus_hierarchy
from core.intelligence.trap_learning_engine import TrapLearningEngine


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _norm(text)).strip("_") or "na"


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _uniq(values: Iterable[str]) -> List[str]:
    out = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


_HARD_UNIT_ANCHOR_RULES: List[Tuple[str, str, str]] = [
    # Mathematics
    ("Mathematics", "Integral Calculus and Differential Equations", r"(\\int)|\bintegral\b"),
    ("Mathematics", "Limits, Continuity and Differentiability", r"(\\lim)|\blimit\b"),
    ("Mathematics", "Matrices and Determinants", r"\bmatrix\b|\bmatrices\b|\bdet\b|\brank\b"),
    ("Mathematics", "Coordinate Geometry", r"\bhyperbola\b|\bellipse\b|\bparabola\b|\bfoci\b|\bfocus\b"),
    ("Mathematics", "Vector Algebra", r"\bvector\b|\bdot\b|\bcross\b"),
    ("Mathematics", "Probability and Statistics", r"\bprobability\b|\brandom\b|\bdistribution\b"),
    ("Mathematics", "Complex Numbers and Quadratic Equations", r"\bargand\b|\bcomplex numbers?\b|\bi\^2\b|\bimaginary\b|\bmodulus\b|\bargument\b"),
    ("Mathematics", "Binomial Theorem and Sequence & Series", r"\bbinomial\b|\bcoefficient\b|\bgeneral term\b"),
    # Physics
    ("Physics", "Electrostatics and Capacitance", r"\bgauss\b|\belectric field\b|\bcapacitance\b"),
    ("Physics", "Laws of Motion and Friction", r"\bnewton\b|\bfriction\b|\bpseudo force\b"),
    ("Physics", "Oscillations and Waves", r"\bshm\b|\boscillation\b|\bangular frequency\b"),
    ("Physics", "Optics and Modern Physics", r"\blens\b|\bmirror\b|\binterference\b|\bdiffraction\b"),
    ("Physics", "Thermodynamics and Kinetic Theory", r"\bentropy\b|\bgibbs\b|\bheat engine\b"),
    ("Physics", "Current Electricity and Circuit Analysis", r"\bkirchhoff\b|\bresistance\b|\bcurrent\b"),
    # Chemistry
    ("Chemistry", "Chemical Equilibrium and Ionic Equilibrium", r"\bph\b|\bbuffer\b|\bequilibrium constant\b"),
    ("Chemistry", "Redox Reactions and Electrochemistry", r"\boxidation\b|\breduction\b|\bnernst\b"),
    ("Chemistry", "Chemical Kinetics and Surface Chemistry", r"\brate law\b|\bactivation energy\b"),
    ("Chemistry", "Chemical Bonding and Molecular Structure", r"\bhybridization\b|\bvsepr\b"),
    ("Chemistry", "Organic Chemistry: General Principles", r"\bsn1\b|\be1\b|\bcarbocation\b|\brearrangement\b"),
    ("Chemistry", "Inorganic Chemistry: p-Block, d/f-Block, Coordination", r"\bcfse\b|\bcoordination\b|\bligand\b"),
]


def _token_overlap(term: str, text: str) -> float:
    t1 = set(_norm(term).replace("_", " ").split())
    t2 = set(_norm(text).replace("_", " ").split())
    if not t1 or not t2:
        return 0.0
    return len(t1.intersection(t2)) / max(1, len(t1))


def _weighted_term_match_score(text: str, terms: Sequence[str], *, exact_boost: float = 1.0, fuzzy_boost: float = 0.5) -> float:
    score = 0.0
    for term in terms:
        key = _norm(term).replace("_", " ")
        if not key:
            continue
        if key in text:
            score += exact_boost
            continue
        overlap = _token_overlap(key, text)
        if overlap >= 0.78:
            score += fuzzy_boost
        elif overlap >= 0.58:
            score += 0.5 * fuzzy_boost
    return score


def detect_strong_unit_signals(
    question_text: str,
    syllabus: Dict[str, Dict[str, Dict]],
    *,
    detected_signals: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Hierarchy-aware deterministic + weighted unit anchoring.
    """
    text = _norm(question_text)
    raw = str(question_text or "").lower()
    detected = detected_signals or StructuralPatternDetector().analyze(question_text)

    rows: List[Dict[str, Any]] = []
    for subject, units in syllabus.items():
        for unit_name, spec in units.items():
            core_score = 3.0 * _weighted_term_match_score(text, spec.get("core_topics", []), exact_boost=1.0, fuzzy_boost=0.55)
            subtopic_score = 2.0 * _weighted_term_match_score(text, spec.get("subtopics", []), exact_boost=1.0, fuzzy_boost=0.50)
            tool_score = 1.5 * _weighted_term_match_score(text, spec.get("tools", []), exact_boost=0.9, fuzzy_boost=0.45)
            archetype_score = 1.2 * (
                len(set(spec.get("reasoning_archetypes", [])).intersection(detected.get("reasoning_archetypes", [])))
                + 0.5 * _weighted_term_match_score(text, spec.get("reasoning_archetypes", []), exact_boost=1.0, fuzzy_boost=0.35)
            )
            structural_score = 1.0 * (
                len(set(spec.get("structural_patterns", [])).intersection(detected.get("structural_patterns", [])))
                + 0.5 * _weighted_term_match_score(text, spec.get("structural_patterns", []), exact_boost=0.9, fuzzy_boost=0.35)
            )
            practical_score = 1.0 * (
                len(set(spec.get("practical_tags", [])).intersection(detected.get("practical_tags", [])))
                + 0.5 * _weighted_term_match_score(text, spec.get("practical_tags", []), exact_boost=0.8, fuzzy_boost=0.35)
            )

            regex_anchor_score = 0.0
            for rule_subject, rule_unit, pattern in _HARD_UNIT_ANCHOR_RULES:
                if rule_subject != subject or rule_unit != unit_name:
                    continue
                if re.search(pattern, raw):
                    regex_anchor_score += 4.25

            signal_score = core_score + subtopic_score + tool_score + archetype_score + structural_score + practical_score
            score = signal_score + regex_anchor_score
            rows.append(
                {
                    "subject": subject,
                    "unit": unit_name,
                    "signal_score": round(signal_score, 6),
                    "regex_anchor_score": round(regex_anchor_score, 6),
                    "score": round(score, 6),
                }
            )

    rows.sort(key=lambda row: row["score"], reverse=True)
    if rows and float(rows[0]["score"]) <= 0.0:
        fallback_subject = "Mathematics" if re.search(r"[=x\^+\-*/0-9]", text) else "Chemistry"
        fallback_unit = next(iter(syllabus[fallback_subject].keys()))
        rows[0] = {
            "subject": fallback_subject,
            "unit": fallback_unit,
            "signal_score": 0.1,
            "regex_anchor_score": 0.0,
            "score": 0.1,
        }
        rows.sort(key=lambda row: row["score"], reverse=True)

    top2 = rows[:2]
    top_score = float(top2[0]["score"]) if top2 else 0.0
    denom = max(1e-9, sum(max(0.0, float(row["score"])) for row in top2))
    anchor_conf = _clamp(top_score / denom, 0.0, 1.0) if top2 else 0.0

    return {
        "rankings": [(row["subject"], row["unit"], float(row["score"])) for row in rows],
        "ranked_details": rows,
        "anchor_units": [str(row["unit"]) for row in top2],
        "anchor_unit_pairs": [{"subject": str(row["subject"]), "unit": str(row["unit"])} for row in top2],
        "anchor_confidence": round(anchor_conf, 6),
    }


class AdvancedSyllabusClassifier:
    """
    Dynamic syllabus-aware classifier with graph expansion + trap signals.
    """

    REQUIRED_OUTPUT_KEYS = [
        "question",
        "subject",
        "unit",
        "subtopic",
        "difficulty",
        "difficulty_score",
        "estimated_entropy",
        "concept_cluster",
        "primary_concepts",
        "secondary_concepts",
        "structural_patterns",
        "trap_signals",
        "source_tag",
    ]

    def __init__(
        self,
        *,
        syllabus: Dict[str, Dict[str, Dict]] | None = None,
        trap_learning: TrapLearningEngine | None = None,
        edge_updater: DynamicEdgeUpdater | None = None,
    ):
        self.syllabus = syllabus or build_syllabus_hierarchy()
        self.trap_learning = trap_learning or TrapLearningEngine()
        self.edge_updater = edge_updater or DynamicEdgeUpdater()
        self.pattern_detector = StructuralPatternDetector()

        graph = ConceptGraphGenerator(self.syllabus).generate()
        self.concept_nodes = graph["concept_nodes"]
        self.node_by_id = {str(node["id"]): node for node in self.concept_nodes}
        self.node_name_to_ids = self._build_name_index(self.concept_nodes)

        base_edges = EdgeBuilder(self.syllabus, self.concept_nodes).build_edges()
        self.concept_edges = self.edge_updater.apply(base_edges)
        self.bfs_engine = ConceptBFSEngine(self.concept_nodes, self.concept_edges)

        self.unit_nodes = self._build_unit_node_index(self.concept_nodes)
        self.unit_terms = self._build_unit_term_index(self.syllabus)

    def classify_question(
        self,
        question: str,
        *,
        source_tag: str = "raw_auto",
        bfs_depth: int = 2,
    ) -> Dict:
        q = str(question or "").strip()
        if not q:
            raise ValueError("question cannot be empty")

        text = _norm(q)
        detected_global = self.pattern_detector.analyze(q)
        legacy_unit_rankings = self._score_units(text, detected_global)
        anchor_details = detect_strong_unit_signals(q, self.syllabus, detected_signals=detected_global)
        unit_rankings = self._merge_unit_rankings(legacy_unit_rankings, anchor_details.get("rankings", []))
        top_subject, top_unit, _ = unit_rankings[0]
        anchor_unit_pairs = list(anchor_details.get("anchor_unit_pairs", []))
        anchor_units = [str(row.get("unit", "")) for row in anchor_unit_pairs if str(row.get("unit", "")).strip()]
        anchor_subject = str(anchor_unit_pairs[0]["subject"]) if anchor_unit_pairs else top_subject
        anchor_core_topics = self._anchor_core_topics(anchor_unit_pairs)
        pre_entropy_hint = self._pre_entropy_hint(unit_rankings, detected_global)

        unit_spec = self.syllabus[top_subject][top_unit]
        detected = self.pattern_detector.analyze(
            q,
            unit_structural_patterns=unit_spec.get("structural_patterns", []),
            unit_common_traps=unit_spec.get("common_traps", []),
            unit_archetypes=unit_spec.get("reasoning_archetypes", []),
        )

        subtopic = self._select_subtopic(text, unit_spec.get("subtopics", []))
        primary_ids = self._select_primary_concepts(
            text=text,
            subject=top_subject,
            unit=top_unit,
            subtopic=subtopic,
            detected=detected,
        )
        primary_names = [self.node_by_id[node_id]["name"] for node_id in primary_ids]

        trap_map = self.trap_learning.trap_frequency_map()
        expansion = self.bfs_engine.expand_concepts(
            primary_ids,
            depth=bfs_depth,
            trap_frequency=trap_map,
            anchor_units=anchor_units,
            anchor_subject=anchor_subject,
            classification_entropy=pre_entropy_hint,
            anchor_core_topics=anchor_core_topics,
        )
        secondary_names = [row["name"] for row in expansion.get("secondary_concepts", [])[:12]]
        structural_nodes = [row["name"] for row in expansion.get("structural_nodes", [])[:8]]
        trap_nodes = [row["name"] for row in expansion.get("trap_nodes", [])[:10]]

        structural_patterns = _uniq(list(detected.get("structural_patterns", [])) + structural_nodes)[:12]
        trap_signals = _uniq(list(detected.get("trap_signals", [])) + trap_nodes)[:12]
        reasoning_archetypes = _uniq(detected.get("reasoning_archetypes", []))[:8]
        practical_tags = _uniq(detected.get("practical_tags", []))[:6]

        concept_depth_score = self._concept_depth_score(expansion, bfs_depth)
        structural_complexity_score = self._structural_complexity_score(text, structural_patterns, reasoning_archetypes, practical_tags)
        trap_density_score = self._trap_density_score(trap_signals, primary_ids, trap_map)
        algebraic_load_score = self._algebraic_load_score(text)
        hybrid_topics, hybrid_bonus = self._detect_hybrid_topics(unit_rankings)

        unit_ambiguity = self._unit_ambiguity(unit_rankings)
        difficulty_score = self._difficulty_score(
            concept_depth_score=concept_depth_score,
            structural_complexity_score=structural_complexity_score,
            trap_density_score=trap_density_score,
            algebraic_load_score=algebraic_load_score,
            hybrid_bonus=hybrid_bonus,
            archetype_count=len(reasoning_archetypes),
        )
        difficulty_label = self._difficulty_label(difficulty_score)
        estimated_entropy = self._entropy_score(
            difficulty_score=difficulty_score,
            concept_depth_score=concept_depth_score,
            structural_complexity_score=structural_complexity_score,
            trap_density_score=trap_density_score,
            unit_ambiguity=unit_ambiguity,
            hybrid_bonus=hybrid_bonus,
        )
        semantic_overlap_score = self._semantic_overlap_score(
            anchor_unit_pairs=anchor_unit_pairs,
            selected_subject=top_subject,
            selected_unit=top_unit,
            selected_subtopic=subtopic,
        )
        confidence = _clamp(1.0 - estimated_entropy, 0.05, 0.99)
        consistency_penalty_applied = bool(semantic_overlap_score <= 0.0)
        if consistency_penalty_applied:
            confidence = _clamp(confidence * 0.8, 0.05, 0.99)

        primary_concepts = _uniq(primary_names)
        secondary_concepts = _uniq(secondary_names)
        concept_cluster = self._build_concept_cluster(
            unit=top_unit,
            subtopic=subtopic,
            primary_concepts=primary_concepts,
            secondary_concepts=secondary_concepts,
            structural_patterns=structural_patterns,
            practical_tags=practical_tags,
        )

        output = {
            "question": q,
            "subject": top_subject,
            "unit": top_unit,
            "subtopic": subtopic,
            "difficulty": difficulty_label,
            "difficulty_score": round(difficulty_score, 6),
            "estimated_entropy": round(estimated_entropy, 6),
            "confidence": round(confidence, 6),
            "concept_cluster": concept_cluster,
            "primary_concepts": primary_concepts,
            "secondary_concepts": secondary_concepts,
            "structural_patterns": structural_patterns,
            "trap_signals": trap_signals,
            "source_tag": str(source_tag),
            "reasoning_archetypes": reasoning_archetypes,
            "practical_tags": practical_tags,
            "hybrid_topics": hybrid_topics,
            "concept_depth_score": round(concept_depth_score, 6),
            "structural_complexity_score": round(structural_complexity_score, 6),
            "trap_density_score": round(trap_density_score, 6),
            "algebraic_load_score": round(algebraic_load_score, 6),
            "calibration_bias_index": round(_clamp(0.5 * trap_density_score + 0.3 * unit_ambiguity + 0.2 * (1.0 if hybrid_topics else 0.0), 0.0, 1.0), 6),
            "routing_influence": {
                "estimated_entropy": round(estimated_entropy, 6),
                "difficulty_score": round(difficulty_score, 6),
                "trap_density_score": round(trap_density_score, 6),
                "hybrid_problem": bool(hybrid_topics),
            },
            "anchor_units": anchor_units[:2],
            "anchor_confidence": round(float(anchor_details.get("anchor_confidence", 0.0)), 6),
            "metadata": {
                "anchor_units": anchor_units[:2],
                "anchor_confidence": round(float(anchor_details.get("anchor_confidence", 0.0)), 6),
                "anchor_unit_pairs": anchor_unit_pairs[:2],
                "semantic_overlap_score": round(semantic_overlap_score, 6),
                "consistency_penalty_applied": consistency_penalty_applied,
            },
            "mini_evolution_features": {
                "subject": top_subject.lower(),
                "difficulty": difficulty_label,
                "concept_clusters": concept_cluster,
                "trap_signals": trap_signals,
                "entropy_hint": round(estimated_entropy, 6),
            },
        }
        return output

    def classify_many(self, raw_questions: Sequence[str], *, source_tag: str = "raw_auto", bfs_depth: int = 2) -> List[Dict]:
        out = []
        for question in raw_questions:
            q = str(question or "").strip()
            if not q:
                continue
            out.append(self.classify_question(q, source_tag=source_tag, bfs_depth=bfs_depth))
        return out

    def to_feeder_payload(self, classification: Dict) -> Dict:
        return {
            "question": str(classification.get("question", "")),
            "subject": str(classification.get("subject", "general")).strip().lower() or "general",
            "difficulty": str(classification.get("difficulty", "unknown")).strip().lower() or "unknown",
            "concept_cluster": list(classification.get("concept_cluster", [])),
            "source_tag": str(classification.get("source_tag", "raw_auto")),
        }

    def record_solve_feedback(
        self,
        classification: Dict,
        *,
        success: bool,
        trap_signals: Sequence[str] | None = None,
    ) -> Dict:
        concept_ids = self._resolve_concept_ids(
            list(classification.get("primary_concepts", []))
            + list(classification.get("secondary_concepts", []))[:4]
            + list(classification.get("trap_signals", [])),
        )
        trap_update = self.trap_learning.record_outcome(
            concept_ids,
            success=bool(success),
            trap_signals=trap_signals or classification.get("trap_signals", []),
        )

        if len(concept_ids) >= 2:
            primary = concept_ids[:4]
            edge_index = {(str(e["from_concept"]), str(e["to_concept"]), str(e["relation_type"])): e for e in self.concept_edges}
            for i in range(len(primary)):
                for j in range(i + 1, len(primary)):
                    a = primary[i]
                    b = primary[j]
                    for relation in ("cross_subject_bridge", "structural_dependency"):
                        if (a, b, relation) in edge_index:
                            self.edge_updater.register_outcome(
                                from_concept=a,
                                to_concept=b,
                                relation_type=relation,
                                failed=(not bool(success)),
                            )

        return trap_update

    def _build_name_index(self, nodes: List[Dict]) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for node in nodes:
            key = _norm(node.get("name", ""))
            out.setdefault(key, []).append(str(node["id"]))
        return out

    def _build_unit_node_index(self, nodes: List[Dict]) -> Dict[Tuple[str, str], Dict[str, List[str]]]:
        out: Dict[Tuple[str, str], Dict[str, List[str]]] = {}
        for node in nodes:
            key = (str(node.get("subject", "")), str(node.get("unit", "")))
            tier = str(node.get("tier", ""))
            out.setdefault(key, {}).setdefault(tier, []).append(str(node["id"]))
        return out

    def _build_unit_term_index(self, syllabus: Dict[str, Dict[str, Dict]]) -> Dict[Tuple[str, str], Dict[str, List[str]]]:
        out: Dict[Tuple[str, str], Dict[str, List[str]]] = {}
        for subject, units in syllabus.items():
            for unit_name, spec in units.items():
                terms = []
                for key in ("core_topics", "subtopics", "structural_patterns", "common_traps", "tools"):
                    terms.extend(str(x) for x in spec.get(key, []))
                out[(subject, unit_name)] = {
                    "terms": sorted({_norm(t).replace("_", " ") for t in terms if _norm(t)}),
                    "archetypes": [str(x) for x in spec.get("reasoning_archetypes", [])],
                }
        return out

    def _score_units(self, text: str, detected: Dict) -> List[Tuple[str, str, float]]:
        rankings: List[Tuple[str, str, float]] = []
        for (subject, unit_name), payload in self.unit_terms.items():
            score = 0.0
            terms = payload["terms"]
            for term in terms:
                if not term:
                    continue
                if term in text:
                    score += 1.1
                elif self._token_overlap(term, text) >= 0.75:
                    score += 0.5

            unit_spec = self.syllabus[subject][unit_name]
            archetypes = set(unit_spec.get("reasoning_archetypes", []))
            patterns = set(unit_spec.get("structural_patterns", []))
            traps = set(unit_spec.get("common_traps", []))
            practical_tags = set(unit_spec.get("practical_tags", []))

            score += 1.2 * len(archetypes.intersection(detected.get("reasoning_archetypes", [])))
            score += 0.9 * len(patterns.intersection(detected.get("structural_patterns", [])))
            score += 0.6 * len(traps.intersection(detected.get("trap_signals", [])))
            score += 1.0 * len(practical_tags.intersection(detected.get("practical_tags", [])))

            # Adaptation from trap learning stats attached to unit nodes.
            unit_node_ids = []
            for tier in ("core", "micro", "trap"):
                unit_node_ids.extend(self.unit_nodes.get((subject, unit_name), {}).get(tier, [])[:5])
            trap_map = self.trap_learning.trap_frequency_map(unit_node_ids)
            if trap_map:
                pressure = sum(trap_map.values()) / max(1, len(trap_map))
                score += 0.35 * pressure

            rankings.append((subject, unit_name, round(score, 6)))

        rankings.sort(key=lambda row: row[2], reverse=True)
        if rankings and rankings[0][2] <= 0.0:
            # fallback to deterministic baseline to keep system total.
            fallback_subject = "Mathematics" if re.search(r"[=x\^+\-*/0-9]", text) else "Chemistry"
            best_unit = next(iter(self.syllabus[fallback_subject].keys()))
            rankings[0] = (fallback_subject, best_unit, 0.1)
            rankings.sort(key=lambda row: row[2], reverse=True)
        return rankings

    def _merge_unit_rankings(
        self,
        legacy_rankings: List[Tuple[str, str, float]],
        anchor_rankings: List[Tuple[str, str, float]],
    ) -> List[Tuple[str, str, float]]:
        merged: Dict[Tuple[str, str], float] = {}
        for subject, unit, score in legacy_rankings:
            merged[(str(subject), str(unit))] = 0.28 * float(score)
        for subject, unit, score in anchor_rankings:
            key = (str(subject), str(unit))
            merged[key] = merged.get(key, 0.0) + 0.72 * float(score)

        out = [(subject, unit, round(score, 6)) for (subject, unit), score in merged.items()]
        out.sort(key=lambda row: row[2], reverse=True)
        if out:
            return out
        return legacy_rankings

    def _anchor_core_topics(self, anchor_unit_pairs: Sequence[Dict[str, str]]) -> List[str]:
        core_topics: List[str] = []
        for row in anchor_unit_pairs:
            subject = str(row.get("subject", ""))
            unit = str(row.get("unit", ""))
            spec = self.syllabus.get(subject, {}).get(unit, {})
            core_topics.extend(spec.get("core_topics", []))
        return sorted({_norm(topic).replace("_", " ") for topic in core_topics if _norm(topic)})

    def _pre_entropy_hint(self, unit_rankings: List[Tuple[str, str, float]], detected: Dict) -> float:
        ambiguity = self._unit_ambiguity(unit_rankings)
        structural = float(detected.get("structural_complexity_score", 0.0))
        trap = float(detected.get("trap_density_score", 0.0))
        return _clamp(0.45 * ambiguity + 0.30 * structural + 0.25 * trap, 0.0, 1.0)

    def _semantic_overlap_score(
        self,
        *,
        anchor_unit_pairs: Sequence[Dict[str, str]],
        selected_subject: str,
        selected_unit: str,
        selected_subtopic: str,
    ) -> float:
        anchor_terms = self._anchor_core_topics(anchor_unit_pairs)
        selected_spec = self.syllabus.get(str(selected_subject), {}).get(str(selected_unit), {})
        selected_terms = [_norm(x).replace("_", " ") for x in selected_spec.get("core_topics", []) if _norm(x)]
        subtopic_text = _norm(selected_subtopic).replace("_", " ")
        if subtopic_text:
            selected_terms.append(subtopic_text)
            for core_topic in selected_spec.get("core_topics", []):
                core_norm = _norm(core_topic).replace("_", " ")
                if core_norm and self._token_overlap(core_norm, subtopic_text) >= 0.5:
                    selected_terms.append(core_norm)

        overlap = 0.0
        for anchor_term in anchor_terms:
            for selected_term in selected_terms:
                if not anchor_term or not selected_term:
                    continue
                if anchor_term == selected_term or self._token_overlap(anchor_term, selected_term) >= 0.6:
                    overlap += 1.0
                    break
        return overlap

    def _token_overlap(self, term: str, text: str) -> float:
        t1 = set(term.split())
        t2 = set(text.split())
        if not t1 or not t2:
            return 0.0
        return len(t1.intersection(t2)) / max(1, len(t1))

    def _select_subtopic(self, text: str, subtopics: List[str]) -> str:
        best = None
        best_score = -1.0
        for subtopic in subtopics:
            st = _norm(subtopic).replace("_", " ")
            score = 0.0
            if st in text:
                score += 1.0
            score += self._token_overlap(st, text)
            if score > best_score:
                best_score = score
                best = subtopic
        return str(best or (subtopics[0] if subtopics else "general_subtopic"))

    def _select_primary_concepts(
        self,
        *,
        text: str,
        subject: str,
        unit: str,
        subtopic: str,
        detected: Dict,
    ) -> List[str]:
        unit_nodes = self.unit_nodes.get((subject, unit), {})
        micro_ids = list(unit_nodes.get("micro", []))
        core_ids = list(unit_nodes.get("core", []))
        structural_ids = list(unit_nodes.get("structural", []))

        scored: List[Tuple[str, float]] = []
        for node_id in micro_ids + structural_ids + core_ids:
            node = self.node_by_id[node_id]
            name = _norm(node.get("name", "")).replace("_", " ")
            score = 0.0
            if name in text:
                score += 2.0
            score += self._token_overlap(name, text)
            for keyword in node.get("keywords", []):
                token = _norm(keyword).replace("_", " ")
                if token and token in text:
                    score += 0.7
            if node_id in core_ids:
                score += 0.5
            if any(_norm(subtopic).replace("_", " ") in name for subtopic in [subtopic]):
                score += 0.6
            if node.get("tier") == "structural" and node.get("name") in detected.get("structural_patterns", []):
                score += 0.8
            scored.append((node_id, score))

        scored.sort(key=lambda row: row[1], reverse=True)
        selected = [node_id for node_id, score in scored if score > 0][:4]
        if not selected and core_ids:
            selected = [core_ids[0]]
        if core_ids and core_ids[0] not in selected:
            selected.insert(0, core_ids[0])
        return _uniq(selected)[:4]

    def _concept_depth_score(self, expansion: Dict, max_depth: int) -> float:
        secondary = expansion.get("secondary_concepts", [])
        if not secondary:
            return 0.08
        avg_depth = sum(int(row.get("depth", 1)) for row in secondary) / max(1, len(secondary))
        count_term = min(1.0, len(secondary) / 14.0)
        depth_term = min(1.0, avg_depth / max(1.0, float(max_depth)))
        return _clamp(0.55 * count_term + 0.45 * depth_term, 0.0, 1.0)

    def _structural_complexity_score(
        self,
        text: str,
        structural_patterns: List[str],
        reasoning_archetypes: List[str],
        practical_tags: List[str],
    ) -> float:
        clause_count = len([x for x in re.split(r"[,;:.]", text) if x.strip()])
        score = 0.10
        score += 0.10 * len(structural_patterns)
        score += 0.12 * len(reasoning_archetypes)
        score += 0.08 * len(practical_tags)
        score += 0.02 * min(12, clause_count)
        return _clamp(score, 0.0, 1.0)

    def _trap_density_score(self, trap_signals: List[str], primary_ids: List[str], trap_map: Dict[str, float]) -> float:
        avg_primary = 0.0
        if primary_ids:
            vals = [float(trap_map.get(node_id, self.node_by_id.get(node_id, {}).get("trap_base_frequency", 0.0))) for node_id in primary_ids]
            avg_primary = sum(vals) / max(1, len(vals))
        score = 0.06 * len(trap_signals) + 0.55 * avg_primary
        return _clamp(score, 0.0, 1.0)

    def _algebraic_load_score(self, text: str) -> float:
        operator_count = len(re.findall(r"[\+\-\*/\^=]", text))
        power_count = len(re.findall(r"\^[0-9a-z]", text))
        variable_count = len(re.findall(r"\b[a-z]\b", text))
        bracket_count = len(re.findall(r"[\(\)\[\]\{\}]", text))
        score = 0.06 * operator_count + 0.10 * power_count + 0.03 * variable_count + 0.02 * bracket_count
        return _clamp(score, 0.0, 1.0)

    def _detect_hybrid_topics(self, unit_rankings: List[Tuple[str, str, float]]) -> Tuple[List[str], float]:
        if len(unit_rankings) < 2:
            return [], 0.0
        (s1, u1, v1), (s2, u2, v2) = unit_rankings[0], unit_rankings[1]
        if s1 == s2 or v1 <= 0.0:
            return [], 0.0
        if v2 < 0.78 * v1:
            return [], 0.0
        return [f"{s1}:{u1}", f"{s2}:{u2}"], 1.0

    def _unit_ambiguity(self, unit_rankings: List[Tuple[str, str, float]]) -> float:
        if len(unit_rankings) < 2:
            return 0.0
        v1 = max(1e-9, float(unit_rankings[0][2]))
        v2 = max(0.0, float(unit_rankings[1][2]))
        ratio = v2 / v1
        return _clamp(ratio, 0.0, 1.0)

    def _difficulty_score(
        self,
        *,
        concept_depth_score: float,
        structural_complexity_score: float,
        trap_density_score: float,
        algebraic_load_score: float,
        hybrid_bonus: float,
        archetype_count: int,
    ) -> float:
        score = 1.7
        score += 2.4 * concept_depth_score
        score += 2.2 * structural_complexity_score
        score += 2.0 * trap_density_score
        score += 1.6 * algebraic_load_score
        score += 0.7 * hybrid_bonus
        score += 0.2 * min(4, archetype_count)
        return _clamp(score, 1.0, 10.0)

    def _difficulty_label(self, difficulty_score: float) -> str:
        if difficulty_score >= 7.0:
            return "hard"
        if difficulty_score >= 4.0:
            return "medium"
        return "easy"

    def _entropy_score(
        self,
        *,
        difficulty_score: float,
        concept_depth_score: float,
        structural_complexity_score: float,
        trap_density_score: float,
        unit_ambiguity: float,
        hybrid_bonus: float,
    ) -> float:
        diff_norm = (difficulty_score - 1.0) / 9.0
        entropy = 0.0
        entropy += 0.22 * trap_density_score
        entropy += 0.18 * unit_ambiguity
        entropy += 0.18 * structural_complexity_score
        entropy += 0.16 * concept_depth_score
        entropy += 0.16 * diff_norm
        entropy += 0.10 * (1.0 if hybrid_bonus > 0 else 0.0)
        return _clamp(entropy, 0.0, 1.0)

    def _build_concept_cluster(
        self,
        *,
        unit: str,
        subtopic: str,
        primary_concepts: List[str],
        secondary_concepts: List[str],
        structural_patterns: List[str],
        practical_tags: List[str],
    ) -> List[str]:
        cluster = [unit, subtopic]
        cluster.extend(primary_concepts[:4])
        cluster.extend(secondary_concepts[:4])
        cluster.extend(structural_patterns[:3])
        cluster.extend(practical_tags[:2])
        return _uniq([_slug(x) for x in cluster if x])[:16]

    def _resolve_concept_ids(self, concept_names: Sequence[str]) -> List[str]:
        out = []
        seen = set()
        for name in concept_names:
            if not name:
                continue
            text = str(name).strip()
            if text in self.node_by_id and text not in seen:
                out.append(text)
                seen.add(text)
                continue
            key = _norm(text)
            for node_id in self.node_name_to_ids.get(key, []):
                if node_id not in seen:
                    out.append(node_id)
                    seen.add(node_id)
        return out
