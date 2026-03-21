from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from core.intelligence.advanced_classifier import AdvancedSyllabusClassifier
from core.intelligence.bfs_engine import ConceptBFSEngine
from core.intelligence.concept_graph_generator import ConceptGraphGenerator
from core.intelligence.dynamic_edge_updater import DynamicEdgeUpdater
from core.intelligence.edge_builder import EdgeBuilder
from core.intelligence.solver_context_injector import SolverContextInjector
from core.intelligence.syllabus_graph import build_syllabus_hierarchy
from core.intelligence.trap_learning_engine import TrapLearningEngine
from core.automation.feeder_engine import FeederEngine
from core.automation.raw_question_intake import RawQuestionIntakeSystem


class IntelligenceLayerIntegrationTests(unittest.TestCase):
    def test_graph_size_and_edges(self):
        syllabus = build_syllabus_hierarchy()
        graph = ConceptGraphGenerator(syllabus).generate()
        nodes = graph["concept_nodes"]
        self.assertGreaterEqual(len(nodes), 300)

        edges = EdgeBuilder(syllabus, nodes).build_edges()
        self.assertGreater(len(edges), 0)
        relation_types = {row["relation_type"] for row in edges}
        self.assertIn("prerequisite", relation_types)
        self.assertIn("extension", relation_types)
        self.assertIn("structural_dependency", relation_types)
        self.assertIn("trap_link", relation_types)
        self.assertIn("cross_subject_bridge", relation_types)

    def test_bfs_expansion(self):
        syllabus = build_syllabus_hierarchy()
        nodes = ConceptGraphGenerator(syllabus).generate()["concept_nodes"]
        edges = EdgeBuilder(syllabus, nodes).build_edges()
        engine = ConceptBFSEngine(nodes, edges)

        out = engine.expand_concepts(["Permutations and Combinations Core"], depth=2)
        self.assertTrue(len(out["primary_concepts"]) >= 1)
        self.assertTrue(len(out["secondary_concepts"]) >= 1)
        self.assertIn("trap_nodes", out)

    def test_classifier_and_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trap_engine = TrapLearningEngine(path=str(root / "trap.json"))
            edge_updater = DynamicEdgeUpdater(path=str(root / "edge.json"))
            classifier = AdvancedSyllabusClassifier(trap_learning=trap_engine, edge_updater=edge_updater)
            injector = SolverContextInjector(classifier=classifier)

            question = "If the coefficients of x^7 and x^8 in the expansion of (2 + x/3)^n are equal, find n."
            output = classifier.classify_question(question, source_tag="test")
            for key in classifier.REQUIRED_OUTPUT_KEYS:
                self.assertIn(key, output)
            self.assertEqual(output["subject"], "Mathematics")

            injected = injector.inject(question=question, base_prompt="Solve the question with concise steps.")
            self.assertIn("[SYSTEM KNOWLEDGE CONTEXT]", injected["prompt"])

    def test_trap_learning_and_edge_update(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trap_engine = TrapLearningEngine(path=str(root / "trap.json"))
            result = trap_engine.record_failure(["math::pc::micro::overcounting"], ["overcounting"])
            self.assertTrue(result["failed"])
            table = trap_engine.table()
            self.assertGreaterEqual(len(table), 1)

            updater = DynamicEdgeUpdater(path=str(root / "edge.json"))
            updater.register_outcome(
                from_concept="a",
                to_concept="b",
                relation_type="cross_subject_bridge",
                failed=True,
            )
            edges = [{"from_concept": "a", "to_concept": "b", "relation_type": "cross_subject_bridge", "weight": 0.8}]
            out = updater.apply(edges)
            self.assertEqual(len(out), 1)
            self.assertGreaterEqual(out[0]["weight"], 0.4)
            self.assertLessEqual(out[0]["weight"], 1.5)

    def test_raw_intake_with_advanced_classifier(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            feeder = FeederEngine(
                queue_path=str(root / "queue.jsonl"),
                training_cases_path=str(root / "cases.jsonl"),
                replay_cases_path=str(root / "replay.jsonl"),
            )
            classifier = AdvancedSyllabusClassifier(
                trap_learning=TrapLearningEngine(path=str(root / "trap.json")),
                edge_updater=DynamicEdgeUpdater(path=str(root / "edge.json")),
            )
            intake = RawQuestionIntakeSystem(feeder=feeder, classifier=classifier)
            out = intake.enqueue_classified(
                intake.classify_raw_questions(
                    [
                        "If the coefficients of x^7 and x^8 in the expansion of (2 + x/3)^n are equal, find n.",
                        "The number of four-digit numbers strictly greater than 4321 formed using digits 0,1,2,3,4,5 is",
                    ],
                    default_source_tag="test_advanced",
                )
            )
            self.assertEqual(out["added"], 2)


if __name__ == "__main__":
    unittest.main()
