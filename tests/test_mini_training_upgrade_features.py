import asyncio
import json
import socket
import tempfile
import unittest
from pathlib import Path

from core.api.entrypoint import lalacore_entry
from core.mini_training.dataset_builder import DatasetBuildConfig, MiniTrainingDatasetBuilder
from core.mini_training.distillation_engine import MiniDistillationEngine
from core.mini_training.evaluation import MiniEvaluationHarness
from core.mini_training.internal_consistency import MiniInternalConsistencyAnalyzer
from core.mini_training.promotion_policy import MiniPromotionPolicy
from core.mini_training.trainer import MiniTrainer, TrainerConfig
from core.mini_training.traffic_simulator import MiniTrafficSimulator


class MiniTrainingUpgradeTests(unittest.TestCase):
    def test_dataset_builder_generates_synthetic_disagreement_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            solver = root / "solver.jsonl"
            hooks = root / "hooks.jsonl"
            shadow = root / "shadow.jsonl"
            out = root / "out"

            solver.write_text("", encoding="utf-8")
            hooks.write_text("", encoding="utf-8")
            shadow_rows = [
                {
                    "question": "How many permutations of 1-6 where 1 appears before 2?",
                    "subject": "math",
                    "difficulty": "hard",
                    "mini_answer": "360",
                    "mini_confidence": 0.92,
                    "mini_verified": False,
                    "arena_winner_provider": "mini",
                    "arena_winner_answer": "360",
                    "winner_verified": False,
                    "agreement_with_winner": False,
                    "entropy": 0.82,
                    "risk": 0.75,
                    "concept_cluster": ["combinatorics"],
                }
            ]
            with shadow.open("w", encoding="utf-8") as f:
                for row in shadow_rows:
                    f.write(json.dumps(row) + "\n")

            builder = MiniTrainingDatasetBuilder(
                solver_debug_path=str(solver),
                automation_hooks_path=str(hooks),
                mini_shadow_logs_path=str(shadow),
                output_dir=str(out),
            )
            result = builder.build_dataset(
                DatasetBuildConfig(
                    require_verified=False,
                    risk_threshold=1.0,
                    entropy_threshold=1.0,
                    include_synthetic_disagreement_rows=True,
                    disagreement_entropy_threshold=0.45,
                )
            )
            self.assertGreater(result.stats.get("synthetic_disagreement_rows", 0), 0)
            self.assertTrue(any(bool(row.get("synthetic_disagreement", False)) for row in result.rows))

    def test_rare_cluster_weighting_amplifies_replay_priority(self):
        engine = MiniDistillationEngine()
        rows = [
            {
                "question": "common-1",
                "verified": False,
                "risk": 0.8,
                "entropy": 0.6,
                "disagreement": 0.4,
                "confidence": 0.9,
                "concept_cluster": ["algebra"],
            },
            {
                "question": "common-2",
                "verified": False,
                "risk": 0.8,
                "entropy": 0.6,
                "disagreement": 0.4,
                "confidence": 0.9,
                "concept_cluster": ["algebra"],
            },
            {
                "question": "rare",
                "verified": False,
                "risk": 0.8,
                "entropy": 0.6,
                "disagreement": 0.4,
                "confidence": 0.9,
                "concept_cluster": ["rare_cluster"],
            },
        ]
        freq = engine.concept_cluster_frequency(rows)
        energy = engine.compute_concept_energy_evolution(rows)
        common_score = engine.replay_priority_score(rows[0], cluster_frequency=freq, concept_energy_state=energy)
        rare_score = engine.replay_priority_score(rows[2], cluster_frequency=freq, concept_energy_state=energy)
        self.assertGreater(rare_score, common_score)

    def test_disagreement_replay_ordering_prefers_amplified_rows(self):
        engine = MiniDistillationEngine()
        base = [
            {
                "question": "q1",
                "verified": False,
                "risk": 0.6,
                "entropy": 0.2,
                "disagreement": 0.0,
                "confidence": 0.9,
                "concept_cluster": ["algebra"],
            },
            {
                "question": "q2",
                "verified": False,
                "risk": 0.8,
                "entropy": 0.8,
                "disagreement": 0.5,
                "confidence": 0.95,
                "concept_cluster": ["combinatorics"],
                "winner_provider": "mini",
            },
        ]
        synthetic = engine.synthesize_disagreement_rows(base)
        self.assertGreaterEqual(len(synthetic), 1)
        hard = engine.mine_hard_negatives(base + synthetic, top_k=2, use_advanced_scoring=True)
        self.assertTrue(any(bool(row.get("synthetic_disagreement", False)) for row in hard))

    def test_overconfidence_penalty_requires_low_entropy_incorrect_case(self):
        trainer = MiniTrainer()
        cfg = TrainerConfig(
            epochs=1,
            calibration_guard_mode=True,
            overconfidence_threshold=0.85,
            overconfidence_entropy_threshold=0.30,
            overconfidence_penalty=1.8,
        )
        model_state = {
            "memory": {
                "q low": {"final_answer": "x", "confidence": 0.95},
                "q high": {"final_answer": "x", "confidence": 0.95},
            },
            "default_answer": "",
            "default_confidence": 0.5,
        }
        rows = [
            {"question": "q low", "final_answer": "1", "verified": False, "entropy": 0.10, "loss_weight": 1.0, "margin_weight": 1.0},
            {"question": "q high", "final_answer": "2", "verified": False, "entropy": 0.80, "loss_weight": 1.0, "margin_weight": 1.0},
        ]
        _, _, stats = trainer._train_epoch(rows=rows, model_state=model_state, cfg=cfg)
        self.assertEqual(stats["suppressed_rows"], 1)
        self.assertIn("heatmap_counts", stats)

    def test_calibration_stratified_shape_and_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            harness = MiniEvaluationHarness(output_dir=tmp)
            rows = [
                {"question": "q1", "final_answer": "1", "entropy": 0.1, "risk": 0.1, "difficulty": "easy", "concept_cluster": ["a"]},
                {"question": "q2", "final_answer": "2", "entropy": 0.9, "risk": 0.9, "difficulty": "hard", "concept_cluster": ["rare"]},
                {"question": "q3", "final_answer": "3", "entropy": 0.8, "risk": 0.8, "difficulty": "hard", "concept_cluster": ["rare"], "disagreement": 0.8},
            ]
            preds = [
                {"final_answer": "1", "confidence": 0.95},
                {"final_answer": "9", "confidence": 0.92},
                {"final_answer": "3", "confidence": 0.65},
            ]
            metrics = harness.evaluate(rows, predictions=preds, bootstrap_samples=120, bootstrap_seed=7)
            self.assertIn("calibration_stratified", metrics)
            stratified = metrics["calibration_stratified"]
            self.assertIn("class_conditional_ece", stratified)
            self.assertIn("rare_cluster_ece", stratified)
            self.assertIn("hard_slice_ece", stratified)
            self.assertIn("temperature_scaling", stratified)

            path = harness.write_calibration_stratified(metrics)
            self.assertTrue(path.exists())
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertIn("class_conditional_ece", payload)

    def test_bootstrap_ci_monotonicity_and_stability_selection(self):
        with tempfile.TemporaryDirectory() as tmp:
            harness = MiniEvaluationHarness(output_dir=tmp)
            rows = [
                {"question": "q1", "final_answer": "1", "entropy": 0.1, "risk": 0.1, "difficulty": "hard", "concept_cluster": ["a"]},
                {"question": "q2", "final_answer": "2", "entropy": 0.9, "risk": 0.9, "difficulty": "hard", "concept_cluster": ["b"]},
                {"question": "q3", "final_answer": "3", "entropy": 0.5, "risk": 0.5, "difficulty": "hard", "concept_cluster": ["b"]},
                {"question": "q4", "final_answer": "4", "entropy": 0.4, "risk": 0.4, "difficulty": "hard", "concept_cluster": ["a"]},
            ]
            preds = [
                {"final_answer": "1", "confidence": 0.8},
                {"final_answer": "7", "confidence": 0.9},
                {"final_answer": "3", "confidence": 0.7},
                {"final_answer": "0", "confidence": 0.6},
            ]
            metrics = harness.evaluate(rows, predictions=preds, bootstrap_samples=120, bootstrap_seed=11)
            ci = metrics["bootstrap_ci"]["hard_case_accuracy_ci95"]
            mean = metrics["bootstrap_ci"]["hard_case_accuracy_mean"]
            self.assertLessEqual(ci[0], mean)
            self.assertLessEqual(mean, ci[1])

            selection = harness.select_checkpoint_by_bootstrap(
                [
                    {"epoch": 1, "hard_case_accuracy": 0.70, "bootstrap_ci": {"hard_case_accuracy_mean": 0.70, "hard_case_accuracy_std": 0.10}},
                    {"epoch": 2, "hard_case_accuracy": 0.68, "bootstrap_ci": {"hard_case_accuracy_mean": 0.68, "hard_case_accuracy_std": 0.02}},
                ]
            )
            self.assertEqual(selection["selected"]["epoch"], 2)
            self.assertTrue(Path(selection["path"]).exists())

    def test_stress_simulation_metrics_structure(self):
        with tempfile.TemporaryDirectory() as tmp:
            simulator = MiniTrafficSimulator(output_dir=tmp)
            rows = [
                {
                    "question": "How many subsets of {1-7} contain both 1 and 7?",
                    "arena_winner_answer": "32",
                    "winner_verified": True,
                    "mini_answer": "32",
                    "mini_confidence": 0.8,
                    "difficulty": "hard",
                    "risk": 0.7,
                    "entropy": 0.8,
                    "disagreement": 0.6,
                    "concept_cluster": ["combinatorics"],
                },
                {
                    "question": "q2",
                    "arena_winner_answer": "4",
                    "winner_verified": True,
                    "mini_answer": "4",
                    "mini_confidence": 0.9,
                    "difficulty": "easy",
                    "risk": 0.1,
                    "entropy": 0.1,
                    "concept_cluster": ["algebra"],
                },
            ]
            candidate = {
                "memory": {
                    "how many subsets of {1-7} contain both 1 and 7?": {"final_answer": "0", "confidence": 0.95},
                    "q2": {"final_answer": "9", "confidence": 0.95},
                },
                "default_answer": "0",
                "default_confidence": 0.5,
            }
            report = simulator.simulate_from_rows(rows, candidate_model_state=candidate)
            self.assertIn("stress_resilience_score", report)
            self.assertIn("promotion_risk_factor", report)
            self.assertIn("stress_scenarios", report)
            self.assertTrue(Path(tmp, "stress_simulation_report.json").exists())

    def test_stability_variance_detection_blocks_promotion(self):
        analyzer = MiniInternalConsistencyAnalyzer(output_dir="data/mini_training/internal_consistency")
        report = analyzer.analyze_samples(
            [
                {"final_answer": "1", "confidence": 0.95, "entropy": 0.1},
                {"final_answer": "9", "confidence": 0.10, "entropy": 0.9},
                {"final_answer": "2", "confidence": 0.85, "entropy": 0.2},
                {"final_answer": "8", "confidence": 0.15, "entropy": 0.8},
                {"final_answer": "3", "confidence": 0.92, "entropy": 0.1},
            ],
            variance_threshold=0.05,
        )
        self.assertTrue(report["variance_blocked"])
        self.assertGreater(report["confidence_variance"], 0.0)
        self.assertIn("disagreement_self_rate", report)

    def test_promotion_policy_blocks_new_hardening_conditions(self):
        policy = MiniPromotionPolicy()
        decision = policy.evaluate(
            {
                "exact_match": 0.82,
                "hard_case_accuracy": 0.62,
                "rare_concept_accuracy": 0.60,
                "disagreement_slice_accuracy": 0.40,
                "calibration_brier": 0.12,
                "ece": 0.20,
                "overconfidence_rate": 0.25,
                "concept_cluster_accuracy": {"a": 0.8, "b": 0.6},
            },
            baseline_metrics={
                "hard_case_accuracy": 0.70,
                "rare_concept_accuracy": 0.66,
                "disagreement_slice_accuracy": 0.65,
                "calibration_brier": 0.10,
                "ece": 0.10,
                "overconfidence_rate": 0.10,
            },
            traffic_simulation={
                "safe_to_promote": True,
                "regression_risk_score": 0.20,
                "stress_resilience_score": 0.30,
                "promotion_risk_factor": 0.70,
            },
            self_disagreement={"self_disagreement_rate": 0.10, "answer_variance": 0.20, "confidence_variance": 0.20},
        )
        self.assertFalse(decision["promote"])
        reasons = " ".join(decision["block_reasons"])
        self.assertIn("ece_drift", reasons)
        self.assertIn("stress_resilience_low", reasons)
        self.assertIn("disagreement_performance_drop", reasons)
        self.assertIn("overconfidence_rate_increased", reasons)
        self.assertIn("stability_variance_too_high", reasons)

    def test_api_free_enforcement_intact(self):
        trainer = MiniTrainer()
        with trainer._api_free_guard():
            with self.assertRaises(RuntimeError):
                socket.create_connection(("example.com", 80), timeout=0.1)

    def test_runtime_solve_interface_still_operational(self):
        out = asyncio.run(
            lalacore_entry(
                "What is 6*7?",
                options={"enable_persona": False, "enable_meta_verification": False},
            )
        )
        self.assertIsInstance(out, dict)
        self.assertNotEqual(str(out.get("status", "")), "error")
        self.assertIn("final_answer", out)


if __name__ == "__main__":
    unittest.main()
