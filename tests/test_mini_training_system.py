import json
import asyncio
import socket
import tempfile
import unittest
from pathlib import Path

from core.mini_training.curriculum_scheduler import CurriculumConfig, CurriculumScheduler, CurriculumThresholds
from core.mini_training.dataset_builder import DatasetBuildConfig, MiniTrainingDatasetBuilder
from core.mini_training.dataset_splitter import SplitConfig, StratifiedDatasetSplitter
from core.mini_training.distillation_engine import MiniDistillationEngine
from core.mini_training.evaluation import MiniEvaluationHarness
from core.mini_training.experiment_tracker import MiniExperimentTracker
from core.mini_training.internal_consistency import MiniInternalConsistencyAnalyzer
from core.mini_training.promotion_policy import MiniPromotionPolicy
from core.mini_training.shadow_evaluator import MiniShadowEvaluator
from core.mini_training.traffic_simulator import MiniTrafficSimulator
from core.mini_training.trainer import MiniTrainer, TrainerConfig


class MiniDatasetBuilderTests(unittest.TestCase):
    def test_dataset_extraction_correctness(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            solver_path = root / "solver.jsonl"
            hooks_path = root / "hooks.jsonl"
            shadow_path = root / "shadow.jsonl"
            output_dir = root / "out"

            solver_rows = [
                {
                    "event_type": "provider_output",
                    "provider": "mini",
                    "question": "If x+1=2 then x is?",
                    "raw_output": "Final Answer: 1",
                    "extracted_answer": "1",
                    "verification": True,
                    "risk": 0.1,
                    "entropy": 0.1,
                },
                {
                    "event_type": "provider_output",
                    "provider": "mini",
                    "question": "Hard failure sample",
                    "raw_output": "Final Answer: 0",
                    "extracted_answer": "0",
                    "verification": False,
                    "risk": 0.95,
                    "entropy": 0.95,
                },
            ]
            hooks_rows = [
                {
                    "event_type": "post_arena_hooks",
                    "subject": "math",
                    "difficulty": "easy",
                    "winner_verified": True,
                    "winner_margin": 0.9,
                    "entropy": 0.1,
                    "disagreement_case_count": 0,
                }
            ]
            shadow_rows = [
                {
                    "question": "If x+1=2 then x is?",
                    "subject": "math",
                    "difficulty": "easy",
                    "concept_cluster": ["algebra"],
                    "mini_answer": "1",
                    "mini_reasoning": "Solve linearly.",
                    "mini_confidence": 0.9,
                    "mini_verified": True,
                    "arena_winner_provider": "mini",
                    "arena_winner_answer": "1",
                    "agreement_with_winner": True,
                    "winner_verified": True,
                    "entropy": 0.1,
                    "winner_margin": 0.9,
                    "uncertainty": 0.1,
                    "risk": 0.1,
                }
            ]

            self._write_jsonl(solver_path, solver_rows)
            self._write_jsonl(hooks_path, hooks_rows)
            self._write_jsonl(shadow_path, shadow_rows)

            builder = MiniTrainingDatasetBuilder(
                solver_debug_path=str(solver_path),
                automation_hooks_path=str(hooks_path),
                mini_shadow_logs_path=str(shadow_path),
                output_dir=str(output_dir),
            )
            result = builder.build_dataset(
                DatasetBuildConfig(
                    require_verified=True,
                    risk_threshold=0.5,
                    entropy_threshold=0.5,
                )
            )

            self.assertGreaterEqual(len(result.rows), 1)
            row = result.rows[0]
            required = {
                "question",
                "final_answer",
                "reasoning_summary",
                "winner_provider",
                "verified",
                "entropy",
                "disagreement",
                "risk",
                "concept_cluster",
                "difficulty",
            }
            self.assertTrue(required.issubset(set(row.keys())))
            self.assertTrue(all(bool(r.get("verified")) for r in result.rows))

    def test_hard_negative_mining_logic(self):
        builder = MiniTrainingDatasetBuilder(
            solver_debug_path="data/lc9/LC9_SOLVER_DEBUG.jsonl",
            automation_hooks_path="data/lc9/LC9_AUTOMATION_HOOK_EVENTS.jsonl",
            mini_shadow_logs_path="data/lc9/LC9_MINI_SHADOW_LOGS.jsonl",
        )
        rows = [
            {"question": "q1", "verified": False, "risk": 0.2, "entropy": 0.3, "disagreement": 0.2, "confidence": 0.8},
            {"question": "q2", "verified": True, "risk": 0.1, "entropy": 0.1, "disagreement": 0.0, "confidence": 0.2},
            {"question": "q3", "verified": False, "risk": 0.8, "entropy": 0.1, "disagreement": 0.0, "confidence": 0.3},
        ]
        hard = builder.extract_hard_negatives(rows)
        hard_ids = {row["question"] for row in hard}
        self.assertIn("q1", hard_ids)
        self.assertIn("q3", hard_ids)
        self.assertNotIn("q2", hard_ids)

    def _write_jsonl(self, path: Path, rows):
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")


class MiniSplitAndDistillationTests(unittest.TestCase):
    def test_stratified_splitter_balances_distribution(self):
        rows = []
        for idx in range(12):
            rows.append(
                {
                    "question": f"q{idx}",
                    "final_answer": str(idx),
                    "reasoning_summary": "s",
                    "winner_provider": "mini",
                    "verified": True,
                    "entropy": 0.2 if idx % 2 == 0 else 0.7,
                    "disagreement": 0.0,
                    "risk": 0.2 if idx % 2 == 0 else 0.8,
                    "concept_cluster": ["algebra" if idx % 3 else "calculus"],
                    "difficulty": "hard" if idx % 4 == 0 else "easy",
                    "subject": "math" if idx % 2 == 0 else "physics",
                    "source": "unit",
                }
            )

        splitter = StratifiedDatasetSplitter(output_dir="data/mini_training/splits")
        splits = splitter.split(rows, SplitConfig(train_ratio=0.6, val_ratio=0.2, test_ratio=0.2, seed=7))

        total = len(splits["train"]) + len(splits["val"]) + len(splits["test"])
        self.assertEqual(total, len(rows))
        self.assertGreater(len(splits["train"]), 0)
        self.assertGreater(len(splits["val"]), 0)
        self.assertGreater(len(splits["test"]), 0)
        self.assertTrue(any(float(r.get("risk", 0.0)) >= 0.65 for r in splits["val"] + splits["test"]))

    def test_distillation_weighting_prefers_deterministic_low_entropy(self):
        engine = MiniDistillationEngine()
        low_entropy = {"entropy": 0.1, "verified": True, "deterministic_verified": True}
        high_entropy = {"entropy": 0.9, "verified": False, "deterministic_verified": False}
        low = engine.confidence_weight(low_entropy)
        high = engine.confidence_weight(high_entropy)
        self.assertGreater(low, high)

        dist = engine.teacher_distribution({"arena_posteriors": {"mini": 0.8, "groq": 0.2}})
        self.assertAlmostEqual(sum(dist.values()), 1.0, places=6)

    def test_calibration_loss_decreases_on_synthetic_data(self):
        engine = MiniDistillationEngine()
        rows = [
            {"confidence": 0.95, "entropy": 0.10, "verified": True},
            {"confidence": 0.88, "entropy": 0.20, "verified": True},
            {"confidence": 0.91, "entropy": 0.15, "verified": True},
            {"confidence": 0.18, "entropy": 0.85, "verified": False},
            {"confidence": 0.24, "entropy": 0.75, "verified": False},
            {"confidence": 0.35, "entropy": 0.70, "verified": False},
        ]
        state = engine.train_calibration_head(rows, epochs=35, lr=0.08)
        self.assertGreater(len(state.losses), 1)
        self.assertLess(state.losses[-1], state.losses[0])


class MiniEvaluationAndPolicyTests(unittest.TestCase):
    def test_evaluation_metrics_computed(self):
        harness = MiniEvaluationHarness(output_dir="data/mini_training/evaluation")
        rows = [
            {
                "id": 1,
                "question": "2+2",
                "final_answer": "4",
                "entropy": 0.2,
                "risk": 0.2,
                "difficulty": "easy",
                "subject": "math",
                "concept_cluster": ["algebra"],
            },
            {
                "id": 2,
                "question": "hard one",
                "final_answer": "5",
                "entropy": 0.8,
                "risk": 0.9,
                "difficulty": "hard",
                "subject": "math",
                "concept_cluster": ["calculus"],
            },
        ]
        preds = [
            {"final_answer": "4", "confidence": 0.90},
            {"final_answer": "7", "confidence": 0.95},
        ]
        metrics = harness.evaluate(rows, predictions=preds, tag="unit")
        self.assertAlmostEqual(metrics["exact_match"], 0.5, places=6)
        self.assertIn("calibration_curve", metrics)
        self.assertIn("concept_cluster_accuracy", metrics)
        self.assertAlmostEqual(metrics["hard_case_accuracy"], 0.0, places=6)

    def test_promotion_policy_blocks_weak_models(self):
        policy = MiniPromotionPolicy()
        decision = policy.evaluate(
            {
                "exact_match": 0.40,
                "hard_case_accuracy": 0.25,
                "calibration_brier": 0.40,
                "overconfidence_rate": 0.45,
                "concept_cluster_accuracy": {"algebra": 0.0, "calculus": 0.2},
            },
            candidate_model_id="mini_v2",
            incumbent_model_id="mini_v1",
        )
        self.assertFalse(decision["eligible"])
        self.assertFalse(decision["auto_replace_allowed"])
        self.assertGreaterEqual(len(decision["blocked_reasons"]), 1)
        self.assertEqual(decision["fallback_model_id"], "mini_v1")


class MiniTrainerAndShadowTests(unittest.TestCase):
    def test_trainer_exports_checkpoints_and_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            trainer = MiniTrainer(
                checkpoint_root=str(root / "checkpoints"),
                evaluation_output_dir=str(root / "evaluation"),
            )
            rows = [
                {
                    "question": "What is 2+2?",
                    "final_answer": "4",
                    "verified": True,
                    "entropy": 0.1,
                    "risk": 0.1,
                    "difficulty": "easy",
                    "subject": "math",
                    "concept_cluster": ["algebra"],
                },
                {
                    "question": "What is 3+3?",
                    "final_answer": "6",
                    "verified": True,
                    "entropy": 0.1,
                    "risk": 0.1,
                    "difficulty": "easy",
                    "subject": "math",
                    "concept_cluster": ["algebra"],
                },
            ]

            result = trainer.train(
                rows,
                val_rows=rows,
                test_rows=rows,
                config=TrainerConfig(
                    epochs=3,
                    early_stopping_patience=2,
                    learning_rate=0.25,
                    gradient_accumulation_steps=1,
                    run_prefix="unitmini",
                    offline_mode=True,
                ),
            )

            self.assertTrue(Path(result["best_model_path"]).exists())
            self.assertTrue(Path(result["calibration_head_path"]).exists())
            self.assertTrue(Path(result["tokenizer_path"]).exists())
            self.assertTrue(Path(result["training_metadata_path"]).exists())
            self.assertTrue(Path(result["run_dir"], "logs", "confidence_error_distribution.json").exists())
            self.assertTrue(result["runtime_engine_untouched"])

    def test_shadow_evaluator_scores_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shadow_log = root / "shadow.jsonl"
            checkpoint = root / "model.json"

            shadow_rows = [
                {
                    "question": "What is 2+2?",
                    "mini_answer": "4",
                    "arena_winner_answer": "4",
                    "winner_verified": True,
                },
                {
                    "question": "What is 3+3?",
                    "mini_answer": "7",
                    "arena_winner_answer": "6",
                    "winner_verified": True,
                },
            ]
            with shadow_log.open("w", encoding="utf-8") as f:
                for row in shadow_rows:
                    f.write(json.dumps(row) + "\n")

            checkpoint.write_text(
                json.dumps(
                    {
                        "model_state": {
                            "memory": {
                                "what is 2+2?": {"final_answer": "4", "confidence": 0.9},
                                "what is 3+3?": {"final_answer": "6", "confidence": 0.8},
                            },
                            "default_answer": "0",
                            "default_confidence": 0.4,
                        }
                    }
                ),
                encoding="utf-8",
            )

            evaluator = MiniShadowEvaluator(shadow_log_path=str(shadow_log), output_dir=str(root / "shadow_out"))
            report = evaluator.evaluate_checkpoint(str(checkpoint))
            self.assertEqual(report["samples"], 2)
            self.assertGreaterEqual(report["upgrade_candidate_score"], 0.0)
            self.assertLessEqual(report["promotion_readiness_score"], 1.0)


class MiniResearchUpgradeTests(unittest.TestCase):
    def test_replay_priority_ordering_and_rare_cluster_amplification(self):
        engine = MiniDistillationEngine()
        rows = [
            {
                "question": "common easy",
                "verified": False,
                "risk": 0.5,
                "entropy": 0.4,
                "disagreement": 0.1,
                "confidence": 0.9,
                "concept_cluster": ["algebra"],
                "difficulty": "easy",
                "ts": "2026-02-20T00:00:00+00:00",
            },
            {
                "question": "rare hard disagreement",
                "verified": False,
                "risk": 0.8,
                "entropy": 0.9,
                "disagreement": 0.8,
                "confidence": 0.95,
                "concept_cluster": ["rare_topic"],
                "difficulty": "hard",
                "ts": "2026-02-26T00:00:00+00:00",
            },
        ]
        hard = engine.mine_hard_negatives(rows, top_k=2, use_advanced_scoring=True)
        self.assertEqual(hard[0]["question"], "rare hard disagreement")
        self.assertGreater(hard[0]["replay_priority_score"], hard[1]["replay_priority_score"])

    def test_replay_diagnostics_export_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine = MiniDistillationEngine()
            rows = [
                {"question": "q1", "risk": 0.8, "entropy": 0.7, "disagreement": 0.6, "verified": False, "concept_cluster": ["rare"]},
                {"question": "q2", "risk": 0.3, "entropy": 0.2, "disagreement": 0.0, "verified": True, "concept_cluster": ["common"]},
            ]
            paths = engine.export_replay_diagnostics(rows, output_dir=tmp)
            self.assertTrue(Path(paths["replay_sampling_distribution"]).exists())
            self.assertTrue(Path(paths["hard_case_weight_analysis"]).exists())
            self.assertTrue(Path(paths["replay_explainability_log"]).exists())

    def test_curriculum_stage_gating(self):
        with tempfile.TemporaryDirectory() as tmp:
            scheduler = CurriculumScheduler(output_dir=tmp)
            rows = [
                {"question": "q1", "verified": True, "entropy": 0.1, "difficulty": "easy", "concept_cluster": ["algebra"]},
                {"question": "q2", "verified": False, "entropy": 0.7, "disagreement": 0.8, "risk": 0.8, "concept_cluster": ["trap", "algebra"]},
            ]
            diag = scheduler.schedule(
                rows,
                metrics_history=[{"hard_case_accuracy": 0.2, "calibration_brier": 0.4, "overconfidence_rate": 0.5}],
                config=CurriculumConfig(
                    thresholds=CurriculumThresholds(
                        min_hard_case_accuracy=0.6,
                        max_calibration_brier=0.2,
                        max_overconfidence_rate=0.2,
                    )
                ),
            )
            transitions = diag["stage_transition_log"]
            self.assertTrue(transitions[0]["unlocked"])
            # second stage should be blocked by gate.
            self.assertFalse(transitions[1]["unlocked"])

    def test_curriculum_regression_guard_blocks_progression(self):
        with tempfile.TemporaryDirectory() as tmp:
            scheduler = CurriculumScheduler(output_dir=tmp)
            rows = [
                {"question": "q1", "verified": True, "entropy": 0.1, "difficulty": "easy", "concept_cluster": ["algebra"]},
                {"question": "q2", "verified": False, "entropy": 0.8, "disagreement": 0.7, "risk": 0.8, "concept_cluster": ["trap", "algebra"]},
            ]
            diag = scheduler.schedule(
                rows,
                metrics_history=[
                    {"hard_case_accuracy": 0.80, "calibration_brier": 0.10, "overconfidence_rate": 0.10},
                    {"hard_case_accuracy": 0.75, "calibration_brier": 0.10, "overconfidence_rate": 0.10},
                ],
            )
            self.assertFalse(diag["stage_transition_log"][1]["unlocked"])

    def test_overconfidence_penalty_behavior(self):
        trainer = MiniTrainer()
        cfg = TrainerConfig(
            epochs=1,
            calibration_guard_mode=True,
            overconfidence_threshold=0.85,
            overconfidence_penalty=1.5,
        )
        model_state = {
            "memory": {"what is 2+2?": {"final_answer": "5", "confidence": 0.95}},
            "default_answer": "",
            "default_confidence": 0.5,
        }
        rows = [
            {
                "question": "What is 2+2?",
                "final_answer": "4",
                "verified": False,
                "loss_weight": 1.0,
                "margin_weight": 1.0,
            }
        ]
        _, _, stats = trainer._train_epoch(rows=rows, model_state=model_state, cfg=cfg)
        self.assertEqual(stats["suppressed_rows"], 1)

    def test_calibration_ece_and_bootstrap_ci(self):
        harness = MiniEvaluationHarness(output_dir="data/mini_training/evaluation")
        rows = [
            {"question": "q1", "final_answer": "1", "entropy": 0.1, "risk": 0.1, "difficulty": "easy", "subject": "math", "concept_cluster": ["a"]},
            {"question": "q2", "final_answer": "2", "entropy": 0.9, "risk": 0.9, "difficulty": "hard", "subject": "math", "concept_cluster": ["b"]},
            {"question": "q3", "final_answer": "3", "entropy": 0.8, "risk": 0.8, "difficulty": "hard", "subject": "math", "concept_cluster": ["b", "c"], "disagreement": 0.6},
            {"question": "q4", "final_answer": "4", "entropy": 0.2, "risk": 0.2, "difficulty": "easy", "subject": "math", "concept_cluster": ["a"]},
        ]
        preds = [
            {"final_answer": "1", "confidence": 0.95},
            {"final_answer": "9", "confidence": 0.90},
            {"final_answer": "3", "confidence": 0.55},
            {"final_answer": "0", "confidence": 0.60},
        ]
        metrics = harness.evaluate(rows, predictions=preds, bootstrap_samples=80, bootstrap_seed=9)
        self.assertIn("ece", metrics)
        self.assertIn("mce", metrics)
        self.assertIn("bootstrap_ci", metrics)
        self.assertGreaterEqual(metrics["ece"], 0.0)
        self.assertIn("exact_match_ci95", metrics["bootstrap_ci"])

    def test_ece_mce_perfect_predictions_zero(self):
        harness = MiniEvaluationHarness(output_dir="data/mini_training/evaluation")
        rows = [
            {"question": "q1", "final_answer": "1", "entropy": 0.1, "risk": 0.1, "difficulty": "easy", "subject": "math", "concept_cluster": ["a"]},
            {"question": "q2", "final_answer": "2", "entropy": 0.1, "risk": 0.1, "difficulty": "easy", "subject": "math", "concept_cluster": ["a"]},
        ]
        preds = [
            {"final_answer": "1", "confidence": 1.0},
            {"final_answer": "2", "confidence": 1.0},
        ]
        metrics = harness.evaluate(rows, predictions=preds, bootstrap_samples=30)
        self.assertAlmostEqual(metrics["ece"], 0.0, places=6)
        self.assertAlmostEqual(metrics["mce"], 0.0, places=6)

    def test_traffic_simulation_regression_detection(self):
        simulator = MiniTrafficSimulator(output_dir="data/mini_training/traffic_simulation")
        rows = [
            {
                "question": "What is 2+2?",
                "arena_winner_answer": "4",
                "winner_verified": True,
                "mini_answer": "4",
                "mini_confidence": 0.9,
                "difficulty": "easy",
                "risk": 0.1,
                "concept_cluster": ["algebra"],
            },
            {
                "question": "What is 3+3?",
                "arena_winner_answer": "6",
                "winner_verified": True,
                "mini_answer": "6",
                "mini_confidence": 0.9,
                "difficulty": "hard",
                "risk": 0.8,
                "concept_cluster": ["calculus"],
            },
        ]
        candidate_state = {
            "memory": {
                "what is 2+2?": {"final_answer": "9", "confidence": 0.95},
                "what is 3+3?": {"final_answer": "7", "confidence": 0.95},
            },
            "default_answer": "0",
            "default_confidence": 0.4,
        }
        report = simulator.simulate_from_rows(rows, candidate_model_state=candidate_state)
        self.assertIn("regression_risk_score", report)
        self.assertGreater(report["regression_risk_score"], 0.0)
        self.assertFalse(report["safe_to_promote"])

    def test_promotion_policy_hardened_blocking(self):
        policy = MiniPromotionPolicy()
        decision = policy.evaluate(
            {
                "exact_match": 0.8,
                "hard_case_accuracy": 0.4,
                "rare_concept_accuracy": 0.2,
                "calibration_brier": 0.25,
                "overconfidence_rate": 0.4,
                "concept_cluster_accuracy": {"a": 0.2, "b": 0.0},
            },
            baseline_metrics={"hard_case_accuracy": 0.7, "rare_concept_accuracy": 0.6, "calibration_brier": 0.1},
            traffic_simulation={"safe_to_promote": False, "regression_risk_score": 0.9},
            self_disagreement={"self_disagreement_rate": 0.8},
        )
        self.assertFalse(decision["promote"])
        self.assertGreater(len(decision["block_reasons"]), 0)
        self.assertTrue(decision["rollback_required"])

    def test_api_free_enforcement_blocks_network_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            trainer = MiniTrainer(checkpoint_root=str(Path(tmp) / "check"), evaluation_output_dir=str(Path(tmp) / "eval"))
            rows = [
                {
                    "question": "What is 2+2?",
                    "final_answer": "4",
                    "verified": True,
                    "entropy": 0.1,
                    "risk": 0.1,
                    "difficulty": "easy",
                    "subject": "math",
                    "concept_cluster": ["algebra"],
                }
            ]

            def probe():
                socket.create_connection(("example.com", 80), timeout=0.1)

            with self.assertRaises(RuntimeError):
                trainer.train(
                    rows,
                    val_rows=rows,
                    test_rows=rows,
                    config=TrainerConfig(epochs=1, api_free_probe=probe, enforce_api_free_mode=True, offline_mode=True),
                )

    def test_api_free_enforcement_blocks_provider_generate(self):
        from core.lalacore_x.providers import ProviderFabric

        with tempfile.TemporaryDirectory() as tmp:
            trainer = MiniTrainer(checkpoint_root=str(Path(tmp) / "check"), evaluation_output_dir=str(Path(tmp) / "eval"))
            rows = [
                {
                    "question": "What is 2+2?",
                    "final_answer": "4",
                    "verified": True,
                    "entropy": 0.1,
                    "risk": 0.1,
                    "difficulty": "easy",
                    "subject": "math",
                    "concept_cluster": ["algebra"],
                }
            ]

            def probe_provider_generate():
                asyncio.run(ProviderFabric.generate(None, "offline probe", provider="mini"))

            with self.assertRaises(RuntimeError):
                trainer.train(
                    rows,
                    val_rows=rows,
                    test_rows=rows,
                    config=TrainerConfig(
                        epochs=1,
                        api_free_probe=probe_provider_generate,
                        enforce_api_free_mode=True,
                        offline_mode=True,
                    ),
                )

    def test_adapter_config_generation_mocked(self):
        trainer = MiniTrainer()
        cfg = TrainerConfig(enable_lora=True, lora_rank=16, lora_alpha=32, lora_dropout=0.1, adapter_name="test_adapter")
        payload = trainer.build_lora_adapter_config(cfg)
        self.assertEqual(payload["r"], 16)
        self.assertEqual(payload["lora_alpha"], 32)
        self.assertEqual(payload["adapter_name"], "test_adapter")
        self.assertIn("target_modules", payload)

    def test_experiment_tracker_checksum_consistency(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = MiniExperimentTracker(output_dir=tmp)
            rows = [{"question": "q1", "final_answer": "1"}, {"question": "q2", "final_answer": "2"}]
            c1 = tracker.dataset_checksum(rows)
            c2 = tracker.dataset_checksum(rows)
            self.assertEqual(c1, c2)
            files = tracker.write_experiment(
                experiment_name="unit",
                hyperparameters={"lr": 0.1},
                dataset_rows=rows,
                evaluation_metrics={"exact_match": 0.8},
            )
            self.assertTrue(Path(files["experiment_summary"]).exists())
            self.assertTrue(Path(files["experiment_manifest"]).exists())
            self.assertTrue(Path(files["reproducibility_hash"]).exists())

    def test_internal_consistency_analysis(self):
        analyzer = MiniInternalConsistencyAnalyzer(output_dir="data/mini_training/internal_consistency")
        report = analyzer.analyze_samples(
            [
                {"final_answer": "4", "reasoning_summary": "a b c", "entropy": 0.2},
                {"final_answer": "4", "reasoning_summary": "a b c", "entropy": 0.25},
                {"final_answer": "5", "reasoning_summary": "x y z", "entropy": 0.7},
            ],
            concept_clusters=["algebra", "trap"],
        )
        self.assertIn("stability_score", report)
        self.assertGreaterEqual(report["self_disagreement_rate"], 0.0)
        self.assertTrue(Path("data/mini_training/internal_consistency/stability_diagnostics.json").exists())


if __name__ == "__main__":
    unittest.main()
