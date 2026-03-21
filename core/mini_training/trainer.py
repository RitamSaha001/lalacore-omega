"""Offline-first Mini training loop with optional HF and LoRA/PEFT research paths."""

from __future__ import annotations

import json
import socket
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Mapping, Sequence

from core.mini_training.checkpoint_manager import CheckpointManager
from core.mini_training.curriculum_scheduler import CurriculumScheduler
from core.mini_training.distillation_engine import CalibrationHeadState, MiniDistillationEngine
from core.mini_training.evaluation import MiniEvaluationHarness
from core.mini_training.experiment_tracker import MiniExperimentTracker
from core.mini_training.internal_consistency import MiniInternalConsistencyAnalyzer
from core.mini_training.kaggle_curriculum import KaggleHardCaseCurriculum
from core.mini_training.promotion_policy import MiniPromotionPolicy
from core.mini_training.reproducibility import set_global_seed
from core.mini_training.traffic_simulator import MiniTrafficSimulator


try:  # pragma: no cover - optional dependency
    import torch
except Exception:  # pragma: no cover - optional dependency
    torch = None

try:  # pragma: no cover - optional dependency
    from transformers import Trainer, TrainingArguments
except Exception:  # pragma: no cover - optional dependency
    Trainer = None
    TrainingArguments = None

try:  # pragma: no cover - optional dependency
    from peft import LoraConfig, TaskType, get_peft_model
except Exception:  # pragma: no cover - optional dependency
    LoraConfig = None
    TaskType = None
    get_peft_model = None


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _norm_question(question: str) -> str:
    return " ".join(str(question or "").strip().lower().split())


@dataclass(slots=True)
class TrainerConfig:
    """Training configuration for offline Mini distillation runs."""

    epochs: int = 8
    batch_size: int = 16
    gradient_accumulation_steps: int = 1
    learning_rate: float = 0.20
    early_stopping_patience: int = 3
    min_improvement: float = 1e-4
    ema_decay: float = 0.92
    mixed_precision: bool = True
    seed: int = 42
    deterministic: bool = True
    offline_mode: bool = True
    enforce_api_free_mode: bool = True
    api_free_probe: Any = None
    use_hf_trainer: bool = False
    hf_max_steps: int = 0
    max_rows_per_epoch: int | None = None
    run_prefix: str = "mini_train"
    strict: bool = False
    # Hard-case specialist mode.
    hard_case_specialist_mode: bool = True
    use_kaggle_curriculum: bool = False
    # Overconfidence suppression in training loss only.
    calibration_guard_mode: bool = True
    overconfidence_threshold: float = 0.85
    overconfidence_penalty: float = 1.8
    overconfidence_entropy_threshold: float = 0.30
    stability_selection_sigma: float = 1.5
    # LoRA / PEFT path (additive and optional).
    enable_lora: bool = False
    lora_rank: int = 8
    lora_alpha: int = 16
    lora_dropout: float = 0.05
    lora_target_modules: tuple[str, ...] = ("q_proj", "v_proj")
    lora_bias: str = "none"
    freeze_base_model: bool = True
    gradient_checkpointing: bool = False
    merge_adapter_for_export: bool = False
    adapter_only_checkpoint: bool = True
    adapter_name: str = "mini_lora_adapter"


class MiniTrainer:
    """Research-grade, additive Mini trainer that does not touch runtime inference logic."""

    def __init__(
        self,
        *,
        checkpoint_root: str = "data/mini_training/checkpoints",
        evaluation_output_dir: str = "data/mini_training/evaluation",
    ) -> None:
        self.checkpoint_root = str(checkpoint_root)
        self.evaluation_output_dir = str(evaluation_output_dir)
        self.distillation_engine = MiniDistillationEngine()
        self.evaluator = MiniEvaluationHarness(output_dir=self.evaluation_output_dir)
        self.checkpoint_manager = CheckpointManager(root=self.checkpoint_root, run_prefix="mini_train")

    def train(
        self,
        train_rows: Sequence[Dict[str, Any]],
        *,
        val_rows: Sequence[Dict[str, Any]] | None = None,
        test_rows: Sequence[Dict[str, Any]] | None = None,
        config: TrainerConfig | None = None,
        initial_model_state: Mapping[str, Any] | None = None,
        hf_components: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Train Mini from offline rows and export best model/calibration/tokenizer artifacts."""
        cfg = config or TrainerConfig()
        if cfg.offline_mode and cfg.enforce_api_free_mode:
            with self._api_free_guard():
                if callable(cfg.api_free_probe):
                    cfg.api_free_probe()
                return self._train_impl(
                    train_rows=train_rows,
                    val_rows=val_rows,
                    test_rows=test_rows,
                    cfg=cfg,
                    initial_model_state=initial_model_state,
                    hf_components=hf_components,
                )
        return self._train_impl(
            train_rows=train_rows,
            val_rows=val_rows,
            test_rows=test_rows,
            cfg=cfg,
            initial_model_state=initial_model_state,
            hf_components=hf_components,
        )

    def _train_impl(
        self,
        *,
        train_rows: Sequence[Dict[str, Any]],
        val_rows: Sequence[Dict[str, Any]] | None,
        test_rows: Sequence[Dict[str, Any]] | None,
        cfg: TrainerConfig,
        initial_model_state: Mapping[str, Any] | None,
        hf_components: Mapping[str, Any] | None,
    ) -> Dict[str, Any]:
        val = list(val_rows or [])
        test = list(test_rows or [])
        train = list(train_rows or [])

        seed_meta = set_global_seed(int(cfg.seed), deterministic=bool(cfg.deterministic))
        manager = CheckpointManager(root=self.checkpoint_root, run_prefix=str(cfg.run_prefix))
        run = manager.create_run()
        tracker = MiniExperimentTracker(output_dir=str(run.run_dir / "experiments"))

        amp_enabled = bool(
            cfg.mixed_precision
            and torch is not None
            and hasattr(torch, "cuda")
            and bool(torch.cuda.is_available())
        )

        manager.write_hyperparameters(run, asdict(cfg))
        manager.write_training_metadata(
            run,
            {
                "stage": "init",
                "offline_mode": bool(cfg.offline_mode),
                "api_free_enforced": bool(cfg.offline_mode and cfg.enforce_api_free_mode),
                "seed_meta": seed_meta,
                "amp_enabled": amp_enabled,
                "train_rows": len(train),
                "val_rows": len(val),
                "test_rows": len(test),
            },
        )

        distilled_rows = self.distillation_engine.build_distillation_rows(train)
        replay_exports = self.distillation_engine.export_replay_diagnostics(
            distilled_rows,
            output_dir=str(run.logs_dir),
        )
        if cfg.hard_case_specialist_mode:
            ordered_rows = self.distillation_engine.mine_hard_negatives(
                distilled_rows,
                top_k=max(1, len(distilled_rows)),
                use_advanced_scoring=True,
            )
        else:
            ordered_rows = list(distilled_rows)

        curriculum_stage_log: List[Dict[str, Any]] = []
        if cfg.use_kaggle_curriculum:
            curriculum = KaggleHardCaseCurriculum(output_dir=str(run.logs_dir / "kaggle_curriculum")).build(
                ordered_rows,
                metrics_history=[],
            )
            curriculum_stage_log = list(curriculum.get("stage_transition_log", []))
            flattened: List[Dict[str, Any]] = []
            for batch in curriculum.get("ordered_training_batches", []):
                flattened.extend(list(batch.get("rows", [])))
            if flattened:
                ordered_rows = flattened

        model_state = self._bootstrap_state(initial_model_state)
        lora_status = self._maybe_lora_integration(cfg=cfg, run_dir=run.run_dir, hf_components=hf_components)
        hf_status = self._maybe_hf_integration(
            cfg=cfg,
            run_dir=run.run_dir,
            rows=ordered_rows,
            hf_components=hf_components,
        )

        best_metric = float("-inf")
        best_payload: Dict[str, Any] | None = None
        best_epoch = 0
        no_improve_epochs = 0
        ema_exact = 0.0
        ema_brier = 0.0
        prev_val_metrics: Dict[str, Any] | None = None
        suppression_log: List[Dict[str, Any]] = []
        confidence_error_distribution: List[Dict[str, Any]] = []
        val_metrics_history: List[Dict[str, Any]] = []
        bootstrap_selection_rows: List[Dict[str, Any]] = []
        epoch_payloads: Dict[int, Dict[str, Any]] = {}
        heatmap_totals: Dict[str, Dict[str, float]] = {}

        max_epochs = max(1, int(cfg.epochs))
        for epoch in range(1, max_epochs + 1):
            epoch_t0 = perf_counter()
            train_loss, step_count, suppression_stats = self._train_epoch(rows=ordered_rows, model_state=model_state, cfg=cfg)
            suppression_stats["epoch"] = epoch
            suppression_log.append(suppression_stats)
            confidence_error_distribution.append(
                {
                    "epoch": int(epoch),
                    "threshold": float(cfg.overconfidence_threshold),
                    "penalty_factor": float(cfg.overconfidence_penalty),
                    "suppressed_rows": int(suppression_stats.get("suppressed_rows", 0)),
                    "mean_error_before_update": float(suppression_stats.get("mean_error_before_update", 0.0)),
                }
            )

            train_predictions = self.evaluator.predict_with_memory_model(train, model_state)
            train_metrics = self.evaluator.evaluate(
                train,
                predictions=train_predictions,
                tag=f"train_epoch_{epoch}",
                previous_metrics=prev_val_metrics,
                bootstrap_samples=120,
            )

            val_predictions = self.evaluator.predict_with_memory_model(val, model_state)
            val_metrics = self.evaluator.evaluate(
                val,
                predictions=val_predictions,
                tag=f"val_epoch_{epoch}",
                previous_metrics=prev_val_metrics,
                bootstrap_samples=120,
            )
            prev_val_metrics = dict(val_metrics)
            val_exact = float(val_metrics.get("exact_match", 0.0))
            val_brier = float(val_metrics.get("calibration_brier", 1.0))

            if epoch == 1:
                ema_exact = val_exact
                ema_brier = val_brier
            else:
                decay = _clamp(float(cfg.ema_decay), 0.0, 0.9999)
                ema_exact = decay * ema_exact + (1.0 - decay) * val_exact
                ema_brier = decay * ema_brier + (1.0 - decay) * val_brier

            bootstrap_ci = val_metrics.get("bootstrap_ci", {})
            if not isinstance(bootstrap_ci, Mapping):
                bootstrap_ci = {}
            hard_mean = float(bootstrap_ci.get("hard_case_accuracy_mean", val_metrics.get("hard_case_accuracy", 0.0)))
            hard_std = float(bootstrap_ci.get("hard_case_accuracy_std", 0.0))
            selection_score = float(hard_mean - float(cfg.stability_selection_sigma) * hard_std)
            bootstrap_selection_rows.append(
                {
                    "epoch": int(epoch),
                    "hard_case_accuracy": float(val_metrics.get("hard_case_accuracy", 0.0)),
                    "bootstrap_ci": dict(bootstrap_ci),
                }
            )
            val_metrics_history.append(dict(val_metrics))
            epoch_payloads[int(epoch)] = {
                "epoch": int(epoch),
                "model_state": self._clone_model_state(model_state),
                "val_metrics": dict(val_metrics),
                "train_metrics": dict(train_metrics),
                "selection_score": float(selection_score),
            }

            improved = (selection_score - best_metric) > float(cfg.min_improvement)
            if improved:
                best_metric = selection_score
                best_epoch = epoch
                best_payload = dict(epoch_payloads[int(epoch)])
                no_improve_epochs = 0
            else:
                no_improve_epochs += 1

            elapsed_s = perf_counter() - epoch_t0
            epoch_metrics = {
                "epoch": int(epoch),
                "train_loss": float(train_loss),
                "steps": int(step_count),
                "elapsed_s": float(elapsed_s),
                "train_exact_match": float(train_metrics.get("exact_match", 0.0)),
                "val_exact_match": float(val_exact),
                "val_brier": float(val_brier),
                "val_selection_score": float(selection_score),
                "ema_exact_match": float(ema_exact),
                "ema_brier": float(ema_brier),
                "amp_enabled": amp_enabled,
                "overconfidence_suppressed_rows": int(suppression_stats.get("suppressed_rows", 0)),
            }
            manager.write_epoch_checkpoint(
                run,
                epoch=epoch,
                model_state=self._clone_model_state(model_state),
                metrics=epoch_metrics,
                ema_state={"ema_exact_match": ema_exact, "ema_brier": ema_brier},
            )
            manager.append_jsonl(run.logs_dir / "losses.jsonl", epoch_metrics)
            for key, cell in dict(suppression_stats.get("heatmap_counts", {})).items():
                if not isinstance(cell, Mapping):
                    continue
                slot = heatmap_totals.setdefault(str(key), {"suppressed": 0.0, "total": 0.0})
                slot["suppressed"] += float(cell.get("suppressed", 0.0))
                slot["total"] += float(cell.get("total", 0.0))

            if no_improve_epochs >= max(1, int(cfg.early_stopping_patience)):
                break

        if best_payload is None:
            best_payload = {
                "epoch": 0,
                "model_state": self._clone_model_state(model_state),
                "val_metrics": {},
                "train_metrics": {},
                "selection_score": 0.0,
            }

        selection_evaluator = MiniEvaluationHarness(output_dir=str(run.logs_dir))
        bootstrap_selection_payload = selection_evaluator.select_checkpoint_by_bootstrap(
            bootstrap_selection_rows,
            filename="bootstrap_selection.json",
        )
        selected_epoch = int(dict(bootstrap_selection_payload.get("selected", {})).get("epoch", 0))
        if selected_epoch > 0 and selected_epoch in epoch_payloads:
            best_payload = dict(epoch_payloads[selected_epoch])
            best_epoch = selected_epoch
            best_metric = float(best_payload.get("selection_score", best_metric))

        best_model_state = dict(best_payload.get("model_state", {}))
        best_model_path = manager.write_best_model(
            run,
            {
                "best_epoch": int(best_epoch or best_payload.get("epoch", 0)),
                "best_metric": float(best_metric if best_metric > float("-inf") else 0.0),
                "model_state": best_model_state,
                "val_metrics": dict(best_payload.get("val_metrics", {})),
                "train_metrics": dict(best_payload.get("train_metrics", {})),
                "hf_integration": hf_status,
                "lora_integration": lora_status,
            },
        )

        calibration_rows = val if val else train
        calibration_state = self.distillation_engine.train_calibration_head(calibration_rows, epochs=max(8, max_epochs * 4), lr=0.12)
        calibration_path = manager.write_calibration_head(run, self._calibration_state_to_dict(calibration_state))
        tokenizer_path = manager.write_tokenizer_stub(
            run,
            {
                "tokenizer_type": "lalacore-mini-memory-v1",
                "offline_mode": bool(cfg.offline_mode),
                "vocab_size": 0,
                "normalization": "lower+whitespace",
            },
        )

        suppression_path = run.logs_dir / "overconfidence_suppression_effect.json"
        suppression_path.write_text(json.dumps(suppression_log, indent=2, sort_keys=True), encoding="utf-8")
        confidence_error_path = run.logs_dir / "confidence_error_distribution.json"
        confidence_error_path.write_text(json.dumps(confidence_error_distribution, indent=2, sort_keys=True), encoding="utf-8")
        overconfidence_heatmap = []
        for key, cell in sorted(heatmap_totals.items()):
            total = float(cell.get("total", 0.0))
            suppressed = float(cell.get("suppressed", 0.0))
            overconfidence_heatmap.append(
                {
                    "bucket": str(key),
                    "suppressed": int(round(suppressed)),
                    "total": int(round(total)),
                    "suppression_rate": float((suppressed / total) if total > 0 else 0.0),
                }
            )
        overconfidence_heatmap_path = run.logs_dir / "overconfidence_heatmap.json"
        overconfidence_heatmap_path.write_text(
            json.dumps(overconfidence_heatmap, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        test_metrics: Dict[str, Any] = {}
        eval_outputs: Dict[str, str] = {}
        run_evaluator = MiniEvaluationHarness(output_dir=str(run.logs_dir))
        if test:
            test_predictions = run_evaluator.predict_with_memory_model(test, best_model_state)
            test_metrics = run_evaluator.evaluate(
                test,
                predictions=test_predictions,
                tag="test",
                previous_metrics=best_payload.get("val_metrics", {}),
                bootstrap_samples=1000,
            )
            eval_outputs = {
                "metrics_json": str(run_evaluator.write_metrics_json(test_metrics, filename="test_metrics.json")),
                "calibration_diagnostics": str(run_evaluator.write_calibration_diagnostics(test_metrics)),
                "calibration_stratified": str(run_evaluator.write_calibration_stratified(test_metrics)),
                "kaggle_diagnostics": str(run_evaluator.write_kaggle_diagnostics(test_metrics)),
                "leaderboard_csv": str(run_evaluator.write_leaderboard_csv(test_metrics, filename="leaderboard.csv")),
                "leaderboard_simulation": str(run_evaluator.write_leaderboard_simulation_csv(test_metrics)),
                "kaggle_submission": str(
                    run_evaluator.write_kaggle_submission(test, test_predictions, filename="kaggle_submission.csv")
                ),
            }
        else:
            baseline_eval = dict(best_payload.get("val_metrics", {}))
            eval_outputs = {
                "metrics_json": str(run_evaluator.write_metrics_json(baseline_eval, filename="val_metrics.json")),
                "calibration_diagnostics": str(run_evaluator.write_calibration_diagnostics(baseline_eval)),
                "calibration_stratified": str(run_evaluator.write_calibration_stratified(baseline_eval)),
                "kaggle_diagnostics": str(run_evaluator.write_kaggle_diagnostics(baseline_eval)),
            }

        selection_path = str(bootstrap_selection_payload.get("path", str(run.logs_dir / "bootstrap_selection.json")))

        eval_rows = test if test else (val if val else train)
        traffic_simulator = MiniTrafficSimulator(output_dir=str(run.logs_dir))
        stress_report = traffic_simulator.simulate_from_rows(
            eval_rows,
            candidate_model_state=best_model_state,
        )
        stress_report_path = run.logs_dir / "stress_simulation_report.json"
        traffic_report_path = run.logs_dir / "traffic_simulation_report.json"

        stability_analyzer = MiniInternalConsistencyAnalyzer(output_dir=str(run.logs_dir))
        stability_report = stability_analyzer.analyze_model_state(
            eval_rows,
            model_state=best_model_state,
            runs=5,
            seed=int(cfg.seed),
        )
        stability_report_path = run.logs_dir / "stability_diagnostics.json"

        replay_frequency: Dict[str, int] = {}
        concept_priority: Dict[str, float] = {}
        for row in ordered_rows:
            q = _norm_question(str(row.get("question", "")))
            if q:
                replay_frequency[q] = replay_frequency.get(q, 0) + 1
            clusters = row.get("concept_cluster", [])
            if isinstance(clusters, list):
                weight = float(row.get("replay_priority_score", 1.0))
                for cluster in clusters:
                    key = str(cluster).strip().lower()
                    if not key:
                        continue
                    concept_priority[key] = max(concept_priority.get(key, 1.0), weight)

        curriculum_scheduler = CurriculumScheduler(output_dir=str(run.logs_dir))
        curriculum_diagnostics = curriculum_scheduler.schedule(
            ordered_rows,
            metrics_history=val_metrics_history,
            replay_frequency=replay_frequency,
            concept_priority=concept_priority,
        )
        curriculum_progression_path = str(
            curriculum_diagnostics.get(
                "curriculum_progression_diagnostics_path",
                str(run.logs_dir / "curriculum_progression_diagnostics.json"),
            )
        )

        promotion_policy = MiniPromotionPolicy()
        promotion_decision = promotion_policy.evaluate(
            test_metrics if test_metrics else dict(best_payload.get("val_metrics", {})),
            baseline_metrics=dict(best_payload.get("train_metrics", {})),
            traffic_simulation=stress_report,
            self_disagreement=stability_report,
        )

        experiment_files = tracker.write_experiment(
            experiment_name=str(cfg.run_prefix),
            hyperparameters=asdict(cfg),
            dataset_rows=train,
            curriculum_transitions=curriculum_stage_log,
            evaluation_metrics=test_metrics or dict(best_payload.get("val_metrics", {})),
            replay_distribution=replay_exports,
            calibration_state=self._calibration_state_to_dict(calibration_state),
            shadow_results={"traffic_simulation": stress_report, "stability": stability_report},
            promotion_decision=promotion_decision,
        )

        metadata = {
            "stage": "completed",
            "runtime_engine_untouched": True,
            "best_epoch": int(best_epoch),
            "best_exact_match": float(best_metric if best_metric > float("-inf") else 0.0),
            "seed_meta": seed_meta,
            "hf_integration": hf_status,
            "lora_integration": lora_status,
            "artifacts": {
                "run_dir": str(run.run_dir),
                "best_model": str(best_model_path),
                "calibration_head": str(calibration_path),
                "tokenizer": str(tokenizer_path),
                "overconfidence_suppression_effect": str(suppression_path),
                "confidence_error_distribution": str(confidence_error_path),
                "overconfidence_heatmap": str(overconfidence_heatmap_path),
                "bootstrap_selection": selection_path,
                "traffic_simulation_report": str(traffic_report_path),
                "stress_simulation_report": str(stress_report_path),
                "stability_diagnostics": str(stability_report_path),
                "curriculum_progression_diagnostics": curriculum_progression_path,
                **replay_exports,
                **experiment_files,
                **eval_outputs,
            },
            "promotion_decision": promotion_decision,
        }
        metadata_path = manager.write_training_metadata(run, metadata)

        return {
            "run_dir": str(run.run_dir),
            "best_model_path": str(best_model_path),
            "calibration_head_path": str(calibration_path),
            "tokenizer_path": str(tokenizer_path),
            "training_metadata_path": str(metadata_path),
            "best_epoch": int(best_epoch),
            "best_exact_match": float(best_metric if best_metric > float("-inf") else 0.0),
            "test_metrics": test_metrics,
            "hf_integration": hf_status,
            "lora_integration": lora_status,
            "replay_exports": replay_exports,
            "runtime_engine_untouched": True,
            "api_usage_changed": False,
            "bootstrap_selection_path": selection_path,
            "stress_simulation_report_path": str(stress_report_path),
            "stability_diagnostics_path": str(stability_report_path),
            "curriculum_progression_diagnostics_path": curriculum_progression_path,
        }

    def build_lora_adapter_config(self, cfg: TrainerConfig) -> Dict[str, Any]:
        """Build deterministic adapter config payload used for PEFT integration and tests."""
        return {
            "r": int(max(1, cfg.lora_rank)),
            "lora_alpha": int(max(1, cfg.lora_alpha)),
            "lora_dropout": float(_clamp(cfg.lora_dropout)),
            "target_modules": [str(module) for module in cfg.lora_target_modules],
            "bias": str(cfg.lora_bias),
            "task_type": "CAUSAL_LM",
            "adapter_name": str(cfg.adapter_name),
            "freeze_base_model": bool(cfg.freeze_base_model),
            "gradient_checkpointing": bool(cfg.gradient_checkpointing),
        }

    def _train_epoch(self, *, rows: Sequence[Dict[str, Any]], model_state: Dict[str, Any], cfg: TrainerConfig) -> tuple[float, int, Dict[str, Any]]:
        """Run one lightweight offline epoch with gradient-accumulation style updates."""
        if not rows:
            return 0.0, 0, {
                "suppressed_rows": 0,
                "penalty_factor": float(cfg.overconfidence_penalty),
                "heatmap_counts": {},
            }

        memory = model_state.setdefault("memory", {})
        default_conf = _clamp(float(model_state.get("default_confidence", 0.5)))
        batch_rows = list(rows)
        if cfg.max_rows_per_epoch is not None:
            limit = max(1, int(cfg.max_rows_per_epoch))
            batch_rows = batch_rows[:limit]

        accum_steps = max(1, int(cfg.gradient_accumulation_steps))
        total_loss = 0.0
        step_count = 0
        suppressed_rows = 0
        error_before_update_total = 0.0
        heatmap_counts: Dict[str, Dict[str, float]] = {}

        def _bucket(conf: float, entropy: float) -> str:
            if conf < 0.70:
                conf_bin = "conf_lt_0.70"
            elif conf < 0.85:
                conf_bin = "conf_0.70_0.85"
            elif conf < 0.93:
                conf_bin = "conf_0.85_0.93"
            else:
                conf_bin = "conf_ge_0.93"
            if entropy < 0.30:
                entropy_bin = "entropy_lt_0.30"
            elif entropy < 0.60:
                entropy_bin = "entropy_0.30_0.60"
            else:
                entropy_bin = "entropy_ge_0.60"
            return f"{conf_bin}|{entropy_bin}"

        pending_grad: Dict[str, float] = {}
        pending_counts: Dict[str, int] = {}

        def flush() -> None:
            for key, grad in pending_grad.items():
                slot = memory.setdefault(key, {"final_answer": "", "confidence": default_conf})
                confidence = _clamp(float(slot.get("confidence", default_conf)))
                denom = max(1, pending_counts.get(key, 1))
                avg_grad = float(grad) / float(denom)
                slot["confidence"] = _clamp(confidence - float(cfg.learning_rate) * avg_grad)
            pending_grad.clear()
            pending_counts.clear()

        for idx, row in enumerate(batch_rows, start=1):
            question = _norm_question(str(row.get("question", "")))
            if not question:
                continue

            slot = memory.setdefault(question, {"final_answer": "", "confidence": default_conf})
            current_conf = _clamp(float(slot.get("confidence", default_conf)))
            target = 1.0 if bool(row.get("verified", False)) else 0.0
            entropy = _clamp(float(row.get("entropy", 0.0)))
            loss_weight = float(row.get("loss_weight", 1.0)) * float(row.get("margin_weight", 1.0))

            if target < 0.5:
                bucket_key = _bucket(current_conf, entropy)
                cell = heatmap_counts.setdefault(bucket_key, {"suppressed": 0.0, "total": 0.0})
                cell["total"] += 1.0

            if (
                bool(cfg.calibration_guard_mode)
                and target < 0.5
                and current_conf > float(cfg.overconfidence_threshold)
                and entropy < float(cfg.overconfidence_entropy_threshold)
            ):
                loss_weight *= float(max(1.0, cfg.overconfidence_penalty))
                suppressed_rows += 1
                bucket_key = _bucket(current_conf, entropy)
                heatmap_counts.setdefault(bucket_key, {"suppressed": 0.0, "total": 0.0})["suppressed"] += 1.0

            error = current_conf - target
            error_before_update_total += abs(error)
            gradient = 2.0 * loss_weight * error
            total_loss += float(loss_weight) * (error * error)
            step_count += 1

            pending_grad[question] = pending_grad.get(question, 0.0) + gradient
            pending_counts[question] = pending_counts.get(question, 0) + 1

            final_answer = str(row.get("final_answer", "")).strip()
            if target >= 0.5 and final_answer:
                slot["final_answer"] = final_answer
            elif not slot.get("final_answer"):
                slot["final_answer"] = final_answer

            if idx % accum_steps == 0:
                flush()

        if pending_grad:
            flush()

        if step_count <= 0:
            return 0.0, 0, {
                "suppressed_rows": 0,
                "penalty_factor": float(cfg.overconfidence_penalty),
                "heatmap_counts": heatmap_counts,
            }
        return (
            float(total_loss / float(step_count)),
            int(step_count),
            {
                "suppressed_rows": int(suppressed_rows),
                "penalty_factor": float(cfg.overconfidence_penalty),
                "threshold": float(cfg.overconfidence_threshold),
                "entropy_threshold": float(cfg.overconfidence_entropy_threshold),
                "mean_error_before_update": float(error_before_update_total / float(step_count)),
                "heatmap_counts": heatmap_counts,
            },
        )

    def _bootstrap_state(self, initial_model_state: Mapping[str, Any] | None) -> Dict[str, Any]:
        state: Dict[str, Any] = {
            "memory": {},
            "default_answer": "",
            "default_confidence": 0.50,
        }
        if not initial_model_state:
            return state

        state["default_answer"] = str(initial_model_state.get("default_answer", ""))
        state["default_confidence"] = _clamp(float(initial_model_state.get("default_confidence", 0.50)))

        memory = initial_model_state.get("memory", {})
        if isinstance(memory, Mapping):
            for raw_key, raw_slot in memory.items():
                key = _norm_question(str(raw_key))
                if not key:
                    continue
                if not isinstance(raw_slot, Mapping):
                    continue
                state["memory"][key] = {
                    "final_answer": str(raw_slot.get("final_answer", "")),
                    "confidence": _clamp(float(raw_slot.get("confidence", state["default_confidence"]))),
                }
        return state

    def _clone_model_state(self, state: Mapping[str, Any]) -> Dict[str, Any]:
        memory = {}
        for key, slot in dict(state.get("memory", {})).items():
            if not isinstance(slot, Mapping):
                continue
            memory[str(key)] = {
                "final_answer": str(slot.get("final_answer", "")),
                "confidence": _clamp(float(slot.get("confidence", 0.50))),
            }
        return {
            "memory": memory,
            "default_answer": str(state.get("default_answer", "")),
            "default_confidence": _clamp(float(state.get("default_confidence", 0.50))),
        }

    def _calibration_state_to_dict(self, state: CalibrationHeadState) -> Dict[str, Any]:
        return {
            "bias": float(state.bias),
            "weight_confidence": float(state.weight_confidence),
            "weight_entropy": float(state.weight_entropy),
            "losses": [float(v) for v in state.losses],
        }

    def _maybe_hf_integration(
        self,
        *,
        cfg: TrainerConfig,
        run_dir: Path,
        rows: Sequence[Dict[str, Any]],
        hf_components: Mapping[str, Any] | None,
    ) -> Dict[str, Any]:
        """Optional HuggingFace integration path; remains additive and offline-safe."""
        if not bool(cfg.use_hf_trainer):
            return {"enabled": False, "status": "disabled"}

        if Trainer is None or TrainingArguments is None or torch is None:
            return {
                "enabled": True,
                "status": "missing_dependency",
                "detail": "transformers/torch unavailable",
            }

        model = None
        tokenizer = None
        if isinstance(hf_components, Mapping):
            model = hf_components.get("model")
            tokenizer = hf_components.get("tokenizer")

        if model is None or tokenizer is None:
            return {
                "enabled": True,
                "status": "missing_components",
                "detail": "pass hf_components={model, tokenizer} to activate",
            }

        try:
            dataset = _HFTrainingDataset(rows=rows, tokenizer=tokenizer)
            hf_dir = run_dir / "hf_trainer"
            hf_dir.mkdir(parents=True, exist_ok=True)

            args = TrainingArguments(
                output_dir=str(hf_dir),
                remove_unused_columns=False,
                per_device_train_batch_size=max(1, int(cfg.batch_size)),
                gradient_accumulation_steps=max(1, int(cfg.gradient_accumulation_steps)),
                num_train_epochs=1,
                max_steps=max(1, int(cfg.hf_max_steps or 1)),
                fp16=bool(
                    cfg.mixed_precision
                    and hasattr(torch, "cuda")
                    and bool(torch.cuda.is_available())
                ),
                report_to=[],
                disable_tqdm=True,
                logging_steps=1,
            )

            trainer = Trainer(
                model=model,
                args=args,
                train_dataset=dataset,
            )
            if int(cfg.hf_max_steps) > 0:
                trainer.train()

            return {
                "enabled": True,
                "status": "initialized",
                "rows": len(dataset),
                "trained_steps": int(cfg.hf_max_steps),
            }
        except Exception as exc:
            if cfg.strict:
                raise
            return {
                "enabled": True,
                "status": "failed",
                "error": str(exc),
            }

    def _maybe_lora_integration(
        self,
        *,
        cfg: TrainerConfig,
        run_dir: Path,
        hf_components: Mapping[str, Any] | None,
    ) -> Dict[str, Any]:
        """Optional LoRA/PEFT adapter setup path (additive, offline-only)."""
        adapter_dir = run_dir / "lora_adapter"
        adapter_dir.mkdir(parents=True, exist_ok=True)
        adapter_payload = self.build_lora_adapter_config(cfg)
        (adapter_dir / "adapter_config.generated.json").write_text(
            json.dumps(adapter_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        if not bool(cfg.enable_lora):
            return {"enabled": False, "status": "disabled", "adapter_dir": str(adapter_dir)}

        if LoraConfig is None or get_peft_model is None:
            return {
                "enabled": True,
                "status": "missing_dependency",
                "detail": "peft unavailable",
                "adapter_dir": str(adapter_dir),
                "adapter_config": adapter_payload,
            }

        model = hf_components.get("model") if isinstance(hf_components, Mapping) else None
        if model is None:
            return {
                "enabled": True,
                "status": "missing_components",
                "detail": "hf_components.model required for LoRA path",
                "adapter_dir": str(adapter_dir),
                "adapter_config": adapter_payload,
            }

        try:
            if bool(cfg.freeze_base_model):
                for _, param in model.named_parameters():
                    param.requires_grad = False

            if bool(cfg.gradient_checkpointing) and hasattr(model, "gradient_checkpointing_enable"):
                model.gradient_checkpointing_enable()

            lora_cfg = LoraConfig(
                r=int(adapter_payload["r"]),
                lora_alpha=int(adapter_payload["lora_alpha"]),
                lora_dropout=float(adapter_payload["lora_dropout"]),
                target_modules=list(adapter_payload["target_modules"]),
                bias=str(adapter_payload["bias"]),
                task_type=TaskType.CAUSAL_LM if TaskType is not None else "CAUSAL_LM",
            )
            peft_model = get_peft_model(model, lora_cfg)
            adapter_state = {
                "adapter_name": str(cfg.adapter_name),
                "trainable_params": int(
                    sum(
                        int(param.numel())
                        for _, param in peft_model.named_parameters()
                        if bool(getattr(param, "requires_grad", False))
                    )
                ),
                "adapter_only_checkpoint": bool(cfg.adapter_only_checkpoint),
                "merge_adapter_for_export": bool(cfg.merge_adapter_for_export),
            }
            (adapter_dir / "adapter_state.json").write_text(json.dumps(adapter_state, indent=2, sort_keys=True), encoding="utf-8")

            merged = False
            if bool(cfg.merge_adapter_for_export) and hasattr(peft_model, "merge_and_unload"):
                peft_model.merge_and_unload()
                merged = True

            return {
                "enabled": True,
                "status": "initialized",
                "adapter_dir": str(adapter_dir),
                "adapter_config": adapter_payload,
                "merged": bool(merged),
            }
        except Exception as exc:
            if cfg.strict:
                raise
            return {
                "enabled": True,
                "status": "failed",
                "adapter_dir": str(adapter_dir),
                "adapter_config": adapter_payload,
                "error": str(exc),
            }

    @contextmanager
    def _api_free_guard(self):
        """Block outbound network attempts during offline training."""
        original_create_connection = socket.create_connection
        original_socket_connect = socket.socket.connect
        patched_httpx = []
        provider_patch_state = None

        def _blocked(*args, **kwargs):  # noqa: ANN001, ANN002
            raise RuntimeError("API_FREE_ENFORCEMENT: outbound network call blocked during offline training")

        async def _blocked_async(*args, **kwargs):  # noqa: ANN001, ANN002
            raise RuntimeError("API_FREE_ENFORCEMENT: provider.generate blocked during offline training")

        socket.create_connection = _blocked  # type: ignore[assignment]
        socket.socket.connect = _blocked  # type: ignore[assignment]

        try:
            try:
                import httpx  # pragma: no cover - optional dependency

                patched_httpx.append((httpx.Client, httpx.Client.request))
                patched_httpx.append((httpx.AsyncClient, httpx.AsyncClient.request))
                httpx.Client.request = _blocked  # type: ignore[assignment]
                httpx.AsyncClient.request = _blocked  # type: ignore[assignment]
            except Exception:
                pass

            try:
                from core.lalacore_x.providers import ProviderFabric  # pragma: no cover - runtime isolation

                provider_patch_state = (
                    ProviderFabric.generate,
                    ProviderFabric.generate_many,
                )
                ProviderFabric.generate = _blocked_async  # type: ignore[assignment]
                ProviderFabric.generate_many = _blocked_async  # type: ignore[assignment]
            except Exception:
                provider_patch_state = None
            yield
        finally:
            socket.create_connection = original_create_connection  # type: ignore[assignment]
            socket.socket.connect = original_socket_connect  # type: ignore[assignment]
            for cls, method in patched_httpx:
                cls.request = method  # type: ignore[assignment]
            if provider_patch_state is not None:
                try:
                    from core.lalacore_x.providers import ProviderFabric  # pragma: no cover - runtime isolation

                    ProviderFabric.generate = provider_patch_state[0]  # type: ignore[assignment]
                    ProviderFabric.generate_many = provider_patch_state[1]  # type: ignore[assignment]
                except Exception:
                    pass


class _HFTrainingDataset:  # pragma: no cover - optional path
    """Small adapter dataset for HuggingFace Trainer experiments."""

    def __init__(self, *, rows: Sequence[Dict[str, Any]], tokenizer: Any) -> None:
        self.rows = list(rows)
        self.tokenizer = tokenizer

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, idx: int) -> Dict[str, Any]:
        row = self.rows[idx]
        prompt = f"Question: {row.get('question', '')}\nAnswer: {row.get('final_answer', '')}"
        encoded = self.tokenizer(
            prompt,
            truncation=True,
            max_length=384,
            padding="max_length",
            return_tensors="pt",
        )
        label = 1 if bool(row.get("verified", False)) else 0
        output = {k: v.squeeze(0) for k, v in encoded.items()}
        output["labels"] = torch.tensor(label, dtype=torch.long)
        return output
