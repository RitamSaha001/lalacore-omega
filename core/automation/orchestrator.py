from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List

from core.automation.dataset_distiller import AutomationDatasetDistiller
from core.automation.feeder_engine import FeederEngine
from core.automation.logging import AutomationLogger
from core.automation.replay_engine import AutomatedReplayEngine
from core.automation.state_manager import AutomationStateManager
from core.lalacore_x.routing import ProviderStatsMemory
from core.lalacore_x.weekly import WeeklyEvolutionJob


class AutomationOrchestrator:
    """
    Central automation scheduler/orchestrator.

    Responsibilities:
    - weekly evolution orchestration
    - replay refresh
    - dataset distillation export
    - provider ranking refresh
    - safe restart + crash-resume via stage checkpoints
    """

    JOB_NAME = "weekly_automation"

    def __init__(
        self,
        *,
        logger: AutomationLogger | None = None,
        state_manager: AutomationStateManager | None = None,
        feeder: FeederEngine | None = None,
        replay_engine: AutomatedReplayEngine | None = None,
        distiller: AutomationDatasetDistiller | None = None,
        weekly_job: WeeklyEvolutionJob | None = None,
        provider_stats: ProviderStatsMemory | None = None,
    ):
        self.logger = logger or AutomationLogger()
        self.state = state_manager or AutomationStateManager()
        self.feeder = feeder or FeederEngine(logger=self.logger, state_manager=self.state)
        self.replay_engine = replay_engine or AutomatedReplayEngine(logger=self.logger, state_manager=self.state)
        self.distiller = distiller or AutomationDatasetDistiller(
            logger=self.logger,
            state_manager=self.state,
            replay_engine=self.replay_engine,
        )
        self.weekly_job = weekly_job or WeeklyEvolutionJob()
        self.provider_stats = provider_stats or ProviderStatsMemory()

    async def run_weekly(
        self,
        *,
        trigger: str = "manual",
        resume: bool = True,
        feeder_batch: int = 12,
        replay_batch: int | None = None,
        execute_replay_pipeline: bool = True,
    ) -> Dict[str, Any]:
        self.state.recover_stale_job(self.JOB_NAME, stale_after_minutes=360)

        previous = self.state.get_job(self.JOB_NAME)
        prev_status = str(previous.get("status"))
        prev_completed = previous.get("completed_stages", [])
        resumable = bool(
            resume
            and previous.get("last_run_id")
            and isinstance(prev_completed, list)
            and len(prev_completed) > 0
            and prev_status in {"running", "failed"}
        )
        run_id = str(previous.get("last_run_id")) if resumable else str(uuid.uuid4())

        self.state.start_job(self.JOB_NAME, run_id=run_id, trigger=trigger, resume=resumable)
        self.logger.job_start(job=self.JOB_NAME, trigger=trigger, run_id=run_id)

        stage_outputs: Dict[str, Any] = {}
        started = time.monotonic()

        stages: List[tuple[str, Callable[[], Awaitable[Dict[str, Any]]]]] = [
            ("feeder_refresh", lambda: self._stage_feeder(feeder_batch=feeder_batch)),
            (
                "weekly_evolution",
                self._stage_weekly_evolution,
            ),
            (
                "replay_refresh",
                lambda: self._stage_replay_refresh(
                    replay_batch=replay_batch,
                    execute_pipeline=execute_replay_pipeline,
                    trigger=trigger,
                ),
            ),
            ("dataset_export", lambda: self._stage_dataset_export(trigger=trigger)),
            ("provider_ranking", self._stage_provider_ranking),
        ]

        completed = set(self.state.completed_stages(self.JOB_NAME))

        try:
            for stage_name, stage_fn in stages:
                if stage_name in completed:
                    stage_outputs[stage_name] = {"resumed": True, "skipped": True}
                    continue

                out = await self._run_with_retries(stage_name=stage_name, fn=stage_fn, run_id=run_id)
                stage_outputs[stage_name] = out
                self.state.mark_stage_complete(self.JOB_NAME, stage_name)

            duration_s = max(0.0, time.monotonic() - started)
            self.state.mark_job_complete(self.JOB_NAME, duration_s=duration_s)
            self.logger.job_complete(
                job=self.JOB_NAME,
                run_id=run_id,
                duration_s=duration_s,
                items_processed=self._items_processed_hint(stage_outputs),
                extra={"stages": list(stage_outputs.keys())},
            )
            return {
                "ok": True,
                "run_id": run_id,
                "trigger": trigger,
                "duration_s": round(duration_s, 6),
                "stages": stage_outputs,
                "resumed": resumable,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as exc:
            duration_s = max(0.0, time.monotonic() - started)
            self.state.mark_job_failure(self.JOB_NAME, error=str(exc), duration_s=duration_s)
            self.logger.job_failure(
                job=self.JOB_NAME,
                run_id=run_id,
                duration_s=duration_s,
                error_type=type(exc).__name__,
                message=str(exc),
                extra={"partial_stages": list(stage_outputs.keys())},
            )
            return {
                "ok": False,
                "run_id": run_id,
                "trigger": trigger,
                "duration_s": round(duration_s, 6),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "stages": stage_outputs,
                "resumed": resumable,
                "ts": datetime.now(timezone.utc).isoformat(),
            }

    async def run_if_due(
        self,
        *,
        min_interval_days: int = 7,
        feeder_batch: int = 12,
        replay_batch: int | None = None,
        execute_replay_pipeline: bool = True,
    ) -> Dict[str, Any]:
        due = self.state.should_run_weekly(self.JOB_NAME, min_interval_days=min_interval_days)
        self.state.checkpoint("scheduler", last_tick_ts=datetime.now(timezone.utc).isoformat(), due=bool(due))
        if not due:
            return {
                "ok": True,
                "skipped": True,
                "reason": "not_due",
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        return await self.run_weekly(
            trigger="scheduled",
            resume=True,
            feeder_batch=feeder_batch,
            replay_batch=replay_batch,
            execute_replay_pipeline=execute_replay_pipeline,
        )

    # -----------------------------
    # Stages
    # -----------------------------

    async def _stage_feeder(self, *, feeder_batch: int) -> Dict[str, Any]:
        return await self.feeder.process_pending(max_items=max(1, int(feeder_batch)), trigger="automation")

    async def _stage_weekly_evolution(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self.weekly_job.run)

    async def _stage_replay_refresh(
        self,
        *,
        replay_batch: int | None,
        execute_pipeline: bool,
        trigger: str,
    ) -> Dict[str, Any]:
        return await self.replay_engine.run_weekly_replay(
            max_items=replay_batch,
            execute_pipeline=bool(execute_pipeline),
            trigger=trigger,
        )

    async def _stage_dataset_export(self, *, trigger: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self.distiller.run_weekly, trigger=trigger)

    async def _stage_provider_ranking(self) -> Dict[str, Any]:
        def _run() -> Dict[str, Any]:
            thresholds = self.provider_stats.auto_tune_thresholds()
            rankings = self.provider_stats.weekly_recompute_rankings()
            return {
                "rows": len(rankings),
                "routing_thresholds": thresholds,
            }

        return await asyncio.to_thread(_run)

    # -----------------------------
    # Helpers
    # -----------------------------

    async def _run_with_retries(
        self,
        *,
        stage_name: str,
        fn: Callable[[], Awaitable[Dict[str, Any]]],
        run_id: str,
        max_attempts: int = 3,
        base_delay_s: float = 0.35,
    ) -> Dict[str, Any]:
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            started = time.monotonic()
            try:
                out = await fn()
                duration_s = max(0.0, time.monotonic() - started)
                self.logger.event(
                    "automation_stage_complete",
                    {
                        "job": self.JOB_NAME,
                        "run_id": run_id,
                        "stage": stage_name,
                        "attempt": attempt,
                        "duration_s": round(duration_s, 6),
                    },
                )
                return out
            except Exception as exc:
                last_exc = exc
                duration_s = max(0.0, time.monotonic() - started)
                will_retry = attempt < max_attempts
                delay = float(base_delay_s) * (2 ** (attempt - 1))
                self.logger.event(
                    "automation_stage_failure",
                    {
                        "job": self.JOB_NAME,
                        "run_id": run_id,
                        "stage": stage_name,
                        "attempt": attempt,
                        "duration_s": round(duration_s, 6),
                        "will_retry": will_retry,
                        "delay_s": round(delay, 6),
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:400],
                    },
                )
                if will_retry:
                    await asyncio.sleep(delay)
        raise last_exc if last_exc is not None else RuntimeError("automation_stage_failed")

    def _items_processed_hint(self, stage_outputs: Dict[str, Any]) -> int:
        count = 0
        feeder = stage_outputs.get("feeder_refresh") or {}
        replay = stage_outputs.get("replay_refresh") or {}
        if isinstance(feeder, dict):
            count += int(feeder.get("processed", 0) or 0)
        if isinstance(replay, dict):
            count += int(replay.get("selected", 0) or 0)
        return count
