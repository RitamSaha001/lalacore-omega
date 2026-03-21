from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.automation.feeder_engine import FeederEngine
from core.automation.orchestrator import AutomationOrchestrator
from core.automation.state_manager import AutomationStateManager
from core.lalacore_x.mini_evolution import MiniEvolutionEngine
from core.lalacore_x.token_budget import TokenBudgetGuardian


class AutomationTests(unittest.TestCase):
    def test_feeder_enqueue_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = AutomationStateManager(path=str(root / "LC9_AUTOMATION_STATE.json"))
            mini = MiniEvolutionEngine(
                state_path=str(root / "mini_state.json"),
                disagreement_path=str(root / "mini_disagreements.jsonl"),
                replay_queue_path=str(root / "mini_queue.jsonl"),
            )
            token_guard = TokenBudgetGuardian(path=str(root / "token_budget.json"))

            feeder = FeederEngine(
                queue_path=str(root / "LC9_FEEDER_QUEUE.jsonl"),
                training_cases_path=str(root / "LC9_FEEDER_CASES.jsonl"),
                replay_cases_path=str(root / "feeder_cases.jsonl"),
                state_manager=state,
                mini_evolution=mini,
                token_guardian=token_guard,
            )

            first = feeder.enqueue_question(
                question="What is 6*7?",
                subject="math",
                difficulty="easy",
                concept_cluster=["arithmetic"],
            )
            second = feeder.enqueue_question(
                question="What is 6*7?",
                subject="math",
                difficulty="easy",
                concept_cluster=["arithmetic"],
            )

            self.assertTrue(first["added"])
            self.assertFalse(first["duplicate"])
            self.assertFalse(second["added"])
            self.assertTrue(second["duplicate"])

            status = feeder.status(limit=5)
            self.assertEqual(status["total"], 1)
            self.assertEqual(status["counts"].get("Pending"), 1)

    def test_state_manager_recovers_stale_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = str(Path(tmp) / "LC9_AUTOMATION_STATE.json")
            state = AutomationStateManager(path=state_path)
            state.start_job("weekly_automation", run_id="r1", trigger="manual", resume=False)

            # Force stale timestamp.
            state.state["jobs"]["weekly_automation"]["last_start_ts"] = (
                datetime.now(timezone.utc) - timedelta(hours=12)
            ).isoformat()
            state.state["jobs"]["weekly_automation"]["status"] = "running"
            state.state["jobs"]["weekly_automation"]["completed_stages"] = ["feeder_refresh"]
            state.path.write_text(json.dumps(state.state), encoding="utf-8")
            state = AutomationStateManager(path=state_path)

            changed = state.recover_stale_job("weekly_automation", stale_after_minutes=60)
            self.assertTrue(changed)
            row = state.get_job("weekly_automation")
            self.assertEqual(row.get("status"), "failed")
            self.assertIn("recovered_stale", str(row.get("last_error", "")))

    def test_orchestrator_run_if_due_skips_when_not_due(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = AutomationStateManager(path=str(Path(tmp) / "LC9_AUTOMATION_STATE.json"))
            state.start_job("weekly_automation", run_id="r2", trigger="manual", resume=False)
            state.mark_job_complete("weekly_automation", duration_s=0.1)

            orchestrator = AutomationOrchestrator(state_manager=state)
            out = asyncio.run(orchestrator.run_if_due(min_interval_days=7))
            self.assertTrue(out.get("ok"))
            self.assertTrue(out.get("skipped"))
            self.assertEqual(out.get("reason"), "not_due")


if __name__ == "__main__":
    unittest.main()
