import json
import tempfile
import unittest
from pathlib import Path

from core.monitoring.google_sheets_monitor import (
    DEFAULT_STATE,
    GoogleSheetsSyncService,
    MonitoringAggregator,
    SHEET_DASHBOARD,
    SHEET_PROVIDER_HEALTH,
    SHEET_QUEUE_MINUTE,
    SHEET_QUESTION_STATUS,
    SHEET_QUESTION_STATUS_CURRENT,
    SHEET_RUNTIME,
    SHEET_SOLVER_EVENTS,
    SHEET_TOKEN_USAGE,
    SHEET_WEEKLY,
)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")


class _FakeSheetClient:
    def __init__(self):
        self.replaced = {}
        self.appended = {}

    def replace_rows(self, *, title, headers, rows):
        self.replaced[title] = {"headers": list(headers), "rows": [list(r) for r in rows]}
        return len(rows)

    def append_rows(self, *, title, headers, rows):
        slot = self.appended.setdefault(title, {"headers": list(headers), "rows": []})
        slot["rows"].extend([list(r) for r in rows])
        return len(rows)


class GoogleSheetsMonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)

        self.queue_path = root / "queue.jsonl"
        self.debug_path = root / "debug.jsonl"
        self.runtime_path = root / "runtime.jsonl"
        self.provider_stats_path = root / "provider_stats.json"
        self.provider_circuit_path = root / "provider_circuit.json"
        self.token_budget_path = root / "token_budget.json"
        self.state_path = root / "state.json"
        self.sync_log_path = root / "sync.jsonl"

        _write_jsonl(
            self.queue_path,
            [
                {
                    "id": 1,
                    "item_hash": "a",
                    "question": "Q1",
                    "status": "Pending",
                    "attempts": 0,
                    "max_attempts": 3,
                    "subject": "math",
                    "difficulty": "easy",
                    "source_tag": "manual",
                    "created_ts": "2026-02-26T09:00:00+00:00",
                    "updated_ts": "2026-02-26T09:00:00+00:00",
                },
                {
                    "id": 2,
                    "item_hash": "b",
                    "question": "Q2",
                    "status": "Completed",
                    "attempts": 1,
                    "max_attempts": 3,
                    "subject": "math",
                    "difficulty": "medium",
                    "source_tag": "manual",
                    "created_ts": "2026-02-26T09:02:00+00:00",
                    "updated_ts": "2026-02-26T09:10:00+00:00",
                    "processed_ts": "2026-02-26T09:10:00+00:00",
                    "result_summary": {
                        "verified": False,
                        "risk": 1.0,
                        "entropy": 0.3,
                        "disagreement": 0.1,
                        "winner_provider": "mini",
                        "final_answer": "0",
                    },
                },
                {
                    "id": 3,
                    "item_hash": "c",
                    "question": "Q3",
                    "status": "Failed",
                    "attempts": 2,
                    "max_attempts": 3,
                    "subject": "physics",
                    "difficulty": "hard",
                    "source_tag": "raw_auto",
                    "created_ts": "2026-02-26T09:03:00+00:00",
                    "updated_ts": "2026-02-26T09:16:00+00:00",
                    "last_error": "quality_gate:plausibility_failed",
                },
            ],
        )
        _write_jsonl(
            self.debug_path,
            [
                {
                    "ts": "2026-02-26T09:11:00+00:00",
                    "event_type": "final_status",
                    "winner_provider": "mini",
                    "final_status": "Failed",
                    "escalate": True,
                    "plausible": False,
                    "plausibility_score": 0.35,
                    "entropy": 0.6,
                },
                {
                    "ts": "2026-02-26T09:19:00+00:00",
                    "event_type": "provider_output",
                    "provider": "openrouter",
                    "question": "Q2",
                    "raw_output": "Reasoning ... Final Answer: 9",
                    "extracted_answer": "9",
                    "tokens_used": 220,
                },
                {
                    "ts": "2026-02-26T09:20:00+00:00",
                    "event_type": "final_status",
                    "winner_provider": "openrouter",
                    "final_status": "Completed",
                    "escalate": False,
                    "plausible": True,
                    "plausibility_score": 0.91,
                    "entropy": 0.1,
                },
            ],
        )
        _write_jsonl(
            self.runtime_path,
            [
                {
                    "ts": "2026-02-26T09:30:00+00:00",
                    "event_type": "runtime_exception",
                    "exception_type": "RuntimeError",
                    "module": "providers",
                    "function": "generate",
                    "active_providers": ["openrouter"],
                    "token_usage": {"total_tokens": 320},
                    "extra": {"provider": "openrouter", "incident": {"reason": "invalid_response"}},
                }
            ],
        )
        _write_json(
            self.provider_stats_path,
            {
                "providers": {
                    "mini": {
                        "ema_reliability": 0.2,
                        "calibration_error": 0.3,
                        "brier_score": 0.25,
                        "token_stats": {"avg_tokens_ema": 180, "total_tokens": 5000, "gain_per_1k_tokens_ema": 0.4},
                        "total": 30,
                        "verified_pass": 8,
                    },
                    "openrouter": {
                        "ema_reliability": 0.62,
                        "calibration_error": 0.11,
                        "brier_score": 0.12,
                        "token_stats": {"avg_tokens_ema": 260, "total_tokens": 800, "gain_per_1k_tokens_ema": 1.2},
                        "total": 5,
                        "verified_pass": 4,
                    },
                }
            },
        )
        _write_json(
            self.provider_circuit_path,
            {
                "providers": {
                    "mini": {"state": "closed", "requests": 35, "success": 25, "failures": 10, "consecutive_failures": 0},
                    "openrouter": {
                        "state": "open",
                        "open_until": 4102444800.0,
                        "requests": 8,
                        "success": 4,
                        "failures": 4,
                        "consecutive_failures": 2,
                        "invalid_response": 2,
                    },
                }
            },
        )
        _write_json(
            self.token_budget_path,
            {
                "weekly": {
                    "2026-W09": {
                        "total_tokens": 6200.0,
                        "sessions": 31,
                        "providers": {
                            "mini": {"sessions": 28, "total_tokens": 5500.0, "avg_tokens": 196.4},
                            "openrouter": {"sessions": 3, "total_tokens": 700.0, "avg_tokens": 233.3},
                        },
                    }
                }
            },
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _build_aggregator(self) -> MonitoringAggregator:
        return MonitoringAggregator(
            queue_path=str(self.queue_path),
            debug_path=str(self.debug_path),
            runtime_path=str(self.runtime_path),
            provider_stats_path=str(self.provider_stats_path),
            provider_circuit_path=str(self.provider_circuit_path),
            token_budget_path=str(self.token_budget_path),
            debug_event_limit=100,
            runtime_event_limit=100,
            question_row_limit=100,
            weekly_days=7,
            weekly_limit_tokens=10000,
        )

    def test_collect_generates_expected_metrics(self):
        agg = self._build_aggregator()
        out = agg.collect(state=dict(DEFAULT_STATE), snapshot_ts="2026-02-26T10:00:00+00:00")

        self.assertEqual(out["queue_snapshot"]["total"], 3)
        self.assertEqual(out["queue_snapshot"]["pending"], 1)
        self.assertEqual(out["queue_snapshot"]["completed"], 1)
        self.assertEqual(out["queue_snapshot"]["failed"], 1)
        self.assertEqual(len(out["question_rows"]), 3)
        self.assertEqual(len(out["solver_event_rows"]), 3)
        self.assertEqual(len(out["runtime_rows"]), 1)
        self.assertGreaterEqual(out["weekly_stats"]["completed_pct"], 0.49)
        self.assertGreaterEqual(out["weekly_stats"]["failed_pct"], 0.49)

    def test_collect_respects_incremental_timestamps(self):
        agg = self._build_aggregator()
        state = {
            "last_sync_ts": "2026-02-26T09:21:00+00:00",
            "last_queue_ts": "2026-02-26T09:12:00+00:00",
            "last_debug_ts": "2026-02-26T09:15:00+00:00",
            "last_runtime_ts": "2026-02-26T09:31:00+00:00",
        }
        out = agg.collect(state=state, snapshot_ts="2026-02-26T10:00:00+00:00")

        self.assertEqual(len(out["question_rows"]), 1)
        self.assertEqual(out["question_rows"][0][1], 3)
        self.assertEqual(len(out["solver_event_rows"]), 2)
        self.assertEqual(len(out["runtime_rows"]), 0)

    def test_sync_service_writes_tabs_and_state(self):
        agg = self._build_aggregator()
        fake_sheet = _FakeSheetClient()
        svc = GoogleSheetsSyncService(
            sheet_client=fake_sheet,
            aggregator=agg,
            state_path=str(self.state_path),
            sync_log_path=str(self.sync_log_path),
        )
        out = svc.sync_once(snapshot_ts="2026-02-26T10:00:00+00:00")

        self.assertIn(SHEET_DASHBOARD, fake_sheet.replaced)
        self.assertIn(SHEET_QUESTION_STATUS_CURRENT, fake_sheet.replaced)
        self.assertIn(SHEET_QUEUE_MINUTE, fake_sheet.appended)
        self.assertIn(SHEET_QUESTION_STATUS, fake_sheet.appended)
        self.assertIn(SHEET_PROVIDER_HEALTH, fake_sheet.appended)
        self.assertIn(SHEET_TOKEN_USAGE, fake_sheet.appended)
        self.assertIn(SHEET_SOLVER_EVENTS, fake_sheet.appended)
        self.assertIn(SHEET_RUNTIME, fake_sheet.appended)
        self.assertIn(SHEET_WEEKLY, fake_sheet.appended)
        self.assertTrue(self.state_path.exists())
        self.assertTrue(self.sync_log_path.exists())
        self.assertEqual(out["snapshot_ts"], "2026-02-26T10:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
