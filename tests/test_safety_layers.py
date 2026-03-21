import tempfile
import time
import unittest
from pathlib import Path

from core.lalacore_x.deterministic_guard import DeterministicDominanceGuard
from core.lalacore_x.provider_circuit import ProviderCircuitBreaker
from core.lalacore_x.statistical_sanity import StatisticalSanityValidator
from core.lalacore_x.token_budget import TokenBudgetGuardian
from core.safe_math import safe_exp, safe_log, safe_sigmoid, safe_softmax, stable_logsumexp


class SafetyLayerTests(unittest.TestCase):
    def test_safe_math_no_overflow(self):
        self.assertGreaterEqual(safe_exp(1e9), 0.0)
        self.assertLessEqual(safe_sigmoid(1e9), 1.0)
        self.assertGreaterEqual(safe_sigmoid(-1e9), 0.0)
        self.assertLess(safe_log(0.0), 0.0)
        probs = safe_softmax([10000.0, 10001.0, 10002.0])
        self.assertAlmostEqual(sum(probs), 1.0, places=6)
        self.assertTrue(stable_logsumexp([1.0, 2.0, 3.0]) > 0.0)

    def test_provider_circuit_opens_and_recovers(self):
        with tempfile.TemporaryDirectory() as tmp:
            breaker = ProviderCircuitBreaker(path=str(Path(tmp) / "circuit.json"), failure_threshold=2, cooldown_s=0.05, half_open_successes=1)
            self.assertTrue(breaker.can_request("groq"))
            breaker.record_failure("groq", "timeout")
            breaker.record_failure("groq", "timeout")
            self.assertFalse(breaker.can_request("groq"))
            time.sleep(0.12)
            self.assertTrue(breaker.can_request("groq"))
            breaker.record_success("groq")
            self.assertTrue(breaker.can_request("groq"))

    def test_provider_circuit_non_consuming_check_does_not_burn_half_open_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            breaker = ProviderCircuitBreaker(
                path=str(Path(tmp) / "circuit.json"),
                failure_threshold=1,
                cooldown_s=0.05,
                half_open_successes=2,
            )
            breaker.record_failure("openrouter", "timeout")
            time.sleep(0.12)

            # Availability checks should not consume half-open probe budget.
            self.assertTrue(breaker.can_request("openrouter", consume_probe=False))
            self.assertTrue(breaker.can_request("openrouter", consume_probe=False))

            # First real probe should still pass.
            self.assertTrue(breaker.can_request("openrouter"))
            breaker.record_success("openrouter")

            # Recovery probes continue while half-open.
            self.assertTrue(breaker.can_request("openrouter"))
            breaker.record_success("openrouter")
            self.assertTrue(breaker.can_request("openrouter", consume_probe=False))

    def test_provider_circuit_does_not_extend_open_window_while_already_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            breaker = ProviderCircuitBreaker(
                path=str(Path(tmp) / "circuit.json"),
                failure_threshold=1,
                cooldown_s=0.5,
                half_open_successes=1,
            )
            first = breaker.record_failure("gemini", "timeout")
            first_until = float(first.get("open_until", 0.0))
            self.assertGreater(first_until, 0.0)

            # Repeated failures while still open must not push open_until forward.
            time.sleep(0.02)
            second = breaker.record_failure("gemini", "timeout")
            second_until = float(second.get("open_until", 0.0))
            self.assertLessEqual(abs(second_until - first_until), 0.03)

    def test_provider_circuit_periodic_force_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            breaker = ProviderCircuitBreaker(
                path=str(Path(tmp) / "circuit.json"),
                failure_threshold=1,
                cooldown_s=10.0,
                half_open_successes=1,
                open_probe_every_requests=3,
            )
            breaker.record_failure("openrouter", "timeout")
            self.assertFalse(breaker.should_force_probe("openrouter"))
            self.assertFalse(breaker.should_force_probe("openrouter"))
            self.assertTrue(breaker.should_force_probe("openrouter"))
            self.assertTrue(breaker.can_request("openrouter"))
            breaker.record_success("openrouter")
            self.assertTrue(breaker.can_request("openrouter", consume_probe=False))

    def test_provider_circuit_non_infra_failures_do_not_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            breaker = ProviderCircuitBreaker(
                path=str(Path(tmp) / "circuit.json"),
                failure_threshold=2,
                failure_window_s=10.0,
                cooldown_s=1.0,
                half_open_successes=1,
            )
            breaker.record_failure("gemini", "empty_response")
            breaker.record_failure("gemini", "schema_validation")
            row = breaker.summary().get("gemini", {})
            self.assertEqual(row.get("state"), "closed")
            self.assertTrue(breaker.can_request("gemini"))

    def test_provider_circuit_failure_window_requires_clustered_infra_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            breaker = ProviderCircuitBreaker(
                path=str(Path(tmp) / "circuit.json"),
                failure_threshold=2,
                failure_window_s=0.05,
                cooldown_s=0.5,
                half_open_successes=1,
            )
            breaker.record_failure("groq", "timeout")
            time.sleep(0.12)
            breaker.record_failure("groq", "timeout")
            row = breaker.summary().get("groq", {})
            self.assertEqual(row.get("state"), "closed")
            self.assertTrue(breaker.can_request("groq"))

    def test_provider_circuit_health_score_recovers_after_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            breaker = ProviderCircuitBreaker(
                path=str(Path(tmp) / "circuit.json"),
                failure_threshold=5,
                cooldown_s=0.2,
                half_open_successes=1,
            )
            breaker.record_failure("openrouter", "network")
            low = float(breaker.summary()["openrouter"]["health_score"])
            breaker.record_success("openrouter")
            high = float(breaker.summary()["openrouter"]["health_score"])
            self.assertLess(low, 1.0)
            self.assertGreater(high, low)

    def test_statistical_sanity_autocorrect(self):
        validator = StatisticalSanityValidator()
        bad = {
            "entropy": float("nan"),
            "thetas": {"a": 1.0, "b": float("inf")},
            "posteriors": {"a": 0.9, "b": 0.9},
            "pairwise": {"uncertainties": {"a": 0.2, "b": 0.3}},
        }
        fixed = validator.auto_correct(
            bad,
            recompute_fn=lambda: {
                "entropy": 0.1,
                "thetas": {"a": 1.0, "b": 0.2},
                "posteriors": {"a": 0.7, "b": 0.3},
                "pairwise": {"uncertainties": {"a": 0.2, "b": 0.3}},
            },
        )
        self.assertTrue(fixed.get("auto_corrected"))
        self.assertAlmostEqual(sum(fixed["posteriors"].values()), 1.0, places=6)

    def test_deterministic_dominance_guard(self):
        guard = DeterministicDominanceGuard()
        winner, event = guard.enforce(
            winner="b",
            posteriors={"a": 0.45, "b": 0.55},
            verification_by_provider={
                "a": {"verified": True},
                "b": {"verified": False},
            },
            structure_by_provider={"a": {"circular_reasoning": 0.0, "missing_inference_rate": 0.1}},
        )
        self.assertEqual(winner, "a")
        self.assertTrue(event["enforced"])

    def test_token_budget_guardian_pressure(self):
        with tempfile.TemporaryDirectory() as tmp:
            guard = TokenBudgetGuardian(path=str(Path(tmp) / "budget.json"), weekly_limit=1000)
            guard.record_session({"a": {"total_tokens": 800}}, debate_triggered=True)
            self.assertTrue(guard.allow_debate())
            guard.record_session({"a": {"total_tokens": 400}}, debate_triggered=False)
            self.assertFalse(guard.allow_debate())
            self.assertLess(guard.arena_iteration_scale(), 1.0)


if __name__ == "__main__":
    unittest.main()
