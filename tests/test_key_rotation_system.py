import time
import unittest

import httpx

from core.key_manager import KeyManager
from core.lalacore_x.provider_circuit import ProviderCircuitBreaker
from core.lalacore_x.providers import ProviderFabric


class KeyRotationSystemTests(unittest.TestCase):
    def test_key_rotation_prefers_non_cooled_keys(self):
        km = KeyManager()
        keys = ["k1", "k2", "k3"]
        km.register_provider_keys("openrouter", keys)

        first = km.get_key("openrouter")
        km.report_failure(first, error_type="rate_limit")
        second = km.get_key("openrouter")
        self.assertNotEqual(first, second)

        # Exclusion should force a different key when available.
        third = km.get_key("openrouter", exclude_keys=[first, second])
        self.assertIn(third, keys)
        self.assertNotEqual(second, third)

    def test_all_cooling_down_selects_earliest_recovery(self):
        km = KeyManager()
        km.register_provider_keys("gemini", ["a", "b"])

        now = time.time()
        km.stats["a"]["cooldown_until"] = now + 120.0
        km.stats["b"]["cooldown_until"] = now + 10.0

        selected = km.get_key("gemini")
        self.assertEqual(selected, "b")

    def test_auth_cooldown_longer_than_rate_limit(self):
        km = KeyManager()
        km.register_provider_keys("hf", ["x", "y"])
        km.report_failure("x", error_type="auth")
        km.report_failure("y", error_type="rate_limit")
        now = time.time()
        auth_remaining = km.stats["x"]["cooldown_until"] - now
        rate_remaining = km.stats["y"]["cooldown_until"] - now
        self.assertGreater(auth_remaining, rate_remaining)

    def test_provider_failure_reason_mapping(self):
        fabric = ProviderFabric()

        req = httpx.Request("POST", "https://example.com")
        exc_401 = httpx.HTTPStatusError("401", request=req, response=httpx.Response(401, request=req))
        exc_429 = httpx.HTTPStatusError("429", request=req, response=httpx.Response(429, request=req))
        exc_422 = httpx.HTTPStatusError("422", request=req, response=httpx.Response(422, request=req))
        exc_504 = httpx.HTTPStatusError("504", request=req, response=httpx.Response(504, request=req))

        self.assertEqual(fabric._failure_reason(exc_401), "auth")
        self.assertEqual(fabric._failure_reason(exc_429), "rate_limit")
        self.assertEqual(fabric._failure_reason(exc_422), "schema_validation")
        self.assertEqual(fabric._failure_reason(exc_504), "timeout")
        self.assertEqual(fabric._failure_reason(RuntimeError("[Errno 8] nodename nor servname provided, or not known")), "network")

    def test_retry_policy_marks_common_failures_retryable(self):
        fabric = ProviderFabric()
        for reason in ("auth", "rate_limit", "timeout", "network", "invalid_response", "schema_validation"):
            self.assertTrue(fabric._should_retry_with_next_key(reason, attempt_idx=0, max_attempts=3))
        self.assertFalse(fabric._should_retry_with_next_key("auth", attempt_idx=2, max_attempts=3))

    def test_circuit_accepts_network_reason(self):
        breaker = ProviderCircuitBreaker(path="data/metrics/provider_circuit_test.json")
        incident = breaker.record_failure("openrouter", "network")
        self.assertEqual(incident["reason"], "network")


if __name__ == "__main__":
    unittest.main()

