import asyncio
import time
import unittest
from unittest.mock import patch

import httpx

from core.lalacore_x.providers import ProviderFabric
from core.lalacore_x.schemas import ProviderAnswer
from core.lalacore_x.schemas import ProblemProfile


class _NetworkFailingAsyncClient:
    def __init__(self, timeout=None):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, headers=None, json=None):
        request = httpx.Request("POST", url, headers=headers)
        raise httpx.ConnectError("[Errno 8] nodename nor servname provided, or not known", request=request)


class _AuthResponseAsyncClient:
    def __init__(self, timeout=None):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, headers=None, json=None):
        request = httpx.Request("POST", url, headers=headers)
        return httpx.Response(401, request=request)


class ProviderRuntimeRotationTests(unittest.TestCase):
    def _profile(self) -> ProblemProfile:
        return ProblemProfile(
            subject="math",
            difficulty="medium",
            numeric=True,
            multi_concept=False,
            trap_probability=0.0,
        )

    def test_openrouter_retries_all_keys_on_network_failure(self):
        fabric = ProviderFabric()
        fabric.max_provider_key_attempts = 3
        fabric.key_retry_backoff_s = 0.0
        fabric.key_manager.register_provider_keys("openrouter", ["k1", "k2", "k3"])
        fabric.circuit.state = {"providers": {}}

        with patch("core.lalacore_x.providers.httpx.AsyncClient", _NetworkFailingAsyncClient):
            answer = asyncio.run(fabric.generate("openrouter", "What is 2+2?", self._profile(), []))

        self.assertEqual((answer.raw or {}).get("failure_reason"), "network")
        self.assertEqual((answer.raw or {}).get("attempt_count"), 3)
        self.assertEqual(len((answer.raw or {}).get("attempted_keys", [])), 3)

    def test_openrouter_classifies_401_as_auth_and_rotates(self):
        fabric = ProviderFabric()
        fabric.max_provider_key_attempts = 3
        fabric.key_retry_backoff_s = 0.0
        fabric.key_manager.register_provider_keys("openrouter", ["k1", "k2", "k3"])
        fabric.circuit.state = {"providers": {}}

        with patch("core.lalacore_x.providers.httpx.AsyncClient", _AuthResponseAsyncClient):
            answer = asyncio.run(fabric.generate("openrouter", "What is 2+2?", self._profile(), []))

        self.assertEqual((answer.raw or {}).get("failure_reason"), "auth")
        self.assertEqual((answer.raw or {}).get("attempt_count"), 3)
        self.assertEqual(len((answer.raw or {}).get("attempted_keys", [])), 3)

    def test_generate_does_not_increment_failures_when_blocked_by_circuit(self):
        fabric = ProviderFabric()
        now = time.time()
        fabric.circuit.state = {
            "providers": {
                "openrouter": {
                    "state": "open",
                    "open_until": now + 30.0,
                    "half_open_success": 0,
                    "probe_counter": 0,
                    "requests": 10,
                    "success": 4,
                    "failures": 6,
                    "consecutive_failures": 6,
                    "timeout": 1,
                    "network": 1,
                    "invalid_response": 2,
                    "empty_response": 0,
                    "schema_validation": 1,
                    "auth": 1,
                    "rate_limit": 0,
                    "generic": 0,
                }
            }
        }
        before = dict(fabric.circuit.state["providers"]["openrouter"])

        answer = asyncio.run(
            fabric.generate("openrouter", "What is 2+2?", self._profile(), [])
        )

        after = dict(fabric.circuit.state["providers"]["openrouter"])
        self.assertIn("provider_temporarily_disabled_by_circuit", answer.reasoning)
        self.assertEqual(int(after.get("failures", 0)), int(before.get("failures", 0)))
        self.assertEqual(
            int(after.get("consecutive_failures", 0)),
            int(before.get("consecutive_failures", 0)),
        )

    def test_open_provider_probe_recovery_executes_request(self):
        fabric = ProviderFabric()
        fabric.circuit.half_open_successes = 1
        fabric.circuit.open_probe_every_requests = 5
        now = time.time()
        fabric.circuit.state = {
            "providers": {
                "openrouter": {
                    "state": "open",
                    "open_until": now + 30.0,
                    "half_open_success": 0,
                    "probe_counter": 0,
                    "open_probe_counter": 4,
                    "requests": 10,
                    "success": 4,
                    "failures": 6,
                    "consecutive_failures": 6,
                    "timeout": 2,
                    "network": 1,
                    "invalid_response": 2,
                    "empty_response": 0,
                    "schema_validation": 0,
                    "auth": 1,
                    "rate_limit": 0,
                    "generic": 0,
                }
            }
        }

        async def _fake_openrouter(_question, _profile, _retrieved):
            return ProviderAnswer(
                provider="openrouter",
                reasoning="Recovered provider probe answer.",
                final_answer="42",
                confidence=0.8,
                raw={},
            )

        fabric._run_openrouter = _fake_openrouter  # type: ignore[method-assign]
        available = fabric.available_providers()
        self.assertIn("openrouter", available)
        answer = asyncio.run(
            fabric.generate("openrouter", "What is 6*7?", self._profile(), [])
        )
        self.assertEqual(answer.provider, "openrouter")
        self.assertEqual(answer.final_answer, "42")
        state = fabric.circuit.state["providers"]["openrouter"]["state"]
        self.assertEqual(state, "closed")

    def test_non_infra_validation_failure_does_not_open_circuit(self):
        fabric = ProviderFabric()
        fabric.circuit.failure_threshold = 1
        fabric.circuit.failure_window_s = 10.0
        fabric.circuit.cooldown_s = 30.0
        fabric.circuit.state = {"providers": {}}

        async def _invalid_shape(_question, _profile, _retrieved):
            return ProviderAnswer(
                provider="openrouter",
                reasoning="No final answer emitted.",
                final_answer="",
                confidence=0.4,
                raw={},
            )

        fabric._run_openrouter = _invalid_shape  # type: ignore[method-assign]
        answer = asyncio.run(
            fabric.generate("openrouter", "Solve quickly", self._profile(), [])
        )
        self.assertEqual(answer.provider, "openrouter")
        row = fabric.circuit.state.get("providers", {}).get("openrouter", {})
        self.assertEqual(row.get("state"), "closed")

    def test_startup_warmup_primes_open_provider_for_probe(self):
        fabric = ProviderFabric()
        fabric.warmup_on_start = True
        fabric._warmup_done = False
        fabric.key_manager.register_provider_keys("openrouter", ["k1"])
        now = time.time()
        fabric.circuit.state = {
            "providers": {
                "openrouter": {
                    "state": "open",
                    "open_until": now + 60.0,
                    "half_open_success": 0,
                    "probe_counter": 0,
                    "open_probe_counter": 0,
                    "requests": 1,
                    "success": 0,
                    "failures": 1,
                    "consecutive_failures": 1,
                    "consecutive_infra_failures": 1,
                    "recent_infra_failures": [now],
                    "health_score": 0.8,
                    "timeout": 1,
                    "network": 0,
                    "invalid_response": 0,
                    "empty_response": 0,
                    "schema_validation": 0,
                    "auth": 0,
                    "rate_limit": 0,
                    "unresolved_answer": 0,
                    "too_short": 0,
                    "generic": 0,
                }
            }
        }

        asyncio.run(fabric.ensure_startup_warmup())
        row = fabric.circuit.state["providers"]["openrouter"]
        self.assertGreaterEqual(
            int(row.get("open_probe_counter", 0)),
            max(0, fabric.circuit.open_probe_every_requests - 1),
        )


if __name__ == "__main__":
    unittest.main()
