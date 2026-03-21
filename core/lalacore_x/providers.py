from __future__ import annotations

import asyncio
import hashlib
import os
import re
import time
from collections import OrderedDict
from typing import Any, Dict, List, Sequence

import httpx

from core.bootstrap import get_key_manager, initialize_keys, provider_registry_snapshot
from core.lalacore_x.answer_extractor import extract_answer
from core.lalacore_x.logging_debug import SolverDebugLogger
from core.lalacore_x.plausibility_checker import check_answer_plausibility
from core.lalacore_x.provider_circuit import ProviderCircuitBreaker
from core.lalacore_x.runtime_telemetry import RuntimeTelemetry
from core.lalacore_x.schemas import ProblemProfile, ProviderAnswer, RetrievedBlock
from core.math.contextual_math_solver import solve_contextual_math_question
from models.mini_loader import run_mini


class ProviderFabric:
    """
    Unified provider surface for free-tier external models + local mini model.
    """

    def __init__(self):
        initialize_keys(silent=True)
        self.key_manager = get_key_manager()
        self.prompt_template_version = "lc9_prompt_v1"
        self.circuit = ProviderCircuitBreaker(
            failure_threshold=max(2, int(os.getenv("LC9_PROVIDER_FAILURE_THRESHOLD", "5") or 5)),
            failure_window_s=max(5.0, float(os.getenv("LC9_PROVIDER_FAILURE_WINDOW_S", "30.0") or 30.0)),
            cooldown_s=max(5.0, float(os.getenv("LC9_PROVIDER_OPEN_TIMEOUT_S", "60.0") or 60.0)),
            cooldown_jitter_s=max(0.0, float(os.getenv("LC9_PROVIDER_OPEN_JITTER_S", "10.0") or 10.0)),
            half_open_successes=max(1, int(os.getenv("LC9_PROVIDER_HALF_OPEN_SUCCESSES", "2") or 2)),
            open_probe_every_requests=max(1, int(os.getenv("LC9_PROVIDER_OPEN_PROBE_EVERY", "5") or 5)),
        )
        self.runtime_telemetry = RuntimeTelemetry()
        self.external_providers = ("openrouter", "groq", "gemini", "hf")
        self.provider_timeouts_s = {
            "mini": 25.0,
            "symbolic_guard": 6.0,
            "openrouter": 45.0,
            "groq": 45.0,
            "gemini": 55.0,
            "hf": 65.0,
            "huggingface": 65.0,
        }
        self._response_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self._response_cache_limit = 256
        self.debug_logger = SolverDebugLogger()
        self.max_provider_key_attempts = max(1, int(os.getenv("LC9_PROVIDER_MAX_KEY_ATTEMPTS", "3") or 3))
        self.key_retry_backoff_s = max(0.0, float(os.getenv("LC9_PROVIDER_KEY_RETRY_BACKOFF_S", "0.20") or 0.20))
        self.warmup_on_start = str(os.getenv("LC9_PROVIDER_WARMUP_ON_START", "1")).strip().lower() in {"1", "true", "yes", "on"}
        self.warmup_timeout_s = max(0.5, float(os.getenv("LC9_PROVIDER_WARMUP_TIMEOUT_S", "4.0") or 4.0))
        self._warmup_done = False

    def available_providers(self) -> List[str]:
        report = self.availability_report()
        weighted = []
        for provider in self.external_providers:
            row = report.get(provider, {})
            if row.get("eligible"):
                weighted.append((provider, float(row.get("routing_weight", 0.1))))
        weighted.sort(key=lambda item: item[1], reverse=True)
        available = ["mini"] + [provider for provider, _ in weighted]
        # Probe recovery: periodically reopen one open provider for a half-open test request.
        for provider in self.external_providers:
            if provider in available:
                continue
            if self.circuit.should_force_probe(provider):
                available.append(provider)
        return available

    def availability_report(self) -> Dict[str, Dict[str, Any]]:
        registry = provider_registry_snapshot().get("providers", {})
        circuit_summary = self.circuit.summary()
        report: Dict[str, Dict[str, Any]] = {
            "mini": {
                "registered": True,
                "key_count": 0,
                "circuit_state": "local",
                "health_score": 1.0,
                "routing_weight": 1.0,
                "eligible": True,
                "reasons": [],
            }
        }

        for provider in self.external_providers:
            reasons: List[str] = []
            key_row = registry.get(provider, {})
            registered = bool(key_row.get("registered", False))
            key_count = int(key_row.get("key_count", 0) or 0)
            if not registered:
                reasons.append("missing_key")

            can_request = False
            if registered:
                try:
                    # Availability checks must not consume half-open probes.
                    can_request = bool(
                        self.circuit.can_request(provider, consume_probe=False)
                    )
                except Exception:
                    can_request = False
                    reasons.append("availability_check_error")

            circuit_row = circuit_summary.get(provider, {})
            circuit_state = str(circuit_row.get("state", "closed"))
            open_for_s = float(circuit_row.get("open_for_s", 0.0) or 0.0)
            health_score = max(0.0, min(1.0, float(circuit_row.get("health_score", 1.0))))
            if circuit_state == "open" and open_for_s > 0.0:
                reasons.append("circuit_open")
            elif circuit_state == "half_open":
                reasons.append("circuit_half_open")
            if health_score < 0.25:
                reasons.append("low_health")

            eligible = bool(registered and can_request)
            if not eligible and registered and "circuit_open" not in reasons and "circuit_half_open" not in reasons:
                reasons.append("runtime_unavailable")

            report[provider] = {
                "registered": registered,
                "key_count": key_count,
                "circuit_state": circuit_state,
                "open_for_s": open_for_s,
                "health_score": health_score,
                "routing_weight": max(0.1, health_score),
                "eligible": eligible,
                "reasons": reasons,
            }

        return report

    async def generate_many(
        self,
        providers: Sequence[str],
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> List[ProviderAnswer]:
        serial_mode = str(os.getenv("LC9_PROVIDER_SERIAL", "0")).strip().lower() in {"1", "true", "yes", "on"}
        inter_provider_gap_s = max(0.0, float(os.getenv("LC9_PROVIDER_MIN_GAP_S", "0.0") or 0.0))

        if serial_mode:
            out: List[ProviderAnswer] = []
            for idx, provider in enumerate(providers):
                try:
                    result = await self.generate(provider, question, profile, retrieved)
                except Exception as exc:
                    self._record_failure(provider, "invalid_response")
                    self.runtime_telemetry.log_exception(
                        exception_type=type(exc).__name__,
                        module="providers",
                        function="generate_many_serial",
                        input_size=len(question or ""),
                        entropy=None,
                        active_providers=list(providers),
                        mini_eligible=None,
                        token_usage=None,
                        extra={"provider": provider},
                    )
                    result = self._error_answer(provider, RuntimeError(f"provider_generate_failed:{provider}"))
                if isinstance(result, ProviderAnswer):
                    out.append(result)
                if inter_provider_gap_s > 0.0 and idx < len(providers) - 1:
                    await asyncio.sleep(inter_provider_gap_s)
            return [result for result in out if result is not None]

        tasks = [asyncio.create_task(self.generate(provider, question, profile, retrieved)) for provider in providers]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
        out: List[ProviderAnswer] = []
        for provider, result in zip(providers, results):
            if isinstance(result, ProviderAnswer):
                out.append(result)
                continue
            self._record_failure(provider, "invalid_response")
            self.runtime_telemetry.log_exception(
                exception_type=type(result).__name__ if isinstance(result, Exception) else "UnknownError",
                module="providers",
                function="generate_many",
                input_size=len(question or ""),
                entropy=None,
                active_providers=list(providers),
                mini_eligible=None,
                token_usage=None,
                extra={"provider": provider},
            )
            out.append(self._error_answer(provider, RuntimeError(f"provider_generate_failed:{provider}")))
        return [result for result in out if result is not None]

    async def ensure_startup_warmup(self) -> None:
        if self._warmup_done or not self.warmup_on_start:
            return
        self._warmup_done = True
        report = self.availability_report()
        probe_profile = ProblemProfile(
            subject="math",
            difficulty="easy",
            numeric=True,
            multi_concept=False,
            trap_probability=0.0,
        )
        for provider in self.external_providers:
            row = report.get(provider, {})
            if not bool(row.get("registered", False)):
                continue
            if not self.circuit.can_request(provider, consume_probe=False):
                # Prepare fast half-open probe next time traffic arrives.
                row_state = self.circuit.state.setdefault("providers", {}).setdefault(provider, {})
                row_state["open_probe_counter"] = max(
                    int(row_state.get("open_probe_counter", 0)),
                    max(0, self.circuit.open_probe_every_requests - 1),
                )
                continue
            try:
                await asyncio.wait_for(
                    self.generate(provider, "What is 2+2?", probe_profile, []),
                    timeout=min(self.warmup_timeout_s, float(self.provider_timeouts_s.get(provider, 45.0))),
                )
            except Exception:
                # Warm-up is best-effort and should never block solve flow.
                continue

    async def generate(
        self,
        provider: str,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> ProviderAnswer:
        runner = self._runner(provider)
        request_key = self._request_hash(provider, question, profile)

        if provider != "mini":
            # Keep probing open circuits periodically so providers can recover
            # without waiting for long cooldown-only windows.
            self.circuit.should_force_probe(provider)
        if provider != "mini" and not self.circuit.can_request(provider):
            self.runtime_telemetry.log_incident(
                "provider_circuit_open",
                {"provider": provider, "state": "open_or_half_open_blocked"},
            )
            cached = self._cache_get(request_key)
            if cached is not None:
                return cached
            return self._error_answer(provider, RuntimeError("provider_temporarily_disabled_by_circuit"))

        timeout_s = float(self.provider_timeouts_s.get(provider, 45.0))
        try:
            answer = await asyncio.wait_for(runner(question, profile, retrieved), timeout=timeout_s)
        except asyncio.TimeoutError as exc:
            incident = self._record_failure(provider, "timeout")
            self.runtime_telemetry.log_incident("provider_timeout", {"provider": provider, "timeout_s": timeout_s, "incident": incident})
            cached = self._cache_get(request_key)
            if cached is not None:
                return cached
            return self._error_answer(provider, exc)
        except asyncio.CancelledError:
            self._record_failure(provider, "timeout")
            return self._error_answer(provider, RuntimeError("provider_call_cancelled"))
        except Exception as exc:
            incident = self._record_failure(provider, self._failure_reason(exc))
            self.runtime_telemetry.log_exception(
                exception_type=type(exc).__name__,
                module="providers",
                function="generate",
                input_size=len(question or ""),
                entropy=None,
                active_providers=[provider],
                mini_eligible=None,
                token_usage=None,
                extra={"provider": provider, "incident": incident},
            )
            cached = self._cache_get(request_key)
            if cached is not None:
                return cached
            return self._error_answer(provider, exc)

        ok, reason = self._validate_answer(answer)
        if not ok:
            repaired = await self._attempt_contract_repair(
                provider=provider,
                question=question,
                profile=profile,
                retrieved=retrieved,
                original_answer=answer,
                validation_reason=reason,
            )
            if repaired is not None:
                answer = repaired
                ok, reason = self._validate_answer(answer)
        if not ok:
            incident = self._record_failure(provider, reason)
            self.runtime_telemetry.log_incident(
                "provider_invalid_answer",
                {"provider": provider, "reason": reason, "incident": incident},
            )
            cached = self._cache_get(request_key)
            if cached is not None:
                return cached
            return answer

        self._record_success(provider)
        self._cache_put(request_key, answer)
        return answer

    def _runner(self, provider: str):
        if provider == "mini":
            return self._run_mini
        if provider == "symbolic_guard":
            return self._run_symbolic_guard
        if provider == "openrouter":
            return self._run_openrouter
        if provider == "groq":
            return self._run_groq
        if provider == "gemini":
            return self._run_gemini
        if provider in {"hf", "huggingface"}:
            return self._run_hf
        raise RuntimeError(f"Unknown provider {provider}")

    async def _run_mini(
        self,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> ProviderAnswer:
        context = "\n".join(f"- {b.title}: {b.text}" for b in retrieved[:5])

        start = time.time()
        out = run_mini(question, context)
        latency = time.time() - start

        mode = str(out.get("mode", "")).strip().lower()
        reasoning = str(out.get("reasoning", ""))
        final_answer = str(out.get("final_answer", "")).strip()
        if not final_answer:
            if self._is_simple_arithmetic_prompt(question):
                final_answer = self._safe_math_guess(question)
            else:
                final_answer = ""

        critique = "Used retrieval context and internal solver fallback."
        conf = float(out.get("confidence", 0.55 + (0.15 if profile.numeric else 0.0)))
        if self._looks_degenerate_mini_output(
            question=question,
            reasoning=reasoning,
            final_answer=final_answer,
            mode=mode,
        ):
            reasoning = (
                f"{reasoning}\nMini output flagged as unreliable and suppressed."
            ).strip()
            final_answer = ""
            conf = min(conf, 0.05)
            critique = "Mini response suppressed due to degenerate replay pattern."
        elif final_answer:
            mini_plausibility = check_answer_plausibility(
                question_text=question,
                final_answer=final_answer,
                metadata={
                    "numeric_expected": bool(profile.numeric),
                    "observed_type": "numeric" if re.search(r"\d", str(final_answer or "")) else "text",
                },
            )
            severe_issues = {"echo_fragment", "formatting_only", "empty_semantics"}
            if severe_issues.intersection(set(mini_plausibility.get("issues", []))):
                reasoning = (
                    f"{reasoning}\nMini output suppressed by plausibility guard "
                    f"({','.join(mini_plausibility.get('issues', []))})."
                ).strip()
                final_answer = ""
                conf = min(conf, 0.05)
                critique = "Mini response suppressed due to implausible echoed output."

        prompt_meta = {
            "provider": "mini",
            "model_name": "mini-shadow",
            "template_version": self.prompt_template_version,
            "system_instructions": "internal-mini-shadow-inference",
            "prompt": question,
        }
        token_usage = {
            "prompt_tokens": self._estimate_tokens(question + "\n" + context),
            "completion_tokens": self._estimate_tokens(reasoning + "\n" + final_answer),
        }
        token_usage["total_tokens"] = token_usage["prompt_tokens"] + token_usage["completion_tokens"]

        self.debug_logger.log_provider_output(
            provider="mini",
            question=question,
            raw_output=(reasoning + "\nFinal Answer: " + final_answer),
            extracted_answer=final_answer,
            tokens_used=int(token_usage["total_tokens"]),
            extraction_matched=True,
            extraction_pattern="mini_structured",
        )
        answer_contract = self._answer_contract(
            final_answer=final_answer,
            reasoning=reasoning or "Mini fallback reasoning generated.",
            confidence=max(0.0, min(1.0, conf)),
        )

        return ProviderAnswer(
            provider="mini",
            reasoning=reasoning or "Mini fallback reasoning generated.",
            final_answer=final_answer,
            confidence=max(0.0, min(1.0, conf)),
            self_critique=critique,
            latency_s=latency,
            answer_contract=answer_contract,
            raw={**out, "prompt_meta": prompt_meta, "token_usage": token_usage, "answer_contract": answer_contract},
        )

    async def _run_symbolic_guard(
        self,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> ProviderAnswer:
        start = time.time()
        deterministic = solve_contextual_math_question(question)
        latency = time.time() - start

        if deterministic and bool(deterministic.get("handled")):
            answer = str(deterministic.get("answer", "")).strip()
            reasoning = str(deterministic.get("reasoning", "Deterministic symbolic guard solve.")).strip()
            token_usage = {
                "prompt_tokens": float(self._estimate_tokens(question)),
                "completion_tokens": float(self._estimate_tokens(reasoning + "\n" + answer)),
            }
            token_usage["total_tokens"] = token_usage["prompt_tokens"] + token_usage["completion_tokens"]
            answer_contract = self._answer_contract(final_answer=answer, reasoning=reasoning, confidence=0.97)
            return ProviderAnswer(
                provider="symbolic_guard",
                reasoning=reasoning,
                final_answer=answer,
                confidence=0.97,
                self_critique="Deterministic symbolic guard.",
                latency_s=latency,
                answer_contract=answer_contract,
                raw={
                    "mode": "deterministic_contextual",
                    "expected_expr": deterministic.get("expected_expr"),
                    "prompt_meta": self._prompt_meta(
                        provider="symbolic_guard",
                        model_name="sympy-contextual-guard",
                        system_instructions="deterministic symbolic solver",
                        prompt=question,
                    ),
                    "token_usage": token_usage,
                    "answer_contract": answer_contract,
                },
            )

        unsupported_contract = self._answer_contract(
            final_answer="",
            reasoning="Symbolic guard could not parse task.",
            confidence=0.08,
        )
        return ProviderAnswer(
            provider="symbolic_guard",
            reasoning="Symbolic guard could not parse task.",
            final_answer="",
            confidence=0.08,
            self_critique="Task unsupported by deterministic symbolic guard.",
            latency_s=latency,
            answer_contract=unsupported_contract,
            raw={"mode": "unsupported", "answer_contract": unsupported_contract},
        )

    async def _run_openrouter(
        self,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> ProviderAnswer:
        # Default to a currently serverless-accessible model. Legacy defaults may 400 on decommission.
        model = os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.1-8b-instruct")

        prompt = self._build_prompt(question, profile, retrieved)
        system_instructions = "Return concise reasoning then final answer."
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_instructions},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
        }

        prompt_meta = self._prompt_meta(
            provider="openrouter",
            model_name=model,
            system_instructions=system_instructions,
            prompt=prompt,
        )
        key_count = max(1, len(self.key_manager.keys.get("openrouter", [])))
        max_attempts = max(1, min(self.max_provider_key_attempts, key_count))
        attempted: List[str] = []
        attempted_hashes: List[str] = []
        last_exc: Exception | None = None
        last_reason = "invalid_response"
        start = time.time()

        for attempt_idx in range(max_attempts):
            key = self.key_manager.get_key("openrouter", exclude_keys=attempted)
            attempted.append(key)
            attempted_hashes.append(self._mask_key(key))
            headers = {
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "http://localhost",
                "X-Title": "LalaCore-X",
            }
            try:
                async with httpx.AsyncClient(timeout=40) as client:
                    response = await client.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers=headers,
                        json=payload,
                    )
                response.raise_for_status()
                data = response.json()
                content = self._extract_chat_content(data)
                if not content:
                    raise RuntimeError("openrouter_empty_content")
                self.key_manager.report_success(key)
                token_usage = self._usage_from_payload(data, prompt=prompt, completion_text=content)
                return self._pack_text_answer(
                    "openrouter",
                    content,
                    time.time() - start,
                    data,
                    question_text=question,
                    profile=profile,
                    prompt_meta=prompt_meta,
                    token_usage=token_usage,
                )
            except Exception as exc:
                last_exc = exc
                last_reason = self._failure_reason(exc)
                self.key_manager.report_failure(key, error_type=last_reason)
                if self._should_retry_with_next_key(last_reason, attempt_idx=attempt_idx, max_attempts=max_attempts):
                    await asyncio.sleep(self.key_retry_backoff_s * float(attempt_idx + 1))
                    continue
                break

        error = last_exc or RuntimeError("openrouter_request_failed")
        return self._error_answer(
            "openrouter",
            error,
            prompt_meta=prompt_meta,
            extra_raw={
                "failure_reason": last_reason,
                "attempted_keys": attempted_hashes,
                "attempt_count": len(attempted_hashes),
            },
        )

    async def _run_groq(
        self,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> ProviderAnswer:
        # Legacy `llama3-8b-8192` is decommissioned.
        model = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
        prompt = self._build_prompt(question, profile, retrieved)
        system_instructions = "Explain briefly and give exact final answer."

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_instructions},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
        }

        prompt_meta = self._prompt_meta(
            provider="groq",
            model_name=model,
            system_instructions=system_instructions,
            prompt=prompt,
        )
        key_count = max(1, len(self.key_manager.keys.get("groq", [])))
        max_attempts = max(1, min(self.max_provider_key_attempts, key_count))
        attempted: List[str] = []
        attempted_hashes: List[str] = []
        last_exc: Exception | None = None
        last_reason = "invalid_response"
        start = time.time()

        for attempt_idx in range(max_attempts):
            key = self.key_manager.get_key("groq", exclude_keys=attempted)
            attempted.append(key)
            attempted_hashes.append(self._mask_key(key))
            headers = {
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            }
            try:
                async with httpx.AsyncClient(timeout=40) as client:
                    response = await client.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                content = self._extract_chat_content(data)
                if not content:
                    raise RuntimeError("groq_empty_content")
                self.key_manager.report_success(key)
                token_usage = self._usage_from_payload(data, prompt=prompt, completion_text=content)
                return self._pack_text_answer(
                    "groq",
                    content,
                    time.time() - start,
                    data,
                    question_text=question,
                    profile=profile,
                    prompt_meta=prompt_meta,
                    token_usage=token_usage,
                )
            except Exception as exc:
                last_exc = exc
                last_reason = self._failure_reason(exc)
                self.key_manager.report_failure(key, error_type=last_reason)
                if self._should_retry_with_next_key(last_reason, attempt_idx=attempt_idx, max_attempts=max_attempts):
                    await asyncio.sleep(self.key_retry_backoff_s * float(attempt_idx + 1))
                    continue
                break

        error = last_exc or RuntimeError("groq_request_failed")
        return self._error_answer(
            "groq",
            error,
            prompt_meta=prompt_meta,
            extra_raw={
                "failure_reason": last_reason,
                "attempted_keys": attempted_hashes,
                "attempt_count": len(attempted_hashes),
            },
        )

    async def _run_gemini(
        self,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> ProviderAnswer:
        # Prefer a free-tier-friendly default; callers can override via GEMINI_MODEL.
        model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")
        prompt = self._build_prompt(question, profile, retrieved)
        system_instructions = "Return concise reasoning then final answer."

        payload = {
            "contents": [
                {
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": {"temperature": 0.1, "topP": 0.95},
        }

        prompt_meta = self._prompt_meta(
            provider="gemini",
            model_name=model,
            system_instructions=system_instructions,
            prompt=prompt,
        )
        key_count = max(1, len(self.key_manager.keys.get("gemini", [])))
        max_attempts = max(1, min(self.max_provider_key_attempts, key_count))
        attempted: List[str] = []
        attempted_hashes: List[str] = []
        last_exc: Exception | None = None
        last_reason = "invalid_response"
        start = time.time()

        for attempt_idx in range(max_attempts):
            key = self.key_manager.get_key("gemini", exclude_keys=attempted)
            attempted.append(key)
            attempted_hashes.append(self._mask_key(key))
            url = f"https://generativelanguage.googleapis.com/v1/models/{model}:generateContent?key={key}"
            try:
                async with httpx.AsyncClient(timeout=50) as client:
                    response = await client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
                content = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                self.key_manager.report_success(key)
                token_usage = self._usage_from_payload(data, prompt=prompt, completion_text=content)
                return self._pack_text_answer(
                    "gemini",
                    content,
                    time.time() - start,
                    data,
                    question_text=question,
                    profile=profile,
                    prompt_meta=prompt_meta,
                    token_usage=token_usage,
                )
            except Exception as exc:
                last_exc = exc
                last_reason = self._failure_reason(exc)
                self.key_manager.report_failure(key, error_type=last_reason)
                if self._should_retry_with_next_key(last_reason, attempt_idx=attempt_idx, max_attempts=max_attempts):
                    await asyncio.sleep(self.key_retry_backoff_s * float(attempt_idx + 1))
                    continue
                break

        error = last_exc or RuntimeError("gemini_request_failed")
        return self._error_answer(
            "gemini",
            error,
            prompt_meta=prompt_meta,
            extra_raw={
                "failure_reason": last_reason,
                "attempted_keys": attempted_hashes,
                "attempt_count": len(attempted_hashes),
            },
        )

    async def _run_hf(
        self,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> ProviderAnswer:
        # `api-inference.huggingface.co` is sunset; use HF Router OpenAI-compatible endpoint.
        model = os.getenv("HF_MODEL", "meta-llama/Llama-3.1-8B-Instruct")
        url = os.getenv("HF_CHAT_COMPLETIONS_URL", "https://router.huggingface.co/v1/chat/completions")
        prompt = self._build_prompt(question, profile, retrieved)
        system_instructions = "Return concise reasoning then exact final answer."
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_instructions},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 300,
        }

        prompt_meta = self._prompt_meta(
            provider="hf",
            model_name=model,
            system_instructions=system_instructions,
            prompt=prompt,
        )
        key_count = max(1, len(self.key_manager.keys.get("hf", [])))
        max_attempts = max(1, min(self.max_provider_key_attempts, key_count))
        attempted: List[str] = []
        attempted_hashes: List[str] = []
        last_exc: Exception | None = None
        last_reason = "invalid_response"
        start = time.time()

        for attempt_idx in range(max_attempts):
            key = self.key_manager.get_key("hf", exclude_keys=attempted)
            attempted.append(key)
            attempted_hashes.append(self._mask_key(key))
            headers = {"Authorization": f"Bearer {key}"}
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict):
                    content = self._extract_chat_content(data)
                elif isinstance(data, list) and data:
                    # Backward-compatible parsing for legacy inference payloads.
                    content = str(data[0].get("generated_text", "")).strip()
                else:
                    content = str(data)
                if not content:
                    raise RuntimeError("hf_empty_content")
                self.key_manager.report_success(key)
                token_usage = self._usage_from_payload(data if isinstance(data, dict) else {"response": data}, prompt=prompt, completion_text=content)
                return self._pack_text_answer(
                    "hf",
                    content,
                    time.time() - start,
                    data if isinstance(data, dict) else {"response": data},
                    question_text=question,
                    profile=profile,
                    prompt_meta=prompt_meta,
                    token_usage=token_usage,
                )
            except Exception as exc:
                last_exc = exc
                last_reason = self._failure_reason(exc)
                self.key_manager.report_failure(key, error_type=last_reason)
                if self._should_retry_with_next_key(last_reason, attempt_idx=attempt_idx, max_attempts=max_attempts):
                    await asyncio.sleep(self.key_retry_backoff_s * float(attempt_idx + 1))
                    continue
                break

        error = last_exc or RuntimeError("hf_request_failed")
        return self._error_answer(
            "hf",
            error,
            prompt_meta=prompt_meta,
            extra_raw={
                "failure_reason": last_reason,
                "attempted_keys": attempted_hashes,
                "attempt_count": len(attempted_hashes),
            },
        )

    def _build_prompt(
        self,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
    ) -> str:
        retrieved_text = "\n".join(f"[{b.title}] {b.text}" for b in retrieved[:5])

        return (
            "Solve the following JEE-style question with concise reasoning.\n"
            f"Subject: {profile.subject}; Difficulty: {profile.difficulty}; Numeric: {profile.numeric}.\n"
            "Use retrieved hints when relevant and avoid hallucinations.\n\n"
            f"Retrieved context:\n{retrieved_text}\n\n"
            f"Question:\n{question}\n\n"
            "Output Contract (strict):\n"
            "- Include line `Final Answer:` exactly once.\n"
            "- Never leave Final Answer blank.\n"
            "- If unresolved, write `Final Answer: [UNRESOLVED]`.\n\n"
            "Return format:\n"
            "Reasoning: <short reasoning>\n"
            "Final Answer: <exact answer>"
        )

    def _pack_text_answer(
        self,
        provider: str,
        text: str,
        latency_s: float,
        raw: Dict[str, Any],
        question_text: str,
        profile: ProblemProfile,
        prompt_meta: Dict[str, Any] | None = None,
        token_usage: Dict[str, float] | None = None,
    ) -> ProviderAnswer:
        extraction = extract_answer(
            question_text=question_text,
            raw_output=text,
            metadata={"numeric_expected": bool(getattr(profile, "numeric", False))},
        )
        reasoning = str(extraction.get("reasoning", ""))
        final_answer = str(extraction.get("final_answer", ""))
        fallback_used = False
        if not final_answer.strip() and str(text or "").strip():
            # Salvage responses that do not follow strict tags to avoid false
            # empty-response circuit failures.
            fallback_reasoning, fallback_answer = self._split_reasoning_answer(text)
            fallback_answer = str(fallback_answer or "").strip()
            if fallback_answer:
                final_answer = fallback_answer[:320].strip()
                fallback_used = True
                if not reasoning.strip():
                    reasoning = str(fallback_reasoning or "").strip()[:1200]
        raw_payload = dict(raw)
        raw_payload["raw_output_text"] = text
        if prompt_meta is not None:
            raw_payload["prompt_meta"] = prompt_meta
        if token_usage is not None:
            raw_payload["token_usage"] = token_usage
        raw_payload["extraction"] = {
            "matched": bool(extraction.get("matched", False)),
            "pattern": str(extraction.get("pattern", "")),
            "expected_type": str(extraction.get("expected_type", "")),
            "candidates": list(extraction.get("candidates", []))[:6],
            "fallback_used": bool(fallback_used),
        }

        used_tokens = int((token_usage or {}).get("total_tokens", self._estimate_tokens(text)))
        self.debug_logger.log_provider_output(
            provider=provider,
            question=question_text,
            raw_output=text,
            extracted_answer=final_answer,
            tokens_used=used_tokens,
            extraction_matched=bool(extraction.get("matched", False)),
            extraction_pattern=str(extraction.get("pattern", "")),
        )
        if not str(final_answer).strip():
            self.debug_logger.log_extraction_failure(
                provider=provider,
                question=question_text,
                raw_output=text,
                reason="empty_extracted_answer",
            )
        answer_contract = self._answer_contract(final_answer=final_answer, reasoning=reasoning, confidence=0.62)
        raw_payload["answer_contract"] = answer_contract
        return ProviderAnswer(
            provider=provider,
            reasoning=reasoning,
            final_answer=final_answer,
            confidence=0.62,
            self_critique="External free-tier provider; requires deterministic verification.",
            latency_s=latency_s,
            answer_contract=answer_contract,
            raw=raw_payload,
        )

    def _error_answer(
        self,
        provider: str,
        exc: Exception,
        prompt_meta: Dict[str, Any] | None = None,
        extra_raw: Dict[str, Any] | None = None,
    ) -> ProviderAnswer:
        raw_payload = {"error": str(exc)}
        if prompt_meta is not None:
            raw_payload["prompt_meta"] = prompt_meta
        if isinstance(extra_raw, dict) and extra_raw:
            raw_payload.update(extra_raw)
        answer_contract = self._answer_contract(
            final_answer="",
            reasoning=f"Provider error: {exc}",
            confidence=0.05,
        )
        raw_payload["answer_contract"] = answer_contract
        return ProviderAnswer(
            provider=provider,
            reasoning=f"Provider error: {exc}",
            final_answer="",
            confidence=0.05,
            self_critique="Provider failed; confidence dropped.",
            latency_s=0.0,
            answer_contract=answer_contract,
            raw=raw_payload,
        )

    def _split_reasoning_answer(self, text: str) -> tuple[str, str]:
        cleaned = text.strip()
        if not cleaned:
            return "", ""

        # Tries to parse common response structure.
        match = re.search(r"final\s*answer\s*:\s*(.*)$", cleaned, flags=re.IGNORECASE | re.DOTALL)
        if match:
            final = match.group(1).strip()
            reasoning = cleaned[: match.start()].strip()
            return reasoning or cleaned, final

        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        if len(lines) >= 2:
            return "\n".join(lines[:-1]), lines[-1]

        return cleaned, cleaned

    def _extract_chat_content(self, data: Dict[str, Any]) -> str:
        if not isinstance(data, dict):
            return ""
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0] if isinstance(choices[0], dict) else {}
            if isinstance(first, dict):
                message = first.get("message")
                if isinstance(message, dict):
                    for key in ("content", "reasoning", "output_text", "text"):
                        token = str(message.get(key, "")).strip()
                        if token:
                            return token
                for key in ("content", "reasoning", "output_text", "text"):
                    token = str(first.get(key, "")).strip()
                    if token:
                        return token
        for key in ("output_text", "content", "response", "text"):
            token = str(data.get(key, "")).strip()
            if token:
                return token
        return ""

    def _safe_math_guess(self, question: str) -> str:
        """
        Tiny fallback for simple arithmetic when mini model is still stubbed.
        """
        expr_match = re.findall(r"([0-9\s\+\-\*/\(\)\.]+)", question)
        for part in expr_match:
            expr = part.strip()
            if (
                not expr
                or not re.search(r"[\+\-\*/]", expr)
                or not re.search(r"\d", expr)
                or not re.search(r"\d\s*[\+\-\*/]\s*\d", expr)
            ):
                continue
            try:
                value = eval(expr, {"__builtins__": {}})
                if isinstance(value, (int, float)):
                    return str(value)
            except Exception:
                continue
        return ""

    async def _attempt_contract_repair(
        self,
        *,
        provider: str,
        question: str,
        profile: ProblemProfile,
        retrieved: Sequence[RetrievedBlock],
        original_answer: ProviderAnswer,
        validation_reason: str,
    ) -> ProviderAnswer | None:
        if provider in {"mini", "symbolic_guard"}:
            return None

        original_raw = dict(original_answer.raw or {})
        raw_text = str(original_raw.get("raw_output_text", "")).strip()
        if not raw_text:
            return None

        local_extraction = extract_answer(
            question_text=question,
            raw_output=raw_text,
            metadata={"numeric_expected": bool(getattr(profile, "numeric", False))},
        )
        local_final = str(local_extraction.get("final_answer", "")).strip()
        if local_final and local_final.lower() not in {"[unresolved]", "unresolved", "unknown", "n/a"}:
            repaired_reasoning = str(local_extraction.get("reasoning", "")).strip() or str(original_answer.reasoning or "").strip()
            repaired_conf = max(0.20, min(0.70, float(original_answer.confidence)))
            answer_contract = self._answer_contract(
                final_answer=local_final,
                reasoning=repaired_reasoning,
                confidence=repaired_conf,
            )
            merged_raw = dict(original_raw)
            merged_raw["repair"] = {
                "attempted": True,
                "mode": "local_reextract",
                "validation_reason": str(validation_reason),
            }
            merged_raw["answer_contract"] = answer_contract
            return ProviderAnswer(
                provider=provider,
                reasoning=repaired_reasoning,
                final_answer=local_final,
                confidence=repaired_conf,
                self_critique=f"{original_answer.self_critique} + local extraction repair".strip(" +"),
                latency_s=float(original_answer.latency_s),
                answer_contract=answer_contract,
                raw=merged_raw,
            )

        repair_question = (
            "You are repairing a solver output contract.\n"
            "Extract the exact final answer from the model response below.\n"
            "If unresolved, output [UNRESOLVED].\n\n"
            "Return format:\n"
            "Reasoning: <one short line>\n"
            "Final Answer: <answer>\n\n"
            f"Original Question:\n{question}\n\n"
            f"Model Response:\n{raw_text}"
        )
        runner = self._runner(provider)
        timeout_s = float(min(20.0, self.provider_timeouts_s.get(provider, 45.0)))
        try:
            repaired = await asyncio.wait_for(
                runner(repair_question, profile, retrieved[:1]),
                timeout=timeout_s,
            )
        except Exception:
            return None

        repaired_final = str(repaired.final_answer or "").strip()
        if not repaired_final:
            return None
        if repaired_final.lower() in {"[unresolved]", "unresolved", "unknown", "n/a"}:
            return None

        repaired_reasoning = str(repaired.reasoning or "").strip() or str(original_answer.reasoning or "").strip()
        repaired_conf = max(0.20, min(0.72, float(repaired.confidence)))
        answer_contract = self._answer_contract(
            final_answer=repaired_final,
            reasoning=repaired_reasoning,
            confidence=repaired_conf,
        )
        repaired_raw = dict(repaired.raw or {})
        repaired_raw["repair"] = {
            "attempted": True,
            "mode": "provider_contract_repair",
            "validation_reason": str(validation_reason),
        }
        repaired_raw["answer_contract"] = answer_contract
        return ProviderAnswer(
            provider=provider,
            reasoning=repaired_reasoning,
            final_answer=repaired_final,
            confidence=repaired_conf,
            self_critique=f"{original_answer.self_critique} + provider contract repair".strip(" +"),
            latency_s=float(original_answer.latency_s) + float(repaired.latency_s),
            answer_contract=answer_contract,
            raw=repaired_raw,
        )

    def _is_simple_arithmetic_prompt(self, question: str) -> bool:
        cleaned = str(question or "").strip().lower()
        if not cleaned:
            return False
        normalized = cleaned.replace("×", "*").replace("÷", "/")
        stripped = re.sub(r"\b(what is|what's|compute|calculate|evaluate)\b", " ", normalized)
        # If meaningful alphabetic tokens remain, treat as symbolic/conceptual (not arithmetic-only).
        if re.search(r"[a-z]", stripped):
            return False
        stripped = re.sub(r"[^0-9\+\-\*/\(\)\.\s]", " ", stripped)
        stripped = re.sub(r"\s+", " ", stripped).strip()
        if not stripped:
            return False
        return bool(
            re.fullmatch(r"[-+*/().\s\d]+", stripped)
            and re.search(r"\d", stripped)
            and re.search(r"[+\-*/]", stripped)
            and re.search(r"\d\s*[\+\-\*/]\s*\d", stripped)
        )

    def _looks_degenerate_mini_output(
        self,
        *,
        question: str,
        reasoning: str,
        final_answer: str,
        mode: str,
    ) -> bool:
        answer = str(final_answer or "").strip().lower()
        reason = str(reasoning or "").strip().lower()
        prompt = str(question or "").strip().lower()
        zero_like = answer in {"0", "0.0", "0.00", "0.000", "0.0000"}
        replay_phrase = "applied retrieval-guided heuristic reasoning" in reason
        memory_phrase = "memory replay" in reason
        unsupported_mode = mode in {"unsupported", "fallback_unsupported"}
        conceptual_prompt = bool(
            re.search(r"\b(what is|what are|explain|define|difference|how to)\b", prompt)
        )
        if unsupported_mode:
            return True
        if zero_like and (replay_phrase or memory_phrase):
            return True
        if zero_like and conceptual_prompt and "key hint:" in reason:
            return True
        return False

    def _prompt_meta(self, provider: str, model_name: str, system_instructions: str, prompt: str) -> Dict[str, Any]:
        return {
            "provider": provider,
            "model_name": model_name,
            "template_version": self.prompt_template_version,
            "system_instructions": system_instructions,
            "prompt": prompt,
        }

    def _usage_from_payload(self, payload: Dict[str, Any], prompt: str, completion_text: str) -> Dict[str, float]:
        usage = payload.get("usage", {}) if isinstance(payload, dict) else {}
        if not usage and isinstance(payload, dict):
            usage = payload.get("usageMetadata", {})

        prompt_tokens = None
        completion_tokens = None
        total_tokens = None

        if isinstance(usage, dict):
            prompt_tokens = usage.get("prompt_tokens", usage.get("promptTokenCount"))
            completion_tokens = usage.get("completion_tokens", usage.get("candidatesTokenCount"))
            total_tokens = usage.get("total_tokens", usage.get("totalTokenCount"))

        p = float(prompt_tokens) if prompt_tokens is not None else float(self._estimate_tokens(prompt))
        c = float(completion_tokens) if completion_tokens is not None else float(self._estimate_tokens(completion_text))
        t = float(total_tokens) if total_tokens is not None else (p + c)
        return {"prompt_tokens": p, "completion_tokens": c, "total_tokens": t}

    def _estimate_tokens(self, text: str) -> int:
        if not text:
            return 0
        return max(1, int(round(len(text) / 4.0)))

    def _answer_contract(self, *, final_answer: str, reasoning: str, confidence: float) -> Dict[str, Any]:
        summary = self._reasoning_summary(reasoning)
        answer_type = self._infer_answer_type(final_answer)
        return {
            "final_answer": str(final_answer or ""),
            "reasoning_summary": summary,
            "answer_type": answer_type,
            "confidence": float(max(0.0, min(1.0, confidence))),
            "units": self._extract_units(final_answer),
        }

    def _reasoning_summary(self, reasoning: str) -> str:
        text = str(reasoning or "").strip()
        if not text:
            return ""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        compact = " ".join(lines)
        return compact[:220]

    def _infer_answer_type(self, answer: str) -> str:
        text = str(answer or "").strip()
        if not text:
            return "unknown"
        lowered = text.lower()
        if lowered in {"true", "false", "yes", "no"}:
            return "boolean"
        if re.fullmatch(r"[-+]?(?:\d+(?:\.\d+)?|\d+/\d+)", text):
            return "numeric"
        if re.search(r"[a-zA-Z]\s*[+\-*/^=]", text) or re.search(r"[+\-*/^=]", text):
            return "expression"
        return "symbolic"

    def _extract_units(self, text: str) -> str | None:
        match = re.search(
            r"\b(cm|mm|m|km|kg|g|mg|s|sec|ms|min|h|hr|N|J|W|V|A|mol|K|Pa|bar|deg|degree|rad|%)\b",
            str(text or ""),
            flags=re.IGNORECASE,
        )
        if match:
            return str(match.group(1))
        return None

    def _validate_answer(self, answer: ProviderAnswer) -> tuple[bool, str]:
        if not isinstance(answer, ProviderAnswer):
            return False, "schema_validation"
        if not str(answer.provider).strip():
            return False, "schema_validation"
        if not isinstance(answer.reasoning, str) or not isinstance(answer.final_answer, str):
            return False, "schema_validation"
        token = answer.final_answer.strip()
        if not token:
            return False, "empty_response"
        lowered = token.lower()
        if lowered in {"[unresolved]", "unresolved", "unknown", "n/a"}:
            return False, "unresolved_answer"
        if re.fullmatch(r"\(?[A-Da-d]\)?", token):
            return True, "ok"
        if re.fullmatch(r"\(?[A-Da-d]\)?(?:\s*(?:,|and|or|&)\s*\(?[A-Da-d]\)?)+", token):
            return True, "ok"
        if lowered in {"true", "false", "yes", "no"}:
            return True, "ok"
        compact = re.sub(r"\s+", "", token)
        numeric_short = bool(
            re.fullmatch(
                r"[-+]?(?:\d+(?:\.\d*)?|\d+/\d+|pi|π|e)",
                token,
                flags=re.IGNORECASE,
            )
        )
        if len(compact) < 2 and not numeric_short:
            if re.fullmatch(r"[A-Za-z0-9]", token):
                return True, "ok"
            return False, "too_short"
        return True, "ok"

    def _record_failure(self, provider: str, reason: str) -> Dict[str, Any]:
        provider = str(provider or "").strip()
        if provider in {"", "mini", "symbolic_guard"}:
            return {"provider": provider, "reason": str(reason), "state": "local", "opened": False}
        normalized = str(reason or "invalid_response").lower().strip()
        return self.circuit.record_failure(provider, normalized)

    def _record_success(self, provider: str) -> None:
        provider = str(provider or "").strip()
        if provider in {"", "mini", "symbolic_guard"}:
            return
        self.circuit.record_success(provider)

    def _failure_reason(self, exc: Exception) -> str:
        text = str(exc).lower()
        if isinstance(exc, httpx.HTTPStatusError):
            status = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
            if status in {401, 403}:
                return "auth"
            if status == 429:
                return "rate_limit"
            if status in {408, 499, 504}:
                return "timeout"
            if status in {400, 404, 409, 415, 422}:
                return "schema_validation"
            if 500 <= status <= 599:
                return "invalid_response"
        if isinstance(exc, (asyncio.TimeoutError, httpx.TimeoutException)) or "timeout" in text:
            return "timeout"
        if isinstance(exc, httpx.NetworkError):
            return "network"
        if "nodename nor servname" in text or "name or service not known" in text or "temporary failure in name resolution" in text:
            return "network"
        if "schema" in text or "json" in text:
            return "schema_validation"
        if "401" in text or "403" in text or "auth" in text:
            return "auth"
        if "429" in text or "rate" in text:
            return "rate_limit"
        return "invalid_response"

    def _should_retry_with_next_key(self, reason: str, *, attempt_idx: int, max_attempts: int) -> bool:
        if int(attempt_idx) >= int(max_attempts) - 1:
            return False
        retryable = {"rate_limit", "auth", "timeout", "network", "invalid_response", "schema_validation"}
        return str(reason).lower().strip() in retryable

    def _mask_key(self, key: str) -> str:
        digest = hashlib.sha256(str(key or "").encode("utf-8")).hexdigest()
        return digest[:12]

    def _request_hash(self, provider: str, question: str, profile: ProblemProfile) -> str:
        payload = f"{provider}|{profile.subject}|{profile.difficulty}|{question}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def _cache_put(self, key: str, answer: ProviderAnswer) -> None:
        if not self._valid_hash(key):
            return
        serialized = {
            "provider": answer.provider,
            "reasoning": answer.reasoning[:500],
            "final_answer": answer.final_answer,
            "confidence": float(answer.confidence),
            "answer_contract": dict(answer.answer_contract or {}),
            "token_usage": (answer.raw or {}).get("token_usage", {}),
        }
        self._response_cache[key] = serialized
        self._response_cache.move_to_end(key)
        while len(self._response_cache) > self._response_cache_limit:
            self._response_cache.popitem(last=False)

    def _cache_get(self, key: str) -> ProviderAnswer | None:
        if not self._valid_hash(key):
            return None
        row = self._response_cache.get(key)
        if row is None:
            return None
        self._response_cache.move_to_end(key)
        return ProviderAnswer(
            provider=str(row.get("provider", "unknown")),
            reasoning=str(row.get("reasoning", "")),
            final_answer=str(row.get("final_answer", "")),
            confidence=float(row.get("confidence", 0.2)),
            self_critique="stale_provider_cache_fallback",
            latency_s=0.0,
            answer_contract=dict(row.get("answer_contract", {}) or {}),
            raw={"cached_fallback": True, "token_usage": row.get("token_usage", {}), "answer_contract": row.get("answer_contract", {})},
        )

    def _valid_hash(self, key: str) -> bool:
        return isinstance(key, str) and len(key) == 40 and all(c in "0123456789abcdef" for c in key.lower())
