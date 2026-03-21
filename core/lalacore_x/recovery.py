from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from core.lalacore_x.runtime_telemetry import RuntimeTelemetry


async def retry_async(
    fn: Callable[[], Awaitable[Any]],
    *,
    component: str,
    operation: str,
    telemetry: RuntimeTelemetry,
    max_attempts: int = 3,
    base_delay_s: float = 0.25,
) -> Any:
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            out = await fn()
            telemetry.log_recovery_attempt(
                component=component,
                operation=operation,
                attempt=attempt,
                max_attempts=max_attempts,
                delay_s=0.0,
                status="success",
            )
            return out
        except Exception as exc:  # pragma: no cover - recovery path
            last_exc = exc
            delay = float(base_delay_s) * (2 ** (attempt - 1))
            telemetry.log_recovery_attempt(
                component=component,
                operation=operation,
                attempt=attempt,
                max_attempts=max_attempts,
                delay_s=delay,
                status="retrying" if attempt < max_attempts else "failed",
                error_type=type(exc).__name__,
            )
            if attempt < max_attempts:
                await asyncio.sleep(delay)
    raise last_exc

