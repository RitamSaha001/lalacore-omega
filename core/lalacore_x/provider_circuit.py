from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List


class ProviderCircuitBreaker:
    """
    Provider failure isolation with cooldown auto-recovery.
    """

    def __init__(
        self,
        path: str = "data/metrics/provider_circuit.json",
        *,
        failure_threshold: int = 5,
        failure_window_s: float = 30.0,
        cooldown_s: float = 60.0,
        cooldown_jitter_s: float = 0.0,
        half_open_successes: int = 2,
        open_probe_every_requests: int = 5,
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.failure_threshold = int(failure_threshold)
        self.failure_window_s = max(0.05, float(failure_window_s))
        self.cooldown_s = float(cooldown_s)
        self.cooldown_jitter_s = max(0.0, float(cooldown_jitter_s))
        self.half_open_successes = int(half_open_successes)
        self.open_probe_every_requests = int(max(1, open_probe_every_requests))
        # Guard against corrupted/stale persisted timestamps keeping circuits open for hours.
        self.max_open_window_s = max(float(self.cooldown_s) * 8.0, 900.0)
        self.state = self._load()

    def active(self, providers: Iterable[str]) -> List[str]:
        return [p for p in providers if self.can_request(p)]

    def can_request(self, provider: str, *, consume_probe: bool = True) -> bool:
        row = self._provider(provider)
        now = time.time()
        dirty = self._refresh_recovery_state(row, now)
        state = str(row.get("state", "closed"))

        if state == "open":
            if dirty:
                self._save()
            return False

        if state == "half_open":
            if not consume_probe:
                if dirty:
                    self._save()
                return True
            row["probe_counter"] = int(row.get("probe_counter", 0)) + 1
            self._save()
            # In half-open we permit probe traffic; failures immediately reopen.
            return True

        if dirty:
            self._save()
        return True

    def should_force_probe(self, provider: str) -> bool:
        """
        Periodic open-circuit probing:
        for providers still in open state, allow one half-open probe every
        `open_probe_every_requests` availability cycles.
        """
        row = self._provider(provider)
        now = time.time()
        dirty = self._refresh_recovery_state(row, now)
        state = str(row.get("state", "closed"))
        if state != "open":
            if dirty:
                self._save()
            return False

        open_until = float(row.get("open_until", 0.0) or 0.0)
        if now >= open_until:
            if dirty:
                self._save()
            return False

        row["open_probe_counter"] = int(row.get("open_probe_counter", 0)) + 1
        if int(row["open_probe_counter"]) < self.open_probe_every_requests:
            self._save()
            return False

        row["open_probe_counter"] = 0
        row["state"] = "half_open"
        row["half_open_success"] = 0
        row["probe_counter"] = 0
        row["open_until"] = now + self.cooldown_s
        self._save()
        return True

    def record_success(self, provider: str) -> None:
        row = self._provider(provider)
        self._refresh_recovery_state(row, time.time())
        row["requests"] += 1
        row["success"] += 1
        row["consecutive_failures"] = 0
        row["consecutive_infra_failures"] = 0
        self._bump_health(row, delta=+0.05)

        if row.get("state") == "half_open":
            row["half_open_success"] = int(row.get("half_open_success", 0)) + 1
            if int(row["half_open_success"]) >= self.half_open_successes:
                row["state"] = "closed"
                row["half_open_success"] = 0
                row["open_until"] = 0.0
                row["open_probe_counter"] = 0

        self._save()

    def record_failure(self, provider: str, reason: str) -> Dict:
        row = self._provider(provider)
        now = time.time()
        self._refresh_recovery_state(row, now)
        row["requests"] += 1
        row["failures"] += 1
        row["consecutive_failures"] += 1

        key = self._reason_key(reason)
        row[key] = int(row.get(key, 0)) + 1
        infra_failure = self._is_infra_reason(key)
        if infra_failure:
            row["consecutive_infra_failures"] = int(row.get("consecutive_infra_failures", 0)) + 1
            self._append_recent_infra_failure(row, now)
            self._bump_health(row, delta=-0.10)
        else:
            # Non-infra failures should influence routing confidence but must not
            # aggressively open provider circuits.
            row["consecutive_infra_failures"] = 0
            self._bump_health(row, delta=-0.02)

        state = str(row.get("state", "closed"))
        open_until = float(row.get("open_until", 0.0) or 0.0)
        incident = {"provider": provider, "reason": key, "state": state, "opened": False}

        # Never extend an already-open window for synthetic "blocked by circuit" paths.
        if state == "open" and now < open_until:
            incident["open_until"] = open_until
        elif state == "half_open":
            if infra_failure:
                row["state"] = "open"
                row["open_until"] = now + self._cooldown_with_jitter(provider)
                row["half_open_success"] = 0
                row["probe_counter"] = 0
                row["open_probe_counter"] = 0
                incident["state"] = "open"
                incident["opened"] = True
                incident["open_until"] = row["open_until"]
        elif infra_failure and self._infra_failure_count(row, now) >= self.failure_threshold:
            row["state"] = "open"
            row["open_until"] = now + self._cooldown_with_jitter(provider)
            row["half_open_success"] = 0
            row["open_probe_counter"] = 0
            incident["state"] = "open"
            incident["opened"] = True
            incident["open_until"] = row["open_until"]

        self._save()
        return incident

    def summary(self) -> Dict:
        providers = self.state.get("providers", {})
        out = {}
        now = time.time()
        dirty = False
        for provider, row in providers.items():
            if self._refresh_recovery_state(row, now):
                dirty = True
            open_until = float(row.get("open_until", 0.0))
            out[provider] = {
                "state": row.get("state", "closed"),
                "open_for_s": max(0.0, open_until - now),
                "requests": int(row.get("requests", 0)),
                "failures": int(row.get("failures", 0)),
                "health_score": float(row.get("health_score", 1.0)),
            }
        if dirty:
            self._save()
        return out

    def _reason_key(self, reason: str) -> str:
        reason = str(reason or "generic").lower().strip()
        if reason in {
            "timeout",
            "network",
            "invalid_response",
            "empty_response",
            "schema_validation",
            "auth",
            "rate_limit",
            "unresolved_answer",
            "too_short",
        }:
            return reason
        return "generic"

    def _is_infra_reason(self, reason: str) -> bool:
        return reason in {"timeout", "network", "invalid_response", "auth", "rate_limit"}

    def _append_recent_infra_failure(self, row: Dict, now: float) -> None:
        raw = row.get("recent_infra_failures", [])
        if not isinstance(raw, list):
            raw = []
        raw = [float(ts) for ts in raw if float(ts) >= now - self.failure_window_s]
        raw.append(float(now))
        row["recent_infra_failures"] = raw

    def _infra_failure_count(self, row: Dict, now: float) -> int:
        raw = row.get("recent_infra_failures", [])
        if not isinstance(raw, list):
            row["recent_infra_failures"] = []
            return 0
        fresh = [float(ts) for ts in raw if float(ts) >= now - self.failure_window_s]
        row["recent_infra_failures"] = fresh
        return len(fresh)

    def _bump_health(self, row: Dict, *, delta: float) -> None:
        health = float(row.get("health_score", 1.0))
        health = max(0.0, min(1.0, health + float(delta)))
        row["health_score"] = round(health, 6)

    def _cooldown_with_jitter(self, provider: str) -> float:
        if self.cooldown_jitter_s <= 0.0:
            return self.cooldown_s
        # Deterministic jitter spreads recovery probes across instances.
        epoch_bucket = int(time.time() // max(1.0, self.cooldown_s))
        seed = f"{provider}:{epoch_bucket}".encode("utf-8")
        digest = hashlib.sha256(seed).digest()
        jitter_ratio = int.from_bytes(digest[:2], "big") / 65535.0
        jitter = jitter_ratio * self.cooldown_jitter_s
        return self.cooldown_s + jitter

    def _provider(self, provider: str) -> Dict:
        rows = self.state.setdefault("providers", {})
        defaults = {
            "state": "closed",
            "open_until": 0.0,
            "half_open_success": 0,
            "probe_counter": 0,
            "open_probe_counter": 0,
            "requests": 0,
            "success": 0,
            "failures": 0,
            "consecutive_failures": 0,
            "consecutive_infra_failures": 0,
            "recent_infra_failures": [],
            "health_score": 1.0,
            "timeout": 0,
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
        row = rows.setdefault(provider, dict(defaults))
        for key, value in defaults.items():
            row.setdefault(key, value)
        return row

    def _refresh_recovery_state(self, row: Dict, now: float) -> bool:
        dirty = False
        state = str(row.get("state", "closed"))
        open_until = float(row.get("open_until", 0.0) or 0.0)

        # Bound unrealistic persisted open windows and return to normal cooldown behavior.
        max_open_until = float(now) + float(self.max_open_window_s)
        if state == "open" and open_until > max_open_until:
            row["open_until"] = float(now) + float(self.cooldown_s)
            open_until = float(row["open_until"])
            dirty = True

        if state == "open" and now >= open_until:
            row["state"] = "half_open"
            row["half_open_success"] = 0
            row["probe_counter"] = 0
            row["open_probe_counter"] = 0
            dirty = True
        return dirty

    def _load(self) -> Dict:
        base = {"providers": {}}
        if not self.path.exists():
            return base
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                base.update(data)
        except Exception:
            pass
        return base

    def _save(self) -> None:
        self.path.write_text(json.dumps(self.state, indent=2, sort_keys=True), encoding="utf-8")
