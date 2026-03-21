import time
import random
from collections import defaultdict
from threading import Lock


class KeyManager:

    def __init__(self):
        self.keys = defaultdict(list)
        self.stats = defaultdict(lambda: {
            "success": 0,
            "fail": 0,
            "cooldown_until": 0,
            "consecutive_failures": 0,
            "last_error": "",
            "last_used_at": 0.0,
        })
        self.provider_cursor = defaultdict(int)
        self.error_cooldowns_s = {
            "rate_limit": 60.0,
            "auth": 4 * 3600.0,
            "timeout": 45.0,
            "network": 25.0,
            "invalid_response": 35.0,
            "schema_validation": 90.0,
            "generic": 120.0,
        }
        self.lock = Lock()

    def register_provider_keys(self, provider_name: str, key_list: list[str]):
        deduped = []
        seen = set()
        for key in key_list:
            value = str(key or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        self.keys[provider_name] = deduped
        self.provider_cursor[str(provider_name)] = 0

    def get_key(self, provider_name: str, *, exclude_keys: list[str] | None = None):

        with self.lock:
            now = time.time()
            available = []
            provider_keys = self.keys.get(provider_name, [])
            excluded = set(str(key) for key in (exclude_keys or []))

            if not provider_keys:
                raise RuntimeError(f"No keys registered for provider '{provider_name}'")

            for key in provider_keys:
                if key in excluded:
                    continue
                if self.stats[key]["cooldown_until"] <= now:
                    available.append(key)

            if not available:
                # If all cooling down, pick the earliest recovering key (deterministic).
                fallback_pool = [key for key in provider_keys if key not in excluded] or list(provider_keys)
                selected = min(
                    fallback_pool,
                    key=lambda key: (
                        float(self.stats[key]["cooldown_until"]),
                        -self._score(key),
                        float(self.stats[key]["last_used_at"]),
                        random.random(),
                    ),
                )
                self.stats[selected]["last_used_at"] = now
                return selected

            # Weighted selection with mild round-robin among top keys to avoid hot-spotting.
            ranked = sorted(
                available,
                key=lambda key: (self._score(key), -float(self.stats[key]["last_used_at"])),
                reverse=True,
            )
            best_score = self._score(ranked[0])
            top = [key for key in ranked if self._score(key) >= (0.95 * best_score)]
            cursor = int(self.provider_cursor[str(provider_name)]) % len(top)
            selected = top[cursor]
            self.provider_cursor[str(provider_name)] = cursor + 1
            self.stats[selected]["last_used_at"] = now
            return selected

    def _score(self, key: str) -> float:
        s = float(self.stats[key]["success"])
        f = float(self.stats[key]["fail"])
        streak = float(self.stats[key]["consecutive_failures"])
        return (s + 1.0) / (f + 1.0 + 0.25 * streak)

    def report_success(self, key: str):
        with self.lock:
            self.stats[key]["success"] += 1
            self.stats[key]["consecutive_failures"] = 0
            self.stats[key]["last_error"] = ""
            self.stats[key]["cooldown_until"] = min(float(self.stats[key]["cooldown_until"]), time.time())
            self.stats[key]["last_used_at"] = time.time()

    def report_failure(self, key: str, error_type="generic"):

        with self.lock:
            self.stats[key]["fail"] += 1
            self.stats[key]["consecutive_failures"] += 1

            now = time.time()
            reason = str(error_type or "generic").strip().lower()
            if reason not in self.error_cooldowns_s:
                reason = "generic"
            self.stats[key]["last_error"] = reason
            self.stats[key]["last_used_at"] = now

            # Dynamic cooldown with bounded streak escalation.
            base = float(self.error_cooldowns_s.get(reason, self.error_cooldowns_s["generic"]))
            streak = max(1.0, float(self.stats[key]["consecutive_failures"]))
            multiplier = min(4.0, 1.0 + 0.35 * (streak - 1.0))
            cooldown = base * multiplier

            self.stats[key]["cooldown_until"] = now + cooldown

    def provider_health(self, provider_name: str) -> dict:
        with self.lock:
            now = time.time()
            out = []
            for key in self.keys.get(provider_name, []):
                row = self.stats[key]
                out.append(
                    {
                        "key_tail": str(key)[-6:],
                        "success": int(row.get("success", 0)),
                        "fail": int(row.get("fail", 0)),
                        "consecutive_failures": int(row.get("consecutive_failures", 0)),
                        "last_error": str(row.get("last_error", "")),
                        "cooldown_remaining_s": float(max(0.0, float(row.get("cooldown_until", 0.0)) - now)),
                        "score": float(self._score(key)),
                    }
                )
            return {"provider": str(provider_name), "keys": out}
