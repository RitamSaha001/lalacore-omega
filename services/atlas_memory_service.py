from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime, timedelta, timezone


class AtlasMemoryService:
    """
    Non-destructive memory adapter over existing app-side storage.
    Reads current study/result/chat artifacts without changing their contracts.
    """

    def __init__(self, root: str | Path | None = None) -> None:
        base = Path(root) if root else Path(__file__).resolve().parents[1] / "data" / "app"
        self._results_file = base / "results.json"
        self._doubts_file = base / "doubts.json"
        self._chat_threads_file = base / "chat_threads.json"
        self._student_memory_file = base / "atlas_student_memory.json"
        self._tool_stats_file = base / "atlas_tool_stats.json"
        self._passive_events_file = base / "atlas_passive_events.json"
        self._memory_decay = 0.95
        self._max_weak_topics = 5
        self._max_recent_actions = 8
        self._passive_event_limit = 2

    def build_student_profile(
        self,
        *,
        user_context: Dict[str, Any] | None,
        fallback_profile: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        user_context = dict(user_context or {})
        profile = dict(fallback_profile or {})
        profile_hint = user_context.get("student_profile")
        if isinstance(profile_hint, dict):
            profile.update(dict(profile_hint))

        account_id = self._first_non_empty(
            user_context.get("account_id"),
            user_context.get("student_id"),
            user_context.get("user_id"),
            profile.get("account_id"),
            profile.get("student_id"),
        )
        if account_id:
            profile["account_id"] = account_id

        results = self._read_rows(self._results_file)
        recent_rows = [
            row
            for row in results
            if self._safe_text(
                row.get("account_id") or row.get("student_id") or row.get("user_id")
            )
            == account_id
        ]
        recent_rows.sort(key=lambda row: int(row.get("ts", 0) or 0), reverse=True)
        recent_rows = recent_rows[:10]

        mastery = self._aggregate_mastery(recent_rows)
        weak_from_results = [
            key for key, value in sorted(mastery.items(), key=lambda item: item[1]) if value < 65.0
        ][:5]
        recent_doubts = self._recent_doubt_topics(account_id=account_id, user_context=user_context)
        recent_chats = self._recent_chat_topics(
            account_id=account_id,
            chat_id=self._first_non_empty(user_context.get("chat_id"), user_context.get("session_id")),
        )

        weak_concepts = self._merge_lists(
            profile.get("weak_concepts") or profile.get("weakest_concepts"),
            weak_from_results,
            recent_doubts,
        )[:6]
        strong_concepts = [
            key for key, value in sorted(mastery.items(), key=lambda item: item[1], reverse=True) if value >= 75.0
        ][:4]

        trend = self._trend_signal(recent_rows)
        burnout = self._burnout_signal(recent_rows)
        preferred_style = self._preferred_style(
            existing=profile.get("preferred_style"),
            weak_concepts=weak_concepts,
            burnout=burnout,
            trend=trend,
        )
        explanation_depth = self._explanation_depth(
            weak_concepts=weak_concepts,
            burnout=burnout,
            trend=trend,
        )

        profile.update(
            {
                "weak_concepts": weak_concepts,
                "strong_concepts": strong_concepts,
                "concept_mastery": mastery,
                "preferred_style": preferred_style,
                "explanation_depth": explanation_depth,
                "recent_doubt_topics": recent_doubts[:5],
                "recent_chat_topics": recent_chats[:5],
                "performance_trend": trend,
                "burnout_risk": burnout,
            }
        )
        return {key: value for key, value in profile.items() if value not in (None, "", [], {})}

    def _aggregate_mastery(self, rows: List[Dict[str, Any]]) -> Dict[str, float]:
        buckets: Dict[str, List[float]] = defaultdict(list)
        for row in rows:
            raw = row.get("section_accuracy")
            if not isinstance(raw, dict):
                continue
            for key, value in raw.items():
                label = self._safe_text(key)
                if not label:
                    continue
                try:
                    buckets[label].append(float(value))
                except Exception:
                    continue
        if not buckets:
            return {}
        return {
            key: round(sum(values) / max(1, len(values)), 3)
            for key, values in buckets.items()
        }

    def _recent_doubt_topics(
        self,
        *,
        account_id: str,
        user_context: Dict[str, Any],
    ) -> List[str]:
        seeded = user_context.get("recent_doubt_topics")
        if isinstance(seeded, list) and seeded:
            return [self._safe_text(item) for item in seeded if self._safe_text(item)]

        rows = self._read_rows(self._doubts_file)
        out: List[str] = []
        for row in rows:
            if account_id:
                owner = self._safe_text(
                    row.get("student_id") or row.get("user_id") or row.get("account_id")
                )
                if owner and owner != account_id:
                    continue
            text = self._safe_text(row.get("question") or row.get("message") or row.get("topic"))
            if text:
                out.append(text)
        return self._dedupe(out)[:5]

    def _recent_chat_topics(self, *, account_id: str, chat_id: str) -> List[str]:
        rows = self._read_map(self._chat_threads_file)
        snippets: List[str] = []
        for _, thread in rows.items():
            if not isinstance(thread, dict):
                continue
            thread_id = self._safe_text(thread.get("chat_id"))
            participants = [self._safe_text(item) for item in (thread.get("participants") or [])]
            if chat_id and thread_id != chat_id:
                continue
            if account_id and account_id not in participants and not chat_id:
                continue
            for message in (thread.get("messages") or [])[-8:]:
                if not isinstance(message, dict):
                    continue
                text = self._safe_text(message.get("text"))
                if text:
                    snippets.append(text)
        ranked = Counter(self._topic_tokens(" ".join(snippets)))
        return [key for key, _ in ranked.most_common(5)]

    def _trend_signal(self, rows: List[Dict[str, Any]]) -> str:
        scores: List[float] = []
        for row in rows[:6]:
            try:
                score = float(row.get("score", 0.0) or 0.0)
                total = float(row.get("total", row.get("max_score", 100.0)) or 100.0)
                scores.append((score / max(1.0, total)) * 100.0)
            except Exception:
                continue
        if len(scores) < 2:
            return "unknown"
        delta = scores[0] - scores[-1]
        if delta >= 6.0:
            return "up"
        if delta <= -6.0:
            return "down"
        return "stable"

    def _burnout_signal(self, rows: List[Dict[str, Any]]) -> float:
        if not rows:
            return 0.0
        skipped = sum(int(row.get("skipped", 0) or 0) for row in rows[:6])
        wrong = sum(int(row.get("wrong", 0) or 0) for row in rows[:6])
        total = sum(
            int(row.get("correct", 0) or 0)
            + int(row.get("wrong", 0) or 0)
            + int(row.get("skipped", 0) or 0)
            for row in rows[:6]
        )
        if total <= 0:
            return 0.0
        return round(min(1.0, (0.55 * skipped + 0.35 * wrong) / max(1, total)), 6)

    def _preferred_style(
        self,
        *,
        existing: Any,
        weak_concepts: List[str],
        burnout: float,
        trend: str,
    ) -> str:
        token = self._safe_text(existing).lower()
        if token:
            return token
        if burnout >= 0.55:
            return "calm_step_by_step"
        if weak_concepts:
            return "example_driven_teaching"
        if trend == "up":
            return "concise_exam_focused"
        return "scaffolded_exam_focused"

    def _explanation_depth(
        self,
        *,
        weak_concepts: List[str],
        burnout: float,
        trend: str,
    ) -> str:
        if burnout >= 0.55 or weak_concepts:
            return "deep"
        if trend == "up":
            return "medium"
        return "deep"

    def _topic_tokens(self, text: str) -> List[str]:
        blocked = {
            "what",
            "when",
            "where",
            "which",
            "therefore",
            "please",
            "solve",
            "find",
            "show",
            "class",
            "lecture",
            "today",
        }
        tokens = []
        for token in self._safe_text(text).lower().split():
            token = "".join(ch for ch in token if ch.isalnum())
            if len(token) < 4 or token in blocked:
                continue
            tokens.append(token)
        return tokens

    def _merge_lists(self, *values: Any) -> List[str]:
        out: List[str] = []
        for value in values:
            if isinstance(value, list):
                items = value
            else:
                items = [value]
            for item in items:
                text = self._safe_text(item)
                if text and text not in out:
                    out.append(text)
        return out

    def _read_rows(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        try:
            decoded = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if not isinstance(decoded, list):
            return []
        return [dict(row) for row in decoded if isinstance(row, dict)]

    def _read_map(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        try:
            decoded = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(decoded, dict):
            return {}
        return {str(key): value for key, value in decoded.items()}

    def _dedupe(self, rows: List[str]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for row in rows:
            text = self._safe_text(row)
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
        return out

    def _first_non_empty(self, *values: Any) -> str:
        for value in values:
            text = self._safe_text(value)
            if text:
                return text
        return ""

    def _safe_text(self, value: Any) -> str:
        return str(value or "").strip()

    def get_student_memory(
        self,
        *,
        account_id: str,
        fallback_profile: Dict[str, Any] | None = None,
        recent_context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        account_id = self._safe_text(account_id)
        stored = self._read_map(self._student_memory_file).get(account_id)
        memory = dict(stored) if isinstance(stored, dict) else {}
        derived = self.build_student_profile(
            user_context={
                "account_id": account_id,
                **(dict(recent_context or {})),
            },
            fallback_profile=fallback_profile,
        )
        weak_topic_scores = self._decay_scored_map(memory.get("weak_topic_scores"))
        weak_topic_scores = self._boost_scores(
            weak_topic_scores,
            derived.get("weak_concepts"),
            weight=1.0,
        )
        weak_topic_scores = self._boost_scores(
            weak_topic_scores,
            (recent_context or {}).get("weak_topics"),
            weight=0.8,
        )
        weak_topic_scores = self._boost_scores(
            weak_topic_scores,
            (fallback_profile or {}).get("weak_topics"),
            weight=0.5,
        )
        preferred_action_scores = self._decay_scored_map(
            memory.get("preferred_action_scores"),
            decay=0.97,
            floor=0.05,
        )
        weak_topics = self._merge_lists(
            [
                key
                for key, _ in sorted(
                    weak_topic_scores.items(),
                    key=lambda item: (-float(item[1] or 0.0), item[0]),
                )
            ],
            memory.get("weak_topics"),
            memory.get("weak_concepts"),
            derived.get("weak_concepts"),
            (recent_context or {}).get("weak_topics"),
        )[: self._max_weak_topics]
        preferred_actions = self._merge_lists(
            [
                key
                for key, _ in sorted(
                    preferred_action_scores.items(),
                    key=lambda item: (-float(item[1] or 0.0), item[0]),
                )
            ],
            memory.get("preferred_actions"),
        )[:5]
        recent_actions = self._prune_recent_actions(
            memory.get("recent_actions"),
            max_items=self._max_recent_actions,
            max_age_days=14,
        )
        merged = {
            **derived,
            **memory,
            "account_id": account_id or self._safe_text(memory.get("account_id")),
            "last_material_id": self._first_non_empty(
                (recent_context or {}).get("selected_material", {}).get("material_id")
                if isinstance((recent_context or {}).get("selected_material"), dict)
                else "",
                memory.get("last_material_id"),
            ),
            "last_subject": self._first_non_empty(
                (recent_context or {}).get("selected_material", {}).get("subject")
                if isinstance((recent_context or {}).get("selected_material"), dict)
                else "",
                memory.get("last_subject"),
                derived.get("strong_concepts", [None])[0] if isinstance(derived.get("strong_concepts"), list) else "",
            ),
            "weak_topics": weak_topics,
            "weak_concepts": weak_topics,
            "preferred_actions": preferred_actions,
            "preferred_action_scores": preferred_action_scores,
            "weak_topic_scores": weak_topic_scores,
            "last_active": self._first_non_empty(memory.get("last_active"), self._now_iso()),
            "streak_days": int(memory.get("streak_days", 0) or 0),
            "recent_actions": recent_actions,
        }
        return {key: value for key, value in merged.items() if value not in (None, "", [], {})}

    def get_tool_stats_summary(self, *, limit: int = 18) -> Dict[str, Any]:
        raw = self._read_map(self._tool_stats_file)
        rows = [
            dict(value, tool_name=key)
            for key, value in raw.items()
            if isinstance(value, dict)
        ]
        rows.sort(
            key=lambda row: (
                -float(row.get("confidence_score", 0.0) or 0.0),
                float(row.get("avg_latency_ms", 999999.0) or 999999.0),
            )
        )
        recent_failures = [
            row["tool_name"]
            for row in rows
            if int(row.get("recent_failures", 0) or 0) > 0
        ][:6]
        preferred_tools = [row["tool_name"] for row in rows[:6]]
        exploration_rows = sorted(
            rows,
            key=lambda row: (
                int(row.get("recent_failures", 0) or 0) > 0,
                int(row.get("success_count", 0) or 0) + int(row.get("fail_count", 0) or 0),
                -float(row.get("confidence_score", 0.0) or 0.0),
                float(row.get("avg_latency_ms", 999999.0) or 999999.0),
            ),
        )
        exploration_candidates = [
            row["tool_name"]
            for row in exploration_rows
            if row["tool_name"] not in preferred_tools
            and int(row.get("recent_failures", 0) or 0) <= 0
        ][:4]
        return {
            "rows": rows[:limit],
            "preferred_tools": preferred_tools,
            "exploration_candidates": exploration_candidates,
            "avoid_tools": recent_failures,
        }

    def record_tool_execution(
        self,
        *,
        account_id: str,
        tool_name: str,
        category: str,
        success: bool,
        latency_ms: int,
        context: Dict[str, Any] | None = None,
        args: Dict[str, Any] | None = None,
        observation: str = "",
    ) -> Dict[str, Any]:
        account_id = self._safe_text(account_id)
        tool_name = self._safe_text(tool_name)
        category = self._safe_text(category) or "general"
        stats = self._read_map(self._tool_stats_file)
        row = dict(stats.get(tool_name) or {})
        success_count = int(row.get("success_count", 0) or 0) + (1 if success else 0)
        fail_count = int(row.get("fail_count", 0) or 0) + (0 if success else 1)
        previous_latency = float(row.get("avg_latency_ms", latency_ms or 0) or latency_ms or 0)
        next_latency = (
            (0.72 * previous_latency) + (0.28 * max(0, int(latency_ms or 0)))
            if previous_latency > 0
            else max(0, int(latency_ms or 0))
        )
        total = max(1, success_count + fail_count)
        confidence_score = round(
            ((success_count / total) * 0.7)
            + ((1.0 / (1.0 + next_latency)) * 0.3),
            6,
        )
        stats[tool_name] = {
            "category": category,
            "success_count": success_count,
            "fail_count": fail_count,
            "avg_latency_ms": round(next_latency, 3),
            "last_used": self._now_iso(),
            "confidence_score": confidence_score,
            "recent_failures": 0 if success else min(5, int(row.get("recent_failures", 0) or 0) + 1),
        }
        self._write_map(self._tool_stats_file, stats)

        memory_map = self._read_map(self._student_memory_file)
        memory = self.get_student_memory(
            account_id=account_id,
            fallback_profile=context.get("student_profile") if isinstance(context, dict) and isinstance(context.get("student_profile"), dict) else None,
            recent_context=context,
        )
        previous_last_active = memory.get("last_active")
        memory["recent_actions"] = self._prune_recent_actions(
            [
                {
                    "tool": tool_name,
                    "success": success,
                    "latency_ms": int(latency_ms or 0),
                    "timestamp": self._now_iso(),
                    "observation": self._safe_text(observation),
                },
                *self._prune_recent_actions(
                    memory.get("recent_actions"),
                    max_items=self._max_recent_actions,
                    max_age_days=14,
                ),
            ],
            max_items=self._max_recent_actions,
            max_age_days=14,
        )
        preferred_scores = self._decay_scored_map(
            memory.get("preferred_action_scores"),
            decay=self._memory_decay,
            floor=0.05,
        )
        preferred_scores[tool_name] = round(
            float(preferred_scores.get(tool_name, 0.0) or 0.0)
            + (1.0 if success else 0.18),
            6,
        )
        memory["preferred_action_scores"] = preferred_scores
        ranked_preferred = sorted(
            preferred_scores.items(),
            key=lambda item: (-float(item[1] or 0.0), item[0]),
        )
        memory["preferred_actions"] = [key for key, _ in ranked_preferred[:5]]
        self._apply_memory_hooks(memory, tool_name=tool_name, context=context, args=args)
        memory["streak_days"] = self._updated_streak_days(previous_last_active, memory.get("streak_days"))
        memory["last_active"] = self._now_iso()
        memory_map[account_id] = memory
        self._write_map(self._student_memory_file, memory_map)

        passive_events = self.generate_passive_events(
            account_id=account_id,
            context=context,
            memory=memory,
        )
        return {
            "ok": True,
            "tool_name": tool_name,
            "student_memory": memory,
            "tool_stats": stats[tool_name],
            "passive_events": passive_events,
        }

    def generate_passive_events(
        self,
        *,
        account_id: str,
        context: Dict[str, Any] | None = None,
        memory: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        account_id = self._safe_text(account_id)
        context = dict(context or {})
        memory = dict(memory or self.get_student_memory(account_id=account_id, recent_context=context))
        pending_homeworks = int(context.get("pending_homework_count", 0) or 0)
        pending_exams = int(context.get("pending_exam_count", 0) or 0)
        weak_topics = self._merge_lists(memory.get("weak_topics"), memory.get("weak_concepts"))[:4]
        recent_events_map = self._read_map(self._passive_events_file)
        events: List[Dict[str, Any]] = []
        if pending_homeworks > 0:
            events.append(
                self._passive_event(
                    account_id=account_id,
                    event="homework_due",
                    message=f"You have {pending_homeworks} homework item(s) still pending.",
                    urgency=min(1.0, 0.35 + (pending_homeworks * 0.2)),
                    importance=0.75,
                )
            )
        if pending_exams > 0:
            events.append(
                self._passive_event(
                    account_id=account_id,
                    event="exam_due",
                    message=f"You have {pending_exams} exam item(s) pending.",
                    urgency=min(1.0, 0.65 + (pending_exams * 0.1)),
                    importance=0.95,
                )
            )
        if weak_topics:
            events.append(
                self._passive_event(
                    account_id=account_id,
                    event="weak_topic_nudge",
                    message=f'Your weakest topic right now is {weak_topics[0]}.',
                    urgency=0.45,
                    importance=0.68,
                )
            )
        last_active_raw = self._safe_text(memory.get("last_active"))
        if last_active_raw:
            try:
                last_active = datetime.fromisoformat(last_active_raw.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - last_active >= timedelta(days=2):
                    events.append(
                        self._passive_event(
                            account_id=account_id,
                            event="inactivity_nudge",
                            message="You have been inactive for a couple of days. A short revision block would help.",
                            urgency=0.52,
                            importance=0.48,
                        )
                    )
            except Exception:
                pass
        deduped = self._dedupe_events(events)
        deduped.sort(
            key=lambda item: (
                -float(item.get("priority_score", 0.0) or 0.0),
                self._safe_text(item.get("created_at")),
            )
        )
        recent_events_map[account_id] = deduped[:4]
        self._write_map(self._passive_events_file, recent_events_map)
        return deduped[: self._passive_event_limit]

    def _apply_memory_hooks(
        self,
        memory: Dict[str, Any],
        *,
        tool_name: str,
        context: Dict[str, Any] | None,
        args: Dict[str, Any] | None,
    ) -> None:
        context = dict(context or {})
        args = dict(args or {})
        selected_material = (
            dict(context.get("selected_material"))
            if isinstance(context.get("selected_material"), dict)
            else {}
        )
        if tool_name in {
            "open_material",
            "download_material",
            "summarize_material_with_ai",
            "make_notes_from_material",
            "ask_material_ai",
            "open_material_notes",
            "open_material_summary",
        }:
            material_id = self._first_non_empty(
                args.get("material_id"),
                selected_material.get("material_id"),
            )
            subject = self._first_non_empty(
                args.get("subject"),
                selected_material.get("subject"),
                memory.get("last_subject"),
            )
            if material_id:
                memory["last_material_id"] = material_id
            if subject:
                memory["last_subject"] = subject
        if tool_name in {"open_homework", "open_exam", "show_remaining_work", "list_pending_homeworks", "list_pending_exams"}:
            pending_counts = [
                value
                for value in (
                    context.get("pending_homework_count"),
                    context.get("pending_exam_count"),
                )
                if isinstance(value, (int, float))
            ]
            if pending_counts:
                memory["last_pending_work_signal"] = int(sum(pending_counts))
        weak_scores = self._decay_scored_map(
            memory.get("weak_topic_scores"),
            decay=self._memory_decay,
            floor=0.05,
        )
        weak_topics = self._merge_lists(
            context.get("weak_topics"),
            context.get("weak_concepts"),
            context.get("student_profile", {}).get("weak_topics")
            if isinstance(context.get("student_profile"), dict)
            else [],
            context.get("student_profile", {}).get("weak_concepts")
            if isinstance(context.get("student_profile"), dict)
            else [],
        )
        weak_scores = self._boost_scores(weak_scores, weak_topics, weight=0.9)
        if weak_scores:
            ranked = sorted(
                weak_scores.items(),
                key=lambda item: (-float(item[1] or 0.0), item[0]),
            )
            memory["weak_topic_scores"] = weak_scores
            memory["weak_topics"] = [key for key, _ in ranked[: self._max_weak_topics]]
            memory["weak_concepts"] = memory["weak_topics"]

    def _updated_streak_days(self, last_active: Any, streak_days: Any) -> int:
        previous = int(streak_days or 0)
        raw = self._safe_text(last_active)
        if not raw:
            return max(1, previous)
        try:
            then = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
            today = datetime.now(timezone.utc).date()
            delta = (today - then).days
            if delta <= 0:
                return max(1, previous)
            if delta == 1:
                return max(1, previous) + 1
            return 1
        except Exception:
            return max(1, previous)

    def _normalize_action_history(self, raw: Any) -> List[Dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        out: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            tool = self._safe_text(item.get("tool"))
            if not tool:
                continue
            out.append(
                {
                    "tool": tool,
                    "success": bool(item.get("success")),
                    "latency_ms": int(item.get("latency_ms", 0) or 0),
                    "timestamp": self._safe_text(item.get("timestamp")) or self._now_iso(),
                    "observation": self._safe_text(item.get("observation")),
                }
            )
        return out

    def _prune_recent_actions(
        self,
        raw: Any,
        *,
        max_items: int,
        max_age_days: int,
    ) -> List[Dict[str, Any]]:
        actions = self._normalize_action_history(raw)
        now = datetime.now(timezone.utc)
        kept: List[Dict[str, Any]] = []
        for item in actions:
            timestamp_raw = self._safe_text(item.get("timestamp"))
            try:
                parsed = datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
            except Exception:
                parsed = now
            if now - parsed > timedelta(days=max_age_days):
                continue
            kept.append(item)
            if len(kept) >= max_items:
                break
        return kept

    def _normalize_scored_map(self, raw: Any) -> Dict[str, float]:
        if not isinstance(raw, dict):
            return {}
        out: Dict[str, float] = {}
        for key, value in raw.items():
            label = self._safe_text(key)
            if not label:
                continue
            try:
                score = float(value or 0.0)
            except Exception:
                continue
            if score <= 0.0:
                continue
            out[label] = round(score, 6)
        return out

    def _decay_scored_map(
        self,
        raw: Any,
        *,
        decay: float | None = None,
        floor: float = 0.08,
    ) -> Dict[str, float]:
        factor = float(decay if decay is not None else self._memory_decay)
        out: Dict[str, float] = {}
        for key, score in self._normalize_scored_map(raw).items():
            next_score = round(float(score) * factor, 6)
            if next_score >= floor:
                out[key] = next_score
        return out

    def _boost_scores(
        self,
        scores: Dict[str, float],
        values: Any,
        *,
        weight: float,
    ) -> Dict[str, float]:
        out = dict(scores)
        for item in values if isinstance(values, list) else [values]:
            label = self._safe_text(item)
            if not label:
                continue
            out[label] = round(float(out.get(label, 0.0) or 0.0) + weight, 6)
        return out

    def _passive_event(
        self,
        *,
        account_id: str,
        event: str,
        message: str,
        urgency: float,
        importance: float,
    ) -> Dict[str, Any]:
        priority_score = round((max(0.0, min(1.0, urgency)) * 0.7) + (max(0.0, min(1.0, importance)) * 0.3), 6)
        if priority_score >= 0.78:
            priority = "high"
        elif priority_score >= 0.54:
            priority = "medium"
        else:
            priority = "low"
        return {
            "id": f"{account_id}:{event}",
            "event": event,
            "priority": priority,
            "priority_score": priority_score,
            "message": message,
            "created_at": self._now_iso(),
            "shown": False,
        }

    def _dedupe_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for event in events:
            key = self._safe_text(event.get("id")) or self._safe_text(event.get("event"))
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(event)
        return out

    def _write_map(self, path: Path, payload: Dict[str, Any]) -> None:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
