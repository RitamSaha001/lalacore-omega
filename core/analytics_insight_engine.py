from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Sequence

from core.lalacore_x.providers import ProviderFabric, provider_runtime_budget
from core.lalacore_x.retrieval import ConceptVault
from core.lalacore_x.schemas import ProblemProfile, ProviderAnswer, RetrievedBlock


_ANALYTICS_PROMPT_MARKER = "[[LC9_ANALYTICS_ENGINE:"


class AnalyticsInsightEngine:
    """
    Dedicated analytics/content engine for profile, exam, and class insights.

    This keeps long-form analytics generation separate from the strict numeric
    solver and from the Study-material generation engine.
    """

    def __init__(self) -> None:
        self.providers = ProviderFabric()
        self.vault = ConceptVault()

    async def analyze_exam(
        self,
        *,
        result: Dict[str, Any],
        options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized = self._normalize_result(result)
        baseline = self._baseline_exam_analysis(normalized)
        prompt = self._exam_prompt(normalized, baseline)
        retrieved = self._build_exam_blocks(normalized, baseline)
        return await self._run_json_task(
            task="analyze_exam",
            subject=self._infer_subject(normalized),
            title=normalized.get("quiz_title") or "Assessment analysis",
            prompt=prompt,
            baseline=baseline,
            retrieved=retrieved,
            response_keys=("analytics", "data"),
            options=options,
        )

    async def student_profile(
        self,
        *,
        history: Sequence[Dict[str, Any]],
        options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_history = self._normalize_history(history)
        baseline = self._baseline_student_profile(normalized_history)
        prompt = self._student_profile_prompt(normalized_history, baseline)
        retrieved = self._build_student_history_blocks(
            normalized_history,
            title="Student profile history",
            baseline=baseline,
        )
        return await self._run_json_task(
            task="student_profile",
            subject=self._history_subject(normalized_history),
            title="Student profile",
            prompt=prompt,
            baseline=baseline,
            retrieved=retrieved,
            response_keys=("profile", "data"),
            options=options,
        )

    async def student_intelligence(
        self,
        *,
        account_id: str,
        latest_result: Dict[str, Any],
        history: Sequence[Dict[str, Any]],
        options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_history = self._normalize_history([*history, latest_result])
        baseline = self._baseline_student_intelligence(
            account_id=account_id,
            history=normalized_history,
        )
        prompt = self._student_intelligence_prompt(
            account_id=account_id,
            history=normalized_history,
            baseline=baseline,
        )
        retrieved = self._build_student_history_blocks(
            normalized_history,
            title=f"Student intelligence for {account_id or 'student'}",
            baseline=baseline,
        )
        return await self._run_json_task(
            task="student_intelligence",
            subject=self._history_subject(normalized_history),
            title="Student intelligence",
            prompt=prompt,
            baseline=baseline,
            retrieved=retrieved,
            response_keys=("data",),
            options=options,
        )

    async def class_summary(
        self,
        *,
        students: Sequence[Dict[str, Any]],
        exams: Sequence[Dict[str, Any]] | None = None,
        homeworks: Sequence[Dict[str, Any]] | None = None,
        study_materials: Sequence[Dict[str, Any]] | None = None,
        scheduled_classes: Sequence[Dict[str, Any]] | None = None,
        options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_students = self._normalize_students(students)
        baseline = self._baseline_class_summary(
            students=normalized_students,
            exams=exams or [],
            homeworks=homeworks or [],
            study_materials=study_materials or [],
            scheduled_classes=scheduled_classes or [],
        )
        prompt = self._class_summary_prompt(
            students=normalized_students,
            exams=exams or [],
            homeworks=homeworks or [],
            study_materials=study_materials or [],
            scheduled_classes=scheduled_classes or [],
            baseline=baseline,
        )
        retrieved = self._build_class_blocks(
            students=normalized_students,
            exams=exams or [],
            homeworks=homeworks or [],
            study_materials=study_materials or [],
            scheduled_classes=scheduled_classes or [],
            baseline=baseline,
        )
        return await self._run_json_task(
            task="class_summary",
            subject=self._class_subject(normalized_students),
            title="Teacher class summary",
            prompt=prompt,
            baseline=baseline,
            retrieved=retrieved,
            response_keys=("data", "analytics"),
            options=options,
        )

    async def _run_json_task(
        self,
        *,
        task: str,
        subject: str,
        title: str,
        prompt: str,
        baseline: Dict[str, Any],
        retrieved: Sequence[RetrievedBlock],
        response_keys: Sequence[str],
        options: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        safe_options = dict(options or {})
        wrapped_prompt = self._wrap_prompt(task=task, title=title, prompt=prompt)
        profile = ProblemProfile(
            subject=(subject or "general").strip().lower() or "general",
            difficulty="medium",
            numeric=False,
            multi_concept=True,
            trap_probability=0.08,
            features={"analytics_task": task},
        )

        try:
            await self.providers.ensure_startup_warmup()
        except Exception:
            pass

        selected = self._select_provider_pool()
        if not selected:
            return self._failure(
                status="ANALYTICS_PROVIDER_UNAVAILABLE",
                message="No AI providers are currently available for analytics generation.",
                task=task,
                baseline=baseline,
                retrieved=retrieved,
            )

        timeout_overrides = (
            safe_options.get("provider_timeout_overrides")
            if isinstance(safe_options.get("provider_timeout_overrides"), dict)
            else None
        )
        try:
            with provider_runtime_budget(timeout_overrides=timeout_overrides):
                candidates = await self.providers.generate_many(
                    selected,
                    wrapped_prompt,
                    profile,
                    list(retrieved),
                )
        except Exception as exc:
            return self._failure(
                status="ANALYTICS_PROVIDER_ERROR",
                message=f"Analytics AI provider call failed: {exc}",
                task=task,
                baseline=baseline,
                retrieved=retrieved,
            )

        ranked = self._rank_candidates(task=task, candidates=candidates, baseline=baseline)
        if not ranked:
            return self._failure(
                status="ANALYTICS_ENGINE_EMPTY_OUTPUT",
                message="Analytics AI did not return valid structured output.",
                task=task,
                baseline=baseline,
                retrieved=retrieved,
                candidates=candidates,
            )

        best = ranked[0]
        payload = self._merge_payload(baseline, best["parsed"])
        out: Dict[str, Any] = {
            "ok": True,
            "status": "SUCCESS",
            "authoritative": True,
            "fallback_used": False,
            "winner_provider": best["provider"],
            "confidence": round(float(best["confidence"]), 6),
            "provider_diagnostics": self._provider_diagnostics(candidates),
            "citations": self._build_citations(retrieved),
            "engine": {
                "name": "ANALYTICS_INSIGHT_ENGINE",
                "version": "analytics-v1",
                "providers_attempted": list(selected),
                "provider_count": len(selected),
                "task": task,
            },
        }
        for key in response_keys:
            out[key] = payload
        return out

    def _wrap_prompt(self, *, task: str, title: str, prompt: str) -> str:
        return (
            f"{_ANALYTICS_PROMPT_MARKER}{task}]]\n"
            f"Analytics title: {title}\n\n"
            f"{prompt.strip()}"
        )

    def _select_provider_pool(self) -> List[str]:
        available = [
            provider
            for provider in self.providers.available_providers()
            if provider not in {"symbolic_guard"}
        ]
        preferred = [provider for provider in available if provider != "mini"]
        selected = preferred[:2]
        if len(selected) < 2 and "mini" in available and "mini" not in selected:
            selected.append("mini")
        if not selected and available:
            selected = available[:1]
        return selected

    def _rank_candidates(
        self,
        *,
        task: str,
        candidates: Sequence[ProviderAnswer],
        baseline: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        ranked: List[Dict[str, Any]] = []
        for candidate in candidates:
            parsed = self._parse_candidate_json(candidate.final_answer)
            if not isinstance(parsed, dict):
                continue
            normalized = self._normalize_output_shape(task=task, parsed=parsed)
            if not self._is_valid_task_payload(task=task, payload=normalized):
                continue
            score = self._task_score(
                payload=normalized,
                baseline=baseline,
                confidence=float(candidate.confidence),
            )
            ranked.append(
                {
                    "provider": candidate.provider,
                    "confidence": float(candidate.confidence),
                    "parsed": normalized,
                    "score": score,
                }
            )
        ranked.sort(key=lambda item: item["score"], reverse=True)
        return ranked

    def _task_score(
        self,
        *,
        payload: Dict[str, Any],
        baseline: Dict[str, Any],
        confidence: float,
    ) -> float:
        coverage = 0.0
        for key in baseline:
            if self._has_meaningful_value(payload.get(key)):
                coverage += 1.0
        coverage /= max(1.0, float(len(baseline)))
        list_bonus = 0.0
        for key, value in payload.items():
            if isinstance(value, list):
                list_bonus += min(1.0, len(value) / 5.0) * 0.08
        return float(max(0.0, min(1.0, confidence))) + coverage + list_bonus

    def _has_meaningful_value(self, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, dict)):
            return bool(value)
        return True

    def _normalize_output_shape(self, *, task: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(parsed)
        if task == "analyze_exam" and isinstance(data.get("analytics"), dict):
            data = dict(data["analytics"])
        if task in {"class_summary", "student_intelligence"} and isinstance(data.get("data"), dict):
            data = dict(data["data"])
        if task == "student_profile" and isinstance(data.get("profile"), dict):
            data = dict(data["profile"])
        return data

    def _is_valid_task_payload(self, *, task: str, payload: Dict[str, Any]) -> bool:
        if task == "analyze_exam":
            return bool(self._text(payload.get("summary")) and self._list(payload.get("strategy")))
        if task == "student_profile":
            return bool(self._text(payload.get("summary")) and self._list(payload.get("action_plan")))
        if task == "student_intelligence":
            return bool(self._text(payload.get("trend_direction")) and isinstance(payload.get("concept_mastery"), dict))
        if task == "class_summary":
            stats = payload.get("stats")
            return bool(self._text(payload.get("summary")) and isinstance(stats, dict))
        return False

    def _parse_candidate_json(self, text: str) -> Dict[str, Any] | None:
        cleaned = str(text or "").strip()
        if not cleaned:
            return None
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                return dict(parsed)
        except Exception:
            pass
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
            return dict(parsed) if isinstance(parsed, dict) else None
        except Exception:
            return None

    def _merge_payload(self, baseline: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for key in set(baseline.keys()).union(overlay.keys()):
            base_value = baseline.get(key)
            overlay_value = overlay.get(key)
            if isinstance(base_value, dict) and isinstance(overlay_value, dict):
                merged[key] = self._merge_payload(base_value, overlay_value)
                continue
            if self._has_meaningful_value(overlay_value):
                merged[key] = overlay_value
            else:
                merged[key] = base_value
        return merged

    def _normalize_result(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        row = dict(raw or {})
        quiz_title = self._first_text(
            row.get("quiz_title"),
            row.get("quizTitle"),
            row.get("topic"),
            row.get("title"),
            "Assessment",
        )
        score = self._to_float(row.get("score"))
        total = self._to_float(
            row.get("total"),
            self._first_value(row.get("max_score"), row.get("maxScore")),
            100.0,
        )
        total = max(total, 1.0)
        section_accuracy = self._normalize_float_map(
            row.get("section_accuracy") or row.get("sectionAccuracy")
        )
        return {
            "quiz_id": self._first_text(row.get("quiz_id"), row.get("quizId")),
            "quiz_title": quiz_title,
            "topic": quiz_title,
            "subject": self._infer_subject(row),
            "score": score,
            "total": total,
            "max_score": total,
            "correct": self._to_int(row.get("correct")),
            "wrong": self._to_int(row.get("wrong")),
            "skipped": self._to_int(row.get("skipped")),
            "total_time": self._to_int(
                self._first_value(
                    row.get("total_time"),
                    row.get("totalTime"),
                    row.get("time"),
                    row.get("total_time_seconds"),
                )
            ),
            "section_accuracy": section_accuracy,
            "user_answers": self._normalize_map(row.get("user_answers") or row.get("userAnswers")),
            "student_name": self._first_text(
                row.get("student_name"),
                row.get("studentName"),
                row.get("name"),
                row.get("student"),
            ),
            "student_id": self._first_text(
                row.get("student_id"),
                row.get("studentId"),
                row.get("account_id"),
                row.get("accountId"),
                row.get("user_id"),
            ),
            "account_id": self._first_text(
                row.get("account_id"),
                row.get("student_id"),
                row.get("user_id"),
            ),
            "submitted_at": self._first_text(row.get("submitted_at"), row.get("savedAt")),
            "ts": self._to_int(self._first_value(row.get("ts"), row.get("savedAt"))),
            "type": self._first_text(row.get("type"), row.get("quiz_type"), "Exam"),
        }

    def _normalize_history(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized = [
            self._normalize_result(dict(row))
            for row in rows
            if isinstance(row, dict)
        ]
        normalized.sort(key=lambda row: int(row.get("ts", 0) or 0), reverse=True)
        return normalized

    def _normalize_students(self, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            history = self._normalize_history(
                row.get("history") if isinstance(row.get("history"), list) else []
            )
            name = self._first_text(
                row.get("student_name"),
                row.get("name"),
                "Student",
            )
            latest = history[0] if history else {}
            average_pct = (
                sum(self._percent_from_result(item) for item in history) / max(1, len(history))
                if history
                else self._to_float(row.get("average_pct"))
            )
            out.append(
                {
                    "name": name,
                    "student_name": name,
                    "history": history,
                    "attempts": len(history) or self._to_int(row.get("attempts")),
                    "average_pct": average_pct,
                    "latest_topic": self._first_text(
                        row.get("latest_topic"),
                        latest.get("quiz_title"),
                    ),
                }
            )
        return out

    def _baseline_exam_analysis(self, result: Dict[str, Any]) -> Dict[str, Any]:
        pct = self._percent_from_result(result)
        section_accuracy = self._normalize_float_map(result.get("section_accuracy"))
        ranked_sections = sorted(section_accuracy.items(), key=lambda item: item[1], reverse=True)
        strengths = [
            f"{label} accuracy is {value:.1f}%."
            for label, value in ranked_sections[:2]
            if value >= 65.0
        ]
        weaknesses = [
            f"{label} accuracy is only {value:.1f}% and needs focused repair."
            for label, value in ranked_sections[-2:]
            if value < 60.0
        ]
        total_attempts = max(
            1,
            self._to_int(result.get("correct"))
            + self._to_int(result.get("wrong"))
            + self._to_int(result.get("skipped")),
        )
        skip_ratio = self._to_int(result.get("skipped")) / total_attempts
        wrong_ratio = self._to_int(result.get("wrong")) / total_attempts
        strategy = [
            "Review wrong and skipped questions before reattempting.",
            "Convert the weakest section into a short drill block today.",
        ]
        if skip_ratio > 0.22:
            strategy.append("Reduce over-skipping by locking easy and medium questions first.")
        if wrong_ratio > 0.35:
            strategy.append("Slow down slightly on first-pass selection to cut negative marking.")
        next_steps = [
            "Reattempt the toughest 5 mistakes within 24 hours.",
            "Do one timed sectional practice set on the weakest topic.",
        ]
        summary = (
            f"Score is {result['score']:.1f}/{result['total']:.1f} ({pct:.1f}%). "
            "The review should focus on retaining strengths while repairing the weakest scoring area."
        )
        return {
            "summary": summary,
            "strengths": strengths or ["A few core questions were handled correctly under test conditions."],
            "weaknesses": weaknesses or ["The attempt needs a deeper error review to isolate weak areas."],
            "strategy": strategy[:4],
            "next_steps": next_steps[:4],
        }

    def _baseline_student_profile(self, history: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        if not history:
            return {
                "summary": "No attempt history is available yet for this student.",
                "action_plan": ["Collect at least one full assessment attempt before coaching."],
                "weekly_plan": ["Day 1: attempt one diagnostic quiz."],
            }
        attempts = [self._percent_from_result(item) for item in history]
        avg = sum(attempts) / max(1, len(attempts))
        latest = attempts[0]
        momentum = latest - attempts[-1] if len(attempts) >= 2 else 0.0
        weak_topics = self._weak_topics_from_history(history)[:3]
        strong_topics = self._strong_topics_from_history(history)[:2]
        summary = (
            f"The student has {len(history)} tracked attempts with a {avg:.1f}% average. "
            f"The latest result is {latest:.1f}% and momentum is {'improving' if momentum >= 4 else 'mixed' if abs(momentum) < 4 else 'slipping'}."
        )
        action_plan = [
            f"Repair {weak_topics[0]} with examples and timed drills."
            if weak_topics
            else "Run a topic-by-topic error review on the latest attempt.",
            "Use mistake review before starting a new full test.",
            "Check time allocation on the next timed attempt.",
        ]
        weekly_plan = [
            "Day 1: redo the latest wrong questions without notes.",
            f"Day 2-3: focused practice on {weak_topics[0]}." if weak_topics else "Day 2-3: sectional practice on the weakest section.",
            "Day 4: one timed mixed set.",
            "Day 5: brief review and spaced recap.",
        ]
        if strong_topics:
            weekly_plan.append(f"Keep {strong_topics[0]} warm with one short confidence set.")
        return {
            "summary": summary,
            "action_plan": action_plan[:5],
            "weekly_plan": weekly_plan[:5],
        }

    def _baseline_student_intelligence(
        self,
        *,
        account_id: str,
        history: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        attempts = [self._percent_from_result(item) for item in history]
        ema = self._ema(attempts, alpha=0.35)
        trend_delta = attempts[0] - attempts[-1] if len(attempts) >= 2 else 0.0
        trend_direction = "up" if trend_delta >= 4 else "down" if trend_delta <= -4 else "stable"
        consistency = self._consistency(attempts)
        latest = history[0] if history else {}
        total_attempted = max(
            1,
            self._to_int(latest.get("correct"))
            + self._to_int(latest.get("wrong"))
            + self._to_int(latest.get("skipped")),
        )
        wrong_ratio = self._to_int(latest.get("wrong")) / total_attempted
        skipped_ratio = self._to_int(latest.get("skipped")) / total_attempted
        time_usage = self._to_int(latest.get("total_time")) / max(1.0, 60.0 * 90.0)
        difficulty_handling = max(0.0, min(1.0, 1.0 - (wrong_ratio * 1.15) - (skipped_ratio * 0.75)))
        burnout_z = (
            (1.65 * (1 - consistency))
            + (1.2 * time_usage)
            + (1.05 * wrong_ratio)
            + (0.8 if trend_delta < 0 else 0.0)
        )
        burnout_probability = self._sigmoid(burnout_z - 1.45)
        improve_z = (
            (trend_delta / 35.0 if trend_delta > 0 else trend_delta / 60.0)
            + (1.1 * consistency)
            + (0.9 * difficulty_handling)
            - (0.95 * burnout_probability)
        )
        improvement_probability = self._sigmoid(improve_z)
        concept_mastery = self._concept_mastery(history)
        weak_concepts = [
            topic
            for topic, value in sorted(concept_mastery.items(), key=lambda item: item[1])
            if value < 60.0
        ][:5]
        recommendations = [
            f"Prioritize {weak_concepts[0]} with shorter focused drills." if weak_concepts else "Rebuild the lowest-scoring section first.",
            "Use one timed mixed test only after reviewing mistakes.",
            "Track burnout risk by watching skipped-question spikes.",
        ]
        return {
            "account_id": account_id,
            "weak_concepts": weak_concepts,
            "concept_mastery": concept_mastery,
            "performance_trend_ema": round(ema, 6),
            "trend_direction": trend_direction,
            "difficulty_handling_score": round(difficulty_handling, 6),
            "burnout_probability": round(burnout_probability, 6),
            "improvement_probability": round(improvement_probability, 6),
            "consistency_score": round(consistency, 6),
            "recommendations": recommendations[:5],
        }

    def _baseline_class_summary(
        self,
        *,
        students: Sequence[Dict[str, Any]],
        exams: Sequence[Dict[str, Any]],
        homeworks: Sequence[Dict[str, Any]],
        study_materials: Sequence[Dict[str, Any]],
        scheduled_classes: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        averages = [self._to_float(student.get("average_pct")) for student in students]
        total = len(students)
        avg_percent = sum(averages) / max(1, len(averages))
        at_risk = [student for student in students if self._to_float(student.get("average_pct")) < 45.0]
        strong = [student for student in students if self._to_float(student.get("average_pct")) >= 70.0]
        weak_topics = self._class_weak_topics(students)
        summary = (
            "No student attempts are available yet."
            if total == 0
            else f"Class average is {avg_percent:.1f}% across {total} tracked student profiles. "
            f"{len(at_risk)} student(s) are currently at risk and {len(strong)} are performing strongly."
        )
        insights = [
            f"Top weak area across the class is {weak_topics[0]}." if weak_topics else "Need more attempt data to identify a stable weak topic.",
            f"{len(exams)} exam(s) and {len(homeworks)} homework set(s) are currently active.",
            f"{len(study_materials)} Study material item(s) are connected to this loop.",
        ]
        actions = [
            "Run one short concept-check on the weakest class topic.",
            "Move at-risk students into a focused repair cohort for the next cycle.",
            "Attach one targeted Study note or formula sheet to the weakest chapter.",
        ]
        intervention_queue = [
            f"{student.get('student_name') or student.get('name')} needs a direct performance review."
            for student in at_risk[:4]
        ]
        schedule_intel = [
            f"{len(scheduled_classes)} live class slot(s) are already scheduled."
            if scheduled_classes
            else "No live classes are scheduled yet. Add one remediation session.",
        ]
        content_intel = [
            f"Coverage is broad enough for {len(study_materials)} Study item(s)." if study_materials else "Study coverage is light; add revision assets for the weakest topic.",
        ]
        return {
            "summary": summary,
            "insights": [item for item in insights if item],
            "actions": actions,
            "stats": {
                "student_count": total,
                "avg_percent": round(avg_percent, 6),
                "at_risk_count": len(at_risk),
                "strong_count": len(strong),
                "upcoming_classes": len(scheduled_classes),
                "study_material_count": len(study_materials),
                "exam_count": len(exams),
                "homework_count": len(homeworks),
            },
            "intervention_queue": intervention_queue,
            "schedule_intel": schedule_intel,
            "content_intel": content_intel,
        }

    def _exam_prompt(self, result: Dict[str, Any], baseline: Dict[str, Any]) -> str:
        return (
            "You are generating a post-exam AI review for a student.\n"
            "Stay grounded in the supplied attempt data. Do not invent scores, citations, or unseen questions.\n"
            "Return strict JSON only with keys: summary, strengths, weaknesses, strategy, next_steps.\n\n"
            f"Attempt data:\n{json.dumps(result, ensure_ascii=True, indent=2)}\n\n"
            f"Grounded baseline:\n{json.dumps(baseline, ensure_ascii=True, indent=2)}"
        )

    def _student_profile_prompt(
        self,
        history: Sequence[Dict[str, Any]],
        baseline: Dict[str, Any],
    ) -> str:
        return (
            "You are generating a teacher-facing student coaching profile.\n"
            "Stay grounded in the provided attempt history. Do not invent scores, chapters, or attendance data.\n"
            "Return strict JSON only with keys: summary, action_plan, weekly_plan.\n\n"
            f"History:\n{json.dumps(list(history)[:10], ensure_ascii=True, indent=2)}\n\n"
            f"Grounded baseline:\n{json.dumps(baseline, ensure_ascii=True, indent=2)}"
        )

    def _student_intelligence_prompt(
        self,
        *,
        account_id: str,
        history: Sequence[Dict[str, Any]],
        baseline: Dict[str, Any],
    ) -> str:
        return (
            "You are generating student-intelligence telemetry for adaptive coaching.\n"
            "Stay grounded in the supplied attempt history and keep numeric fields realistic.\n"
            "Return strict JSON only with keys: account_id, weak_concepts, concept_mastery, performance_trend_ema, trend_direction, difficulty_handling_score, burnout_probability, improvement_probability, consistency_score, recommendations.\n\n"
            f"Account ID: {account_id}\n"
            f"History:\n{json.dumps(list(history)[:10], ensure_ascii=True, indent=2)}\n\n"
            f"Grounded baseline:\n{json.dumps(baseline, ensure_ascii=True, indent=2)}"
        )

    def _class_summary_prompt(
        self,
        *,
        students: Sequence[Dict[str, Any]],
        exams: Sequence[Dict[str, Any]],
        homeworks: Sequence[Dict[str, Any]],
        study_materials: Sequence[Dict[str, Any]],
        scheduled_classes: Sequence[Dict[str, Any]],
        baseline: Dict[str, Any],
    ) -> str:
        compact_students = [
            {
                "student_name": row.get("student_name") or row.get("name"),
                "attempts": row.get("attempts"),
                "average_pct": row.get("average_pct"),
                "latest_topic": row.get("latest_topic"),
            }
            for row in students[:16]
        ]
        return (
            "You are generating a teacher-side whole-class performance summary.\n"
            "Stay grounded in the supplied student histories and dashboard signals. Do not invent sections, counts, or student names.\n"
            "Return strict JSON only with keys: summary, insights, actions, stats, intervention_queue, schedule_intel, content_intel.\n\n"
            f"Students:\n{json.dumps(compact_students, ensure_ascii=True, indent=2)}\n\n"
            f"Exams: {len(exams)}\n"
            f"Homeworks: {len(homeworks)}\n"
            f"Study materials: {len(study_materials)}\n"
            f"Scheduled classes: {len(scheduled_classes)}\n\n"
            f"Grounded baseline:\n{json.dumps(baseline, ensure_ascii=True, indent=2)}"
        )

    def _build_exam_blocks(
        self,
        result: Dict[str, Any],
        baseline: Dict[str, Any],
    ) -> List[RetrievedBlock]:
        blocks = [
            RetrievedBlock(
                block_id="exam_result",
                title=result.get("quiz_title") or "Assessment",
                text=json.dumps(result, ensure_ascii=True, indent=2)[:5000],
                score=1.35,
                source="exam_result",
                tags=["analytics", "exam"],
            ),
            RetrievedBlock(
                block_id="exam_baseline",
                title="Baseline analytics",
                text=json.dumps(baseline, ensure_ascii=True, indent=2)[:3000],
                score=1.2,
                source="exam_baseline",
                tags=["analytics", "baseline"],
            ),
        ]
        return blocks

    def _build_student_history_blocks(
        self,
        history: Sequence[Dict[str, Any]],
        *,
        title: str,
        baseline: Dict[str, Any],
    ) -> List[RetrievedBlock]:
        blocks = [
            RetrievedBlock(
                block_id="student_history",
                title=title,
                text=json.dumps(list(history)[:12], ensure_ascii=True, indent=2)[:5000],
                score=1.35,
                source="student_history",
                tags=["analytics", "student"],
            ),
            RetrievedBlock(
                block_id="student_baseline",
                title="Student baseline",
                text=json.dumps(baseline, ensure_ascii=True, indent=2)[:3200],
                score=1.18,
                source="student_baseline",
                tags=["analytics", "baseline"],
            ),
        ]
        subject = self._history_subject(history)
        query = f"{subject} student analytics"
        try:
            blocks.extend(self.vault.retrieve(query, subject=subject, top_k=3))
        except Exception:
            pass
        return blocks[:6]

    def _build_class_blocks(
        self,
        *,
        students: Sequence[Dict[str, Any]],
        exams: Sequence[Dict[str, Any]],
        homeworks: Sequence[Dict[str, Any]],
        study_materials: Sequence[Dict[str, Any]],
        scheduled_classes: Sequence[Dict[str, Any]],
        baseline: Dict[str, Any],
    ) -> List[RetrievedBlock]:
        compact_students = [
            {
                "student_name": row.get("student_name") or row.get("name"),
                "attempts": row.get("attempts"),
                "average_pct": row.get("average_pct"),
                "latest_topic": row.get("latest_topic"),
            }
            for row in students[:18]
        ]
        blocks = [
            RetrievedBlock(
                block_id="class_students",
                title="Teacher student cohort",
                text=json.dumps(compact_students, ensure_ascii=True, indent=2)[:4800],
                score=1.35,
                source="class_students",
                tags=["analytics", "class"],
            ),
            RetrievedBlock(
                block_id="class_baseline",
                title="Class baseline",
                text=json.dumps(baseline, ensure_ascii=True, indent=2)[:3200],
                score=1.2,
                source="class_baseline",
                tags=["analytics", "baseline"],
            ),
            RetrievedBlock(
                block_id="class_dashboard",
                title="Class dashboard counts",
                text=json.dumps(
                    {
                        "exam_count": len(exams),
                        "homework_count": len(homeworks),
                        "study_material_count": len(study_materials),
                        "scheduled_class_count": len(scheduled_classes),
                    },
                    ensure_ascii=True,
                    indent=2,
                ),
                score=1.08,
                source="class_dashboard",
                tags=["analytics", "counts"],
            ),
        ]
        return blocks

    def _weak_topics_from_history(self, history: Sequence[Dict[str, Any]]) -> List[str]:
        topic_scores: Dict[str, List[float]] = defaultdict(list)
        for row in history:
            topic = self._text(row.get("quiz_title")) or self._text(row.get("topic"))
            if not topic:
                continue
            topic_scores[topic].append(self._percent_from_result(row))
        ranked = sorted(
            topic_scores.items(),
            key=lambda item: sum(item[1]) / max(1, len(item[1])),
        )
        return [topic for topic, _ in ranked]

    def _strong_topics_from_history(self, history: Sequence[Dict[str, Any]]) -> List[str]:
        topic_scores: Dict[str, List[float]] = defaultdict(list)
        for row in history:
            topic = self._text(row.get("quiz_title")) or self._text(row.get("topic"))
            if not topic:
                continue
            topic_scores[topic].append(self._percent_from_result(row))
        ranked = sorted(
            topic_scores.items(),
            key=lambda item: sum(item[1]) / max(1, len(item[1])),
            reverse=True,
        )
        return [topic for topic, _ in ranked]

    def _class_weak_topics(self, students: Sequence[Dict[str, Any]]) -> List[str]:
        counter: Counter[str] = Counter()
        for student in students:
            history = student.get("history") if isinstance(student.get("history"), list) else []
            for topic in self._weak_topics_from_history(history)[:2]:
                counter[topic] += 1
        return [topic for topic, _ in counter.most_common(6)]

    def _concept_mastery(self, history: Sequence[Dict[str, Any]]) -> Dict[str, float]:
        buckets: Dict[str, List[float]] = defaultdict(list)
        for row in history:
            section_accuracy = self._normalize_float_map(row.get("section_accuracy"))
            for key, value in section_accuracy.items():
                buckets[key].append(value)
        if not buckets:
            return {"General": 50.0}
        return {
            key: round(sum(values) / max(1, len(values)), 6)
            for key, values in buckets.items()
        }

    def _ema(self, values: Sequence[float], *, alpha: float) -> float:
        if not values:
            return 0.0
        ema = float(values[-1])
        for value in reversed(values[:-1]):
            ema = (alpha * float(value)) + ((1.0 - alpha) * ema)
        return ema

    def _consistency(self, values: Sequence[float]) -> float:
        if len(values) <= 1:
            return 1.0
        mean = sum(values) / max(1, len(values))
        variance = sum((value - mean) ** 2 for value in values) / max(1, len(values))
        std = math.sqrt(max(0.0, variance))
        return max(0.0, min(1.0, 1.0 - (std / 40.0)))

    def _sigmoid(self, value: float) -> float:
        try:
            return 1.0 / (1.0 + math.exp(-value))
        except OverflowError:
            return 0.0 if value < 0 else 1.0

    def _percent_from_result(self, row: Dict[str, Any]) -> float:
        score = self._to_float(row.get("score"))
        total = max(
            1.0,
            self._to_float(self._first_value(row.get("total"), row.get("max_score"), row.get("maxScore"), 100.0)),
        )
        return max(0.0, min(100.0, (score / total) * 100.0))

    def _history_subject(self, history: Sequence[Dict[str, Any]]) -> str:
        for row in history:
            subject = self._infer_subject(row)
            if subject != "General":
                return subject
        return "General"

    def _class_subject(self, students: Sequence[Dict[str, Any]]) -> str:
        for row in students:
            subject = self._history_subject(row.get("history") if isinstance(row.get("history"), list) else [])
            if subject != "General":
                return subject
        return "General"

    def _infer_subject(self, row: Dict[str, Any]) -> str:
        title = self._first_text(
            row.get("subject"),
            row.get("quiz_title"),
            row.get("quizTitle"),
            row.get("topic"),
            row.get("title"),
        )
        lowered = title.lower()
        if "physics" in lowered:
            return "Physics"
        if "chemistry" in lowered:
            return "Chemistry"
        if any(token in lowered for token in ("math", "mathematics", "algebra", "calculus", "geometry")):
            return "Mathematics"
        return "General"

    def _provider_diagnostics(self, candidates: Sequence[ProviderAnswer]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for candidate in candidates:
            preview = re.sub(r"\s+", " ", str(candidate.final_answer or "")).strip()
            rows.append(
                {
                    "provider": candidate.provider,
                    "confidence": round(float(candidate.confidence), 6),
                    "latency_s": round(float(candidate.latency_s), 6),
                    "preview": preview[:180],
                }
            )
        return rows

    def _build_citations(self, retrieved: Sequence[RetrievedBlock]) -> List[Dict[str, Any]]:
        return [
            {
                "title": str(block.title),
                "source": str(block.source),
                "score": float(block.score),
                "excerpt": str(block.text)[:220],
            }
            for block in retrieved[:6]
        ]

    def _failure(
        self,
        *,
        status: str,
        message: str,
        task: str,
        baseline: Dict[str, Any],
        retrieved: Sequence[RetrievedBlock],
        candidates: Sequence[ProviderAnswer] | None = None,
    ) -> Dict[str, Any]:
        return {
            "ok": False,
            "status": status,
            "message": message,
            "authoritative": False,
            "fallback_used": False,
            "provider_diagnostics": self._provider_diagnostics(candidates or []),
            "citations": self._build_citations(retrieved),
            "engine": {
                "name": "ANALYTICS_INSIGHT_ENGINE",
                "version": "analytics-v1",
                "task": task,
            },
            "baseline": baseline,
        }

    def _normalize_map(self, value: Any) -> Dict[str, Any]:
        return dict(value) if isinstance(value, dict) else {}

    def _normalize_float_map(self, value: Any) -> Dict[str, float]:
        if not isinstance(value, dict):
            return {}
        out: Dict[str, float] = {}
        for key, raw in value.items():
            token = self._text(key)
            if not token:
                continue
            out[token] = round(self._to_float(raw), 6)
        return out

    def _text(self, value: Any) -> str:
        return str(value or "").strip()

    def _list(self, value: Any) -> List[Any]:
        return list(value) if isinstance(value, list) else []

    def _to_float(self, *values: Any) -> float:
        for value in values:
            if value is None:
                continue
            if isinstance(value, bool):
                return 1.0 if value else 0.0
            if isinstance(value, (int, float)):
                return float(value)
            try:
                return float(str(value).strip())
            except Exception:
                continue
        return 0.0

    def _to_int(self, value: Any) -> int:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        try:
            return int(str(value or "").strip())
        except Exception:
            return 0

    def _first_text(self, *values: Any) -> str:
        for value in values:
            token = self._text(value)
            if token:
                return token
        return ""

    def _first_value(self, *values: Any) -> Any:
        for value in values:
            if value not in (None, ""):
                return value
        return None


_ANALYTICS_ENGINE = AnalyticsInsightEngine()


async def analyze_exam_entry(
    *,
    result: Dict[str, Any],
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return await _ANALYTICS_ENGINE.analyze_exam(result=result, options=options)


async def student_profile_entry(
    *,
    history: Sequence[Dict[str, Any]],
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return await _ANALYTICS_ENGINE.student_profile(history=history, options=options)


async def student_intelligence_entry(
    *,
    account_id: str,
    latest_result: Dict[str, Any],
    history: Sequence[Dict[str, Any]],
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return await _ANALYTICS_ENGINE.student_intelligence(
        account_id=account_id,
        latest_result=latest_result,
        history=history,
        options=options,
    )


async def class_summary_entry(
    *,
    students: Sequence[Dict[str, Any]],
    exams: Sequence[Dict[str, Any]] | None = None,
    homeworks: Sequence[Dict[str, Any]] | None = None,
    study_materials: Sequence[Dict[str, Any]] | None = None,
    scheduled_classes: Sequence[Dict[str, Any]] | None = None,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return await _ANALYTICS_ENGINE.class_summary(
        students=students,
        exams=exams,
        homeworks=homeworks,
        study_materials=study_materials,
        scheduled_classes=scheduled_classes,
        options=options,
    )
