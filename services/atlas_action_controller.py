from __future__ import annotations

from typing import Any, Dict, List


class AtlasActionController:
    """
    Post-solve action planner.
    It does not replace existing AI features; it decides when to call them.
    """

    def plan(
        self,
        *,
        question: str,
        concepts: List[str],
        student_profile: Dict[str, Any] | None,
        verification: Dict[str, Any] | None,
        calibration_metrics: Dict[str, Any] | None,
        research_verification: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        student_profile = dict(student_profile or {})
        verification = dict(verification or {})
        calibration_metrics = dict(calibration_metrics or {})
        research_verification = dict(research_verification or {})

        confidence = float(calibration_metrics.get("confidence_score", 0.0) or 0.0)
        risk = float(calibration_metrics.get("risk_score", 1.0) or 1.0)
        verified = bool(verification.get("verified", False))
        weak_concepts = [
            str(item).strip()
            for item in (student_profile.get("weak_concepts") or [])
            if str(item).strip()
        ]
        repeated_doubts = len(
            [
                item
                for item in (student_profile.get("recent_doubt_topics") or [])
                if str(item).strip()
            ]
        )
        issue_count = len(
            [
                item
                for item in (research_verification.get("issues") or [])
                if str(item).strip()
            ]
        )

        reasons: List[str] = []
        if not verified:
            reasons.append("verification_failed")
        if confidence < 0.68:
            reasons.append("low_confidence")
        if risk > 0.34:
            reasons.append("high_risk")
        if repeated_doubts >= 2:
            reasons.append("repeated_doubt_pattern")
        if weak_concepts:
            reasons.append("weak_concept_detected")
        if issue_count >= 2:
            reasons.append("critic_flagged_answer")

        actions: List[Dict[str, Any]] = []
        if reasons:
            actions.append(
                {
                    "action": "simplified_explanation",
                    "priority": "high",
                    "reason": "Answer should be restated in a simpler teaching mode.",
                }
            )
            actions.append(
                {
                    "action": "worked_example",
                    "priority": "high",
                    "reason": "A nearby worked example will reduce doubt recurrence.",
                }
            )
        if weak_concepts or repeated_doubts >= 1:
            actions.append(
                {
                    "action": "flashcards",
                    "priority": "medium",
                    "reason": "Convert fragile concepts into revision prompts.",
                }
            )
            actions.append(
                {
                    "action": "mini_quiz",
                    "priority": "medium",
                    "reason": "Run one concept check on weak or confusing areas.",
                }
            )
        if concepts:
            actions.append(
                {
                    "action": "notes",
                    "priority": "medium",
                    "reason": "Capture the verified explanation as structured study notes.",
                }
            )

        return {
            "triggered": bool(reasons),
            "reason_codes": reasons,
            "recommended_actions": actions[:5],
            "student_profile_used": bool(student_profile),
            "concept_overlap": concepts[:6],
            "confidence": round(confidence, 6),
            "risk_score": round(risk, 6),
            "verified": verified,
            "question_preview": str(question or "")[:220],
        }
