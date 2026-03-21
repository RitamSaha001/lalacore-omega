from __future__ import annotations

from typing import Dict, List

from core.intelligence.syllabus_graph import build_syllabus_hierarchy


class SolverContextBuilder:
    """
    Builds syllabus-aware solver context block for prompt injection.
    """

    def __init__(self, syllabus: Dict[str, Dict[str, Dict]] | None = None):
        self.syllabus = syllabus or build_syllabus_hierarchy()

    def build_context(self, classification: Dict) -> Dict:
        subject = str(classification.get("subject", "General"))
        unit = str(classification.get("unit", "General Unit"))
        subtopic = str(classification.get("subtopic", "General Subtopic"))
        primary = list(classification.get("primary_concepts", []))
        secondary = list(classification.get("secondary_concepts", []))
        structural_patterns = list(classification.get("structural_patterns", []))
        trap_signals = list(classification.get("trap_signals", []))
        reasoning_archetypes = list(classification.get("reasoning_archetypes", []))
        practical_tags = list(classification.get("practical_tags", []))
        hybrid_topics = list(classification.get("hybrid_topics", []))
        estimated_entropy = float(classification.get("estimated_entropy", 0.0))

        unit_spec = self.syllabus.get(subject, {}).get(unit, {})
        prerequisites = list(unit_spec.get("prerequisite_units", []))

        trap_risk = "low"
        if len(trap_signals) >= 3 or float(classification.get("trap_density_score", 0.0)) >= 0.45:
            trap_risk = "high"
        elif len(trap_signals) >= 1 or float(classification.get("trap_density_score", 0.0)) >= 0.20:
            trap_risk = "medium"

        context = {
            "subject": subject,
            "unit": unit,
            "subtopic": subtopic,
            "primary_concepts": primary[:4],
            "secondary_concepts": secondary[:6],
            "structural_patterns": structural_patterns[:6],
            "reasoning_archetypes": reasoning_archetypes[:6],
            "trap_signals": trap_signals[:6],
            "trap_risk_level": trap_risk,
            "prerequisite_units": prerequisites[:6],
            "hybrid_topics": hybrid_topics[:4],
            "practical_tags": practical_tags[:4],
            "estimated_entropy": round(estimated_entropy, 6),
        }
        context["context_block"] = self.render_context_block(context)
        return context

    def render_context_block(self, context: Dict) -> str:
        primary = ", ".join(context.get("primary_concepts", [])) or "N/A"
        secondary = ", ".join(context.get("secondary_concepts", [])) or "N/A"
        structural = ", ".join(context.get("structural_patterns", [])) or "N/A"
        archetypes = ", ".join(context.get("reasoning_archetypes", [])) or "N/A"
        trap = ", ".join(context.get("trap_signals", [])) or "N/A"
        prereq = ", ".join(context.get("prerequisite_units", [])) or "N/A"
        hybrid = ", ".join(context.get("hybrid_topics", [])) or "None"
        practical = ", ".join(context.get("practical_tags", [])) or "N/A"
        entropy = context.get("estimated_entropy", 0.0)

        return (
            "[SYSTEM KNOWLEDGE CONTEXT]\n"
            f"Subject: {context.get('subject', 'General')}\n"
            f"Unit: {context.get('unit', 'General Unit')}\n"
            f"Subtopic: {context.get('subtopic', 'General Subtopic')}\n"
            f"Primary Concepts: {primary}\n"
            f"Secondary Concepts: {secondary}\n"
            f"Structural Pattern: {structural}\n"
            f"Reasoning Archetype: {archetypes}\n"
            f"Trap Warning: {trap}\n"
            f"Trap Risk Level: {context.get('trap_risk_level', 'low')}\n"
            f"Prerequisite Reminder: {prereq}\n"
            f"Hybrid Topic Warning: {hybrid}\n"
            f"Practical Skill Tags: {practical}\n"
            f"Estimated Entropy: {entropy}\n"
            "Instruction: Prioritize structurally consistent reasoning and verify trap-sensitive steps."
        )

    def inject_into_prompt(self, base_prompt: str, context: Dict) -> str:
        block = str(context.get("context_block", "")).strip()
        prompt = str(base_prompt or "").strip()
        if not block:
            return prompt
        if not prompt:
            return block
        return f"{block}\n\n{prompt}"
