from __future__ import annotations

from typing import Dict

from core.intelligence.advanced_classifier import AdvancedSyllabusClassifier
from core.intelligence.solver_context_builder import SolverContextBuilder


class SolverContextInjector:
    """
    Plug-in context injector for solver prompts.
    Additive module: does not alter core solve pipeline unless explicitly used.
    """

    def __init__(
        self,
        classifier: AdvancedSyllabusClassifier | None = None,
        context_builder: SolverContextBuilder | None = None,
    ):
        self.classifier = classifier or AdvancedSyllabusClassifier()
        self.context_builder = context_builder or SolverContextBuilder(syllabus=self.classifier.syllabus)

    def inject(
        self,
        *,
        question: str,
        base_prompt: str,
        source_tag: str = "solver_context_injector",
        bfs_depth: int = 2,
    ) -> Dict:
        classification = self.classifier.classify_question(
            question,
            source_tag=source_tag,
            bfs_depth=bfs_depth,
        )
        context = self.context_builder.build_context(classification)
        prompt = self.context_builder.inject_into_prompt(base_prompt, context)
        return {
            "prompt": prompt,
            "classification": classification,
            "context": context,
        }

    def inject_with_classification(self, *, base_prompt: str, classification: Dict) -> Dict:
        context = self.context_builder.build_context(classification)
        prompt = self.context_builder.inject_into_prompt(base_prompt, context)
        return {
            "prompt": prompt,
            "classification": classification,
            "context": context,
        }
