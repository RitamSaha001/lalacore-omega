from __future__ import annotations

from importlib import import_module

__all__ = [
    "InputAnalyzer",
    "QuestionNormalizer",
    "QuestionSearchEngine",
    "SolutionFetcher",
    "RetrievalContextBuilder",
    "MCTSLogger",
    "SearchCacheStore",
    "ReasoningGraphLogger",
]

_EXPORTS = {
    "InputAnalyzer": ("services.input_analyzer", "InputAnalyzer"),
    "QuestionNormalizer": ("services.question_normalizer", "QuestionNormalizer"),
    "QuestionSearchEngine": ("services.question_search_engine", "QuestionSearchEngine"),
    "SolutionFetcher": ("services.solution_fetcher", "SolutionFetcher"),
    "RetrievalContextBuilder": ("services.context_builder", "RetrievalContextBuilder"),
    "MCTSLogger": ("services.mcts_logger", "MCTSLogger"),
    "SearchCacheStore": ("services.search_cache", "SearchCacheStore"),
    "ReasoningGraphLogger": ("services.reasoning_graph_logger", "ReasoningGraphLogger"),
}


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
