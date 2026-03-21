from __future__ import annotations

import ast
import asyncio
import json
from typing import Any, Dict, List, Sequence

from core.automation.adaptive_question_classifier import AdaptiveQuestionClassifier
from core.automation.feeder_engine import FeederEngine


class RawQuestionIntakeSystem:
    """
    Additive raw-question intake layer.

    - Accepts raw questions (strings or dict rows)
    - Applies adaptive classification
    - Enqueues into existing feeder queue
    - Optionally processes queue through existing feeder pipeline
    """

    def __init__(
        self,
        *,
        feeder: FeederEngine | None = None,
        classifier: AdaptiveQuestionClassifier | None = None,
    ):
        self.feeder = feeder or FeederEngine()
        self.classifier = classifier or AdaptiveQuestionClassifier()

    def classify_raw_questions(
        self,
        raw_questions: Sequence[Any],
        *,
        default_source_tag: str = "raw_auto",
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for raw in raw_questions:
            if isinstance(raw, str):
                question = raw.strip()
                if not question:
                    continue
                payload = self._classify_to_payload(question=question, source_tag=default_source_tag)
                rows.append(payload)
                continue

            if isinstance(raw, dict):
                question = str(raw.get("question", "")).strip()
                if not question:
                    continue

                if raw.get("subject") and raw.get("difficulty") and raw.get("concept_cluster") is not None:
                    row = {
                        "question": question,
                        "subject": str(raw.get("subject", "general")).strip().lower() or "general",
                        "difficulty": str(raw.get("difficulty", "unknown")).strip().lower() or "unknown",
                        "concept_cluster": self._normalize_clusters(raw.get("concept_cluster")),
                        "source_tag": str(raw.get("source_tag", default_source_tag)),
                        "_classification": {"mode": "provided"},
                    }
                    rows.append(row)
                    continue

                outcome = self.classifier.classify(question)
                payload = self._classify_to_payload(
                    question=question,
                    source_tag=str(raw.get("source_tag", default_source_tag)),
                )
                if raw.get("subject"):
                    payload["subject"] = str(raw.get("subject")).strip().lower() or payload["subject"]
                if raw.get("difficulty"):
                    payload["difficulty"] = str(raw.get("difficulty")).strip().lower() or payload["difficulty"]
                if raw.get("concept_cluster") is not None:
                    payload["concept_cluster"] = self._normalize_clusters(raw.get("concept_cluster"))
                rows.append(payload)
        return rows

    def _classify_to_payload(self, *, question: str, source_tag: str) -> Dict[str, Any]:
        # AdaptiveQuestionClassifier returns dataclass-like output.
        if hasattr(self.classifier, "classify"):
            result = self.classifier.classify(question)
            if hasattr(result, "to_feeder_payload"):
                payload = result.to_feeder_payload(source_tag=source_tag)
                payload["_classification"] = {
                    "confidence": getattr(result, "confidence", 0.5),
                    "metadata": getattr(result, "metadata", {}),
                }
                return payload

        # AdvancedSyllabusClassifier (or compatible) exposes classify_question.
        if hasattr(self.classifier, "classify_question"):
            result = self.classifier.classify_question(question, source_tag=source_tag)
            if hasattr(self.classifier, "to_feeder_payload"):
                payload = self.classifier.to_feeder_payload(result)
            else:
                payload = {
                    "question": str(result.get("question", question)),
                    "subject": str(result.get("subject", "general")).strip().lower() or "general",
                    "difficulty": str(result.get("difficulty", "unknown")).strip().lower() or "unknown",
                    "concept_cluster": list(result.get("concept_cluster", [])),
                    "source_tag": str(result.get("source_tag", source_tag)),
                }
            confidence = result.get("confidence")
            if confidence is None:
                entropy = float(result.get("estimated_entropy", 0.5))
                confidence = max(0.05, min(0.99, 1.0 - entropy))
            payload["_classification"] = {
                "confidence": float(confidence),
                "metadata": {
                    "difficulty_score": result.get("difficulty_score"),
                    "estimated_entropy": result.get("estimated_entropy"),
                    "unit": result.get("unit"),
                    "subtopic": result.get("subtopic"),
                    "structural_patterns": result.get("structural_patterns", []),
                    "trap_signals": result.get("trap_signals", []),
                },
            }
            return payload

        raise TypeError("classifier must expose classify(...) or classify_question(...)")

    def enqueue_classified(self, classified_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        added = 0
        duplicate = 0
        errors = 0
        queue_items = []
        entries = []

        for row in classified_rows:
            try:
                result = self.feeder.enqueue_question(
                    question=str(row.get("question", "")),
                    subject=str(row.get("subject", "general")),
                    difficulty=str(row.get("difficulty", "unknown")),
                    concept_cluster=list(row.get("concept_cluster", [])),
                    source_tag=str(row.get("source_tag", "raw_auto")),
                )
            except Exception as exc:
                errors += 1
                entries.append(
                    {
                        "question": str(row.get("question", ""))[:200],
                        "status": "error",
                        "error": str(exc),
                        "classification": row.get("_classification", {}),
                    }
                )
                continue

            if bool(result.get("added")):
                added += 1
            if bool(result.get("duplicate")):
                duplicate += 1

            queue_item = result.get("queue_item", {})
            if isinstance(queue_item, dict):
                queue_items.append(queue_item)
            entries.append(
                {
                    "question": str(row.get("question", ""))[:200],
                    "status": "added" if bool(result.get("added")) else "duplicate",
                    "queue_item": queue_item,
                    "classification": row.get("_classification", {}),
                }
            )

        return {
            "requested": len(classified_rows),
            "added": added,
            "duplicate": duplicate,
            "errors": errors,
            "entries": entries,
            "queue_items": queue_items,
        }

    async def ingest(
        self,
        raw_questions: Sequence[Any],
        *,
        default_source_tag: str = "raw_auto",
        process: bool = False,
        max_items: int = 20,
        trigger: str = "raw_intake",
    ) -> Dict[str, Any]:
        classified = self.classify_raw_questions(raw_questions, default_source_tag=default_source_tag)
        enqueue = self.enqueue_classified(classified)

        out: Dict[str, Any] = {
            "classified_count": len(classified),
            "enqueue": enqueue,
            "status": self.feeder.status(limit=20),
        }

        if process:
            processed = await self.feeder.process_pending(max_items=max(1, int(max_items)), trigger=str(trigger))
            out["process"] = processed
            out["status"] = self.feeder.status(limit=20)
        return out

    def status(self, *, limit: int = 20) -> Dict[str, Any]:
        return self.feeder.status(limit=max(1, int(limit)))

    @staticmethod
    def parse_raw_python_literal(raw_python: str) -> List[Any]:
        obj = ast.literal_eval(str(raw_python))
        if not isinstance(obj, list):
            raise ValueError("raw input must evaluate to a list")
        return list(obj)

    @staticmethod
    def parse_raw_json(raw_json: str) -> List[Any]:
        obj = json.loads(str(raw_json))
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict) and isinstance(obj.get("raw_questions"), list):
            return list(obj["raw_questions"])
        raise ValueError("raw JSON must be a list or object with 'raw_questions'")

    @staticmethod
    def _normalize_clusters(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            out = []
            for item in value:
                text = str(item or "").strip().lower()
                if text and text not in out:
                    out.append(text)
            return out

        text = str(value or "").strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                return RawQuestionIntakeSystem._normalize_clusters(parsed)
            except Exception:
                pass
        out = []
        for chunk in text.split(","):
            token = chunk.strip().lower()
            if token and token not in out:
                out.append(token)
        return out


def ingest_sync(
    raw_questions: Sequence[Any],
    *,
    default_source_tag: str = "raw_auto",
    process: bool = False,
    max_items: int = 20,
    trigger: str = "raw_intake",
) -> Dict[str, Any]:
    system = RawQuestionIntakeSystem()
    return asyncio.run(
        system.ingest(
            raw_questions,
            default_source_tag=default_source_tag,
            process=process,
            max_items=max_items,
            trigger=trigger,
        )
    )
