from __future__ import annotations

import re
from typing import Any, Dict, List

from core.lalacore_x.providers import ProviderFabric, provider_runtime_budget
from core.lalacore_x.retrieval import ConceptVault
from core.lalacore_x.schemas import ProblemProfile, ProviderAnswer, RetrievedBlock


_MATERIAL_PROMPT_MARKER = "[[LC9_MATERIAL_ENGINE:"


class MaterialGenerationEngine:
    """
    Dedicated long-form study-material engine.

    This intentionally bypasses the normal short-answer solver/verification
    contract so material generation and material-grounded Q&A are not degraded
    by numeric-answer extraction logic.
    """

    def __init__(self) -> None:
        self.providers = ProviderFabric()
        self.vault = ConceptVault()

    async def run(
        self,
        *,
        prompt: str,
        title: str,
        mode: str,
        card: Dict[str, Any] | None = None,
        options: Dict[str, Any] | None = None,
        question: str = "",
    ) -> Dict[str, Any]:
        safe_prompt = str(prompt or "").strip()
        safe_title = str(title or "").strip() or "Study material"
        safe_mode = str(mode or "").strip() or "summarize"
        safe_question = str(question or "").strip()
        merged_card = dict(card or {})
        merged_options = dict(options or {})
        function = str(
            merged_options.get("function")
            or ("material_query" if safe_question else "material_generate")
        ).strip().lower() or "material_generate"

        if not safe_prompt:
            return self._failure(
                status="MATERIAL_PROMPT_MISSING",
                message="Material AI prompt is required.",
                function=function,
                title=safe_title,
                mode=safe_mode,
            )

        subject = str(
            merged_card.get("subject") or merged_card.get("class_name") or "General"
        ).strip() or "General"
        retrieved = self._build_retrieved_blocks(
            title=safe_title,
            mode=safe_mode,
            question=safe_question,
            card=merged_card,
            options=merged_options,
            subject=subject,
        )
        profile = self._build_profile(subject=subject, mode=safe_mode, question=safe_question)
        wrapped_prompt = self._wrap_prompt(
            prompt=safe_prompt,
            function=function,
            mode=safe_mode,
            title=safe_title,
        )

        try:
            await self.providers.ensure_startup_warmup()
        except Exception:
            pass

        selected = self._select_provider_pool()
        if not selected:
            return self._failure(
                status="MATERIAL_PROVIDER_UNAVAILABLE",
                message="No AI providers are currently available for material generation.",
                function=function,
                title=safe_title,
                mode=safe_mode,
                retrieved=retrieved,
            )

        timeout_overrides = (
            merged_options.get("provider_timeout_overrides")
            if isinstance(merged_options.get("provider_timeout_overrides"), dict)
            else None
        )
        try:
            with provider_runtime_budget(timeout_overrides=timeout_overrides):
                candidates = await self.providers.generate_many(
                    selected,
                    wrapped_prompt,
                    profile,
                    retrieved,
                )
        except Exception as exc:
            return self._failure(
                status="MATERIAL_PROVIDER_ERROR",
                message=f"Material AI provider call failed: {exc}",
                function=function,
                title=safe_title,
                mode=safe_mode,
                retrieved=retrieved,
            )

        ranked = self._rank_candidates(candidates=candidates, function=function)
        if not ranked:
            return self._failure(
                status="MATERIAL_ENGINE_EMPTY_OUTPUT",
                message="Material AI did not return a usable study response.",
                function=function,
                title=safe_title,
                mode=safe_mode,
                retrieved=retrieved,
                candidates=candidates,
            )

        best = ranked[0]
        content = self._sanitize_content(str(best.final_answer or "").strip())
        citations = self._build_citations(retrieved)
        diagnostics = self._provider_diagnostics(candidates, function=function)

        return {
            "ok": True,
            "status": "ok",
            "title": safe_title,
            "mode": safe_mode,
            "function": function,
            "content": content,
            "answer": content,
            "final_answer": content,
            "reasoning": str(best.reasoning or "").strip(),
            "explanation": str(best.reasoning or "").strip(),
            "winner_provider": best.provider,
            "confidence": round(float(best.confidence), 6),
            "provider_diagnostics": diagnostics,
            "citations": citations,
            "source_groups": [
                {
                    "title": str(block.title),
                    "source": str(block.source),
                    "score": float(block.score),
                }
                for block in retrieved[:8]
            ],
            "web_retrieval": {
                "enabled": False,
                "query": self._retrieval_query(
                    title=safe_title,
                    mode=safe_mode,
                    question=safe_question,
                    card=merged_card,
                    options=merged_options,
                ),
                "context_injected": bool(retrieved),
                "citations": citations,
                "sources_consulted": [
                    str(block.source)
                    for block in retrieved
                    if str(block.source).strip()
                ],
            },
            "engine": {
                "name": "MATERIAL_GENERATION_ENGINE",
                "version": "material-v1",
                "providers_attempted": list(selected),
                "provider_count": len(selected),
                "backward_compatible": True,
            },
        }

    def _build_profile(self, *, subject: str, mode: str, question: str) -> ProblemProfile:
        lowered_mode = str(mode or "").strip().lower()
        return ProblemProfile(
            subject=str(subject or "general").strip().lower(),
            difficulty="medium" if question else ("hard" if "notes" in lowered_mode else "medium"),
            numeric=False,
            multi_concept=True,
            trap_probability=0.18 if question else 0.22,
            features={
                "material_mode": lowered_mode,
                "task_kind": "query" if question else "generate",
            },
        )

    def _wrap_prompt(
        self,
        *,
        prompt: str,
        function: str,
        mode: str,
        title: str,
    ) -> str:
        normalized_function = function if function in {"material_generate", "material_query"} else "material_generate"
        return (
            f"{_MATERIAL_PROMPT_MARKER}{normalized_function}]]\n"
            f"Material mode: {mode}\n"
            f"Material title: {title}\n\n"
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

    def _build_retrieved_blocks(
        self,
        *,
        title: str,
        mode: str,
        question: str,
        card: Dict[str, Any],
        options: Dict[str, Any],
        subject: str,
    ) -> List[RetrievedBlock]:
        blocks: List[RetrievedBlock] = []
        notes = str(card.get("material_notes") or "").strip()
        chapter = str(card.get("chapter") or "").strip()
        material_type = str(card.get("material_type") or "").strip()
        material_url = str(card.get("material_url") or "").strip()
        query = self._retrieval_query(
            title=title,
            mode=mode,
            question=question,
            card=card,
            options=options,
        )

        if notes:
            blocks.append(
                RetrievedBlock(
                    block_id="material_notes",
                    title=f"{title} notes",
                    text=notes[:5000],
                    score=1.35,
                    source="material_notes",
                    tags=["material", "notes", str(subject).lower()],
                )
            )
        if chapter or material_type or material_url:
            descriptor = "\n".join(
                line
                for line in [
                    f"Title: {title}",
                    f"Chapter: {chapter}" if chapter else "",
                    f"Type: {material_type}" if material_type else "",
                    f"URL: {material_url}" if material_url else "",
                ]
                if line
            ).strip()
            if descriptor:
                blocks.append(
                    RetrievedBlock(
                        block_id="material_descriptor",
                        title=f"{title} descriptor",
                        text=descriptor,
                        score=1.12,
                        source="material_descriptor",
                        tags=["material", "descriptor", str(subject).lower()],
                    )
                )
        try:
            vault_blocks = self.vault.retrieve(query, subject=subject, top_k=4)
        except Exception:
            vault_blocks = []
        blocks.extend(vault_blocks)
        return blocks[:6]

    def _retrieval_query(
        self,
        *,
        title: str,
        mode: str,
        question: str,
        card: Dict[str, Any],
        options: Dict[str, Any],
    ) -> str:
        override = str(options.get("retrieval_query_override") or "").strip()
        if override:
            return override
        subject = str(card.get("subject") or "").strip()
        chapter = str(card.get("chapter") or "").strip()
        notes = str(card.get("material_notes") or "").strip()
        notes_keywords = " ".join(self._keywords(notes, limit=6))
        if question:
            return " ".join(
                chunk
                for chunk in [subject, chapter or title, question, notes_keywords]
                if str(chunk).strip()
            ).strip()
        return " ".join(
            chunk
            for chunk in [subject, chapter or title, mode, notes_keywords]
            if str(chunk).strip()
        ).strip()

    def _keywords(self, text: str, *, limit: int) -> List[str]:
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9_\-]{2,}", str(text or ""))
        stopwords = {
            "the",
            "and",
            "with",
            "that",
            "this",
            "from",
            "using",
            "material",
            "study",
            "title",
            "question",
            "answer",
            "chapter",
            "subject",
        }
        out: List[str] = []
        seen = set()
        for token in tokens:
            lowered = token.lower()
            if lowered in stopwords or lowered in seen:
                continue
            seen.add(lowered)
            out.append(token)
            if len(out) >= limit:
                break
        return out

    def _rank_candidates(
        self,
        *,
        candidates: List[ProviderAnswer],
        function: str,
    ) -> List[ProviderAnswer]:
        usable = [
            candidate
            for candidate in candidates
            if self._is_meaningful_content(candidate.final_answer, function=function)
        ]
        usable.sort(
            key=lambda candidate: self._content_score(candidate.final_answer, candidate.confidence, function=function),
            reverse=True,
        )
        return usable

    def _is_meaningful_content(self, content: str, *, function: str) -> bool:
        text = str(content or "").strip()
        if not text or self._is_placeholder_content(text) or self._is_heading_only(text):
            return False
        compact = re.sub(r"\s+", " ", text).strip()
        min_chars = 40 if function == "material_query" else 120
        return len(compact) >= min_chars

    def _is_placeholder_content(self, text: str) -> bool:
        lowered = re.sub(r"\s+", " ", str(text or "")).strip().lower()
        if not lowered:
            return True
        if lowered in {"[unresolved]", "unresolved", "n/a", "na"}:
            return True
        placeholder_tokens = (
            "uncertain answer:",
            "insufficient evidence",
            "provider error:",
            "actual question is missing",
            "unknown action",
            "engine returned empty output",
        )
        return any(token in lowered for token in placeholder_tokens)

    def _is_heading_only(self, text: str) -> bool:
        lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
        if not lines:
            return True
        if len(lines) == 1 and lines[0].startswith("#"):
            return True
        if len(lines) == 1 and len(lines[0].split()) <= 10:
            return True
        if len(lines) == 2 and lines[0].startswith("#") and len(lines[1].split()) <= 8:
            return True
        return False

    def _content_score(self, text: str, confidence: float, *, function: str) -> float:
        compact = re.sub(r"\s+", " ", str(text or "")).strip()
        section_count = len(re.findall(r"(?m)^(?:#{1,3}\s+|\*\*[^*]+\*\*)", str(text or "")))
        length_score = min(2.2, len(compact) / (260.0 if function == "material_query" else 520.0))
        section_bonus = min(1.2, section_count * 0.18)
        confidence_score = max(0.0, min(1.0, float(confidence)))
        return confidence_score + length_score + section_bonus

    def _provider_diagnostics(
        self,
        candidates: List[ProviderAnswer],
        *,
        function: str,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for candidate in candidates:
            preview = re.sub(r"\s+", " ", str(candidate.final_answer or "")).strip()
            rows.append(
                {
                    "provider": str(candidate.provider),
                    "confidence": round(float(candidate.confidence), 6),
                    "latency_s": round(float(candidate.latency_s), 6),
                    "usable": self._is_meaningful_content(
                        candidate.final_answer,
                        function=function,
                    ),
                    "preview": preview[:180],
                }
            )
        return rows

    def _sanitize_content(self, content: str) -> str:
        text = str(content or "").strip()
        if not text:
            return ""
        lines = text.splitlines()
        cleaned_lines: List[str] = []
        drop_section = False
        for line in lines:
            stripped = line.strip()
            is_heading = bool(re.match(r"^(?:#{1,6}\s+|\*\*)", stripped))
            normalized_heading = re.sub(r"^[#*\s]+", "", stripped).strip(": ").lower()
            if normalized_heading in {
                "supporting source",
                "supporting sources",
                "sources",
                "references",
                "citations",
            }:
                drop_section = True
                continue
            if drop_section and is_heading:
                drop_section = False
            if drop_section:
                continue
            cleaned_lines.append(line)
        text = "\n".join(cleaned_lines).strip()
        text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _build_citations(self, retrieved: List[RetrievedBlock]) -> List[Dict[str, Any]]:
        return [
            {
                "title": str(block.title),
                "source": str(block.source),
                "score": float(block.score),
                "excerpt": str(block.text)[:240],
            }
            for block in retrieved[:6]
        ]

    def _failure(
        self,
        *,
        status: str,
        message: str,
        function: str,
        title: str,
        mode: str,
        retrieved: List[RetrievedBlock] | None = None,
        candidates: List[ProviderAnswer] | None = None,
    ) -> Dict[str, Any]:
        return {
            "ok": False,
            "status": status,
            "message": message,
            "function": function,
            "title": title,
            "mode": mode,
            "provider_diagnostics": self._provider_diagnostics(
                list(candidates or []),
                function=function,
            ),
            "citations": self._build_citations(list(retrieved or [])),
            "engine": {
                "name": "MATERIAL_GENERATION_ENGINE",
                "version": "material-v1",
                "backward_compatible": True,
            },
        }


_MATERIAL_ENGINE = MaterialGenerationEngine()


async def material_generation_entry(
    *,
    prompt: str,
    title: str,
    mode: str,
    card: Dict[str, Any] | None = None,
    options: Dict[str, Any] | None = None,
    question: str = "",
) -> Dict[str, Any]:
    return await _MATERIAL_ENGINE.run(
        prompt=prompt,
        title=title,
        mode=mode,
        card=card,
        options=options,
        question=question,
    )
