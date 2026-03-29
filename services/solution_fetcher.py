from __future__ import annotations

import asyncio
import html
import re
from typing import Any, Dict, List, Sequence

from core.network.resilient_http import request_async


class SolutionFetcher:
    """
    Extracts solution hints/evidence from highly similar web matches.
    """

    async def fetch(
        self,
        search_payload: Dict[str, Any],
        *,
        similarity_threshold: float = 0.75,
        timeout_s: float = 1.1,
        max_tokens: int = 1500,
    ) -> Dict[str, Any]:
        matches = [
            dict(row)
            for row in (search_payload.get("matches") or [])
            if isinstance(row, dict)
            and float(row.get("similarity", 0.0)) >= float(similarity_threshold)
        ]
        if not matches:
            return {
                "solution_text": "",
                "answer": "",
                "hint": "",
                "formulas": [],
                "confidence": 0.0,
                "source_url": "",
                "source": "",
                "consulted": [],
            }

        top = matches[: min(4, len(matches))]
        results = await self._fetch_many(top, timeout_s=timeout_s)
        best = self._pick_best(results, search_payload=search_payload)
        if not best:
            return {
                "solution_text": "",
                "answer": "",
                "hint": "",
                "formulas": [],
                "confidence": 0.0,
                "source_url": "",
                "source": "",
                "consulted": [self._consulted_row(row) for row in top],
            }

        solution_text = self._limit_tokens(str(best.get("solution_text", "")), max_tokens=max_tokens)
        hint = self._limit_tokens(str(best.get("hint", "")), max_tokens=220)
        answer = str(best.get("answer", ""))[:180]
        formulas = [str(x)[:180] for x in (best.get("formulas") or []) if str(x).strip()][:8]
        confidence = float(best.get("confidence", 0.0))
        return {
            "solution_text": solution_text,
            "answer": answer,
            "hint": hint,
            "formulas": formulas,
            "confidence": round(max(0.0, min(0.98, confidence)), 6),
            "source_url": str(best.get("source_url") or ""),
            "source": str(best.get("source") or ""),
            "consulted": [self._consulted_row(row) for row in top],
        }

    async def _fetch_many(self, matches: Sequence[Dict[str, Any]], *, timeout_s: float) -> List[Dict[str, Any]]:
        async def _single(match: Dict[str, Any]) -> Dict[str, Any]:
            url = str(match.get("url") or "").strip()
            fetch_url = str(match.get("fetch_url") or url).strip()
            if not url.startswith(("http://", "https://")):
                return {"ok": False, "source_url": url, "source": str(match.get("source") or "")}
            try:
                response = await request_async(
                    "GET",
                    fetch_url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (LalaCore/1.0; +https://lalacore.local)",
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                    timeout_s=max(0.5, timeout_s),
                    follow_redirects=True,
                )
                raw = response.text or ""
            except Exception:
                return {"ok": False, "source_url": url, "source": str(match.get("source") or "")}

            text = self._clean_html(raw)
            if not text:
                return {"ok": False, "source_url": url, "source": str(match.get("source") or "")}

            answer = self._extract_answer(text)
            hint = self._extract_hint(text)
            solution_text = self._extract_solution(text)
            formulas = self._extract_formulas(raw, text)
            confidence = (
                (0.45 * float(match.get("similarity", 0.0)))
                + (0.18 if bool(answer) else 0.0)
                + (0.16 if bool(hint) else 0.0)
                + (0.18 if bool(solution_text) else 0.0)
            )
            return {
                "ok": True,
                "source_url": url,
                "source": str(match.get("source") or ""),
                "title": str(match.get("title") or ""),
                "snippet": str(match.get("snippet") or ""),
                "query_variant": str(match.get("query_variant") or ""),
                "similarity": float(match.get("similarity", 0.0) or 0.0),
                "answer": answer,
                "hint": hint,
                "solution_text": solution_text,
                "formulas": formulas,
                "confidence": confidence,
            }

        tasks = [asyncio.create_task(_single(match)) for match in matches]
        try:
            rows = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
        out: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                out.append(row)
        return out

    def _pick_best(
        self,
        rows: Sequence[Dict[str, Any]],
        *,
        search_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        usable = [row for row in rows if isinstance(row, dict) and bool(row.get("ok"))]
        if not usable:
            return None
        signals = dict((search_payload or {}).get("query_signals") or {})
        semantic_tokens = self._tokenize(str(signals.get("semantic_query") or ""))
        stem_tokens = self._tokenize(str(signals.get("stem") or signals.get("search_query") or ""))
        equation_tokens = self._equation_tokens(
            str(signals.get("equation_query") or signals.get("math_only_query") or "")
        )
        usable.sort(
            key=lambda row: (
                round(
                    float(row.get("confidence", 0.0))
                    + (0.18 * self._overlap_score(semantic_tokens, self._tokenize(self._row_text(row))))
                    + (0.14 * self._overlap_score(stem_tokens, self._tokenize(self._row_text(row))))
                    + (0.16 * self._equation_overlap_score(equation_tokens, self._row_text(row))),
                    6,
                ),
                1.0 if str(row.get("query_variant", "")).strip().lower() in {"equation", "semantic_equation"} else 0.0,
                1.0 if str(row.get("solution_text", "")).strip() else 0.0,
                1.0 if str(row.get("answer", "")).strip() else 0.0,
            ),
            reverse=True,
        )
        return usable[0]

    def _row_text(self, row: Dict[str, Any]) -> str:
        return " ".join(
            part
            for part in (
                str(row.get("title") or ""),
                str(row.get("snippet") or ""),
                str(row.get("answer") or ""),
                str(row.get("hint") or ""),
                str(row.get("solution_text") or ""),
                " ".join(str(x) for x in (row.get("formulas") or [])[:8]),
            )
            if part
        ).strip()

    def _tokenize(self, text: str) -> set[str]:
        return {
            tok
            for tok in re.findall(r"[a-z0-9_+\-*/=^]+", str(text or "").lower())
            if len(tok) >= 2
        }

    def _equation_tokens(self, text: str) -> set[str]:
        return {
            tok
            for tok in re.findall(r"[a-z0-9^]+", str(text or "").lower())
            if tok not in {"x", "y"} and len(tok) >= 1
        }

    def _overlap_score(self, wanted: set[str], observed: set[str]) -> float:
        if not wanted or not observed:
            return 0.0
        hits = len(wanted.intersection(observed))
        return float(hits / max(1, len(wanted)))

    def _equation_overlap_score(self, wanted: set[str], row_text: str) -> float:
        if not wanted:
            return 0.0
        low = str(row_text or "").lower()
        hits = sum(1 for tok in wanted if tok in low)
        return float(hits / max(1, len(wanted)))

    def _clean_html(self, raw_html: str) -> str:
        text = str(raw_html or "")
        text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
        text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
        text = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:24_000]

    def _extract_solution(self, text: str) -> str:
        low = str(text or "")
        patterns = (
            r"(?is)\b(?:solution|explanation|method)\b[:\-\s]{0,6}(.{80,1300}?)\b(?:final answer|answer[:\-\s]|therefore)\b",
            r"(?is)\b(?:step\s*1|hence|therefore)\b.{80,1300}",
        )
        for pat in patterns:
            m = re.search(pat, low)
            if m:
                chunk = m.group(1) if m.lastindex else m.group(0)
                return re.sub(r"\s+", " ", str(chunk)).strip()
        sentences = re.split(r"(?<=[.!?])\s+", low)
        take = [s.strip() for s in sentences if 35 <= len(s.strip()) <= 300][:5]
        return " ".join(take)

    def _extract_hint(self, text: str) -> str:
        low = str(text or "")
        patterns = (
            r"(?is)\bhint\b[:\-\s]{0,6}(.{20,420})",
            r"(?is)\b(use|consider|observe|start with)\b.{20,220}",
        )
        for pat in patterns:
            m = re.search(pat, low)
            if m:
                chunk = m.group(1) if m.lastindex else m.group(0)
                chunk = re.sub(r"\s+", " ", str(chunk)).strip()
                return chunk[:420]
        return ""

    def _extract_answer(self, text: str) -> str:
        low = str(text or "")
        patterns = (
            r"(?is)\b(?:final answer|answer|correct option)\b\s*[:=\-]\s*([A-D]|\-?\d+(?:\.\d+)?(?:/\d+)?)\b",
            r"(?is)\boption\s*([A-D])\s*(?:is\s*)?correct\b",
            r"(?is)\btherefore\b.{0,100}\b(?:is|=)\s*([A-D]|\-?\d+(?:\.\d+)?(?:/\d+)?)\b",
        )
        for pat in patterns:
            m = re.search(pat, low)
            if m:
                return str(m.group(1)).strip()
        return ""

    def _extract_formulas(self, raw_html: str, clean_text: str) -> List[str]:
        formulas: List[str] = []
        for block in re.findall(r"(?is)\$([^$]{2,180})\$", str(raw_html or ""))[:10]:
            token = re.sub(r"\s+", " ", block).strip()
            if token and token not in formulas:
                formulas.append(token)
        for block in re.findall(r"\\(?:frac|sqrt|int|sum|sin|cos|tan|log)\b[^\s]{0,80}", str(raw_html or ""))[:10]:
            token = str(block).strip()
            if token and token not in formulas:
                formulas.append(token)
        if not formulas:
            for chunk in re.findall(r"\b[a-zA-Z0-9]+\s*=\s*[^.;]{1,80}", str(clean_text or ""))[:6]:
                token = re.sub(r"\s+", " ", chunk).strip()
                if token and token not in formulas:
                    formulas.append(token)
        return formulas[:8]

    def _limit_tokens(self, text: str, *, max_tokens: int) -> str:
        words = [w for w in str(text or "").split() if w]
        if len(words) <= int(max_tokens):
            return " ".join(words)
        return " ".join(words[: int(max_tokens)])

    def _consulted_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "url": str(row.get("url") or ""),
            "title": str(row.get("title") or "")[:220],
            "source": str(row.get("source") or ""),
            "similarity": float(row.get("similarity", 0.0) or 0.0),
        }
