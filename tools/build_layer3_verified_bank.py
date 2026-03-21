#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from urllib.request import Request, urlopen


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _norm_text_for_sim(raw: str) -> str:
    out = _to_str(raw).lower()
    out = re.sub(r"\s+", " ", out)
    return out.strip()


def _char_trigram_cosine(a: str, b: str) -> float:
    def vec(text: str) -> dict[str, int]:
        t = re.sub(r"[^a-z0-9]+", "", text.lower())
        if len(t) < 3:
            return {}
        out: dict[str, int] = {}
        for i in range(len(t) - 2):
            tri = t[i : i + 3]
            out[tri] = out.get(tri, 0) + 1
        return out

    va = vec(a)
    vb = vec(b)
    if not va or not vb:
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for k, v in va.items():
        na += float(v * v)
        dot += float(v * vb.get(k, 0))
    for v in vb.values():
        nb += float(v * v)
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return float(dot / ((na ** 0.5) * (nb ** 0.5)))


def _normalize_option_list(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if isinstance(raw, list):
        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                label = _to_str(item.get("label")).upper() or chr(65 + min(idx, 25))
                text = _to_str(item.get("text") or item.get("value") or item.get("option")).strip()
            else:
                label = chr(65 + min(idx, 25))
                text = _to_str(item).strip()
            if text:
                out.append({"label": label, "text": text})
    elif isinstance(raw, dict):
        items = sorted(raw.items(), key=lambda kv: _to_str(kv[0]).upper())
        for idx, (_, value) in enumerate(items):
            text = _to_str(value).strip()
            if text:
                out.append({"label": chr(65 + min(idx, 25)), "text": text})
    return out


def _normalize_correct_answer(row: dict[str, Any]) -> dict[str, Any]:
    ans = row.get("correct_answer")
    if isinstance(ans, dict):
        multiple = ans.get("multiple")
        if not isinstance(multiple, list):
            multiple = []
        return {
            "single": (_to_str(ans.get("single")).upper() or None),
            "multiple": [_to_str(x).upper() for x in multiple if _to_str(x).strip()],
            "numerical": (_to_str(ans.get("numerical")).strip() or None),
            "tolerance": ans.get("tolerance"),
        }
    return {
        "single": (_to_str(row.get("_correct_option") or row.get("correct_option")).upper() or None),
        "multiple": [],
        "numerical": (_to_str(row.get("_numerical_answer") or row.get("numerical_answer")).strip() or None),
        "tolerance": row.get("numerical_tolerance") or row.get("tolerance"),
    }


def _looks_heavily_corrupted(text: str) -> bool:
    low = _to_str(text).lower()
    if not low.strip():
        return True
    corruption_tokens = (
        "xxfx",
        "()'rf",
        "()'lf",
        " +<== +>",
        " cd0",
        "www.allen.in",
        "answer key",
        "exercise",
    )
    if any(tok in low for tok in corruption_tokens):
        return True
    if re.search(r"\b\d+\s+\d+\s+for\s+\d+", low):
        return True
    if re.search(r"[=+\-*/^]\s*$", low):
        return True
    return False


def _contextual_piecewise_repair(text: str) -> tuple[str, list[str]]:
    out = _to_str(text)
    actions: list[str] = []
    # Typical OCR-degraded piecewise fragment:
    # "22 3 for 1() 3 2 for 1 xxfx xx +<== +>"
    pat = re.compile(
        r"(?is)\b(\d+)\s*(?:x\^?2|2)\s*([+\-]\s*\d+)?\s*for\s*x?\s*<=?\s*([-\d.]+)\s*"
        r"(?:[,;:]|\s)+\s*(\d+)\s*x\s*([+\-]\s*\d+)?\s*for\s*x?\s*>\s*([-\d.]+)"
    )
    m = pat.search(out)
    if m:
        a = m.group(1)
        b = (m.group(2) or "").replace(" ", "")
        c = m.group(3)
        d = m.group(4)
        e = (m.group(5) or "").replace(" ", "")
        f = m.group(6)
        if c == f:
            fx = f"f(x) = {{ {a}x^2{b}, x<={c} ; {d}x{e}, x>{f} }}"
            out = pat.sub(fx, out)
            actions.append("piecewise_reconstructed_contextual")
    out2 = re.sub(r"\bCD\d{2,4}[-_]\d+\b", "", out, flags=re.IGNORECASE).strip()
    if out2 != out:
        actions.append("noise_removed")
        out = out2
    return out, actions


def _score_integrity(
    *,
    repair_confidence: float,
    issues: list[str],
    question_text: str,
    options: list[dict[str, str]],
) -> float:
    score = max(0.0, min(1.0, float(repair_confidence)))
    penalties = {
        "empty_question": 0.35,
        "unbalanced_brackets": 0.22,
        "dangling_operator": 0.16,
        "expression_parse_failure": 0.14,
        "missing_options": 0.12,
        "answer_mismatch": 0.18,
        "invalid_token_sequences": 0.10,
    }
    for issue in issues:
        score -= penalties.get(issue, 0.03)
    if len(_to_str(question_text)) < 16:
        score -= 0.25
    if options and len(options) < 2:
        score -= 0.12
    if _looks_heavily_corrupted(question_text):
        score -= 0.20
    return round(max(0.0, min(1.0, score)), 4)


def _status_from_integrity(integrity: float) -> str:
    if integrity >= 0.9:
        return "safe"
    if integrity >= 0.75:
        return "review"
    if integrity >= 0.5:
        return "reject"
    return "unrecoverable"


@dataclass
class WebCandidate:
    url: str
    title: str
    snippet: str
    text: str


class WebVerifier:
    def __init__(self, *, enabled: bool, max_rows: int = 3, timeout_s: float = 7.0) -> None:
        self.enabled = bool(enabled)
        self.max_rows = max(1, int(max_rows))
        self.timeout_s = max(3.0, float(timeout_s))

    def verify(self, *, question_text: str, chapter: str, subject: str) -> dict[str, Any]:
        if not self.enabled:
            return {
                "enabled": False,
                "used": False,
                "reason": "disabled",
                "matches": [],
                "best_similarity": 0.0,
            }
        query = self._build_query(question_text=question_text, chapter=chapter, subject=subject)
        rows = self._search_rows(query)
        candidates: list[WebCandidate] = []
        for row in rows[: self.max_rows]:
            page = self._fetch_text(row.get("url", ""))
            if not page:
                continue
            candidates.append(
                WebCandidate(
                    url=_to_str(row.get("url")),
                    title=_to_str(row.get("title")),
                    snippet=_to_str(row.get("snippet")),
                    text=page,
                )
            )
        matches: list[dict[str, Any]] = []
        best_similarity = 0.0
        best_question = ""
        for cand in candidates:
            extracted = self._extract_question_like_text(cand.text)
            sim = _char_trigram_cosine(_norm_text_for_sim(question_text), _norm_text_for_sim(extracted))
            if sim > best_similarity:
                best_similarity = sim
                best_question = extracted
            matches.append(
                {
                    "url": cand.url,
                    "title": cand.title[:220],
                    "similarity": round(sim, 4),
                    "question_excerpt": extracted[:320],
                }
            )
        return {
            "enabled": True,
            "used": True,
            "query": query,
            "matches": matches,
            "best_similarity": round(best_similarity, 4),
            "best_question_text": best_question[:1200],
        }

    def _build_query(self, *, question_text: str, chapter: str, subject: str) -> str:
        stem = re.sub(r"\s+", " ", _to_str(question_text)).strip()
        stem = re.sub(r"[^A-Za-z0-9()^+\-*/=<>| .,]", " ", stem)
        tokens = [tok for tok in stem.split(" ") if tok]
        core = " ".join(tokens[:14])
        chapter_part = _to_str(chapter).strip()
        subject_part = _to_str(subject).strip()
        suffix = "JEE PYQ"
        domain_hint = "site:mathongo.com OR site:questions.examside.com OR site:jeeadv.ac.in OR site:vedantu.com"
        return f"{core} {chapter_part} {subject_part} {suffix} {domain_hint}".strip()

    def _search_rows(self, query: str) -> list[dict[str, str]]:
        q = quote_plus(query)
        url = f"https://www.bing.com/search?format=rss&q={q}&setlang=en-US"
        raw = self._fetch_text(url)
        rows: list[dict[str, str]] = []
        if not raw:
            return rows
        items = re.findall(r"(?is)<item>(.*?)</item>", raw)
        seen: set[str] = set()
        for item in items:
            link_match = re.search(r"(?is)<link>(.*?)</link>", item)
            title_match = re.search(r"(?is)<title>(.*?)</title>", item)
            desc_match = re.search(r"(?is)<description>(.*?)</description>", item)
            link = html.unescape(_to_str(link_match.group(1) if link_match else "")).strip()
            if not link or not link.startswith(("http://", "https://")):
                continue
            domain = re.sub(r"^www\.", "", link.split("/")[2].lower()) if "://" in link else ""
            if domain not in {
                "mathongo.com",
                "questions.examside.com",
                "jeeadv.ac.in",
                "vedantu.com",
                "testbook.com",
            }:
                continue
            key = link.lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "url": link,
                    "title": html.unescape(_to_str(title_match.group(1) if title_match else "")),
                    "snippet": html.unescape(_to_str(desc_match.group(1) if desc_match else "")),
                }
            )
            if len(rows) >= max(1, self.max_rows):
                break
        return rows

    def _fetch_text(self, url: str, *, max_bytes: int = 280_000) -> str:
        u = _to_str(url).strip()
        if not u:
            return ""
        req = Request(
            u,
            headers={
                "User-Agent": "Mozilla/5.0 (LalaCore-Layer3/1.0)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        try:
            with urlopen(req, timeout=self.timeout_s) as resp:  # nosec - controlled public fetch
                raw = resp.read(max(4096, max_bytes)).decode("utf-8", errors="ignore")
                return raw
        except Exception:
            pass
        # DNS/HTTP fallback through jina proxy.
        try:
            proxy = f"https://r.jina.ai/http://{u.replace('https://', '').replace('http://', '')}"
            req2 = Request(proxy, headers={"User-Agent": "Mozilla/5.0 (LalaCore-Layer3/1.0)"})
            with urlopen(req2, timeout=max(self.timeout_s, 10.0)) as resp:  # nosec - controlled public fetch
                return resp.read(max(4096, max_bytes)).decode("utf-8", errors="ignore")
        except Exception:
            return ""

    def _extract_question_like_text(self, raw: str) -> str:
        text = _to_str(raw)
        if not text:
            return ""
        # Prefer question-like blocks from known sources.
        pats = (
            r"(?is)question\s*[:\-]\s*(.{40,900}?)(?:options?\s*[:\-]|answer\s*[:\-]|solution\s*[:\-]|$)",
            r"(?is)\bq(?:uestion)?\s*no\.?\s*\d+\s*(.{40,900}?)(?:\(\s*[A-D1-4]\s*\)|answer|solution|$)",
            r"(?is)problem\s*[:\-]\s*(.{40,900}?)(?:options?\s*[:\-]|answer|solution|$)",
        )
        for pat in pats:
            m = re.search(pat, text)
            if m:
                return re.sub(r"\s+", " ", m.group(1)).strip()
        # Fallback: compact start block.
        compact = re.sub(r"(?is)<script.*?</script>", " ", text)
        compact = re.sub(r"(?is)<style.*?</style>", " ", compact)
        compact = re.sub(r"(?is)<[^>]+>", " ", compact)
        compact = html.unescape(compact)
        compact = re.sub(r"\s+", " ", compact).strip()
        return compact[:900]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Layer-3 research-grade contextual fixer + optional web re-verification for doubtful rows"
    )
    parser.add_argument(
        "--input",
        default="data/app/import_question_bank_layer2_finalized.live.json",
        help="Layer-2 finalized input JSON list",
    )
    parser.add_argument(
        "--output",
        default="data/app/import_question_bank_layer3_verified.live.json",
        help="Layer-3 verified full output JSON list",
    )
    parser.add_argument(
        "--best-output",
        default="data/app/import_question_bank_layer3_best.live.json",
        help="Layer-3 best/safe output JSON list",
    )
    parser.add_argument(
        "--report",
        default="data/app/repair_report_layer3.live.json",
        help="Layer-3 run report JSON",
    )
    parser.add_argument(
        "--snapshot",
        default="data/app/import_question_bank.layer3_snapshot.live.json",
        help="Layer-3 snapshot input copy",
    )
    parser.add_argument(
        "--low-confidence-threshold",
        type=float,
        default=0.85,
        help="Rows below this repair_confidence are marked doubtful and deeply rechecked",
    )
    parser.add_argument(
        "--integrity-safe-threshold",
        type=float,
        default=0.90,
        help="Rows above this structural_integrity_score go to best output",
    )
    parser.add_argument(
        "--enable-web-verify",
        action="store_true",
        help="Enable web re-verification for doubtful rows",
    )
    parser.add_argument(
        "--web-max-rows",
        type=int,
        default=2,
        help="Max web candidates per doubtful row",
    )
    parser.add_argument(
        "--web-min-similarity-upgrade",
        type=float,
        default=0.84,
        help="Apply web question text only when similarity exceeds this threshold",
    )
    parser.add_argument(
        "--web-timeout-s",
        type=float,
        default=2.8,
        help="Per-request timeout for web re-verification fetches",
    )
    parser.add_argument(
        "--web-integrity-threshold",
        type=float,
        default=0.72,
        help="Run web verification only for doubtful rows at or below this integrity baseline",
    )
    parser.add_argument(
        "--web-verify-cap",
        type=int,
        default=900,
        help="Maximum doubtful rows to send through web re-verification (0 = unlimited)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=250,
        help="Emit progress log every N rows",
    )
    parser.add_argument(
        "--progress-file",
        default="data/app/repair_report_layer3.progress.live.json",
        help="Live progress JSON path updated periodically",
    )
    parser.add_argument(
        "--fast-safe-confidence",
        type=float,
        default=0.95,
        help="Pass through rows already marked safe above this confidence without deep repair",
    )
    parser.add_argument(
        "--fast-safe-integrity",
        type=float,
        default=0.92,
        help="Pass through rows already marked safe above this integrity without deep repair",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    sys.path.insert(0, str((root / "app").resolve()))

    from app.data.repair_engine.math_repair_engine import MathRepairEngine

    input_path = (root / args.input).resolve()
    output_path = (root / args.output).resolve()
    best_path = (root / args.best_output).resolve()
    report_path = (root / args.report).resolve()
    snapshot_path = (root / args.snapshot).resolve()
    progress_path = (root / args.progress_file).resolve()

    if not input_path.exists():
        raise SystemExit(f"missing input file: {input_path}")

    rows = _load_json(input_path)
    if not isinstance(rows, list):
        raise SystemExit(f"input is not a list: {input_path}")

    _atomic_write_json(snapshot_path, rows)

    started_at = _now_iso()
    total = len(rows)
    repair_engine = MathRepairEngine()
    web_verifier = WebVerifier(
        enabled=bool(args.enable_web_verify),
        max_rows=max(1, int(args.web_max_rows)),
        timeout_s=float(args.web_timeout_s),
    )

    corpus = [
        {
            "question_id": _to_str(r.get("question_id") or r.get("id")),
            "question_text": _to_str(r.get("question_text")),
        }
        for r in rows
        if isinstance(r, dict)
        and _to_str(r.get("repair_status")) == "safe"
        and float(r.get("repair_confidence") or 0.0) >= 0.9
    ]

    finalized: list[dict[str, Any]] = []
    best: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    issue_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    doubtful_count = 0
    web_checked_count = 0
    web_upgraded_count = 0
    fast_passthrough_count = 0
    confidence_sum = 0.0
    integrity_sum = 0.0
    risk_sum = 0.0
    t0 = time.time()

    _atomic_write_json(
        progress_path,
        {
            "stage": "layer3_verify",
            "started_at": started_at,
            "status": "running",
            "done": 0,
            "total": total,
            "progress_pct": 0.0,
            "safe_count": 0,
            "doubtful_count": 0,
            "web_checked_count": 0,
            "web_upgraded_count": 0,
            "fast_passthrough_count": 0,
        },
    )

    for idx, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            continue

        row = dict(raw)
        qid = _to_str(row.get("question_id") or row.get("id"))
        qtext = _to_str(row.get("question_text"))
        options = _normalize_option_list(row.get("options"))
        correct_answer = _normalize_correct_answer(row)
        qtype = _to_str(row.get("type") or row.get("question_type"))
        chapter = _to_str(row.get("chapter"))
        subject = _to_str(row.get("subject"))
        base_conf = float(row.get("repair_confidence") or 0.0)
        base_status = _to_str(row.get("repair_status"))
        base_integrity = float(row.get("structural_integrity_score") or 0.0)
        base_issues = [_to_str(x).strip() for x in (row.get("detected_issues") or []) if _to_str(x).strip()]

        fast_passthrough = (
            base_status == "safe"
            and base_conf >= float(args.fast_safe_confidence)
            and base_integrity >= float(args.fast_safe_integrity)
            and not base_issues
            and not _looks_heavily_corrupted(qtext)
            and (qtype == "NUMERICAL" or len(options) >= 2)
        )
        if fast_passthrough:
            fast_passthrough_count += 1
            row["layer3_verification"] = {
                "doubtful_row": False,
                "web_reverify": {
                    "enabled": bool(args.enable_web_verify),
                    "used": False,
                    "reason": "fast_safe_passthrough",
                },
                "stage": "layer3_contextual_repair",
                "fast_passthrough": True,
                "processed_at": _now_iso(),
            }
            finalized.append(row)
            if float(row.get("structural_integrity_score") or 0.0) >= float(args.integrity_safe_threshold):
                best.append(row)
            status_counts[_to_str(row.get("repair_status")) or "safe"] += 1
            confidence_sum += float(row.get("repair_confidence") or 0.0)
            integrity_sum += float(row.get("structural_integrity_score") or 0.0)
            risk_sum += float(row.get("publish_risk_score") or 0.0)
            for tok in base_issues:
                issue_counts[tok] += 1
            for tok in [_to_str(x).strip() for x in (row.get("repair_actions") or []) if _to_str(x).strip()]:
                action_counts[tok] += 1
            if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == total):
                elapsed = max(0.001, time.time() - t0)
                pct = (idx / max(1, total)) * 100.0
                rate = idx / elapsed
                eta_s = int((max(0, total - idx) / max(0.01, rate)))
                progress_payload = {
                    "stage": "layer3_verify",
                    "status": "running",
                    "updated_at": _now_iso(),
                    "progress_pct": round(pct, 2),
                    "done": idx,
                    "total": total,
                    "rows_per_s": round(rate, 2),
                    "eta_s": eta_s,
                    "safe_count": len(best),
                    "doubtful_count": doubtful_count,
                    "web_checked_count": web_checked_count,
                    "web_upgraded_count": web_upgraded_count,
                    "fast_passthrough_count": fast_passthrough_count,
                    "status_counts": dict(status_counts),
                }
                _atomic_write_json(progress_path, progress_payload)
                print(json.dumps(progress_payload, ensure_ascii=False))
            continue

        repaired_qtext, contextual_actions = _contextual_piecewise_repair(qtext)
        if repaired_qtext != qtext:
            qtext = repaired_qtext

        needs_deep_repair = (
            base_conf < float(args.low_confidence_threshold)
            or base_status in {"reject", "unrecoverable", "review"}
            or base_integrity < 0.85
            or _looks_heavily_corrupted(qtext)
        )
        if needs_deep_repair:
            doubtful_count += 1

        payload = {
            "question_id": qid,
            "question_text": qtext,
            "options": options,
            "correct_answer": correct_answer,
            "type": qtype,
            "subject": subject,
            "chapter": chapter,
        }
        repaired = repair_engine.repair_question(payload, corpus=corpus if needs_deep_repair else None)
        final_text = _to_str(repaired.repaired_question_text).strip() or qtext
        final_opts = list(repaired.options or options)
        final_ans = dict(repaired.correct_answer or correct_answer)
        final_issues = list(dict.fromkeys(list(repaired.validation_issues or [])))
        final_actions = list(dict.fromkeys([*contextual_actions, *list(repaired.repair_actions or [])]))
        final_conf = float(repaired.repair_confidence or base_conf)

        web_diag: dict[str, Any] = {
            "enabled": bool(args.enable_web_verify),
            "used": False,
            "best_similarity": 0.0,
            "upgraded": False,
        }
        web_row_candidate = (
            base_status in {"reject", "unrecoverable"}
            or base_integrity <= float(args.web_integrity_threshold)
            or _looks_heavily_corrupted(qtext)
        )
        allow_web_for_row = bool(args.enable_web_verify) and web_row_candidate and (
            int(args.web_verify_cap) <= 0 or web_checked_count < int(args.web_verify_cap)
        )
        if needs_deep_repair and allow_web_for_row:
            web_checked_count += 1
            web_diag = web_verifier.verify(
                question_text=final_text,
                chapter=chapter,
                subject=subject,
            )
            best_sim = float(web_diag.get("best_similarity") or 0.0)
            best_web_text = _to_str(web_diag.get("best_question_text"))
            if (
                best_sim >= float(args.web_min_similarity_upgrade)
                and len(best_web_text) >= 30
                and _looks_heavily_corrupted(final_text)
            ):
                final_text = best_web_text
                final_actions.append("web_reverification_applied")
                final_conf = min(1.0, final_conf + 0.08)
                web_diag["upgraded"] = True
                web_upgraded_count += 1

        integrity = _score_integrity(
            repair_confidence=final_conf,
            issues=final_issues,
            question_text=final_text,
            options=final_opts,
        )
        status = _status_from_integrity(integrity)
        risk = round(max(0.0, min(1.0, 1.0 - integrity)), 4)

        row["question_text"] = final_text
        if "reconstructed_question" in row:
            row["reconstructed_question"] = final_text
        if "options" in row:
            row["options"] = final_opts
        if "correct_answer" in row:
            row["correct_answer"] = final_ans
        row["repair_actions"] = list(dict.fromkeys(final_actions))
        row["repair_confidence"] = round(final_conf, 4)
        row["repair_status"] = status
        row["detected_issues"] = final_issues
        row["structural_integrity_score"] = integrity
        row["publish_risk_score"] = risk
        row["layer3_verification"] = {
            "doubtful_row": bool(needs_deep_repair),
            "web_reverify": web_diag,
            "stage": "layer3_contextual_repair",
            "processed_at": _now_iso(),
        }

        finalized.append(row)
        if integrity >= float(args.integrity_safe_threshold):
            best.append(row)

        status_counts[status] += 1
        confidence_sum += final_conf
        integrity_sum += integrity
        risk_sum += risk
        for tok in final_issues:
            issue_counts[tok] += 1
        for tok in final_actions:
            action_counts[tok] += 1

        if args.progress_every > 0 and (idx % args.progress_every == 0 or idx == total):
            elapsed = max(0.001, time.time() - t0)
            pct = (idx / max(1, total)) * 100.0
            rate = idx / elapsed
            eta_s = int((max(0, total - idx) / max(0.01, rate)))
            progress_payload = {
                "stage": "layer3_verify",
                "status": "running",
                "updated_at": _now_iso(),
                "progress_pct": round(pct, 2),
                "done": idx,
                "total": total,
                "rows_per_s": round(rate, 2),
                "eta_s": eta_s,
                "safe_count": len(best),
                "doubtful_count": doubtful_count,
                "web_checked_count": web_checked_count,
                "web_upgraded_count": web_upgraded_count,
                "fast_passthrough_count": fast_passthrough_count,
                "status_counts": dict(status_counts),
            }
            _atomic_write_json(progress_path, progress_payload)
            print(json.dumps(progress_payload, ensure_ascii=False))

    _atomic_write_json(output_path, finalized)
    _atomic_write_json(best_path, best)

    seen = max(1, len(finalized))
    report = {
        "started_at": started_at,
        "finished_at": _now_iso(),
        "input": str(input_path),
        "snapshot": str(snapshot_path),
        "output": str(output_path),
        "best_output": str(best_path),
        "progress_file": str(progress_path),
        "rows_seen": len(finalized),
        "best_rows": len(best),
        "doubtful_rows": doubtful_count,
        "web_verify_enabled": bool(args.enable_web_verify),
        "web_checked_rows": web_checked_count,
        "web_upgraded_rows": web_upgraded_count,
        "fast_passthrough_rows": fast_passthrough_count,
        "status_counts": dict(status_counts),
        "avg_repair_confidence": round(confidence_sum / seen, 4),
        "avg_structural_integrity_score": round(integrity_sum / seen, 4),
        "avg_publish_risk_score": round(risk_sum / seen, 4),
        "top_detected_issues": dict(issue_counts.most_common(25)),
        "top_repair_actions": dict(action_counts.most_common(25)),
        "config": {
            "low_confidence_threshold": float(args.low_confidence_threshold),
            "integrity_safe_threshold": float(args.integrity_safe_threshold),
            "web_max_rows": int(args.web_max_rows),
            "web_min_similarity_upgrade": float(args.web_min_similarity_upgrade),
            "web_timeout_s": float(args.web_timeout_s),
            "web_integrity_threshold": float(args.web_integrity_threshold),
            "web_verify_cap": int(args.web_verify_cap),
            "progress_every": int(args.progress_every),
            "fast_safe_confidence": float(args.fast_safe_confidence),
            "fast_safe_integrity": float(args.fast_safe_integrity),
        },
    }
    _atomic_write_json(report_path, report)
    _atomic_write_json(
        progress_path,
        {
            "stage": "layer3_verify",
            "status": "done",
            "updated_at": _now_iso(),
            "done": len(finalized),
            "total": total,
            "progress_pct": 100.0,
            "safe_count": len(best),
            "doubtful_count": doubtful_count,
            "web_checked_count": web_checked_count,
            "web_upgraded_count": web_upgraded_count,
            "fast_passthrough_count": fast_passthrough_count,
            "report": str(report_path),
            "output": str(output_path),
            "best_output": str(best_path),
        },
    )
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
