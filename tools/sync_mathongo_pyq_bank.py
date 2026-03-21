#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests

from app.data.local_app_data_service import LocalAppDataService


MATHONGO_SEED_URLS = [
    "https://www.mathongo.com/iit-jee/jee-main-previous-year-question-paper",
    "https://www.mathongo.com/iit-jee/jee-advanced-2019-question-paper",
    "https://www.mathongo.com/iit-jee/jee-advanced-2018-question-paper",
    "https://www.mathongo.com/iit-jee/jee-advanced-2017-question-paper",
    "https://www.mathongo.com/iit-jee/jee-advanced-2016-question-paper",
    "https://www.mathongo.com/iit-jee/jee-advanced-2015-question-paper",
    "https://www.mathongo.com/iit-jee/jee-advanced-2014-question-paper",
    "https://www.mathongo.com/iit-jee/jee-advanced-2013-question-paper",
]
EXTERNAL_SOLUTION_SEED_URLS = [
    "https://www.mathongo.com/iit-jee/jee-main-chapter-wise-questions-with-solutions",
    "https://www.mathongo.com/iit-jee",
    "https://www.vedantu.com/jee-main/previous-year-question-paper",
    "https://www.vedantu.com/jee-main",
    "https://jeeadv.ac.in/archive",
    "https://testbook.com/jee-main",
    "https://www.selfstudys.com",
    "https://www.scribd.com/document/848228363/JEE-Main-Previous-10-Year-Questions-With-Detailed-Solutions-2016-2025-1743659915811",
    "https://questions.examside.com/past-years/jee/jee-main/chemistry/solutions",
    "https://www.esaral.com/jee/jee-main-question-paper/",
    "https://ecareerpoint.com/jee-main-previous-year-question-paper",
    "https://questions.examside.com/past-years/jee/jee-advanced/chemistry/solutions",
    "https://jeeadv.ac.in/archive.html",
    "https://www.aakash.ac.in/jee-advanced-previous-year-question-papers",
    "https://www.shiksha.com/engineering/articles/last-10-year-jee-advanced-question-papers-with-solutions-blogId-186822",
]
EXTERNAL_DIRECT_PDF_URLS = [
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/sm_sa/Answers%26Solutions_JEE-%28Advanced%29-2022_Paper-1_%28Combined%29.pdf?1sVfgxJ7vMfiJQvKWAVBrVERGdj4q9ZC",
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/sm_sa/Answers%26Solutions_JEE-%28Advanced%29-2022_Paper-2_%28Combined%29_0.pdf?fi9xpM3xsxk5pYT9LuLihkTt1.N.a2DM",
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/target_solutions/2021_1_English.pdf",
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/target_solutions/2020_1_English.pdf",
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/target_solutions/2019_1_English.pdf",
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/target_solutions/2018_1.pdf",
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/target_solutions/Answers%20%26%20Solutions_JEE-%28Advanced%29-2023_Paper-1_%28FINAL%29.pdf",
    "https://dcx0p3on5z8dw.cloudfront.net/Aakash/s3fs-public/pdf_management_files/target_solutions/Answers_and_Solutions_JEE_Advanced_2023_Paper-2_FINAL.pdf",
]
SEED_URLS = list(dict.fromkeys([*MATHONGO_SEED_URLS, *EXTERNAL_SOLUTION_SEED_URLS]))

HREF_RE = re.compile(r"(?is)href=[\"']([^\"'#]+)[\"']")
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\((https?://[^)\s]+)\)", re.I)
PLAIN_URL_RE = re.compile(r"https?://[^\s<>\"]+", re.I)
SHORTLINK_RE = re.compile(r"https?://links\.mathongo\.com/[A-Za-z0-9_-]+", re.I)
DRIVE_ID_PATTERNS = (
    re.compile(r"[?&]id=([A-Za-z0-9_-]{10,})"),
    re.compile(r"/d/([A-Za-z0-9_-]{10,})"),
)
PAGE_DISCOVERY_HINTS = (
    "jee",
    "question",
    "paper",
    "solution",
    "archive",
    "past-years",
    "previous-year",
    "download",
    "iit-jee",
)
SKIP_SOURCE_HOSTS = {
    "api.whatsapp.com",
    "wa.me",
    "facebook.com",
    "m.facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "linkedin.com",
    "youtube.com",
    "youtu.be",
    "t.me",
    "telegram.me",
}
VIEWER_QUERY_KEYS = ("u", "url", "pdf", "file", "download", "src")
LIVE_REPORT_MIN_INTERVAL_S = 1.5
REPORT_FAIL_BACKOFF_S = 6.0


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def sanitize_name(raw: str) -> str:
    text = re.sub(r"\s+", " ", raw).strip()
    text = re.sub(r"[^A-Za-z0-9._() -]+", "_", text)
    text = text.replace("/", "-")
    if not text:
        return "unnamed.pdf"
    if not text.lower().endswith(".pdf"):
        text += ".pdf"
    return text[:220]


def parse_content_disposition_filename(value: str) -> str:
    if not value:
        return ""
    m = re.search(r"filename\*=UTF-8''([^;]+)", value, re.I)
    if m:
        return sanitize_name(requests.utils.unquote(m.group(1)))
    m = re.search(r'filename="?([^";]+)"?', value, re.I)
    if m:
        return sanitize_name(m.group(1))
    return ""


def guess_subject(url: str) -> str:
    bag = url.lower()
    if "physics" in bag:
        return "Physics"
    if "chemistry" in bag:
        return "Chemistry"
    if "biology" in bag:
        return "Biology"
    return "Mathematics"


def default_chapter_for_subject(subject: str) -> str:
    return f"General JEE {subject}"


def infer_chapter_from_page(url: str, subject: str) -> str:
    path = urlparse(url).path.lower()
    bag = requests.utils.unquote(url).lower()
    year_match = re.search(r"(20\d{2})", path)
    if year_match is None:
        year_match = re.search(r"(20\d{2})", bag)
    year = year_match.group(1) if year_match else ""
    if "chapter-wise-questions-with-solutions" in path or "chapterwise" in path:
        return f"All JEE Chapters ({subject})"
    if "jee-advanced" in path:
        return f"JEE Advanced {year} Mixed" if year else "JEE Advanced Mixed"
    if "jee" in bag and "advanced" in bag:
        return f"JEE Advanced {year} Mixed" if year else "JEE Advanced Mixed"
    if "jee-main" in path:
        return f"JEE Main {year} Mixed" if year else "JEE Main Mixed"
    if "jee" in bag and "main" in bag:
        return f"JEE Main {year} Mixed" if year else "JEE Main Mixed"
    return default_chapter_for_subject(subject)


def extract_drive_id(url: str) -> str:
    if not url:
        return ""
    for pat in DRIVE_ID_PATTERNS:
        m = pat.search(url)
        if m:
            return m.group(1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    vals = qs.get("id") or qs.get("file") or []
    if vals:
        return vals[0]
    return ""


def is_dns_resolution_error(exc: Exception | str) -> bool:
    token = str(exc).lower()
    return (
        "name resolution" in token
        or "failed to resolve" in token
        or "nodename nor servname provided" in token
        or "temporary failure in name resolution" in token
    )


def jina_proxy_url(url: str) -> str:
    clean = url.strip()
    if clean.lower().startswith("https://r.jina.ai/http://"):
        return clean
    if clean.lower().startswith("https://r.jina.ai/https://"):
        return clean
    if clean.lower().startswith("http://"):
        clean = clean[len("http://") :]
    elif clean.lower().startswith("https://"):
        clean = clean[len("https://") :]
    return f"https://r.jina.ai/http://{clean}"


def likely_discovery_page(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if not path or path == "/":
        return True
    if path.endswith(".pdf"):
        return False
    return any(token in path for token in PAGE_DISCOVERY_HINTS)


def normalize_host(host: str) -> str:
    token = host.strip().lower()
    return token[4:] if token.startswith("www.") else token


def normalize_url_for_dedupe(url: str) -> str:
    parsed = urlparse(url.strip())
    host = normalize_host(parsed.netloc)
    path = parsed.path or "/"
    if parsed.query and ("id=" in parsed.query or "token=" in parsed.query):
        return f"{parsed.scheme}://{host}{path}?{parsed.query}"
    return f"{parsed.scheme}://{host}{path}"


def extract_nested_pdf_url(link: str) -> str:
    """Extract embedded PDF links from viewer/share URLs.

    Example: https://site/viewer?u=https%3A%2F%2Fhost%2Fa.pdf
    """
    if not link:
        return ""
    parsed = urlparse(link)
    qs = parse_qs(parsed.query)
    for key in VIEWER_QUERY_KEYS:
        vals = qs.get(key) or []
        for raw in vals:
            val = unquote(str(raw).strip())
            low = val.lower()
            if low.startswith("http://") or low.startswith("https://"):
                if ".pdf" in low or "export=download" in low:
                    return val
    return ""


def should_skip_candidate(link: str) -> str:
    low = link.lower().strip()
    if not low:
        return "empty_link"
    if low.startswith("javascript:") or low.startswith("mailto:"):
        return "non_http_link"
    host = normalize_host(urlparse(low).netloc or "")
    if host in SKIP_SOURCE_HOSTS:
        return f"skip_host:{host}"
    if any(token in low for token in ("/share", "share=", "intent/", "/intent")) and ".pdf" not in low:
        return "share_link_non_pdf"
    return ""


class Http:
    def __init__(self, timeout_s: float = 18.0) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            }
        )
        self.timeout_s = timeout_s
        self.max_download_s = max(
            30.0,
            float(os.environ.get("PYQ_MAX_DOWNLOAD_S", "150")),
        )

    def get_text(self, url: str, *, max_retries: int = 2) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                r = self._session.get(url, timeout=self.timeout_s, allow_redirects=True)
                r.raise_for_status()
                return r.text
            except Exception as exc:  # pragma: no cover - network runtime
                last_exc = exc
                if attempt < max_retries:
                    time.sleep(min(4.0, 0.8 * attempt))
                    continue
                if is_dns_resolution_error(exc):
                    proxy = jina_proxy_url(url)
                    try:
                        r = self._session.get(
                            proxy,
                            timeout=max(self.timeout_s, 30),
                            allow_redirects=True,
                        )
                        r.raise_for_status()
                        return r.text
                    except Exception as proxy_exc:  # pragma: no cover - network runtime
                        raise RuntimeError(
                            f"fetch_failed:{url}:{exc};proxy_failed:{proxy_exc}"
                        ) from proxy_exc
                raise RuntimeError(f"fetch_failed:{url}:{exc}") from exc
        raise RuntimeError(f"fetch_failed:{url}:{last_exc}")

    def resolve_url(self, url: str, *, max_retries: int = 2) -> str:
        last_exc: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                r = self._session.get(url, timeout=self.timeout_s, allow_redirects=True)
                r.raise_for_status()
                return r.url
            except Exception as exc:  # pragma: no cover - network runtime
                last_exc = exc
                if attempt < max_retries:
                    time.sleep(min(4.0, 0.8 * attempt))
                    continue
                raise RuntimeError(f"resolve_failed:{url}:{exc}") from exc
        raise RuntimeError(f"resolve_failed:{url}:{last_exc}")

    def download_pdf(self, url: str, out_path: Path, *, max_retries: int = 2) -> tuple[int, str]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        last_exc: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                started = time.monotonic()
                with self._session.get(url, timeout=max(self.timeout_s, 45), stream=True, allow_redirects=True) as r:
                    r.raise_for_status()
                    content_type = (r.headers.get("content-type") or "").lower()
                    if "pdf" not in content_type and "octet-stream" not in content_type:
                        # Some servers still return HTML landing page; keep only real binaries.
                        first = next(r.iter_content(chunk_size=2048), b"")
                        if (time.monotonic() - started) > self.max_download_s:
                            raise RuntimeError(f"download_wall_timeout:{self.max_download_s:.0f}s:{r.url}")
                        if b"%PDF" not in first[:1024]:
                            raise RuntimeError(
                                f"not_pdf_content:{content_type}:{r.url}"
                            )
                        with out_path.open("wb") as fh:
                            fh.write(first)
                            for chunk in r.iter_content(chunk_size=1024 * 64):
                                if (time.monotonic() - started) > self.max_download_s:
                                    raise RuntimeError(f"download_wall_timeout:{self.max_download_s:.0f}s:{r.url}")
                                if chunk:
                                    fh.write(chunk)
                    else:
                        with out_path.open("wb") as fh:
                            for chunk in r.iter_content(chunk_size=1024 * 64):
                                if (time.monotonic() - started) > self.max_download_s:
                                    raise RuntimeError(f"download_wall_timeout:{self.max_download_s:.0f}s:{r.url}")
                                if chunk:
                                    fh.write(chunk)
                    size = out_path.stat().st_size
                    if size < 4096:
                        raise RuntimeError(f"file_too_small:{size}:{r.url}")
                    return size, r.url
            except Exception as exc:  # pragma: no cover - network runtime
                last_exc = exc
                if out_path.exists():
                    out_path.unlink(missing_ok=True)
                if attempt < max_retries:
                    time.sleep(min(5.0, attempt * 1.2))
                    continue
                raise RuntimeError(f"download_failed:{url}:{exc}") from exc
        raise RuntimeError(f"download_failed:{url}:{last_exc}")


@dataclass
class DownloadCandidate:
    source_page: str
    source_link: str
    subject: str
    chapter: str


def extract_hrefs(base_url: str, html_text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in HREF_RE.findall(html_text):
        link = urljoin(base_url, raw.strip())
        if not link or link in seen:
            continue
        seen.add(link)
        out.append(link)
    for raw in MARKDOWN_LINK_RE.findall(html_text):
        link = urljoin(base_url, raw.strip())
        if not link or link in seen:
            continue
        seen.add(link)
        out.append(link)
    for raw in PLAIN_URL_RE.findall(html_text):
        link = urljoin(base_url, raw.strip())
        if not link or link in seen:
            continue
        seen.add(link)
        out.append(link)
    return out


def load_direct_links(paths: list[str]) -> list[str]:
    out: list[str] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        text = text.strip()
        if not text:
            continue
        # Accept JSON array/object and plain text list formats.
        if text.startswith("{") or text.startswith("["):
            try:
                payload = json.loads(text)
                if isinstance(payload, dict):
                    for key in ("direct_links", "urls", "links"):
                        rows = payload.get(key)
                        if isinstance(rows, list):
                            out.extend([str(x).strip() for x in rows if str(x).strip()])
                elif isinstance(payload, list):
                    out.extend([str(x).strip() for x in payload if str(x).strip()])
            except Exception:
                pass
        for line in text.splitlines():
            item = line.strip()
            if not item or item.startswith("#"):
                continue
            if item.lower().startswith("http://") or item.lower().startswith("https://"):
                out.append(item)
    deduped: list[str] = []
    seen: set[str] = set()
    for url in out:
        key = normalize_url_for_dedupe(url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(url)
    return deduped


def build_direct_candidates(direct_links: list[str]) -> list[DownloadCandidate]:
    out: list[DownloadCandidate] = []
    seen: set[str] = set()
    for raw_url in direct_links:
        url = raw_url.strip()
        if not url:
            continue
        low = url.lower()
        if not (low.startswith("http://") or low.startswith("https://")):
            continue
        if ".pdf" not in low and "questions.examside.com/past-years/jee/question/" not in low:
            continue
        key = normalize_url_for_dedupe(url)
        if key in seen:
            continue
        seen.add(key)
        subject = guess_subject(url)
        chapter = infer_chapter_from_page(url, subject)
        out.append(
            DownloadCandidate(
                source_page=url,
                source_link=url,
                subject=subject,
                chapter=chapter,
            )
        )
    return out


def discover_pages(http: Http, seed_urls: list[str], *, max_pages: int) -> list[str]:
    queue: list[str] = []
    seen: set[str] = set()
    allowed_hosts: set[str] = set()
    allowed_base_hosts: set[str] = set()

    for row in seed_urls:
        clean = row.strip()
        if clean and clean not in seen:
            seen.add(clean)
            queue.append(clean)
        parsed = urlparse(clean)
        if parsed.netloc:
            base_host = normalize_host(parsed.netloc)
            allowed_base_hosts.add(base_host)
            allowed_hosts.add(base_host)
            allowed_hosts.add(f"www.{base_host}")

    idx = 0
    while idx < len(queue) and len(queue) < max_pages:
        url = queue[idx]
        idx += 1
        try:
            html_text = http.get_text(url)
        except Exception:
            continue
        for link in extract_hrefs(url, html_text):
            parsed = urlparse(link)
            host = normalize_host(parsed.netloc) if parsed.netloc else ""
            if host and host not in allowed_base_hosts:
                continue
            if not likely_discovery_page(link):
                continue
            link = f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.netloc else link
            if link not in seen and len(queue) < max_pages:
                seen.add(link)
                queue.append(link)
    return queue


def collect_download_candidates(
    http: Http,
    pages: list[str],
    *,
    target_candidates: int = 0,
    on_progress: Callable[[int, int, int], None] | None = None,
) -> list[DownloadCandidate]:
    candidates: list[DownloadCandidate] = []
    seen: set[tuple[str, str]] = set()
    total = len(pages)
    for i, page in enumerate(pages, start=1):
        pct = (i / max(1, total)) * 100.0
        print(f"[crawl {pct:6.2f}%] page {i}/{total}: {page}")
        try:
            html_text = http.get_text(page)
        except Exception as exc:
            print(f"  skip page (fetch failed): {exc}")
            if callable(on_progress):
                try:
                    on_progress(i, total, len(candidates))
                except Exception:
                    pass
            continue
        links = extract_hrefs(page, html_text)
        page_subject = guess_subject(page)
        page_chapter = infer_chapter_from_page(page, page_subject)
        for link in links:
            low = link.lower()
            examside_question_link = "questions.examside.com/past-years/jee/question/" in low
            probable_pdf_download = (
                "pdf" in low
                and any(token in low for token in ("download", "question-paper", "previous-year", "archive"))
            )
            if not (
                SHORTLINK_RE.search(link)
                or low.endswith(".pdf")
                or "drive.google.com" in low
                or probable_pdf_download
                or examside_question_link
            ):
                continue
            key = (page, link)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                DownloadCandidate(
                    source_page=page,
                    source_link=link,
                    subject=page_subject,
                    chapter=page_chapter,
                )
            )
        if callable(on_progress):
            try:
                on_progress(i, total, len(candidates))
            except Exception:
                pass
        if target_candidates > 0 and len(candidates) >= target_candidates:
            print(
                f"[collect] early-stop: candidates={len(candidates)} reached target={target_candidates}"
            )
            break
    return candidates


def choose_filename(candidate: DownloadCandidate, final_url: str, headers: dict[str, str]) -> str:
    if "questions.examside.com/past-years/jee/question/" in candidate.source_link.lower():
        parsed = urlparse(candidate.source_link)
        tail = Path(parsed.path).name or "examside_question"
        tail = re.sub(r"[^A-Za-z0-9._() -]+", "_", tail).strip() or "examside_question"
        return f"{tail[:200]}.txt"
    cd = headers.get("content-disposition") or ""
    filename = parse_content_disposition_filename(cd)
    if filename:
        return filename
    parsed_final = urlparse(final_url)
    tail = Path(parsed_final.path).name
    if tail and tail.lower().endswith(".pdf"):
        return sanitize_name(tail)
    slug = Path(urlparse(candidate.source_page).path).name or "mathongo_pyq"
    token = Path(urlparse(candidate.source_link).path).name or "file"
    return sanitize_name(f"{slug}-{token}.pdf")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_to_download_url(http: Http, link: str) -> tuple[str, str]:
    low = link.lower()
    if low.endswith(".pdf"):
        return link, ""
    nested = extract_nested_pdf_url(link)
    if nested:
        return nested, ""

    resolved = http.resolve_url(link, max_retries=2)
    drive_id = extract_drive_id(link) or extract_drive_id(resolved)
    if drive_id:
        return f"https://drive.google.com/uc?export=download&id={drive_id}", drive_id

    nested_resolved = extract_nested_pdf_url(resolved)
    if nested_resolved:
        return nested_resolved, ""

    if resolved.lower().endswith(".pdf"):
        return resolved, ""

    return resolved, ""


def download_all(
    http: Http,
    candidates: list[DownloadCandidate],
    *,
    out_dir: Path,
    max_pdfs: int,
    skip_existing_files: bool = False,
    existing_filenames: set[str] | None = None,
    prior_failed_links: set[str] | None = None,
    on_download: Callable[[dict[str, Any]], None] | None = None,
    on_failure: Callable[[dict[str, Any]], None] | None = None,
    on_skip: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    seen_final_urls: set[str] = set()
    seen_hashes: set[str] = set()
    seen_source_links: set[str] = set()
    known_existing = set(existing_filenames or set())
    failed_links = prior_failed_links if prior_failed_links is not None else set()
    skipped_existing = 0
    skipped_prior_failed = 0

    total = min(len(candidates), max_pdfs)
    for i, cand in enumerate(candidates[:total], start=1):
        pct = (i / max(1, total)) * 100.0
        print(f"[download {pct:6.2f}%] {i}/{total}: {cand.source_link}")
        try:
            source_key = normalize_url_for_dedupe(cand.source_link)
            if source_key in seen_source_links:
                continue
            seen_source_links.add(source_key)
            if source_key in failed_links:
                skipped_prior_failed += 1
                if callable(on_skip):
                    on_skip(
                        {
                            "reason": "prior_failed_link",
                            "source_link": cand.source_link,
                            "source_page": cand.source_page,
                            "subject": cand.subject,
                        }
                    )
                continue
            skip_reason = should_skip_candidate(cand.source_link)
            if skip_reason:
                raise RuntimeError(skip_reason)
            if "questions.examside.com/past-years/jee/question/" in cand.source_link.lower():
                text = http.get_text(cand.source_link, max_retries=2)
                text = re.sub(r"\r\n?", "\n", text)
                if len(text.strip()) < 120:
                    raise RuntimeError(f"question_page_too_short:{cand.source_link}")
                text_name = choose_filename(cand, cand.source_link, {})
                text_path = out_dir / text_name
                text_path.write_text(text, encoding="utf-8")
                file_hash = sha256_file(text_path)
                if file_hash in seen_hashes:
                    text_path.unlink(missing_ok=True)
                    continue
                seen_hashes.add(file_hash)
                downloaded.append(
                    {
                        "source_page": cand.source_page,
                        "source_link": cand.source_link,
                        "download_url": cand.source_link,
                        "final_url": cand.source_link,
                        "drive_file_id": "",
                        "subject": cand.subject,
                        "chapter": cand.chapter,
                        "saved_path": str(text_path),
                        "filename": text_path.name,
                        "size_bytes": text_path.stat().st_size,
                        "sha256": file_hash,
                        "fetched_at": now_iso(),
                        "source_kind": "raw_text_question_page",
                    }
                )
                if callable(on_download):
                    on_download(downloaded[-1])
                continue

            dl_url, drive_id = resolve_to_download_url(http, cand.source_link)
            skip_reason = should_skip_candidate(dl_url)
            if skip_reason:
                raise RuntimeError(skip_reason)
            if dl_url in seen_final_urls:
                continue
            seen_final_urls.add(dl_url)

            head_headers: dict[str, str] = {}
            final_url = dl_url
            try:
                head = requests.head(dl_url, allow_redirects=True, timeout=8)
                head_headers = {k.lower(): v for k, v in head.headers.items()}
                final_url = head.url or dl_url
            except Exception:
                # HEAD often fails on anti-bot endpoints; GET path still validates PDF magic bytes.
                pass
            filename = choose_filename(cand, final_url, head_headers)
            if skip_existing_files and filename in known_existing:
                skipped_existing += 1
                if callable(on_skip):
                    on_skip(
                        {
                            "reason": "existing_file",
                            "source_link": cand.source_link,
                            "filename": filename,
                            "source_page": cand.source_page,
                            "subject": cand.subject,
                        }
                    )
                continue
            path = out_dir / filename
            if path.exists() and path.stat().st_size > 4096:
                if skip_existing_files:
                    skipped_existing += 1
                    known_existing.add(path.name)
                    if callable(on_skip):
                        on_skip(
                            {
                                "reason": "existing_path",
                                "source_link": cand.source_link,
                                "filename": path.name,
                                "source_page": cand.source_page,
                                "subject": cand.subject,
                            }
                        )
                    continue
                file_hash = sha256_file(path)
                if file_hash in seen_hashes:
                    continue
                seen_hashes.add(file_hash)
                downloaded.append(
                    {
                        "source_page": cand.source_page,
                        "source_link": cand.source_link,
                        "download_url": dl_url,
                        "final_url": final_url,
                        "drive_file_id": drive_id,
                        "subject": cand.subject,
                        "chapter": cand.chapter,
                        "saved_path": str(path),
                        "filename": path.name,
                        "size_bytes": path.stat().st_size,
                        "sha256": file_hash,
                        "fetched_at": now_iso(),
                    }
                )
                if callable(on_download):
                    on_download(downloaded[-1])
                continue

            max_retries = max(1, int(os.environ.get("PYQ_DOWNLOAD_MAX_RETRIES", "1")))
            size, final_after_get = http.download_pdf(dl_url, path, max_retries=max_retries)
            file_hash = sha256_file(path)
            if file_hash in seen_hashes:
                path.unlink(missing_ok=True)
                continue
            seen_hashes.add(file_hash)
            downloaded.append(
                {
                    "source_page": cand.source_page,
                    "source_link": cand.source_link,
                    "download_url": dl_url,
                    "final_url": final_after_get,
                    "drive_file_id": drive_id,
                    "subject": cand.subject,
                    "chapter": cand.chapter,
                    "saved_path": str(path),
                    "filename": path.name,
                    "size_bytes": size,
                    "sha256": file_hash,
                    "fetched_at": now_iso(),
                }
            )
            if callable(on_download):
                on_download(downloaded[-1])
        except Exception as exc:  # pragma: no cover - network runtime
            failed_links.add(normalize_url_for_dedupe(cand.source_link))
            if is_dns_resolution_error(exc):
                try:
                    proxy_text = http.get_text(cand.source_link, max_retries=1)
                    proxy_text = re.sub(r"\r\n?", "\n", proxy_text).strip()
                    if len(proxy_text) >= 220:
                        text_name = re.sub(r"[^A-Za-z0-9._() -]+", "_", Path(urlparse(cand.source_link).path).name or f"fallback_{i}")
                        text_path = out_dir / f"{text_name[:200]}.txt"
                        text_path.write_text(proxy_text, encoding="utf-8")
                        file_hash = sha256_file(text_path)
                        if file_hash not in seen_hashes:
                            seen_hashes.add(file_hash)
                            downloaded.append(
                                {
                                    "source_page": cand.source_page,
                                    "source_link": cand.source_link,
                                    "download_url": cand.source_link,
                                    "final_url": cand.source_link,
                                    "drive_file_id": "",
                                    "subject": cand.subject,
                                    "chapter": cand.chapter,
                                    "saved_path": str(text_path),
                                    "filename": text_path.name,
                                    "size_bytes": text_path.stat().st_size,
                                    "sha256": file_hash,
                                    "fetched_at": now_iso(),
                                    "source_kind": "raw_text_dns_proxy_fallback",
                                }
                            )
                            if callable(on_download):
                                on_download(downloaded[-1])
                            continue
                except Exception:
                    pass
            failure = {
                "source_page": cand.source_page,
                "source_link": cand.source_link,
                "subject": cand.subject,
                "error": str(exc),
            }
            failures.append(failure)
            if callable(on_failure):
                on_failure(failure)
            print(f"  failed: {exc}")
    return downloaded, failures, {
        "skipped_existing": skipped_existing,
        "skipped_prior_failed": skipped_prior_failed,
    }


def _empty_parse_report(files_total: int = 0) -> dict[str, Any]:
    return {
        "files_total": files_total,
        "parsed_ok": 0,
        "parsed_failed": 0,
        "parse_question_count": 0,
        "text_extract_parse_count": 0,
        "ocr_parse_count": 0,
        "quality_retry_ocr": 0,
        "publish_success": 0,
        "publish_no_new": 0,
        "published_count": 0,
        "duplicates_skipped": 0,
        "solutions_enriched_count": 0,
        "failures": [],
        "per_file": [],
    }


def _questions_and_non_invalid(parsed: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    questions = [q for q in (parsed.get("questions") or []) if isinstance(q, dict)]
    non_invalid = [q for q in questions if str(q.get("validation_status", "")).lower() != "invalid"]
    return questions, non_invalid


def _should_retry_ocr_for_quality(
    questions: list[dict[str, Any]],
    non_invalid: list[dict[str, Any]],
) -> bool:
    total = len(questions)
    valid = len(non_invalid)
    if total == 0 or valid == 0:
        return True
    if total >= 8 and (valid / max(1, total)) < 0.40:
        return True
    if total < 8 and valid < 2:
        return True
    return False


async def _svc_handle_action_with_timeout(
    svc: LocalAppDataService,
    payload: dict[str, Any],
    *,
    timeout_s: float,
    stage: str,
    pdf_path: str,
) -> dict[str, Any]:
    try:
        return await asyncio.wait_for(
            svc.handle_action(payload),
            timeout=max(5.0, float(timeout_s)),
        )
    except asyncio.TimeoutError:
        return {
            "ok": False,
            "status": "TIMEOUT",
            "message": f"{stage} timed out after {timeout_s:.0f}s for {Path(pdf_path).name}",
        }
    except Exception as exc:  # pragma: no cover - runtime guard
        return {
            "ok": False,
            "status": "EXCEPTION",
            "message": f"{stage} failed for {Path(pdf_path).name}: {exc}",
        }


async def parse_and_publish_single(
    svc: LocalAppDataService,
    row: dict[str, Any],
    *,
    progress_prefix: str = "",
) -> dict[str, Any]:
    report = _empty_parse_report(files_total=1)
    pdf_path = str(row.get("saved_path") or "")
    subject = str(row.get("subject") or "Mathematics")
    chapter = str(row.get("chapter") or default_chapter_for_subject(subject))
    if progress_prefix:
        print(f"{progress_prefix}{Path(pdf_path).name}")
    meta = {
        "teacher_id": "mathongo_offline_sync",
        "subject": subject,
        "chapter": chapter,
        "difficulty": "Hard",
    }
    parse_payload_ocr = {
        "action": "lc9_parse_questions",
        "pdf_path": pdf_path,
        "meta": meta,
        "web_ocr_fusion_mode": False,
    }
    parse_timeout_s = float(os.environ.get("PYQ_PARSE_ACTION_TIMEOUT_S", "180"))
    publish_timeout_s = float(os.environ.get("PYQ_PUBLISH_ACTION_TIMEOUT_S", "120"))

    parse_payload = parse_payload_ocr
    parse_mode = "ocr_pdf"
    used_quality_retry = False
    extracted_text = ""
    path_obj = Path(pdf_path)
    raw_text_inline = str(row.get("raw_text") or "").strip()
    if raw_text_inline:
        extracted_text = raw_text_inline
        parse_payload = {
            "action": "lc9_parse_questions",
            "raw_text": extracted_text,
            "meta": meta,
            "web_ocr_fusion_mode": False,
        }
        parse_mode = "raw_text_inline"
    elif path_obj.exists() and path_obj.suffix.lower() in {".txt", ".md"}:
        try:
            extracted_text = path_obj.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            extracted_text = ""
        if extracted_text.strip():
            parse_payload = {
                "action": "lc9_parse_questions",
                "raw_text": extracted_text,
                "meta": meta,
                "web_ocr_fusion_mode": False,
            }
            parse_mode = "raw_text_file"
    elif path_obj.exists():
        try:
            blob = path_obj.read_bytes()
            extracted_text = svc._extract_text_from_pdf_bytes(blob)
        except Exception:
            extracted_text = ""
    if extracted_text and len(extracted_text) >= 1200 and not svc._looks_like_binary_pdf_text(extracted_text):
        parse_payload = {
            "action": "lc9_parse_questions",
            "raw_text": extracted_text,
            "meta": meta,
            "web_ocr_fusion_mode": False,
        }
        parse_mode = "pdf_text_extract"

    parsed = await _svc_handle_action_with_timeout(
        svc,
        parse_payload,
        timeout_s=parse_timeout_s,
        stage=f"parse:{parse_mode}",
        pdf_path=pdf_path,
    )
    if (not bool(parsed.get("ok"))) and parse_mode == "pdf_text_extract":
        # Fallback to OCR pipeline only when text-extract parse fails.
        parse_mode = "ocr_pdf"
        parsed = await _svc_handle_action_with_timeout(
            svc,
            parse_payload_ocr,
            timeout_s=parse_timeout_s,
            stage="parse:ocr_pdf_fallback",
            pdf_path=pdf_path,
        )
    if not bool(parsed.get("ok")):
        report["parsed_failed"] += 1
        report["failures"].append(
            {
                "pdf": pdf_path,
                "stage": "parse",
                "status": parsed.get("status"),
                "message": parsed.get("message"),
            }
        )
        return report

    questions, non_invalid = _questions_and_non_invalid(parsed)
    if parse_mode == "pdf_text_extract" and _should_retry_ocr_for_quality(questions, non_invalid):
        ocr_parsed = await _svc_handle_action_with_timeout(
            svc,
            parse_payload_ocr,
            timeout_s=parse_timeout_s,
            stage="parse:ocr_quality_retry",
            pdf_path=pdf_path,
        )
        if bool(ocr_parsed.get("ok")):
            ocr_questions, ocr_non_invalid = _questions_and_non_invalid(ocr_parsed)
            if (len(ocr_non_invalid), len(ocr_questions)) > (len(non_invalid), len(questions)):
                parsed = ocr_parsed
                questions = ocr_questions
                non_invalid = ocr_non_invalid
                parse_mode = "ocr_pdf"
                used_quality_retry = True

    report["parsed_ok"] += 1
    report["parse_question_count"] += len(questions)
    if parse_mode in {"pdf_text_extract", "raw_text_file", "raw_text_inline"}:
        report["text_extract_parse_count"] += 1
    else:
        report["ocr_parse_count"] += 1
    if used_quality_retry:
        report["quality_retry_ocr"] += 1

    if not non_invalid:
        report["per_file"].append(
            {
                "pdf": pdf_path,
                "subject": subject,
                "chapter": chapter,
                "parse_mode": parse_mode,
                "quality_retry_ocr": used_quality_retry,
                "parsed_count": len(questions),
                "publish_status": "SKIPPED_NO_NON_INVALID",
                "published_count": 0,
            }
        )
        return report

    publish_ready = list(non_invalid)
    prepublish_pruned = 0
    try:
        coerced_rows, invalid_rows, _ = svc._coerce_import_questions(
            {"questions": publish_ready, "meta": meta}
        )
        invalid_ids = {
            str(x.get("question_id"))
            for x in invalid_rows
            if isinstance(x, dict) and str(x.get("question_id"))
        }
        if invalid_ids:
            prepublish_pruned = len(invalid_ids)
            publish_ready = [
                q for q in coerced_rows if str(q.get("question_id")) not in invalid_ids
            ]
        else:
            publish_ready = list(coerced_rows)
    except Exception:
        pass

    if not publish_ready:
        report["per_file"].append(
            {
                "pdf": pdf_path,
                "subject": subject,
                "chapter": chapter,
                "parse_mode": parse_mode,
                "quality_retry_ocr": used_quality_retry,
                "parsed_count": len(questions),
                "non_invalid_count": len(non_invalid),
                "prepublish_pruned": prepublish_pruned,
                "publish_status": "SKIPPED_ALL_INVALID_AFTER_NORMALIZE",
                "published_count": 0,
            }
        )
        return report

    publish_payload = {
        "action": "lc9_publish_questions",
        "questions": publish_ready,
        "meta": meta,
        "publish_gate_profile": "legacy",
        "fix_suggestions_applied": False,
    }
    published = await _svc_handle_action_with_timeout(
        svc,
        publish_payload,
        timeout_s=publish_timeout_s,
        stage="publish",
        pdf_path=pdf_path,
    )
    status = str(published.get("status") or "")
    if status == "INVALID_IMPORT_QUESTIONS":
        invalid_rows = [x for x in (published.get("invalid") or []) if isinstance(x, dict)]
        invalid_ids = {
            str(x.get("question_id"))
            for x in invalid_rows
            if str(x.get("question_id"))
        }
        if invalid_ids:
            retry_questions = [
                q for q in publish_ready if str(q.get("question_id")) not in invalid_ids
            ]
            if retry_questions and len(retry_questions) < len(publish_ready):
                publish_payload["questions"] = retry_questions
                retry_published = await _svc_handle_action_with_timeout(
                    svc,
                    publish_payload,
                    timeout_s=publish_timeout_s,
                    stage="publish_retry",
                    pdf_path=pdf_path,
                )
                if bool(retry_published.get("ok")):
                    published = retry_published
                    status = str(published.get("status") or "")

    pub_count = int(published.get("published_count") or 0)
    dup_count = int(published.get("duplicates_skipped") or 0)
    solutions_enriched = int(published.get("solutions_enriched_count") or 0)
    report["duplicates_skipped"] += dup_count
    report["published_count"] += pub_count
    report["solutions_enriched_count"] += solutions_enriched
    if status == "SUCCESS":
        report["publish_success"] += 1
    elif status == "NO_NEW_QUESTIONS":
        report["publish_no_new"] += 1
    else:
        report["failures"].append(
            {
                "pdf": pdf_path,
                "stage": "publish",
                "status": status,
                "message": published.get("message"),
            }
        )

    report["per_file"].append(
        {
            "pdf": pdf_path,
            "subject": subject,
            "chapter": chapter,
            "parse_mode": parse_mode,
            "quality_retry_ocr": used_quality_retry,
            "parsed_count": len(questions),
            "non_invalid_count": len(non_invalid),
            "publish_ready_count": len(publish_ready),
            "prepublish_pruned": prepublish_pruned,
            "publish_status": status,
            "published_count": pub_count,
            "duplicates_skipped": dup_count,
            "solutions_enriched_count": solutions_enriched,
        }
    )
    return report


async def parse_and_publish(downloaded: list[dict[str, Any]]) -> dict[str, Any]:
    svc = LocalAppDataService()
    reports: list[dict[str, Any]] = []
    total = len(downloaded)
    for i, row in enumerate(downloaded, start=1):
        pct = (i / max(1, total)) * 100.0
        per = await parse_and_publish_single(
            svc,
            row,
            progress_prefix=f"[parse   {pct:6.2f}%] {i}/{total}: ",
        )
        reports.append(per)
    return merge_parse_reports(reports)


def merge_parse_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "files_total": 0,
        "parsed_ok": 0,
        "parsed_failed": 0,
        "parse_question_count": 0,
        "text_extract_parse_count": 0,
        "ocr_parse_count": 0,
        "quality_retry_ocr": 0,
        "publish_success": 0,
        "publish_no_new": 0,
        "published_count": 0,
        "duplicates_skipped": 0,
        "solutions_enriched_count": 0,
        "failures": [],
        "per_file": [],
    }
    for row in reports:
        if not isinstance(row, dict):
            continue
        for key in (
            "files_total",
            "parsed_ok",
            "parsed_failed",
            "parse_question_count",
            "text_extract_parse_count",
            "ocr_parse_count",
            "quality_retry_ocr",
            "publish_success",
            "publish_no_new",
            "published_count",
            "duplicates_skipped",
            "solutions_enriched_count",
        ):
            merged[key] += int(row.get(key) or 0)
        merged["failures"].extend([x for x in (row.get("failures") or []) if isinstance(x, dict)])
        merged["per_file"].extend([x for x in (row.get("per_file") or []) if isinstance(x, dict)])
    return merged


class ParsePublishWorker:
    def __init__(self, *, workers: int = 6, on_progress: Callable[[], None] | None = None) -> None:
        self._queue: Queue[dict[str, Any] | None] = Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._lock = threading.Lock()
        self._reports: list[dict[str, Any]] = []
        self._submitted_keys: set[str] = set()
        self._done = 0
        self._started = False
        self._workers = max(1, min(10, int(workers)))
        self._on_progress = on_progress

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread.start()

    def submit(self, entry: dict[str, Any]) -> None:
        key = str(entry.get("sha256") or entry.get("saved_path") or "")
        if not key:
            key = str(entry)
        with self._lock:
            if key in self._submitted_keys:
                return
            self._submitted_keys.add(key)
        self._queue.put(dict(entry))

    def close(self) -> None:
        if not self._started:
            return
        for _ in range(self._workers):
            self._queue.put(None)
        self._thread.join()

    def report(self) -> dict[str, Any]:
        with self._lock:
            return merge_parse_reports(list(self._reports))

    def stats(self) -> dict[str, int]:
        with self._lock:
            submitted = len(self._submitted_keys)
            done = self._done
        return {
            "submitted": submitted,
            "done": done,
            "pending": max(0, submitted - done),
            "workers": self._workers,
        }

    def _run(self) -> None:
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        svc = LocalAppDataService()

        async def worker_loop(worker_id: int) -> None:
            while True:
                row = await asyncio.to_thread(self._queue.get)
                if row is None:
                    self._queue.task_done()
                    break
                try:
                    per = await parse_and_publish_single(
                        svc,
                        row,
                        progress_prefix=f"[stream worker {worker_id}] ",
                    )
                except Exception as exc:  # pragma: no cover - runtime guard
                    per = {
                        "files_total": 1,
                        "parsed_ok": 0,
                        "parsed_failed": 1,
                        "parse_question_count": 0,
                        "text_extract_parse_count": 0,
                        "ocr_parse_count": 0,
                        "quality_retry_ocr": 0,
                        "publish_success": 0,
                        "publish_no_new": 0,
                        "published_count": 0,
                        "duplicates_skipped": 0,
                        "solutions_enriched_count": 0,
                        "failures": [
                            {
                                "pdf": str(row.get("saved_path") or ""),
                                "stage": "stream_parse_publish",
                                "status": "EXCEPTION",
                                "message": str(exc),
                            }
                        ],
                        "per_file": [],
                    }
                with self._lock:
                    self._reports.append(per)
                    self._done += 1
                    done = self._done
                print(
                    json.dumps(
                        {
                            "stream_parse_done": done,
                            "parsed_ok": int(per.get("parsed_ok") or 0),
                            "text_extract_parse_count": int(per.get("text_extract_parse_count") or 0),
                            "ocr_parse_count": int(per.get("ocr_parse_count") or 0),
                            "quality_retry_ocr": int(per.get("quality_retry_ocr") or 0),
                            "published_count": int(per.get("published_count") or 0),
                            "duplicates_skipped": int(per.get("duplicates_skipped") or 0),
                            "solutions_enriched_count": int(per.get("solutions_enriched_count") or 0),
                        },
                        ensure_ascii=False,
                    )
                )
                self._queue.task_done()
                if callable(self._on_progress):
                    try:
                        self._on_progress()
                    except Exception:
                        pass

        tasks = [asyncio.create_task(worker_loop(i + 1)) for i in range(self._workers)]
        await asyncio.gather(*tasks)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Download MathonGo PYQ PDFs and import into offline bank")
    p.add_argument("--out-dir", default="data/offline_pyq_pdfs/mathongo", help="Directory to store PDFs")
    p.add_argument("--manifest", default="data/offline_pyq_pdfs/mathongo/manifest.json", help="Manifest JSON path")
    p.add_argument("--report", default="data/offline_pyq_pdfs/mathongo/sync_report.json", help="Run report JSON path")
    p.add_argument(
        "--seed-profile",
        choices=("all", "mathongo", "external"),
        default="all",
        help="Choose crawl seeds: all, only mathongo, or only external solution sites",
    )
    p.add_argument(
        "--direct-link",
        action="append",
        default=[],
        help="Direct PDF/question URL to ingest immediately (repeatable)",
    )
    p.add_argument(
        "--direct-links-file",
        action="append",
        default=[],
        help="Path to txt/json file containing direct links to ingest (repeatable)",
    )
    p.add_argument(
        "--direct-only",
        action="store_true",
        help="Skip discovery crawl and process only direct links",
    )
    p.add_argument("--max-pages", type=int, default=240, help="Maximum MathonGo pages to crawl")
    p.add_argument("--max-pdfs", type=int, default=500, help="Maximum PDFs to download in one run")
    p.add_argument("--parse-workers", type=int, default=6, help="Concurrent OCR/parse/publish workers (1-10)")
    p.add_argument(
        "--resume-skip-existing",
        action="store_true",
        help="Resume run by skipping already-downloaded local files and continuing with remaining links",
    )
    p.add_argument(
        "--reverse-candidates",
        action="store_true",
        help="Process download candidates in reverse order (end-to-beginning)",
    )
    p.add_argument("--skip-download", action="store_true", help="Skip network download; reuse manifest entries")
    p.add_argument("--skip-parse", action="store_true", help="Skip parse/publish stage")
    return p


def main() -> None:
    args = build_parser().parse_args()
    out_dir = Path(args.out_dir)
    manifest_path = Path(args.manifest)
    report_path = Path(args.report)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    http_timeout = float(os.environ.get("PYQ_HTTP_TIMEOUT_S", "18"))
    http = Http(timeout_s=max(8.0, http_timeout))
    if args.seed_profile == "mathongo":
        active_seed_urls = list(MATHONGO_SEED_URLS)
    elif args.seed_profile == "external":
        active_seed_urls = list(EXTERNAL_SOLUTION_SEED_URLS)
    else:
        active_seed_urls = list(SEED_URLS)
    configured_direct_links: list[str] = []
    if args.seed_profile in {"all", "external"}:
        configured_direct_links.extend(EXTERNAL_DIRECT_PDF_URLS)
    configured_direct_links.extend([str(x).strip() for x in (args.direct_link or []) if str(x).strip()])
    configured_direct_links.extend(load_direct_links(list(args.direct_links_file or [])))
    direct_links: list[str] = []
    seen_direct: set[str] = set()
    for raw in configured_direct_links:
        key = normalize_url_for_dedupe(raw)
        if not key or key in seen_direct:
            continue
        seen_direct.add(key)
        direct_links.append(raw)

    downloaded: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    live_downloaded: list[dict[str, Any]] = []
    live_failures: list[dict[str, Any]] = []
    live_download_skip_existing = 0
    live_download_skip_failed = 0
    crawl_pages: list[str] = []
    candidates_count = 0
    crawl_pages_processed = 0
    crawl_candidates_found = 0
    parse_worker: ParsePublishWorker | None = None
    phase = "init"
    latest_download = ""
    latest_failure = ""
    live_lock = threading.Lock()
    report_write_lock = threading.Lock()
    last_live_emit_ts = 0.0
    last_report_error_ts = 0.0
    failed_links_path = out_dir / "failed_links.json"
    failed_link_keys: set[str] = set()

    if failed_links_path.exists():
        try:
            prior_failed_payload = json.loads(failed_links_path.read_text(encoding="utf-8"))
            if isinstance(prior_failed_payload, list):
                for row in prior_failed_payload:
                    key = normalize_url_for_dedupe(str(row))
                    if key:
                        failed_link_keys.add(key)
        except Exception:
            failed_link_keys = set()

    def emit_live_progress(*, force: bool = False) -> None:
        nonlocal last_live_emit_ts, last_report_error_ts
        now_ts = time.time()
        if not force and (now_ts - last_live_emit_ts) < LIVE_REPORT_MIN_INTERVAL_S:
            return
        if not force and (now_ts - last_report_error_ts) < REPORT_FAIL_BACKOFF_S:
            return
        if not report_write_lock.acquire(blocking=False):
            return
        last_live_emit_ts = now_ts
        try:
            with live_lock:
                phase_now = phase
                downloaded_count = len(live_downloaded)
                failure_count = len(live_failures)
                latest_download_now = latest_download
                latest_failure_now = latest_failure
                pages_count = len(crawl_pages)
                candidate_count = candidates_count
                pages_processed = crawl_pages_processed
                candidates_found = crawl_candidates_found
            crawl_progress_pct = 0.0
            if pages_count > 0:
                crawl_progress_pct = round((pages_processed / max(1, pages_count)) * 100.0, 2)
            parse_report_live: dict[str, Any] = {}
            parse_queue_live: dict[str, Any] = {}
            if parse_worker is not None:
                raw_parse_report = parse_worker.report()
                parse_report_live = {
                    "files_total": int(raw_parse_report.get("files_total") or 0),
                    "parsed_ok": int(raw_parse_report.get("parsed_ok") or 0),
                    "parsed_failed": int(raw_parse_report.get("parsed_failed") or 0),
                    "parse_question_count": int(raw_parse_report.get("parse_question_count") or 0),
                    "text_extract_parse_count": int(raw_parse_report.get("text_extract_parse_count") or 0),
                    "ocr_parse_count": int(raw_parse_report.get("ocr_parse_count") or 0),
                    "quality_retry_ocr": int(raw_parse_report.get("quality_retry_ocr") or 0),
                    "publish_success": int(raw_parse_report.get("publish_success") or 0),
                    "publish_no_new": int(raw_parse_report.get("publish_no_new") or 0),
                    "published_count": int(raw_parse_report.get("published_count") or 0),
                    "duplicates_skipped": int(raw_parse_report.get("duplicates_skipped") or 0),
                    "solutions_enriched_count": int(raw_parse_report.get("solutions_enriched_count") or 0),
                    "failures_count": len(raw_parse_report.get("failures") or []),
                    "per_file_count": len(raw_parse_report.get("per_file") or []),
                }
                parse_queue_live = parse_worker.stats()
            payload = {
                "status": "running",
                "generated_at": now_iso(),
                "phase": phase_now,
                "out_dir": str(out_dir),
                "manifest": str(manifest_path),
                "downloaded_count": downloaded_count,
                "download_failures_count": failure_count,
                "download_skipped_existing": int(live_download_skip_existing),
                "download_skipped_prior_failed": int(live_download_skip_failed),
                "download_candidates": candidate_count,
                "direct_links_count": len(direct_links),
                "pages_discovered_count": pages_count,
                "pages_processed_count": pages_processed,
                "crawl_progress_pct": crawl_progress_pct,
                "crawl_candidates_found": candidates_found,
                "latest_download": latest_download_now,
                "latest_failure": latest_failure_now,
                "parse_workers": int(args.parse_workers),
                "parse_queue": parse_queue_live,
                "parse_report": parse_report_live,
            }
            atomic_write_json(report_path, payload)
        except Exception as exc:
            last_report_error_ts = time.time()
            print(f"[warn] live report write skipped: {exc}")
        finally:
            report_write_lock.release()

    def handle_download(entry: dict[str, Any]) -> None:
        nonlocal latest_download
        with live_lock:
            live_downloaded.append(dict(entry))
            latest_download = str(entry.get("filename") or Path(str(entry.get("saved_path") or "")).name)
        if parse_worker is not None:
            parse_worker.submit(entry)
        emit_live_progress()

    def handle_failure(entry: dict[str, Any]) -> None:
        nonlocal latest_failure
        with live_lock:
            live_failures.append(dict(entry))
            latest_failure = str(entry.get("source_link") or entry.get("error") or "")
            key = normalize_url_for_dedupe(latest_failure)
            if key:
                failed_link_keys.add(key)
                try:
                    _atomic_failed = sorted(failed_link_keys)
                    _atomic_tmp = failed_links_path.with_suffix(".json.tmp")
                    _atomic_tmp.write_text(
                        json.dumps(_atomic_failed, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8",
                    )
                    _atomic_tmp.replace(failed_links_path)
                except Exception:
                    pass
        emit_live_progress()

    def handle_skip(entry: dict[str, Any]) -> None:
        nonlocal latest_failure, live_download_skip_existing, live_download_skip_failed
        reason = str(entry.get("reason") or "")
        with live_lock:
            if reason in {"existing_file", "existing_path"}:
                live_download_skip_existing += 1
            elif reason == "prior_failed_link":
                live_download_skip_failed += 1
            latest_failure = str(entry.get("source_link") or latest_failure or "")
        emit_live_progress()

    def handle_collect_progress(done: int, total: int, candidate_found: int) -> None:
        nonlocal crawl_pages_processed, crawl_candidates_found
        with live_lock:
            crawl_pages_processed = int(done)
            if total > 0 and len(crawl_pages) != total:
                # Keep live payload consistent even if page list is rebuilt.
                if len(crawl_pages) < total:
                    crawl_pages.extend([""] * (total - len(crawl_pages)))
            crawl_candidates_found = int(candidate_found)
        emit_live_progress()

    if not args.skip_parse:
        parse_worker = ParsePublishWorker(workers=args.parse_workers, on_progress=emit_live_progress)
        parse_worker.start()

    if args.skip_download:
        with live_lock:
            phase = "load_manifest"
        emit_live_progress(force=True)
        if manifest_path.exists():
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            downloaded = [x for x in (payload.get("downloads") or []) if isinstance(x, dict)]
            failures = [x for x in (payload.get("download_failures") or []) if isinstance(x, dict)]
            with live_lock:
                live_downloaded[:] = [dict(x) for x in downloaded]
                live_failures[:] = [dict(x) for x in failures]
        else:
            print("manifest missing; nothing to parse")
            downloaded = []
        if parse_worker is not None and downloaded:
            with live_lock:
                phase = "queue_parse_jobs"
            emit_live_progress(force=True)
            print(f"[phase] queueing {len(downloaded)} manifest entries for parse/publish")
            for row in downloaded:
                parse_worker.submit(row)
            with live_lock:
                phase = "drain_parse_queue"
            emit_live_progress(force=True)
            print("[phase] waiting for parse/publish worker to finish queued manifest entries")
            parse_worker.close()
        emit_live_progress(force=True)
    else:
        candidates: list[DownloadCandidate] = []
        if not args.direct_only:
            with live_lock:
                phase = "discover_pages"
            emit_live_progress(force=True)
            print("[phase] discovering pages")
            crawl_pages = discover_pages(http, active_seed_urls, max_pages=max(1, args.max_pages))
            print(f"[phase] discovered pages: {len(crawl_pages)}")
            emit_live_progress(force=True)

            with live_lock:
                phase = "collect_links"
            emit_live_progress(force=True)
            print("[phase] collecting download links")
            # Stop link collection once we have enough links to satisfy max_pdfs.
            collect_target = max(400, int(max(1, args.max_pdfs)))
            candidates = collect_download_candidates(
                http,
                crawl_pages,
                target_candidates=collect_target,
                on_progress=handle_collect_progress,
            )
            print(f"[phase] crawl candidates: {len(candidates)}")
            emit_live_progress(force=True)
        else:
            print("[phase] direct-only mode: skipping discovery crawl")

        direct_candidates = build_direct_candidates(direct_links)
        if direct_candidates:
            print(f"[phase] direct-link candidates: {len(direct_candidates)}")
        merged_candidates: list[DownloadCandidate] = []
        seen_candidate_links: set[str] = set()
        for cand in [*candidates, *direct_candidates]:
            key = normalize_url_for_dedupe(cand.source_link)
            if key in seen_candidate_links:
                continue
            seen_candidate_links.add(key)
            merged_candidates.append(cand)
        candidates = merged_candidates
        if args.reverse_candidates:
            candidates = list(reversed(candidates))
            print(f"[phase] reverse-candidates enabled; processing {len(candidates)} candidates end-to-beginning")
        candidates_count = len(candidates)
        print(f"[phase] total download candidates: {candidates_count}")
        emit_live_progress(force=True)

        with live_lock:
            phase = "download_parse_publish_streaming"
        emit_live_progress(force=True)
        print("[phase] downloading pdfs")
        try:
            existing_names: set[str] = set()
            if args.resume_skip_existing:
                for p in out_dir.iterdir():
                    if not p.is_file():
                        continue
                    if p.name in {"manifest.json", "sync_report.json", "sync_report.json.tmp"}:
                        continue
                    existing_names.add(p.name)
            downloaded, failures, dl_stats = download_all(
                http,
                candidates,
                out_dir=out_dir,
                max_pdfs=max(1, args.max_pdfs),
                skip_existing_files=bool(args.resume_skip_existing),
                existing_filenames=existing_names,
                prior_failed_links=failed_link_keys,
                on_download=handle_download,
                on_failure=handle_failure,
                on_skip=handle_skip,
            )
            with live_lock:
                live_downloaded[:] = [dict(x) for x in downloaded]
                live_failures[:] = [dict(x) for x in failures]
                live_download_skip_existing = int(dl_stats.get("skipped_existing") or 0)
                live_download_skip_failed = int(dl_stats.get("skipped_prior_failed") or 0)
            emit_live_progress(force=True)
        finally:
            if parse_worker is not None:
                with live_lock:
                    phase = "drain_parse_queue"
                emit_live_progress(force=True)
                print("[phase] waiting for parse/publish worker to finish queued PDFs")
                parse_worker.close()
                emit_live_progress(force=True)

        manifest = {
            "generated_at": now_iso(),
            "seed_urls": active_seed_urls,
            "direct_links": direct_links,
            "pages_discovered": crawl_pages,
            "download_candidates": candidates_count,
            "downloads": downloaded,
            "download_failures": failures,
        }
        atomic_write_json(manifest_path, manifest)
        print(f"[phase] manifest written: {manifest_path}")

    parse_report: dict[str, Any] = {}
    if parse_worker is not None:
        parse_report = parse_worker.report()
    elif not args.skip_parse and downloaded:
        with live_lock:
            phase = "parse_publish"
        emit_live_progress(force=True)
        print("[phase] parsing and publishing")
        parse_report = asyncio.run(parse_and_publish(downloaded))
        emit_live_progress(force=True)
    elif args.skip_parse:
        print("[phase] parse skipped")
    else:
        print("[phase] no downloaded files to parse")

    final_report = {
        "status": "done",
        "generated_at": now_iso(),
        "out_dir": str(out_dir),
        "manifest": str(manifest_path),
        "parse_workers": int(args.parse_workers),
        "downloaded_count": len(downloaded),
        "download_failures": failures,
        "parse_report": parse_report,
    }
    atomic_write_json(report_path, final_report)
    print(f"[done] report: {report_path}")
    print(json.dumps({
        "downloaded_count": len(downloaded),
        "download_failures": len(failures),
        "parsed_ok": int(parse_report.get("parsed_ok") or 0),
        "text_extract_parse_count": int(parse_report.get("text_extract_parse_count") or 0),
        "ocr_parse_count": int(parse_report.get("ocr_parse_count") or 0),
        "quality_retry_ocr": int(parse_report.get("quality_retry_ocr") or 0),
        "published_count": int(parse_report.get("published_count") or 0),
        "duplicates_skipped": int(parse_report.get("duplicates_skipped") or 0),
        "solutions_enriched_count": int(parse_report.get("solutions_enriched_count") or 0),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
