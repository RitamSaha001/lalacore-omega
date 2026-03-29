from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx
import requests


_STATUS_MARKER = "__LC9_HTTP_STATUS__:"
_DNS_ERROR_MARKERS = (
    "could not resolve host",
    "nodename nor servname provided",
    "name or service not known",
    "temporary failure in name resolution",
    "failed to resolve",
    "[errno 8]",
)


@dataclass
class ResilientHttpResponse:
    status_code: int
    text: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    transport: str = "httpx"
    request_method: str = "GET"

    def json(self) -> Any:
        return json.loads(self.text or "")

    def raise_for_status(self) -> None:
        if 400 <= int(self.status_code):
            request = httpx.Request(self.request_method.upper(), self.url)
            response = httpx.Response(
                status_code=int(self.status_code),
                request=request,
                text=self.text,
                headers=self.headers,
            )
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code} for {self.request_method.upper()} {self.url}",
                request=request,
                response=response,
            )


def looks_like_dns_failure(error: Any) -> bool:
    text = str(error or "").lower()
    return any(marker in text for marker in _DNS_ERROR_MARKERS)


def _curl_dns_fallback_enabled() -> bool:
    token = str(os.getenv("LC9_ENABLE_CURL_DNS_FALLBACK", "1")).strip().lower()
    return token not in {"0", "false", "no", "off"}


def _normalize_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(headers, Mapping):
        return out
    for key, value in headers.items():
        k = str(key or "").strip()
        v = str(value or "").strip()
        if k and v:
            out[k] = v
    return out


def _merge_url_params(url: str, params: Mapping[str, Any] | None) -> str:
    if not isinstance(params, Mapping) or not params:
        return url
    parsed = urlparse(str(url or ""))
    existing = list(parse_qsl(parsed.query, keep_blank_values=True))
    extra = [(str(k), str(v)) for k, v in params.items() if str(k or "").strip()]
    merged = urlencode(existing + extra)
    return urlunparse(parsed._replace(query=merged))


def _curl_timeout_arg(timeout_s: float) -> str:
    return str(max(3, int(math.ceil(float(timeout_s)))))


def _curl_connect_timeout_arg(timeout_s: float) -> str:
    return str(max(2, int(math.ceil(max(1.0, float(timeout_s) * 0.4)))))


def _prepare_body(
    *,
    headers: dict[str, str],
    json_body: Any = None,
    data: Any = None,
    files: Mapping[str, Any] | None = None,
) -> tuple[dict[str, str], bytes | None, Mapping[str, Any] | None]:
    prepared_headers = dict(headers)
    if files:
        prepared_headers.pop("Content-Type", None)
        return prepared_headers, None, files
    if json_body is not None:
        prepared_headers.setdefault("Content-Type", "application/json")
        return prepared_headers, json.dumps(json_body).encode("utf-8"), None
    if data is None:
        return prepared_headers, None, None
    if isinstance(data, bytes):
        return prepared_headers, data, None
    if isinstance(data, str):
        return prepared_headers, data.encode("utf-8"), None
    if isinstance(data, Mapping):
        prepared_headers.setdefault(
            "Content-Type", "application/x-www-form-urlencoded"
        )
        return prepared_headers, urlencode(
            [(str(k), str(v)) for k, v in data.items()]
        ).encode("utf-8"), None
    return prepared_headers, str(data).encode("utf-8"), None


def _parse_curl_output(stdout: bytes, url: str, method: str) -> ResilientHttpResponse:
    text = stdout.decode("utf-8", errors="ignore")
    match = re.search(rf"\n{re.escape(_STATUS_MARKER)}(\d{{3}})\s*$", text)
    status_code = 0
    body = text
    if match:
        status_code = int(match.group(1))
        body = text[: match.start()]
    return ResilientHttpResponse(
        status_code=status_code,
        text=body,
        url=url,
        transport="curl",
        request_method=method.upper(),
    )


def _curl_file_parts(files: Mapping[str, Any]) -> tuple[list[str], list[Path]]:
    args: list[str] = []
    temp_paths: list[Path] = []
    for field_name, value in files.items():
        if not isinstance(value, tuple) or len(value) < 2:
            continue
        filename = str(value[0] or "upload.bin")
        content = value[1]
        content_type = str(value[2] or "").strip() if len(value) > 2 else ""
        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
        else:
            content_bytes = bytes(content or b"")
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(content_bytes)
            temp_paths.append(Path(tmp.name))
        part = f"{field_name}=@{temp_paths[-1]};filename={filename}"
        if content_type:
            part += f";type={content_type}"
        args.extend(["-F", part])
    return args, temp_paths


async def _curl_request_async(
    *,
    method: str,
    url: str,
    headers: Mapping[str, Any] | None = None,
    json_body: Any = None,
    data: Any = None,
    files: Mapping[str, Any] | None = None,
    timeout_s: float = 30.0,
    follow_redirects: bool = False,
) -> ResilientHttpResponse:
    curl_bin = shutil.which("curl")
    if not curl_bin:
        raise RuntimeError("curl_unavailable")
    normalized_headers = _normalize_headers(headers)
    normalized_headers, body_bytes, file_parts = _prepare_body(
        headers=normalized_headers,
        json_body=json_body,
        data=data,
        files=files,
    )
    file_args, temp_paths = _curl_file_parts(file_parts or {})
    cmd = [
        curl_bin,
        "-sS",
        "-X",
        method.upper(),
        "--max-time",
        _curl_timeout_arg(timeout_s),
        "--connect-timeout",
        _curl_connect_timeout_arg(timeout_s),
        "-w",
        f"\n{_STATUS_MARKER}%{{http_code}}",
    ]
    if follow_redirects:
        cmd.append("-L")
    for key, value in normalized_headers.items():
        cmd.extend(["-H", f"{key}: {value}"])
    if file_args:
        cmd.extend(file_args)
    elif body_bytes is not None:
        cmd.extend(["--data-binary", "@-"])
    cmd.append(url)
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE if body_bytes is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            process.communicate(body_bytes),
            timeout=max(4.0, float(timeout_s) + 5.0),
        )
        if int(process.returncode or 0) != 0:
            err = stderr.decode("utf-8", errors="ignore").strip()
            raise RuntimeError(err or f"curl_request_failed:{process.returncode}")
        return _parse_curl_output(stdout, url=url, method=method)
    finally:
        for path in temp_paths:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


def _curl_request_sync(
    *,
    method: str,
    url: str,
    headers: Mapping[str, Any] | None = None,
    json_body: Any = None,
    data: Any = None,
    files: Mapping[str, Any] | None = None,
    timeout_s: float = 30.0,
    follow_redirects: bool = False,
) -> ResilientHttpResponse:
    curl_bin = shutil.which("curl")
    if not curl_bin:
        raise RuntimeError("curl_unavailable")
    normalized_headers = _normalize_headers(headers)
    normalized_headers, body_bytes, file_parts = _prepare_body(
        headers=normalized_headers,
        json_body=json_body,
        data=data,
        files=files,
    )
    file_args, temp_paths = _curl_file_parts(file_parts or {})
    cmd = [
        curl_bin,
        "-sS",
        "-X",
        method.upper(),
        "--max-time",
        _curl_timeout_arg(timeout_s),
        "--connect-timeout",
        _curl_connect_timeout_arg(timeout_s),
        "-w",
        f"\n{_STATUS_MARKER}%{{http_code}}",
    ]
    if follow_redirects:
        cmd.append("-L")
    for key, value in normalized_headers.items():
        cmd.extend(["-H", f"{key}: {value}"])
    if file_args:
        cmd.extend(file_args)
    elif body_bytes is not None:
        cmd.extend(["--data-binary", "@-"])
    cmd.append(url)
    try:
        result = subprocess.run(
            cmd,
            input=body_bytes,
            capture_output=True,
            timeout=max(4.0, float(timeout_s) + 5.0),
            check=False,
        )
        if int(result.returncode or 0) != 0:
            err = result.stderr.decode("utf-8", errors="ignore").strip()
            raise RuntimeError(err or f"curl_request_failed:{result.returncode}")
        return _parse_curl_output(result.stdout, url=url, method=method)
    finally:
        for path in temp_paths:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


async def request_async(
    method: str,
    url: str,
    *,
    headers: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    data: Any = None,
    files: Mapping[str, Any] | None = None,
    timeout_s: float = 30.0,
    follow_redirects: bool = False,
    allow_curl_dns_fallback: bool | None = None,
) -> ResilientHttpResponse:
    resolved_url = _merge_url_params(url, params)
    try:
        async with httpx.AsyncClient(
            timeout=float(timeout_s),
            follow_redirects=bool(follow_redirects),
        ) as client:
            response = await client.request(
                method.upper(),
                resolved_url,
                headers=_normalize_headers(headers),
                json=json_body,
                content=data if isinstance(data, (bytes, str)) else None,
                data=data if isinstance(data, Mapping) else None,
            )
        return ResilientHttpResponse(
            status_code=int(response.status_code),
            text=response.text or "",
            url=str(response.request.url),
            headers={str(k): str(v) for k, v in response.headers.items()},
            transport="httpx",
            request_method=method.upper(),
        )
    except Exception as exc:
        should_fallback = (
            _curl_dns_fallback_enabled()
            if allow_curl_dns_fallback is None
            else bool(allow_curl_dns_fallback)
        )
        if not should_fallback or not looks_like_dns_failure(exc):
            raise
        return await _curl_request_async(
            method=method,
            url=resolved_url,
            headers=headers,
            json_body=json_body,
            data=data,
            files=files,
            timeout_s=timeout_s,
            follow_redirects=follow_redirects,
        )


def request_sync(
    method: str,
    url: str,
    *,
    headers: Mapping[str, Any] | None = None,
    params: Mapping[str, Any] | None = None,
    json_body: Any = None,
    data: Any = None,
    files: Mapping[str, Any] | None = None,
    timeout_s: float = 30.0,
    follow_redirects: bool = False,
    allow_curl_dns_fallback: bool | None = None,
) -> ResilientHttpResponse:
    resolved_url = _merge_url_params(url, params)
    try:
        response = requests.request(
            method.upper(),
            resolved_url,
            headers=_normalize_headers(headers),
            json=json_body,
            data=data,
            files=files,
            timeout=float(timeout_s),
            allow_redirects=bool(follow_redirects),
        )
        return ResilientHttpResponse(
            status_code=int(response.status_code),
            text=response.text or "",
            url=str(response.url),
            headers={str(k): str(v) for k, v in response.headers.items()},
            transport="requests",
            request_method=method.upper(),
        )
    except Exception as exc:
        should_fallback = (
            _curl_dns_fallback_enabled()
            if allow_curl_dns_fallback is None
            else bool(allow_curl_dns_fallback)
        )
        if not should_fallback or not looks_like_dns_failure(exc):
            raise
        return _curl_request_sync(
            method=method,
            url=resolved_url,
            headers=headers,
            json_body=json_body,
            data=data,
            files=files,
            timeout_s=timeout_s,
            follow_redirects=follow_redirects,
        )
