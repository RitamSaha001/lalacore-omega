import json
import os
import socket
import threading
import time
from typing import Optional


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def start_discovery_responder(
    *,
    port: int = 37999,
    http_port: int = 8000,
    enabled: Optional[bool] = None,
) -> None:
    if enabled is None:
        enabled = _bool_env("LC9_DISCOVERY_ENABLED", True)
    if not enabled:
        return
    thread = threading.Thread(
        target=_serve,
        args=(port, http_port),
        daemon=True,
        name="lc9-discovery",
    )
    thread.start()


def _serve(port: int, http_port: int) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(("", port))

    payload = json.dumps(
        {
            "ok": True,
            "port": int(http_port),
            "auth_path": "/auth/action",
            "ts": time.time(),
        }
    ).encode("utf-8")

    while True:
        try:
            data, addr = sock.recvfrom(2048)
        except OSError:
            continue
        if not data:
            continue
        try:
            message = data.decode("utf-8", errors="ignore").strip()
        except Exception:
            message = ""
        if not message.startswith("LC9_DISCOVER"):
            continue
        try:
            sock.sendto(payload, addr)
        except OSError:
            continue
