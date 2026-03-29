from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class SQLiteJsonBlobStore:
    """Small durable JSON blob store for app/runtime authority data."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self._db_path),
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA temp_store=MEMORY")
        return connection

    @property
    def path(self) -> Path:
        return self._db_path

    def _initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS json_blobs (
                    blob_key TEXT PRIMARY KEY,
                    json_value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )

    def read_json(self, blob_key: str) -> Any | None:
        key = str(blob_key or "").strip()
        if not key:
            return None
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT json_value FROM json_blobs WHERE blob_key = ?",
                (key,),
            ).fetchone()
        if row is None or not row[0]:
            return None
        try:
            return json.loads(str(row[0]))
        except Exception:
            return None

    def write_json(self, blob_key: str, value: Any) -> None:
        key = str(blob_key or "").strip()
        if not key:
            raise ValueError("blob_key is required")
        payload = json.dumps(value, ensure_ascii=True, indent=2)
        updated_at = int(time.time() * 1000)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO json_blobs (blob_key, json_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(blob_key) DO UPDATE
                SET json_value = excluded.json_value,
                    updated_at = excluded.updated_at
                """,
                (key, payload, updated_at),
            )

    def has_key(self, blob_key: str) -> bool:
        key = str(blob_key or "").strip()
        if not key:
            return False
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM json_blobs WHERE blob_key = ? LIMIT 1",
                (key,),
            ).fetchone()
        return row is not None
