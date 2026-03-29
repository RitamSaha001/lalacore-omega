import os
import unittest
from unittest.mock import AsyncMock, patch

from core.db.connection import Database


class DatabaseConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        Database._pool = None

    class _AcquireContext:
        def __init__(self, conn) -> None:
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def __init__(self, conn) -> None:
            self._conn = conn
            self.closed = False

        def acquire(self):
            return DatabaseConnectionTests._AcquireContext(self._conn)

        async def close(self) -> None:
            self.closed = True

    async def test_init_prefers_database_url(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://user:pass@db.example.com:5432/appdb",
                "DB_USER": "legacy_user",
                "DB_PASSWORD": "legacy_pass",
                "DB_NAME": "legacy_db",
                "DB_HOST": "legacy-host",
                "DB_PORT": "5433",
            },
            clear=False,
        ), patch(
            "core.db.connection.asyncpg.create_pool",
            new=AsyncMock(return_value=object()),
        ) as create_pool, patch(
            "core.db.connection.Database._run_startup_migrations",
            new=AsyncMock(return_value=None),
        ):
            await Database.init()

        create_pool.assert_awaited_once()
        _, kwargs = create_pool.await_args
        self.assertEqual(
            kwargs.get("dsn"),
            "postgresql://user:pass@db.example.com:5432/appdb",
        )
        self.assertNotIn("user", kwargs)
        self.assertNotIn("host", kwargs)

    async def test_health_check_attempts_lazy_init_with_database_url(self) -> None:
        conn = AsyncMock()
        pool = self._Pool(conn)

        async def _init_side_effect() -> None:
            Database._pool = pool

        with patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://user:pass@db.example.com:5432/appdb"},
            clear=False,
        ), patch(
            "core.db.connection.Database.init",
            new=AsyncMock(side_effect=_init_side_effect),
        ) as init_mock:
            self.assertTrue(await Database.health_check())

        init_mock.assert_awaited_once()
        conn.execute.assert_awaited_once_with("SELECT 1;")

    async def test_health_check_retries_after_query_failure(self) -> None:
        broken_conn = AsyncMock()
        broken_conn.execute = AsyncMock(side_effect=RuntimeError("boom"))
        healthy_conn = AsyncMock()
        broken_pool = self._Pool(broken_conn)
        healthy_pool = self._Pool(healthy_conn)
        Database._pool = broken_pool

        async def _init_side_effect() -> None:
            Database._pool = healthy_pool

        with patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://user:pass@db.example.com:5432/appdb"},
            clear=False,
        ), patch(
            "core.db.connection.Database.init",
            new=AsyncMock(side_effect=_init_side_effect),
        ) as init_mock:
            self.assertTrue(await Database.health_check())

        self.assertTrue(broken_pool.closed)
        init_mock.assert_awaited_once()
        healthy_conn.execute.assert_awaited_once_with("SELECT 1;")


if __name__ == "__main__":
    unittest.main()
