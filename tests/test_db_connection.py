import os
import unittest
from unittest.mock import AsyncMock, patch

from core.db.connection import Database


class DatabaseConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        Database._pool = None

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


if __name__ == "__main__":
    unittest.main()
