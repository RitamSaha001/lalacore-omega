import asyncpg
import os
import logging
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

class Database:
    _pool: Optional[asyncpg.Pool] = None
    _MIGRATION_LOCK_KEY = 22820740512026

    @classmethod
    async def init(cls):
        if cls._pool is not None:
            return  # already initialized
        pool_kwargs = {
            "min_size": 5,
            "max_size": 20,
            "command_timeout": 30,
        }
        database_url = (os.getenv("DATABASE_URL") or "").strip()
        if database_url:
            cls._pool = await asyncpg.create_pool(
                dsn=database_url,
                **pool_kwargs,
            )
        else:
            cls._pool = await asyncpg.create_pool(
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", 5432)),
                **pool_kwargs,
            )

        await cls._run_startup_migrations()
        logger.info("✅ Postgres connection pool created")

    @classmethod
    async def _run_startup_migrations(cls):
        if cls._pool is None:
            raise RuntimeError("Database pool not initialized for migrations.")

        migrations_dir = Path(__file__).resolve().parents[2] / "migrations"
        if not migrations_dir.exists():
            logger.warning("Migrations directory missing at %s", migrations_dir)
            return

        sql_files = sorted(migrations_dir.glob("*.sql"))
        if not sql_files:
            return

        async with cls._pool.acquire() as conn:
            await conn.execute(
                f"SELECT pg_advisory_lock({cls._MIGRATION_LOCK_KEY})"
            )
            try:
                for sql_file in sql_files:
                    sql = sql_file.read_text(encoding="utf-8").strip()
                    if not sql:
                        continue
                    try:
                        await conn.execute(sql)
                        logger.info("Applied migration: %s", sql_file.name)
                    except (
                        asyncpg.exceptions.DuplicateObjectError,
                        asyncpg.exceptions.DuplicateTableError,
                        asyncpg.exceptions.UniqueViolationError,
                    ):
                        # Handles concurrent init races safely.
                        logger.info(
                            "Migration already applied concurrently: %s",
                            sql_file.name,
                        )
            finally:
                await conn.execute(
                    f"SELECT pg_advisory_unlock({cls._MIGRATION_LOCK_KEY})"
                )

    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        if cls._pool is None:
            raise RuntimeError("Database not initialized. Call Database.init() first.")
        return cls._pool

    @classmethod
    async def close(cls):
        if cls._pool:
            await cls._pool.close()
            logger.info("🛑 Postgres connection pool closed")
            cls._pool = None

    @classmethod
    async def health_check(cls) -> bool:
        if cls._pool is None:
            return False

        try:
            async with cls._pool.acquire() as conn:
                await conn.execute("SELECT 1;")
            return True
        except Exception as e:
            logger.error(f"DB health check failed: {e}")
            return False


async def init_db():
    """
    Backward-compatible bootstrap entrypoint.
    """
    await Database.init()


async def close_db():
    """
    Backward-compatible shutdown entrypoint.
    """
    await Database.close()
