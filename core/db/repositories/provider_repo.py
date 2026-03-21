from typing import Optional, List, Tuple
from core.db.connection import Database


class ProviderRepository:

    async def get_by_name(self, name: str) -> Optional[Tuple[int, str]]:
        """
        Returns (id, name) or None
        """
        pool = await Database.get_pool()

        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, name
                FROM providers
                WHERE name = $1
            """, name)

            if row:
                return row["id"], row["name"]

            return None

    async def create_if_not_exists(self, name: str) -> int:
        """
        Ensures provider exists and returns its ID.
        """
        pool = await Database.get_pool()

        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO providers (name)
                VALUES ($1)
                ON CONFLICT (name)
                DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, name)

            return row["id"]

    async def get_all_active(self) -> List[Tuple[int, str]]:
        """
        Returns list of (id, name) for active providers.
        """
        pool = await Database.get_pool()

        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, name
                FROM providers
                WHERE is_active = TRUE
            """)

            return [(row["id"], row["name"]) for row in rows]