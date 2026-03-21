from typing import Optional, Tuple, List
from core.db.connection import Database


class SkillRepository:

    async def get_skill(
        self,
        provider_id: int,
        subject: str,
        difficulty: int
    ) -> Optional[Tuple[float, float]]:
        """
        Returns (mu, sigma) for a provider.
        Returns None if no skill record exists.
        """
        pool = await Database.get_pool()

        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT mu, sigma
                FROM provider_skill
                WHERE provider_id = $1
                AND subject = $2
                AND difficulty = $3
            """, provider_id, subject, difficulty)

            if row:
                return row["mu"], row["sigma"]

            return None

    async def upsert_skill(
        self,
        provider_id: int,
        subject: str,
        difficulty: int,
        mu: float,
        sigma: float
    ) -> None:
        """
        Inserts or updates a provider skill rating.
        Automatically increments match count on update.
        """
        pool = await Database.get_pool()

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO provider_skill
                (provider_id, subject, difficulty, mu, sigma, matches)
                VALUES ($1, $2, $3, $4, $5, 1)
                ON CONFLICT (provider_id, subject, difficulty)
                DO UPDATE SET
                    mu = $4,
                    sigma = $5,
                    matches = provider_skill.matches + 1,
                    last_updated = NOW()
            """, provider_id, subject, difficulty, mu, sigma)

    async def get_all_skills_for_subject(
        self,
        subject: str,
        difficulty: int
    ) -> List[Tuple[int, float, float]]:
        """
        Returns list of (provider_id, mu, sigma)
        for routing comparisons.
        """
        pool = await Database.get_pool()

        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT provider_id, mu, sigma
                FROM provider_skill
                WHERE subject = $1
                AND difficulty = $2
            """, subject, difficulty)

            return [
                (row["provider_id"], row["mu"], row["sigma"])
                for row in rows
            ]

    async def reset_skill(
        self,
        provider_id: int,
        subject: str,
        difficulty: int
    ) -> None:
        """
        Resets a provider skill to default.
        Useful for testing or catastrophic drift correction.
        """
        pool = await Database.get_pool()

        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE provider_skill
                SET mu = 25.0,
                    sigma = 8.333,
                    matches = 0,
                    last_updated = NOW()
                WHERE provider_id = $1
                AND subject = $2
                AND difficulty = $3
            """, provider_id, subject, difficulty)