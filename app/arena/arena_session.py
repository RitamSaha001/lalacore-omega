import asyncio

from core.db.connection import Database


class ArenaSessionManager:

    async def create_session(self, question_id, subject, difficulty, entropy, conn=None):

        if conn is not None:
            row = await conn.fetchrow(
                """
                INSERT INTO arena_sessions
                (question_id, subject, difficulty, entropy)
                VALUES ($1,$2,$3,$4)
                RETURNING id
                """,
                question_id,
                subject,
                difficulty,
                entropy
            )
            return row["id"]

        pool = await self._get_pool_with_retry()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO arena_sessions
                (question_id, subject, difficulty, entropy)
                VALUES ($1,$2,$3,$4)
                RETURNING id
                """,
                question_id,
                subject,
                difficulty,
                entropy
            )

        return row["id"]

    async def add_participant(
        self,
        session_id,
        provider,
        final_answer,
        deterministic_pass,
        critic_score,
        confidence,
        mu,
        sigma,
        conn=None,
    ):

        if conn is not None:
            await conn.execute(
                """
                INSERT INTO arena_participants
                (
                    session_id,
                    provider,
                    final_answer,
                    deterministic_pass,
                    critic_score,
                    confidence,
                    global_skill_mu,
                    global_skill_sigma
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                session_id,
                provider,
                final_answer,
                deterministic_pass,
                critic_score,
                confidence,
                mu,
                sigma
            )
            return

        pool = await self._get_pool_with_retry()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO arena_participants
                (
                    session_id,
                    provider,
                    final_answer,
                    deterministic_pass,
                    critic_score,
                    confidence,
                    global_skill_mu,
                    global_skill_sigma
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                session_id,
                provider,
                final_answer,
                deterministic_pass,
                critic_score,
                confidence,
                mu,
                sigma
            )

    async def update_theta_and_posterior(
        self,
        session_id,
        provider,
        local_theta,
        posterior,
        won,
        conn=None,
    ):

        if conn is not None:
            await conn.execute(
                """
                UPDATE arena_participants
                SET local_theta=$1,
                    bayesian_posterior=$2,
                    won=$3
                WHERE session_id=$4
                  AND provider=$5
                """,
                local_theta,
                posterior,
                won,
                session_id,
                provider
            )
            return

        pool = await self._get_pool_with_retry()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE arena_participants
                SET local_theta=$1,
                    bayesian_posterior=$2,
                    won=$3
                WHERE session_id=$4
                  AND provider=$5
                """,
                local_theta,
                posterior,
                won,
                session_id,
                provider
            )

    async def log_pairwise_matches(self, session_id, matches, conn=None):

        if conn is not None:
            for m in matches:
                await conn.execute(
                    """
                    INSERT INTO arena_pairwise
                    (
                        session_id,
                        provider_a,
                        provider_b,
                        winner,
                        score_diff,
                        similarity
                    )
                    VALUES ($1,$2,$3,$4,$5,$6)
                    """,
                    session_id,
                    m["provider_a"],
                    m["provider_b"],
                    m["winner"],
                    m["score_diff"],
                    m.get("similarity", 0.0)
                )
            return

        pool = await self._get_pool_with_retry()
        async with pool.acquire() as conn:
            for m in matches:
                await conn.execute(
                    """
                    INSERT INTO arena_pairwise
                    (
                        session_id,
                        provider_a,
                        provider_b,
                        winner,
                        score_diff,
                        similarity
                    )
                    VALUES ($1,$2,$3,$4,$5,$6)
                    """,
                    session_id,
                    m["provider_a"],
                    m["provider_b"],
                    m["winner"],
                    m["score_diff"],
                    m.get("similarity", 0.0)
                )

    async def _get_pool_with_retry(self, max_attempts: int = 3, base_delay_s: float = 0.2):
        last = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await Database.get_pool()
            except Exception as exc:
                last = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_delay_s * (2 ** (attempt - 1)))
        raise last
