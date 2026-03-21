import asyncio

from app.arena.arena_session import ArenaSessionManager
from app.arena.entropy import compute_entropy
from app.arena.bradley_terry import BradleyTerryEngine
from app.arena.pairwise_engine import PairwiseEngine
from app.arena.bayesian_aggregator import BayesianAggregator


class ArenaOrchestrator:

    def __init__(
        self,
        db,
        reasoning_parser,
        similarity_engine=None
    ):
        self.db = db
        self.reasoning_parser = reasoning_parser
        self.similarity_engine = similarity_engine

        self.session_manager = ArenaSessionManager()

    async def run(
        self,
        question_id,
        subject,
        difficulty,
        responses
    ):
        """
        responses must contain:
            provider
            final_answer
            critic_score
            deterministic_pass
            confidence
            skill
            reasoning (raw text)
        """

        # 1️⃣ Compute entropy
        entropy = compute_entropy(responses)

        pool = await self.session_manager._get_pool_with_retry()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 2️⃣ Create session
                session_id = await self.session_manager.create_session(
                    question_id=question_id,
                    subject=subject,
                    difficulty=difficulty,
                    entropy=entropy,
                    conn=conn,
                )

                # 3️⃣ Parse reasoning → structured graph
                for r in responses:
                    graph = await self.reasoning_parser.parse_and_store(
                        session_id=session_id,
                        provider=r["provider"],
                        reasoning_text=r["reasoning"],
                        conn=conn,
                    )
                    r["graph"] = graph

                # 4️⃣ Insert participants (initial state)
                for r in responses:
                    await self.session_manager.add_participant(
                        session_id=session_id,
                        provider=r["provider"],
                        final_answer=r["final_answer"],
                        deterministic_pass=r["deterministic_pass"],
                        critic_score=r["critic_score"],
                        confidence=r["confidence"],
                        mu=25.0,        # replace with real skill engine later
                        sigma=8.0,
                        conn=conn,
                    )

                # 5️⃣ Run Pairwise Tournament
                bt_engine = BradleyTerryEngine()
                pairwise_engine = PairwiseEngine(
                    bt_engine,
                    similarity_engine=self.similarity_engine
                )

                thetas, matches, pairwise_details = pairwise_engine.run(
                    responses,
                    entropy=entropy,
                    return_details=True
                )

                # 6️⃣ Compute Bayesian Posterior
                aggregator = BayesianAggregator()
                bayesian_details = aggregator.compute(
                    responses,
                    thetas,
                    uncertainties=pairwise_details.get("uncertainties", {}),
                    entropy=entropy,
                    return_details=True
                )
                posteriors = bayesian_details["posteriors"]

                winner = max(posteriors, key=posteriors.get)

                # 7️⃣ Persist theta + posterior
                for r in responses:
                    await self.session_manager.update_theta_and_posterior(
                        session_id=session_id,
                        provider=r["provider"],
                        local_theta=thetas[r["provider"]],
                        posterior=posteriors[r["provider"]],
                        won=(r["provider"] == winner),
                        conn=conn,
                    )

                # 8️⃣ Log pairwise matches
                await self.session_manager.log_pairwise_matches(
                    session_id=session_id,
                    matches=matches,
                    conn=conn,
                )

        return {
            "session_id": session_id,
            "winner": winner,
            "posteriors": posteriors,
            "thetas": thetas,
            "entropy": entropy,
            "winner_margin": bayesian_details.get("winner_margin", 0.0),
            "arena_confidence": bayesian_details.get("confidence", 0.0),
            "pairwise": pairwise_details
        }

    async def _retry(self, fn, max_attempts: int = 3, base_delay_s: float = 0.2):
        last = None
        for attempt in range(1, max_attempts + 1):
            try:
                return await fn()
            except Exception as exc:
                last = exc
                if attempt < max_attempts:
                    await asyncio.sleep(base_delay_s * (2 ** (attempt - 1)))
        raise last
