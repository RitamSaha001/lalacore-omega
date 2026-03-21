from typing import Dict, Any

from core.orchestration.router import Router
from core.orchestration.skill_engine import SkillEngine
from core.db.repositories.provider_repo import ProviderRepository

from core.providers.registry import ProviderRegistry


class Orchestrator:

    def __init__(self):
        self.router = Router()
        self.skill_engine = SkillEngine()
        self.provider_repo = ProviderRepository()
        self.registry = ProviderRegistry()

    # -------------------------------------------------
    # MAIN ENTRY POINT
    # -------------------------------------------------

    async def solve(
        self,
        question: str,
        subject: str,
        difficulty: int
    ) -> Dict[str, Any]:

        # 🔥 Register providers dynamically
        await self._register_providers()

        # Rank providers
        ranked = await self.router.rank_providers(subject, difficulty)

        if len(ranked) < 2:
            raise RuntimeError("Need at least 2 providers for orchestration.")

        provider_a = ranked[0][0]
        provider_b = ranked[1][0]

        # Call providers
        provider_instance_a = self.registry.get(provider_a)
        provider_instance_b = self.registry.get(provider_b)

        response_a = await provider_instance_a.generate(question, subject, difficulty)
        response_b = await provider_instance_b.generate(question, subject, difficulty)

        # Compare answers (temporary simple logic)
        if response_a["answer"] == response_b["answer"]:
            winner = provider_a
            final_answer = response_a["answer"]
        else:
            winner = provider_a
            final_answer = response_a["answer"]

        loser = provider_b if winner == provider_a else provider_a

        # Update skill
        await self.skill_engine.update_1v1(
            winner_id=winner,
            loser_id=loser,
            subject=subject,
            difficulty=difficulty
        )

        return {
            "final_answer": final_answer,
            "winner_provider_id": winner
        }

    # -------------------------------------------------
    # INTERNAL: Provider Registration
    # -------------------------------------------------

    async def _register_providers(self):

        providers = await self.provider_repo.get_all_active()

        for provider_id, name in providers:

            # Skip if already registered
            if provider_id in self.registry._providers:
                continue

            self.registry.register(provider_id, name)