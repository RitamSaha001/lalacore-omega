from typing import List, Tuple, Optional

from core.db.repositories.skill_repo import SkillRepository
from core.db.repositories.provider_repo import ProviderRepository


DEFAULT_MU = 25.0
DEFAULT_SIGMA = 8.333


class Router:

    def __init__(self):
        self.skill_repo = SkillRepository()
        self.provider_repo = ProviderRepository()

    # ----------------------------
    # Core Conservative Score
    # ----------------------------

    @staticmethod
    def conservative_score(mu: float, sigma: float) -> float:
        """
        Conservative estimate of provider skill.
        Lower sigma (uncertainty) increases score.
        """
        return mu - 3 * sigma

    # ----------------------------
    # Single Provider Score
    # ----------------------------

    async def get_provider_score(
        self,
        provider_id: int,
        subject: str,
        difficulty: int
    ) -> float:

        skill = await self.skill_repo.get_skill(
            provider_id,
            subject,
            difficulty
        )

        if not skill:
            # Cold start neutral conservative score
            return DEFAULT_MU - 3 * DEFAULT_SIGMA

        mu, sigma = skill
        return self.conservative_score(mu, sigma)

    # ----------------------------
    # Rank All Active Providers
    # ----------------------------

    async def rank_providers(
        self,
        subject: str,
        difficulty: int
    ) -> List[Tuple[int, float]]:
        """
        Returns list of (provider_id, score)
        sorted by highest score first.
        """

        active_providers = await self.provider_repo.get_all_active()

        ranked = []

        for provider_id, _ in active_providers:
            score = await self.get_provider_score(
                provider_id,
                subject,
                difficulty
            )
            ranked.append((provider_id, score))

        # Sort descending by score
        ranked.sort(key=lambda x: x[1], reverse=True)

        return ranked

    # ----------------------------
    # Select Best Provider
    # ----------------------------

    async def select_best_provider(
        self,
        subject: str,
        difficulty: int
    ) -> Optional[int]:
        """
        Returns provider_id of best provider
        or None if no providers exist.
        """

        ranked = await self.rank_providers(subject, difficulty)

        if not ranked:
            return None

        return ranked[0][0]