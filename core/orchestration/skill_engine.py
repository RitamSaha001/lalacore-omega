from trueskill import Rating, rate_1vs1
from core.db.repositories.skill_repo import SkillRepository


DEFAULT_MU = 25.0
DEFAULT_SIGMA = 8.333


class SkillEngine:

    def __init__(self):
        self.repo = SkillRepository()

    async def update_1v1(
        self,
        winner_id: int,
        loser_id: int,
        subject: str,
        difficulty: int
    ):
        # Fetch existing ratings
        w_skill = await self.repo.get_skill(winner_id, subject, difficulty)
        l_skill = await self.repo.get_skill(loser_id, subject, difficulty)

        # Initialize ratings (cold start safe)
        w_rating = Rating(*(w_skill if w_skill else (DEFAULT_MU, DEFAULT_SIGMA)))
        l_rating = Rating(*(l_skill if l_skill else (DEFAULT_MU, DEFAULT_SIGMA)))

        # TrueSkill update
        new_w, new_l = rate_1vs1(w_rating, l_rating)

        # Persist updated ratings
        await self.repo.upsert_skill(
            winner_id,
            subject,
            difficulty,
            new_w.mu,
            new_w.sigma
        )

        await self.repo.upsert_skill(
            loser_id,
            subject,
            difficulty,
            new_l.mu,
            new_l.sigma
        )