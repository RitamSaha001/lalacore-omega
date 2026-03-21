import asyncio

from core.db.connection import Database
from core.db.repositories.provider_repo import ProviderRepository
from core.db.repositories.skill_repo import SkillRepository
from core.orchestration.skill_engine import SkillEngine
from core.orchestration.router import Router


async def main():
    print("🚀 Starting Omega integration test...\n")

    # Init DB
    await Database.init()
    print("✅ Database connected")

    provider_repo = ProviderRepository()
    skill_repo = SkillRepository()
    skill_engine = SkillEngine()
    router = Router()

    # Create providers safely
    openrouter_id = await provider_repo.create_if_not_exists("openrouter")
    groq_id = await provider_repo.create_if_not_exists("groq")

    print(f"✅ Providers registered: openrouter={openrouter_id}, groq={groq_id}")

    subject = "math"
    difficulty = 3

    # Check initial scores (cold start)
    score1 = await router.get_provider_score(openrouter_id, subject, difficulty)
    score2 = await router.get_provider_score(groq_id, subject, difficulty)

    print(f"Cold start scores → openrouter: {score1:.2f}, groq: {score2:.2f}")

    # Simulate arena match (openrouter wins)
    await skill_engine.update_1v1(
        winner_id=openrouter_id,
        loser_id=groq_id,
        subject=subject,
        difficulty=difficulty
    )

    print("✅ Skill updated after 1v1")

    # Check updated scores
    score1 = await router.get_provider_score(openrouter_id, subject, difficulty)
    score2 = await router.get_provider_score(groq_id, subject, difficulty)

    print(f"After update → openrouter: {score1:.2f}, groq: {score2:.2f}")

    # Fetch raw skill
    skill = await skill_repo.get_skill(openrouter_id, subject, difficulty)
    print(f"Raw skill for openrouter → mu={skill[0]:.2f}, sigma={skill[1]:.2f}")

    await Database.close()
    print("\n🎉 Integration test completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())