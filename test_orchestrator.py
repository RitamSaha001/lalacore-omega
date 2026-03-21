import asyncio

from core.db.connection import Database
from core.orchestration.orchestrator import Orchestrator
from core.db.repositories.provider_repo import ProviderRepository


async def main():
    await Database.init()

    provider_repo = ProviderRepository()

    # Ensure providers exist
    await provider_repo.create_if_not_exists("openrouter")
    await provider_repo.create_if_not_exists("groq")

    orchestrator = Orchestrator()

    result = await orchestrator.solve(
        question="What is 6 * 7?",
        subject="math",
        difficulty=3
    )

    print(result)

    await Database.close()


if __name__ == "__main__":
    asyncio.run(main())
