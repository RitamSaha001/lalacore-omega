import asyncio

from core.db.connection import Database
from app.arena.arena_orchestrator import ArenaOrchestrator
from app.arena.reasoning_parser import ReasoningParserEngine
from app.arena.similarity_engine import SimilarityEngine
from app.providers.openrouter import OpenRouterProvider


# 🔥 Hard chemistry mechanism question
QUESTION = """
Explain the detailed mechanism of the E1 elimination of tert-butyl bromide
in aqueous ethanol. Include:

1) Formation of carbocation
2) Rate determining step
3) Why rearrangement does or does not occur
4) Role of solvent
5) Final alkene formation step
"""


async def main():

    print("🚀 Initializing Database...")
    await Database.init()

    # ---------------------------
    # Judge model for reasoning parsing
    # ---------------------------
    judge = OpenRouterProvider(
        model="openai/gpt-4o-mini"   # Use your strongest stable judge
    )

    reasoning_parser = ReasoningParserEngine(
        judge_provider=judge,
        db=None  # Not needed now (we use Database.get_pool internally)
    )

    similarity_engine = SimilarityEngine()

    arena = ArenaOrchestrator(
        db=None,  # Not used directly (Database class handles pool)
        reasoning_parser=reasoning_parser,
        similarity_engine=similarity_engine
    )

    # ---------------------------
    # Competing models
    # ---------------------------
    competitors = [
    OpenRouterProvider(model="openai/gpt-3.5-turbo"),
    OpenRouterProvider(model="mistralai/mistral-7b-instruct"),
    OpenRouterProvider(model="meta-llama/llama-3-8b-instruct")
]
    

    responses = []

    print("🧠 Generating responses from models...")

    for provider in competitors:

        print(f"   → {provider.model}")

        raw_output = await provider.generate(
            prompt=QUESTION,
            temperature=0.3,
            max_tokens=1200
        )

        responses.append({
            "provider": provider.model,
            "final_answer": "mechanism explanation",
            "critic_score": 0.85,         # Placeholder until critic added
            "deterministic_pass": True,   # Non-numeric
            "confidence": 0.8,
            "skill": 0.75,                # Placeholder until real skill engine used
            "reasoning": raw_output
        })

    print("⚔ Running Arena Tournament...")

    result = await arena.run(
        question_id="REAL_E1_MECHANISM_TEST",
        subject="chemistry",
        difficulty="hard",
        responses=responses
    )

    print("\n🔥 FINAL RESULT 🔥")
    print("Session ID:", result["session_id"])
    print("Winner:", result["winner"])
    print("Posteriors:", result["posteriors"])
    print("Thetas:", result["thetas"])
    print("Entropy:", result["entropy"])


if __name__ == "__main__":
    asyncio.run(main())