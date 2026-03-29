import unittest
from unittest.mock import AsyncMock, patch

from core.lalacore_x.schemas import ProviderAnswer
from core.material_generation_engine import MaterialGenerationEngine


class MaterialGenerationEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_material_engine_prefers_long_form_material_candidate(self) -> None:
        engine = MaterialGenerationEngine()
        with patch.object(engine.providers, "ensure_startup_warmup", new=AsyncMock()), patch.object(
            engine.providers,
            "available_providers",
            return_value=["gemini", "mini"],
        ), patch.object(
            engine.providers,
            "generate_many",
            new=AsyncMock(
                return_value=[
                    ProviderAnswer(
                        provider="gemini",
                        reasoning="Planned the study summary with concept sections.",
                        final_answer=(
                            "# Electrostatics Summary\n\n"
                            "## Core Idea Map\n- Superposition and field lines.\n\n"
                            "## Common Traps\n- Sign mistakes in vector addition."
                        ),
                        confidence=0.78,
                    ),
                    ProviderAnswer(
                        provider="mini",
                        reasoning="Short heading only.",
                        final_answer="# Electrostatics Summary",
                        confidence=0.61,
                    ),
                ]
            ),
        ), patch.object(engine.vault, "retrieve", return_value=[]):
            out = await engine.run(
                prompt="Task: produce a material-grounded JEE study output for 'Electrostatics'.",
                title="Electrostatics",
                mode="summarize",
                card={"subject": "Physics", "material_notes": "Electric field and superposition."},
                options={"function": "material_generate"},
            )

        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("winner_provider"), "gemini")
        self.assertIn("Core Idea Map", str(out.get("content", "")))

    async def test_material_engine_rejects_placeholder_outputs(self) -> None:
        engine = MaterialGenerationEngine()
        with patch.object(engine.providers, "ensure_startup_warmup", new=AsyncMock()), patch.object(
            engine.providers,
            "available_providers",
            return_value=["gemini"],
        ), patch.object(
            engine.providers,
            "generate_many",
            new=AsyncMock(
                return_value=[
                    ProviderAnswer(
                        provider="gemini",
                        reasoning="The actual question is missing.",
                        final_answer="[UNRESOLVED]",
                        confidence=0.12,
                    )
                ]
            ),
        ), patch.object(engine.vault, "retrieve", return_value=[]):
            out = await engine.run(
                prompt="Task: produce a material-grounded JEE study output for 'Thermodynamics'.",
                title="Thermodynamics",
                mode="summarize",
                card={"subject": "Physics"},
                options={"function": "material_generate"},
            )

        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("status"), "MATERIAL_ENGINE_EMPTY_OUTPUT")


if __name__ == "__main__":
    unittest.main()
