import unittest

from core.lalacore_x.providers import ProviderFabric
from core.lalacore_x.schemas import ProviderAnswer
from core.lalacore_x.schemas import ProblemProfile


class ProviderAnswerContractTests(unittest.TestCase):
    def test_answer_contract_is_attached(self):
        fabric = ProviderFabric()
        profile = ProblemProfile(
            subject="physics",
            difficulty="easy",
            numeric=True,
            multi_concept=False,
            trap_probability=0.0,
        )
        out = fabric._pack_text_answer(
            provider="test_provider",
            text="Reasoning: distance computed.\nFinal Answer: 5 m",
            latency_s=0.01,
            raw={},
            question_text="Find the distance in m",
            profile=profile,
        )

        contract = out.answer_contract
        self.assertIn("final_answer", contract)
        self.assertIn("reasoning_summary", contract)
        self.assertIn("answer_type", contract)
        self.assertIn("confidence", contract)
        self.assertIn("units", contract)

    def test_arithmetic_guard_rejects_symbolic_binomial_prompt(self):
        fabric = ProviderFabric()
        prompt = "find the middle term in expansion of (a + 2b ) ^9"
        self.assertFalse(fabric._is_simple_arithmetic_prompt(prompt))
        self.assertEqual(fabric._safe_math_guess(prompt), "")

    def test_arithmetic_guard_accepts_pure_numeric_prompt(self):
        fabric = ProviderFabric()
        prompt = "what is (12 + 6) / 3"
        self.assertTrue(fabric._is_simple_arithmetic_prompt(prompt))
        self.assertEqual(fabric._safe_math_guess(prompt), "6.0")

    def test_validate_answer_accepts_short_option_label(self):
        fabric = ProviderFabric()
        ok, reason = fabric._validate_answer(
            ProviderAnswer(
                provider="test_provider",
                reasoning="MCQ selection",
                final_answer="A",
                confidence=0.6,
            )
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_validate_answer_accepts_multi_option_labels(self):
        fabric = ProviderFabric()
        ok, reason = fabric._validate_answer(
            ProviderAnswer(
                provider="test_provider",
                reasoning="Multiple correct options.",
                final_answer="A, B and D",
                confidence=0.6,
            )
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_validate_answer_rejects_unresolved_marker(self):
        fabric = ProviderFabric()
        ok, reason = fabric._validate_answer(
            ProviderAnswer(
                provider="test_provider",
                reasoning="Could not solve.",
                final_answer="[UNRESOLVED]",
                confidence=0.2,
            )
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "unresolved_answer")

    def test_pack_text_answer_salvages_non_tagged_output(self):
        fabric = ProviderFabric()
        profile = ProblemProfile(
            subject="math",
            difficulty="medium",
            numeric=True,
            multi_concept=False,
            trap_probability=0.0,
        )
        out = fabric._pack_text_answer(
            provider="test_provider",
            text="Using AM-GM, minimum value is 9 when x=y=z=1/3.",
            latency_s=0.01,
            raw={},
            question_text="Find minimum value of 1/x+1/y+1/z for x+y+z=1.",
            profile=profile,
        )

        self.assertNotEqual(out.final_answer.strip(), "")
        self.assertIn("9", out.final_answer)

    def test_pack_text_answer_preserves_multiline_material_output(self):
        fabric = ProviderFabric()
        profile = ProblemProfile(
            subject="physics",
            difficulty="medium",
            numeric=False,
            multi_concept=True,
            trap_probability=0.0,
        )
        prompt = (
            "[[LC9_MATERIAL_ENGINE:material_generate]]\n"
            "Task: produce a material-grounded JEE study output for 'Electrostatics'."
        )
        out = fabric._pack_text_answer(
            provider="test_provider",
            text=(
                "Reasoning: Build a revision-ready summary.\n"
                "Final Answer:\n"
                "# Electrostatics Summary\n\n"
                "## Core Idea Map\n"
                "- Field and potential.\n"
            ),
            latency_s=0.01,
            raw={},
            question_text=prompt,
            profile=profile,
        )

        self.assertIn("Electrostatics Summary", out.final_answer)
        self.assertIn("Core Idea Map", out.final_answer)
        self.assertNotIn("Final Answer:", out.final_answer)

    def test_pack_text_answer_preserves_multiline_analytics_output(self):
        fabric = ProviderFabric()
        profile = ProblemProfile(
            subject="physics",
            difficulty="medium",
            numeric=False,
            multi_concept=True,
            trap_probability=0.0,
        )
        prompt = (
            "[[LC9_ANALYTICS_ENGINE:analyze_exam]]\n"
            "Generate a grounded post-exam analytics report."
        )
        out = fabric._pack_text_answer(
            provider="test_provider",
            text=(
                "Reasoning: Build a structured analytics report.\n"
                "Final Answer:\n"
                '{"summary":"Good momentum.","strengths":["Mechanics"],'
                '"weaknesses":["Thermodynamics"],"strategy":["Repair weak topic"],'
                '"next_steps":["Retry mistakes"]}'
            ),
            latency_s=0.01,
            raw={},
            question_text=prompt,
            profile=profile,
        )

        self.assertIn('"summary":"Good momentum."', out.final_answer)
        self.assertNotIn("Final Answer:", out.final_answer)


class SymbolicGuardReasoningTests(unittest.IsolatedAsyncioTestCase):
    async def test_symbolic_guard_prefers_detailed_expected_solution_text(self):
        fabric = ProviderFabric()
        profile = ProblemProfile(
            subject="math",
            difficulty="hard",
            numeric=True,
            multi_concept=False,
            trap_probability=0.0,
        )
        out = await fabric._run_symbolic_guard(
            "For the hyperbola x^2/16 - y^2/9 = 1, find its eccentricity and equations of asymptotes.",
            profile,
            [],
        )

        self.assertEqual(out.provider, "symbolic_guard")
        self.assertIn(r"\frac{5}{4}", out.reasoning)
        self.assertIn(r"c^2 = a^2 + b^2", out.reasoning)
        self.assertIn("e = 5/4", out.final_answer)


if __name__ == "__main__":
    unittest.main()
