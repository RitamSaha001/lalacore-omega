import unittest

from core.math.combinatorics_modules import (
    DerangementSolver,
    DistributionSolver,
    InclusionExclusionSolver,
)
from core.math.contextual_math_solver import solve_contextual_math_question


class CombinatoricsModuleTests(unittest.TestCase):
    def test_inclusion_exclusion_solver(self):
        solver = InclusionExclusionSolver()
        question = "How many integers from 1-200 are divisible by at least one of 2,3,5?"
        self.assertEqual(solver.solve(question), 146)

    def test_derangement_solver(self):
        solver = DerangementSolver()
        self.assertEqual(solver.solve("Find derangements of 1-5"), 44)

    def test_distribution_solver_stars_bars(self):
        solver = DistributionSolver()
        question = "Distribute 7 identical balls into 3 distinct boxes with at least one in each."
        self.assertEqual(solver.solve(question), 15)

    def test_distribution_solver_no_two_vowels(self):
        solver = DistributionSolver()
        question = "How many arrangements of word BANANA have no two vowels together?"
        self.assertEqual(solver.solve(question), 12)

    def test_contextual_solver_uses_modular_modules(self):
        out = solve_contextual_math_question("How many derangements of 1-5 are possible?")
        self.assertIsNotNone(out)
        self.assertTrue(bool(out.get("handled", False)))
        self.assertEqual(str(out.get("answer", "")).strip(), "44")
        self.assertIn("modular_", str(out.get("kind", "")))


if __name__ == "__main__":
    unittest.main()
