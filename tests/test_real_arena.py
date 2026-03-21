import asyncio
import unittest

from core.solver import solve_question


class SolverIntegrationSmoke(unittest.TestCase):
    def test_async_solver_smoke(self):
        result = asyncio.run(solve_question("What is 6 * 7 = 42"))
        self.assertIn("final_answer", result)
        self.assertIn("verification", result)
        self.assertIn("profile", result)
        self.assertIn("arena", result)
        self.assertIn("winner_margin", result["arena"])
        self.assertIn("entropy", result["arena"])
        self.assertIn("disagreement_case_count", result["arena"])


if __name__ == "__main__":
    unittest.main()
