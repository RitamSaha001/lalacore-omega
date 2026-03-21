import unittest

from core.lalacore_x.classifier import ProblemClassifier


class ProblemClassifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.classifier = ProblemClassifier()

    def test_electrostatics_question_routes_to_physics(self):
        profile = self.classifier.classify(
            "For charge density rho(r)=rho0(1-r^2/R^2), find electric field inside off-center spherical cavity using Gauss law."
        )
        self.assertEqual(profile.subject, "physics")

    def test_permutation_question_routes_to_math(self):
        profile = self.classifier.classify(
            "How many linear permutations of BARAAKOBAMA are possible if all letters are used?"
        )
        self.assertEqual(profile.subject, "math")


if __name__ == "__main__":
    unittest.main()
