import unittest

from core.lalacore_x.classifier import ProblemClassifier
from core.lalacore_x.providers import ProviderFabric


class ProblemClassifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.classifier = ProblemClassifier()
        self.fabric = ProviderFabric()

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

    def test_conics_locus_question_upgrades_out_of_easy(self):
        profile = self.classifier.classify(
            "Let a variable circle pass through fixed points A(2,3) and B(6,7) such that its center lies on the line x + y = 10. Find the locus of the midpoint of the chord AB of the circle."
        )
        self.assertEqual(profile.subject, "math")
        self.assertIn(profile.difficulty, {"medium", "hard"})
        self.assertGreaterEqual(int(profile.features.get("advanced_math_hits") or 0), 4)

    def test_conics_profile_unlocks_stronger_gemini_candidates(self):
        profile = self.classifier.classify(
            "A point P moves on parabola y^2 = 4ax. The normal at P meets the axis at N. Find the locus of the midpoint of segment PN."
        )
        models = self.fabric.candidate_models("gemini", profile)
        self.assertIn(profile.difficulty, {"medium", "hard"})
        self.assertGreaterEqual(len(models), 2)
        self.assertEqual(models[0], "gemini-2.5-pro")


if __name__ == "__main__":
    unittest.main()
