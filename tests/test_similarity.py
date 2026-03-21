import unittest

from core.lalacore_x.retrieval import ConceptVault


class RetrievalTests(unittest.TestCase):
    def test_retrieval_returns_blocks(self):
        vault = ConceptVault(root="data/vault")
        blocks = vault.retrieve("Solve quadratic equation with roots", subject="math", top_k=3)
        self.assertTrue(len(blocks) >= 1)

    def test_trap_note_generation(self):
        vault = ConceptVault(root="data/vault")
        blocks = vault.retrieve("Find sqrt equation roots", subject="math", top_k=3)
        joined = " ".join(b.text.lower() for b in blocks)
        self.assertIn("extraneous", joined)

    def test_concept_cluster_expansion(self):
        vault = ConceptVault(root="data/vault")
        expanded = vault.expand_concept_clusters(["quadratic"], depth=2)
        self.assertIn("quadratic", expanded)
        self.assertGreaterEqual(len(expanded), 1)


if __name__ == "__main__":
    unittest.main()
