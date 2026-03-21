import importlib
import sys
import types
import unittest
from collections import defaultdict

from app.arena.arena_orchestrator import ArenaOrchestrator
from app.arena.bradley_terry import BradleyTerryEngine
from app.arena.stability_guard import ArenaStabilityGuard
from core.safe_math import safe_sigmoid


class ArenaBugFixTests(unittest.TestCase):
    def test_entropy_sanitizer_does_not_upper_clamp(self):
        guard = ArenaStabilityGuard()
        self.assertAlmostEqual(guard.sanitize_entropy(1.79), 1.79, places=6)
        self.assertEqual(guard.sanitize_entropy(-0.25), 0.0)

    def test_bradley_terry_loser_gradient_uses_p(self):
        bt = BradleyTerryEngine(
            learning_rate=1.0,
            reg_lambda=0.0,
            max_theta=100.0,
        )
        bt.theta = {"winner": 2.0, "loser": 0.0}
        bt.match_counts = defaultdict(int)

        bt.fit([("winner", "loser", 0.0)], iterations=1)

        p = safe_sigmoid(2.0)
        self.assertAlmostEqual(bt.theta["winner"], 2.0 + (1.0 - p), places=6)
        self.assertAlmostEqual(bt.theta["loser"], -p, places=6)

    def test_similarity_graph_key_changes_with_summary(self):
        if "sentence_transformers" not in sys.modules:
            stub = types.ModuleType("sentence_transformers")

            class _DummySentenceTransformer:
                def __init__(self, *args, **kwargs):
                    pass

                def encode(self, text):
                    return [0.0]

            stub.SentenceTransformer = _DummySentenceTransformer
            sys.modules["sentence_transformers"] = stub

        module = importlib.import_module("app.arena.similarity_engine")
        engine = module.SimilarityEngine.__new__(module.SimilarityEngine)

        graph_a = {"nodes": [{"id": 1, "type": "conclusion", "summary": "alpha"}]}
        graph_b = {"nodes": [{"id": 1, "type": "conclusion", "summary": "beta"}]}
        self.assertNotEqual(engine._graph_key(graph_a), engine._graph_key(graph_b))


class ArenaTransactionTests(unittest.IsolatedAsyncioTestCase):
    async def test_orchestrator_uses_single_transaction_connection(self):
        class _TxCtx:
            def __init__(self, conn):
                self.conn = conn

            async def __aenter__(self):
                self.conn.transaction_entered += 1
                return self.conn

            async def __aexit__(self, exc_type, exc, tb):
                self.conn.transaction_exited += 1
                return False

        class _Conn:
            def __init__(self):
                self.transaction_entered = 0
                self.transaction_exited = 0

            def transaction(self):
                return _TxCtx(self)

        class _AcquireCtx:
            def __init__(self, conn):
                self.conn = conn

            async def __aenter__(self):
                return self.conn

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class _Pool:
            def __init__(self, conn):
                self.conn = conn
                self.acquire_calls = 0

            def acquire(self):
                self.acquire_calls += 1
                return _AcquireCtx(self.conn)

        class _FakeSessionManager:
            def __init__(self):
                self.conn = _Conn()
                self.pool = _Pool(self.conn)
                self.calls = {
                    "create_session": 0,
                    "add_participant": 0,
                    "update_theta_and_posterior": 0,
                    "log_pairwise_matches": 0,
                }
                self.conn_args = []

            async def _get_pool_with_retry(self, max_attempts=3, base_delay_s=0.2):
                return self.pool

            async def create_session(self, question_id, subject, difficulty, entropy, conn=None):
                self.calls["create_session"] += 1
                self.conn_args.append(conn)
                return 123

            async def add_participant(
                self,
                session_id,
                provider,
                final_answer,
                deterministic_pass,
                critic_score,
                confidence,
                mu,
                sigma,
                conn=None,
            ):
                self.calls["add_participant"] += 1
                self.conn_args.append(conn)

            async def update_theta_and_posterior(
                self,
                session_id,
                provider,
                local_theta,
                posterior,
                won,
                conn=None,
            ):
                self.calls["update_theta_and_posterior"] += 1
                self.conn_args.append(conn)

            async def log_pairwise_matches(self, session_id, matches, conn=None):
                self.calls["log_pairwise_matches"] += 1
                self.conn_args.append(conn)

        class _FakeReasoningParser:
            def __init__(self):
                self.conn_args = []

            async def parse_and_store(self, session_id, provider, reasoning_text, conn=None):
                self.conn_args.append(conn)
                return {
                    "nodes": [{"id": 1, "type": "conclusion", "summary": "ok"}],
                    "edges": [],
                }

        parser = _FakeReasoningParser()
        session_manager = _FakeSessionManager()

        orchestrator = ArenaOrchestrator(
            db=None,
            reasoning_parser=parser,
            similarity_engine=None,
        )
        orchestrator.session_manager = session_manager

        responses = [
            {
                "provider": "a",
                "final_answer": "4",
                "critic_score": 0.9,
                "deterministic_pass": True,
                "confidence": 0.8,
                "skill": 0.8,
                "reasoning": "2+2=4",
            },
            {
                "provider": "b",
                "final_answer": "5",
                "critic_score": 0.4,
                "deterministic_pass": False,
                "confidence": 0.3,
                "skill": 0.4,
                "reasoning": "2+2=5",
            },
        ]

        result = await orchestrator.run(
            question_id="q1",
            subject="math",
            difficulty="easy",
            responses=responses,
        )

        self.assertEqual(result["session_id"], 123)
        self.assertEqual(session_manager.pool.acquire_calls, 1)
        self.assertEqual(session_manager.conn.transaction_entered, 1)
        self.assertEqual(session_manager.conn.transaction_exited, 1)
        self.assertEqual(session_manager.calls["create_session"], 1)
        self.assertEqual(session_manager.calls["add_participant"], 2)
        self.assertEqual(session_manager.calls["update_theta_and_posterior"], 2)
        self.assertEqual(session_manager.calls["log_pairwise_matches"], 1)
        self.assertEqual(len(parser.conn_args), 2)

        all_conns = session_manager.conn_args + parser.conn_args
        self.assertTrue(all(c is session_manager.conn for c in all_conns))


if __name__ == "__main__":
    unittest.main()
