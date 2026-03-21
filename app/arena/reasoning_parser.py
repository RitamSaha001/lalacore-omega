import json
import jsonschema

from core.db.connection import Database


REASONING_GRAPH_SCHEMA = {
    "type": "object",
    "properties": {
        "nodes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "type": {"type": "string"},
                    "summary": {"type": "string"}
                },
                "required": ["id", "type"]
            }
        },
        "edges": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "from": {"type": "integer"},
                    "to": {"type": "integer"}
                },
                "required": ["from", "to"]
            }
        }
    },
    "required": ["nodes", "edges"]
}


class ReasoningParserEngine:
    """
    Converts raw reasoning text into structured DAG using judge model.
    """

    def __init__(self, judge_provider, db):
        self.judge_provider = judge_provider
        self.db = db

    async def parse_and_store(self, session_id, provider, reasoning_text, conn=None):

        prompt = self._build_prompt(reasoning_text)

        response = await self.judge_provider.generate(
            prompt=prompt,
            temperature=0.0,
            max_tokens=800
        )

        graph = self._safe_parse(response)

        await self._store_graph(session_id, provider, graph, conn=conn)

        return graph

    # -------------------------
    # Prompt Builder
    # -------------------------

    def _build_prompt(self, reasoning_text):

        return f"""
You are a reasoning graph extractor.

Convert the following reasoning into a structured graph.

Rules:
- Break into minimal logical steps.
- Use step types from:
  assumption,
  formula_application,
  algebra_step,
  substitution,
  simplification,
  logical_inference,
  numeric_evaluation,
  case_analysis,
  conclusion.
- Provide short summaries (max 15 words).
- Output valid JSON only.

Reasoning:
{reasoning_text}
"""

    # -------------------------
    # Safe Parsing
    # -------------------------

    def _safe_parse(self, raw_response):

        try:
            graph = json.loads(raw_response)
            jsonschema.validate(graph, REASONING_GRAPH_SCHEMA)
            return graph
        except Exception:
            # fallback minimal graph
            return {
                "nodes": [
                    {"id": 1, "type": "conclusion", "summary": "unparsed"}
                ],
                "edges": []
            }

    # -------------------------
    # DB Storage
    # -------------------------

    async def _store_graph(self, session_id, provider, graph, conn=None):

        node_values = [
            (
                session_id,
                provider,
                n["id"],
                n["type"],
                n.get("summary", "")
            )
            for n in graph["nodes"]
        ]

        edge_values = [
            (
                session_id,
                provider,
                e["from"],
                e["to"]
            )
            for e in graph["edges"]
        ]

        if conn is not None:
            await conn.executemany(
                """
                INSERT INTO arena_reasoning_nodes
                (session_id, provider, node_id, node_type, summary)
                VALUES ($1,$2,$3,$4,$5)
                """,
                node_values
            )

            if edge_values:
                await conn.executemany(
                    """
                    INSERT INTO arena_reasoning_edges
                    (session_id, provider, from_node, to_node)
                    VALUES ($1,$2,$3,$4)
                    """,
                    edge_values
                )
            return

        # Supports injected DB adapter or direct asyncpg pool fallback.
        if self.db is not None:
            await self.db.executemany(
                """
                INSERT INTO arena_reasoning_nodes
                (session_id, provider, node_id, node_type, summary)
                VALUES ($1,$2,$3,$4,$5)
                """,
                node_values
            )

            if edge_values:
                await self.db.executemany(
                    """
                    INSERT INTO arena_reasoning_edges
                    (session_id, provider, from_node, to_node)
                    VALUES ($1,$2,$3,$4)
                    """,
                    edge_values
                )
            return

        pool = await Database.get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO arena_reasoning_nodes
                (session_id, provider, node_id, node_type, summary)
                VALUES ($1,$2,$3,$4,$5)
                """,
                node_values
            )

            if edge_values:
                await conn.executemany(
                    """
                    INSERT INTO arena_reasoning_edges
                    (session_id, provider, from_node, to_node)
                    VALUES ($1,$2,$3,$4)
                    """,
                    edge_values
                )
