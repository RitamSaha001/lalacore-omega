CREATE TABLE IF NOT EXISTS ai_reasoning_graph_log (
    id BIGSERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    node_count INTEGER NOT NULL DEFAULT 0,
    tool_calls INTEGER NOT NULL DEFAULT 0,
    retrieval_nodes INTEGER NOT NULL DEFAULT 0,
    verification_pass BOOLEAN NOT NULL DEFAULT FALSE,
    final_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_reasoning_graph_log_created_at
    ON ai_reasoning_graph_log (created_at DESC);
