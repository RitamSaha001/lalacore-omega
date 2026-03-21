CREATE TABLE IF NOT EXISTS ai_mcts_log (
    id BIGSERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    iterations INTEGER NOT NULL DEFAULT 0,
    nodes_explored INTEGER NOT NULL DEFAULT 0,
    tool_calls INTEGER NOT NULL DEFAULT 0,
    retrieval_calls INTEGER NOT NULL DEFAULT 0,
    verification_pass BOOLEAN NOT NULL DEFAULT FALSE,
    final_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_mcts_log_created_at
    ON ai_mcts_log (created_at DESC);
