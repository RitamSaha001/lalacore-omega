CREATE TABLE IF NOT EXISTS question_search_cache (
    query_hash TEXT PRIMARY KEY,
    query_text TEXT NOT NULL,
    results_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_question_search_cache_updated_at
    ON question_search_cache (updated_at DESC);

CREATE TABLE IF NOT EXISTS ai_chat_search_log (
    id BIGSERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    ocr_used BOOLEAN NOT NULL DEFAULT FALSE,
    web_results_found INTEGER NOT NULL DEFAULT 0,
    solution_used BOOLEAN NOT NULL DEFAULT FALSE,
    lalacore_provider TEXT NOT NULL DEFAULT '',
    arena_triggered BOOLEAN NOT NULL DEFAULT FALSE,
    verification_passed BOOLEAN NOT NULL DEFAULT FALSE,
    mismatch_detected BOOLEAN NOT NULL DEFAULT FALSE,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_chat_search_log_created_at
    ON ai_chat_search_log (created_at DESC);
