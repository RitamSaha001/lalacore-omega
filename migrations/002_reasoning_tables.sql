CREATE TABLE IF NOT EXISTS arena_reasoning_nodes (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES arena_sessions(id) ON DELETE CASCADE,
    provider TEXT,
    node_id INTEGER,
    node_type TEXT,
    summary TEXT
);

CREATE TABLE IF NOT EXISTS arena_reasoning_edges (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES arena_sessions(id) ON DELETE CASCADE,
    provider TEXT,
    from_node INTEGER,
    to_node INTEGER
);

CREATE TABLE IF NOT EXISTS arena_rebuttals (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES arena_sessions(id) ON DELETE CASCADE,
    provider TEXT,
    rebuttal_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);