CREATE TABLE IF NOT EXISTS arena_sessions (
    id SERIAL PRIMARY KEY,
    question_id TEXT NOT NULL,
    subject TEXT,
    difficulty TEXT,
    entropy FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS arena_participants (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES arena_sessions(id) ON DELETE CASCADE,
    provider TEXT,
    final_answer TEXT,
    deterministic_pass BOOLEAN,
    critic_score FLOAT,
    confidence FLOAT,
    global_skill_mu FLOAT,
    global_skill_sigma FLOAT,
    local_theta FLOAT,
    bayesian_posterior FLOAT,
    won BOOLEAN DEFAULT FALSE,
    verified_correct BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS arena_pairwise (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES arena_sessions(id) ON DELETE CASCADE,
    provider_a TEXT,
    provider_b TEXT,
    winner TEXT,
    score_diff FLOAT,
    similarity FLOAT
);