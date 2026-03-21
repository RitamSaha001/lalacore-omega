CREATE TABLE IF NOT EXISTS providers (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS provider_skill (
    id SERIAL PRIMARY KEY,
    provider_id INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    difficulty INTEGER NOT NULL,
    mu DOUBLE PRECISION NOT NULL DEFAULT 25.0,
    sigma DOUBLE PRECISION NOT NULL DEFAULT 8.333,
    matches INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT NOW(),
    UNIQUE (provider_id, subject, difficulty)
);

CREATE TABLE IF NOT EXISTS routing_stats (
    id SERIAL PRIMARY KEY,
    provider_id INTEGER REFERENCES providers(id) ON DELETE CASCADE,
    subject TEXT,
    difficulty TEXT,
    ema_reliability DOUBLE PRECISION DEFAULT 0.5,
    calibration_error DOUBLE PRECISION DEFAULT 0.5,
    brier_score DOUBLE PRECISION DEFAULT 0.5,
    sample_count INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS failure_replay (
    id SERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    subject TEXT,
    difficulty TEXT,
    provider TEXT,
    risk DOUBLE PRECISION,
    reason TEXT,
    final_answer TEXT,
    cluster TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
