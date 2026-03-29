CREATE TABLE IF NOT EXISTS app_runtime_json_store (
    blob_key TEXT PRIMARY KEY,
    json_value TEXT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_app_runtime_json_store_updated_at
    ON app_runtime_json_store(updated_at DESC);

CREATE TABLE IF NOT EXISTS app_upload_blobs (
    file_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    mime TEXT NOT NULL,
    content BYTEA NOT NULL,
    size_bytes BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_app_upload_blobs_updated_at
    ON app_upload_blobs(updated_at DESC);
