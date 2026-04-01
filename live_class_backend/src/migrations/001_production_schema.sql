CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('teacher', 'student')),
  display_name TEXT NOT NULL,
  token_version INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  family_id TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_at TIMESTAMPTZ NULL,
  replaced_by_token_id UUID NULL REFERENCES refresh_tokens(id),
  token_version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS refresh_tokens_user_id_idx ON refresh_tokens(user_id);
CREATE INDEX IF NOT EXISTS refresh_tokens_family_id_idx ON refresh_tokens(family_id);

CREATE TABLE IF NOT EXISTS class_sessions (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  teacher_id TEXT NOT NULL REFERENCES users(id),
  teacher_name TEXT NOT NULL,
  active_room_id TEXT NOT NULL,
  chat_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  meeting_locked BOOLEAN NOT NULL DEFAULT FALSE,
  waiting_room_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  is_recording BOOLEAN NOT NULL DEFAULT FALSE,
  recording_status TEXT NOT NULL DEFAULT 'idle',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  active_whiteboard_user_id TEXT NULL,
  whiteboard_strokes JSONB NOT NULL DEFAULT '[]'::jsonb,
  active_recording JSONB NULL,
  version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS participants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id TEXT NOT NULL REFERENCES users(id),
  class_id TEXT NOT NULL REFERENCES class_sessions(id) ON DELETE CASCADE,
  user_name TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('teacher', 'student')),
  status TEXT NOT NULL,
  breakout_room_id UUID NULL,
  muted BOOLEAN NOT NULL DEFAULT FALSE,
  camera_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  whiteboard_access BOOLEAN NOT NULL DEFAULT FALSE,
  request_id TEXT NULL,
  requested_at TIMESTAMPTZ NULL,
  approved_at TIMESTAMPTZ NULL,
  rejected_at TIMESTAMPTZ NULL,
  presence_status TEXT NOT NULL DEFAULT 'offline',
  last_seen_at TIMESTAMPTZ NULL,
  disconnected_at TIMESTAMPTZ NULL,
  disconnect_grace_expires_at TIMESTAMPTZ NULL,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (class_id, user_id)
);
CREATE INDEX IF NOT EXISTS participants_class_status_idx ON participants(class_id, status);

CREATE TABLE IF NOT EXISTS breakout_rooms (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  class_id TEXT NOT NULL REFERENCES class_sessions(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  livekit_room_name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  version INTEGER NOT NULL DEFAULT 1
);

ALTER TABLE participants
  ADD CONSTRAINT participants_breakout_room_fk
  FOREIGN KEY (breakout_room_id) REFERENCES breakout_rooms(id) ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS chat_messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  class_id TEXT NOT NULL REFERENCES class_sessions(id) ON DELETE CASCADE,
  sender_id TEXT NOT NULL REFERENCES users(id),
  sender_name TEXT NOT NULL,
  message TEXT NOT NULL,
  attachment JSONB NULL,
  dedupe_key TEXT NULL,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (class_id, dedupe_key)
);
ALTER TABLE chat_messages
  ADD COLUMN IF NOT EXISTS attachment JSONB NULL;
CREATE INDEX IF NOT EXISTS chat_messages_class_timestamp_idx ON chat_messages(class_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  scope TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  status TEXT NOT NULL,
  response_kind TEXT NULL,
  response_status_code INTEGER NULL,
  response_content_type TEXT NULL,
  response_body JSONB NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS class_event_log (
  class_id TEXT NOT NULL REFERENCES class_sessions(id) ON DELETE CASCADE,
  sequence_number BIGINT NOT NULL,
  channel TEXT NOT NULL,
  message_id UUID NOT NULL,
  payload JSONB NOT NULL,
  origin_node_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (class_id, sequence_number),
  UNIQUE (message_id)
);
CREATE INDEX IF NOT EXISTS class_event_log_class_channel_idx
  ON class_event_log(class_id, channel, sequence_number);

CREATE TABLE IF NOT EXISTS client_inbound_sequences (
  client_key TEXT PRIMARY KEY,
  last_processed_sequence BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS recording_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  class_id TEXT NOT NULL REFERENCES class_sessions(id) ON DELETE CASCADE,
  egress_id TEXT NULL,
  raw_recording_path TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  result JSONB NULL,
  error JSONB NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS recording_jobs_class_created_idx
  ON recording_jobs(class_id, created_at DESC);
