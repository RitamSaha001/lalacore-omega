# Live Class Backend

Authoritative backend for a LiveKit-powered classroom system with JWT auth,
idempotent write APIs, resumable websocket delivery, Redis fanout, BullMQ
recording workers, and optional PostgreSQL persistence.

## Principles

- Backend is the source of truth.
- LiveKit is only the media pipe.
- The frontend never decides who can join, chat, or switch rooms.

## Folder structure

- `src/routes/` HTTP route modules
- `src/services/` backend authority and domain services
- `src/models/` entity factories
- `src/repositories/` storage interfaces plus in-memory and PostgreSQL adapters
- `src/repositories/postgres/` production repository implementations
- `src/migrations/` SQL schema for PostgreSQL
- `src/middleware/` auth and idempotency middleware
- `src/queue/` BullMQ queue integration
- `src/workers/` background recording processors
- `src/websocket/` websocket hub and Redis fanout
- `src/livekit/` LiveKit token, room, and egress integration
- `src/observability/` request context and optional OTEL bootstrap
- `load/` stress and reconnect test harnesses

## Install

```bash
npm install
cp .env.example .env
npm run dev
npm run worker
```

Optional OTEL dependencies are already listed in `package.json`. Telemetry only
boots when `OTEL_ENABLED=true`.

## Storage mode

Set `STORAGE_DRIVER=postgres` to use PostgreSQL. Otherwise the backend runs
with the in-memory repositories for local development.

When PostgreSQL is enabled:

```bash
psql "$DATABASE_URL" -f src/migrations/001_production_schema.sql
```

## Hardening added

- All mutating HTTP requests require `Idempotency-Key`
- Duplicate POST retries replay the original response instead of re-running side effects
- Write paths run through lock-scoped transactions to avoid join, approval, breakout, and chat races
- Access tokens expire in 15 minutes and websocket sessions are closed when JWT auth expires
- Refresh tokens are rotated and stored hashed
- Websocket events now carry `message_id` and `sequence_number`
- Reconnect clients can resume from `last_received_sequence`
- Participants are marked `temporarily_disconnected` before being marked `left`
- Optional Redis pub/sub fans out websocket events across backend instances
- LiveKit and Redis calls are wrapped with retries plus circuit breakers
- Recording stop enqueues BullMQ jobs for transcript, notes, flashcards, and summary processing
- Structured JSON logs cover duplicate requests, reconnects, token failures, replays, and dropped messages
- Websocket heartbeats now terminate dead sockets instead of waiting forever
- `/health/live`, `/health/ready`, `/ops/metrics`, and `/ops/metrics.prometheus` expose runtime health
- Async request context carries `requestId`, `classId`, `userId`, and `role` into logs automatically
- Optional OpenTelemetry bootstraps HTTP, database, and worker tracing when enabled
- Load harnesses exist for k6 HTTP pressure and websocket reconnect storms

## API surface

Primary routes:

- `POST /auth/login`
- `POST /auth/refresh`
- `POST /auth/logout`
- `GET /auth/me`
- `POST /request-join`
- `POST /approve-join`
- `POST /reject-join`
- `GET /live-token`
- `POST /chat/send`
- `POST /breakout/create`
- `POST /breakout/assign`
- `POST /breakout/join`
- `POST /breakout/leave`
- `POST /recording/start`
- `POST /recording/stop`
- `GET /recording/process_status`
- `GET /recording/process_result`

Compatibility aliases used by the Flutter live-class client are still
supported:

- `GET /class/session`
- `POST /class/join_request`
- `POST /class/admit`
- `POST /class/reject`
- `POST /class/admit_all`
- `POST /class/lock`
- `POST /class/chat`
- `POST /class/waiting_room`
- `GET /class/state`
- `POST /live/token`

## Identity and auth boundary

This package derives identity, role, and permissions from signed JWTs plus
backend session state.

- `Authorization: Bearer <access-token>` is required on protected HTTP routes
- Websocket clients must send the same JWT via `access_token` query param,
  `Authorization` header, or websocket subprotocol bearer pair
- Role checks are enforced with backend middleware and participant/session state
- `user_id` is never trusted from client headers

## WebSocket channels

- `/class/events` for waiting-room snapshots and server state changes
- `/class/sync` for classroom event fanout
- `/chat/stream` for chat broadcasts and chat state changes

Protected websocket connections emit:

- `connection_ready`
- `auth_token_expiring`
- `auth_token_expired`

Every server event includes:

- `message_id`
- `sequence_number`
- `channel`
- `sent_at`

Reconnect clients should reconnect with:

- `client_id`
- `last_received_sequence`
- a fresh JWT if the previous access token expired

The server will replay missed events from the per-class event log.

ACK format:

```json
{
  "type": "ack",
  "sequence_number": 42
}
```

Resume format:

```json
{
  "type": "resume",
  "last_received_sequence": 42
}
```

## Critical flows

### Student join

1. Student authenticates with `POST /auth/login`
2. Student calls `POST /request-join`
3. Backend stores `pending` participant state
4. Teacher calls `POST /approve-join`
5. Student calls `GET /live-token`
6. Backend checks meeting lock, waiting-room approval, and participant status
7. Only then does backend mint a LiveKit token

### Breakout switch

1. Teacher creates a breakout room
2. Teacher assigns participant to the breakout room
3. Backend stores `breakout_room_id`
4. Participant calls `POST /breakout/join`
5. Backend mints a token for the breakout room, not the main room
6. Client disconnects main room and reconnects to breakout room
7. If assignment races with reconnect, token issuance uses the latest stored breakout assignment

### Chat send

1. Client calls `POST /chat/send`
2. Backend resolves sender identity and role from JWT
3. Backend checks `chat_enabled`
4. If disabled for students, request is rejected server-side
5. If accepted, message is stored and broadcast on websocket
6. Duplicate chat POST retries return the original stored chat message

## Reconnect flow

1. Websocket disconnect marks the participant `temporarily_disconnected`
2. A 30-second grace timer starts
3. Client reconnects with the same `client_id`
4. Client fetches `GET /class/state`
5. Client resumes websocket replay using `last_received_sequence`
6. If JWT expired, client refreshes auth and reconnects with a new token
7. If the grace timer expires first, the backend marks the participant `left`

## Recording worker flow

1. Teacher calls `POST /recording/start`
2. Backend starts LiveKit egress and persists recording state
3. Teacher calls `POST /recording/stop`
4. Backend stores recording metadata and enqueues BullMQ job
5. Worker fetches the artifact and calls AI pipeline endpoints
6. Worker stores transcript, notes, flashcards, and summary
7. If YouTube publishing is enabled, worker uploads the `.mp4`, stores `videoId`, and exposes YouTube replay URLs
8. Clients fetch status from `GET /recording/process_status`
9. Clients fetch final output from `GET /recording/process_result`

## Multi-server mode

- Set `REDIS_URL` to enable websocket pub/sub fanout and BullMQ
- Keep session, participant, breakout, chat, idempotency, and websocket replay state in shared storage
- Event ordering stays globally correct when sequence allocation is backed by PostgreSQL

## Ops endpoints

- `GET /health/live`
- `GET /health/ready`
- `GET /ops/metrics`
- `GET /ops/metrics.prometheus`

## Telemetry env

- `OTEL_ENABLED=true`
- `OTEL_SERVICE_NAME=live-class-backend`
- `OTEL_SERVICE_VERSION=0.1.0`
- `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces`
- `WS_HEARTBEAT_INTERVAL_MS=15000`
- `WS_HEARTBEAT_TIMEOUT_MS=45000`

## Load and chaos validation

Use the scripts in [load/README.md](/Users/ritamsaha/lalacore_omega/live_class_backend/load/README.md) to pressure:

- state fetch and token issuance
- chat send with idempotency
- websocket replay and reconnect storms
- readiness and metrics under failure

## Required production env

- `JWT_SECRET`
- `LIVEKIT_WS_URL` or `LIVEKIT_URL`
- `LIVEKIT_HTTP_URL` if you want to override the derived HTTP URL
- `LIVEKIT_API_KEY`
- `LIVEKIT_API_SECRET`
- `REDIS_URL`
- `DATABASE_URL` when `STORAGE_DRIVER=postgres`
- a valid teacher user matching `DEFAULT_TEACHER_ID` or seed credentials via
  `DEFAULT_TEACHER_EMAIL` plus `DEFAULT_TEACHER_PASSWORD` or
  `DEFAULT_TEACHER_PASSWORD_HASH`

For LiveKit Cloud, you can normally set:

- `LIVEKIT_URL=wss://your-project.livekit.cloud`
- `LIVEKIT_API_KEY=...`
- `LIVEKIT_API_SECRET=...`

The backend derives `https://your-project.livekit.cloud` automatically for
server-side LiveKit API calls when `LIVEKIT_HTTP_URL` is omitted.

## Optional YouTube publishing

If you want the recording pipeline to publish each completed class replay to
YouTube from the Railway/Node worker, set:

- `YOUTUBE_UPLOAD_ENABLED=true`
- either `YOUTUBE_ACCESS_TOKEN=...`
- or `YOUTUBE_CLIENT_ID=...`
- `YOUTUBE_CLIENT_SECRET=...`
- `YOUTUBE_REFRESH_TOKEN=...`

Recommended production flags:

- `YOUTUBE_UPLOAD_REQUIRED=true`
- `YOUTUBE_PRIVACY_STATUS=unlisted`
- `RECORDING_PUBLIC_BASE_URL=https://...`

`RECORDING_PUBLIC_BASE_URL` is important when LiveKit gives the worker a
relative `.mp4` path such as `recordings/<class>/<file>.mp4` and the worker
must fetch the actual binary from object storage/CDN before uploading it to
YouTube.
