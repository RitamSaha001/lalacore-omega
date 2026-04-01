# Load And Chaos Harness

These scripts are for real resilience validation of the authoritative backend.
They intentionally exercise:

- token issuance under concurrency
- chat transport under retries
- websocket replay and reconnect storms
- readiness and metrics visibility during load

## 1. HTTP Load With k6

This script hits:

- `GET /class/state`
- `GET /live-token`
- `POST /chat/send`
- teacher chat-toggle traffic when `TEACHER_ACCESS_TOKEN` is provided

Run:

```bash
k6 run \
  -e BASE_URL=http://localhost:8080 \
  -e CLASS_ID=physics_live_01 \
  -e TEACHER_ACCESS_TOKEN=... \
  -e PARTICIPANT_ACCESS_TOKENS=token1,token2,token3 \
  load/k6-classroom.js
```

## 2. WebSocket Reconnect Storm

This script opens many websocket clients, force-drops a random percentage, and
measures replay/reconnect recovery.

Run:

```bash
node load/ws-reconnect-storm.mjs
```

Environment:

- `BASE_URL`
- `CLASS_ID`
- `ACCESS_TOKEN`
- `CLIENTS`
- `DROP_PERCENT`
- `TEST_DURATION_MS`
- `STORM_INTERVAL_MS`
- `WS_CHANNEL`

Example:

```bash
BASE_URL=http://localhost:8080 \
CLASS_ID=physics_live_01 \
ACCESS_TOKEN=eyJ... \
CLIENTS=60 \
DROP_PERCENT=0.4 \
TEST_DURATION_MS=120000 \
node load/ws-reconnect-storm.mjs
```

## 3. Ops Endpoints During Load

Keep these open while testing:

- `GET /health/ready`
- `GET /ops/metrics`
- `GET /ops/metrics.prometheus`

Recommended watch loop:

```bash
watch -n 2 "curl -s http://localhost:8080/health/ready && echo && curl -s http://localhost:8080/ops/metrics.prometheus | head -n 80"
```

## 4. Network Chaos Matrix

Run the websocket storm alongside device-level shaping:

- high latency: `250-450ms`
- packet loss: `5-20%`
- mobile handoff: Wi-Fi to hotspot and back
- app background/foreground
- device sleep/wake

Success criteria:

- no silent websocket hangs
- replay resumes from the last acknowledged sequence
- participants remain `temporarily_disconnected` inside the grace window
- no duplicate chat messages after retries
