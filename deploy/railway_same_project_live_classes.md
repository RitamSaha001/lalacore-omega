# Railway Same-Project Live Classes Setup

This project can run all production pieces inside a single Railway **project**
while still keeping the services cleanly separated.

Recommended layout:

1. `lalacore-omega`
   Root directory: `/`
   Runtime: Python FastAPI
   Purpose: auth, app data, Atlas, update feed, automail, student/teacher APIs

2. `live-class-api`
   Root directory: `/live_class_backend`
   Runtime: Node
   Start command: `npm start`
   Purpose: breakout rooms, recording routes, LiveKit authorization, classroom authority

3. `live-class-worker`
   Root directory: `/live_class_backend`
   Runtime: Node worker
   Start command: `npm run worker`
   Purpose: async recording processing, replay generation, optional YouTube publishing

All 3 services can live in the same Railway project and share the same Project
Settings -> Shared Variables pool.

## Why this is the right setup

- The current root Railway service starts only Python:
  - [Dockerfile](/Users/ritamsaha/lalacore_omega/Dockerfile)
  - [railway.json](/Users/ritamsaha/lalacore_omega/railway.json)
- The live-class backend is already a separate Node app:
  - [live_class_backend/package.json](/Users/ritamsaha/lalacore_omega/live_class_backend/package.json)
  - [live_class_backend/railway.json](/Users/ritamsaha/lalacore_omega/live_class_backend/railway.json)
- The recording worker is already a separate process:
  - [live_class_backend/Procfile](/Users/ritamsaha/lalacore_omega/live_class_backend/Procfile)

Trying to run Python API + Node API + Node worker inside one single Railway
service would require a custom process supervisor and would be less reliable.

## Shared Variables

Keep these in Project Settings -> Shared Variables:

```env
APP_PUBLIC_BASE_URL=https://lalacore-omega-production.up.railway.app
APP_UPDATE_CONFIRMATION_ENABLED=true
APP_UPDATE_SHEET_URL=https://docs.google.com/spreadsheets/d/1Il-ojLV1TecCPG43_a_ookL-Hb6EA46zS--a2xjVhng/export?format=csv&gid=1537205702

ATLAS_ASSIGNMENT_ANNOUNCEMENT_ENABLED=true
ATLAS_AUTOMAIL_ALLOW_SMTP_FALLBACK=false
ATLAS_AUTOMAIL_TIMEOUT_SECONDS=40
ATLAS_AUTOMAIL_WEBHOOK_URL=<apps-script-webhook>
ATLAS_SUPPORT_EMAIL_RECIPIENT=saharitam171@gmail.com,sanny86@gmail.com,halder.saptajit2009@gmail.com
ATLAS_ASSESSMENT_SUBMISSION_EMAIL_RECIPIENT=sanny86@gmail.com

DATABASE_URL=<railway-postgres-url>
REDIS_URL=<railway-redis-url>

JWT_SECRET=<shared-strong-random-secret>
LIVEKIT_API_KEY=<livekit-api-key>
LIVEKIT_API_SECRET=<livekit-api-secret>
LIVEKIT_WS_URL=wss://lalacoreomega-7r673nef.livekit.cloud

OPENROUTER_KEYS=<openrouter-key>
GEMINI_KEYS=<gemini-key>
GROQ_KEYS=<groq-key>
HF_KEYS=<hf-key-if-used>

OTP_FROM_NAME=God of Maths
OTP_SENDER_EMAIL=<smtp-sender-email>
OTP_SENDER_PASSWORD=<smtp-password>
OTP_SMTP_HOST=smtp.gmail.com
OTP_SMTP_PORT=587
OTP_SMTP_SECURITY=tls
```

Optional shared AI/STT variables:

```env
STT_PROVIDER=deepgram
DEEPGRAM_API_KEY=<deepgram-key>
DEEPGRAM_MODEL=nova-2
DEEPGRAM_LANGUAGE=multi
OPENAI_API_KEY=<openai-key-if-used>
OPENAI_STT_MODEL=gpt-4o-mini-transcribe

ATLAS_ENGINE_PRIMARY_PROVIDER=openrouter
ATLAS_ENGINE_PRIMARY_MODEL=openai/gpt-4o-mini
ATLAS_ENGINE_FALLBACK_1_PROVIDER=gemini
ATLAS_ENGINE_FALLBACK_1_MODEL=models/gemini-1.5-flash
ATLAS_ENGINE_FALLBACK_2_PROVIDER=groq
ATLAS_ENGINE_FALLBACK_2_MODEL=llama-3.3-70b-versatile
ATLAS_ENGINE_TIMEOUT_S=28
ATLAS_ENGINE_RETRY_COUNT=1
```

## Service-Specific Variables

### `lalacore-omega`

```env
APP_ENV=production
NODE_ENV=production
LC9_DISABLE_DISCOVERY=true

LIVE_CLASSES_RTC_PROVIDER=livekit
LIVE_CLASSES_TOKEN_SECRET=<strong-random-secret>
REQUEST_SIGNING_SECRET=<strong-random-secret>

LIVE_CLASSES_BACKEND_PROXY_URL=http://live-class-api.railway.internal:8080
LIVE_CLASSES_ALLOW_RECORDING_FALLBACK=false

APP_UPDATE_CONFIRMATION_TICK_SECONDS=300
```

Notes:

- `LIVE_CLASSES_BACKEND_PROXY_URL` should use Railway private networking so the
  Python backend talks to the Node live-class API internally.
- Railway docs recommend using the internal hostname plus the service port for
  private networking.

### `live-class-api`

```env
NODE_ENV=production
DATABASE_SSL=true
AI_PIPELINE_BASE_URL=https://lalacore-omega-production.up.railway.app
```

No custom start command is needed if the service root directory is
`/live_class_backend`, because [live_class_backend/railway.json](/Users/ritamsaha/lalacore_omega/live_class_backend/railway.json)
already uses `npm start`.

### `live-class-worker`

```env
NODE_ENV=production
DATABASE_SSL=true
AI_PIPELINE_BASE_URL=https://lalacore-omega-production.up.railway.app

YOUTUBE_UPLOAD_ENABLED=true
YOUTUBE_UPLOAD_REQUIRED=false
YOUTUBE_CLIENT_ID=<youtube-client-id>
YOUTUBE_CLIENT_SECRET=<youtube-client-secret>
YOUTUBE_REFRESH_TOKEN=<youtube-refresh-token>
YOUTUBE_PRIVACY_STATUS=unlisted
RECORDING_PUBLIC_BASE_URL=<public-recording-cdn-or-storage-base>
```

Worker start command must be:

```bash
npm run worker
```

This matches [live_class_backend/package.json](/Users/ritamsaha/lalacore_omega/live_class_backend/package.json)
and [live_class_backend/Procfile](/Users/ritamsaha/lalacore_omega/live_class_backend/Procfile).

## Railway Dashboard Steps

1. Keep the current service `lalacore-omega` as your main Python backend.
2. Add a new service named `live-class-api`.
3. Connect it to the same GitHub repo.
4. Set its Root Directory to `/live_class_backend`.
5. Let it use the root directory's `railway.json`, which starts `npm start`.
6. Add another new service named `live-class-worker`.
7. Connect it to the same GitHub repo.
8. Set its Root Directory to `/live_class_backend`.
9. Override the start command to `npm run worker`.
10. Attach the shared variables to both services.
11. Add the service-specific variables above.
12. Deploy all 3 services.

## Verification Checklist

After deployment:

1. Main backend:
   - `GET /health/ready`
   - `POST /live/token` returns `provider: "livekit"`

2. Live-class API:
   - `GET /health/ready`
   - `POST /breakout/create` returns `200`
   - `POST /recording/start` returns `200`

3. Worker:
   - check logs for successful Redis connection
   - check logs for recording queue startup

4. End-to-end:
   - publish a class
   - student requests join
   - teacher admits
   - breakout room create/list works
   - recording start/stop/process works
   - replay endpoint returns result

## Notes

- If you stay on a single Python service only, set
  `LIVE_CLASSES_ALLOW_RECORDING_FALLBACK=true` and leave
  `LIVE_CLASSES_BACKEND_PROXY_URL` empty.
- If you use the 3-service setup above, set
  `LIVE_CLASSES_ALLOW_RECORDING_FALLBACK=false`.
- Railway private networking docs:
  - [Private Networking](https://docs.railway.com/private-networking)
  - [Using Variables](https://docs.railway.com/variables)
  - [Deploying a Monorepo](https://docs.railway.com/guides/monorepo)
