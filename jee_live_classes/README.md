# JEE Live Classes

Production-ready live classroom module with join/waiting room, live class UI, quick polls, recording/transcription/OCR/AI pipelines, and main-app embedding support.

## Real-Service Mode (Required Defines)

Enable real mode:

- `LIVE_CLASSES_ENABLE_REAL_SERVICES=true`

Required backend/auth defines:

- `LIVE_CLASSES_API_BASE_URL`
- `LIVE_CLASSES_SESSION_TOKEN` or `LIVE_CLASSES_JWT_ACCESS_TOKEN`
- `LIVE_CLASSES_TRANSCRIPTION_WS_URL`
- `LIVE_CLASSES_OCR_ENDPOINT`
- `LIVE_CLASSES_REQUEST_SIGNING_SECRET`

Recommended identity/class defines:

- `LIVE_CLASSES_CLASS_ID`
- `LIVE_CLASSES_USER_ID`
- `LIVE_CLASSES_USER_NAME`
- `LIVE_CLASSES_USER_ROLE` (`student` or `teacher`)
- `LIVE_CLASSES_DEFAULT_TITLE`
- `LIVE_CLASSES_DEFAULT_SUBJECT`
- `LIVE_CLASSES_DEFAULT_TOPIC`
- `LIVE_CLASSES_DEFAULT_TEACHER_NAME`
- `LIVE_CLASSES_DEFAULT_START_TIME`

If real mode is enabled and required values are missing, bootstrap now fails fast with a clear setup error screen.

## Backend Endpoints

You can override endpoint paths with environment defines (defaults in parentheses):

Class join/waiting room:

- `LIVE_CLASSES_ENDPOINT_CLASS_SESSION` (`/class/session`)
- `LIVE_CLASSES_ENDPOINT_CLASS_STATE` (`/class/state`)
- `LIVE_CLASSES_ENDPOINT_CLASS_JOIN_REQUEST` (`/class/join_request`)
- `LIVE_CLASSES_ENDPOINT_CLASS_JOIN_CANCEL` (`/class/join_cancel`)
- `LIVE_CLASSES_ENDPOINT_CLASS_ADMIT` (`/class/admit`)
- `LIVE_CLASSES_ENDPOINT_CLASS_REJECT` (`/class/reject`)
- `LIVE_CLASSES_ENDPOINT_CLASS_ADMIT_ALL` (`/class/admit_all`)
- `LIVE_CLASSES_ENDPOINT_CLASS_EVENTS` (`/class/events`)
- `LIVE_CLASSES_ENDPOINT_CLASS_SYNC` (`/class/sync`)
- `LIVE_CLASSES_ENDPOINT_CLASS_LOCK` (`/class/lock`)
- `LIVE_CLASSES_ENDPOINT_CLASS_CHAT` (`/class/chat`)
- `LIVE_CLASSES_ENDPOINT_CLASS_WAITING_ROOM` (`/class/waiting_room`)
- `LIVE_CLASSES_ENDPOINT_CLASS_RECORDING` (`/class/recording`)
- `LIVE_CLASSES_ENDPOINT_CLASS_MUTE` (`/class/mute`)
- `LIVE_CLASSES_ENDPOINT_CLASS_BREAKOUT_MOVE` (`/class/breakout/move`)
- `LIVE_CLASSES_ENDPOINT_CLASS_BREAKOUT_BROADCAST` (`/class/breakout/broadcast`)
- `LIVE_CLASSES_ENDPOINT_CLASS_WHITEBOARD_ACCESS` (`/class/whiteboard/access`)
- `LIVE_CLASSES_ENDPOINT_HEALTH_PING` (`/health/ping`)

Quiz + live poll:

- `LIVE_CLASSES_ENDPOINT_QUIZ_CREATE` (`/quiz/create`)
- `LIVE_CLASSES_ENDPOINT_QUIZ_START` (`/quiz/start`)
- `LIVE_CLASSES_ENDPOINT_QUIZ_SUBMIT` (`/quiz/submit`)
- `LIVE_CLASSES_ENDPOINT_QUIZ_RESULTS` (`/quiz/results`)
- `LIVE_CLASSES_ENDPOINT_QUIZ_LIBRARY` (`/quiz/library`)
- `LIVE_CLASSES_ENDPOINT_LIVE_POLL_CREATE` (`/live_poll/create`)
- `LIVE_CLASSES_ENDPOINT_LIVE_POLL_SUBMIT` (`/live_poll/submit`)
- `LIVE_CLASSES_ENDPOINT_LIVE_POLL_RESULTS` (`/live_poll/results`)
- `LIVE_CLASSES_ENDPOINT_LIVE_POLL_END` (`/live_poll/end`)
- `LIVE_CLASSES_ENDPOINT_PRACTICE_EXTRACT` (`/practice/extract`)
- `LIVE_CLASSES_ENDPOINT_PRACTICE_REVIEW_QUEUE` (`/practice/review_queue`)
- `LIVE_CLASSES_ENDPOINT_PRACTICE_REVIEW_ACTION` (`/practice/review_action`)

Recording:

- `LIVE_CLASSES_ENDPOINT_RECORDING_START` (`/recording/start`)
- `LIVE_CLASSES_ENDPOINT_RECORDING_STOP` (`/recording/stop`)
- `LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS` (`/recording/process`)
- `LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_ASYNC` (`/recording/process_async`)
- `LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_STATUS` (`/recording/process_status`)
- `LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_RESULT` (`/recording/process_result`)
- `LIVE_CLASSES_ENDPOINT_RECORDING_REPLAY` (`/recording/replay`)

AI (LalaCore):

- `LIVE_CLASSES_ENDPOINT_AI_EXPLAIN` (`/ai/class/explain`)
- `LIVE_CLASSES_ENDPOINT_AI_NOTES` (`/ai/class/notes`)
- `LIVE_CLASSES_ENDPOINT_AI_QUIZ` (`/ai/class/quiz`)
- `LIVE_CLASSES_ENDPOINT_AI_CONCEPTS` (`/ai/class/concepts`)
- `LIVE_CLASSES_ENDPOINT_AI_FLASHCARDS` (`/ai/class/flashcards`)
- `LIVE_CLASSES_ENDPOINT_AI_ANALYSIS` (`/ai/class/analysis`)

Failover:

- `LIVE_CLASSES_ENDPOINT_WEBRTC_FALLBACK` (`/class/fallback_token`)

RTC provider:

- `LIVE_CLASSES_RTC_PROVIDER` (`native_bridge` or `livekit`)
- `LIVEKIT_API_KEY`
- `LIVEKIT_API_SECRET`
- `LIVEKIT_WS_URL`

When `LIVE_CLASSES_RTC_PROVIDER=livekit`, the backend `POST /live/token` route now issues a LiveKit-compatible JWT and returns `ws_url` for the client transport.

## Zoom Native Channel

`RealZoomService` uses method channel:

- `jee_live_classes/zoom_videosdk`

Platform handlers are implemented in:

- Android: `android/app/src/main/kotlin/com/example/jee_live_classes/MainActivity.kt`
- iOS: `ios/Runner/AppDelegate.swift`

These handlers now implement all method names/events consumed by Dart (`joinSession`, media toggles, waiting room, moderation, active speaker, network quality, reactions, screen share, recording hooks).

To wire full Zoom Video SDK media transport in production, replace internal simulated state operations in these handlers with actual Zoom Video SDK calls and callbacks while keeping the same method/event contract.

## Android/iOS Permissions

Configured:

- Android: camera/mic/network/audio/bluetooth/notifications in `AndroidManifest.xml`
- iOS: camera/mic/photo usage descriptions in `Info.plist`

## Quick Run (Standalone Module)

1. Create runtime secrets file:

```bash
cp /Users/ritamsaha/lalacore_omega/jee_live_classes/.env.live_classes.example \
   /Users/ritamsaha/lalacore_omega/jee_live_classes/.env.live_classes
```

2. Fill real values in `.env.live_classes`.
3. Use the helper script:

```bash
cd /Users/ritamsaha/lalacore_omega/jee_live_classes
./tool/run_real_live_classes.sh
```

Or run manually:

```bash
flutter run \
  --dart-define=LIVE_CLASSES_ENABLE_REAL_SERVICES=true \
  --dart-define=LIVE_CLASSES_API_BASE_URL=https://your-api.example.com \
  --dart-define=LIVE_CLASSES_TRANSCRIPTION_WS_URL=wss://your-api.example.com/transcription/stream \
  --dart-define=LIVE_CLASSES_OCR_ENDPOINT=https://your-api.example.com/ocr/frame \
  --dart-define=LIVE_CLASSES_REQUEST_SIGNING_SECRET=replace_me \
  --dart-define=LIVE_CLASSES_SESSION_TOKEN=replace_me \
  --dart-define=LIVE_CLASSES_CLASS_ID=physics_live_01 \
  --dart-define=LIVE_CLASSES_USER_ID=student_123 \
  --dart-define=LIVE_CLASSES_USER_NAME="Ritam Saha" \
  --dart-define=LIVE_CLASSES_USER_ROLE=student
```

## Real E2E Audit Runner

Runs a backend E2E probe for:

- join class
- failover token
- recording pipeline
- AI notes/analysis
- doubt queue (AI + backend action probe)
- practice extraction/review queue

```bash
cd /Users/ritamsaha/lalacore_omega/jee_live_classes
./tool/run_real_e2e_classroom_audit.sh
```

Report output:

- `build/reports/live_class_real_e2e_report.json`

## 50-Student Stress Simulation

Runs a local 50-student simulation for:

- join latency
- sync latency
- failover stability
- API usage estimate

```bash
cd /Users/ritamsaha/lalacore_omega/jee_live_classes
./tool/run_stress_test_50_students.sh
```

Report output:

- `build/reports/live_class_stress_report.json`
