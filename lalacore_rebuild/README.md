# LalaCore Rebuild

Advanced Flutter rebuild with:
- Student and teacher workflows
- Quiz creation, exam attempt with autosave, analytics dashboard, answer key
- Study material upload/consumption with AI summarize/notes/QA
- LalaCore AI engine integration (with backend fallback)
- Liquid glass visual system across core surfaces

## What Was Removed

Per rebuild requirements, this version excludes:
- Persona-based answer styling
- Doubt thread system
- Peer chat system

## AI Engine Compatibility

The app uses compile-time defines for the dedicated AI engine:
- `AI_ENGINE_URL` (optional but recommended)
- `AI_ENGINE_API_KEY` (optional)
- `AI_ENGINE_MODEL` (optional)

If no dedicated engine URL is provided, AI calls automatically fall back to existing backend AI actions.

## Backend Compatibility

The app also supports configurable backend endpoints:
- `GOOGLE_SCRIPT_URL`
- `MASTER_SHEET_URL`

Defaults are already set to your existing script/sheet URLs in code.

## Run

```bash
cd /Users/ritamsaha/lalacore_omega/lalacore_rebuild
flutter pub get
flutter run \
  --dart-define=AI_ENGINE_URL=https://your-engine-endpoint \
  --dart-define=AI_ENGINE_API_KEY=your_api_key \
  --dart-define=AI_ENGINE_MODEL=your_model
```

## Validation Done

- `dart format lib test`
- `dart analyze`
- `flutter test`

`flutter analyze` currently fails in this environment due a local Flutter SDK analysis-server issue, not project code.
