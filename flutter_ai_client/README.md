# Flutter AI Client

Flutter project compatible with:
- VS Code (Flutter + Dart extensions)
- Android Studio (Flutter + Dart plugins)

## Prerequisites

- Flutter SDK (verified with Flutter 3.38.7)
- Android Studio or VS Code
- An AI engine HTTP endpoint

## Configure Your Fixed AI Engine

This app reads engine settings from compile-time defines:
- `AI_ENGINE_URL` (required)
- `AI_ENGINE_API_KEY` (optional)
- `AI_ENGINE_MODEL` (optional)

Example run:

```bash
flutter run \
  --dart-define=AI_ENGINE_URL=https://your-engine-endpoint \
  --dart-define=AI_ENGINE_API_KEY=your_api_key \
  --dart-define=AI_ENGINE_MODEL=your_model
```

## Open In VS Code

1. Open `/Users/ritamsaha/lalacore_omega/flutter_ai_client`.
2. Install extensions:
   - Dart Code
   - Flutter
3. Select an emulator/device and run with the command above.

## Open In Android Studio

1. Open `/Users/ritamsaha/lalacore_omega/flutter_ai_client`.
2. Confirm Flutter and Dart plugins are enabled.
3. Open an Android emulator.
4. Run:
   - `Run > Edit Configurations...`
   - Add `--dart-define` arguments in **Additional run args**.

## Project Structure

- `lib/main.dart`: UI and prompt flow
- `lib/src/ai_engine_client.dart`: AI engine HTTP client

