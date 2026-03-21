import 'package:flutter_test/flutter_test.dart';

extension DeterministicPumpExtension on WidgetTester {
  Future<void> pumpDeterministic([
    Duration step = const Duration(milliseconds: 16),
    int maxTicks = 600,
    int minTicks = 80,
    int idleTicksToFinish = 12,
  ]) async {
    final TestWidgetsFlutterBinding binding =
        TestWidgetsFlutterBinding.ensureInitialized();
    int idleTicks = 0;
    for (int i = 0; i < maxTicks; i++) {
      await pump(step);
      final bool hasWork =
          binding.hasScheduledFrame || binding.transientCallbackCount > 0;
      if (hasWork) {
        idleTicks = 0;
        continue;
      }
      idleTicks += 1;
      if ((i + 1) >= minTicks && idleTicks >= idleTicksToFinish) {
        break;
      }
    }
  }

  Future<bool> pumpUntilFound(
    Finder finder, {
    Duration step = const Duration(milliseconds: 100),
    int maxTicks = 120,
  }) async {
    for (int i = 0; i < maxTicks; i++) {
      if (finder.evaluate().isNotEmpty) {
        return true;
      }
      await pump(step);
    }
    return finder.evaluate().isNotEmpty;
  }
}
