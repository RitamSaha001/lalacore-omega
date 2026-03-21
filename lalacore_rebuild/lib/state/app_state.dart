import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';

const bool kFlutterTestMode = bool.fromEnvironment('FLUTTER_TEST');

final ValueNotifier<ThemeMode> themeNotifier = ValueNotifier<ThemeMode>(
  ThemeMode.light,
);
final ValueNotifier<PerformanceMode> performanceModeNotifier =
    ValueNotifier<PerformanceMode>(PerformanceMode.auto);

enum PerformanceMode { auto, reduced, full }

bool isLikelyWidgetTestMode() {
  if (kFlutterTestMode) {
    return true;
  }
  try {
    final String bindingType = WidgetsBinding.instance.runtimeType.toString();
    return bindingType.contains('TestWidgetsFlutterBinding') ||
        bindingType.contains('AutomatedTestWidgetsFlutterBinding') ||
        bindingType.contains('LiveTestWidgetsFlutterBinding');
  } catch (_) {
    return false;
  }
}

bool shouldUseReducedEffects(BuildContext context) {
  if (isLikelyWidgetTestMode()) {
    return true;
  }
  final PerformanceMode mode = performanceModeNotifier.value;
  if (mode == PerformanceMode.reduced) {
    return true;
  }
  if (mode == PerformanceMode.full) {
    return false;
  }
  final MediaQueryData media = MediaQuery.of(context);
  final bool lowEndAndroid =
      Theme.of(context).platform == TargetPlatform.android &&
      (media.size.shortestSide < 400 || media.devicePixelRatio <= 2.0);
  return media.disableAnimations || lowEndAndroid;
}

Future<void> loadThemePreference() async {
  if (isLikelyWidgetTestMode()) {
    themeNotifier.value = ThemeMode.light;
    performanceModeNotifier.value = PerformanceMode.reduced;
    return;
  }
  final SharedPreferences prefs = await SharedPreferences.getInstance();
  themeNotifier.value = (prefs.getBool('isDark') ?? false)
      ? ThemeMode.dark
      : ThemeMode.light;
  final String mode = (prefs.getString('perf_mode') ?? 'auto').toLowerCase();
  performanceModeNotifier.value = switch (mode) {
    'reduced' => PerformanceMode.reduced,
    'full' => PerformanceMode.full,
    _ => PerformanceMode.auto,
  };
}

Future<void> toggleTheme() async {
  final bool isDark = themeNotifier.value == ThemeMode.dark;
  themeNotifier.value = isDark ? ThemeMode.light : ThemeMode.dark;
  final SharedPreferences prefs = await SharedPreferences.getInstance();
  await prefs.setBool('isDark', !isDark);
}

Future<void> setPerformanceMode(PerformanceMode mode) async {
  performanceModeNotifier.value = mode;
  final SharedPreferences prefs = await SharedPreferences.getInstance();
  await prefs.setString('perf_mode', mode.name);
}

class AppColors {
  const AppColors._();

  static const Color primary = Color(0xFF2F7DFF);
  static const Color primaryDark = Color(0xFF9FBEDC);
  static const Color secondary = Color(0xFF6D63E8);
  static const Color success = Color(0xFF23A380);
  static const Color successDark = Color(0xFF8FBFAC);
  static const Color error = Color(0xFFFF3B30);
  static const Color bgLight = Color(0xFFF2F2F7);
  static const Color cardLight = Colors.white;
  static const Color bgDark = Color(0xFF0C1118);
  static const Color cardDark = Color(0xFF161E29);
  static const Color orange = Color(0xFFFF9500);

  static const Color blueDark = Color(0xFFA8C0D9);
  static const Color greenDark = Color(0xFF95BFAE);
  static const Color tealDark = Color(0xFF9BC4C4);

  static bool _isDark(BuildContext context) {
    return Theme.of(context).brightness == Brightness.dark;
  }

  static Color primaryTone(BuildContext context) {
    return _isDark(context) ? primaryDark : primary;
  }

  static Color successTone(BuildContext context) {
    return _isDark(context) ? successDark : success;
  }

  static Color blueTone(BuildContext context) {
    return _isDark(context) ? blueDark : Colors.blue;
  }

  static Color greenTone(BuildContext context) {
    return _isDark(context) ? greenDark : Colors.green;
  }

  static Color tealTone(BuildContext context) {
    return _isDark(context) ? tealDark : Colors.teal;
  }
}
