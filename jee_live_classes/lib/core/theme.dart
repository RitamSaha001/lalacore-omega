import 'package:flutter/material.dart';

class AppTheme {
  static const Color _ink900 = Color(0xFF081020);
  static const Color _ink700 = Color(0xFF18304E);
  static const Color _aqua500 = Color(0xFF36C5F0);
  static const Color _lime300 = Color(0xFF8CEB7C);

  static ThemeData get lightTheme {
    final base = ThemeData.light(useMaterial3: true);

    return base.copyWith(
      scaffoldBackgroundColor: const Color(0xFFF3F8FF),
      colorScheme: ColorScheme.fromSeed(
        seedColor: _aqua500,
        primary: _ink900,
        secondary: _aqua500,
        tertiary: _lime300,
        brightness: Brightness.light,
      ),
      textTheme: base.textTheme.apply(
        fontFamily: 'Avenir Next',
        bodyColor: _ink900,
        displayColor: _ink900,
      ),
      appBarTheme: const AppBarTheme(
        backgroundColor: Colors.white,
        foregroundColor: _ink900,
        elevation: 0,
      ),
      pageTransitionsTheme: const PageTransitionsTheme(
        builders: {
          TargetPlatform.android: CupertinoPageTransitionsBuilder(),
          TargetPlatform.iOS: CupertinoPageTransitionsBuilder(),
          TargetPlatform.macOS: CupertinoPageTransitionsBuilder(),
        },
      ),
      cardTheme: CardThemeData(
        color: Colors.white.withValues(alpha: 0.9),
        elevation: 0,
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      ),
      iconTheme: const IconThemeData(color: _ink700),
      dividerTheme: const DividerThemeData(color: Color(0xFFE2EBF8)),
    );
  }
}
