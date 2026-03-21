import 'dart:math' as math;

import 'package:flutter/material.dart';

import '../state/app_state.dart';

class LiquidBackground extends StatefulWidget {
  const LiquidBackground({super.key, required this.child});

  final Widget child;

  @override
  State<LiquidBackground> createState() => _LiquidBackgroundState();
}

class _LiquidBackgroundState extends State<LiquidBackground>
    with SingleTickerProviderStateMixin, WidgetsBindingObserver {
  late final AnimationController _ctrl;
  bool _appActive = true;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _ctrl = AnimationController(
      vsync: this,
      duration: const Duration(seconds: 18),
    );
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _ctrl.dispose();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    final bool active = state == AppLifecycleState.resumed;
    if (_appActive == active) {
      return;
    }
    _appActive = active;
    if (!active) {
      _ctrl.stop();
      return;
    }
    if (performanceModeNotifier.value != PerformanceMode.reduced &&
        !isLikelyWidgetTestMode() &&
        !_ctrl.isAnimating) {
      _ctrl.repeat();
    }
  }

  void _syncAnimation(PerformanceMode mode) {
    final bool shouldAnimate =
        _appActive &&
        mode != PerformanceMode.reduced &&
        !isLikelyWidgetTestMode();
    if (shouldAnimate) {
      if (!_ctrl.isAnimating) {
        _ctrl.repeat();
      }
    } else if (_ctrl.isAnimating) {
      _ctrl.stop();
    }
  }

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<PerformanceMode>(
      valueListenable: performanceModeNotifier,
      builder: (BuildContext context, PerformanceMode mode, _) {
        _syncAnimation(mode);
        final bool reduce = mode == PerformanceMode.reduced;
        final bool isDark = Theme.of(context).brightness == Brightness.dark;
        final Widget paintedLayer = reduce
            ? CustomPaint(painter: _LiquidPainter(progress: 0, isDark: isDark))
            : AnimatedBuilder(
                animation: _ctrl,
                builder: (_, __) {
                  return CustomPaint(
                    painter: _LiquidPainter(
                      progress: _ctrl.value,
                      isDark: isDark,
                    ),
                  );
                },
              );
        return Stack(
          fit: StackFit.expand,
          children: <Widget>[
            Positioned.fill(
              child: IgnorePointer(child: RepaintBoundary(child: paintedLayer)),
            ),
            widget.child,
          ],
        );
      },
    );
  }
}

class _LiquidPainter extends CustomPainter {
  const _LiquidPainter({required this.progress, required this.isDark});

  final double progress;
  final bool isDark;

  @override
  void paint(Canvas canvas, Size size) {
    final Rect rect = Offset.zero & size;
    final Paint base = Paint()
      ..shader = LinearGradient(
        begin: Alignment.topCenter,
        end: Alignment.bottomCenter,
        colors: isDark
            ? const <Color>[
                Color(0xFF0B111A),
                Color(0xFF121C2A),
                Color(0xFF162638),
              ]
            : const <Color>[
                Color(0xFFF5FAFF),
                Color(0xFFEFF6FF),
                Color(0xFFF8FBFF),
              ],
      ).createShader(rect);
    canvas.drawRect(rect, base);

    final double phase = progress * math.pi * 2;
    final Offset c1 = Offset(
      size.width * (0.18 + 0.06 * math.sin(phase)),
      size.height * (0.24 + 0.06 * math.cos(phase * 0.7)),
    );
    final Offset c2 = Offset(
      size.width * (0.82 + 0.05 * math.cos(phase * 0.9)),
      size.height * (0.68 + 0.05 * math.sin(phase * 0.6)),
    );
    final double r1 = size.shortestSide * 0.62;
    final double r2 = size.shortestSide * 0.58;

    final Paint glow1 = Paint()
      ..shader = RadialGradient(
        colors: isDark
            ? const <Color>[Color(0x2240B6FF), Color(0x002C7ED0)]
            : const <Color>[Color(0x6680D6FF), Color(0x00BEE9FF)],
      ).createShader(Rect.fromCircle(center: c1, radius: r1));
    final Paint glow2 = Paint()
      ..shader = RadialGradient(
        colors: isDark
            ? const <Color>[Color(0x2238E0C0), Color(0x0038E0C0)]
            : const <Color>[Color(0x55A7F4E7), Color(0x00A7F4E7)],
      ).createShader(Rect.fromCircle(center: c2, radius: r2));

    canvas.drawCircle(c1, r1, glow1);
    canvas.drawCircle(c2, r2, glow2);
  }

  @override
  bool shouldRepaint(covariant _LiquidPainter oldDelegate) {
    return oldDelegate.progress != progress || oldDelegate.isDark != isDark;
  }
}
