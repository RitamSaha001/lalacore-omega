import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import 'liquid_glass.dart';

class PremiumExamTimer extends StatefulWidget {
  const PremiumExamTimer({
    super.key,
    required this.secondsLeft,
    required this.totalSeconds,
    this.onTimesUp,
  });

  final ValueListenable<int> secondsLeft;
  final int totalSeconds;
  final VoidCallback? onTimesUp;

  @override
  State<PremiumExamTimer> createState() => _PremiumExamTimerState();
}

class _PremiumExamTimerState extends State<PremiumExamTimer> {
  int _lastSeen = -1;
  bool _endedPulse = false;
  final Set<int> _hapticMarks = <int>{};

  @override
  void initState() {
    super.initState();
    widget.secondsLeft.addListener(_onTick);
    _lastSeen = widget.secondsLeft.value;
  }

  @override
  void didUpdateWidget(covariant PremiumExamTimer oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.secondsLeft != widget.secondsLeft) {
      oldWidget.secondsLeft.removeListener(_onTick);
      widget.secondsLeft.addListener(_onTick);
      _lastSeen = widget.secondsLeft.value;
      _hapticMarks.clear();
      _endedPulse = false;
    }
  }

  @override
  void dispose() {
    widget.secondsLeft.removeListener(_onTick);
    super.dispose();
  }

  void _onTick() {
    final int sec = widget.secondsLeft.value;
    if (sec == _lastSeen) {
      return;
    }
    _lastSeen = sec;

    if (sec <= 0 && !_endedPulse) {
      _endedPulse = true;
      HapticFeedback.heavyImpact();
      widget.onTimesUp?.call();
      return;
    }

    if (sec > 0 && _endedPulse) {
      _endedPulse = false;
    }
    if (sec > 0 && sec % 60 == 0) {
      HapticFeedback.lightImpact();
    }
    _emitUrgencyHaptic(sec);
  }

  void _emitUrgencyHaptic(int sec) {
    if (sec <= 0 || sec > 300) {
      return;
    }
    final _UrgencyProfile profile = _urgencyProfileFor(sec);
    if (sec % profile.cadence != 0) {
      return;
    }
    if (!_hapticMarks.add(sec)) {
      return;
    }

    switch (profile.level) {
      case _UrgencyLevel.calm:
        HapticFeedback.selectionClick();
        break;
      case _UrgencyLevel.attention:
        HapticFeedback.lightImpact();
        break;
      case _UrgencyLevel.warning:
        HapticFeedback.mediumImpact();
        break;
      case _UrgencyLevel.critical:
        HapticFeedback.heavyImpact();
        if (sec <= 20) {
          Future<void>.delayed(const Duration(milliseconds: 120), () {
            if (!mounted || widget.secondsLeft.value != sec) {
              return;
            }
            HapticFeedback.selectionClick();
          });
        }
        break;
    }
  }

  _UrgencyProfile _urgencyProfileFor(int sec) {
    if (sec <= 20) {
      return const _UrgencyProfile(cadence: 4, level: _UrgencyLevel.critical);
    }
    if (sec <= 60) {
      return const _UrgencyProfile(cadence: 8, level: _UrgencyLevel.warning);
    }
    if (sec <= 120) {
      return const _UrgencyProfile(cadence: 12, level: _UrgencyLevel.attention);
    }
    return const _UrgencyProfile(cadence: 24, level: _UrgencyLevel.calm);
  }

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<int>(
      valueListenable: widget.secondsLeft,
      builder: (_, int value, __) {
        final int mm = max(0, value ~/ 60);
        final int ss = max(0, value % 60);
        final String text =
            '${mm.toString().padLeft(2, '0')}${ss.toString().padLeft(2, '0')}';
        final bool critical = value <= 300;
        final bool danger = value <= 120;
        final double frac = widget.totalSeconds <= 0
            ? 0
            : (value / widget.totalSeconds).clamp(0.0, 1.0).toDouble();
        final Color accent = frac > 0.3
            ? Colors.green
            : frac > 0.1
            ? Colors.orange
            : Colors.redAccent;
        final double pulse = danger ? (value.isEven ? 1.012 : 1.0) : 1.0;

        return AnimatedScale(
          duration: const Duration(milliseconds: 220),
          curve: Curves.easeOut,
          scale: pulse,
          child: LiquidGlass(
            borderRadius: BorderRadius.circular(30),
            quality: LiquidGlassQuality.low,
            enableRipple: false,
            enableHaptics: false,
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: <Widget>[
                Icon(
                  Icons.av_timer_rounded,
                  size: 14,
                  color: accent.withValues(alpha: 0.92),
                ),
                const SizedBox(width: 6),
                _Digit(text[0], accent: accent, critical: critical),
                _Digit(text[1], accent: accent, critical: critical),
                Padding(
                  padding: const EdgeInsets.symmetric(horizontal: 4),
                  child: Text(
                    ':',
                    style: TextStyle(
                      fontSize: 20,
                      fontWeight: FontWeight.w700,
                      color: accent,
                    ),
                  ),
                ),
                _Digit(text[2], accent: accent, critical: critical),
                _Digit(text[3], accent: accent, critical: critical),
              ],
            ),
          ),
        );
      },
    );
  }
}

enum _UrgencyLevel { calm, attention, warning, critical }

class _UrgencyProfile {
  const _UrgencyProfile({required this.cadence, required this.level});

  final int cadence;
  final _UrgencyLevel level;
}

class _Digit extends StatelessWidget {
  const _Digit(this.value, {required this.accent, required this.critical});

  final String value;
  final Color accent;
  final bool critical;

  @override
  Widget build(BuildContext context) {
    return AnimatedContainer(
      duration: const Duration(milliseconds: 220),
      curve: Curves.easeOutBack,
      width: 20,
      margin: const EdgeInsets.symmetric(horizontal: 1),
      padding: const EdgeInsets.symmetric(vertical: 2),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(10),
        color: accent.withValues(alpha: 0.14),
        boxShadow: critical
            ? <BoxShadow>[
                BoxShadow(
                  color: accent.withValues(alpha: 0.20),
                  blurRadius: 10,
                  spreadRadius: 0.4,
                ),
              ]
            : const <BoxShadow>[],
      ),
      child: AnimatedSwitcher(
        duration: const Duration(milliseconds: 180),
        switchInCurve: Curves.easeOutBack,
        switchOutCurve: Curves.easeIn,
        transitionBuilder: (Widget child, Animation<double> animation) {
          final Animation<Offset> slide = Tween<Offset>(
            begin: const Offset(0, 0.22),
            end: Offset.zero,
          ).animate(animation);
          return SlideTransition(
            position: slide,
            child: FadeTransition(opacity: animation, child: child),
          );
        },
        child: Text(
          value,
          key: ValueKey<String>(value),
          textAlign: TextAlign.center,
          style: TextStyle(
            fontFeatures: const <FontFeature>[FontFeature.tabularFigures()],
            fontSize: 16,
            fontWeight: FontWeight.w800,
            color: accent,
          ),
        ),
      ),
    );
  }
}
