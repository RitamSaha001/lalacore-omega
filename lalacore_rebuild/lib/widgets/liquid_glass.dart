import 'dart:ui' as ui;

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../state/app_state.dart';

enum LiquidGlassQuality { adaptive, high, low }

class LiquidGlass extends StatefulWidget {
  const LiquidGlass({
    super.key,
    required this.child,
    this.padding,
    this.color,
    this.solidFill = false,
    this.onTap,
    this.borderRadius = const BorderRadius.all(Radius.circular(24)),
    this.quality = LiquidGlassQuality.adaptive,
    this.enableRipple = true,
    this.enableHaptics = true,
  });

  final Widget child;
  final EdgeInsets? padding;
  final Color? color;
  final bool solidFill;
  final VoidCallback? onTap;
  final BorderRadius borderRadius;
  final LiquidGlassQuality quality;
  final bool enableRipple;
  final bool enableHaptics;

  @override
  State<LiquidGlass> createState() => _LiquidGlassState();
}

class _LiquidGlassState extends State<LiquidGlass>
    with TickerProviderStateMixin {
  AnimationController? _rippleCtrl;
  AnimationController? _scaleCtrl;
  Offset? _rippleOrigin;

  @override
  void initState() {
    super.initState();
    _syncInteractiveControllers(hasTap: widget.onTap != null);
  }

  @override
  void didUpdateWidget(covariant LiquidGlass oldWidget) {
    super.didUpdateWidget(oldWidget);
    if ((oldWidget.onTap != null) == (widget.onTap != null)) {
      return;
    }
    _syncInteractiveControllers(hasTap: widget.onTap != null);
  }

  @override
  void dispose() {
    _disposeInteractiveControllers();
    super.dispose();
  }

  void _syncInteractiveControllers({required bool hasTap}) {
    if (hasTap) {
      _rippleCtrl ??= AnimationController(
        vsync: this,
        duration: const Duration(milliseconds: 320),
      );
      _scaleCtrl ??= AnimationController(
        vsync: this,
        lowerBound: 0,
        upperBound: 1,
        duration: const Duration(milliseconds: 90),
      );
      return;
    }
    _disposeInteractiveControllers();
  }

  void _disposeInteractiveControllers() {
    _rippleCtrl?.dispose();
    _rippleCtrl = null;
    _scaleCtrl?.dispose();
    _scaleCtrl = null;
    _rippleOrigin = null;
  }

  @override
  Widget build(BuildContext context) {
    final bool isDark = Theme.of(context).brightness == Brightness.dark;
    final bool hasTap = widget.onTap != null;
    final _GlassProfile profile = _GlassProfile.resolve(
      context: context,
      quality: widget.quality,
      hasTap: hasTap,
      allowRipple: widget.enableRipple,
    );
    final AnimationController? rippleCtrl = _rippleCtrl;
    final AnimationController? scaleCtrl = _scaleCtrl;

    final Widget glassBody = RepaintBoundary(
      child: ClipRRect(
        borderRadius: widget.borderRadius,
        child: Stack(
          fit: StackFit.passthrough,
          children: <Widget>[
            BackdropFilter(
              filter: ui.ImageFilter.blur(
                sigmaX: profile.blurSigma,
                sigmaY: profile.blurSigma,
              ),
              child: DecoratedBox(
                decoration: BoxDecoration(
                  borderRadius: widget.borderRadius,
                  gradient: _surfaceGradient(
                    colorOverride: widget.color,
                    isDark: isDark,
                    profile: profile,
                    solidFill: widget.solidFill,
                  ),
                  boxShadow: <BoxShadow>[
                    BoxShadow(
                      color: Colors.black.withValues(
                        alpha: isDark
                            ? profile.shadowAlpha * 1.4
                            : profile.shadowAlpha,
                      ),
                      blurRadius: profile.shadowBlur,
                      offset: const Offset(0, 8),
                    ),
                  ],
                  border: Border.all(
                    color: (isDark ? Colors.white : Colors.black).withValues(
                      alpha: isDark ? 0.08 : 0.05,
                    ),
                    width: 0.8,
                  ),
                ),
                child: Padding(
                  padding: widget.padding ?? EdgeInsets.zero,
                  child: RepaintBoundary(child: widget.child),
                ),
              ),
            ),
            Positioned.fill(
              child: IgnorePointer(
                child: CustomPaint(
                  painter: _SpecularHighlightPainter(
                    isDark: isDark,
                    borderRadius: widget.borderRadius,
                    lowPerformance: profile.lowPerformance,
                  ),
                ),
              ),
            ),
            Positioned.fill(
              child: IgnorePointer(
                child: rippleCtrl == null
                    ? CustomPaint(
                        painter: _GlassBorderPainter(
                          isDark: isDark,
                          borderRadius: widget.borderRadius,
                          lowPerformance: profile.lowPerformance,
                          rippleValue: 0,
                          rippleOrigin: null,
                          showRipple: false,
                        ),
                      )
                    : AnimatedBuilder(
                        animation: rippleCtrl,
                        builder: (_, __) {
                          return CustomPaint(
                            painter: _GlassBorderPainter(
                              isDark: isDark,
                              borderRadius: widget.borderRadius,
                              lowPerformance: profile.lowPerformance,
                              rippleValue: rippleCtrl.value,
                              rippleOrigin: _rippleOrigin,
                              showRipple: profile.showRipple,
                            ),
                          );
                        },
                      ),
              ),
            ),
          ],
        ),
      ),
    );

    final Widget scaled = scaleCtrl == null
        ? glassBody
        : AnimatedBuilder(
            animation: scaleCtrl,
            child: glassBody,
            builder: (_, Widget? child) {
              final double scale = 1.0 - (scaleCtrl.value * profile.pressDepth);
              return Transform.scale(scale: scale, child: child);
            },
          );

    return GestureDetector(
      behavior: HitTestBehavior.translucent,
      onTapDown: !hasTap || scaleCtrl == null
          ? null
          : (TapDownDetails details) {
              final RenderBox? box = context.findRenderObject() as RenderBox?;
              if (box != null) {
                _rippleOrigin = box.globalToLocal(details.globalPosition);
              }
              if (widget.enableHaptics) {
                HapticFeedback.lightImpact();
              }
              scaleCtrl.forward();
            },
      onTapCancel: !hasTap || scaleCtrl == null
          ? null
          : () => scaleCtrl.reverse(),
      onTapUp: !hasTap || scaleCtrl == null
          ? null
          : (_) {
              if (widget.enableHaptics) {
                HapticFeedback.selectionClick();
              }
              scaleCtrl.reverse();
              if (profile.showRipple && rippleCtrl != null) {
                rippleCtrl.forward(from: 0);
              }
              widget.onTap!.call();
            },
      child: scaled,
    );
  }
}

LinearGradient _surfaceGradient({
  required Color? colorOverride,
  required bool isDark,
  required _GlassProfile profile,
  required bool solidFill,
}) {
  final Color seed =
      colorOverride ??
      (isDark ? const Color(0xFF1C1C1E) : const Color(0xFFF8FAFC));
  if (solidFill) {
    return LinearGradient(
      begin: Alignment.topLeft,
      end: Alignment.bottomRight,
      colors: <Color>[
        seed.withValues(alpha: profile.fillAlphaBottom),
        seed.withValues(alpha: profile.fillAlphaBottom),
      ],
    );
  }
  if (isDark) {
    return LinearGradient(
      begin: Alignment.topLeft,
      end: Alignment.bottomRight,
      colors: <Color>[
        seed.withValues(alpha: profile.fillAlphaTop),
        const Color(0xFF14161B).withValues(alpha: profile.fillAlphaBottom),
      ],
    );
  }
  return LinearGradient(
    begin: Alignment.topLeft,
    end: Alignment.bottomRight,
    colors: <Color>[
      const Color(0xFFFFFFFF).withValues(alpha: profile.fillAlphaTop),
      seed.withValues(alpha: profile.fillAlphaBottom),
    ],
  );
}

class _GlassProfile {
  const _GlassProfile({
    required this.lowPerformance,
    required this.blurSigma,
    required this.shadowBlur,
    required this.shadowAlpha,
    required this.fillAlphaTop,
    required this.fillAlphaBottom,
    required this.showRipple,
    required this.pressDepth,
  });

  final bool lowPerformance;
  final double blurSigma;
  final double shadowBlur;
  final double shadowAlpha;
  final double fillAlphaTop;
  final double fillAlphaBottom;
  final bool showRipple;
  final double pressDepth;

  static _GlassProfile resolve({
    required BuildContext context,
    required LiquidGlassQuality quality,
    required bool hasTap,
    required bool allowRipple,
  }) {
    final MediaQueryData? media = MediaQuery.maybeOf(context);
    final bool disableAnimations = media?.disableAnimations ?? false;
    final double dpr = media?.devicePixelRatio ?? 3.0;
    final double shortestSide = media?.size.shortestSide ?? 460;
    final bool likelyLowEndAndroid =
        defaultTargetPlatform == TargetPlatform.android &&
        (shortestSide < 420 || dpr <= 2.0 || disableAnimations);
    final PerformanceMode mode = performanceModeNotifier.value;

    final bool lowPerformance = switch (quality) {
      LiquidGlassQuality.low => true,
      LiquidGlassQuality.high => false,
      LiquidGlassQuality.adaptive =>
        mode == PerformanceMode.reduced
            ? true
            : mode == PerformanceMode.full
            ? false
            : (likelyLowEndAndroid || disableAnimations),
    };

    if (lowPerformance) {
      return _GlassProfile(
        lowPerformance: true,
        blurSigma: 8.0,
        shadowBlur: 10,
        shadowAlpha: 0.08,
        fillAlphaTop: 0.88,
        fillAlphaBottom: 0.84,
        showRipple: hasTap && allowRipple && !disableAnimations,
        pressDepth: hasTap ? 0.018 : 0.0,
      );
    }

    return _GlassProfile(
      lowPerformance: false,
      blurSigma: 16.0,
      shadowBlur: 22,
      shadowAlpha: 0.12,
      fillAlphaTop: 0.74,
      fillAlphaBottom: 0.58,
      showRipple: hasTap && allowRipple && !disableAnimations,
      pressDepth: hasTap ? 0.028 : 0.0,
    );
  }
}

class _GlassBorderPainter extends CustomPainter {
  const _GlassBorderPainter({
    required this.isDark,
    required this.borderRadius,
    required this.lowPerformance,
    required this.rippleValue,
    required this.rippleOrigin,
    required this.showRipple,
  });

  final bool isDark;
  final BorderRadius borderRadius;
  final bool lowPerformance;
  final double rippleValue;
  final Offset? rippleOrigin;
  final bool showRipple;

  @override
  void paint(Canvas canvas, Size size) {
    final RRect rect = borderRadius.toRRect(Offset.zero & size);

    final Paint outer = Paint()
      ..style = PaintingStyle.stroke
      ..strokeWidth = lowPerformance ? 1.4 : 2.1
      ..shader = LinearGradient(
        begin: Alignment.topLeft,
        end: Alignment.bottomRight,
        colors: <Color>[
          Colors.white.withValues(alpha: isDark ? 0.34 : 0.82),
          Colors.white.withValues(alpha: 0.02),
          Colors.white.withValues(alpha: isDark ? 0.2 : 0.56),
        ],
      ).createShader(Offset.zero & size);
    canvas.drawRRect(rect, outer);

    final Paint inner = Paint()
      ..style = PaintingStyle.stroke
      ..strokeWidth = 1
      ..color = Colors.white.withValues(alpha: isDark ? 0.07 : 0.28);
    canvas.drawRRect(rect.deflate(1.2), inner);

    if (!showRipple || rippleValue <= 0 || rippleOrigin == null) {
      return;
    }

    final double maxRadius = size.longestSide * (lowPerformance ? 1.05 : 1.35);
    final double radius =
        maxRadius * Curves.easeOutCubic.transform(rippleValue);
    final double alpha = (1.0 - rippleValue).clamp(0.0, 1.0);
    final Offset center = rippleOrigin!;

    final Paint ripple = Paint()
      ..style = PaintingStyle.stroke
      ..strokeWidth = lowPerformance ? 1.2 : 2.0
      ..color = (isDark ? Colors.lightBlueAccent : Colors.white).withValues(
        alpha: alpha * 0.5,
      )
      ..blendMode = BlendMode.plus;

    if (!lowPerformance) {
      ripple.maskFilter = const MaskFilter.blur(BlurStyle.normal, 7);
    }

    canvas.save();
    canvas.clipRRect(rect);
    canvas.drawCircle(center, radius, ripple);
    if (!lowPerformance && rippleValue > 0.2) {
      canvas.drawCircle(
        center,
        radius * 0.72,
        ripple
          ..strokeWidth = 2.6
          ..color = ripple.color.withValues(alpha: alpha * 0.34),
      );
    }
    canvas.restore();
  }

  @override
  bool shouldRepaint(covariant _GlassBorderPainter oldDelegate) {
    return oldDelegate.rippleValue != rippleValue ||
        oldDelegate.rippleOrigin != rippleOrigin ||
        oldDelegate.borderRadius != borderRadius ||
        oldDelegate.lowPerformance != lowPerformance ||
        oldDelegate.showRipple != showRipple;
  }
}

class _SpecularHighlightPainter extends CustomPainter {
  const _SpecularHighlightPainter({
    required this.isDark,
    required this.borderRadius,
    required this.lowPerformance,
  });

  final bool isDark;
  final BorderRadius borderRadius;
  final bool lowPerformance;

  @override
  void paint(Canvas canvas, Size size) {
    final Rect rect = Offset.zero & size;
    final RRect rr = borderRadius.toRRect(rect);

    final Paint topGloss = Paint()
      ..shader = LinearGradient(
        begin: Alignment.topCenter,
        end: Alignment.bottomCenter,
        colors: <Color>[
          Colors.white.withValues(alpha: isDark ? 0.10 : 0.28),
          Colors.white.withValues(alpha: 0.0),
        ],
        stops: const <double>[0.0, 0.35],
      ).createShader(rect)
      ..blendMode = BlendMode.screen;
    canvas.drawRRect(rr, topGloss);

    if (!lowPerformance) {
      final Paint sideSheen = Paint()
        ..shader = LinearGradient(
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
          colors: <Color>[
            Colors.white.withValues(alpha: isDark ? 0.08 : 0.16),
            Colors.transparent,
          ],
        ).createShader(rect)
        ..blendMode = BlendMode.plus;
      canvas.drawRRect(rr, sideSheen);
    }
  }

  @override
  bool shouldRepaint(covariant _SpecularHighlightPainter oldDelegate) {
    return oldDelegate.isDark != isDark ||
        oldDelegate.borderRadius != borderRadius ||
        oldDelegate.lowPerformance != lowPerformance;
  }
}
