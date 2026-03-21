import 'dart:math' as math;
import 'dart:ui';

import 'package:flutter/material.dart';

class GlassPanel extends StatelessWidget {
  const GlassPanel({
    super.key,
    required this.child,
    this.padding = const EdgeInsets.all(12),
    this.borderRadius = const BorderRadius.all(Radius.circular(22)),
    this.blurSigma = 10,
    this.enableBlur = true,
    this.tintColor = const Color(0xCCFFFFFF),
    this.borderColor = const Color(0x33FFFFFF),
    this.shadowColor = const Color(0x14081830),
  });

  final Widget child;
  final EdgeInsetsGeometry padding;
  final BorderRadius borderRadius;
  final double blurSigma;
  final bool enableBlur;
  final Color tintColor;
  final Color borderColor;
  final Color shadowColor;

  @override
  Widget build(BuildContext context) {
    final media = MediaQuery.maybeOf(context);
    final disableEffects =
        (media?.disableAnimations ?? false) ||
        (media?.accessibleNavigation ?? false) ||
        (media?.highContrast ?? false);
    final compactScreen = (media?.size.shortestSide ?? 0) < 600;
    final shouldBlur = enableBlur && !disableEffects;
    final effectiveBlurSigma = shouldBlur
        ? math.min<double>(blurSigma, compactScreen ? 8 : 12)
        : 0.0;
    final effectiveShadowBlur = shouldBlur ? 24.0 : 12.0;
    final effectiveShadowOffset = shouldBlur
        ? const Offset(0, 10)
        : const Offset(0, 4);

    final decoratedChild = DecoratedBox(
      decoration: BoxDecoration(
        gradient: LinearGradient(
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
          colors: [
            tintColor.withValues(alpha: 0.92),
            tintColor.withValues(alpha: 0.72),
          ],
        ),
        borderRadius: borderRadius,
        border: Border.all(color: borderColor),
        boxShadow: [
          BoxShadow(
            blurRadius: effectiveShadowBlur,
            offset: effectiveShadowOffset,
            color: shadowColor,
          ),
        ],
      ),
      child: Padding(padding: padding, child: child),
    );

    return RepaintBoundary(
      child: ClipRRect(
        borderRadius: borderRadius,
        child: shouldBlur
            ? BackdropFilter(
                filter: ImageFilter.blur(
                  sigmaX: effectiveBlurSigma,
                  sigmaY: effectiveBlurSigma,
                ),
                child: decoratedChild,
              )
            : decoratedChild,
      ),
    );
  }
}
