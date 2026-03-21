import 'dart:math';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../state/app_state.dart';

class StaggeredList extends StatelessWidget {
  const StaggeredList({super.key, required this.index, required this.child});

  final int index;
  final Widget child;

  @override
  Widget build(BuildContext context) {
    return TweenAnimationBuilder<double>(
      tween: Tween<double>(begin: 0, end: 1),
      duration: Duration(milliseconds: 300 + (index * 50)),
      curve: Curves.easeOutQuad,
      builder: (BuildContext context, double value, Widget? _) {
        return Opacity(
          opacity: value,
          child: Transform.translate(
            offset: Offset(0, 30 * (1 - value)),
            child: child,
          ),
        );
      },
    );
  }
}

class BouncyNavItem extends StatefulWidget {
  const BouncyNavItem({
    super.key,
    required this.icon,
    required this.label,
    required this.isSelected,
    required this.onTap,
    this.showSelectionBackground = true,
    this.reducedMotion = false,
    this.selectedColor,
    this.unselectedColor,
  });

  final IconData icon;
  final String label;
  final bool isSelected;
  final VoidCallback onTap;
  final bool showSelectionBackground;
  final bool reducedMotion;
  final Color? selectedColor;
  final Color? unselectedColor;

  @override
  State<BouncyNavItem> createState() => _BouncyNavItemState();
}

class _BouncyNavItemState extends State<BouncyNavItem>
    with SingleTickerProviderStateMixin {
  late final AnimationController _ctrl;

  @override
  void initState() {
    super.initState();
    _ctrl = AnimationController(
      vsync: this,
      duration: Duration(milliseconds: widget.reducedMotion ? 180 : 320),
    );
  }

  @override
  void didUpdateWidget(covariant BouncyNavItem oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.reducedMotion != widget.reducedMotion) {
      _ctrl.duration = Duration(milliseconds: widget.reducedMotion ? 180 : 320);
    }
    if (widget.isSelected && !oldWidget.isSelected) {
      _ctrl.forward(from: 0.0);
      if (!widget.reducedMotion) {
        HapticFeedback.selectionClick();
      }
    } else if (!widget.isSelected && oldWidget.isSelected) {
      _ctrl.reverse();
    }
  }

  @override
  void dispose() {
    _ctrl.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final bool isDark = Theme.of(context).brightness == Brightness.dark;
    final Color selectedColor =
        widget.selectedColor ?? AppColors.primaryTone(context);
    final Color unselectedColor =
        widget.unselectedColor ??
        (isDark ? const Color(0xFF9FB0C7) : const Color(0xFF6B7280));
    final double selectedScaleBoost = widget.reducedMotion ? 0.028 : 0.075;
    final double selectedLift = widget.reducedMotion ? -1.0 : -2.6;
    return GestureDetector(
      onTap: widget.onTap,
      behavior: HitTestBehavior.translucent,
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: <Widget>[
          AnimatedBuilder(
            animation: _ctrl,
            builder: (BuildContext context, _) {
              final double pulse = Curves.easeOutBack.transform(_ctrl.value);
              final double scale = widget.isSelected
                  ? 1.0 + (selectedScaleBoost * pulse)
                  : 1.0;
              final double lift = widget.isSelected
                  ? selectedLift * Curves.easeOut.transform(_ctrl.value)
                  : 0.0;
              return Transform.scale(
                scale: scale,
                child: Transform.translate(
                  offset: Offset(0, lift),
                  child: Container(
                    padding: const EdgeInsets.all(10),
                    decoration: BoxDecoration(
                      color:
                          (widget.showSelectionBackground && widget.isSelected)
                          ? selectedColor.withOpacity(
                              widget.reducedMotion ? 0.10 : 0.15,
                            )
                          : Colors.transparent,
                      shape: BoxShape.circle,
                    ),
                    child: Icon(
                      widget.icon,
                      color: widget.isSelected
                          ? selectedColor
                          : unselectedColor,
                    ),
                  ),
                ),
              );
            },
          ),
          const SizedBox(height: 4),
          Text(
            widget.label,
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            softWrap: false,
            textAlign: TextAlign.center,
            style: TextStyle(
              fontSize: widget.label.length > 12 ? 9 : 10,
              fontWeight: widget.isSelected ? FontWeight.w900 : FontWeight.w500,
              color: widget.isSelected ? selectedColor : unselectedColor,
            ),
          ),
        ],
      ),
    );
  }
}

class AnimatedStudyFAB extends StatefulWidget {
  const AnimatedStudyFAB({super.key, required this.onTap});

  final VoidCallback onTap;

  @override
  State<AnimatedStudyFAB> createState() => _AnimatedStudyFABState();
}

class _AnimatedStudyFABState extends State<AnimatedStudyFAB>
    with SingleTickerProviderStateMixin {
  late final AnimationController _ctrl;
  late final Animation<double> _scale;
  late final Animation<double> _rotate;

  @override
  void initState() {
    super.initState();
    _ctrl = AnimationController(
      vsync: this,
      duration: const Duration(milliseconds: 600),
    );
    _scale = Tween<double>(
      begin: 1.0,
      end: 1.15,
    ).animate(CurvedAnimation(parent: _ctrl, curve: Curves.easeOutBack));
    _rotate = Tween<double>(
      begin: 0.0,
      end: 0.25,
    ).animate(CurvedAnimation(parent: _ctrl, curve: Curves.easeOut));
  }

  @override
  void dispose() {
    _ctrl.dispose();
    super.dispose();
  }

  Future<void> _handleTap() async {
    await _ctrl.forward();
    await _ctrl.reverse();
    widget.onTap();
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: _ctrl,
      builder: (BuildContext context, Widget? child) {
        return Transform.scale(
          scale: _scale.value,
          child: Transform.rotate(angle: _rotate.value, child: child),
        );
      },
      child: GestureDetector(
        onTap: _handleTap,
        child: ClipRRect(
          borderRadius: BorderRadius.circular(30),
          child: Container(
            padding: const EdgeInsets.symmetric(horizontal: 18, vertical: 14),
            decoration: BoxDecoration(
              color: Colors.yellow.withOpacity(0.25),
              borderRadius: BorderRadius.circular(30),
              border: Border.all(color: Colors.yellow.withOpacity(0.5)),
              boxShadow: <BoxShadow>[
                BoxShadow(
                  color: Colors.yellow.withOpacity(0.35),
                  blurRadius: 25,
                  spreadRadius: 2,
                ),
              ],
            ),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: const <Widget>[
                Icon(Icons.menu_book, color: Colors.black, size: 26),
                SizedBox(width: 8),
                Text(
                  'Study',
                  style: TextStyle(
                    fontWeight: FontWeight.bold,
                    color: Colors.black,
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

void showSuccessBurst(BuildContext context, {Offset? origin}) {
  final OverlayState? overlay = Overlay.of(context);
  if (overlay == null) {
    return;
  }

  late OverlayEntry entry;
  entry = OverlayEntry(
    builder: (BuildContext context) =>
        _ConfettiBurst(origin: origin, onFinish: entry.remove),
  );
  overlay.insert(entry);
}

class _ConfettiBurst extends StatefulWidget {
  const _ConfettiBurst({required this.onFinish, this.origin});

  final VoidCallback onFinish;
  final Offset? origin;

  @override
  State<_ConfettiBurst> createState() => _ConfettiBurstState();
}

class _ConfettiBurstState extends State<_ConfettiBurst>
    with SingleTickerProviderStateMixin {
  late final AnimationController _controller;
  final Random _rnd = Random();
  late final List<_ConfettiPiece> _pieces;

  @override
  void initState() {
    super.initState();
    _controller = AnimationController(
      vsync: this,
      duration: const Duration(milliseconds: 1800),
    )..forward();
    _pieces = List<_ConfettiPiece>.generate(
      90,
      (_) => _ConfettiPiece.random(_rnd),
    );
    _controller.addStatusListener((AnimationStatus status) {
      if (status == AnimationStatus.completed) {
        widget.onFinish();
      }
    });
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final Size size = MediaQuery.of(context).size;
    return IgnorePointer(
      child: AnimatedBuilder(
        animation: _controller,
        builder: (BuildContext context, _) {
          return CustomPaint(
            size: size,
            painter: _ConfettiPainter(
              progress: _controller.value,
              pieces: _pieces,
              origin: widget.origin,
            ),
          );
        },
      ),
    );
  }
}

class _ConfettiPainter extends CustomPainter {
  const _ConfettiPainter({
    required this.progress,
    required this.pieces,
    required this.origin,
  });

  final double progress;
  final List<_ConfettiPiece> pieces;
  final Offset? origin;

  @override
  void paint(Canvas canvas, Size size) {
    final Offset center = origin ?? Offset(size.width / 2, size.height / 3);
    for (final _ConfettiPiece piece in pieces) {
      final double t = Curves.easeOut.transform(progress);
      final double dx = center.dx + piece.vx * t * size.width;
      final double dy = center.dy + piece.vy * t * size.height + t * t * 500;
      final Paint paint = Paint()..color = piece.color;
      canvas.save();
      canvas.translate(dx, dy);
      canvas.rotate(piece.rotation * t * 6);
      canvas.drawRRect(
        RRect.fromRectAndRadius(
          Rect.fromCenter(center: Offset.zero, width: 10, height: 6),
          const Radius.circular(2),
        ),
        paint,
      );
      canvas.restore();
    }
  }

  @override
  bool shouldRepaint(covariant _ConfettiPainter oldDelegate) {
    return oldDelegate.progress != progress;
  }
}

class _ConfettiPiece {
  const _ConfettiPiece({
    required this.vx,
    required this.vy,
    required this.rotation,
    required this.color,
  });

  final double vx;
  final double vy;
  final double rotation;
  final Color color;

  factory _ConfettiPiece.random(Random rnd) {
    return _ConfettiPiece(
      vx: (rnd.nextDouble() * 2 - 1) * 0.9,
      vy: -(rnd.nextDouble() * 1.3 + 0.3),
      rotation: rnd.nextDouble() * pi,
      color: Colors.primaries[rnd.nextInt(Colors.primaries.length)],
    );
  }
}
