import 'package:flutter/material.dart';

import '../classroom/classroom_state.dart';

class WhiteboardCanvas extends StatefulWidget {
  const WhiteboardCanvas({
    super.key,
    required this.strokes,
    required this.canDraw,
    required this.eraserEnabled,
    required this.onClear,
    required this.onStroke,
    required this.onEraserChanged,
  });

  final List<WhiteboardStroke> strokes;
  final bool canDraw;
  final bool eraserEnabled;
  final VoidCallback onClear;
  final ValueChanged<WhiteboardStroke> onStroke;
  final ValueChanged<bool> onEraserChanged;

  @override
  State<WhiteboardCanvas> createState() => _WhiteboardCanvasState();
}

class _WhiteboardCanvasState extends State<WhiteboardCanvas> {
  List<Offset> _activePoints = [];

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        Padding(
          padding: const EdgeInsets.all(8),
          child: Row(
            children: [
              const Expanded(
                child: Text(
                  'Shared Whiteboard',
                  overflow: TextOverflow.ellipsis,
                  style: TextStyle(fontWeight: FontWeight.w700),
                ),
              ),
              Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  IconButton(
                    tooltip: 'Eraser',
                    onPressed: widget.canDraw
                        ? () => widget.onEraserChanged(!widget.eraserEnabled)
                        : null,
                    icon: Icon(
                      Icons.cleaning_services,
                      color: widget.eraserEnabled
                          ? const Color(0xFF0C72B8)
                          : const Color(0xFF4A607C),
                    ),
                  ),
                  IconButton(
                    tooltip: 'Clear',
                    onPressed: widget.canDraw ? widget.onClear : null,
                    icon: const Icon(Icons.delete_outline),
                  ),
                ],
              ),
            ],
          ),
        ),
        Expanded(
          child: LayoutBuilder(
            builder: (context, constraints) {
              final size = constraints.biggest;
              return DecoratedBox(
                decoration: BoxDecoration(
                  color: Colors.white,
                  borderRadius: BorderRadius.circular(12),
                  border: Border.all(color: const Color(0xFFDCE7F7)),
                ),
                child: GestureDetector(
                  onPanStart: widget.canDraw
                      ? (details) {
                          setState(() {
                            _activePoints = [
                              _normalizePoint(details.localPosition, size),
                            ];
                          });
                        }
                      : null,
                  onPanUpdate: widget.canDraw
                      ? (details) {
                          setState(() {
                            _activePoints = [
                              ..._activePoints,
                              _normalizePoint(details.localPosition, size),
                            ];
                          });
                        }
                      : null,
                  onPanEnd: widget.canDraw
                      ? (_) {
                          if (_activePoints.isNotEmpty) {
                            widget.onStroke(
                              WhiteboardStroke(
                                points: List<Offset>.unmodifiable(
                                  _activePoints,
                                ),
                                color: widget.eraserEnabled
                                    ? Colors.white
                                    : const Color(0xFF0F4973),
                                width: widget.eraserEnabled ? 12 : 3,
                              ),
                            );
                          }
                          setState(() {
                            _activePoints = [];
                          });
                        }
                      : null,
                  child: RepaintBoundary(
                    child: CustomPaint(
                      isComplex: widget.strokes.isNotEmpty,
                      willChange: _activePoints.isNotEmpty,
                      painter: _WhiteboardPainter(
                        strokes: widget.strokes,
                        activePoints: _activePoints,
                        activeColor: widget.eraserEnabled
                            ? Colors.white
                            : const Color(0xFF0F4973),
                      ),
                      child: const SizedBox.expand(),
                    ),
                  ),
                ),
              );
            },
          ),
        ),
      ],
    );
  }

  Offset _normalizePoint(Offset point, Size size) {
    final safeWidth = size.width <= 0 ? 1.0 : size.width;
    final safeHeight = size.height <= 0 ? 1.0 : size.height;
    return Offset(
      (point.dx / safeWidth).clamp(0.0, 1.0),
      (point.dy / safeHeight).clamp(0.0, 1.0),
    );
  }
}

class _WhiteboardPainter extends CustomPainter {
  const _WhiteboardPainter({
    required this.strokes,
    required this.activePoints,
    required this.activeColor,
  });

  final List<WhiteboardStroke> strokes;
  final List<Offset> activePoints;
  final Color activeColor;

  @override
  void paint(Canvas canvas, Size size) {
    for (final stroke in strokes) {
      _paintStroke(canvas, size, stroke.points, stroke.color, stroke.width);
    }
    if (activePoints.isNotEmpty) {
      _paintStroke(canvas, size, activePoints, activeColor, 3);
    }
  }

  @override
  bool shouldRepaint(covariant _WhiteboardPainter oldDelegate) {
    return oldDelegate.strokes != strokes ||
        oldDelegate.activePoints != activePoints ||
        oldDelegate.activeColor != activeColor;
  }

  void _paintStroke(
    Canvas canvas,
    Size size,
    List<Offset> points,
    Color color,
    double width,
  ) {
    if (points.length < 2) {
      return;
    }

    final paint = Paint()
      ..color = color
      ..strokeWidth = width
      ..strokeCap = StrokeCap.round
      ..style = PaintingStyle.stroke;

    for (var i = 0; i < points.length - 1; i += 1) {
      canvas.drawLine(
        Offset(points[i].dx * size.width, points[i].dy * size.height),
        Offset(points[i + 1].dx * size.width, points[i + 1].dy * size.height),
        paint,
      );
    }
  }
}
