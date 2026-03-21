import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:jee_live_classes/modules/classroom/classroom_state.dart';
import 'package:jee_live_classes/modules/whiteboard/whiteboard_canvas.dart';

void main() {
  testWidgets('Whiteboard canvas emits normalized collaborative strokes', (
    tester,
  ) async {
    WhiteboardStroke? emitted;

    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: SizedBox(
            width: 240,
            height: 240,
            child: WhiteboardCanvas(
              strokes: const [],
              canDraw: true,
              eraserEnabled: false,
              onClear: () {},
              onStroke: (stroke) => emitted = stroke,
              onEraserChanged: (_) {},
            ),
          ),
        ),
      ),
    );

    final canvas = find.byType(GestureDetector).last;
    final topLeft = tester.getTopLeft(canvas);
    final gesture = await tester.startGesture(topLeft + const Offset(20, 20));
    await gesture.moveTo(topLeft + const Offset(180, 120));
    await gesture.up();
    await tester.pump();

    expect(emitted, isNotNull);
    expect(emitted!.points.length, greaterThanOrEqualTo(2));
    for (final point in emitted!.points) {
      expect(point.dx, inInclusiveRange(0.0, 1.0));
      expect(point.dy, inInclusiveRange(0.0, 1.0));
    }
  });
}
