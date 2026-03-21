import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:jee_live_classes/widgets/glass_panel.dart';

void main() {
  testWidgets('GlassPanel uses blur effects when motion reduction is off', (
    tester,
  ) async {
    await tester.pumpWidget(
      const MaterialApp(
        home: Scaffold(
          body: GlassPanel(child: SizedBox(width: 120, height: 60)),
        ),
      ),
    );

    expect(find.byType(BackdropFilter), findsOneWidget);
  });

  testWidgets('GlassPanel disables blur when animations are disabled', (
    tester,
  ) async {
    await tester.pumpWidget(
      MaterialApp(
        home: MediaQuery(
          data: const MediaQueryData(disableAnimations: true),
          child: const Scaffold(
            body: GlassPanel(child: SizedBox(width: 120, height: 60)),
          ),
        ),
      ),
    );

    expect(find.byType(BackdropFilter), findsNothing);
  });
}
