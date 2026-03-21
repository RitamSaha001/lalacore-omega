import 'package:flutter_test/flutter_test.dart';

import 'package:jee_live_classes/core/navigation.dart';

void main() {
  testWidgets('join readiness screen renders', (tester) async {
    await tester.pumpWidget(const LiveClassesApp());
    await tester.pump(const Duration(milliseconds: 200));

    expect(find.byType(LiveClassesApp), findsOneWidget);
  });
}
