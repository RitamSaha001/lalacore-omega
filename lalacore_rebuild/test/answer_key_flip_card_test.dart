import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:lalacore_rebuild/widgets/answer_key_flip_card.dart';
import 'package:lalacore_rebuild/widgets/smart_text.dart';

import 'test_pump_utils.dart';

Finder _smartTextContains(String snippet) {
  return find.byWidgetPredicate(
    (Widget widget) => widget is SmartText && widget.text.contains(snippet),
  );
}

AnswerKeyCardPayload _payload() {
  return const AnswerKeyCardPayload(
    heroTag: 'ak_test_1',
    questionIndex: 0,
    questionText: 'What is 1 + 1?',
    options: <String>['A) 1', 'B) 2', 'C) 3', 'D) 4'],
    correctAnswer: 'B) 2',
    studentAnswer: 'A) 1',
    solution: 'Add one and one.',
    concept: 'Arithmetic',
    marksDelta: -1,
    statusLabel: 'WRONG',
    statusColor: Colors.redAccent,
  );
}

void main() {
  testWidgets('student mode shows only correct answer summary', (
    WidgetTester tester,
  ) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: AnswerKeyFlipCard(
            payload: _payload(),
            teacherMode: false,
            onAskAi: () {},
            onRaiseDoubt: () {},
          ),
        ),
      ),
    );

    expect(_smartTextContains('Correct: B) 2'), findsOneWidget);
    expect(_smartTextContains('You: A) 1'), findsNothing);
    expect(_smartTextContains('• A) 1'), findsNothing);
    expect(
      find.text('Student mode: only correct answers are visible.'),
      findsOneWidget,
    );
  });

  testWidgets('teacher mode shows options and full review details', (
    WidgetTester tester,
  ) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: AnswerKeyFlipCard(
            payload: _payload(),
            teacherMode: true,
            onAskAi: () {},
            onRaiseDoubt: () {},
          ),
        ),
      ),
    );

    expect(_smartTextContains('• A) 1'), findsOneWidget);
    expect(_smartTextContains('You: A) 1'), findsOneWidget);

    await tester.tap(find.byType(AnswerKeyFlipCard));
    await tester.pumpDeterministic(const Duration(milliseconds: 500));
    expect(find.text('Solution'), findsOneWidget);
    expect(find.text('Ask LalaCore'), findsOneWidget);
  });
}
