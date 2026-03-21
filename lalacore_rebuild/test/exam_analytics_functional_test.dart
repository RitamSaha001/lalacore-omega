import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

import 'package:lalacore_rebuild/main.dart';
import 'package:lalacore_rebuild/models/quiz_models.dart';
import 'package:lalacore_rebuild/models/session.dart';
import 'package:lalacore_rebuild/widgets/smart_text.dart';

import 'test_pump_utils.dart';

void main() {
  setUp(() {
    Session.isTeacher = false;
    Session.studentId = '';
    Session.studentName = '';
    Session.accountId = '';
    Session.email = '';
    Session.username = '';
    Session.chatId = '';
    Session.chatName = '';
    SharedPreferences.setMockInitialValues(<String, Object>{});
  });

  testWidgets(
    'Exam palette is functionally wired to answer state and quick jump',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1080, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final QuizItem item = QuizItem(
        id: 'quiz_exam_functional',
        title: 'Mock Exam|Physics',
        url: '',
        deadline: DateTime.now().add(const Duration(days: 1)),
        type: 'Exam',
        durationMinutes: 30,
        isAiGenerated: false,
      );

      final List<Question> questions = <Question>[
        const Question(
          text: 'Question 1',
          imageUrl: '',
          type: 'MCQ',
          section: 'Mechanics',
          posMark: 4,
          negMark: 1,
          options: <String>['A1', 'B1', 'C1', 'D1'],
          correctAnswers: <String>['A1'],
        ),
        const Question(
          text: 'Question 2',
          imageUrl: '',
          type: 'MCQ',
          section: 'Mechanics',
          posMark: 4,
          negMark: 1,
          options: <String>['A2', 'B2', 'C2', 'D2'],
          correctAnswers: <String>['A2'],
        ),
        const Question(
          text: 'Question 3',
          imageUrl: '',
          type: 'MCQ',
          section: 'Mechanics',
          posMark: 4,
          negMark: 1,
          options: <String>['A3', 'B3', 'C3', 'D3'],
          correctAnswers: <String>['A3'],
        ),
      ];

      await tester.pumpWidget(
        MaterialApp(
          home: ExamScreen(
            questions: questions,
            item: item,
            studentName: 'Tester',
            isResume: false,
          ),
        ),
      );

      await tester.pump(const Duration(milliseconds: 250));
      expect(find.textContaining('Question 1/3'), findsOneWidget);

      await tester.tap(find.text('A1'));
      await tester.pump(const Duration(milliseconds: 120));

      await tester.tap(find.byTooltip('Mark for review').first);
      await tester.pump(const Duration(milliseconds: 120));
      expect(find.byTooltip('Unmark review'), findsWidgets);

      await tester.tap(find.byTooltip('Question palette'));
      await tester.pump(const Duration(milliseconds: 250));
      expect(find.text('Question Map'), findsOneWidget);
      expect(find.text('Answered 1'), findsOneWidget);

      await tester.tapAt(const Offset(24, 24));
      await tester.pump(const Duration(milliseconds: 220));

      await tester.tap(find.byTooltip('Next unanswered').first);
      await tester.pump(const Duration(milliseconds: 320));
      expect(find.textContaining('Question 2/3'), findsOneWidget);
    },
  );

  testWidgets(
    'Exam question and options render with SmartText for latex support',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1080, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final QuizItem item = QuizItem(
        id: 'quiz_exam_latex',
        title: 'Latex Exam|Math',
        url: '',
        deadline: DateTime.now().add(const Duration(days: 1)),
        type: 'Exam',
        durationMinutes: 20,
        isAiGenerated: true,
      );

      final List<Question> questions = <Question>[
        const Question(
          text: r'Solve $x^2 - 1 = 0$',
          imageUrl: '',
          type: 'MCQ',
          section: 'Algebra',
          posMark: 4,
          negMark: 1,
          options: <String>[
            r'$x = 1$',
            r'$x = -1$',
            r'$x = \pm 1$',
            r'$x = 0$',
          ],
          correctAnswers: <String>[r'$x = \pm 1$'],
        ),
      ];

      await tester.pumpWidget(
        MaterialApp(
          home: ExamScreen(
            questions: questions,
            item: item,
            studentName: 'Tester',
            isResume: false,
          ),
        ),
      );

      await tester.pump(const Duration(milliseconds: 300));

      final int smartTextCount = find.byType(SmartText).evaluate().length;
      expect(smartTextCount >= 5, true);

      await tester.tap(find.byType(RadioListTile<String>).at(2));
      await tester.pump(const Duration(milliseconds: 120));
      expect(find.text('Answered: 1'), findsOneWidget);
    },
  );

  testWidgets(
    'Analytics rank/percentile/leaderboard are functionally computed from saved attempts',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1080, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final QuizItem item = QuizItem(
        id: 'quiz_rank_functional',
        title: 'Rank Test|Math',
        url: '',
        deadline: DateTime.now().add(const Duration(days: 2)),
        type: 'Exam',
        durationMinutes: 60,
        isAiGenerated: false,
      );

      Map<String, dynamic> row({
        required String accountId,
        required String studentName,
        required double score,
        required int ts,
      }) {
        return <String, dynamic>{
          'quizId': item.id,
          'quizTitle': item.title,
          'score': score,
          'maxScore': 100.0,
          'correct': score.toInt(),
          'wrong': 0,
          'skipped': 0,
          'totalTime': 1200,
          'sectionAccuracy': <String, double>{'Math': score},
          'userAnswers': <String, List<String>>{},
          'studentName': studentName,
          'studentId': accountId,
          'accountId': accountId,
          'savedAt': ts,
        };
      }

      SharedPreferences.setMockInitialValues(<String, Object>{
        'res_${item.id}_1001': jsonEncode(
          row(accountId: 'alice', studentName: 'Alice', score: 90, ts: 1001),
        ),
        'res_${item.id}_1002': jsonEncode(
          row(accountId: 'me', studentName: 'Me', score: 75, ts: 1002),
        ),
        'res_${item.id}_1003': jsonEncode(
          row(accountId: 'bob', studentName: 'Bob', score: 60, ts: 1003),
        ),
      });

      Session.studentId = 'me';
      Session.studentName = 'Me';
      Session.accountId = 'me';

      final ResultData current = ResultData(
        quizId: item.id,
        quizTitle: item.title,
        score: 75,
        maxScore: 100,
        correct: 15,
        wrong: 5,
        skipped: 0,
        totalTime: 1200,
        sectionAccuracy: <String, double>{'Math': 75},
        userAnswers: <int, List<String>>{},
      );

      await tester.pumpWidget(
        MaterialApp(
          home: AnalyticsScreen(
            data: current,
            studentName: 'Me',
            title: item.title,
            item: item,
            aiAvailable: false,
            aiResult: null,
            questions: const <Question>[],
          ),
        ),
      );

      for (int i = 0; i < 20; i++) {
        await tester.pump(const Duration(milliseconds: 120));
        if (find.text('Rank Intelligence').evaluate().isNotEmpty) {
          break;
        }
      }
      for (int i = 0; i < 30; i++) {
        await tester.pump(const Duration(milliseconds: 120));
        if (find.text('Alice').evaluate().isNotEmpty) {
          break;
        }
      }

      expect(find.text('Rank Intelligence'), findsOneWidget);
      expect(find.textContaining('#2'), findsWidgets);
      expect(find.textContaining('50.0%'), findsWidgets);
      expect(find.text('Leaderboard (Best Attempt)'), findsOneWidget);
      expect(find.text('Alice'), findsOneWidget);
      expect(find.text('Me'), findsOneWidget);
    },
  );

  testWidgets(
    'Analytics reattempt opens instruction screen and clears autosave',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1080, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final QuizItem item = QuizItem(
        id: 'quiz_reattempt_functional',
        title: 'Reattempt Test|Physics',
        url: '',
        deadline: DateTime.now().add(const Duration(days: 1)),
        type: 'Exam',
        durationMinutes: 30,
        isAiGenerated: false,
      );

      SharedPreferences.setMockInitialValues(<String, Object>{
        'auto_${item.id}': '{"quizId":"${item.id}"}',
      });

      final ResultData current = ResultData(
        quizId: item.id,
        quizTitle: item.title,
        score: 52,
        maxScore: 100,
        correct: 13,
        wrong: 12,
        skipped: 5,
        totalTime: 1100,
        sectionAccuracy: const <String, double>{'Physics': 52},
        userAnswers: const <int, List<String>>{},
      );

      await tester.pumpWidget(
        MaterialApp(
          home: AnalyticsScreen(
            data: current,
            studentName: 'Tester',
            title: item.title,
            item: item,
            aiAvailable: false,
            aiResult: null,
            questions: const <Question>[],
          ),
        ),
      );
      await tester.pumpDeterministic();

      for (int i = 0; i < 8 && find.text('Reattempt').evaluate().isEmpty; i++) {
        await tester.drag(find.byType(Scrollable).first, const Offset(0, -420));
        await tester.pumpDeterministic();
      }

      expect(find.text('Reattempt'), findsOneWidget);
      await tester.tap(find.text('Reattempt'));
      await tester.pumpDeterministic();

      expect(find.text('Before You Start'), findsOneWidget);

      final SharedPreferences prefs = await SharedPreferences.getInstance();
      expect(prefs.getString('auto_${item.id}'), isNull);
    },
  );
}
