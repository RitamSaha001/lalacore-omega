import 'dart:convert';
import 'dart:typed_data';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

import 'package:lalacore_rebuild/main.dart';
import 'package:lalacore_rebuild/models/quiz_models.dart';
import 'package:lalacore_rebuild/models/session.dart';
import 'package:lalacore_rebuild/services/ai_engine_service.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';
import 'package:lalacore_rebuild/widgets/smart_text.dart';

import 'test_pump_utils.dart';

class _FakeFilePicker extends FilePicker {
  _FakeFilePicker({required this.result});

  final FilePickerResult? result;

  @override
  Future<FilePickerResult?> pickFiles({
    String? dialogTitle,
    String? initialDirectory,
    FileType type = FileType.any,
    List<String>? allowedExtensions,
    Function(FilePickerStatus)? onFileLoading,
    bool allowCompression = false,
    int compressionQuality = 0,
    bool allowMultiple = false,
    bool withData = false,
    bool withReadStream = false,
    bool lockParentWindow = false,
    bool readSequential = false,
  }) async {
    return result;
  }
}

class _FakePaperImportAiEngineService extends AiEngineService {
  _FakePaperImportAiEngineService() : super(backendService: BackendService());

  @override
  Future<Map<String, dynamic>> sendChat({
    required String prompt,
    required String userId,
    required String chatId,
    String function = 'general_chat',
    String responseStyle = 'exam_coach',
    bool enablePersona = true,
    String personaMode = 'soft_possessive_academic_girlfriend',
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    final Map<String, dynamic> payload = <String, dynamic>{
      'title': 'Imported Algebra Set',
      'part_name': 'Part A',
      'questions': <Map<String, dynamic>>[
        <String, dynamic>{
          'number': '1',
          'question_text': r'Solve $\frac{1}{x} = 2$',
          'question_type': 'MCQ',
          'options': <String>[
            r'$x=\frac{1}{2}$',
            r'$x=2$',
            r'$x=-2$',
            r'$x=0$',
          ],
          'correct_answers': <String>[r'$x=\frac{1}{2}$'],
          'marks': <String, dynamic>{'positive': 4, 'negative': 1},
        },
      ],
    };
    return <String, dynamic>{'ok': true, 'answer': jsonEncode(payload)};
  }
}

List<Map<String, dynamic>> _hardMixedTenQuestions({bool binomialLead = false}) {
  final Map<String, dynamic> lead = binomialLead
      ? <String, dynamic>{
          'question_text': r'Binomial: coefficient of $x^3$ in $(1 + x)^8$ is',
          'question_type': 'MCQ',
          'options': <String>['56', '28', '70', '84'],
          'correct_answers': <String>['56'],
          'difficulty': 2,
          'concept_tags': <String>['Binomial Theorem', 'PYQ'],
          'solution_explanation':
              r'Step 1: In $(1+x)^n$, coefficient of $x^r$ is $^nC_r$. Step 2: Put $n=8,r=3$ to get $^8C_3=56$.',
        }
      : <String, dynamic>{
          'question_text': r'PYQ-style: If $\int_0^1 x^2\,dx = ?$',
          'question_type': 'MCQ',
          'options': <String>[
            r'$\frac{1}{2}$',
            r'$\frac{1}{3}$',
            r'$\frac{1}{4}$',
            r'$1$',
          ],
          'correct_answers': <String>[r'$\frac{1}{3}$'],
          'difficulty': 1,
          'concept_tags': <String>['Calculus', 'PYQ'],
          'solution_explanation':
              r'Step 1: Integrate to get $\int x^2 dx = \frac{x^3}{3}$. Step 2: Evaluate from $0$ to $1$ to obtain $\frac{1}{3}$.',
        };

  return <Map<String, dynamic>>[
    lead,
    <String, dynamic>{
      'question_text':
          r'Find the smallest integer $n$ such that $2^n > 1000$, then report $n \bmod 10$.',
      'question_type': 'NUMERICAL',
      'correct_answer': '0',
      'difficulty': 2,
      'concept_tags': <String>['Exponents', 'Inequality'],
      'solution_explanation':
          r'Step 1: Note $2^9=512<1000$ and $2^{10}=1024>1000$, so the least $n$ is $10$. Step 2: Compute $10\bmod 10=0$.',
    },
    <String, dynamic>{
      'question_text': r'For $p(x)=x^4-5x^2+4$, choose all correct statements.',
      'question_type': 'MULTI',
      'options': <String>[
        r'All four roots are real.',
        r'The product of all roots is $4$.',
        r'Exactly two roots are positive.',
        r'The sum of squares of roots is $10$.',
      ],
      'correct_answers': <String>[
        r'All four roots are real.',
        r'The product of all roots is $4$.',
        r'Exactly two roots are positive.',
        r'The sum of squares of roots is $10$.',
      ],
      'difficulty': 3,
      'concept_tags': <String>['Polynomial', 'Roots'],
      'solution_explanation':
          r'Step 1: Put $y=x^2$ so $y^2-5y+4=0$ gives $y=1,4$, hence roots are $\pm1,\pm2$. Step 2: Verify each statement directly from these roots.',
    },
    <String, dynamic>{
      'question_text':
          r'For the circle $x^2+y^2-6x+4y-12=0$, the length of tangent from $(8,2)$ is',
      'question_type': 'MCQ',
      'options': <String>[r'$4$', r'$2\sqrt{5}$', r'$2$', r'$4\sqrt{2}$'],
      'correct_answers': <String>[r'$4$'],
      'difficulty': 4,
      'concept_tags': <String>['Circle', 'Tangent Length'],
      'solution_explanation':
          r'Step 1: Write circle as $(x-3)^2+(y+2)^2=25$ so center $(3,-2)$ and radius $5$. Step 2: Distance from $(8,2)$ to center is $\sqrt{41}$, so tangent length is $\sqrt{41-25}=4$.',
    },
    <String, dynamic>{
      'question_text':
          r'If $A=\begin{bmatrix}1&1&1\\1&2&3\\1&3&6\end{bmatrix}$, find $|\det(A)| \bmod 10$.',
      'question_type': 'NUMERICAL',
      'correct_answer': '1',
      'difficulty': 5,
      'concept_tags': <String>['Determinant', 'Row Operation'],
      'solution_explanation':
          r'Step 1: Apply $R_2\leftarrow R_2-R_1$ and $R_3\leftarrow R_3-R_1$ to get first column with one non-zero entry. Step 2: Reduced $2\times2$ determinant is $1$, so $|\det(A)|\bmod 10=1$.',
    },
    <String, dynamic>{
      'question_text':
          r'A fair die is rolled thrice. For $X=$ number of sixes, choose all correct statements.',
      'question_type': 'MULTI',
      'options': <String>[
        r'$P(X=0)=\left(\frac{5}{6}\right)^3$',
        r'$E[X]=\frac{1}{2}$',
        r'$P(X\ge1)=1-\left(\frac{5}{6}\right)^3$',
        r'$\mathrm{Var}(X)=\frac{5}{12}$',
      ],
      'correct_answers': <String>[
        r'$P(X=0)=\left(\frac{5}{6}\right)^3$',
        r'$E[X]=\frac{1}{2}$',
        r'$P(X\ge1)=1-\left(\frac{5}{6}\right)^3$',
        r'$\mathrm{Var}(X)=\frac{5}{12}$',
      ],
      'difficulty': 3,
      'concept_tags': <String>['Probability', 'Binomial Distribution'],
      'solution_explanation':
          r'Step 1: Model $X\sim\mathrm{Binomial}(n=3,p=\frac{1}{6})$. Step 2: Use standard formulas for $P(X=0)$, $E[X]=np$, $P(X\ge1)=1-P(X=0)$ and $\mathrm{Var}(X)=np(1-p)$.',
    },
    <String, dynamic>{
      'question_text':
          r'If vectors $\vec a,\vec b$ satisfy $|\vec a|=3$, $|\vec b|=4$, and $\vec a\cdot\vec b=6$, then $|\vec a-\vec b|$ equals',
      'question_type': 'MCQ',
      'options': <String>[
        r'$\sqrt{13}$',
        r'$\sqrt{7}$',
        r'$5$',
        r'$\sqrt{19}$',
      ],
      'correct_answers': <String>[r'$\sqrt{13}$'],
      'difficulty': 2,
      'concept_tags': <String>['Vector Algebra', 'Dot Product'],
      'solution_explanation':
          r'Step 1: Use $|\vec a-\vec b|^2=|\vec a|^2+|\vec b|^2-2\vec a\cdot\vec b$. Step 2: Substitute values to get $9+16-12=13$, hence $|\vec a-\vec b|=\sqrt{13}$.',
    },
    <String, dynamic>{
      'question_text':
          r'In $[0,2\pi)$, number of solutions of $2\sin x\cos x=\cos x$ is',
      'question_type': 'NUMERICAL',
      'correct_answer': '4',
      'difficulty': 4,
      'concept_tags': <String>['Trigonometry', 'Equation'],
      'solution_explanation':
          r'Step 1: Factor as $\cos x(2\sin x-1)=0$. Step 2: Solutions are $x=\frac{\pi}{2},\frac{3\pi}{2},\frac{\pi}{6},\frac{5\pi}{6}$, so count is $4$.',
    },
    <String, dynamic>{
      'question_text':
          r'For invertible matrices $A,B$ of same order, choose all true statements.',
      'question_type': 'MULTI',
      'options': <String>[
        r'$(AB)^{-1}=B^{-1}A^{-1}$',
        r'$\det(AB)=\det(A)\det(B)$',
        r'$(A^T)^{-1}=(A^{-1})^T$',
        r'$\det(A+B)=\det(A)+\det(B)$',
      ],
      'correct_answers': <String>[
        r'$(AB)^{-1}=B^{-1}A^{-1}$',
        r'$\det(AB)=\det(A)\det(B)$',
        r'$(A^T)^{-1}=(A^{-1})^T$',
      ],
      'difficulty': 5,
      'concept_tags': <String>['Matrices', 'Determinants'],
      'solution_explanation':
          'Step 1: Use standard inverse and determinant identities for matrix products and transpose. Step 2: Check additivity of determinant is false in general, so only first three are correct.',
    },
    <String, dynamic>{
      'question_text':
          r'If $S_n=1+2+\cdots+n$ and $S_n=55$, find $n \bmod 10$.',
      'question_type': 'NUMERICAL',
      'correct_answer': '0',
      'difficulty': 1,
      'concept_tags': <String>['Sequence and Series', 'AP Sum'],
      'solution_explanation':
          r'Step 1: Use $S_n=\frac{n(n+1)}{2}=55$, so $n(n+1)=110$. Step 2: Solve to get $n=10$, hence $n \bmod 10=0$.',
    },
  ];
}

List<Map<String, dynamic>> _hardBinomialFiveQuestions() {
  return <Map<String, dynamic>>[
    <String, dynamic>{
      'question_text': r'In $(1+x)^9$, coefficient of $x^4$ is',
      'question_type': 'MCQ',
      'options': <String>['126', '84', '70', '36'],
      'correct_answers': <String>['126'],
      'difficulty': 2,
      'concept_tags': <String>['Binomial Theorem', 'Coefficient'],
      'solution_explanation':
          r'Step 1: Coefficient of $x^r$ in $(1+x)^n$ is $^nC_r$. Step 2: Put $n=9,r=4$ to get $^9C_4=126$.',
    },
    <String, dynamic>{
      'question_text':
          r'If coefficient of $x^2$ in $(1+x)^n$ is $45$, find $n \bmod 10$.',
      'question_type': 'NUMERICAL',
      'correct_answer': '0',
      'difficulty': 3,
      'concept_tags': <String>['Binomial Theorem', 'Inverse Parameter'],
      'solution_explanation':
          r'Step 1: Set $^nC_2=45 \Rightarrow \frac{n(n-1)}{2}=45$. Step 2: Solve $n^2-n-90=0$ to get $n=10$, hence remainder is $0$.',
    },
    <String, dynamic>{
      'question_text':
          r'For expansion of $(1+x)^8$, choose all correct statements.',
      'question_type': 'MULTI',
      'options': <String>[
        r'Coefficient of $x^2$ is $28$.',
        r'The middle coefficient is $70$.',
        r'Sum of all coefficients is $256$.',
        r'The constant term is $8$.',
      ],
      'correct_answers': <String>[
        r'Coefficient of $x^2$ is $28$.',
        r'The middle coefficient is $70$.',
        r'Sum of all coefficients is $256$.',
      ],
      'difficulty': 4,
      'concept_tags': <String>['Binomial Theorem', 'Middle Term', 'Identity'],
      'solution_explanation':
          r'Step 1: Use $^8C_2=28$ and for even $n$, middle coefficient is $^8C_4=70$. Step 2: Sum coefficients from $(1+1)^8=256$ and constant term is $1$, not $8$.',
    },
    <String, dynamic>{
      'question_text': r'Greatest coefficient in expansion of $(1+x)^9$ equals',
      'question_type': 'MCQ',
      'options': <String>['126', '84', '252', '36'],
      'correct_answers': <String>['126'],
      'difficulty': 5,
      'concept_tags': <String>['Binomial Theorem', 'Greatest Coefficient'],
      'solution_explanation':
          r'Step 1: For odd $n=9$, two middle terms have greatest coefficients: $^9C_4$ and $^9C_5$. Step 2: Both are equal to $126$.',
    },
    <String, dynamic>{
      'question_text':
          r'Number of terms independent of $x$ in expansion of $(x+\frac{1}{x})^8$ is',
      'question_type': 'NUMERICAL',
      'correct_answer': '1',
      'difficulty': 1,
      'concept_tags': <String>['Binomial Theorem', 'Constant Term'],
      'solution_explanation':
          r'Step 1: General term is $^8C_r x^{8-2r}$. Step 2: Set exponent $8-2r=0 \Rightarrow r=4$, giving exactly one constant term.',
    },
  ];
}

class _FakeQuizFallbackAiEngineService extends AiEngineService {
  _FakeQuizFallbackAiEngineService() : super(backendService: BackendService());

  @override
  Future<Map<String, dynamic>> sendChat({
    required String prompt,
    required String userId,
    required String chatId,
    String function = 'general_chat',
    String responseStyle = 'exam_coach',
    bool enablePersona = true,
    String personaMode = 'soft_possessive_academic_girlfriend',
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    if (function != 'ai_generate_quiz') {
      return <String, dynamic>{'ok': true, 'answer': '{}'};
    }
    final Map<String, dynamic> payload = <String, dynamic>{
      'quiz_id': 'fallback_quiz_1',
      'questions': _hardMixedTenQuestions(),
    };
    return <String, dynamic>{'ok': true, 'answer': jsonEncode(payload)};
  }
}

class _FakeQuizMarkdownFallbackAiEngineService extends AiEngineService {
  _FakeQuizMarkdownFallbackAiEngineService()
    : super(backendService: BackendService());

  @override
  Future<Map<String, dynamic>> sendChat({
    required String prompt,
    required String userId,
    required String chatId,
    String function = 'general_chat',
    String responseStyle = 'exam_coach',
    bool enablePersona = true,
    String personaMode = 'soft_possessive_academic_girlfriend',
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    if (function != 'ai_generate_quiz') {
      return <String, dynamic>{'ok': true, 'answer': '{}'};
    }
    final Map<String, dynamic> payload = <String, dynamic>{
      'quiz_id': 'fallback_markdown_1',
      'questions': _hardMixedTenQuestions(binomialLead: true),
    };
    final String pretty = const JsonEncoder.withIndent('  ').convert(payload);
    return <String, dynamic>{
      'ok': true,
      'questions_json': _hardMixedTenQuestions(binomialLead: true),
      'answer': '**Final Answer**\n\n```json\n$pretty\n```',
    };
  }
}

class _FakePaperImportTextFallbackAiEngineService extends AiEngineService {
  _FakePaperImportTextFallbackAiEngineService()
    : super(backendService: BackendService());

  @override
  Future<Map<String, dynamic>> sendChat({
    required String prompt,
    required String userId,
    required String chatId,
    String function = 'general_chat',
    String responseStyle = 'exam_coach',
    bool enablePersona = true,
    String personaMode = 'soft_possessive_academic_girlfriend',
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    return <String, dynamic>{
      'ok': true,
      'answer': '''
10. If a line, y = mx + c is a tangent to the circle, (x - 3)^2 + y^2 = 1 and it is perpendicular to a line L1, where L1 is the tangent to the circle, x^2 + y^2 = 1 at the point (1/sqrt(2), 1/sqrt(2)), then [JEE (Main) 2020]
(1) c^2 - 6c + 7 = 0   (2) c^2 + 6c + 7 = 0   (3) c^2 + 7c + 6 = 0   (4) c^2 - 7c + 6 = 0
MCR036
''',
    };
  }
}

class _FakeBinomialFiveQuestionAiEngineService extends AiEngineService {
  _FakeBinomialFiveQuestionAiEngineService()
    : super(backendService: BackendService());

  @override
  Future<Map<String, dynamic>> sendChat({
    required String prompt,
    required String userId,
    required String chatId,
    String function = 'general_chat',
    String responseStyle = 'exam_coach',
    bool enablePersona = true,
    String personaMode = 'soft_possessive_academic_girlfriend',
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    if (function != 'ai_generate_quiz') {
      return <String, dynamic>{'ok': true, 'answer': '{}'};
    }
    final Map<String, dynamic> payload = <String, dynamic>{
      'quiz_id': 'binomial_quiz_5q',
      'title': 'Binomial Theorem 5Q',
      'questions': _hardBinomialFiveQuestions(),
    };
    return <String, dynamic>{'ok': true, 'answer': jsonEncode(payload)};
  }
}

Finder _smartTextContains(String snippet) {
  return find.byWidgetPredicate(
    (Widget widget) => widget is SmartText && widget.text.contains(snippet),
  );
}

FilePicker? _currentFilePickerOrNull() {
  try {
    return FilePicker.platform;
  } catch (_) {
    return null;
  }
}

void main() {
  setUp(() {
    Session.isTeacher = true;
    Session.studentId = 'TEACHER';
    Session.studentName = 'Teacher';
    Session.accountId = 'TEACHER';
    Session.email = '';
    Session.username = 'Teacher';
    Session.chatId = 'TEACHER';
    Session.chatName = 'Teacher';
    SharedPreferences.setMockInitialValues(<String, Object>{});
  });

  testWidgets(
    'teacher can add and edit questions in quiz draft with latex content',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1200, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      await tester.pumpWidget(
        const MaterialApp(home: CreateQuizScreen(selfPracticeMode: false)),
      );
      await tester.pumpDeterministic();

      expect(find.text('Import Question Paper'), findsOneWidget);
      expect(find.text('Add Question'), findsOneWidget);

      await tester.tap(find.text('Add Question'));
      await tester.pumpDeterministic();

      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_text')),
        r'Solve $x^2 + 1 = 0$',
      );
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_0')),
        r'$i$',
      );
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_1')),
        r'$-i$',
      );
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_2')),
        r'$1$',
      );
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_3')),
        r'$0$',
      );

      await tester.tap(find.text('Save Question'));
      await tester.pumpDeterministic();

      final bool editVisible = await tester.pumpUntilFound(
        find.text('Edit This Question'),
      );
      expect(editVisible, isTrue);
      expect(find.text('Edit This Question'), findsOneWidget);
      expect(find.text('Options'), findsOneWidget);

      await tester.tap(find.text('Edit This Question'));
      await tester.pumpDeterministic();

      expect(find.text('Edit Question'), findsOneWidget);
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_text')),
        r'Updated: Solve $\frac{1}{2}x = 1$',
      );

      await tester.tap(find.text('Save Question'));
      await tester.pumpDeterministic();

      expect(find.text('Edit This Question'), findsOneWidget);
      expect(find.text('Correct Answer'), findsOneWidget);
    },
  );

  testWidgets(
    'teacher can import paper questions, keep latex, and edit imported question',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1200, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final AiEngineService originalAiService = aiService;
      final FilePicker? originalFilePicker = _currentFilePickerOrNull();
      addTearDown(() {
        aiService = originalAiService;
        if (originalFilePicker != null) {
          FilePicker.platform = originalFilePicker;
        }
      });

      aiService = _FakePaperImportAiEngineService();
      FilePicker.platform = _FakeFilePicker(
        result: FilePickerResult(<PlatformFile>[
          PlatformFile(
            name: 'paper_page_1.jpg',
            size: 8,
            bytes: Uint8List.fromList(<int>[1, 2, 3, 4, 5, 6, 7, 8]),
          ),
        ]),
      );

      await tester.pumpWidget(
        const MaterialApp(home: CreateQuizScreen(selfPracticeMode: false)),
      );
      await tester.pumpDeterministic();

      await tester.tap(find.text('Import Question Paper'));
      await tester.pumpDeterministic();
      await tester.tap(find.text('Pick From Files'));
      await tester.pumpDeterministic();

      expect(find.text('Assign Sub-Parts'), findsOneWidget);
      await tester.tap(find.text('Continue'));
      await tester.pumpDeterministic();

      final bool importedVisible = await tester.pumpUntilFound(
        _smartTextContains(r'Solve $\frac{1}{x} = 2$'),
        maxTicks: 220,
      );
      expect(importedVisible, isTrue);
      expect(_smartTextContains(r'Solve $\frac{1}{x} = 2$'), findsOneWidget);
      expect(_smartTextContains(r'A) $x=\frac{1}{2}$'), findsOneWidget);
      expect(_smartTextContains(r'$x=\frac{1}{2}$'), findsWidgets);

      await tester.tap(find.text('Edit This Question'));
      await tester.pumpDeterministic();
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_text')),
        r'Edited import: Solve $\frac{1}{x} = 4$',
      );
      await tester.tap(find.text('Save Question'));
      await tester.pumpDeterministic();

      expect(
        _smartTextContains(r'Edited import: Solve $\frac{1}{x} = 4$'),
        findsOneWidget,
      );
    },
  );

  testWidgets(
    'teacher AI generation falls back and returns hard editable draft',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1200, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final AiEngineService originalAiService = aiService;
      addTearDown(() {
        aiService = originalAiService;
      });

      aiService = _FakeQuizFallbackAiEngineService();

      await tester.pumpWidget(
        const MaterialApp(home: CreateQuizScreen(selfPracticeMode: false)),
      );
      await tester.pumpDeterministic();

      await tester.tap(find.text('Generate Hard Teacher Draft'));
      await tester.pumpDeterministic();

      expect(find.text('Hardness Profile'), findsOneWidget);
      await tester.tap(find.text('Generate Hard Quiz'));
      await tester.pump();

      for (int i = 0; i < 60; i++) {
        await tester.pump(const Duration(milliseconds: 200));
        if (find.text('Edit This Question').evaluate().isNotEmpty) {
          break;
        }
      }

      expect(find.text('Edit This Question'), findsWidgets);
      expect(
        _smartTextContains(r'PYQ-style: If $\int_0^1 x^2\,dx = ?$'),
        findsOneWidget,
      );
    },
  );

  testWidgets(
    'student AI generation falls back and opens exam directly in self practice',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1200, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      Session.isTeacher = false;
      Session.studentId = 'S1';
      Session.studentName = 'Student';
      Session.accountId = 'S1';
      Session.chatId = 'S1';
      Session.chatName = 'Student';

      final AiEngineService originalAiService = aiService;
      addTearDown(() {
        aiService = originalAiService;
      });

      aiService = _FakeQuizFallbackAiEngineService();

      await tester.pumpWidget(
        const MaterialApp(home: CreateQuizScreen(selfPracticeMode: true)),
      );
      await tester.pumpDeterministic();

      await tester.tap(find.text('Generate Hard Practice Quiz'));
      await tester.pumpDeterministic();
      await tester.tap(find.text('Generate Hard Quiz'));
      await tester.pump();

      for (int i = 0; i < 90; i++) {
        await tester.pump(const Duration(milliseconds: 200));
        if (find.textContaining('Question 1/').evaluate().isNotEmpty) {
          break;
        }
      }

      expect(find.textContaining('Question 1/'), findsOneWidget);
      expect(
        _smartTextContains(r'PYQ-style: If $\int_0^1 x^2\,dx = ?$'),
        findsOneWidget,
      );
    },
  );

  testWidgets(
    'teacher AI generation parses markdown-wrapped JSON fallback payload',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1200, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final AiEngineService originalAiService = aiService;
      addTearDown(() {
        aiService = originalAiService;
      });
      aiService = _FakeQuizMarkdownFallbackAiEngineService();

      await tester.pumpWidget(
        const MaterialApp(home: CreateQuizScreen(selfPracticeMode: false)),
      );
      await tester.pumpDeterministic();

      await tester.tap(find.text('Generate Hard Teacher Draft'));
      await tester.pumpDeterministic();
      await tester.tap(find.text('Generate Hard Quiz'));
      await tester.pump();

      for (int i = 0; i < 60; i++) {
        await tester.pump(const Duration(milliseconds: 200));
        if (find.text('Edit This Question').evaluate().isNotEmpty) {
          break;
        }
      }

      expect(find.text('Edit This Question'), findsWidgets);
      expect(_smartTextContains('Binomial: coefficient'), findsOneWidget);
    },
  );

  testWidgets(
    'teacher can generate/publish binomial 5Q and student can attempt it',
    (WidgetTester tester) async {
      await tester.binding.setSurfaceSize(const Size(1200, 2600));
      addTearDown(() => tester.binding.setSurfaceSize(null));

      final AiEngineService originalAiService = aiService;
      addTearDown(() {
        aiService = originalAiService;
      });
      aiService = _FakeBinomialFiveQuestionAiEngineService();

      Session.isTeacher = true;
      Session.studentId = 'TEACHER';
      Session.studentName = 'Teacher';
      Session.accountId = 'TEACHER';
      Session.chatId = 'TEACHER';
      Session.chatName = 'Teacher';

      final GlobalKey<NavigatorState> teacherNavKey =
          GlobalKey<NavigatorState>();
      await tester.pumpWidget(
        MaterialApp(navigatorKey: teacherNavKey, home: const SizedBox.shrink()),
      );
      teacherNavKey.currentState!.push(
        MaterialPageRoute<void>(
          builder: (_) => const CreateQuizScreen(selfPracticeMode: false),
        ),
      );
      await tester.pumpDeterministic();

      final Finder titleField = find.byWidgetPredicate(
        (Widget widget) =>
            widget is TextField && widget.decoration?.labelText == 'Quiz Title',
      );
      expect(titleField, findsOneWidget);
      await tester.enterText(titleField, 'Binomial Theorem 5Q');

      await tester.tap(find.text('Generate Hard Teacher Draft'));
      await tester.pumpDeterministic();
      await tester.drag(find.byType(Slider).first, const Offset(-1200, 0));
      await tester.pumpDeterministic();
      expect(find.text('Questions: 5'), findsOneWidget);
      await tester.tap(find.text('Generate Hard Quiz'));
      await tester.pump();

      for (int i = 0; i < 90; i++) {
        await tester.pump(const Duration(milliseconds: 200));
        if (find.text('Edit This Question').evaluate().isNotEmpty) {
          break;
        }
      }

      expect(find.text('Edit This Question'), findsWidgets);

      await tester.tap(find.text('Edit This Question').first);
      await tester.pumpDeterministic();
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_0')),
        '252',
      );
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_1')),
        '210',
      );
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_2')),
        '120',
      );
      await tester.enterText(
        find.byKey(const ValueKey<String>('quiz_question_option_3')),
        '56',
      );
      final Finder radios = find.byType(Radio<int>);
      expect(radios, findsNWidgets(4));
      await tester.tap(radios.at(3));
      await tester.pumpDeterministic();
      await tester.tap(find.text('Save Question'));
      await tester.pumpDeterministic();

      expect(find.text('Correct Answer'), findsOneWidget);
      expect(_smartTextContains('56'), findsWidgets);

      await tester.tap(find.byIcon(Icons.check_circle));
      await tester.pumpDeterministic();
      await tester.tap(find.text('Publish'));
      await tester.pumpDeterministic();

      Session.isTeacher = false;
      Session.studentId = 'S1';
      Session.studentName = 'Student';
      Session.accountId = 'S1';
      Session.chatId = 'S1';
      Session.chatName = 'Student';

      final List<QuizItem> studentItems = await localQuizStore.listQuizItems(
        viewerAccountId: Session.effectiveAccountId,
        viewerRole: Session.userRole,
      );
      final QuizItem published = studentItems.firstWhere(
        (QuizItem q) => q.title == 'Binomial Theorem 5Q',
      );
      final List<Question> storedQuestions = await localQuizStore.loadQuestions(
        published.id,
      );
      expect(storedQuestions.length, 5);

      await tester.pumpWidget(
        MaterialApp(
          home: ExamScreen(
            questions: storedQuestions,
            item: published,
            studentName: Session.effectiveDisplayName,
            isResume: false,
          ),
        ),
      );
      await tester.pumpDeterministic();

      final bool openedQuestion = await tester.pumpUntilFound(
        find.byType(RadioListTile<String>),
        maxTicks: 120,
      );
      expect(openedQuestion, isTrue);
      final Finder firstOption = find.byType(RadioListTile<String>).first;
      await tester.tap(firstOption);
      await tester.pumpDeterministic();
    },
  );

  testWidgets('teacher import pipeline can parse OCR text-style MCQ block', (
    WidgetTester tester,
  ) async {
    await tester.binding.setSurfaceSize(const Size(1200, 2600));
    addTearDown(() => tester.binding.setSurfaceSize(null));

    final AiEngineService originalAiService = aiService;
    final FilePicker? originalFilePicker = _currentFilePickerOrNull();
    addTearDown(() {
      aiService = originalAiService;
      if (originalFilePicker != null) {
        FilePicker.platform = originalFilePicker;
      }
    });

    aiService = _FakePaperImportTextFallbackAiEngineService();
    FilePicker.platform = _FakeFilePicker(
      result: FilePickerResult(<PlatformFile>[
        PlatformFile(
          name: 'mcq_page.jpg',
          size: 8,
          bytes: Uint8List.fromList(<int>[1, 2, 3, 4, 5, 6, 7, 8]),
        ),
      ]),
    );

    await tester.pumpWidget(
      const MaterialApp(home: CreateQuizScreen(selfPracticeMode: false)),
    );
    await tester.pumpDeterministic();

    await tester.tap(find.text('Import Question Paper'));
    await tester.pumpDeterministic();
    await tester.tap(find.text('Pick From Files'));
    await tester.pumpDeterministic();
    await tester.tap(find.text('Continue'));
    await tester.pumpDeterministic();

    final bool importedVisible = await tester.pumpUntilFound(
      _smartTextContains('If a line, y = mx + c is a tangent to the circle'),
      maxTicks: 220,
    );
    expect(importedVisible, isTrue);
    expect(
      _smartTextContains('If a line, y = mx + c is a tangent to the circle'),
      findsOneWidget,
    );
    expect(_smartTextContains('A) c^2 - 6c + 7 = 0'), findsOneWidget);
    expect(_smartTextContains('D) c^2 - 7c + 6 = 0'), findsOneWidget);
  });
}
