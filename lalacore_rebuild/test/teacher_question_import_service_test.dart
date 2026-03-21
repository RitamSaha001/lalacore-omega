import 'package:flutter_test/flutter_test.dart';
import 'package:lalacore_rebuild/models/import_question_models.dart';
import 'package:lalacore_rebuild/services/teacher_question_import_service.dart';

void main() {
  group('TeacherQuestionImportService parser and validation', () {
    final TeacherQuestionImportService service = TeacherQuestionImportService();

    test('Single correct MCQ input is parsed and validated', () {
      const String raw = '''
Choose the correct option
1. If x + 1 = 2, then x equals:
(A) 0
(B) 1
(C) 2
(D) 3
Ans: B
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.mcqSingle);
      expect(out.first.correctAnswer.single, 'B');
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });

    test('Multi-correct instruction detection works', () {
      const String raw = '''
Section: Select all correct options
1. Which are prime numbers?
A) 2
B) 4
C) 3
D) 6
Ans: A, C
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.mcqMulti);
      expect(out.first.correctAnswer.multiple, <String>['A', 'C']);
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });

    test('Numerical-only section is classified as NUMERICAL', () {
      const String raw = '''
Section: Numerical answer type
Q1) Evaluate 2 + 3.
Ans: 5
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.numerical);
      expect(out.first.options, isEmpty);
      expect(out.first.correctAnswer.numerical, '5');
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });

    test('OCR messy formatting keeps multiline options', () {
      const String raw = r'''
Choose the correct option
1) Evaluate $\frac{1}{2} + \frac{1}{2}$.
A) 1
B) 0
continues as zero case
C) 2
D) -1
Ans: A
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.mcqSingle);
      expect(out.first.options.length, 4);
      expect(out.first.options[1].text, contains('continues as zero case'));
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });

    test('Mixed section with multiple types is parsed correctly', () {
      const String raw = '''
Section: Select all correct options
1. Select all true statements.
A. 1 < 2
B. 2 < 1
C. 3 > 1
D. 0 > 1
Ans: A, C
Section: Numerical answer type
Q2) Enter the correct value of 2^3.
Ans: 8
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 2);
      expect(out[0].type, ImportedQuestionType.mcqMulti);
      expect(out[0].validationStatus, ImportValidationStatus.valid);
      expect(out[1].type, ImportedQuestionType.numerical);
      expect(out[1].correctAnswer.numerical, '8');
      expect(out[1].validationStatus, ImportValidationStatus.valid);
    });

    test('No options and no numerical cue is marked invalid', () {
      const String raw = '''
1. Explain why the sky appears blue.
Ans: Scattering
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.mcqSingle);
      expect(out.first.validationStatus, ImportValidationStatus.invalid);
      expect(
        out.first.validationErrors.any(
          (String e) => e.contains('No options detected'),
        ),
        isTrue,
      );
    });

    test('Blank placeholder is inferred as numerical and flagged review', () {
      const String raw = '''
1. If x + y = 7 and x - y = 1, then x is ____.
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.numerical);
      expect(out.first.validationStatus, ImportValidationStatus.review);
      expect(out.first.options, isEmpty);
      expect(
        out.first.validationErrors.any(
          (String e) => e.toLowerCase().contains('not detected'),
        ),
        isTrue,
      );
    });

    test(
      'OCR normalization keeps radius indices and preserves existing powers',
      () {
        const String raw = '''
1. Two circles in first quadrant of radii r1 and r2 touch the axes. Then r1^2 + r2^2 - r1r2 = ____.
''';

        final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
        expect(out.length, 1);
        expect(out.first.questionText, contains('r1 and r2 touch the axes'));
        expect(out.first.questionText, contains('r1^2 + r2^2 - r1r2'));
        expect(out.first.questionText, isNot(contains('r^2')));
      },
    );

    test('OCR normalization converts x2 and y2 in equation context', () {
      const String raw = '''
1. For circle x2 + y2 - 4x + 6 = 0, radius is ____.
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.questionText, contains('x^2 + y^2 - 4x + 6 = 0'));
    });

    test('Multi-correct phrase in question text is detected', () {
      const String raw = '''
1. Which of the following statements are correct?
A) 1 < 2
B) 2 < 1
C) 3 > 1
D) 0 > 1
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.mcqMulti);
    });

    test('Duplicate option text is invalid', () {
      const String raw = '''
Choose the correct option
1. Duplicate options test
A) 42
B) 42
C) 11
D) 10
Ans: A
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.validationStatus, ImportValidationStatus.invalid);
      expect(
        out.first.validationErrors.any(
          (String e) => e.toLowerCase().contains('duplicate option'),
        ),
        isTrue,
      );
    });

    test('MCQ_MULTI with only one correct answer is flagged review', () {
      const String raw = '''
Section: Select all correct options
1. Multi answer but single detected
A) Option A
B) Option B
C) Option C
D) Option D
Ans: A
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.type, ImportedQuestionType.mcqMulti);
      expect(out.first.validationStatus, ImportValidationStatus.review);
      expect(
        out.first.validationErrors.any(
          (String e) => e.toLowerCase().contains('review required'),
        ),
        isTrue,
      );
    });

    test('Inline options in one row are split correctly', () {
      const String raw = '''
1. If x + 1 = 2, then x equals:
(A) 0 (B) 1 (C) 2 (D) 3
Ans: B
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.options.length, 4);
      expect(out.first.correctAnswer.single, 'B');
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });

    test('Compact exercise answer-key table is applied in order', () {
      const String raw = '''
EXERCISE (O-1)
1. First question
(A) one (B) two (C) three (D) four
2. Second question
(A) alpha (B) beta (C) gamma (D) delta

EXERCISE (O-1)
Que. 1 2
Ans. C A
ANSWER KEY
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 2);
      expect(out[0].correctAnswer.single, 'C');
      expect(out[1].correctAnswer.single, 'A');
      expect(out[0].validationStatus, ImportValidationStatus.valid);
      expect(out[1].validationStatus, ImportValidationStatus.valid);
    });

    test('Numeric inline options are split as A-D options', () {
      const String raw = '''
1. Pick the correct value.
(1) ten (2) twenty (3) thirty (4) forty
Ans: 2
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.options.length, 4);
      expect(out.first.correctAnswer.single, 'B');
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });

    test('Answer-key artifact rows are not parsed as questions', () {
      const String raw = '''
1. Real question
A) one
B) two
C) three
D) four
Ans: B

EXERCISE (O-1)
Que. 1 2 3
Ans. B C A
ANSWER KEY
1. 6  2. 3  3. 4
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.questionText, contains('Real question'));
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });

    test('Symbol-font artifacts are normalized to proper math symbols', () {
      const String raw = '''
1. Let x  y and x  A with x  y and y  x.
A) True
B) False
Ans: A
''';

      final List<ImportedQuestion> out = service.lc9ParseQuestions(raw);
      expect(out.length, 1);
      expect(out.first.questionText, contains('β'));
      expect(out.first.questionText, contains('∈'));
      expect(out.first.questionText, contains('<'));
      expect(out.first.questionText, contains('≥'));
      expect(out.first.questionText.contains('\uf0ce'), isFalse);
      expect(out.first.validationStatus, ImportValidationStatus.valid);
    });
  });
}
