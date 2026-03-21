import 'dart:convert';

import 'package:lalacore_rebuild/models/import_question_models.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';
import 'package:lalacore_rebuild/services/teacher_question_import_service.dart';

ImportedQuestion _autoFillForPublish(ImportedQuestion q) {
  if (q.type != ImportedQuestionType.numerical && q.options.isEmpty) {
    return q.copyWith(
      type: ImportedQuestionType.numerical,
      correctAnswer: q.correctAnswer.copyWith(
        numerical: (q.correctAnswer.numerical ?? '').trim().isEmpty
            ? '0'
            : q.correctAnswer.numerical,
        multiple: const <String>[],
        clearSingle: true,
      ),
    );
  }
  switch (q.type) {
    case ImportedQuestionType.numerical:
      return q.copyWith(
        correctAnswer: q.correctAnswer.copyWith(
          numerical: (q.correctAnswer.numerical ?? '').trim().isEmpty
              ? '0'
              : q.correctAnswer.numerical,
        ),
      );
    case ImportedQuestionType.mcqMulti:
      final String first = q.options.isEmpty ? 'A' : q.options.first.label;
      return q.copyWith(
        correctAnswer: q.correctAnswer.copyWith(
          single: first,
          multiple: q.correctAnswer.multiple.isEmpty
              ? <String>[first]
              : q.correctAnswer.multiple,
          clearNumerical: true,
        ),
      );
    case ImportedQuestionType.mcqSingle:
      final String first = q.options.isEmpty ? 'A' : q.options.first.label;
      return q.copyWith(
        correctAnswer: q.correctAnswer.copyWith(
          single: (q.correctAnswer.single ?? '').trim().isEmpty
              ? first
              : q.correctAnswer.single,
          multiple: (q.correctAnswer.single ?? '').trim().isEmpty
              ? <String>[first]
              : <String>[q.correctAnswer.single!.trim()],
          clearNumerical: true,
        ),
      );
  }
}

void _printQuestionSummary(List<ImportedQuestion> questions) {
  int valid = 0;
  int review = 0;
  int invalid = 0;
  for (final ImportedQuestion q in questions) {
    switch (q.validationStatus) {
      case ImportValidationStatus.valid:
        valid++;
      case ImportValidationStatus.review:
        review++;
      case ImportValidationStatus.invalid:
        invalid++;
    }
  }
  print(
    'questions=${questions.length} valid=$valid review=$review invalid=$invalid',
  );
}

Future<void> main() async {
  final TeacherQuestionImportService importService =
      TeacherQuestionImportService();
  final BackendService backendService = BackendService();

  const String rawText = '''
9. Let the tangents drawn from the origin to the circle, x^2 + y^2 - 8x - 4y + 16 = 0 touch it at the points A and B. The (AB)^2 is equal to:
(1) 52/5
(2) 32/5
(3) 56/5
(4) 64/5

10. If a line, y = mx + c is a tangent to the circle, (x - 3)^2 + y^2 = 1 and it is perpendicular to a line L1, where L1 is the tangent to the circle, x^2 + y^2 = 1 at the point (1/sqrt(2), 1/sqrt(2)), then
(1) c^2 - 6c + 7 = 0
(2) c^2 + 6c + 7 = 0
(3) c^2 + 7c + 6 = 0
(4) c^2 - 7c + 6 = 0

11. If the curves, x^2 - 6x + y^2 + 8 = 0 and x^2 - 8y + y^2 + 16 - k = 0, (k > 0) touch each other at a point, then the largest value of k is ____.

12. If one of the diameters of the circle x^2 + y^2 - 2x - 6y + 6 = 0 is a chord of another circle C', whose center is at (2,1), then its radius is ____.

13. If the locus of the mid-point of the line segment from the point (3, 2) to a point on the circle, x^2 + y^2 = 1 is a circle of radius r, then r is equal to:
(1) 1
(2) 1/2
(3) 1/3
(4) 1/4
''';

  final List<ImportedQuestion> parsed = importService
      .lc9ParseQuestions(
        rawText,
        subject: 'Mathematics',
        chapter: 'Coordinate Geometry',
        difficulty: 'JEE Main',
      )
      .map(importService.validateQuestion)
      .toList();

  print('=== PARSE OUTPUT ===');
  _printQuestionSummary(parsed);
  for (int i = 0; i < parsed.length; i++) {
    final ImportedQuestion q = parsed[i];
    print(
      'Q${i + 1}: ${importedQuestionTypeToString(q.type)} status=${importValidationStatusToString(q.validationStatus)} '
      'options=${q.options.length} errors=${q.validationErrors.length}',
    );
    if (i < 2) {
      print(
        '  text=${q.questionText.substring(0, q.questionText.length > 120 ? 120 : q.questionText.length)}',
      );
    }
  }

  final List<ImportedQuestion> edited = parsed
      .map(_autoFillForPublish)
      .map(importService.validateQuestion)
      .toList();

  print('\n=== AFTER AUTO-FILL (simulate teacher edit before publish) ===');
  _printQuestionSummary(edited);

  final List<Map<String, dynamic>> payload = edited
      .map((ImportedQuestion q) => q.toJson())
      .toList();
  final Map<String, dynamic> meta = <String, dynamic>{
    'teacher_id': 'teacher_import_pipeline',
    'subject': 'Mathematics',
    'chapter': 'Coordinate Geometry',
    'difficulty': 'JEE Main',
  };

  final Map<String, dynamic> saveRes = await backendService.lc9SaveImportDrafts(
    questions: payload,
    meta: meta,
  );
  final Map<String, dynamic> publishRes = await backendService
      .lc9PublishImportQuestions(questions: payload, meta: meta);

  print('\n=== BACKEND SYNC RESULTS ===');
  print('save status=${saveRes['status']} ok=${saveRes['ok']}');
  print('save body=${jsonEncode(saveRes)}');
  print('publish status=${publishRes['status']} ok=${publishRes['ok']}');
  print('publish body=${jsonEncode(publishRes)}');
}
