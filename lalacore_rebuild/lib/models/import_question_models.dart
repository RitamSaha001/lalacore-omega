import 'dart:convert';

enum ImportedQuestionType { mcqSingle, mcqMulti, numerical }

enum ImportValidationStatus { valid, invalid, review }

ImportedQuestionType importedQuestionTypeFromString(String raw) {
  final String token = raw.trim().toUpperCase();
  if (token == 'MCQ_MULTI' || token == 'MULTI' || token == 'MULTIPLE') {
    return ImportedQuestionType.mcqMulti;
  }
  if (token == 'NUMERICAL' || token == 'NUMERIC' || token == 'INTEGER') {
    return ImportedQuestionType.numerical;
  }
  return ImportedQuestionType.mcqSingle;
}

String importedQuestionTypeToString(ImportedQuestionType type) {
  switch (type) {
    case ImportedQuestionType.mcqSingle:
      return 'MCQ_SINGLE';
    case ImportedQuestionType.mcqMulti:
      return 'MCQ_MULTI';
    case ImportedQuestionType.numerical:
      return 'NUMERICAL';
  }
}

String importValidationStatusToString(ImportValidationStatus status) {
  switch (status) {
    case ImportValidationStatus.valid:
      return 'valid';
    case ImportValidationStatus.invalid:
      return 'invalid';
    case ImportValidationStatus.review:
      return 'review';
  }
}

ImportValidationStatus importValidationStatusFromString(String raw) {
  switch (raw.trim().toLowerCase()) {
    case 'valid':
      return ImportValidationStatus.valid;
    case 'invalid':
      return ImportValidationStatus.invalid;
    default:
      return ImportValidationStatus.review;
  }
}

class ImportOption {
  const ImportOption({required this.label, required this.text});

  final String label;
  final String text;

  ImportOption copyWith({String? label, String? text}) {
    return ImportOption(label: label ?? this.label, text: text ?? this.text);
  }

  Map<String, dynamic> toJson() => <String, dynamic>{
    'label': label,
    'text': text,
  };

  factory ImportOption.fromJson(Map<String, dynamic> json) {
    return ImportOption(
      label: (json['label'] ?? '').toString().trim(),
      text: (json['text'] ?? '').toString().trim(),
    );
  }
}

class ImportCorrectAnswer {
  const ImportCorrectAnswer({
    this.single,
    this.multiple = const <String>[],
    this.numerical,
    this.tolerance,
  });

  final String? single;
  final List<String> multiple;
  final String? numerical;
  final double? tolerance;

  ImportCorrectAnswer copyWith({
    String? single,
    List<String>? multiple,
    String? numerical,
    double? tolerance,
    bool clearSingle = false,
    bool clearNumerical = false,
  }) {
    return ImportCorrectAnswer(
      single: clearSingle ? null : (single ?? this.single),
      multiple: multiple ?? this.multiple,
      numerical: clearNumerical ? null : (numerical ?? this.numerical),
      tolerance: tolerance ?? this.tolerance,
    );
  }

  Map<String, dynamic> toJson() => <String, dynamic>{
    'single': single,
    'multiple': multiple,
    'numerical': numerical,
    if (tolerance != null) 'tolerance': tolerance,
  };

  factory ImportCorrectAnswer.fromJson(Map<String, dynamic> json) {
    final dynamic rawMultiple = json['multiple'];
    return ImportCorrectAnswer(
      single: (json['single'] ?? '').toString().trim().isEmpty
          ? null
          : (json['single'] ?? '').toString().trim(),
      multiple: rawMultiple is List
          ? rawMultiple
                .map((dynamic e) => e.toString().trim())
                .where((String e) => e.isNotEmpty)
                .toList()
          : const <String>[],
      numerical: (json['numerical'] ?? '').toString().trim().isEmpty
          ? null
          : (json['numerical'] ?? '').toString().trim(),
      tolerance: double.tryParse((json['tolerance'] ?? '').toString()),
    );
  }
}

class ImportedQuestion {
  const ImportedQuestion({
    required this.questionId,
    required this.type,
    required this.questionText,
    this.questionTextLatex = '',
    required this.options,
    this.optionsLatex = const <String, String>{},
    required this.correctAnswer,
    required this.subject,
    required this.chapter,
    required this.difficulty,
    required this.aiConfidence,
    this.publishRiskScore = 0.0,
    this.answerFillSource = 'manual',
    required this.validationStatus,
    required this.validationErrors,
  });

  final String questionId;
  final ImportedQuestionType type;
  final String questionText;
  final String questionTextLatex;
  final List<ImportOption> options;
  final Map<String, String> optionsLatex;
  final ImportCorrectAnswer correctAnswer;
  final String subject;
  final String chapter;
  final String difficulty;
  final double aiConfidence;
  final double publishRiskScore;
  final String answerFillSource;
  final ImportValidationStatus validationStatus;
  final List<String> validationErrors;

  ImportedQuestion copyWith({
    String? questionId,
    ImportedQuestionType? type,
    String? questionText,
    String? questionTextLatex,
    List<ImportOption>? options,
    Map<String, String>? optionsLatex,
    ImportCorrectAnswer? correctAnswer,
    String? subject,
    String? chapter,
    String? difficulty,
    double? aiConfidence,
    double? publishRiskScore,
    String? answerFillSource,
    ImportValidationStatus? validationStatus,
    List<String>? validationErrors,
  }) {
    return ImportedQuestion(
      questionId: questionId ?? this.questionId,
      type: type ?? this.type,
      questionText: questionText ?? this.questionText,
      questionTextLatex: questionTextLatex ?? this.questionTextLatex,
      options: options ?? this.options,
      optionsLatex: optionsLatex ?? this.optionsLatex,
      correctAnswer: correctAnswer ?? this.correctAnswer,
      subject: subject ?? this.subject,
      chapter: chapter ?? this.chapter,
      difficulty: difficulty ?? this.difficulty,
      aiConfidence: aiConfidence ?? this.aiConfidence,
      publishRiskScore: publishRiskScore ?? this.publishRiskScore,
      answerFillSource: answerFillSource ?? this.answerFillSource,
      validationStatus: validationStatus ?? this.validationStatus,
      validationErrors: validationErrors ?? this.validationErrors,
    );
  }

  Map<String, dynamic> toJson() => <String, dynamic>{
    'question_id': questionId,
    'type': importedQuestionTypeToString(type),
    'question_text': questionText,
    'question_text_latex': questionTextLatex,
    'options': options.map((ImportOption e) => e.toJson()).toList(),
    'options_latex': optionsLatex,
    'correct_answer': correctAnswer.toJson(),
    'subject': subject,
    'chapter': chapter,
    'difficulty': difficulty,
    'ai_confidence': aiConfidence,
    'publish_risk_score': publishRiskScore,
    'answer_fill_source': answerFillSource,
    'validation_status': importValidationStatusToString(validationStatus),
    'validation_errors': validationErrors,
  };

  String toJsonString() => jsonEncode(toJson());

  factory ImportedQuestion.fromJson(Map<String, dynamic> json) {
    final dynamic rawOptions = json['options'];
    final dynamic rawCorrect = json['correct_answer'];
    return ImportedQuestion(
      questionId: (json['question_id'] ?? '').toString().trim(),
      type: importedQuestionTypeFromString((json['type'] ?? '').toString()),
      questionText: (json['question_text'] ?? '').toString(),
      questionTextLatex: (json['question_text_latex'] ?? '').toString(),
      options: rawOptions is List
          ? rawOptions
                .whereType<Map>()
                .map(
                  (Map<dynamic, dynamic> e) =>
                      ImportOption.fromJson(Map<String, dynamic>.from(e)),
                )
                .toList()
          : const <ImportOption>[],
      optionsLatex: json['options_latex'] is Map
          ? (json['options_latex'] as Map<dynamic, dynamic>).map(
              (dynamic k, dynamic v) =>
                  MapEntry(k.toString().trim().toUpperCase(), v.toString()),
            )
          : const <String, String>{},
      correctAnswer: rawCorrect is Map
          ? ImportCorrectAnswer.fromJson(Map<String, dynamic>.from(rawCorrect))
          : const ImportCorrectAnswer(),
      subject: (json['subject'] ?? '').toString(),
      chapter: (json['chapter'] ?? '').toString(),
      difficulty: (json['difficulty'] ?? '').toString(),
      aiConfidence:
          double.tryParse((json['ai_confidence'] ?? '').toString()) ?? 0.0,
      publishRiskScore:
          double.tryParse((json['publish_risk_score'] ?? '').toString()) ?? 0.0,
      answerFillSource: (json['answer_fill_source'] ?? 'manual').toString(),
      validationStatus: importValidationStatusFromString(
        (json['validation_status'] ?? '').toString(),
      ),
      validationErrors: (json['validation_errors'] is List)
          ? (json['validation_errors'] as List<dynamic>)
                .map((dynamic e) => e.toString().trim())
                .where((String e) => e.isNotEmpty)
                .toList()
          : const <String>[],
    );
  }
}
