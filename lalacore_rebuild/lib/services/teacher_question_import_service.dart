import 'dart:convert';

import '../models/import_question_models.dart';
import '../services/ai_engine_service.dart';
import '../utils/latex_support.dart';

class TeacherQuestionImportService {
  TeacherQuestionImportService({AiEngineService? aiService})
    : _aiService = aiService;

  final AiEngineService? _aiService;
  final Map<String, Map<String, dynamic>> _aiValidationCache =
      <String, Map<String, dynamic>>{};

  static final RegExp _questionStartRegex = RegExp(
    r'^\s*(?:q(?:uestion)?\s*)?\d+\s*[\).:\-]\s*',
    caseSensitive: false,
  );
  static final RegExp _optionStartRegex = RegExp(
    r'^\s*(?:\(?([A-Za-z]|[1-9])\)?[\).:\-])\s*(.+)$',
  );
  static final RegExp _answerLineRegex = RegExp(
    r'^\s*(?:ans(?:wer)?|correct(?:\s*answer)?)\s*[:\-]\s*(.+)$',
    caseSensitive: false,
  );
  static final RegExp _numericTokenRegex = RegExp(
    r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?',
  );
  static final RegExp _inlineOptionTokenRegex = RegExp(
    r'\(([A-Da-d]|[1-4])\)\s*',
  );

  ImportedQuestionType lc9DetectQuestionType(
    String sectionText,
    String questionText,
  ) {
    final String bag = '${sectionText.trim()} ${questionText.trim()}'
        .toLowerCase();
    const List<String> multiKeywords = <String>[
      'select all correct',
      'more than one correct',
      'more than one option is correct',
      'multiple options are correct',
      'one or more options may be correct',
      'one or more options are correct',
      'select one or more',
      'select all that apply',
      'which of the following are correct',
      'which of the following statements are correct',
      'multiple correct',
      'multiple correct type questions',
      'multi correct',
    ];
    const List<String> numericalKeywords = <String>[
      'integer type',
      'numerical answer type',
      'enter the correct value',
      'answer in',
      'answer upto',
      'answer up to',
      'numerical value',
      'decimal places',
    ];
    for (final String key in multiKeywords) {
      if (bag.contains(key)) {
        return ImportedQuestionType.mcqMulti;
      }
    }
    for (final String key in numericalKeywords) {
      if (bag.contains(key)) {
        return ImportedQuestionType.numerical;
      }
    }
    return ImportedQuestionType.mcqSingle;
  }

  List<ImportedQuestion> lc9ParseQuestions(
    String rawText, {
    String subject = '',
    String chapter = '',
    String difficulty = '',
  }) {
    final String normalizedRaw = _normalizeRawOcrWithLcIieHeuristics(rawText);
    final List<String> lines = _explodeNormalizedLines(normalizedRaw);
    final List<String> compactAnswerTokens = _extractCompactAnswerTokensFromKey(
      normalizedRaw,
    );
    if (lines.isEmpty) {
      return <ImportedQuestion>[];
    }

    String activeInstruction = '';
    _ImportDraftBuilder? current;
    final List<_ImportDraftBuilder> blocks = <_ImportDraftBuilder>[];
    bool inCompactAnswerKeyMode = false;

    void flushCurrent() {
      if (current == null) {
        return;
      }
      if (current!.questionText().trim().isNotEmpty) {
        blocks.add(current!);
      }
      current = null;
    }

    for (final String line in lines) {
      if (_isCompactAnswerKeyMarkerLine(line)) {
        flushCurrent();
        inCompactAnswerKeyMode = true;
        activeInstruction = line;
        continue;
      }
      if (inCompactAnswerKeyMode) {
        continue;
      }
      if (current == null && _looksLikeInstructionLine(line)) {
        activeInstruction = line;
        continue;
      }
      if (_questionStartRegex.hasMatch(line)) {
        flushCurrent();
        current = _ImportDraftBuilder(
          questionText: _questionStartRegex.firstMatch(line) == null
              ? line
              : line.replaceFirst(_questionStartRegex, '').trim(),
          sectionInstruction: activeInstruction,
        );
        continue;
      }
      if (current == null) {
        if (_looksLikeInstructionLine(line)) {
          activeInstruction = line;
        }
        continue;
      }

      final Match? ansMatch = _answerLineRegex.firstMatch(line);
      if (ansMatch != null) {
        current!.answerHint = (ansMatch.group(1) ?? '').trim();
        current!.closeOption();
        continue;
      }

      final Match? optMatch = _optionStartRegex.firstMatch(line);
      if (optMatch != null) {
        final String label = _normalizeOptionLabel(optMatch.group(1) ?? '');
        final String text = (optMatch.group(2) ?? '').trim();
        if (label.isNotEmpty && text.isNotEmpty) {
          current!.startOption(label, text);
          continue;
        }
      }

      if (current!.hasOpenOption && !_questionStartRegex.hasMatch(line)) {
        current!.appendOption(line);
      } else {
        current!.appendQuestionText(line);
      }
    }
    flushCurrent();

    final List<ImportedQuestion> out = <ImportedQuestion>[];
    int compactAnswerCursor = 0;
    for (int i = 0; i < blocks.length; i++) {
      final _ImportDraftBuilder block = blocks[i];
      String questionText = block.questionText();
      List<ImportOption> options = _expandInlineFragmentsFromOptions(
        block.optionsAsList(),
      );
      if (options.isEmpty) {
        final _QuestionOptionExtraction extracted =
            _extractOptionsFromQuestionText(questionText);
        if (extracted.options.isNotEmpty) {
          questionText = extracted.questionText;
          options = extracted.options;
        }
      }
      if (_isLikelyAnswerKeyArtifact(
        questionText: questionText,
        sectionInstruction: block.sectionInstruction,
        options: options,
      )) {
        continue;
      }
      if (_isLikelyBrokenMcqFragment(
        questionText: questionText,
        options: options,
      )) {
        continue;
      }
      String resolvedAnswerHint = block.answerHint;
      if (resolvedAnswerHint.trim().isEmpty && compactAnswerTokens.isNotEmpty) {
        final int probeLimit = (compactAnswerCursor + 4).clamp(
          0,
          compactAnswerTokens.length,
        );
        for (int probe = compactAnswerCursor; probe < probeLimit; probe++) {
          final String inferred = _normalizeInferredAnswerHint(
            compactAnswerTokens[probe],
            options: options,
          );
          if (inferred.isEmpty) {
            continue;
          }
          resolvedAnswerHint = inferred;
          compactAnswerCursor = probe + 1;
          break;
        }
      }
      final _ParsedAnswerHint answerHint = _parseAnswerHint(
        answerHint: resolvedAnswerHint,
        options: options,
      );
      ImportedQuestionType type = lc9DetectQuestionType(
        block.sectionInstruction,
        questionText,
      );

      if (options.isEmpty) {
        final bool hasNumericCue =
            answerHint.numerical != null ||
            _looksNumericPrompt(questionText) ||
            _looksBlankAnswerPrompt(questionText);
        if (hasNumericCue) {
          type = ImportedQuestionType.numerical;
        }
      }
      if (answerHint.labels.isNotEmpty && options.isNotEmpty) {
        type =
            type == ImportedQuestionType.mcqMulti ||
                answerHint.labels.length > 1
            ? ImportedQuestionType.mcqMulti
            : ImportedQuestionType.mcqSingle;
      } else if (answerHint.numerical != null) {
        type = ImportedQuestionType.numerical;
      }

      final ImportCorrectAnswer correct = _buildCorrectAnswer(
        type: type,
        options: options,
        answerHint: answerHint,
      );
      final ImportedQuestion drafted = ImportedQuestion(
        questionId: 'imp_q_${i + 1}',
        type: type,
        questionText: questionText,
        questionTextLatex: _canonicalLatex(questionText),
        options: type == ImportedQuestionType.numerical
            ? const <ImportOption>[]
            : options,
        optionsLatex: <String, String>{
          for (final ImportOption option in options)
            option.label.toUpperCase(): _canonicalLatex(option.text),
        },
        correctAnswer: correct,
        subject: subject,
        chapter: chapter,
        difficulty: difficulty,
        aiConfidence: 0.0,
        publishRiskScore: 0.45,
        answerFillSource: resolvedAnswerHint.trim().isEmpty
            ? 'manual'
            : 'inline_hint',
        validationStatus: ImportValidationStatus.review,
        validationErrors: const <String>[],
      );
      out.add(_validateAndDecorate(drafted));
    }
    return out;
  }

  Future<List<ImportedQuestion>> runAiValidationBatch({
    required List<ImportedQuestion> questions,
    required String userId,
    String chatPrefix = 'lc9_import',
  }) async {
    final List<ImportedQuestion> out = <ImportedQuestion>[];
    for (int i = 0; i < questions.length; i++) {
      out.add(
        await runAiStructureValidation(
          question: questions[i],
          userId: userId,
          chatId: '${chatPrefix}_${i + 1}',
        ),
      );
    }
    return out;
  }

  Future<ImportedQuestion> runAiStructureValidation({
    required ImportedQuestion question,
    required String userId,
    required String chatId,
  }) async {
    if (_aiService == null) {
      return question;
    }
    final String cacheKey = question.toJsonString();
    final Map<String, dynamic>? cached = _aiValidationCache[cacheKey];
    final Map<String, dynamic> aiPayload =
        cached ??
        await _requestAiValidation(
          question: question,
          userId: userId,
          chatId: chatId,
        );
    _aiValidationCache[cacheKey] = aiPayload;

    final double confidence =
        double.tryParse((aiPayload['confidence_score'] ?? '').toString()) ??
        question.aiConfidence;
    final bool structureValid = aiPayload['structure_valid'] != false;
    final String suggestedTypeRaw = (aiPayload['suggested_type'] ?? '')
        .toString();
    final ImportedQuestionType suggestedType = suggestedTypeRaw.trim().isEmpty
        ? question.type
        : importedQuestionTypeFromString(suggestedTypeRaw);
    final String suggestedDifficulty = (aiPayload['suggested_difficulty'] ?? '')
        .toString()
        .trim();
    final List<String> aiIssues = (aiPayload['issues_detected'] is List)
        ? (aiPayload['issues_detected'] as List<dynamic>)
              .map((dynamic e) => e.toString().trim())
              .where((String e) => e.isNotEmpty)
              .toList()
        : const <String>[];

    final ImportedQuestion validated = _validateAndDecorate(
      question.copyWith(
        aiConfidence: confidence,
        difficulty: suggestedDifficulty.isEmpty
            ? question.difficulty
            : suggestedDifficulty,
      ),
    );

    final List<String> errors = <String>[...validated.validationErrors];
    ImportValidationStatus status = validated.validationStatus;

    if (!structureValid) {
      errors.add('AI flagged structure as invalid.');
      status = ImportValidationStatus.invalid;
    }
    if (suggestedType != question.type) {
      errors.add(
        'AI suggested ${importedQuestionTypeToString(suggestedType)} instead of '
        '${importedQuestionTypeToString(question.type)}.',
      );
      if (status == ImportValidationStatus.valid) {
        status = ImportValidationStatus.review;
      }
    }
    if (confidence < 0.6) {
      errors.add(
        'AI confidence ${(confidence * 100).toStringAsFixed(1)}% is below threshold.',
      );
      if (status == ImportValidationStatus.valid) {
        status = ImportValidationStatus.review;
      }
    }
    errors.addAll(aiIssues);

    return validated.copyWith(
      validationStatus: status,
      validationErrors: errors.toSet().toList(),
    );
  }

  ImportedQuestion validateQuestion(ImportedQuestion question) {
    return _validateAndDecorate(question);
  }

  ImportedQuestion updateQuestionType(
    ImportedQuestion question,
    ImportedQuestionType nextType,
  ) {
    if (nextType == question.type) {
      return _validateAndDecorate(question);
    }
    ImportCorrectAnswer correct = question.correctAnswer;
    List<ImportOption> options = question.options;
    if (nextType == ImportedQuestionType.numerical) {
      options = <ImportOption>[];
      correct = correct.copyWith(
        multiple: const <String>[],
        clearSingle: true,
        numerical: correct.numerical ?? '',
      );
    } else {
      if (options.isEmpty) {
        options = <ImportOption>[
          const ImportOption(label: 'A', text: ''),
          const ImportOption(label: 'B', text: ''),
          const ImportOption(label: 'C', text: ''),
          const ImportOption(label: 'D', text: ''),
        ];
      }
      if (nextType == ImportedQuestionType.mcqSingle) {
        final String selected =
            correct.single ??
            (correct.multiple.isNotEmpty ? correct.multiple.first : '');
        correct = correct.copyWith(
          single: selected,
          multiple: selected.isEmpty ? const <String>[] : <String>[selected],
          clearNumerical: true,
        );
      } else {
        final List<String> selected = correct.multiple.isNotEmpty
            ? List<String>.from(correct.multiple)
            : (correct.single == null || correct.single!.trim().isEmpty
                  ? <String>[]
                  : <String>[correct.single!.trim()]);
        correct = correct.copyWith(
          single: selected.isNotEmpty ? selected.first : null,
          multiple: selected,
          clearNumerical: true,
        );
      }
    }
    return _validateAndDecorate(
      question.copyWith(
        type: nextType,
        options: options,
        correctAnswer: correct,
      ),
    );
  }

  ImportedQuestion _validateAndDecorate(ImportedQuestion question) {
    final List<String> errors = <String>[];
    final String qText = question.questionText.trim();
    if (qText.isEmpty) {
      errors.add('Question text cannot be empty.');
    }

    final Set<String> duplicateTracker = <String>{};
    final List<ImportOption> options = question.options
        .map(
          (ImportOption e) => e.copyWith(
            label: e.label.trim().isEmpty ? '' : e.label.trim().toUpperCase(),
            text: e.text.trim(),
          ),
        )
        .toList();
    for (final ImportOption option in options) {
      if (option.text.isEmpty) {
        errors.add('Option ${option.label} cannot be empty.');
      }
      final String key = option.text.toLowerCase();
      if (key.isNotEmpty && !duplicateTracker.add(key)) {
        errors.add('Duplicate option text is invalid.');
      }
    }

    ImportValidationStatus status = ImportValidationStatus.valid;
    final ImportCorrectAnswer correct = question.correctAnswer;
    switch (question.type) {
      case ImportedQuestionType.mcqSingle:
        final String single = (correct.single ?? '').trim().toUpperCase();
        if (options.length < 2) {
          if (single.isEmpty) {
            errors.add('MCQ_SINGLE requires at least 2 options.');
          } else {
            status = ImportValidationStatus.review;
            errors.add(
              'Options not extracted from OCR; review before publish.',
            );
          }
        }
        if (single.isEmpty) {
          errors.add('MCQ_SINGLE requires exactly one correct answer.');
        } else if (options.isNotEmpty &&
            !options.any((ImportOption e) => e.label == single)) {
          errors.add('Correct single answer label does not match options.');
        }
        break;
      case ImportedQuestionType.mcqMulti:
        if (options.length < 2) {
          errors.add('MCQ_MULTI requires at least 2 options.');
        }
        final List<String> multiple = correct.multiple
            .map((String e) => e.trim().toUpperCase())
            .where((String e) => e.isNotEmpty)
            .toList();
        if (multiple.isEmpty) {
          errors.add('MCQ_MULTI requires one or more correct answers.');
        } else {
          for (final String label in multiple) {
            if (!options.any((ImportOption e) => e.label == label)) {
              errors.add('MCQ_MULTI contains invalid answer label "$label".');
            }
          }
          if (multiple.length == 1) {
            status = ImportValidationStatus.review;
            errors.add(
              'MCQ_MULTI has only one correct option; review required.',
            );
          }
        }
        break;
      case ImportedQuestionType.numerical:
        if (options.isNotEmpty) {
          errors.add('NUMERICAL questions must not contain options.');
        }
        final String numeric = (correct.numerical ?? '').trim();
        if (numeric.isEmpty) {
          status = ImportValidationStatus.review;
          errors.add('Numerical answer not detected; set during review.');
          break;
        }
        final Match? numericMatch = _numericTokenRegex.firstMatch(numeric);
        if (numericMatch == null) {
          errors.add('NUMERICAL answer must be a valid number.');
        }
        break;
    }

    if (question.type != ImportedQuestionType.numerical &&
        options.isEmpty &&
        qText.isNotEmpty) {
      final bool hasAnswer =
          (correct.single ?? '').trim().isNotEmpty ||
          correct.multiple.any((String e) => e.trim().isNotEmpty);
      if (hasAnswer) {
        status = ImportValidationStatus.review;
        errors.add('Options not extracted from OCR; review before publish.');
      } else {
        errors.add('No options detected for non-numerical question.');
      }
    }

    if (errors.isNotEmpty) {
      final bool hardFail = errors.any(
        (String e) =>
            e.contains('cannot') ||
            e.contains('requires') ||
            e.contains('must') ||
            e.contains('invalid') ||
            e.contains('duplicate'),
      );
      status = hardFail
          ? ImportValidationStatus.invalid
          : ImportValidationStatus.review;
    }

    final ImportValidationStatus finalStatus = status;
    final double publishRisk = _estimatePublishRisk(
      status: finalStatus,
      aiConfidence: question.aiConfidence,
      issues: errors,
      answerFillSource: question.answerFillSource,
    );
    return question.copyWith(
      options: options,
      questionTextLatex: _canonicalLatex(question.questionText),
      optionsLatex: <String, String>{
        for (final ImportOption option in options)
          option.label.toUpperCase(): _canonicalLatex(option.text),
      },
      validationStatus: finalStatus,
      validationErrors: errors.toSet().toList(),
      publishRiskScore: publishRisk,
      answerFillSource: _normalizeAnswerFillSource(question.answerFillSource),
    );
  }

  String _normalizeAnswerFillSource(String raw) {
    final String token = raw.trim().toLowerCase();
    if (token.isEmpty) {
      return 'manual';
    }
    if (token == 'web_verified' ||
        token == 'inline_hint' ||
        token == 'global_answer_key') {
      return token;
    }
    return 'manual';
  }

  String _canonicalLatex(String text) {
    final String raw = text.trim();
    if (raw.isEmpty) {
      return '';
    }
    return normalizeUniversalLatex(raw);
  }

  double _estimatePublishRisk({
    required ImportValidationStatus status,
    required double aiConfidence,
    required List<String> issues,
    required String answerFillSource,
  }) {
    final double conf = aiConfidence.clamp(0.0, 1.0);
    double base = 0.08;
    if (status == ImportValidationStatus.review) {
      base = 0.36;
    } else if (status == ImportValidationStatus.invalid) {
      base = 0.86;
    }
    final int criticalIssueCount = issues.where((String e) {
      final String token = e.toLowerCase();
      return token.contains('cannot') ||
          token.contains('requires') ||
          token.contains('must') ||
          token.contains('invalid') ||
          token.contains('duplicate') ||
          token.contains('missing');
    }).length;
    final double sourcePenalty =
        answerFillSource.trim().toLowerCase() == 'manual' ? 0.1 : 0.04;
    final double risk =
        base + (1.0 - conf) * 0.48 + (criticalIssueCount * 0.2) + sourcePenalty;
    return risk.clamp(0.0, 1.0).toDouble();
  }

  Future<Map<String, dynamic>> _requestAiValidation({
    required ImportedQuestion question,
    required String userId,
    required String chatId,
  }) async {
    final String prompt =
        '''
Validate imported exam question structure and return strict JSON only.

Required output schema:
{
  "confidence_score": 0.0,
  "structure_valid": true,
  "suggested_type": "MCQ_SINGLE | MCQ_MULTI | NUMERICAL",
  "suggested_difficulty": "Easy | Medium | Hard",
  "issues_detected": []
}

Question:
${jsonEncode(question.toJson())}
''';
    final Map<String, dynamic> response = await _aiService!.sendChat(
      prompt: prompt,
      userId: userId,
      chatId: chatId,
      function: 'question_import_validator',
      responseStyle: 'strict_json',
      enablePersona: false,
      card: <String, dynamic>{
        'task': 'lc9_structure_validation',
        'question_type': importedQuestionTypeToString(question.type),
      },
    );
    final List<dynamic> candidates = <dynamic>[
      response['data'],
      response['answer'],
      response['explanation'],
      response['raw'],
      '${response['answer'] ?? ''}\n${response['explanation'] ?? ''}',
    ];
    for (final dynamic candidate in candidates) {
      final Map<String, dynamic>? parsed = _tryDecodeJsonMap(candidate);
      if (parsed != null && parsed.containsKey('confidence_score')) {
        return parsed;
      }
    }
    return <String, dynamic>{
      'confidence_score': question.aiConfidence,
      'structure_valid': true,
      'suggested_type': importedQuestionTypeToString(question.type),
      'suggested_difficulty': question.difficulty,
      'issues_detected': <String>[],
    };
  }

  Map<String, dynamic>? _tryDecodeJsonMap(dynamic raw) {
    if (raw is Map<String, dynamic>) {
      return raw;
    }
    if (raw is Map) {
      return Map<String, dynamic>.from(raw);
    }
    final String text = (raw ?? '').toString().trim();
    if (text.isEmpty) {
      return null;
    }
    try {
      final dynamic decoded = jsonDecode(text);
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
      if (decoded is Map) {
        return Map<String, dynamic>.from(decoded);
      }
    } catch (_) {}
    final int a = text.indexOf('{');
    final int b = text.lastIndexOf('}');
    if (a >= 0 && b > a) {
      final String cut = text.substring(a, b + 1);
      try {
        final dynamic decoded = jsonDecode(cut);
        if (decoded is Map<String, dynamic>) {
          return decoded;
        }
        if (decoded is Map) {
          return Map<String, dynamic>.from(decoded);
        }
      } catch (_) {}
    }
    return null;
  }

  ImportCorrectAnswer _buildCorrectAnswer({
    required ImportedQuestionType type,
    required List<ImportOption> options,
    required _ParsedAnswerHint answerHint,
  }) {
    switch (type) {
      case ImportedQuestionType.numerical:
        return ImportCorrectAnswer(
          numerical: answerHint.numerical ?? '',
          multiple: const <String>[],
          single: null,
        );
      case ImportedQuestionType.mcqMulti:
        final List<String> labels = answerHint.labels.isNotEmpty
            ? answerHint.labels
            : <String>[];
        return ImportCorrectAnswer(
          single: labels.isEmpty ? null : labels.first,
          multiple: labels,
          numerical: null,
        );
      case ImportedQuestionType.mcqSingle:
        final String single = answerHint.labels.isNotEmpty
            ? answerHint.labels.first
            : '';
        return ImportCorrectAnswer(
          single: single.isEmpty ? null : single,
          multiple: single.isEmpty ? const <String>[] : <String>[single],
          numerical: null,
        );
    }
  }

  _ParsedAnswerHint _parseAnswerHint({
    required String answerHint,
    required List<ImportOption> options,
  }) {
    final String raw = answerHint.trim();
    if (raw.isEmpty) {
      return const _ParsedAnswerHint(labels: <String>[], numerical: null);
    }
    final String compact = raw.replaceAll(RegExp(r'\s+'), ' ').trim();
    final List<String> pieces = compact
        .split(RegExp(r'[,/;|]+'))
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .toList();
    final Set<String> labels = <String>{};
    for (final String piece in pieces) {
      final String token = piece.toUpperCase();
      final String label = _normalizeOptionLabel(token);
      if (label.isNotEmpty) {
        labels.add(label);
        continue;
      }
      for (final ImportOption option in options) {
        if (option.text.toLowerCase() == piece.toLowerCase()) {
          labels.add(option.label);
        }
      }
    }
    final Match? number = _numericTokenRegex.firstMatch(compact);
    final String? numerical = number == null ? null : number.group(0);
    return _ParsedAnswerHint(labels: labels.toList(), numerical: numerical);
  }

  bool _looksLikeInstructionLine(String line) {
    final String lower = line.toLowerCase();
    return lower.startsWith('section') ||
        lower.contains('choose the correct option') ||
        lower.contains('single correct type questions') ||
        lower.contains('multiple correct type questions') ||
        lower.contains('subjective type questions') ||
        lower.contains('select all correct') ||
        lower.contains('more than one correct') ||
        lower.contains('numerical answer type') ||
        lower.contains('integer type') ||
        lower.contains('one or more options may be correct');
  }

  bool _looksNumericPrompt(String questionText) {
    final String lower = questionText.toLowerCase();
    return lower.contains('integer') ||
        lower.contains('numerical') ||
        lower.contains('enter value') ||
        lower.contains('decimal places') ||
        lower.contains('answer in');
  }

  bool _looksBlankAnswerPrompt(String questionText) {
    return RegExp(r'_{3,}').hasMatch(questionText) ||
        RegExp(r'\bblank\b', caseSensitive: false).hasMatch(questionText) ||
        RegExp(r'\bfill in\b', caseSensitive: false).hasMatch(questionText);
  }

  bool _isCompactAnswerKeyMarkerLine(String line) {
    final String lower = line.trim().toLowerCase();
    if (lower.isEmpty) {
      return false;
    }
    return lower == 'answer key' ||
        lower.startsWith('que.') ||
        lower.startsWith('ans.') ||
        lower.startsWith('ans .') ||
        (lower.startsWith('que') && lower.contains('ans'));
  }

  bool _isLikelyBrokenMcqFragment({
    required String questionText,
    required List<ImportOption> options,
  }) {
    if (options.isNotEmpty) {
      return false;
    }
    final String text = questionText.trim();
    if (text.isEmpty) {
      return true;
    }
    final int markerCount = RegExp(
      r'\(([A-Da-d]|[1-4])\)',
    ).allMatches(text).length;
    if (markerCount == 1 && !RegExp(r'_{3,}').hasMatch(text)) {
      return true;
    }
    if (text.contains('SR01-') &&
        RegExp(r'^[0-9\sA-Za-z+\-=().,;:/]*$').hasMatch(text) &&
        markerCount <= 1 &&
        text.length < 55) {
      return true;
    }
    return false;
  }

  List<String> _explodeNormalizedLines(String normalizedRaw) {
    final List<String> out = <String>[];
    final List<String> baseLines = normalizedRaw
        .replaceAll('\r\n', '\n')
        .replaceAll('\r', '\n')
        .split('\n');
    for (final String rawLine in baseLines) {
      final String clean = _normalizeLine(rawLine);
      if (clean.isEmpty) {
        continue;
      }
      final List<String> exploded = _splitInlineOptions(clean);
      for (final String entry in exploded) {
        final String row = _normalizeLine(entry);
        if (row.isNotEmpty) {
          out.add(row);
        }
      }
    }
    return out;
  }

  List<String> _splitInlineOptions(String line) {
    final String compact = line.replaceAll(RegExp(r'\s+'), ' ').trim();
    final List<RegExpMatch> matches = _inlineOptionTokenRegex
        .allMatches(compact)
        .toList();
    if (matches.length < 2) {
      return <String>[line];
    }
    final List<String> out = <String>[];
    final int firstStart = matches.first.start;
    if (firstStart > 0) {
      final String questionPart = compact.substring(0, firstStart).trim();
      if (questionPart.isNotEmpty) {
        out.add(questionPart);
      }
    }
    for (int i = 0; i < matches.length; i++) {
      final RegExpMatch current = matches[i];
      final int start = current.end;
      final int end = i + 1 < matches.length
          ? matches[i + 1].start
          : compact.length;
      final String rawLabel = (current.group(1) ?? '').toUpperCase();
      final String label = _normalizeOptionLabel(rawLabel);
      final String text = compact.substring(start, end).trim();
      if (label.isEmpty || text.isEmpty) {
        continue;
      }
      out.add('$label) $text');
    }
    return out.isEmpty ? <String>[line] : out;
  }

  _QuestionOptionExtraction _extractOptionsFromQuestionText(
    String questionText,
  ) {
    final List<String> pieces = _splitInlineOptions(questionText);
    if (pieces.length <= 1) {
      return _QuestionOptionExtraction(
        questionText: questionText,
        options: const <ImportOption>[],
      );
    }
    final String stem = pieces.first.trim();
    final List<ImportOption> options = <ImportOption>[];
    for (final String part in pieces.skip(1)) {
      final Match? opt = _optionStartRegex.firstMatch(part);
      if (opt == null) {
        continue;
      }
      final String label = _normalizeOptionLabel(opt.group(1) ?? '');
      final String text = (opt.group(2) ?? '').trim();
      if (label.isEmpty || text.isEmpty) {
        continue;
      }
      options.add(ImportOption(label: label, text: text));
    }
    return _QuestionOptionExtraction(
      questionText: stem.isEmpty ? questionText : stem,
      options: _expandInlineFragmentsFromOptions(options),
    );
  }

  List<ImportOption> _expandInlineFragmentsFromOptions(
    List<ImportOption> options,
  ) {
    if (options.isEmpty) {
      return options;
    }
    final Map<String, StringBuffer> merged = <String, StringBuffer>{};
    for (final ImportOption option in options) {
      final List<String> split = _splitInlineOptions(
        '${option.label}) ${option.text}',
      );
      bool consumed = false;
      for (final String part in split) {
        final Match? opt = _optionStartRegex.firstMatch(part);
        if (opt == null) {
          continue;
        }
        final String label = _normalizeOptionLabel(opt.group(1) ?? '');
        final String text = (opt.group(2) ?? '').trim();
        if (label.isEmpty || text.isEmpty) {
          continue;
        }
        consumed = true;
        final StringBuffer buf = merged.putIfAbsent(
          label,
          () => StringBuffer(),
        );
        if (buf.isNotEmpty) {
          buf.write(' ');
        }
        buf.write(text);
      }
      if (consumed) {
        continue;
      }
      final String normalized = _normalizeOptionLabel(option.label);
      if (normalized.isEmpty || option.text.trim().isEmpty) {
        continue;
      }
      final StringBuffer buf = merged.putIfAbsent(
        normalized,
        () => StringBuffer(),
      );
      if (buf.isNotEmpty) {
        buf.write(' ');
      }
      buf.write(option.text.trim());
    }
    final List<String> labels = merged.keys.toList()..sort();
    return labels
        .map(
          (String label) => ImportOption(
            label: label,
            text: merged[label]!.toString().trim(),
          ),
        )
        .where((ImportOption e) => e.text.isNotEmpty)
        .toList();
  }

  bool _isLikelyAnswerKeyArtifact({
    required String questionText,
    required String sectionInstruction,
    required List<ImportOption> options,
  }) {
    final String text = questionText.trim();
    if (text.isEmpty) {
      return true;
    }
    final String lower = text.toLowerCase();
    final String sectionLower = sectionInstruction.toLowerCase();
    if (lower.contains('answer key')) {
      return true;
    }
    if (lower.contains('que.') && lower.contains('ans.')) {
      return true;
    }
    if (sectionLower.contains('answer key') ||
        sectionLower.contains('exercise (o-1)') && lower.startsWith('que.')) {
      return true;
    }
    if (options.isEmpty &&
        RegExp(r'^(?:\d+(?:\.\d+)?\s+){3,}\d+(?:\.\d+)?\s*$').hasMatch(text)) {
      return true;
    }
    if (options.isEmpty &&
        RegExp(r'^(?:\d+\.\s*[A-Za-z0-9.]+\s*){2,}$').hasMatch(text)) {
      return true;
    }
    if (options.isEmpty &&
        lower.contains('exercise -') &&
        !text.contains('?') &&
        text.length < 140) {
      return true;
    }
    return false;
  }

  List<String> _extractCompactAnswerTokensFromKey(String normalizedRaw) {
    final String lower = normalizedRaw.toLowerCase();
    int start = lower.lastIndexOf('exercise (o-1)');
    if (start < 0) {
      start = lower.lastIndexOf('answer key');
    }
    if (start < 0) {
      return const <String>[];
    }
    final String keySlice = normalizedRaw.substring(start);
    final List<String> lines = keySlice
        .replaceAll('\r\n', '\n')
        .replaceAll('\r', '\n')
        .split('\n')
        .map((String e) => e.replaceAll(RegExp(r'\s+'), ' ').trim())
        .where((String e) => e.isNotEmpty)
        .toList();
    final List<String> out = <String>[];
    for (final String line in lines) {
      final String lowerLine = line.toLowerCase();
      if (lowerLine.startsWith('ans.')) {
        final String payload = line.substring(4).trim();
        out.addAll(_splitAnswerPayloadTokens(payload));
        continue;
      }
      final Iterable<RegExpMatch> numberedPairs = RegExp(
        r'\b\d+\.\s*([A-Za-z0-9.,]+)',
      ).allMatches(line);
      final List<String> captured = numberedPairs
          .map((RegExpMatch m) => (m.group(1) ?? '').trim())
          .where((String e) => e.isNotEmpty)
          .toList();
      if (captured.length >= 2) {
        out.addAll(captured);
      }
    }
    return out;
  }

  List<String> _splitAnswerPayloadTokens(String payload) {
    return payload
        .split(RegExp(r'\s+'))
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .where(
          (String token) =>
              RegExp(r'^[A-Za-z0-9][A-Za-z0-9,.\-]*$').hasMatch(token),
        )
        .toList();
  }

  String _normalizeInferredAnswerHint(
    String token, {
    required List<ImportOption> options,
  }) {
    final String raw = token.trim().toUpperCase();
    if (raw.isEmpty) {
      return '';
    }
    if (RegExp(r'^[A-D](?:,[A-D])*$').hasMatch(raw)) {
      return raw;
    }
    if (RegExp(r'^[1-4](?:,[1-4])*$').hasMatch(raw)) {
      final List<String> labels = raw
          .split(',')
          .map((String part) => _normalizeOptionLabel(part))
          .where((String e) => e.isNotEmpty)
          .toList();
      if (labels.isNotEmpty) {
        return labels.join(', ');
      }
    }
    if (options.isEmpty) {
      final String numeric = _numericTokenRegex.firstMatch(raw)?.group(0) ?? '';
      if (numeric.isNotEmpty) {
        return numeric;
      }
    }
    return '';
  }

  String _normalizeLine(String raw) {
    return raw
        .replaceAll('\t', ' ')
        .replaceAll(RegExp(r'[ ]{2,}'), ' ')
        .trimRight();
  }

  String _normalizeRawOcrWithLcIieHeuristics(String raw) {
    String out = _normalizeSymbolFontArtifacts(raw);
    out = out.replaceAll('×', '*').replaceAll('÷', '/').replaceAll('−', '-');
    out = out.replaceAllMapped(RegExp(r'(?<=\d)[Oo](?=\d)'), (_) => '0');
    out = out.replaceAllMapped(RegExp(r'(?<=\d)[lI](?=\d)'), (_) => '1');
    out = out.replaceAllMapped(RegExp(r'(?<=\d)S(?=\d)'), (_) => '5');
    out = _normalizeLikelySquaredSymbols(out);
    out = out.replaceAllMapped(
      RegExp(r'\brn(?=[a-z])', caseSensitive: false),
      (_) => 'm',
    );
    return out;
  }

  String _normalizeSymbolFontArtifacts(String raw) {
    String out = raw;
    const Map<String, String> map = <String, String>{
      '\uf022': '∅',
      '\uf03c': '<',
      '\uf03e': '>',
      '\uf061': 'α',
      '\uf062': 'β',
      '\uf066': '∅',
      '\uf07b': '{',
      '\uf07d': '}',
      '\uf0a3': '≤',
      '\uf0b3': '≥',
      '\uf0b4': '×',
      '\uf0b9': '≠',
      '\uf0c6': '∀',
      '\uf0c7': '∩',
      '\uf0c8': '∪',
      '\uf0ce': '∈',
      '\uf0cf': '∉',
      '\uf0db': '⇔',
      '\uf0e5': '∑',
      '\uf0ec': '{',
      '\uf0ed': '{',
      '\uf0ee': '{',
      '\uf0fc': '}',
      '\uf0fd': '}',
      '\uf0fe': '}',
    };
    map.forEach((String from, String to) {
      out = out.replaceAll(from, to);
    });
    return out;
  }

  String _normalizeLikelySquaredSymbols(String raw) {
    final RegExp token = RegExp(r'\b([a-z])\s*2\b');
    final StringBuffer out = StringBuffer();
    int cursor = 0;
    for (final Match m in token.allMatches(raw)) {
      final int start = m.start;
      final int end = m.end;
      final String symbol = m.group(1) ?? '';
      final String? next = _nextNonSpaceChar(raw, end);
      final bool symbolOk = RegExp(r'^[xyzabcmnt]$').hasMatch(symbol);
      final bool nextOk =
          next == null || RegExp(r'[+\-*/=),:\]\}\.;,]').hasMatch(next);
      final bool alreadySquared = next == '^';
      final bool convert = symbolOk && nextOk && !alreadySquared;

      out.write(raw.substring(cursor, start));
      if (convert) {
        out.write('$symbol^2');
      } else {
        out.write(raw.substring(start, end));
      }
      cursor = end;
    }
    out.write(raw.substring(cursor));
    return out.toString();
  }

  String? _nextNonSpaceChar(String text, int indexInclusive) {
    for (int i = indexInclusive; i < text.length; i++) {
      final String ch = text[i];
      if (ch.trim().isNotEmpty) {
        return ch;
      }
    }
    return null;
  }

  String _normalizeOptionLabel(String raw) {
    final String t = raw.trim().toUpperCase();
    if (t.isEmpty) {
      return '';
    }
    if (RegExp(r'^[A-Z]$').hasMatch(t)) {
      return t;
    }
    if (RegExp(r'^[1-9]$').hasMatch(t)) {
      final int n = int.parse(t);
      return String.fromCharCode(64 + n);
    }
    if (t.length >= 2 &&
        RegExp(r'^[A-Z][\).:\-]$').hasMatch(t.substring(0, 2))) {
      return t[0];
    }
    return '';
  }
}

class _ParsedAnswerHint {
  const _ParsedAnswerHint({required this.labels, required this.numerical});

  final List<String> labels;
  final String? numerical;
}

class _QuestionOptionExtraction {
  const _QuestionOptionExtraction({
    required this.questionText,
    required this.options,
  });

  final String questionText;
  final List<ImportOption> options;
}

class _ImportDraftBuilder {
  _ImportDraftBuilder({
    required String questionText,
    required this.sectionInstruction,
  }) {
    _questionLines = <String>[questionText];
  }

  late List<String> _questionLines;
  final String sectionInstruction;
  final Map<String, StringBuffer> _options = <String, StringBuffer>{};
  String? _activeOptionLabel;
  String answerHint = '';

  bool get hasOpenOption => _activeOptionLabel != null;

  void closeOption() {
    _activeOptionLabel = null;
  }

  void startOption(String label, String text) {
    _activeOptionLabel = label;
    _options.putIfAbsent(label, () => StringBuffer()).write(text.trim());
  }

  void appendOption(String text) {
    final String? label = _activeOptionLabel;
    if (label == null) {
      return;
    }
    final String cleaned = text.trim();
    if (cleaned.isEmpty) {
      return;
    }
    final StringBuffer buffer = _options.putIfAbsent(
      label,
      () => StringBuffer(),
    );
    if (buffer.isNotEmpty) {
      buffer.write(' ');
    }
    buffer.write(cleaned);
  }

  void appendQuestionText(String text) {
    final String cleaned = text.trim();
    if (cleaned.isEmpty) {
      return;
    }
    _activeOptionLabel = null;
    _questionLines.add(cleaned);
  }

  List<ImportOption> optionsAsList() {
    final List<String> labels = _options.keys.toList()..sort();
    return labels
        .map(
          (String label) => ImportOption(
            label: label,
            text: (_options[label]?.toString() ?? '').trim(),
          ),
        )
        .where((ImportOption e) => e.text.isNotEmpty)
        .toList();
  }

  String questionText() {
    return _questionLines.join(' ').replaceAll(RegExp(r'\s+'), ' ').trim();
  }
}
