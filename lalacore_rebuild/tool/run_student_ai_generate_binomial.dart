import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:lalacore_rebuild/services/ai_engine_service.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';

class ParsedQuestion {
  ParsedQuestion({
    required this.type,
    required this.question,
    required this.options,
    required this.correctAnswers,
    required this.solution,
    required this.difficulty,
    required this.tags,
  });

  final String type;
  final String question;
  final List<String> options;
  final List<String> correctAnswers;
  final String solution;
  final int difficulty;
  final List<String> tags;
}

void _log(String msg) {
  final String ts = DateTime.now().toIso8601String();
  print('[$ts] $msg');
}

String _aiHardnessLabel(int level) {
  final int clamped = level.clamp(1, 5);
  switch (clamped) {
    case 1:
      return 'Stage 1 • Hard Foundation';
    case 2:
      return 'Stage 2 • JEE Main Hard';
    case 3:
      return 'Stage 3 • JEE Main+ Bridge';
    case 4:
      return 'Stage 4 • JEE Advanced Hard';
    case 5:
    default:
      return 'Stage 5 • Olympiad Trap';
  }
}

Map<String, dynamic> _hardnessProfile(int level) {
  final int clamped = level.clamp(1, 5);
  switch (clamped) {
    case 1:
      return <String, dynamic>{
        'difficulty': 4,
        'trap_intensity': 'high',
        'reasoning_depth': 'high',
        'target_level': 'hard_foundation',
        'strict_hard_mode': true,
      };
    case 2:
      return <String, dynamic>{
        'difficulty': 5,
        'trap_intensity': 'high',
        'reasoning_depth': 'high',
        'target_level': 'jee_main_hard',
        'strict_hard_mode': true,
      };
    case 3:
      return <String, dynamic>{
        'difficulty': 6,
        'trap_intensity': 'high',
        'reasoning_depth': 'high',
        'target_level': 'jee_main_advanced_bridge',
        'strict_hard_mode': true,
      };
    case 4:
      return <String, dynamic>{
        'difficulty': 7,
        'trap_intensity': 'high',
        'reasoning_depth': 'very_high',
        'target_level': 'jee_advanced_hard',
        'strict_hard_mode': true,
      };
    case 5:
    default:
      return <String, dynamic>{
        'difficulty': 8,
        'trap_intensity': 'extreme',
        'reasoning_depth': 'very_high',
        'target_level': 'olympiad_trap',
        'strict_hard_mode': true,
      };
  }
}

Map<String, dynamic> _toMap(dynamic value) {
  if (value is Map<String, dynamic>) {
    return value;
  }
  if (value is Map) {
    return Map<String, dynamic>.from(value);
  }
  return <String, dynamic>{};
}

List<dynamic> _listFromUnknown(dynamic raw) {
  if (raw is List) {
    return raw;
  }
  if (raw is String) {
    final String text = raw.trim();
    if (text.isEmpty) {
      return <dynamic>[];
    }
    try {
      final dynamic decoded = jsonDecode(text);
      if (decoded is List) {
        return decoded;
      }
      if (decoded is Map) {
        final Map<String, dynamic> map = Map<String, dynamic>.from(decoded);
        final dynamic nested =
            map['questions'] ??
            map['questions_json'] ??
            map['quiz_questions'] ??
            map['items'];
        if (nested is List) {
          return nested;
        }
      }
    } catch (_) {}
  }
  if (raw is Map) {
    final dynamic nested =
        raw['questions'] ??
        raw['questions_json'] ??
        raw['quiz_questions'] ??
        raw['items'];
    if (nested is List) {
      return nested;
    }
    if (nested is String) {
      return _listFromUnknown(nested);
    }
  }
  return <dynamic>[];
}

List<dynamic> _extractAiQuestionRawList(Map<String, dynamic> response) {
  final List<dynamic> direct = _listFromUnknown(
    response['questions_json'] ??
        response['questions'] ??
        response['quiz_questions'] ??
        response['items'],
  );
  if (direct.isNotEmpty) {
    return direct;
  }
  final Map<String, dynamic> data = _toMap(response['data']);
  final List<dynamic> fromData = _listFromUnknown(
    data['questions_json'] ??
        data['questions'] ??
        data['quiz_questions'] ??
        data['items'],
  );
  if (fromData.isNotEmpty) {
    return fromData;
  }
  final Map<String, dynamic> raw = _toMap(response['raw']);
  final List<dynamic> fromRaw = _listFromUnknown(
    raw['questions_json'] ??
        raw['questions'] ??
        raw['quiz_questions'] ??
        raw['items'],
  );
  if (fromRaw.isNotEmpty) {
    return fromRaw;
  }
  Map<String, dynamic>? decodedAnswer;
  final String answer = (response['answer'] ?? '').toString().trim();
  if (answer.isNotEmpty) {
    try {
      final dynamic decoded = jsonDecode(answer);
      if (decoded is Map<String, dynamic>) {
        decodedAnswer = decoded;
      } else if (decoded is Map) {
        decodedAnswer = Map<String, dynamic>.from(decoded);
      }
    } catch (_) {}
  }
  if (decodedAnswer != null) {
    final List<dynamic> fromAnswer = _listFromUnknown(
      decodedAnswer['questions'] ??
          decodedAnswer['questions_json'] ??
          decodedAnswer['quiz_questions'],
    );
    if (fromAnswer.isNotEmpty) {
      return fromAnswer;
    }
  }
  final List<String> candidateTexts = <String>[
    (response['answer'] ?? '').toString(),
    (response['final_answer'] ?? '').toString(),
    (response['reasoning'] ?? '').toString(),
    (response['explanation'] ?? '').toString(),
    (data['answer'] ?? '').toString(),
    (data['final_answer'] ?? '').toString(),
    (data['reasoning'] ?? '').toString(),
    (raw['answer'] ?? '').toString(),
    (raw['final_answer'] ?? '').toString(),
    (raw['reasoning'] ?? '').toString(),
    (raw['output'] ?? '').toString(),
    (raw['content'] ?? '').toString(),
    (raw['text'] ?? '').toString(),
  ];
  for (final String text in candidateTexts) {
    final String trimmed = text.trim();
    if (trimmed.isEmpty) {
      continue;
    }
    final List<dynamic> parsedDirect = _listFromUnknown(trimmed);
    if (parsedDirect.isNotEmpty) {
      return parsedDirect;
    }
    final int objStart = trimmed.indexOf('{');
    final int objEnd = trimmed.lastIndexOf('}');
    if (objStart >= 0 && objEnd > objStart) {
      final List<dynamic> parsedObject = _listFromUnknown(
        trimmed.substring(objStart, objEnd + 1),
      );
      if (parsedObject.isNotEmpty) {
        return parsedObject;
      }
    }
    final int arrStart = trimmed.indexOf('[');
    final int arrEnd = trimmed.lastIndexOf(']');
    if (arrStart >= 0 && arrEnd > arrStart) {
      final List<dynamic> parsedArray = _listFromUnknown(
        trimmed.substring(arrStart, arrEnd + 1),
      );
      if (parsedArray.isNotEmpty) {
        return parsedArray;
      }
    }
  }
  return <dynamic>[];
}

String _normalizeAiText(String raw) {
  return raw.replaceAll('\r', '\n').trim().replaceAll(RegExp(r'\s+'), ' ');
}

String _normalizeAiQuestionType(dynamic rawType) {
  final String normalized = rawType.toString().trim().toLowerCase();
  final bool singleCorrectSignal =
      normalized.contains('single correct') ||
      normalized.contains('single answer') ||
      normalized.contains('mcq_single') ||
      normalized == 'single';
  final bool multiSignal =
      normalized == 'multi' ||
      normalized == 'multicorrect' ||
      normalized == 'multi_correct' ||
      normalized == 'multi-correct' ||
      normalized == 'multiple' ||
      normalized == 'multiple_correct' ||
      normalized == 'multiple correct' ||
      normalized == 'multiple_choice_multiple_answer' ||
      normalized == 'msq' ||
      normalized == 'mcma' ||
      normalized == 'select_all_that_apply' ||
      normalized.contains('multiple correct') ||
      normalized.contains('multi correct') ||
      normalized.contains('select all') ||
      (normalized.contains('multiple') && !singleCorrectSignal);
  if (multiSignal) {
    return 'MULTI';
  }
  if (normalized == 'numerical' ||
      normalized == 'num' ||
      normalized == 'integer' ||
      normalized == 'integer_type' ||
      normalized == 'integer type' ||
      normalized == 'numerical answer type' ||
      normalized.contains('numerical') ||
      normalized.contains('integer') ||
      normalized.contains('numeric')) {
    return 'NUMERICAL';
  }
  return 'MCQ';
}

String _normalizeAiOptionText(String raw) {
  String out = raw.trim();
  if (out.isEmpty) {
    return '';
  }
  out = out.replaceFirst(RegExp(r'^\s*[-*]\s*'), '');
  out = out.replaceFirst(RegExp(r'^\s*\(?[A-Da-d1-4]\)?[.)\-:]\s*'), '');
  out = out.replaceFirst(RegExp(r'^\s*[A-Da-d]\s*\]\s*'), '');
  return _normalizeAiText(out);
}

List<String> _normalizeAiOptions(dynamic rawOptions) {
  if (rawOptions is! List) {
    return <String>[];
  }
  final List<String> out = <String>[];
  final Set<String> seen = <String>{};
  for (final dynamic entry in rawOptions) {
    final String cleaned = _normalizeAiOptionText(entry.toString());
    if (cleaned.isEmpty) {
      continue;
    }
    final String key = cleaned.toLowerCase();
    if (seen.add(key)) {
      out.add(cleaned);
    }
    if (out.length >= 4) {
      break;
    }
  }
  return out;
}

List<String> _extractAiCorrectAnswers(
  Map<String, dynamic> payload,
  List<String> options,
) {
  final Set<String> out = <String>{};
  final dynamic rawAnswers =
      payload['correct_answers'] ??
      payload['correct_answer'] ??
      payload['answer'];

  void addByLetter(String letter) {
    final String upper = letter.trim().toUpperCase();
    if (upper.isEmpty) {
      return;
    }
    if (upper == '1' || upper == '2' || upper == '3' || upper == '4') {
      final int idx = int.parse(upper) - 1;
      if (idx >= 0 && idx < options.length) {
        out.add(options[idx]);
      }
      return;
    }
    if (upper.codeUnitAt(0) >= 65 && upper.codeUnitAt(0) <= 68) {
      final int idx = upper.codeUnitAt(0) - 65;
      if (idx >= 0 && idx < options.length) {
        out.add(options[idx]);
      }
    }
  }

  if (rawAnswers is List) {
    for (final dynamic entry in rawAnswers) {
      final String token = _normalizeAiOptionText(entry.toString());
      if (token.isEmpty) {
        continue;
      }
      if (RegExp(r'^[A-Da-d1-4]$').hasMatch(token)) {
        addByLetter(token);
      } else {
        for (final String option in options) {
          if (_normalizeAiOptionText(option).toLowerCase() ==
              token.toLowerCase()) {
            out.add(option);
          }
        }
      }
    }
  } else {
    final String answerText = _normalizeAiOptionText(
      (rawAnswers ?? '').toString(),
    );
    if (answerText.isNotEmpty) {
      if (RegExp(r'^[A-Da-d]{2,4}$').hasMatch(answerText)) {
        for (final int code in answerText.toUpperCase().codeUnits) {
          addByLetter(String.fromCharCode(code));
        }
      }
      for (final String token in answerText.split(RegExp(r'[,/|]+'))) {
        final String cleaned = _normalizeAiOptionText(token);
        if (cleaned.isEmpty) {
          continue;
        }
        if (RegExp(r'^[A-Da-d1-4]$').hasMatch(cleaned)) {
          addByLetter(cleaned);
          continue;
        }
        for (final String option in options) {
          if (_normalizeAiOptionText(option).toLowerCase() ==
              cleaned.toLowerCase()) {
            out.add(option);
          }
        }
      }
    }
  }
  return out.toList();
}

String _sanitizeNumericalAnswer(dynamic ans) {
  if (ans is num) {
    if (ans >= 0 && ans <= 9 && ans == ans.roundToDouble()) {
      return ans.toInt().toString();
    }
  }
  if (ans is String) {
    final int? parsed = int.tryParse(ans.trim());
    if (parsed != null && parsed >= 0 && parsed <= 9) {
      return parsed.toString();
    }
  }
  throw const FormatException('Invalid numerical answer. Regenerate.');
}

String _extractSanitizedNumericalAnswer(Map<String, dynamic> q) {
  final dynamic rawCandidate =
      q['correct_answer'] ?? q['answer'] ?? q['correct_answers'];
  if (rawCandidate is List && rawCandidate.isNotEmpty) {
    return _sanitizeNumericalAnswer(rawCandidate.first);
  }
  return _sanitizeNumericalAnswer(rawCandidate);
}

int _extractAiDifficultyLevel(Map<String, dynamic> q) {
  final dynamic raw = q['difficulty'] ?? q['difficulty_level'];
  if (raw is num) {
    final int level = raw.toInt();
    if (level >= 1 && level <= 5) {
      return level;
    }
  }
  if (raw is String) {
    final int? parsed = int.tryParse(raw.trim());
    if (parsed != null && parsed >= 1 && parsed <= 5) {
      return parsed;
    }
  }
  return 0;
}

List<ParsedQuestion> _parseAiQuestions(List<dynamic> raw) {
  final List<ParsedQuestion> out = <ParsedQuestion>[];
  for (final dynamic item in raw) {
    if (item is! Map) {
      continue;
    }
    final Map<String, dynamic> q = Map<String, dynamic>.from(item);
    final String question = _normalizeAiText(
      (q['question'] ?? q['question_text'] ?? '').toString(),
    );
    String type = _normalizeAiQuestionType(q['type'] ?? q['question_type']);
    final List<String> options = _normalizeAiOptions(q['options']);
    if (question.isEmpty) {
      continue;
    }
    if (type != 'NUMERICAL' && options.length != 4) {
      continue;
    }

    final List<String> tags = <String>[];
    final dynamic tagRaw = q['concept_tags'];
    if (tagRaw is List) {
      for (final dynamic tag in tagRaw) {
        final String v = _normalizeAiText(tag.toString());
        if (v.isNotEmpty) {
          tags.add(v);
        }
      }
    }
    final int difficulty = _extractAiDifficultyLevel(q);
    if (difficulty > 0) {
      tags.insert(0, 'Difficulty L$difficulty');
    }

    List<String> correctAnswers;
    if (type == 'NUMERICAL') {
      try {
        correctAnswers = <String>[_extractSanitizedNumericalAnswer(q)];
      } catch (_) {
        // Student payloads may hide keys; keep question and validate other fields.
        correctAnswers = <String>[];
      }
    } else {
      correctAnswers = _extractAiCorrectAnswers(q, options)
          .map(_normalizeAiOptionText)
          .where((String e) => e.isNotEmpty)
          .toSet()
          .toList();
      if (type == 'MCQ' && correctAnswers.length > 1) {
        correctAnswers = <String>[correctAnswers.first];
      }
    }

    final String solution = _normalizeAiText(
      (q['solution'] ?? q['solution_explanation'] ?? '').toString(),
    );
    out.add(
      ParsedQuestion(
        type: type,
        question: question,
        options: type == 'NUMERICAL' ? <String>['', '', '', ''] : options,
        correctAnswers: correctAnswers,
        solution: solution,
        difficulty: difficulty,
        tags: tags,
      ),
    );
  }
  return out;
}

bool _isQuestionLikelyTooEasy(ParsedQuestion q) {
  final String text = q.question.trim().toLowerCase();
  if (text.isEmpty) {
    return true;
  }
  final bool veryShort = text.length < 40;
  final bool singleStepMath = RegExp(
    r'(\d+\s*[x*]\s*\d+|\d+\s*[+\-]\s*\d+)',
  ).hasMatch(text);
  final bool hasReasoningCue =
      text.contains('solve') ||
      text.contains('larger root') ||
      text.contains('ratio') ||
      text.contains('choose') ||
      text.contains('term');
  final bool placeholderOption = q.options
      .map((String e) => e.trim().toLowerCase())
      .any((String e) => e.startsWith('option '));
  if (placeholderOption) {
    return true;
  }
  if (singleStepMath && !hasReasoningCue) {
    return true;
  }
  return veryShort && !hasReasoningCue;
}

bool _quizLikelyTooEasy(List<ParsedQuestion> questions) {
  if (questions.isEmpty) {
    return true;
  }
  final int weak = questions.where(_isQuestionLikelyTooEasy).length;
  return weak >= max(2, (questions.length * 0.55).round());
}

int _reasoningStepCount(String solution) {
  final String text = solution.trim();
  if (text.isEmpty) {
    return 0;
  }
  final int explicit = RegExp(
    r'(step\s*\d+\s*:|^\s*\d+\s*[.)])',
    caseSensitive: false,
    multiLine: true,
  ).allMatches(text).length;
  if (explicit > 0) {
    return explicit;
  }
  final List<String> sentences = text
      .split(RegExp(r'(?<=[.!?])\s+'))
      .map((String e) => e.trim())
      .where((String e) => e.isNotEmpty)
      .toList();
  return sentences.length;
}

bool _hasStrictDifficultyCoverage(List<ParsedQuestion> questions) {
  if (questions.length < 5) {
    return true;
  }
  final Set<int> seen = <int>{};
  final RegExp marker = RegExp(
    r'\bdifficulty\s*L?\s*([1-5])\b',
    caseSensitive: false,
  );
  for (final ParsedQuestion q in questions) {
    final String bag = q.tags.join(' ');
    for (final Match m in marker.allMatches(bag)) {
      final int? level = int.tryParse(m.group(1) ?? '');
      if (level != null) {
        seen.add(level);
      }
    }
  }
  return seen.length == 5;
}

bool _matchesCustomPracticeDistribution(List<ParsedQuestion> questions) {
  if (questions.isEmpty) {
    return false;
  }
  final int total = questions.length;
  final int expectedNumerical = (total * 0.4).round();
  final int expectedMcq = (total * 0.3).round();
  final int expectedMulti = total - expectedNumerical - expectedMcq;
  final int numerical = questions
      .where((ParsedQuestion q) => q.type == 'NUMERICAL')
      .length;
  final int mcq = questions.where((ParsedQuestion q) => q.type == 'MCQ').length;
  final int multi = questions
      .where((ParsedQuestion q) => q.type == 'MULTI')
      .length;
  return numerical == expectedNumerical &&
      mcq == expectedMcq &&
      multi == expectedMulti;
}

bool _passesCustomPracticeContract(
  List<ParsedQuestion> questions, {
  required int expectedCount,
}) {
  if (questions.length != expectedCount) {
    return false;
  }
  final bool hasVisibleAnswerKeys = questions.any(
    (ParsedQuestion q) => q.correctAnswers.isNotEmpty,
  );
  final bool hasVisibleSolutions = questions.any(
    (ParsedQuestion q) => q.solution.trim().isNotEmpty,
  );
  if (hasVisibleAnswerKeys) {
    if (!_matchesCustomPracticeDistribution(questions)) {
      return false;
    }
    if (!_hasStrictDifficultyCoverage(questions)) {
      return false;
    }
  }
  for (final ParsedQuestion q in questions) {
    if (hasVisibleSolutions && _reasoningStepCount(q.solution) < 2) {
      return false;
    }
    if (hasVisibleAnswerKeys && q.type == 'NUMERICAL') {
      // In student mode answer keys can be omitted; enforce numeric bounds only if provided.
      if (q.correctAnswers.isNotEmpty) {
        final int? value = int.tryParse(q.correctAnswers.first.trim());
        if (value == null || value < 0 || value > 9) {
          return false;
        }
      }
    }
  }
  return true;
}

String _extractAiFailureReason(Map<String, dynamic> response) {
  final List<String> chunks = <String>[
    (response['status'] ?? '').toString().trim(),
    (response['message'] ?? '').toString().trim(),
    (response['error'] ?? '').toString().trim(),
    (_toMap(response['data'])['message'] ?? '').toString().trim(),
    (_toMap(response['raw'])['message'] ?? '').toString().trim(),
    (_toMap(response['raw'])['error'] ?? '').toString().trim(),
  ].where((String e) => e.isNotEmpty).toList();
  if (chunks.isEmpty) {
    return 'backend response did not include usable questions';
  }
  return chunks.join(' | ');
}

Future<Map<String, dynamic>> _generateAiQuizFallbackViaEngine({
  required AiEngineService aiService,
  required Map<String, dynamic> payload,
  required int attempt,
}) async {
  final List<String> chapters =
      (payload['chapters'] as List<dynamic>? ?? <dynamic>[])
          .map((dynamic e) => e.toString().trim())
          .where((String e) => e.isNotEmpty)
          .toList();
  final List<String> subtopics =
      (payload['subtopics'] as List<dynamic>? ?? <dynamic>[])
          .map((dynamic e) => e.toString().trim())
          .where((String e) => e.isNotEmpty)
          .toList();
  final int requestedCount = (payload['question_count'] as num?)?.toInt() ?? 10;
  final int requestedDifficulty = (payload['difficulty'] as num?)?.toInt() ?? 5;
  final bool preferPyq = (payload['pyq_focus'] as bool?) ?? true;
  final int expectedNumerical = (requestedCount * 0.4).round();
  final int expectedMcq = (requestedCount * 0.3).round();
  final int expectedMulti = requestedCount - expectedNumerical - expectedMcq;

  final String prompt = <String>[
    'Generate a JEE Advanced custom practice quiz in strict JSON only.',
    'Role: student',
    'Difficulty profile: ${_aiHardnessLabel(5)}',
    'Difficulty level (numeric): $requestedDifficulty',
    'Question count: $requestedCount',
    'Chapters: ${chapters.isEmpty ? 'General' : chapters.join(', ')}',
    'Subtopics: ${subtopics.isEmpty ? 'General' : subtopics.join(', ')}',
    if (preferPyq)
      'Priority: Use previous-year-question style framing and trap patterns without copying exact copyrighted text.',
    'Attempt index: $attempt',
    'Output contract (must follow exactly):',
    '{',
    '  "questions": [',
    '    {',
    '      "type": "numerical | mcq | multi",',
    '      "difficulty": "integer 1-5",',
    '      "question": "string",',
    '      "options": ["A", "B", "C", "D"],',
    '      "correct_answer": "single digit integer OR option letters",',
    '      "solution": "stepwise clean explanation"',
    '    }',
    '  ]',
    '}',
    'Strict rules:',
    '- Return ONLY valid JSON.',
    '- No markdown, no code fences, no commentary.',
    '- No LaTeX in correct_answer.',
    '- For numerical type: correct_answer must be one integer from 0 to 9.',
    '- Numerical correct_answer cannot be negative, decimal, fraction, expression, or text.',
    '- Each solution must include at least two reasoning steps.',
    '- If a question is solvable in less than two reasoning steps, regenerate internally.',
    '- Distribution must be exact: numerical=$expectedNumerical, mcq=$expectedMcq, multi=$expectedMulti.',
    if (requestedCount >= 5)
      '- Each difficulty level 1,2,3,4,5 must appear at least once.',
    '- Difficulty calibration:',
    '  Level 1: conceptual but needs 2-3 reasoning steps.',
    '  Level 2: multi-step with trap possibility.',
    '  Level 3: multi-concept integration with manipulation.',
    '  Level 4: deep conceptual application and elimination/identity transformation.',
    '  Level 5: true JEE Advanced with hidden constraint and case/symmetry/substitution trick.',
    '- Forbidden: direct formula plug-in, board-level trivial questions.',
    'Internal self-check before output:',
    '1) Validate numerical answers are 0-9 integers.',
    '2) Validate difficulty tags match complexity.',
    '3) Validate strict JSON validity.',
    '4) Validate no text outside JSON.',
    'If any check fails, regenerate internally and return ONLY final valid JSON.',
  ].join('\n');

  final Map<String, dynamic> response = await aiService.sendChat(
    prompt: prompt,
    userId: 'student_pipeline_runner',
    chatId:
        'student_ai_quiz_fallback_${DateTime.now().millisecondsSinceEpoch}_$attempt',
    function: 'ai_generate_quiz',
    responseStyle: 'structured_json',
    enablePersona: false,
    card: const <String, dynamic>{'surface': 'student_ai_quiz_fallback'},
  );

  final List<dynamic> extracted = _extractAiQuestionRawList(response);
  if (extracted.isNotEmpty) {
    return <String, dynamic>{
      'ok': true,
      'status': 'SUCCESS',
      'quiz_id': 'ai_engine_${DateTime.now().millisecondsSinceEpoch}_$attempt',
      'questions_json': extracted,
      'source': 'ai_engine_fallback',
      'upstream': response,
    };
  }
  final String repairInput =
      <String>[
            (response['answer'] ?? '').toString(),
            (response['final_answer'] ?? '').toString(),
            (response['reasoning'] ?? '').toString(),
            (response['explanation'] ?? '').toString(),
            (_toMap(response['raw'])['content'] ?? '').toString(),
            (_toMap(response['raw'])['output'] ?? '').toString(),
          ]
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty)
          .take(3)
          .join('\n\n');
  if (repairInput.isNotEmpty) {
    final Map<String, dynamic> repairedResponse = await aiService.sendChat(
      prompt: <String>[
        'Transform the following quiz content into strict JSON only.',
        'Return one JSON object with key "questions".',
        'Allowed question types: MCQ, MULTI, INTEGER.',
        'No markdown, no commentary.',
        'Input content:',
        repairInput,
      ].join('\n'),
      userId: 'student_pipeline_runner',
      chatId:
          'student_ai_quiz_json_repair_${DateTime.now().millisecondsSinceEpoch}_$attempt',
      function: 'ai_generate_quiz',
      responseStyle: 'structured_json',
      enablePersona: false,
      card: const <String, dynamic>{
        'surface': 'student_ai_quiz_json_repair',
        'json_repair': true,
      },
    );
    final List<dynamic> repaired = _extractAiQuestionRawList(repairedResponse);
    if (repaired.isNotEmpty) {
      return <String, dynamic>{
        'ok': true,
        'status': 'SUCCESS',
        'quiz_id':
            'ai_engine_${DateTime.now().millisecondsSinceEpoch}_${attempt}_repair',
        'questions_json': repaired,
        'source': 'ai_engine_fallback_json_repair',
        'upstream': repairedResponse,
      };
    }
  }
  throw Exception('AI engine fallback did not return structured questions.');
}

Future<void> main() async {
  final BackendService backendService = BackendService();
  final AiEngineService aiService = AiEngineService(
    backendService: backendService,
  );

  const int aiDifficulty = 5;
  const int aiQuestionCount = 10;
  const bool aiPreferPyq = true;
  const int hardnessBoost = 0;

  const List<String> selectedChapters = <String>['Binomial Theorem'];
  final List<String> subtopics = <String>[
    'Binomial Expansion',
    'General Term',
    'Middle Term',
    'Greatest Coefficient',
  ];
  const String selectedClass = 'Class 12';
  const String selectedSubject = 'Mathematics';

  final Map<String, dynamic> hardness = _hardnessProfile(aiDifficulty);
  final int baseDifficulty = (hardness['difficulty'] as int?) ?? aiDifficulty;
  final int effectiveDifficulty = max(baseDifficulty + hardnessBoost, 5);
  final int duration = max(20, aiQuestionCount * 2);

  final Map<String, dynamic> basePayload = <String, dynamic>{
    'title': 'AI Practice Quiz • Binomial Theorem',
    'subject': selectedSubject,
    'chapters': selectedChapters,
    'subtopics': subtopics,
    'difficulty': effectiveDifficulty,
    'difficulty_stage': aiDifficulty,
    'difficulty_stage_label': _aiHardnessLabel(aiDifficulty),
    'question_count': aiQuestionCount,
    'trap_intensity': hardness['trap_intensity'] ?? 'extreme',
    'weakness_mode': true,
    'cross_concept': true,
    'class': selectedClass,
    'type': 'Exam',
    'duration': duration,
    'user_id': 'student_pipeline_runner',
    'student_id': 'student_pipeline_runner',
    'role': 'student',
    'request_role': 'student',
    'self_practice_mode': true,
    'authoring_mode': false,
    'include_answer_key': true,
    'include_solutions': true,
    'time_mode': 'unlimited',
    'unlimited_time_mode': true,
    'enforce_difficulty': true,
    'avoid_easy_questions': true,
    'reasoning_depth': hardness['reasoning_depth'] ?? 'very_high',
    'strict_hard_mode': hardness['strict_hard_mode'] ?? true,
    'target_level': hardness['target_level'] ?? 'olympiad_trap',
    'pyq_focus': aiPreferPyq,
    'prefer_pyq': aiPreferPyq,
    'use_pyq_patterns': aiPreferPyq,
    'prefer_previous_year_questions': aiPreferPyq,
    'pyq_weight': aiPreferPyq ? 0.75 : 0.35,
    'exam_patterns': aiPreferPyq
        ? <String>['JEE Main', 'JEE Advanced', 'NEET']
        : <String>[],
  };

  _log('Student AI Generate Pipeline Run Started');
  _log(
    'Config: subject=$selectedSubject chapter=${selectedChapters.join(", ")} '
    'difficultyStage=$aiDifficulty effectiveDifficulty=$effectiveDifficulty '
    'questionCount=$aiQuestionCount pyqFocus=$aiPreferPyq',
  );

  Map<String, dynamic> finalResponse = <String, dynamic>{};
  List<ParsedQuestion> finalQuestions = <ParsedQuestion>[];
  String finalSource = '';
  final List<String> failures = <String>[];

  const int maxAttempts = 5;
  for (int attempt = 0; attempt < maxAttempts; attempt++) {
    final int tunedDifficulty = max(effectiveDifficulty, 5 + attempt);
    final Map<String, dynamic> payload = <String, dynamic>{
      ...basePayload,
      'difficulty': tunedDifficulty,
      'trap_intensity': attempt >= 2
          ? 'extreme'
          : (hardness['trap_intensity'] ?? 'high'),
      'weakness_mode': true,
      'cross_concept': true,
      'strict_hard_mode': true,
      'target_level': hardness['target_level'] ?? 'advanced',
      'retry_index': attempt + 1,
      'require_answer_key': true,
      'hardness_pass': attempt + 1,
    };

    _log(
      'Attempt ${attempt + 1}/$maxAttempts: requesting backend.generateAiQuiz '
      '(difficulty=$tunedDifficulty, trap=${payload['trap_intensity']})',
    );

    Map<String, dynamic> generated = <String, dynamic>{};
    List<dynamic> rawQuestions = <dynamic>[];
    String source = 'backend.generateAiQuiz';

    try {
      generated = await backendService
          .generateAiQuiz(payload)
          .timeout(Duration(seconds: attempt == 0 ? 30 : 36));
      rawQuestions = _extractAiQuestionRawList(generated);

      final bool backendOk = backendService.isSuccessfulResponse(generated);
      _log(
        'Attempt ${attempt + 1}: backend status=${generated['status']} ok=$backendOk '
        'rawQuestions=${rawQuestions.length}',
      );

      if (rawQuestions.isEmpty) {
        if (!backendOk) {
          final String reason = _extractAiFailureReason(generated);
          failures.add('attempt ${attempt + 1} backend: $reason');
          _log('Attempt ${attempt + 1}: backend failure reason: $reason');
        } else {
          failures.add(
            'attempt ${attempt + 1} backend: success-without-questions',
          );
          _log(
            'Attempt ${attempt + 1}: backend returned success but no structured questions.',
          );
        }
        _log('Attempt ${attempt + 1}: invoking ai fallback generation.');
        generated = await _generateAiQuizFallbackViaEngine(
          aiService: aiService,
          payload: payload,
          attempt: attempt + 1,
        );
        rawQuestions = _extractAiQuestionRawList(generated);
        source = 'ai_engine_fallback';
        _log(
          'Attempt ${attempt + 1}: fallback returned rawQuestions=${rawQuestions.length}',
        );
      }

      if (rawQuestions.isEmpty) {
        failures.add('attempt ${attempt + 1}: no valid questions found');
        _log('Attempt ${attempt + 1}: no valid questions found, continuing.');
        continue;
      }

      final List<ParsedQuestion> parsed = _parseAiQuestions(rawQuestions);
      _log('Attempt ${attempt + 1}: parsedQuestions=${parsed.length}');

      if (parsed.isEmpty) {
        failures.add('attempt ${attempt + 1}: parsed questions were empty');
        _log('Attempt ${attempt + 1}: parsed questions empty, continuing.');
        continue;
      }

      final bool tooEasy = _quizLikelyTooEasy(parsed);
      final bool contract = _passesCustomPracticeContract(
        parsed,
        expectedCount: aiQuestionCount,
      );
      _log(
        'Attempt ${attempt + 1}: qualityCheck tooEasy=$tooEasy customPracticeContract=$contract',
      );

      if (tooEasy) {
        failures.add('attempt ${attempt + 1}: set too easy after validation');
        continue;
      }
      if (!contract) {
        failures.add('attempt ${attempt + 1}: custom practice contract failed');
        continue;
      }

      finalResponse = generated;
      finalQuestions = parsed;
      finalSource = source;
      _log('Attempt ${attempt + 1}: accepted.');
      break;
    } catch (e) {
      _log('Attempt ${attempt + 1}: exception: $e');
      failures.add('attempt ${attempt + 1} backend exception: $e');
      try {
        _log(
          'Attempt ${attempt + 1}: invoking ai fallback generation after exception.',
        );
        final Map<String, dynamic> generated =
            await _generateAiQuizFallbackViaEngine(
              aiService: aiService,
              payload: payload,
              attempt: attempt + 1,
            );
        final List<dynamic> rawQuestions = _extractAiQuestionRawList(generated);
        _log(
          'Attempt ${attempt + 1}: fallback-after-exception rawQuestions=${rawQuestions.length}',
        );
        if (rawQuestions.isEmpty) {
          failures.add(
            'attempt ${attempt + 1}: fallback returned empty question payload',
          );
          continue;
        }

        final List<ParsedQuestion> parsed = _parseAiQuestions(rawQuestions);
        _log(
          'Attempt ${attempt + 1}: fallback-after-exception parsedQuestions=${parsed.length}',
        );
        if (parsed.isEmpty) {
          failures.add(
            'attempt ${attempt + 1}: fallback parsed questions were empty',
          );
          continue;
        }

        final bool tooEasy = _quizLikelyTooEasy(parsed);
        final bool contract = _passesCustomPracticeContract(
          parsed,
          expectedCount: aiQuestionCount,
        );
        _log(
          'Attempt ${attempt + 1}: fallback qualityCheck tooEasy=$tooEasy customPracticeContract=$contract',
        );
        if (tooEasy) {
          failures.add(
            'attempt ${attempt + 1}: fallback set too easy after validation',
          );
          continue;
        }
        if (!contract) {
          failures.add(
            'attempt ${attempt + 1}: fallback custom practice contract failed',
          );
          continue;
        }

        finalResponse = generated;
        finalQuestions = parsed;
        finalSource = 'ai_engine_fallback_after_exception';
        _log('Attempt ${attempt + 1}: fallback-after-exception accepted.');
        break;
      } catch (fallbackError) {
        failures.add(
          'attempt ${attempt + 1} fallback exception: $fallbackError',
        );
        _log('Attempt ${attempt + 1}: fallback exception: $fallbackError');
      }
    }
  }

  print('');
  print(
    '========== STUDENT AI GENERATION REPORT (BINOMIAL THEOREM) ==========',
  );
  print('run_time: ${DateTime.now().toIso8601String()}');
  print('requested_chapter: Binomial Theorem');
  print('difficulty_stage: $aiDifficulty (${_aiHardnessLabel(aiDifficulty)})');
  print('requested_question_count: $aiQuestionCount');
  print('pyq_focus: $aiPreferPyq');
  print('selected_source: ${finalSource.isEmpty ? 'none' : finalSource}');
  print('final_status: ${finalQuestions.isEmpty ? 'FAILED' : 'SUCCESS'}');
  print('attempt_failures_count: ${failures.length}');
  if (failures.isNotEmpty) {
    print('attempt_failures:');
    for (final String failure in failures) {
      print('- $failure');
    }
  }

  if (finalQuestions.isEmpty) {
    print('no_final_questions_generated=true');
    print(
      'final_response_snippet=${jsonEncode(finalResponse).substring(0, min(1200, jsonEncode(finalResponse).length))}',
    );
    return;
  }

  final int numerical = finalQuestions
      .where((ParsedQuestion q) => q.type == 'NUMERICAL')
      .length;
  final int mcq = finalQuestions
      .where((ParsedQuestion q) => q.type == 'MCQ')
      .length;
  final int multi = finalQuestions
      .where((ParsedQuestion q) => q.type == 'MULTI')
      .length;
  print('final_question_count: ${finalQuestions.length}');
  print('distribution: numerical=$numerical mcq=$mcq multi=$multi');
  print(
    'contract_passed: ${_passesCustomPracticeContract(finalQuestions, expectedCount: aiQuestionCount)}',
  );
  print('likely_too_easy: ${_quizLikelyTooEasy(finalQuestions)}');
  print('');
  print('QUESTIONS:');
  for (int i = 0; i < finalQuestions.length; i++) {
    final ParsedQuestion q = finalQuestions[i];
    print('--- Q${i + 1} ---');
    print('type: ${q.type}');
    print('difficulty: ${q.difficulty}');
    if (q.tags.isNotEmpty) {
      print('tags: ${q.tags.join(', ')}');
    }
    print('question: ${q.question}');
    if (q.type != 'NUMERICAL') {
      for (int k = 0; k < q.options.length; k++) {
        final String label = String.fromCharCode(65 + k);
        print('  $label) ${q.options[k]}');
      }
    }
    print('correct: ${q.correctAnswers.join(', ')}');
    print('solution: ${q.solution}');
  }

  print('');
  print('RAW_RESPONSE_HEAD:');
  final String raw = const JsonEncoder.withIndent('  ').convert(finalResponse);
  print(raw.substring(0, min(raw.length, 5000)));
}
