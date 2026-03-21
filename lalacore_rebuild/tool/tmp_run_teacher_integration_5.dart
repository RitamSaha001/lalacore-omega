import 'dart:convert';

import 'package:lalacore_rebuild/services/backend_service.dart';

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

List<dynamic> _extractQuestions(Map<String, dynamic> response) {
  final List<dynamic> direct = _listFromUnknown(
    response['questions_json'] ??
        response['questions'] ??
        response['quiz_questions'] ??
        response['items'],
  );
  if (direct.isNotEmpty) {
    return direct;
  }
  final dynamic data = response['data'];
  if (data is Map) {
    final Map<String, dynamic> map = Map<String, dynamic>.from(data);
    final List<dynamic> fromData = _listFromUnknown(
      map['questions_json'] ??
          map['questions'] ??
          map['quiz_questions'] ??
          map['items'],
    );
    if (fromData.isNotEmpty) {
      return fromData;
    }
  }
  final dynamic raw = response['raw'];
  if (raw is Map) {
    final Map<String, dynamic> map = Map<String, dynamic>.from(raw);
    final List<dynamic> fromRaw = _listFromUnknown(
      map['questions_json'] ??
          map['questions'] ??
          map['quiz_questions'] ??
          map['items'],
    );
    if (fromRaw.isNotEmpty) {
      return fromRaw;
    }
  }
  return <dynamic>[];
}

String _qText(Map<String, dynamic> q) {
  final String v =
      (q['question_text'] ?? q['question'] ?? q['text'] ?? '').toString();
  return v.trim();
}

String _qType(Map<String, dynamic> q) {
  final String raw = (q['type'] ?? q['question_type'] ?? 'MCQ').toString();
  final String t = raw.trim().toUpperCase();
  if (t == 'INTEGER') {
    return 'NUMERICAL';
  }
  return t;
}

List<String> _qOptions(Map<String, dynamic> q) {
  final dynamic raw = q['options'];
  if (raw is! List) {
    return <String>[];
  }
  final List<String> out = <String>[];
  for (final dynamic item in raw) {
    if (item is Map) {
      final Map<String, dynamic> m = Map<String, dynamic>.from(item);
      final String v = (m['text'] ?? m['option'] ?? m['value'] ?? '').toString();
      if (v.trim().isNotEmpty) {
        out.add(v.trim());
      }
    } else {
      final String v = item.toString().trim();
      if (v.isNotEmpty) {
        out.add(v);
      }
    }
  }
  return out;
}

List<String> _qAnswer(Map<String, dynamic> q) {
  final dynamic ans = q['correct_answer'] ?? q['answer'] ?? q['correct_answers'];
  if (ans is List) {
    return ans.map((dynamic e) => e.toString().trim()).where((String e) => e.isNotEmpty).toList();
  }
  if (ans is Map) {
    final Map<String, dynamic> m = Map<String, dynamic>.from(ans);
    final List<String> out = <String>[];
    final String single = (m['single'] ?? '').toString().trim();
    if (single.isNotEmpty) {
      out.add(single);
    }
    final dynamic multiRaw = m['multiple'];
    if (multiRaw is List) {
      out.addAll(
        multiRaw
            .map((dynamic e) => e.toString().trim())
            .where((String e) => e.isNotEmpty),
      );
    }
    final String numerical = (m['numerical'] ?? '').toString().trim();
    if (numerical.isNotEmpty) {
      out.add(numerical);
    }
    return out.toSet().toList();
  }
  final String text = ans?.toString().trim() ?? '';
  return text.isEmpty ? <String>[] : <String>[text];
}

void _printQuiz(List<Map<String, dynamic>> rows) {
  for (int i = 0; i < rows.length; i++) {
    final Map<String, dynamic> q = rows[i];
    final String type = _qType(q);
    final String text = _qText(q);
    final List<String> options = _qOptions(q);
    final List<String> answers = _qAnswer(q);
    final String solution =
        (q['solution_explanation'] ?? q['solution'] ?? '').toString().trim();
    print('\nQ${i + 1} [$type]');
    print(text);
    if (options.isNotEmpty) {
      const List<String> labels = <String>['A', 'B', 'C', 'D'];
      for (int j = 0; j < options.length && j < labels.length; j++) {
        print('  ${labels[j]}. ${options[j]}');
      }
    }
    if (answers.isNotEmpty) {
      print('Answer: ${answers.join(', ')}');
    }
    if (solution.isNotEmpty) {
      print('Solution: $solution');
    }
  }
}

Future<void> main() async {
  final BackendService backend = BackendService();

  final Map<String, dynamic> payload = <String, dynamic>{
    'title': 'Teacher Integration Set • 5Q',
    'subject': 'Mathematics',
    'chapters': const <String>['Integrals'],
    'subtopics': const <String>[
      'Indefinite Integrals',
      'Definite Integrals',
      'Properties of Definite Integrals',
      'Area Under Curves',
      'Substitution',
      'Integration by Parts',
      'Partial Fractions',
    ],
    'difficulty': 5,
    'question_count': 5,
    'trap_intensity': 'high',
    'weakness_mode': true,
    'cross_concept': true,
    'role': 'teacher',
    'request_role': 'teacher',
    'authoring_mode': true,
    'teacher_authoring_mode': true,
    'self_practice_mode': false,
    'include_answer_key': true,
    'include_solutions': true,
    'pyq_focus': true,
    'prefer_pyq': true,
    'allow_web_search': true,
    'web_research_enabled': true,
    'pyq_mode': 'strict_related_web',
    'pyq_web_only_mode': false,
    'pyq_answer_retrieval_required': true,
    'require_type_variety': true,
    'chapter_coverage_required': true,
    'strict_hard_mode': true,
    'minimum_reasoning_steps': 2,
    'user_id': 'teacher_pipeline_runner',
    'teacher_id': 'teacher_pipeline_runner',
  };

  final Map<String, dynamic> response = await backend.generateAiQuiz(payload);
  final List<dynamic> rawQuestions = _extractQuestions(response);
  final List<Map<String, dynamic>> questions = rawQuestions
      .whereType<Map>()
      .map((Map<dynamic, dynamic> e) => Map<String, dynamic>.from(e))
      .toList();

  print('status=${response['status']} ok=${response['ok']}');
  print('source=${response['source'] ?? response['mode'] ?? ''}');
  print('question_count=${questions.length}');
  if (questions.isEmpty) {
    print('message=${response['message'] ?? response['error'] ?? ''}');
    print(jsonEncode(response));
    return;
  }
  _printQuiz(questions);
}
