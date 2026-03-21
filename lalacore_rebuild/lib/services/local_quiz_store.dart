import 'dart:convert';

import 'package:shared_preferences/shared_preferences.dart';

import '../models/quiz_models.dart';

class LocalQuizStore {
  static const String _indexKey = 'local_quiz_index_v1';
  static const String _payloadPrefix = 'local_quiz_payload_v1_';
  static const int _maxStored = 250;

  String localQuizUrl(String quizId) => 'localquiz://$quizId';

  bool isLocalQuizUrl(String url) {
    final Uri? uri = Uri.tryParse(url);
    return uri != null && uri.scheme == 'localquiz';
  }

  String? quizIdFromUrl(String url) {
    final Uri? uri = Uri.tryParse(url);
    if (uri == null || uri.scheme != 'localquiz') {
      return null;
    }
    if (uri.host.trim().isNotEmpty) {
      return uri.host.trim();
    }
    final String path = uri.path.replaceAll('/', '').trim();
    return path.isEmpty ? null : path;
  }

  Future<void> saveQuiz({
    required QuizItem item,
    required List<Question> questions,
    String ownerAccountId = '',
    String ownerRole = '',
    String visibility = '',
    bool includeInIndex = true,
  }) async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final String normalizedRole = ownerRole.trim().toLowerCase();
    final String normalizedVisibility = visibility.trim().toLowerCase().isEmpty
        ? 'private'
        : visibility.trim().toLowerCase();
    final Map<String, dynamic> payload = <String, dynamic>{
      'id': item.id,
      'title': item.title,
      'url': item.url,
      'deadline': item.deadline.toIso8601String(),
      'type': item.type,
      'duration_minutes': item.durationMinutes,
      'is_ai_generated': item.isAiGenerated,
      'is_unlimited_time': item.isUnlimitedTime,
      'created_at': DateTime.now().millisecondsSinceEpoch,
      'owner_account_id': ownerAccountId.trim(),
      'owner_role': normalizedRole,
      'visibility': normalizedVisibility,
      'ui_spec': item.uiSpec,
      'student_adaptive_data': item.studentAdaptiveData,
      'questions': questions.map(_questionToMap).toList(),
    };
    await prefs.setString(_payloadKey(item.id), jsonEncode(payload));

    if (!includeInIndex) {
      return;
    }

    final List<String> ids = List<String>.from(
      prefs.getStringList(_indexKey) ?? <String>[],
    );
    ids.remove(item.id);
    ids.insert(0, item.id);
    if (ids.length > _maxStored) {
      ids.removeRange(_maxStored, ids.length);
    }
    await prefs.setStringList(_indexKey, ids);
  }

  Future<bool> hasQuiz(String quizId) async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    return prefs.containsKey(_payloadKey(quizId));
  }

  Future<List<QuizItem>> listQuizItems({
    String viewerAccountId = '',
    String viewerRole = '',
  }) async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final List<String> ids = List<String>.from(
      prefs.getStringList(_indexKey) ?? <String>[],
    );
    final List<_StoredQuiz> withTimestamp = <_StoredQuiz>[];
    for (final String id in ids) {
      final String raw = prefs.getString(_payloadKey(id)) ?? '';
      if (raw.trim().isEmpty) {
        continue;
      }
      try {
        final Map<String, dynamic> payload = Map<String, dynamic>.from(
          jsonDecode(raw) as Map,
        );
        final DateTime deadline =
            DateTime.tryParse((payload['deadline'] ?? '').toString()) ??
            DateTime.now().add(const Duration(days: 7));
        final QuizItem item = QuizItem(
          id: (payload['id'] ?? id).toString().trim(),
          title: (payload['title'] ?? 'Custom Quiz').toString().trim(),
          url: (payload['url'] ?? localQuizUrl(id)).toString().trim().isEmpty
              ? localQuizUrl(id)
              : (payload['url'] ?? localQuizUrl(id)).toString().trim(),
          deadline: deadline,
          type: (payload['type'] ?? 'Exam').toString().trim().isEmpty
              ? 'Exam'
              : (payload['type'] ?? 'Exam').toString().trim(),
          durationMinutes: _asInt(payload['duration_minutes'], fallback: 30),
          isAiGenerated: payload['is_ai_generated'] == true,
          isUnlimitedTime:
              payload['is_unlimited_time'] == true ||
              _asInt(payload['duration_minutes'], fallback: 30) <= 0,
          uiSpec: _asMap(payload['ui_spec']),
          studentAdaptiveData: _asMap(payload['student_adaptive_data']),
        );
        final String visibility = (payload['visibility'] ?? '')
            .toString()
            .trim()
            .toLowerCase();
        final String ownerId = (payload['owner_account_id'] ?? '')
            .toString()
            .trim();
        final String ownerRole = (payload['owner_role'] ?? '')
            .toString()
            .trim()
            .toLowerCase();
        if (!_canViewerSeeQuiz(
          viewerAccountId: viewerAccountId.trim(),
          viewerRole: viewerRole.trim().toLowerCase(),
          ownerAccountId: ownerId,
          ownerRole: ownerRole,
          visibility: visibility,
        )) {
          continue;
        }
        withTimestamp.add(
          _StoredQuiz(
            item: item,
            createdAt: _asInt(payload['created_at'], fallback: 0),
          ),
        );
      } catch (_) {
        continue;
      }
    }
    withTimestamp.sort((a, b) => b.createdAt.compareTo(a.createdAt));
    return withTimestamp.map((e) => e.item).toList();
  }

  Future<List<Question>> loadQuestions(String quizId) async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final String raw = prefs.getString(_payloadKey(quizId)) ?? '';
    if (raw.trim().isEmpty) {
      return <Question>[];
    }
    try {
      final Map<String, dynamic> payload = Map<String, dynamic>.from(
        jsonDecode(raw) as Map,
      );
      final dynamic rawQuestions = payload['questions'];
      if (rawQuestions is! List) {
        return <Question>[];
      }
      final List<Question> out = <Question>[];
      for (final dynamic entry in rawQuestions) {
        if (entry is! Map) {
          continue;
        }
        final Map<String, dynamic> q = Map<String, dynamic>.from(entry);
        final List<String> options = (q['options'] is List)
            ? (q['options'] as List<dynamic>)
                  .map((dynamic e) => e.toString())
                  .toList()
            : <String>[];
        while (options.length < 4) {
          options.add('');
        }
        out.add(
          Question(
            text: (q['text'] ?? '').toString(),
            imageUrl: (q['image_url'] ?? '').toString(),
            type: (q['type'] ?? 'MCQ').toString(),
            section: (q['section'] ?? 'General').toString(),
            posMark: _asDouble(q['pos_mark'], fallback: 4),
            negMark: _asDouble(q['neg_mark'], fallback: 1),
            options: options.take(4).toList(),
            correctAnswers: (q['correct_answers'] is List)
                ? (q['correct_answers'] as List<dynamic>)
                      .map((dynamic e) => e.toString())
                      .where((String e) => e.trim().isNotEmpty)
                      .toList()
                : <String>[],
            solution: (q['solution'] ?? '').toString(),
            concept: (q['concept'] ?? '').toString(),
            difficultyLabel: (q['difficulty_label'] ?? q['difficulty'] ?? '')
                .toString(),
            confidenceScore: _asDouble(q['confidence_score'], fallback: -1),
            adaptiveScore: _asDouble(q['adaptive_score'], fallback: -1),
          ),
        );
      }
      return out;
    } catch (_) {
      return <Question>[];
    }
  }

  String _payloadKey(String quizId) => '$_payloadPrefix$quizId';

  bool _canViewerSeeQuiz({
    required String viewerAccountId,
    required String viewerRole,
    required String ownerAccountId,
    required String ownerRole,
    required String visibility,
  }) {
    if (viewerRole.isEmpty) {
      return true;
    }

    if (visibility == 'published') {
      return true;
    }

    final bool hasLegacyOwner =
        ownerAccountId.isEmpty && ownerRole.isEmpty && visibility.isEmpty;
    if (hasLegacyOwner) {
      return viewerRole == 'teacher';
    }

    if (viewerRole == 'teacher') {
      if (ownerRole == 'teacher') {
        return true;
      }
      if (ownerAccountId.isNotEmpty && viewerAccountId == ownerAccountId) {
        return true;
      }
      return false;
    }

    if (ownerAccountId.isNotEmpty && viewerAccountId == ownerAccountId) {
      return true;
    }
    return false;
  }

  Map<String, dynamic> _questionToMap(Question q) {
    return <String, dynamic>{
      'text': q.text,
      'image_url': q.imageUrl,
      'type': q.type,
      'section': q.section,
      'pos_mark': q.posMark,
      'neg_mark': q.negMark,
      'options': q.options,
      'correct_answers': q.correctAnswers,
      'solution': q.solution,
      'concept': q.concept,
      'difficulty_label': q.difficultyLabel,
      'confidence_score': q.confidenceScore,
      'adaptive_score': q.adaptiveScore,
    };
  }

  static int _asInt(dynamic value, {required int fallback}) {
    if (value is int) {
      return value;
    }
    if (value is num) {
      return value.toInt();
    }
    return int.tryParse((value ?? '').toString()) ?? fallback;
  }

  static double _asDouble(dynamic value, {required double fallback}) {
    if (value is num) {
      return value.toDouble();
    }
    return double.tryParse((value ?? '').toString()) ?? fallback;
  }

  static Map<String, dynamic> _asMap(dynamic value) {
    if (value is Map<String, dynamic>) {
      return value;
    }
    if (value is Map) {
      return Map<String, dynamic>.from(value);
    }
    if (value is String) {
      final String text = value.trim();
      if (text.isEmpty) {
        return <String, dynamic>{};
      }
      try {
        final dynamic decoded = jsonDecode(text);
        if (decoded is Map<String, dynamic>) {
          return decoded;
        }
        if (decoded is Map) {
          return Map<String, dynamic>.from(decoded);
        }
      } catch (_) {
        return <String, dynamic>{};
      }
    }
    return <String, dynamic>{};
  }
}

class _StoredQuiz {
  const _StoredQuiz({required this.item, required this.createdAt});

  final QuizItem item;
  final int createdAt;
}
