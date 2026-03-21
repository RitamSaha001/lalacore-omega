import 'dart:convert';

class QuizItem {
  const QuizItem({
    required this.id,
    required this.title,
    required this.url,
    required this.deadline,
    required this.type,
    required this.durationMinutes,
    this.isAiGenerated = false,
    this.isUnlimitedTime = false,
    this.uiSpec = const <String, dynamic>{},
    this.studentAdaptiveData = const <String, dynamic>{},
  });

  final String id;
  final String title;
  final String url;
  final DateTime deadline;
  final String type;
  final int durationMinutes;
  final bool isAiGenerated;
  final bool isUnlimitedTime;
  final Map<String, dynamic> uiSpec;
  final Map<String, dynamic> studentAdaptiveData;
}

class Question {
  const Question({
    required this.text,
    required this.imageUrl,
    required this.type,
    required this.section,
    required this.posMark,
    required this.negMark,
    required this.options,
    required this.correctAnswers,
    this.solution = '',
    this.concept = '',
    this.difficultyLabel = '',
    this.confidenceScore = -1,
    this.adaptiveScore = -1,
  });

  final String text;
  final String imageUrl;
  final String type;
  final String section;
  final double posMark;
  final double negMark;
  final List<String> options;
  final List<String> correctAnswers;
  final String solution;
  final String concept;
  final String difficultyLabel;
  final double confidenceScore;
  final double adaptiveScore;
}

class ResultData {
  ResultData({
    required this.quizId,
    required this.quizTitle,
    required this.score,
    required this.maxScore,
    required this.correct,
    required this.wrong,
    required this.skipped,
    required this.totalTime,
    required this.sectionAccuracy,
    required this.userAnswers,
  });

  String quizId;
  String quizTitle;
  double score;
  double maxScore;
  int correct;
  int wrong;
  int skipped;
  int totalTime;
  Map<String, double> sectionAccuracy;
  Map<int, List<String>> userAnswers;

  Map<String, dynamic> toJson() => <String, dynamic>{
    'quizId': quizId,
    'quizTitle': quizTitle,
    'score': score,
    'maxScore': maxScore,
    'correct': correct,
    'wrong': wrong,
    'skipped': skipped,
    'totalTime': totalTime,
    'sectionAccuracy': sectionAccuracy,
    'userAnswers': userAnswers.map(
      (int k, List<String> v) => MapEntry(k.toString(), v),
    ),
  };

  factory ResultData.fromJson(Map<String, dynamic> json) {
    final Map rawAnswers = (json['userAnswers'] as Map?) ?? <String, dynamic>{};
    return ResultData(
      quizId: (json['quizId'] ?? '').toString(),
      quizTitle: (json['quizTitle'] ?? 'Quiz').toString(),
      score: _toDouble(json['score']),
      maxScore: _toDouble(json['maxScore']),
      correct: (json['correct'] ?? 0) as int,
      wrong: (json['wrong'] ?? 0) as int,
      skipped: (json['skipped'] ?? 0) as int,
      totalTime: (json['totalTime'] ?? 0) as int,
      sectionAccuracy: Map<String, double>.from(
        (json['sectionAccuracy'] ?? <String, double>{}) as Map,
      ),
      userAnswers: rawAnswers.map(
        (dynamic k, dynamic v) =>
            MapEntry(int.parse(k.toString()), List<String>.from(v as List)),
      ),
    );
  }
}

class AutoSaveExam {
  AutoSaveExam({
    required this.quizId,
    required this.currentIndex,
    required this.remainingSeconds,
    required this.answers,
    required this.review,
    required this.visited,
  });

  final String quizId;
  final int currentIndex;
  final int remainingSeconds;
  final Map<int, List<String>> answers;
  final Set<int> review;
  final Set<int> visited;

  Map<String, dynamic> toJson() => <String, dynamic>{
    'quizId': quizId,
    'currentIndex': currentIndex,
    'remainingSeconds': remainingSeconds,
    'answers': answers.map(
      (int k, List<String> v) => MapEntry(k.toString(), v),
    ),
    'review': review.toList(),
    'visited': visited.toList(),
  };

  factory AutoSaveExam.fromJson(Map<String, dynamic> j) {
    final Map rawAnswers = (j['answers'] as Map?) ?? <String, dynamic>{};
    return AutoSaveExam(
      quizId: (j['quizId'] ?? '').toString(),
      currentIndex: (j['currentIndex'] ?? 0) as int,
      remainingSeconds: (j['remainingSeconds'] ?? 0) as int,
      answers: rawAnswers.map(
        (dynamic k, dynamic v) =>
            MapEntry(int.parse(k.toString()), List<String>.from(v as List)),
      ),
      review: Set<int>.from((j['review'] ?? <int>[]) as List),
      visited: Set<int>.from((j['visited'] ?? <int>[]) as List),
    );
  }
}

class StudyMaterialItem {
  const StudyMaterialItem({
    required this.chapter,
    required this.title,
    required this.type,
    required this.url,
    required this.time,
    required this.materialId,
    this.subject = '',
    this.className = '',
    this.description = '',
    this.notes = '',
  });

  final String chapter;
  final String title;
  final String type;
  final String url;
  final String time;
  final String materialId;
  final String subject;
  final String className;
  final String description;
  final String notes;

  factory StudyMaterialItem.fromJson(Map<String, dynamic> j) {
    final String chapter = (j['chapter'] ?? j['chapters'] ?? '').toString();
    final String time = _materialDisplayTime(
      j['time'] ?? j['created_at'] ?? j['updated_at'] ?? '',
    );
    return StudyMaterialItem(
      chapter: chapter,
      title: (j['title'] ?? '').toString(),
      type: (j['type'] ?? '').toString(),
      url: (j['url'] ?? '').toString(),
      time: time,
      materialId: (j['material_id'] ?? '').toString(),
      subject: (j['subject'] ?? '').toString(),
      className: (j['class'] ?? j['class_name'] ?? j['target_class'] ?? '')
          .toString(),
      description: (j['description'] ?? '').toString(),
      notes: (j['notes'] ?? '').toString(),
    );
  }

  static String _materialDisplayTime(dynamic raw) {
    final String text = (raw ?? '').toString().trim();
    if (text.isEmpty) {
      return '';
    }
    final int? asInt = int.tryParse(text);
    if (asInt != null) {
      final int ms = asInt > 9999999999 ? asInt : asInt * 1000;
      final DateTime dt = DateTime.fromMillisecondsSinceEpoch(ms).toLocal();
      return _ddmmyyyy(dt);
    }
    final DateTime? parsed = DateTime.tryParse(text);
    if (parsed != null) {
      return _ddmmyyyy(parsed.toLocal());
    }
    return text;
  }

  static String _ddmmyyyy(DateTime dt) {
    final String dd = dt.day.toString().padLeft(2, '0');
    final String mm = dt.month.toString().padLeft(2, '0');
    final String yyyy = dt.year.toString();
    return '$dd/$mm/$yyyy';
  }
}

class AppNotification {
  AppNotification({
    required this.id,
    required this.title,
    required this.body,
    required this.type,
    required this.created,
    required this.seen,
    this.payload,
  });

  final String id;
  final String title;
  final String body;
  final String type;
  final DateTime created;
  final bool seen;
  final Map<String, dynamic>? payload;

  AppNotification copyWith({bool? seen}) {
    return AppNotification(
      id: id,
      title: title,
      body: body,
      type: type,
      created: created,
      seen: seen ?? this.seen,
      payload: payload,
    );
  }

  Map<String, dynamic> toJson() => <String, dynamic>{
    'id': id,
    'title': title,
    'body': body,
    'type': type,
    'created': created.toIso8601String(),
    'seen': seen,
    'payload': payload,
  };

  factory AppNotification.fromJson(Map<String, dynamic> j) {
    return AppNotification(
      id: (j['id'] ?? '').toString(),
      title: (j['title'] ?? '').toString(),
      body: (j['body'] ?? '').toString(),
      type: (j['type'] ?? 'ai').toString(),
      created:
          DateTime.tryParse((j['created'] ?? '').toString()) ?? DateTime.now(),
      seen: j['seen'] == true,
      payload: j['payload'] is Map
          ? Map<String, dynamic>.from(j['payload'] as Map)
          : null,
    );
  }
}

class ChatMessage {
  ChatMessage({
    required this.role,
    required this.text,
    this.meta,
    this.id,
    this.confidence,
    this.concept,
    this.visualization,
    this.webRetrieval,
    this.mctsSearch,
    this.reasoningGraph,
    this.citationMap,
    this.evidence,
  });

  final String role;
  final String text;
  final String? meta;
  final String? id;
  final String? confidence;
  final String? concept;
  final Map<String, dynamic>? visualization;
  final Map<String, dynamic>? webRetrieval;
  final Map<String, dynamic>? mctsSearch;
  final Map<String, dynamic>? reasoningGraph;
  final List<Map<String, dynamic>>? citationMap;
  final Map<String, dynamic>? evidence;

  Map<String, dynamic> toJson() => <String, dynamic>{
    if (id != null) 'id': id,
    'role': role,
    'content': text,
    if (meta != null) 'meta': meta,
    if (confidence != null) 'confidence': confidence,
    if (concept != null) 'concept': concept,
    if (visualization != null) 'visualization': visualization,
    if (webRetrieval != null) 'web_retrieval': webRetrieval,
    if (mctsSearch != null) 'mcts_search': mctsSearch,
    if (reasoningGraph != null) 'reasoning_graph': reasoningGraph,
    if (citationMap != null) 'citation_map': citationMap,
    if (evidence != null) 'evidence': evidence,
  };

  factory ChatMessage.fromJson(Map<String, dynamic> json) {
    return ChatMessage(
      id: json['id']?.toString(),
      role: (json['role'] ?? 'assistant').toString(),
      text: (json['content'] ?? json['text'] ?? '').toString(),
      meta: json['meta']?.toString(),
      confidence: json['confidence']?.toString(),
      concept: json['concept']?.toString(),
      visualization: _decodeMap(json['visualization']),
      webRetrieval: _decodeMap(json['web_retrieval']),
      mctsSearch: _decodeMap(json['mcts_search']),
      reasoningGraph: _decodeMap(json['reasoning_graph']),
      citationMap: _decodeMapList(json['citation_map']),
      evidence: _decodeMap(json['evidence']),
    );
  }
}

double _toDouble(dynamic value) {
  if (value is num) {
    return value.toDouble();
  }
  return double.tryParse(value?.toString() ?? '') ?? 0;
}

String serializeResult(ResultData result) => jsonEncode(result.toJson());

Map<String, dynamic>? _decodeMap(dynamic raw) {
  if (raw is Map<String, dynamic>) {
    return raw;
  }
  if (raw is Map) {
    return Map<String, dynamic>.from(raw);
  }
  if (raw is String && raw.trim().startsWith('{')) {
    try {
      final dynamic decoded = jsonDecode(raw);
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

List<Map<String, dynamic>>? _decodeMapList(dynamic raw) {
  if (raw is List) {
    return raw
        .whereType<Map>()
        .map((Map row) => Map<String, dynamic>.from(row))
        .toList();
  }
  if (raw is String && raw.trim().startsWith('[')) {
    try {
      final dynamic decoded = jsonDecode(raw);
      if (decoded is List) {
        return decoded
            .whereType<Map>()
            .map((Map row) => Map<String, dynamic>.from(row))
            .toList();
      }
    } catch (_) {}
  }
  return null;
}
