import 'dart:convert';

import 'package:path/path.dart' as p;
import 'package:sqflite/sqflite.dart';

class AiCacheEntry {
  const AiCacheEntry({
    required this.quizId,
    required this.questionIndex,
    required this.modelVersion,
    required this.questionHash,
    required this.questionText,
    required this.answer,
    required this.explanation,
    required this.confidence,
    required this.concept,
    required this.metadata,
    required this.updatedAtMs,
  });

  final String quizId;
  final int questionIndex;
  final String modelVersion;
  final String questionHash;
  final String questionText;
  final String answer;
  final String explanation;
  final double confidence;
  final String concept;
  final Map<String, dynamic> metadata;
  final int updatedAtMs;

  String get cacheKey => '$quizId#$questionIndex#$modelVersion';

  Map<String, Object?> toRow() => <String, Object?>{
    'cache_key': cacheKey,
    'quiz_id': quizId,
    'question_index': questionIndex,
    'model_version': modelVersion,
    'question_hash': questionHash,
    'question_text': questionText,
    'answer': answer,
    'explanation': explanation,
    'confidence': confidence,
    'concept': concept,
    'metadata': jsonEncode(metadata),
    'updated_at': updatedAtMs,
  };

  factory AiCacheEntry.fromRow(Map<String, Object?> row) {
    return AiCacheEntry(
      quizId: (row['quiz_id'] ?? '').toString(),
      questionIndex: (row['question_index'] ?? 0) as int,
      modelVersion: (row['model_version'] ?? '').toString(),
      questionHash: (row['question_hash'] ?? '').toString(),
      questionText: (row['question_text'] ?? '').toString(),
      answer: (row['answer'] ?? '').toString(),
      explanation: (row['explanation'] ?? '').toString(),
      confidence: ((row['confidence'] ?? 0) as num).toDouble(),
      concept: (row['concept'] ?? '').toString(),
      metadata: row['metadata'] == null
          ? const <String, dynamic>{}
          : Map<String, dynamic>.from(
              jsonDecode((row['metadata'] ?? '{}').toString()) as Map,
            ),
      updatedAtMs: ((row['updated_at'] ?? 0) as num).toInt(),
    );
  }
}

class AiCacheManager {
  AiCacheManager._();
  static final AiCacheManager instance = AiCacheManager._();
  static const String defaultModelVersion = 'lalacore-v10';
  static const Duration defaultTtl = Duration(days: 21);

  Database? _db;

  Future<Database> _open() async {
    if (_db != null) {
      return _db!;
    }
    final String base = await getDatabasesPath();
    final String path = p.join(base, 'lalacore_ai_cache.db');
    _db = await openDatabase(
      path,
      version: 2,
      onCreate: (Database db, int version) async {
        await db.execute('''
          CREATE TABLE IF NOT EXISTS ai_question_cache (
            cache_key TEXT PRIMARY KEY,
            quiz_id TEXT NOT NULL,
            question_index INTEGER NOT NULL,
            model_version TEXT NOT NULL,
            question_hash TEXT NOT NULL,
            question_text TEXT NOT NULL,
            answer TEXT NOT NULL,
            explanation TEXT NOT NULL,
            confidence REAL NOT NULL,
            concept TEXT NOT NULL,
            metadata TEXT NOT NULL,
            updated_at INTEGER NOT NULL
          )
        ''');
        await db.execute(
          'CREATE INDEX IF NOT EXISTS idx_ai_cache_quiz ON ai_question_cache(quiz_id)',
        );
        await db.execute(
          'CREATE INDEX IF NOT EXISTS idx_ai_cache_lookup ON ai_question_cache(quiz_id, question_index, model_version)',
        );
      },
      onUpgrade: (Database db, int oldVersion, int newVersion) async {
        if (oldVersion < 2) {
          await db.execute(
            "ALTER TABLE ai_question_cache ADD COLUMN model_version TEXT NOT NULL DEFAULT '${AiCacheManager.defaultModelVersion}'",
          );
          await db.execute(
            'CREATE INDEX IF NOT EXISTS idx_ai_cache_lookup ON ai_question_cache(quiz_id, question_index, model_version)',
          );
        }
      },
    );
    return _db!;
  }

  Future<AiCacheEntry?> get({
    required String quizId,
    required int questionIndex,
    String modelVersion = defaultModelVersion,
  }) async {
    final Database db = await _open();
    final List<Map<String, Object?>> rows = await db.query(
      'ai_question_cache',
      where: 'quiz_id = ? AND question_index = ? AND model_version = ?',
      whereArgs: <Object>[quizId, questionIndex, modelVersion],
      orderBy: 'updated_at DESC',
      limit: 1,
    );
    if (rows.isEmpty) {
      return null;
    }
    return AiCacheEntry.fromRow(rows.first);
  }

  Future<void> upsert(AiCacheEntry entry) async {
    final Database db = await _open();
    await db.insert(
      'ai_question_cache',
      entry.toRow(),
      conflictAlgorithm: ConflictAlgorithm.replace,
    );
  }

  Future<bool> needsRefresh({
    required String quizId,
    required int questionIndex,
    required String questionText,
    String modelVersion = defaultModelVersion,
    Duration ttl = defaultTtl,
  }) async {
    final AiCacheEntry? current = await get(
      quizId: quizId,
      questionIndex: questionIndex,
      modelVersion: modelVersion,
    );
    if (current == null) {
      return true;
    }
    final String hash = hashQuestion(questionText);
    if (current.questionHash != hash) {
      return true;
    }
    final int ageMs =
        DateTime.now().millisecondsSinceEpoch - current.updatedAtMs;
    return ageMs > ttl.inMilliseconds;
  }

  Future<void> clearQuiz(String quizId) async {
    final Database db = await _open();
    await db.delete(
      'ai_question_cache',
      where: 'quiz_id = ?',
      whereArgs: <Object>[quizId],
    );
  }

  Future<void> prune({Duration ttl = defaultTtl}) async {
    final Database db = await _open();
    final int threshold = DateTime.now().subtract(ttl).millisecondsSinceEpoch;
    await db.delete(
      'ai_question_cache',
      where: 'updated_at < ?',
      whereArgs: <Object>[threshold],
    );
  }

  static String hashQuestion(String text) {
    int h = 5381;
    for (final int code in text.runes) {
      h = ((h << 5) + h) ^ code;
    }
    final int val = h & 0x7fffffff;
    return val.toRadixString(16);
  }
}
