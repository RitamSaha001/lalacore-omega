import 'dart:async';
import 'dart:collection';
import 'dart:math';

import 'package:flutter/foundation.dart';

import '../models/quiz_models.dart';
import 'ai_cache_manager.dart';
import 'ai_engine_service.dart';
import 'backend_service.dart';

class AiPrecomputeProgress {
  const AiPrecomputeProgress({
    required this.quizId,
    required this.total,
    required this.completed,
    required this.failed,
    required this.running,
  });

  final String quizId;
  final int total;
  final int completed;
  final int failed;
  final bool running;

  double get fraction => total <= 0 ? 0 : (completed / total).clamp(0, 1);

  static const AiPrecomputeProgress idle = AiPrecomputeProgress(
    quizId: '',
    total: 0,
    completed: 0,
    failed: 0,
    running: false,
  );
}

class AiPrecomputeService {
  AiPrecomputeService._({
    required AiEngineService aiService,
    required BackendService backendService,
    required this.modelVersion,
    this.cacheTtl = AiCacheManager.defaultTtl,
    this.maxAttempts = 3,
  }) : _ai = aiService,
       _backend = backendService;

  static AiPrecomputeService create({
    required AiEngineService aiService,
    required BackendService backendService,
    String modelVersion = AiCacheManager.defaultModelVersion,
    Duration cacheTtl = AiCacheManager.defaultTtl,
    int maxAttempts = 3,
  }) {
    _instance ??= AiPrecomputeService._(
      aiService: aiService,
      backendService: backendService,
      modelVersion: modelVersion,
      cacheTtl: cacheTtl,
      maxAttempts: maxAttempts,
    );
    return _instance!;
  }

  static AiPrecomputeService? _instance;

  final AiEngineService _ai;
  final BackendService _backend;
  final String modelVersion;
  final Duration cacheTtl;
  final int maxAttempts;
  final Queue<_CacheJob> _queue = Queue<_CacheJob>();
  final Set<String> _queuedKeys = <String>{};
  final Map<String, DateTime> _retryAfter = <String, DateTime>{};
  int _runningWorkers = 0;
  static const int _maxWorkers = 2;

  final ValueNotifier<AiPrecomputeProgress> progress =
      ValueNotifier<AiPrecomputeProgress>(AiPrecomputeProgress.idle);

  Future<void> enqueueQuiz({
    required String quizId,
    required List<Question> questions,
    required String userId,
    bool forceRefresh = false,
  }) async {
    unawaited(AiCacheManager.instance.prune(ttl: cacheTtl));
    int enqueued = 0;
    for (int i = 0; i < questions.length; i++) {
      final Question q = questions[i];
      final String key = '$quizId#$i';
      final DateTime? retryAt = _retryAfter[key];
      if (retryAt != null && retryAt.isAfter(DateTime.now())) {
        continue;
      }
      if (_queuedKeys.contains(key)) {
        continue;
      }
      _queuedKeys.add(key);
      _queue.add(
        _CacheJob(
          quizId: quizId,
          index: i,
          question: q,
          userId: userId,
          forceRefresh: forceRefresh,
        ),
      );
      enqueued++;
    }
    if (enqueued == 0) {
      return;
    }
    progress.value = AiPrecomputeProgress(
      quizId: quizId,
      total: questions.length,
      completed: 0,
      failed: 0,
      running: true,
    );
    _pump();
  }

  Future<AiCacheEntry?> cached({
    required String quizId,
    required int questionIndex,
  }) {
    return AiCacheManager.instance.get(
      quizId: quizId,
      questionIndex: questionIndex,
      modelVersion: modelVersion,
    );
  }

  void _pump() {
    while (_runningWorkers < _maxWorkers && _queue.isNotEmpty) {
      final _CacheJob job = _queue.removeFirst();
      _runningWorkers++;
      unawaited(_process(job));
    }
  }

  Future<void> _process(_CacheJob job) async {
    try {
      final bool refreshNeeded =
          job.forceRefresh ||
          await AiCacheManager.instance.needsRefresh(
            quizId: job.quizId,
            questionIndex: job.index,
            questionText: job.question.text,
            modelVersion: modelVersion,
            ttl: cacheTtl,
          );
      if (!refreshNeeded) {
        _incrementDone(job.quizId, failed: false);
        return;
      }

      final String prompt =
          '''
Solve this exam question and return concise high-signal output.
Question: ${job.question.text}
Options: ${job.question.options.join(" | ")}
Correct Answer: ${job.question.correctAnswers.join(", ")}
Official Solution: ${job.question.solution}

Return:
1) Answer
2) Explanation
3) Confidence score from 0 to 1
''';

      Map<String, dynamic>? response;
      Object? lastError;
      for (int attempt = 0; attempt < maxAttempts; attempt++) {
        try {
          response = await _ai.sendChat(
            prompt: prompt,
            userId: job.userId,
            chatId: 'AI_PRECOMPUTE_${job.quizId}',
            function: 'answer_precompute',
            responseStyle: 'structured_exam_solution',
            enablePersona: false,
            card: <String, dynamic>{
              'quiz_id': job.quizId,
              'question_index': job.index,
              'precompute': true,
            },
          );
          break;
        } catch (e) {
          lastError = e;
          final int ms = min(4200, 350 * (1 << attempt));
          await Future<void>.delayed(Duration(milliseconds: ms));
        }
      }
      if (response == null) {
        throw lastError ?? StateError('Precompute failed');
      }

      final String answer = (response['answer'] ?? '').toString().trim();
      final String explanation = (response['explanation'] ?? '')
          .toString()
          .trim();
      final double confidence = _parseConfidence(response['confidence']);
      final String concept = (response['concept'] ?? '').toString().trim();

      final AiCacheEntry entry = AiCacheEntry(
        quizId: job.quizId,
        questionIndex: job.index,
        modelVersion: modelVersion,
        questionHash: AiCacheManager.hashQuestion(job.question.text),
        questionText: job.question.text,
        answer: answer,
        explanation: explanation,
        confidence: confidence,
        concept: concept,
        metadata: <String, dynamic>{
          'source': 'precompute',
          'updated_via': 'ai_precompute_service',
          'model_version': modelVersion,
        },
        updatedAtMs: DateTime.now().millisecondsSinceEpoch,
      );
      await AiCacheManager.instance.upsert(entry);
      _retryAfter.remove(job.key);
      unawaited(_syncRemote(entry));
      _incrementDone(job.quizId, failed: false);
    } catch (_) {
      final int attempts = max(1, min(maxAttempts, 6));
      final int cooldownMs = min(15 * 60 * 1000, 500 * (1 << attempts));
      _retryAfter[job.key] = DateTime.now().add(
        Duration(milliseconds: cooldownMs),
      );
      _incrementDone(job.quizId, failed: true);
    } finally {
      _queuedKeys.remove(job.key);
      _runningWorkers--;
      _pump();
    }
  }

  Future<void> _syncRemote(AiCacheEntry entry) async {
    try {
      await _backend.postJsonActionWithFallback(<Map<String, dynamic>>[
        <String, dynamic>{
          'action': 'ai_cache_upsert',
          'quiz_id': entry.quizId,
          'question_index': entry.questionIndex,
          'model_version': entry.modelVersion,
          'question_hash': entry.questionHash,
          'question_text': entry.questionText,
          'answer': entry.answer,
          'explanation': entry.explanation,
          'confidence': entry.confidence,
          'concept': entry.concept,
          'metadata': entry.metadata,
        },
        <String, dynamic>{
          'action': 'cache_ai_answer',
          'quiz_id': entry.quizId,
          'question_index': entry.questionIndex,
          'model_version': entry.modelVersion,
          'question_hash': entry.questionHash,
          'answer': entry.answer,
          'explanation': entry.explanation,
          'confidence': entry.confidence,
          'concept': entry.concept,
        },
      ]);
    } catch (_) {}
  }

  double _parseConfidence(dynamic value) {
    if (value is num) {
      return value.toDouble().clamp(0, 1);
    }
    final String t = (value ?? '').toString().trim().toLowerCase();
    final double? parsed = double.tryParse(t);
    if (parsed != null) {
      return parsed.clamp(0, 1);
    }
    if (t.contains('high')) {
      return 0.88;
    }
    if (t.contains('medium')) {
      return 0.62;
    }
    if (t.contains('low')) {
      return 0.35;
    }
    return 0.5;
  }

  void _incrementDone(String quizId, {required bool failed}) {
    final AiPrecomputeProgress current = progress.value;
    final int nextCompleted = current.completed + 1;
    final int nextFailed = current.failed + (failed ? 1 : 0);
    final bool running = nextCompleted < current.total;
    progress.value = AiPrecomputeProgress(
      quizId: quizId,
      total: current.total,
      completed: nextCompleted,
      failed: nextFailed,
      running: running,
    );
  }
}

class _CacheJob {
  const _CacheJob({
    required this.quizId,
    required this.index,
    required this.question,
    required this.userId,
    required this.forceRefresh,
  });

  final String quizId;
  final int index;
  final Question question;
  final String userId;
  final bool forceRefresh;

  String get key => '$quizId#$index';
}
