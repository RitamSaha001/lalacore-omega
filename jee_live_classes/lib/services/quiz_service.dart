import '../core/app_config.dart';
import '../models/extracted_practice_question_model.dart';
import '../models/live_poll_model.dart';
import 'secure_api_client.dart';

class QuizQuestion {
  const QuizQuestion({
    required this.id,
    required this.question,
    required this.options,
  });

  final String id;
  final String question;
  final List<String> options;
}

class QuizSession {
  const QuizSession({
    required this.quizId,
    required this.question,
    required this.correctIndex,
  });

  final String quizId;
  final QuizQuestion question;
  final int correctIndex;
}

class QuizResult {
  const QuizResult({
    required this.totalResponses,
    required this.correctResponses,
  });

  final int totalResponses;
  final int correctResponses;
}

class QuizService {
  const QuizService({required this.config, required this.apiClient});

  final AppConfig config;
  final SecureApiClient apiClient;

  Future<QuizSession> createQuiz({
    required String classId,
    required List<String> concepts,
  }) async {
    final response = await apiClient.postJson(
      config.quizApiUri(config.quizCreateEndpoint),
      {'class_id': classId, 'concepts': concepts},
    );

    return QuizSession(
      quizId:
          response['quiz_id']?.toString() ??
          'quiz_${DateTime.now().millisecondsSinceEpoch}',
      question: QuizQuestion(
        id: response['question_id']?.toString() ?? 'q1',
        question: response['question']?.toString() ?? 'Question unavailable',
        options: (response['options'] is List)
            ? (response['options'] as List)
                  .map((item) => item.toString())
                  .toList(growable: false)
            : const ['A', 'B', 'C', 'D'],
      ),
      correctIndex: (response['correct_index'] as num?)?.toInt() ?? 0,
    );
  }

  Future<void> startQuiz({
    required String classId,
    required String quizId,
  }) async {
    await apiClient.postJson(config.quizApiUri(config.quizStartEndpoint), {
      'class_id': classId,
      'quiz_id': quizId,
    });
  }

  Future<void> submitAnswer({
    required String quizId,
    required String participantId,
    required int selectedIndex,
  }) async {
    await apiClient.postJson(config.quizApiUri(config.quizSubmitEndpoint), {
      'quiz_id': quizId,
      'participant_id': participantId,
      'selected_index': selectedIndex,
    });
  }

  Future<QuizResult> fetchResults(String quizId) async {
    final response = await apiClient.getJson(
      config.quizApiUri(
        config.quizResultsEndpoint,
        queryParameters: {'quiz_id': quizId},
      ),
    );

    return QuizResult(
      totalResponses: (response['total_responses'] as num?)?.toInt() ?? 0,
      correctResponses: (response['correct_responses'] as num?)?.toInt() ?? 0,
    );
  }

  Future<LivePollModel> createLivePoll({
    required String classId,
    required LivePollDraft draft,
  }) async {
    final response = await apiClient
        .postJson(config.quizApiUri(config.livePollCreateEndpoint), {
          'class_id': classId,
          'question': draft.question,
          'options': draft.options,
          'correct_option': draft.correctOption,
          'timer_seconds': draft.timerSeconds,
        });

    return LivePollModel(
      pollId:
          response['poll_id']?.toString() ??
          'poll_${DateTime.now().millisecondsSinceEpoch}',
      question: response['question']?.toString() ?? draft.question,
      options: _pollOptionsFrom(response['options'], fallback: draft.options),
      correctOption:
          (response['correct_option'] as num?)?.toInt() ?? draft.correctOption,
      timerSeconds:
          (response['timer_seconds'] as num?)?.toInt() ?? draft.timerSeconds,
      startTime:
          DateTime.tryParse(response['start_time']?.toString() ?? '') ??
          DateTime.now(),
      status: _pollStatusFrom(response['status']?.toString()),
    );
  }

  Future<void> submitLivePollAnswer({
    required String pollId,
    required String participantId,
    required int selectedIndex,
  }) async {
    await apiClient.postJson(config.quizApiUri(config.livePollSubmitEndpoint), {
      'poll_id': pollId,
      'participant_id': participantId,
      'selected_index': selectedIndex,
    });
  }

  Future<LivePollResultsModel> fetchLivePollResults({
    required String pollId,
    required int optionCount,
  }) async {
    final response = await apiClient.getJson(
      config.quizApiUri(
        config.livePollResultsEndpoint,
        queryParameters: {'poll_id': pollId},
      ),
    );

    final counts = _parsePollCounts(response['option_counts'], optionCount);
    final totalFromCounts = counts.values.fold<int>(
      0,
      (sum, item) => sum + item,
    );

    return LivePollResultsModel(
      pollId: pollId,
      optionCounts: counts,
      totalResponses:
          (response['total_responses'] as num?)?.toInt() ?? totalFromCounts,
      correctOption: (response['correct_option'] as num?)?.toInt(),
      revealed: response['revealed'] == true,
    );
  }

  Future<void> endLivePoll(String pollId) async {
    await apiClient.postJson(config.quizApiUri(config.livePollEndEndpoint), {
      'poll_id': pollId,
    });
  }

  Future<List<LivePollDraft>> fetchImportablePolls({
    required String classId,
    int limit = 10,
  }) async {
    final response = await apiClient.getJson(
      config.quizApiUri(
        config.quizLibraryEndpoint,
        queryParameters: {'class_id': classId, 'limit': limit},
      ),
    );

    final list = response['questions'];
    if (list is! List) {
      return const [];
    }

    return list
        .whereType<Map>()
        .map((item) {
          final options = _pollOptionsFrom(item['options']);
          return LivePollDraft(
            question: item['question']?.toString() ?? 'Imported question',
            options: options.length >= 2
                ? options
                : const ['Option A', 'Option B'],
            timerSeconds: (item['timer_seconds'] as num?)?.toInt() ?? 20,
            correctOption: (item['correct_option'] as num?)?.toInt(),
            topic: item['topic']?.toString(),
            difficulty: item['difficulty']?.toString(),
          );
        })
        .toList(growable: false);
  }

  Future<void> saveExtractedPracticeQuestion({
    required String classId,
    required ExtractedPracticeQuestionModel question,
  }) async {
    await apiClient
        .postJson(config.quizApiUri(config.practiceExtractEndpoint), {
          'class_id': classId,
          'question_id': question.id,
          'question': question.question,
          'solution_steps': question.solutionSteps,
          'final_answer': question.finalAnswer,
          'concept_tags': question.conceptTags,
          'difficulty': question.difficulty,
          'timestamp_seconds': question.timestampSeconds,
          'created_at': question.createdAt.toIso8601String(),
          'review_status': _reviewStatusToWire(question.reviewStatus),
          'reviewed_by': question.reviewedBy,
          'reviewed_at': question.reviewedAt?.toIso8601String(),
          'reviewer_comment': question.reviewerComment,
          'edited_question': question.editedQuestion,
        });
  }

  Future<List<ExtractedPracticeQuestionModel>> fetchPracticeReviewQueue({
    required String classId,
    int limit = 100,
  }) async {
    final response = await apiClient.getJson(
      config.quizApiUri(
        config.practiceReviewQueueEndpoint,
        queryParameters: {'class_id': classId, 'limit': limit},
      ),
    );
    final items = response['items'];
    if (items is! List) {
      return const [];
    }
    return items
        .whereType<Map>()
        .map((item) => _practiceQuestionFromMap(item))
        .whereType<ExtractedPracticeQuestionModel>()
        .toList(growable: false);
  }

  Future<ExtractedPracticeQuestionModel?> reviewPracticeQuestion({
    required String classId,
    required String questionId,
    required ExtractedQuestionReviewStatus status,
    String? editedQuestion,
    String? reviewerComment,
  }) async {
    final response = await apiClient
        .postJson(config.quizApiUri(config.practiceReviewActionEndpoint), {
          'class_id': classId,
          'question_id': questionId,
          'status': _reviewStatusToWire(status),
          if (editedQuestion != null && editedQuestion.trim().isNotEmpty)
            'edited_question': editedQuestion.trim(),
          if (reviewerComment != null && reviewerComment.trim().isNotEmpty)
            'reviewer_comment': reviewerComment.trim(),
        });
    if (response.isEmpty) {
      return null;
    }
    final item = response['item'];
    if (item is Map) {
      return _practiceQuestionFromMap(item);
    }
    return null;
  }

  LivePollStatus _pollStatusFrom(String? raw) {
    switch (raw) {
      case 'active':
        return LivePollStatus.active;
      case 'ended':
        return LivePollStatus.ended;
      default:
        return LivePollStatus.draft;
    }
  }

  List<String> _pollOptionsFrom(dynamic raw, {List<String>? fallback}) {
    if (raw is List) {
      final options = raw
          .map((item) => item.toString())
          .toList(growable: false);
      if (options.isNotEmpty) {
        return options;
      }
    }
    return fallback ?? const ['Option A', 'Option B', 'Option C', 'Option D'];
  }

  Map<int, int> _parsePollCounts(dynamic raw, int optionCount) {
    final defaultCounts = {
      for (var index = 0; index < optionCount; index += 1) index: 0,
    };

    if (raw is List) {
      final counts = Map<int, int>.from(defaultCounts);
      for (
        var index = 0;
        index < raw.length && index < optionCount;
        index += 1
      ) {
        counts[index] = (raw[index] as num?)?.toInt() ?? 0;
      }
      return counts;
    }

    if (raw is Map) {
      final counts = Map<int, int>.from(defaultCounts);
      for (var index = 0; index < optionCount; index += 1) {
        final key = index.toString();
        counts[index] = (raw[key] as num?)?.toInt() ?? 0;
      }
      return counts;
    }

    return defaultCounts;
  }

  String _reviewStatusToWire(ExtractedQuestionReviewStatus status) {
    switch (status) {
      case ExtractedQuestionReviewStatus.pending:
        return 'pending';
      case ExtractedQuestionReviewStatus.approved:
        return 'approved';
      case ExtractedQuestionReviewStatus.rejected:
        return 'rejected';
      case ExtractedQuestionReviewStatus.edited:
        return 'edited';
    }
  }

  ExtractedQuestionReviewStatus _reviewStatusFromWire(String raw) {
    switch (raw) {
      case 'approved':
        return ExtractedQuestionReviewStatus.approved;
      case 'rejected':
        return ExtractedQuestionReviewStatus.rejected;
      case 'edited':
        return ExtractedQuestionReviewStatus.edited;
      default:
        return ExtractedQuestionReviewStatus.pending;
    }
  }

  ExtractedPracticeQuestionModel? _practiceQuestionFromMap(Map raw) {
    final id = raw['question_id']?.toString() ?? raw['id']?.toString() ?? '';
    final question = raw['question']?.toString() ?? '';
    if (id.isEmpty || question.isEmpty) {
      return null;
    }

    final stepsRaw = raw['solution_steps'];
    final tagsRaw = raw['concept_tags'];

    return ExtractedPracticeQuestionModel(
      id: id,
      question: question,
      solutionSteps: stepsRaw is List
          ? stepsRaw.map((item) => item.toString()).toList(growable: false)
          : const [],
      finalAnswer: raw['final_answer']?.toString() ?? '',
      conceptTags: tagsRaw is List
          ? tagsRaw.map((item) => item.toString()).toList(growable: false)
          : const [],
      difficulty: raw['difficulty']?.toString() ?? 'easy',
      timestampSeconds: (raw['timestamp_seconds'] as num?)?.toInt() ?? 0,
      createdAt:
          DateTime.tryParse(raw['created_at']?.toString() ?? '') ??
          DateTime.now(),
      reviewStatus: _reviewStatusFromWire(
        raw['review_status']?.toString() ?? '',
      ),
      reviewedBy: raw['reviewed_by']?.toString(),
      reviewedAt: DateTime.tryParse(raw['reviewed_at']?.toString() ?? ''),
      reviewerComment: raw['reviewer_comment']?.toString(),
      editedQuestion: raw['edited_question']?.toString(),
    );
  }
}
