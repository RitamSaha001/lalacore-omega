import '../../core/app_config.dart';
import '../../models/live_poll_model.dart';
import '../../models/lecture_index_model.dart';
import '../../models/transcript_model.dart';
import '../../services/secure_api_client.dart';

class AiRequestContext {
  const AiRequestContext({
    required this.transcript,
    required this.chatMessages,
    required this.ocrSnippets,
    required this.lectureMaterials,
    required this.detectedConcepts,
    required this.timestamps,
  });

  final List<TranscriptModel> transcript;
  final List<String> chatMessages;
  final List<String> ocrSnippets;
  final List<String> lectureMaterials;
  final List<String> detectedConcepts;
  final List<int> timestamps;

  Map<String, dynamic> toJson() {
    final transcriptJoined = transcript
        .map((item) => item.message)
        .where((item) => item.trim().isNotEmpty)
        .join(' ');
    final ocrJoined = ocrSnippets.join(' ');
    final combined = '$transcriptJoined $ocrJoined';
    final containsBengali = RegExp(r'[\u0980-\u09FF]').hasMatch(combined);
    return {
      'transcript': transcript
          .map(
            (item) => {
              'speaker': item.speakerName,
              'message': item.message,
              'timestamp': item.timestamp.toIso8601String(),
              'confidence': item.confidence,
            },
          )
          .toList(growable: false),
      'chat_messages': chatMessages,
      'ocr_snippets': ocrSnippets,
      'lecture_materials': lectureMaterials,
      'lecture_concepts': detectedConcepts,
      'timestamps': timestamps,
      'language_profile': {
        'mixed_bengali_english_possible': true,
        'bengali_script_detected': containsBengali,
        'preferred_output_language': 'english',
        'preserve_math_notation': true,
        'handwritten_board_content_possible': true,
      },
    };
  }
}

class ClassNotesPayload {
  const ClassNotesPayload({
    required this.keyConcepts,
    required this.formulas,
    required this.shortcuts,
    required this.commonMistakes,
  });

  final List<String> keyConcepts;
  final List<String> formulas;
  final List<String> shortcuts;
  final List<String> commonMistakes;
}

class HomeworkPack {
  const HomeworkPack({
    required this.easy,
    required this.medium,
    required this.hard,
  });

  final List<String> easy;
  final List<String> medium;
  final List<String> hard;
}

class LalacoreApi {
  // BEGIN_PHASE2_IMPLEMENTATION
  const LalacoreApi({
    required this.config,
    required this.apiClient,
    this.useMockResponses = true,
  });

  final AppConfig config;
  final SecureApiClient apiClient;
  final bool useMockResponses;

  static const String _multilingualInstruction =
      'The live classroom transcript and board OCR may contain mixed Bengali '
      'and English, plus handwritten math or English text. Understand both '
      'languages, preserve equations and symbols exactly, and produce clean '
      'final output in English unless the user explicitly asks otherwise.';

  Future<String> askLalacore({
    required String prompt,
    required AiRequestContext context,
  }) async {
    if (useMockResponses) {
      return _mockAnswer(prompt: prompt, context: context);
    }

    final response = await apiClient.postJson(
      config.lalacoreUri(config.aiExplainEndpoint),
      {
        'prompt': '$_multilingualInstruction\n\n$prompt',
        'context': context.toJson(),
        'instruction': _multilingualInstruction,
      },
    );

    return response['answer']?.toString() ?? 'No response from AI backend.';
  }

  Future<Stream<String>> streamExplain({
    required String prompt,
    required AiRequestContext context,
  }) async {
    if (useMockResponses) {
      return Stream<String>.value(
        _mockAnswer(prompt: prompt, context: context),
      );
    }

    return apiClient.postStreaming(
      config.lalacoreUri(config.aiExplainEndpoint),
      {
        'prompt': '$_multilingualInstruction\n\n$prompt',
        'context': context.toJson(),
        'instruction': _multilingualInstruction,
        'stream': true,
      },
    );
  }

  Future<ClassNotesPayload> generateNotes({
    required AiRequestContext context,
  }) async {
    if (useMockResponses) {
      final topic = _inferTopic(context);
      return ClassNotesPayload(
        keyConcepts: [
          '$topic core idea and problem setup',
          '$topic exam pattern and common question framing',
        ],
        formulas: [
          _mockFormula(topic, context),
          'Always verify conditions, notation, and edge cases before finalizing.',
        ],
        shortcuts: [
          'Convert the lecture explanation into a 2-step solving template.',
          'Pause before substitution and check the governing assumption first.',
        ],
        commonMistakes: [
          'Jumping to a familiar pattern without validating the setup.',
          'Ignoring restrictions, sign changes, or hidden conditions.',
        ],
      );
    }

    final response = await apiClient.postJson(
      config.lalacoreUri(config.aiNotesEndpoint),
      {
        'context': context.toJson(),
        'instruction': _multilingualInstruction,
      },
    );

    return ClassNotesPayload(
      keyConcepts: _toStringList(response['key_concepts']),
      formulas: _toStringList(response['formulas']),
      shortcuts: _toStringList(response['shortcuts']),
      commonMistakes: _toStringList(response['common_mistakes']),
    );
  }

  Future<List<LectureIndexModel>> generateLectureIndex({
    required AiRequestContext context,
  }) async {
    if (useMockResponses) {
      final topic = _inferTopic(context);
      return [
        LectureIndexModel(
          timestampSeconds: 180,
          topic: '$topic refresher',
          summary: 'Fast recap of the core definition and notation.',
        ),
        LectureIndexModel(
          timestampSeconds: 1180,
          topic: '$topic main derivation',
          summary:
              'Teacher moved from concept setup to problem-solving structure.',
        ),
        const LectureIndexModel(
          timestampSeconds: 2290,
          topic: 'JEE shortcut patterns',
          summary:
              'High-frequency traps, elimination logic, and accuracy checks.',
        ),
      ];
    }

    final response = await apiClient.postJson(
      config.lalacoreUri(config.aiConceptsEndpoint),
      {
        'context': context.toJson(),
        'instruction': _multilingualInstruction,
      },
    );

    return _parseTimeline(response['timeline']);
  }

  Future<List<Map<String, String>>> generateFlashcards({
    required AiRequestContext context,
  }) async {
    if (useMockResponses) {
      final topic = _inferTopic(context);
      return [
        {
          'front': 'What is the first checkpoint in $topic?',
          'back':
              'Identify the governing definition, condition, or assumption before solving.',
        },
        {
          'front': 'What is a common mistake in $topic?',
          'back':
              'Rushing into substitution without checking constraints or interpretation.',
        },
      ];
    }

    final response = await apiClient.postJson(
      config.lalacoreUri(config.aiFlashcardsEndpoint),
      {
        'context': context.toJson(),
        'instruction': _multilingualInstruction,
      },
    );

    final cards = response['flashcards'];
    if (cards is! List) {
      return const [];
    }

    return cards
        .whereType<Map>()
        .map(
          (item) => {
            'front': (item['front'] ?? '').toString(),
            'back': (item['back'] ?? '').toString(),
          },
        )
        .toList(growable: false);
  }

  Future<Map<String, dynamic>> generateClassAnalysis({
    required AiRequestContext context,
    bool webVerification = false,
  }) async {
    if (useMockResponses) {
      final topic = _inferTopic(context);
      return {
        'insights': ['Students need one more reinforcement pass on $topic.'],
        'doubt_clusters': ['$topic setup', '$topic mistake patterns'],
        if (webVerification)
          'verification_notes': [
            'Cross-check class notes against standard textbook formulations for $topic.',
            'OCR/transcript snippets were aligned to the active lecture topic.',
          ],
      };
    }

    return apiClient.postJson(config.lalacoreUri(config.aiAnalysisEndpoint), {
      'context': context.toJson(),
      'web_verification': webVerification,
      'instruction': _multilingualInstruction,
    });
  }

  Future<Map<String, dynamic>> generateMiniQuiz({
    required AiRequestContext context,
  }) async {
    if (useMockResponses) {
      final topic = _inferTopic(context);
      return {
        'question': 'Which first-step habit is most reliable for $topic?',
        'options': [
          'Check the governing condition before solving',
          'Ignore the setup and pattern-match blindly',
          'Memorize only the options',
          'Skip verification entirely',
        ],
        'correct_index': 0,
      };
    }

    return apiClient.postJson(config.lalacoreUri(config.aiQuizEndpoint), {
      'context': context.toJson(),
      'instruction': _multilingualInstruction,
    });
  }

  Future<LivePollDraft> generateLivePollDraft({
    required String topic,
    required String difficulty,
  }) async {
    if (useMockResponses) {
      return LivePollDraft(
        question: 'Which first check matters most in $topic?',
        options: const [
          'Understand the governing condition',
          'Skip straight to option matching',
          'Ignore constraints',
          'Assume every shortcut applies',
        ],
        timerSeconds: 20,
        correctOption: 0,
        topic: topic,
        difficulty: difficulty,
      );
    }

    final response = await apiClient.postJson(
      config.lalacoreUri(config.aiQuizEndpoint),
      {
        'topic': topic,
        'difficulty': difficulty,
        'question_type': 'MCQ',
        'live_mode': true,
        'instruction': _multilingualInstruction,
      },
    );

    final options = response['options'] is List
        ? (response['options'] as List)
              .map((item) => item.toString())
              .toList(growable: false)
        : const ['Option A', 'Option B', 'Option C', 'Option D'];

    return LivePollDraft(
      question: response['question']?.toString() ?? 'Generated live poll',
      options: options,
      timerSeconds: (response['timer_seconds'] as num?)?.toInt() ?? 20,
      correctOption: (response['correct_option'] as num?)?.toInt(),
      topic: topic,
      difficulty: difficulty,
    );
  }

  Future<HomeworkPack> generateHomework({
    required AiRequestContext context,
  }) async {
    if (useMockResponses) {
      final topic = _inferTopic(context);
      return HomeworkPack(
        easy: [
          'Write the core definition and one standard example for $topic.',
          'Solve one basic JEE-style question on $topic with all steps shown.',
        ],
        medium: [
          'Compare two solving approaches for $topic and state when each applies.',
        ],
        hard: [
          'Solve a multi-step $topic problem and list the checkpoints that prevent errors.',
        ],
      );
    }

    final response = await apiClient.postJson(
      config.lalacoreUri(config.aiAnalysisEndpoint),
      {
        'context': context.toJson(),
        'task': 'homework_generation',
        'instruction': _multilingualInstruction,
      },
    );

    return HomeworkPack(
      easy: _toStringList(response['easy']),
      medium: _toStringList(response['medium']),
      hard: _toStringList(response['hard']),
    );
  }

  List<LectureIndexModel> _parseTimeline(dynamic rawItems) {
    if (rawItems is! List) {
      return const [];
    }

    return rawItems
        .whereType<Map<String, dynamic>>()
        .map(
          (item) => LectureIndexModel(
            timestampSeconds: (item['timestamp_seconds'] as num?)?.toInt() ?? 0,
            topic: item['topic']?.toString() ?? 'Topic',
            summary: item['summary']?.toString() ?? '',
          ),
        )
        .toList(growable: false);
  }

  String _mockAnswer({
    required String prompt,
    required AiRequestContext context,
  }) {
    final topic = _inferTopic(context);
    final latestTranscript = context.transcript.isNotEmpty
        ? context.transcript.last.message
        : 'No transcript yet';
    final ocrHint = context.ocrSnippets.isNotEmpty
        ? context.ocrSnippets.last
        : 'No OCR text yet';

    return 'LalaCore: "$prompt"\n\n'
        'Current context summary:\n'
        '- Active topic: $topic\n'
        '- Latest transcript: $latestTranscript\n'
        '- Latest board OCR: $ocrHint\n'
        '- Suggested action: restate the governing condition for $topic before applying a shortcut.';
  }

  String _inferTopic(AiRequestContext context) {
    if (context.detectedConcepts.isNotEmpty) {
      return context.detectedConcepts.first;
    }
    if (context.lectureMaterials.isNotEmpty) {
      final candidate = context.lectureMaterials.firstWhere(
        (item) => item.trim().isNotEmpty,
        orElse: () => 'Current Topic',
      );
      final cleaned = candidate
          .replaceAll(':', ' ')
          .replaceAll(RegExp(r'\s+'), ' ')
          .trim();
      final lower = cleaned.toLowerCase();
      if (lower.endsWith('live class')) {
        return cleaned
            .substring(0, cleaned.length - 'live class'.length)
            .trim();
      }
      if (lower.endsWith('guided session')) {
        return cleaned
            .substring(0, cleaned.length - 'guided session'.length)
            .trim();
      }
      return cleaned;
    }
    return 'Current Topic';
  }

  String _mockFormula(String topic, AiRequestContext context) {
    final normalized = topic.toLowerCase();
    if (normalized.contains('integr')) {
      return 'Integral strategy = identify form + apply condition + verify result.';
    }
    if (normalized.contains('matrix')) {
      return 'Matrix workflow = observe structure + choose operation + verify dimensions.';
    }
    if (normalized.contains('complex')) {
      return 'Complex-number workflow = switch to the most useful form before solving.';
    }
    if (normalized.contains('equilibrium') || normalized.contains('kinetic')) {
      return 'Chemical workflow = write the governing relation + apply condition carefully.';
    }
    if (normalized.contains('organic')) {
      return 'Organic workflow = identify mechanism + reagent effect + product constraint.';
    }
    if (normalized.contains('motion') || normalized.contains('electric')) {
      return 'Physics workflow = define system + apply the governing law + check signs.';
    }
    return context.ocrSnippets.isNotEmpty
        ? context.ocrSnippets.last
        : 'Core workflow = understand setup + apply principle + verify edge cases.';
  }

  List<String> _toStringList(dynamic value) {
    if (value is! List) {
      return const [];
    }
    return value.map((item) => item.toString()).toList(growable: false);
  }

  // END_PHASE2_IMPLEMENTATION
}
