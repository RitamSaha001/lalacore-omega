import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:http/http.dart' as http;

import 'runtime_overrides.dart';
import '../utils/latex_support.dart';
import 'backend_service.dart';

class AiEngineService {
  AiEngineService({
    required BackendService backendService,
    http.Client? httpClient,
  }) : _backend = backendService,
       _http = httpClient ?? http.Client();

  final BackendService _backend;
  final http.Client _http;

  bool get hasDedicatedEngine => RuntimeOverrides.aiEngineUrl.trim().isNotEmpty;

  Future<Map<String, dynamic>> sendChat({
    required String prompt,
    required String userId,
    required String chatId,
    String function = 'general_chat',
    String responseStyle = 'exam_coach',
    bool enablePersona = true,
    String personaMode = 'soft_possessive_academic_girlfriend',
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    final bool heavyPrompt = _isLikelyHeavyPrompt(
      prompt: prompt,
      hasImage: base64Image != null,
    );

    final Map<String, dynamic> options = <String, dynamic>{
      'function': function,
      'response_style': responseStyle,
      'enable_persona': enablePersona,
      'persona_mode': personaMode,
      'enable_pre_reasoning_context': true,
      'enable_web_retrieval': true,
      'enable_graph_of_thought': true,
      'enable_mcts_reasoning': true,
      'require_citations': 'auto',
      'evidence_mode': 'auto',
      'min_citation_count': 1,
      'min_evidence_score': 0.58,
      'return_structured': true,
      'return_markdown': true,
      'return_latex': true,
      'count_tokens': true,
      'app_surface': 'flutter',
      if (base64Image != null) 'input_type': 'image',
      if (base64Image != null) 'multimodal_mode': 'strict_ocr',
    };

    Map<String, dynamic>? dedicatedFailure;
    if (hasDedicatedEngine) {
      try {
        final Map<String, dynamic> response = await _callDedicatedEngine(
          task: 'chat',
          payload: <String, dynamic>{
            'prompt': prompt,
            'user_id': userId,
            'chat_id': chatId,
            'options': options,
            if (card != null) 'card': card,
            if (base64Image != null) 'image': base64Image,
          },
        );
        return _normalizeChatResponse(
          response,
          defaultProvider: 'lalacore-engine',
          defaultModel: RuntimeOverrides.aiEngineModel,
          enablePersona: enablePersona,
          personaMode: personaMode,
        );
      } catch (e) {
        dedicatedFailure = <String, dynamic>{
          'stage': 'dedicated_engine',
          'error': e.toString(),
        };
      }
    }

    final Map<String, dynamic>? local = await _tryLocalSolve(
      prompt: prompt,
      userId: userId,
      chatId: chatId,
      options: options,
      enablePersona: enablePersona,
      personaMode: personaMode,
      card: card,
      base64Image: base64Image,
    );
    if (local != null) {
      Map<String, dynamic> localNormalized = _applyCompletenessRepair(
        response: local,
        prompt: prompt,
        enablePersona: enablePersona,
        personaMode: personaMode,
        function: function,
      );
      localNormalized = _maybeApplyLowConfidenceRescue(
        response: localNormalized,
        prompt: prompt,
        function: function,
        enablePersona: enablePersona,
        personaMode: personaMode,
        base64Image: base64Image,
        heavyPrompt: heavyPrompt,
      );

      final bool localDegraded = _isDegradedChatResponse(
        localNormalized,
        prompt: prompt,
      );
      if (localDegraded) {
        return <String, dynamic>{
          ...localNormalized,
          'ok': false,
          'status': 'DEGRADED_ENGINE_OUTPUT',
          'error':
              'Engine returned degraded output. Please check local backend/provider diagnostics.',
        };
      }

      if (!_hasMeaningfulAnswer(localNormalized)) {
        final Map<String, dynamic> raw = _toMap(localNormalized['raw']);
        final String upstreamStatus = _firstNonEmpty(<dynamic>[
          localNormalized['status'],
          raw['status'],
        ]);
        final String upstreamError = _firstNonEmpty(<dynamic>[
          localNormalized['error'],
          localNormalized['message'],
          raw['error'],
          raw['message'],
          raw['detail'],
        ]);
        return <String, dynamic>{
          ...localNormalized,
          'ok': false,
          'status':
              upstreamStatus.isEmpty || upstreamStatus.toLowerCase() == 'ok'
              ? 'EMPTY_ENGINE_OUTPUT'
              : upstreamStatus,
          'error': upstreamError.isEmpty
              ? 'Engine returned empty output. Please retry after checking backend connectivity.'
              : upstreamError,
        };
      }
      return localNormalized;
    }

    final Map<String, dynamic> backendResponse = await _runBackendChatRequest(
      prompt: prompt,
      userId: userId,
      chatId: chatId,
      card: card,
      base64Image: base64Image,
      options: options,
      heavyPrompt: heavyPrompt,
    );

    Map<String, dynamic> normalized = _normalizeChatResponse(
      <String, dynamic>{
        ...backendResponse,
        if (dedicatedFailure != null) 'dedicated_failure': dedicatedFailure,
      },
      enablePersona: enablePersona,
      personaMode: personaMode,
    );

    bool degraded = _isDegradedChatResponse(normalized, prompt: prompt);
    if (responseStyle != 'exam_coach' &&
        (_looksUnsupportedResponseStyleFailure(normalized) || degraded)) {
      return sendChat(
        prompt: prompt,
        userId: userId,
        chatId: chatId,
        function: function,
        responseStyle: 'exam_coach',
        enablePersona: enablePersona,
        personaMode: personaMode,
        card: card,
        base64Image: base64Image,
      );
    }

    if (_shouldRetryForQuality(
      response: normalized,
      prompt: prompt,
      function: function,
      responseStyle: responseStyle,
      heavyPrompt: heavyPrompt,
    )) {
      normalized = await _runQualityEscalationRetry(
        prompt: prompt,
        userId: userId,
        chatId: chatId,
        function: function,
        responseStyle: responseStyle,
        enablePersona: enablePersona,
        personaMode: personaMode,
        card: card,
        base64Image: base64Image,
        baseOptions: options,
        heavyPrompt: heavyPrompt,
        currentResponse: normalized,
      );
      degraded = _isDegradedChatResponse(normalized, prompt: prompt);
    }

    normalized = _applyCompletenessRepair(
      response: normalized,
      prompt: prompt,
      enablePersona: enablePersona,
      personaMode: personaMode,
      function: function,
    );

    normalized = _maybeApplyLowConfidenceRescue(
      response: normalized,
      prompt: prompt,
      function: function,
      enablePersona: enablePersona,
      personaMode: personaMode,
      base64Image: base64Image,
      heavyPrompt: heavyPrompt,
    );

    degraded = _isDegradedChatResponse(normalized, prompt: prompt);
    if (degraded) {
      return <String, dynamic>{
        ...normalized,
        'ok': false,
        'status': 'DEGRADED_ENGINE_OUTPUT',
        'error':
            'Engine returned degraded output. Please check local backend/provider diagnostics.',
      };
    }

    if (!_hasMeaningfulAnswer(normalized)) {
      final Map<String, dynamic> raw = _toMap(normalized['raw']);
      final String upstreamStatus = _firstNonEmpty(<dynamic>[
        normalized['status'],
        raw['status'],
      ]);
      final String upstreamError = _firstNonEmpty(<dynamic>[
        normalized['error'],
        normalized['message'],
        raw['error'],
        raw['message'],
        raw['detail'],
      ]);
      return <String, dynamic>{
        ...normalized,
        'ok': false,
        'status': upstreamStatus.isEmpty || upstreamStatus.toLowerCase() == 'ok'
            ? 'EMPTY_ENGINE_OUTPUT'
            : upstreamStatus,
        'error': upstreamError.isEmpty
            ? 'Engine returned empty output. Please retry after checking backend connectivity.'
            : upstreamError,
      };
    }

    return normalized;
  }

  Future<Map<String, dynamic>> materialGenerate({
    required String materialId,
    required String mode,
    required String title,
    Map<String, dynamic>? options,
  }) async {
    final Map<String, dynamic> payload = <String, dynamic>{
      'material_id': materialId,
      'mode': mode,
      'title': title,
      if (options != null) ...options,
    };

    try {
      final Map<String, dynamic> response = await _backend
          .postJsonActionWithFallback(<Map<String, dynamic>>[
            <String, dynamic>{'action': 'material_generate', ...payload},
            <String, dynamic>{'action': 'ai_material_generate', ...payload},
          ])
          .timeout(const Duration(seconds: 25));

      final String content = _extractMaterialContent(response);
      if ((response['ok'] == true || _statusLooksOk(response)) &&
          content.isNotEmpty) {
        return <String, dynamic>{
          ...response,
          'ok': true,
          'content': content,
          'raw': response,
        };
      }
    } catch (_) {}

    try {
      final Map<String, dynamic> chat = await sendChat(
        prompt:
            'Generate ${mode.trim().isEmpty ? 'study notes' : mode} for "$title" with JEE focus. Return concise and structured markdown.',
        userId: 'material_ai',
        chatId: 'material_$materialId',
        function: 'material_generate',
        responseStyle: 'exam_coach',
        enablePersona: false,
        card: <String, dynamic>{
          'material_id': materialId,
          'mode': mode,
          'title': title,
          if (options != null) ...options,
        },
        base64Image: _asImageDataUrl(options),
      );
      final String content = _composeMaterialContent(chat);
      if (content.isNotEmpty) {
        return <String, dynamic>{...chat, 'ok': true, 'content': content};
      }
    } catch (_) {}

    return <String, dynamic>{
      'ok': true,
      'source': 'local_fallback',
      'content': _localMaterialFallback(mode: mode, title: title),
    };
  }

  Future<Map<String, dynamic>> materialStatus(String materialId) async {
    try {
      final Map<String, dynamic> response = await _backend
          .postJsonActionWithFallback(<Map<String, dynamic>>[
            <String, dynamic>{
              'action': 'material_status',
              'material_id': materialId,
            },
            <String, dynamic>{
              'action': 'material_generate_status',
              'material_id': materialId,
            },
          ])
          .timeout(const Duration(seconds: 8));
      final String status = (response['status'] ?? response['state'] ?? '')
          .toString();
      if (status.isNotEmpty) {
        return response;
      }
    } catch (_) {}
    return <String, dynamic>{'ok': true, 'status': 'ready'};
  }

  Future<Map<String, dynamic>> materialQuery({
    required String materialId,
    required String question,
    String contextMode = 'qa',
  }) async {
    final Map<String, dynamic> chat = await sendChat(
      prompt: question,
      userId: 'material_qa',
      chatId: 'material_${materialId}_qa',
      function: 'material_qa',
      responseStyle: 'exam_coach',
      enablePersona: false,
      card: <String, dynamic>{
        'material_id': materialId,
        'context_mode': contextMode,
      },
    );
    final String content = _composeMaterialContent(chat);
    return <String, dynamic>{
      ...chat,
      if (content.isNotEmpty) 'content': content,
    };
  }

  Future<Map<String, dynamic>> classSummary(
    List<Map<String, dynamic>> students,
  ) async {
    try {
      final Map<String, dynamic> response = await _backend
          .postJsonActionWithFallback(<Map<String, dynamic>>[
            <String, dynamic>{
              'action': 'ai_class_summary',
              'students': students,
            },
            <String, dynamic>{
              'action': 'ai_teacher_class_summary',
              'students': students,
            },
            <String, dynamic>{'action': 'class_summary', 'students': students},
          ])
          .timeout(const Duration(seconds: 20));
      final Map<String, dynamic> data = _extractMap(response);
      if (_statusLooksOk(response) && data.isNotEmpty) {
        return <String, dynamic>{...response, 'ok': true, 'data': data};
      }
    } catch (_) {}

    final int total = students.length;
    final List<double> percentages = students
        .map((Map<String, dynamic> row) {
          final double score = _toDouble(row['score']);
          final double totalScore = max(1, _toDouble(row['total']));
          return (score / totalScore) * 100;
        })
        .toList(growable: false);
    final double avg = percentages.isEmpty
        ? 0
        : percentages.reduce((double a, double b) => a + b) /
              percentages.length;

    return <String, dynamic>{
      'ok': true,
      'source': 'local_fallback',
      'data': <String, dynamic>{
        'summary': total == 0
            ? 'No student attempts are available yet.'
            : 'Class average is ${avg.toStringAsFixed(1)}%. Focus remediation on the bottom quartile.',
        'insights': <String>[
          if (total > 0) 'Total students analyzed: $total',
          if (avg < 45)
            'Overall retention is low. Reinforce core concepts with examples.',
          if (avg >= 45 && avg < 70)
            'Class is mid-band. Add timed mixed-difficulty practice.',
          if (avg >= 70)
            'Class performance is healthy. Introduce advanced PYQ traps.',
        ],
        'actions': <String>[
          'Launch one quick concept-check poll in next class.',
          'Assign chapter-wise adaptive practice for weak areas.',
        ],
      },
    };
  }

  Future<Map<String, dynamic>> studentProfile(List<dynamic> history) async {
    try {
      final Map<String, dynamic> response = await _backend
          .postJsonActionWithFallback(<Map<String, dynamic>>[
            <String, dynamic>{
              'action': 'ai_student_profile',
              'history': history,
            },
            <String, dynamic>{
              'action': 'student_profile_ai',
              'history': history,
            },
          ])
          .timeout(const Duration(seconds: 20));
      final Map<String, dynamic> profile = _extractMap(response);
      if (_statusLooksOk(response) && profile.isNotEmpty) {
        return <String, dynamic>{...response, 'ok': true, 'profile': profile};
      }
    } catch (_) {}

    return <String, dynamic>{
      'ok': true,
      'source': 'local_fallback',
      'profile': <String, dynamic>{
        'summary': 'AI analysis not available right now.',
        'action_plan': <String>[
          'Revise weak topics with solved examples.',
          'Attempt one timed quiz daily for consistency.',
        ],
        'weekly_plan': <String>[
          'Day 1-2: Concept revision',
          'Day 3-4: Mixed practice',
          'Day 5: PYQ timed set',
        ],
      },
    };
  }

  Future<Map<String, dynamic>> studentIntelligence({
    required String accountId,
    required Map<String, dynamic> latestResult,
    required List<Map<String, dynamic>> history,
  }) async {
    try {
      final Map<String, dynamic> response = await _backend
          .postJsonActionWithFallback(<Map<String, dynamic>>[
            <String, dynamic>{
              'action': 'student_intelligence',
              'account_id': accountId,
              'latest_result': latestResult,
              'history': history,
            },
            <String, dynamic>{
              'action': 'ai_student_intelligence',
              'account_id': accountId,
              'latest_result': latestResult,
              'history': history,
            },
          ])
          .timeout(const Duration(seconds: 22));
      if (_statusLooksOk(response)) {
        return <String, dynamic>{...response, 'ok': true};
      }
    } catch (_) {}

    return <String, dynamic>{
      'ok': true,
      'source': 'local_fallback',
      'data': <String, dynamic>{
        'account_id': accountId,
        'weak_concepts': <String>[],
        'concept_mastery': <String, double>{},
      },
    };
  }

  Future<Map<String, dynamic>> analyzeExam(Map<String, dynamic> result) async {
    try {
      final Map<String, dynamic> response = await _backend
          .postJsonActionWithFallback(<Map<String, dynamic>>[
            <String, dynamic>{'action': 'analyze_exam', 'result': result},
            <String, dynamic>{'action': 'ai_analyze_exam', 'result': result},
          ])
          .timeout(const Duration(seconds: 20));
      if (_statusLooksOk(response)) {
        return <String, dynamic>{...response, 'ok': true, 'ai_available': true};
      }
    } catch (_) {}

    final double score = _toDouble(result['score']);
    final double total = max(1, _toDouble(result['maxScore']));
    final double pct = (score / total) * 100;

    return <String, dynamic>{
      'ok': true,
      'ai_available': true,
      'source': 'local_fallback',
      'summary':
          'Score ${score.toStringAsFixed(1)}/${total.toStringAsFixed(1)} (${pct.toStringAsFixed(1)}%).',
      'insights': <String>[
        if (pct < 50)
          'Accuracy is below target. Rebuild fundamentals and reduce negative marking.',
        if (pct >= 50 && pct < 75)
          'Performance is moderate. Improve speed on easy-to-medium problems.',
        if (pct >= 75)
          'Strong result. Focus on high-difficulty traps for rank gains.',
      ],
      'recommendations': <String>[
        'Review wrong and skipped questions first.',
        'Run one timed sectional test within 24 hours.',
      ],
    };
  }

  Future<Map<String, dynamic>?> _tryLocalSolve({
    required String prompt,
    required String userId,
    required String chatId,
    required Map<String, dynamic> options,
    required bool enablePersona,
    required String personaMode,
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    try {
      final Map<String, dynamic> decoded = await _backend.postLocalSolve(
        <String, dynamic>{
          'input_type': base64Image == null ? 'text' : 'mixed',
          'input_data': base64Image == null
              ? prompt
              : <String, dynamic>{'text': prompt, 'image': base64Image},
          'user_context': <String, dynamic>{
            'user_id': userId,
            'chat_id': chatId,
            if (card != null) 'card': card,
          },
          'options': options,
        },
        timeout: base64Image == null
            ? const Duration(seconds: 22)
            : const Duration(seconds: 35),
      );
      final Map<String, dynamic> normalized = _normalizeChatResponse(
        decoded,
        defaultProvider: 'lalacore-local',
        defaultModel: 'lalacore-omega',
        enablePersona: enablePersona,
        personaMode: personaMode,
      );
      if (_hasMeaningfulAnswer(normalized) ||
          _isDegradedChatResponse(normalized, prompt: prompt)) {
        return normalized;
      }
      return null;
    } catch (_) {
      return null;
    }
  }

  Future<Map<String, dynamic>> _runBackendChatRequest({
    required String prompt,
    required String userId,
    required String chatId,
    required Map<String, dynamic> options,
    required bool heavyPrompt,
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    final Duration localActionTimeout = heavyPrompt
        ? const Duration(seconds: 360)
        : const Duration(seconds: 24);
    final Duration scriptActionTimeout = heavyPrompt
        ? const Duration(seconds: 120)
        : (base64Image == null
              ? const Duration(seconds: 22)
              : const Duration(seconds: 30));
    final Duration backendGuardTimeout = heavyPrompt
        ? const Duration(seconds: 510)
        : (base64Image == null
              ? const Duration(seconds: 34)
              : const Duration(seconds: 44));

    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'ai_solve',
        'prompt': prompt,
        'user_id': userId,
        'chat_id': chatId,
        'options': options,
        if (card != null) 'card': card,
        if (base64Image != null) 'image': base64Image,
      },
      <String, dynamic>{
        'action': 'ai_chat',
        'prompt': prompt,
        'user_id': userId,
        'chat_id': chatId,
        'options': options,
        if (card != null) 'card': card,
        if (base64Image != null) 'image': base64Image,
      },
    ];

    try {
      return await _backend
          .postActionWithLocalAndScriptFallback(
            payloads,
            localTimeout: localActionTimeout,
            scriptTimeout: scriptActionTimeout,
          )
          .timeout(
            backendGuardTimeout,
            onTimeout: () => <String, dynamic>{
              'ok': false,
              'status': 'AI_BACKEND_TIMEOUT',
              'error': 'AI backend request timed out',
            },
          );
    } catch (e) {
      return <String, dynamic>{
        'ok': false,
        'status': 'AI_BACKEND_ERROR',
        'error': 'AI backend route failed',
        'details': e.toString(),
      };
    }
  }

  Future<Map<String, dynamic>> _runQualityEscalationRetry({
    required String prompt,
    required String userId,
    required String chatId,
    required String function,
    required String responseStyle,
    required bool enablePersona,
    required String personaMode,
    required Map<String, dynamic> baseOptions,
    required bool heavyPrompt,
    required Map<String, dynamic> currentResponse,
    Map<String, dynamic>? card,
    String? base64Image,
  }) async {
    final List<Map<String, dynamic>> retryProfiles = <Map<String, dynamic>>[
      <String, dynamic>{
        'provider_priority': const <String>['gemini-pro', 'gemini'],
        'preferred_provider': 'gemini',
        'preferred_model': 'gemini-pro',
      },
      <String, dynamic>{
        'provider_priority': const <String>[
          'gpt-4',
          'gpt-4o',
          'claude-3-7-sonnet',
          'claude-3-5-sonnet',
        ],
        'preferred_provider': 'openrouter',
        'preferred_model': 'gpt-4o',
      },
      <String, dynamic>{
        'provider_priority': const <String>['claude-3-7-sonnet', 'gpt-4o'],
        'preferred_provider': 'openrouter',
        'preferred_model': 'claude-3-7-sonnet',
      },
    ];

    Map<String, dynamic> best = currentResponse;
    double bestScore = _qualityScore(
      response: currentResponse,
      prompt: prompt,
      heavyPrompt: heavyPrompt,
    );

    for (int i = 0; i < retryProfiles.length; i += 1) {
      final Map<String, dynamic> retryOptions = <String, dynamic>{
        ...baseOptions,
        ...retryProfiles[i],
        'quality_retry': true,
        'quality_retry_index': i + 1,
        'target_confidence_floor': 0.60,
        'prefer_final_numeric_or_symbolic_answer': true,
        'prefer_equation_rich_derivation': heavyPrompt,
      };
      if (_isQuizGenerationFunction(function)) {
        final bool pyqPrompt = _promptSuggestsPyq(prompt);
        final int expectedCount = _requestedQuizCountFromPrompt(prompt) ?? 10;
        final Map<String, int> requiredTypeCounts = _expectedQuizTypeMix(
          expectedCount,
        );
        retryOptions.addAll(<String, dynamic>{
          'response_style': 'structured_json',
          'return_structured': true,
          'return_markdown': false,
          'json_only': true,
          'strict_json_only': true,
          'enforce_difficulty': true,
          'avoid_easy_questions': true,
          'require_type_variety': true,
          'enforce_type_distribution': true,
          'disallow_single_type_set': true,
          'required_type_counts': requiredTypeCounts,
          'type_distribution_lock': requiredTypeCounts,
          'require_mcq_count': requiredTypeCounts['MCQ'],
          'require_multi_count': requiredTypeCounts['MULTI'],
          'require_numerical_count': requiredTypeCounts['NUMERICAL'],
          'require_unique_patterns': true,
          'allow_web_search': true,
          'web_research_enabled': true,
          'search_hard_pyq': pyqPrompt,
          'pyq_mode': pyqPrompt ? 'strict_related_web' : 'hybrid',
        });
      }

      final Map<String, dynamic> backend = await _runBackendChatRequest(
        prompt: prompt,
        userId: userId,
        chatId: chatId,
        card: card,
        base64Image: base64Image,
        options: retryOptions,
        heavyPrompt: true,
      );

      final Map<String, dynamic> candidate = _normalizeChatResponse(
        backend,
        enablePersona: enablePersona,
        personaMode: personaMode,
      );
      final double candidateScore = _qualityScore(
        response: candidate,
        prompt: prompt,
        heavyPrompt: heavyPrompt,
      );
      if (candidateScore > bestScore) {
        best = candidate;
        bestScore = candidateScore;
      }

      final bool quizReady = _isQuizGenerationFunction(function)
          ? !_quizPayloadNeedsEscalation(response: candidate, prompt: prompt)
          : false;
      final bool confidentEnough = _confidenceScore(candidate) >= 0.60;
      final bool sufficient = _isQuizGenerationFunction(function)
          ? quizReady
          : !_isInsufficientAnswer(
              response: candidate,
              prompt: prompt,
              heavyPrompt: heavyPrompt,
            );
      final bool degraded = _isDegradedChatResponse(candidate, prompt: prompt);
      if (candidate['ok'] == true &&
          (_isQuizGenerationFunction(function) || confidentEnough) &&
          sufficient &&
          !degraded) {
        return <String, dynamic>{
          ...candidate,
          'quality_retry': <String, dynamic>{
            'attempted': i + 1,
            'trigger': 'low_confidence_or_insufficient',
            'selected': true,
          },
        };
      }
    }

    return <String, dynamic>{
      ...best,
      'quality_retry': <String, dynamic>{
        'attempted': retryProfiles.length,
        'trigger': 'low_confidence_or_insufficient',
        'selected':
            bestScore >
            _qualityScore(
              response: currentResponse,
              prompt: prompt,
              heavyPrompt: heavyPrompt,
            ),
      },
    };
  }

  bool _shouldRetryForQuality({
    required Map<String, dynamic> response,
    required String prompt,
    required String function,
    required String responseStyle,
    required bool heavyPrompt,
  }) {
    if (response['ok'] != true) {
      return false;
    }
    final bool quizGeneration = _isQuizGenerationFunction(function);
    if (quizGeneration) {
      if (_isDegradedChatResponse(response, prompt: prompt)) {
        return true;
      }
      return _quizPayloadNeedsEscalation(response: response, prompt: prompt);
    }
    final String answer = (response['answer'] ?? '').toString();
    if (responseStyle.toLowerCase().contains('json') &&
        _looksLikeStructuredJson(answer)) {
      return false;
    }
    if (_isDegradedChatResponse(response, prompt: prompt)) {
      return true;
    }
    final double confidence = _confidenceScore(response);
    final bool lowConfidence = confidence >= 0 && confidence < 0.60;
    final bool insufficient = _isInsufficientAnswer(
      response: response,
      prompt: prompt,
      heavyPrompt: heavyPrompt,
    );
    return lowConfidence || insufficient;
  }

  double _qualityScore({
    required Map<String, dynamic> response,
    required String prompt,
    required bool heavyPrompt,
  }) {
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    final String merged = '$answer\n$explanation'.trim();
    final int words = merged
        .split(RegExp(r'\s+'))
        .where((String e) => e.trim().isNotEmpty)
        .length;
    final bool equationLike = RegExp(
      r'(=|\\int|\\frac|\\sqrt|\\sum|\^|epsilon|sigma|rho)',
      caseSensitive: false,
    ).hasMatch(merged);
    final bool finalCue = RegExp(
      r'\b(final answer|therefore|hence|thus|result)\b',
      caseSensitive: false,
    ).hasMatch(merged);
    final bool degraded = _isDegradedChatResponse(response, prompt: prompt);
    final bool insufficient = _isInsufficientAnswer(
      response: response,
      prompt: prompt,
      heavyPrompt: heavyPrompt,
    );
    final double confidence = _confidenceScore(response);

    double score = response['ok'] == true ? 2.0 : 0.0;
    score += degraded ? -1.5 : 1.2;
    score += insufficient ? -1.2 : 1.4;
    score += confidence >= 0 ? confidence * 2.5 : 0.5;
    score += min(2.0, words / 120.0);
    if (equationLike) {
      score += 0.6;
    }
    if (finalCue) {
      score += 0.4;
    }
    final String provider = (response['provider'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    final String model = (response['model'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    if (provider.isNotEmpty && provider != 'none' && provider != 'unknown') {
      score += 0.2;
    }
    if (model.isNotEmpty && model != 'none' && model != 'unknown') {
      score += 0.2;
    }
    return score;
  }

  double _confidenceScore(Map<String, dynamic> response) {
    final Map<String, dynamic> raw = _toMap(response['raw']);
    dynamic value =
        response['confidence'] ??
        raw['confidence'] ??
        _toMap(raw['calibration_metrics'])['confidence_score'];
    final double? inferred = _inferredConfidenceFromVerification(response);
    if (value == null) {
      return inferred ?? -1;
    }
    if (value is String) {
      final String txt = value.trim().toLowerCase();
      if (txt.isEmpty) {
        return inferred ?? -1;
      }
      if (txt == 'high') {
        return 0.85;
      }
      if (txt == 'medium') {
        return 0.60;
      }
      if (txt == 'low') {
        return 0.30;
      }
      final String normalized = txt.endsWith('%')
          ? txt.substring(0, txt.length - 1).trim()
          : txt;
      final double? parsed = double.tryParse(normalized);
      if (parsed == null) {
        return inferred ?? -1;
      }
      final double score = parsed > 1.0 ? parsed / 100.0 : parsed;
      if (score <= 0.01 && inferred != null) {
        return inferred;
      }
      return score.clamp(0.0, 1.0);
    }
    if (value is num) {
      final double score = value.toDouble();
      final double normalized = (score > 1.0 ? score / 100.0 : score).clamp(
        0.0,
        1.0,
      );
      if (normalized <= 0.01 && inferred != null) {
        return inferred;
      }
      return normalized;
    }
    return inferred ?? -1;
  }

  double? _inferredConfidenceFromVerification(Map<String, dynamic> response) {
    final Map<String, dynamic> raw = _toMap(response['raw']);
    final Map<String, dynamic> verification = _toMap(raw['verification']);
    final String failureReason = (verification['failure_reason'] ?? '')
        .toString()
        .toLowerCase();
    final String reason = (verification['reason'] ?? '')
        .toString()
        .toLowerCase();
    final bool missingGroundTruth =
        failureReason.contains('missing_ground_truth') ||
        reason.contains('no expected answer');
    if (!missingGroundTruth) {
      return null;
    }

    final String provider = _firstNonEmpty(<dynamic>[
      response['provider'],
      raw['provider'],
      response['winner_provider'],
      _toMap(raw['provider_diagnostics'])['winner_provider'],
      _toMap(raw['engine'])['provider'],
    ]).toLowerCase();
    final String model = _firstNonEmpty(<dynamic>[
      response['model'],
      raw['model'],
      _toMap(raw['engine'])['model'],
      _toMap(raw['engine'])['version'],
      _toMap(raw['engine'])['model_name'],
    ]).toLowerCase();
    final bool hasProviderSignal =
        provider.isNotEmpty &&
        provider != 'none' &&
        provider != 'unknown' &&
        model.isNotEmpty &&
        model != 'none' &&
        model != 'unknown';
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    final int combinedLen = answer.length + explanation.length;
    if (combinedLen < 80) {
      return null;
    }
    if (!hasProviderSignal && combinedLen < 120) {
      return null;
    }

    final Map<String, dynamic> plausibility = _toMap(
      verification['plausibility'],
    );
    final double plausibilityScore =
        (plausibility['score'] as num?)?.toDouble() ?? 0.7;
    final double riskScore =
        (verification['risk_score'] as num?)?.toDouble() ??
        (_toMap(raw['calibration_metrics'])['risk_score'] as num?)
            ?.toDouble() ??
        0.5;

    final bool hasVisualization = response['visualization'] is Map;
    final bool equationLike = RegExp(
      r'(=|\\int|\\frac|\\sqrt|\\sum|\^|epsilon|sigma|rho)',
      caseSensitive: false,
    ).hasMatch('$answer\n$explanation');

    double inferred = 0.52 + (plausibilityScore * 0.24) - (riskScore * 0.14);
    if (hasVisualization) {
      inferred += 0.05;
    }
    if (equationLike) {
      inferred += 0.03;
    }
    return inferred.clamp(0.45, 0.82);
  }

  bool _isInsufficientAnswer({
    required Map<String, dynamic> response,
    required String prompt,
    required bool heavyPrompt,
  }) {
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    final String merged = '$answer\n$explanation'.trim();
    if (merged.isEmpty) {
      return true;
    }
    if (_looksLikeStructuredJson(answer)) {
      return false;
    }

    final String promptLower = prompt.toLowerCase();
    final bool derivationIntent = RegExp(
      r'\b(find|derive|calculate|closed form|solve|proof|show|potential|field|speed|difference)\b',
      caseSensitive: false,
    ).hasMatch(promptLower);
    final bool equationLike = RegExp(
      r'(=|\\int|\\frac|\\sqrt|\\sum|\^|epsilon|sigma|rho)',
      caseSensitive: false,
    ).hasMatch(merged);
    final bool numericLike = RegExp(r'[-+]?\d+(\.\d+)?').hasMatch(merged);
    final bool finalCue = RegExp(
      r'\b(final answer|therefore|hence|thus|result)\b',
      caseSensitive: false,
    ).hasMatch(merged);
    final int words = merged
        .split(RegExp(r'\s+'))
        .where((String e) => e.trim().isNotEmpty)
        .length;

    if (answer.endsWith(':') && words < 85) {
      return true;
    }
    if (heavyPrompt) {
      if (words < 90) {
        return true;
      }
      if (derivationIntent && !equationLike) {
        return true;
      }
      if (derivationIntent && !numericLike) {
        return true;
      }
      if (!finalCue && derivationIntent && words < 160) {
        return true;
      }
    } else if (derivationIntent && words < 35 && !equationLike) {
      return true;
    }

    final String lowerMerged = merged.toLowerCase();
    final bool genericOnly =
        lowerMerged.contains("to solve this problem, we'll use") &&
        !equationLike &&
        words < 140;
    if (genericOnly) {
      return true;
    }
    return false;
  }

  bool _looksUnsupportedResponseStyleFailure(Map<String, dynamic> response) {
    final String raw = <String>[
      (response['error'] ?? '').toString(),
      (response['message'] ?? '').toString(),
      (response['status'] ?? '').toString(),
      (response['raw'] ?? '').toString(),
    ].join(' ').toLowerCase();
    if (raw.isEmpty) {
      return false;
    }
    return raw.contains('response_style') ||
        raw.contains('response style') ||
        raw.contains('structured_exam_solution') ||
        raw.contains('return_structured') ||
        raw.contains('unsupported style') ||
        raw.contains('invalid style') ||
        raw.contains('unsupported response');
  }

  bool _isQuizGenerationFunction(String function) {
    final String f = function.trim().toLowerCase();
    if (f.isEmpty) {
      return false;
    }
    return f.contains('generate_quiz') ||
        f.contains('ai_generate_quiz') ||
        f.contains('quiz_generate');
  }

  bool _promptSuggestsPyq(String prompt) {
    return RegExp(
      r'\b(pyq|previous year|jee advanced|jee main|past year)\b',
      caseSensitive: false,
    ).hasMatch(prompt);
  }

  bool _quizPayloadNeedsEscalation({
    required Map<String, dynamic> response,
    required String prompt,
  }) {
    final List<Map<String, dynamic>> questions = _extractQuizQuestionMaps(
      response,
    );
    if (questions.isEmpty) {
      return true;
    }

    final int? expectedCount = _requestedQuizCountFromPrompt(prompt);
    if (expectedCount != null &&
        questions.length < max(2, (expectedCount * 0.8).round())) {
      return true;
    }

    if (_quizTypeMixNeedsEscalation(questions, expectedCount: expectedCount)) {
      return true;
    }

    int shortStemCount = 0;
    int advancedLikeCount = 0;
    final Set<String> skeletons = <String>{};
    final Set<String> openings = <String>{};
    for (final Map<String, dynamic> q in questions) {
      final String text = _firstNonEmpty(<dynamic>[
        q['statement'],
        q['question'],
        q['question_text'],
        q['text'],
      ]);
      if (text.trim().length < 50) {
        shortStemCount++;
      }
      final String skeleton = _quizQuestionSkeleton(text);
      if (skeleton.isNotEmpty) {
        skeletons.add(skeleton);
      }
      final String opening = skeleton
          .split(' ')
          .where((String t) => t.trim().isNotEmpty)
          .take(7)
          .join(' ')
          .trim();
      if (opening.isNotEmpty) {
        openings.add(opening);
      }
      if (_quizQuestionDifficultyScore(q) >= 4.0) {
        advancedLikeCount++;
      }
    }

    final bool tooShortSet =
        shortStemCount >= max(2, (questions.length * 0.4).round());
    if (tooShortSet) {
      return true;
    }
    final bool repetitive =
        skeletons.length <= (questions.length * 0.6).floor();
    if (repetitive) {
      return true;
    }
    final bool repeatedOpenings =
        questions.length >= 5 &&
        openings.length <= max(2, (questions.length * 0.5).floor());
    if (repeatedOpenings) {
      return true;
    }

    final bool hardIntent = RegExp(
      r'\b(hard|advanced|ultra|difficulty\s*[4-9]|trap|non-routine|pyq)\b',
      caseSensitive: false,
    ).hasMatch(prompt);
    if (hardIntent) {
      final double avgHardness =
          questions
              .map(_quizQuestionDifficultyScore)
              .fold<double>(0.0, (double a, double b) => a + b) /
          questions.length;
      if (avgHardness < 3.35) {
        return true;
      }
      if (advancedLikeCount < max(1, (questions.length * 0.3).round())) {
        return true;
      }
    }

    return false;
  }

  int? _requestedQuizCountFromPrompt(String prompt) {
    final RegExp pattern = RegExp(
      r'\b(question count|questions?|total questions?)\s*[:=]?\s*(\d{1,3})\b',
      caseSensitive: false,
    );
    final Match? match = pattern.firstMatch(prompt);
    if (match == null) {
      return null;
    }
    final int? parsed = int.tryParse(match.group(2) ?? '');
    if (parsed == null || parsed <= 0) {
      return null;
    }
    return parsed.clamp(1, 200);
  }

  Map<String, int> _expectedQuizTypeMix(int total) {
    final int safeTotal = total <= 0 ? 1 : total;
    final int numerical = (safeTotal * 0.4).round();
    final int mcq = (safeTotal * 0.3).round();
    final int multi = safeTotal - numerical - mcq;
    return <String, int>{
      'MCQ': mcq.clamp(0, safeTotal),
      'MULTI': multi.clamp(0, safeTotal),
      'NUMERICAL': numerical.clamp(0, safeTotal),
    };
  }

  String _quizQuestionType(Map<String, dynamic> q) {
    final String raw = _firstNonEmpty(<dynamic>[
      q['type'],
      q['question_type'],
      q['questionType'],
    ]).toLowerCase();
    final bool singleCorrectSignal =
        raw.contains('single correct') ||
        raw.contains('single answer') ||
        raw.contains('mcq_single') ||
        raw == 'single';
    final bool multiSignal =
        raw == 'multi' ||
        raw == 'mcq_multi' ||
        raw == 'multicorrect' ||
        raw == 'multi_correct' ||
        raw == 'multi-correct' ||
        raw == 'multiple' ||
        raw == 'multiple_correct' ||
        raw == 'multiple correct' ||
        raw == 'multiple_choice_multiple_answer' ||
        raw == 'msq' ||
        raw == 'mcma' ||
        raw == 'select_all_that_apply' ||
        raw.contains('multiple correct') ||
        raw.contains('multi correct') ||
        raw.contains('select all') ||
        (raw.contains('multiple') && !singleCorrectSignal);
    if (multiSignal) {
      return 'MULTI';
    }
    if (raw == 'numerical' ||
        raw == 'num' ||
        raw == 'integer' ||
        raw == 'integer_type' ||
        raw == 'integer type' ||
        raw == 'numerical answer type' ||
        raw == 'nat' ||
        raw == 'non_mcq' ||
        raw.contains('numerical') ||
        raw.contains('integer') ||
        raw.contains('numeric')) {
      return 'NUMERICAL';
    }
    final dynamic answerRaw = q['correct_answer'] ?? q['correct_answers'];
    if (answerRaw is List && answerRaw.length > 1) {
      return 'MULTI';
    }
    return 'MCQ';
  }

  bool _quizTypeMixNeedsEscalation(
    List<Map<String, dynamic>> questions, {
    required int? expectedCount,
  }) {
    if (questions.isEmpty) {
      return true;
    }
    int mcq = 0;
    int multi = 0;
    int numerical = 0;
    for (final Map<String, dynamic> q in questions) {
      final String type = _quizQuestionType(q);
      if (type == 'MCQ') {
        mcq++;
      } else if (type == 'MULTI') {
        multi++;
      } else if (type == 'NUMERICAL') {
        numerical++;
      }
    }
    final int distinctTypes = <int>[
      mcq,
      multi,
      numerical,
    ].where((int count) => count > 0).length;
    final int total = questions.length;
    final int targetCount = expectedCount ?? total;
    if (targetCount <= 1) {
      return false;
    }
    if (targetCount == 2) {
      return distinctTypes < 2;
    }
    if (targetCount >= 3 && (mcq < 1 || multi < 1 || numerical < 1)) {
      return true;
    }
    if (targetCount >= 8) {
      final int minPerType = max(1, (targetCount * 0.2).round());
      if (mcq < minPerType || multi < minPerType || numerical < minPerType) {
        return true;
      }
    }
    return distinctTypes < 2;
  }

  List<Map<String, dynamic>> _extractQuizQuestionMaps(
    Map<String, dynamic> response,
  ) {
    List<dynamic> asList(dynamic raw) {
      if (raw is List) {
        return raw;
      }
      if (raw is String) {
        final String text = raw.trim();
        if (text.isEmpty) {
          return const <dynamic>[];
        }
        try {
          final dynamic decoded = jsonDecode(text);
          if (decoded is List) {
            return decoded;
          }
          if (decoded is Map) {
            return asList(
              decoded['questions'] ??
                  decoded['questions_json'] ??
                  decoded['quiz_questions'] ??
                  decoded['items'],
            );
          }
        } catch (_) {}
      }
      if (raw is Map) {
        final dynamic nested =
            raw['questions'] ??
            raw['questions_json'] ??
            raw['quiz_questions'] ??
            raw['items'] ??
            _toMap(raw['lc_aqie'])['questions'];
        return asList(nested);
      }
      return const <dynamic>[];
    }

    final List<dynamic> rawQuestions = asList(
      response['questions'] ??
          response['questions_json'] ??
          response['quiz_questions'] ??
          response['answer'] ??
          response['raw'] ??
          _toMap(response['data'])['questions'] ??
          _toMap(response['data'])['quiz_questions'] ??
          _toMap(response['data'])['questions_json'],
    );
    return rawQuestions
        .whereType<Map>()
        .map((Map<dynamic, dynamic> row) => Map<String, dynamic>.from(row))
        .where((Map<String, dynamic> q) {
          final String text = _firstNonEmpty(<dynamic>[
            q['statement'],
            q['question'],
            q['question_text'],
            q['text'],
          ]);
          return text.trim().isNotEmpty;
        })
        .toList(growable: false);
  }

  String _quizQuestionSkeleton(String text) {
    String out = text.toLowerCase();
    out = out.replaceAll(RegExp(r'\$[^$]*\$'), ' M ');
    out = out.replaceAll(RegExp(r'-?\d+(?:\.\d+)?'), '#');
    out = out.replaceAll(RegExp(r'[^a-z0-9# ]+'), ' ');
    out = out.replaceAll(RegExp(r'\s+'), ' ').trim();
    return out;
  }

  double _quizQuestionDifficultyScore(Map<String, dynamic> q) {
    final dynamic raw = q['difficulty'] ?? q['difficulty_level'];
    if (raw is num) {
      return raw.toDouble().clamp(1.0, 5.0);
    }
    final String label = _firstNonEmpty(<dynamic>[
      q['difficulty_label'],
      raw,
    ]).toLowerCase();
    if (label.contains('advanced') ||
        label.contains('hard') ||
        label.contains('l5') ||
        label.contains('l4')) {
      return 4.3;
    }
    if (label.contains('main') ||
        label.contains('medium') ||
        label.contains('l3')) {
      return 3.1;
    }
    if (label.contains('basic') ||
        label.contains('easy') ||
        label.contains('l1') ||
        label.contains('l2')) {
      return 1.8;
    }

    final String bag = _firstNonEmpty(<dynamic>[
      q['question'],
      q['statement'],
      q['question_text'],
      q['solution'],
    ]).toLowerCase();
    int signal = 0;
    if (RegExp(
      r'\b(case|constraint|eliminate|non[- ]routine|prove)\b',
    ).hasMatch(bag)) {
      signal += 2;
    }
    if (RegExp(
      r'\\int|\\sum|\\prod|det|matrix|vector|probability|binomial',
    ).hasMatch(bag)) {
      signal += 1;
    }
    if (bag.length > 180) {
      signal += 1;
    }
    if (signal >= 3) {
      return 4.2;
    }
    if (signal == 2) {
      return 3.6;
    }
    if (signal == 1) {
      return 2.9;
    }
    return 2.2;
  }

  bool _isDegradedChatResponse(
    Map<String, dynamic> response, {
    required String prompt,
  }) {
    final Map<String, dynamic> raw = _toMap(response['raw']);
    final Map<String, dynamic> meta = _toMap(raw['meta']);
    final String provider = (response['provider'] ?? raw['provider'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    final String model = (response['model'] ?? raw['model'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    final String answerLower = answer.toLowerCase();

    final bool explicitDegraded =
        meta['degraded'] == true || raw['degraded'] == true;
    if (explicitDegraded) {
      return true;
    }

    final bool providerMissing =
        provider.isEmpty ||
        provider == 'none' ||
        provider == 'unknown' ||
        provider == 'null';
    final bool modelMissing =
        model.isEmpty ||
        model == 'none' ||
        model == 'unknown' ||
        model == 'null';
    final bool inferStub =
        answerLower.contains('cannot fully solve') ||
        answerLower.contains('here is what i can infer');
    final bool promptEcho =
        _normalizedComparableText(answer) == _normalizedComparableText(prompt);

    if ((providerMissing || modelMissing) &&
        (inferStub || promptEcho) &&
        explanation.isEmpty) {
      return true;
    }
    if (response['ok'] == true && providerMissing && modelMissing) {
      final int wordCount = answer
          .split(RegExp(r'\s+'))
          .where((String e) => e.trim().isNotEmpty)
          .length;
      if (wordCount < 12 && explanation.isEmpty) {
        return true;
      }
    }
    return false;
  }

  Map<String, dynamic> _normalizeChatResponse(
    Map<String, dynamic> response, {
    String defaultProvider = '',
    String defaultModel = '',
    bool enablePersona = false,
    String personaMode = '',
  }) {
    final Map<String, dynamic> normalized = _extractStructuredPayload(response);
    final bool flaggedFailure = _looksFailure(response);
    final bool canUseExtractedFallback =
        !flaggedFailure &&
        (response.containsKey('output') ||
            response.containsKey('data') ||
            response.containsKey('response') ||
            response.containsKey('content') ||
            response.containsKey('text'));

    final String answerSeed = _firstNonEmpty(<dynamic>[
      normalized['answer'],
      response['answer'],
      response['final_answer'],
      response['text'],
      response['response'],
      response['content'],
      if (canUseExtractedFallback) _extractText(response),
    ]);
    final String explanationSeed = _firstNonEmpty(<dynamic>[
      normalized['explanation'],
      response['explanation'],
      response['reasoning'],
    ]);

    final String answer = _normalizeMathArtifacts(answerSeed);
    final String explanation = _normalizeMathArtifacts(explanationSeed);
    final Map<String, String> personaAdjusted = _applyPersonaTone(
      answer: answer,
      explanation: explanation,
      enablePersona: enablePersona,
      personaMode: personaMode,
    );

    final String finalAnswer = (personaAdjusted['answer'] ?? answer).trim();
    final String finalExplanation =
        (personaAdjusted['explanation'] ?? explanation).trim();

    final String confidence = _extractConfidenceLabel(response, normalized);
    final String concept = _extractConcept(response, normalized);
    final Map<String, dynamic>? visualization = _extractVisualization(
      response,
      normalized,
    );

    final Map<String, dynamic> providerDiagnostics = _toMap(
      response['provider_diagnostics'],
    );
    final Map<String, dynamic> engineMeta = _toMap(response['engine']);

    final String resolvedProvider = _firstNonEmpty(<dynamic>[
      response['provider'],
      normalized['provider'],
      response['winner_provider'],
      providerDiagnostics['winner_provider'],
      defaultProvider,
    ]);
    final String resolvedModel = _firstNonEmpty(<dynamic>[
      response['model'],
      normalized['model'],
      engineMeta['model'],
      engineMeta['model_name'],
      engineMeta['version'],
      defaultModel,
    ]);

    final bool ok =
        response['ok'] == true ||
        (!flaggedFailure &&
            (finalAnswer.isNotEmpty || finalExplanation.isNotEmpty));
    final String resolvedStatus = _firstNonEmpty(<dynamic>[
      response['status'],
      normalized['status'],
    ]);
    final String resolvedError = _firstNonEmpty(<dynamic>[
      response['error'],
      normalized['error'],
      response['message'],
      normalized['message'],
    ]);
    final String resolvedMessage = _firstNonEmpty(<dynamic>[
      response['message'],
      normalized['message'],
      response['error'],
      normalized['error'],
    ]);

    return <String, dynamic>{
      'ok': ok,
      'answer': finalAnswer,
      if (resolvedStatus.isNotEmpty) 'status': resolvedStatus,
      if (resolvedError.isNotEmpty) 'error': resolvedError,
      if (resolvedMessage.isNotEmpty) 'message': resolvedMessage,
      if (finalExplanation.isNotEmpty) 'explanation': finalExplanation,
      if (confidence.isNotEmpty) 'confidence': confidence,
      if (concept.isNotEmpty) 'concept': concept,
      if (visualization != null) 'visualization': visualization,
      if (response['web_retrieval'] is Map)
        'web_retrieval': _toMap(response['web_retrieval']),
      if (response['mcts_search'] is Map)
        'mcts_search': _toMap(response['mcts_search']),
      if (response['reasoning_graph'] is Map)
        'reasoning_graph': _toMap(response['reasoning_graph']),
      if (response['citations'] is List) 'citations': response['citations'],
      if (response['citation_map'] is List)
        'citation_map': response['citation_map'],
      if (response['sources_consulted'] is List)
        'sources_consulted': response['sources_consulted'],
      'provider': resolvedProvider,
      'model': resolvedModel,
      'raw': response,
    };
  }

  Map<String, dynamic> _extractStructuredPayload(
    Map<String, dynamic> response,
  ) {
    final List<dynamic> candidates = <dynamic>[
      response['payload'],
      response['data'],
      response['result'],
      response['output'],
      response['raw'],
    ];
    for (final dynamic candidate in candidates) {
      final Map<String, dynamic> map = _toMap(candidate);
      if (map.isNotEmpty &&
          (map.containsKey('answer') ||
              map.containsKey('final_answer') ||
              map.containsKey('explanation') ||
              map.containsKey('reasoning'))) {
        return map;
      }
      if (candidate is String && candidate.trim().isNotEmpty) {
        try {
          final dynamic decoded = jsonDecode(candidate);
          final Map<String, dynamic> nested = _toMap(decoded);
          if (nested.isNotEmpty) {
            return nested;
          }
        } catch (_) {}
      }
    }

    final String answer = (response['answer'] ?? '').toString().trim();
    if (_looksLikeStructuredJson(answer)) {
      try {
        final dynamic decoded = jsonDecode(answer);
        final Map<String, dynamic> map = _toMap(decoded);
        if (map.isNotEmpty) {
          return map;
        }
      } catch (_) {}
    }
    return <String, dynamic>{};
  }

  String _extractText(Map<String, dynamic> response) {
    final dynamic output =
        response['output'] ??
        response['answer'] ??
        response['text'] ??
        response['response'] ??
        response['content'];
    if (output is String) {
      return output;
    }
    if (output is Map<String, dynamic>) {
      return output['content']?.toString() ?? jsonEncode(output);
    }
    if (response['data'] is Map<String, dynamic>) {
      final Map<String, dynamic> data =
          response['data'] as Map<String, dynamic>;
      return _extractText(data);
    }
    return jsonEncode(response);
  }

  Future<Map<String, dynamic>> _callDedicatedEngine({
    required String task,
    required Map<String, dynamic> payload,
  }) async {
    final Uri uri = Uri.parse(RuntimeOverrides.aiEngineUrl);
    final Map<String, dynamic> body = <String, dynamic>{
      'task': task,
      ...payload,
    };
    final Map<String, String> headers = <String, String>{
      'Content-Type': 'application/json',
      if (RuntimeOverrides.aiEngineApiKey.trim().isNotEmpty)
        'Authorization': 'Bearer ${RuntimeOverrides.aiEngineApiKey.trim()}',
    };
    final http.Response response = await _http
        .post(uri, headers: headers, body: jsonEncode(body))
        .timeout(const Duration(seconds: 30));
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw Exception(
        'AI engine error ${response.statusCode}: ${response.body}',
      );
    }
    if (response.body.trim().isEmpty) {
      return <String, dynamic>{};
    }
    final dynamic decoded = jsonDecode(response.body);
    if (decoded is Map<String, dynamic>) {
      return decoded;
    }
    return <String, dynamic>{'output': decoded.toString()};
  }

  bool _isLikelyHeavyPrompt({required String prompt, required bool hasImage}) {
    if (hasImage) {
      return true;
    }
    final String lower = prompt.toLowerCase();
    int score = 0;
    if (prompt.length >= 350) {
      score += 2;
    } else if (prompt.length >= 220) {
      score += 1;
    }
    if (RegExp(
      r'\b(jee advanced|very hard|derive|proof|closed form|superposition|multi-step|vector field)\b',
    ).hasMatch(lower)) {
      score += 1;
    }
    if (RegExp(r'(^|\n)\s*[1-6][\)\.]', multiLine: true).hasMatch(prompt)) {
      score += 1;
    }
    return score >= 2;
  }

  Map<String, dynamic> _toMap(dynamic raw) {
    if (raw is Map<String, dynamic>) {
      return raw;
    }
    if (raw is Map) {
      return Map<String, dynamic>.from(raw);
    }
    return <String, dynamic>{};
  }

  String _firstNonEmpty(List<dynamic> values) {
    for (final dynamic value in values) {
      final String token = (value ?? '').toString().trim();
      if (token.isNotEmpty) {
        return token;
      }
    }
    return '';
  }

  bool _statusLooksOk(Map<String, dynamic> map) {
    final String status = (map['status'] ?? map['state'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    if (map['ok'] == true) {
      return true;
    }
    return status.isEmpty ||
        status == 'ok' ||
        status == 'success' ||
        status == 'completed' ||
        status == 'ready';
  }

  bool _looksFailure(Map<String, dynamic> response) {
    final String status = (response['status'] ?? '').toString().toLowerCase();
    final String message = (response['message'] ?? response['error'] ?? '')
        .toString()
        .toLowerCase();
    return status.contains('error') ||
        status.contains('fail') ||
        status.contains('unknown') ||
        message.contains('error') ||
        message.contains('failed') ||
        message.contains('unknown action') ||
        message.contains('invalid action');
  }

  String _normalizedComparableText(String text) {
    return text
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9]+'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  bool _looksLikeStructuredJson(String text) {
    final String trimmed = text.trim();
    if (trimmed.isEmpty) {
      return false;
    }
    if (!(trimmed.startsWith('{') || trimmed.startsWith('['))) {
      return false;
    }
    try {
      final dynamic decoded = jsonDecode(trimmed);
      return decoded is Map || decoded is List;
    } catch (_) {
      return false;
    }
  }

  bool _hasMeaningfulAnswer(Map<String, dynamic> response) {
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    return answer.isNotEmpty || explanation.isNotEmpty;
  }

  bool _isConceptualPrompt(String prompt) {
    return RegExp(
      r'\b(what is|what are|explain|define|difference|how to|meaning of)\b',
      caseSensitive: false,
    ).hasMatch(prompt);
  }

  Map<String, dynamic> _maybeApplyLowConfidenceRescue({
    required Map<String, dynamic> response,
    required String prompt,
    required String function,
    required bool enablePersona,
    required String personaMode,
    required String? base64Image,
    required bool heavyPrompt,
  }) {
    final String normalizedAnswer = (response['answer'] ?? '').toString();
    final String normalizedExplanation = (response['explanation'] ?? '')
        .toString();
    final double normalizedConfidence = _confidenceScore(response);
    final bool lowConfidence =
        normalizedConfidence >= 0 && normalizedConfidence < 0.60;
    final bool truncatedCandidate =
        normalizedAnswer.trim().isNotEmpty &&
        _looksTruncatedAnswer(
          answer: normalizedAnswer,
          explanation: normalizedExplanation,
        );
    final bool lowConfidenceConceptual =
        base64Image == null &&
        !heavyPrompt &&
        _isConceptualPrompt(prompt) &&
        lowConfidence;
    final bool lowConfidenceTruncated =
        base64Image == null && lowConfidence && truncatedCandidate;
    final bool graphPrompt = RegExp(
      r'\b(plot|graph|draw|sketch)\b',
      caseSensitive: false,
    ).hasMatch(prompt);
    if (graphPrompt) {
      return response;
    }

    if (!(lowConfidenceConceptual || lowConfidenceTruncated)) {
      return response;
    }
    if (_looksLikeStructuredJson(normalizedAnswer)) {
      return response;
    }

    final Map<String, dynamic> rescued = _localChatRescueResponse(
      prompt: prompt,
      function: function,
      enablePersona: enablePersona,
      personaMode: personaMode,
      degradedResponse: response,
    );
    if (!_hasMeaningfulAnswer(rescued)) {
      return response;
    }

    return <String, dynamic>{
      ...rescued,
      if (response['visualization'] is Map)
        'visualization': _toMap(response['visualization']),
      if (response['web_retrieval'] is Map)
        'web_retrieval': _toMap(response['web_retrieval']),
      if (response['mcts_search'] is Map)
        'mcts_search': _toMap(response['mcts_search']),
      if (response['reasoning_graph'] is Map)
        'reasoning_graph': _toMap(response['reasoning_graph']),
      if (response['citations'] is List) 'citations': response['citations'],
      if (response['citation_map'] is List)
        'citation_map': response['citation_map'],
      if (response['sources_consulted'] is List)
        'sources_consulted': response['sources_consulted'],
      if ((response['concept'] ?? '').toString().trim().isNotEmpty)
        'concept': response['concept'],
      if ((response['confidence'] ?? '').toString().trim().isNotEmpty)
        'confidence': response['confidence'],
      'quality_retry': response['quality_retry'],
      'rescue_applied': <String, dynamic>{
        'trigger': lowConfidenceConceptual
            ? 'low_confidence_conceptual'
            : 'low_confidence_truncated_answer',
        'upstream_provider': response['provider'],
      },
      'raw': <String, dynamic>{..._toMap(rescued['raw']), 'upstream': response},
    };
  }

  Map<String, dynamic> _applyCompletenessRepair({
    required Map<String, dynamic> response,
    required String prompt,
    required bool enablePersona,
    required String personaMode,
    required String function,
  }) {
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    if (answer.isEmpty ||
        explanation.isEmpty ||
        _looksLikeStructuredJson(answer)) {
      return response;
    }
    if (!_looksTruncatedAnswer(answer: answer, explanation: explanation)) {
      return response;
    }
    final String repaired = _deriveAnswerFromExplanation(explanation);
    if (repaired.isEmpty) {
      return response;
    }
    final Map<String, String> personaAdjusted = _applyPersonaTone(
      answer: repaired,
      explanation: explanation,
      enablePersona: enablePersona,
      personaMode: personaMode,
    );
    final String repairedAnswer = (personaAdjusted['answer'] ?? repaired)
        .trim();
    if (repairedAnswer.isEmpty) {
      return response;
    }
    return <String, dynamic>{
      ...response,
      'answer': repairedAnswer,
      'completeness_repair': <String, dynamic>{
        'applied': true,
        'trigger': 'truncated_answer',
        'function': function,
        'prompt_type': _isConceptualPrompt(prompt) ? 'conceptual' : 'general',
      },
    };
  }

  bool _looksTruncatedAnswer({
    required String answer,
    required String explanation,
  }) {
    final String lower = answer.toLowerCase();
    final int answerWords = answer
        .split(RegExp(r'\s+'))
        .where((String e) => e.trim().isNotEmpty)
        .length;
    final int explanationWords = explanation
        .split(RegExp(r'\s+'))
        .where((String e) => e.trim().isNotEmpty)
        .length;
    final bool danglingTail =
        lower.endsWith(',') ||
        lower.endsWith(':') ||
        RegExp(r'\b(to|and|or|with|if|when)$').hasMatch(lower);
    String answerTerminalScan = answer.trim();
    while (answerTerminalScan.endsWith('"') ||
        answerTerminalScan.endsWith("'")) {
      answerTerminalScan = answerTerminalScan
          .substring(0, answerTerminalScan.length - 1)
          .trimRight();
    }
    final bool noTerminalPunctuation = !RegExp(
      r'[.!?]$',
    ).hasMatch(answerTerminalScan);
    final bool trailingStopWord = RegExp(
      r'\b(just|from|the|a|an|or|and|to|of|with|for|in|on|at|by)$',
      caseSensitive: false,
    ).hasMatch(answer.trim());
    final bool explicitIncomplete = RegExp(
      r'\b(incomplete|cuts off|cut off|truncated)\b',
      caseSensitive: false,
    ).hasMatch(explanation);
    final bool tooBriefAgainstExplanation =
        answerWords < 18 && explanationWords >= 40;
    final bool likelyAbruptEnding =
        answerWords >= 20 && noTerminalPunctuation && trailingStopWord;
    return danglingTail ||
        tooBriefAgainstExplanation ||
        likelyAbruptEnding ||
        explicitIncomplete;
  }

  String _deriveAnswerFromExplanation(String explanation) {
    if (explanation.trim().isEmpty) {
      return '';
    }
    String clean = explanation.trim();
    clean = clean.replaceFirst(
      RegExp(r'^\s*reasoning\s*:\s*', caseSensitive: false),
      '',
    );
    final List<String> sentences = clean
        .split(RegExp(r'(?<=[.!?])\s+'))
        .map((String s) => s.trim())
        .where((String s) => s.isNotEmpty)
        .toList(growable: false);
    if (sentences.isEmpty) {
      return '';
    }
    if (sentences.first.length >= 40 || sentences.length == 1) {
      return sentences.first;
    }
    return '${sentences.first} ${sentences[1]}'.trim();
  }

  Map<String, dynamic> _localChatRescueResponse({
    required String prompt,
    required String function,
    required bool enablePersona,
    required String personaMode,
    required Map<String, dynamic> degradedResponse,
  }) {
    if (_isQuizGenerationFunction(function)) {
      return <String, dynamic>{...degradedResponse, 'ok': false};
    }

    final Map<String, String> textFallback = _buildLocalStudyFallbackText(
      prompt,
    );
    final Map<String, String> personaAdjusted = _applyPersonaTone(
      answer: textFallback['answer'] ?? '',
      explanation: textFallback['explanation'] ?? '',
      enablePersona: enablePersona,
      personaMode: personaMode,
    );
    final String answer = (personaAdjusted['answer'] ?? '').trim();
    final String explanation = (personaAdjusted['explanation'] ?? '').trim();
    return <String, dynamic>{
      'ok': answer.isNotEmpty || explanation.isNotEmpty,
      'answer': answer,
      if (explanation.isNotEmpty) 'explanation': explanation,
      'provider': 'local-fallback',
      'model': 'rule-based-chat-v1',
      'raw': <String, dynamic>{
        'source': 'local_rescue_chat',
        'fallback_reason': 'low_confidence',
        'upstream': degradedResponse,
      },
    };
  }

  Map<String, String> _buildLocalStudyFallbackText(String prompt) {
    final String normalized = _normalizedComparableText(prompt);
    final bool isVernierPrompt =
        normalized.contains('vernier') &&
        (normalized.contains('caliper') || normalized.contains('calliper'));
    if (isVernierPrompt) {
      return <String, String>{
        'answer':
            'A vernier caliper is a precision tool used to measure external diameter, internal diameter, and depth with high accuracy.',
        'explanation': <String>[
          'How to measure:',
          '1. Check zero error before use.',
          '2. Place object between jaws and close gently.',
          '3. Note main scale reading just before vernier zero.',
          '4. Find aligned vernier division and multiply by least count.',
          '5. Final reading = main scale + vernier contribution, then apply zero correction.',
        ].join('\n'),
      };
    }

    return <String, String>{
      'answer':
          'Use definition first, then solve with step-wise equations and a final verified answer line.',
      'explanation': <String>[
        '1. Identify the chapter concept and known formula.',
        '2. Apply the formula with correct substitutions.',
        '3. Verify units/sign and state final answer clearly.',
      ].join('\n'),
    };
  }

  String _localMaterialFallback({required String mode, required String title}) {
    final String m = mode.trim().toLowerCase();
    final String header = m.contains('summary')
        ? 'Summary'
        : m.contains('qa')
        ? 'Q&A Notes'
        : 'JEE Notes';
    return <String>[
      '$header: $title',
      '',
      '1. Core concept revision points.',
      '2. Formula checkpoints with units/sign conventions.',
      '3. Typical traps and quick validation steps.',
      '4. Practice recommendation: easy -> medium -> PYQ hard set.',
    ].join('\n');
  }

  String _extractMaterialContent(Map<String, dynamic> response) {
    final String content = (response['content'] ?? response['answer'] ?? '')
        .toString()
        .trim();
    if (content.isNotEmpty) {
      return content;
    }
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    if (explanation.isNotEmpty) {
      return explanation;
    }
    final Map<String, dynamic> data = _toMap(response['data']);
    final String nested = (data['content'] ?? data['answer'] ?? '')
        .toString()
        .trim();
    return nested;
  }

  String _composeMaterialContent(Map<String, dynamic> response) {
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    final String concept = (response['concept'] ?? '').toString().trim();
    final String confidence = (response['confidence'] ?? '').toString().trim();
    final String assembled = <String>[
      if (answer.isNotEmpty) '**Answer**\n$answer',
      if (explanation.isNotEmpty) '**Explanation**\n$explanation',
      if (concept.isNotEmpty) '**Concept**: $concept',
      if (confidence.isNotEmpty) '**Confidence**: $confidence',
    ].join('\n\n').trim();
    if (assembled.isNotEmpty) {
      return assembled;
    }
    return _extractMaterialContent(response);
  }

  String? _asImageDataUrl(Map<String, dynamic>? options) {
    if (options == null) {
      return null;
    }
    final dynamic raw =
        options['image'] ?? options['image_base64'] ?? options['image_data'];
    if (raw == null) {
      return null;
    }
    final String text = raw.toString().trim();
    if (text.isEmpty) {
      return null;
    }
    if (text.startsWith('data:image')) {
      return text;
    }
    return 'data:image/jpeg;base64,$text';
  }

  Map<String, dynamic>? _extractVisualization(
    Map<String, dynamic> response,
    Map<String, dynamic> normalized,
  ) {
    dynamic raw =
        response['visualization'] ??
        normalized['visualization'] ??
        response['graph'] ??
        response['desmos'] ??
        _toMap(response['meta'])['visualization'];
    if (raw == null) {
      return null;
    }

    final Map<String, dynamic> vis = _toMap(raw);
    final Map<String, dynamic> graph = _toMap(vis['graph']);
    final dynamic eqRaw =
        vis['equations'] ?? graph['equations'] ?? vis['expressions'];

    final List<Map<String, dynamic>> expressions = <Map<String, dynamic>>[];
    if (eqRaw is List) {
      for (final dynamic item in eqRaw) {
        String latex = '';
        String color = '#2D70B3';
        String lineStyle = 'solid';
        if (item is String) {
          latex = item.trim();
        } else if (item is Map) {
          latex = _firstNonEmpty(<dynamic>[
            item['latex'],
            item['expression'],
            item['eq'],
            item['text'],
          ]);
          color = _firstNonEmpty(<dynamic>[item['color'], '#2D70B3']);
          final String style = _firstNonEmpty(<dynamic>[
            item['lineStyle'],
            item['style'],
          ]).toLowerCase();
          if (style.contains('dash')) {
            lineStyle = 'dashed';
          }
        }
        latex = latex.trim();
        if (latex.isEmpty) {
          continue;
        }
        expressions.add(<String, dynamic>{
          'id': 'eq${expressions.length + 1}',
          'latex': latex,
          'color': color,
          'lineStyle': lineStyle,
        });
      }
    }

    if (expressions.isEmpty) {
      return null;
    }

    final Map<String, dynamic> viewport = _normalizeViewport(
      vis['viewport'] ?? graph['window'] ?? vis['window'],
    );
    return <String, dynamic>{
      'type': 'desmos',
      'expressions': expressions,
      'viewport': viewport,
    };
  }

  Map<String, dynamic> _normalizeViewport(dynamic raw) {
    final Map<String, dynamic> map = _toMap(raw);
    double read(List<String> keys, double fallback) {
      for (final String key in keys) {
        final dynamic value = map[key];
        if (value is num) {
          return value.toDouble();
        }
        final double? parsed = double.tryParse((value ?? '').toString());
        if (parsed != null) {
          return parsed;
        }
      }
      return fallback;
    }

    double xmin = read(<String>['xmin', 'left', 'xMin'], -10);
    double xmax = read(<String>['xmax', 'right', 'xMax'], 10);
    double ymin = read(<String>['ymin', 'bottom', 'yMin'], -10);
    double ymax = read(<String>['ymax', 'top', 'yMax'], 10);
    if (xmax <= xmin) {
      final double mid = (xmin + xmax) / 2;
      xmin = mid - 10;
      xmax = mid + 10;
    }
    if (ymax <= ymin) {
      final double mid = (ymin + ymax) / 2;
      ymin = mid - 10;
      ymax = mid + 10;
    }

    double clampValue(double v) => max(-1000, min(1000, v));
    return <String, dynamic>{
      'xmin': clampValue(xmin),
      'xmax': clampValue(xmax),
      'ymin': clampValue(ymin),
      'ymax': clampValue(ymax),
    };
  }

  String _extractConfidenceLabel(
    Map<String, dynamic> response,
    Map<String, dynamic> normalized,
  ) {
    final dynamic raw =
        normalized['confidence'] ??
        response['confidence'] ??
        _toMap(response['calibration_metrics'])['confidence_score'];
    if (raw == null) {
      return '';
    }
    final double score = _confidenceScore(<String, dynamic>{
      'confidence': raw,
      'raw': response,
      'answer':
          normalized['answer'] ??
          response['answer'] ??
          response['final_answer'],
      'explanation':
          normalized['explanation'] ??
          response['explanation'] ??
          response['reasoning'],
      'provider': normalized['provider'] ?? response['provider'],
      'model': normalized['model'] ?? response['model'],
      'visualization': normalized['visualization'] ?? response['visualization'],
    });
    if (score < 0) {
      return '';
    }
    if (score >= 0.75) {
      return 'High';
    }
    if (score >= 0.50) {
      return 'Medium';
    }
    return 'Low';
  }

  String _extractConcept(
    Map<String, dynamic> response,
    Map<String, dynamic> normalized,
  ) {
    return _firstNonEmpty(<dynamic>[
      normalized['concept'],
      response['concept'],
      response['topic'],
      _toMap(response['input_analysis'])['topic'],
    ]);
  }

  String _normalizeMathArtifacts(String text) {
    String cleaned = text.trim();
    if (cleaned.isEmpty) {
      return cleaned;
    }
    cleaned = normalizeUniversalLatex(cleaned);
    return cleaned;
  }

  Map<String, String> _applyPersonaTone({
    required String answer,
    required String explanation,
    required bool enablePersona,
    required String personaMode,
  }) {
    if (!enablePersona) {
      return <String, String>{'answer': answer, 'explanation': explanation};
    }
    // Keep output academically focused; persona styling is intentionally subtle.
    if (!personaMode.toLowerCase().contains('academic')) {
      return <String, String>{'answer': answer, 'explanation': explanation};
    }
    return <String, String>{'answer': answer, 'explanation': explanation};
  }

  Map<String, dynamic> _extractMap(Map<String, dynamic> response) {
    if (response['data'] is Map<String, dynamic>) {
      return Map<String, dynamic>.from(response['data'] as Map);
    }
    if (response['analytics'] is Map<String, dynamic>) {
      return Map<String, dynamic>.from(response['analytics'] as Map);
    }
    if (response['profile'] is Map<String, dynamic>) {
      return Map<String, dynamic>.from(response['profile'] as Map);
    }
    if (response['output'] is Map<String, dynamic>) {
      return Map<String, dynamic>.from(response['output'] as Map);
    }
    return response;
  }

  double _toDouble(dynamic value) {
    if (value is num) {
      return value.toDouble();
    }
    return double.tryParse((value ?? '').toString()) ?? 0.0;
  }
}
