import 'dart:convert';
import 'dart:io';

import 'package:jee_live_classes/core/app_config.dart';
import 'package:jee_live_classes/models/extracted_practice_question_model.dart';
import 'package:jee_live_classes/models/transcript_model.dart';
import 'package:jee_live_classes/modules/ai/lalacore_api_service.dart';
import 'package:jee_live_classes/services/class_join_service.dart';
import 'package:jee_live_classes/services/live_services_readiness.dart';
import 'package:jee_live_classes/services/quiz_service.dart';
import 'package:jee_live_classes/services/recording_service.dart';
import 'package:jee_live_classes/services/secure_api_client.dart';

Future<void> main(List<String> args) async {
  final startedAt = DateTime.now().toUtc();
  final outputPath =
      _argValue(args, '--out') ??
      'build/reports/live_class_real_e2e_report.json';

  final report = <String, dynamic>{
    'started_at': startedAt.toIso8601String(),
    'mode': 'real',
    'steps': <Map<String, dynamic>>[],
  };

  final config = AppConfig.fromEnvironment();
  if (config.enableMockServices) {
    _addStep(
      report,
      name: 'config_guard',
      ok: false,
      details: {
        'reason':
            'LIVE_CLASSES_ENABLE_REAL_SERVICES is false. Real E2E requires real-service mode.',
      },
    );
    await _writeReport(outputPath, report);
    stderr.writeln(
      'Real E2E aborted: LIVE_CLASSES_ENABLE_REAL_SERVICES must be true.',
    );
    exitCode = 2;
    return;
  }

  try {
    LiveServicesReadiness.ensureReadyForRealServices(config);
    _addStep(report, name: 'readiness', ok: true);
  } catch (error) {
    _addStep(
      report,
      name: 'readiness',
      ok: false,
      details: {'error': error.toString()},
    );
    report['finished_at'] = DateTime.now().toUtc().toIso8601String();
    report['duration_ms'] = DateTime.now()
        .toUtc()
        .difference(startedAt)
        .inMilliseconds;
    report['pass'] = false;
    await _writeReport(outputPath, report);
    stderr.writeln('Real E2E readiness failed: $error');
    exitCode = 2;
    return;
  }

  final apiClient = SecureApiClient(config: config);
  final joinService = RealClassJoinService(
    config: config,
    apiClient: apiClient,
  );
  final recordingService = RealRecordingService(
    config: config,
    apiClient: apiClient,
  );
  final quizService = QuizService(config: config, apiClient: apiClient);
  final lalacoreApi = LalacoreApi(
    config: config,
    apiClient: apiClient,
    useMockResponses: false,
  );

  final context = config.toLiveClassContext();

  String? joinRequestId;
  String sessionId = context.classId;

  try {
    final sw = Stopwatch()..start();
    final session = await joinService.fetchClassSession(context);
    sessionId = session.id;
    joinRequestId = await joinService.requestJoin(
      context: context,
      deviceInfo: {
        'platform': Platform.operatingSystem,
        'locale': Platform.localeName,
        'e2e': true,
      },
      cameraEnabled: true,
      micEnabled: true,
    );
    sw.stop();

    _addStep(
      report,
      name: 'join_class',
      ok: joinRequestId.trim().isNotEmpty,
      latencyMs: sw.elapsedMilliseconds,
      details: {'class_id': session.id, 'request_id': joinRequestId},
    );
  } catch (error) {
    _addStep(
      report,
      name: 'join_class',
      ok: false,
      details: {'error': error.toString()},
    );
  }

  try {
    final sw = Stopwatch()..start();
    final token = await joinService.fetchWebRtcFallbackToken(
      classId: sessionId,
      userId: context.userId,
    );
    sw.stop();
    final ok =
        token != null &&
        (token['token'] ?? '').toString().trim().isNotEmpty &&
        (token['url'] ?? '').toString().trim().isNotEmpty;

    _addStep(
      report,
      name: 'failover_token',
      ok: ok,
      latencyMs: sw.elapsedMilliseconds,
      details: {
        'provider': token?['provider'],
        'room': token?['room'],
        'url_present': (token?['url'] ?? '').toString().isNotEmpty,
      },
    );
  } catch (error) {
    _addStep(
      report,
      name: 'failover_token',
      ok: false,
      details: {'error': error.toString()},
    );
  }

  try {
    final sw = Stopwatch()..start();
    await recordingService.startRecording(sessionId);
    final rawPath = await recordingService.stopRecording(sessionId);

    RecordingArtifact? artifact;
    final job = await recordingService.queueProcessingJob(
      sessionId: sessionId,
      rawRecordingPath: rawPath,
    );

    if (job != null) {
      var status = job.status;
      for (var attempt = 0; attempt < 12; attempt += 1) {
        status = await recordingService.fetchProcessingStatus(job.jobId);
        final normalized = status.toLowerCase();
        if (normalized == 'completed' || normalized == 'done') {
          break;
        }
        if (normalized == 'failed' || normalized == 'error') {
          break;
        }
        await Future<void>.delayed(const Duration(milliseconds: 500));
      }
      artifact = await recordingService.fetchProcessedArtifact(
        sessionId: sessionId,
        jobId: job.jobId,
      );
    }

    artifact ??= await recordingService.processRecording(
      sessionId: sessionId,
      rawRecordingPath: rawPath,
      transcript: [
        TranscriptModel(
          id: 'txn_e2e_1',
          speakerId: 'teacher_01',
          speakerName: 'Teacher',
          message: 'Gauss law links enclosed charge with electric flux.',
          timestamp: DateTime.now().toUtc(),
          confidence: 0.95,
          source: 'e2e',
        ),
      ],
    );
    sw.stop();

    _addStep(
      report,
      name: 'recording_pipeline',
      ok:
          artifact.recordingUrl.trim().isNotEmpty ||
          artifact.notes.trim().isNotEmpty,
      latencyMs: sw.elapsedMilliseconds,
      details: {
        'recording_url': artifact.recordingUrl,
        'notes_length': artifact.notes.length,
        'concept_index_size': artifact.lectureIndex.length,
      },
    );
  } catch (error) {
    _addStep(
      report,
      name: 'recording_pipeline',
      ok: false,
      details: {'error': error.toString()},
    );
  }

  final aiContext = AiRequestContext(
    transcript: [
      TranscriptModel(
        id: 'txn_ai_1',
        speakerId: 'teacher_01',
        speakerName: 'Teacher',
        message: 'Inside conductor electric field is zero in electrostatics.',
        timestamp: DateTime.now().toUtc(),
        confidence: 0.97,
        source: 'e2e',
      ),
    ],
    chatMessages: const [
      'Why do we choose a pillbox surface?',
      'What is enclosed charge here?',
    ],
    ocrSnippets: const ['closed integral E.dA = q/epsilon0'],
    lectureMaterials: const ['Electrostatics lecture board snapshots'],
    detectedConcepts: const ['Electric Flux', 'Gauss Law'],
    timestamps: const [0, 320, 620],
  );

  try {
    final sw = Stopwatch()..start();
    final notes = await lalacoreApi.generateNotes(context: aiContext);
    final analysis = await lalacoreApi.generateClassAnalysis(
      context: aiContext,
      webVerification: true,
    );
    sw.stop();

    _addStep(
      report,
      name: 'ai_notes',
      ok: notes.keyConcepts.isNotEmpty && notes.formulas.isNotEmpty,
      latencyMs: sw.elapsedMilliseconds,
      details: {
        'key_concepts': notes.keyConcepts.length,
        'formulas': notes.formulas.length,
        'analysis_keys': analysis.keys.toList(),
      },
    );
  } catch (error) {
    _addStep(
      report,
      name: 'ai_notes',
      ok: false,
      details: {'error': error.toString()},
    );
  }

  try {
    final sw = Stopwatch()..start();
    final aiAnswer = await lalacoreApi.askLalacore(
      prompt:
          'Solve this student doubt briefly: Why can we apply Gauss law with symmetry here?',
      context: aiContext,
    );

    bool backendQueueOk = false;
    String? queueError;
    try {
      final raiseResponse = await apiClient
          .postJson(config.apiUri('/app/action'), {
            'action': 'raise_doubt',
            'class_id': sessionId,
            'account_id': context.userId,
            'message': 'Why does enclosed charge ignore outside shell?',
          }, signRequest: false);
      final listResponse = await apiClient.postJson(
        config.apiUri('/app/action'),
        {
          'action': 'get_doubts',
          'class_id': sessionId,
          'account_id': context.userId,
        },
        signRequest: false,
      );
      backendQueueOk =
          raiseResponse.isNotEmpty &&
          (listResponse['doubts'] is List || listResponse['items'] is List);
    } catch (error) {
      queueError = error.toString();
    }
    sw.stop();

    _addStep(
      report,
      name: 'doubt_queue',
      ok: aiAnswer.trim().isNotEmpty,
      latencyMs: sw.elapsedMilliseconds,
      details: {
        'ai_answer_preview': aiAnswer.substring(
          0,
          aiAnswer.length.clamp(0, 140),
        ),
        'backend_queue_checked': backendQueueOk,
        if (queueError != null) 'backend_queue_error': queueError,
      },
    );
  } catch (error) {
    _addStep(
      report,
      name: 'doubt_queue',
      ok: false,
      details: {'error': error.toString()},
    );
  }

  try {
    final sw = Stopwatch()..start();
    final question = ExtractedPracticeQuestionModel(
      id: 'practice_e2e_${DateTime.now().millisecondsSinceEpoch}',
      question: 'Find electric field at distance r from infinite line charge.',
      solutionSteps: const [
        'Choose cylindrical Gaussian surface.',
        'Use symmetry to keep E constant on curved surface.',
        'Apply closed integral E.dA = q_enclosed / epsilon0.',
      ],
      finalAnswer: 'E = lambda / (2*pi*epsilon0*r)',
      conceptTags: const ['Gauss Law', 'Line Charge'],
      difficulty: 'medium',
      timestampSeconds: 420,
      createdAt: DateTime.now().toUtc(),
    );

    await quizService.saveExtractedPracticeQuestion(
      classId: sessionId,
      question: question,
    );
    final queue = await quizService.fetchPracticeReviewQueue(
      classId: sessionId,
      limit: 25,
    );
    sw.stop();

    _addStep(
      report,
      name: 'practice_extraction',
      ok: true,
      latencyMs: sw.elapsedMilliseconds,
      details: {'queue_size': queue.length, 'saved_question_id': question.id},
    );
  } catch (error) {
    _addStep(
      report,
      name: 'practice_extraction',
      ok: false,
      details: {'error': error.toString()},
    );
  }

  if (joinRequestId != null && joinRequestId.trim().isNotEmpty) {
    try {
      await joinService.cancelJoinRequest(
        context: context,
        requestId: joinRequestId,
      );
      _addStep(report, name: 'cleanup_join_request', ok: true);
    } catch (error) {
      _addStep(
        report,
        name: 'cleanup_join_request',
        ok: false,
        details: {'error': error.toString()},
      );
    }
  }

  joinService.dispose();

  final steps = (report['steps'] as List).cast<Map<String, dynamic>>();
  report['pass'] = steps.every((step) => step['ok'] == true);
  report['finished_at'] = DateTime.now().toUtc().toIso8601String();
  report['duration_ms'] = DateTime.now()
      .toUtc()
      .difference(startedAt)
      .inMilliseconds;

  await _writeReport(outputPath, report);

  final status = report['pass'] == true ? 'PASS' : 'FAIL';
  stdout.writeln('Live class real E2E audit: $status');
  stdout.writeln('Report: ${File(outputPath).absolute.path}');

  if (report['pass'] != true) {
    exitCode = 1;
  }
}

String? _argValue(List<String> args, String key) {
  for (var i = 0; i < args.length; i += 1) {
    if (args[i] != key) {
      continue;
    }
    if (i + 1 < args.length) {
      return args[i + 1];
    }
    return null;
  }
  return null;
}

Future<void> _writeReport(
  String outputPath,
  Map<String, dynamic> report,
) async {
  final file = File(outputPath);
  await file.parent.create(recursive: true);
  await file.writeAsString(const JsonEncoder.withIndent('  ').convert(report));
}

void _addStep(
  Map<String, dynamic> report, {
  required String name,
  required bool ok,
  int? latencyMs,
  Map<String, dynamic>? details,
}) {
  final step = <String, dynamic>{
    'name': name,
    'ok': ok,
    'timestamp': DateTime.now().toUtc().toIso8601String(),
    if (latencyMs != null) 'latency_ms': latencyMs,
    if (details != null && details.isNotEmpty) 'details': details,
  };
  (report['steps'] as List<Map<String, dynamic>>).add(step);
}
