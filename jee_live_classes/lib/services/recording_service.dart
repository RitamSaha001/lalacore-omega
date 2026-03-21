import 'dart:async';

import '../core/app_config.dart';
import '../models/lecture_index_model.dart';
import '../models/replay_model.dart';
import '../models/transcript_model.dart';
import 'secure_api_client.dart';

class RecordingArtifact {
  const RecordingArtifact({
    required this.recordingUrl,
    required this.notes,
    required this.lectureIndex,
    required this.generatedAt,
  });

  final String recordingUrl;
  final String notes;
  final List<LectureIndexModel> lectureIndex;
  final DateTime generatedAt;
}

class RecordingProcessingJob {
  const RecordingProcessingJob({required this.jobId, required this.status});

  final String jobId;
  final String status;
}

abstract class RecordingService {
  Future<void> startRecording(String sessionId);
  Future<String> stopRecording(String sessionId);
  Future<RecordingProcessingJob?> queueProcessingJob({
    required String sessionId,
    required String rawRecordingPath,
  });
  Future<String> fetchProcessingStatus(String jobId);
  Future<RecordingArtifact?> fetchProcessedArtifact({
    required String sessionId,
    required String jobId,
  });
  Future<RecordingArtifact> processRecording({
    required String sessionId,
    required String rawRecordingPath,
    required List<TranscriptModel> transcript,
  });
  Future<ReplayModel?> fetchReplay(String sessionId);
}

class MockRecordingService implements RecordingService {
  // BEGIN_PHASE2_IMPLEMENTATION
  @override
  Future<void> startRecording(String sessionId) async {
    await Future<void>.delayed(const Duration(milliseconds: 200));
  }

  @override
  Future<String> stopRecording(String sessionId) async {
    await Future<void>.delayed(const Duration(milliseconds: 300));
    return '/cache/$sessionId.mp4';
  }

  @override
  Future<RecordingProcessingJob?> queueProcessingJob({
    required String sessionId,
    required String rawRecordingPath,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 120));
    return RecordingProcessingJob(
      jobId: 'mock_job_${DateTime.now().millisecondsSinceEpoch}',
      status: 'queued',
    );
  }

  @override
  Future<String> fetchProcessingStatus(String jobId) async {
    await Future<void>.delayed(const Duration(milliseconds: 80));
    return 'completed';
  }

  @override
  Future<RecordingArtifact?> fetchProcessedArtifact({
    required String sessionId,
    required String jobId,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 120));
    return RecordingArtifact(
      recordingUrl: 'https://cdn.example.com/recordings/$sessionId.mp4',
      notes: 'Processed by mock background worker ($jobId).',
      lectureIndex: const [
        LectureIndexModel(
          timestampSeconds: 120,
          topic: 'Mock Worker Output',
          summary: 'Background job completed and indexed lecture.',
        ),
      ],
      generatedAt: DateTime.now(),
    );
  }

  @override
  Future<RecordingArtifact> processRecording({
    required String sessionId,
    required String rawRecordingPath,
    required List<TranscriptModel> transcript,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 550));
    final topics = <LectureIndexModel>[
      const LectureIndexModel(
        timestampSeconds: 0,
        topic: 'Class intro',
        summary: 'Overview of Gauss law strategy for JEE.',
      ),
      const LectureIndexModel(
        timestampSeconds: 620,
        topic: 'Coulomb law recap',
        summary: 'Symmetry assumptions and vector direction shortcuts.',
      ),
      const LectureIndexModel(
        timestampSeconds: 2110,
        topic: 'Gauss law application',
        summary: 'Choosing Gaussian surfaces under time constraints.',
      ),
    ];

    return RecordingArtifact(
      recordingUrl: 'https://cdn.example.com/recordings/$sessionId.mp4',
      notes:
          'Auto-notes: Key formula E = q / (4*pi*eps0*r^2), symmetry-driven shortcuts, and common sign mistakes.',
      lectureIndex: topics,
      generatedAt: DateTime.now(),
    );
  }

  @override
  Future<ReplayModel?> fetchReplay(String sessionId) async {
    return ReplayModel(
      classId: sessionId,
      videoUrl: 'https://cdn.example.com/recordings/$sessionId.mp4',
      transcript: const [],
      conceptIndex: const [
        LectureIndexModel(
          timestampSeconds: 10,
          topic: 'Introduction',
          summary: 'Lecture setup and recap.',
        ),
      ],
    );
  }

  // END_PHASE2_IMPLEMENTATION
}

class RealRecordingService implements RecordingService {
  // BEGIN_PHASE2_IMPLEMENTATION
  const RealRecordingService({required this.config, required this.apiClient});

  final AppConfig config;
  final SecureApiClient apiClient;

  @override
  Future<void> startRecording(String sessionId) async {
    await apiClient.postJson(
      config.recordingApiUri(config.recordingStartEndpoint),
      {'class_id': sessionId},
    );
  }

  @override
  Future<String> stopRecording(String sessionId) async {
    final response = await apiClient.postJson(
      config.recordingApiUri(config.recordingStopEndpoint),
      {'class_id': sessionId},
    );
    return response['raw_recording_path']?.toString() ??
        '/recordings/$sessionId.mp4';
  }

  @override
  Future<RecordingProcessingJob?> queueProcessingJob({
    required String sessionId,
    required String rawRecordingPath,
  }) async {
    final response = await apiClient.postJson(
      config.recordingApiUri(config.recordingProcessAsyncEndpoint),
      {'class_id': sessionId, 'raw_recording_path': rawRecordingPath},
    );
    final jobId = response['job_id']?.toString();
    if (jobId == null || jobId.isEmpty) {
      return null;
    }
    return RecordingProcessingJob(
      jobId: jobId,
      status: response['status']?.toString() ?? 'queued',
    );
  }

  @override
  Future<String> fetchProcessingStatus(String jobId) async {
    final response = await apiClient.getJson(
      config.recordingApiUri(
        config.recordingProcessStatusEndpoint,
        queryParameters: {'job_id': jobId},
      ),
    );
    return response['status']?.toString() ?? 'unknown';
  }

  @override
  Future<RecordingArtifact?> fetchProcessedArtifact({
    required String sessionId,
    required String jobId,
  }) async {
    final response = await apiClient.getJson(
      config.recordingApiUri(
        config.recordingProcessResultEndpoint,
        queryParameters: {'job_id': jobId, 'class_id': sessionId},
      ),
    );
    final recordingUrl = _resolveRecordingUrl(
      response['recording_url']?.toString(),
    );
    if (recordingUrl.isEmpty) {
      return null;
    }
    final indexList = response['concept_index'] is List
        ? (response['concept_index'] as List)
              .whereType<Map<String, dynamic>>()
              .map(
                (item) => LectureIndexModel(
                  timestampSeconds:
                      (item['timestamp_seconds'] as num?)?.toInt() ?? 0,
                  topic: item['topic']?.toString() ?? 'Topic',
                  summary: item['summary']?.toString() ?? '',
                ),
              )
              .toList(growable: false)
        : const <LectureIndexModel>[];

    return RecordingArtifact(
      recordingUrl: recordingUrl,
      notes: response['ai_notes']?.toString() ?? '',
      lectureIndex: indexList,
      generatedAt: DateTime.now(),
    );
  }

  @override
  Future<RecordingArtifact> processRecording({
    required String sessionId,
    required String rawRecordingPath,
    required List<TranscriptModel> transcript,
  }) async {
    final response = await apiClient.postJson(
      config.recordingApiUri(config.recordingProcessEndpoint),
      {
        'class_id': sessionId,
        'raw_recording_path': rawRecordingPath,
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
      },
    );

    final indexList = response['concept_index'] is List
        ? (response['concept_index'] as List)
              .whereType<Map<String, dynamic>>()
              .map(
                (item) => LectureIndexModel(
                  timestampSeconds:
                      (item['timestamp_seconds'] as num?)?.toInt() ?? 0,
                  topic: item['topic']?.toString() ?? 'Topic',
                  summary: item['summary']?.toString() ?? '',
                ),
              )
              .toList(growable: false)
        : const <LectureIndexModel>[];

    return RecordingArtifact(
      recordingUrl: _resolveRecordingUrl(response['recording_url']?.toString()),
      notes: response['ai_notes']?.toString() ?? '',
      lectureIndex: indexList,
      generatedAt: DateTime.now(),
    );
  }

  @override
  Future<ReplayModel?> fetchReplay(String sessionId) async {
    final response = await apiClient.getJson(
      config.recordingApiUri(
        config.recordingReplayEndpoint,
        queryParameters: {'class_id': sessionId},
      ),
    );

    final transcript = response['transcript'] is List
        ? (response['transcript'] as List)
              .whereType<Map<String, dynamic>>()
              .map(
                (item) => TranscriptModel(
                  id: item['id']?.toString() ?? 'txn',
                  speakerId: item['speaker_id']?.toString() ?? '',
                  speakerName: item['speaker_name']?.toString() ?? 'Teacher',
                  message: item['message']?.toString() ?? '',
                  timestamp:
                      DateTime.tryParse(item['timestamp']?.toString() ?? '') ??
                      DateTime.now(),
                  confidence: (item['confidence'] as num?)?.toDouble() ?? 1,
                  source: 'replay',
                ),
              )
              .toList(growable: false)
        : const <TranscriptModel>[];

    final conceptIndex = response['concept_index'] is List
        ? (response['concept_index'] as List)
              .whereType<Map<String, dynamic>>()
              .map(
                (item) => LectureIndexModel(
                  timestampSeconds:
                      (item['timestamp_seconds'] as num?)?.toInt() ?? 0,
                  topic: item['topic']?.toString() ?? '',
                  summary: item['summary']?.toString() ?? '',
                ),
              )
              .toList(growable: false)
        : const <LectureIndexModel>[];

    final resolvedUrl = _resolveRecordingUrl(
      response['recording_url']?.toString(),
    );
    if (resolvedUrl.isEmpty) {
      return null;
    }

    return ReplayModel(
      classId: sessionId,
      videoUrl: resolvedUrl,
      transcript: transcript,
      conceptIndex: conceptIndex,
    );
  }

  String _resolveRecordingUrl(String? raw) {
    final input = (raw ?? '').trim();
    if (input.isEmpty) {
      return '';
    }
    if (input.startsWith('http://') || input.startsWith('https://')) {
      return input;
    }
    final cdn = config.recordingCdnBaseUrl.trim();
    if (cdn.isNotEmpty) {
      final normalized = input.startsWith('/') ? input : '/$input';
      return '$cdn$normalized';
    }
    return input;
  }

  // END_PHASE2_IMPLEMENTATION
}
