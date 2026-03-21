import '../models/live_class_context.dart';

class AppConfig {
  // BEGIN_PHASE2_IMPLEMENTATION
  const AppConfig({
    required this.environment,
    required this.baseApiUrl,
    required this.lalacoreBaseUrl,
    required this.lalacoreApiKey,
    required this.zoomSessionId,
    required this.zoomSessionToken,
    required this.jwtAccessToken,
    required this.requestSigningSecret,
    required this.enableMockServices,
    required this.enablePushNotifications,
    required this.transcriptionWsUrl,
    required this.ocrEndpoint,
    required this.quizApiBaseUrl,
    required this.recordingApiBaseUrl,
    required this.recordingCdnBaseUrl,
    required this.liveUserId,
    required this.liveUserName,
    required this.liveUserRole,
    required this.liveClassTitle,
    required this.liveClassClassName,
    required this.liveClassSubject,
    required this.liveClassTopic,
    required this.liveClassStartTime,
    required this.liveTeacherName,
    this.classSessionEndpoint = '/class/session',
    this.classStateEndpoint = '/class/state',
    this.classJoinRequestEndpoint = '/class/join_request',
    this.classJoinCancelEndpoint = '/class/join_cancel',
    this.classAdmitEndpoint = '/class/admit',
    this.classRejectEndpoint = '/class/reject',
    this.classAdmitAllEndpoint = '/class/admit_all',
    this.classEventsEndpoint = '/class/events',
    this.classSyncEndpoint = '/class/sync',
    this.classLockEndpoint = '/class/lock',
    this.classChatEndpoint = '/class/chat',
    this.classWaitingRoomEndpoint = '/class/waiting_room',
    this.classRecordingEndpoint = '/class/recording',
    this.classMuteEndpoint = '/class/mute',
    this.classCameraEndpoint = '/class/camera',
    this.classRemoveEndpoint = '/class/remove',
    this.classBreakoutMoveEndpoint = '/class/breakout/move',
    this.classBreakoutBroadcastEndpoint = '/class/breakout/broadcast',
    this.classWhiteboardAccessEndpoint = '/class/whiteboard/access',
    this.liveTokenEndpoint = '/live/token',
    this.healthPingEndpoint = '/health/ping',
    this.recordingStartEndpoint = '/recording/start',
    this.recordingStopEndpoint = '/recording/stop',
    this.recordingProcessEndpoint = '/recording/process',
    this.recordingProcessAsyncEndpoint = '/recording/process_async',
    this.recordingProcessStatusEndpoint = '/recording/process_status',
    this.recordingProcessResultEndpoint = '/recording/process_result',
    this.recordingReplayEndpoint = '/recording/replay',
    this.quizCreateEndpoint = '/quiz/create',
    this.quizStartEndpoint = '/quiz/start',
    this.quizSubmitEndpoint = '/quiz/submit',
    this.quizResultsEndpoint = '/quiz/results',
    this.quizLibraryEndpoint = '/quiz/library',
    this.livePollCreateEndpoint = '/live_poll/create',
    this.livePollSubmitEndpoint = '/live_poll/submit',
    this.livePollResultsEndpoint = '/live_poll/results',
    this.livePollEndEndpoint = '/live_poll/end',
    this.practiceExtractEndpoint = '/practice/extract',
    this.practiceReviewQueueEndpoint = '/practice/review_queue',
    this.practiceReviewActionEndpoint = '/practice/review_action',
    this.webrtcFallbackEndpoint = '/class/fallback_token',
    this.aiExplainEndpoint = '/ai/class/explain',
    this.aiNotesEndpoint = '/ai/class/notes',
    this.aiQuizEndpoint = '/ai/class/quiz',
    this.aiConceptsEndpoint = '/ai/class/concepts',
    this.aiFlashcardsEndpoint = '/ai/class/flashcards',
    this.aiAnalysisEndpoint = '/ai/class/analysis',
  });

  final String environment;
  final String baseApiUrl;
  final String lalacoreBaseUrl;
  final String lalacoreApiKey;
  final String zoomSessionId;
  final String zoomSessionToken;
  final String jwtAccessToken;
  final String requestSigningSecret;
  final bool enableMockServices;
  final bool enablePushNotifications;
  final String transcriptionWsUrl;
  final String ocrEndpoint;
  final String quizApiBaseUrl;
  final String recordingApiBaseUrl;
  final String recordingCdnBaseUrl;
  final String liveUserId;
  final String liveUserName;
  final String liveUserRole;
  final String liveClassTitle;
  final String liveClassClassName;
  final String liveClassSubject;
  final String liveClassTopic;
  final String liveClassStartTime;
  final String liveTeacherName;

  final String classSessionEndpoint;
  final String classStateEndpoint;
  final String classJoinRequestEndpoint;
  final String classJoinCancelEndpoint;
  final String classAdmitEndpoint;
  final String classRejectEndpoint;
  final String classAdmitAllEndpoint;
  final String classEventsEndpoint;
  final String classSyncEndpoint;
  final String classLockEndpoint;
  final String classChatEndpoint;
  final String classWaitingRoomEndpoint;
  final String classRecordingEndpoint;
  final String classMuteEndpoint;
  final String classCameraEndpoint;
  final String classRemoveEndpoint;
  final String classBreakoutMoveEndpoint;
  final String classBreakoutBroadcastEndpoint;
  final String classWhiteboardAccessEndpoint;
  final String liveTokenEndpoint;
  final String healthPingEndpoint;

  final String recordingStartEndpoint;
  final String recordingStopEndpoint;
  final String recordingProcessEndpoint;
  final String recordingProcessAsyncEndpoint;
  final String recordingProcessStatusEndpoint;
  final String recordingProcessResultEndpoint;
  final String recordingReplayEndpoint;

  final String quizCreateEndpoint;
  final String quizStartEndpoint;
  final String quizSubmitEndpoint;
  final String quizResultsEndpoint;
  final String quizLibraryEndpoint;
  final String livePollCreateEndpoint;
  final String livePollSubmitEndpoint;
  final String livePollResultsEndpoint;
  final String livePollEndEndpoint;
  final String practiceExtractEndpoint;
  final String practiceReviewQueueEndpoint;
  final String practiceReviewActionEndpoint;
  final String webrtcFallbackEndpoint;

  final String aiExplainEndpoint;
  final String aiNotesEndpoint;
  final String aiQuizEndpoint;
  final String aiConceptsEndpoint;
  final String aiFlashcardsEndpoint;
  final String aiAnalysisEndpoint;

  static const String _unset = '__LIVE_CLASSES_UNSET__';

  static AppConfig fromEnvironment() {
    final baseApiUrl = _trimTrailingSlash(
      _readString(const [
        'LIVE_CLASSES_API_BASE_URL',
        'BASE_API_URL',
      ], fallback: 'https://api.example.com'),
    );

    final explicitRealServices = _readBool(const [
      'LIVE_CLASSES_ENABLE_REAL_SERVICES',
    ], fallback: false);
    final forceMockServices = _readBool(const [
      'LIVE_CLASSES_FORCE_MOCK_SERVICES',
    ], fallback: false);
    final useMockServicesFlag = _readBool(const [
      'USE_MOCK_SERVICES',
      'LIVE_CLASSES_USE_MOCK_SERVICES',
    ], fallback: true);

    final enableMockServices =
        forceMockServices || (!explicitRealServices && useMockServicesFlag);

    final transcriptionWsUrl = _readString(const [
      'LIVE_CLASSES_TRANSCRIPTION_WS_URL',
      'TRANSCRIPTION_WS_URL',
    ], fallback: _deriveWsUrl(baseApiUrl));

    final ocrEndpoint = _readString(const [
      'LIVE_CLASSES_OCR_ENDPOINT',
      'OCR_ENDPOINT',
    ], fallback: '$baseApiUrl/ocr/frame');

    final quizApiBaseUrl = _trimTrailingSlash(
      _readString(const [
        'LIVE_CLASSES_QUIZ_API_BASE_URL',
        'QUIZ_API_BASE_URL',
      ], fallback: baseApiUrl),
    );

    final recordingApiBaseUrl = _trimTrailingSlash(
      _readString(const [
        'LIVE_CLASSES_RECORDING_API_BASE_URL',
        'RECORDING_API_BASE_URL',
      ], fallback: baseApiUrl),
    );
    final recordingCdnBaseUrl = _trimTrailingSlash(
      _readString(const [
        'LIVE_CLASSES_RECORDING_CDN_BASE_URL',
        'LIVE_CLASSES_CDN_BASE_URL',
      ], fallback: ''),
    );

    final lalacoreBaseUrl = _trimTrailingSlash(
      _readString(const [
        'LIVE_CLASSES_LALACORE_BASE_URL',
        'LALACORE_BASE_URL',
      ], fallback: baseApiUrl),
    );

    final liveClassId = _readString(const [
      'LIVE_CLASSES_CLASS_ID',
      'ZOOM_SESSION_ID',
    ], fallback: 'jee_session_101');

    final sessionToken = _readString(const [
      'LIVE_CLASSES_SESSION_TOKEN',
      'ZOOM_SESSION_TOKEN',
    ], fallback: '');

    final jwtToken = _readString(const [
      'LIVE_CLASSES_JWT_ACCESS_TOKEN',
      'JWT_ACCESS_TOKEN',
    ], fallback: '');

    return AppConfig(
      environment: _readString(const [
        'LIVE_CLASSES_APP_ENV',
        'APP_ENV',
      ], fallback: 'development'),
      baseApiUrl: baseApiUrl,
      lalacoreBaseUrl: lalacoreBaseUrl,
      lalacoreApiKey: _readString(const [
        'LIVE_CLASSES_LALACORE_API_KEY',
        'LALACORE_API_KEY',
      ], fallback: ''),
      zoomSessionId: liveClassId,
      zoomSessionToken: sessionToken,
      jwtAccessToken: jwtToken,
      requestSigningSecret: _readString(const [
        'LIVE_CLASSES_REQUEST_SIGNING_SECRET',
        'REQUEST_SIGNING_SECRET',
      ], fallback: ''),
      enableMockServices: enableMockServices,
      enablePushNotifications: _readBool(const [
        'LIVE_CLASSES_ENABLE_PUSH_NOTIFICATIONS',
        'ENABLE_PUSH_NOTIFICATIONS',
      ], fallback: false),
      transcriptionWsUrl: transcriptionWsUrl,
      ocrEndpoint: ocrEndpoint,
      quizApiBaseUrl: quizApiBaseUrl,
      recordingApiBaseUrl: recordingApiBaseUrl,
      recordingCdnBaseUrl: recordingCdnBaseUrl,
      liveUserId: _readString(const [
        'LIVE_CLASSES_USER_ID',
        'LIVE_USER_ID',
      ], fallback: 'student_01'),
      liveUserName: _readString(const [
        'LIVE_CLASSES_USER_NAME',
        'LIVE_USER_NAME',
      ], fallback: 'Ritam Saha'),
      liveUserRole: _readString(const [
        'LIVE_CLASSES_USER_ROLE',
        'LIVE_USER_ROLE',
      ], fallback: 'student'),
      liveClassTitle: _readString(const [
        'LIVE_CLASSES_DEFAULT_TITLE',
        'LIVE_CLASS_TITLE',
      ], fallback: 'JEE Advanced: Definite Integration Masterclass'),
      liveClassClassName: _readString(const [
        'LIVE_CLASSES_DEFAULT_CLASS_NAME',
        'LIVE_CLASS_CLASS_NAME',
      ], fallback: 'Class 11'),
      liveClassSubject: _readString(const [
        'LIVE_CLASSES_DEFAULT_SUBJECT',
        'LIVE_CLASS_SUBJECT',
      ], fallback: 'Physics'),
      liveClassTopic: _readString(const [
        'LIVE_CLASSES_DEFAULT_TOPIC',
        'LIVE_CLASS_TOPIC',
      ], fallback: 'Electrostatics'),
      liveClassStartTime: _readString(const [
        'LIVE_CLASSES_DEFAULT_START_TIME',
        'LIVE_CLASS_START_TIME',
      ], fallback: '6:00 PM'),
      liveTeacherName: _readString(const [
        'LIVE_CLASSES_DEFAULT_TEACHER_NAME',
        'LIVE_CLASS_TEACHER',
      ], fallback: 'Dr. A. Sharma'),
      classSessionEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_SESSION',
      ], fallback: '/class/session'),
      classStateEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_STATE',
      ], fallback: '/class/state'),
      classJoinRequestEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_JOIN_REQUEST',
      ], fallback: '/class/join_request'),
      classJoinCancelEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_JOIN_CANCEL',
      ], fallback: '/class/join_cancel'),
      classAdmitEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_ADMIT',
      ], fallback: '/class/admit'),
      classRejectEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_REJECT',
      ], fallback: '/class/reject'),
      classAdmitAllEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_ADMIT_ALL',
      ], fallback: '/class/admit_all'),
      classEventsEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_EVENTS',
      ], fallback: '/class/events'),
      classSyncEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_SYNC',
      ], fallback: '/class/sync'),
      classLockEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_LOCK',
      ], fallback: '/class/lock'),
      classChatEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_CHAT',
      ], fallback: '/class/chat'),
      classWaitingRoomEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_WAITING_ROOM',
      ], fallback: '/class/waiting_room'),
      classRecordingEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_RECORDING',
      ], fallback: '/class/recording'),
      classMuteEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_MUTE',
      ], fallback: '/class/mute'),
      classCameraEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_CAMERA',
      ], fallback: '/class/camera'),
      classRemoveEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_REMOVE',
      ], fallback: '/class/remove'),
      classBreakoutMoveEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_BREAKOUT_MOVE',
      ], fallback: '/class/breakout/move'),
      classBreakoutBroadcastEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_BREAKOUT_BROADCAST',
      ], fallback: '/class/breakout/broadcast'),
      classWhiteboardAccessEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_CLASS_WHITEBOARD_ACCESS',
      ], fallback: '/class/whiteboard/access'),
      liveTokenEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_LIVE_TOKEN',
      ], fallback: '/live/token'),
      healthPingEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_HEALTH_PING',
      ], fallback: '/health/ping'),
      recordingStartEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_RECORDING_START',
      ], fallback: '/recording/start'),
      recordingStopEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_RECORDING_STOP',
      ], fallback: '/recording/stop'),
      recordingProcessEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS',
      ], fallback: '/recording/process'),
      recordingProcessAsyncEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_ASYNC',
      ], fallback: '/recording/process_async'),
      recordingProcessStatusEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_STATUS',
      ], fallback: '/recording/process_status'),
      recordingProcessResultEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_RESULT',
      ], fallback: '/recording/process_result'),
      recordingReplayEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_RECORDING_REPLAY',
      ], fallback: '/recording/replay'),
      quizCreateEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_QUIZ_CREATE',
      ], fallback: '/quiz/create'),
      quizStartEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_QUIZ_START',
      ], fallback: '/quiz/start'),
      quizSubmitEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_QUIZ_SUBMIT',
      ], fallback: '/quiz/submit'),
      quizResultsEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_QUIZ_RESULTS',
      ], fallback: '/quiz/results'),
      quizLibraryEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_QUIZ_LIBRARY',
      ], fallback: '/quiz/library'),
      livePollCreateEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_LIVE_POLL_CREATE',
      ], fallback: '/live_poll/create'),
      livePollSubmitEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_LIVE_POLL_SUBMIT',
      ], fallback: '/live_poll/submit'),
      livePollResultsEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_LIVE_POLL_RESULTS',
      ], fallback: '/live_poll/results'),
      livePollEndEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_LIVE_POLL_END',
      ], fallback: '/live_poll/end'),
      practiceExtractEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_PRACTICE_EXTRACT',
      ], fallback: '/practice/extract'),
      practiceReviewQueueEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_PRACTICE_REVIEW_QUEUE',
      ], fallback: '/practice/review_queue'),
      practiceReviewActionEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_PRACTICE_REVIEW_ACTION',
      ], fallback: '/practice/review_action'),
      webrtcFallbackEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_WEBRTC_FALLBACK',
      ], fallback: '/class/fallback_token'),
      aiExplainEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_AI_EXPLAIN',
      ], fallback: '/ai/class/explain'),
      aiNotesEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_AI_NOTES',
      ], fallback: '/ai/class/notes'),
      aiQuizEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_AI_QUIZ',
      ], fallback: '/ai/class/quiz'),
      aiConceptsEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_AI_CONCEPTS',
      ], fallback: '/ai/class/concepts'),
      aiFlashcardsEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_AI_FLASHCARDS',
      ], fallback: '/ai/class/flashcards'),
      aiAnalysisEndpoint: _readString(const [
        'LIVE_CLASSES_ENDPOINT_AI_ANALYSIS',
      ], fallback: '/ai/class/analysis'),
    );
  }

  Uri apiUri(
    String endpoint, {
    Map<String, dynamic>? queryParameters,
    String? customBaseUrl,
  }) {
    final target = endpoint.trim();
    final hasAbsoluteScheme =
        target.startsWith('https://') || target.startsWith('http://');
    final uri = hasAbsoluteScheme
        ? Uri.parse(target)
        : Uri.parse(_joinUrl(customBaseUrl ?? baseApiUrl, target));

    if (queryParameters == null || queryParameters.isEmpty) {
      return uri;
    }

    return uri.replace(
      queryParameters: {
        ...uri.queryParameters,
        ...queryParameters.map((key, value) => MapEntry(key, value.toString())),
      },
    );
  }

  Uri quizApiUri(String endpoint, {Map<String, dynamic>? queryParameters}) {
    return apiUri(
      endpoint,
      queryParameters: queryParameters,
      customBaseUrl: quizApiBaseUrl,
    );
  }

  Uri recordingApiUri(
    String endpoint, {
    Map<String, dynamic>? queryParameters,
  }) {
    return apiUri(
      endpoint,
      queryParameters: queryParameters,
      customBaseUrl: recordingApiBaseUrl,
    );
  }

  Uri lalacoreUri(String endpoint, {Map<String, dynamic>? queryParameters}) {
    return apiUri(
      endpoint,
      queryParameters: queryParameters,
      customBaseUrl: lalacoreBaseUrl,
    );
  }

  LiveClassContext toLiveClassContext() {
    return LiveClassContext(
      userId: liveUserId,
      userName: liveUserName,
      role: liveUserRole,
      classId: zoomSessionId,
      sessionToken: zoomSessionToken.isNotEmpty
          ? zoomSessionToken
          : jwtAccessToken,
      classTitle: liveClassTitle,
      className: liveClassClassName,
      subject: liveClassSubject,
      topic: liveClassTopic,
      teacherName: liveTeacherName,
      startTimeLabel: liveClassStartTime,
    );
  }

  static String _readString(List<String> keys, {required String fallback}) {
    for (final key in keys) {
      final value = String.fromEnvironment(key, defaultValue: _unset);
      if (value != _unset && value.trim().isNotEmpty) {
        return value.trim();
      }
    }
    return fallback;
  }

  static bool _readBool(List<String> keys, {required bool fallback}) {
    for (final key in keys) {
      final value = String.fromEnvironment(key, defaultValue: _unset);
      if (value == _unset) {
        continue;
      }
      final normalized = value.trim().toLowerCase();
      if (normalized == '1' ||
          normalized == 'true' ||
          normalized == 'yes' ||
          normalized == 'on') {
        return true;
      }
      if (normalized == '0' ||
          normalized == 'false' ||
          normalized == 'no' ||
          normalized == 'off') {
        return false;
      }
    }
    return fallback;
  }

  static String _deriveWsUrl(String baseApiUrl) {
    final baseUri = Uri.tryParse(baseApiUrl);
    if (baseUri == null || baseUri.host.isEmpty) {
      return 'wss://api.example.com/transcription/stream';
    }
    final wsScheme = baseUri.scheme == 'http' ? 'ws' : 'wss';
    return baseUri
        .replace(scheme: wsScheme, path: '/transcription/stream', query: '')
        .toString();
  }

  static String _joinUrl(String base, String endpoint) {
    final normalizedBase = _trimTrailingSlash(base);
    final normalizedPath = endpoint.startsWith('/') ? endpoint : '/$endpoint';
    return '$normalizedBase$normalizedPath';
  }

  static String _trimTrailingSlash(String value) {
    final trimmed = value.trim();
    if (trimmed.isEmpty) {
      return trimmed;
    }
    return trimmed.endsWith('/')
        ? trimmed.substring(0, trimmed.length - 1)
        : trimmed;
  }

  // END_PHASE2_IMPLEMENTATION
}
