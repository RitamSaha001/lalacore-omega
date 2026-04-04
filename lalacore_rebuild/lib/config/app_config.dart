class AppConfig {
  const AppConfig._();

  static const String googleScriptUrl = String.fromEnvironment(
    'GOOGLE_SCRIPT_URL',
    defaultValue:
        'https://script.google.com/macros/s/AKfycbxAYWZoKZF2p7fla8BNx_3vBJ_AIsF1_PrzVMKRdBhmkfm73TJ2lAAcCWs0hI3hPyH0fQ/exec?v=3',
  );

  static const String authBackendUrl = String.fromEnvironment(
    'AUTH_BACKEND_URL',
    defaultValue: 'http://10.0.2.2:8000/auth/action',
  );

  // Optional comma-separated fallback URLs for local backend failover.
  // Useful for real Android devices where host mapping differs from emulator.
  static const String authBackendFallbackUrls = String.fromEnvironment(
    'AUTH_BACKEND_FALLBACK_URLS',
    defaultValue:
        'http://127.0.0.1:8000/auth/action,http://localhost:8000/auth/action',
  );

  static const String masterSheetUrl = String.fromEnvironment(
    'MASTER_SHEET_URL',
    defaultValue:
        'https://docs.google.com/spreadsheets/d/e/2PACX-1vQhEsDCst4okp1QZD-Nn-MfbGl-zpIt8_W9K_622PpFC59VGGW1QzxuJoCyvPrM22Ato8E6KnOdCmpK/pub?gid=0&single=true&output=csv',
  );

  static const String aiEngineUrl = String.fromEnvironment(
    'AI_ENGINE_URL',
    defaultValue: '',
  );

  static const String aiEngineApiKey = String.fromEnvironment(
    'AI_ENGINE_API_KEY',
    defaultValue: '',
  );

  static const String aiEngineModel = String.fromEnvironment(
    'AI_ENGINE_MODEL',
    defaultValue: '',
  );

  static const String liveClassesApiBaseUrl = String.fromEnvironment(
    'LIVE_CLASSES_API_BASE_URL',
    defaultValue: '',
  );

  static const String liveClassesTranscriptionWsUrl = String.fromEnvironment(
    'LIVE_CLASSES_TRANSCRIPTION_WS_URL',
    defaultValue: '',
  );

  static const String liveClassesOcrEndpoint = String.fromEnvironment(
    'LIVE_CLASSES_OCR_ENDPOINT',
    defaultValue: '',
  );

  static const String liveClassesRecordingCdnBaseUrl = String.fromEnvironment(
    'LIVE_CLASSES_RECORDING_CDN_BASE_URL',
    defaultValue: '',
  );

  static const String liveClassesSessionToken = String.fromEnvironment(
    'LIVE_CLASSES_SESSION_TOKEN',
    defaultValue: '',
  );

  static const String liveClassesJwtAccessToken = String.fromEnvironment(
    'LIVE_CLASSES_JWT_ACCESS_TOKEN',
    defaultValue: '',
  );

  static const String liveClassesRequestSigningSecret = String.fromEnvironment(
    'LIVE_CLASSES_REQUEST_SIGNING_SECRET',
    defaultValue: '',
  );

  static const bool liveClassesEnablePushNotifications = bool.fromEnvironment(
    'LIVE_CLASSES_ENABLE_PUSH_NOTIFICATIONS',
    defaultValue: false,
  );

  static const bool liveClassesEnableRealServices = bool.fromEnvironment(
    'LIVE_CLASSES_ENABLE_REAL_SERVICES',
    defaultValue: false,
  );

  static const bool liveClassesForceMockServices = bool.fromEnvironment(
    'LIVE_CLASSES_FORCE_MOCK_SERVICES',
    defaultValue: false,
  );

  static const String liveClassesDefaultClassId = String.fromEnvironment(
    'LIVE_CLASSES_DEFAULT_CLASS_ID',
    defaultValue: 'physics_live_01',
  );

  static const String liveClassesDefaultTitle = String.fromEnvironment(
    'LIVE_CLASSES_DEFAULT_TITLE',
    defaultValue: 'JEE Live Class',
  );

  static const String liveClassesDefaultClassName = String.fromEnvironment(
    'LIVE_CLASSES_DEFAULT_CLASS_NAME',
    defaultValue: 'Class 11',
  );

  static const String liveClassesDefaultSubject = String.fromEnvironment(
    'LIVE_CLASSES_DEFAULT_SUBJECT',
    defaultValue: 'Physics',
  );

  static const String liveClassesDefaultTopic = String.fromEnvironment(
    'LIVE_CLASSES_DEFAULT_TOPIC',
    defaultValue: 'Electrostatics',
  );

  static const String liveClassesDefaultTeacherName = String.fromEnvironment(
    'LIVE_CLASSES_DEFAULT_TEACHER_NAME',
    defaultValue: 'Dr Sharma',
  );

  static const String liveClassesDefaultStartTime = String.fromEnvironment(
    'LIVE_CLASSES_DEFAULT_START_TIME',
    defaultValue: '6:00 PM',
  );

  static const String liveClassesEndpointClassSession = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_CLASS_SESSION',
    defaultValue: '/class/session',
  );

  static const String liveClassesEndpointClassState = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_CLASS_STATE',
    defaultValue: '/class/state',
  );

  static const String liveClassesEndpointClassJoinRequest =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_CLASS_JOIN_REQUEST',
        defaultValue: '/class/join_request',
      );

  static const String liveClassesEndpointClassJoinCancel =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_CLASS_JOIN_CANCEL',
        defaultValue: '/class/join_cancel',
      );

  static const String liveClassesEndpointClassAdmit = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_CLASS_ADMIT',
    defaultValue: '/class/admit',
  );

  static const String liveClassesEndpointClassReject = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_CLASS_REJECT',
    defaultValue: '/class/reject',
  );

  static const String liveClassesEndpointClassAdmitAll = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_CLASS_ADMIT_ALL',
    defaultValue: '/class/admit_all',
  );

  static const String liveClassesEndpointClassEvents = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_CLASS_EVENTS',
    defaultValue: '/class/events',
  );

  static const String liveClassesEndpointLiveToken = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_LIVE_TOKEN',
    defaultValue: '/live/token',
  );

  static const String liveClassesEndpointHealthPing = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_HEALTH_PING',
    defaultValue: '/health/ping',
  );

  static const String liveClassesEndpointRecordingStart =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_RECORDING_START',
        defaultValue: '/recording/start',
      );

  static const String liveClassesEndpointRecordingStop = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_RECORDING_STOP',
    defaultValue: '/recording/stop',
  );

  static const String liveClassesEndpointRecordingProcess =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS',
        defaultValue: '/recording/process',
      );

  static const String liveClassesEndpointRecordingProcessAsync =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_ASYNC',
        defaultValue: '/recording/process_async',
      );

  static const String liveClassesEndpointRecordingProcessStatus =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_RECORDING_PROCESS_STATUS',
        defaultValue: '/recording/process_status',
      );

  static const String liveClassesEndpointRecordingReplay =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_RECORDING_REPLAY',
        defaultValue: '/recording/replay',
      );

  static const String liveClassesEndpointQuizCreate = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_QUIZ_CREATE',
    defaultValue: '/quiz/create',
  );

  static const String liveClassesEndpointQuizStart = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_QUIZ_START',
    defaultValue: '/quiz/start',
  );

  static const String liveClassesEndpointQuizSubmit = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_QUIZ_SUBMIT',
    defaultValue: '/quiz/submit',
  );

  static const String liveClassesEndpointQuizResults = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_QUIZ_RESULTS',
    defaultValue: '/quiz/results',
  );

  static const String liveClassesEndpointQuizLibrary = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_QUIZ_LIBRARY',
    defaultValue: '/quiz/library',
  );

  static const String liveClassesEndpointLivePollCreate =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_LIVE_POLL_CREATE',
        defaultValue: '/live_poll/create',
      );

  static const String liveClassesEndpointLivePollSubmit =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_LIVE_POLL_SUBMIT',
        defaultValue: '/live_poll/submit',
      );

  static const String liveClassesEndpointLivePollResults =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_LIVE_POLL_RESULTS',
        defaultValue: '/live_poll/results',
      );

  static const String liveClassesEndpointLivePollEnd = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_LIVE_POLL_END',
    defaultValue: '/live_poll/end',
  );

  static const String liveClassesEndpointPracticeExtract =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_PRACTICE_EXTRACT',
        defaultValue: '/practice/extract',
      );

  static const String liveClassesEndpointWebrtcFallback =
      String.fromEnvironment(
        'LIVE_CLASSES_ENDPOINT_WEBRTC_FALLBACK',
        defaultValue: '/class/fallback_token',
      );

  static const String liveClassesEndpointAiExplain = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_AI_EXPLAIN',
    defaultValue: '/ai/class/explain',
  );

  static const String liveClassesEndpointAiNotes = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_AI_NOTES',
    defaultValue: '/ai/class/notes',
  );

  static const String liveClassesEndpointAiQuiz = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_AI_QUIZ',
    defaultValue: '/ai/class/quiz',
  );

  static const String liveClassesEndpointAiConcepts = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_AI_CONCEPTS',
    defaultValue: '/ai/class/concepts',
  );

  static const String liveClassesEndpointAiFlashcards = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_AI_FLASHCARDS',
    defaultValue: '/ai/class/flashcards',
  );

  static const String liveClassesEndpointAiAnalysis = String.fromEnvironment(
    'LIVE_CLASSES_ENDPOINT_AI_ANALYSIS',
    defaultValue: '/ai/class/analysis',
  );

  static const String teacherPasscode = String.fromEnvironment(
    'TEACHER_PASSCODE',
    defaultValue: 'Ritam@2026',
  );

  static const String forgotOtpSenderEmail = String.fromEnvironment(
    'FORGOT_OTP_SENDER_EMAIL',
    defaultValue: 'saharitam1212@gmail.com',
  );
}
