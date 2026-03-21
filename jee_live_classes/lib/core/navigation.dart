import 'dart:async';

import 'package:flutter/material.dart';

import '../models/class_session_model.dart';
import '../models/live_class_context.dart';
import '../models/replay_model.dart';
import '../modules/ai/lalacore_api_service.dart';
import '../modules/classroom/classroom_controller.dart';
import '../modules/join/join_readiness_screen.dart';
import '../modules/replay/lecture_replay_screen.dart';
import '../services/analytics_service.dart';
import '../services/class_join_service.dart';
import '../services/classroom_intelligence_service.dart';
import '../services/classroom_sync_service.dart';
import '../services/intelligence_storage.dart';
import '../services/live_services_readiness.dart';
import '../services/network_quality_service.dart';
import '../services/notification_service.dart';
import '../services/ocr_capture_service.dart';
import '../services/prejoin_settings_service.dart';
import '../services/quiz_service.dart';
import '../services/recording_service.dart';
import '../services/secure_api_client.dart';
import '../services/study_material_sync_service.dart';
import '../services/transcription_service.dart';
import '../services/webrtc_failover_service.dart';
import '../services/zoom_service.dart';
import 'app_config.dart';
import 'theme.dart';

class AppNavigation {
  static const String joinReadinessRoute = '/join-readiness';
  static const String classroomRoute = '/classroom';
  static const String replayRoute = '/replay';

  static Route<dynamic> onGenerateRoute(RouteSettings settings) {
    switch (settings.name) {
      case joinReadinessRoute:
        return MaterialPageRoute<void>(
          builder: (_) => const LiveClassFlowBootstrapPage(),
        );
      case classroomRoute:
        return MaterialPageRoute<void>(
          builder: (_) =>
              const Scaffold(body: Center(child: Text('Open via join flow'))),
        );
      case replayRoute:
        final replay = settings.arguments;
        if (replay is ReplayModel) {
          return MaterialPageRoute<void>(
            builder: (_) => LectureReplayScreen(replay: replay),
          );
        }
        return MaterialPageRoute<void>(
          builder: (_) =>
              const Scaffold(body: Center(child: Text('Replay data missing'))),
        );
      default:
        return MaterialPageRoute<void>(
          builder: (_) =>
              const Scaffold(body: Center(child: Text('Route not found'))),
        );
    }
  }
}

class LiveClassesApp extends StatelessWidget {
  const LiveClassesApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'JEE Live Classes',
      debugShowCheckedModeBanner: false,
      theme: AppTheme.lightTheme,
      initialRoute: AppNavigation.joinReadinessRoute,
      onGenerateRoute: AppNavigation.onGenerateRoute,
    );
  }
}

class LiveClassFlowBootstrapPage extends StatefulWidget {
  const LiveClassFlowBootstrapPage({
    super.key,
    this.contextOverride,
    this.configOverride,
  });

  final LiveClassContext? contextOverride;
  final AppConfig? configOverride;

  @override
  State<LiveClassFlowBootstrapPage> createState() =>
      _LiveClassFlowBootstrapPageState();
}

class _LiveClassFlowBootstrapPageState
    extends State<LiveClassFlowBootstrapPage> {
  // BEGIN_PHASE2_IMPLEMENTATION
  ClassroomController? _controller;
  String? _bootstrapError;
  late final PreJoinSettingsService _preJoinSettingsService;

  @override
  void initState() {
    super.initState();
    _bootstrap();
  }

  void _bootstrap() {
    try {
      final config = widget.configOverride ?? AppConfig.fromEnvironment();
      LiveServicesReadiness.ensureReadyForRealServices(config);

      final apiClient = SecureApiClient(config: config);
      final storage = IntelligenceStorage();
      unawaited(storage.initialize());
      _preJoinSettingsService = PreJoinSettingsService();

      final useMocks = config.enableMockServices;
      final liveClassContext =
          widget.contextOverride ?? config.toLiveClassContext();

      final zoomService = useMocks
          ? MockZoomService(currentUserId: liveClassContext.userId)
          : RealZoomService(
              currentUserId: liveClassContext.userId,
              currentUserName: liveClassContext.userName,
            );
      final transcriptionService = useMocks
          ? MockTranscriptionService()
          : RealTranscriptionService(
              streamUrl: config.transcriptionWsUrl,
              jwtToken: config.jwtAccessToken,
              speakerId: liveClassContext.userId,
              speakerName: liveClassContext.userName,
            );
      final recordingService = useMocks
          ? MockRecordingService()
          : RealRecordingService(config: config, apiClient: apiClient);
      final ocrService = useMocks
          ? MockOcrCaptureService()
          : RealOcrCaptureService(
              endpoint: config.ocrEndpoint,
              jwtToken: config.jwtAccessToken,
            );

      final networkQualityService = NetworkQualityService();
      final analyticsService = AnalyticsService();
      final intelligenceService = ClassroomIntelligenceService(
        storage: storage,
      );

      final lalacoreApi = LalacoreApi(
        config: config,
        apiClient: apiClient,
        useMockResponses: useMocks,
      );

      final notificationService = NotificationService(
        config: config,
        apiClient: apiClient,
      );
      unawaited(notificationService.initialize());
      final Uri? backendUri = Uri.tryParse(config.baseApiUrl.trim());
      final bool hasRealAppBackend =
          backendUri != null &&
          backendUri.host.trim().isNotEmpty &&
          backendUri.host.trim().toLowerCase() != 'api.example.com';
      final studyMaterialSyncService = StudyMaterialSyncService(
        config: config,
        apiClient: apiClient,
        enabled: hasRealAppBackend,
      );

      final quizService = QuizService(config: config, apiClient: apiClient);
      final classJoinService = useMocks
          ? MockClassJoinService()
          : RealClassJoinService(config: config, apiClient: apiClient);
      final classroomSyncService = useMocks
          ? MockClassroomSyncService()
          : RealClassroomSyncService(config: config);
      final webRtcFailoverService = useMocks
          ? MockWebRtcFailoverService()
          : RealWebRtcFailoverService();

      final session = ClassSessionModel(
        id: liveClassContext.classId,
        title: liveClassContext.classTitle,
        teacherName: liveClassContext.teacherName,
        startedAt: null,
        isRecording: false,
      );

      _controller = ClassroomController(
        session: session,
        zoomService: zoomService,
        transcriptionService: transcriptionService,
        recordingService: recordingService,
        networkQualityService: networkQualityService,
        ocrCaptureService: ocrService,
        lalacoreApi: lalacoreApi,
        intelligenceService: intelligenceService,
        analyticsService: analyticsService,
        quizService: quizService,
        notificationService: notificationService,
        studyMaterialSyncService: studyMaterialSyncService,
        intelligenceStorage: storage,
        classJoinService: classJoinService,
        classroomSyncService: classroomSyncService,
        webRtcFailoverService: webRtcFailoverService,
        liveClassContext: liveClassContext,
        authToken: liveClassContext.sessionToken,
        currentUserId: liveClassContext.userId,
        currentUserName: liveClassContext.userName,
        currentUserRole: liveClassContext.role,
      );
    } catch (error) {
      _bootstrapError = error.toString();
    }
  }

  @override
  void dispose() {
    _controller?.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final controller = _controller;
    if (_bootstrapError != null) {
      return Scaffold(
        appBar: AppBar(title: const Text('Live Classes Setup Error')),
        body: Padding(
          padding: const EdgeInsets.all(16),
          child: SelectableText(
            _bootstrapError!,
            style: const TextStyle(fontSize: 14),
          ),
        ),
      );
    }
    if (controller == null) {
      return const Scaffold(body: Center(child: CircularProgressIndicator()));
    }
    return JoinReadinessScreen(
      controller: controller,
      contextData: controller.liveClassContext,
      settingsService: _preJoinSettingsService,
    );
  }

  // END_PHASE2_IMPLEMENTATION
}
