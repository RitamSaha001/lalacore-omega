import 'dart:async';
import 'dart:ui';

import 'package:flutter/foundation.dart';

import '../../models/class_session_model.dart';
import '../../models/doubt_queue_model.dart';
import '../../models/extracted_practice_question_model.dart';
import '../../models/lecture_index_model.dart';
import '../../models/lecture_intelligence_model.dart';
import '../../models/lecture_notes_model.dart';
import '../../models/live_class_context.dart';
import '../../models/live_poll_model.dart';
import '../../models/network_stats_model.dart';
import '../../models/participant_model.dart';
import '../../models/replay_model.dart';
import '../../models/transcript_model.dart';
import '../../models/waiting_room_request_model.dart';
import '../../services/analytics_service.dart';
import '../../services/class_join_service.dart';
import '../../services/classroom_intelligence_service.dart';
import '../../services/classroom_sync_service.dart';
import '../../services/intelligence_storage.dart';
import '../../services/lecture_notes_pdf_service.dart';
import '../../services/network_quality_service.dart';
import '../../services/notification_service.dart';
import '../../services/ocr_capture_service.dart';
import '../../services/quiz_service.dart';
import '../../services/recording_service.dart';
import '../../services/study_material_sync_service.dart';
import '../../services/transcription_service.dart';
import '../../services/webrtc_failover_service.dart';
import '../../services/zoom_service.dart';
import '../ai/lalacore_api_service.dart';
import '../breakout_rooms/breakout_room_manager.dart';
import 'classroom_state.dart';

class ClassroomController extends ValueNotifier<ClassroomState> {
  // BEGIN_PHASE2_IMPLEMENTATION
  ClassroomController({
    required ClassSessionModel session,
    required this.zoomService,
    required this.transcriptionService,
    required this.recordingService,
    required this.networkQualityService,
    required this.ocrCaptureService,
    required this.lalacoreApi,
    required this.intelligenceService,
    required this.quizService,
    required this.notificationService,
    required this.studyMaterialSyncService,
    required this.intelligenceStorage,
    required this.classJoinService,
    required this.classroomSyncService,
    required this.webRtcFailoverService,
    required this.liveClassContext,
    required String authToken,
    required this.currentUserId,
    required this.currentUserName,
    required this.currentUserRole,
    LectureNotesPdfService? lectureNotesPdfService,
    AnalyticsService? analyticsService,
  }) : lectureNotesPdfService =
           lectureNotesPdfService ?? const LectureNotesPdfService(),
       _authToken = authToken,
       analyticsService = analyticsService ?? AnalyticsService(),
       super(ClassroomState.initial(session: session));

  final ZoomService zoomService;
  final TranscriptionService transcriptionService;
  final RecordingService recordingService;
  final NetworkQualityService networkQualityService;
  final OcrCaptureService ocrCaptureService;
  final LalacoreApi lalacoreApi;
  final ClassroomIntelligenceService intelligenceService;
  final QuizService quizService;
  final NotificationService notificationService;
  final StudyMaterialSyncService studyMaterialSyncService;
  final IntelligenceStorage intelligenceStorage;
  final ClassJoinService classJoinService;
  final ClassroomSyncService classroomSyncService;
  final WebRtcFailoverService webRtcFailoverService;
  final LiveClassContext liveClassContext;
  final LectureNotesPdfService lectureNotesPdfService;
  final AnalyticsService analyticsService;

  String _authToken;
  final String currentUserId;
  final String currentUserName;
  final String currentUserRole;

  final BreakoutRoomManager _breakoutRoomManager = BreakoutRoomManager();
  final List<StreamSubscription<dynamic>> _subscriptions = [];

  bool _initialized = false;
  bool _preJoinPrepared = false;
  bool _classroomStreamsBound = false;
  bool _joinStreamsBound = false;
  bool _syncConnected = false;
  bool _isGeneratingIndexes = false;
  bool _isRunningIntelligence = false;
  int _eventCounter = 0;
  int _trimmedTranscriptSegments = 0;
  Timer? _intelligenceDebounce;
  Timer? _livePollTimer;
  Timer? _reconnectTimer;
  int _reconnectAttempts = 0;
  List<WaitingRoomRequestModel> _zoomWaitingRequests = const [];
  List<WaitingRoomRequestModel> _joinWaitingRequests = const [];
  final Set<String> _livePollSubmitters = <String>{};
  final List<TranscriptModel> _fullTranscript = <TranscriptModel>[];

  static const int _liveTranscriptWindowSize = 160;
  static const int _maxTranscriptHistorySize = 2000;

  bool get canManageClass {
    final normalized = currentUserRole.toLowerCase();
    final roleTeacher =
        normalized == 'teacher' ||
        normalized == 'host' ||
        normalized == 'cohost' ||
        normalized == 'co_host';
    return roleTeacher ||
        liveClassContext.isTeacher ||
        (_currentUser?.isTeacher ?? false);
  }

  bool get canDrawWhiteboard {
    return canManageClass || value.activeWhiteboardUserId == currentUserId;
  }

  String get authToken => _authToken;

  bool get usesMockServices {
    return zoomService is MockZoomService ||
        zoomService.isSimulatedBridge ||
        transcriptionService is MockTranscriptionService ||
        recordingService is MockRecordingService ||
        ocrCaptureService is MockOcrCaptureService ||
        classJoinService is MockClassJoinService ||
        classroomSyncService is MockClassroomSyncService ||
        webRtcFailoverService is MockWebRtcFailoverService ||
        lalacoreApi.useMockResponses;
  }

  String get serviceModeLabel =>
      usesMockServices ? 'Simulated Mode' : 'Live Services';

  String get serviceModeSummary {
    if (usesMockServices) {
      return 'Some classroom services are running in simulated mode. Core flows remain usable, but media, transcription, OCR, or AI may be mock-backed.';
    }
    return 'Live classroom integrations are enabled by configuration for media, sync, OCR, AI, and recording.';
  }

  String get preJoinMediaReadinessNote {
    if (usesMockServices) {
      return 'Camera and mic readiness cards are simulated in mock mode.';
    }
    return 'Network checks are live. Camera preview and mic meter still need native hardware-readiness wiring for true Zoom-grade prejoin validation.';
  }

  Future<void> prepareJoinFlow() async {
    if (_preJoinPrepared) {
      return;
    }
    await _refreshLiveSessionAccess();
    await zoomService.initialize(session: value.session, authToken: authToken);
    await classJoinService.startPresenceSubscription(liveClassContext);
    if (!_syncConnected) {
      await classroomSyncService.connect(liveClassContext);
      _syncConnected = true;
    }
    _bindJoinStreams();
    _preJoinPrepared = true;
  }

  Future<void> initialize() async {
    if (_initialized) {
      return;
    }
    _initialized = true;
    value = value.copyWith(isJoining: true, clearError: true);

    try {
      await prepareJoinFlow();
      _bindStreams();

      await zoomService.join();
      await transcriptionService.start();
      try {
        await _rehydrateClassroomState();
      } catch (_) {
        // Joining the live class should not fail just because the
        // state snapshot endpoint is temporarily unavailable.
      }

      value = value.copyWith(
        isJoining: false,
        isConnected: true,
        session: value.session.copyWith(startedAt: DateTime.now()),
        joinFlowStatus: JoinFlowStatus.approved,
        clearPendingJoinRequestId: true,
        clearJoinStatusMessage: true,
      );

      unawaited(
        notificationService.notifyClassStarting(classId: value.session.id),
      );
    } catch (error) {
      value = value.copyWith(
        isJoining: false,
        joinFlowStatus: JoinFlowStatus.rejected,
        joinStatusMessage: 'Failed to join classroom',
        error: 'Failed to join classroom: $error',
      );
    }
  }

  Future<ClassSessionModel> fetchClassSessionForJoin() async {
    final session = await classJoinService.fetchClassSession(liveClassContext);
    value = value.copyWith(session: session);
    return session;
  }

  Future<NetworkStatsModel> checkJoinNetworkQuality() async {
    final stats = await classJoinService.checkNetworkQuality();
    value = value.copyWith(networkStats: stats);
    return stats;
  }

  Future<String> requestJoin({
    required bool cameraEnabled,
    required bool micEnabled,
    required bool speakerTested,
  }) async {
    if (value.joinFlowStatus == JoinFlowStatus.waitingApproval &&
        value.pendingJoinRequestId != null) {
      return value.pendingJoinRequestId!;
    }
    await prepareJoinFlow();
    value = value.copyWith(
      joinFlowStatus: JoinFlowStatus.requesting,
      clearJoinStatusMessage: true,
      clearPendingJoinRequestId: true,
    );
    try {
      final requestId = await classJoinService.requestJoin(
        context: liveClassContext,
        deviceInfo: _buildDeviceInfo(speakerTested: speakerTested),
        cameraEnabled: cameraEnabled,
        micEnabled: micEnabled,
      );
      value = value.copyWith(
        joinFlowStatus: JoinFlowStatus.waitingApproval,
        pendingJoinRequestId: requestId,
      );
      return requestId;
    } catch (error) {
      value = value.copyWith(
        joinFlowStatus: JoinFlowStatus.rejected,
        joinStatusMessage: 'Join request failed: $error',
      );
      rethrow;
    }
  }

  Future<void> cancelJoinRequest(String requestId) async {
    try {
      await classJoinService.cancelJoinRequest(
        context: liveClassContext,
        requestId: requestId,
      );
    } finally {
      value = value.copyWith(
        joinFlowStatus: JoinFlowStatus.idle,
        clearPendingJoinRequestId: true,
      );
    }
  }

  Future<void> applyPreJoinSettings({
    required bool cameraEnabled,
    required bool micEnabled,
  }) async {
    try {
      await zoomService.toggleCamera(
        participantId: currentUserId,
        enabled: cameraEnabled,
      );
    } catch (_) {}
    try {
      await zoomService.toggleMic(
        participantId: currentUserId,
        enabled: micEnabled,
      );
    } catch (_) {}
    value = value.copyWith(
      currentUserCameraEnabled: cameraEnabled,
      currentUserMicEnabled: micEnabled,
    );
  }

  Future<void> startClassFromReadiness({
    required bool cameraEnabled,
    required bool micEnabled,
  }) async {
    await initialize();
    await applyPreJoinSettings(
      cameraEnabled: cameraEnabled,
      micEnabled: micEnabled,
    );
  }

  Stream<JoinApprovalEvent> get joinEventsStream =>
      classJoinService.joinApprovalStream;

  int queuePositionForCurrentUser() {
    final index = value.waitingRoomRequests.indexWhere(
      (item) => item.participantId == currentUserId,
    );
    return index == -1 ? -1 : index + 1;
  }

  Future<void> leaveClass() async {
    _livePollTimer?.cancel();
    _reconnectTimer?.cancel();
    await transcriptionService.stop();
    await ocrCaptureService.stopCapture();
    await webRtcFailoverService.stop();
    await zoomService.leave();
    _initialized = false;

    value = value.copyWith(
      isConnected: false,
      clearActiveSpeaker: true,
      clearPinnedParticipant: true,
      clearSharedContent: true,
      panel: ClassroomPanel.none,
      quiz: QuizState.idle,
      clearActiveBreakoutRoom: true,
      clearCurrentPoll: true,
      pollActive: false,
      pollTimer: 0,
      pollResults: const {},
      clearSubmittedPollOption: true,
      pollResultsRevealed: false,
      joinFlowStatus: JoinFlowStatus.idle,
      clearJoinStatusMessage: true,
      clearPendingJoinRequestId: true,
      doubtQueue: const [],
      clearActiveDoubtId: true,
      raisedHands: const [],
      whiteboardAccessRequests: const [],
      clearActiveWhiteboardUserId: true,
      focusModeEnabled: false,
      laserPointerEnabled: false,
      clearLaserPointerPosition: true,
      silentConceptCheckMode: false,
      clearAiTeachingSuggestion: true,
      clearTeacherSummaryReport: true,
      extractedPracticeQuestions: const [],
      clearRecordingJobId: true,
      clearRecordingJobStatus: true,
      failoverModeEnabled: false,
      clearFailoverMessage: true,
    );
  }

  void setLayoutMode(ClassroomLayoutMode layoutMode) {
    value = value.copyWith(layoutMode: layoutMode);
  }

  void setPanel(ClassroomPanel panel) {
    value = value.copyWith(panel: panel);
    if (panel == ClassroomPanel.analytics && canManageClass) {
      unawaited(refreshPracticeReviewQueue());
    }
  }

  void toggleRevisionMode() {
    value = value.copyWith(revisionModeEnabled: !value.revisionModeEnabled);
  }

  Future<void> pinParticipant(String participantId) async {
    await zoomService.pinParticipant(participantId);
    value = value.copyWith(pinnedParticipantId: participantId);
  }

  Future<void> clearPinnedParticipant() async {
    await zoomService.unpinParticipant();
    value = value.copyWith(clearPinnedParticipant: true);
  }

  Future<void> toggleMic() async {
    final enabled = !value.currentUserMicEnabled;
    await zoomService.toggleMic(participantId: currentUserId, enabled: enabled);
    value = value.copyWith(currentUserMicEnabled: enabled);
  }

  Future<void> toggleCamera() async {
    final enabled = !value.currentUserCameraEnabled;
    await zoomService.toggleCamera(
      participantId: currentUserId,
      enabled: enabled,
    );
    value = value.copyWith(currentUserCameraEnabled: enabled);
  }

  Future<void> toggleRaiseHand() async {
    if (value.currentUserHandRaised) {
      await lowerHand();
    } else {
      await raiseHand();
    }
  }

  Future<void> raiseHand() async {
    if (value.focusModeEnabled && !canManageClass) {
      value = value.copyWith(
        error: 'Focus mode is active. Please wait for teacher instructions.',
      );
      return;
    }
    await zoomService.setRaiseHand(participantId: currentUserId, raised: true);
    await _publishSyncEvent(
      ClassroomSyncEvent(
        type: ClassroomSyncEventType.raiseHand,
        classId: value.session.id,
        senderId: currentUserId,
        targetUserId: currentUserId,
        timestamp: DateTime.now(),
      ),
    );
    analyticsService.onReactionOrHandRaise();
    _scheduleIntelligenceRefresh();
    value = value.copyWith(currentUserHandRaised: true);
  }

  Future<void> lowerHand() async {
    await zoomService.setRaiseHand(participantId: currentUserId, raised: false);
    await _publishSyncEvent(
      ClassroomSyncEvent(
        type: ClassroomSyncEventType.lowerHand,
        classId: value.session.id,
        senderId: currentUserId,
        targetUserId: currentUserId,
        timestamp: DateTime.now(),
      ),
    );
    value = value.copyWith(currentUserHandRaised: false);
  }

  Future<void> approveStudentMic(String participantId) async {
    await zoomService.toggleMic(participantId: participantId, enabled: true);
    await lowerHandFor(participantId);
    await _publishSyncEvent(
      ClassroomSyncEvent(
        type: ClassroomSyncEventType.approveMic,
        classId: value.session.id,
        senderId: currentUserId,
        targetUserId: participantId,
        timestamp: DateTime.now(),
      ),
    );
  }

  Future<void> lowerHandFor(String participantId) async {
    await zoomService.setRaiseHand(participantId: participantId, raised: false);
    await _publishSyncEvent(
      ClassroomSyncEvent(
        type: ClassroomSyncEventType.lowerHand,
        classId: value.session.id,
        senderId: currentUserId,
        targetUserId: participantId,
        timestamp: DateTime.now(),
      ),
    );
    value = value.copyWith(
      raisedHands: value.raisedHands
          .where((participant) => participant.id != participantId)
          .toList(growable: false),
      currentUserHandRaised: participantId == currentUserId
          ? false
          : value.currentUserHandRaised,
    );
  }

  Future<void> sendReaction(String emoji) async {
    if (value.focusModeEnabled && !canManageClass) {
      return;
    }
    await zoomService.sendReaction(emoji);
    analyticsService.onReactionOrHandRaise();
  }

  Future<void> setFocusMode(bool enabled) async {
    if (!canManageClass) {
      return;
    }
    if (enabled) {
      await setChatEnabled(false);
      value = value.copyWith(
        focusModeEnabled: true,
        panel: ClassroomPanel.none,
        layoutMode: ClassroomLayoutMode.presentation,
        broadcastMessage:
            'Focus mode enabled. Chat and reactions are temporarily restricted.',
      );
      return;
    }

    await setChatEnabled(true);
    value = value.copyWith(
      focusModeEnabled: false,
      broadcastMessage: 'Focus mode disabled.',
    );
  }

  void toggleLaserPointer() {
    final enabled = !value.laserPointerEnabled;
    value = value.copyWith(
      laserPointerEnabled: enabled,
      laserPointerPosition: enabled
          ? (value.laserPointerPosition ?? const Offset(0.5, 0.5))
          : null,
      clearLaserPointerPosition: !enabled,
    );
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.laserToggle,
          classId: value.session.id,
          senderId: currentUserId,
          enabled: enabled,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  void updateLaserPointer(Offset normalizedPosition) {
    if (!value.laserPointerEnabled || !canManageClass) {
      return;
    }
    final clamped = Offset(
      normalizedPosition.dx.clamp(0.0, 1.0),
      normalizedPosition.dy.clamp(0.0, 1.0),
    );
    value = value.copyWith(laserPointerPosition: clamped);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.laserMove,
          classId: value.session.id,
          senderId: currentUserId,
          positionX: clamped.dx,
          positionY: clamped.dy,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  void requestWhiteboardAccess() {
    if (canManageClass) {
      return;
    }
    if (value.whiteboardAccessRequests.contains(currentUserId)) {
      return;
    }
    value = value.copyWith(
      whiteboardAccessRequests: [
        ...value.whiteboardAccessRequests,
        currentUserId,
      ],
      broadcastMessage: 'Whiteboard access requested by $_currentUserName.',
    );
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.whiteboardRequest,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: currentUserId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> approveWhiteboardAccess(String participantId) async {
    if (!canManageClass) {
      return;
    }
    await classJoinService.updateWhiteboardAccess(
      classId: value.session.id,
      userId: participantId,
      enabled: true,
    );
    final filtered = value.whiteboardAccessRequests
        .where((item) => item != participantId)
        .toList(growable: false);
    value = value.copyWith(
      activeWhiteboardUserId: participantId,
      whiteboardAccessRequests: filtered,
      panel: ClassroomPanel.whiteboard,
    );
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.whiteboardGrant,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: participantId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  void dismissWhiteboardRequest(String participantId) {
    final filtered = value.whiteboardAccessRequests
        .where((item) => item != participantId)
        .toList(growable: false);
    value = value.copyWith(whiteboardAccessRequests: filtered);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.whiteboardDismiss,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: participantId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> revokeWhiteboardAccess() async {
    if (!_requireHostAction('revoke whiteboard access')) {
      return;
    }
    final activeUserId = value.activeWhiteboardUserId;
    if (activeUserId != null && activeUserId.isNotEmpty) {
      await classJoinService.updateWhiteboardAccess(
        classId: value.session.id,
        userId: activeUserId,
        enabled: false,
      );
    }
    value = value.copyWith(clearActiveWhiteboardUserId: true);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.whiteboardRevoke,
          classId: value.session.id,
          senderId: currentUserId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> startScreenShare() async {
    if (!_requireHostAction('start screen share')) {
      return;
    }
    await zoomService.startScreenShare('Teacher Screen Share');
  }

  Future<void> stopScreenShare() async {
    if (!_requireHostAction('stop screen share')) {
      return;
    }
    await zoomService.stopScreenShare();
  }

  Future<void> approveWaitingRoomRequest(String participantId) async {
    if (!_requireHostAction('approve waiting-room requests')) {
      return;
    }
    await classJoinService.approveJoinRequest(
      classId: value.session.id,
      userId: participantId,
    );
    try {
      await zoomService.approveWaitingRoomUser(participantId);
    } catch (_) {}
  }

  Future<void> rejectWaitingRoomRequest(String participantId) async {
    if (!_requireHostAction('reject waiting-room requests')) {
      return;
    }
    await classJoinService.rejectJoinRequest(
      classId: value.session.id,
      userId: participantId,
    );
    try {
      await zoomService.rejectWaitingRoomUser(participantId);
    } catch (_) {}
  }

  Future<void> approveAllWaitingRoomRequests() async {
    if (!_requireHostAction('approve all waiting-room requests')) {
      return;
    }
    await classJoinService.approveAll(classId: value.session.id);
  }

  Future<void> setMeetingLocked(bool locked) async {
    if (!_requireHostAction(
      locked ? 'lock the meeting' : 'unlock the meeting',
    )) {
      return;
    }
    await classJoinService.updateMeetingLock(
      classId: value.session.id,
      locked: locked,
    );
    await zoomService.lockMeeting(locked);
    value = value.copyWith(isMeetingLocked: locked);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.meetingLockChanged,
          classId: value.session.id,
          senderId: currentUserId,
          enabled: locked,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> joinBreakoutRoom(String roomId) async {
    final context = _breakoutRoomManager.joinBreakoutRoom(roomId);
    await zoomService.joinBreakoutRoom(roomId);
    value = value.copyWith(
      activeBreakoutRoomId: context.roomId,
      broadcastMessage:
          'Switched to breakout room ${context.roomId} (${context.chatChannel})',
    );
  }

  Future<void> leaveBreakoutRoom() async {
    _breakoutRoomManager.leaveBreakoutRoom();
    await zoomService.leaveBreakoutRoom();
    value = value.copyWith(clearActiveBreakoutRoom: true);
  }

  void addWhiteboardStroke(WhiteboardStroke stroke) {
    value = value.copyWith(
      whiteboardStrokes: [...value.whiteboardStrokes, stroke],
    );
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.whiteboardStroke,
          classId: value.session.id,
          senderId: currentUserId,
          timestamp: DateTime.now(),
          metadata: _encodeWhiteboardStroke(stroke),
        ),
      ),
    );
  }

  void clearWhiteboard() {
    value = value.copyWith(whiteboardStrokes: const []);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.whiteboardClear,
          classId: value.session.id,
          senderId: currentUserId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  void setWhiteboardEraser(bool enabled) {
    value = value.copyWith(isWhiteboardEraser: enabled);
  }

  void sendChatMessage(String text) {
    final trimmed = text.trim();
    if (trimmed.isEmpty) {
      return;
    }
    if (value.focusModeEnabled && !canManageClass) {
      value = value.copyWith(
        error: 'Focus mode is active. Chat is temporarily disabled.',
      );
      return;
    }
    if (!value.chatEnabled) {
      value = value.copyWith(error: 'Chat is disabled by the host.');
      return;
    }

    final message = ChatMessage(
      id: _nextId('chat'),
      sender: _currentUserName,
      message: _normalizeLatex(trimmed),
      timestamp: DateTime.now(),
      isTeacher: canManageClass,
      isLatex: _isLatex(trimmed),
    );

    if (trimmed.contains('?')) {
      analyticsService.onDoubtAsked();
    }

    value = value.copyWith(
      chatMessages: [...value.chatMessages, message],
      clearError: true,
    );

    _scheduleIntelligenceRefresh();
  }

  void sendChatAttachment(ChatAttachment attachment) {
    if (!value.chatEnabled) {
      value = value.copyWith(error: 'Chat is disabled by the host.');
      return;
    }

    final message = ChatMessage(
      id: _nextId('chat_file'),
      sender: _currentUserName,
      message: attachment.type == ChatAttachmentType.image
          ? 'Shared an image attachment.'
          : 'Shared a file attachment.',
      timestamp: DateTime.now(),
      isTeacher: canManageClass,
      attachment: attachment,
    );

    value = value.copyWith(chatMessages: [...value.chatMessages, message]);
  }

  Future<String> askDoubtWithAi(String doubtText) async {
    final question = doubtText.trim();
    if (question.isEmpty) {
      return '';
    }

    analyticsService.onDoubtAsked();

    final studentMessage = ChatMessage(
      id: _nextId('doubt_user'),
      sender: _currentUserName,
      message: '❓ $question',
      timestamp: DateTime.now(),
      isTeacher: canManageClass,
    );

    value = value.copyWith(
      chatMessages: [...value.chatMessages, studentMessage],
    );

    final prompt =
        'A student asked this live-class doubt:\n"$question"\n\n'
        'Give a concise JEE-focused explanation with steps, common mistake, and one quick check.';

    try {
      final answer = await lalacoreApi.askLalacore(
        prompt: prompt,
        context: _buildAiContext(),
      );

      final aiChatMessage = ChatMessage(
        id: _nextId('doubt_ai'),
        sender: 'LalaCore',
        message: answer,
        timestamp: DateTime.now(),
        isTeacher: true,
      );
      final aiPanelMessage = AiMessage(
        id: _nextId('doubt_ai_panel'),
        message: answer,
        timestamp: DateTime.now(),
        fromUser: false,
      );

      value = value.copyWith(
        chatMessages: [...value.chatMessages, aiChatMessage],
        aiMessages: [...value.aiMessages, aiPanelMessage],
      );
      _scheduleIntelligenceRefresh();
      return answer;
    } catch (error) {
      value = value.copyWith(error: 'AI doubt resolution failed: $error');
      rethrow;
    }
  }

  Future<void> queueUnresolvedDoubt({
    required String question,
    required String aiAttemptAnswer,
  }) async {
    final cleaned = question.trim();
    if (cleaned.isEmpty) {
      return;
    }

    final duplicate = value.doubtQueue.any(
      (item) =>
          item.question.toLowerCase() == cleaned.toLowerCase() &&
          (item.isQueued || item.isSelected),
    );
    if (duplicate) {
      return;
    }

    final doubt = DoubtQueueModel(
      id: _nextId('doubt_queue'),
      studentId: currentUserId,
      studentName: _currentUserName,
      question: cleaned,
      aiAttemptAnswer: aiAttemptAnswer,
      createdAt: DateTime.now(),
      status: DoubtQueueStatus.queued,
    );

    value = value.copyWith(
      doubtQueue: [...value.doubtQueue, doubt],
      broadcastMessage:
          'New doubt queued by ${doubt.studentName}: ${doubt.question}',
    );
  }

  void selectDoubtForLiveAnswer(String doubtId) {
    final updated = value.doubtQueue
        .map((item) {
          if (item.id == doubtId) {
            return item.copyWith(status: DoubtQueueStatus.selected);
          }
          if (item.status == DoubtQueueStatus.selected) {
            return item.copyWith(status: DoubtQueueStatus.queued);
          }
          return item;
        })
        .toList(growable: false);

    value = value.copyWith(
      doubtQueue: updated,
      activeDoubtId: doubtId,
      panel: ClassroomPanel.doubtQueue,
      broadcastMessage: 'Teacher selected a doubt for live explanation.',
    );
  }

  void clearActiveDoubt() {
    final activeId = value.activeDoubtId;
    if (activeId == null) {
      return;
    }
    final updated = value.doubtQueue
        .map(
          (item) =>
              item.id == activeId && item.status == DoubtQueueStatus.selected
              ? item.copyWith(status: DoubtQueueStatus.queued)
              : item,
        )
        .toList(growable: false);
    value = value.copyWith(doubtQueue: updated, clearActiveDoubtId: true);
  }

  void resolveActiveDoubt(String teacherAnswer) {
    final activeId = value.activeDoubtId;
    if (activeId == null) {
      return;
    }
    final resolution = teacherAnswer.trim();
    if (resolution.isEmpty) {
      return;
    }
    final now = DateTime.now();
    final updated = value.doubtQueue
        .map(
          (item) => item.id == activeId
              ? item.copyWith(
                  status: DoubtQueueStatus.resolved,
                  teacherResolution: resolution,
                  resolvedAt: now,
                )
              : item,
        )
        .toList(growable: false);

    final chatMessage = ChatMessage(
      id: _nextId('doubt_resolved'),
      sender: canManageClass ? _currentUserName : 'Teacher',
      message: 'Doubt Resolved: $resolution',
      timestamp: now,
      isTeacher: true,
    );

    value = value.copyWith(
      doubtQueue: updated,
      chatMessages: [...value.chatMessages, chatMessage],
      clearActiveDoubtId: true,
      broadcastMessage: 'Teacher resolved a queued doubt.',
    );
  }

  Future<void> generateLectureNotes() async {
    if (value.isGeneratingLectureNotes) {
      return;
    }

    value = value.copyWith(
      isGeneratingLectureNotes: true,
      clearError: true,
      broadcastMessage: 'Generating AI lecture notes from transcript + OCR...',
    );

    try {
      final context = _buildAiContext();
      final notes = await lalacoreApi.generateNotes(context: context);
      final analysis = await lalacoreApi.generateClassAnalysis(
        context: context,
        webVerification: true,
      );
      final structured = _composeLectureNotes(notes: notes, analysis: analysis);

      value = value.copyWith(
        isGeneratingLectureNotes: false,
        lectureNotes: structured,
        recordingNotes: structured.toPlainText(),
        broadcastMessage: 'Lecture notes generated. Ready to download PDF.',
      );

      await intelligenceStorage.storeLectureIntelligence(
        sessionId: value.session.id,
        intelligence: value.intelligence,
        notes: structured.toPlainText(),
      );
      await _syncLectureNotesToStudy(structured);

      unawaited(
        notificationService.notifyNotesAvailable(classId: value.session.id),
      );
    } catch (error) {
      value = value.copyWith(
        isGeneratingLectureNotes: false,
        error: 'Lecture notes generation failed: $error',
      );
    }
  }

  Future<void> downloadLectureNotesPdf() async {
    LectureNotesModel? notes = value.lectureNotes;
    if (notes == null) {
      await generateLectureNotes();
      notes = value.lectureNotes;
      if (notes == null) {
        return;
      }
    }

    try {
      await lectureNotesPdfService.sharePdf(notes);
    } catch (error) {
      value = value.copyWith(error: 'PDF export failed: $error');
    }
  }

  Future<void> askLalacore(String prompt) async {
    final question = prompt.trim();
    if (question.isEmpty) {
      return;
    }

    value = value.copyWith(
      aiMessages: [
        ...value.aiMessages,
        AiMessage(
          id: _nextId('ai_user'),
          message: question,
          timestamp: DateTime.now(),
          fromUser: true,
        ),
      ],
    );

    try {
      final answer = await lalacoreApi.askLalacore(
        prompt: question,
        context: _buildAiContext(),
      );

      value = value.copyWith(
        aiMessages: [
          ...value.aiMessages,
          AiMessage(
            id: _nextId('ai_model'),
            message: answer,
            timestamp: DateTime.now(),
            fromUser: false,
          ),
        ],
      );
    } catch (error) {
      value = value.copyWith(error: 'LalaCore request failed: $error');
    }
  }

  Future<void> searchLecture(String query) async {
    final cleaned = query.trim();
    if (cleaned.isEmpty) {
      value = value.copyWith(searchQuery: '', searchResults: const []);
      return;
    }

    final results = await intelligenceService.search(
      query: cleaned,
      intelligence: value.intelligence,
    );

    value = value.copyWith(searchQuery: cleaned, searchResults: results);
  }

  Future<void> startRecording() async {
    if (!_requireHostAction('start recording')) {
      return;
    }
    if (value.isRecording) {
      return;
    }

    await classJoinService.updateRecordingEnabled(
      classId: value.session.id,
      enabled: true,
    );
    await zoomService.startRecording();
    await recordingService.startRecording(value.session.id);

    value = value.copyWith(
      isRecording: true,
      session: value.session.copyWith(isRecording: true),
      clearRecordingNotes: true,
      clearRecordingJobId: true,
      clearRecordingJobStatus: true,
    );
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.recordingChanged,
          classId: value.session.id,
          senderId: currentUserId,
          enabled: true,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> stopRecordingAndProcess() async {
    if (!_requireHostAction('stop recording')) {
      return;
    }
    if (!value.isRecording) {
      return;
    }

    try {
      value = value.copyWith(
        isProcessingRecording: true,
        recordingJobStatus: 'stopping_recording',
      );
      await classJoinService.updateRecordingEnabled(
        classId: value.session.id,
        enabled: false,
      );
      await zoomService.stopRecording();
      final localPath = await recordingService.stopRecording(value.session.id);
      final queued = await recordingService.queueProcessingJob(
        sessionId: value.session.id,
        rawRecordingPath: localPath,
      );
      value = value.copyWith(
        recordingJobId: queued?.jobId,
        recordingJobStatus: queued?.status ?? 'queued_local',
      );
      unawaited(
        _publishSyncEvent(
          ClassroomSyncEvent(
            type: ClassroomSyncEventType.recordingChanged,
            classId: value.session.id,
            senderId: currentUserId,
            enabled: false,
            timestamp: DateTime.now(),
          ),
        ),
      );
      unawaited(_runRecordingPipelineBackground(localPath));
    } catch (error) {
      value = value.copyWith(
        isProcessingRecording: false,
        error: 'Recording pipeline failed: $error',
      );
    }
  }

  Future<void> _runRecordingPipelineBackground(String localPath) async {
    try {
      RecordingArtifact? artifact;
      final jobId = value.recordingJobId;
      if (jobId != null && jobId.isNotEmpty) {
        var status = await recordingService.fetchProcessingStatus(jobId);
        value = value.copyWith(recordingJobStatus: status);

        var attempts = 0;
        while (_isRecordingJobPending(status) && attempts < 120) {
          await Future<void>.delayed(const Duration(seconds: 2));
          status = await recordingService.fetchProcessingStatus(jobId);
          attempts += 1;
          value = value.copyWith(recordingJobStatus: status);
        }

        final lowered = status.toLowerCase();
        if (lowered == 'completed' ||
            lowered == 'done' ||
            lowered == 'success' ||
            lowered == 'finished') {
          artifact = await recordingService.fetchProcessedArtifact(
            sessionId: value.session.id,
            jobId: jobId,
          );
        } else if (lowered.contains('fail') || lowered.contains('error')) {
          throw StateError('Background recording worker failed: $status');
        }
      }

      artifact ??= await recordingService.processRecording(
        sessionId: value.session.id,
        rawRecordingPath: localPath,
        transcript: _transcriptHistory,
      );

      final notes = await lalacoreApi.generateNotes(context: _buildAiContext());
      final analysis = await lalacoreApi.generateClassAnalysis(
        context: _buildAiContext(),
        webVerification: true,
      );
      final timeline = await lalacoreApi.generateLectureIndex(
        context: _buildAiContext(),
      );
      final homework = await lalacoreApi.generateHomework(
        context: _buildAiContext(),
      );
      final flashcards = await lalacoreApi.generateFlashcards(
        context: _buildAiContext(),
      );
      final structuredNotes = _composeLectureNotes(
        notes: notes,
        analysis: analysis,
      );

      value = value.copyWith(
        isRecording: false,
        isProcessingRecording: false,
        session: value.session.copyWith(isRecording: false),
        recordingJobStatus: 'completed',
        recordingNotes:
            'Recording URL: ${artifact.recordingUrl}\n\n'
            'Key concepts:\n- ${notes.keyConcepts.join('\n- ')}\n\n'
            'Formulas:\n- ${notes.formulas.join('\n- ')}\n\n'
            'Shortcuts:\n- ${notes.shortcuts.join('\n- ')}\n\n'
            'Common mistakes:\n- ${notes.commonMistakes.join('\n- ')}',
        lectureIndex: timeline.isEmpty ? artifact.lectureIndex : timeline,
        lectureNotes: structuredNotes,
        homework: {
          'easy': homework.easy,
          'medium': homework.medium,
          'hard': homework.hard,
        },
        intelligence: value.intelligence.copyWith(
          flashcards: flashcards
              .map(
                (item) => FlashcardModel(
                  front: item['front'] ?? '',
                  back: item['back'] ?? '',
                ),
              )
              .where((item) => item.front.isNotEmpty && item.back.isNotEmpty)
              .toList(growable: false),
          adaptivePractice: {
            'level_1': homework.easy,
            'level_2': homework.medium,
            'level_3': homework.hard,
          },
        ),
      );

      await generateTeacherIntelligenceReport();
      await intelligenceStorage.storeLectureIntelligence(
        sessionId: value.session.id,
        intelligence: value.intelligence,
        notes: value.recordingNotes ?? '',
      );
      await _syncLectureNotesToStudy(structuredNotes);
      await _syncFlashcardsToStudy(value.intelligence.flashcards);
      await _syncAdaptivePracticeToStudy(value.homework);

      unawaited(
        notificationService.notifyNotesAvailable(classId: value.session.id),
      );
      unawaited(
        notificationService.notifyHomeworkGenerated(classId: value.session.id),
      );

      _scheduleIntelligenceRefresh();
    } catch (error) {
      value = value.copyWith(
        isProcessingRecording: false,
        recordingJobStatus: 'failed',
        error: 'Recording pipeline failed: $error',
      );
    }
  }

  Future<ReplayModel?> loadReplay() {
    return recordingService.fetchReplay(value.session.id);
  }

  void launchQuiz() {
    if (!_requireHostAction('launch a class quiz')) {
      return;
    }
    unawaited(_launchQuiz());
  }

  Future<LivePollDraft> generateLivePollWithAi({
    required String topic,
    required String difficulty,
  }) {
    return lalacoreApi.generateLivePollDraft(
      topic: topic,
      difficulty: difficulty,
    );
  }

  Future<void> startSilentConceptCheck({
    String question = 'Do you understand this concept?',
    int timerSeconds = 20,
  }) async {
    await startLivePoll(
      LivePollDraft(
        question: question,
        options: const ['Yes', 'No'],
        timerSeconds: timerSeconds,
        correctOption: null,
        topic: 'Concept Check',
        difficulty: 'easy',
      ),
      silentMode: true,
    );
  }

  Future<void> suggestNextTeachingStep() async {
    if (!canManageClass) {
      return;
    }
    final prompt =
        'Act as an AI teaching assistant for a live JEE class.\n'
        'Based on current transcript, doubts, quiz and participation, suggest the next best teaching step in 3 bullets.';
    try {
      final suggestion = await lalacoreApi.askLalacore(
        prompt: prompt,
        context: _buildAiContext(),
      );
      value = value.copyWith(
        aiTeachingSuggestion: suggestion,
        aiMessages: [
          ...value.aiMessages,
          AiMessage(
            id: _nextId('ai_step'),
            message: suggestion,
            timestamp: DateTime.now(),
            fromUser: false,
          ),
        ],
        broadcastMessage: 'AI suggested next teaching step.',
      );
    } catch (error) {
      value = value.copyWith(
        error: 'Failed to get AI teaching suggestion: $error',
      );
    }
  }

  Future<void> generateFlashcardsFromLecture() async {
    try {
      final cards = await lalacoreApi.generateFlashcards(
        context: _buildAiContext(),
      );
      final mapped = cards
          .map(
            (item) => FlashcardModel(
              front: item['front'] ?? '',
              back: item['back'] ?? '',
            ),
          )
          .where((item) => item.front.isNotEmpty && item.back.isNotEmpty)
          .toList(growable: false);
      if (mapped.isEmpty) {
        return;
      }
      value = value.copyWith(
        intelligence: value.intelligence.copyWith(flashcards: mapped),
        broadcastMessage: 'AI flashcards generated and stored in study vault.',
      );
      await intelligenceStorage.storeLectureIntelligence(
        sessionId: value.session.id,
        intelligence: value.intelligence.copyWith(flashcards: mapped),
        notes: value.recordingNotes ?? '',
      );
      await _syncFlashcardsToStudy(mapped);
    } catch (error) {
      value = value.copyWith(error: 'Flashcard generation failed: $error');
    }
  }

  Future<void> generateAdaptivePracticeSet() async {
    try {
      final homework = await lalacoreApi.generateHomework(
        context: _buildAiContext(),
      );
      final adaptive = {
        'level_1': homework.easy,
        'level_2': homework.medium,
        'level_3': homework.hard,
      };
      value = value.copyWith(
        homework: {
          'easy': homework.easy,
          'medium': homework.medium,
          'hard': homework.hard,
        },
        intelligence: value.intelligence.copyWith(adaptivePractice: adaptive),
        broadcastMessage: 'Adaptive practice set generated from lecture.',
      );
      await _syncAdaptivePracticeToStudy(value.homework);
    } catch (error) {
      value = value.copyWith(
        error: 'Adaptive practice generation failed: $error',
      );
    }
  }

  Future<void> generateTeacherIntelligenceReport() async {
    if (!_requireHostAction('generate the teacher class report')) {
      return;
    }
    try {
      final analysis = await lalacoreApi.generateClassAnalysis(
        context: _buildAiContext(),
        webVerification: true,
      );
      final doubts = analysis['doubt_clusters'] is List
          ? (analysis['doubt_clusters'] as List)
                .map((item) => item.toString())
                .join(', ')
          : 'N/A';
      final pendingReviews = value.extractedPracticeQuestions
          .where(
            (item) =>
                item.reviewStatus == ExtractedQuestionReviewStatus.pending,
          )
          .length;
      final approvedReviews = value.extractedPracticeQuestions
          .where(
            (item) =>
                item.reviewStatus == ExtractedQuestionReviewStatus.approved ||
                item.reviewStatus == ExtractedQuestionReviewStatus.edited,
          )
          .length;
      final rejectedReviews = value.extractedPracticeQuestions
          .where(
            (item) =>
                item.reviewStatus == ExtractedQuestionReviewStatus.rejected,
          )
          .length;
      final report =
          'Class Intelligence Report\n\n'
          'Confusion Areas: $doubts\n'
          'Participation: ${(value.analytics.participationRate * 100).toStringAsFixed(0)}%\n'
          'Quiz Accuracy: ${_quizAccuracyPercent()}\n'
          'Practice Extraction QC: pending=$pendingReviews, approved=$approvedReviews, rejected=$rejectedReviews\n'
          'Recording Worker: ${value.recordingJobStatus ?? 'not_started'}\n'
          'Recommended Next Class Step: Reinforce weak concepts with one more guided example.';

      value = value.copyWith(teacherSummaryReport: report);
    } catch (error) {
      value = value.copyWith(error: 'Teacher report generation failed: $error');
    }
  }

  Future<List<LivePollDraft>> loadImportedLivePolls() async {
    try {
      final imported = await quizService.fetchImportablePolls(
        classId: value.session.id,
        limit: 10,
      );
      if (imported.isNotEmpty) {
        return imported;
      }
    } catch (_) {
      // Fallback below keeps teacher flow available when API is unavailable.
    }

    return _fallbackLivePollLibrary();
  }

  Future<void> startLivePoll(
    LivePollDraft draft, {
    bool silentMode = false,
  }) async {
    if (!canManageClass) {
      value = value.copyWith(error: 'Only host/co-host can launch live polls.');
      return;
    }
    if (draft.options.length < 2) {
      value = value.copyWith(error: 'Live poll needs at least 2 options.');
      return;
    }

    _livePollTimer?.cancel();
    _livePollSubmitters.clear();

    LivePollModel poll;
    try {
      poll = await quizService.createLivePoll(
        classId: value.session.id,
        draft: draft,
      );
    } catch (_) {
      poll = LivePollModel(
        pollId: 'poll_${DateTime.now().millisecondsSinceEpoch}',
        question: draft.question,
        options: draft.options,
        correctOption: draft.correctOption,
        timerSeconds: draft.timerSeconds,
        startTime: DateTime.now(),
        status: LivePollStatus.active,
      );
    }

    final baseCounts = {
      for (var index = 0; index < poll.options.length; index += 1) index: 0,
    };

    value = value.copyWith(
      currentPoll: poll.copyWith(status: LivePollStatus.active),
      pollActive: true,
      pollTimer: poll.timerSeconds,
      pollResults: baseCounts,
      clearSubmittedPollOption: true,
      pollResultsRevealed: false,
      silentConceptCheckMode: silentMode,
      clearError: true,
      broadcastMessage:
          'Live poll started: "${poll.question}" (${poll.timerSeconds}s)',
    );

    _startLivePollCountdown();
    Future<void>.delayed(const Duration(seconds: 4), () {
      if (!hasListeners) {
        return;
      }
      value = value.copyWith(clearBroadcast: true);
    });
  }

  void submitStudentAnswer(int selectedIndex) {
    unawaited(_submitStudentAnswer(selectedIndex));
  }

  Future<void> _submitStudentAnswer(int selectedIndex) async {
    final poll = value.currentPoll;
    if (poll == null || !value.pollActive) {
      return;
    }
    if (value.pollTimer <= 0) {
      return;
    }
    if (value.submittedPollOption != null ||
        _livePollSubmitters.contains(currentUserId)) {
      return;
    }
    if (selectedIndex < 0 || selectedIndex >= poll.options.length) {
      return;
    }

    final counts = Map<int, int>.from(value.pollResults);
    counts[selectedIndex] = (counts[selectedIndex] ?? 0) + 1;
    _livePollSubmitters.add(currentUserId);

    value = value.copyWith(
      submittedPollOption: selectedIndex,
      pollResults: counts,
    );

    try {
      await quizService.submitLivePollAnswer(
        pollId: poll.pollId,
        participantId: currentUserId,
        selectedIndex: selectedIndex,
      );
      await _refreshLivePollResults();
    } catch (_) {
      // Keep local submission accepted even when backend submit fails.
    }
  }

  Future<void> endLivePoll({bool revealResults = true}) async {
    final poll = value.currentPoll;
    if (poll == null) {
      return;
    }

    _livePollTimer?.cancel();

    try {
      await quizService.endLivePoll(poll.pollId);
    } catch (_) {
      // Poll should still end locally even if backend call fails.
    }

    await _refreshLivePollResults();

    value = value.copyWith(
      currentPoll: poll.copyWith(status: LivePollStatus.ended),
      pollActive: false,
      pollTimer: 0,
      pollResultsRevealed: revealResults,
    );

    if (revealResults) {
      await broadcastPollResults();
    }
  }

  Future<void> revealLivePollResults() async {
    final poll = value.currentPoll;
    if (poll == null) {
      return;
    }
    await _refreshLivePollResults();
    value = value.copyWith(
      currentPoll: poll.copyWith(status: LivePollStatus.ended),
      pollActive: false,
      pollResultsRevealed: true,
    );
    await broadcastPollResults();
  }

  void clearLivePoll() {
    _livePollTimer?.cancel();
    _livePollSubmitters.clear();
    value = value.copyWith(
      clearCurrentPoll: true,
      pollActive: false,
      pollTimer: 0,
      pollResults: const {},
      clearSubmittedPollOption: true,
      pollResultsRevealed: false,
      silentConceptCheckMode: false,
    );
  }

  Future<void> broadcastPollResults() async {
    await _refreshLivePollResults();
    final poll = value.currentPoll;
    if (poll == null) {
      return;
    }

    final counts = value.pollResults;
    final total = counts.values.fold<int>(0, (sum, item) => sum + item);
    final top = counts.entries.isEmpty
        ? const MapEntry<int, int>(0, 0)
        : counts.entries.reduce((a, b) => a.value >= b.value ? a : b);
    final topLabel = top.key < poll.options.length
        ? poll.options[top.key]
        : 'N/A';
    final topPercent = total == 0 ? 0 : ((top.value / total) * 100).round();
    final confusionPercent =
        value.silentConceptCheckMode && poll.options.length >= 2
        ? (total == 0 ? 0 : (((counts[1] ?? 0) / total) * 100).round())
        : null;

    value = value.copyWith(
      broadcastMessage: value.silentConceptCheckMode
          ? 'Concept check: $confusionPercent% students need reinforcement.'
          : 'Poll results: $total response(s). Top option: $topLabel ($topPercent%).',
    );
    Future<void>.delayed(const Duration(seconds: 5), () {
      if (!hasListeners) {
        return;
      }
      value = value.copyWith(clearBroadcast: true);
    });
  }

  Future<void> _launchQuiz() async {
    try {
      final concepts = value.intelligence.concepts
          .map((item) => item.concept)
          .toSet()
          .toList(growable: false);

      final quizSession = await quizService.createQuiz(
        classId: value.session.id,
        concepts: concepts,
      );
      await quizService.startQuiz(
        classId: value.session.id,
        quizId: quizSession.quizId,
      );

      value = value.copyWith(
        quiz: QuizState(
          quizId: quizSession.quizId,
          isActive: true,
          question: quizSession.question.question,
          options: quizSession.question.options,
          correctIndex: quizSession.correctIndex,
          selectedIndex: null,
          totalResponses: 0,
          correctResponses: 0,
        ),
      );

      unawaited(
        notificationService.notifyQuizStarting(classId: value.session.id),
      );
    } catch (_) {
      value = value.copyWith(quiz: _fallbackQuizState());
    }
  }

  void launchMiniQuizSuggestion() {
    unawaited(_launchMiniQuiz());
  }

  Future<void> _launchMiniQuiz() async {
    try {
      final generated = await lalacoreApi.generateMiniQuiz(
        context: _buildAiContext(),
      );
      value = value.copyWith(
        quiz: QuizState(
          quizId: null,
          isActive: true,
          question: generated['question']?.toString() ?? 'Quick check',
          options: (generated['options'] is List)
              ? (generated['options'] as List)
                    .map((item) => item.toString())
                    .toList(growable: false)
              : const ['A', 'B', 'C', 'D'],
          correctIndex: (generated['correct_index'] as num?)?.toInt() ?? 0,
          selectedIndex: null,
          totalResponses: 0,
          correctResponses: 0,
        ),
      );
    } catch (_) {
      final suggestion = value.intelligence.miniQuizSuggestion;
      if (suggestion == null) {
        return;
      }

      value = value.copyWith(
        quiz: QuizState(
          quizId: null,
          isActive: true,
          question: '$suggestion: Which statement is correct?',
          options: const [
            'Flux is always zero in a closed surface.',
            'Gauss law connects enclosed charge and electric flux.',
            'Electric field is scalar.',
            'Potential is vector.',
          ],
          correctIndex: 1,
          selectedIndex: null,
          totalResponses: 0,
          correctResponses: 0,
        ),
        intelligence: value.intelligence.copyWith(clearMiniQuiz: true),
      );
    }
  }

  void submitQuizAnswer(int index) {
    unawaited(_submitQuizAnswer(index));
  }

  Future<void> _submitQuizAnswer(int index) async {
    final quiz = value.quiz;
    if (!quiz.isActive || quiz.selectedIndex != null) {
      return;
    }

    final isCorrect = index == quiz.correctIndex;
    analyticsService.onQuizSubmitted();

    value = value.copyWith(
      quiz: quiz.copyWith(
        selectedIndex: index,
        totalResponses: quiz.totalResponses + 1,
        correctResponses: quiz.correctResponses + (isCorrect ? 1 : 0),
      ),
    );

    if (quiz.quizId != null) {
      try {
        await quizService.submitAnswer(
          quizId: quiz.quizId!,
          participantId: currentUserId,
          selectedIndex: index,
        );
        final result = await quizService.fetchResults(quiz.quizId!);
        value = value.copyWith(
          quiz: value.quiz.copyWith(
            totalResponses: result.totalResponses,
            correctResponses: result.correctResponses,
          ),
        );
      } catch (_) {
        // Backend outages should not block local quiz UX.
      }
    }

    _scheduleIntelligenceRefresh();
  }

  void closeQuiz() {
    value = value.copyWith(quiz: QuizState.idle);
  }

  Future<void> muteParticipant(String participantId) async {
    if (!_requireHostAction('mute participants')) {
      return;
    }
    await classJoinService.updateParticipantMuted(
      classId: value.session.id,
      userId: participantId,
      muted: true,
    );
    await zoomService.muteParticipant(participantId);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.participantMuted,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: participantId,
          enabled: true,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> removeParticipant(String participantId) async {
    if (!_requireHostAction('remove participants')) {
      return;
    }
    await classJoinService.removeParticipant(
      classId: value.session.id,
      userId: participantId,
    );
    await zoomService.removeParticipant(participantId);
    _removeParticipantFromState(participantId);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.participantRemoved,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: participantId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> disableParticipantCamera(String participantId) async {
    if (!_requireHostAction('disable participant cameras')) {
      return;
    }
    await classJoinService.updateParticipantCameraDisabled(
      classId: value.session.id,
      userId: participantId,
      disabled: true,
    );
    await zoomService.disableParticipantCamera(participantId);
    _applyParticipantCameraState(participantId, false);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.participantCameraDisabled,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: participantId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> promoteToCoHost(String participantId) async {
    if (!_requireHostAction('promote participants to co-host')) {
      return;
    }
    await zoomService.promoteToCoHost(participantId);
    _applyParticipantRoleState(participantId, ParticipantRole.coHost);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.participantPromoted,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: participantId,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> muteAllParticipants() async {
    if (!_requireHostAction('mute all participants')) {
      return;
    }
    await zoomService.muteAll();
    _muteAllParticipantsLocally(includeHosts: false);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.muteAllParticipants,
          classId: value.session.id,
          senderId: currentUserId,
          enabled: true,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> setChatEnabled(bool enabled) async {
    if (!_requireHostAction(
      enabled ? 'enable classroom chat' : 'disable classroom chat',
    )) {
      return;
    }
    await classJoinService.updateChatEnabled(
      classId: value.session.id,
      enabled: enabled,
    );
    await zoomService.setChatEnabled(enabled);
    value = value.copyWith(chatEnabled: enabled);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.chatEnabledChanged,
          classId: value.session.id,
          senderId: currentUserId,
          enabled: enabled,
          timestamp: DateTime.now(),
        ),
      ),
    );
  }

  Future<void> setWaitingRoomEnabled(bool enabled) async {
    if (!_requireHostAction(
      enabled
          ? 'enable waiting-room approval'
          : 'disable waiting-room approval',
    )) {
      return;
    }
    await classJoinService.updateWaitingRoomEnabled(
      classId: value.session.id,
      enabled: enabled,
    );
    value = value.copyWith(waitingRoomEnabled: enabled);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.waitingRoomChanged,
          classId: value.session.id,
          senderId: currentUserId,
          enabled: enabled,
          timestamp: DateTime.now(),
        ),
      ),
    );
    if (!enabled && canManageClass) {
      unawaited(approveAllWaitingRoomRequests());
    }
  }

  void createBreakoutRoom(String name) {
    if (!_requireHostAction('create breakout rooms')) {
      return;
    }
    final room = _breakoutRoomManager.createRoom(name);
    value = value.copyWith(breakoutRooms: [...value.breakoutRooms, room]);
  }

  Future<void> assignParticipantToRoom({
    required String participantId,
    required String roomId,
  }) async {
    if (!_requireHostAction('assign breakout rooms')) {
      return;
    }
    await classJoinService.updateBreakoutAssignment(
      classId: value.session.id,
      userId: participantId,
      roomId: roomId,
    );
    _breakoutRoomManager.assignParticipant(
      participantId: participantId,
      roomId: roomId,
    );
    value = value.copyWith(breakoutRooms: _breakoutRoomManager.rooms);
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.breakoutAssignmentChanged,
          classId: value.session.id,
          senderId: currentUserId,
          targetUserId: participantId,
          timestamp: DateTime.now(),
          metadata: {'room_id': roomId},
        ),
      ),
    );
  }

  void removeBreakoutRoom(String roomId) {
    if (!_requireHostAction('remove breakout rooms')) {
      return;
    }
    _breakoutRoomManager.removeRoom(roomId);
    value = value.copyWith(breakoutRooms: _breakoutRoomManager.rooms);
  }

  Future<void> broadcastToBreakoutRooms(String message) async {
    if (!_requireHostAction('broadcast to breakout rooms')) {
      return;
    }
    await classJoinService.broadcastBreakoutMessage(
      classId: value.session.id,
      message: message,
    );
    unawaited(zoomService.broadcastMessageToRooms(message));
    unawaited(
      _publishSyncEvent(
        ClassroomSyncEvent(
          type: ClassroomSyncEventType.breakoutBroadcast,
          classId: value.session.id,
          senderId: currentUserId,
          timestamp: DateTime.now(),
          metadata: {'message': message},
        ),
      ),
    );

    final built = _breakoutRoomManager.broadcastMessageToRooms(
      message: message,
      participants: value.participants,
    );

    value = value.copyWith(broadcastMessage: built);
    Future<void>.delayed(const Duration(seconds: 4), () {
      if (!hasListeners) {
        return;
      }
      value = value.copyWith(clearBroadcast: true);
    });
  }

  void clearError() {
    value = value.copyWith(clearError: true, clearFailoverMessage: true);
  }

  Future<void> retryFailover() {
    return _attemptFailoverLiveMode();
  }

  void _bindStreams() {
    if (_classroomStreamsBound) {
      return;
    }
    _classroomStreamsBound = true;
    _bindJoinStreams();

    _subscriptions.add(
      zoomService.participantsStream.listen((participants) {
        analyticsService.onAttendanceChanged(participants.length);

        final me = _findParticipant(participants, currentUserId);
        final raisedHands = participants
            .where(
              (participant) => participant.handRaised && !participant.isTeacher,
            )
            .toList(growable: false);
        final activeWhiteboardValid = value.activeWhiteboardUserId == null
            ? null
            : _findParticipant(participants, value.activeWhiteboardUserId!)?.id;
        final clearWhiteboardOwner =
            value.activeWhiteboardUserId != null &&
            activeWhiteboardValid == null;

        value = value.copyWith(
          participants: participants,
          raisedHands: raisedHands,
          activeWhiteboardUserId: activeWhiteboardValid,
          clearActiveWhiteboardUserId: clearWhiteboardOwner,
          currentUserMicEnabled: me?.micEnabled ?? value.currentUserMicEnabled,
          currentUserCameraEnabled:
              me?.cameraEnabled ?? value.currentUserCameraEnabled,
          currentUserHandRaised: me?.handRaised ?? value.currentUserHandRaised,
        );
      }),
    );

    _subscriptions.add(
      zoomService.activeSpeakerStream.listen((activeSpeakerId) {
        value = value.copyWith(activeSpeakerId: activeSpeakerId);
      }),
    );

    _subscriptions.add(
      zoomService.networkStatsStream.listen((networkStats) {
        value = value.copyWith(networkStats: networkStats);
      }),
    );

    _subscriptions.add(
      zoomService.reactionsStream.listen((emoji) {
        if (value.focusModeEnabled && !canManageClass) {
          return;
        }
        final reaction = ReactionEvent(
          id: _nextId('reaction'),
          emoji: emoji,
          createdAt: DateTime.now(),
        );
        _pushReaction(reaction);
        analyticsService.onReactionOrHandRaise();
      }),
    );

    _subscriptions.add(
      zoomService.sharedContentStream.listen((source) async {
        value = value.copyWith(sharedContentSource: source);
        if (source == null) {
          await ocrCaptureService.stopCapture();
        } else {
          await ocrCaptureService.startCapture();
        }
      }),
    );

    _subscriptions.add(
      zoomService.waitingRoomRequestsStream.listen((requests) {
        _updateWaitingRequests(requests, fromJoinService: false);
      }),
    );

    _subscriptions.add(
      zoomService.connectionStateStream.listen((connectionState) {
        value = value.copyWith(connectionState: connectionState);
        if (connectionState == RtcConnectionState.reconnecting) {
          value = value.copyWith(error: 'Network unstable. Reconnecting...');
        }
        if (connectionState == RtcConnectionState.connected) {
          _reconnectAttempts = 0;
          _reconnectTimer?.cancel();
          if (value.failoverModeEnabled) {
            unawaited(webRtcFailoverService.stop());
          }
          value = value.copyWith(
            clearError: true,
            failoverModeEnabled: false,
            clearFailoverMessage: true,
          );
        }
        if (connectionState == RtcConnectionState.failed) {
          value = value.copyWith(
            error: 'Connection lost. Please rejoin class.',
          );
          _scheduleReconnect();
        }
      }),
    );

    _subscriptions.add(
      zoomService.meetingLockStream.listen((locked) {
        value = value.copyWith(isMeetingLocked: locked);
      }),
    );

    _subscriptions.add(
      classroomSyncService.events.listen((event) {
        _applyClassroomSyncEvent(event);
      }),
    );

    _subscriptions.add(
      transcriptionService.transcriptStream.listen(
        (chunk) {
          _appendTranscriptChunk(chunk);
          _processLiveConceptChunk(chunk);
          _extractPracticeQuestionFromChunk(chunk);

          if (_transcriptCount % 6 == 0) {
            _refreshLectureIndex();
          }
          _scheduleIntelligenceRefresh();
        },
        onError: (Object error, StackTrace stackTrace) {
          value = value.copyWith(
            broadcastMessage:
                'Live transcription is temporarily unavailable. AI notes and class intelligence will continue using OCR and classroom context.',
          );
        },
      ),
    );

    _subscriptions.add(
      ocrCaptureService.textStream.listen((text) {
        final snippets = [...value.ocrSnippets, text];
        final clipped = snippets.length > 20
            ? snippets.sublist(snippets.length - 20)
            : snippets;
        value = value.copyWith(ocrSnippets: clipped);
        _processOcrConceptSignals(text);
        _scheduleIntelligenceRefresh();
      }),
    );

    _subscriptions.add(
      analyticsService.snapshotStream.listen((snapshot) {
        value = value.copyWith(analytics: snapshot);
      }),
    );
  }

  void _bindJoinStreams() {
    if (_joinStreamsBound) {
      return;
    }
    _joinStreamsBound = true;

    _subscriptions.add(
      classJoinService.waitingRequestsStream.listen((requests) {
        final previousCount = value.waitingRoomRequests.length;
        _updateWaitingRequests(requests, fromJoinService: true);
        if (canManageClass &&
            value.waitingRoomRequests.length > previousCount &&
            requests.isNotEmpty) {
          final latest = requests.last;
          value = value.copyWith(
            broadcastMessage: 'Join Request: ${latest.name} wants to join.',
          );
          Future<void>.delayed(const Duration(seconds: 4), () {
            if (!hasListeners) {
              return;
            }
            value = value.copyWith(clearBroadcast: true);
          });
        }
      }),
    );

    _subscriptions.add(
      classJoinService.joinApprovalStream.listen((event) {
        if (event.userId != currentUserId) {
          return;
        }
        switch (event.status) {
          case JoinApprovalStatus.pending:
            value = value.copyWith(
              joinFlowStatus: JoinFlowStatus.waitingApproval,
              pendingJoinRequestId: event.requestId,
            );
            break;
          case JoinApprovalStatus.approved:
            value = value.copyWith(
              joinFlowStatus: JoinFlowStatus.approved,
              joinStatusMessage: 'Approved. Joining class...',
            );
            break;
          case JoinApprovalStatus.rejected:
            value = value.copyWith(
              joinFlowStatus: JoinFlowStatus.rejected,
              joinStatusMessage:
                  event.message ?? 'Teacher declined your request.',
            );
            break;
          case JoinApprovalStatus.duplicate:
            value = value.copyWith(
              joinFlowStatus: JoinFlowStatus.rejected,
              joinStatusMessage:
                  event.message ?? 'Duplicate session detected for this class.',
            );
            break;
          case JoinApprovalStatus.canceled:
            value = value.copyWith(
              joinFlowStatus: JoinFlowStatus.idle,
              clearPendingJoinRequestId: true,
              clearJoinStatusMessage: true,
            );
            break;
        }
      }),
    );
  }

  void _updateWaitingRequests(
    List<WaitingRoomRequestModel> requests, {
    required bool fromJoinService,
  }) {
    if (fromJoinService) {
      _joinWaitingRequests = requests;
    } else {
      _zoomWaitingRequests = requests;
    }
    final merged = <String, WaitingRoomRequestModel>{};
    for (final item in _zoomWaitingRequests) {
      merged[item.participantId] = item;
    }
    for (final item in _joinWaitingRequests) {
      merged[item.participantId] = item;
    }
    value = value.copyWith(
      waitingRoomRequests: merged.values.toList(growable: false),
    );
  }

  Future<void> _publishSyncEvent(ClassroomSyncEvent event) async {
    try {
      await classroomSyncService.publish(event);
    } catch (_) {
      // Sync channel is best-effort; local classroom state remains primary.
    }
  }

  void _applyClassroomSyncEvent(ClassroomSyncEvent event) {
    if (event.classId != value.session.id || event.senderId == currentUserId) {
      return;
    }

    switch (event.type) {
      case ClassroomSyncEventType.raiseHand:
        _applyParticipantHandState(event.targetUserId ?? event.senderId, true);
        break;
      case ClassroomSyncEventType.lowerHand:
        _applyParticipantHandState(event.targetUserId ?? event.senderId, false);
        break;
      case ClassroomSyncEventType.approveMic:
        final participantId = event.targetUserId ?? event.senderId;
        _applyParticipantMicState(participantId, true);
        _applyParticipantHandState(participantId, false);
        if (participantId == currentUserId) {
          unawaited(
            zoomService.toggleMic(participantId: currentUserId, enabled: true),
          );
        }
        break;
      case ClassroomSyncEventType.participantMuted:
        final participantId = event.targetUserId ?? event.senderId;
        final muted = event.enabled ?? true;
        _applyParticipantMicState(participantId, !muted);
        if (participantId == currentUserId) {
          unawaited(
            zoomService.toggleMic(
              participantId: currentUserId,
              enabled: !muted,
            ),
          );
        }
        break;
      case ClassroomSyncEventType.participantRemoved:
        final participantId = event.targetUserId ?? event.senderId;
        _removeParticipantFromState(participantId);
        if (participantId == currentUserId) {
          value = value.copyWith(
            broadcastMessage:
                'You were removed from the live class by the host.',
          );
          unawaited(leaveClass());
        }
        break;
      case ClassroomSyncEventType.participantCameraDisabled:
        final participantId = event.targetUserId ?? event.senderId;
        _applyParticipantCameraState(participantId, false);
        if (participantId == currentUserId) {
          unawaited(
            zoomService.toggleCamera(
              participantId: currentUserId,
              enabled: false,
            ),
          );
        }
        break;
      case ClassroomSyncEventType.participantPromoted:
        final participantId = event.targetUserId ?? event.senderId;
        _applyParticipantRoleState(participantId, ParticipantRole.coHost);
        break;
      case ClassroomSyncEventType.muteAllParticipants:
        _muteAllParticipantsLocally(includeHosts: false);
        if (!canManageClass) {
          unawaited(
            zoomService.toggleMic(participantId: currentUserId, enabled: false),
          );
        }
        break;
      case ClassroomSyncEventType.whiteboardRequest:
        final userId = event.targetUserId ?? event.senderId;
        if (!value.whiteboardAccessRequests.contains(userId)) {
          value = value.copyWith(
            whiteboardAccessRequests: [
              ...value.whiteboardAccessRequests,
              userId,
            ],
          );
        }
        break;
      case ClassroomSyncEventType.whiteboardGrant:
        final userId = event.targetUserId ?? event.senderId;
        value = value.copyWith(
          activeWhiteboardUserId: userId,
          whiteboardAccessRequests: value.whiteboardAccessRequests
              .where((item) => item != userId)
              .toList(growable: false),
        );
        break;
      case ClassroomSyncEventType.whiteboardDismiss:
        final userId = event.targetUserId ?? event.senderId;
        value = value.copyWith(
          whiteboardAccessRequests: value.whiteboardAccessRequests
              .where((item) => item != userId)
              .toList(growable: false),
        );
        break;
      case ClassroomSyncEventType.whiteboardRevoke:
        value = value.copyWith(clearActiveWhiteboardUserId: true);
        break;
      case ClassroomSyncEventType.whiteboardStroke:
        final stroke = _decodeWhiteboardStroke(event.metadata);
        if (stroke != null) {
          value = value.copyWith(
            whiteboardStrokes: [...value.whiteboardStrokes, stroke],
          );
        }
        break;
      case ClassroomSyncEventType.whiteboardClear:
        value = value.copyWith(whiteboardStrokes: const []);
        break;
      case ClassroomSyncEventType.meetingLockChanged:
        value = value.copyWith(isMeetingLocked: event.enabled ?? false);
        break;
      case ClassroomSyncEventType.chatEnabledChanged:
        value = value.copyWith(chatEnabled: event.enabled ?? true);
        break;
      case ClassroomSyncEventType.waitingRoomChanged:
        value = value.copyWith(waitingRoomEnabled: event.enabled ?? true);
        break;
      case ClassroomSyncEventType.recordingChanged:
        final recording = event.enabled ?? false;
        value = value.copyWith(
          isRecording: recording,
          session: value.session.copyWith(isRecording: recording),
        );
        break;
      case ClassroomSyncEventType.breakoutAssignmentChanged:
        final participantId = event.targetUserId ?? '';
        final assignedRoom = event.metadata['room_id']?.toString();
        if (participantId == currentUserId) {
          value = value.copyWith(
            activeBreakoutRoomId: assignedRoom,
            broadcastMessage: assignedRoom == null || assignedRoom.isEmpty
                ? 'Returned to the main classroom.'
                : 'You were moved to breakout room $assignedRoom.',
          );
        }
        break;
      case ClassroomSyncEventType.breakoutBroadcast:
        final message = event.metadata['message']?.toString() ?? '';
        if (message.isNotEmpty) {
          value = value.copyWith(
            broadcastMessage: 'Breakout broadcast: $message',
          );
          Future<void>.delayed(const Duration(seconds: 4), () {
            if (!hasListeners) {
              return;
            }
            value = value.copyWith(clearBroadcast: true);
          });
        }
        break;
      case ClassroomSyncEventType.laserToggle:
        final enabled = event.enabled ?? false;
        value = value.copyWith(
          laserPointerEnabled: enabled,
          laserPointerPosition: enabled
              ? (value.laserPointerPosition ?? const Offset(0.5, 0.5))
              : null,
          clearLaserPointerPosition: !enabled,
        );
        break;
      case ClassroomSyncEventType.laserMove:
        final x = event.positionX;
        final y = event.positionY;
        if (x == null || y == null || !value.laserPointerEnabled) {
          return;
        }
        value = value.copyWith(
          laserPointerPosition: Offset(x.clamp(0.0, 1.0), y.clamp(0.0, 1.0)),
        );
        break;
    }
  }

  void _applyParticipantHandState(String participantId, bool raised) {
    final participants = value.participants;
    final index = participants.indexWhere((item) => item.id == participantId);
    if (index == -1) {
      return;
    }
    final updated = [...participants];
    updated[index] = updated[index].copyWith(handRaised: raised);
    final raisedHands = updated
        .where(
          (participant) => participant.handRaised && !participant.isTeacher,
        )
        .toList(growable: false);
    value = value.copyWith(
      participants: updated,
      raisedHands: raisedHands,
      currentUserHandRaised: participantId == currentUserId
          ? raised
          : value.currentUserHandRaised,
    );
  }

  void _applyParticipantMicState(String participantId, bool enabled) {
    final participants = value.participants;
    final index = participants.indexWhere((item) => item.id == participantId);
    if (index == -1) {
      return;
    }
    final updated = [...participants];
    updated[index] = updated[index].copyWith(micEnabled: enabled);
    value = value.copyWith(
      participants: updated,
      currentUserMicEnabled: participantId == currentUserId
          ? enabled
          : value.currentUserMicEnabled,
    );
  }

  void _applyParticipantCameraState(String participantId, bool enabled) {
    final participants = value.participants;
    final index = participants.indexWhere((item) => item.id == participantId);
    if (index == -1) {
      return;
    }
    final updated = [...participants];
    updated[index] = updated[index].copyWith(cameraEnabled: enabled);
    value = value.copyWith(
      participants: updated,
      currentUserCameraEnabled: participantId == currentUserId
          ? enabled
          : value.currentUserCameraEnabled,
    );
  }

  void _applyParticipantRoleState(String participantId, ParticipantRole role) {
    final participants = value.participants;
    final index = participants.indexWhere((item) => item.id == participantId);
    if (index == -1) {
      return;
    }
    final updated = [...participants];
    updated[index] = updated[index].copyWith(role: role);
    value = value.copyWith(participants: updated);
  }

  void _removeParticipantFromState(String participantId) {
    final updated = value.participants
        .where((item) => item.id != participantId)
        .toList(growable: false);
    value = value.copyWith(
      participants: updated,
      raisedHands: value.raisedHands
          .where((item) => item.id != participantId)
          .toList(growable: false),
      waitingRoomRequests: value.waitingRoomRequests
          .where((item) => item.participantId != participantId)
          .toList(growable: false),
      activeWhiteboardUserId: value.activeWhiteboardUserId == participantId
          ? null
          : value.activeWhiteboardUserId,
      clearActiveWhiteboardUserId:
          value.activeWhiteboardUserId == participantId,
      currentUserHandRaised: participantId == currentUserId
          ? false
          : value.currentUserHandRaised,
    );
  }

  void _muteAllParticipantsLocally({required bool includeHosts}) {
    final updated = value.participants
        .map(
          (item) => item.isTeacher && !includeHosts
              ? item
              : item.copyWith(micEnabled: false, handRaised: false),
        )
        .toList(growable: false);
    value = value.copyWith(
      participants: updated,
      raisedHands: includeHosts
          ? const []
          : value.raisedHands
                .where((item) => item.isTeacher)
                .toList(growable: false),
      currentUserMicEnabled: includeHosts || !canManageClass
          ? false
          : value.currentUserMicEnabled,
      currentUserHandRaised: includeHosts || !canManageClass
          ? false
          : value.currentUserHandRaised,
    );
  }

  Map<String, dynamic> _encodeWhiteboardStroke(WhiteboardStroke stroke) {
    return {
      'points': stroke.points
          .map((point) => {'x': point.dx, 'y': point.dy})
          .toList(growable: false),
      'color': stroke.color.toARGB32(),
      'width': stroke.width,
    };
  }

  WhiteboardStroke? _decodeWhiteboardStroke(Map<String, dynamic> metadata) {
    final rawPoints = metadata['points'];
    if (rawPoints is! List || rawPoints.length < 2) {
      return null;
    }
    final points = rawPoints
        .whereType<Map>()
        .map(
          (item) => Offset(
            ((item['x'] as num?)?.toDouble() ?? 0).clamp(0.0, 1.0),
            ((item['y'] as num?)?.toDouble() ?? 0).clamp(0.0, 1.0),
          ),
        )
        .toList(growable: false);
    if (points.length < 2) {
      return null;
    }
    final colorValue = (metadata['color'] as num?)?.toInt();
    final width = (metadata['width'] as num?)?.toDouble() ?? 3;
    return WhiteboardStroke(
      points: points,
      color: colorValue == null ? const Color(0xFF0F4973) : Color(colorValue),
      width: width,
    );
  }

  Future<void> _syncLectureNotesToStudy(LectureNotesModel notes) async {
    try {
      await studyMaterialSyncService.upsertTeacherStudyNote(
        context: liveClassContext,
        materialKey: 'ai_notes',
        titleSuffix: 'AI Lecture Notes',
        description:
            'AI lecture notes generated from live transcript, board OCR, and class analysis.',
        body: notes.toPlainText(),
      );
    } catch (_) {
      // Study-tab syncing should not interrupt the live classroom workflow.
    }
  }

  Future<void> _syncFlashcardsToStudy(List<FlashcardModel> cards) async {
    if (cards.isEmpty) {
      return;
    }
    final String body = cards
        .map((item) => 'Q: ${item.front.trim()}\nA: ${item.back.trim()}')
        .join('\n\n');
    try {
      await studyMaterialSyncService.upsertTeacherStudyNote(
        context: liveClassContext,
        materialKey: 'flashcards',
        titleSuffix: 'AI Flashcards',
        description:
            'Lecture flashcards generated from the live class for revision in the Study tab.',
        body: body,
      );
    } catch (_) {}
  }

  Future<void> _syncAdaptivePracticeToStudy(
    Map<String, List<String>> practice,
  ) async {
    final sections = <String>[];
    for (final entry in practice.entries) {
      final items = entry.value
          .map((item) => item.trim())
          .where((item) => item.isNotEmpty)
          .toList(growable: false);
      if (items.isEmpty) {
        continue;
      }
      sections.add(
        '${entry.key.toUpperCase()}\n${items.asMap().entries.map((e) => '${e.key + 1}. ${e.value}').join('\n')}',
      );
    }
    if (sections.isEmpty) {
      return;
    }
    try {
      await studyMaterialSyncService.upsertTeacherStudyNote(
        context: liveClassContext,
        materialKey: 'adaptive_practice',
        titleSuffix: 'Adaptive Practice',
        description:
            'AI-generated adaptive practice from the live lecture, synced for students in Study.',
        body: sections.join('\n\n'),
      );
    } catch (_) {}
  }

  void _scheduleReconnect() {
    if (!value.isConnected) {
      return;
    }
    if (_reconnectAttempts >= 3) {
      value = value.copyWith(
        error: 'Reconnection failed. Switching to failover live mode...',
      );
      unawaited(_attemptFailoverLiveMode());
      return;
    }
    if (_reconnectTimer?.isActive == true) {
      return;
    }

    _reconnectAttempts += 1;
    final delay = Duration(seconds: _reconnectAttempts * 2);
    _reconnectTimer = Timer(delay, () async {
      try {
        await _rehydrateClassroomState();
        await _refreshLiveSessionAccess();
        await zoomService.joinSession(
          sessionId: value.session.id,
          token: authToken,
        );
      } catch (_) {
        _scheduleReconnect();
      }
    });
  }

  Future<void> _attemptFailoverLiveMode() async {
    try {
      final fallback = await classJoinService.fetchWebRtcFallbackToken(
        classId: value.session.id,
        userId: currentUserId,
      );
      if (fallback == null || fallback.isEmpty) {
        value = value.copyWith(
          failoverModeEnabled: true,
          failoverMessage:
              'Failover requested, but no WebRTC fallback token received.',
        );
        return;
      }
      final provider = fallback['provider'] ?? 'webrtc';
      final room = fallback['room'] ?? value.session.id;
      final token = fallback['token'] ?? '';
      final signaling = fallback['url'] ?? fallback['ws'] ?? '';
      await webRtcFailoverService.start(
        roomId: room,
        token: token,
        signalingUrl: signaling,
        userId: currentUserId,
        cameraEnabled: value.currentUserCameraEnabled,
        micEnabled: value.currentUserMicEnabled,
        provider: provider,
      );
      value = value.copyWith(
        failoverModeEnabled: true,
        failoverMessage:
            webRtcFailoverService.snapshotListenable.value.statusMessage,
        broadcastMessage: 'Switched to live failover mode.',
      );
    } catch (error) {
      value = value.copyWith(
        failoverModeEnabled: true,
        failoverMessage: 'Failover activation failed: $error',
      );
    }
  }

  Future<void> _refreshLiveSessionAccess() async {
    final access = await classJoinService.fetchLiveSessionAccess(
      context: liveClassContext,
    );
    if (access == null || access.token.trim().isEmpty) {
      return;
    }
    _authToken = access.token;
    value = value.copyWith(
      session: value.session.copyWith(
        id: access.sessionId.trim().isNotEmpty
            ? access.sessionId
            : value.session.id,
        rtcProvider: access.provider,
        rtcServerUrl: access.serverUrl,
      ),
    );
  }

  Future<void> _rehydrateClassroomState() async {
    final snapshot = await classJoinService.fetchClassroomState(
      classId: value.session.id,
      userId: currentUserId,
    );
    if (snapshot == null) {
      return;
    }
    final shouldClearWhiteboardUser =
        !snapshot.whiteboardAccess &&
        value.activeWhiteboardUserId == currentUserId;
    final restoredWhiteboardStrokes = snapshot.whiteboardStrokes
        .map(_decodeWhiteboardStroke)
        .whereType<WhiteboardStroke>()
        .toList(growable: false);
    value = value.copyWith(
      activeBreakoutRoomId:
          snapshot.activeBreakoutRoomId ?? value.activeBreakoutRoomId,
      activeWhiteboardUserId: snapshot.activeWhiteboardUserId,
      clearActiveWhiteboardUserId:
          shouldClearWhiteboardUser || snapshot.activeWhiteboardUserId == null,
      whiteboardStrokes: restoredWhiteboardStrokes,
      isMeetingLocked: snapshot.meetingLocked,
      chatEnabled: snapshot.chatEnabled,
      waitingRoomEnabled: snapshot.waitingRoomEnabled,
      currentUserMicEnabled: snapshot.muted
          ? false
          : value.currentUserMicEnabled,
      currentUserCameraEnabled: snapshot.cameraDisabled
          ? false
          : value.currentUserCameraEnabled,
      isRecording: snapshot.isRecording,
    );
    if (snapshot.muted) {
      unawaited(
        zoomService.toggleMic(participantId: currentUserId, enabled: false),
      );
    }
    if (snapshot.cameraDisabled) {
      unawaited(
        zoomService.toggleCamera(participantId: currentUserId, enabled: false),
      );
    }
  }

  void _startLivePollCountdown() {
    _livePollTimer?.cancel();
    _livePollTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      if (!value.pollActive) {
        _livePollTimer?.cancel();
        return;
      }
      final remaining = value.pollTimer - 1;
      if (remaining <= 0) {
        value = value.copyWith(pollTimer: 0);
        _livePollTimer?.cancel();
        unawaited(endLivePoll(revealResults: false));
        return;
      }
      value = value.copyWith(pollTimer: remaining);
    });
  }

  Future<void> _refreshLivePollResults() async {
    final poll = value.currentPoll;
    if (poll == null) {
      return;
    }
    try {
      final results = await quizService.fetchLivePollResults(
        pollId: poll.pollId,
        optionCount: poll.options.length,
      );
      value = value.copyWith(
        pollResults: results.optionCounts,
        pollResultsRevealed: value.pollResultsRevealed || results.revealed,
        currentPoll: poll.copyWith(
          correctOption: results.correctOption ?? poll.correctOption,
        ),
      );
    } catch (_) {
      // Non-blocking, local counts remain visible.
    }
  }

  Future<void> _refreshLectureIndex() async {
    if (_isGeneratingIndexes) {
      return;
    }
    _isGeneratingIndexes = true;

    try {
      final generated = await lalacoreApi.generateLectureIndex(
        context: _buildAiContext(),
      );
      if (generated.isNotEmpty) {
        final merged = [...value.lectureIndex];
        for (final item in generated) {
          final exists = merged.any(
            (current) =>
                current.topic == item.topic &&
                (current.timestampSeconds - item.timestampSeconds).abs() < 60,
          );
          if (!exists) {
            merged.add(item);
          }
        }
        merged.sort((a, b) => a.timestampSeconds.compareTo(b.timestampSeconds));
        value = value.copyWith(lectureIndex: merged);
      }
    } catch (_) {
      // Keep classroom flow uninterrupted.
    } finally {
      _isGeneratingIndexes = false;
    }
  }

  void _scheduleIntelligenceRefresh() {
    _intelligenceDebounce?.cancel();
    _intelligenceDebounce = Timer(const Duration(milliseconds: 350), () {
      _refreshIntelligence();
    });
  }

  Future<void> _refreshIntelligence() async {
    if (_isRunningIntelligence) {
      return;
    }
    _isRunningIntelligence = true;

    try {
      final quiz = value.quiz;
      final quizAccuracy = quiz.totalResponses == 0
          ? 0.7
          : quiz.correctResponses / quiz.totalResponses;

      final result = await intelligenceService.analyze(
        ClassroomIntelligenceInput(
          transcript: _transcriptHistory,
          ocrText: value.ocrSnippets,
          chatQuestions: value.chatMessages
              .where((message) => message.message.contains('?'))
              .map((item) => item.message)
              .toList(growable: false),
          quizAccuracy: quizAccuracy,
          participationRate: value.analytics.participationRate,
          conceptCoverage: value.intelligence.concepts.isEmpty ? 0.2 : 0.8,
          doubtFrequency:
              value.analytics.doubtCount / (value.participants.length + 1),
        ),
        value.intelligence,
      );

      value = value.copyWith(intelligence: result);

      await intelligenceStorage.storeLectureIntelligence(
        sessionId: value.session.id,
        intelligence: result,
        notes: value.recordingNotes ?? '',
      );
    } finally {
      _isRunningIntelligence = false;
    }
  }

  void _pushReaction(ReactionEvent reaction) {
    value = value.copyWith(reactions: [...value.reactions, reaction]);

    Future<void>.delayed(const Duration(seconds: 3), () {
      if (!hasListeners) {
        return;
      }
      value = value.copyWith(
        reactions: value.reactions
            .where((item) => item.id != reaction.id)
            .toList(growable: false),
      );
    });
  }

  LectureNotesModel _composeLectureNotes({
    required ClassNotesPayload notes,
    required Map<String, dynamic> analysis,
  }) {
    final concepts = notes.keyConcepts;
    final formulas = notes.formulas;
    final shortcuts = notes.shortcuts;
    final mistakes = notes.commonMistakes;
    final transcriptSlice = _transcriptHistory
        .skip(_transcriptHistory.length > 5 ? _transcriptHistory.length - 5 : 0)
        .map((item) => item.message)
        .toList(growable: false);
    final ocrSlice = value.ocrSnippets
        .take(value.ocrSnippets.length > 4 ? 4 : value.ocrSnippets.length)
        .toList(growable: false);

    final sections = <LectureNotesSection>[];
    final maxSections = concepts.length > 4 ? 4 : concepts.length;
    for (var i = 0; i < maxSections; i += 1) {
      final concept = concepts[i];
      final formulaSet = formulas.isEmpty
          ? const <String>[]
          : formulas.skip(i).take(2).toList(growable: false);
      final shortcut = shortcuts.isEmpty
          ? 'Apply symmetry-first strategy before substitution.'
          : shortcuts[i % shortcuts.length];
      final example = transcriptSlice.isNotEmpty
          ? transcriptSlice[i % transcriptSlice.length]
          : 'Teacher explained this with a class example and shortcut.';

      sections.add(
        LectureNotesSection(
          topic: 'Topic ${i + 1}: $concept',
          concept: concept,
          formulas: formulaSet,
          example: example,
          keyPoints: [shortcut, if (mistakes.isNotEmpty) mistakes.first],
        ),
      );
    }

    if (sections.isEmpty) {
      sections.add(
        LectureNotesSection(
          topic: value.session.title,
          concept: 'Core lecture concept',
          formulas: formulas.take(2).toList(growable: false),
          example: transcriptSlice.isNotEmpty
              ? transcriptSlice.first
              : 'No transcript excerpt available.',
          keyPoints: shortcuts.take(2).toList(growable: false),
        ),
      );
    }

    final verificationNotes = <String>[
      if (analysis['insights'] is List &&
          (analysis['insights'] as List).isNotEmpty)
        'AI analysis insights: ${(analysis['insights'] as List).map((item) => item.toString()).join('; ')}',
      if (analysis['doubt_clusters'] is List &&
          (analysis['doubt_clusters'] as List).isNotEmpty)
        'Top doubt clusters: ${(analysis['doubt_clusters'] as List).map((item) => item.toString()).join(', ')}',
      if (analysis['verification_notes'] is List &&
          (analysis['verification_notes'] as List).isNotEmpty)
        'Web verification: ${(analysis['verification_notes'] as List).map((item) => item.toString()).join(' | ')}',
      if (ocrSlice.isNotEmpty)
        'OCR verification snippets: ${ocrSlice.join(' | ')}',
      if (_transcriptCount > 0) 'Transcript segments used: $_transcriptCount.',
      if (_transcriptCount == 0)
        'Transcript unavailable; notes derived from OCR and AI context only.',
    ];

    final sourceSummary =
        'Built from $_transcriptCount transcript segments, '
        '${value.ocrSnippets.length} OCR captures, and LalaCore analysis.';

    return LectureNotesModel(
      classId: value.session.id,
      classTitle: value.session.title,
      generatedAt: DateTime.now(),
      sourceSummary: sourceSummary,
      sections: sections,
      verificationNotes: verificationNotes,
    );
  }

  void _processLiveConceptChunk(TranscriptModel chunk) {
    final topic = _detectTopicFromText(chunk.message);
    final summary = _buildTopicSummary(topic, source: 'transcript');

    if (topic == null) {
      return;
    }

    final seconds = _elapsedSeconds(chunk.timestamp);
    final duplicate = value.lectureIndex.any(
      (item) =>
          item.topic == topic && (item.timestampSeconds - seconds).abs() < 90,
    );
    if (duplicate) {
      return;
    }

    value = value.copyWith(
      lectureIndex: [
        ...value.lectureIndex,
        LectureIndexModel(
          timestampSeconds: seconds,
          topic: topic,
          summary: summary,
        ),
      ],
    );
  }

  void _processOcrConceptSignals(String ocrText) {
    final lower = ocrText.toLowerCase();
    if (!(lower.contains('=') ||
        lower.contains('integral') ||
        lower.contains('matrix') ||
        lower.contains('reaction') ||
        lower.contains('vector') ||
        lower.contains('derivative'))) {
      return;
    }
    final topic = _detectTopicFromText(ocrText) ?? 'Formula Explanation';
    final seconds = _elapsedSeconds(DateTime.now());
    final duplicate = value.lectureIndex.any(
      (item) =>
          item.topic == topic && (item.timestampSeconds - seconds).abs() < 120,
    );
    if (duplicate) {
      return;
    }
    value = value.copyWith(
      lectureIndex: [
        ...value.lectureIndex,
        LectureIndexModel(
          timestampSeconds: seconds,
          topic: topic,
          summary: _buildTopicSummary(topic, source: 'ocr'),
        ),
      ],
    );
  }

  void _extractPracticeQuestionFromChunk(TranscriptModel chunk) {
    final text = chunk.message.trim();
    final lower = text.toLowerCase();
    final looksLikeQuestion =
        text.endsWith('?') ||
        lower.contains('solve') ||
        lower.contains('find') ||
        lower.contains('determine') ||
        lower.contains('calculate');
    if (!looksLikeQuestion) {
      return;
    }
    final duplicate = value.extractedPracticeQuestions.any(
      (item) => item.question.toLowerCase() == text.toLowerCase(),
    );
    if (duplicate) {
      return;
    }

    final answerMatch = RegExp(
      r'answer is ([^.,;]+)',
      caseSensitive: false,
    ).firstMatch(text);
    final finalAnswer = answerMatch?.group(1)?.trim() ?? '';
    final conceptTags = value.intelligence.concepts
        .take(3)
        .map((item) => item.concept)
        .toList(growable: false);
    final question = ExtractedPracticeQuestionModel(
      id: _nextId('practice'),
      question: text,
      solutionSteps: _recentSolutionContext(),
      finalAnswer: finalAnswer,
      conceptTags: conceptTags,
      difficulty: _estimateDifficulty(text),
      timestampSeconds: _elapsedSeconds(chunk.timestamp),
      createdAt: DateTime.now(),
    );

    value = value.copyWith(
      extractedPracticeQuestions: [
        ...value.extractedPracticeQuestions,
        question,
      ],
    );
    unawaited(
      quizService.saveExtractedPracticeQuestion(
        classId: value.session.id,
        question: question,
      ),
    );
  }

  Future<void> refreshPracticeReviewQueue() async {
    if (!canManageClass) {
      return;
    }
    try {
      final queue = await quizService.fetchPracticeReviewQueue(
        classId: value.session.id,
      );
      if (queue.isEmpty) {
        return;
      }
      final merged = <String, ExtractedPracticeQuestionModel>{
        for (final item in value.extractedPracticeQuestions) item.id: item,
      };
      for (final item in queue) {
        merged[item.id] = item;
      }
      value = value.copyWith(
        extractedPracticeQuestions: merged.values.toList(growable: false),
      );
    } catch (_) {
      // Keep local review queue if backend fetch is unavailable.
    }
  }

  Future<void> approvePracticeQuestion({
    required String questionId,
    String? editedQuestion,
    String? reviewerComment,
  }) async {
    if (!canManageClass) {
      return;
    }
    final status = (editedQuestion ?? '').trim().isNotEmpty
        ? ExtractedQuestionReviewStatus.edited
        : ExtractedQuestionReviewStatus.approved;
    await _reviewPracticeQuestion(
      questionId: questionId,
      status: status,
      editedQuestion: editedQuestion,
      reviewerComment: reviewerComment,
    );
  }

  Future<void> rejectPracticeQuestion({
    required String questionId,
    String? reviewerComment,
  }) async {
    if (!canManageClass) {
      return;
    }
    await _reviewPracticeQuestion(
      questionId: questionId,
      status: ExtractedQuestionReviewStatus.rejected,
      reviewerComment: reviewerComment,
    );
  }

  Future<void> _reviewPracticeQuestion({
    required String questionId,
    required ExtractedQuestionReviewStatus status,
    String? editedQuestion,
    String? reviewerComment,
  }) async {
    final index = value.extractedPracticeQuestions.indexWhere(
      (item) => item.id == questionId,
    );
    if (index == -1) {
      return;
    }
    final now = DateTime.now();
    final local = value.extractedPracticeQuestions[index].copyWith(
      reviewStatus: status,
      reviewedBy: currentUserName,
      reviewedAt: now,
      reviewerComment: reviewerComment,
      editedQuestion: editedQuestion,
    );
    final localUpdated = [...value.extractedPracticeQuestions];
    localUpdated[index] = local;
    value = value.copyWith(extractedPracticeQuestions: localUpdated);

    try {
      final backend = await quizService.reviewPracticeQuestion(
        classId: value.session.id,
        questionId: questionId,
        status: status,
        editedQuestion: editedQuestion,
        reviewerComment: reviewerComment,
      );
      if (backend != null) {
        final merged = [...value.extractedPracticeQuestions];
        final backendIndex = merged.indexWhere((item) => item.id == questionId);
        if (backendIndex != -1) {
          merged[backendIndex] = backend;
          value = value.copyWith(extractedPracticeQuestions: merged);
        }
      }
    } catch (_) {
      // Local state remains updated for uninterrupted review flow.
    }
  }

  List<String> _recentSolutionContext() {
    final transcript = _transcriptHistory;
    if (transcript.isEmpty) {
      return const [];
    }
    final start = transcript.length > 3 ? transcript.length - 3 : 0;
    return transcript
        .sublist(start)
        .map((item) => item.message)
        .toList(growable: false);
  }

  String _estimateDifficulty(String question) {
    final lower = question.toLowerCase();
    if (lower.contains('advanced') ||
        lower.contains('tricky') ||
        lower.contains('multi-step')) {
      return 'hard';
    }
    if (lower.contains('derive') || lower.contains('prove')) {
      return 'medium';
    }
    return 'easy';
  }

  bool _isRecordingJobPending(String status) {
    final lowered = status.toLowerCase();
    return lowered == 'queued' ||
        lowered == 'pending' ||
        lowered == 'processing' ||
        lowered == 'running' ||
        lowered == 'stopping_recording';
  }

  int _elapsedSeconds(DateTime timestamp) {
    final startedAt = value.session.startedAt ?? DateTime.now();
    final seconds = timestamp.difference(startedAt).inSeconds;
    return seconds < 0 ? 0 : seconds;
  }

  String _quizAccuracyPercent() {
    final quiz = value.quiz;
    if (quiz.totalResponses <= 0) {
      return 'N/A';
    }
    final percent = (quiz.correctResponses / quiz.totalResponses) * 100;
    return '${percent.toStringAsFixed(0)}%';
  }

  Map<String, dynamic> _buildDeviceInfo({required bool speakerTested}) {
    return {
      'platform': defaultTargetPlatform.name,
      'app_env': liveClassContext.classId,
      'speaker_tested': speakerTested,
      'camera_enabled': value.currentUserCameraEnabled,
      'mic_enabled': value.currentUserMicEnabled,
      'latency_ms': value.networkStats.latencyMs,
      'packet_loss': value.networkStats.packetLossPercent,
    };
  }

  AiRequestContext _buildAiContext() {
    final concepts = value.intelligence.concepts
        .map((item) => item.concept)
        .toSet()
        .toList(growable: false);

    final timestamps = value.intelligence.concepts
        .map((item) => item.timestampSeconds)
        .toList(growable: false);

    return AiRequestContext(
      transcript: _transcriptHistory,
      chatMessages: value.chatMessages
          .map((item) => '${item.sender}: ${item.message}')
          .toList(),
      ocrSnippets: value.ocrSnippets,
      lectureMaterials: [
        '${liveClassContext.subject} live class',
        if (liveClassContext.topic.trim().isNotEmpty)
          '${liveClassContext.topic.trim()} guided session',
        if (value.session.title.trim().isNotEmpty) value.session.title.trim(),
      ],
      detectedConcepts: concepts.isNotEmpty
          ? concepts
          : [
              if (liveClassContext.topic.trim().isNotEmpty)
                liveClassContext.topic.trim(),
              if (liveClassContext.subject.trim().isNotEmpty)
                liveClassContext.subject.trim(),
            ],
      timestamps: timestamps,
    );
  }

  List<TranscriptModel> get _transcriptHistory =>
      List<TranscriptModel>.unmodifiable(_fullTranscript);

  int get _transcriptCount =>
      _trimmedTranscriptSegments + _fullTranscript.length;

  List<TranscriptModel> _appendTranscriptChunk(TranscriptModel chunk) {
    _fullTranscript.add(chunk);
    if (_fullTranscript.length > _maxTranscriptHistorySize) {
      final overflow = _fullTranscript.length - _maxTranscriptHistorySize;
      _fullTranscript.removeRange(0, overflow);
      _trimmedTranscriptSegments += overflow;
    }
    final start = _fullTranscript.length > _liveTranscriptWindowSize
        ? _fullTranscript.length - _liveTranscriptWindowSize
        : 0;
    final liveTranscript = List<TranscriptModel>.unmodifiable(
      _fullTranscript.sublist(start),
    );
    value = value.copyWith(transcript: liveTranscript);
    return liveTranscript;
  }

  bool _requireHostAction(String action) {
    if (canManageClass) {
      return true;
    }
    value = value.copyWith(error: 'Only the host/co-host can $action.');
    return false;
  }

  String get _subjectKey {
    final normalized = liveClassContext.subject.trim().toLowerCase();
    if (normalized.contains('math')) {
      return 'mathematics';
    }
    if (normalized.contains('chem')) {
      return 'chemistry';
    }
    if (normalized.contains('bio')) {
      return 'biology';
    }
    return 'physics';
  }

  List<LivePollDraft> _fallbackLivePollLibrary() {
    switch (_subjectKey) {
      case 'mathematics':
        return [
          LivePollDraft(
            question:
                'For ${liveClassContext.topic}, what should you verify first before solving?',
            options: const [
              'Domain or condition check',
              'Units only',
              'Only the final answer',
              'Nothing, apply formula directly',
            ],
            timerSeconds: 20,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'medium',
          ),
          LivePollDraft(
            question:
                'Which step best improves JEE accuracy in ${liveClassContext.topic}?',
            options: const [
              'Track sign/constraint changes',
              'Skip intermediate steps completely',
              'Ignore special cases',
              'Memorize options only',
            ],
            timerSeconds: 15,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'easy',
          ),
        ];
      case 'chemistry':
        return [
          LivePollDraft(
            question:
                'In ${liveClassContext.topic}, the safest first move is to identify:',
            options: const [
              'The governing reaction/concept',
              'Only the calculator shortcut',
              'Only the answer option lengths',
              'Nothing before substitution',
            ],
            timerSeconds: 20,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'medium',
          ),
          LivePollDraft(
            question: 'What causes the most avoidable chemistry errors in JEE?',
            options: const [
              'Missing conditions or assumptions',
              'Writing balanced equations',
              'Checking limiting cases',
              'Re-reading the question',
            ],
            timerSeconds: 15,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'easy',
          ),
        ];
      case 'biology':
        return [
          LivePollDraft(
            question:
                'For ${liveClassContext.topic}, the best recall strategy is:',
            options: const [
              'Link concept to mechanism/process',
              'Memorize one keyword without context',
              'Ignore exceptions',
              'Skip diagrams completely',
            ],
            timerSeconds: 20,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'medium',
          ),
          LivePollDraft(
            question: 'Which habit improves biology MCQ accuracy the most?',
            options: const [
              'Read every qualifier carefully',
              'Pick the longest option',
              'Ignore NCERT wording',
              'Answer without elimination',
            ],
            timerSeconds: 15,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'easy',
          ),
        ];
      default:
        return [
          LivePollDraft(
            question:
                'In ${liveClassContext.topic}, what should you identify first?',
            options: const [
              'The governing principle and assumptions',
              'Only the final units',
              'Only the numerical data',
              'Nothing before substitution',
            ],
            timerSeconds: 20,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'medium',
          ),
          LivePollDraft(
            question:
                'Which classroom habit best reduces mistakes in JEE Physics?',
            options: const [
              'Check direction/sign conventions',
              'Ignore special cases',
              'Memorize answers directly',
              'Skip diagrams',
            ],
            timerSeconds: 15,
            correctOption: 0,
            topic: liveClassContext.topic,
            difficulty: 'easy',
          ),
        ];
    }
  }

  QuizState _fallbackQuizState() {
    switch (_subjectKey) {
      case 'mathematics':
        return QuizState(
          quizId: null,
          isActive: true,
          question:
              'In ${liveClassContext.topic}, which habit is most reliable under timed conditions?',
          options: const [
            'Check domain/constraints before finalizing',
            'Expand everything immediately',
            'Ignore special cases',
            'Skip verification',
          ],
          correctIndex: 0,
          selectedIndex: null,
          totalResponses: 0,
          correctResponses: 0,
        );
      case 'chemistry':
        return QuizState(
          quizId: null,
          isActive: true,
          question:
              'Which step is most important before solving a JEE Chemistry question on ${liveClassContext.topic}?',
          options: const [
            'Identify the governing reaction/concept',
            'Memorize the option order',
            'Ignore conditions',
            'Skip units entirely',
          ],
          correctIndex: 0,
          selectedIndex: null,
          totalResponses: 0,
          correctResponses: 0,
        );
      case 'biology':
        return QuizState(
          quizId: null,
          isActive: true,
          question:
              'For ${liveClassContext.topic}, which strategy improves accuracy the most?',
          options: const [
            'Match statements to the underlying process',
            'Ignore exceptions',
            'Answer from memory without reading options',
            'Skip diagrams and tables',
          ],
          correctIndex: 0,
          selectedIndex: null,
          totalResponses: 0,
          correctResponses: 0,
        );
      default:
        return const QuizState(
          quizId: null,
          isActive: true,
          question:
              'Which step is most important before using a physics formula?',
          options: [
            'Check assumptions and sign conventions',
            'Only memorize the final answer',
            'Ignore the diagram',
            'Use any equation that looks familiar',
          ],
          correctIndex: 0,
          selectedIndex: null,
          totalResponses: 0,
          correctResponses: 0,
        );
    }
  }

  String? _detectTopicFromText(String text) {
    final lower = text.toLowerCase();
    final trimmedTopic = liveClassContext.topic.trim();
    if (trimmedTopic.isNotEmpty) {
      final topicWords = trimmedTopic
          .toLowerCase()
          .split(RegExp(r'[^a-z0-9]+'))
          .where((item) => item.length > 3)
          .toList(growable: false);
      if (topicWords.any(lower.contains)) {
        return trimmedTopic;
      }
    }

    if (lower.contains('integral') || lower.contains('integration')) {
      return 'Integration';
    }
    if (lower.contains('derivative') || lower.contains('differentiat')) {
      return 'Differentiation';
    }
    if (lower.contains('matrix') || lower.contains('determinant')) {
      return 'Matrices and Determinants';
    }
    if (lower.contains('complex')) {
      return 'Complex Numbers';
    }
    if (lower.contains('vector')) {
      return 'Vector Algebra';
    }
    if (lower.contains('probability') || lower.contains('permutation')) {
      return 'Probability and Combinatorics';
    }
    if (lower.contains('equilibrium') || lower.contains('kinetics')) {
      return 'Chemical Equilibrium / Kinetics';
    }
    if (lower.contains('mole') || lower.contains('stoichiometry')) {
      return 'Mole Concept';
    }
    if (lower.contains('organic') || lower.contains('hydrocarbon')) {
      return 'Organic Chemistry';
    }
    if (lower.contains('motion') || lower.contains('kinematic')) {
      return 'Kinematics';
    }
    if (lower.contains('electric field') || lower.contains('electrostat')) {
      return 'Electrostatics';
    }
    if (lower.contains('flux') || lower.contains('gauss')) {
      return 'Gauss Law';
    }
    if (lower.contains('current') || lower.contains('circuit')) {
      return 'Current Electricity';
    }
    if (lower.contains('wave') || lower.contains('optics')) {
      return 'Waves / Optics';
    }
    if (lower.contains('theorem') || lower.contains('definition')) {
      return trimmedTopic.isNotEmpty ? trimmedTopic : 'Theory Segment';
    }
    if (lower.contains('example') || lower.contains('let us solve')) {
      return 'Worked Example';
    }
    if (lower.contains('formula') || lower.contains('=')) {
      return 'Formula Explanation';
    }
    return null;
  }

  String _buildTopicSummary(String? topic, {required String source}) {
    final resolved = topic ?? 'Current Topic';
    final sourceLabel = source == 'ocr' ? 'OCR' : 'Transcript';
    if (resolved == 'Worked Example') {
      return '$sourceLabel detected a worked-example segment.';
    }
    if (resolved == 'Formula Explanation') {
      return '$sourceLabel detected a formula or derivation segment.';
    }
    return '$sourceLabel detected a focus shift to $resolved.';
  }

  ParticipantModel? _findParticipant(
    List<ParticipantModel> participants,
    String id,
  ) {
    for (final participant in participants) {
      if (participant.id == id) {
        return participant;
      }
    }
    return null;
  }

  ParticipantModel? get _currentUser {
    return _findParticipant(value.participants, currentUserId);
  }

  String get _currentUserName => _currentUser?.name ?? currentUserName;

  bool _isLatex(String text) {
    return text.startsWith(r'$$') && text.endsWith(r'$$') && text.length > 4;
  }

  String _normalizeLatex(String text) {
    if (_isLatex(text)) {
      return text.substring(2, text.length - 2).trim();
    }
    return text;
  }

  String _nextId(String prefix) {
    _eventCounter += 1;
    return '${prefix}_$_eventCounter';
  }

  @override
  void dispose() {
    _livePollTimer?.cancel();
    _intelligenceDebounce?.cancel();
    _reconnectTimer?.cancel();

    for (final subscription in _subscriptions) {
      subscription.cancel();
    }

    transcriptionService.dispose();
    ocrCaptureService.dispose();
    zoomService.dispose();
    classJoinService.dispose();
    classroomSyncService.dispose();
    webRtcFailoverService.dispose();
    analyticsService.dispose();

    super.dispose();
  }

  // END_PHASE2_IMPLEMENTATION
}
