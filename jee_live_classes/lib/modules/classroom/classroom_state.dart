import 'package:flutter/material.dart';

import '../../models/class_session_model.dart';
import '../../models/doubt_queue_model.dart';
import '../../models/extracted_practice_question_model.dart';
import '../../models/lecture_index_model.dart';
import '../../models/lecture_intelligence_model.dart';
import '../../models/lecture_notes_model.dart';
import '../../models/live_poll_model.dart';
import '../../models/network_stats_model.dart';
import '../../models/participant_model.dart';
import '../../models/transcript_model.dart';
import '../../models/waiting_room_request_model.dart';
import '../../modules/breakout_rooms/breakout_room_manager.dart';
import '../../services/analytics_service.dart';

enum ClassroomLayoutMode { grid, speaker, presentation, focus }

enum JoinFlowStatus { idle, requesting, waitingApproval, approved, rejected }

enum ClassroomPanel {
  none,
  participants,
  handsRaised,
  chat,
  doubtQueue,
  ai,
  whiteboard,
  breakout,
  analytics,
  waitingRoom,
}

enum ChatAttachmentType { image, file }

class ChatAttachment {
  // BEGIN_PHASE2_IMPLEMENTATION
  const ChatAttachment({
    required this.type,
    required this.name,
    required this.path,
  });

  final ChatAttachmentType type;
  final String name;
  final String path;
  // END_PHASE2_IMPLEMENTATION
}

class ChatMessage {
  // BEGIN_PHASE2_IMPLEMENTATION
  const ChatMessage({
    required this.id,
    required this.sender,
    required this.message,
    required this.timestamp,
    required this.isTeacher,
    this.attachment,
    this.isLatex = false,
  });

  final String id;
  final String sender;
  final String message;
  final DateTime timestamp;
  final bool isTeacher;
  final ChatAttachment? attachment;
  final bool isLatex;
  // END_PHASE2_IMPLEMENTATION
}

class AiMessage {
  const AiMessage({
    required this.id,
    required this.message,
    required this.timestamp,
    required this.fromUser,
  });

  final String id;
  final String message;
  final DateTime timestamp;
  final bool fromUser;
}

class ReactionEvent {
  const ReactionEvent({
    required this.id,
    required this.emoji,
    required this.createdAt,
  });

  final String id;
  final String emoji;
  final DateTime createdAt;
}

class WhiteboardStroke {
  const WhiteboardStroke({
    required this.points,
    required this.color,
    required this.width,
  });

  final List<Offset> points;
  final Color color;
  final double width;

  WhiteboardStroke copyWith({
    List<Offset>? points,
    Color? color,
    double? width,
  }) {
    return WhiteboardStroke(
      points: points ?? this.points,
      color: color ?? this.color,
      width: width ?? this.width,
    );
  }
}

class QuizState {
  // BEGIN_PHASE2_IMPLEMENTATION
  const QuizState({
    required this.quizId,
    required this.isActive,
    required this.question,
    required this.options,
    required this.correctIndex,
    required this.selectedIndex,
    required this.totalResponses,
    required this.correctResponses,
  });

  final String? quizId;
  final bool isActive;
  final String question;
  final List<String> options;
  final int correctIndex;
  final int? selectedIndex;
  final int totalResponses;
  final int correctResponses;

  QuizState copyWith({
    String? quizId,
    bool? isActive,
    String? question,
    List<String>? options,
    int? correctIndex,
    int? selectedIndex,
    int? totalResponses,
    int? correctResponses,
    bool clearSelection = false,
    bool clearQuizId = false,
  }) {
    return QuizState(
      quizId: clearQuizId ? null : quizId ?? this.quizId,
      isActive: isActive ?? this.isActive,
      question: question ?? this.question,
      options: options ?? this.options,
      correctIndex: correctIndex ?? this.correctIndex,
      selectedIndex: clearSelection
          ? null
          : selectedIndex ?? this.selectedIndex,
      totalResponses: totalResponses ?? this.totalResponses,
      correctResponses: correctResponses ?? this.correctResponses,
    );
  }

  static const QuizState idle = QuizState(
    quizId: null,
    isActive: false,
    question: '',
    options: [],
    correctIndex: 0,
    selectedIndex: null,
    totalResponses: 0,
    correctResponses: 0,
  );
  // END_PHASE2_IMPLEMENTATION
}

class ClassroomState {
  // BEGIN_PHASE2_IMPLEMENTATION
  const ClassroomState({
    required this.session,
    required this.isJoining,
    required this.isConnected,
    required this.error,
    required this.layoutMode,
    required this.panel,
    required this.participants,
    required this.activeSpeakerId,
    required this.pinnedParticipantId,
    required this.sharedContentSource,
    required this.networkStats,
    required this.transcript,
    required this.chatMessages,
    required this.aiMessages,
    required this.ocrSnippets,
    required this.reactions,
    required this.lectureIndex,
    required this.homework,
    required this.whiteboardStrokes,
    required this.isWhiteboardEraser,
    required this.isRecording,
    required this.isProcessingRecording,
    required this.recordingNotes,
    required this.quiz,
    required this.breakoutRooms,
    required this.analytics,
    required this.chatEnabled,
    required this.waitingRoomEnabled,
    required this.currentUserMicEnabled,
    required this.currentUserCameraEnabled,
    required this.currentUserHandRaised,
    required this.broadcastMessage,
    required this.intelligence,
    required this.searchQuery,
    required this.searchResults,
    required this.revisionModeEnabled,
    required this.waitingRoomRequests,
    required this.isMeetingLocked,
    required this.connectionState,
    required this.activeBreakoutRoomId,
    required this.currentPoll,
    required this.pollTimer,
    required this.pollResults,
    required this.pollActive,
    required this.submittedPollOption,
    required this.pollResultsRevealed,
    required this.joinFlowStatus,
    required this.joinStatusMessage,
    required this.pendingJoinRequestId,
    required this.doubtQueue,
    required this.activeDoubtId,
    required this.lectureNotes,
    required this.isGeneratingLectureNotes,
    required this.raisedHands,
    required this.activeWhiteboardUserId,
    required this.whiteboardAccessRequests,
    required this.focusModeEnabled,
    required this.laserPointerEnabled,
    required this.laserPointerPosition,
    required this.silentConceptCheckMode,
    required this.aiTeachingSuggestion,
    required this.teacherSummaryReport,
    required this.extractedPracticeQuestions,
    required this.recordingJobId,
    required this.recordingJobStatus,
    required this.failoverModeEnabled,
    required this.failoverMessage,
  });

  final ClassSessionModel session;
  final bool isJoining;
  final bool isConnected;
  final String? error;
  final ClassroomLayoutMode layoutMode;
  final ClassroomPanel panel;
  final List<ParticipantModel> participants;
  final String? activeSpeakerId;
  final String? pinnedParticipantId;
  final String? sharedContentSource;
  final NetworkStatsModel networkStats;
  final List<TranscriptModel> transcript;
  final List<ChatMessage> chatMessages;
  final List<AiMessage> aiMessages;
  final List<String> ocrSnippets;
  final List<ReactionEvent> reactions;
  final List<LectureIndexModel> lectureIndex;
  final Map<String, List<String>> homework;
  final List<WhiteboardStroke> whiteboardStrokes;
  final bool isWhiteboardEraser;
  final bool isRecording;
  final bool isProcessingRecording;
  final String? recordingNotes;
  final QuizState quiz;
  final List<BreakoutRoom> breakoutRooms;
  final AnalyticsSnapshot analytics;
  final bool chatEnabled;
  final bool waitingRoomEnabled;
  final bool currentUserMicEnabled;
  final bool currentUserCameraEnabled;
  final bool currentUserHandRaised;
  final String? broadcastMessage;
  final LectureIntelligenceModel intelligence;
  final String searchQuery;
  final List<LectureSearchResult> searchResults;
  final bool revisionModeEnabled;
  final List<WaitingRoomRequestModel> waitingRoomRequests;
  final bool isMeetingLocked;
  final RtcConnectionState connectionState;
  final String? activeBreakoutRoomId;
  final LivePollModel? currentPoll;
  final int pollTimer;
  final Map<int, int> pollResults;
  final bool pollActive;
  final int? submittedPollOption;
  final bool pollResultsRevealed;
  final JoinFlowStatus joinFlowStatus;
  final String? joinStatusMessage;
  final String? pendingJoinRequestId;
  final List<DoubtQueueModel> doubtQueue;
  final String? activeDoubtId;
  final LectureNotesModel? lectureNotes;
  final bool isGeneratingLectureNotes;
  final List<ParticipantModel> raisedHands;
  final String? activeWhiteboardUserId;
  final List<String> whiteboardAccessRequests;
  final bool focusModeEnabled;
  final bool laserPointerEnabled;
  final Offset? laserPointerPosition;
  final bool silentConceptCheckMode;
  final String? aiTeachingSuggestion;
  final String? teacherSummaryReport;
  final List<ExtractedPracticeQuestionModel> extractedPracticeQuestions;
  final String? recordingJobId;
  final String? recordingJobStatus;
  final bool failoverModeEnabled;
  final String? failoverMessage;

  ClassroomState copyWith({
    ClassSessionModel? session,
    bool? isJoining,
    bool? isConnected,
    String? error,
    bool clearError = false,
    ClassroomLayoutMode? layoutMode,
    ClassroomPanel? panel,
    List<ParticipantModel>? participants,
    String? activeSpeakerId,
    bool clearActiveSpeaker = false,
    String? pinnedParticipantId,
    bool clearPinnedParticipant = false,
    String? sharedContentSource,
    bool clearSharedContent = false,
    NetworkStatsModel? networkStats,
    List<TranscriptModel>? transcript,
    List<ChatMessage>? chatMessages,
    List<AiMessage>? aiMessages,
    List<String>? ocrSnippets,
    List<ReactionEvent>? reactions,
    List<LectureIndexModel>? lectureIndex,
    Map<String, List<String>>? homework,
    List<WhiteboardStroke>? whiteboardStrokes,
    bool? isWhiteboardEraser,
    bool? isRecording,
    bool? isProcessingRecording,
    String? recordingNotes,
    bool clearRecordingNotes = false,
    QuizState? quiz,
    List<BreakoutRoom>? breakoutRooms,
    AnalyticsSnapshot? analytics,
    bool? chatEnabled,
    bool? waitingRoomEnabled,
    bool? currentUserMicEnabled,
    bool? currentUserCameraEnabled,
    bool? currentUserHandRaised,
    String? broadcastMessage,
    bool clearBroadcast = false,
    LectureIntelligenceModel? intelligence,
    String? searchQuery,
    List<LectureSearchResult>? searchResults,
    bool? revisionModeEnabled,
    List<WaitingRoomRequestModel>? waitingRoomRequests,
    bool? isMeetingLocked,
    RtcConnectionState? connectionState,
    String? activeBreakoutRoomId,
    bool clearActiveBreakoutRoom = false,
    LivePollModel? currentPoll,
    bool clearCurrentPoll = false,
    int? pollTimer,
    Map<int, int>? pollResults,
    bool? pollActive,
    int? submittedPollOption,
    bool clearSubmittedPollOption = false,
    bool? pollResultsRevealed,
    JoinFlowStatus? joinFlowStatus,
    String? joinStatusMessage,
    bool clearJoinStatusMessage = false,
    String? pendingJoinRequestId,
    bool clearPendingJoinRequestId = false,
    List<DoubtQueueModel>? doubtQueue,
    String? activeDoubtId,
    bool clearActiveDoubtId = false,
    LectureNotesModel? lectureNotes,
    bool clearLectureNotes = false,
    bool? isGeneratingLectureNotes,
    List<ParticipantModel>? raisedHands,
    String? activeWhiteboardUserId,
    bool clearActiveWhiteboardUserId = false,
    List<String>? whiteboardAccessRequests,
    bool? focusModeEnabled,
    bool? laserPointerEnabled,
    Offset? laserPointerPosition,
    bool clearLaserPointerPosition = false,
    bool? silentConceptCheckMode,
    String? aiTeachingSuggestion,
    bool clearAiTeachingSuggestion = false,
    String? teacherSummaryReport,
    bool clearTeacherSummaryReport = false,
    List<ExtractedPracticeQuestionModel>? extractedPracticeQuestions,
    String? recordingJobId,
    bool clearRecordingJobId = false,
    String? recordingJobStatus,
    bool clearRecordingJobStatus = false,
    bool? failoverModeEnabled,
    String? failoverMessage,
    bool clearFailoverMessage = false,
  }) {
    return ClassroomState(
      session: session ?? this.session,
      isJoining: isJoining ?? this.isJoining,
      isConnected: isConnected ?? this.isConnected,
      error: clearError ? null : error ?? this.error,
      layoutMode: layoutMode ?? this.layoutMode,
      panel: panel ?? this.panel,
      participants: participants ?? this.participants,
      activeSpeakerId: clearActiveSpeaker
          ? null
          : activeSpeakerId ?? this.activeSpeakerId,
      pinnedParticipantId: clearPinnedParticipant
          ? null
          : pinnedParticipantId ?? this.pinnedParticipantId,
      sharedContentSource: clearSharedContent
          ? null
          : sharedContentSource ?? this.sharedContentSource,
      networkStats: networkStats ?? this.networkStats,
      transcript: transcript ?? this.transcript,
      chatMessages: chatMessages ?? this.chatMessages,
      aiMessages: aiMessages ?? this.aiMessages,
      ocrSnippets: ocrSnippets ?? this.ocrSnippets,
      reactions: reactions ?? this.reactions,
      lectureIndex: lectureIndex ?? this.lectureIndex,
      homework: homework ?? this.homework,
      whiteboardStrokes: whiteboardStrokes ?? this.whiteboardStrokes,
      isWhiteboardEraser: isWhiteboardEraser ?? this.isWhiteboardEraser,
      isRecording: isRecording ?? this.isRecording,
      isProcessingRecording:
          isProcessingRecording ?? this.isProcessingRecording,
      recordingNotes: clearRecordingNotes
          ? null
          : recordingNotes ?? this.recordingNotes,
      quiz: quiz ?? this.quiz,
      breakoutRooms: breakoutRooms ?? this.breakoutRooms,
      analytics: analytics ?? this.analytics,
      chatEnabled: chatEnabled ?? this.chatEnabled,
      waitingRoomEnabled: waitingRoomEnabled ?? this.waitingRoomEnabled,
      currentUserMicEnabled:
          currentUserMicEnabled ?? this.currentUserMicEnabled,
      currentUserCameraEnabled:
          currentUserCameraEnabled ?? this.currentUserCameraEnabled,
      currentUserHandRaised:
          currentUserHandRaised ?? this.currentUserHandRaised,
      broadcastMessage: clearBroadcast
          ? null
          : broadcastMessage ?? this.broadcastMessage,
      intelligence: intelligence ?? this.intelligence,
      searchQuery: searchQuery ?? this.searchQuery,
      searchResults: searchResults ?? this.searchResults,
      revisionModeEnabled: revisionModeEnabled ?? this.revisionModeEnabled,
      waitingRoomRequests: waitingRoomRequests ?? this.waitingRoomRequests,
      isMeetingLocked: isMeetingLocked ?? this.isMeetingLocked,
      connectionState: connectionState ?? this.connectionState,
      activeBreakoutRoomId: clearActiveBreakoutRoom
          ? null
          : activeBreakoutRoomId ?? this.activeBreakoutRoomId,
      currentPoll: clearCurrentPoll ? null : currentPoll ?? this.currentPoll,
      pollTimer: pollTimer ?? this.pollTimer,
      pollResults: pollResults ?? this.pollResults,
      pollActive: pollActive ?? this.pollActive,
      submittedPollOption: clearSubmittedPollOption
          ? null
          : submittedPollOption ?? this.submittedPollOption,
      pollResultsRevealed: pollResultsRevealed ?? this.pollResultsRevealed,
      joinFlowStatus: joinFlowStatus ?? this.joinFlowStatus,
      joinStatusMessage: clearJoinStatusMessage
          ? null
          : joinStatusMessage ?? this.joinStatusMessage,
      pendingJoinRequestId: clearPendingJoinRequestId
          ? null
          : pendingJoinRequestId ?? this.pendingJoinRequestId,
      doubtQueue: doubtQueue ?? this.doubtQueue,
      activeDoubtId: clearActiveDoubtId
          ? null
          : activeDoubtId ?? this.activeDoubtId,
      lectureNotes: clearLectureNotes
          ? null
          : lectureNotes ?? this.lectureNotes,
      isGeneratingLectureNotes:
          isGeneratingLectureNotes ?? this.isGeneratingLectureNotes,
      raisedHands: raisedHands ?? this.raisedHands,
      activeWhiteboardUserId: clearActiveWhiteboardUserId
          ? null
          : activeWhiteboardUserId ?? this.activeWhiteboardUserId,
      whiteboardAccessRequests:
          whiteboardAccessRequests ?? this.whiteboardAccessRequests,
      focusModeEnabled: focusModeEnabled ?? this.focusModeEnabled,
      laserPointerEnabled: laserPointerEnabled ?? this.laserPointerEnabled,
      laserPointerPosition: clearLaserPointerPosition
          ? null
          : laserPointerPosition ?? this.laserPointerPosition,
      silentConceptCheckMode:
          silentConceptCheckMode ?? this.silentConceptCheckMode,
      aiTeachingSuggestion: clearAiTeachingSuggestion
          ? null
          : aiTeachingSuggestion ?? this.aiTeachingSuggestion,
      teacherSummaryReport: clearTeacherSummaryReport
          ? null
          : teacherSummaryReport ?? this.teacherSummaryReport,
      extractedPracticeQuestions:
          extractedPracticeQuestions ?? this.extractedPracticeQuestions,
      recordingJobId: clearRecordingJobId
          ? null
          : recordingJobId ?? this.recordingJobId,
      recordingJobStatus: clearRecordingJobStatus
          ? null
          : recordingJobStatus ?? this.recordingJobStatus,
      failoverModeEnabled: failoverModeEnabled ?? this.failoverModeEnabled,
      failoverMessage: clearFailoverMessage
          ? null
          : failoverMessage ?? this.failoverMessage,
    );
  }

  factory ClassroomState.initial({required ClassSessionModel session}) {
    return ClassroomState(
      session: session,
      isJoining: false,
      isConnected: false,
      error: null,
      layoutMode: ClassroomLayoutMode.speaker,
      panel: ClassroomPanel.none,
      participants: const [],
      activeSpeakerId: null,
      pinnedParticipantId: null,
      sharedContentSource: null,
      networkStats: const NetworkStatsModel(
        latencyMs: 0,
        packetLossPercent: 0,
        jitterMs: 0,
        uplinkKbps: 0,
        downlinkKbps: 0,
        quality: NetworkQuality.good,
      ),
      transcript: const [],
      chatMessages: const [],
      aiMessages: const [],
      ocrSnippets: const [],
      reactions: const [],
      lectureIndex: const [],
      homework: const {'easy': [], 'medium': [], 'hard': []},
      whiteboardStrokes: const [],
      isWhiteboardEraser: false,
      isRecording: false,
      isProcessingRecording: false,
      recordingNotes: null,
      quiz: QuizState.idle,
      breakoutRooms: const [],
      analytics: const AnalyticsSnapshot(
        attendance: 0,
        quizAttempts: 0,
        doubtCount: 0,
        participationRate: 0,
      ),
      chatEnabled: true,
      waitingRoomEnabled: true,
      currentUserMicEnabled: true,
      currentUserCameraEnabled: true,
      currentUserHandRaised: false,
      broadcastMessage: null,
      intelligence: LectureIntelligenceModel.empty,
      searchQuery: '',
      searchResults: const [],
      revisionModeEnabled: false,
      waitingRoomRequests: const [],
      isMeetingLocked: false,
      connectionState: RtcConnectionState.disconnected,
      activeBreakoutRoomId: null,
      currentPoll: null,
      pollTimer: 0,
      pollResults: const {},
      pollActive: false,
      submittedPollOption: null,
      pollResultsRevealed: false,
      joinFlowStatus: JoinFlowStatus.idle,
      joinStatusMessage: null,
      pendingJoinRequestId: null,
      doubtQueue: const [],
      activeDoubtId: null,
      lectureNotes: null,
      isGeneratingLectureNotes: false,
      raisedHands: const [],
      activeWhiteboardUserId: null,
      whiteboardAccessRequests: const [],
      focusModeEnabled: false,
      laserPointerEnabled: false,
      laserPointerPosition: null,
      silentConceptCheckMode: false,
      aiTeachingSuggestion: null,
      teacherSummaryReport: null,
      extractedPracticeQuestions: const [],
      recordingJobId: null,
      recordingJobStatus: null,
      failoverModeEnabled: false,
      failoverMessage: null,
    );
  }
  // END_PHASE2_IMPLEMENTATION
}
