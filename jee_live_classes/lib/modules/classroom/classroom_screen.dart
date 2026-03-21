import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:livekit_client/livekit_client.dart'
    show VideoRenderMode, VideoTrackRenderer, VideoViewFit;

import '../../models/extracted_practice_question_model.dart';
import '../../models/participant_model.dart';
import '../../models/transcript_model.dart';
import '../../modules/ai/ai_panel.dart';
import '../../modules/chat/chat_panel.dart';
import '../../modules/layouts/grid_layout.dart';
import '../../modules/layouts/presentation_layout.dart';
import '../../modules/layouts/speaker_layout.dart';
import '../../modules/participants/participants_panel.dart';
import '../../modules/replay/lecture_replay_screen.dart';
import '../../modules/whiteboard/whiteboard_canvas.dart';
import '../../services/zoom_service.dart';
import '../../widgets/bottom_control_bar.dart';
import '../../widgets/glass_panel.dart';
import '../../widgets/layout_selector.dart';
import '../../widgets/network_indicator.dart';
import '../../widgets/poll_results_chart.dart';
import '../../widgets/reaction_overlay.dart';
import '../../widgets/video_tile.dart';
import '../../widgets/webrtc_failover_stage.dart';
import 'classroom_controller.dart';
import 'classroom_state.dart';
import 'doubt_queue_panel.dart';
import 'hands_raised_panel.dart';
import 'live_poll_student_view.dart';
import 'quick_quiz_panel.dart';
import 'waiting_room_panel.dart';

class ClassroomScreen extends StatefulWidget {
  const ClassroomScreen({super.key, required this.controller});

  final ClassroomController controller;

  @override
  State<ClassroomScreen> createState() => _ClassroomScreenState();
}

class _ClassroomScreenState extends State<ClassroomScreen> {
  bool _captionsEnabled = true;

  @override
  void initState() {
    super.initState();
    _enterLandscape();
    widget.controller.initialize();
  }

  @override
  void dispose() {
    _restoreOrientation();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final reduceMotion =
        MediaQuery.maybeOf(context)?.disableAnimations ?? false;
    final switchDuration = reduceMotion
        ? Duration.zero
        : const Duration(milliseconds: 220);
    return ValueListenableBuilder<ClassroomState>(
      valueListenable: widget.controller,
      builder: (context, state, _) {
        if (state.isJoining) {
          return const Scaffold(
            body: Center(child: CircularProgressIndicator()),
          );
        }

        return Scaffold(
          body: DecoratedBox(
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFFEFF6FF), Color(0xFFDDEBFD)],
                begin: Alignment.topCenter,
                end: Alignment.bottomCenter,
              ),
            ),
            child: SafeArea(
              child: Column(
                children: [
                  _TopBar(
                    state: state,
                    controller: widget.controller,
                    onLeave: _onLeave,
                  ),
                  if (state.error != null ||
                      state.broadcastMessage != null ||
                      state.failoverMessage != null)
                    _StatusBar(
                      error: state.error,
                      broadcastMessage: state.broadcastMessage,
                      failoverMessage: state.failoverMessage,
                      onCloseError: widget.controller.clearError,
                    ),
                  Expanded(
                    child: Row(
                      children: [
                        Expanded(
                          child: Stack(
                            children: [
                              AnimatedSwitcher(
                                duration: switchDuration,
                                switchInCurve: Curves.easeOutCubic,
                                switchOutCurve: Curves.easeInCubic,
                                transitionBuilder: (child, animation) {
                                  final slide = Tween<Offset>(
                                    begin: const Offset(0.02, 0),
                                    end: Offset.zero,
                                  ).animate(animation);
                                  return FadeTransition(
                                    opacity: animation,
                                    child: SlideTransition(
                                      position: slide,
                                      child: child,
                                    ),
                                  );
                                },
                                child: KeyedSubtree(
                                  key: ValueKey(
                                    'layout_${state.layoutMode.name}',
                                  ),
                                  child: _buildMainLayout(state),
                                ),
                              ),
                              if (state.focusModeEnabled)
                                Positioned.fill(
                                  child: Padding(
                                    padding: const EdgeInsets.all(12),
                                    child: WhiteboardCanvas(
                                      strokes: state.whiteboardStrokes,
                                      canDraw:
                                          widget.controller.canDrawWhiteboard,
                                      eraserEnabled: state.isWhiteboardEraser,
                                      onClear:
                                          widget.controller.clearWhiteboard,
                                      onStroke:
                                          widget.controller.addWhiteboardStroke,
                                      onEraserChanged:
                                          widget.controller.setWhiteboardEraser,
                                    ),
                                  ),
                                ),
                              if (!state.focusModeEnabled)
                                ReactionOverlay(reactions: state.reactions),
                              if (state.laserPointerEnabled)
                                _LaserPointerOverlay(
                                  normalizedPosition:
                                      state.laserPointerPosition ??
                                      const Offset(0.5, 0.5),
                                ),
                              if (state.laserPointerEnabled &&
                                  widget.controller.canManageClass)
                                Positioned.fill(
                                  child: GestureDetector(
                                    behavior: HitTestBehavior.translucent,
                                    onPanDown: (details) {
                                      final size = MediaQuery.of(context).size;
                                      widget.controller.updateLaserPointer(
                                        Offset(
                                          details.localPosition.dx / size.width,
                                          details.localPosition.dy /
                                              size.height,
                                        ),
                                      );
                                    },
                                    onPanUpdate: (details) {
                                      final size = MediaQuery.of(context).size;
                                      widget.controller.updateLaserPointer(
                                        Offset(
                                          details.localPosition.dx / size.width,
                                          details.localPosition.dy /
                                              size.height,
                                        ),
                                      );
                                    },
                                  ),
                                ),
                              if (_activeDoubt(state) != null)
                                _ActiveDoubtOverlay(
                                  doubt: _activeDoubt(state)!,
                                  canResolve: widget.controller.canManageClass,
                                ),
                              if (_captionsEnabled &&
                                  state.transcript.isNotEmpty)
                                _CaptionOverlay(
                                  transcript: state.transcript,
                                  bottomOffset: state.failoverModeEnabled
                                      ? 96
                                      : 88,
                                ),
                              if (state.failoverModeEnabled)
                                Positioned(
                                  left: 12,
                                  right: 12,
                                  bottom: 12,
                                  top: 12,
                                  child: WebRtcFailoverStage(
                                    service:
                                        widget.controller.webRtcFailoverService,
                                    onRetry: widget.controller.retryFailover,
                                  ),
                                ),
                              if (state.currentPoll != null)
                                _buildLivePollOverlay(state),
                              if (state.quiz.isActive)
                                Positioned.fill(
                                  child: _QuizOverlay(
                                    state: state,
                                    onAnswer:
                                        widget.controller.submitQuizAnswer,
                                    onClose: widget.controller.closeQuiz,
                                  ),
                                ),
                            ],
                          ),
                        ),
                        AnimatedSwitcher(
                          duration: switchDuration,
                          switchInCurve: Curves.easeOutCubic,
                          switchOutCurve: Curves.easeInCubic,
                          transitionBuilder: (child, animation) {
                            final slide = Tween<Offset>(
                              begin: const Offset(0.08, 0),
                              end: Offset.zero,
                            ).animate(animation);
                            return FadeTransition(
                              opacity: animation,
                              child: SlideTransition(
                                position: slide,
                                child: child,
                              ),
                            );
                          },
                          child: state.panel == ClassroomPanel.none
                              ? const SizedBox.shrink(key: ValueKey('no_panel'))
                              : SizedBox(
                                  key: ValueKey('panel_${state.panel.name}'),
                                  width: 420,
                                  child: Padding(
                                    padding: const EdgeInsets.fromLTRB(
                                      0,
                                      6,
                                      10,
                                      6,
                                    ),
                                    child: GlassPanel(
                                      padding: EdgeInsets.zero,
                                      borderRadius: const BorderRadius.all(
                                        Radius.circular(28),
                                      ),
                                      tintColor: const Color(0xDDF9FBFF),
                                      borderColor: const Color(0x52FFFFFF),
                                      child: _buildPanel(state),
                                    ),
                                  ),
                                ),
                        ),
                      ],
                    ),
                  ),
                  if (!state.focusModeEnabled)
                    _ParticipantsStrip(
                      participants: state.participants,
                      activeSpeakerId: state.activeSpeakerId,
                      onTap: (participant) {
                        widget.controller.pinParticipant(participant.id);
                      },
                    ),
                  BottomControlBar(
                    isMicOn: state.currentUserMicEnabled,
                    isCameraOn: state.currentUserCameraEnabled,
                    handRaised: state.currentUserHandRaised,
                    onMicTap: widget.controller.toggleMic,
                    onCameraTap: widget.controller.toggleCamera,
                    onRaiseHandTap: widget.controller.toggleRaiseHand,
                    onChatTap: () =>
                        widget.controller.setPanel(ClassroomPanel.chat),
                    onReactionTap: _showReactionPicker,
                    onMoreTap: () => _showMoreSheet(state),
                  ),
                ],
              ),
            ),
          ),
        );
      },
    );
  }

  Widget _buildMainLayout(ClassroomState state) {
    switch (state.layoutMode) {
      case ClassroomLayoutMode.grid:
        return GridLayout(
          participants: state.participants,
          activeSpeakerId: state.activeSpeakerId,
          participantMediaBuilder: _buildParticipantMedia,
          onParticipantTap: (participant) {
            widget.controller.pinParticipant(participant.id);
          },
        );
      case ClassroomLayoutMode.speaker:
        return SpeakerLayout(
          participants: state.participants,
          activeSpeakerId: state.activeSpeakerId,
          pinnedParticipantId: state.pinnedParticipantId,
          participantMediaBuilder: _buildParticipantMedia,
          onParticipantTap: (participant) {
            widget.controller.pinParticipant(participant.id);
          },
        );
      case ClassroomLayoutMode.presentation:
        return PresentationLayout(
          participants: state.participants,
          activeSpeakerId: state.activeSpeakerId,
          sharedContentSource: state.sharedContentSource,
          participantMediaBuilder: _buildParticipantMedia,
          sharedContent: _buildSharedContentStage(state),
          onParticipantTap: (participant) {
            widget.controller.pinParticipant(participant.id);
          },
        );
      case ClassroomLayoutMode.focus:
        final teacher = state.participants
            .where((participant) => participant.isTeacher)
            .toList(growable: false);
        if (teacher.isEmpty) {
          return const SizedBox.shrink();
        }
        return Padding(
          padding: const EdgeInsets.all(12),
          child: VideoTile(
            participant: teacher.first,
            isActiveSpeaker: teacher.first.id == state.activeSpeakerId,
            onTap: () => widget.controller.pinParticipant(teacher.first.id),
            media: _buildParticipantMedia(teacher.first),
          ),
        );
    }
  }

  Widget? _buildParticipantMedia(ParticipantModel participant) {
    final service = widget.controller.zoomService;
    if (service is! RealZoomService || !service.usesLiveKitMediaPlane) {
      return null;
    }
    final track = service.participantVideoTrack(participant.id);
    if (track == null) {
      return null;
    }
    return VideoTrackRenderer(
      track,
      key: ValueKey('livekit-video-${participant.id}-${track.sid}'),
      fit: VideoViewFit.cover,
      renderMode: VideoRenderMode.texture,
    );
  }

  Widget? _buildSharedContentStage(ClassroomState state) {
    final service = widget.controller.zoomService;
    if (service is! RealZoomService || !service.usesLiveKitMediaPlane) {
      return null;
    }
    final track = service.activeScreenShareTrack();
    if (track == null) {
      return null;
    }
    return Stack(
      fit: StackFit.expand,
      children: [
        ColoredBox(
          color: const Color(0xFF08111F),
          child: VideoTrackRenderer(
            track,
            key: ValueKey('livekit-screen-share-${track.sid}'),
            fit: VideoViewFit.contain,
            renderMode: VideoRenderMode.texture,
          ),
        ),
        Positioned(
          left: 16,
          right: 16,
          bottom: 16,
          child: DecoratedBox(
            decoration: BoxDecoration(
              color: Colors.black.withValues(alpha: 0.52),
              borderRadius: BorderRadius.circular(14),
            ),
            child: Padding(
              padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
              child: Text(
                state.sharedContentSource ?? 'Live screen share',
                style: const TextStyle(
                  color: Colors.white,
                  fontSize: 14,
                  fontWeight: FontWeight.w600,
                ),
              ),
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildPanel(ClassroomState state) {
    switch (state.panel) {
      case ClassroomPanel.none:
        return const SizedBox.shrink();
      case ClassroomPanel.participants:
        return ParticipantsPanel(
          participants: state.participants,
          canManageClass: widget.controller.canManageClass,
          networkQualityService: widget.controller.networkQualityService,
          onMute: widget.controller.muteParticipant,
          onRemove: widget.controller.removeParticipant,
          onDisableCamera: widget.controller.disableParticipantCamera,
          onPromote: widget.controller.promoteToCoHost,
        );
      case ClassroomPanel.handsRaised:
        return HandsRaisedPanel(
          raisedHands: state.raisedHands,
          onAllowMic: widget.controller.approveStudentMic,
          onDismissHand: (participantId) {
            widget.controller.lowerHandFor(participantId);
          },
        );
      case ClassroomPanel.chat:
        return ChatPanel(
          messages: state.chatMessages,
          chatEnabled: state.chatEnabled,
          onSend: widget.controller.sendChatMessage,
          onSendAttachment: widget.controller.sendChatAttachment,
          showAskDoubtAction: !widget.controller.canManageClass,
          onAskDoubt: widget.controller.askDoubtWithAi,
          onQueueDoubt:
              ({required String question, required String aiAttempt}) =>
                  widget.controller.queueUnresolvedDoubt(
                    question: question,
                    aiAttemptAnswer: aiAttempt,
                  ),
        );
      case ClassroomPanel.doubtQueue:
        return DoubtQueuePanel(
          doubts: state.doubtQueue,
          activeDoubtId: state.activeDoubtId,
          onSelect: widget.controller.selectDoubtForLiveAnswer,
          onResolve: widget.controller.resolveActiveDoubt,
          onClearActive: widget.controller.clearActiveDoubt,
        );
      case ClassroomPanel.ai:
        return AiPanel(
          messages: state.aiMessages,
          intelligence: state.intelligence,
          searchResults: state.searchResults,
          lectureNotes: state.lectureNotes,
          isGeneratingLectureNotes: state.isGeneratingLectureNotes,
          teacherSummaryReport: state.teacherSummaryReport,
          aiTeachingSuggestion: state.aiTeachingSuggestion,
          transcript: state.transcript,
          homework: state.homework,
          canManageClass: widget.controller.canManageClass,
          onSend: widget.controller.askLalacore,
          onSearch: widget.controller.searchLecture,
          onLaunchMiniQuiz: widget.controller.launchMiniQuizSuggestion,
          onGenerateLectureNotes: widget.controller.generateLectureNotes,
          onDownloadLectureNotes: widget.controller.downloadLectureNotesPdf,
          onGenerateFlashcards: widget.controller.generateFlashcardsFromLecture,
          onGenerateAdaptivePractice:
              widget.controller.generateAdaptivePracticeSet,
          onGenerateTeacherReport:
              widget.controller.generateTeacherIntelligenceReport,
          onGenerateAiPoll: _showQuickPollPanel,
        );
      case ClassroomPanel.whiteboard:
        return _WhiteboardPanel(controller: widget.controller, state: state);
      case ClassroomPanel.breakout:
        return _BreakoutPanel(controller: widget.controller, state: state);
      case ClassroomPanel.analytics:
        return _AnalyticsPanel(controller: widget.controller, state: state);
      case ClassroomPanel.waitingRoom:
        return WaitingRoomPanel(
          requests: state.waitingRoomRequests,
          onApprove: widget.controller.approveWaitingRoomRequest,
          onReject: widget.controller.rejectWaitingRoomRequest,
          onApproveAll: widget.controller.approveAllWaitingRoomRequests,
        );
    }
  }

  Widget _buildLivePollOverlay(ClassroomState state) {
    final poll = state.currentPoll;
    if (poll == null) {
      return const SizedBox.shrink();
    }

    return Positioned(
      top: 16,
      left: 0,
      right: 0,
      child: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 520),
          child: widget.controller.canManageClass
              ? _TeacherLivePollCard(
                  pollQuestion: poll.question,
                  options: poll.options,
                  optionCounts: state.pollResults,
                  timerSecondsRemaining: state.pollTimer,
                  pollActive: state.pollActive,
                  pollResultsRevealed: state.pollResultsRevealed,
                  correctOption: poll.correctOption,
                  silentMode: state.silentConceptCheckMode,
                  onEndPoll: () =>
                      widget.controller.endLivePoll(revealResults: false),
                  onReveal: widget.controller.revealLivePollResults,
                  onClose: widget.controller.clearLivePoll,
                )
              : LivePollStudentView(
                  poll: poll,
                  timeRemaining: state.pollTimer,
                  pollActive: state.pollActive,
                  submittedOption: state.submittedPollOption,
                  pollResultsRevealed: state.pollResultsRevealed,
                  onSubmit: widget.controller.submitStudentAnswer,
                ),
        ),
      ),
    );
  }

  Future<void> _showMoreSheet(ClassroomState state) async {
    await showModalBottomSheet<void>(
      context: context,
      showDragHandle: true,
      builder: (context) {
        return SafeArea(
          child: ListView(
            shrinkWrap: true,
            children: [
              ListTile(
                leading: const Icon(Icons.groups),
                title: const Text('Participants'),
                onTap: () {
                  widget.controller.setPanel(ClassroomPanel.participants);
                  Navigator.of(context).pop();
                },
              ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.pan_tool_alt_outlined),
                  title: Text('Hands Raised (${state.raisedHands.length})'),
                  onTap: () {
                    widget.controller.setPanel(ClassroomPanel.handsRaised);
                    Navigator.of(context).pop();
                  },
                ),
              ListTile(
                leading: const Icon(Icons.auto_awesome),
                title: const Text('AI Intelligence Panel'),
                onTap: () {
                  widget.controller.setPanel(ClassroomPanel.ai);
                  Navigator.of(context).pop();
                },
              ),
              SwitchListTile(
                secondary: const Icon(Icons.closed_caption_outlined),
                title: const Text('Live captions'),
                value: _captionsEnabled,
                onChanged: (enabled) {
                  setState(() {
                    _captionsEnabled = enabled;
                  });
                },
              ),
              ListTile(
                leading: const Icon(Icons.draw),
                title: const Text('Whiteboard'),
                onTap: () {
                  widget.controller.setPanel(ClassroomPanel.whiteboard);
                  Navigator.of(context).pop();
                },
              ),
              if (!widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.edit_note_outlined),
                  title: const Text('Request whiteboard access'),
                  onTap: () {
                    widget.controller.requestWhiteboardAccess();
                    Navigator.of(context).pop();
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.meeting_room),
                  title: const Text('Breakout Rooms'),
                  onTap: () {
                    widget.controller.setPanel(ClassroomPanel.breakout);
                    Navigator.of(context).pop();
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.insights),
                  title: const Text('Analytics & Mastery'),
                  onTap: () {
                    widget.controller.setPanel(ClassroomPanel.analytics);
                    Navigator.of(context).pop();
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.queue_outlined),
                  title: Text(
                    'Doubt Queue (${state.doubtQueue.where((item) => item.isQueued || item.isSelected).length})',
                  ),
                  onTap: () {
                    widget.controller.setPanel(ClassroomPanel.doubtQueue);
                    Navigator.of(context).pop();
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.how_to_reg),
                  title: Text(
                    'Waiting room queue (${state.waitingRoomRequests.length})',
                  ),
                  onTap: () {
                    widget.controller.setPanel(ClassroomPanel.waitingRoom);
                    Navigator.of(context).pop();
                  },
                ),
              const Divider(),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.screen_share),
                  title: Text(
                    state.sharedContentSource == null
                        ? 'Start screen share'
                        : 'Stop screen share',
                  ),
                  onTap: () {
                    if (state.sharedContentSource == null) {
                      widget.controller.startScreenShare();
                    } else {
                      widget.controller.stopScreenShare();
                    }
                    Navigator.of(context).pop();
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.quiz),
                  title: const Text('Launch quiz'),
                  onTap: () {
                    widget.controller.launchQuiz();
                    Navigator.of(context).pop();
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.poll),
                  title: const Text('Launch live quick poll'),
                  onTap: () {
                    Navigator.of(context).pop();
                    WidgetsBinding.instance.addPostFrameCallback((_) {
                      if (mounted) {
                        _showQuickPollPanel();
                      }
                    });
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.check_circle_outline),
                  title: const Text('Silent concept check (Yes/No)'),
                  onTap: () async {
                    Navigator.of(context).pop();
                    final result = await _showSilentConceptCheckDialog();
                    if (result != null) {
                      await widget.controller.startSilentConceptCheck(
                        question: result.$1,
                        timerSeconds: result.$2,
                      );
                    }
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.psychology_alt_outlined),
                  title: const Text('Suggest next step (AI)'),
                  onTap: () {
                    widget.controller.suggestNextTeachingStep();
                    Navigator.of(context).pop();
                  },
                ),
              ListTile(
                leading: const Icon(Icons.summarize_outlined),
                title: const Text('Generate AI lecture notes'),
                onTap: () {
                  widget.controller.generateLectureNotes();
                  Navigator.of(context).pop();
                },
              ),
              ListTile(
                leading: const Icon(Icons.style_outlined),
                title: const Text('Generate lecture flashcards'),
                onTap: () {
                  widget.controller.generateFlashcardsFromLecture();
                  Navigator.of(context).pop();
                },
              ),
              ListTile(
                leading: const Icon(Icons.school_outlined),
                title: const Text('Generate adaptive practice'),
                onTap: () {
                  widget.controller.generateAdaptivePracticeSet();
                  Navigator.of(context).pop();
                },
              ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.assessment_outlined),
                  title: const Text('Generate teacher class report'),
                  onTap: () {
                    widget.controller.generateTeacherIntelligenceReport();
                    Navigator.of(context).pop();
                  },
                ),
              ListTile(
                leading: const Icon(Icons.auto_fix_high),
                title: Text(
                  state.revisionModeEnabled
                      ? 'Disable AI Revision Mode'
                      : 'Enable AI Revision Mode',
                ),
                onTap: () {
                  widget.controller.toggleRevisionMode();
                  Navigator.of(context).pop();
                },
              ),
              const Divider(),
              if (widget.controller.canManageClass)
                SwitchListTile(
                  secondary: const Icon(Icons.filter_center_focus),
                  title: const Text('Focus mode'),
                  value: state.focusModeEnabled,
                  onChanged: (enabled) {
                    widget.controller.setFocusMode(enabled);
                  },
                ),
              if (widget.controller.canManageClass)
                SwitchListTile(
                  secondary: const Icon(Icons.adjust),
                  title: const Text('Teacher laser pointer'),
                  value: state.laserPointerEnabled,
                  onChanged: (_) {
                    widget.controller.toggleLaserPointer();
                  },
                ),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: const Icon(Icons.volume_off),
                  title: const Text('Mute all participants'),
                  onTap: () {
                    widget.controller.muteAllParticipants();
                    Navigator.of(context).pop();
                  },
                ),
              if (widget.controller.canManageClass)
                SwitchListTile(
                  secondary: const Icon(Icons.chat_bubble_outline),
                  title: const Text('Enable chat'),
                  value: state.chatEnabled,
                  onChanged: (enabled) {
                    widget.controller.setChatEnabled(enabled);
                  },
                ),
              if (widget.controller.canManageClass)
                SwitchListTile(
                  secondary: const Icon(Icons.security),
                  title: const Text('Waiting room approval'),
                  value: state.waitingRoomEnabled,
                  onChanged: (enabled) {
                    widget.controller.setWaitingRoomEnabled(enabled);
                  },
                ),
              if (widget.controller.canManageClass)
                SwitchListTile(
                  secondary: const Icon(Icons.lock_outline),
                  title: const Text('Lock meeting'),
                  value: state.isMeetingLocked,
                  onChanged: (locked) {
                    widget.controller.setMeetingLocked(locked);
                  },
                ),
              const Divider(),
              if (widget.controller.canManageClass)
                ListTile(
                  leading: Icon(
                    state.isRecording
                        ? Icons.stop_circle
                        : Icons.fiber_manual_record,
                    color: state.isRecording
                        ? Colors.red
                        : const Color(0xFFC0292A),
                  ),
                  title: Text(
                    state.isRecording
                        ? 'Stop recording + process AI'
                        : 'Start recording',
                  ),
                  onTap: () {
                    if (state.isRecording) {
                      widget.controller.stopRecordingAndProcess();
                    } else {
                      widget.controller.startRecording();
                    }
                    Navigator.of(context).pop();
                  },
                ),
              ListTile(
                leading: const Icon(Icons.play_circle_outline),
                title: const Text('Open lecture replay'),
                onTap: () async {
                  Navigator.of(context).pop();
                  await _openReplay();
                },
              ),
            ],
          ),
        );
      },
    );
  }

  Future<void> _showReactionPicker() async {
    final emoji = await showDialog<String>(
      context: context,
      builder: (context) {
        const items = ['👍', '👏', '🔥', '❓'];
        return AlertDialog(
          title: const Text('Send reaction'),
          content: Wrap(
            spacing: 8,
            children: items
                .map(
                  (item) => ActionChip(
                    label: Text(item, style: const TextStyle(fontSize: 22)),
                    onPressed: () => Navigator.of(context).pop(item),
                  ),
                )
                .toList(growable: false),
          ),
        );
      },
    );

    if (emoji != null) {
      widget.controller.sendReaction(emoji);
    }
  }

  Future<void> _showQuickPollPanel() async {
    await showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      showDragHandle: true,
      builder: (sheetContext) {
        return QuickQuizPanel(
          onCancel: () => Navigator.of(sheetContext).pop(),
          onGenerateWithAi: (topic, difficulty) {
            return widget.controller.generateLivePollWithAi(
              topic: topic,
              difficulty: difficulty,
            );
          },
          onLoadImportedPolls: widget.controller.loadImportedLivePolls,
          onStartPoll: (draft) async {
            await widget.controller.startLivePoll(draft);
            if (sheetContext.mounted) {
              Navigator.of(sheetContext).pop();
            }
          },
        );
      },
    );
  }

  Future<(String, int)?> _showSilentConceptCheckDialog() async {
    final questionController = TextEditingController(
      text: 'Do you understand this concept?',
    );
    final timerController = TextEditingController(text: '15');
    final result = await showDialog<(String, int)>(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: const Text('Silent Concept Check'),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextField(
                controller: questionController,
                decoration: const InputDecoration(
                  labelText: 'Question',
                  border: OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 10),
              TextField(
                controller: timerController,
                keyboardType: TextInputType.number,
                decoration: const InputDecoration(
                  labelText: 'Timer (seconds)',
                  border: OutlineInputBorder(),
                ),
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Cancel'),
            ),
            FilledButton(
              onPressed: () {
                final question = questionController.text.trim();
                final seconds = int.tryParse(timerController.text.trim()) ?? 15;
                if (question.isEmpty) {
                  return;
                }
                Navigator.of(
                  context,
                ).pop((question, seconds.clamp(5, 120).toInt()));
              },
              child: const Text('Start'),
            ),
          ],
        );
      },
    );
    questionController.dispose();
    timerController.dispose();
    return result;
  }

  _ActiveDoubtViewModel? _activeDoubt(ClassroomState state) {
    final id = state.activeDoubtId;
    if (id == null) {
      return null;
    }
    for (final doubt in state.doubtQueue) {
      if (doubt.id == id) {
        return _ActiveDoubtViewModel(
          studentName: doubt.studentName,
          question: doubt.question,
        );
      }
    }
    return null;
  }

  Future<void> _openReplay() async {
    final replay = await widget.controller.loadReplay();
    if (!mounted) {
      return;
    }
    if (replay == null) {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('Replay is not ready yet.')));
      return;
    }
    await Navigator.of(context).push(
      MaterialPageRoute<void>(
        builder: (_) => LectureReplayScreen(
          replay: replay,
          lalacoreApiOverride: widget.controller.lalacoreApi,
        ),
      ),
    );
  }

  Future<void> _onLeave() async {
    await widget.controller.leaveClass();
    if (!mounted) {
      return;
    }
    Navigator.of(context).maybePop();
  }

  Future<void> _enterLandscape() {
    return SystemChrome.setPreferredOrientations(const [
      DeviceOrientation.landscapeLeft,
      DeviceOrientation.landscapeRight,
    ]);
  }

  Future<void> _restoreOrientation() {
    return SystemChrome.setPreferredOrientations(DeviceOrientation.values);
  }
}

class _TopBar extends StatelessWidget {
  const _TopBar({
    required this.state,
    required this.controller,
    required this.onLeave,
  });

  final ClassroomState state;
  final ClassroomController controller;
  final VoidCallback onLeave;

  @override
  Widget build(BuildContext context) {
    final participantCount = state.participants.length;
    final waitingCount = state.waitingRoomRequests.length;
    return Container(
      margin: const EdgeInsets.fromLTRB(12, 10, 12, 8),
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
      decoration: BoxDecoration(
        color: Colors.white.withValues(alpha: 0.78),
        borderRadius: BorderRadius.circular(24),
        border: Border.all(color: const Color(0xFFD9E4F5)),
        boxShadow: const [
          BoxShadow(
            blurRadius: 18,
            offset: Offset(0, 8),
            color: Color(0x140C1A30),
          ),
        ],
      ),
      child: LayoutBuilder(
        builder: (context, constraints) {
          final compact = constraints.maxWidth < 1180;
          final leading = <Widget>[
            FilledButton.tonalIcon(
              onPressed: () => controller.setPanel(ClassroomPanel.ai),
              icon: const Icon(Icons.auto_awesome),
              label: const Text('Ask LalaCore'),
            ),
            if (controller.canManageClass) ...[
              const SizedBox(width: 8),
              FilledButton.tonalIcon(
                onPressed: controller.suggestNextTeachingStep,
                icon: const Icon(Icons.psychology_alt_outlined),
                label: const Text('Suggest Next Step'),
              ),
            ],
          ];

          final statusPills = Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              _StatusPill(
                icon: controller.usesMockServices
                    ? Icons.science_outlined
                    : Icons.verified_outlined,
                label: controller.serviceModeLabel,
                tinted: controller.usesMockServices,
              ),
              _StatusPill(
                icon: Icons.groups_rounded,
                label: '$participantCount participants',
              ),
              if (waitingCount > 0)
                _StatusPill(
                  icon: Icons.pending_actions_outlined,
                  label: '$waitingCount waiting',
                  tinted: true,
                ),
              if (state.isRecording)
                const _StatusPill(
                  icon: Icons.fiber_manual_record,
                  label: 'Recording',
                  danger: true,
                ),
              if (state.isMeetingLocked)
                const _StatusPill(
                  icon: Icons.lock_outline,
                  label: 'Locked',
                  tinted: true,
                ),
              if (state.focusModeEnabled)
                const _StatusPill(
                  icon: Icons.filter_center_focus,
                  label: 'Focus',
                ),
            ],
          );

          if (!compact) {
            return Row(
              children: [
                ...leading,
                const SizedBox(width: 12),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        state.session.title,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(
                          fontSize: 18,
                          fontWeight: FontWeight.w800,
                          color: Color(0xFF12263F),
                        ),
                      ),
                      const SizedBox(height: 8),
                      statusPills,
                    ],
                  ),
                ),
                const SizedBox(width: 12),
                LayoutSelector(
                  selected: state.layoutMode,
                  onSelected: controller.setLayoutMode,
                ),
                const SizedBox(width: 10),
                NetworkIndicator(
                  stats: state.networkStats,
                  qualityService: controller.networkQualityService,
                ),
                const SizedBox(width: 10),
                FilledButton(
                  style: FilledButton.styleFrom(
                    backgroundColor: const Color(0xFFD04848),
                    foregroundColor: Colors.white,
                  ),
                  onPressed: onLeave,
                  child: const Text('Leave'),
                ),
              ],
            );
          }

          return Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  ...leading,
                  const Spacer(),
                  FilledButton(
                    style: FilledButton.styleFrom(
                      backgroundColor: const Color(0xFFD04848),
                      foregroundColor: Colors.white,
                    ),
                    onPressed: onLeave,
                    child: const Text('Leave'),
                  ),
                ],
              ),
              const SizedBox(height: 10),
              Text(
                state.session.title,
                overflow: TextOverflow.ellipsis,
                style: const TextStyle(
                  fontSize: 16,
                  fontWeight: FontWeight.w800,
                  color: Color(0xFF12263F),
                ),
              ),
              const SizedBox(height: 8),
              statusPills,
              const SizedBox(height: 8),
              Row(
                children: [
                  PopupMenuButton<ClassroomLayoutMode>(
                    onSelected: controller.setLayoutMode,
                    itemBuilder: (context) => ClassroomLayoutMode.values
                        .map(
                          (mode) => PopupMenuItem<ClassroomLayoutMode>(
                            value: mode,
                            child: Text(_layoutLabel(mode)),
                          ),
                        )
                        .toList(growable: false),
                    child: Chip(
                      label: Text('Layout: ${_layoutLabel(state.layoutMode)}'),
                    ),
                  ),
                  const Spacer(),
                  NetworkIndicator(
                    stats: state.networkStats,
                    qualityService: controller.networkQualityService,
                  ),
                ],
              ),
            ],
          );
        },
      ),
    );
  }

  String _layoutLabel(ClassroomLayoutMode mode) {
    switch (mode) {
      case ClassroomLayoutMode.grid:
        return 'Grid';
      case ClassroomLayoutMode.speaker:
        return 'Speaker';
      case ClassroomLayoutMode.presentation:
        return 'Presentation';
      case ClassroomLayoutMode.focus:
        return 'Focus';
    }
  }
}

class _StatusPill extends StatelessWidget {
  const _StatusPill({
    required this.icon,
    required this.label,
    this.tinted = false,
    this.danger = false,
  });

  final IconData icon;
  final String label;
  final bool tinted;
  final bool danger;

  @override
  Widget build(BuildContext context) {
    final background = danger
        ? const Color(0xFFFFE9EA)
        : tinted
        ? const Color(0xFFFFF4DE)
        : const Color(0xFFF2F7FF);
    final border = danger
        ? const Color(0xFFF2B4B8)
        : tinted
        ? const Color(0xFFE8CB88)
        : const Color(0xFFD8E5F7);
    final foreground = danger
        ? const Color(0xFFB9373F)
        : tinted
        ? const Color(0xFF8A5A00)
        : const Color(0xFF1A4069);
    return DecoratedBox(
      decoration: BoxDecoration(
        color: background,
        borderRadius: BorderRadius.circular(999),
        border: Border.all(color: border),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(icon, size: 16, color: foreground),
            const SizedBox(width: 6),
            Text(
              label,
              style: TextStyle(fontWeight: FontWeight.w700, color: foreground),
            ),
          ],
        ),
      ),
    );
  }
}

class _ParticipantsStrip extends StatelessWidget {
  const _ParticipantsStrip({
    required this.participants,
    required this.activeSpeakerId,
    required this.onTap,
  });

  final List<ParticipantModel> participants;
  final String? activeSpeakerId;
  final ValueChanged<ParticipantModel> onTap;

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      height: 92,
      child: DecoratedBox(
        decoration: const BoxDecoration(
          border: Border(
            top: BorderSide(color: Color(0xFFD7E5F7)),
            bottom: BorderSide(color: Color(0xFFD7E5F7)),
          ),
          color: Colors.white,
        ),
        child: ListView.separated(
          padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
          scrollDirection: Axis.horizontal,
          itemCount: participants.length,
          separatorBuilder: (_, _) => const SizedBox(width: 8),
          itemBuilder: (context, index) {
            final participant = participants[index];
            return SizedBox(
              width: 135,
              child: VideoTile(
                participant: participant,
                isActiveSpeaker: participant.id == activeSpeakerId,
                onTap: () => onTap(participant),
              ),
            );
          },
        ),
      ),
    );
  }
}

class _StatusBar extends StatelessWidget {
  const _StatusBar({
    required this.error,
    required this.broadcastMessage,
    required this.failoverMessage,
    required this.onCloseError,
  });

  final String? error;
  final String? broadcastMessage;
  final String? failoverMessage;
  final VoidCallback onCloseError;

  @override
  Widget build(BuildContext context) {
    final hasError = error != null;
    final hasFailover = !hasError && failoverMessage != null;
    final message = hasError
        ? error!
        : hasFailover
        ? failoverMessage!
        : broadcastMessage!;

    return Padding(
      padding: const EdgeInsets.fromLTRB(12, 0, 12, 8),
      child: GlassPanel(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        borderRadius: const BorderRadius.all(Radius.circular(18)),
        blurSigma: 8,
        tintColor: hasError
            ? const Color(0xDDFDECEC)
            : hasFailover
            ? const Color(0xDDFFF7E8)
            : const Color(0xDDE7F3FF),
        borderColor: hasError
            ? const Color(0x40F4B1B1)
            : hasFailover
            ? const Color(0x40F0D08A)
            : const Color(0x52FFFFFF),
        child: Row(
          children: [
            Icon(
              hasError
                  ? Icons.error_outline
                  : hasFailover
                  ? Icons.swap_horiz
                  : Icons.campaign_outlined,
              size: 18,
              color: hasError
                  ? const Color(0xFFAD1E1E)
                  : hasFailover
                  ? const Color(0xFF8A5400)
                  : const Color(0xFF0F5D96),
            ),
            const SizedBox(width: 8),
            Expanded(
              child: Text(
                message,
                maxLines: 2,
                overflow: TextOverflow.ellipsis,
              ),
            ),
            if (hasError)
              IconButton(
                onPressed: onCloseError,
                icon: const Icon(Icons.close, size: 18),
              ),
          ],
        ),
      ),
    );
  }
}

class _ActiveDoubtViewModel {
  const _ActiveDoubtViewModel({
    required this.studentName,
    required this.question,
  });

  final String studentName;
  final String question;
}

class _ActiveDoubtOverlay extends StatelessWidget {
  const _ActiveDoubtOverlay({required this.doubt, required this.canResolve});

  final _ActiveDoubtViewModel doubt;
  final bool canResolve;

  @override
  Widget build(BuildContext context) {
    return Positioned(
      top: 16,
      left: 16,
      child: ConstrainedBox(
        constraints: const BoxConstraints(maxWidth: 420),
        child: DecoratedBox(
          decoration: BoxDecoration(
            color: const Color(0xFFFFF7E8),
            borderRadius: BorderRadius.circular(12),
            border: Border.all(color: const Color(0xFFF0D38A)),
            boxShadow: const [
              BoxShadow(
                blurRadius: 8,
                offset: Offset(0, 2),
                color: Color(0x22000000),
              ),
            ],
          ),
          child: Padding(
            padding: const EdgeInsets.all(10),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  canResolve
                      ? 'Live Doubt (Selected)'
                      : 'Teacher is answering this doubt',
                  style: const TextStyle(fontWeight: FontWeight.w700),
                ),
                const SizedBox(height: 4),
                Text(
                  '${doubt.studentName}: ${doubt.question}',
                  maxLines: 4,
                  overflow: TextOverflow.ellipsis,
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _QuizOverlay extends StatelessWidget {
  const _QuizOverlay({
    required this.state,
    required this.onAnswer,
    required this.onClose,
  });

  final ClassroomState state;
  final ValueChanged<int> onAnswer;
  final VoidCallback onClose;

  @override
  Widget build(BuildContext context) {
    final quiz = state.quiz;

    return DecoratedBox(
      decoration: BoxDecoration(color: Colors.black.withValues(alpha: 0.4)),
      child: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 620),
          child: Card(
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      const Text(
                        'Quick Quiz',
                        style: TextStyle(
                          fontWeight: FontWeight.w700,
                          fontSize: 18,
                        ),
                      ),
                      const Spacer(),
                      IconButton(
                        onPressed: onClose,
                        icon: const Icon(Icons.close),
                      ),
                    ],
                  ),
                  Text(quiz.question),
                  const SizedBox(height: 10),
                  ...quiz.options.asMap().entries.map((entry) {
                    final index = entry.key;
                    final option = entry.value;

                    final selected = quiz.selectedIndex == index;
                    final correct = quiz.correctIndex == index;

                    Color? tileColor;
                    if (quiz.selectedIndex != null) {
                      if (correct) {
                        tileColor = const Color(0xFFD7F6DE);
                      } else if (selected) {
                        tileColor = const Color(0xFFF9D5D5);
                      }
                    }

                    return Padding(
                      padding: const EdgeInsets.only(bottom: 8),
                      child: ListTile(
                        tileColor: tileColor,
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(10),
                          side: const BorderSide(color: Color(0xFFDCE7F7)),
                        ),
                        title: Text(option),
                        onTap: quiz.selectedIndex == null
                            ? () => onAnswer(index)
                            : null,
                      ),
                    );
                  }),
                  Text(
                    'Responses: ${quiz.totalResponses} | Correct: ${quiz.correctResponses}',
                    style: const TextStyle(
                      fontSize: 12,
                      color: Color(0xFF3F5672),
                    ),
                  ),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }
}

class _TeacherLivePollCard extends StatelessWidget {
  const _TeacherLivePollCard({
    required this.pollQuestion,
    required this.options,
    required this.optionCounts,
    required this.timerSecondsRemaining,
    required this.pollActive,
    required this.pollResultsRevealed,
    required this.correctOption,
    required this.silentMode,
    required this.onEndPoll,
    required this.onReveal,
    required this.onClose,
  });

  final String pollQuestion;
  final List<String> options;
  final Map<int, int> optionCounts;
  final int timerSecondsRemaining;
  final bool pollActive;
  final bool pollResultsRevealed;
  final int? correctOption;
  final bool silentMode;
  final Future<void> Function() onEndPoll;
  final Future<void> Function() onReveal;
  final VoidCallback onClose;

  @override
  Widget build(BuildContext context) {
    final total = optionCounts.values.fold<int>(0, (sum, item) => sum + item);

    return Card(
      elevation: 10,
      child: Padding(
        padding: const EdgeInsets.all(14),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                const Icon(Icons.poll_outlined, size: 18),
                const SizedBox(width: 6),
                Expanded(
                  child: Text(
                    silentMode ? 'Silent Concept Check' : 'Live Quick Poll',
                    style: TextStyle(fontWeight: FontWeight.w700),
                  ),
                ),
                Text(
                  pollActive ? '⏱ ${timerSecondsRemaining}s' : 'Closed',
                  style: TextStyle(
                    fontWeight: FontWeight.w700,
                    color: pollActive
                        ? const Color(0xFF173F72)
                        : const Color(0xFF6B7890),
                  ),
                ),
                IconButton(onPressed: onClose, icon: const Icon(Icons.close)),
              ],
            ),
            Text(
              pollQuestion,
              style: const TextStyle(fontWeight: FontWeight.w600),
            ),
            const SizedBox(height: 10),
            PollResultsChart(
              options: options,
              optionCounts: optionCounts,
              correctOption: pollResultsRevealed ? correctOption : null,
            ),
            Text(
              'Responses: $total',
              style: const TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
            ),
            if (silentMode && total > 0)
              Text(
                'Confusion level: ${(((optionCounts[1] ?? 0) / total) * 100).toStringAsFixed(0)}% selected "No"',
                style: const TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
              ),
            if (!pollActive && !pollResultsRevealed)
              const Text(
                'Poll closed. Students are waiting for reveal.',
                style: TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
              ),
            const SizedBox(height: 8),
            Row(
              children: [
                if (pollActive)
                  OutlinedButton.icon(
                    onPressed: onEndPoll,
                    icon: const Icon(Icons.stop_circle_outlined),
                    label: const Text('End Poll'),
                  ),
                if (pollActive) const SizedBox(width: 8),
                if (!pollResultsRevealed)
                  FilledButton.icon(
                    onPressed: onReveal,
                    icon: const Icon(Icons.visibility),
                    label: const Text('Reveal Results'),
                  ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

class _WhiteboardPanel extends StatelessWidget {
  const _WhiteboardPanel({required this.controller, required this.state});

  final ClassroomController controller;
  final ClassroomState state;

  @override
  Widget build(BuildContext context) {
    final activeUser = _participantById(state.activeWhiteboardUserId);

    return Column(
      children: [
        _PanelHeader(
          icon: Icons.draw,
          title: 'Whiteboard',
          subtitle: controller.canManageClass
              ? 'Teacher controls board access'
              : 'Request access to annotate on board',
        ),
        if (controller.canManageClass &&
            state.whiteboardAccessRequests.isNotEmpty)
          Padding(
            padding: const EdgeInsets.all(12),
            child: Wrap(
              spacing: 8,
              runSpacing: 8,
              children: state.whiteboardAccessRequests
                  .map((userId) {
                    final participant = _participantById(userId);
                    final label = participant?.name ?? userId;
                    return DecoratedBox(
                      decoration: BoxDecoration(
                        color: const Color(0xFFF8FAFF),
                        borderRadius: BorderRadius.circular(10),
                        border: Border.all(color: const Color(0xFFDCE7F7)),
                      ),
                      child: Padding(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 8,
                          vertical: 6,
                        ),
                        child: Row(
                          mainAxisSize: MainAxisSize.min,
                          children: [
                            Text(label),
                            const SizedBox(width: 6),
                            IconButton(
                              visualDensity: VisualDensity.compact,
                              onPressed: () =>
                                  controller.approveWhiteboardAccess(userId),
                              icon: const Icon(
                                Icons.check_circle_outline,
                                size: 18,
                              ),
                            ),
                            IconButton(
                              visualDensity: VisualDensity.compact,
                              onPressed: () =>
                                  controller.dismissWhiteboardRequest(userId),
                              icon: const Icon(Icons.close, size: 18),
                            ),
                          ],
                        ),
                      ),
                    );
                  })
                  .toList(growable: false),
            ),
          ),
        if (controller.canManageClass && activeUser != null)
          Padding(
            padding: const EdgeInsets.fromLTRB(12, 0, 12, 8),
            child: Row(
              children: [
                Expanded(
                  child: Text(
                    'Student annotation active: ${activeUser.name}',
                    style: const TextStyle(
                      fontSize: 12,
                      color: Color(0xFF3F5672),
                    ),
                  ),
                ),
                TextButton(
                  onPressed: controller.revokeWhiteboardAccess,
                  child: const Text('Revoke'),
                ),
              ],
            ),
          ),
        Expanded(
          child: Padding(
            padding: const EdgeInsets.all(12),
            child: WhiteboardCanvas(
              strokes: state.whiteboardStrokes,
              canDraw: controller.canDrawWhiteboard,
              eraserEnabled: state.isWhiteboardEraser,
              onClear: controller.clearWhiteboard,
              onStroke: controller.addWhiteboardStroke,
              onEraserChanged: controller.setWhiteboardEraser,
            ),
          ),
        ),
      ],
    );
  }

  ParticipantModel? _participantById(String? id) {
    if (id == null) {
      return null;
    }
    for (final participant in state.participants) {
      if (participant.id == id) {
        return participant;
      }
    }
    return null;
  }
}

class _LaserPointerOverlay extends StatelessWidget {
  const _LaserPointerOverlay({required this.normalizedPosition});

  final Offset normalizedPosition;

  @override
  Widget build(BuildContext context) {
    return Positioned.fill(
      child: IgnorePointer(
        child: LayoutBuilder(
          builder: (context, constraints) {
            final x =
                normalizedPosition.dx.clamp(0.0, 1.0) * constraints.maxWidth;
            final y =
                normalizedPosition.dy.clamp(0.0, 1.0) * constraints.maxHeight;
            return Stack(
              children: [
                Positioned(
                  left: x - 7,
                  top: y - 7,
                  child: Container(
                    width: 14,
                    height: 14,
                    decoration: BoxDecoration(
                      color: const Color(0xFFFF2B2B),
                      shape: BoxShape.circle,
                      boxShadow: const [
                        BoxShadow(
                          blurRadius: 10,
                          spreadRadius: 2,
                          color: Color(0x66FF2B2B),
                        ),
                      ],
                    ),
                  ),
                ),
              ],
            );
          },
        ),
      ),
    );
  }
}

class _BreakoutPanel extends StatefulWidget {
  const _BreakoutPanel({required this.controller, required this.state});

  final ClassroomController controller;
  final ClassroomState state;

  @override
  State<_BreakoutPanel> createState() => _BreakoutPanelState();
}

class _BreakoutPanelState extends State<_BreakoutPanel> {
  final TextEditingController _roomController = TextEditingController();
  final TextEditingController _broadcastController = TextEditingController();

  @override
  void dispose() {
    _roomController.dispose();
    _broadcastController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final rooms = widget.state.breakoutRooms;
    final students = widget.state.participants
        .where((participant) => !participant.isTeacher)
        .toList(growable: false);

    return Column(
      children: [
        const _PanelHeader(
          icon: Icons.meeting_room,
          title: 'Breakout Rooms',
          subtitle: 'Create collaborative problem-solving rooms',
        ),
        Padding(
          padding: const EdgeInsets.all(12),
          child: Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _roomController,
                  decoration: InputDecoration(
                    hintText: 'Room name',
                    border: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(10),
                    ),
                    isDense: true,
                  ),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton.tonal(
                onPressed: () {
                  final name = _roomController.text.trim();
                  if (name.isEmpty) {
                    return;
                  }
                  widget.controller.createBreakoutRoom(name);
                  _roomController.clear();
                },
                child: const Text('Create'),
              ),
            ],
          ),
        ),
        Expanded(
          child: ListView(
            padding: const EdgeInsets.symmetric(horizontal: 12),
            children: [
              ...rooms.map(
                (room) => Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      color: Colors.white,
                      borderRadius: BorderRadius.circular(12),
                      border: Border.all(color: const Color(0xFFDCE7F7)),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.all(10),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Row(
                            children: [
                              Expanded(
                                child: Text(
                                  room.name,
                                  style: const TextStyle(
                                    fontWeight: FontWeight.w700,
                                  ),
                                ),
                              ),
                              IconButton(
                                onPressed: () {
                                  widget.controller.removeBreakoutRoom(room.id);
                                },
                                icon: const Icon(Icons.delete_outline),
                              ),
                            ],
                          ),
                          Text(
                            'Members: ${room.participantIds.length}',
                            style: const TextStyle(fontSize: 12),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
              ),
              const SizedBox(height: 8),
              const Text(
                'Assign Students',
                style: TextStyle(fontWeight: FontWeight.w700),
              ),
              const SizedBox(height: 6),
              ...students.map(
                (student) => Padding(
                  padding: const EdgeInsets.only(bottom: 6),
                  child: Row(
                    children: [
                      Expanded(child: Text(student.name)),
                      PopupMenuButton<String>(
                        onSelected: (roomId) {
                          widget.controller.assignParticipantToRoom(
                            participantId: student.id,
                            roomId: roomId,
                          );
                        },
                        itemBuilder: (context) => rooms
                            .map(
                              (room) => PopupMenuItem<String>(
                                value: room.id,
                                child: Text(room.name),
                              ),
                            )
                            .toList(growable: false),
                        child: const Chip(label: Text('Assign room')),
                      ),
                    ],
                  ),
                ),
              ),
            ],
          ),
        ),
        Padding(
          padding: const EdgeInsets.all(12),
          child: Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _broadcastController,
                  decoration: InputDecoration(
                    hintText: 'Broadcast message to rooms',
                    border: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(10),
                    ),
                    isDense: true,
                  ),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton(
                onPressed: () {
                  final text = _broadcastController.text.trim();
                  if (text.isEmpty) {
                    return;
                  }
                  widget.controller.broadcastToBreakoutRooms(text);
                  _broadcastController.clear();
                },
                child: const Text('Broadcast'),
              ),
            ],
          ),
        ),
      ],
    );
  }
}

class _AnalyticsPanel extends StatelessWidget {
  const _AnalyticsPanel({required this.controller, required this.state});

  final ClassroomController controller;
  final ClassroomState state;

  @override
  Widget build(BuildContext context) {
    final mastery = state.intelligence.masteryScores.entries.toList(
      growable: false,
    );
    final pending = state.extractedPracticeQuestions
        .where(
          (item) => item.reviewStatus == ExtractedQuestionReviewStatus.pending,
        )
        .toList(growable: false);
    final approved = state.extractedPracticeQuestions
        .where(
          (item) =>
              item.reviewStatus == ExtractedQuestionReviewStatus.approved ||
              item.reviewStatus == ExtractedQuestionReviewStatus.edited,
        )
        .length;
    final rejected = state.extractedPracticeQuestions
        .where(
          (item) => item.reviewStatus == ExtractedQuestionReviewStatus.rejected,
        )
        .length;

    return Column(
      children: [
        const _PanelHeader(
          icon: Icons.insights,
          title: 'AI Analytics',
          subtitle: 'Mastery, engagement, and lecture quality insights',
        ),
        Expanded(
          child: ListView(
            padding: const EdgeInsets.all(12),
            children: [
              _MetricCard(
                title: 'Attendance',
                value: '${state.analytics.attendance}',
              ),
              _MetricCard(
                title: 'Participation Rate',
                value:
                    '${(state.analytics.participationRate * 100).toStringAsFixed(0)}%',
              ),
              _MetricCard(
                title: 'Quiz Attempts',
                value: '${state.analytics.quizAttempts}',
              ),
              _MetricCard(
                title: 'Doubts Asked',
                value: '${state.analytics.doubtCount}',
              ),
              _MetricCard(
                title: 'Knowledge Vault Entries',
                value: '${state.intelligence.knowledgeVaultEntries}',
              ),
              _MetricCard(
                title: 'Extracted JEE Questions',
                value: '${state.extractedPracticeQuestions.length}',
              ),
              _MetricCard(title: 'QC Pending', value: '${pending.length}'),
              _MetricCard(title: 'QC Approved', value: '$approved'),
              _MetricCard(title: 'QC Rejected', value: '$rejected'),
              if ((state.teacherSummaryReport ?? '').isNotEmpty)
                Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      color: const Color(0xFFF7F9FF),
                      borderRadius: BorderRadius.circular(12),
                      border: Border.all(color: const Color(0xFFDCE7F7)),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.all(10),
                      child: Text(state.teacherSummaryReport!),
                    ),
                  ),
                ),
              if (state.recordingJobStatus != null)
                _MetricCard(
                  title: 'Recording Worker Status',
                  value: state.recordingJobStatus!,
                ),
              const SizedBox(height: 10),
              const Text(
                'Concept Mastery',
                style: TextStyle(fontWeight: FontWeight.w700),
              ),
              const SizedBox(height: 6),
              ...mastery.map(
                (entry) => Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: _MasteryTile(concept: entry.key, score: entry.value),
                ),
              ),
              const SizedBox(height: 10),
              const Text(
                'Revision Recommendations',
                style: TextStyle(fontWeight: FontWeight.w700),
              ),
              const SizedBox(height: 6),
              ...state.intelligence.revisionRecommendations.entries.map(
                (entry) => Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      color: const Color(0xFFF5F9FF),
                      borderRadius: BorderRadius.circular(10),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.all(10),
                      child: Text(
                        '${entry.key}:\n- ${entry.value.join('\n- ')}',
                        style: const TextStyle(fontSize: 12),
                      ),
                    ),
                  ),
                ),
              ),
              if (state.extractedPracticeQuestions.isNotEmpty) ...[
                const SizedBox(height: 10),
                Text(
                  controller.canManageClass
                      ? 'Practice Extraction Review Queue'
                      : 'Auto Extracted Questions',
                  style: TextStyle(fontWeight: FontWeight.w700),
                ),
                const SizedBox(height: 6),
                ...state.extractedPracticeQuestions
                    .take(controller.canManageClass ? 12 : 5)
                    .map(
                      (item) => Padding(
                        padding: const EdgeInsets.only(bottom: 8),
                        child: _PracticeReviewCard(
                          question: item,
                          canReview: controller.canManageClass,
                          onApprove: () => controller.approvePracticeQuestion(
                            questionId: item.id,
                          ),
                          onEditAndApprove: () =>
                              _showEditApprovalDialog(context, item),
                          onReject: () => _showRejectDialog(context, item),
                        ),
                      ),
                    ),
              ],
            ],
          ),
        ),
      ],
    );
  }

  Future<void> _showEditApprovalDialog(
    BuildContext context,
    ExtractedPracticeQuestionModel item,
  ) async {
    final questionController = TextEditingController(
      text: item.effectiveQuestion,
    );
    final noteController = TextEditingController(
      text: item.reviewerComment ?? '',
    );
    final result = await showDialog<(String, String)>(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: const Text('Edit + Approve Question'),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextField(
                controller: questionController,
                maxLines: 4,
                decoration: const InputDecoration(
                  labelText: 'Edited Question',
                  border: OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 10),
              TextField(
                controller: noteController,
                maxLines: 2,
                decoration: const InputDecoration(
                  labelText: 'Review note',
                  border: OutlineInputBorder(),
                ),
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Cancel'),
            ),
            FilledButton(
              onPressed: () {
                Navigator.of(context).pop((
                  questionController.text.trim(),
                  noteController.text.trim(),
                ));
              },
              child: const Text('Approve'),
            ),
          ],
        );
      },
    );
    if (result == null) {
      return;
    }
    await controller.approvePracticeQuestion(
      questionId: item.id,
      editedQuestion: result.$1,
      reviewerComment: result.$2,
    );
  }

  Future<void> _showRejectDialog(
    BuildContext context,
    ExtractedPracticeQuestionModel item,
  ) async {
    final noteController = TextEditingController();
    final reason = await showDialog<String>(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: const Text('Reject Extracted Question'),
          content: TextField(
            controller: noteController,
            maxLines: 3,
            decoration: const InputDecoration(
              labelText: 'Reason',
              border: OutlineInputBorder(),
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Cancel'),
            ),
            FilledButton.tonal(
              onPressed: () => Navigator.of(context).pop(noteController.text),
              child: const Text('Reject'),
            ),
          ],
        );
      },
    );
    if (reason == null) {
      return;
    }
    await controller.rejectPracticeQuestion(
      questionId: item.id,
      reviewerComment: reason.trim(),
    );
  }
}

class _PracticeReviewCard extends StatelessWidget {
  const _PracticeReviewCard({
    required this.question,
    required this.canReview,
    required this.onApprove,
    required this.onEditAndApprove,
    required this.onReject,
  });

  final ExtractedPracticeQuestionModel question;
  final bool canReview;
  final Future<void> Function() onApprove;
  final Future<void> Function() onEditAndApprove;
  final Future<void> Function() onReject;

  @override
  Widget build(BuildContext context) {
    final color = switch (question.reviewStatus) {
      ExtractedQuestionReviewStatus.pending => const Color(0xFFFFF8E7),
      ExtractedQuestionReviewStatus.approved => const Color(0xFFE9F8EF),
      ExtractedQuestionReviewStatus.edited => const Color(0xFFE6F3FF),
      ExtractedQuestionReviewStatus.rejected => const Color(0xFFFFECEC),
    };
    final label = switch (question.reviewStatus) {
      ExtractedQuestionReviewStatus.pending => 'Pending',
      ExtractedQuestionReviewStatus.approved => 'Approved',
      ExtractedQuestionReviewStatus.edited => 'Edited+Approved',
      ExtractedQuestionReviewStatus.rejected => 'Rejected',
    };

    return DecoratedBox(
      decoration: BoxDecoration(
        color: color,
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: const Color(0xFFDCE7F7)),
      ),
      child: Padding(
        padding: const EdgeInsets.all(10),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Expanded(
                  child: Text(
                    question.effectiveQuestion,
                    style: const TextStyle(
                      fontSize: 12,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
                const SizedBox(width: 8),
                Chip(label: Text(label)),
              ],
            ),
            const SizedBox(height: 4),
            Text(
              'Difficulty: ${question.difficulty} | Tags: ${question.conceptTags.join(', ')}',
              style: const TextStyle(fontSize: 11, color: Color(0xFF3F5672)),
            ),
            if ((question.reviewerComment ?? '').isNotEmpty)
              Padding(
                padding: const EdgeInsets.only(top: 4),
                child: Text(
                  'Review note: ${question.reviewerComment}',
                  style: const TextStyle(fontSize: 11),
                ),
              ),
            if (canReview &&
                question.reviewStatus == ExtractedQuestionReviewStatus.pending)
              Padding(
                padding: const EdgeInsets.only(top: 8),
                child: Wrap(
                  spacing: 6,
                  runSpacing: 6,
                  children: [
                    FilledButton.tonal(
                      onPressed: () {
                        unawaited(onApprove());
                      },
                      child: const Text('Approve'),
                    ),
                    OutlinedButton(
                      onPressed: () {
                        unawaited(onEditAndApprove());
                      },
                      child: const Text('Edit + Approve'),
                    ),
                    TextButton(
                      onPressed: () {
                        unawaited(onReject());
                      },
                      child: const Text('Reject'),
                    ),
                  ],
                ),
              ),
          ],
        ),
      ),
    );
  }
}

class _MetricCard extends StatelessWidget {
  const _MetricCard({required this.title, required this.value});

  final String title;
  final String value;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: DecoratedBox(
        decoration: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: const Color(0xFFDCE7F7)),
        ),
        child: Padding(
          padding: const EdgeInsets.all(10),
          child: Row(
            children: [
              Expanded(
                child: Text(
                  title,
                  style: const TextStyle(fontWeight: FontWeight.w600),
                ),
              ),
              Text(value, style: const TextStyle(fontWeight: FontWeight.w800)),
            ],
          ),
        ),
      ),
    );
  }
}

class _CaptionOverlay extends StatelessWidget {
  const _CaptionOverlay({required this.transcript, required this.bottomOffset});

  final List<TranscriptModel> transcript;
  final double bottomOffset;

  @override
  Widget build(BuildContext context) {
    final items = transcript.length > 2
        ? transcript.sublist(transcript.length - 2)
        : transcript;
    return Positioned(
      left: 16,
      right: 16,
      bottom: bottomOffset,
      child: IgnorePointer(
        child: Center(
          child: ConstrainedBox(
            constraints: const BoxConstraints(maxWidth: 760),
            child: GlassPanel(
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
              borderRadius: const BorderRadius.all(Radius.circular(18)),
              blurSigma: 8,
              tintColor: const Color(0xB3141B2A),
              borderColor: const Color(0x33FFFFFF),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                mainAxisSize: MainAxisSize.min,
                children: items
                    .map(
                      (item) => Padding(
                        padding: const EdgeInsets.only(bottom: 4),
                        child: RichText(
                          text: TextSpan(
                            style: const TextStyle(
                              color: Colors.white,
                              fontSize: 14,
                              height: 1.35,
                            ),
                            children: [
                              TextSpan(
                                text: '${item.speakerName}: ',
                                style: const TextStyle(
                                  fontWeight: FontWeight.w700,
                                ),
                              ),
                              TextSpan(text: item.message),
                            ],
                          ),
                        ),
                      ),
                    )
                    .toList(growable: false),
              ),
            ),
          ),
        ),
      ),
    );
  }
}

class _MasteryTile extends StatelessWidget {
  const _MasteryTile({required this.concept, required this.score});

  final String concept;
  final double score;

  @override
  Widget build(BuildContext context) {
    final percent = (score * 100).toStringAsFixed(0);

    return DecoratedBox(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: const Color(0xFFDCE7F7)),
      ),
      child: Padding(
        padding: const EdgeInsets.all(10),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(concept, style: const TextStyle(fontWeight: FontWeight.w600)),
            const SizedBox(height: 6),
            LinearProgressIndicator(value: score),
            const SizedBox(height: 4),
            Text('Mastery: $percent%'),
          ],
        ),
      ),
    );
  }
}

class _PanelHeader extends StatelessWidget {
  const _PanelHeader({
    required this.icon,
    required this.title,
    required this.subtitle,
  });

  final IconData icon;
  final String title;
  final String subtitle;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: const BoxDecoration(
        border: Border(bottom: BorderSide(color: Color(0xFFDCE7F7))),
      ),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          children: [
            Icon(icon, size: 20),
            const SizedBox(width: 8),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    title,
                    style: const TextStyle(fontWeight: FontWeight.w700),
                  ),
                  Text(
                    subtitle,
                    style: const TextStyle(
                      fontSize: 12,
                      color: Color(0xFF4A607C),
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
