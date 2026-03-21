import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart';

import '../../models/class_session_model.dart';
import '../../models/live_class_context.dart';
import '../../models/network_stats_model.dart';
import '../../services/prejoin_media_service.dart';
import '../../services/prejoin_settings_service.dart';
import '../classroom/classroom_controller.dart';
import '../classroom/classroom_screen.dart';
import 'mic_test_widget.dart';
import 'waiting_room_screen.dart';

class JoinReadinessScreen extends StatefulWidget {
  const JoinReadinessScreen({
    super.key,
    required this.controller,
    required this.contextData,
    required this.settingsService,
  });

  final ClassroomController controller;
  final LiveClassContext contextData;
  final PreJoinSettingsService settingsService;

  @override
  State<JoinReadinessScreen> createState() => _JoinReadinessScreenState();
}

class _JoinReadinessScreenState extends State<JoinReadinessScreen> {
  late final PreJoinMediaService _preJoinMediaService;
  bool _loading = true;
  bool _requesting = false;
  bool _cameraEnabled = true;
  bool _micEnabled = true;
  bool _speakerTested = false;
  ClassSessionModel? _session;
  NetworkStatsModel? _networkStats;
  String? _error;

  @override
  void initState() {
    super.initState();
    _preJoinMediaService = RealPreJoinMediaService();
    _bootstrap();
  }

  @override
  void dispose() {
    _preJoinMediaService.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    final session = _session;
    final title = session?.title ?? widget.contextData.classTitle;
    final teacher = session?.teacherName ?? widget.contextData.teacherName;
    final network = _networkStats ?? widget.controller.value.networkStats;
    final usesMocks = widget.controller.usesMockServices;
    final fallbackNote = widget.controller.preJoinMediaReadinessNote;

    return Scaffold(
      body: DecoratedBox(
        decoration: const BoxDecoration(
          gradient: LinearGradient(
            colors: [Color(0xFFF4F8FF), Color(0xFFDCE8FB)],
            begin: Alignment.topCenter,
            end: Alignment.bottomCenter,
          ),
        ),
        child: SafeArea(
          child: _loading
              ? const Center(child: CircularProgressIndicator())
              : Center(
                  child: ConstrainedBox(
                    constraints: const BoxConstraints(maxWidth: 1080),
                    child: Padding(
                      padding: const EdgeInsets.all(16),
                      child: Column(
                        children: [
                          Row(
                            children: [
                              const Icon(Icons.videocam_outlined),
                              const SizedBox(width: 8),
                              Text(
                                'Live Class Readiness',
                                style: theme.textTheme.titleLarge?.copyWith(
                                  fontWeight: FontWeight.w700,
                                ),
                              ),
                              const SizedBox(width: 12),
                              _ModeChip(
                                label: widget.controller.serviceModeLabel,
                                tinted: usesMocks,
                              ),
                              const Spacer(),
                              if (_error != null)
                                Text(
                                  _error!,
                                  style: const TextStyle(
                                    color: Color(0xFFB32B2B),
                                  ),
                                ),
                            ],
                          ),
                          const SizedBox(height: 12),
                          Expanded(
                            child: Row(
                              children: [
                                Expanded(
                                  flex: 6,
                                  child: Column(
                                    children: [
                                      ValueListenableBuilder<
                                        PreJoinMediaSnapshot
                                      >(
                                        valueListenable: _preJoinMediaService
                                            .snapshotListenable,
                                        builder: (context, media, _) {
                                          return ValueListenableBuilder<
                                            RTCVideoRenderer?
                                          >(
                                            valueListenable:
                                                _preJoinMediaService
                                                    .rendererListenable,
                                            builder: (context, renderer, _) {
                                              return _CameraPreviewCard(
                                                cameraEnabled: _cameraEnabled,
                                                usesMockServices: usesMocks,
                                                note: media.errorMessage != null
                                                    ? '${media.statusMessage} ${media.errorMessage}'
                                                    : media
                                                          .statusMessage
                                                          .isNotEmpty
                                                    ? media.statusMessage
                                                    : fallbackNote,
                                                renderer: renderer,
                                                previewLive: media.previewLive,
                                              );
                                            },
                                          );
                                        },
                                      ),
                                      const SizedBox(height: 10),
                                      ValueListenableBuilder<
                                        PreJoinMediaSnapshot
                                      >(
                                        valueListenable: _preJoinMediaService
                                            .snapshotListenable,
                                        builder: (context, media, _) {
                                          final caption = media.previewLive
                                              ? (media.micLevelApproximate
                                                    ? 'Mic input is live. Level is best-effort from local media stats.'
                                                    : 'Mic input is live.')
                                              : fallbackNote;
                                          return MicTestWidget(
                                            level: media.micLevel,
                                            enabled: _micEnabled,
                                            caption: caption,
                                          );
                                        },
                                      ),
                                      const SizedBox(height: 10),
                                      _NetworkCard(
                                        stats: network,
                                        qualityLabel: widget
                                            .controller
                                            .networkQualityService
                                            .qualityLabel(network.quality),
                                        onRefresh: _refreshNetwork,
                                      ),
                                    ],
                                  ),
                                ),
                                const SizedBox(width: 14),
                                Expanded(
                                  flex: 5,
                                  child: _SideCard(
                                    contextData: widget.contextData,
                                    title: title,
                                    teacher: teacher,
                                    micEnabled: _micEnabled,
                                    cameraEnabled: _cameraEnabled,
                                    speakerTested: _speakerTested,
                                    requesting: _requesting,
                                    isTeacher: widget.contextData.isTeacher,
                                    serviceModeLabel:
                                        widget.controller.serviceModeLabel,
                                    serviceModeSummary:
                                        widget.controller.serviceModeSummary,
                                    onMicToggle: (enabled) {
                                      setState(() {
                                        _micEnabled = enabled;
                                      });
                                      unawaited(
                                        _preJoinMediaService.setMicEnabled(
                                          enabled,
                                        ),
                                      );
                                      _saveSettings();
                                    },
                                    onCameraToggle: (enabled) {
                                      setState(() {
                                        _cameraEnabled = enabled;
                                      });
                                      unawaited(
                                        _preJoinMediaService.setCameraEnabled(
                                          enabled,
                                        ),
                                      );
                                      _saveSettings();
                                    },
                                    onTestSpeaker: _testSpeaker,
                                    onJoin: _onJoin,
                                    onCancel: _onCancel,
                                  ),
                                ),
                              ],
                            ),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
        ),
      ),
    );
  }

  Future<void> _bootstrap() async {
    try {
      final settings = await widget.settingsService.load(
        classId: widget.contextData.classId,
        userId: widget.contextData.userId,
      );
      final session = await widget.controller.fetchClassSessionForJoin();
      await widget.controller.prepareJoinFlow();
      final network = await widget.controller.checkJoinNetworkQuality();
      if (!mounted) {
        return;
      }
      setState(() {
        _session = session;
        _networkStats = network;
        _cameraEnabled = settings.cameraEnabled;
        _micEnabled = settings.micEnabled;
        _speakerTested = settings.speakerTested;
        _loading = false;
      });
      unawaited(
        _preJoinMediaService.start(
          cameraEnabled: settings.cameraEnabled,
          micEnabled: settings.micEnabled,
        ),
      );
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _loading = false;
        _error = 'Setup failed: $error';
      });
    }
  }

  Future<void> _saveSettings() async {
    await widget.settingsService.save(
      classId: widget.contextData.classId,
      userId: widget.contextData.userId,
      settings: PreJoinSettings(
        cameraEnabled: _cameraEnabled,
        micEnabled: _micEnabled,
        speakerTested: _speakerTested,
      ),
    );
  }

  Future<void> _refreshNetwork() async {
    final stats = await widget.controller.checkJoinNetworkQuality();
    if (!mounted) {
      return;
    }
    setState(() {
      _networkStats = stats;
    });
  }

  Future<void> _testSpeaker() async {
    await SystemSound.play(SystemSoundType.click);
    if (!mounted) {
      return;
    }
    setState(() {
      _speakerTested = true;
    });
    await _saveSettings();
  }

  Future<void> _onJoin() async {
    setState(() {
      _requesting = true;
      _error = null;
    });
    await _saveSettings();

    try {
      if (widget.contextData.isTeacher) {
        await widget.controller.startClassFromReadiness(
          cameraEnabled: _cameraEnabled,
          micEnabled: _micEnabled,
        );
        if (!mounted) {
          return;
        }
        await Navigator.of(context).push(
          MaterialPageRoute<void>(
            builder: (_) => ClassroomScreen(controller: widget.controller),
          ),
        );
      } else {
        final requestId = await widget.controller.requestJoin(
          cameraEnabled: _cameraEnabled,
          micEnabled: _micEnabled,
          speakerTested: _speakerTested,
        );
        if (!mounted) {
          return;
        }
        await Navigator.of(context).push(
          MaterialPageRoute<void>(
            builder: (_) => WaitingRoomScreen(
              controller: widget.controller,
              contextData: widget.contextData,
              requestId: requestId,
              cameraEnabled: _cameraEnabled,
              micEnabled: _micEnabled,
            ),
          ),
        );
      }
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _error = '$error';
      });
    } finally {
      if (mounted) {
        setState(() {
          _requesting = false;
        });
      }
    }
  }

  void _onCancel() {
    Navigator.of(context).maybePop();
  }
}

class _CameraPreviewCard extends StatelessWidget {
  const _CameraPreviewCard({
    required this.cameraEnabled,
    required this.usesMockServices,
    required this.note,
    required this.renderer,
    required this.previewLive,
  });

  final bool cameraEnabled;
  final bool usesMockServices;
  final String note;
  final RTCVideoRenderer? renderer;
  final bool previewLive;

  @override
  Widget build(BuildContext context) {
    return Expanded(
      child: DecoratedBox(
        decoration: BoxDecoration(
          color: Colors.black,
          borderRadius: BorderRadius.circular(16),
          border: Border.all(color: const Color(0xFF1E2F47)),
        ),
        child: Stack(
          children: [
            Positioned.fill(
              child: AnimatedOpacity(
                duration: const Duration(milliseconds: 220),
                opacity: cameraEnabled ? 1 : 0.35,
                child: renderer != null && previewLive
                    ? ClipRRect(
                        borderRadius: BorderRadius.circular(16),
                        child: RTCVideoView(
                          renderer!,
                          mirror: true,
                          objectFit:
                              RTCVideoViewObjectFit.RTCVideoViewObjectFitCover,
                        ),
                      )
                    : const DecoratedBox(
                        decoration: BoxDecoration(
                          gradient: RadialGradient(
                            colors: [Color(0xFF2A455F), Color(0xFF101B28)],
                          ),
                        ),
                      ),
              ),
            ),
            if (renderer == null || !previewLive)
              Center(
                child: Icon(
                  cameraEnabled ? Icons.videocam : Icons.videocam_off,
                  size: 58,
                  color: Colors.white,
                ),
              ),
            Positioned(
              left: 12,
              bottom: 12,
              child: Chip(
                label: Text(
                  cameraEnabled
                      ? (previewLive
                            ? 'Live Preview'
                            : (usesMockServices
                                  ? 'Simulated Preview'
                                  : 'Camera Enabled'))
                      : 'Camera Off',
                ),
              ),
            ),
            Positioned(
              left: 12,
              right: 12,
              top: 12,
              child: DecoratedBox(
                decoration: BoxDecoration(
                  color: Colors.white.withValues(alpha: 0.14),
                  borderRadius: BorderRadius.circular(12),
                  border: Border.all(
                    color: Colors.white.withValues(alpha: 0.18),
                  ),
                ),
                child: Padding(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 12,
                    vertical: 8,
                  ),
                  child: Text(
                    note,
                    style: const TextStyle(fontSize: 12, color: Colors.white),
                  ),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _NetworkCard extends StatelessWidget {
  const _NetworkCard({
    required this.stats,
    required this.qualityLabel,
    required this.onRefresh,
  });

  final NetworkStatsModel stats;
  final String qualityLabel;
  final Future<void> Function() onRefresh;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFFD7E5F7)),
      ),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                const Icon(Icons.network_check, size: 18),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    'Network: $qualityLabel (${stats.latencyMs} ms)',
                    style: const TextStyle(fontWeight: FontWeight.w700),
                  ),
                ),
                IconButton(
                  onPressed: onRefresh,
                  icon: const Icon(Icons.refresh),
                ),
              ],
            ),
            Text('Packet loss: ${stats.packetLossPercent.toStringAsFixed(1)}%'),
            Text('Jitter: ${stats.jitterMs} ms'),
            Text('Downlink: ${stats.downlinkKbps} kbps'),
          ],
        ),
      ),
    );
  }
}

class _SideCard extends StatelessWidget {
  const _SideCard({
    required this.contextData,
    required this.title,
    required this.teacher,
    required this.micEnabled,
    required this.cameraEnabled,
    required this.speakerTested,
    required this.requesting,
    required this.isTeacher,
    required this.serviceModeLabel,
    required this.serviceModeSummary,
    required this.onMicToggle,
    required this.onCameraToggle,
    required this.onTestSpeaker,
    required this.onJoin,
    required this.onCancel,
  });

  final LiveClassContext contextData;
  final String title;
  final String teacher;
  final bool micEnabled;
  final bool cameraEnabled;
  final bool speakerTested;
  final bool requesting;
  final bool isTeacher;
  final String serviceModeLabel;
  final String serviceModeSummary;
  final ValueChanged<bool> onMicToggle;
  final ValueChanged<bool> onCameraToggle;
  final Future<void> Function() onTestSpeaker;
  final Future<void> Function() onJoin;
  final VoidCallback onCancel;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: const Color(0xFFD7E5F7)),
      ),
      child: Padding(
        padding: const EdgeInsets.all(14),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              title,
              style: const TextStyle(fontWeight: FontWeight.w700, fontSize: 16),
            ),
            const SizedBox(height: 6),
            Text('Subject: ${contextData.subject}'),
            Text('Topic: ${contextData.topic}'),
            Text('Teacher: $teacher'),
            if (contextData.startTimeLabel != null)
              Text('Start Time: ${contextData.startTimeLabel}'),
            const Divider(height: 20),
            DecoratedBox(
              decoration: BoxDecoration(
                color: const Color(0xFFF5F8FE),
                borderRadius: BorderRadius.circular(14),
                border: Border.all(color: const Color(0xFFD7E3F4)),
              ),
              child: Padding(
                padding: const EdgeInsets.all(12),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Row(
                      children: [
                        const Icon(Icons.shield_outlined, size: 18),
                        const SizedBox(width: 8),
                        Text(
                          serviceModeLabel,
                          style: const TextStyle(fontWeight: FontWeight.w700),
                        ),
                      ],
                    ),
                    const SizedBox(height: 6),
                    Text(
                      serviceModeSummary,
                      style: const TextStyle(
                        fontSize: 12,
                        color: Color(0xFF526A88),
                      ),
                    ),
                  ],
                ),
              ),
            ),
            const SizedBox(height: 10),
            Text('Joining as: ${contextData.userName}'),
            Text('Role: ${contextData.role}'),
            const SizedBox(height: 8),
            SwitchListTile(
              contentPadding: EdgeInsets.zero,
              value: micEnabled,
              onChanged: onMicToggle,
              title: const Text('Mic'),
            ),
            SwitchListTile(
              contentPadding: EdgeInsets.zero,
              value: cameraEnabled,
              onChanged: onCameraToggle,
              title: const Text('Camera'),
            ),
            const SizedBox(height: 4),
            OutlinedButton.icon(
              onPressed: onTestSpeaker,
              icon: Icon(
                speakerTested ? Icons.check_circle : Icons.volume_up_outlined,
              ),
              label: Text(speakerTested ? 'Speaker Tested' : 'Test Speaker'),
            ),
            const Spacer(),
            Row(
              children: [
                TextButton(
                  onPressed: requesting ? null : onCancel,
                  child: const Text('Cancel'),
                ),
                const Spacer(),
                FilledButton(
                  onPressed: requesting ? null : onJoin,
                  child: requesting
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : Text(isTeacher ? 'Start Class' : 'Join Class'),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

class _ModeChip extends StatelessWidget {
  const _ModeChip({required this.label, required this.tinted});

  final String label;
  final bool tinted;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: tinted ? const Color(0xFFFFF1D6) : const Color(0xFFE8F4EB),
        borderRadius: BorderRadius.circular(999),
        border: Border.all(
          color: tinted ? const Color(0xFFE7BE6F) : const Color(0xFF9DCEA8),
        ),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 7),
        child: Text(
          label,
          style: TextStyle(
            fontWeight: FontWeight.w700,
            color: tinted ? const Color(0xFF8A5400) : const Color(0xFF215C2D),
          ),
        ),
      ),
    );
  }
}
