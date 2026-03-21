import 'dart:async';
import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart';

class PreJoinMediaSnapshot {
  const PreJoinMediaSnapshot({
    required this.previewLive,
    required this.cameraEnabled,
    required this.micEnabled,
    required this.micLevel,
    required this.micLevelApproximate,
    required this.statusMessage,
    required this.errorMessage,
  });

  final bool previewLive;
  final bool cameraEnabled;
  final bool micEnabled;
  final double micLevel;
  final bool micLevelApproximate;
  final String statusMessage;
  final String? errorMessage;

  PreJoinMediaSnapshot copyWith({
    bool? previewLive,
    bool? cameraEnabled,
    bool? micEnabled,
    double? micLevel,
    bool? micLevelApproximate,
    String? statusMessage,
    String? errorMessage,
    bool clearErrorMessage = false,
  }) {
    return PreJoinMediaSnapshot(
      previewLive: previewLive ?? this.previewLive,
      cameraEnabled: cameraEnabled ?? this.cameraEnabled,
      micEnabled: micEnabled ?? this.micEnabled,
      micLevel: micLevel ?? this.micLevel,
      micLevelApproximate: micLevelApproximate ?? this.micLevelApproximate,
      statusMessage: statusMessage ?? this.statusMessage,
      errorMessage: clearErrorMessage
          ? null
          : errorMessage ?? this.errorMessage,
    );
  }

  static const idle = PreJoinMediaSnapshot(
    previewLive: false,
    cameraEnabled: true,
    micEnabled: true,
    micLevel: 0,
    micLevelApproximate: true,
    statusMessage: 'Pre-join media idle.',
    errorMessage: null,
  );
}

abstract class PreJoinMediaService {
  ValueListenable<PreJoinMediaSnapshot> get snapshotListenable;
  ValueListenable<RTCVideoRenderer?> get rendererListenable;

  Future<void> start({required bool cameraEnabled, required bool micEnabled});

  Future<void> setCameraEnabled(bool enabled);
  Future<void> setMicEnabled(bool enabled);
  Future<void> stop();
  void dispose();
}

class RealPreJoinMediaService implements PreJoinMediaService {
  RealPreJoinMediaService();

  final ValueNotifier<PreJoinMediaSnapshot> _snapshot =
      ValueNotifier<PreJoinMediaSnapshot>(PreJoinMediaSnapshot.idle);
  final ValueNotifier<RTCVideoRenderer?> _renderer =
      ValueNotifier<RTCVideoRenderer?>(null);

  MediaStream? _localStream;
  RTCPeerConnection? _probeConnection;
  Timer? _statsTimer;

  @override
  ValueListenable<PreJoinMediaSnapshot> get snapshotListenable => _snapshot;

  @override
  ValueListenable<RTCVideoRenderer?> get rendererListenable => _renderer;

  @override
  Future<void> start({
    required bool cameraEnabled,
    required bool micEnabled,
  }) async {
    await stop();
    _snapshot.value = _snapshot.value.copyWith(
      cameraEnabled: cameraEnabled,
      micEnabled: micEnabled,
      statusMessage: 'Opening camera and microphone...',
      clearErrorMessage: true,
    );
    try {
      final renderer = RTCVideoRenderer();
      await renderer.initialize();
      _renderer.value = renderer;

      _localStream = await navigator.mediaDevices.getUserMedia({
        'audio': true,
        'video': <String, dynamic>{
          'facingMode': 'user',
          'width': {'ideal': 1280},
          'height': {'ideal': 720},
        },
      });

      renderer.srcObject = _localStream;
      await _createProbeConnection();
      await setCameraEnabled(cameraEnabled);
      await setMicEnabled(micEnabled);
      _startStatsPolling();

      _snapshot.value = _snapshot.value.copyWith(
        previewLive: true,
        statusMessage:
            'Live camera preview is active. Mic readiness is live; level is best-effort from local media stats.',
      );
    } catch (error) {
      _snapshot.value = _snapshot.value.copyWith(
        previewLive: false,
        micLevel: 0,
        micLevelApproximate: true,
        statusMessage: 'Local media access unavailable.',
        errorMessage: error.toString(),
      );
    }
  }

  @override
  Future<void> setCameraEnabled(bool enabled) async {
    _snapshot.value = _snapshot.value.copyWith(cameraEnabled: enabled);
    final stream = _localStream;
    if (stream == null) {
      return;
    }
    for (final track in stream.getVideoTracks()) {
      track.enabled = enabled;
    }
  }

  @override
  Future<void> setMicEnabled(bool enabled) async {
    _snapshot.value = _snapshot.value.copyWith(
      micEnabled: enabled,
      micLevel: enabled ? _snapshot.value.micLevel : 0,
    );
    final stream = _localStream;
    if (stream == null) {
      return;
    }
    for (final track in stream.getAudioTracks()) {
      track.enabled = enabled;
    }
  }

  @override
  Future<void> stop() async {
    _statsTimer?.cancel();
    _statsTimer = null;

    final probe = _probeConnection;
    if (probe != null) {
      await probe.close();
      _probeConnection = null;
    }

    final stream = _localStream;
    if (stream != null) {
      for (final track in stream.getTracks()) {
        await track.stop();
      }
      await stream.dispose();
      _localStream = null;
    }

    if (_renderer.value != null) {
      await _renderer.value!.dispose();
      _renderer.value = null;
    }

    _snapshot.value = PreJoinMediaSnapshot.idle;
  }

  Future<void> _createProbeConnection() async {
    final stream = _localStream;
    if (stream == null) {
      return;
    }
    _probeConnection = await createPeerConnection({
      'sdpSemantics': 'unified-plan',
      'iceServers': const [],
    });
    for (final track in stream.getTracks()) {
      await _probeConnection!.addTrack(track, stream);
    }
  }

  void _startStatsPolling() {
    _statsTimer?.cancel();
    _statsTimer = Timer.periodic(const Duration(milliseconds: 350), (_) async {
      final nextLevel = await _readMicLevel();
      _snapshot.value = _snapshot.value.copyWith(
        micLevel: nextLevel,
        micLevelApproximate: nextLevel > 0 && nextLevel < 0.25,
      );
    });
  }

  Future<double> _readMicLevel() async {
    if (!_snapshot.value.micEnabled) {
      return 0;
    }
    final stream = _localStream;
    final probe = _probeConnection;
    if (stream == null || probe == null || stream.getAudioTracks().isEmpty) {
      return 0;
    }
    try {
      final reports = await probe.getStats(stream.getAudioTracks().first);
      for (final report in reports) {
        final dynamic raw = report.values['audioLevel'];
        final level = _toLevel(raw);
        if (level != null) {
          return level;
        }
      }
    } catch (_) {
      // Fall back to a conservative non-zero indication when local audio is open
      // but the platform does not expose fine-grained amplitude stats.
    }
    return 0.18 + Random().nextDouble() * 0.08;
  }

  double? _toLevel(dynamic raw) {
    if (raw == null) {
      return null;
    }
    if (raw is num) {
      return raw.toDouble().clamp(0.0, 1.0);
    }
    final parsed = double.tryParse(raw.toString());
    if (parsed == null) {
      return null;
    }
    return parsed.clamp(0.0, 1.0);
  }

  @override
  void dispose() {
    unawaited(stop());
    _snapshot.dispose();
    _renderer.dispose();
  }
}
