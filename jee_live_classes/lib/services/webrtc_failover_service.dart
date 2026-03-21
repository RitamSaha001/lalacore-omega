import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

enum WebRtcFailoverConnectionState {
  idle,
  connecting,
  connected,
  reconnecting,
  failed,
  stopped,
}

class WebRtcFailoverSnapshot {
  const WebRtcFailoverSnapshot({
    required this.connectionState,
    required this.roomId,
    required this.provider,
    required this.cameraEnabled,
    required this.micEnabled,
    required this.statusMessage,
  });

  final WebRtcFailoverConnectionState connectionState;
  final String? roomId;
  final String provider;
  final bool cameraEnabled;
  final bool micEnabled;
  final String statusMessage;

  bool get isConnected =>
      connectionState == WebRtcFailoverConnectionState.connected;

  WebRtcFailoverSnapshot copyWith({
    WebRtcFailoverConnectionState? connectionState,
    String? roomId,
    bool clearRoomId = false,
    String? provider,
    bool? cameraEnabled,
    bool? micEnabled,
    String? statusMessage,
  }) {
    return WebRtcFailoverSnapshot(
      connectionState: connectionState ?? this.connectionState,
      roomId: clearRoomId ? null : roomId ?? this.roomId,
      provider: provider ?? this.provider,
      cameraEnabled: cameraEnabled ?? this.cameraEnabled,
      micEnabled: micEnabled ?? this.micEnabled,
      statusMessage: statusMessage ?? this.statusMessage,
    );
  }

  static const WebRtcFailoverSnapshot idle = WebRtcFailoverSnapshot(
    connectionState: WebRtcFailoverConnectionState.idle,
    roomId: null,
    provider: 'webrtc',
    cameraEnabled: true,
    micEnabled: true,
    statusMessage: 'Failover inactive',
  );
}

abstract class WebRtcFailoverService {
  ValueListenable<WebRtcFailoverSnapshot> get snapshotListenable;
  ValueListenable<RTCVideoRenderer?> get localRendererListenable;
  ValueListenable<RTCVideoRenderer?> get remoteRendererListenable;
  Stream<String> get logStream;

  Future<void> start({
    required String roomId,
    required String token,
    required String signalingUrl,
    required String userId,
    required bool cameraEnabled,
    required bool micEnabled,
    String provider = 'webrtc',
  });

  Future<void> stop();
  Future<void> setMicEnabled(bool enabled);
  Future<void> setCameraEnabled(bool enabled);
  Future<void> sendData(Map<String, dynamic> payload);
  void dispose();
}

class MockWebRtcFailoverService implements WebRtcFailoverService {
  MockWebRtcFailoverService();

  final ValueNotifier<WebRtcFailoverSnapshot> _snapshot =
      ValueNotifier<WebRtcFailoverSnapshot>(WebRtcFailoverSnapshot.idle);
  final ValueNotifier<RTCVideoRenderer?> _localRenderer =
      ValueNotifier<RTCVideoRenderer?>(null);
  final ValueNotifier<RTCVideoRenderer?> _remoteRenderer =
      ValueNotifier<RTCVideoRenderer?>(null);
  final StreamController<String> _logs = StreamController<String>.broadcast();

  @override
  ValueListenable<WebRtcFailoverSnapshot> get snapshotListenable => _snapshot;

  @override
  ValueListenable<RTCVideoRenderer?> get localRendererListenable =>
      _localRenderer;

  @override
  ValueListenable<RTCVideoRenderer?> get remoteRendererListenable =>
      _remoteRenderer;

  @override
  Stream<String> get logStream => _logs.stream;

  @override
  Future<void> start({
    required String roomId,
    required String token,
    required String signalingUrl,
    required String userId,
    required bool cameraEnabled,
    required bool micEnabled,
    String provider = 'webrtc',
  }) async {
    _snapshot.value = _snapshot.value.copyWith(
      connectionState: WebRtcFailoverConnectionState.connecting,
      roomId: roomId,
      provider: provider,
      cameraEnabled: cameraEnabled,
      micEnabled: micEnabled,
      statusMessage: 'Mock WebRTC failover connecting...',
    );
    await Future<void>.delayed(const Duration(milliseconds: 350));
    _snapshot.value = _snapshot.value.copyWith(
      connectionState: WebRtcFailoverConnectionState.connected,
      statusMessage: 'Mock WebRTC failover connected',
    );
    _logs.add('mock_failover_connected:$roomId');
  }

  @override
  Future<void> stop() async {
    _snapshot.value = WebRtcFailoverSnapshot.idle;
    _logs.add('mock_failover_stopped');
  }

  @override
  Future<void> setMicEnabled(bool enabled) async {
    _snapshot.value = _snapshot.value.copyWith(micEnabled: enabled);
  }

  @override
  Future<void> setCameraEnabled(bool enabled) async {
    _snapshot.value = _snapshot.value.copyWith(cameraEnabled: enabled);
  }

  @override
  Future<void> sendData(Map<String, dynamic> payload) async {
    _logs.add('mock_data:${jsonEncode(payload)}');
  }

  @override
  void dispose() {
    _logs.close();
    _snapshot.dispose();
    _localRenderer.dispose();
    _remoteRenderer.dispose();
  }
}

class RealWebRtcFailoverService implements WebRtcFailoverService {
  RealWebRtcFailoverService();

  final ValueNotifier<WebRtcFailoverSnapshot> _snapshot =
      ValueNotifier<WebRtcFailoverSnapshot>(WebRtcFailoverSnapshot.idle);
  final ValueNotifier<RTCVideoRenderer?> _localRenderer =
      ValueNotifier<RTCVideoRenderer?>(null);
  final ValueNotifier<RTCVideoRenderer?> _remoteRenderer =
      ValueNotifier<RTCVideoRenderer?>(null);
  final StreamController<String> _logs = StreamController<String>.broadcast();

  RTCPeerConnection? _peerConnection;
  MediaStream? _localStream;
  WebSocketChannel? _signalingChannel;
  StreamSubscription<dynamic>? _signalingSubscription;

  String _roomId = '';
  String _userId = '';
  String _token = '';
  String _provider = 'webrtc';

  @override
  ValueListenable<WebRtcFailoverSnapshot> get snapshotListenable => _snapshot;

  @override
  ValueListenable<RTCVideoRenderer?> get localRendererListenable =>
      _localRenderer;

  @override
  ValueListenable<RTCVideoRenderer?> get remoteRendererListenable =>
      _remoteRenderer;

  @override
  Stream<String> get logStream => _logs.stream;

  @override
  Future<void> start({
    required String roomId,
    required String token,
    required String signalingUrl,
    required String userId,
    required bool cameraEnabled,
    required bool micEnabled,
    String provider = 'webrtc',
  }) async {
    await stop();
    _roomId = roomId;
    _userId = userId;
    _token = token;
    _provider = provider;

    _snapshot.value = _snapshot.value.copyWith(
      connectionState: WebRtcFailoverConnectionState.connecting,
      roomId: roomId,
      provider: provider,
      cameraEnabled: cameraEnabled,
      micEnabled: micEnabled,
      statusMessage: 'WebRTC failover connecting...',
    );

    try {
      final localRenderer = RTCVideoRenderer();
      await localRenderer.initialize();
      _localRenderer.value = localRenderer;

      final remoteRenderer = RTCVideoRenderer();
      await remoteRenderer.initialize();
      _remoteRenderer.value = remoteRenderer;

      _localStream = await navigator.mediaDevices.getUserMedia({
        'audio': true,
        'video': cameraEnabled
            ? <String, dynamic>{'facingMode': 'user'}
            : false,
      });

      await setMicEnabled(micEnabled);
      await setCameraEnabled(cameraEnabled);

      localRenderer.srcObject = _localStream;

      _peerConnection = await createPeerConnection({
        'sdpSemantics': 'unified-plan',
        'iceServers': const [
          {
            'urls': ['stun:stun.l.google.com:19302'],
          },
        ],
      });

      for (final track in _localStream!.getTracks()) {
        await _peerConnection!.addTrack(track, _localStream!);
      }

      _peerConnection!.onIceCandidate = (candidate) {
        if (candidate.candidate == null) {
          return;
        }
        _sendSignaling({
          'type': 'candidate',
          'room': _roomId,
          'user_id': _userId,
          'candidate': candidate.toMap(),
        });
      };

      _peerConnection!.onTrack = (event) {
        if (event.streams.isNotEmpty && _remoteRenderer.value != null) {
          _remoteRenderer.value!.srcObject = event.streams.first;
        }
      };

      _peerConnection!.onConnectionState = (state) {
        switch (state) {
          case RTCPeerConnectionState.RTCPeerConnectionStateConnected:
            _snapshot.value = _snapshot.value.copyWith(
              connectionState: WebRtcFailoverConnectionState.connected,
              statusMessage: 'WebRTC failover connected',
            );
            break;
          case RTCPeerConnectionState.RTCPeerConnectionStateConnecting:
            _snapshot.value = _snapshot.value.copyWith(
              connectionState: WebRtcFailoverConnectionState.reconnecting,
              statusMessage: 'WebRTC failover connecting...',
            );
            break;
          case RTCPeerConnectionState.RTCPeerConnectionStateFailed:
          case RTCPeerConnectionState.RTCPeerConnectionStateDisconnected:
          case RTCPeerConnectionState.RTCPeerConnectionStateClosed:
            _snapshot.value = _snapshot.value.copyWith(
              connectionState: WebRtcFailoverConnectionState.failed,
              statusMessage: 'WebRTC failover disconnected',
            );
            break;
          default:
            break;
        }
      };

      final signalingUri = _buildSignalingUri(signalingUrl);
      _signalingChannel = WebSocketChannel.connect(signalingUri);
      _signalingSubscription = _signalingChannel!.stream.listen(
        _handleSignalingMessage,
        onError: (Object error, StackTrace stackTrace) {
          _snapshot.value = _snapshot.value.copyWith(
            connectionState: WebRtcFailoverConnectionState.failed,
            statusMessage: 'Signaling error: $error',
          );
        },
        onDone: () {
          if (_snapshot.value.connectionState !=
              WebRtcFailoverConnectionState.stopped) {
            _snapshot.value = _snapshot.value.copyWith(
              connectionState: WebRtcFailoverConnectionState.failed,
              statusMessage: 'Signaling channel closed',
            );
          }
        },
      );

      _sendSignaling({
        'type': 'join',
        'room': _roomId,
        'user_id': _userId,
        'token': _token,
        'provider': _provider,
      });

      Future<void>.delayed(const Duration(milliseconds: 500), () {
        if (_snapshot.value.connectionState ==
            WebRtcFailoverConnectionState.connecting) {
          unawaited(_createAndSendOffer());
        }
      });
    } catch (error) {
      _snapshot.value = _snapshot.value.copyWith(
        connectionState: WebRtcFailoverConnectionState.failed,
        statusMessage: 'WebRTC failover failed: $error',
      );
      rethrow;
    }
  }

  @override
  Future<void> stop() async {
    await _signalingSubscription?.cancel();
    _signalingSubscription = null;

    await _signalingChannel?.sink.close();
    _signalingChannel = null;

    final local = _localStream;
    if (local != null) {
      for (final track in local.getTracks()) {
        await track.stop();
      }
      await local.dispose();
      _localStream = null;
    }

    final peer = _peerConnection;
    if (peer != null) {
      await peer.close();
      _peerConnection = null;
    }

    if (_localRenderer.value != null) {
      await _localRenderer.value!.dispose();
      _localRenderer.value = null;
    }
    if (_remoteRenderer.value != null) {
      await _remoteRenderer.value!.dispose();
      _remoteRenderer.value = null;
    }

    _snapshot.value = _snapshot.value.copyWith(
      connectionState: WebRtcFailoverConnectionState.stopped,
      clearRoomId: true,
      statusMessage: 'WebRTC failover stopped',
    );
  }

  @override
  Future<void> setMicEnabled(bool enabled) async {
    _snapshot.value = _snapshot.value.copyWith(micEnabled: enabled);
    final stream = _localStream;
    if (stream == null) {
      return;
    }
    for (final track in stream.getAudioTracks()) {
      track.enabled = enabled;
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
  Future<void> sendData(Map<String, dynamic> payload) async {
    _sendSignaling({
      'type': 'data',
      'room': _roomId,
      'user_id': _userId,
      'payload': payload,
    });
  }

  void _handleSignalingMessage(dynamic raw) {
    if (raw is! String) {
      return;
    }
    final map = _decodeMap(raw);
    if (map.isEmpty) {
      return;
    }
    final type = map['type']?.toString() ?? '';
    switch (type) {
      case 'ready':
      case 'peer_joined':
        unawaited(_createAndSendOffer());
        break;
      case 'offer':
        unawaited(_handleRemoteOffer(map));
        break;
      case 'answer':
        unawaited(_handleRemoteAnswer(map));
        break;
      case 'candidate':
        unawaited(_handleRemoteCandidate(map));
        break;
      case 'ping':
        _sendSignaling({
          'type': 'pong',
          'room': _roomId,
          'user_id': _userId,
          'at': DateTime.now().toUtc().toIso8601String(),
        });
        break;
      default:
        break;
    }
  }

  Future<void> _createAndSendOffer() async {
    final connection = _peerConnection;
    if (connection == null) {
      return;
    }
    final offer = await connection.createOffer();
    await connection.setLocalDescription(offer);
    _sendSignaling({
      'type': 'offer',
      'room': _roomId,
      'user_id': _userId,
      'sdp': offer.sdp,
      'sdp_type': offer.type,
    });
  }

  Future<void> _handleRemoteOffer(Map<String, dynamic> map) async {
    final connection = _peerConnection;
    if (connection == null) {
      return;
    }
    final sdp = map['sdp']?.toString();
    final type = map['sdp_type']?.toString() ?? 'offer';
    if (sdp == null || sdp.isEmpty) {
      return;
    }
    await connection.setRemoteDescription(RTCSessionDescription(sdp, type));
    final answer = await connection.createAnswer();
    await connection.setLocalDescription(answer);
    _sendSignaling({
      'type': 'answer',
      'room': _roomId,
      'user_id': _userId,
      'sdp': answer.sdp,
      'sdp_type': answer.type,
    });
  }

  Future<void> _handleRemoteAnswer(Map<String, dynamic> map) async {
    final connection = _peerConnection;
    if (connection == null) {
      return;
    }
    final sdp = map['sdp']?.toString();
    final type = map['sdp_type']?.toString() ?? 'answer';
    if (sdp == null || sdp.isEmpty) {
      return;
    }
    await connection.setRemoteDescription(RTCSessionDescription(sdp, type));
  }

  Future<void> _handleRemoteCandidate(Map<String, dynamic> map) async {
    final connection = _peerConnection;
    if (connection == null) {
      return;
    }
    final candidate = map['candidate'];
    if (candidate is! Map) {
      return;
    }
    final rtcCandidate = RTCIceCandidate(
      candidate['candidate']?.toString(),
      candidate['sdpMid']?.toString(),
      (candidate['sdpMLineIndex'] as num?)?.toInt(),
    );
    await connection.addCandidate(rtcCandidate);
  }

  void _sendSignaling(Map<String, dynamic> payload) {
    final channel = _signalingChannel;
    if (channel == null) {
      return;
    }
    channel.sink.add(jsonEncode(payload));
  }

  Uri _buildSignalingUri(String raw) {
    final trimmed = raw.trim();
    if (trimmed.startsWith('ws://') || trimmed.startsWith('wss://')) {
      final base = Uri.parse(trimmed);
      return base.replace(
        queryParameters: {
          ...base.queryParameters,
          'room': _roomId,
          'user_id': _userId,
          'token': _token,
        },
      );
    }
    if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
      final base = Uri.parse(trimmed);
      return base.replace(
        scheme: base.scheme == 'https' ? 'wss' : 'ws',
        queryParameters: {
          ...base.queryParameters,
          'room': _roomId,
          'user_id': _userId,
          'token': _token,
        },
      );
    }
    return Uri.parse('wss://$trimmed').replace(
      queryParameters: {'room': _roomId, 'user_id': _userId, 'token': _token},
    );
  }

  Map<String, dynamic> _decodeMap(String raw) {
    try {
      final decoded = jsonDecode(raw);
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
      return const {};
    } catch (_) {
      return const {};
    }
  }

  @override
  void dispose() {
    unawaited(stop());
    _logs.close();
    _snapshot.dispose();
    _localRenderer.dispose();
    _remoteRenderer.dispose();
  }
}
