import 'dart:async';
import 'dart:math';

import 'package:flutter/services.dart';
import 'package:livekit_client/livekit_client.dart' as lk;

import '../models/class_session_model.dart';
import '../models/network_stats_model.dart';
import '../models/participant_model.dart';
import '../models/waiting_room_request_model.dart';
import 'livekit_room_service.dart';

abstract class ZoomService {
  bool get isSimulatedBridge;

  Stream<List<ParticipantModel>> get participantsStream;
  Stream<String?> get activeSpeakerStream;
  Stream<NetworkStatsModel> get networkStatsStream;
  Stream<String> get reactionsStream;
  Stream<String?> get sharedContentStream;
  Stream<List<WaitingRoomRequestModel>> get waitingRoomRequestsStream;
  Stream<RtcConnectionState> get connectionStateStream;
  Stream<bool> get meetingLockStream;

  Future<void> initialize({
    required ClassSessionModel session,
    required String authToken,
  });

  Future<void> initializeZoom();

  Future<void> join();

  Future<void> joinSession({required String sessionId, required String token});

  Future<void> leave();
  Future<void> leaveSession();

  Future<void> toggleMic({
    required String participantId,
    required bool enabled,
  });

  Future<void> toggleCamera({
    required String participantId,
    required bool enabled,
  });

  Future<void> setRaiseHand({
    required String participantId,
    required bool raised,
  });

  Future<void> muteParticipant(String participantId);
  Future<void> removeParticipant(String participantId);
  Future<void> disableParticipantCamera(String participantId);
  Future<void> promoteToCoHost(String participantId);
  Future<void> muteAll();
  Future<void> setChatEnabled(bool enabled);
  Future<void> lockMeeting(bool locked);

  Future<void> pinParticipant(String participantId);
  Future<void> unpinParticipant();

  Future<void> startScreenShare(String source);
  Future<void> stopScreenShare();

  Future<void> startRecording();
  Future<void> stopRecording();

  Future<void> sendReaction(String emoji);

  Future<List<ParticipantModel>> getParticipants();
  Future<void> subscribeVideoStream(String participantId);
  Future<void> unsubscribeVideoStream(String participantId);

  Future<void> joinBreakoutRoom(String roomId);
  Future<void> leaveBreakoutRoom();
  Future<void> broadcastMessageToRooms(String message);

  Future<void> approveWaitingRoomUser(String participantId);
  Future<void> rejectWaitingRoomUser(String participantId);

  void dispose();
}

class MockZoomService implements ZoomService {
  // BEGIN_PHASE2_IMPLEMENTATION
  MockZoomService({required this.currentUserId});

  final String currentUserId;

  final _participantsController =
      StreamController<List<ParticipantModel>>.broadcast();
  final _activeSpeakerController = StreamController<String?>.broadcast();
  final _networkController = StreamController<NetworkStatsModel>.broadcast();
  final _reactionController = StreamController<String>.broadcast();
  final _sharedContentController = StreamController<String?>.broadcast();
  final _waitingRoomController =
      StreamController<List<WaitingRoomRequestModel>>.broadcast();
  final _connectionController =
      StreamController<RtcConnectionState>.broadcast();
  final _meetingLockController = StreamController<bool>.broadcast();

  final Random _random = Random();
  final List<ParticipantModel> _participants = [];
  final List<WaitingRoomRequestModel> _waitingRequests = [];

  Timer? _speakerTimer;
  Timer? _networkTimer;
  Timer? _reactionTimer;

  String? _sessionId;
  String? _token;
  bool _chatEnabled = true;
  bool _connected = false;

  @override
  bool get isSimulatedBridge => true;

  @override
  Stream<List<ParticipantModel>> get participantsStream =>
      _participantsController.stream;

  @override
  Stream<String?> get activeSpeakerStream => _activeSpeakerController.stream;

  @override
  Stream<NetworkStatsModel> get networkStatsStream => _networkController.stream;

  @override
  Stream<String> get reactionsStream => _reactionController.stream;

  @override
  Stream<String?> get sharedContentStream => _sharedContentController.stream;

  @override
  Stream<List<WaitingRoomRequestModel>> get waitingRoomRequestsStream =>
      _waitingRoomController.stream;

  @override
  Stream<RtcConnectionState> get connectionStateStream =>
      _connectionController.stream;

  @override
  Stream<bool> get meetingLockStream => _meetingLockController.stream;

  @override
  Future<void> initialize({
    required ClassSessionModel session,
    required String authToken,
  }) async {
    _sessionId = session.id;
    _token = authToken;
    await initializeZoom();
  }

  @override
  Future<void> initializeZoom() async {
    _participants
      ..clear()
      ..addAll(const [
        ParticipantModel(
          id: 'teacher_01',
          name: 'Dr. A. Sharma',
          role: ParticipantRole.host,
          micEnabled: true,
          cameraEnabled: true,
          handRaised: false,
          networkQuality: NetworkQuality.excellent,
        ),
        ParticipantModel(
          id: 'student_01',
          name: 'Ritam',
          role: ParticipantRole.student,
          micEnabled: true,
          cameraEnabled: true,
          handRaised: false,
          networkQuality: NetworkQuality.good,
        ),
        ParticipantModel(
          id: 'student_02',
          name: 'Ananya',
          role: ParticipantRole.student,
          micEnabled: false,
          cameraEnabled: true,
          handRaised: false,
          networkQuality: NetworkQuality.good,
        ),
        ParticipantModel(
          id: 'student_03',
          name: 'Karthik',
          role: ParticipantRole.student,
          micEnabled: true,
          cameraEnabled: true,
          handRaised: true,
          networkQuality: NetworkQuality.fair,
        ),
        ParticipantModel(
          id: 'student_04',
          name: 'Meera',
          role: ParticipantRole.student,
          micEnabled: false,
          cameraEnabled: false,
          handRaised: false,
          networkQuality: NetworkQuality.excellent,
        ),
      ]);

    _waitingRequests
      ..clear()
      ..addAll([
        WaitingRoomRequestModel(
          participantId: 'student_wait_01',
          name: 'Rahul',
          requestedAt: DateTime.now(),
        ),
      ]);

    _meetingLockController.add(false);
    _connectionController.add(RtcConnectionState.disconnected);
  }

  @override
  Future<void> join() async {
    await joinSession(sessionId: _sessionId ?? '', token: _token ?? '');
  }

  @override
  Future<void> joinSession({
    required String sessionId,
    required String token,
  }) async {
    if (_connected) {
      return;
    }
    _connected = true;
    _sessionId = sessionId;
    _token = token;

    _connectionController.add(RtcConnectionState.connecting);

    _participantsController.add(
      List<ParticipantModel>.unmodifiable(_participants),
    );
    _waitingRoomController.add(
      List<WaitingRoomRequestModel>.unmodifiable(_waitingRequests),
    );
    _sharedContentController.add(null);

    _speakerTimer = Timer.periodic(const Duration(seconds: 2), (_) {
      if (_participants.isEmpty) {
        _activeSpeakerController.add(null);
        return;
      }
      final speaking = _participants
          .where((p) => p.micEnabled)
          .toList(growable: false);
      final active = speaking.isEmpty
          ? _participants[_random.nextInt(_participants.length)]
          : speaking[_random.nextInt(speaking.length)];
      _activeSpeakerController.add(active.id);
    });

    _networkTimer = Timer.periodic(const Duration(seconds: 2), (_) {
      final latency = 26 + _random.nextInt(80);
      final packetLoss = _random.nextDouble() * 0.8;
      final jitter = 4 + _random.nextInt(18);
      final uplink = 1600 + _random.nextInt(2200);
      final downlink = 1900 + _random.nextInt(2400);

      final quality = latency < 45 && packetLoss < 0.2
          ? NetworkQuality.excellent
          : latency < 65 && packetLoss < 0.4
          ? NetworkQuality.good
          : latency < 90 && packetLoss < 0.6
          ? NetworkQuality.fair
          : NetworkQuality.poor;

      _networkController.add(
        NetworkStatsModel(
          latencyMs: latency,
          packetLossPercent: packetLoss,
          jitterMs: jitter,
          uplinkKbps: uplink,
          downlinkKbps: downlink,
          quality: quality,
        ),
      );
    });

    _reactionTimer = Timer.periodic(const Duration(seconds: 7), (_) {
      const reactions = ['👍', '👏', '🔥', '❓'];
      _reactionController.add(reactions[_random.nextInt(reactions.length)]);
    });

    _connectionController.add(RtcConnectionState.connected);
  }

  @override
  Future<void> leave() async {
    await leaveSession();
  }

  @override
  Future<void> leaveSession() async {
    _connected = false;
    _speakerTimer?.cancel();
    _networkTimer?.cancel();
    _reactionTimer?.cancel();
    _activeSpeakerController.add(null);
    _connectionController.add(RtcConnectionState.disconnected);
  }

  @override
  Future<void> toggleMic({
    required String participantId,
    required bool enabled,
  }) async {
    _replaceParticipant(participantId, (p) => p.copyWith(micEnabled: enabled));
  }

  @override
  Future<void> toggleCamera({
    required String participantId,
    required bool enabled,
  }) async {
    _replaceParticipant(
      participantId,
      (p) => p.copyWith(cameraEnabled: enabled),
    );
  }

  @override
  Future<void> setRaiseHand({
    required String participantId,
    required bool raised,
  }) async {
    _replaceParticipant(participantId, (p) => p.copyWith(handRaised: raised));
  }

  @override
  Future<void> muteParticipant(String participantId) async {
    await toggleMic(participantId: participantId, enabled: false);
  }

  @override
  Future<void> removeParticipant(String participantId) async {
    _participants.removeWhere((element) => element.id == participantId);
    _emitParticipants();
  }

  @override
  Future<void> disableParticipantCamera(String participantId) async {
    await toggleCamera(participantId: participantId, enabled: false);
  }

  @override
  Future<void> promoteToCoHost(String participantId) async {
    _replaceParticipant(
      participantId,
      (p) => p.copyWith(role: ParticipantRole.coHost),
    );
  }

  @override
  Future<void> muteAll() async {
    for (final participant in _participants.where((p) => !p.isTeacher)) {
      _replaceParticipant(
        participant.id,
        (p) => p.copyWith(micEnabled: false),
        emit: false,
      );
    }
    _emitParticipants();
  }

  @override
  Future<void> setChatEnabled(bool enabled) async {
    _chatEnabled = enabled;
  }

  @override
  Future<void> lockMeeting(bool locked) async {
    _meetingLockController.add(locked);
  }

  @override
  Future<void> pinParticipant(String participantId) async {
    _activeSpeakerController.add(participantId);
  }

  @override
  Future<void> unpinParticipant() async {
    _activeSpeakerController.add(null);
  }

  @override
  Future<void> startScreenShare(String source) async {
    _sharedContentController.add(source);
    final hostIndex = _participants.indexWhere(
      (p) => p.role == ParticipantRole.host,
    );
    if (hostIndex != -1) {
      _participants[hostIndex] = _participants[hostIndex].copyWith(
        isScreenSharing: true,
      );
      _emitParticipants();
    }
  }

  @override
  Future<void> stopScreenShare() async {
    _sharedContentController.add(null);
    final hostIndex = _participants.indexWhere(
      (p) => p.role == ParticipantRole.host,
    );
    if (hostIndex != -1) {
      _participants[hostIndex] = _participants[hostIndex].copyWith(
        isScreenSharing: false,
      );
      _emitParticipants();
    }
  }

  @override
  Future<void> startRecording() async {}

  @override
  Future<void> stopRecording() async {}

  @override
  Future<void> sendReaction(String emoji) async {
    _reactionController.add(emoji);
  }

  @override
  Future<List<ParticipantModel>> getParticipants() async {
    return List<ParticipantModel>.unmodifiable(_participants);
  }

  @override
  Future<void> subscribeVideoStream(String participantId) async {}

  @override
  Future<void> unsubscribeVideoStream(String participantId) async {}

  @override
  Future<void> joinBreakoutRoom(String roomId) async {
    _sharedContentController.add('Breakout room: $roomId');
  }

  @override
  Future<void> leaveBreakoutRoom() async {
    _sharedContentController.add(null);
  }

  @override
  Future<void> broadcastMessageToRooms(String message) async {
    _reactionController.add('📢');
  }

  @override
  Future<void> approveWaitingRoomUser(String participantId) async {
    final request = _waitingRequests.firstWhere(
      (item) => item.participantId == participantId,
      orElse: () => WaitingRoomRequestModel(
        participantId: '',
        name: '',
        requestedAt: DateTime.now(),
      ),
    );
    if (request.participantId.isEmpty) {
      return;
    }

    _waitingRequests.removeWhere((item) => item.participantId == participantId);
    _waitingRoomController.add(
      List<WaitingRoomRequestModel>.unmodifiable(_waitingRequests),
    );

    _participants.add(
      ParticipantModel(
        id: request.participantId,
        name: request.name,
        role: ParticipantRole.student,
        micEnabled: false,
        cameraEnabled: true,
        handRaised: false,
        networkQuality: NetworkQuality.good,
      ),
    );
    _emitParticipants();
  }

  @override
  Future<void> rejectWaitingRoomUser(String participantId) async {
    _waitingRequests.removeWhere((item) => item.participantId == participantId);
    _waitingRoomController.add(
      List<WaitingRoomRequestModel>.unmodifiable(_waitingRequests),
    );
  }

  bool get chatEnabled => _chatEnabled;

  void _replaceParticipant(
    String id,
    ParticipantModel Function(ParticipantModel participant) transform, {
    bool emit = true,
  }) {
    final index = _participants.indexWhere(
      (participant) => participant.id == id,
    );
    if (index == -1) {
      return;
    }
    _participants[index] = transform(_participants[index]);
    if (emit) {
      _emitParticipants();
    }
  }

  void _emitParticipants() {
    _participantsController.add(
      List<ParticipantModel>.unmodifiable(_participants),
    );
  }

  @override
  void dispose() {
    _speakerTimer?.cancel();
    _networkTimer?.cancel();
    _reactionTimer?.cancel();
    _participantsController.close();
    _activeSpeakerController.close();
    _networkController.close();
    _reactionController.close();
    _sharedContentController.close();
    _waitingRoomController.close();
    _connectionController.close();
    _meetingLockController.close();
  }

  // END_PHASE2_IMPLEMENTATION
}

class RealZoomService implements ZoomService {
  // BEGIN_PHASE2_IMPLEMENTATION
  RealZoomService({required this.currentUserId, String? currentUserName})
    : currentUserName = currentUserName ?? currentUserId;

  static const MethodChannel _channel = MethodChannel(
    'jee_live_classes/zoom_videosdk',
  );

  final String currentUserId;
  final String currentUserName;

  final _participantsController =
      StreamController<List<ParticipantModel>>.broadcast();
  final _activeSpeakerController = StreamController<String?>.broadcast();
  final _networkController = StreamController<NetworkStatsModel>.broadcast();
  final _reactionController = StreamController<String>.broadcast();
  final _sharedContentController = StreamController<String?>.broadcast();
  final _waitingRoomController =
      StreamController<List<WaitingRoomRequestModel>>.broadcast();
  final _connectionController =
      StreamController<RtcConnectionState>.broadcast();
  final _meetingLockController = StreamController<bool>.broadcast();

  List<ParticipantModel> _participants = const [];
  List<WaitingRoomRequestModel> _waitingRequests = const [];
  String? _sessionId;
  String? _token;
  String? _rtcProvider;
  String? _rtcServerUrl;
  bool _isSimulatedBridge = true;
  final LiveKitRoomService _liveKitRoomService = LiveKitRoomService();

  bool get _usesLiveKitTransport =>
      (_rtcProvider ?? '').trim().toLowerCase() == 'livekit' &&
      (_rtcServerUrl ?? '').trim().isNotEmpty;

  bool get usesLiveKitMediaPlane => _usesLiveKitTransport;

  lk.VideoTrack? participantVideoTrack(String participantId) =>
      _usesLiveKitTransport
      ? _liveKitRoomService.participantVideoTrack(participantId)
      : null;

  lk.VideoTrack? participantScreenShareTrack(String participantId) =>
      _usesLiveKitTransport
      ? _liveKitRoomService.participantScreenShareTrack(participantId)
      : null;

  lk.VideoTrack? activeScreenShareTrack() => _usesLiveKitTransport
      ? _liveKitRoomService.activeScreenShareTrack()
      : null;

  @override
  bool get isSimulatedBridge => _isSimulatedBridge;

  @override
  Stream<List<ParticipantModel>> get participantsStream => _usesLiveKitTransport
      ? _liveKitRoomService.participantsStream
      : _participantsController.stream;

  @override
  Stream<String?> get activeSpeakerStream => _usesLiveKitTransport
      ? _liveKitRoomService.activeSpeakerStream
      : _activeSpeakerController.stream;

  @override
  Stream<NetworkStatsModel> get networkStatsStream => _usesLiveKitTransport
      ? _liveKitRoomService.networkStatsStream
      : _networkController.stream;

  @override
  Stream<String> get reactionsStream => _usesLiveKitTransport
      ? _liveKitRoomService.reactionsStream
      : _reactionController.stream;

  @override
  Stream<String?> get sharedContentStream => _usesLiveKitTransport
      ? _liveKitRoomService.sharedContentStream
      : _sharedContentController.stream;

  @override
  Stream<List<WaitingRoomRequestModel>> get waitingRoomRequestsStream =>
      _waitingRoomController.stream;

  @override
  Stream<RtcConnectionState> get connectionStateStream => _usesLiveKitTransport
      ? _liveKitRoomService.connectionStateStream
      : _connectionController.stream;

  @override
  Stream<bool> get meetingLockStream => _usesLiveKitTransport
      ? _liveKitRoomService.meetingLockStream
      : _meetingLockController.stream;

  @override
  Future<void> initialize({
    required ClassSessionModel session,
    required String authToken,
  }) async {
    _sessionId = session.id;
    _token = authToken;
    _rtcProvider = session.rtcProvider;
    _rtcServerUrl = session.rtcServerUrl;
    await initializeZoom();
  }

  @override
  Future<void> initializeZoom() async {
    if (_usesLiveKitTransport) {
      _isSimulatedBridge = false;
      return;
    }
    _channel.setMethodCallHandler(_handleCallback);
    try {
      final status = await _channel.invokeMapMethod<String, dynamic>(
        'bridgeStatus',
      );
      final implementation = status?['implementation']?.toString() ?? '';
      _isSimulatedBridge = implementation != 'vendor_sdk';
    } catch (_) {
      _isSimulatedBridge = true;
    }
    await _channel.invokeMethod<void>('initializeZoom');
  }

  @override
  Future<void> join() async {
    await joinSession(sessionId: _sessionId ?? '', token: _token ?? '');
  }

  @override
  Future<void> joinSession({
    required String sessionId,
    required String token,
  }) async {
    if (_usesLiveKitTransport) {
      _connectionController.add(RtcConnectionState.connecting);
      await _liveKitRoomService.connect(
        serverUrl: _rtcServerUrl!,
        token: token,
        currentUserId: currentUserId,
        currentUserName: currentUserName,
        cameraEnabled: true,
        micEnabled: true,
      );
      return;
    }
    _connectionController.add(RtcConnectionState.connecting);
    await _channel.invokeMethod<void>('joinSession', {
      'sessionId': sessionId,
      'token': token,
      'displayName': currentUserId,
    });
    _connectionController.add(RtcConnectionState.connected);
  }

  @override
  Future<void> leave() async {
    await leaveSession();
  }

  @override
  Future<void> leaveSession() async {
    if (_usesLiveKitTransport) {
      await _liveKitRoomService.disconnect();
      return;
    }
    await _channel.invokeMethod<void>('leaveSession');
    _connectionController.add(RtcConnectionState.disconnected);
  }

  @override
  Future<void> toggleMic({
    required String participantId,
    required bool enabled,
  }) {
    if (_usesLiveKitTransport) {
      if (participantId == currentUserId) {
        return _liveKitRoomService.setMicrophoneEnabled(enabled);
      }
      return _liveKitRoomService.setParticipantMuted(participantId, !enabled);
    }
    return _channel.invokeMethod<void>('toggleMic', {
      'participantId': participantId,
      'enabled': enabled,
    });
  }

  @override
  Future<void> toggleCamera({
    required String participantId,
    required bool enabled,
  }) {
    if (_usesLiveKitTransport && participantId == currentUserId) {
      return _liveKitRoomService.setCameraEnabled(enabled);
    }
    return _channel.invokeMethod<void>('toggleCamera', {
      'participantId': participantId,
      'enabled': enabled,
    });
  }

  @override
  Future<void> setRaiseHand({
    required String participantId,
    required bool raised,
  }) {
    return _channel.invokeMethod<void>('setRaiseHand', {
      'participantId': participantId,
      'raised': raised,
    });
  }

  @override
  Future<void> muteParticipant(String participantId) {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.setParticipantMuted(participantId, true);
    }
    return _channel.invokeMethod<void>('muteParticipant', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> removeParticipant(String participantId) {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.removeParticipantLocally(participantId);
    }
    return _channel.invokeMethod<void>('removeParticipant', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> disableParticipantCamera(String participantId) {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.setParticipantCameraForcedOff(
        participantId,
        true,
      );
    }
    return _channel.invokeMethod<void>('disableParticipantCamera', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> promoteToCoHost(String participantId) {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.setParticipantPromotedToCoHost(
        participantId,
        true,
      );
    }
    return _channel.invokeMethod<void>('promoteCoHost', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> muteAll() {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.setMuteAllParticipants(true);
    }
    return _channel.invokeMethod<void>('muteAll');
  }

  @override
  Future<void> setChatEnabled(bool enabled) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('setChatEnabled', {'enabled': enabled});
  }

  @override
  Future<void> lockMeeting(bool locked) async {
    if (_usesLiveKitTransport) {
      _meetingLockController.add(locked);
      return;
    }
    await _channel.invokeMethod<void>('lockMeeting', {'locked': locked});
    _meetingLockController.add(locked);
  }

  @override
  Future<void> pinParticipant(String participantId) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('pinParticipant', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> unpinParticipant() {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('unpinParticipant');
  }

  @override
  Future<void> startScreenShare(String source) {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.setScreenShareEnabled(true);
    }
    return _channel.invokeMethod<void>('startScreenShare', {'source': source});
  }

  @override
  Future<void> stopScreenShare() {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.setScreenShareEnabled(false);
    }
    return _channel.invokeMethod<void>('stopScreenShare');
  }

  @override
  Future<void> startRecording() {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('startRecording');
  }

  @override
  Future<void> stopRecording() {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('stopRecording');
  }

  @override
  Future<void> sendReaction(String emoji) {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.sendReaction(emoji);
    }
    return _channel.invokeMethod<void>('sendReaction', {'emoji': emoji});
  }

  @override
  Future<List<ParticipantModel>> getParticipants() async {
    if (_usesLiveKitTransport) {
      return _liveKitRoomService.currentParticipants;
    }
    return _participants;
  }

  @override
  Future<void> subscribeVideoStream(String participantId) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('subscribeVideoStream', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> unsubscribeVideoStream(String participantId) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('unsubscribeVideoStream', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> joinBreakoutRoom(String roomId) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('joinBreakoutRoom', {'roomId': roomId});
  }

  @override
  Future<void> leaveBreakoutRoom() {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('leaveBreakoutRoom');
  }

  @override
  Future<void> broadcastMessageToRooms(String message) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('broadcastMessageToRooms', {
      'message': message,
    });
  }

  @override
  Future<void> approveWaitingRoomUser(String participantId) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('approveWaitingUser', {
      'participantId': participantId,
    });
  }

  @override
  Future<void> rejectWaitingRoomUser(String participantId) {
    if (_usesLiveKitTransport) {
      return Future<void>.value();
    }
    return _channel.invokeMethod<void>('rejectWaitingUser', {
      'participantId': participantId,
    });
  }

  Future<void> _handleCallback(MethodCall call) async {
    final args = call.arguments;
    switch (call.method) {
      case 'onUserJoin':
      case 'onUserLeave':
      case 'onUserVideoStatusChanged':
      case 'onUserAudioStatusChanged':
      case 'onParticipantsUpdated':
        _participants = _parseParticipants(args);
        _participantsController.add(_participants);
        break;
      case 'onActiveSpeakerChanged':
        final speakerId = (args as Map?)?['participantId']?.toString();
        _activeSpeakerController.add(speakerId);
        break;
      case 'onNetworkQualityChanged':
        final map = (args as Map?) ?? const {};
        _networkController.add(
          NetworkStatsModel(
            latencyMs: (map['latencyMs'] as num?)?.toInt() ?? 0,
            packetLossPercent: (map['packetLoss'] as num?)?.toDouble() ?? 0,
            jitterMs: (map['jitterMs'] as num?)?.toInt() ?? 0,
            uplinkKbps: (map['uplinkKbps'] as num?)?.toInt() ?? 0,
            downlinkKbps: (map['downlinkKbps'] as num?)?.toInt() ?? 0,
            quality: _qualityFromSdk((map['quality'] as num?)?.toInt() ?? 2),
          ),
        );
        break;
      case 'onScreenShareStatusChanged':
        final map = args is Map ? args : const {};
        final source = map['source']?.toString();
        final active = map['active'] == true;
        _sharedContentController.add(active ? source : null);
        break;
      case 'onReaction':
        final emoji = (args as Map?)?['emoji']?.toString();
        if (emoji != null && emoji.isNotEmpty) {
          _reactionController.add(emoji);
        }
        break;
      case 'onWaitingRoomUpdated':
        _waitingRequests = _parseWaitingRequests(args);
        _waitingRoomController.add(_waitingRequests);
        break;
      case 'onMeetingLocked':
        _meetingLockController.add((args as Map?)?['locked'] == true);
        break;
      case 'onReconnecting':
        _connectionController.add(RtcConnectionState.reconnecting);
        break;
      case 'onReconnected':
        _connectionController.add(RtcConnectionState.connected);
        break;
      case 'onConnectionFailed':
        _connectionController.add(RtcConnectionState.failed);
        break;
      default:
        return;
    }
  }

  List<ParticipantModel> _parseParticipants(dynamic args) {
    if (args is! List) {
      return const [];
    }
    return args
        .whereType<Map>()
        .map((item) {
          final role = (item['role'] ?? 'student').toString();
          return ParticipantModel(
            id: (item['id'] ?? '').toString(),
            name: (item['name'] ?? 'Participant').toString(),
            role: role == 'host'
                ? ParticipantRole.host
                : role == 'cohost'
                ? ParticipantRole.coHost
                : ParticipantRole.student,
            micEnabled: item['micEnabled'] == true,
            cameraEnabled: item['cameraEnabled'] == true,
            handRaised: item['handRaised'] == true,
            isScreenSharing: item['isScreenSharing'] == true,
            networkQuality: _qualityFromSdk(
              (item['networkQuality'] as num?)?.toInt() ?? 2,
            ),
          );
        })
        .toList(growable: false);
  }

  List<WaitingRoomRequestModel> _parseWaitingRequests(dynamic args) {
    if (args is! List) {
      return const [];
    }
    return args
        .whereType<Map>()
        .map((item) {
          return WaitingRoomRequestModel(
            participantId: (item['participantId'] ?? '').toString(),
            name: (item['name'] ?? 'Student').toString(),
            requestedAt:
                DateTime.tryParse((item['requestedAt'] ?? '').toString()) ??
                DateTime.now(),
          );
        })
        .toList(growable: false);
  }

  NetworkQuality _qualityFromSdk(int value) {
    if (value <= 0) {
      return NetworkQuality.poor;
    }
    if (value == 1) {
      return NetworkQuality.fair;
    }
    if (value == 2) {
      return NetworkQuality.good;
    }
    return NetworkQuality.excellent;
  }

  @override
  void dispose() {
    _liveKitRoomService.dispose();
    _participantsController.close();
    _activeSpeakerController.close();
    _networkController.close();
    _reactionController.close();
    _sharedContentController.close();
    _waitingRoomController.close();
    _connectionController.close();
    _meetingLockController.close();
  }

  // END_PHASE2_IMPLEMENTATION
}
