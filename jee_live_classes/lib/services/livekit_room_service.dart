import 'dart:async';
import 'dart:convert';

import 'package:livekit_client/livekit_client.dart' as lk;

import '../models/network_stats_model.dart';
import '../models/participant_model.dart';
import '../models/waiting_room_request_model.dart';

class LiveKitRoomService {
  LiveKitRoomService();

  final _participantsController =
      StreamController<List<ParticipantModel>>.broadcast();
  final _activeSpeakerController = StreamController<String?>.broadcast();
  final _networkController = StreamController<NetworkStatsModel>.broadcast();
  final _reactionController = StreamController<String>.broadcast();
  final _sharedContentController = StreamController<String?>.broadcast();
  final _connectionController =
      StreamController<RtcConnectionState>.broadcast();
  final _meetingLockController = StreamController<bool>.broadcast();

  lk.Room? _room;
  lk.EventsListener<lk.RoomEvent>? _listener;
  String _currentUserId = '';
  String _currentUserName = '';
  bool _cameraEnabled = true;
  bool _micEnabled = true;
  List<ParticipantModel> _participants = const [];
  final Set<String> _removedParticipantIds = <String>{};
  final Set<String> _forcedMutedParticipantIds = <String>{};
  final Set<String> _explicitlyUnmutedParticipantIds = <String>{};
  final Set<String> _forcedCameraOffParticipantIds = <String>{};
  final Set<String> _promotedCoHostParticipantIds = <String>{};
  bool _muteAllParticipants = false;

  Stream<List<ParticipantModel>> get participantsStream =>
      _participantsController.stream;
  Stream<String?> get activeSpeakerStream => _activeSpeakerController.stream;
  Stream<NetworkStatsModel> get networkStatsStream => _networkController.stream;
  Stream<String> get reactionsStream => _reactionController.stream;
  Stream<String?> get sharedContentStream => _sharedContentController.stream;
  Stream<RtcConnectionState> get connectionStateStream =>
      _connectionController.stream;
  Stream<bool> get meetingLockStream => _meetingLockController.stream;

  List<ParticipantModel> get currentParticipants => _participants;
  lk.Room? get room => _room;

  lk.VideoTrack? participantVideoTrack(String participantId) =>
      _isParticipantRemoved(participantId) ||
          _forcedCameraOffParticipantIds.contains(participantId)
      ? null
      : _participantTrack(participantId, source: lk.TrackSource.camera);

  lk.VideoTrack? participantScreenShareTrack(String participantId) =>
      _isParticipantRemoved(participantId)
      ? null
      : _participantTrack(
          participantId,
          source: lk.TrackSource.screenShareVideo,
        );

  lk.VideoTrack? activeScreenShareTrack() {
    final room = _room;
    if (room == null) {
      return null;
    }
    final local = room.localParticipant;
    if (local != null) {
      final track = _videoTrackForParticipant(
        local,
        source: lk.TrackSource.screenShareVideo,
      );
      if (track != null && !_isParticipantRemoved(_identityForParticipant(local))) {
        return track;
      }
    }
    for (final participant in room.remoteParticipants.values) {
      final track = _videoTrackForParticipant(
        participant,
        source: lk.TrackSource.screenShareVideo,
      );
      if (track != null &&
          !_isParticipantRemoved(_identityForParticipant(participant))) {
        return track;
      }
    }
    return null;
  }

  Future<void> setParticipantMuted(String participantId, bool muted) async {
    if (muted) {
      _forcedMutedParticipantIds.add(participantId);
      _explicitlyUnmutedParticipantIds.remove(participantId);
    } else {
      _forcedMutedParticipantIds.remove(participantId);
      _explicitlyUnmutedParticipantIds.add(participantId);
    }
    _emitRoomDerivedState();
  }

  Future<void> removeParticipantLocally(String participantId) async {
    _removedParticipantIds.add(participantId);
    _forcedMutedParticipantIds.remove(participantId);
    _explicitlyUnmutedParticipantIds.remove(participantId);
    _forcedCameraOffParticipantIds.remove(participantId);
    _promotedCoHostParticipantIds.remove(participantId);
    _emitRoomDerivedState();
  }

  Future<void> setParticipantCameraForcedOff(
    String participantId,
    bool disabled,
  ) async {
    if (disabled) {
      _forcedCameraOffParticipantIds.add(participantId);
    } else {
      _forcedCameraOffParticipantIds.remove(participantId);
    }
    _emitRoomDerivedState();
  }

  Future<void> setParticipantPromotedToCoHost(
    String participantId,
    bool enabled,
  ) async {
    if (enabled) {
      _promotedCoHostParticipantIds.add(participantId);
    } else {
      _promotedCoHostParticipantIds.remove(participantId);
    }
    _emitRoomDerivedState();
  }

  Future<void> setMuteAllParticipants(bool enabled) async {
    _muteAllParticipants = enabled;
    if (!enabled) {
      _explicitlyUnmutedParticipantIds.clear();
    }
    _emitRoomDerivedState();
  }

  Future<void> connect({
    required String serverUrl,
    required String token,
    required String currentUserId,
    required String currentUserName,
    required bool cameraEnabled,
    required bool micEnabled,
  }) async {
    await disconnect();
    _currentUserId = currentUserId;
    _currentUserName = currentUserName;
    _cameraEnabled = cameraEnabled;
    _micEnabled = micEnabled;

    final room = lk.Room(
      roomOptions: const lk.RoomOptions(adaptiveStream: true, dynacast: true),
    );
    _room = room;
    room.addListener(_emitRoomDerivedState);
    _listener = room.createListener()
      ..on<lk.RoomConnectedEvent>((_) async {
        _connectionController.add(RtcConnectionState.connected);
        await _applyLocalMediaState();
        _emitRoomDerivedState();
      })
      ..on<lk.RoomReconnectingEvent>((_) {
        _connectionController.add(RtcConnectionState.reconnecting);
      })
      ..on<lk.RoomDisconnectedEvent>((_) {
        _connectionController.add(RtcConnectionState.disconnected);
        _emitRoomDerivedState();
      })
      ..on<lk.ParticipantConnectedEvent>((_) => _emitRoomDerivedState())
      ..on<lk.ParticipantDisconnectedEvent>((_) => _emitRoomDerivedState())
      ..on<lk.ActiveSpeakersChangedEvent>((event) {
        final speaker = event.speakers.isEmpty ? null : event.speakers.first;
        _activeSpeakerController.add(speaker?.identity);
        _emitRoomDerivedState();
      })
      ..on<lk.TrackPublishedEvent>((_) => _emitRoomDerivedState())
      ..on<lk.TrackUnpublishedEvent>((_) => _emitRoomDerivedState())
      ..on<lk.TrackMutedEvent>((_) => _emitRoomDerivedState())
      ..on<lk.TrackUnmutedEvent>((_) => _emitRoomDerivedState())
      ..on<lk.ParticipantConnectionQualityUpdatedEvent>((_) {
        _emitRoomDerivedState();
      })
      ..on<lk.DataReceivedEvent>((event) {
        final decoded = _decodeDataPayload(event.data);
        if (decoded['type']?.toString() == 'reaction') {
          final emoji = decoded['emoji']?.toString() ?? '';
          if (emoji.isNotEmpty) {
            _reactionController.add(emoji);
          }
        }
      });

    _connectionController.add(RtcConnectionState.connecting);
    await room.prepareConnection(serverUrl, token);
    await room.connect(serverUrl, token);
  }

  Future<void> disconnect() async {
    final room = _room;
    if (room != null) {
      room.removeListener(_emitRoomDerivedState);
    }
    await _listener?.dispose();
    _listener = null;
    if (room != null) {
      await room.disconnect();
    }
    _room = null;
    _removedParticipantIds.clear();
    _forcedMutedParticipantIds.clear();
    _explicitlyUnmutedParticipantIds.clear();
    _forcedCameraOffParticipantIds.clear();
    _promotedCoHostParticipantIds.clear();
    _muteAllParticipants = false;
    _participants = const [];
    _participantsController.add(const []);
    _activeSpeakerController.add(null);
    _sharedContentController.add(null);
    _connectionController.add(RtcConnectionState.disconnected);
  }

  Future<void> setCameraEnabled(bool enabled) async {
    _cameraEnabled = enabled;
    final participant = _room?.localParticipant;
    if (participant != null) {
      await participant.setCameraEnabled(enabled);
    }
    _emitRoomDerivedState();
  }

  Future<void> setMicrophoneEnabled(bool enabled) async {
    _micEnabled = enabled;
    final participant = _room?.localParticipant;
    if (participant != null) {
      await participant.setMicrophoneEnabled(enabled);
    }
    _emitRoomDerivedState();
  }

  Future<void> setScreenShareEnabled(bool enabled) async {
    final participant = _room?.localParticipant;
    if (participant == null) {
      return;
    }
    await participant.setScreenShareEnabled(enabled);
    _emitRoomDerivedState();
  }

  Future<void> sendReaction(String emoji) async {
    if (emoji.trim().isEmpty) {
      return;
    }
    _reactionController.add(emoji);
    final participant = _room?.localParticipant;
    if (participant == null) {
      return;
    }
    await participant.publishData(
      utf8.encode(jsonEncode({'type': 'reaction', 'emoji': emoji})),
      reliable: false,
      topic: 'reaction',
    );
  }

  Future<void> _applyLocalMediaState() async {
    final participant = _room?.localParticipant;
    if (participant == null) {
      return;
    }
    try {
      await participant.setCameraEnabled(_cameraEnabled);
    } catch (_) {}
    try {
      await participant.setMicrophoneEnabled(_micEnabled);
    } catch (_) {}
  }

  void _emitRoomDerivedState() {
    final room = _room;
    if (room == null) {
      _participants = const [];
      _participantsController.add(_participants);
      _networkController.add(_networkFromQuality(NetworkQuality.poor));
      _sharedContentController.add(null);
      return;
    }

    final participants = <ParticipantModel>[];
    final local = room.localParticipant;
    if (local != null) {
      final mapped = _applyModerationOverlay(_mapParticipant(local));
      if (!_isParticipantRemoved(mapped.id)) {
        participants.add(mapped);
      }
    }
    for (final participant in room.remoteParticipants.values) {
      final mapped = _applyModerationOverlay(_mapParticipant(participant));
      if (!_isParticipantRemoved(mapped.id)) {
        participants.add(mapped);
      }
    }
    _participants = List<ParticipantModel>.unmodifiable(participants);
    _participantsController.add(_participants);

    final activeSpeaker = room.activeSpeakers.isEmpty
        ? null
        : room.activeSpeakers.first.identity;
    _activeSpeakerController.add(
      activeSpeaker == null || _isEffectivelyMuted(activeSpeaker)
          ? null
          : activeSpeaker,
    );

    final sharedSource = participants
        .where((item) => item.isScreenSharing)
        .map((item) => item.name)
        .cast<String?>()
        .followedBy(const [null])
        .first;
    _sharedContentController.add(sharedSource);

    final quality = _aggregateQuality(participants);
    _networkController.add(_networkFromQuality(quality));
  }

  ParticipantModel _applyModerationOverlay(ParticipantModel participant) {
    final bool forcedMuted = _isEffectivelyMuted(participant.id, participant: participant);
    final bool forcedCameraOff = _forcedCameraOffParticipantIds.contains(
      participant.id,
    );
    final bool promoted = _promotedCoHostParticipantIds.contains(participant.id);
    return participant.copyWith(
      micEnabled: forcedMuted ? false : participant.micEnabled,
      cameraEnabled: forcedCameraOff ? false : participant.cameraEnabled,
      role: promoted && participant.role == ParticipantRole.student
          ? ParticipantRole.coHost
          : participant.role,
    );
  }

  bool _isParticipantRemoved(String participantId) =>
      _removedParticipantIds.contains(participantId);

  bool _isEffectivelyMuted(
    String participantId, {
    ParticipantModel? participant,
  }) {
    if (_forcedMutedParticipantIds.contains(participantId)) {
      return true;
    }
    if (_explicitlyUnmutedParticipantIds.contains(participantId)) {
      return false;
    }
    if (!_muteAllParticipants) {
      return false;
    }
    ParticipantModel? target = participant;
    if (target == null) {
      for (final item in _participants) {
        if (item.id == participantId) {
          target = item;
          break;
        }
      }
    }
    return !(target?.isTeacher ?? false);
  }

  String _identityForParticipant(lk.Participant participant) {
    final identity = participant.identity;
    return identity.isNotEmpty ? identity : _currentUserId;
  }

  lk.VideoTrack? _participantTrack(
    String participantId, {
    required lk.TrackSource source,
  }) {
    final participant = _resolveParticipant(participantId);
    if (participant == null) {
      return null;
    }
    return _videoTrackForParticipant(participant, source: source);
  }

  lk.Participant? _resolveParticipant(String participantId) {
    final room = _room;
    if (room == null) {
      return null;
    }
    final local = room.localParticipant;
    if (local != null &&
        (participantId == _currentUserId || participantId == local.identity)) {
      return local;
    }
    return room.remoteParticipants[participantId];
  }

  lk.VideoTrack? _videoTrackForParticipant(
    lk.Participant participant, {
    required lk.TrackSource source,
  }) {
    final publication = participant.getTrackPublicationBySource(source);
    final track = publication?.track;
    if (publication == null || publication.muted || track is! lk.VideoTrack) {
      return null;
    }
    return track;
  }

  ParticipantModel _mapParticipant(lk.Participant participant) {
    final metadata = _decodeMetadata(participant.metadata ?? '');
    final identity = participant.identity;
    final name = participant.name.isNotEmpty
        ? participant.name
        : identity.isNotEmpty
        ? identity
        : _currentUserName;
    return ParticipantModel(
      id: identity.isNotEmpty ? identity : _currentUserId,
      name: name,
      role: _mapRole(
        metadata['role']?.toString(),
        isLocal: participant is lk.LocalParticipant,
      ),
      micEnabled: participant.isMicrophoneEnabled(),
      cameraEnabled: participant.isCameraEnabled(),
      handRaised:
          metadata['handRaised'] == true || metadata['hand_raised'] == true,
      isScreenSharing: participant.isScreenShareEnabled(),
      networkQuality: _qualityFromLiveKit(participant.connectionQuality),
      audioLevel: participant.audioLevel,
    );
  }

  ParticipantRole _mapRole(String? rawRole, {required bool isLocal}) {
    final normalized = (rawRole ?? '').trim().toLowerCase();
    if (normalized == 'teacher' ||
        normalized == 'host' ||
        normalized == 'cohost' ||
        normalized == 'co_host') {
      return normalized == 'cohost' || normalized == 'co_host'
          ? ParticipantRole.coHost
          : ParticipantRole.host;
    }
    return isLocal && rawRole == null
        ? ParticipantRole.student
        : ParticipantRole.student;
  }

  NetworkQuality _aggregateQuality(List<ParticipantModel> participants) {
    if (participants.isEmpty) {
      return NetworkQuality.poor;
    }
    final scores = participants
        .map((item) {
          switch (item.networkQuality) {
            case NetworkQuality.poor:
              return 0;
            case NetworkQuality.fair:
              return 1;
            case NetworkQuality.good:
              return 2;
            case NetworkQuality.excellent:
              return 3;
          }
        })
        .toList(growable: false);
    final total = scores.fold<int>(0, (sum, item) => sum + item);
    final average = total / scores.length;
    if (average >= 2.5) {
      return NetworkQuality.excellent;
    }
    if (average >= 1.5) {
      return NetworkQuality.good;
    }
    if (average >= 0.5) {
      return NetworkQuality.fair;
    }
    return NetworkQuality.poor;
  }

  NetworkQuality _qualityFromLiveKit(lk.ConnectionQuality quality) {
    switch (quality) {
      case lk.ConnectionQuality.excellent:
        return NetworkQuality.excellent;
      case lk.ConnectionQuality.good:
        return NetworkQuality.good;
      case lk.ConnectionQuality.poor:
        return NetworkQuality.fair;
      case lk.ConnectionQuality.lost:
      case lk.ConnectionQuality.unknown:
        return NetworkQuality.poor;
    }
  }

  NetworkStatsModel _networkFromQuality(NetworkQuality quality) {
    switch (quality) {
      case NetworkQuality.excellent:
        return const NetworkStatsModel(
          latencyMs: 28,
          packetLossPercent: 0.1,
          jitterMs: 6,
          uplinkKbps: 2600,
          downlinkKbps: 3200,
          quality: NetworkQuality.excellent,
        );
      case NetworkQuality.good:
        return const NetworkStatsModel(
          latencyMs: 48,
          packetLossPercent: 0.2,
          jitterMs: 10,
          uplinkKbps: 1900,
          downlinkKbps: 2400,
          quality: NetworkQuality.good,
        );
      case NetworkQuality.fair:
        return const NetworkStatsModel(
          latencyMs: 88,
          packetLossPercent: 0.7,
          jitterMs: 20,
          uplinkKbps: 1100,
          downlinkKbps: 1500,
          quality: NetworkQuality.fair,
        );
      case NetworkQuality.poor:
        return const NetworkStatsModel(
          latencyMs: 160,
          packetLossPercent: 2.0,
          jitterMs: 38,
          uplinkKbps: 600,
          downlinkKbps: 900,
          quality: NetworkQuality.poor,
        );
    }
  }

  Map<String, dynamic> _decodeMetadata(String raw) {
    if (raw.trim().isEmpty) {
      return const {};
    }
    try {
      final decoded = jsonDecode(raw);
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
      if (decoded is Map) {
        return Map<String, dynamic>.from(decoded);
      }
      return const {};
    } catch (_) {
      return const {};
    }
  }

  Map<String, dynamic> _decodeDataPayload(List<int> raw) {
    try {
      final decoded = jsonDecode(utf8.decode(raw));
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
      if (decoded is Map) {
        return Map<String, dynamic>.from(decoded);
      }
      return const {};
    } catch (_) {
      return const {};
    }
  }

  void dispose() {
    unawaited(disconnect());
    _participantsController.close();
    _activeSpeakerController.close();
    _networkController.close();
    _reactionController.close();
    _sharedContentController.close();
    _connectionController.close();
    _meetingLockController.close();
  }
}
