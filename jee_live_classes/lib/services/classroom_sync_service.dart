import 'dart:async';
import 'dart:convert';

import 'package:web_socket_channel/web_socket_channel.dart';

import '../core/app_config.dart';
import '../models/live_class_context.dart';

enum ClassroomSyncEventType {
  raiseHand,
  lowerHand,
  approveMic,
  participantMuted,
  participantRemoved,
  participantCameraDisabled,
  participantPromoted,
  muteAllParticipants,
  whiteboardRequest,
  whiteboardGrant,
  whiteboardDismiss,
  whiteboardRevoke,
  whiteboardStroke,
  whiteboardClear,
  meetingLockChanged,
  chatEnabledChanged,
  waitingRoomChanged,
  recordingChanged,
  breakoutAssignmentChanged,
  breakoutBroadcast,
  laserToggle,
  laserMove,
}

class ClassroomSyncEvent {
  const ClassroomSyncEvent({
    required this.type,
    required this.classId,
    required this.senderId,
    required this.timestamp,
    this.targetUserId,
    this.enabled,
    this.positionX,
    this.positionY,
    this.metadata = const {},
  });

  final ClassroomSyncEventType type;
  final String classId;
  final String senderId;
  final DateTime timestamp;
  final String? targetUserId;
  final bool? enabled;
  final double? positionX;
  final double? positionY;
  final Map<String, dynamic> metadata;

  Map<String, dynamic> toJson() {
    return {
      'type': _typeToWire(type),
      'class_id': classId,
      'sender_id': senderId,
      'target_user_id': targetUserId,
      'enabled': enabled,
      'position_x': positionX,
      'position_y': positionY,
      'metadata': metadata,
      'timestamp': timestamp.toUtc().toIso8601String(),
    };
  }

  static ClassroomSyncEvent? fromJson(Map<String, dynamic> json) {
    final type = _wireToType(json['type']?.toString() ?? '');
    if (type == null) {
      return null;
    }
    final classId = json['class_id']?.toString() ?? '';
    final senderId = json['sender_id']?.toString() ?? '';
    if (classId.isEmpty || senderId.isEmpty) {
      return null;
    }
    return ClassroomSyncEvent(
      type: type,
      classId: classId,
      senderId: senderId,
      targetUserId: json['target_user_id']?.toString(),
      enabled: json['enabled'] is bool ? json['enabled'] as bool : null,
      positionX: (json['position_x'] as num?)?.toDouble(),
      positionY: (json['position_y'] as num?)?.toDouble(),
      metadata: json['metadata'] is Map
          ? Map<String, dynamic>.from(json['metadata'] as Map)
          : const {},
      timestamp:
          DateTime.tryParse(json['timestamp']?.toString() ?? '') ??
          DateTime.now(),
    );
  }
}

abstract class ClassroomSyncService {
  Stream<ClassroomSyncEvent> get events;

  Future<void> connect(LiveClassContext context);
  Future<void> publish(ClassroomSyncEvent event);
  Future<void> disconnect();
  void dispose();
}

class MockClassroomSyncService implements ClassroomSyncService {
  MockClassroomSyncService();

  final StreamController<ClassroomSyncEvent> _events =
      StreamController<ClassroomSyncEvent>.broadcast();

  @override
  Stream<ClassroomSyncEvent> get events => _events.stream;

  @override
  Future<void> connect(LiveClassContext context) async {}

  @override
  Future<void> publish(ClassroomSyncEvent event) async {
    _events.add(event);
  }

  @override
  Future<void> disconnect() async {}

  @override
  void dispose() {
    _events.close();
  }
}

class RealClassroomSyncService implements ClassroomSyncService {
  RealClassroomSyncService({required this.config});

  final AppConfig config;
  final StreamController<ClassroomSyncEvent> _events =
      StreamController<ClassroomSyncEvent>.broadcast();

  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _subscription;
  LiveClassContext? _context;

  @override
  Stream<ClassroomSyncEvent> get events => _events.stream;

  @override
  Future<void> connect(LiveClassContext context) async {
    _context = context;
    final uri = _buildUri(context);
    await _subscription?.cancel();
    await _channel?.sink.close();
    _channel = WebSocketChannel.connect(uri);
    _subscription = _channel!.stream.listen(
      _handleRawEvent,
      onError: (_) {},
      onDone: () {},
      cancelOnError: false,
    );
  }

  @override
  Future<void> publish(ClassroomSyncEvent event) async {
    final channel = _channel;
    if (channel == null) {
      return;
    }
    channel.sink.add(jsonEncode(event.toJson()));
  }

  @override
  Future<void> disconnect() async {
    await _subscription?.cancel();
    _subscription = null;
    await _channel?.sink.close();
    _channel = null;
  }

  void _handleRawEvent(dynamic raw) {
    if (raw is! String) {
      return;
    }
    try {
      final decoded = jsonDecode(raw);
      if (decoded is! Map<String, dynamic>) {
        return;
      }
      final event = ClassroomSyncEvent.fromJson(decoded);
      if (event == null) {
        return;
      }
      if (_context != null && event.classId != _context!.classId) {
        return;
      }
      _events.add(event);
    } catch (_) {
      // Keep sync channel best-effort and non-blocking.
    }
  }

  Uri _buildUri(LiveClassContext context) {
    final endpoint = config.classSyncEndpoint.trim();
    if (endpoint.startsWith('ws://') || endpoint.startsWith('wss://')) {
      final base = Uri.parse(endpoint);
      return base.replace(
        queryParameters: {
          ...base.queryParameters,
          'class_id': context.classId,
          'user_id': context.userId,
          'token': context.sessionToken,
        },
      );
    }

    if (endpoint.startsWith('http://') || endpoint.startsWith('https://')) {
      final base = Uri.parse(endpoint);
      return base.replace(
        scheme: base.scheme == 'https' ? 'wss' : 'ws',
        queryParameters: {
          ...base.queryParameters,
          'class_id': context.classId,
          'user_id': context.userId,
          'token': context.sessionToken,
        },
      );
    }

    final base = Uri.parse(config.baseApiUrl);
    final path = endpoint.startsWith('/') ? endpoint : '/$endpoint';
    return base.replace(
      scheme: base.scheme == 'https' ? 'wss' : 'ws',
      path: path,
      queryParameters: {
        'class_id': context.classId,
        'user_id': context.userId,
        'token': context.sessionToken,
      },
    );
  }

  @override
  void dispose() {
    unawaited(disconnect());
    _events.close();
  }
}

String _typeToWire(ClassroomSyncEventType type) {
  switch (type) {
    case ClassroomSyncEventType.raiseHand:
      return 'raise_hand';
    case ClassroomSyncEventType.lowerHand:
      return 'lower_hand';
    case ClassroomSyncEventType.approveMic:
      return 'approve_mic';
    case ClassroomSyncEventType.participantMuted:
      return 'participant_muted';
    case ClassroomSyncEventType.participantRemoved:
      return 'participant_removed';
    case ClassroomSyncEventType.participantCameraDisabled:
      return 'participant_camera_disabled';
    case ClassroomSyncEventType.participantPromoted:
      return 'participant_promoted';
    case ClassroomSyncEventType.muteAllParticipants:
      return 'mute_all_participants';
    case ClassroomSyncEventType.whiteboardRequest:
      return 'whiteboard_request';
    case ClassroomSyncEventType.whiteboardGrant:
      return 'whiteboard_grant';
    case ClassroomSyncEventType.whiteboardDismiss:
      return 'whiteboard_dismiss';
    case ClassroomSyncEventType.whiteboardRevoke:
      return 'whiteboard_revoke';
    case ClassroomSyncEventType.whiteboardStroke:
      return 'whiteboard_stroke';
    case ClassroomSyncEventType.whiteboardClear:
      return 'whiteboard_clear';
    case ClassroomSyncEventType.meetingLockChanged:
      return 'meeting_lock_changed';
    case ClassroomSyncEventType.chatEnabledChanged:
      return 'chat_enabled_changed';
    case ClassroomSyncEventType.waitingRoomChanged:
      return 'waiting_room_changed';
    case ClassroomSyncEventType.recordingChanged:
      return 'recording_changed';
    case ClassroomSyncEventType.breakoutAssignmentChanged:
      return 'breakout_assignment_changed';
    case ClassroomSyncEventType.breakoutBroadcast:
      return 'breakout_broadcast';
    case ClassroomSyncEventType.laserToggle:
      return 'laser_toggle';
    case ClassroomSyncEventType.laserMove:
      return 'laser_move';
  }
}

ClassroomSyncEventType? _wireToType(String wire) {
  switch (wire) {
    case 'raise_hand':
      return ClassroomSyncEventType.raiseHand;
    case 'lower_hand':
      return ClassroomSyncEventType.lowerHand;
    case 'approve_mic':
      return ClassroomSyncEventType.approveMic;
    case 'participant_muted':
      return ClassroomSyncEventType.participantMuted;
    case 'participant_removed':
      return ClassroomSyncEventType.participantRemoved;
    case 'participant_camera_disabled':
      return ClassroomSyncEventType.participantCameraDisabled;
    case 'participant_promoted':
      return ClassroomSyncEventType.participantPromoted;
    case 'mute_all_participants':
      return ClassroomSyncEventType.muteAllParticipants;
    case 'whiteboard_request':
      return ClassroomSyncEventType.whiteboardRequest;
    case 'whiteboard_grant':
      return ClassroomSyncEventType.whiteboardGrant;
    case 'whiteboard_dismiss':
      return ClassroomSyncEventType.whiteboardDismiss;
    case 'whiteboard_revoke':
      return ClassroomSyncEventType.whiteboardRevoke;
    case 'whiteboard_stroke':
      return ClassroomSyncEventType.whiteboardStroke;
    case 'whiteboard_clear':
      return ClassroomSyncEventType.whiteboardClear;
    case 'meeting_lock_changed':
      return ClassroomSyncEventType.meetingLockChanged;
    case 'chat_enabled_changed':
      return ClassroomSyncEventType.chatEnabledChanged;
    case 'waiting_room_changed':
      return ClassroomSyncEventType.waitingRoomChanged;
    case 'recording_changed':
      return ClassroomSyncEventType.recordingChanged;
    case 'breakout_assignment_changed':
      return ClassroomSyncEventType.breakoutAssignmentChanged;
    case 'breakout_broadcast':
      return ClassroomSyncEventType.breakoutBroadcast;
    case 'laser_toggle':
      return ClassroomSyncEventType.laserToggle;
    case 'laser_move':
      return ClassroomSyncEventType.laserMove;
    default:
      return null;
  }
}
