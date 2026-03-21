import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:web_socket_channel/web_socket_channel.dart';

import '../core/app_config.dart';
import '../models/class_session_model.dart';
import '../models/live_class_context.dart';
import '../models/network_stats_model.dart';
import '../models/waiting_room_request_model.dart';
import 'secure_api_client.dart';

enum JoinApprovalStatus { pending, approved, rejected, duplicate, canceled }

class LiveSessionAccess {
  const LiveSessionAccess({
    required this.sessionId,
    required this.roomId,
    required this.token,
    required this.provider,
    this.serverUrl,
    this.expiresAtEpochSeconds,
  });

  final String sessionId;
  final String roomId;
  final String token;
  final String provider;
  final String? serverUrl;
  final int? expiresAtEpochSeconds;
}

class ClassroomServerState {
  const ClassroomServerState({
    required this.classId,
    required this.userId,
    required this.whiteboardAccess,
    required this.whiteboardStrokes,
    required this.muted,
    required this.cameraDisabled,
    required this.meetingLocked,
    required this.chatEnabled,
    required this.waitingRoomEnabled,
    required this.isRecording,
    this.activeBreakoutRoomId,
    this.activeWhiteboardUserId,
  });

  final String classId;
  final String userId;
  final String? activeBreakoutRoomId;
  final String? activeWhiteboardUserId;
  final bool whiteboardAccess;
  final List<Map<String, dynamic>> whiteboardStrokes;
  final bool muted;
  final bool cameraDisabled;
  final bool meetingLocked;
  final bool chatEnabled;
  final bool waitingRoomEnabled;
  final bool isRecording;
}

class JoinApprovalEvent {
  const JoinApprovalEvent({
    required this.classId,
    required this.userId,
    required this.status,
    required this.requestId,
    this.message,
  });

  final String classId;
  final String userId;
  final JoinApprovalStatus status;
  final String requestId;
  final String? message;
}

abstract class ClassJoinService {
  Stream<List<WaitingRoomRequestModel>> get waitingRequestsStream;
  Stream<JoinApprovalEvent> get joinApprovalStream;

  Future<ClassSessionModel> fetchClassSession(LiveClassContext context);

  Future<void> startPresenceSubscription(LiveClassContext context);

  Future<String> requestJoin({
    required LiveClassContext context,
    required Map<String, dynamic> deviceInfo,
    required bool cameraEnabled,
    required bool micEnabled,
  });

  Future<void> cancelJoinRequest({
    required LiveClassContext context,
    required String requestId,
  });

  Future<void> approveJoinRequest({
    required String classId,
    required String userId,
  });

  Future<void> rejectJoinRequest({
    required String classId,
    required String userId,
    String? reason,
  });

  Future<void> approveAll({required String classId});

  Future<void> updateMeetingLock({
    required String classId,
    required bool locked,
  });

  Future<void> updateChatEnabled({
    required String classId,
    required bool enabled,
  });

  Future<void> updateWaitingRoomEnabled({
    required String classId,
    required bool enabled,
  });

  Future<void> updateRecordingEnabled({
    required String classId,
    required bool enabled,
  });

  Future<void> updateParticipantMuted({
    required String classId,
    required String userId,
    required bool muted,
  });

  Future<void> updateParticipantCameraDisabled({
    required String classId,
    required String userId,
    required bool disabled,
  });

  Future<void> removeParticipant({
    required String classId,
    required String userId,
  });

  Future<void> updateBreakoutAssignment({
    required String classId,
    required String userId,
    required String? roomId,
  });

  Future<void> broadcastBreakoutMessage({
    required String classId,
    required String message,
  });

  Future<void> updateWhiteboardAccess({
    required String classId,
    required String userId,
    required bool enabled,
  });

  Future<NetworkStatsModel> checkNetworkQuality();

  Future<LiveSessionAccess?> fetchLiveSessionAccess({
    required LiveClassContext context,
  });

  Future<ClassroomServerState?> fetchClassroomState({
    required String classId,
    required String userId,
  });

  Future<Map<String, String>?> fetchWebRtcFallbackToken({
    required String classId,
    required String userId,
  });

  void dispose();
}

class MockClassJoinService implements ClassJoinService {
  MockClassJoinService();

  final _waitingController =
      StreamController<List<WaitingRoomRequestModel>>.broadcast();
  final _approvalController = StreamController<JoinApprovalEvent>.broadcast();
  final List<WaitingRoomRequestModel> _pending = [];
  final Set<String> _approvedUsers = <String>{};
  final Map<String, String> _userRequestIds = <String, String>{};
  Timer? _autoApproveTimer;
  String? _subscribedClassId;

  @override
  Stream<List<WaitingRoomRequestModel>> get waitingRequestsStream =>
      _waitingController.stream;

  @override
  Stream<JoinApprovalEvent> get joinApprovalStream =>
      _approvalController.stream;

  @override
  Future<ClassSessionModel> fetchClassSession(LiveClassContext context) async {
    await Future<void>.delayed(const Duration(milliseconds: 180));
    return ClassSessionModel(
      id: context.classId,
      title: context.classTitle,
      teacherName: context.teacherName,
      startedAt: null,
      isRecording: false,
    );
  }

  @override
  Future<void> startPresenceSubscription(LiveClassContext context) async {
    _subscribedClassId = context.classId;
    _emitWaiting();
  }

  @override
  Future<String> requestJoin({
    required LiveClassContext context,
    required Map<String, dynamic> deviceInfo,
    required bool cameraEnabled,
    required bool micEnabled,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 120));
    if (_approvedUsers.contains(context.userId) ||
        _userRequestIds.containsKey(context.userId)) {
      final duplicateRequest = _userRequestIds[context.userId] ?? 'duplicate';
      _approvalController.add(
        JoinApprovalEvent(
          classId: context.classId,
          userId: context.userId,
          status: JoinApprovalStatus.duplicate,
          requestId: duplicateRequest,
          message: 'Duplicate join detected.',
        ),
      );
      return duplicateRequest;
    }

    final requestId = 'join_${DateTime.now().millisecondsSinceEpoch}';
    _userRequestIds[context.userId] = requestId;
    _pending.add(
      WaitingRoomRequestModel(
        participantId: context.userId,
        name: context.userName,
        requestedAt: DateTime.now(),
      ),
    );
    _emitWaiting();
    _approvalController.add(
      JoinApprovalEvent(
        classId: context.classId,
        userId: context.userId,
        status: JoinApprovalStatus.pending,
        requestId: requestId,
      ),
    );

    // In standalone mock mode there may be no teacher console open, so
    // auto-approve after a short delay to keep student flow testable.
    _autoApproveTimer?.cancel();
    _autoApproveTimer = Timer(const Duration(seconds: 8), () {
      if (!_userRequestIds.containsKey(context.userId)) {
        return;
      }
      unawaited(
        approveJoinRequest(classId: context.classId, userId: context.userId),
      );
    });

    return requestId;
  }

  @override
  Future<void> cancelJoinRequest({
    required LiveClassContext context,
    required String requestId,
  }) async {
    _userRequestIds.remove(context.userId);
    _pending.removeWhere((item) => item.participantId == context.userId);
    _emitWaiting();
    _approvalController.add(
      JoinApprovalEvent(
        classId: context.classId,
        userId: context.userId,
        status: JoinApprovalStatus.canceled,
        requestId: requestId,
      ),
    );
  }

  @override
  Future<void> approveJoinRequest({
    required String classId,
    required String userId,
  }) async {
    final requestId = _userRequestIds[userId];
    if (requestId == null) {
      return;
    }
    _approvedUsers.add(userId);
    _userRequestIds.remove(userId);
    _pending.removeWhere((item) => item.participantId == userId);
    _emitWaiting();
    _approvalController.add(
      JoinApprovalEvent(
        classId: classId,
        userId: userId,
        status: JoinApprovalStatus.approved,
        requestId: requestId,
      ),
    );
  }

  @override
  Future<void> rejectJoinRequest({
    required String classId,
    required String userId,
    String? reason,
  }) async {
    final requestId = _userRequestIds[userId];
    if (requestId == null) {
      return;
    }
    _userRequestIds.remove(userId);
    _pending.removeWhere((item) => item.participantId == userId);
    _emitWaiting();
    _approvalController.add(
      JoinApprovalEvent(
        classId: classId,
        userId: userId,
        status: JoinApprovalStatus.rejected,
        requestId: requestId,
        message: reason ?? 'Teacher declined your request.',
      ),
    );
  }

  @override
  Future<void> approveAll({required String classId}) async {
    final users = _pending
        .map((item) => item.participantId)
        .toList(growable: false);
    for (final userId in users) {
      await approveJoinRequest(classId: classId, userId: userId);
    }
  }

  @override
  Future<void> updateMeetingLock({
    required String classId,
    required bool locked,
  }) async {}

  @override
  Future<void> updateChatEnabled({
    required String classId,
    required bool enabled,
  }) async {}

  @override
  Future<void> updateWaitingRoomEnabled({
    required String classId,
    required bool enabled,
  }) async {}

  @override
  Future<void> updateRecordingEnabled({
    required String classId,
    required bool enabled,
  }) async {}

  @override
  Future<void> updateParticipantMuted({
    required String classId,
    required String userId,
    required bool muted,
  }) async {}

  @override
  Future<void> updateParticipantCameraDisabled({
    required String classId,
    required String userId,
    required bool disabled,
  }) async {}

  @override
  Future<void> removeParticipant({
    required String classId,
    required String userId,
  }) async {}

  @override
  Future<void> updateBreakoutAssignment({
    required String classId,
    required String userId,
    required String? roomId,
  }) async {}

  @override
  Future<void> broadcastBreakoutMessage({
    required String classId,
    required String message,
  }) async {}

  @override
  Future<void> updateWhiteboardAccess({
    required String classId,
    required String userId,
    required bool enabled,
  }) async {}

  @override
  Future<NetworkStatsModel> checkNetworkQuality() async {
    await Future<void>.delayed(const Duration(milliseconds: 120));
    return const NetworkStatsModel(
      latencyMs: 45,
      packetLossPercent: 0.1,
      jitterMs: 8,
      uplinkKbps: 2400,
      downlinkKbps: 2900,
      quality: NetworkQuality.good,
    );
  }

  @override
  Future<LiveSessionAccess?> fetchLiveSessionAccess({
    required LiveClassContext context,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 60));
    return LiveSessionAccess(
      sessionId: context.classId,
      roomId: context.classId,
      token: context.sessionToken.isNotEmpty
          ? context.sessionToken
          : 'mock_live_token_${context.userId}',
      provider: 'mock_live',
      serverUrl: null,
      expiresAtEpochSeconds:
          DateTime.now()
              .add(const Duration(minutes: 10))
              .millisecondsSinceEpoch ~/
          1000,
    );
  }

  @override
  Future<ClassroomServerState?> fetchClassroomState({
    required String classId,
    required String userId,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 60));
    return ClassroomServerState(
      classId: classId,
      userId: userId,
      activeBreakoutRoomId: null,
      activeWhiteboardUserId: null,
      whiteboardAccess: false,
      whiteboardStrokes: const [],
      muted: false,
      cameraDisabled: false,
      meetingLocked: false,
      chatEnabled: true,
      waitingRoomEnabled: true,
      isRecording: false,
    );
  }

  @override
  Future<Map<String, String>?> fetchWebRtcFallbackToken({
    required String classId,
    required String userId,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 80));
    return <String, String>{
      'provider': 'mock_webrtc',
      'room': classId,
      'token': 'mock_fallback_token_$userId',
      'url': 'wss://fallback.example.com/$classId',
    };
  }

  void _emitWaiting() {
    if (_subscribedClassId == null) {
      return;
    }
    _waitingController.add(
      List<WaitingRoomRequestModel>.unmodifiable(_pending),
    );
  }

  @override
  void dispose() {
    _autoApproveTimer?.cancel();
    _waitingController.close();
    _approvalController.close();
  }
}

class RealClassJoinService implements ClassJoinService {
  RealClassJoinService({required this.config, required this.apiClient});

  final AppConfig config;
  final SecureApiClient apiClient;

  final _waitingController =
      StreamController<List<WaitingRoomRequestModel>>.broadcast();
  final _approvalController = StreamController<JoinApprovalEvent>.broadcast();
  final List<WaitingRoomRequestModel> _realtimePending = [];

  WebSocketChannel? _ws;

  @override
  Stream<List<WaitingRoomRequestModel>> get waitingRequestsStream =>
      _waitingController.stream;

  @override
  Stream<JoinApprovalEvent> get joinApprovalStream =>
      _approvalController.stream;

  @override
  Future<ClassSessionModel> fetchClassSession(LiveClassContext context) async {
    final response = await apiClient.getJson(
      config.apiUri(
        config.classSessionEndpoint,
        queryParameters: {'class_id': context.classId},
      ),
      signRequest: true,
    );
    return ClassSessionModel(
      id: context.classId,
      title: response['title']?.toString() ?? context.classTitle,
      teacherName: response['teacher_name']?.toString() ?? context.teacherName,
      startedAt: null,
      isRecording: response['is_recording'] == true,
    );
  }

  @override
  Future<void> startPresenceSubscription(LiveClassContext context) async {
    final endpoint = config.classEventsEndpoint.trim();
    Uri uri;
    if (endpoint.startsWith('ws://') || endpoint.startsWith('wss://')) {
      final base = Uri.parse(endpoint);
      uri = base.replace(
        queryParameters: {
          ...base.queryParameters,
          'class_id': context.classId,
          'user_id': context.userId,
          'token': context.sessionToken,
        },
      );
    } else if (endpoint.startsWith('http://') ||
        endpoint.startsWith('https://')) {
      final base = Uri.parse(endpoint);
      uri = base.replace(
        scheme: base.scheme == 'https' ? 'wss' : 'ws',
        queryParameters: {
          ...base.queryParameters,
          'class_id': context.classId,
          'user_id': context.userId,
          'token': context.sessionToken,
        },
      );
    } else {
      final base = Uri.parse(config.baseApiUrl);
      final normalizedPath = endpoint.startsWith('/') ? endpoint : '/$endpoint';
      uri = base.replace(
        scheme: base.scheme == 'https' ? 'wss' : 'ws',
        path: normalizedPath,
        queryParameters: {
          'class_id': context.classId,
          'user_id': context.userId,
          'token': context.sessionToken,
        },
      );
    }

    await _ws?.sink.close();
    _ws = WebSocketChannel.connect(uri);
    _ws!.stream.listen(
      _handleWebSocketEvent,
      onError: (_) {},
      onDone: () {},
      cancelOnError: false,
    );
  }

  @override
  Future<String> requestJoin({
    required LiveClassContext context,
    required Map<String, dynamic> deviceInfo,
    required bool cameraEnabled,
    required bool micEnabled,
  }) async {
    final response = await apiClient
        .postJson(config.apiUri(config.classJoinRequestEndpoint), {
          'class_id': context.classId,
          'user_id': context.userId,
          'user_name': context.userName,
          'role': context.role,
          'device_info': deviceInfo,
          'session_token': context.sessionToken,
          'camera_enabled': cameraEnabled,
          'mic_enabled': micEnabled,
        });

    return response['request_id']?.toString() ??
        'join_${DateTime.now().millisecondsSinceEpoch}';
  }

  @override
  Future<void> cancelJoinRequest({
    required LiveClassContext context,
    required String requestId,
  }) async {
    await apiClient.postJson(config.apiUri(config.classJoinCancelEndpoint), {
      'class_id': context.classId,
      'user_id': context.userId,
      'request_id': requestId,
      'session_token': context.sessionToken,
    });
  }

  @override
  Future<void> approveJoinRequest({
    required String classId,
    required String userId,
  }) async {
    await apiClient.postJson(config.apiUri(config.classAdmitEndpoint), {
      'class_id': classId,
      'user_id': userId,
    });
  }

  @override
  Future<void> rejectJoinRequest({
    required String classId,
    required String userId,
    String? reason,
  }) async {
    await apiClient.postJson(config.apiUri(config.classRejectEndpoint), {
      'class_id': classId,
      'user_id': userId,
      if (reason != null && reason.isNotEmpty) 'reason': reason,
    });
  }

  @override
  Future<void> approveAll({required String classId}) async {
    await apiClient.postJson(config.apiUri(config.classAdmitAllEndpoint), {
      'class_id': classId,
    });
  }

  @override
  Future<NetworkStatsModel> checkNetworkQuality() async {
    final watch = Stopwatch()..start();
    final client = HttpClient();
    try {
      final request = await client
          .getUrl(config.apiUri(config.healthPingEndpoint))
          .timeout(const Duration(seconds: 2));
      final response = await request.close().timeout(
        const Duration(seconds: 2),
      );
      await response.drain();
      watch.stop();

      final latency = watch.elapsedMilliseconds;
      final quality = latency <= 70
          ? NetworkQuality.good
          : latency <= 130
          ? NetworkQuality.fair
          : NetworkQuality.poor;
      return NetworkStatsModel(
        latencyMs: latency,
        packetLossPercent: quality == NetworkQuality.poor ? 1.2 : 0.2,
        jitterMs: quality == NetworkQuality.poor ? 25 : 9,
        uplinkKbps: quality == NetworkQuality.poor ? 900 : 2100,
        downlinkKbps: quality == NetworkQuality.poor ? 1100 : 2400,
        quality: quality,
      );
    } catch (_) {
      return const NetworkStatsModel(
        latencyMs: 999,
        packetLossPercent: 2.0,
        jitterMs: 40,
        uplinkKbps: 600,
        downlinkKbps: 700,
        quality: NetworkQuality.poor,
      );
    } finally {
      client.close(force: true);
    }
  }

  @override
  Future<LiveSessionAccess?> fetchLiveSessionAccess({
    required LiveClassContext context,
  }) async {
    final response = await apiClient
        .postJson(config.apiUri(config.liveTokenEndpoint), {
          'class_id': context.classId,
          'user_id': context.userId,
          'display_name': context.userName,
          'role': context.role,
          'title': context.classTitle,
          'teacher_name': context.teacherName,
          'subject': context.subject,
          'topic': context.topic,
        });
    final token = response['token']?.toString() ?? '';
    if (token.isEmpty) {
      return null;
    }
    final sessionId = response['session_id']?.toString() ?? context.classId;
    final roomId = response['room_id']?.toString() ?? context.classId;
    return LiveSessionAccess(
      sessionId: sessionId.trim().isNotEmpty ? sessionId : context.classId,
      roomId: roomId.trim().isNotEmpty ? roomId : context.classId,
      token: token,
      provider: response['provider']?.toString() ?? 'lalacore_live',
      serverUrl: response['ws_url']?.toString(),
      expiresAtEpochSeconds: (response['expires_at'] as num?)?.toInt(),
    );
  }

  @override
  Future<void> updateMeetingLock({
    required String classId,
    required bool locked,
  }) async {
    await apiClient.postJson(config.apiUri(config.classLockEndpoint), {
      'class_id': classId,
      'locked': locked,
    });
  }

  @override
  Future<void> updateChatEnabled({
    required String classId,
    required bool enabled,
  }) async {
    await apiClient.postJson(config.apiUri(config.classChatEndpoint), {
      'class_id': classId,
      'enabled': enabled,
    });
  }

  @override
  Future<void> updateWaitingRoomEnabled({
    required String classId,
    required bool enabled,
  }) async {
    await apiClient.postJson(config.apiUri(config.classWaitingRoomEndpoint), {
      'class_id': classId,
      'enabled': enabled,
    });
  }

  @override
  Future<void> updateRecordingEnabled({
    required String classId,
    required bool enabled,
  }) async {
    await apiClient.postJson(config.apiUri(config.classRecordingEndpoint), {
      'class_id': classId,
      'enabled': enabled,
    });
  }

  @override
  Future<void> updateParticipantMuted({
    required String classId,
    required String userId,
    required bool muted,
  }) async {
    await apiClient.postJson(config.apiUri(config.classMuteEndpoint), {
      'class_id': classId,
      'user_id': userId,
      'muted': muted,
    });
  }

  @override
  Future<void> updateParticipantCameraDisabled({
    required String classId,
    required String userId,
    required bool disabled,
  }) async {
    await apiClient.postJson(config.apiUri(config.classCameraEndpoint), {
      'class_id': classId,
      'user_id': userId,
      'disabled': disabled,
    });
  }

  @override
  Future<void> removeParticipant({
    required String classId,
    required String userId,
  }) async {
    await apiClient.postJson(config.apiUri(config.classRemoveEndpoint), {
      'class_id': classId,
      'user_id': userId,
    });
  }

  @override
  Future<void> updateBreakoutAssignment({
    required String classId,
    required String userId,
    required String? roomId,
  }) async {
    await apiClient.postJson(config.apiUri(config.classBreakoutMoveEndpoint), {
      'class_id': classId,
      'user_id': userId,
      'room_id': roomId,
    });
  }

  @override
  Future<void> broadcastBreakoutMessage({
    required String classId,
    required String message,
  }) async {
    await apiClient.postJson(
      config.apiUri(config.classBreakoutBroadcastEndpoint),
      {'class_id': classId, 'message': message},
    );
  }

  @override
  Future<void> updateWhiteboardAccess({
    required String classId,
    required String userId,
    required bool enabled,
  }) async {
    await apiClient.postJson(
      config.apiUri(config.classWhiteboardAccessEndpoint),
      {'class_id': classId, 'user_id': userId, 'enabled': enabled},
    );
  }

  @override
  Future<ClassroomServerState?> fetchClassroomState({
    required String classId,
    required String userId,
  }) async {
    final response = await apiClient.getJson(
      config.apiUri(
        config.classStateEndpoint,
        queryParameters: {'class_id': classId, 'user_id': userId},
      ),
      signRequest: true,
    );
    if (response.isEmpty) {
      return null;
    }
    return ClassroomServerState(
      classId: response['class_id']?.toString() ?? classId,
      userId: response['user_id']?.toString() ?? userId,
      activeBreakoutRoomId: response['active_breakout_room_id']?.toString(),
      activeWhiteboardUserId: response['active_whiteboard_user_id']?.toString(),
      whiteboardAccess: response['whiteboard_access'] == true,
      whiteboardStrokes: response['whiteboard_strokes'] is List
          ? (response['whiteboard_strokes'] as List)
                .whereType<Map>()
                .map(
                  (item) =>
                      Map<String, dynamic>.from(item.cast<dynamic, dynamic>()),
                )
                .toList(growable: false)
          : const [],
      muted: response['muted'] == true,
      cameraDisabled: response['camera_disabled'] == true,
      meetingLocked: response['meeting_locked'] == true,
      chatEnabled: response['chat_enabled'] != false,
      waitingRoomEnabled: response['waiting_room_enabled'] != false,
      isRecording: response['is_recording'] == true,
    );
  }

  @override
  Future<Map<String, String>?> fetchWebRtcFallbackToken({
    required String classId,
    required String userId,
  }) async {
    final response = await apiClient.postJson(
      config.apiUri(config.webrtcFallbackEndpoint),
      {'class_id': classId, 'user_id': userId},
    );
    if (response.isEmpty) {
      return null;
    }
    return response.map((key, value) => MapEntry(key, value.toString()));
  }

  void _handleWebSocketEvent(dynamic raw) {
    if (raw is! String) {
      return;
    }
    final event = raw.trim();
    if (event.isEmpty) {
      return;
    }

    // Expected JSON events. Keep parser defensive to avoid hard failures.
    final map = _toMap(event);
    if (map.isEmpty) {
      return;
    }

    final type = map['type']?.toString() ?? '';
    if (type == 'waiting_room_snapshot' && map['requests'] is List) {
      final requests = (map['requests'] as List)
          .whereType<Map>()
          .map(
            (item) => WaitingRoomRequestModel(
              participantId: item['user_id']?.toString() ?? '',
              name: item['user_name']?.toString() ?? 'Student',
              requestedAt:
                  DateTime.tryParse(item['requested_at']?.toString() ?? '') ??
                  DateTime.now(),
            ),
          )
          .where((item) => item.participantId.isNotEmpty)
          .toList(growable: false);
      _realtimePending
        ..clear()
        ..addAll(requests);
      _waitingController.add(
        List<WaitingRoomRequestModel>.unmodifiable(_realtimePending),
      );
      return;
    }

    if (type == 'join_approved' || type == 'join_rejected') {
      _approvalController.add(
        JoinApprovalEvent(
          classId: map['class_id']?.toString() ?? '',
          userId: map['user_id']?.toString() ?? '',
          status: type == 'join_approved'
              ? JoinApprovalStatus.approved
              : JoinApprovalStatus.rejected,
          requestId: map['request_id']?.toString() ?? '',
          message: map['message']?.toString(),
        ),
      );
    }

    if (type == 'join_request_received') {
      final request = WaitingRoomRequestModel(
        participantId: map['user_id']?.toString() ?? '',
        name: map['user_name']?.toString() ?? 'Student',
        requestedAt:
            DateTime.tryParse(map['requested_at']?.toString() ?? '') ??
            DateTime.now(),
      );
      _realtimePending.removeWhere(
        (item) => item.participantId == request.participantId,
      );
      _realtimePending.add(request);
      _waitingController.add(
        List<WaitingRoomRequestModel>.unmodifiable(_realtimePending),
      );
    }

    if (type == 'join_request_removed') {
      final userId = map['user_id']?.toString() ?? '';
      _realtimePending.removeWhere((item) => item.participantId == userId);
      _waitingController.add(
        List<WaitingRoomRequestModel>.unmodifiable(_realtimePending),
      );
    }
  }

  Map<String, dynamic> _toMap(String raw) {
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
    _ws?.sink.close();
    _waitingController.close();
    _approvalController.close();
  }
}
