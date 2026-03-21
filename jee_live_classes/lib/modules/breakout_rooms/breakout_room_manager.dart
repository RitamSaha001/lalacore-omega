import '../../models/participant_model.dart';

class BreakoutRoom {
  const BreakoutRoom({
    required this.id,
    required this.name,
    required this.participantIds,
  });

  final String id;
  final String name;
  final List<String> participantIds;

  BreakoutRoom copyWith({
    String? id,
    String? name,
    List<String>? participantIds,
  }) {
    return BreakoutRoom(
      id: id ?? this.id,
      name: name ?? this.name,
      participantIds: participantIds ?? this.participantIds,
    );
  }
}

class BreakoutMediaContext {
  const BreakoutMediaContext({
    required this.roomId,
    required this.audioChannel,
    required this.videoChannel,
    required this.chatChannel,
  });

  final String roomId;
  final String audioChannel;
  final String videoChannel;
  final String chatChannel;
}

class BreakoutRoomManager {
  // BEGIN_PHASE2_IMPLEMENTATION
  final List<BreakoutRoom> _rooms = [];
  String? _currentRoomId;

  List<BreakoutRoom> get rooms => List<BreakoutRoom>.unmodifiable(_rooms);
  String? get currentRoomId => _currentRoomId;

  BreakoutRoom createRoom(String name) {
    final room = BreakoutRoom(
      id: 'room_${_rooms.length + 1}',
      name: name,
      participantIds: const [],
    );
    _rooms.add(room);
    return room;
  }

  void assignParticipant({
    required String participantId,
    required String roomId,
  }) {
    for (var index = 0; index < _rooms.length; index += 1) {
      final room = _rooms[index];
      if (room.id == roomId) {
        final ids = List<String>.from(room.participantIds);
        if (!ids.contains(participantId)) {
          ids.add(participantId);
          _rooms[index] = room.copyWith(participantIds: ids);
        }
      } else {
        final ids = List<String>.from(room.participantIds)
          ..remove(participantId);
        _rooms[index] = room.copyWith(participantIds: ids);
      }
    }
  }

  BreakoutMediaContext joinBreakoutRoom(String roomId) {
    _currentRoomId = roomId;
    return BreakoutMediaContext(
      roomId: roomId,
      audioChannel: 'audio_$roomId',
      videoChannel: 'video_$roomId',
      chatChannel: 'chat_$roomId',
    );
  }

  void leaveBreakoutRoom() {
    _currentRoomId = null;
  }

  void removeRoom(String roomId) {
    if (_currentRoomId == roomId) {
      _currentRoomId = null;
    }
    _rooms.removeWhere((room) => room.id == roomId);
  }

  String broadcastMessageToRooms({
    required String message,
    required List<ParticipantModel> participants,
  }) {
    final totalAssigned = _rooms.fold<int>(
      0,
      (count, room) => count + room.participantIds.length,
    );
    final totalParticipants = participants.length;

    return 'Broadcast to ${_rooms.length} room(s), '
        '$totalAssigned assigned / $totalParticipants total participant(s): '
        '$message';
  }

  String buildBroadcastMessage(
    String message,
    List<ParticipantModel> participants,
  ) {
    return broadcastMessageToRooms(
      message: message,
      participants: participants,
    );
  }
  // END_PHASE2_IMPLEMENTATION
}
