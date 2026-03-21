class WaitingRoomRequestModel {
  const WaitingRoomRequestModel({
    required this.participantId,
    required this.name,
    required this.requestedAt,
  });

  final String participantId;
  final String name;
  final DateTime requestedAt;
}

enum RtcConnectionState {
  disconnected,
  connecting,
  connected,
  reconnecting,
  failed,
}
