import 'network_stats_model.dart';

enum ParticipantRole { host, coHost, student }

class ParticipantModel {
  const ParticipantModel({
    required this.id,
    required this.name,
    required this.role,
    required this.micEnabled,
    required this.cameraEnabled,
    required this.handRaised,
    required this.networkQuality,
    this.isScreenSharing = false,
    this.audioLevel = 0,
  });

  final String id;
  final String name;
  final ParticipantRole role;
  final bool micEnabled;
  final bool cameraEnabled;
  final bool handRaised;
  final bool isScreenSharing;
  final NetworkQuality networkQuality;
  final double audioLevel;

  bool get isTeacher =>
      role == ParticipantRole.host || role == ParticipantRole.coHost;

  ParticipantModel copyWith({
    String? id,
    String? name,
    ParticipantRole? role,
    bool? micEnabled,
    bool? cameraEnabled,
    bool? handRaised,
    bool? isScreenSharing,
    NetworkQuality? networkQuality,
    double? audioLevel,
  }) {
    return ParticipantModel(
      id: id ?? this.id,
      name: name ?? this.name,
      role: role ?? this.role,
      micEnabled: micEnabled ?? this.micEnabled,
      cameraEnabled: cameraEnabled ?? this.cameraEnabled,
      handRaised: handRaised ?? this.handRaised,
      isScreenSharing: isScreenSharing ?? this.isScreenSharing,
      networkQuality: networkQuality ?? this.networkQuality,
      audioLevel: audioLevel ?? this.audioLevel,
    );
  }
}
