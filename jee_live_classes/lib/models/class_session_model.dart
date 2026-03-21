class ClassSessionModel {
  const ClassSessionModel({
    required this.id,
    required this.title,
    required this.teacherName,
    required this.startedAt,
    required this.isRecording,
    this.rtcProvider,
    this.rtcServerUrl,
  });

  final String id;
  final String title;
  final String teacherName;
  final DateTime? startedAt;
  final bool isRecording;
  final String? rtcProvider;
  final String? rtcServerUrl;

  ClassSessionModel copyWith({
    String? id,
    String? title,
    String? teacherName,
    DateTime? startedAt,
    bool? isRecording,
    String? rtcProvider,
    bool clearRtcProvider = false,
    String? rtcServerUrl,
    bool clearRtcServerUrl = false,
  }) {
    return ClassSessionModel(
      id: id ?? this.id,
      title: title ?? this.title,
      teacherName: teacherName ?? this.teacherName,
      startedAt: startedAt ?? this.startedAt,
      isRecording: isRecording ?? this.isRecording,
      rtcProvider: clearRtcProvider ? null : rtcProvider ?? this.rtcProvider,
      rtcServerUrl: clearRtcServerUrl
          ? null
          : rtcServerUrl ?? this.rtcServerUrl,
    );
  }
}
