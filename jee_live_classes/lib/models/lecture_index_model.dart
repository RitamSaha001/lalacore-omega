class LectureIndexModel {
  const LectureIndexModel({
    required this.timestampSeconds,
    required this.topic,
    required this.summary,
  });

  final int timestampSeconds;
  final String topic;
  final String summary;

  LectureIndexModel copyWith({
    int? timestampSeconds,
    String? topic,
    String? summary,
  }) {
    return LectureIndexModel(
      timestampSeconds: timestampSeconds ?? this.timestampSeconds,
      topic: topic ?? this.topic,
      summary: summary ?? this.summary,
    );
  }
}
