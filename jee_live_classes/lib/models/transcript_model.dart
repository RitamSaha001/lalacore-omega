class TranscriptModel {
  // BEGIN_PHASE2_IMPLEMENTATION
  const TranscriptModel({
    required this.id,
    required this.speakerId,
    required this.speakerName,
    required this.message,
    required this.timestamp,
    this.confidence = 1.0,
    this.source = 'stream',
  });

  final String id;
  final String speakerId;
  final String speakerName;
  final String message;
  final DateTime timestamp;
  final double confidence;
  final String source;

  TranscriptModel copyWith({
    String? id,
    String? speakerId,
    String? speakerName,
    String? message,
    DateTime? timestamp,
    double? confidence,
    String? source,
  }) {
    return TranscriptModel(
      id: id ?? this.id,
      speakerId: speakerId ?? this.speakerId,
      speakerName: speakerName ?? this.speakerName,
      message: message ?? this.message,
      timestamp: timestamp ?? this.timestamp,
      confidence: confidence ?? this.confidence,
      source: source ?? this.source,
    );
  }
  // END_PHASE2_IMPLEMENTATION
}
