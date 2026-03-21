enum DoubtQueueStatus { queued, selected, resolved }

class DoubtQueueModel {
  const DoubtQueueModel({
    required this.id,
    required this.studentId,
    required this.studentName,
    required this.question,
    required this.aiAttemptAnswer,
    required this.createdAt,
    required this.status,
    this.teacherResolution,
    this.resolvedAt,
  });

  final String id;
  final String studentId;
  final String studentName;
  final String question;
  final String aiAttemptAnswer;
  final DateTime createdAt;
  final DoubtQueueStatus status;
  final String? teacherResolution;
  final DateTime? resolvedAt;

  bool get isQueued => status == DoubtQueueStatus.queued;
  bool get isSelected => status == DoubtQueueStatus.selected;
  bool get isResolved => status == DoubtQueueStatus.resolved;

  DoubtQueueModel copyWith({
    String? id,
    String? studentId,
    String? studentName,
    String? question,
    String? aiAttemptAnswer,
    DateTime? createdAt,
    DoubtQueueStatus? status,
    String? teacherResolution,
    bool clearTeacherResolution = false,
    DateTime? resolvedAt,
    bool clearResolvedAt = false,
  }) {
    return DoubtQueueModel(
      id: id ?? this.id,
      studentId: studentId ?? this.studentId,
      studentName: studentName ?? this.studentName,
      question: question ?? this.question,
      aiAttemptAnswer: aiAttemptAnswer ?? this.aiAttemptAnswer,
      createdAt: createdAt ?? this.createdAt,
      status: status ?? this.status,
      teacherResolution: clearTeacherResolution
          ? null
          : teacherResolution ?? this.teacherResolution,
      resolvedAt: clearResolvedAt ? null : resolvedAt ?? this.resolvedAt,
    );
  }
}
