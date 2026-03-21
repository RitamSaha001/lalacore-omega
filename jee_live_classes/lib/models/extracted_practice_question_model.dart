enum ExtractedQuestionReviewStatus { pending, approved, rejected, edited }

class ExtractedPracticeQuestionModel {
  const ExtractedPracticeQuestionModel({
    required this.id,
    required this.question,
    required this.solutionSteps,
    required this.finalAnswer,
    required this.conceptTags,
    required this.difficulty,
    required this.timestampSeconds,
    required this.createdAt,
    this.reviewStatus = ExtractedQuestionReviewStatus.pending,
    this.reviewedBy,
    this.reviewedAt,
    this.reviewerComment,
    this.editedQuestion,
  });

  final String id;
  final String question;
  final List<String> solutionSteps;
  final String finalAnswer;
  final List<String> conceptTags;
  final String difficulty;
  final int timestampSeconds;
  final DateTime createdAt;
  final ExtractedQuestionReviewStatus reviewStatus;
  final String? reviewedBy;
  final DateTime? reviewedAt;
  final String? reviewerComment;
  final String? editedQuestion;

  String get effectiveQuestion =>
      (editedQuestion ?? '').trim().isEmpty ? question : editedQuestion!;

  ExtractedPracticeQuestionModel copyWith({
    String? id,
    String? question,
    List<String>? solutionSteps,
    String? finalAnswer,
    List<String>? conceptTags,
    String? difficulty,
    int? timestampSeconds,
    DateTime? createdAt,
    ExtractedQuestionReviewStatus? reviewStatus,
    String? reviewedBy,
    bool clearReviewedBy = false,
    DateTime? reviewedAt,
    bool clearReviewedAt = false,
    String? reviewerComment,
    bool clearReviewerComment = false,
    String? editedQuestion,
    bool clearEditedQuestion = false,
  }) {
    return ExtractedPracticeQuestionModel(
      id: id ?? this.id,
      question: question ?? this.question,
      solutionSteps: solutionSteps ?? this.solutionSteps,
      finalAnswer: finalAnswer ?? this.finalAnswer,
      conceptTags: conceptTags ?? this.conceptTags,
      difficulty: difficulty ?? this.difficulty,
      timestampSeconds: timestampSeconds ?? this.timestampSeconds,
      createdAt: createdAt ?? this.createdAt,
      reviewStatus: reviewStatus ?? this.reviewStatus,
      reviewedBy: clearReviewedBy ? null : reviewedBy ?? this.reviewedBy,
      reviewedAt: clearReviewedAt ? null : reviewedAt ?? this.reviewedAt,
      reviewerComment: clearReviewerComment
          ? null
          : reviewerComment ?? this.reviewerComment,
      editedQuestion: clearEditedQuestion
          ? null
          : editedQuestion ?? this.editedQuestion,
    );
  }
}
