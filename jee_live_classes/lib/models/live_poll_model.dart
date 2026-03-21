enum LivePollStatus { draft, active, ended }

class LivePollDraft {
  const LivePollDraft({
    required this.question,
    required this.options,
    required this.timerSeconds,
    this.correctOption,
    this.topic,
    this.difficulty,
  });

  final String question;
  final List<String> options;
  final int timerSeconds;
  final int? correctOption;
  final String? topic;
  final String? difficulty;

  LivePollDraft copyWith({
    String? question,
    List<String>? options,
    int? timerSeconds,
    int? correctOption,
    bool clearCorrectOption = false,
    String? topic,
    String? difficulty,
  }) {
    return LivePollDraft(
      question: question ?? this.question,
      options: options ?? this.options,
      timerSeconds: timerSeconds ?? this.timerSeconds,
      correctOption: clearCorrectOption
          ? null
          : correctOption ?? this.correctOption,
      topic: topic ?? this.topic,
      difficulty: difficulty ?? this.difficulty,
    );
  }
}

class LivePollModel {
  const LivePollModel({
    required this.pollId,
    required this.question,
    required this.options,
    required this.correctOption,
    required this.timerSeconds,
    required this.startTime,
    required this.status,
  });

  final String pollId;
  final String question;
  final List<String> options;
  final int? correctOption;
  final int timerSeconds;
  final DateTime startTime;
  final LivePollStatus status;

  LivePollModel copyWith({
    String? pollId,
    String? question,
    List<String>? options,
    int? correctOption,
    bool clearCorrectOption = false,
    int? timerSeconds,
    DateTime? startTime,
    LivePollStatus? status,
  }) {
    return LivePollModel(
      pollId: pollId ?? this.pollId,
      question: question ?? this.question,
      options: options ?? this.options,
      correctOption: clearCorrectOption
          ? null
          : correctOption ?? this.correctOption,
      timerSeconds: timerSeconds ?? this.timerSeconds,
      startTime: startTime ?? this.startTime,
      status: status ?? this.status,
    );
  }
}

class LivePollResultsModel {
  const LivePollResultsModel({
    required this.pollId,
    required this.optionCounts,
    required this.totalResponses,
    required this.correctOption,
    required this.revealed,
  });

  final String pollId;
  final Map<int, int> optionCounts;
  final int totalResponses;
  final int? correctOption;
  final bool revealed;

  double percentageFor(int optionIndex) {
    if (totalResponses <= 0) {
      return 0;
    }
    return (optionCounts[optionIndex] ?? 0) / totalResponses;
  }
}
