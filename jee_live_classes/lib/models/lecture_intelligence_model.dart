class ConceptMarker {
  const ConceptMarker({required this.concept, required this.timestampSeconds});

  final String concept;
  final int timestampSeconds;
}

class FlashcardModel {
  const FlashcardModel({required this.front, required this.back});

  final String front;
  final String back;
}

class DoubtClusterModel {
  const DoubtClusterModel({
    required this.topic,
    required this.count,
    required this.examples,
  });

  final String topic;
  final int count;
  final List<String> examples;
}

class LectureSearchResult {
  const LectureSearchResult({
    required this.concept,
    required this.timestampSeconds,
    required this.note,
    required this.formula,
    required this.practiceQuestion,
  });

  final String concept;
  final int timestampSeconds;
  final String note;
  final String formula;
  final String practiceQuestion;
}

class LectureIntelligenceModel {
  const LectureIntelligenceModel({
    required this.concepts,
    required this.conceptGraph,
    required this.formulas,
    required this.importantPoints,
    required this.conceptSummaries,
    required this.flashcards,
    required this.adaptivePractice,
    required this.masteryScores,
    required this.doubtClusters,
    required this.teacherInsights,
    required this.revisionRecommendations,
    required this.miniQuizSuggestion,
    required this.knowledgeVaultEntries,
  });

  final List<ConceptMarker> concepts;
  final Map<String, List<String>> conceptGraph;
  final List<String> formulas;
  final List<String> importantPoints;
  final List<String> conceptSummaries;
  final List<FlashcardModel> flashcards;
  final Map<String, List<String>> adaptivePractice;
  final Map<String, double> masteryScores;
  final List<DoubtClusterModel> doubtClusters;
  final List<String> teacherInsights;
  final Map<String, List<String>> revisionRecommendations;
  final String? miniQuizSuggestion;
  final int knowledgeVaultEntries;

  LectureIntelligenceModel copyWith({
    List<ConceptMarker>? concepts,
    Map<String, List<String>>? conceptGraph,
    List<String>? formulas,
    List<String>? importantPoints,
    List<String>? conceptSummaries,
    List<FlashcardModel>? flashcards,
    Map<String, List<String>>? adaptivePractice,
    Map<String, double>? masteryScores,
    List<DoubtClusterModel>? doubtClusters,
    List<String>? teacherInsights,
    Map<String, List<String>>? revisionRecommendations,
    String? miniQuizSuggestion,
    bool clearMiniQuiz = false,
    int? knowledgeVaultEntries,
  }) {
    return LectureIntelligenceModel(
      concepts: concepts ?? this.concepts,
      conceptGraph: conceptGraph ?? this.conceptGraph,
      formulas: formulas ?? this.formulas,
      importantPoints: importantPoints ?? this.importantPoints,
      conceptSummaries: conceptSummaries ?? this.conceptSummaries,
      flashcards: flashcards ?? this.flashcards,
      adaptivePractice: adaptivePractice ?? this.adaptivePractice,
      masteryScores: masteryScores ?? this.masteryScores,
      doubtClusters: doubtClusters ?? this.doubtClusters,
      teacherInsights: teacherInsights ?? this.teacherInsights,
      revisionRecommendations:
          revisionRecommendations ?? this.revisionRecommendations,
      miniQuizSuggestion: clearMiniQuiz
          ? null
          : miniQuizSuggestion ?? this.miniQuizSuggestion,
      knowledgeVaultEntries:
          knowledgeVaultEntries ?? this.knowledgeVaultEntries,
    );
  }

  static const LectureIntelligenceModel empty = LectureIntelligenceModel(
    concepts: [],
    conceptGraph: {'Electrostatics': []},
    formulas: [],
    importantPoints: [],
    conceptSummaries: [],
    flashcards: [],
    adaptivePractice: {'easy': [], 'medium': [], 'advanced': []},
    masteryScores: {},
    doubtClusters: [],
    teacherInsights: [],
    revisionRecommendations: {},
    miniQuizSuggestion: null,
    knowledgeVaultEntries: 0,
  );
}
