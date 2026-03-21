import 'dart:math';

import '../models/lecture_intelligence_model.dart';
import '../models/transcript_model.dart';
import 'intelligence_storage.dart';

class ClassroomIntelligenceInput {
  // BEGIN_PHASE2_IMPLEMENTATION
  const ClassroomIntelligenceInput({
    required this.transcript,
    required this.ocrText,
    required this.chatQuestions,
    required this.quizAccuracy,
    required this.participationRate,
    required this.conceptCoverage,
    required this.doubtFrequency,
  });

  final List<TranscriptModel> transcript;
  final List<String> ocrText;
  final List<String> chatQuestions;
  final double quizAccuracy;
  final double participationRate;
  final double conceptCoverage;
  final double doubtFrequency;
  // END_PHASE2_IMPLEMENTATION
}

class ClassroomIntelligenceService {
  // BEGIN_PHASE2_IMPLEMENTATION
  ClassroomIntelligenceService({
    this.storage,
  });

  final IntelligenceStorage? storage;

  static const Map<String, List<String>> _conceptKeywords = {
    'Coulomb Law': ['coulomb', 'force'],
    'Electric Field': ['electric field', 'field line'],
    'Electric Flux': ['flux', 'surface integral'],
    'Gauss Law': ['gauss', 'gaussian surface'],
    'Potential': ['potential', 'voltage'],
  };

  static const Map<String, String> _formulaMap = {
    'Coulomb Law': 'E = kq / r^2',
    'Electric Flux': 'Phi = closed integral E.dA',
    'Gauss Law': 'closed integral E.dA = q / epsilon_0',
    'Electric Field': 'E = F / q',
    'Potential': 'V = W / q',
  };

  Future<LectureIntelligenceModel> analyze(
    ClassroomIntelligenceInput input,
    LectureIntelligenceModel previous,
  ) async {
    final concepts = _extractConcepts(input);
    final graph = _buildGraph(concepts);
    final formulas = _extractFormulas(concepts, input.ocrText);
    final important = _extractImportantPoints(input.transcript);
    final summaries = _buildConceptSummaries(concepts, input.transcript);
    final flashcards = _buildFlashcards(concepts);
    final doubtClusters = _clusterDoubts(input.chatQuestions);

    final mastery = _estimateMastery(
      concepts: concepts,
      quizAccuracy: input.quizAccuracy,
      participationRate: input.participationRate,
      conceptCoverage: input.conceptCoverage,
      doubtFrequency: input.doubtFrequency,
      doubtClusters: doubtClusters,
    );

    final adaptivePractice = _buildAdaptivePractice(mastery);
    final revision = _buildRevisionRecommendations(mastery);
    final insights = _buildTeacherInsights(doubtClusters, mastery);
    final miniQuiz = _suggestMiniQuiz(concepts, doubtClusters);

    return previous.copyWith(
      concepts: concepts,
      conceptGraph: graph,
      formulas: formulas,
      importantPoints: important,
      conceptSummaries: summaries,
      flashcards: flashcards,
      adaptivePractice: adaptivePractice,
      masteryScores: mastery,
      doubtClusters: doubtClusters,
      teacherInsights: insights,
      revisionRecommendations: revision,
      miniQuizSuggestion: miniQuiz,
      knowledgeVaultEntries: previous.knowledgeVaultEntries + concepts.length,
    );
  }

  Future<List<LectureSearchResult>> search({
    required String query,
    required LectureIntelligenceModel intelligence,
  }) async {
    final normalized = query.toLowerCase();

    final conceptHits = intelligence.concepts
        .where((item) => item.concept.toLowerCase().contains(normalized))
        .toList(growable: false);

    if (conceptHits.isEmpty && storage != null) {
      final persisted = await storage!.searchConcept(query);
      return persisted
          .map(
            (item) => LectureSearchResult(
              concept: query,
              timestampSeconds: (item['timestamp_seconds'] as num?)?.toInt() ?? 0,
              note: 'Found in intelligence vault session ${item['session_id']}.',
              formula: intelligence.formulas.isNotEmpty
                  ? intelligence.formulas.first
                  : 'No formula indexed yet.',
              practiceQuestion:
                  intelligence.adaptivePractice['medium']?.firstOrNull ??
                  'Practice question pending generation.',
            ),
          )
          .toList(growable: false);
    }

    return conceptHits
        .map((concept) {
          final formula = intelligence.formulas.firstWhere(
            (line) => line.toLowerCase().contains(normalized),
            orElse: () => intelligence.formulas.isNotEmpty
                ? intelligence.formulas.first
                : 'No formula indexed yet.',
          );

          return LectureSearchResult(
            concept: concept.concept,
            timestampSeconds: concept.timestampSeconds,
            note: intelligence.conceptSummaries.firstWhere(
              (summary) =>
                  summary.toLowerCase().contains(concept.concept.toLowerCase()),
              orElse: () => 'Summary unavailable for this concept.',
            ),
            formula: formula,
            practiceQuestion:
                intelligence.adaptivePractice['medium']?.firstOrNull ??
                'Practice question pending generation.',
          );
        })
        .toList(growable: false);
  }

  List<ConceptMarker> _extractConcepts(ClassroomIntelligenceInput input) {
    final detected = <ConceptMarker>[];

    for (final entry in _conceptKeywords.entries) {
      final concept = entry.key;
      final keywords = entry.value;

      for (var i = 0; i < input.transcript.length; i += 1) {
        final sentence = input.transcript[i].message.toLowerCase();
        if (keywords.any(sentence.contains)) {
          detected.add(
            ConceptMarker(concept: concept, timestampSeconds: i * 40),
          );
          break;
        }
      }

      if (!detected.any((item) => item.concept == concept)) {
        for (var i = 0; i < input.ocrText.length; i += 1) {
          final line = input.ocrText[i].toLowerCase();
          if (keywords.any(line.contains)) {
            detected.add(
              ConceptMarker(concept: concept, timestampSeconds: i * 55),
            );
            break;
          }
        }
      }
    }

    if (detected.isEmpty && input.transcript.isNotEmpty) {
      detected.add(
        const ConceptMarker(
          concept: 'Lecture Core Concept',
          timestampSeconds: 0,
        ),
      );
    }

    detected.sort((a, b) => a.timestampSeconds.compareTo(b.timestampSeconds));
    return detected;
  }

  Map<String, List<String>> _buildGraph(List<ConceptMarker> concepts) {
    final graph = <String, List<String>>{
      'Electrostatics': concepts
          .map((item) => item.concept)
          .toList(growable: false),
    };

    for (var i = 0; i < concepts.length; i += 1) {
      final current = concepts[i].concept;
      final next = i + 1 < concepts.length ? concepts[i + 1].concept : null;
      graph[current] = next == null ? const [] : [next];
    }

    return graph;
  }

  List<String> _extractFormulas(
    List<ConceptMarker> concepts,
    List<String> ocrText,
  ) {
    final formulas = <String>{
      for (final concept in concepts)
        if (_formulaMap.containsKey(concept.concept)) _formulaMap[concept.concept]!,
    };

    final formulaLike = RegExp(
      r'[=]|integral|epsilon|sigma|\^2',
      caseSensitive: false,
    );
    for (final line in ocrText) {
      if (formulaLike.hasMatch(line)) {
        formulas.add(line);
      }
    }

    return formulas.toList(growable: false);
  }

  List<String> _extractImportantPoints(List<TranscriptModel> transcript) {
    final points = <String>[];
    for (final line in transcript) {
      final lower = line.message.toLowerCase();
      if (lower.contains('important') ||
          lower.contains('always') ||
          lower.contains('never') ||
          lower.contains('shortcut')) {
        points.add(line.message);
      }
    }

    if (points.isEmpty && transcript.isNotEmpty) {
      points.add(
        'Important JEE point: verify symmetry before applying Gauss law shortcuts.',
      );
    }

    return points;
  }

  List<String> _buildConceptSummaries(
    List<ConceptMarker> concepts,
    List<TranscriptModel> transcript,
  ) {
    final latest = transcript.isNotEmpty ? transcript.last.message : '';
    return concepts
        .map(
          (concept) =>
              '${concept.concept}: quick revision summary from lecture context. '
              'Latest context: $latest',
        )
        .toList(growable: false);
  }

  List<FlashcardModel> _buildFlashcards(List<ConceptMarker> concepts) {
    return concepts
        .map(
          (concept) => FlashcardModel(
            front: 'What is ${concept.concept}?',
            back:
                '${concept.concept} is a key electrostatics idea discussed around '
                '${_formatDuration(concept.timestampSeconds)}.',
          ),
        )
        .toList(growable: false);
  }

  List<DoubtClusterModel> _clusterDoubts(List<String> chatQuestions) {
    final flux = <String>[];
    final gauss = <String>[];
    final field = <String>[];

    for (final question in chatQuestions) {
      final lower = question.toLowerCase();
      if (lower.contains('flux')) {
        flux.add(question);
      } else if (lower.contains('gauss') || lower.contains('surface')) {
        gauss.add(question);
      } else if (lower.contains('field')) {
        field.add(question);
      }
    }

    final clusters = <DoubtClusterModel>[];
    if (flux.isNotEmpty) {
      clusters.add(
        DoubtClusterModel(
          topic: 'Electric flux definition',
          count: flux.length,
          examples: flux.take(2).toList(),
        ),
      );
    }
    if (gauss.isNotEmpty) {
      clusters.add(
        DoubtClusterModel(
          topic: 'Gaussian surface meaning',
          count: gauss.length,
          examples: gauss.take(2).toList(),
        ),
      );
    }
    if (field.isNotEmpty) {
      clusters.add(
        DoubtClusterModel(
          topic: 'Electric field direction',
          count: field.length,
          examples: field.take(2).toList(),
        ),
      );
    }

    return clusters;
  }

  Map<String, double> _estimateMastery({
    required List<ConceptMarker> concepts,
    required double quizAccuracy,
    required double participationRate,
    required double conceptCoverage,
    required double doubtFrequency,
    required List<DoubtClusterModel> doubtClusters,
  }) {
    final random = Random(13);
    final penalty = doubtClusters.fold<double>(
      0,
      (sum, item) => sum + item.count * 0.03,
    );

    return {
      for (final concept in concepts)
        concept.concept:
            ((quizAccuracy * 0.45 +
                        participationRate * 0.20 +
                        conceptCoverage * 0.35 -
                        penalty -
                        doubtFrequency * 0.15) +
                    random.nextDouble() * 0.08)
                .clamp(0.2, 0.98),
    };
  }

  Map<String, List<String>> _buildAdaptivePractice(
    Map<String, double> mastery,
  ) {
    final weak = mastery.entries
        .where((entry) => entry.value < 0.65)
        .map((entry) => entry.key)
        .toList(growable: false);

    return {
      'easy': [
        'Define electric flux and solve one direct substitution problem.',
        'Compute field due to point charge at fixed distance.',
      ],
      'medium': ['Use Gauss law for uniformly charged sphere and sketch E(r).'],
      'advanced': [
        'Solve multi-region Gauss law question with piecewise density.',
        if (weak.isNotEmpty)
          'Advanced remediation set focused on: ${weak.join(', ')}',
      ],
    };
  }

  Map<String, List<String>> _buildRevisionRecommendations(
    Map<String, double> mastery,
  ) {
    final result = <String, List<String>>{};
    mastery.forEach((concept, score) {
      result[concept] = [
        if (score < 0.7)
          'Watch concept replay segment and revisit formula sheet.',
        if (score < 0.7) 'Attempt adaptive medium set before advanced set.',
        if (score >= 0.7) 'Attempt advanced timed quiz for speed optimization.',
      ];
    });
    return result;
  }

  List<String> _buildTeacherInsights(
    List<DoubtClusterModel> clusters,
    Map<String, double> mastery,
  ) {
    final weak = mastery.entries
        .where((entry) => entry.value < 0.65)
        .map((entry) => entry.key)
        .toList(growable: false);

    final insights = <String>[];

    if (weak.isNotEmpty) {
      insights.add('Students struggling with ${weak.join(', ')}.');
    }
    if (clusters.isNotEmpty) {
      insights.add(
        'Common doubts: ${clusters.map((item) => item.topic).join(', ')}.',
      );
    }
    if (insights.isEmpty) {
      insights.add('Engagement stable. Consider launching a quick advanced quiz.');
    }

    return insights;
  }

  String? _suggestMiniQuiz(
    List<ConceptMarker> concepts,
    List<DoubtClusterModel> clusters,
  ) {
    if (clusters.isEmpty || concepts.isEmpty) {
      return null;
    }
    return 'Quick Check Quiz: ${clusters.first.topic}';
  }

  String _formatDuration(int seconds) {
    final minutes = (seconds ~/ 60).toString().padLeft(2, '0');
    final remain = (seconds % 60).toString().padLeft(2, '0');
    return '$minutes:$remain';
  }
  // END_PHASE2_IMPLEMENTATION
}

extension<T> on List<T> {
  T? get firstOrNull => isEmpty ? null : first;
}
