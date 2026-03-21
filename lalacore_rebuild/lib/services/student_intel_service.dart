import 'dart:math';

import 'package:shared_preferences/shared_preferences.dart';

import '../models/quiz_models.dart';
import '../models/student_intel_models.dart';
import 'ai_engine_service.dart';

class StudentIntelService {
  StudentIntelService({required AiEngineService aiService}) : _ai = aiService;

  final AiEngineService _ai;

  static const String _baseKey = 'AI_STUDENT_INTEL';

  String storageKey(String accountId) => '${_baseKey}_$accountId';

  Future<StudentIntelSnapshot?> load(String accountId) async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final String? raw = prefs.getString(storageKey(accountId));
    if (raw == null || raw.isEmpty) {
      return null;
    }
    try {
      return StudentIntelSnapshot.decode(raw);
    } catch (_) {
      return null;
    }
  }

  Future<StudentIntelSnapshot> buildAndStore({
    required String accountId,
    required ResultData latest,
    required List<ResultData> history,
    required int quizDurationMinutes,
  }) async {
    final List<ResultData> ordered = <ResultData>[...history, latest];
    final List<double> percentages = ordered
        .map(
          (ResultData r) =>
              ((r.score / (r.maxScore == 0 ? 1 : r.maxScore)) * 100).clamp(
                0.0,
                100.0,
              ),
        )
        .toList();

    final double ema = _computeEma(percentages, alpha: 0.35);
    final double trendDelta = percentages.length >= 2
        ? percentages.last - percentages.first
        : 0.0;
    final String trendDirection = trendDelta > 4
        ? 'up'
        : trendDelta < -4
            ? 'down'
            : 'stable';

    final double consistency = _consistencyScore(percentages);
    final double wrongRatio =
        latest.wrong / max(1, latest.correct + latest.wrong + latest.skipped);
    final double skippedRatio =
        latest.skipped / max(1, latest.correct + latest.wrong + latest.skipped);
    final double timeUsage = latest.totalTime / max(1, quizDurationMinutes * 60);

    final Map<String, double> mastery = _aggregateMastery(ordered);
    final List<String> weakConcepts = _weakConceptsFromMastery(mastery);

    final double difficultyHandling = (1.0 - (wrongRatio * 1.15) - (skippedRatio * 0.75))
        .clamp(0.0, 1.0);

    // Logistic-style burnout probability.
    final double burnoutZ =
        (1.65 * (1 - consistency)) + (1.2 * timeUsage) + (1.05 * wrongRatio) + (trendDelta < 0 ? 0.8 : 0);
    final double burnout = _sigmoid(burnoutZ - 1.45);

    // Logistic-style improvement probability.
    final double improveZ =
        (trendDelta > 0 ? trendDelta / 35 : trendDelta / 60) +
            (1.1 * consistency) +
            (0.9 * difficultyHandling) -
            (0.95 * burnout);
    final double improvement = _sigmoid(improveZ);

    final List<String> recommendations = _recommendations(
      weakConcepts: weakConcepts,
      burnout: burnout,
      improvement: improvement,
      trendDirection: trendDirection,
      mastery: mastery,
    );

    final StudentIntelSnapshot local = StudentIntelSnapshot(
      accountId: accountId,
      generatedAtMillis: DateTime.now().millisecondsSinceEpoch,
      weakConcepts: weakConcepts,
      conceptMastery: mastery,
      performanceTrendEma: ema,
      trendDirection: trendDirection,
      difficultyHandlingScore: difficultyHandling,
      burnoutProbability: burnout,
      improvementProbability: improvement,
      consistencyScore: consistency,
      recommendations: recommendations,
    );

    StudentIntelSnapshot merged = local;
    try {
      final Map<String, dynamic> remote = await _ai.studentIntelligence(
        accountId: accountId,
        latestResult: latest.toJson(),
        history: ordered.map((ResultData e) => e.toJson()).toList(),
      );
      if (remote['ok'] == true || remote['status']?.toString().toUpperCase() == 'SUCCESS') {
        final Map<String, dynamic> data = (remote['data'] is Map)
            ? Map<String, dynamic>.from(remote['data'] as Map)
            : Map<String, dynamic>.from(remote);
        merged = _mergeRemote(local, data);
      }
    } catch (_) {}

    final SharedPreferences prefs = await SharedPreferences.getInstance();
    await prefs.setString(storageKey(accountId), merged.encode());
    return merged;
  }

  StudentIntelSnapshot _mergeRemote(
    StudentIntelSnapshot local,
    Map<String, dynamic> remote,
  ) {
    final List<String> weakConcepts =
        ((remote['weak_concepts'] ?? remote['weakConcepts'] ?? local.weakConcepts) as List<dynamic>)
            .map((dynamic e) => e.toString())
            .toList();

    final Map<String, dynamic> remoteMasteryRaw = (remote['concept_mastery'] is Map)
        ? Map<String, dynamic>.from(remote['concept_mastery'] as Map)
        : (remote['conceptMastery'] is Map)
            ? Map<String, dynamic>.from(remote['conceptMastery'] as Map)
            : <String, dynamic>{};
    final Map<String, double> mastery = remoteMasteryRaw.isEmpty
        ? local.conceptMastery
        : remoteMasteryRaw.map(
            (String key, dynamic value) => MapEntry(
              key,
              value is num ? value.toDouble() : double.tryParse(value.toString()) ?? 0.0,
            ),
          );

    double readDouble(String key, double fallback) {
      final dynamic value = remote[key];
      if (value is num) {
        return value.toDouble();
      }
      final double? parsed = double.tryParse((value ?? '').toString());
      return parsed ?? fallback;
    }

    final List<String> recommendations =
        ((remote['recommendations'] ?? local.recommendations) as List<dynamic>)
            .map((dynamic e) => e.toString())
            .toList();

    return StudentIntelSnapshot(
      accountId: local.accountId,
      generatedAtMillis: local.generatedAtMillis,
      weakConcepts: weakConcepts,
      conceptMastery: mastery,
      performanceTrendEma:
          readDouble('performance_trend_ema', local.performanceTrendEma),
      trendDirection: (remote['trend_direction'] ?? local.trendDirection).toString(),
      difficultyHandlingScore:
          readDouble('difficulty_handling_score', local.difficultyHandlingScore),
      burnoutProbability:
          readDouble('burnout_probability', local.burnoutProbability),
      improvementProbability:
          readDouble('improvement_probability', local.improvementProbability),
      consistencyScore: readDouble('consistency_score', local.consistencyScore),
      recommendations: recommendations,
    );
  }

  Map<String, double> _aggregateMastery(List<ResultData> history) {
    final Map<String, List<double>> perSection = <String, List<double>>{};
    for (final ResultData r in history) {
      r.sectionAccuracy.forEach((String section, double value) {
        perSection.putIfAbsent(section, () => <double>[]).add(value);
      });
    }
    if (perSection.isEmpty) {
      return <String, double>{'General': 50};
    }
    return perSection.map((String section, List<double> values) {
      final double avg = values.reduce((double a, double b) => a + b) / values.length;
      return MapEntry(section, avg.clamp(0, 100).toDouble());
    });
  }

  List<String> _weakConceptsFromMastery(Map<String, double> mastery) {
    final List<MapEntry<String, double>> sorted = mastery.entries.toList()
      ..sort((MapEntry<String, double> a, MapEntry<String, double> b) => a.value.compareTo(b.value));
    final List<String> weak = sorted.where((MapEntry<String, double> e) => e.value < 60).map((MapEntry<String, double> e) => e.key).toList();
    if (weak.isNotEmpty) {
      return weak.take(5).toList();
    }
    return sorted.take(min(3, sorted.length)).map((MapEntry<String, double> e) => e.key).toList();
  }

  List<String> _recommendations({
    required List<String> weakConcepts,
    required double burnout,
    required double improvement,
    required String trendDirection,
    required Map<String, double> mastery,
  }) {
    final List<String> out = <String>[];
    if (weakConcepts.isNotEmpty) {
      out.add('Prioritize ${weakConcepts.take(2).join(', ')} with 30-minute focused drills.');
    }
    if (burnout > 0.62) {
      out.add('Burnout risk is elevated. Reduce session length and add one rest block daily.');
    }
    if (trendDirection == 'down') {
      out.add('Performance trend is falling. Revise solved mistakes before new problem sets.');
    }
    if (improvement > 0.65) {
      out.add('Improvement trajectory is strong. Increase mixed-difficulty timed practice.');
    }
    final List<MapEntry<String, double>> strongest = mastery.entries.toList()
      ..sort(
        (MapEntry<String, double> a, MapEntry<String, double> b) =>
            b.value.compareTo(a.value),
      );
    if (mastery.isNotEmpty) {
      out.add('Use ${strongest.first.key} as confidence anchor before weak-topic blocks.');
    }
    return out.take(5).toList();
  }

  double _computeEma(List<double> values, {required double alpha}) {
    if (values.isEmpty) {
      return 0;
    }
    double ema = values.first;
    for (int i = 1; i < values.length; i++) {
      ema = alpha * values[i] + (1 - alpha) * ema;
    }
    return ema.clamp(0.0, 100.0);
  }

  double _consistencyScore(List<double> values) {
    if (values.length <= 1) {
      return 0.55;
    }
    final double mean = values.reduce((double a, double b) => a + b) / values.length;
    double variance = 0;
    for (final double v in values) {
      variance += pow(v - mean, 2).toDouble();
    }
    variance /= values.length;
    final double std = sqrt(variance);
    return (1 - (std / 35)).clamp(0.0, 1.0);
  }

  double _sigmoid(double x) => 1 / (1 + exp(-x));
}
