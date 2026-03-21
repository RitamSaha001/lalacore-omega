import 'dart:convert';

class StudentIntelSnapshot {
  const StudentIntelSnapshot({
    required this.accountId,
    required this.generatedAtMillis,
    required this.weakConcepts,
    required this.conceptMastery,
    required this.performanceTrendEma,
    required this.trendDirection,
    required this.difficultyHandlingScore,
    required this.burnoutProbability,
    required this.improvementProbability,
    required this.consistencyScore,
    required this.recommendations,
  });

  final String accountId;
  final int generatedAtMillis;
  final List<String> weakConcepts;
  final Map<String, double> conceptMastery;
  final double performanceTrendEma;
  final String trendDirection;
  final double difficultyHandlingScore;
  final double burnoutProbability;
  final double improvementProbability;
  final double consistencyScore;
  final List<String> recommendations;

  Map<String, dynamic> toJson() => <String, dynamic>{
        'account_id': accountId,
        'generated_at': generatedAtMillis,
        'weak_concepts': weakConcepts,
        'concept_mastery': conceptMastery,
        'performance_trend_ema': performanceTrendEma,
        'trend_direction': trendDirection,
        'difficulty_handling_score': difficultyHandlingScore,
        'burnout_probability': burnoutProbability,
        'improvement_probability': improvementProbability,
        'consistency_score': consistencyScore,
        'recommendations': recommendations,
      };

  factory StudentIntelSnapshot.fromJson(Map<String, dynamic> j) {
    final Map<String, dynamic> masteryRaw = (j['concept_mastery'] is Map)
        ? Map<String, dynamic>.from(j['concept_mastery'] as Map)
        : <String, dynamic>{};

    return StudentIntelSnapshot(
      accountId: (j['account_id'] ?? '').toString(),
      generatedAtMillis: _toInt(j['generated_at']),
      weakConcepts: ((j['weak_concepts'] ?? <dynamic>[]) as List<dynamic>)
          .map((dynamic e) => e.toString())
          .toList(),
      conceptMastery: masteryRaw.map(
        (String key, dynamic value) => MapEntry(key, _toDouble(value)),
      ),
      performanceTrendEma: _toDouble(j['performance_trend_ema']),
      trendDirection: (j['trend_direction'] ?? 'stable').toString(),
      difficultyHandlingScore: _toDouble(j['difficulty_handling_score']),
      burnoutProbability: _toDouble(j['burnout_probability']),
      improvementProbability: _toDouble(j['improvement_probability']),
      consistencyScore: _toDouble(j['consistency_score']),
      recommendations: ((j['recommendations'] ?? <dynamic>[]) as List<dynamic>)
          .map((dynamic e) => e.toString())
          .toList(),
    );
  }

  String encode() => jsonEncode(toJson());

  factory StudentIntelSnapshot.decode(String raw) =>
      StudentIntelSnapshot.fromJson(jsonDecode(raw) as Map<String, dynamic>);
}

double _toDouble(dynamic value) {
  if (value is num) {
    return value.toDouble();
  }
  return double.tryParse((value ?? '').toString()) ?? 0.0;
}

int _toInt(dynamic value) {
  if (value is int) {
    return value;
  }
  if (value is num) {
    return value.toInt();
  }
  return int.tryParse((value ?? '').toString()) ?? 0;
}
