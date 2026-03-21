import 'package:hive_flutter/hive_flutter.dart';

import '../models/lecture_intelligence_model.dart';

class IntelligenceStorage {
  IntelligenceStorage();

  static const _vaultBoxName = 'intelligence_vault';
  static const _searchBoxName = 'intelligence_search';

  bool _initialized = false;

  Future<void> initialize() async {
    if (_initialized) {
      return;
    }

    await Hive.initFlutter();
    await Hive.openBox<Map>(_vaultBoxName);
    await Hive.openBox<List>(_searchBoxName);
    _initialized = true;
  }

  Future<void> storeLectureIntelligence({
    required String sessionId,
    required LectureIntelligenceModel intelligence,
    required String notes,
  }) async {
    await initialize();
    final vault = Hive.box<Map>(_vaultBoxName);
    final search = Hive.box<List>(_searchBoxName);

    final payload = {
      'concepts': intelligence.concepts
          .map(
            (item) => {
              'concept': item.concept,
              'timestamp_seconds': item.timestampSeconds,
            },
          )
          .toList(growable: false),
      'concept_graph': intelligence.conceptGraph,
      'formulas': intelligence.formulas,
      'flashcards': intelligence.flashcards
          .map((item) => {'front': item.front, 'back': item.back})
          .toList(growable: false),
      'adaptive_practice': intelligence.adaptivePractice,
      'mastery_scores': intelligence.masteryScores,
      'notes': notes,
      'updated_at': DateTime.now().toIso8601String(),
    };

    await vault.put(sessionId, payload);

    for (final concept in intelligence.concepts) {
      final key = concept.concept.toLowerCase();
      final existing =
          search.get(key)?.cast<Map<String, dynamic>>() ?? const [];
      final merged = [
        ...existing,
        {
          'session_id': sessionId,
          'timestamp_seconds': concept.timestampSeconds,
        },
      ];
      await search.put(key, merged);
    }
  }

  Future<Map<String, dynamic>?> loadLectureIntelligence(
    String sessionId,
  ) async {
    await initialize();
    final vault = Hive.box<Map>(_vaultBoxName);
    final map = vault.get(sessionId);
    if (map == null) {
      return null;
    }
    return Map<String, dynamic>.from(map);
  }

  Future<List<Map<String, dynamic>>> searchConcept(String query) async {
    await initialize();
    final search = Hive.box<List>(_searchBoxName);
    final hits =
        search.get(query.toLowerCase())?.cast<Map<String, dynamic>>() ??
        const [];
    return hits
        .map((item) => Map<String, dynamic>.from(item))
        .toList(growable: false);
  }
}
