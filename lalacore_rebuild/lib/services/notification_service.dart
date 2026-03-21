import 'dart:convert';
import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:shared_preferences/shared_preferences.dart';

import '../models/quiz_models.dart';
import '../models/student_intel_models.dart';
import 'backend_service.dart';

class NotificationService {
  NotificationService({required BackendService backendService})
    : _backend = backendService;

  final BackendService _backend;
  static const String _premiumStatePrefix = 'ai_premium_activity_state_';
  static const String _teacherStatePrefix = 'ai_teacher_activity_state_';
  static const Duration _backendSyncMinGap = Duration(seconds: 12);

  final ValueNotifier<List<AppNotification>> notifications =
      ValueNotifier<List<AppNotification>>(<AppNotification>[]);
  final ValueNotifier<int> unreadCount = ValueNotifier<int>(0);
  final Map<String, int> _lastBackendSyncAtMs = <String, int>{};
  final Map<String, Future<void>> _backendSyncInFlight = <String, Future<void>>{};

  String _storageKey = 'ai_notifications_default';

  Future<void> initForUser({
    required String userId,
    required String role,
  }) async {
    _storageKey = 'ai_notifications_${role}_$userId';
    await _loadLocal();
    await syncBackendNotifications(userId: userId, role: role);
  }

  Future<void> _loadLocal() async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final String? raw = prefs.getString(_storageKey);
    if (raw == null || raw.isEmpty) {
      notifications.value = <AppNotification>[];
      unreadCount.value = 0;
      return;
    }
    final dynamic decoded = jsonDecode(raw);
    if (decoded is! List) {
      notifications.value = <AppNotification>[];
      unreadCount.value = 0;
      return;
    }
    final List<AppNotification> parsed = decoded
        .map(
          (dynamic e) =>
              AppNotification.fromJson(Map<String, dynamic>.from(e as Map)),
        )
        .toList();
    parsed.sort(
      (AppNotification a, AppNotification b) => b.created.compareTo(a.created),
    );
    notifications.value = parsed;
    unreadCount.value = parsed.where((AppNotification n) => !n.seen).length;
  }

  Future<void> _persist() async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final String encoded = jsonEncode(
      notifications.value.map((AppNotification e) => e.toJson()).toList(),
    );
    await prefs.setString(_storageKey, encoded);
    unreadCount.value = notifications.value
        .where((AppNotification n) => !n.seen)
        .length;
  }

  Future<void> addAiNotification({
    required String title,
    required String body,
    String type = 'ai',
    Map<String, dynamic>? payload,
  }) async {
    final List<AppNotification> next =
        List<AppNotification>.from(notifications.value)..insert(
          0,
          AppNotification(
            id: 'local_${DateTime.now().millisecondsSinceEpoch}',
            title: title,
            body: body,
            type: type,
            created: DateTime.now(),
            seen: false,
            payload: payload,
          ),
        );
    notifications.value = next;
    await _persist();
  }

  Future<void> syncBackendNotifications({
    required String userId,
    required String role,
    bool force = false,
  }) async {
    final String key = '$role|$userId';
    final int nowMs = DateTime.now().millisecondsSinceEpoch;
    final int lastMs = _lastBackendSyncAtMs[key] ?? 0;
    if (!force && nowMs - lastMs < _backendSyncMinGap.inMilliseconds) {
      return;
    }
    final Future<void>? inFlight = _backendSyncInFlight[key];
    if (inFlight != null) {
      return inFlight;
    }

    _lastBackendSyncAtMs[key] = nowMs;
    final Future<void> run = () async {
      try {
        final Map<String, dynamic> response = await _backend.getNotifications(
          userId: userId,
          role: role,
        );
        final List<dynamic> list = (response['list'] as List?) ?? <dynamic>[];
        if (list.isEmpty) {
          return;
        }

        final Map<String, AppNotification> merged = <String, AppNotification>{
          for (final AppNotification item in notifications.value) item.id: item,
        };

        for (final dynamic raw in list) {
          final Map<String, dynamic> item = Map<String, dynamic>.from(raw as Map);
          final AppNotification n = AppNotification(
            id: (item['id'] ?? '').toString(),
            title: (item['title'] ?? 'Notification').toString(),
            body: (item['body'] ?? '').toString(),
            type: (item['type'] ?? 'ai').toString(),
            created:
                DateTime.tryParse((item['created'] ?? '').toString()) ??
                DateTime.now(),
            seen: item['seen'] == true,
            payload: item['payload'] is Map
                ? Map<String, dynamic>.from(item['payload'] as Map)
                : null,
          );
          if (n.id.isNotEmpty) {
            merged[n.id] = n;
          }
        }

        final List<AppNotification> sorted = merged.values.toList()
          ..sort(
            (AppNotification a, AppNotification b) =>
                b.created.compareTo(a.created),
          );
        notifications.value = sorted;
        await _persist();
      } catch (_) {
        // Keep local state when sync fails.
      }
    }();

    _backendSyncInFlight[key] = run;
    try {
      await run;
    } finally {
      _backendSyncInFlight.remove(key);
    }
  }

  Future<void> markSeen(String id) async {
    final List<AppNotification> next = notifications.value
        .map((AppNotification n) => n.id == id ? n.copyWith(seen: true) : n)
        .toList();
    notifications.value = next;
    await _persist();
    if (!id.startsWith('local_')) {
      try {
        await _backend.markNotificationSeen(id);
      } catch (_) {}
    }
  }

  Future<void> markAllSeen() async {
    notifications.value = notifications.value
        .map((AppNotification n) => n.copyWith(seen: true))
        .toList();
    await _persist();
  }

  Future<void> deleteNotification(String id) async {
    notifications.value = notifications.value
        .where((AppNotification n) => n.id != id)
        .toList();
    await _persist();
  }

  Future<void> runPremiumStudentActivityScan({
    required String accountId,
    required String studentName,
    required List<QuizItem> quizzes,
    required List<ResultData> history,
    StudentIntelSnapshot? intel,
    bool force = false,
    Duration interval = const Duration(minutes: 12),
  }) async {
    final String safeAccountId = accountId.trim();
    if (safeAccountId.isEmpty) {
      return;
    }

    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final String stateKey = '$_premiumStatePrefix$safeAccountId';
    final Map<String, dynamic> state = _decodeState(prefs.getString(stateKey));
    final int nowMs = DateTime.now().millisecondsSinceEpoch;
    final int lastScanMs = _readInt(state['last_scan']);
    if (!force && nowMs - lastScanMs < interval.inMilliseconds) {
      return;
    }

    final Map<String, int> signalAt = _readSignalMap(state['signal_at']);
    final List<_PremiumNotificationCandidate> candidates =
        _buildPremiumCandidates(
          studentName: studentName,
          quizzes: quizzes,
          history: history,
          intel: intel,
        );

    int emitted = 0;
    for (final _PremiumNotificationCandidate candidate in candidates) {
      final int lastSignalMs = signalAt[candidate.signalKey] ?? 0;
      final bool coolDownPassed =
          nowMs - lastSignalMs >= candidate.cooldown.inMilliseconds;
      if (!coolDownPassed) {
        continue;
      }
      if (_hasRecentDuplicate(candidate.title, candidate.body)) {
        signalAt[candidate.signalKey] = nowMs;
        continue;
      }

      await addAiNotification(
        title: candidate.title,
        body: candidate.body,
        type: 'ai_premium',
        payload: <String, dynamic>{
          'signal': candidate.signalKey,
          'tier': 'premium',
          'engine': 'lalacore',
          ...candidate.payload,
        },
      );
      signalAt[candidate.signalKey] = nowMs;
      emitted += 1;
      if (emitted >= 3) {
        break;
      }
    }

    state['last_scan'] = nowMs;
    state['signal_at'] = signalAt;
    await prefs.setString(stateKey, jsonEncode(state));
  }

  Future<void> runTeacherPerformanceScan({
    required String teacherId,
    required List<Map<String, dynamic>> exams,
    required List<Map<String, dynamic>> homeworks,
    required Map<String, List<dynamic>> studentHistory,
    bool force = false,
    Duration interval = const Duration(minutes: 7),
  }) async {
    final String safeTeacherId = teacherId.trim();
    if (safeTeacherId.isEmpty) {
      return;
    }
    if (studentHistory.isEmpty && exams.isEmpty && homeworks.isEmpty) {
      return;
    }

    final SharedPreferences prefs = await SharedPreferences.getInstance();
    final String stateKey = '$_teacherStatePrefix$safeTeacherId';
    final Map<String, dynamic> state = _decodeState(prefs.getString(stateKey));
    final int nowMs = DateTime.now().millisecondsSinceEpoch;
    final int lastScanMs = _readInt(state['last_scan']);
    if (!force && nowMs - lastScanMs < interval.inMilliseconds) {
      return;
    }

    final Map<String, dynamic>? latestAssessment = _latestAssessment(
      exams: exams,
      homeworks: homeworks,
    );
    final _TeacherAssessmentSummary? latestSummary = latestAssessment == null
        ? null
        : _summarizeAssessment(latestAssessment);
    final String latestFingerprint = latestSummary == null
        ? ''
        : _teacherAssessmentFingerprint(latestSummary);
    final String prevTestFingerprint = (state['last_test_fingerprint'] ?? '')
        .toString();
    if (latestSummary != null &&
        latestFingerprint.isNotEmpty &&
        (force || latestFingerprint != prevTestFingerprint)) {
      await addAiNotification(
        title: 'Teacher AI: ${latestSummary.title}',
        body:
            'Test quality: ${latestSummary.qualityLabel}. Avg ${latestSummary.averagePercent.toStringAsFixed(1)}%. '
            'Good ${latestSummary.goodCount}, Average ${latestSummary.averageCount}, Bad ${latestSummary.badCount}.',
        type: 'ai_teacher',
        payload: <String, dynamic>{
          'assessment': latestSummary.title,
          'avg_percent': latestSummary.averagePercent,
          'quality': latestSummary.qualityLabel,
          'good_count': latestSummary.goodCount,
          'average_count': latestSummary.averageCount,
          'bad_count': latestSummary.badCount,
          'submission_count': latestSummary.submissionCount,
        },
      );
      state['last_test_fingerprint'] = latestFingerprint;
    }

    final List<_TeacherStudentStatus> studentStatuses =
        _buildTeacherStudentStatuses(studentHistory);
    final String studentFingerprint = _teacherStudentFingerprint(
      studentStatuses,
    );
    final String prevStudentFingerprint =
        (state['last_student_fingerprint'] ?? '').toString();
    if (studentStatuses.isNotEmpty &&
        studentFingerprint.isNotEmpty &&
        (force || studentFingerprint != prevStudentFingerprint)) {
      final int goodCount = studentStatuses
          .where((_TeacherStudentStatus e) => e.band == 'good')
          .length;
      final int avgCount = studentStatuses
          .where((_TeacherStudentStatus e) => e.band == 'average')
          .length;
      final int badCount = studentStatuses
          .where((_TeacherStudentStatus e) => e.band == 'bad')
          .length;
      final String digest = studentStatuses
          .map(
            (_TeacherStudentStatus e) =>
                '${e.name}: ${e.label} (${e.averagePercent.toStringAsFixed(0)}%)',
          )
          .join(' | ');
      await addAiNotification(
        title: 'Teacher AI: Student Performance Scan',
        body: 'Good $goodCount, Average $avgCount, Bad $badCount. $digest',
        type: 'ai_teacher',
        payload: <String, dynamic>{
          'good_count': goodCount,
          'average_count': avgCount,
          'bad_count': badCount,
          'students': studentStatuses
              .map((_TeacherStudentStatus e) => e.toJson())
              .toList(),
        },
      );
      state['last_student_fingerprint'] = studentFingerprint;
    }

    state['last_scan'] = nowMs;
    await prefs.setString(stateKey, jsonEncode(state));
  }

  bool _hasRecentDuplicate(String title, String body) {
    final DateTime cutoff = DateTime.now().subtract(const Duration(hours: 2));
    for (final AppNotification n in notifications.value) {
      if (n.created.isBefore(cutoff)) {
        continue;
      }
      if (n.title == title && n.body == body) {
        return true;
      }
    }
    return false;
  }

  List<_PremiumNotificationCandidate> _buildPremiumCandidates({
    required String studentName,
    required List<QuizItem> quizzes,
    required List<ResultData> history,
    required StudentIntelSnapshot? intel,
  }) {
    final DateTime now = DateTime.now();
    final Set<String> attempted = history
        .map((ResultData e) => e.quizId.trim())
        .where((String e) => e.isNotEmpty)
        .toSet();
    final List<QuizItem> missedHomework = quizzes
        .where(
          (QuizItem q) =>
              q.type.trim().toLowerCase() == 'homework' &&
              now.isAfter(q.deadline.add(const Duration(minutes: 2))),
        )
        .toList();
    final List<QuizItem> missedExams = quizzes
        .where(
          (QuizItem q) =>
              q.type.trim().toLowerCase() == 'exam' &&
              now.isAfter(q.deadline.add(const Duration(minutes: 2))),
        )
        .toList();
    final List<QuizItem> unresolvedHomework = missedHomework
        .where((QuizItem q) => !attempted.contains(q.id))
        .toList();
    final List<QuizItem> unresolvedExams = missedExams
        .where((QuizItem q) => !attempted.contains(q.id))
        .toList();
    final List<QuizItem> dueSoon = quizzes
        .where(
          (QuizItem q) =>
              q.deadline.isAfter(now) &&
              q.deadline.isBefore(now.add(const Duration(hours: 10))) &&
              !attempted.contains(q.id),
        )
        .toList();

    final List<double> percentages = history
        .map(
          (ResultData r) =>
              ((r.score / (r.maxScore == 0 ? 1 : r.maxScore)) * 100).clamp(
                0.0,
                100.0,
              ),
        )
        .toList();
    final double recentAvg = _windowAverage(percentages, from: 0, take: 3);
    final double prevAvg = _windowAverage(percentages, from: 3, take: 3);
    final double trendDelta = percentages.length >= 2
        ? (prevAvg == 0
              ? percentages.first - percentages.last
              : recentAvg - prevAvg)
        : 0.0;

    final List<String> weakTopics =
        intel != null && intel.weakConcepts.isNotEmpty
        ? intel.weakConcepts.take(3).toList()
        : _extractWeakTopicsFromHistory(history);

    final ResultData? latest = history.isEmpty ? null : history.first;
    final int latestTotal = latest == null
        ? 0
        : latest.correct + latest.wrong + latest.skipped;
    final double latestSkipRatio = latestTotal == 0
        ? 0
        : latest!.skipped / latestTotal;

    final List<_PremiumNotificationCandidate> out =
        <_PremiumNotificationCandidate>[];

    if (unresolvedExams.isNotEmpty) {
      final String names = unresolvedExams
          .take(2)
          .map((QuizItem q) => _cleanQuizName(q.title))
          .join(', ');
      out.add(
        _PremiumNotificationCandidate(
          title: 'Critical: Missed Exam Activity',
          body:
              '$studentName has ${unresolvedExams.length} missed exam(s) pending review: $names.',
          signalKey: 'missed_exam',
          priority: 100,
          cooldown: const Duration(hours: 6),
          payload: <String, dynamic>{
            'missed_exam_count': unresolvedExams.length,
          },
        ),
      );
    }

    if (unresolvedHomework.isNotEmpty) {
      final String names = unresolvedHomework
          .take(2)
          .map((QuizItem q) => _cleanQuizName(q.title))
          .join(', ');
      out.add(
        _PremiumNotificationCandidate(
          title: 'Missed Homework Alert',
          body:
              '$studentName has ${unresolvedHomework.length} missed homework(s): $names. Clear one today to recover momentum.',
          signalKey: 'missed_homework',
          priority: 95,
          cooldown: const Duration(hours: 8),
          payload: <String, dynamic>{
            'missed_homework_count': unresolvedHomework.length,
          },
        ),
      );
    }

    if (trendDelta <= -5) {
      out.add(
        _PremiumNotificationCandidate(
          title: 'Performance Dip Detected',
          body:
              'Recent score trend dropped by ${trendDelta.abs().toStringAsFixed(1)}%. Rework mistakes before starting new sets.',
          signalKey: 'trend_down',
          priority: 92,
          cooldown: const Duration(hours: 8),
          payload: <String, dynamic>{'trend_delta': trendDelta},
        ),
      );
    } else if (trendDelta >= 5) {
      out.add(
        _PremiumNotificationCandidate(
          title: 'Improvement Streak',
          body:
              'Recent average is up by +${trendDelta.toStringAsFixed(1)}%. Keep timed mixed practice to lock the gains.',
          signalKey: 'trend_up',
          priority: 60,
          cooldown: const Duration(hours: 10),
          payload: <String, dynamic>{'trend_delta': trendDelta},
        ),
      );
    }

    if (weakTopics.isNotEmpty) {
      final String topics = weakTopics.join(', ');
      out.add(
        _PremiumNotificationCandidate(
          title: 'Weak Topics Identified',
          body:
              'Focus topic recovery now: $topics. Do a focused 30-minute drill before your next quiz.',
          signalKey: 'weak_${weakTopics.map(_slug).join('_')}',
          priority: 90,
          cooldown: const Duration(hours: 10),
          payload: <String, dynamic>{'weak_topics': weakTopics},
        ),
      );
    }

    if (dueSoon.isNotEmpty) {
      final String closest = dueSoon
          .map((QuizItem q) => q.deadline)
          .reduce((DateTime a, DateTime b) => a.isBefore(b) ? a : b)
          .difference(now)
          .inHours
          .clamp(0, 999)
          .toString();
      out.add(
        _PremiumNotificationCandidate(
          title: 'Upcoming Deadline Window',
          body:
              '${dueSoon.length} task(s) are due soon. Earliest due in ~$closest hr. Start now to avoid carryover.',
          signalKey: 'due_soon',
          priority: 78,
          cooldown: const Duration(hours: 4),
          payload: <String, dynamic>{'due_soon_count': dueSoon.length},
        ),
      );
    }

    if (intel != null && intel.burnoutProbability > 0.66) {
      out.add(
        _PremiumNotificationCandidate(
          title: 'High Burnout Probability',
          body:
              'Fatigue risk is elevated at ${(intel.burnoutProbability * 100).toStringAsFixed(0)}%. Shorten sessions and add one recovery block.',
          signalKey: 'burnout_risk',
          priority: 88,
          cooldown: const Duration(hours: 10),
          payload: <String, dynamic>{
            'burnout_probability': intel.burnoutProbability,
          },
        ),
      );
    }

    if (latestSkipRatio >= 0.34 && latest != null) {
      out.add(
        _PremiumNotificationCandidate(
          title: 'High Skip Ratio In Last Attempt',
          body:
              'Skipped ${(latestSkipRatio * 100).toStringAsFixed(0)}% in the latest attempt. Solve easiest sections first to increase coverage.',
          signalKey: 'skip_ratio',
          priority: 84,
          cooldown: const Duration(hours: 8),
          payload: <String, dynamic>{'skip_ratio': latestSkipRatio},
        ),
      );
    }

    out.sort(
      (_PremiumNotificationCandidate a, _PremiumNotificationCandidate b) =>
          b.priority.compareTo(a.priority),
    );
    return out;
  }

  String _cleanQuizName(String raw) {
    final String name = raw.split('|').first.trim();
    return name.isEmpty ? 'Untitled' : name;
  }

  List<String> _extractWeakTopicsFromHistory(List<ResultData> history) {
    if (history.isEmpty) {
      return <String>[];
    }
    final Map<String, List<double>> byTopic = <String, List<double>>{};
    for (final ResultData r in history.take(5)) {
      r.sectionAccuracy.forEach((String section, double value) {
        final String key = section.trim().isEmpty ? 'General' : section.trim();
        byTopic.putIfAbsent(key, () => <double>[]).add(value);
      });
    }
    if (byTopic.isEmpty) {
      return <String>[];
    }
    final List<MapEntry<String, double>> ranked =
        byTopic.entries.map((MapEntry<String, List<double>> e) {
          final double avg =
              e.value.reduce((double a, double b) => a + b) / e.value.length;
          return MapEntry<String, double>(e.key, avg);
        }).toList()..sort(
          (MapEntry<String, double> a, MapEntry<String, double> b) =>
              a.value.compareTo(b.value),
        );
    return ranked
        .where((MapEntry<String, double> e) => e.value < 62)
        .take(3)
        .map((MapEntry<String, double> e) => e.key)
        .toList();
  }

  double _windowAverage(
    List<double> values, {
    required int from,
    required int take,
  }) {
    if (values.isEmpty || from >= values.length || take <= 0) {
      return 0;
    }
    final int end = min(values.length, from + take);
    final List<double> window = values.sublist(from, end);
    if (window.isEmpty) {
      return 0;
    }
    return window.reduce((double a, double b) => a + b) / window.length;
  }

  String _slug(String value) {
    return value
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9]+'), '_')
        .replaceAll(RegExp(r'_+'), '_')
        .replaceAll(RegExp(r'^_|_$'), '');
  }

  Map<String, dynamic> _decodeState(String? raw) {
    if (raw == null || raw.trim().isEmpty) {
      return <String, dynamic>{};
    }
    try {
      final dynamic decoded = jsonDecode(raw);
      if (decoded is Map) {
        return Map<String, dynamic>.from(decoded);
      }
    } catch (_) {}
    return <String, dynamic>{};
  }

  Map<String, int> _readSignalMap(dynamic raw) {
    if (raw is! Map) {
      return <String, int>{};
    }
    return Map<String, dynamic>.from(raw).map((String key, dynamic value) {
      return MapEntry<String, int>(key, _readInt(value));
    });
  }

  int _readInt(dynamic value) {
    if (value is int) {
      return value;
    }
    if (value is num) {
      return value.toInt();
    }
    return int.tryParse((value ?? '').toString()) ?? 0;
  }

  Map<String, dynamic>? _latestAssessment({
    required List<Map<String, dynamic>> exams,
    required List<Map<String, dynamic>> homeworks,
  }) {
    final List<Map<String, dynamic>> all = <Map<String, dynamic>>[
      ...exams,
      ...homeworks,
    ];
    if (all.isEmpty) {
      return null;
    }

    DateTime readDate(Map<String, dynamic> item) {
      final dynamic raw = item['date'];
      if (raw is DateTime) {
        return raw;
      }
      return DateTime.tryParse((raw ?? '').toString()) ??
          DateTime.fromMillisecondsSinceEpoch(0);
    }

    all.sort(
      (Map<String, dynamic> a, Map<String, dynamic> b) =>
          readDate(b).compareTo(readDate(a)),
    );

    for (final Map<String, dynamic> item in all) {
      final List<dynamic> subs = item['subs'] is List
          ? (item['subs'] as List)
          : <dynamic>[];
      if (subs.isNotEmpty) {
        return item;
      }
    }
    return all.first;
  }

  _TeacherAssessmentSummary _summarizeAssessment(Map<String, dynamic> item) {
    final String title = _cleanQuizName((item['title'] ?? '').toString());
    final List<dynamic> subs = item['subs'] is List
        ? (item['subs'] as List)
        : <dynamic>[];

    final List<double> percents = subs
        .map((dynamic raw) => _scorePercent(raw))
        .where((double e) => e >= 0)
        .toList();
    final double avg = percents.isEmpty
        ? 0
        : percents.reduce((double a, double b) => a + b) / percents.length;
    final int goodCount = percents.where((double e) => e >= 70).length;
    final int averageCount = percents
        .where((double e) => e >= 45 && e < 70)
        .length;
    final int badCount = percents.where((double e) => e < 45).length;

    final String qualityLabel = avg >= 65 ? 'Good' : 'Not good';
    return _TeacherAssessmentSummary(
      title: title.isEmpty ? 'Latest Test' : title,
      averagePercent: avg,
      qualityLabel: qualityLabel,
      goodCount: goodCount,
      averageCount: averageCount,
      badCount: badCount,
      submissionCount: percents.length,
    );
  }

  List<_TeacherStudentStatus> _buildTeacherStudentStatuses(
    Map<String, List<dynamic>> studentHistory,
  ) {
    final List<_TeacherStudentStatus> out = <_TeacherStudentStatus>[];
    studentHistory.forEach((String student, List<dynamic> attempts) {
      if (attempts.isEmpty) {
        return;
      }
      final List<double> percents = attempts
          .map((dynamic raw) => _scorePercent(raw))
          .where((double e) => e >= 0)
          .toList();
      if (percents.isEmpty) {
        return;
      }
      final double avg =
          percents.reduce((double a, double b) => a + b) / percents.length;
      String label = 'Average';
      String band = 'average';
      if (avg >= 70) {
        label = 'Good';
        band = 'good';
      } else if (avg < 45) {
        label = 'Bad';
        band = 'bad';
      }
      out.add(
        _TeacherStudentStatus(
          name: student.trim().isEmpty ? 'Student' : student.trim(),
          averagePercent: avg,
          label: label,
          band: band,
        ),
      );
    });
    out.sort((_TeacherStudentStatus a, _TeacherStudentStatus b) {
      final int byBand = _studentBandPriority(
        a.band,
      ).compareTo(_studentBandPriority(b.band));
      if (byBand != 0) {
        return byBand;
      }
      return a.name.toLowerCase().compareTo(b.name.toLowerCase());
    });
    return out;
  }

  int _studentBandPriority(String band) {
    switch (band) {
      case 'bad':
        return 0;
      case 'average':
        return 1;
      default:
        return 2;
    }
  }

  double _scorePercent(dynamic raw) {
    if (raw is! Map) {
      return -1;
    }
    final Map<String, dynamic> row = Map<String, dynamic>.from(raw);
    final double score =
        double.tryParse((row['score'] ?? row['marks'] ?? '').toString()) ?? 0;
    final double total =
        double.tryParse((row['total'] ?? row['max_score'] ?? '').toString()) ??
        0;

    if (total > 0) {
      return ((score / total) * 100).clamp(0.0, 100.0);
    }
    return score.clamp(0.0, 100.0);
  }

  String _teacherAssessmentFingerprint(_TeacherAssessmentSummary s) {
    return '${_slug(s.title)}|${s.qualityLabel}|'
        '${s.averagePercent.toStringAsFixed(1)}|'
        '${s.goodCount}|${s.averageCount}|${s.badCount}|${s.submissionCount}';
  }

  String _teacherStudentFingerprint(List<_TeacherStudentStatus> statuses) {
    if (statuses.isEmpty) {
      return '';
    }
    final List<String> tokens =
        statuses
            .map(
              (_TeacherStudentStatus e) =>
                  '${_slug(e.name)}:${e.band}:${e.averagePercent.toStringAsFixed(1)}',
            )
            .toList()
          ..sort();
    return tokens.join('|');
  }
}

class _PremiumNotificationCandidate {
  const _PremiumNotificationCandidate({
    required this.title,
    required this.body,
    required this.signalKey,
    required this.priority,
    required this.cooldown,
    required this.payload,
  });

  final String title;
  final String body;
  final String signalKey;
  final int priority;
  final Duration cooldown;
  final Map<String, dynamic> payload;
}

class _TeacherAssessmentSummary {
  const _TeacherAssessmentSummary({
    required this.title,
    required this.averagePercent,
    required this.qualityLabel,
    required this.goodCount,
    required this.averageCount,
    required this.badCount,
    required this.submissionCount,
  });

  final String title;
  final double averagePercent;
  final String qualityLabel;
  final int goodCount;
  final int averageCount;
  final int badCount;
  final int submissionCount;
}

class _TeacherStudentStatus {
  const _TeacherStudentStatus({
    required this.name,
    required this.averagePercent,
    required this.label,
    required this.band,
  });

  final String name;
  final double averagePercent;
  final String label;
  final String band;

  Map<String, dynamic> toJson() => <String, dynamic>{
    'name': name,
    'avg_percent': averagePercent,
    'label': label,
    'band': band,
  };
}
