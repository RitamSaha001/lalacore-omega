import 'dart:async';

class AnalyticsSnapshot {
  const AnalyticsSnapshot({
    required this.attendance,
    required this.quizAttempts,
    required this.doubtCount,
    required this.participationRate,
  });

  final int attendance;
  final int quizAttempts;
  final int doubtCount;
  final double participationRate;

  AnalyticsSnapshot copyWith({
    int? attendance,
    int? quizAttempts,
    int? doubtCount,
    double? participationRate,
  }) {
    return AnalyticsSnapshot(
      attendance: attendance ?? this.attendance,
      quizAttempts: quizAttempts ?? this.quizAttempts,
      doubtCount: doubtCount ?? this.doubtCount,
      participationRate: participationRate ?? this.participationRate,
    );
  }
}

class AnalyticsService {
  final _controller = StreamController<AnalyticsSnapshot>.broadcast();

  AnalyticsSnapshot _snapshot = const AnalyticsSnapshot(
    attendance: 0,
    quizAttempts: 0,
    doubtCount: 0,
    participationRate: 0,
  );

  Stream<AnalyticsSnapshot> get snapshotStream => _controller.stream;

  AnalyticsSnapshot get snapshot => _snapshot;

  void onAttendanceChanged(int count) {
    _snapshot = _snapshot.copyWith(attendance: count);
    _controller.add(_snapshot);
  }

  void onQuizSubmitted() {
    _snapshot = _snapshot.copyWith(quizAttempts: _snapshot.quizAttempts + 1);
    _controller.add(_snapshot);
  }

  void onDoubtAsked() {
    _snapshot = _snapshot.copyWith(doubtCount: _snapshot.doubtCount + 1);
    _controller.add(_snapshot);
  }

  void onReactionOrHandRaise() {
    final increased = (_snapshot.participationRate + 0.03).clamp(0.0, 1.0);
    _snapshot = _snapshot.copyWith(participationRate: increased);
    _controller.add(_snapshot);
  }

  void dispose() {
    _controller.close();
  }
}
