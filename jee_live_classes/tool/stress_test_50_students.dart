import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:jee_live_classes/core/app_config.dart';
import 'package:jee_live_classes/models/live_class_context.dart';
import 'package:jee_live_classes/services/class_join_service.dart';
import 'package:jee_live_classes/services/classroom_sync_service.dart';

Future<void> main(List<String> args) async {
  final students = int.tryParse(_argValue(args, '--students') ?? '') ?? 50;
  final outputPath =
      _argValue(args, '--out') ?? 'build/reports/live_class_stress_report.json';

  final report = <String, dynamic>{
    'started_at': DateTime.now().toUtc().toIso8601String(),
    'mode': 'simulation',
    'students': students,
  };

  final config = AppConfig.fromEnvironment();
  final baseContext = config.toLiveClassContext();

  final joinService = MockClassJoinService();
  final syncService = MockClassroomSyncService();

  final contexts = List<LiveClassContext>.generate(students, (index) {
    return LiveClassContext(
      userId: 'sim_student_${index + 1}',
      userName: 'Sim Student ${index + 1}',
      role: 'student',
      classId: baseContext.classId,
      sessionToken: baseContext.sessionToken,
      classTitle: baseContext.classTitle,
      subject: baseContext.subject,
      topic: baseContext.topic,
      teacherName: baseContext.teacherName,
      startTimeLabel: baseContext.startTimeLabel,
    );
  });

  final joinLatency = <int>[];
  final joinRequestIds = <String>[];

  for (final context in contexts) {
    await joinService.startPresenceSubscription(context);
  }

  final joinTasks = contexts.map((context) async {
    final sw = Stopwatch()..start();
    final requestId = await joinService.requestJoin(
      context: context,
      deviceInfo: {'platform': Platform.operatingSystem, 'simulation': true},
      cameraEnabled: true,
      micEnabled: true,
    );
    sw.stop();
    joinLatency.add(sw.elapsedMilliseconds);
    joinRequestIds.add(requestId);
  });
  await Future.wait(joinTasks);

  final syncLatency = <int>[];
  final pending = <String, Completer<void>>{};
  int nowMicros() => DateTime.now().microsecondsSinceEpoch;

  final sub = syncService.events.listen((event) {
    final probeId = (event.metadata['probe_id'] ?? '').toString();
    if (probeId.isEmpty) {
      return;
    }
    final sendMicros = int.tryParse(
      (event.metadata['sent_micros'] ?? '').toString(),
    );
    if (sendMicros == null) {
      return;
    }
    final completer = pending[probeId];
    if (completer == null || completer.isCompleted) {
      return;
    }
    final latency = max(0, ((nowMicros() - sendMicros) / 1000).round());
    syncLatency.add(latency);
    completer.complete();
  });

  final syncTasks = contexts.map((context) async {
    final probeId =
        'probe_${context.userId}_${DateTime.now().microsecondsSinceEpoch}';
    final completer = Completer<void>();
    pending[probeId] = completer;

    await syncService.publish(
      ClassroomSyncEvent(
        type: ClassroomSyncEventType.raiseHand,
        classId: context.classId,
        senderId: context.userId,
        timestamp: DateTime.now().toUtc(),
        metadata: {'probe_id': probeId, 'sent_micros': nowMicros().toString()},
      ),
    );

    await completer.future.timeout(
      const Duration(seconds: 2),
      onTimeout: () {},
    );
    pending.remove(probeId);
  });
  await Future.wait(syncTasks);
  await sub.cancel();

  var failoverSuccess = 0;
  var failoverFailures = 0;
  final failoverStateHistogram = <String, int>{'connected': 0, 'failed': 0};

  for (final context in contexts) {
    try {
      final token = await joinService.fetchWebRtcFallbackToken(
        classId: context.classId,
        userId: context.userId,
      );
      final tokenPresent =
          token != null &&
          (token['token'] ?? '').toString().trim().isNotEmpty &&
          (token['url'] ?? '').toString().trim().isNotEmpty;
      if (tokenPresent) {
        failoverStateHistogram['connected'] =
            (failoverStateHistogram['connected'] ?? 0) + 1;
        failoverSuccess += 1;
      } else {
        failoverStateHistogram['failed'] =
            (failoverStateHistogram['failed'] ?? 0) + 1;
        failoverFailures += 1;
      }
    } catch (_) {
      failoverStateHistogram['failed'] =
          (failoverStateHistogram['failed'] ?? 0) + 1;
      failoverFailures += 1;
    }
  }

  for (var i = 0; i < contexts.length && i < joinRequestIds.length; i += 1) {
    await joinService.cancelJoinRequest(
      context: contexts[i],
      requestId: joinRequestIds[i],
    );
  }

  joinService.dispose();
  syncService.dispose();

  report['join'] = {
    'count': joinLatency.length,
    'p50_ms': _percentile(joinLatency, 0.50),
    'p95_ms': _percentile(joinLatency, 0.95),
    'max_ms': joinLatency.isEmpty ? 0 : joinLatency.reduce(max),
    'avg_ms': _average(joinLatency),
  };

  report['sync_latency'] = {
    'events_received': syncLatency.length,
    'expected': students,
    'p50_ms': _percentile(syncLatency, 0.50),
    'p95_ms': _percentile(syncLatency, 0.95),
    'max_ms': syncLatency.isEmpty ? 0 : syncLatency.reduce(max),
    'avg_ms': _average(syncLatency),
  };

  report['failover'] = {
    'success': failoverSuccess,
    'failures': failoverFailures,
    'success_rate': students == 0 ? 0 : failoverSuccess / students,
    'state_histogram': failoverStateHistogram,
  };

  report['api_usage_estimate'] = {
    'join_request': students,
    'join_cancel': students,
    'sync_event_publish': students,
    'failover_start': students,
    'failover_stop': students,
    'total_estimated_operations': students * 5,
  };

  final bool stableSync =
      syncLatency.length >= (students * 0.95).floor() &&
      _percentile(syncLatency, 0.95) <= 200;
  final bool stableFailover = failoverFailures == 0;
  report['verdict'] = {
    'pass': stableSync && stableFailover,
    'stable_sync': stableSync,
    'stable_failover': stableFailover,
  };

  report['finished_at'] = DateTime.now().toUtc().toIso8601String();

  final file = File(outputPath);
  await file.parent.create(recursive: true);
  await file.writeAsString(const JsonEncoder.withIndent('  ').convert(report));

  stdout.writeln(
    '50-student stress simulation complete. Report: ${file.absolute.path}',
  );
}

String? _argValue(List<String> args, String key) {
  for (var i = 0; i < args.length; i += 1) {
    if (args[i] != key) {
      continue;
    }
    if (i + 1 < args.length) {
      return args[i + 1];
    }
    return null;
  }
  return null;
}

double _average(List<int> values) {
  if (values.isEmpty) {
    return 0;
  }
  final total = values.fold<int>(0, (sum, value) => sum + value);
  return total / values.length;
}

int _percentile(List<int> values, double percentile) {
  if (values.isEmpty) {
    return 0;
  }
  final sorted = List<int>.from(values)..sort();
  final rank = ((sorted.length - 1) * percentile).round();
  return sorted[rank.clamp(0, sorted.length - 1)];
}
