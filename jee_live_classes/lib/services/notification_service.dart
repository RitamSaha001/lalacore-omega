import 'package:firebase_core/firebase_core.dart';
import 'package:firebase_messaging/firebase_messaging.dart';

import '../core/app_config.dart';
import 'secure_api_client.dart';

class NotificationService {
  NotificationService({
    required this.config,
    required this.apiClient,
  });

  final AppConfig config;
  final SecureApiClient apiClient;

  bool _initialized = false;

  Future<void> initialize() async {
    if (!config.enablePushNotifications || _initialized) {
      return;
    }

    await Firebase.initializeApp();
    final messaging = FirebaseMessaging.instance;
    await messaging.requestPermission();

    final token = await messaging.getToken();
    if (token != null) {
      await apiClient.postJson(
        Uri.parse('${config.baseApiUrl}/notifications/register-token'),
        {'token': token},
      );
    }

    _initialized = true;
  }

  Future<void> notifyClassStarting({
    required String classId,
  }) {
    return _sendEvent(classId: classId, event: 'class_starting');
  }

  Future<void> notifyNotesAvailable({
    required String classId,
  }) {
    return _sendEvent(classId: classId, event: 'new_notes_available');
  }

  Future<void> notifyHomeworkGenerated({
    required String classId,
  }) {
    return _sendEvent(classId: classId, event: 'homework_generated');
  }

  Future<void> notifyQuizStarting({
    required String classId,
  }) {
    return _sendEvent(classId: classId, event: 'quiz_starting');
  }

  Future<void> _sendEvent({
    required String classId,
    required String event,
  }) async {
    if (!config.enablePushNotifications) {
      return;
    }
    try {
      await apiClient.postJson(
        Uri.parse('${config.baseApiUrl}/notifications/event'),
        {
          'class_id': classId,
          'event': event,
        },
      );
    } catch (_) {
      // Notification delivery is best-effort and must not interrupt class flow.
    }
  }
}
