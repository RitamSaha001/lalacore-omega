import '../core/app_config.dart';
import '../models/live_class_context.dart';
import 'secure_api_client.dart';

class StudyMaterialSyncService {
  const StudyMaterialSyncService({
    required this.config,
    required this.apiClient,
    this.enabled = true,
  });

  final AppConfig config;
  final SecureApiClient apiClient;
  final bool enabled;

  static const String _appActionEndpoint = '/app/action';
  static const int _maxNotesLength = 60000;

  Future<void> upsertTeacherStudyNote({
    required LiveClassContext context,
    required String materialKey,
    required String titleSuffix,
    required String body,
    String description = '',
  }) async {
    if (!enabled || !context.isTeacher) {
      return;
    }
    final normalizedBody = body.trim();
    if (normalizedBody.isEmpty) {
      return;
    }

    final String subject = context.subject.trim().isEmpty
        ? 'General'
        : context.subject.trim();
    final String chapter = context.topic.trim().isEmpty
        ? context.classTitle.trim()
        : context.topic.trim();
    final String className = (context.className ?? '').trim().isEmpty
        ? 'Class 11'
        : context.className!.trim();
    final String titleBase = context.classTitle.trim().isEmpty
        ? '$subject Live Class'
        : context.classTitle.trim();

    final payload = <String, dynamic>{
      'action': 'add_study_material',
      'role': 'teacher',
      'material_id': 'live_${context.classId}_$materialKey',
      'title': '$titleBase • $titleSuffix',
      'type': 'note',
      'url': 'inline://note',
      'description': description.trim(),
      'notes': normalizedBody.length > _maxNotesLength
          ? normalizedBody.substring(0, _maxNotesLength)
          : normalizedBody,
      'subject': subject,
      'chapters': chapter,
      'class': className,
    };

    await apiClient.postJson(
      config.apiUri(_appActionEndpoint),
      payload,
      signRequest: true,
    );
  }
}
