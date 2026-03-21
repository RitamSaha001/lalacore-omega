import 'package:flutter_test/flutter_test.dart';
import 'package:jee_live_classes/core/app_config.dart';
import 'package:jee_live_classes/models/transcript_model.dart';
import 'package:jee_live_classes/modules/ai/lalacore_api_service.dart';
import 'package:jee_live_classes/services/secure_api_client.dart';

void main() {
  test(
    'mock LalaCore service returns structured notes, poll, and homework',
    () async {
      final config = AppConfig.fromEnvironment();
      final api = LalacoreApi(
        config: config,
        apiClient: SecureApiClient(config: config),
        useMockResponses: true,
      );
      final context = AiRequestContext(
        transcript: [
          TranscriptModel(
            id: 't1',
            speakerId: 'teacher_01',
            speakerName: 'Dr Sharma',
            message:
                'Today we solve definite integration with symmetry checks.',
            timestamp: DateTime(2026, 3, 12, 11),
          ),
        ],
        chatMessages: const ['Student: Why does symmetry help here?'],
        ocrSnippets: const ['\\int_0^a f(x)dx'],
        lectureMaterials: const ['Mathematics live class'],
        detectedConcepts: const ['Definite Integration'],
        timestamps: const [120],
      );

      final notes = await api.generateNotes(context: context);
      final poll = await api.generateLivePollDraft(
        topic: 'Definite Integration',
        difficulty: 'hard',
      );
      final homework = await api.generateHomework(context: context);

      expect(notes.keyConcepts, isNotEmpty);
      expect(notes.formulas, isNotEmpty);
      expect(poll.question, contains('Definite Integration'));
      expect(poll.options, hasLength(4));
      expect(homework.easy, isNotEmpty);
      expect(homework.medium, isNotEmpty);
      expect(homework.hard, isNotEmpty);
    },
  );

  test('AI request context marks mixed Bengali-English lecture content', () {
    final context = AiRequestContext(
      transcript: [
        TranscriptModel(
          id: 't1',
          speakerId: 'teacher_01',
          speakerName: 'Dr Sharma',
          message: 'আজ definite integration-er symmetry ta dekho.',
          timestamp: DateTime(2026, 3, 12, 11),
        ),
      ],
      chatMessages: const <String>[],
      ocrSnippets: const <String>['\\int_0^a f(x)dx'],
      lectureMaterials: const <String>['Mathematics live class'],
      detectedConcepts: const <String>['Definite Integration'],
      timestamps: const <int>[120],
    );

    final json = context.toJson();
    final languageProfile =
        json['language_profile'] as Map<String, dynamic>? ?? const {};
    expect(languageProfile['mixed_bengali_english_possible'], isTrue);
    expect(languageProfile['bengali_script_detected'], isTrue);
    expect(languageProfile['preferred_output_language'], 'english');
    expect(languageProfile['preserve_math_notation'], isTrue);
  });
}
