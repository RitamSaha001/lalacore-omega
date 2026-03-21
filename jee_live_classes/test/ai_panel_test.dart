import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:jee_live_classes/models/lecture_intelligence_model.dart';
import 'package:jee_live_classes/models/transcript_model.dart';
import 'package:jee_live_classes/modules/ai/ai_panel.dart';
import 'package:jee_live_classes/modules/classroom/classroom_state.dart';

void main() {
  testWidgets('AI panel exposes transcript and teacher AI actions', (
    tester,
  ) async {
    await tester.binding.setSurfaceSize(const Size(1280, 1400));
    addTearDown(() => tester.binding.setSurfaceSize(null));

    var sentPrompt = '';
    var searched = '';
    var launchedMiniQuiz = 0;
    var generatedNotes = 0;
    var generatedFlashcards = 0;
    var generatedPractice = 0;
    var generatedReport = 0;
    var generatedPoll = 0;

    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: SizedBox(
            width: 420,
            height: 900,
            child: AiPanel(
              messages: [
                AiMessage(
                  id: 'assistant_1',
                  message: 'Start with symmetry before substitution.',
                  timestamp: DateTime(2026, 3, 12, 9),
                  fromUser: false,
                ),
              ],
              intelligence: const LectureIntelligenceModel(
                concepts: [
                  ConceptMarker(concept: 'Gauss law', timestampSeconds: 120),
                ],
                conceptGraph: {
                  'Gauss law': ['Flux'],
                },
                formulas: ['Phi = q/eps0'],
                importantPoints: ['Choose a symmetric Gaussian surface.'],
                conceptSummaries: ['Flux depends on enclosed charge.'],
                flashcards: [
                  FlashcardModel(
                    front: 'What does Gauss law relate?',
                    back: 'Electric flux and enclosed charge.',
                  ),
                ],
                adaptivePractice: {
                  'level_1': ['Basic flux question'],
                  'level_2': ['Symmetry-based field question'],
                  'level_3': ['Mixed conductor problem'],
                },
                masteryScores: {'Gauss law': 0.84},
                doubtClusters: [],
                teacherInsights: ['Reinforce symmetry before numerics.'],
                revisionRecommendations: {
                  'Gauss law': ['Solve 3 PYQs'],
                },
                miniQuizSuggestion: 'Ask one symmetry checkpoint question.',
                knowledgeVaultEntries: 3,
              ),
              searchResults: const [],
              lectureNotes: null,
              isGeneratingLectureNotes: false,
              teacherSummaryReport: 'Class Intelligence Report',
              aiTeachingSuggestion: 'Do one more guided example.',
              transcript: [
                TranscriptModel(
                  id: 't1',
                  speakerId: 'teacher_01',
                  speakerName: 'Dr Sharma',
                  message:
                      'Gauss law reduces the field calculation with symmetry.',
                  timestamp: DateTime(2026, 3, 12, 9, 0, 5),
                ),
              ],
              homework: const {
                'easy': ['Define electric flux.'],
              },
              canManageClass: true,
              onSend: (value) => sentPrompt = value,
              onSearch: (value) => searched = value,
              onLaunchMiniQuiz: () => launchedMiniQuiz += 1,
              onGenerateLectureNotes: () async => generatedNotes += 1,
              onDownloadLectureNotes: () async {},
              onGenerateFlashcards: () async => generatedFlashcards += 1,
              onGenerateAdaptivePractice: () async => generatedPractice += 1,
              onGenerateTeacherReport: () async => generatedReport += 1,
              onGenerateAiPoll: () async => generatedPoll += 1,
            ),
          ),
        ),
      ),
    );

    expect(find.text('AI Transcript Feed'), findsOneWidget);
    expect(find.text('Live'), findsWidgets);
    expect(find.text('AI Poll'), findsOneWidget);
    expect(find.text('Report'), findsOneWidget);

    await tester.tap(find.text('AI Poll').first);
    await tester.pump();
    await tester.tap(find.text('Notes').first);
    await tester.pump();
    await tester.tap(find.text('Flashcards').first);
    await tester.pump();
    await tester.tap(find.text('Practice').first);
    await tester.pump();
    await tester.tap(find.text('Report').first);
    await tester.pump();
    await tester.tap(find.text('Mini Quiz').first);
    await tester.pump();

    await tester.enterText(
      find.byType(TextField).last,
      'Summarize this lecture',
    );
    await tester.tap(find.byKey(const ValueKey('ai_panel_send_button')));
    await tester.pump();

    await tester.enterText(find.byType(TextField).first, 'Gauss');
    await tester.tap(find.byKey(const ValueKey('ai_panel_search_button')));
    await tester.pump();

    expect(generatedPoll, 1);
    expect(generatedNotes, 1);
    expect(generatedFlashcards, 1);
    expect(generatedPractice, 1);
    expect(generatedReport, 1);
    expect(launchedMiniQuiz, greaterThan(0));
    expect(sentPrompt, 'Summarize this lecture');
    expect(searched, 'Gauss');
  });

  testWidgets('AI panel hides teacher-only controls for students', (
    tester,
  ) async {
    await tester.binding.setSurfaceSize(const Size(1280, 1000));
    addTearDown(() => tester.binding.setSurfaceSize(null));

    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: SizedBox(
            width: 420,
            height: 700,
            child: AiPanel(
              messages: const [],
              intelligence: LectureIntelligenceModel.empty,
              searchResults: const [],
              lectureNotes: null,
              isGeneratingLectureNotes: false,
              teacherSummaryReport: null,
              aiTeachingSuggestion: null,
              transcript: const <TranscriptModel>[],
              homework: const {},
              canManageClass: false,
              onSend: (_) {},
              onSearch: (_) {},
              onLaunchMiniQuiz: () {},
              onGenerateLectureNotes: () async {},
              onDownloadLectureNotes: () async {},
              onGenerateFlashcards: () async {},
              onGenerateAdaptivePractice: () async {},
              onGenerateTeacherReport: () async {},
              onGenerateAiPoll: () async {},
            ),
          ),
        ),
      ),
    );

    expect(find.text('AI Poll'), findsNothing);
    expect(find.text('Report'), findsNothing);
  });
}
