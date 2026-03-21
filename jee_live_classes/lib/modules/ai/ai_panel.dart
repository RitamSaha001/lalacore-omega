import 'package:flutter/material.dart';

import '../../models/lecture_intelligence_model.dart';
import '../../models/lecture_notes_model.dart';
import '../../models/transcript_model.dart';
import '../../widgets/glass_panel.dart';
import '../classroom/classroom_state.dart';

class AiPanel extends StatefulWidget {
  const AiPanel({
    super.key,
    required this.messages,
    required this.intelligence,
    required this.searchResults,
    required this.lectureNotes,
    required this.isGeneratingLectureNotes,
    required this.teacherSummaryReport,
    required this.aiTeachingSuggestion,
    required this.transcript,
    required this.homework,
    required this.canManageClass,
    required this.onSend,
    required this.onSearch,
    required this.onLaunchMiniQuiz,
    required this.onGenerateLectureNotes,
    required this.onDownloadLectureNotes,
    required this.onGenerateFlashcards,
    required this.onGenerateAdaptivePractice,
    required this.onGenerateTeacherReport,
    required this.onGenerateAiPoll,
  });

  final List<AiMessage> messages;
  final LectureIntelligenceModel intelligence;
  final List<LectureSearchResult> searchResults;
  final LectureNotesModel? lectureNotes;
  final bool isGeneratingLectureNotes;
  final String? teacherSummaryReport;
  final String? aiTeachingSuggestion;
  final List<TranscriptModel> transcript;
  final Map<String, List<String>> homework;
  final bool canManageClass;
  final ValueChanged<String> onSend;
  final ValueChanged<String> onSearch;
  final VoidCallback onLaunchMiniQuiz;
  final Future<void> Function() onGenerateLectureNotes;
  final Future<void> Function() onDownloadLectureNotes;
  final Future<void> Function() onGenerateFlashcards;
  final Future<void> Function() onGenerateAdaptivePractice;
  final Future<void> Function() onGenerateTeacherReport;
  final Future<void> Function() onGenerateAiPoll;

  @override
  State<AiPanel> createState() => _AiPanelState();
}

class _AiPanelState extends State<AiPanel> {
  final TextEditingController _askController = TextEditingController();
  final TextEditingController _searchController = TextEditingController();

  @override
  void dispose() {
    _askController.dispose();
    _searchController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        _Header(
          hasMiniQuiz: widget.intelligence.miniQuizSuggestion != null,
          transcriptCount: widget.transcript.length,
          onLaunchMiniQuiz: widget.onLaunchMiniQuiz,
        ),
        Expanded(
          child: ListView(
            padding: const EdgeInsets.all(12),
            children: [
              _ActionStrip(
                canManageClass: widget.canManageClass,
                onGenerateAiPoll: widget.onGenerateAiPoll,
                onGenerateLectureNotes: widget.onGenerateLectureNotes,
                onGenerateFlashcards: widget.onGenerateFlashcards,
                onGenerateAdaptivePractice: widget.onGenerateAdaptivePractice,
                onGenerateTeacherReport: widget.onGenerateTeacherReport,
                onLaunchMiniQuiz: widget.onLaunchMiniQuiz,
              ),
              const SizedBox(height: 10),
              _TranscriptCard(transcript: widget.transcript),
              const SizedBox(height: 10),
              GlassPanel(
                padding: const EdgeInsets.all(10),
                child: _SearchBox(
                  controller: _searchController,
                  onSearch: () => widget.onSearch(_searchController.text),
                ),
              ),
              const SizedBox(height: 10),
              _LectureNotesCard(
                lectureNotes: widget.lectureNotes,
                isGenerating: widget.isGeneratingLectureNotes,
                onGenerate: widget.onGenerateLectureNotes,
                onDownload: widget.onDownloadLectureNotes,
              ),
              if ((widget.aiTeachingSuggestion ?? '').isNotEmpty) ...[
                const SizedBox(height: 10),
                _Section(
                  title: 'AI Teaching Assistant',
                  children: [Text(widget.aiTeachingSuggestion!)],
                ),
              ],
              if ((widget.teacherSummaryReport ?? '').isNotEmpty) ...[
                const SizedBox(height: 10),
                _Section(
                  title: 'Teacher Class Intelligence Report',
                  children: [Text(widget.teacherSummaryReport!)],
                ),
              ],
              if (widget.searchResults.isNotEmpty) ...[
                const SizedBox(height: 10),
                _SearchResultList(results: widget.searchResults),
              ],
              _Section(
                title: 'Detected Concepts',
                children: widget.intelligence.concepts
                    .map(
                      (concept) => Text(
                        '- ${concept.concept} -> ${_fmt(concept.timestampSeconds)}',
                      ),
                    )
                    .toList(growable: false),
              ),
              _Section(
                title: 'Formula Sheet',
                children: widget.intelligence.formulas
                    .map((formula) => Text('- $formula'))
                    .toList(growable: false),
              ),
              _Section(
                title: 'Important Points',
                children: widget.intelligence.importantPoints
                    .map((point) => Text('- $point'))
                    .toList(growable: false),
              ),
              _Section(
                title: 'Teacher Insights',
                children: widget.intelligence.teacherInsights
                    .map((point) => Text('- $point'))
                    .toList(growable: false),
              ),
              _Section(
                title: 'Auto Flashcards',
                children: widget.intelligence.flashcards
                    .take(6)
                    .map(
                      (card) => Text('- Q: ${card.front}\n  A: ${card.back}'),
                    )
                    .toList(growable: false),
              ),
              _Section(
                title: 'Adaptive Practice',
                children: widget.intelligence.adaptivePractice.entries
                    .map(
                      (entry) => Text(
                        '${entry.key}: ${entry.value.join(' | ')}',
                        style: const TextStyle(fontSize: 12),
                      ),
                    )
                    .toList(growable: false),
              ),
              _HomeworkCard(homework: widget.homework),
              if (widget.messages.isNotEmpty) ...[
                const SizedBox(height: 14),
                const Padding(
                  padding: EdgeInsets.only(bottom: 8),
                  child: Text(
                    'AI Conversation',
                    style: TextStyle(fontWeight: FontWeight.w700),
                  ),
                ),
              ],
              ...widget.messages.map((message) {
                return Align(
                  alignment: message.fromUser
                      ? Alignment.centerRight
                      : Alignment.centerLeft,
                  child: ConstrainedBox(
                    constraints: const BoxConstraints(maxWidth: 320),
                    child: Padding(
                      padding: const EdgeInsets.only(bottom: 8),
                      child: GlassPanel(
                        padding: const EdgeInsets.all(10),
                        blurSigma: 8,
                        tintColor: message.fromUser
                            ? const Color(0xCC193E63)
                            : const Color(0xDDEFF5FF),
                        borderColor: message.fromUser
                            ? const Color(0x33193E63)
                            : const Color(0x52FFFFFF),
                        child: Text(
                          message.message,
                          style: TextStyle(
                            color: message.fromUser
                                ? Colors.white
                                : const Color(0xFF112840),
                          ),
                        ),
                      ),
                    ),
                  ),
                );
              }),
            ],
          ),
        ),
        Padding(
          padding: const EdgeInsets.all(12),
          child: GlassPanel(
            padding: const EdgeInsets.all(10),
            child: Row(
              children: [
                Expanded(
                  child: TextField(
                    controller: _askController,
                    decoration: InputDecoration(
                      hintText:
                          'Ask LalaCore: explain, summarize, generate questions...',
                      border: OutlineInputBorder(
                        borderRadius: BorderRadius.circular(14),
                      ),
                      isDense: true,
                      filled: true,
                      fillColor: Colors.white.withValues(alpha: 0.5),
                    ),
                    onSubmitted: (_) => _send(),
                  ),
                ),
                const SizedBox(width: 8),
                FilledButton(
                  key: const ValueKey('ai_panel_send_button'),
                  onPressed: _send,
                  child: const Icon(Icons.auto_awesome),
                ),
              ],
            ),
          ),
        ),
      ],
    );
  }

  void _send() {
    final text = _askController.text.trim();
    if (text.isEmpty) {
      return;
    }
    widget.onSend(text);
    _askController.clear();
  }

  String _fmt(int seconds) {
    final m = (seconds ~/ 60).toString().padLeft(2, '0');
    final s = (seconds % 60).toString().padLeft(2, '0');
    return '$m:$s';
  }
}

class _Header extends StatelessWidget {
  const _Header({
    required this.hasMiniQuiz,
    required this.transcriptCount,
    required this.onLaunchMiniQuiz,
  });

  final bool hasMiniQuiz;
  final int transcriptCount;
  final VoidCallback onLaunchMiniQuiz;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(12, 12, 12, 0),
      child: GlassPanel(
        padding: const EdgeInsets.all(12),
        child: LayoutBuilder(
          builder: (context, constraints) {
            final compact = constraints.maxWidth < 390;
            final titleBlock = Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'LalaCore AI',
                  style: TextStyle(fontWeight: FontWeight.w700),
                ),
                Text(
                  'Polls, transcript intelligence, notes, flashcards, adaptive practice',
                  style: TextStyle(
                    fontSize: 12,
                    color: const Color(0xFF4A607C).withValues(alpha: 0.95),
                  ),
                ),
              ],
            );
            final actionRow = Wrap(
              spacing: 8,
              runSpacing: 8,
              crossAxisAlignment: WrapCrossAlignment.center,
              children: [
                _MetricChip(
                  icon: Icons.closed_caption,
                  label: '$transcriptCount transcript',
                ),
                if (hasMiniQuiz)
                  FilledButton.tonal(
                    onPressed: onLaunchMiniQuiz,
                    child: const Text('Mini Quiz'),
                  ),
              ],
            );

            if (compact) {
              return Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Icon(Icons.auto_awesome, size: 20),
                      const SizedBox(width: 10),
                      Expanded(child: titleBlock),
                    ],
                  ),
                  const SizedBox(height: 10),
                  actionRow,
                ],
              );
            }

            return Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Icon(Icons.auto_awesome, size: 20),
                const SizedBox(width: 10),
                Expanded(child: titleBlock),
                const SizedBox(width: 10),
                Flexible(child: actionRow),
              ],
            );
          },
        ),
      ),
    );
  }
}

class _ActionStrip extends StatelessWidget {
  const _ActionStrip({
    required this.canManageClass,
    required this.onGenerateAiPoll,
    required this.onGenerateLectureNotes,
    required this.onGenerateFlashcards,
    required this.onGenerateAdaptivePractice,
    required this.onGenerateTeacherReport,
    required this.onLaunchMiniQuiz,
  });

  final bool canManageClass;
  final Future<void> Function() onGenerateAiPoll;
  final Future<void> Function() onGenerateLectureNotes;
  final Future<void> Function() onGenerateFlashcards;
  final Future<void> Function() onGenerateAdaptivePractice;
  final Future<void> Function() onGenerateTeacherReport;
  final VoidCallback onLaunchMiniQuiz;

  @override
  Widget build(BuildContext context) {
    return GlassPanel(
      padding: const EdgeInsets.all(10),
      child: Wrap(
        spacing: 8,
        runSpacing: 8,
        children: [
          FilledButton.tonalIcon(
            onPressed: onLaunchMiniQuiz,
            icon: const Icon(Icons.quiz_outlined),
            label: const Text('Mini Quiz'),
          ),
          if (canManageClass)
            FilledButton.tonalIcon(
              onPressed: onGenerateAiPoll,
              icon: const Icon(Icons.poll_outlined),
              label: const Text('AI Poll'),
            ),
          FilledButton.tonalIcon(
            onPressed: onGenerateLectureNotes,
            icon: const Icon(Icons.summarize_outlined),
            label: const Text('Notes'),
          ),
          FilledButton.tonalIcon(
            onPressed: onGenerateFlashcards,
            icon: const Icon(Icons.style_outlined),
            label: const Text('Flashcards'),
          ),
          FilledButton.tonalIcon(
            onPressed: onGenerateAdaptivePractice,
            icon: const Icon(Icons.school_outlined),
            label: const Text('Practice'),
          ),
          if (canManageClass)
            FilledButton.tonalIcon(
              onPressed: onGenerateTeacherReport,
              icon: const Icon(Icons.assessment_outlined),
              label: const Text('Report'),
            ),
        ],
      ),
    );
  }
}

class _TranscriptCard extends StatelessWidget {
  const _TranscriptCard({required this.transcript});

  final List<TranscriptModel> transcript;

  @override
  Widget build(BuildContext context) {
    final items = transcript.length > 3
        ? transcript.sublist(transcript.length - 3)
        : transcript;
    return GlassPanel(
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              const Icon(Icons.graphic_eq_rounded, size: 18),
              const SizedBox(width: 8),
              const Expanded(
                child: Text(
                  'AI Transcript Feed',
                  style: TextStyle(fontWeight: FontWeight.w700),
                ),
              ),
              _MetricChip(
                icon: Icons.waves_outlined,
                label: transcript.isEmpty ? 'Waiting' : 'Live',
              ),
            ],
          ),
          const SizedBox(height: 8),
          if (items.isEmpty)
            const Text(
              'Transcript stream is active, but no speech segments have arrived yet.',
              style: TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
            )
          else
            ...items.map(
              (item) => Padding(
                padding: const EdgeInsets.only(bottom: 6),
                child: RichText(
                  text: TextSpan(
                    style: const TextStyle(
                      color: Color(0xFF14304D),
                      fontSize: 12.5,
                      height: 1.35,
                    ),
                    children: [
                      TextSpan(
                        text: '${item.speakerName}: ',
                        style: const TextStyle(fontWeight: FontWeight.w700),
                      ),
                      TextSpan(text: item.message),
                    ],
                  ),
                ),
              ),
            ),
        ],
      ),
    );
  }
}

class _SearchBox extends StatelessWidget {
  const _SearchBox({required this.controller, required this.onSearch});

  final TextEditingController controller;
  final VoidCallback onSearch;

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        Expanded(
          child: TextField(
            controller: controller,
            decoration: InputDecoration(
              hintText: 'Smart lecture search (e.g. Gauss Law)',
              border: OutlineInputBorder(
                borderRadius: BorderRadius.circular(14),
              ),
              isDense: true,
              filled: true,
              fillColor: Colors.white.withValues(alpha: 0.45),
            ),
            onSubmitted: (_) => onSearch(),
          ),
        ),
        const SizedBox(width: 8),
        FilledButton.tonal(
          key: const ValueKey('ai_panel_search_button'),
          onPressed: onSearch,
          child: const Icon(Icons.search),
        ),
      ],
    );
  }
}

class _SearchResultList extends StatelessWidget {
  const _SearchResultList({required this.results});

  final List<LectureSearchResult> results;

  @override
  Widget build(BuildContext context) {
    return GlassPanel(
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text(
            'Search Results',
            style: TextStyle(fontWeight: FontWeight.w700),
          ),
          const SizedBox(height: 8),
          ...results
              .take(4)
              .map(
                (item) => Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      color: const Color(0xA3F3F8FF),
                      borderRadius: BorderRadius.circular(14),
                      border: Border.all(color: const Color(0xFFE0EBFA)),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.all(10),
                      child: Text(
                        '${item.concept} @ ${_fmt(item.timestampSeconds)}\n'
                        '${item.note}\n'
                        'Formula: ${item.formula}',
                        style: const TextStyle(fontSize: 12),
                      ),
                    ),
                  ),
                ),
              ),
        ],
      ),
    );
  }

  String _fmt(int seconds) {
    final m = (seconds ~/ 60).toString().padLeft(2, '0');
    final s = (seconds % 60).toString().padLeft(2, '0');
    return '$m:$s';
  }
}

class _LectureNotesCard extends StatelessWidget {
  const _LectureNotesCard({
    required this.lectureNotes,
    required this.isGenerating,
    required this.onGenerate,
    required this.onDownload,
  });

  final LectureNotesModel? lectureNotes;
  final bool isGenerating;
  final Future<void> Function() onGenerate;
  final Future<void> Function() onDownload;

  @override
  Widget build(BuildContext context) {
    return GlassPanel(
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text(
            'AI Lecture Notes',
            style: TextStyle(fontWeight: FontWeight.w700),
          ),
          const SizedBox(height: 4),
          Text(
            lectureNotes == null
                ? 'Generate structured notes from transcript + OCR + AI analysis.'
                : 'Generated ${lectureNotes!.sections.length} topic section(s) at '
                      '${lectureNotes!.generatedAt.hour.toString().padLeft(2, '0')}:'
                      '${lectureNotes!.generatedAt.minute.toString().padLeft(2, '0')}.',
            style: const TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
          ),
          const SizedBox(height: 8),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              FilledButton.icon(
                onPressed: isGenerating ? null : onGenerate,
                icon: isGenerating
                    ? const SizedBox(
                        width: 14,
                        height: 14,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      )
                    : const Icon(Icons.auto_awesome),
                label: Text(isGenerating ? 'Generating...' : 'Generate Notes'),
              ),
              OutlinedButton.icon(
                onPressed: lectureNotes == null ? null : onDownload,
                icon: const Icon(Icons.picture_as_pdf_outlined),
                label: const Text('Download PDF'),
              ),
            ],
          ),
          if (lectureNotes != null && lectureNotes!.sections.isNotEmpty) ...[
            const SizedBox(height: 8),
            Text(
              'Preview: ${lectureNotes!.sections.first.topic}',
              style: const TextStyle(fontSize: 12, fontWeight: FontWeight.w600),
            ),
            const SizedBox(height: 2),
            Text(
              lectureNotes!.sections.first.concept,
              style: const TextStyle(fontSize: 12),
              maxLines: 2,
              overflow: TextOverflow.ellipsis,
            ),
          ],
        ],
      ),
    );
  }
}

class _HomeworkCard extends StatelessWidget {
  const _HomeworkCard({required this.homework});

  final Map<String, List<String>> homework;

  @override
  Widget build(BuildContext context) {
    final hasItems = homework.values.any((items) => items.isNotEmpty);
    if (!hasItems) {
      return const SizedBox.shrink();
    }
    return Padding(
      padding: const EdgeInsets.only(top: 12),
      child: GlassPanel(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'Generated Practice',
              style: TextStyle(fontWeight: FontWeight.w700),
            ),
            const SizedBox(height: 8),
            ...homework.entries
                .where((entry) => entry.value.isNotEmpty)
                .map(
                  (entry) => Padding(
                    padding: const EdgeInsets.only(bottom: 8),
                    child: Text(
                      '${entry.key}: ${entry.value.take(2).join(' | ')}',
                      style: const TextStyle(fontSize: 12),
                    ),
                  ),
                ),
          ],
        ),
      ),
    );
  }
}

class _Section extends StatelessWidget {
  const _Section({required this.title, required this.children});

  final String title;
  final List<Widget> children;

  @override
  Widget build(BuildContext context) {
    if (children.isEmpty) {
      return const SizedBox.shrink();
    }

    return Padding(
      padding: const EdgeInsets.only(top: 12),
      child: GlassPanel(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(title, style: const TextStyle(fontWeight: FontWeight.w700)),
            const SizedBox(height: 6),
            ...children,
          ],
        ),
      ),
    );
  }
}

class _MetricChip extends StatelessWidget {
  const _MetricChip({required this.icon, required this.label});

  final IconData icon;
  final String label;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: Colors.white.withValues(alpha: 0.48),
        borderRadius: BorderRadius.circular(999),
        border: Border.all(color: Colors.white.withValues(alpha: 0.55)),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(icon, size: 14, color: const Color(0xFF15304C)),
            const SizedBox(width: 6),
            Text(
              label,
              style: const TextStyle(
                fontSize: 12,
                fontWeight: FontWeight.w600,
                color: Color(0xFF15304C),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
