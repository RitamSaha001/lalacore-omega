import 'package:flutter/material.dart';

import '../../models/doubt_queue_model.dart';

class DoubtQueuePanel extends StatefulWidget {
  const DoubtQueuePanel({
    super.key,
    required this.doubts,
    required this.activeDoubtId,
    required this.onSelect,
    required this.onResolve,
    required this.onClearActive,
  });

  final List<DoubtQueueModel> doubts;
  final String? activeDoubtId;
  final ValueChanged<String> onSelect;
  final ValueChanged<String> onResolve;
  final VoidCallback onClearActive;

  @override
  State<DoubtQueuePanel> createState() => _DoubtQueuePanelState();
}

class _DoubtQueuePanelState extends State<DoubtQueuePanel> {
  final TextEditingController _resolutionController = TextEditingController();

  @override
  void dispose() {
    _resolutionController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final active = widget.doubts
        .where((item) => item.id == widget.activeDoubtId)
        .firstOrNull;
    final queued = widget.doubts
        .where((item) => item.status != DoubtQueueStatus.resolved)
        .toList(growable: false);
    final resolved = widget.doubts
        .where((item) => item.status == DoubtQueueStatus.resolved)
        .toList(growable: false);

    return Column(
      children: [
        const _Header(),
        if (active != null)
          _ActiveDoubtCard(
            doubt: active,
            controller: _resolutionController,
            onResolve: () {
              final text = _resolutionController.text.trim();
              if (text.isEmpty) {
                return;
              }
              widget.onResolve(text);
              _resolutionController.clear();
            },
            onClear: () {
              _resolutionController.clear();
              widget.onClearActive();
            },
          ),
        Expanded(
          child: ListView(
            padding: const EdgeInsets.all(12),
            children: [
              _SectionTitle(
                title: 'Pending Doubts (${queued.length})',
                icon: Icons.help_outline,
              ),
              const SizedBox(height: 6),
              if (queued.isEmpty)
                const _EmptyState(message: 'No pending doubts in queue.'),
              ...queued.map(
                (item) => _DoubtTile(
                  doubt: item,
                  active: item.id == widget.activeDoubtId,
                  onSelect: () => widget.onSelect(item.id),
                ),
              ),
              const SizedBox(height: 14),
              _SectionTitle(
                title: 'Resolved Doubts (${resolved.length})',
                icon: Icons.task_alt,
              ),
              const SizedBox(height: 6),
              if (resolved.isEmpty)
                const _EmptyState(message: 'Resolved doubts will appear here.'),
              ...resolved.take(8).map((item) => _ResolvedTile(doubt: item)),
            ],
          ),
        ),
      ],
    );
  }
}

class _Header extends StatelessWidget {
  const _Header();

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: const BoxDecoration(
        border: Border(bottom: BorderSide(color: Color(0xFFDCE7F7))),
      ),
      child: const Padding(
        padding: EdgeInsets.all(12),
        child: Row(
          children: [
            Icon(Icons.question_answer_outlined),
            SizedBox(width: 8),
            Expanded(
              child: Text(
                'Doubt Queue',
                style: TextStyle(fontWeight: FontWeight.w700),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _ActiveDoubtCard extends StatelessWidget {
  const _ActiveDoubtCard({
    required this.doubt,
    required this.controller,
    required this.onResolve,
    required this.onClear,
  });

  final DoubtQueueModel doubt;
  final TextEditingController controller;
  final VoidCallback onResolve;
  final VoidCallback onClear;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(12, 12, 12, 0),
      child: DecoratedBox(
        decoration: BoxDecoration(
          color: const Color(0xFFFFF7E8),
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: const Color(0xFFF0D38A)),
        ),
        child: Padding(
          padding: const EdgeInsets.all(10),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              const Text(
                'Selected Doubt (Live)',
                style: TextStyle(fontWeight: FontWeight.w700),
              ),
              const SizedBox(height: 4),
              Text('${doubt.studentName}: ${doubt.question}'),
              const SizedBox(height: 6),
              Text(
                'AI attempt: ${doubt.aiAttemptAnswer}',
                style: const TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
              ),
              const SizedBox(height: 8),
              TextField(
                controller: controller,
                minLines: 2,
                maxLines: 4,
                decoration: InputDecoration(
                  hintText: 'Type live teacher answer...',
                  border: OutlineInputBorder(
                    borderRadius: BorderRadius.circular(10),
                  ),
                  isDense: true,
                ),
              ),
              const SizedBox(height: 8),
              Row(
                children: [
                  FilledButton.icon(
                    onPressed: onResolve,
                    icon: const Icon(Icons.check_circle_outline),
                    label: const Text('Mark Resolved'),
                  ),
                  const SizedBox(width: 8),
                  OutlinedButton(
                    onPressed: onClear,
                    child: const Text('Unselect'),
                  ),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class _SectionTitle extends StatelessWidget {
  const _SectionTitle({required this.title, required this.icon});

  final String title;
  final IconData icon;

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        Icon(icon, size: 16),
        const SizedBox(width: 6),
        Text(title, style: const TextStyle(fontWeight: FontWeight.w700)),
      ],
    );
  }
}

class _DoubtTile extends StatelessWidget {
  const _DoubtTile({
    required this.doubt,
    required this.active,
    required this.onSelect,
  });

  final DoubtQueueModel doubt;
  final bool active;
  final VoidCallback onSelect;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: DecoratedBox(
        decoration: BoxDecoration(
          color: active ? const Color(0xFFFFF7E8) : Colors.white,
          borderRadius: BorderRadius.circular(12),
          border: Border.all(
            color: active ? const Color(0xFFF0D38A) : const Color(0xFFDCE7F7),
          ),
        ),
        child: Padding(
          padding: const EdgeInsets.all(10),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  Expanded(
                    child: Text(
                      doubt.studentName,
                      style: const TextStyle(fontWeight: FontWeight.w700),
                    ),
                  ),
                  FilledButton.tonal(
                    onPressed: onSelect,
                    child: Text(active ? 'Selected' : 'Answer Live'),
                  ),
                ],
              ),
              const SizedBox(height: 4),
              Text(doubt.question),
            ],
          ),
        ),
      ),
    );
  }
}

class _ResolvedTile extends StatelessWidget {
  const _ResolvedTile({required this.doubt});

  final DoubtQueueModel doubt;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: DecoratedBox(
        decoration: BoxDecoration(
          color: const Color(0xFFF1FAF3),
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: const Color(0xFFCFEAD5)),
        ),
        child: Padding(
          padding: const EdgeInsets.all(10),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                '${doubt.studentName}: ${doubt.question}',
                style: const TextStyle(fontWeight: FontWeight.w600),
              ),
              if ((doubt.teacherResolution ?? '').isNotEmpty) ...[
                const SizedBox(height: 4),
                Text(
                  'Teacher: ${doubt.teacherResolution!}',
                  style: const TextStyle(fontSize: 12),
                ),
              ],
            ],
          ),
        ),
      ),
    );
  }
}

class _EmptyState extends StatelessWidget {
  const _EmptyState({required this.message});

  final String message;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: const Color(0xFFF8FAFF),
        borderRadius: BorderRadius.circular(10),
      ),
      child: Padding(
        padding: const EdgeInsets.all(10),
        child: Text(
          message,
          style: const TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
        ),
      ),
    );
  }
}

extension<T> on Iterable<T> {
  T? get firstOrNull => isEmpty ? null : first;
}
