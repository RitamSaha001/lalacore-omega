import 'package:flutter/material.dart';

import '../../models/live_poll_model.dart';

class LivePollStudentView extends StatefulWidget {
  const LivePollStudentView({
    super.key,
    required this.poll,
    required this.timeRemaining,
    required this.pollActive,
    required this.submittedOption,
    required this.pollResultsRevealed,
    required this.onSubmit,
  });

  final LivePollModel poll;
  final int timeRemaining;
  final bool pollActive;
  final int? submittedOption;
  final bool pollResultsRevealed;
  final ValueChanged<int> onSubmit;

  @override
  State<LivePollStudentView> createState() => _LivePollStudentViewState();
}

class _LivePollStudentViewState extends State<LivePollStudentView> {
  int? _selected;

  @override
  void didUpdateWidget(covariant LivePollStudentView oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (widget.submittedOption != null) {
      _selected = widget.submittedOption;
    }
  }

  @override
  Widget build(BuildContext context) {
    final poll = widget.poll;
    final submitted = widget.submittedOption != null;
    final active = widget.pollActive;

    return Card(
      elevation: 8,
      child: Padding(
        padding: const EdgeInsets.all(14),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                const Icon(Icons.poll_outlined, size: 18),
                const SizedBox(width: 6),
                const Expanded(
                  child: Text(
                    'Live Poll',
                    style: TextStyle(fontWeight: FontWeight.w700),
                  ),
                ),
                Text(
                  active ? '⏱ ${widget.timeRemaining}s' : 'Closed',
                  style: TextStyle(
                    fontWeight: FontWeight.w700,
                    color: active
                        ? const Color(0xFF163E71)
                        : const Color(0xFF6F7E93),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 10),
            Text(
              poll.question,
              style: const TextStyle(fontSize: 15, fontWeight: FontWeight.w600),
            ),
            const SizedBox(height: 8),
            ...poll.options.asMap().entries.map((entry) {
              final index = entry.key;
              final option = entry.value;
              final label = '${String.fromCharCode(65 + index)}) $option';
              final selectedOption = submitted
                  ? widget.submittedOption
                  : _selected;

              Color? tileColor;
              if (!active &&
                  widget.pollResultsRevealed &&
                  poll.correctOption != null) {
                if (poll.correctOption == index) {
                  tileColor = const Color(0xFFD9F5E1);
                } else if (widget.submittedOption == index) {
                  tileColor = const Color(0xFFF8DEDE);
                }
              }

              return Padding(
                padding: const EdgeInsets.only(bottom: 6),
                child: ListTile(
                  dense: true,
                  tileColor: tileColor,
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(10),
                    side: const BorderSide(color: Color(0xFFD8E4F4)),
                  ),
                  leading: Icon(
                    selectedOption == index
                        ? Icons.radio_button_checked
                        : Icons.radio_button_unchecked,
                    color: selectedOption == index
                        ? const Color(0xFF215D9D)
                        : const Color(0xFF7D8CA0),
                  ),
                  title: Text(label),
                  onTap: (!active || submitted)
                      ? null
                      : () {
                          setState(() {
                            _selected = index;
                          });
                        },
                ),
              );
            }),
            const SizedBox(height: 8),
            if (active && !submitted)
              Align(
                alignment: Alignment.centerRight,
                child: FilledButton(
                  onPressed: _selected == null
                      ? null
                      : () => widget.onSubmit(_selected!),
                  child: const Text('Submit'),
                ),
              ),
            if (active && submitted)
              const Text(
                'Answer submitted. Waiting for timer to end.',
                style: TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
              ),
            if (!active && !widget.pollResultsRevealed)
              const Text(
                'Poll ended. Waiting for teacher reveal.',
                style: TextStyle(fontSize: 12, color: Color(0xFF4A607C)),
              ),
            if (!active && widget.pollResultsRevealed)
              _ResultSummary(
                correctOption: poll.correctOption,
                submittedOption: widget.submittedOption,
              ),
          ],
        ),
      ),
    );
  }
}

class _ResultSummary extends StatelessWidget {
  const _ResultSummary({
    required this.correctOption,
    required this.submittedOption,
  });

  final int? correctOption;
  final int? submittedOption;

  @override
  Widget build(BuildContext context) {
    final correctLabel = correctOption == null
        ? 'Not revealed'
        : String.fromCharCode(65 + correctOption!);
    final submittedLabel = submittedOption == null
        ? 'No submission'
        : String.fromCharCode(65 + submittedOption!);

    return Text(
      'Correct Answer: $correctLabel | Your Answer: $submittedLabel',
      style: const TextStyle(fontSize: 12, fontWeight: FontWeight.w600),
    );
  }
}
