import 'dart:math';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../widgets/liquid_glass.dart';
import 'desmos_graph_view.dart';
import 'smart_text.dart';

class AnswerKeyCardPayload {
  const AnswerKeyCardPayload({
    required this.heroTag,
    required this.questionIndex,
    required this.questionText,
    required this.options,
    required this.correctAnswer,
    required this.studentAnswer,
    required this.solution,
    required this.concept,
    required this.marksDelta,
    required this.statusLabel,
    required this.statusColor,
    this.imageUrl = '',
    this.visualization,
  });

  final String heroTag;
  final int questionIndex;
  final String questionText;
  final List<String> options;
  final String correctAnswer;
  final String studentAnswer;
  final String solution;
  final String concept;
  final double marksDelta;
  final String statusLabel;
  final Color statusColor;
  final String imageUrl;
  final Map<String, dynamic>? visualization;

  Map<String, dynamic> toCardMap() {
    return <String, dynamic>{
      'hero_tag': heroTag,
      'question_index': questionIndex,
      'question_text': questionText,
      'options': options,
      'correct_answer': correctAnswer,
      'student_answer': studentAnswer,
      'solution': solution,
      'concept': concept,
      'marks_delta': marksDelta,
      'status_label': statusLabel,
      'status_color': statusColor.value,
      'image_url': imageUrl,
      if (visualization != null) 'visualization': visualization,
    };
  }
}

class AnswerKeyFlipCard extends StatefulWidget {
  const AnswerKeyFlipCard({
    super.key,
    required this.payload,
    required this.onAskAi,
    required this.onRaiseDoubt,
    this.teacherMode = false,
    this.showActionButtons = true,
  });

  final AnswerKeyCardPayload payload;
  final VoidCallback onAskAi;
  final VoidCallback onRaiseDoubt;
  final bool teacherMode;
  final bool showActionButtons;

  @override
  State<AnswerKeyFlipCard> createState() => _AnswerKeyFlipCardState();
}

class _AnswerKeyFlipCardState extends State<AnswerKeyFlipCard>
    with SingleTickerProviderStateMixin {
  static const int _collapsedStepLimit = 6;
  late final AnimationController _ctrl;
  bool _front = true;
  List<String> _solutionSteps = <String>[];
  int _visibleStepCount = 0;
  bool _showAllSteps = false;
  int _revealRunId = 0;

  @override
  void initState() {
    super.initState();
    _prepareSolutionSteps(resetVisibility: true);
    _ctrl = AnimationController(
      vsync: this,
      duration: const Duration(milliseconds: 430),
    );
    _ctrl.addStatusListener(_handleFlipStatus);
  }

  @override
  void didUpdateWidget(covariant AnswerKeyFlipCard oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.payload.solution != widget.payload.solution) {
      _prepareSolutionSteps(resetVisibility: true);
    }
  }

  @override
  void dispose() {
    _ctrl.removeStatusListener(_handleFlipStatus);
    _ctrl.dispose();
    super.dispose();
  }

  void _prepareSolutionSteps({required bool resetVisibility}) {
    _solutionSteps = _splitSolutionSteps(widget.payload.solution);
    if (resetVisibility) {
      _showAllSteps = _solutionSteps.length <= _collapsedStepLimit;
      _visibleStepCount = 0;
    }
  }

  List<String> _splitSolutionSteps(String raw) {
    final String normalized = raw.trim().isEmpty
        ? 'No official solution uploaded for this question yet.'
        : raw.trim();
    final RegExp marker = RegExp(
      r'(Step\s*\d+\s*:|Final\s*Answer\s*:)',
      caseSensitive: false,
    );
    final List<Match> matches = marker.allMatches(normalized).toList();
    if (matches.isEmpty) {
      final List<String> lines = normalized
          .split('\n')
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty)
          .toList();
      if (lines.length <= 1) {
        return <String>['Step 1: $normalized'];
      }
      return List<String>.generate(lines.length, (int i) {
        final String line = lines[i];
        if (RegExp(
          r'^(step\s*\d+\s*:|final\s*answer\s*:)',
          caseSensitive: false,
        ).hasMatch(line)) {
          return line;
        }
        return 'Step ${i + 1}: $line';
      });
    }

    final List<String> out = <String>[];
    final String prefix = normalized.substring(0, matches.first.start).trim();
    if (prefix.isNotEmpty) {
      out.add('Step 1: $prefix');
    }
    for (int i = 0; i < matches.length; i++) {
      final Match current = matches[i];
      final int nextStart = i == matches.length - 1
          ? normalized.length
          : matches[i + 1].start;
      final String label = normalized
          .substring(current.start, current.end)
          .trim();
      final String body = normalized.substring(current.end, nextStart).trim();
      final String step = body.isEmpty ? label : '$label $body';
      out.add(step.replaceAll(RegExp(r'\s+'), ' ').trim());
    }
    return out;
  }

  bool _isFinalAnswerStep(String step) {
    return RegExp(
          r'^final\s*answer\s*:',
          caseSensitive: false,
        ).hasMatch(step.trim()) ||
        step.toLowerCase().contains('final answer:');
  }

  int _targetStepCount() {
    if (_showAllSteps) {
      return _solutionSteps.length;
    }
    return min(_collapsedStepLimit, _solutionSteps.length);
  }

  Future<void> _startReveal({required bool fromCurrent}) async {
    if (!mounted || _front) {
      return;
    }
    final int runId = ++_revealRunId;
    final int target = _targetStepCount();
    int start = fromCurrent ? _visibleStepCount : 0;
    if (!fromCurrent) {
      setState(() => _visibleStepCount = 0);
    }
    if (start < 0) {
      start = 0;
    }
    for (int i = start; i < target; i++) {
      final bool isFinal = _isFinalAnswerStep(_solutionSteps[i]);
      await Future<void>.delayed(Duration(milliseconds: isFinal ? 500 : 400));
      if (!mounted || runId != _revealRunId || _front) {
        return;
      }
      setState(() {
        _visibleStepCount = i + 1;
      });
    }
  }

  void _resetReveal() {
    _revealRunId++;
    if (!mounted) {
      return;
    }
    setState(() {
      _showAllSteps = _solutionSteps.length <= _collapsedStepLimit;
      _visibleStepCount = 0;
    });
  }

  void _toggleExpandCollapse() {
    if (_solutionSteps.length <= _collapsedStepLimit) {
      return;
    }
    if (_showAllSteps) {
      setState(() {
        _showAllSteps = false;
        _visibleStepCount = min(_visibleStepCount, _collapsedStepLimit);
      });
      return;
    }
    setState(() => _showAllSteps = true);
    _startReveal(fromCurrent: true);
  }

  void _handleFlipStatus(AnimationStatus status) {
    if (status == AnimationStatus.completed && !_front) {
      _startReveal(fromCurrent: false);
      return;
    }
    if (status == AnimationStatus.dismissed && _front) {
      _resetReveal();
    }
  }

  void _flip() {
    HapticFeedback.selectionClick();
    if (_front) {
      _ctrl.forward();
    } else {
      _ctrl.reverse();
    }
    _front = !_front;
  }

  @override
  Widget build(BuildContext context) {
    return Hero(
      tag: widget.payload.heroTag,
      child: GestureDetector(
        onTap: widget.teacherMode ? _flip : null,
        child: AnimatedBuilder(
          animation: _ctrl,
          builder: (_, __) {
            final double value = Curves.easeOutBack.transform(_ctrl.value);
            final double angle = value * pi;
            final Matrix4 m = Matrix4.identity()
              ..setEntry(3, 2, 0.0012)
              ..rotateY(angle);
            final bool showFront = value < 0.5;
            return Transform(
              transform: m,
              alignment: Alignment.center,
              child: showFront
                  ? _frontFace(context)
                  : Transform(
                      transform: Matrix4.identity()..rotateY(pi),
                      alignment: Alignment.center,
                      child: _backFace(context),
                    ),
            );
          },
        ),
      ),
    );
  }

  Widget _frontFace(BuildContext context) {
    final AnswerKeyCardPayload p = widget.payload;
    return LiquidGlass(
      padding: const EdgeInsets.all(16),
      color: p.statusColor.withOpacity(0.08),
      child: ListView(
        physics: const BouncingScrollPhysics(),
        children: <Widget>[
          Row(
            children: <Widget>[
              Text(
                'Question ${p.questionIndex + 1}',
                style: const TextStyle(
                  fontWeight: FontWeight.w800,
                  fontSize: 16,
                ),
              ),
              const Spacer(),
              if (widget.teacherMode)
                Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 10,
                    vertical: 5,
                  ),
                  decoration: BoxDecoration(
                    color: p.statusColor.withOpacity(0.12),
                    borderRadius: BorderRadius.circular(999),
                  ),
                  child: Text(
                    p.statusLabel,
                    style: TextStyle(
                      color: p.statusColor,
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
            ],
          ),
          const SizedBox(height: 10),
          SmartText(p.questionText),
          const SizedBox(height: 10),
          if (widget.teacherMode && p.options.isNotEmpty)
            ...p.options.map(
              (String e) => Padding(
                padding: const EdgeInsets.only(bottom: 4),
                child: SmartText(
                  '• $e',
                  style: const TextStyle(fontSize: 13.5),
                ),
              ),
            ),
          const SizedBox(height: 8),
          SmartText(
            'Correct: ${p.correctAnswer}',
            style: const TextStyle(
              fontWeight: FontWeight.w700,
              color: Colors.green,
            ),
          ),
          if (!widget.teacherMode && widget.showActionButtons) ...<Widget>[
            const SizedBox(height: 12),
            _actionButtonsRow(),
          ],
          if (widget.teacherMode) ...<Widget>[
            SmartText(
              'You: ${p.studentAnswer}',
              style: TextStyle(
                fontWeight: FontWeight.w700,
                color: p.statusColor,
              ),
            ),
            Text(
              'Marks: ${p.marksDelta >= 0 ? '+' : ''}${p.marksDelta.toStringAsFixed(2)}',
              style: TextStyle(
                fontWeight: FontWeight.w800,
                color: p.marksDelta >= 0 ? Colors.green : Colors.redAccent,
              ),
            ),
          ],
          if (widget.teacherMode && p.concept.trim().isNotEmpty) ...<Widget>[
            const SizedBox(height: 6),
            SmartText(
              'Concept: ${p.concept}',
              style: const TextStyle(fontSize: 12.5),
            ),
          ],
          const SizedBox(height: 10),
          Text(
            widget.teacherMode
                ? 'Tap card for AI actions'
                : 'Student mode: only correct answers are visible.',
            style: const TextStyle(fontSize: 11, color: Colors.grey),
          ),
        ],
      ),
    );
  }

  Widget _backFace(BuildContext context) {
    if (!widget.teacherMode) {
      return _frontFace(context);
    }
    final AnswerKeyCardPayload p = widget.payload;
    final bool canExpand = _solutionSteps.length > _collapsedStepLimit;
    final int visible = _visibleStepCount.clamp(0, _targetStepCount());
    return LiquidGlass(
      padding: const EdgeInsets.all(16),
      color: p.statusColor.withOpacity(0.1),
      child: ListView(
        physics: const BouncingScrollPhysics(),
        children: <Widget>[
          const Text(
            'Solution',
            style: TextStyle(fontWeight: FontWeight.w800, fontSize: 16),
          ),
          const SizedBox(height: 8),
          Container(
            padding: const EdgeInsets.all(12),
            decoration: BoxDecoration(
              color: Colors.green.withOpacity(0.08),
              borderRadius: BorderRadius.circular(14),
            ),
            child: SmartText(
              'Correct Answer: ${p.correctAnswer.trim().isEmpty ? 'Not available' : p.correctAnswer}',
              style: const TextStyle(
                fontWeight: FontWeight.w700,
                color: Colors.green,
              ),
            ),
          ),
          const SizedBox(height: 12),
          AnimatedSize(
            duration: const Duration(milliseconds: 340),
            curve: Curves.easeInOutCubic,
            alignment: Alignment.topCenter,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: List<Widget>.generate(visible, (int index) {
                final String step = _solutionSteps[index];
                return Padding(
                  padding: EdgeInsets.only(
                    bottom: index == visible - 1 ? 0 : 12,
                  ),
                  child: AnimatedStepWidget(
                    text: step,
                    isFinalAnswer: _isFinalAnswerStep(step),
                  ),
                );
              }),
            ),
          ),
          if (canExpand) ...<Widget>[
            const SizedBox(height: 10),
            Align(
              alignment: Alignment.centerLeft,
              child: TextButton(
                onPressed: _toggleExpandCollapse,
                child: Text(
                  _showAllSteps ? 'Collapse ▲' : 'Reveal Full Solution ▼',
                ),
              ),
            ),
          ],
          if (p.visualization != null) ...<Widget>[
            const SizedBox(height: 12),
            DesmosGraphView(visualization: p.visualization!),
          ],
          if (widget.showActionButtons) ...<Widget>[
            const SizedBox(height: 12),
            _actionButtonsRow(),
          ],
          const SizedBox(height: 10),
          const Text(
            'Tap card to flip back',
            style: TextStyle(fontSize: 11, color: Colors.grey),
          ),
        ],
      ),
    );
  }

  Widget _actionButtonsRow() {
    return Row(
      children: <Widget>[
        Expanded(
          child: ElevatedButton.icon(
            onPressed: widget.onAskAi,
            icon: const Icon(Icons.smart_toy),
            label: const Text('Ask LalaCore'),
          ),
        ),
        const SizedBox(width: 8),
        Expanded(
          child: OutlinedButton.icon(
            onPressed: widget.onRaiseDoubt,
            icon: const Icon(Icons.help_outline),
            label: const Text('Send to Teacher'),
          ),
        ),
      ],
    );
  }
}

class AnimatedStepWidget extends StatelessWidget {
  const AnimatedStepWidget({
    super.key,
    required this.text,
    this.isFinalAnswer = false,
  });

  final String text;
  final bool isFinalAnswer;

  @override
  Widget build(BuildContext context) {
    final Widget content = SelectionArea(
      child: SmartText(text, style: const TextStyle(height: 1.35)),
    );
    final Widget wrapped = isFinalAnswer
        ? Container(
            padding: const EdgeInsets.all(14),
            decoration: BoxDecoration(
              color: Colors.green.withOpacity(0.08),
              borderRadius: BorderRadius.circular(16),
            ),
            child: content,
          )
        : content;
    return TweenAnimationBuilder<double>(
      duration: Duration(milliseconds: isFinalAnswer ? 500 : 350),
      tween: Tween<double>(begin: 0, end: 1),
      curve: Curves.easeOutCubic,
      builder: (BuildContext context, double value, Widget? child) {
        return Opacity(
          opacity: value,
          child: Transform.translate(
            offset: Offset(0, (1 - value) * 10),
            child: child,
          ),
        );
      },
      child: wrapped,
    );
  }
}
