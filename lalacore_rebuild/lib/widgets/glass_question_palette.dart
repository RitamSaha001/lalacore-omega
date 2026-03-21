import 'package:flutter/material.dart';

import 'liquid_glass.dart';

enum QuestionPaletteState { unseen, answered, skipped, review, current }

class GlassQuestionPaletteSheet extends StatefulWidget {
  const GlassQuestionPaletteSheet({
    super.key,
    required this.total,
    required this.current,
    required this.answeredCount,
    required this.reviewCount,
    required this.skippedCount,
    required this.stateFor,
    required this.onJump,
    this.onPrevUnanswered,
    this.onNextUnanswered,
    this.onFirstReview,
    this.onJumpFirst,
    this.onJumpLast,
  });

  final int total;
  final int current;
  final int answeredCount;
  final int reviewCount;
  final int skippedCount;
  final QuestionPaletteState Function(int index) stateFor;
  final ValueChanged<int> onJump;
  final VoidCallback? onPrevUnanswered;
  final VoidCallback? onNextUnanswered;
  final VoidCallback? onFirstReview;
  final VoidCallback? onJumpFirst;
  final VoidCallback? onJumpLast;

  @override
  State<GlassQuestionPaletteSheet> createState() =>
      _GlassQuestionPaletteSheetState();
}

class _GlassQuestionPaletteSheetState extends State<GlassQuestionPaletteSheet> {
  bool _expanded = false;

  @override
  Widget build(BuildContext context) {
    final int unanswered = (widget.total - widget.answeredCount).clamp(
      0,
      widget.total,
    );
    final int unseen =
        (widget.total -
                widget.answeredCount -
                widget.skippedCount -
                widget.reviewCount)
            .clamp(0, widget.total);

    return Container(
      height: MediaQuery.of(context).size.height * 0.72,
      padding: const EdgeInsets.all(16),
      child: Column(
        children: <Widget>[
          Row(
            children: <Widget>[
              const Text(
                'Question Map',
                style: TextStyle(fontSize: 18, fontWeight: FontWeight.w800),
              ),
              const Spacer(),
              Container(
                padding: const EdgeInsets.symmetric(
                  horizontal: 10,
                  vertical: 6,
                ),
                decoration: BoxDecoration(
                  color: const Color(0xFF3F8CFF).withValues(alpha: 0.12),
                  borderRadius: BorderRadius.circular(999),
                ),
                child: Text(
                  '${widget.current + 1}/${widget.total}',
                  style: const TextStyle(
                    fontSize: 12,
                    fontWeight: FontWeight.w700,
                    color: Color(0xFF265EA8),
                  ),
                ),
              ),
              const SizedBox(width: 6),
              IconButton(
                onPressed: () => setState(() => _expanded = !_expanded),
                icon: Icon(_expanded ? Icons.unfold_less : Icons.unfold_more),
              ),
            ],
          ),
          const SizedBox(height: 8),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: <Widget>[
              _MetricPill(
                label: 'Answered',
                value: '${widget.answeredCount}',
                color: _colorFor(QuestionPaletteState.answered),
              ),
              _MetricPill(
                label: 'Review',
                value: '${widget.reviewCount}',
                color: _colorFor(QuestionPaletteState.review),
              ),
              _MetricPill(
                label: 'Skipped',
                value: '${widget.skippedCount}',
                color: _colorFor(QuestionPaletteState.skipped),
              ),
              _MetricPill(
                label: 'Unanswered',
                value: '$unanswered',
                color: const Color(0xFF5C6BC0),
              ),
              _MetricPill(
                label: 'Unseen',
                value: '$unseen',
                color: _colorFor(QuestionPaletteState.unseen),
              ),
            ],
          ),
          const SizedBox(height: 10),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            alignment: WrapAlignment.center,
            children: <Widget>[
              _QuickJumpButton(
                label: 'First',
                icon: Icons.first_page_rounded,
                onTap: widget.onJumpFirst,
              ),
              _QuickJumpButton(
                label: 'Last',
                icon: Icons.last_page_rounded,
                onTap: widget.onJumpLast,
              ),
              _QuickJumpButton(
                label: 'Prev Left',
                icon: Icons.skip_previous_rounded,
                onTap: widget.onPrevUnanswered,
              ),
              _QuickJumpButton(
                label: 'Next Left',
                icon: Icons.skip_next_rounded,
                onTap: widget.onNextUnanswered,
              ),
              _QuickJumpButton(
                label: 'Review',
                icon: Icons.flag_rounded,
                onTap: widget.onFirstReview,
              ),
            ],
          ),
          const SizedBox(height: 10),
          Expanded(
            child: LayoutBuilder(
              builder: (BuildContext context, BoxConstraints constraints) {
                final int crossAxisCount =
                    (constraints.maxWidth / (_expanded ? 54 : 64))
                        .floor()
                        .clamp(4, _expanded ? 9 : 7);
                return GridView.builder(
                  physics: const BouncingScrollPhysics(),
                  itemCount: widget.total,
                  gridDelegate: SliverGridDelegateWithFixedCrossAxisCount(
                    crossAxisCount: crossAxisCount,
                    mainAxisSpacing: 10,
                    crossAxisSpacing: 10,
                  ),
                  itemBuilder: (_, int i) {
                    final QuestionPaletteState state = widget.stateFor(i);
                    final Color c = _colorFor(state);
                    final bool selected = i == widget.current;
                    return AnimatedScale(
                      scale: selected ? 1.06 : 1.0,
                      duration: const Duration(milliseconds: 180),
                      curve: Curves.easeOutBack,
                      child: GestureDetector(
                        onTap: () => widget.onJump(i),
                        onLongPress: () {
                          showDialog<void>(
                            context: context,
                            builder: (_) => AlertDialog(
                              title: Text('Question ${i + 1}'),
                              content: Text(_labelFor(state)),
                            ),
                          );
                        },
                        child: LiquidGlass(
                          quality: LiquidGlassQuality.low,
                          borderRadius: BorderRadius.circular(999),
                          padding: const EdgeInsets.all(0),
                          child: Container(
                            decoration: BoxDecoration(
                              shape: BoxShape.circle,
                              color: c.withValues(alpha: 0.12),
                              border: Border.all(
                                color: c,
                                width: selected ? 2.2 : 1.4,
                              ),
                            ),
                            child: Center(
                              child: Text(
                                '${i + 1}',
                                style: TextStyle(
                                  color: c,
                                  fontWeight: FontWeight.w800,
                                ),
                              ),
                            ),
                          ),
                        ),
                      ),
                    );
                  },
                );
              },
            ),
          ),
          const SizedBox(height: 8),
          Wrap(
            spacing: 10,
            runSpacing: 8,
            alignment: WrapAlignment.center,
            children: <Widget>[
              _Legend(
                color: _colorFor(QuestionPaletteState.current),
                text: 'Current',
              ),
              _Legend(
                color: _colorFor(QuestionPaletteState.answered),
                text: 'Answered',
              ),
              _Legend(
                color: _colorFor(QuestionPaletteState.skipped),
                text: 'Skipped',
              ),
              _Legend(
                color: _colorFor(QuestionPaletteState.review),
                text: 'Review',
              ),
              _Legend(
                color: _colorFor(QuestionPaletteState.unseen),
                text: 'Unseen',
              ),
            ],
          ),
        ],
      ),
    );
  }

  Color _colorFor(QuestionPaletteState state) {
    return switch (state) {
      QuestionPaletteState.current => const Color(0xFF1D78E8),
      QuestionPaletteState.answered => const Color(0xFF28A55E),
      QuestionPaletteState.skipped => const Color(0xFF5C6BC0),
      QuestionPaletteState.review => const Color(0xFFE58A00),
      QuestionPaletteState.unseen => const Color(0xFF8A8A8A),
    };
  }

  String _labelFor(QuestionPaletteState state) {
    return switch (state) {
      QuestionPaletteState.current => 'Current',
      QuestionPaletteState.answered => 'Answered',
      QuestionPaletteState.skipped => 'Skipped / visited',
      QuestionPaletteState.review => 'Marked for review',
      QuestionPaletteState.unseen => 'Unseen',
    };
  }
}

class _MetricPill extends StatelessWidget {
  const _MetricPill({
    required this.label,
    required this.value,
    required this.color,
  });

  final String label;
  final String value;
  final Color color;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(999),
        color: color.withValues(alpha: 0.12),
        border: Border.all(color: color.withValues(alpha: 0.28)),
      ),
      child: Text(
        '$label $value',
        style: TextStyle(
          fontSize: 11.5,
          fontWeight: FontWeight.w700,
          color: color,
        ),
      ),
    );
  }
}

class _QuickJumpButton extends StatelessWidget {
  const _QuickJumpButton({
    required this.label,
    required this.icon,
    required this.onTap,
  });

  final String label;
  final IconData icon;
  final VoidCallback? onTap;

  @override
  Widget build(BuildContext context) {
    final bool disabled = onTap == null;
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(12),
      child: Opacity(
        opacity: disabled ? 0.45 : 1.0,
        child: Container(
          padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(12),
            border: Border.all(
              color: const Color(0xFF3F8CFF).withValues(alpha: 0.28),
            ),
            color: const Color(0xFF3F8CFF).withValues(alpha: 0.08),
          ),
          child: Row(
            mainAxisSize: MainAxisSize.min,
            children: <Widget>[
              Icon(icon, size: 15, color: const Color(0xFF245FA8)),
              const SizedBox(width: 6),
              Text(
                label,
                style: const TextStyle(
                  fontSize: 11.5,
                  fontWeight: FontWeight.w700,
                  color: Color(0xFF245FA8),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class _Legend extends StatelessWidget {
  const _Legend({required this.color, required this.text});

  final Color color;
  final String text;

  @override
  Widget build(BuildContext context) {
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: <Widget>[
        Container(
          width: 9,
          height: 9,
          decoration: BoxDecoration(color: color, shape: BoxShape.circle),
        ),
        const SizedBox(width: 4),
        Text(text, style: const TextStyle(fontSize: 12)),
      ],
    );
  }
}
