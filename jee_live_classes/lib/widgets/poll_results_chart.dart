import 'package:flutter/material.dart';

class PollResultsChart extends StatelessWidget {
  const PollResultsChart({
    super.key,
    required this.options,
    required this.optionCounts,
    this.correctOption,
  });

  final List<String> options;
  final Map<int, int> optionCounts;
  final int? correctOption;

  @override
  Widget build(BuildContext context) {
    final totalResponses = optionCounts.values.fold<int>(
      0,
      (sum, item) => sum + item,
    );

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: options
          .asMap()
          .entries
          .map((entry) {
            final index = entry.key;
            final label = entry.value;
            final count = optionCounts[index] ?? 0;
            final ratio = totalResponses == 0 ? 0.0 : count / totalResponses;
            final percent = (ratio * 100).toStringAsFixed(0);
            final isCorrect = correctOption == index;

            return Padding(
              padding: const EdgeInsets.only(bottom: 8),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      Expanded(
                        child: Text(
                          '${String.fromCharCode(65 + index)}) $label',
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                          style: TextStyle(
                            fontWeight: isCorrect
                                ? FontWeight.w700
                                : FontWeight.w500,
                            color: isCorrect
                                ? const Color(0xFF0D7A2A)
                                : const Color(0xFF2A3D55),
                          ),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Text('$percent% ($count)'),
                    ],
                  ),
                  const SizedBox(height: 4),
                  ClipRRect(
                    borderRadius: BorderRadius.circular(999),
                    child: LinearProgressIndicator(
                      minHeight: 8,
                      value: ratio,
                      backgroundColor: const Color(0xFFE8EEF8),
                      valueColor: AlwaysStoppedAnimation<Color>(
                        isCorrect
                            ? const Color(0xFF3CBF64)
                            : const Color(0xFF4F81C7),
                      ),
                    ),
                  ),
                ],
              ),
            );
          })
          .toList(growable: false),
    );
  }
}
