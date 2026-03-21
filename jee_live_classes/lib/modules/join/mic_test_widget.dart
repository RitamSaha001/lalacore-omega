import 'package:flutter/material.dart';

class MicTestWidget extends StatelessWidget {
  const MicTestWidget({
    super.key,
    required this.level,
    required this.enabled,
    this.caption,
  });

  final double level;
  final bool enabled;
  final String? caption;

  @override
  Widget build(BuildContext context) {
    final clamped = level.clamp(0.0, 1.0);
    return DecoratedBox(
      decoration: BoxDecoration(
        color: const Color(0xFFF4F8FF),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFFD7E5F7)),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Icon(enabled ? Icons.mic : Icons.mic_off, size: 18),
                const SizedBox(width: 8),
                Text(
                  enabled ? 'Mic level' : 'Mic disabled',
                  style: const TextStyle(fontWeight: FontWeight.w600),
                ),
              ],
            ),
            const SizedBox(height: 8),
            ClipRRect(
              borderRadius: BorderRadius.circular(999),
              child: LinearProgressIndicator(
                value: enabled ? clamped : 0,
                minHeight: 8,
                backgroundColor: const Color(0xFFD9E4F4),
                valueColor: const AlwaysStoppedAnimation<Color>(
                  Color(0xFF2F9E65),
                ),
              ),
            ),
            if ((caption ?? '').isNotEmpty) ...[
              const SizedBox(height: 8),
              Text(
                caption!,
                style: const TextStyle(fontSize: 12, color: Color(0xFF59728F)),
              ),
            ],
          ],
        ),
      ),
    );
  }
}
