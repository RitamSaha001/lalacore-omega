import 'package:flutter/material.dart';

import '../../models/live_class_context.dart';
import '../../widgets/glass_panel.dart';

class JoinClassScreen extends StatelessWidget {
  const JoinClassScreen({
    super.key,
    required this.contextData,
    required this.onJoinClass,
    required this.onCancel,
  });

  final LiveClassContext contextData;
  final VoidCallback onJoinClass;
  final VoidCallback onCancel;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: DecoratedBox(
        decoration: const BoxDecoration(
          gradient: LinearGradient(
            colors: <Color>[Color(0xFFF3F8FF), Color(0xFFE4EEFB)],
            begin: Alignment.topCenter,
            end: Alignment.bottomCenter,
          ),
        ),
        child: SafeArea(
          child: Center(
            child: ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 560),
              child: Padding(
                padding: const EdgeInsets.all(20),
                child: GlassPanel(
                  padding: const EdgeInsets.all(22),
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Row(
                        children: <Widget>[
                          Icon(
                            Icons.live_tv_rounded,
                            color: Color(0xFF285EA8),
                          ),
                          SizedBox(width: 10),
                          Expanded(
                            child: Text(
                              'Join Live Class',
                              style: TextStyle(
                                fontSize: 22,
                                fontWeight: FontWeight.w800,
                              ),
                            ),
                          ),
                        ],
                      ),
                      const SizedBox(height: 12),
                      Text(
                        contextData.classTitle,
                        style: const TextStyle(
                          fontSize: 18,
                          fontWeight: FontWeight.w700,
                          height: 1.2,
                        ),
                      ),
                      const SizedBox(height: 8),
                      Wrap(
                        spacing: 8,
                        runSpacing: 8,
                        children: <Widget>[
                          _MetaChip(
                            icon: Icons.school_outlined,
                            label: (contextData.className ?? '').trim().isEmpty
                                ? 'Live Class'
                                : contextData.className!.trim(),
                          ),
                          _MetaChip(
                            icon: Icons.menu_book_rounded,
                            label: contextData.subject,
                          ),
                          _MetaChip(
                            icon: Icons.draw_outlined,
                            label: contextData.topic,
                          ),
                        ],
                      ),
                      const SizedBox(height: 14),
                      _DetailRow(
                        icon: Icons.person_outline_rounded,
                        label: 'Teacher',
                        value: contextData.teacherName,
                      ),
                      if ((contextData.startTimeLabel ?? '').trim().isNotEmpty)
                        _DetailRow(
                          icon: Icons.schedule_rounded,
                          label: 'Starts',
                          value: contextData.startTimeLabel!.trim(),
                        ),
                      _DetailRow(
                        icon: Icons.badge_outlined,
                        label: 'Joining as',
                        value: contextData.userName,
                      ),
                      const SizedBox(height: 18),
                      Row(
                        children: [
                          Expanded(
                            child: OutlinedButton(
                              onPressed: onCancel,
                              child: const Text('Cancel'),
                            ),
                          ),
                          const SizedBox(width: 12),
                          Expanded(
                            child: FilledButton.icon(
                              onPressed: onJoinClass,
                              icon: const Icon(
                                Icons.video_camera_front_outlined,
                              ),
                              label: const Text('Continue'),
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}

class _MetaChip extends StatelessWidget {
  const _MetaChip({required this.icon, required this.label});

  final IconData icon;
  final String label;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: const Color(0xFFE9F2FF),
        borderRadius: BorderRadius.circular(999),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: <Widget>[
            Icon(icon, size: 16, color: const Color(0xFF285EA8)),
            const SizedBox(width: 6),
            Text(
              label,
              style: const TextStyle(
                color: Color(0xFF23456E),
                fontWeight: FontWeight.w700,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _DetailRow extends StatelessWidget {
  const _DetailRow({
    required this.icon,
    required this.label,
    required this.value,
  });

  final IconData icon;
  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 10),
      child: Row(
        children: <Widget>[
          Icon(icon, size: 18, color: const Color(0xFF5C7393)),
          const SizedBox(width: 10),
          Text(
            '$label:',
            style: const TextStyle(
              color: Color(0xFF5C7393),
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(width: 8),
          Expanded(
            child: Text(
              value,
              style: const TextStyle(
                fontWeight: FontWeight.w700,
                color: Color(0xFF203145),
              ),
            ),
          ),
        ],
      ),
    );
  }
}
