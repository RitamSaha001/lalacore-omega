import 'package:flutter/material.dart';

import '../../models/participant_model.dart';
import '../../services/network_quality_service.dart';

class ParticipantsPanel extends StatelessWidget {
  const ParticipantsPanel({
    super.key,
    required this.participants,
    required this.canManageClass,
    required this.networkQualityService,
    required this.onMute,
    required this.onRemove,
    required this.onDisableCamera,
    required this.onPromote,
  });

  final List<ParticipantModel> participants;
  final bool canManageClass;
  final NetworkQualityService networkQualityService;

  final ValueChanged<String> onMute;
  final ValueChanged<String> onRemove;
  final ValueChanged<String> onDisableCamera;
  final ValueChanged<String> onPromote;

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        const _PanelHeader(
          icon: Icons.groups,
          title: 'Participants',
          subtitle: 'Manage class attendees in real time',
        ),
        Expanded(
          child: ListView.separated(
            padding: const EdgeInsets.all(12),
            itemCount: participants.length,
            separatorBuilder: (_, _) => const SizedBox(height: 8),
            itemBuilder: (context, index) {
              final participant = participants[index];
              return _ParticipantTile(
                participant: participant,
                qualityLabel: networkQualityService.qualityLabel(
                  participant.networkQuality,
                ),
                canManageClass: canManageClass,
                onMute: () => onMute(participant.id),
                onRemove: () => onRemove(participant.id),
                onDisableCamera: () => onDisableCamera(participant.id),
                onPromote: () => onPromote(participant.id),
              );
            },
          ),
        ),
      ],
    );
  }
}

class _ParticipantTile extends StatelessWidget {
  const _ParticipantTile({
    required this.participant,
    required this.qualityLabel,
    required this.canManageClass,
    required this.onMute,
    required this.onRemove,
    required this.onDisableCamera,
    required this.onPromote,
  });

  final ParticipantModel participant;
  final String qualityLabel;
  final bool canManageClass;
  final VoidCallback onMute;
  final VoidCallback onRemove;
  final VoidCallback onDisableCamera;
  final VoidCallback onPromote;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0xFFDFE8F6)),
      ),
      child: Padding(
        padding: const EdgeInsets.all(10),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                CircleAvatar(
                  radius: 16,
                  child: Text(participant.name.characters.first),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: Text(
                    participant.name,
                    style: const TextStyle(fontWeight: FontWeight.w700),
                  ),
                ),
                if (participant.handRaised)
                  const Icon(
                    Icons.pan_tool_alt,
                    color: Color(0xFFF28A17),
                    size: 18,
                  ),
                const SizedBox(width: 8),
                Icon(
                  participant.micEnabled ? Icons.mic : Icons.mic_off,
                  size: 18,
                ),
                const SizedBox(width: 6),
                Icon(
                  participant.cameraEnabled
                      ? Icons.videocam
                      : Icons.videocam_off,
                  size: 18,
                ),
              ],
            ),
            const SizedBox(height: 6),
            Text(
              'Quality: $qualityLabel',
              style: const TextStyle(fontSize: 12, color: Color(0xFF35506E)),
            ),
            if (canManageClass && !participant.isTeacher)
              Padding(
                padding: const EdgeInsets.only(top: 8),
                child: Wrap(
                  spacing: 6,
                  runSpacing: 6,
                  children: [
                    _ActionChip(label: 'Mute', onTap: onMute),
                    _ActionChip(label: 'Disable Cam', onTap: onDisableCamera),
                    _ActionChip(label: 'Promote', onTap: onPromote),
                    _ActionChip(label: 'Remove', onTap: onRemove),
                  ],
                ),
              ),
          ],
        ),
      ),
    );
  }
}

class _ActionChip extends StatelessWidget {
  const _ActionChip({required this.label, required this.onTap});

  final String label;
  final VoidCallback onTap;

  @override
  Widget build(BuildContext context) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(999),
      child: Ink(
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
        decoration: BoxDecoration(
          color: const Color(0xFFF0F5FF),
          borderRadius: BorderRadius.circular(999),
          border: Border.all(color: const Color(0xFFCEDDF3)),
        ),
        child: Text(
          label,
          style: const TextStyle(fontSize: 12, fontWeight: FontWeight.w600),
        ),
      ),
    );
  }
}

class _PanelHeader extends StatelessWidget {
  const _PanelHeader({
    required this.icon,
    required this.title,
    required this.subtitle,
  });

  final IconData icon;
  final String title;
  final String subtitle;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: const BoxDecoration(
        border: Border(bottom: BorderSide(color: Color(0xFFDCE7F7))),
      ),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          children: [
            Icon(icon, size: 20),
            const SizedBox(width: 8),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    title,
                    style: const TextStyle(fontWeight: FontWeight.w700),
                  ),
                  Text(
                    subtitle,
                    style: const TextStyle(
                      fontSize: 12,
                      color: Color(0xFF4A607C),
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
