import 'package:flutter/material.dart';

import '../../models/participant_model.dart';
import '../../widgets/video_tile.dart';

class SpeakerLayout extends StatelessWidget {
  const SpeakerLayout({
    super.key,
    required this.participants,
    required this.activeSpeakerId,
    required this.pinnedParticipantId,
    required this.onParticipantTap,
    this.participantMediaBuilder,
  });

  final List<ParticipantModel> participants;
  final String? activeSpeakerId;
  final String? pinnedParticipantId;
  final ValueChanged<ParticipantModel> onParticipantTap;
  final Widget? Function(ParticipantModel participant)? participantMediaBuilder;

  @override
  Widget build(BuildContext context) {
    if (participants.isEmpty) {
      return const SizedBox.shrink();
    }

    final pinned = participants
        .where((p) => p.id == pinnedParticipantId)
        .toList();
    final speaker = participants.where((p) => p.id == activeSpeakerId).toList();
    final main = pinned.isNotEmpty
        ? pinned.first
        : speaker.isNotEmpty
        ? speaker.first
        : participants.first;

    final secondary = participants
        .where((p) => p.id != main.id)
        .toList(growable: false);

    return Padding(
      padding: const EdgeInsets.all(12),
      child: Column(
        children: [
          Expanded(
            child: VideoTile(
              participant: main,
              isActiveSpeaker: main.id == activeSpeakerId,
              onTap: () => onParticipantTap(main),
              media: participantMediaBuilder?.call(main),
            ),
          ),
          const SizedBox(height: 10),
          SizedBox(
            height: 110,
            child: ListView.separated(
              scrollDirection: Axis.horizontal,
              itemCount: secondary.length,
              separatorBuilder: (_, _) => const SizedBox(width: 10),
              itemBuilder: (context, index) {
                final participant = secondary[index];
                return AspectRatio(
                  aspectRatio: 1.6,
                  child: VideoTile(
                    participant: participant,
                    isActiveSpeaker: participant.id == activeSpeakerId,
                    onTap: () => onParticipantTap(participant),
                    media: participantMediaBuilder?.call(participant),
                  ),
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}
