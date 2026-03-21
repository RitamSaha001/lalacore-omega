import 'package:flutter/material.dart';

import '../../models/participant_model.dart';
import '../../widgets/video_tile.dart';

class GridLayout extends StatelessWidget {
  const GridLayout({
    super.key,
    required this.participants,
    required this.activeSpeakerId,
    required this.onParticipantTap,
    this.participantMediaBuilder,
  });

  final List<ParticipantModel> participants;
  final String? activeSpeakerId;
  final ValueChanged<ParticipantModel> onParticipantTap;
  final Widget? Function(ParticipantModel participant)? participantMediaBuilder;

  @override
  Widget build(BuildContext context) {
    final count = participants.length;
    final crossAxisCount = count <= 2
        ? 2
        : count <= 4
        ? 2
        : 3;

    return GridView.builder(
      padding: const EdgeInsets.all(12),
      itemCount: participants.length,
      gridDelegate: SliverGridDelegateWithFixedCrossAxisCount(
        crossAxisCount: crossAxisCount,
        mainAxisSpacing: 10,
        crossAxisSpacing: 10,
        childAspectRatio: 1.4,
      ),
      itemBuilder: (context, index) {
        final participant = participants[index];
        return VideoTile(
          participant: participant,
          isActiveSpeaker: participant.id == activeSpeakerId,
          onTap: () => onParticipantTap(participant),
          media: participantMediaBuilder?.call(participant),
        );
      },
    );
  }
}
