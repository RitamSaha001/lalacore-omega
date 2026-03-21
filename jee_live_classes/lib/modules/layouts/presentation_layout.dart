import 'package:flutter/material.dart';

import '../../models/participant_model.dart';
import '../../widgets/video_tile.dart';

class PresentationLayout extends StatelessWidget {
  const PresentationLayout({
    super.key,
    required this.participants,
    required this.activeSpeakerId,
    required this.sharedContentSource,
    required this.onParticipantTap,
    this.participantMediaBuilder,
    this.sharedContent,
  });

  final List<ParticipantModel> participants;
  final String? activeSpeakerId;
  final String? sharedContentSource;
  final ValueChanged<ParticipantModel> onParticipantTap;
  final Widget? Function(ParticipantModel participant)? participantMediaBuilder;
  final Widget? sharedContent;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(12),
      child: Row(
        children: [
          Expanded(
            flex: 4,
            child: _PresentationStage(
              sharedContentSource: sharedContentSource,
              sharedContent: sharedContent,
            ),
          ),
          const SizedBox(width: 10),
          Expanded(
            flex: 2,
            child: ListView.separated(
              itemCount: participants.length,
              separatorBuilder: (_, _) => const SizedBox(height: 10),
              itemBuilder: (context, index) {
                final participant = participants[index];
                return AspectRatio(
                  aspectRatio: 1.55,
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

class _PresentationStage extends StatelessWidget {
  const _PresentationStage({
    required this.sharedContentSource,
    this.sharedContent,
  });

  final String? sharedContentSource;
  final Widget? sharedContent;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(18),
        gradient: const LinearGradient(
          colors: [Color(0xFF0B172B), Color(0xFF1D3655)],
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
      ),
      child: ClipRRect(
        borderRadius: BorderRadius.circular(18),
        child:
            sharedContent ??
            Center(
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  const Icon(Icons.slideshow, color: Colors.white, size: 42),
                  const SizedBox(height: 10),
                  Text(
                    sharedContentSource ?? 'No active presentation',
                    textAlign: TextAlign.center,
                    style: const TextStyle(
                      color: Colors.white,
                      fontSize: 18,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  const SizedBox(height: 6),
                  const Text(
                    'Screen share / slides / whiteboard stream',
                    style: TextStyle(color: Colors.white70, fontSize: 13),
                  ),
                ],
              ),
            ),
      ),
    );
  }
}
