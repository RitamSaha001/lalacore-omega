import 'package:flutter/material.dart';

import '../models/participant_model.dart';
import 'speaker_glow_border.dart';

class VideoTile extends StatelessWidget {
  const VideoTile({
    super.key,
    required this.participant,
    required this.isActiveSpeaker,
    required this.onTap,
    this.media,
  });

  final ParticipantModel participant;
  final bool isActiveSpeaker;
  final VoidCallback onTap;
  final Widget? media;

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: onTap,
      child: SpeakerGlowBorder(
        isActive: isActiveSpeaker,
        child: ClipRRect(
          borderRadius: BorderRadius.circular(18),
          child: Stack(
            fit: StackFit.expand,
            children: [
              DecoratedBox(
                decoration: BoxDecoration(
                  gradient: LinearGradient(
                    begin: Alignment.topLeft,
                    end: Alignment.bottomRight,
                    colors: participant.cameraEnabled
                        ? const [Color(0xFF132443), Color(0xFF223F62)]
                        : const [Color(0xFF3A404D), Color(0xFF1D222C)],
                  ),
                ),
                child: media ?? _buildFallback(),
              ),
              Positioned(
                left: 10,
                bottom: 10,
                child: _NameChip(participant: participant),
              ),
              if (participant.isTeacher)
                const Positioned(
                  right: 10,
                  top: 10,
                  child: _Badge(label: 'Host'),
                ),
              if (participant.handRaised)
                const Positioned(
                  right: 10,
                  bottom: 10,
                  child: _Badge(label: 'Hand'),
                ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildFallback() {
    if (!participant.cameraEnabled) {
      return const Center(
        child: Icon(Icons.videocam_off, color: Colors.white70, size: 22),
      );
    }
    return Center(
      child: CircleAvatar(
        radius: 24,
        backgroundColor: Colors.white.withValues(alpha: 0.15),
        child: Text(
          participant.name.characters.first.toUpperCase(),
          style: const TextStyle(
            color: Colors.white,
            fontWeight: FontWeight.w700,
            fontSize: 22,
          ),
        ),
      ),
    );
  }
}

class _NameChip extends StatelessWidget {
  const _NameChip({required this.participant});

  final ParticipantModel participant;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: Colors.black.withValues(alpha: 0.55),
        borderRadius: BorderRadius.circular(999),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 5),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(
              participant.micEnabled ? Icons.mic : Icons.mic_off,
              color: Colors.white,
              size: 13,
            ),
            const SizedBox(width: 4),
            Text(
              participant.name,
              style: const TextStyle(
                color: Colors.white,
                fontWeight: FontWeight.w600,
                fontSize: 11,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _Badge extends StatelessWidget {
  const _Badge({required this.label});

  final String label;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: const Color(0xFF0D9AD8).withValues(alpha: 0.9),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
        child: Text(
          label,
          style: const TextStyle(
            color: Colors.white,
            fontWeight: FontWeight.w700,
            fontSize: 10,
          ),
        ),
      ),
    );
  }
}
