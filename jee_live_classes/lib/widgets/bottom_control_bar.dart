import 'package:flutter/material.dart';

import 'glass_panel.dart';

class BottomControlBar extends StatelessWidget {
  const BottomControlBar({
    super.key,
    required this.isMicOn,
    required this.isCameraOn,
    required this.handRaised,
    required this.onMicTap,
    required this.onCameraTap,
    required this.onRaiseHandTap,
    required this.onChatTap,
    required this.onReactionTap,
    required this.onMoreTap,
  });

  final bool isMicOn;
  final bool isCameraOn;
  final bool handRaised;
  final VoidCallback onMicTap;
  final VoidCallback onCameraTap;
  final VoidCallback onRaiseHandTap;
  final VoidCallback onChatTap;
  final VoidCallback onReactionTap;
  final VoidCallback onMoreTap;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(12, 0, 12, 12),
      child: GlassPanel(
        padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
        borderRadius: const BorderRadius.all(Radius.circular(26)),
        blurSigma: 12,
        tintColor: const Color(0xD9F8FBFF),
        borderColor: const Color(0x52FFFFFF),
        child: SingleChildScrollView(
          scrollDirection: Axis.horizontal,
          child: Row(
            children: [
              _ControlButton(
                icon: isMicOn ? Icons.mic_rounded : Icons.mic_off_rounded,
                label: 'Mic',
                active: isMicOn,
                destructive: !isMicOn,
                onTap: onMicTap,
              ),
              _ControlButton(
                icon: isCameraOn
                    ? Icons.videocam_rounded
                    : Icons.videocam_off_rounded,
                label: 'Camera',
                active: isCameraOn,
                destructive: !isCameraOn,
                onTap: onCameraTap,
              ),
              _ControlButton(
                icon: handRaised
                    ? Icons.pan_tool_rounded
                    : Icons.back_hand_rounded,
                label: handRaised ? 'Lower Hand' : 'Raise Hand',
                active: handRaised,
                onTap: onRaiseHandTap,
              ),
              _ControlButton(
                icon: Icons.chat_bubble_outline_rounded,
                label: 'Chat',
                active: false,
                onTap: onChatTap,
              ),
              _ControlButton(
                icon: Icons.emoji_emotions_outlined,
                label: 'React',
                active: false,
                onTap: onReactionTap,
              ),
              _ControlButton(
                icon: Icons.tune_rounded,
                label: 'Controls',
                active: false,
                onTap: onMoreTap,
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class _ControlButton extends StatelessWidget {
  const _ControlButton({
    required this.icon,
    required this.label,
    required this.active,
    required this.onTap,
    this.destructive = false,
  });

  final IconData icon;
  final String label;
  final bool active;
  final VoidCallback onTap;
  final bool destructive;

  @override
  Widget build(BuildContext context) {
    return InkWell(
      borderRadius: BorderRadius.circular(18),
      onTap: onTap,
      child: Ink(
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
        decoration: BoxDecoration(
          color: active
              ? const Color(0xCCE8F3FF)
              : destructive
              ? const Color(0xCCFFEDEE)
              : const Color(0xB3F4F7FC),
          borderRadius: BorderRadius.circular(18),
          border: Border.all(
            color: active
                ? const Color(0xFF9BC8F0)
                : destructive
                ? const Color(0xFFF2B8BC)
                : const Color(0xFFE0E7F1),
          ),
        ),
        child: Row(
          children: [
            Icon(
              icon,
              size: 18,
              color: destructive && !active
                  ? const Color(0xFFB74242)
                  : const Color(0xFF16304C),
            ),
            const SizedBox(width: 6),
            Text(
              label,
              style: TextStyle(
                fontWeight: FontWeight.w700,
                color: destructive && !active
                    ? const Color(0xFF8D2F2F)
                    : const Color(0xFF16304C),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
