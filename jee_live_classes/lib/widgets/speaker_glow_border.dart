import 'package:flutter/material.dart';

class SpeakerGlowBorder extends StatelessWidget {
  const SpeakerGlowBorder({
    super.key,
    required this.isActive,
    required this.child,
  });

  final bool isActive;
  final Widget child;

  @override
  Widget build(BuildContext context) {
    return AnimatedContainer(
      duration: const Duration(milliseconds: 220),
      curve: Curves.easeOut,
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(18),
        border: Border.all(
          color: isActive ? const Color(0xFF19B8FF) : Colors.transparent,
          width: 2.2,
        ),
        boxShadow: isActive
            ? const [
                BoxShadow(
                  color: Color(0x5519B8FF),
                  blurRadius: 16,
                  spreadRadius: 1.5,
                ),
              ]
            : const [],
      ),
      child: ClipRRect(borderRadius: BorderRadius.circular(16), child: child),
    );
  }
}
