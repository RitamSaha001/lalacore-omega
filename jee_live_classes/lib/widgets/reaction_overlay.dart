import 'dart:math';

import 'package:flutter/material.dart';

import '../modules/classroom/classroom_state.dart';

class ReactionOverlay extends StatelessWidget {
  const ReactionOverlay({super.key, required this.reactions});

  final List<ReactionEvent> reactions;

  @override
  Widget build(BuildContext context) {
    final random = Random(17);

    return IgnorePointer(
      child: Stack(
        children: reactions
            .map((reaction) {
              final left = 40.0 + random.nextDouble() * 220;
              final bottom = 24.0 + random.nextDouble() * 150;

              return Positioned(
                left: left,
                bottom: bottom,
                child: TweenAnimationBuilder<double>(
                  duration: const Duration(milliseconds: 700),
                  tween: Tween<double>(begin: 1, end: 0),
                  builder: (context, value, child) {
                    return Opacity(
                      opacity: value,
                      child: Transform.translate(
                        offset: Offset(0, -48 * (1 - value)),
                        child: Transform.scale(
                          scale: 0.8 + (value * 0.5),
                          child: child,
                        ),
                      ),
                    );
                  },
                  child: Text(
                    reaction.emoji,
                    style: const TextStyle(fontSize: 34),
                  ),
                ),
              );
            })
            .toList(growable: false),
      ),
    );
  }
}
