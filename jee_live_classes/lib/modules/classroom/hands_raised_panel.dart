import 'package:flutter/material.dart';

import '../../models/participant_model.dart';

class HandsRaisedPanel extends StatelessWidget {
  const HandsRaisedPanel({
    super.key,
    required this.raisedHands,
    required this.onAllowMic,
    required this.onDismissHand,
  });

  final List<ParticipantModel> raisedHands;
  final ValueChanged<String> onAllowMic;
  final ValueChanged<String> onDismissHand;

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        const _Header(),
        Expanded(
          child: raisedHands.isEmpty
              ? const Center(child: Text('No hands raised right now.'))
              : ListView.builder(
                  padding: const EdgeInsets.all(12),
                  itemCount: raisedHands.length,
                  itemBuilder: (context, index) {
                    final participant = raisedHands[index];
                    return Padding(
                      padding: const EdgeInsets.only(bottom: 8),
                      child: DecoratedBox(
                        decoration: BoxDecoration(
                          color: Colors.white,
                          borderRadius: BorderRadius.circular(12),
                          border: Border.all(color: const Color(0xFFDCE7F7)),
                        ),
                        child: Padding(
                          padding: const EdgeInsets.all(10),
                          child: Row(
                            children: [
                              const Icon(Icons.back_hand_outlined),
                              const SizedBox(width: 8),
                              Expanded(
                                child: Text(
                                  participant.name,
                                  style: const TextStyle(
                                    fontWeight: FontWeight.w700,
                                  ),
                                ),
                              ),
                              OutlinedButton(
                                onPressed: () => onDismissHand(participant.id),
                                child: const Text('Dismiss'),
                              ),
                              const SizedBox(width: 6),
                              FilledButton.tonal(
                                onPressed: () => onAllowMic(participant.id),
                                child: const Text('Allow Mic'),
                              ),
                            ],
                          ),
                        ),
                      ),
                    );
                  },
                ),
        ),
      ],
    );
  }
}

class _Header extends StatelessWidget {
  const _Header();

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: const BoxDecoration(
        border: Border(bottom: BorderSide(color: Color(0xFFDCE7F7))),
      ),
      child: const Padding(
        padding: EdgeInsets.all(12),
        child: Row(
          children: [
            Icon(Icons.pan_tool_alt_outlined),
            SizedBox(width: 8),
            Text('Hands Raised', style: TextStyle(fontWeight: FontWeight.w700)),
          ],
        ),
      ),
    );
  }
}
