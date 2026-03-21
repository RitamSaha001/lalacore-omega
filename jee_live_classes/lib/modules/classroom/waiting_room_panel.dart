import 'package:flutter/material.dart';

import '../../models/waiting_room_request_model.dart';
import '../../widgets/glass_panel.dart';

class WaitingRoomPanel extends StatelessWidget {
  const WaitingRoomPanel({
    super.key,
    required this.requests,
    required this.onApprove,
    required this.onReject,
    this.onApproveAll,
  });

  final List<WaitingRoomRequestModel> requests;
  final ValueChanged<String> onApprove;
  final ValueChanged<String> onReject;
  final VoidCallback? onApproveAll;

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(12, 12, 12, 8),
          child: GlassPanel(
            padding: const EdgeInsets.all(12),
            child: Row(
              children: [
                Container(
                  width: 36,
                  height: 36,
                  decoration: BoxDecoration(
                    color: const Color(0xFFE9F2FF),
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: const Icon(
                    Icons.pending_actions_rounded,
                    color: Color(0xFF285EA8),
                  ),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text(
                        'Waiting Room',
                        style: TextStyle(fontWeight: FontWeight.w800),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        requests.isEmpty
                            ? 'No pending join requests'
                            : '${requests.length} student${requests.length == 1 ? '' : 's'} waiting',
                        style: const TextStyle(
                          color: Color(0xFF52677D),
                          fontSize: 12.5,
                        ),
                      ),
                    ],
                  ),
                ),
                if (onApproveAll != null && requests.isNotEmpty)
                  FilledButton.tonal(
                    onPressed: onApproveAll,
                    child: const Text('Admit All'),
                  ),
              ],
            ),
          ),
        ),
        Expanded(
          child: requests.isEmpty
              ? const Center(child: Text('No pending join requests.'))
              : ListView.builder(
                  padding: const EdgeInsets.fromLTRB(12, 4, 12, 12),
                  itemCount: requests.length,
                  itemBuilder: (context, index) {
                    final item = requests[index];
                    return Padding(
                      padding: const EdgeInsets.only(bottom: 8),
                      child: GlassPanel(
                        padding: const EdgeInsets.all(12),
                        child: Row(
                          children: [
                            Container(
                              width: 42,
                              height: 42,
                              decoration: BoxDecoration(
                                color: const Color(0xFFEAF2FF),
                                borderRadius: BorderRadius.circular(14),
                              ),
                              alignment: Alignment.center,
                              child: Text(
                                item.name.trim().isEmpty
                                    ? '?'
                                    : item.name.trim()[0].toUpperCase(),
                                style: const TextStyle(
                                  fontWeight: FontWeight.w800,
                                  color: Color(0xFF23456E),
                                ),
                              ),
                            ),
                            const SizedBox(width: 12),
                            Expanded(
                              child: Column(
                                crossAxisAlignment: CrossAxisAlignment.start,
                                children: [
                                  Text(
                                    item.name,
                                    style: const TextStyle(
                                      fontWeight: FontWeight.w800,
                                    ),
                                  ),
                                  const SizedBox(height: 2),
                                  Text(
                                    'Requested at ${item.requestedAt.hour.toString().padLeft(2, '0')}:${item.requestedAt.minute.toString().padLeft(2, '0')}',
                                    style: const TextStyle(
                                      color: Color(0xFF52677D),
                                      fontSize: 12,
                                    ),
                                  ),
                                ],
                              ),
                            ),
                            TextButton(
                              onPressed: () => onReject(item.participantId),
                              child: const Text('Reject'),
                            ),
                            const SizedBox(width: 6),
                            FilledButton(
                              onPressed: () => onApprove(item.participantId),
                              child: const Text('Approve'),
                            ),
                          ],
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
