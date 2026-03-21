import 'dart:async';

import 'package:flutter/material.dart';

import '../../models/live_class_context.dart';
import '../../services/class_join_service.dart';
import '../../widgets/glass_panel.dart';
import '../classroom/classroom_controller.dart';
import '../classroom/classroom_screen.dart';

class WaitingRoomScreen extends StatefulWidget {
  const WaitingRoomScreen({
    super.key,
    required this.controller,
    required this.contextData,
    required this.requestId,
    required this.cameraEnabled,
    required this.micEnabled,
  });

  final ClassroomController controller;
  final LiveClassContext contextData;
  final String requestId;
  final bool cameraEnabled;
  final bool micEnabled;

  @override
  State<WaitingRoomScreen> createState() => _WaitingRoomScreenState();
}

class _WaitingRoomScreenState extends State<WaitingRoomScreen> {
  StreamSubscription<JoinApprovalEvent>? _approvalSubscription;
  bool _joining = false;
  String? _statusMessage;

  @override
  void initState() {
    super.initState();
    _approvalSubscription = widget.controller.joinEventsStream.listen((event) {
      if (event.userId != widget.contextData.userId) {
        return;
      }
      if (event.status == JoinApprovalStatus.approved) {
        _onApproved();
      } else if (event.status == JoinApprovalStatus.rejected ||
          event.status == JoinApprovalStatus.duplicate) {
        setState(() {
          _statusMessage = event.message ?? 'Teacher declined your request.';
        });
      }
    });
  }

  @override
  void dispose() {
    _approvalSubscription?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder(
      valueListenable: widget.controller,
      builder: (context, state, _) {
        final queue = widget.controller.queuePositionForCurrentUser();
        return Scaffold(
          body: DecoratedBox(
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFFF2F7FF), Color(0xFFDCE9FB)],
                begin: Alignment.topCenter,
                end: Alignment.bottomCenter,
              ),
            ),
            child: SafeArea(
              child: Center(
                child: ConstrainedBox(
                  constraints: const BoxConstraints(maxWidth: 620),
                  child: Padding(
                    padding: const EdgeInsets.all(20),
                    child: GlassPanel(
                      padding: const EdgeInsets.all(22),
                      child: Column(
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          Container(
                            width: 58,
                            height: 58,
                            decoration: BoxDecoration(
                              shape: BoxShape.circle,
                              color: const Color(0xFFE9F2FF),
                              border: Border.all(
                                color: const Color(0xFFBED6F6),
                              ),
                            ),
                            child: const Icon(
                              Icons.hourglass_top_rounded,
                              size: 30,
                              color: Color(0xFF2B5FA6),
                            ),
                          ),
                          const SizedBox(height: 14),
                          const Text(
                            'Waiting for Teacher Approval',
                            textAlign: TextAlign.center,
                            style: TextStyle(
                              fontSize: 21,
                              fontWeight: FontWeight.w800,
                            ),
                          ),
                          const SizedBox(height: 10),
                          Wrap(
                            alignment: WrapAlignment.center,
                            spacing: 8,
                            runSpacing: 8,
                            children: <Widget>[
                              _QueueChip(
                                icon: Icons.draw_outlined,
                                label: widget.contextData.topic,
                              ),
                              _QueueChip(
                                icon: Icons.person_outline_rounded,
                                label: widget.contextData.teacherName,
                              ),
                              if (queue != -1)
                                _QueueChip(
                                  icon: Icons.queue_rounded,
                                  label: 'Queue #$queue',
                                ),
                            ],
                          ),
                          const SizedBox(height: 14),
                          Text(
                            _statusMessage ??
                                state.joinStatusMessage ??
                                'Your join request is in the waiting room. The class will open automatically once the teacher admits you.',
                            textAlign: TextAlign.center,
                            style: const TextStyle(
                              color: Color(0xFF4E6484),
                              height: 1.35,
                            ),
                          ),
                          const SizedBox(height: 18),
                          if (_joining)
                            const CircularProgressIndicator()
                          else
                            SizedBox(
                              width: double.infinity,
                              child: OutlinedButton.icon(
                                onPressed: _leaveQueue,
                                icon: const Icon(Icons.logout_rounded),
                                label: const Text('Leave Waiting Room'),
                              ),
                            ),
                        ],
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        );
      },
    );
  }

  Future<void> _onApproved() async {
    if (_joining) {
      return;
    }
    setState(() {
      _joining = true;
      _statusMessage = 'Approved. Joining classroom...';
    });

    try {
      await widget.controller.initialize();
      await widget.controller.applyPreJoinSettings(
        cameraEnabled: widget.cameraEnabled,
        micEnabled: widget.micEnabled,
      );
      if (!mounted) {
        return;
      }
      await Navigator.of(context).pushReplacement(
        MaterialPageRoute<void>(
          builder: (_) => ClassroomScreen(controller: widget.controller),
        ),
      );
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _statusMessage = 'Join failed: $error';
        _joining = false;
      });
    }
  }

  Future<void> _leaveQueue() async {
    await widget.controller.cancelJoinRequest(widget.requestId);
    if (!mounted) {
      return;
    }
    Navigator.of(context).maybePop();
  }
}

class _QueueChip extends StatelessWidget {
  const _QueueChip({required this.icon, required this.label});

  final IconData icon;
  final String label;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: const Color(0xFFEAF3FF),
        borderRadius: BorderRadius.circular(999),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: <Widget>[
            Icon(icon, size: 16, color: const Color(0xFF2B5FA6)),
            const SizedBox(width: 6),
            Text(
              label,
              style: const TextStyle(
                color: Color(0xFF23456E),
                fontWeight: FontWeight.w700,
              ),
            ),
          ],
        ),
      ),
    );
  }
}
