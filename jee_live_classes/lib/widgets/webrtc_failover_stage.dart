import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart';

import '../services/webrtc_failover_service.dart';

class WebRtcFailoverStage extends StatelessWidget {
  const WebRtcFailoverStage({
    super.key,
    required this.service,
    required this.onRetry,
  });

  final WebRtcFailoverService service;
  final Future<void> Function() onRetry;

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<WebRtcFailoverSnapshot>(
      valueListenable: service.snapshotListenable,
      builder: (context, snapshot, _) {
        return DecoratedBox(
          decoration: BoxDecoration(
            color: const Color(0xCC0A1A2C),
            borderRadius: BorderRadius.circular(16),
            border: Border.all(color: const Color(0x335FB5FF)),
          ),
          child: Padding(
            padding: const EdgeInsets.all(12),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    const Icon(Icons.wifi_tethering, color: Colors.white),
                    const SizedBox(width: 8),
                    Expanded(
                      child: Text(
                        'WebRTC Failover',
                        style: const TextStyle(
                          color: Colors.white,
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                    ),
                    Text(
                      snapshot.connectionState.name.toUpperCase(),
                      style: const TextStyle(
                        color: Color(0xFF9FD2FF),
                        fontSize: 11,
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ],
                ),
                const SizedBox(height: 6),
                Text(
                  snapshot.statusMessage,
                  style: const TextStyle(
                    color: Color(0xFFC7DDF4),
                    fontSize: 12,
                  ),
                ),
                const SizedBox(height: 10),
                Expanded(
                  child: Row(
                    children: [
                      Expanded(
                        child: _VideoPane(
                          title: 'Fallback Remote',
                          rendererListenable: service.remoteRendererListenable,
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: _VideoPane(
                          title: 'Fallback Local',
                          rendererListenable: service.localRendererListenable,
                          mirror: true,
                        ),
                      ),
                    ],
                  ),
                ),
                const SizedBox(height: 10),
                Row(
                  children: [
                    FilledButton.tonalIcon(
                      onPressed: onRetry,
                      icon: const Icon(Icons.refresh),
                      label: const Text('Retry Failover'),
                    ),
                    const SizedBox(width: 8),
                    FilledButton.tonalIcon(
                      onPressed: () =>
                          service.setMicEnabled(!snapshot.micEnabled),
                      icon: Icon(
                        snapshot.micEnabled ? Icons.mic : Icons.mic_off,
                      ),
                      label: Text(
                        snapshot.micEnabled ? 'Mute Mic' : 'Unmute Mic',
                      ),
                    ),
                    const SizedBox(width: 8),
                    FilledButton.tonalIcon(
                      onPressed: () =>
                          service.setCameraEnabled(!snapshot.cameraEnabled),
                      icon: Icon(
                        snapshot.cameraEnabled
                            ? Icons.videocam
                            : Icons.videocam_off,
                      ),
                      label: Text(
                        snapshot.cameraEnabled ? 'Camera Off' : 'Camera On',
                      ),
                    ),
                  ],
                ),
              ],
            ),
          ),
        );
      },
    );
  }
}

class _VideoPane extends StatelessWidget {
  const _VideoPane({
    required this.title,
    required this.rendererListenable,
    this.mirror = false,
  });

  final String title;
  final ValueListenable<RTCVideoRenderer?> rendererListenable;
  final bool mirror;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: const Color(0xFF071322),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: const Color(0x334683CB)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Padding(
            padding: const EdgeInsets.all(8),
            child: Text(
              title,
              style: const TextStyle(
                color: Color(0xFFCBE4FF),
                fontWeight: FontWeight.w600,
                fontSize: 12,
              ),
            ),
          ),
          Expanded(
            child: ValueListenableBuilder<RTCVideoRenderer?>(
              valueListenable: rendererListenable,
              builder: (context, renderer, _) {
                if (renderer == null || renderer.srcObject == null) {
                  return const Center(
                    child: Text(
                      'Waiting for stream...',
                      style: TextStyle(color: Color(0xFF86A7CA), fontSize: 12),
                    ),
                  );
                }
                return ClipRRect(
                  borderRadius: const BorderRadius.only(
                    bottomLeft: Radius.circular(12),
                    bottomRight: Radius.circular(12),
                  ),
                  child: RTCVideoView(renderer, mirror: mirror),
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}
