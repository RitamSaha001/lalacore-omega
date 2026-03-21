import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:jee_live_classes/services/transcription_service.dart';

void main() {
  test(
    'real transcription service emits transcript messages from websocket',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() async {
        await server.close(force: true);
      });

      final serverSubscription = server
          .transform(WebSocketTransformer())
          .listen((socket) {
            socket.listen((dynamic raw) {
              final payload = jsonDecode(raw as String) as Map<String, dynamic>;
              socket.add(
                jsonEncode({
                  'speaker_id': payload['speaker_id'],
                  'speaker_name': payload['speaker_name'],
                  'text': 'Gauss law is symmetry driven.',
                  'timestamp': '2026-03-12T10:00:00Z',
                  'confidence': 0.97,
                }),
              );
            });
          });
      addTearDown(serverSubscription.cancel);

      final service = RealTranscriptionService(
        streamUrl: 'ws://127.0.0.1:${server.port}',
        jwtToken: 'test-token',
        speakerId: 'teacher_01',
        speakerName: 'Dr Sharma',
        flushInterval: const Duration(milliseconds: 10),
      );
      addTearDown(service.dispose);

      await service.start();
      await Future<void>.delayed(const Duration(milliseconds: 40));

      service.pushAudioChunk(
        Uint8List.fromList(const [1, 2, 3, 4]),
        speakerId: 'teacher_01',
        speakerName: 'Dr Sharma',
      );

      final chunk = await service.transcriptStream.first.timeout(
        const Duration(seconds: 2),
      );

      expect(chunk.speakerId, 'teacher_01');
      expect(chunk.speakerName, 'Dr Sharma');
      expect(chunk.message, 'Gauss law is symmetry driven.');
      expect(chunk.confidence, 0.97);

      await service.stop();
    },
  );
}
