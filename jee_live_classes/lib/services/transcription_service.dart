import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:record/record.dart';
import 'package:speech_to_text/speech_to_text.dart' as stt;
import 'package:web_socket_channel/web_socket_channel.dart';

import '../models/transcript_model.dart';

abstract class TranscriptionService {
  Stream<TranscriptModel> get transcriptStream;

  Future<void> start();
  Future<void> stop();
  void pushAudioChunk(
    Uint8List bytes, {
    required String speakerId,
    required String speakerName,
  });
  void dispose();
}

class MockTranscriptionService implements TranscriptionService {
  // BEGIN_PHASE2_IMPLEMENTATION
  final _controller = StreamController<TranscriptModel>.broadcast();

  Timer? _timer;
  int _counter = 0;

  final List<Map<String, String>> _chunks = const [
    {
      'speakerId': 'teacher_01',
      'speakerName': 'Dr. A. Sharma',
      'text': 'Today we derive Gauss law from Coulomb symmetry arguments.',
    },
    {
      'speakerId': 'teacher_01',
      'speakerName': 'Dr. A. Sharma',
      'text':
          'Notice the electric field is radial for spherical charge distributions.',
    },
    {
      'speakerId': 'student_02',
      'speakerName': 'Ananya',
      'text': 'Can we use the same shortcut for non-uniform density?',
    },
    {
      'speakerId': 'teacher_01',
      'speakerName': 'Dr. A. Sharma',
      'text':
          'Only if the symmetry holds, otherwise direct integration is safer.',
    },
  ];

  @override
  Stream<TranscriptModel> get transcriptStream => _controller.stream;

  @override
  Future<void> start() async {
    _timer?.cancel();
    _timer = Timer.periodic(const Duration(seconds: 3), (_) {
      final chunk = _chunks[_counter % _chunks.length];
      _counter += 1;
      _controller.add(
        TranscriptModel(
          id: 'txn_$_counter',
          speakerId: chunk['speakerId']!,
          speakerName: chunk['speakerName']!,
          message: chunk['text']!,
          timestamp: DateTime.now(),
          confidence: 0.96,
          source: 'mock',
        ),
      );
    });
  }

  @override
  Future<void> stop() async {
    _timer?.cancel();
  }

  @override
  void pushAudioChunk(
    Uint8List bytes, {
    required String speakerId,
    required String speakerName,
  }) {
    // Intentionally ignored in mock mode.
  }

  @override
  void dispose() {
    _timer?.cancel();
    _controller.close();
  }

  // END_PHASE2_IMPLEMENTATION
}

class RealTranscriptionService implements TranscriptionService {
  // BEGIN_PHASE2_IMPLEMENTATION
  RealTranscriptionService({
    required this.streamUrl,
    required this.jwtToken,
    required this.speakerId,
    required this.speakerName,
    this.enableOnDeviceSpeechRecognition = true,
    this.enableServerAudioStreaming = true,
    this.preferredLocaleIds = const ['bn_IN', 'en_IN', 'bn_BD', 'en_US'],
    this.flushInterval = const Duration(milliseconds: 450),
    this.audioSampleRate = 16000,
    this.audioChannels = 1,
  });

  final String streamUrl;
  final String jwtToken;
  final String speakerId;
  final String speakerName;
  final bool enableOnDeviceSpeechRecognition;
  final bool enableServerAudioStreaming;
  final List<String> preferredLocaleIds;
  final Duration flushInterval;
  final int audioSampleRate;
  final int audioChannels;

  final _controller = StreamController<TranscriptModel>.broadcast();
  final List<TranscriptModel> _pendingSegments = [];
  final stt.SpeechToText _speechToText = stt.SpeechToText();
  final AudioRecorder _recorder = AudioRecorder();

  WebSocketChannel? _channel;
  StreamSubscription<Uint8List>? _audioStreamSubscription;
  Timer? _flushTimer;
  Timer? _reconnectTimer;
  Timer? _speechRestartTimer;
  int _counter = 0;
  int _reconnectAttempts = 0;
  bool _stopped = true;
  bool _disposed = false;
  bool _speechInitialized = false;
  bool _speechListening = false;
  bool _audioStreaming = false;
  String _lastSpeechText = '';
  String? _selectedLocaleId;

  @override
  Stream<TranscriptModel> get transcriptStream => _controller.stream;

  @override
  Future<void> start() async {
    if (_disposed) {
      return;
    }
    _stopped = false;
    _flushTimer ??= Timer.periodic(flushInterval, (_) => _flushPending());
    await _startOnDeviceSpeechRecognitionIfNeeded();
    if (_channel == null && streamUrl.trim().isNotEmpty) {
      try {
        await _connect();
      } catch (error, stackTrace) {
        _controller.addError('Transcription stream failed: $error', stackTrace);
        _scheduleReconnect();
        if (!_speechListening) {
          rethrow;
        }
      }
    }
  }

  @override
  Future<void> stop() async {
    _stopped = true;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _speechRestartTimer?.cancel();
    _speechRestartTimer = null;
    _flushTimer?.cancel();
    _flushTimer = null;
    await _stopSpeechRecognition();
    await _stopServerAudioStreaming();
    _flushPending();
    await _closeChannel();
  }

  @override
  void pushAudioChunk(
    Uint8List bytes, {
    required String speakerId,
    required String speakerName,
  }) {
    final channel = _channel;
    if (channel == null) {
      return;
    }

    final payload = {
      'speaker_id': speakerId,
      'speaker_name': speakerName,
      'audio_base64': base64Encode(bytes),
      'timestamp': DateTime.now().toUtc().toIso8601String(),
    };

    channel.sink.add(jsonEncode(payload));
  }

  @override
  void dispose() {
    _disposed = true;
    _stopped = true;
    _flushTimer?.cancel();
    _reconnectTimer?.cancel();
    _speechRestartTimer?.cancel();
    unawaited(_stopSpeechRecognition());
    unawaited(_stopServerAudioStreaming());
    unawaited(_closeChannel());
    _controller.close();
  }

  Future<void> _connect() async {
    final uri = Uri.parse('$streamUrl?token=${Uri.encodeComponent(jwtToken)}');
    final channel = WebSocketChannel.connect(uri);
    _channel = channel;
    _reconnectAttempts = 0;

    channel.stream.listen(
      _handleMessage,
      onError: (Object error, StackTrace stackTrace) {
        _controller.addError('Transcription stream failed: $error', stackTrace);
        _scheduleReconnect();
      },
      onDone: _scheduleReconnect,
      cancelOnError: false,
    );

    await _startServerAudioStreaming();
  }

  Future<void> _closeChannel() async {
    final channel = _channel;
    _channel = null;
    await _stopServerAudioStreaming();
    if (channel != null) {
      await channel.sink.close();
    }
  }

  void _scheduleReconnect() {
    if (_stopped || _disposed) {
      return;
    }
    if (_reconnectTimer?.isActive == true) {
      return;
    }
    _channel = null;
    unawaited(_stopServerAudioStreaming());
    _reconnectAttempts += 1;
    final seconds = _reconnectAttempts > 5 ? 5 : _reconnectAttempts;
    _reconnectTimer = Timer(Duration(seconds: seconds), () {
      _reconnectTimer = null;
      unawaited(_connect());
    });
  }

  Future<void> _startServerAudioStreaming() async {
    if (!enableServerAudioStreaming ||
        _audioStreaming ||
        _disposed ||
        _stopped) {
      return;
    }
    if (!streamUrl.contains('/transcription/stream')) {
      return;
    }
    final hasPermission = await _recorder.hasPermission();
    if (!hasPermission) {
      return;
    }
    try {
      final stream = await _recorder.startStream(
        RecordConfig(
          encoder: AudioEncoder.pcm16bits,
          sampleRate: audioSampleRate,
          numChannels: audioChannels,
          bitRate: audioSampleRate,
        ),
      );
      _audioStreaming = true;
      _audioStreamSubscription = stream.listen(
        (chunk) {
          if (_stopped || _disposed || chunk.isEmpty) {
            return;
          }
          _sendAudioChunk(chunk);
        },
        onError: (_) {
          unawaited(_stopServerAudioStreaming());
        },
        cancelOnError: true,
      );
    } catch (_) {
      _audioStreaming = false;
    }
  }

  Future<void> _stopServerAudioStreaming() async {
    _audioStreaming = false;
    await _audioStreamSubscription?.cancel();
    _audioStreamSubscription = null;
    try {
      if (await _recorder.isRecording()) {
        await _recorder.stop();
      }
    } catch (_) {}
  }

  void _sendAudioChunk(Uint8List chunk) {
    final channel = _channel;
    if (channel == null) {
      return;
    }
    final payload = {
      'speaker_id': speakerId,
      'speaker_name': speakerName,
      'audio_base64': base64Encode(chunk),
      'timestamp': DateTime.now().toUtc().toIso8601String(),
      'content_type':
          'audio/raw;encoding=signed-integer;bits=16;rate=$audioSampleRate;channels=$audioChannels',
      'sample_rate': audioSampleRate,
      'channels': audioChannels,
    };
    channel.sink.add(jsonEncode(payload));
  }

  Future<void> _startOnDeviceSpeechRecognitionIfNeeded() async {
    if (!enableOnDeviceSpeechRecognition) {
      return;
    }
    if (!streamUrl.contains('/transcription/stream') && streamUrl.isNotEmpty) {
      return;
    }
    if (!_speechInitialized) {
      try {
        _speechInitialized = await _speechToText.initialize(
          onStatus: _handleSpeechStatus,
          onError: (_) => _scheduleSpeechRestart(),
        );
      } catch (_) {
        _speechInitialized = false;
      }
    }
    if (!_speechInitialized || _speechListening) {
      return;
    }
    _selectedLocaleId ??= await _pickPreferredLocale();
    try {
      await _speechToText.listen(
        onResult: (result) {
          final text = result.recognizedWords.trim();
          if (text.isEmpty || text == _lastSpeechText) {
            return;
          }
          _lastSpeechText = text;
          if (result.finalResult) {
            _queueTranscriptText(
              speakerId: speakerId,
              speakerName: speakerName,
              text: text,
              source: 'on_device_speech',
              confidence: 0.82,
            );
          }
        },
        pauseFor: const Duration(seconds: 4),
        listenFor: const Duration(minutes: 30),
        localeId: _selectedLocaleId,
        listenOptions: stt.SpeechListenOptions(
          listenMode: stt.ListenMode.dictation,
          partialResults: true,
          cancelOnError: false,
        ),
      );
      _speechListening = true;
    } catch (_) {
      _speechListening = false;
    }
  }

  Future<String?> _pickPreferredLocale() async {
    try {
      final locales = await _speechToText.locales();
      if (locales.isEmpty) {
        return null;
      }
      for (final preferred in preferredLocaleIds) {
        final normalizedPreferred = preferred.toLowerCase().replaceAll(
          '-',
          '_',
        );
        for (final locale in locales) {
          final localeId = locale.localeId.toLowerCase().replaceAll('-', '_');
          if (localeId == normalizedPreferred) {
            return locale.localeId;
          }
        }
      }
    } catch (_) {}
    return null;
  }

  Future<void> _stopSpeechRecognition() async {
    _speechListening = false;
    _lastSpeechText = '';
    try {
      if (_speechToText.isListening) {
        await _speechToText.stop();
      }
    } catch (_) {}
  }

  void _handleSpeechStatus(String status) {
    final normalized = status.trim().toLowerCase();
    _speechListening = normalized == 'listening';
    if (_stopped || _disposed) {
      return;
    }
    if (normalized == 'done' || normalized == 'notlistening') {
      _scheduleSpeechRestart();
    }
  }

  void _scheduleSpeechRestart() {
    if (_stopped || _disposed || !_speechInitialized) {
      return;
    }
    if (_speechRestartTimer?.isActive == true) {
      return;
    }
    _speechRestartTimer = Timer(const Duration(milliseconds: 900), () {
      _speechRestartTimer = null;
      unawaited(_startOnDeviceSpeechRecognitionIfNeeded());
    });
  }

  void _handleMessage(dynamic message) {
    Map<String, dynamic> json;
    if (message is String) {
      final decoded = jsonDecode(message);
      if (decoded is! Map<String, dynamic>) {
        return;
      }
      json = decoded;
    } else {
      return;
    }

    final transcriptText = (json['text'] ?? '').toString().trim();
    if (transcriptText.isEmpty) {
      return;
    }

    _queueTranscriptText(
      speakerId: json['speaker_id']?.toString() ?? speakerId,
      speakerName: json['speaker_name']?.toString() ?? speakerName,
      text: transcriptText,
      timestamp: DateTime.tryParse(json['timestamp']?.toString() ?? ''),
      confidence: (json['confidence'] as num?)?.toDouble() ?? 0.9,
      source: 'speech_stream',
      id: json['id']?.toString(),
    );
  }

  void _queueTranscriptText({
    required String speakerId,
    required String speakerName,
    required String text,
    required String source,
    double confidence = 0.9,
    DateTime? timestamp,
    String? id,
  }) {
    final normalized = text.trim();
    if (normalized.isEmpty) {
      return;
    }
    _counter += 1;
    _pendingSegments.add(
      TranscriptModel(
        id: id ?? 'rt_txn_$_counter',
        speakerId: speakerId,
        speakerName: speakerName,
        message: normalized,
        timestamp: timestamp ?? DateTime.now(),
        confidence: confidence,
        source: source,
      ),
    );
  }

  void _flushPending() {
    if (_pendingSegments.isEmpty) {
      return;
    }

    // Emit at a controlled pace to avoid rebuilding UI for every partial token.
    final next = _pendingSegments.removeAt(0);
    _controller.add(next);
  }

  // END_PHASE2_IMPLEMENTATION
}
