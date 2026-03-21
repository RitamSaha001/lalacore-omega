import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:image/image.dart' as img;

abstract class OcrCaptureService {
  Stream<String> get textStream;

  Future<void> startCapture();
  Future<void> stopCapture();
  Future<String> analyzeFrame(Uint8List frameBytes);
  void submitFrame(Uint8List frameBytes);
  void dispose();
}

class MockOcrCaptureService implements OcrCaptureService {
  // BEGIN_PHASE2_IMPLEMENTATION
  final _controller = StreamController<String>.broadcast();

  Timer? _timer;

  final List<String> _chunks = const [
    'Integral_0_to_a (x^2 + 1) dx = a^3/3 + a',
    'E = sigma / (2 epsilon_0) for infinite sheet',
    'Gauss Law: closed integral E.dA = q_enclosed / epsilon_0',
  ];

  int _index = 0;

  @override
  Stream<String> get textStream => _controller.stream;

  @override
  Future<void> startCapture() async {
    _timer?.cancel();
    _timer = Timer.periodic(const Duration(seconds: 5), (_) {
      _controller.add(_chunks[_index % _chunks.length]);
      _index += 1;
    });
  }

  @override
  Future<void> stopCapture() async {
    _timer?.cancel();
  }

  @override
  Future<String> analyzeFrame(Uint8List frameBytes) async {
    await Future<void>.delayed(const Duration(milliseconds: 120));
    return 'Detected board text: sigma = q / A';
  }

  @override
  void submitFrame(Uint8List frameBytes) {}

  @override
  void dispose() {
    _timer?.cancel();
    _controller.close();
  }
  // END_PHASE2_IMPLEMENTATION
}

class RealOcrCaptureService implements OcrCaptureService {
  // BEGIN_PHASE2_IMPLEMENTATION
  RealOcrCaptureService({
    required this.endpoint,
    required this.jwtToken,
    this.maxDimension = 1280,
    this.processInterval = const Duration(seconds: 2),
  });

  final String endpoint;
  final String jwtToken;
  final int maxDimension;
  final Duration processInterval;

  final _controller = StreamController<String>.broadcast();
  final List<Uint8List> _queue = [];

  Timer? _processTimer;
  bool _capturing = false;

  @override
  Stream<String> get textStream => _controller.stream;

  @override
  Future<void> startCapture() async {
    _capturing = true;
    _processTimer?.cancel();
    _processTimer = Timer.periodic(processInterval, (_) => _drainOneFrame());
  }

  @override
  Future<void> stopCapture() async {
    _capturing = false;
    _processTimer?.cancel();
  }

  @override
  Future<String> analyzeFrame(Uint8List frameBytes) async {
    final optimized = _downscale(frameBytes);
    final payload = {
      'image_base64': base64Encode(optimized),
      'timestamp': DateTime.now().toUtc().toIso8601String(),
      'language_hints': const ['en', 'bn'],
      'handwriting_expected': true,
      'math_priority': true,
      'return_latex': true,
      'normalize_to_english_notes': true,
    };

    final client = HttpClient();
    try {
      final request = await client.postUrl(Uri.parse(endpoint));
      request.headers.set(HttpHeaders.contentTypeHeader, 'application/json');
      if (jwtToken.isNotEmpty) {
        request.headers.set('Authorization', 'Bearer $jwtToken');
      }
      request.add(utf8.encode(jsonEncode(payload)));

      final response = await request.close();
      final body = await response.transform(utf8.decoder).join();

      if (response.statusCode < 200 || response.statusCode >= 300) {
        throw HttpException('OCR request failed: ${response.statusCode}');
      }

      final decoded = jsonDecode(body);
      if (decoded is Map<String, dynamic>) {
        final text = decoded['text']?.toString() ?? '';
        return text.trim();
      }
      return '';
    } finally {
      client.close(force: true);
    }
  }

  @override
  void submitFrame(Uint8List frameBytes) {
    if (!_capturing) {
      return;
    }
    _queue.add(frameBytes);
    if (_queue.length > 4) {
      _queue.removeAt(0);
    }
  }

  @override
  void dispose() {
    _processTimer?.cancel();
    _controller.close();
  }

  Future<void> _drainOneFrame() async {
    if (_queue.isEmpty) {
      return;
    }
    final frame = _queue.removeLast();
    try {
      final text = await analyzeFrame(frame);
      if (text.isNotEmpty) {
        _controller.add(text);
      }
    } catch (_) {
      // Keep OCR asynchronous and non-blocking for classroom flow.
    }
  }

  Uint8List _downscale(Uint8List input) {
    final decoded = img.decodeImage(input);
    if (decoded == null) {
      return input;
    }

    final longest = decoded.width > decoded.height ? decoded.width : decoded.height;
    if (longest <= maxDimension) {
      return input;
    }

    final resized = decoded.width >= decoded.height
        ? img.copyResize(decoded, width: maxDimension)
        : img.copyResize(decoded, height: maxDimension);

    final jpg = img.encodeJpg(resized, quality: 82);
    return Uint8List.fromList(jpg);
  }
  // END_PHASE2_IMPLEMENTATION
}
