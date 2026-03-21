import 'dart:convert';

import 'package:http/http.dart' as http;

class AiEngineClient {
  AiEngineClient({http.Client? httpClient})
    : _httpClient = httpClient ?? http.Client();

  final http.Client _httpClient;

  static const String _engineUrl = String.fromEnvironment(
    'AI_ENGINE_URL',
    defaultValue: '',
  );
  static const String _apiKey = String.fromEnvironment(
    'AI_ENGINE_API_KEY',
    defaultValue: '',
  );
  static const String _model = String.fromEnvironment(
    'AI_ENGINE_MODEL',
    defaultValue: '',
  );

  Future<String> generate(String prompt) async {
    if (_engineUrl.isEmpty) {
      throw const AiEngineException(
        'Missing AI_ENGINE_URL. Pass it with --dart-define=AI_ENGINE_URL=https://your-engine-endpoint.',
      );
    }

    final Map<String, String> headers = <String, String>{
      'Content-Type': 'application/json',
    };

    if (_apiKey.isNotEmpty) {
      headers['Authorization'] = 'Bearer $_apiKey';
      headers['x-api-key'] = _apiKey;
    }

    final Map<String, dynamic> payload = <String, dynamic>{'prompt': prompt};

    if (_model.isNotEmpty) {
      payload['model'] = _model;
    }

    final http.Response response = await _httpClient
        .post(
          Uri.parse(_engineUrl),
          headers: headers,
          body: jsonEncode(payload),
        )
        .timeout(const Duration(seconds: 30));

    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw AiEngineException(
        'Engine returned ${response.statusCode}: ${response.body}',
      );
    }

    if (response.body.isEmpty) {
      return '';
    }

    final dynamic decoded = jsonDecode(response.body);
    if (decoded is Map<String, dynamic>) {
      final dynamic content =
          decoded['output'] ??
          decoded['answer'] ??
          decoded['text'] ??
          decoded['response'] ??
          decoded['content'];
      if (content is String) {
        return content;
      }
      return jsonEncode(decoded);
    }

    if (decoded is String) {
      return decoded;
    }

    return jsonEncode(decoded);
  }
}

class AiEngineException implements Exception {
  const AiEngineException(this.message);

  final String message;

  @override
  String toString() => message;
}
