import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';

import '../core/app_config.dart';

class ApiException implements Exception {
  ApiException({
    required this.statusCode,
    required this.message,
    this.responseBody,
  });

  final int statusCode;
  final String message;
  final String? responseBody;

  @override
  String toString() {
    return 'ApiException($statusCode): $message ${responseBody ?? ''}'.trim();
  }
}

class SecureApiClient {
  const SecureApiClient({required this.config});

  final AppConfig config;

  Future<Map<String, dynamic>> postJson(
    Uri uri,
    Map<String, dynamic> payload, {
    bool signRequest = true,
    Duration timeout = const Duration(seconds: 15),
  }) async {
    final client = HttpClient();
    final body = jsonEncode(payload);

    try {
      final request = await client.postUrl(uri).timeout(timeout);
      request.headers.set(HttpHeaders.contentTypeHeader, 'application/json');
      _applyAuthHeaders(request);
      if (signRequest) {
        _applySignatureHeaders(request, body);
      }

      request.add(utf8.encode(body));
      final response = await request.close().timeout(timeout);
      final responseBody = await response.transform(utf8.decoder).join();

      if (response.statusCode < 200 || response.statusCode >= 300) {
        throw ApiException(
          statusCode: response.statusCode,
          message: 'POST ${uri.path} failed',
          responseBody: responseBody,
        );
      }

      if (responseBody.isEmpty) {
        return const {};
      }

      final decoded = jsonDecode(responseBody);
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
      return const {};
    } on SocketException catch (error) {
      throw ApiException(
        statusCode: 0,
        message: 'Network error: ${error.message}',
      );
    } finally {
      client.close(force: true);
    }
  }

  Future<Map<String, dynamic>> getJson(
    Uri uri, {
    bool signRequest = true,
    Duration timeout = const Duration(seconds: 15),
  }) async {
    final client = HttpClient();

    try {
      final request = await client.getUrl(uri).timeout(timeout);
      _applyAuthHeaders(request);
      if (signRequest) {
        _applySignatureHeaders(request, '');
      }

      final response = await request.close().timeout(timeout);
      final responseBody = await response.transform(utf8.decoder).join();

      if (response.statusCode < 200 || response.statusCode >= 300) {
        throw ApiException(
          statusCode: response.statusCode,
          message: 'GET ${uri.path} failed',
          responseBody: responseBody,
        );
      }

      if (responseBody.isEmpty) {
        return const {};
      }

      final decoded = jsonDecode(responseBody);
      if (decoded is Map<String, dynamic>) {
        return decoded;
      }
      return const {};
    } on SocketException catch (error) {
      throw ApiException(
        statusCode: 0,
        message: 'Network error: ${error.message}',
      );
    } finally {
      client.close(force: true);
    }
  }

  Future<Stream<String>> postStreaming(
    Uri uri,
    Map<String, dynamic> payload, {
    Duration timeout = const Duration(seconds: 30),
  }) async {
    final client = HttpClient();
    final body = jsonEncode(payload);

    final request = await client.postUrl(uri).timeout(timeout);
    request.headers.set(HttpHeaders.contentTypeHeader, 'application/json');
    _applyAuthHeaders(request);
    _applySignatureHeaders(request, body);
    request.add(utf8.encode(body));

    final response = await request.close().timeout(timeout);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      final text = await response.transform(utf8.decoder).join();
      client.close(force: true);
      throw ApiException(
        statusCode: response.statusCode,
        message: 'Streaming endpoint failed',
        responseBody: text,
      );
    }

    return response.transform(utf8.decoder).handleError((Object _) {
      client.close(force: true);
    });
  }

  void _applyAuthHeaders(HttpClientRequest request) {
    if (config.jwtAccessToken.isNotEmpty) {
      request.headers.set('Authorization', 'Bearer ${config.jwtAccessToken}');
    }
    if (config.lalacoreApiKey.isNotEmpty) {
      request.headers.set('x-api-key', config.lalacoreApiKey);
    }
  }

  void _applySignatureHeaders(HttpClientRequest request, String body) {
    if (config.requestSigningSecret.isEmpty) {
      return;
    }

    final timestamp = DateTime.now().toUtc().toIso8601String();
    final payloadToSign = '$timestamp.$body';
    final digest = Hmac(
      sha256,
      utf8.encode(config.requestSigningSecret),
    ).convert(utf8.encode(payloadToSign));

    request.headers.set('x-signature-ts', timestamp);
    request.headers.set('x-signature', digest.toString());
  }
}
