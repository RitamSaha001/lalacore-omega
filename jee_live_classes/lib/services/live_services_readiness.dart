import '../core/app_config.dart';

class LiveServicesReadiness {
  const LiveServicesReadiness._();

  static void ensureReadyForRealServices(AppConfig config) {
    if (config.enableMockServices) {
      return;
    }

    final errors = <String>[];

    if (_isMissing(config.baseApiUrl) || _looksPlaceholder(config.baseApiUrl)) {
      errors.add(
        'LIVE_CLASSES_API_BASE_URL is missing or still points to a placeholder.',
      );
    }

    if (_isMissing(config.zoomSessionId)) {
      errors.add('LIVE_CLASSES_CLASS_ID/ZOOM_SESSION_ID is missing.');
    }

    final authToken = config.zoomSessionToken.trim().isNotEmpty
        ? config.zoomSessionToken
        : config.jwtAccessToken;
    if (_isMissing(authToken) && _isMissing(config.liveTokenEndpoint)) {
      errors.add(
        'Provide LIVE_CLASSES_SESSION_TOKEN/LIVE_CLASSES_JWT_ACCESS_TOKEN or configure LIVE_CLASSES_ENDPOINT_LIVE_TOKEN.',
      );
    }

    if (_isMissing(config.transcriptionWsUrl) ||
        _looksPlaceholder(config.transcriptionWsUrl) ||
        !(config.transcriptionWsUrl.startsWith('ws://') ||
            config.transcriptionWsUrl.startsWith('wss://'))) {
      errors.add(
        'LIVE_CLASSES_TRANSCRIPTION_WS_URL must be a valid ws:// or wss:// URL.',
      );
    }

    if (_isMissing(config.ocrEndpoint) ||
        _looksPlaceholder(config.ocrEndpoint) ||
        !(config.ocrEndpoint.startsWith('http://') ||
            config.ocrEndpoint.startsWith('https://'))) {
      errors.add(
        'LIVE_CLASSES_OCR_ENDPOINT must be a valid http:// or https:// URL.',
      );
    }

    if (_isMissing(config.requestSigningSecret)) {
      errors.add(
        'LIVE_CLASSES_REQUEST_SIGNING_SECRET is required in real-service mode.',
      );
    }

    if (_isMissing(config.webrtcFallbackEndpoint)) {
      errors.add('LIVE_CLASSES_ENDPOINT_WEBRTC_FALLBACK cannot be empty.');
    }

    if (_isMissing(config.classSyncEndpoint)) {
      errors.add('LIVE_CLASSES_ENDPOINT_CLASS_SYNC cannot be empty.');
    }

    if (_isMissing(config.classStateEndpoint)) {
      errors.add('LIVE_CLASSES_ENDPOINT_CLASS_STATE cannot be empty.');
    }

    if (errors.isNotEmpty) {
      throw StateError(
        'Live class real-service setup is incomplete:\n- ${errors.join('\n- ')}',
      );
    }
  }

  static bool _isMissing(String value) {
    return value.trim().isEmpty;
  }

  static bool _looksPlaceholder(String value) {
    final normalized = value.toLowerCase();
    return normalized.contains('example.com') ||
        normalized.contains('unset') ||
        normalized.contains('placeholder');
  }
}
