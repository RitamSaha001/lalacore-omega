import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:shared_preferences/shared_preferences.dart';

import '../config/app_config.dart';

class RuntimeOverrides {
  RuntimeOverrides._();

  static const String _authBackendUrlKey = 'runtime_auth_backend_url';
  static const String _authBackendFallbackKey =
      'runtime_auth_backend_fallback_urls';
  static const String _aiEngineUrlKey = 'runtime_ai_engine_url';
  static const String _aiEngineApiKeyKey = 'runtime_ai_engine_api_key';
  static const String _aiEngineModelKey = 'runtime_ai_engine_model';
  static const int _discoveryPort = 37999;

  static bool _loaded = false;
  static String _authBackendUrl = '';
  static String _authBackendFallbackUrls = '';
  static String _aiEngineUrl = '';
  static String _aiEngineApiKey = '';
  static String _aiEngineModel = '';

  static Future<void> load() async {
    if (_loaded) {
      return;
    }
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    _authBackendUrl = (prefs.getString(_authBackendUrlKey) ?? '').trim();
    _authBackendFallbackUrls =
        (prefs.getString(_authBackendFallbackKey) ?? '').trim();
    _aiEngineUrl = (prefs.getString(_aiEngineUrlKey) ?? '').trim();
    _aiEngineApiKey = (prefs.getString(_aiEngineApiKeyKey) ?? '').trim();
    _aiEngineModel = (prefs.getString(_aiEngineModelKey) ?? '').trim();
    _loaded = true;
  }

  static Future<bool> autoDiscoverBackend({
    Duration timeout = const Duration(milliseconds: 1400),
  }) async {
    if (_authBackendUrl.isNotEmpty) {
      return false;
    }
    final String? localIp = await _guessLocalIpv4();
    final Set<String> targets = <String>{
      '255.255.255.255',
      if (localIp != null) _broadcastFor(localIp),
    }..removeWhere((String value) => value.isEmpty);

    final RawDatagramSocket socket = await RawDatagramSocket.bind(
      InternetAddress.anyIPv4,
      0,
    );
    socket.broadcastEnabled = true;

    bool discovered = false;
    final Completer<void> done = Completer<void>();

    void finish() {
      if (!done.isCompleted) {
        done.complete();
      }
    }

    socket.listen((RawSocketEvent event) async {
      if (event != RawSocketEvent.read) {
        return;
      }
      final Datagram? datagram = socket.receive();
      if (datagram == null) {
        return;
      }
      if (discovered) {
        return;
      }
      final InternetAddress server = datagram.address;
      int port = 8000;
      try {
        final String text = utf8.decode(datagram.data, allowMalformed: true);
        final dynamic decoded = jsonDecode(text);
        if (decoded is Map<String, dynamic>) {
          final dynamic rawPort = decoded['port'];
          if (rawPort is int) {
            port = rawPort;
          } else if (rawPort is String) {
            port = int.tryParse(rawPort) ?? port;
          }
        }
      } catch (_) {}

      discovered = true;
      final String authUrl = 'http://${server.address}:$port/auth/action';
      await save(
        authBackendUrl: authUrl,
        authBackendFallbackUrls: authUrl,
      );
      finish();
    });

    for (final String target in targets) {
      try {
        socket.send(
          utf8.encode('LC9_DISCOVER'),
          InternetAddress(target),
          _discoveryPort,
        );
      } catch (_) {}
    }

    await Future<void>.any(<Future<void>>[
      done.future,
      Future<void>.delayed(timeout),
    ]);

    socket.close();
    return discovered;
  }

  static Future<String?> _guessLocalIpv4() async {
    try {
      final Socket socket = await Socket.connect(
        '8.8.8.8',
        53,
        timeout: const Duration(milliseconds: 700),
      );
      final InternetAddress address = socket.address;
      await socket.close();
      if (address.type == InternetAddressType.IPv4 &&
          !address.isLoopback) {
        return address.address;
      }
    } catch (_) {}
    return null;
  }

  static String _broadcastFor(String ip) {
    final List<String> parts = ip.split('.');
    if (parts.length != 4) {
      return '';
    }
    return '${parts[0]}.${parts[1]}.${parts[2]}.255';
  }

  static Future<void> save({
    String? authBackendUrl,
    String? authBackendFallbackUrls,
    String? aiEngineUrl,
    String? aiEngineApiKey,
    String? aiEngineModel,
  }) async {
    final SharedPreferences prefs = await SharedPreferences.getInstance();
    if (authBackendUrl != null) {
      _authBackendUrl = authBackendUrl.trim();
      if (_authBackendUrl.isEmpty) {
        await prefs.remove(_authBackendUrlKey);
      } else {
        await prefs.setString(_authBackendUrlKey, _authBackendUrl);
      }
    }
    if (authBackendFallbackUrls != null) {
      _authBackendFallbackUrls = authBackendFallbackUrls.trim();
      if (_authBackendFallbackUrls.isEmpty) {
        await prefs.remove(_authBackendFallbackKey);
      } else {
        await prefs.setString(
          _authBackendFallbackKey,
          _authBackendFallbackUrls,
        );
      }
    }
    if (aiEngineUrl != null) {
      _aiEngineUrl = aiEngineUrl.trim();
      if (_aiEngineUrl.isEmpty) {
        await prefs.remove(_aiEngineUrlKey);
      } else {
        await prefs.setString(_aiEngineUrlKey, _aiEngineUrl);
      }
    }
    if (aiEngineApiKey != null) {
      _aiEngineApiKey = aiEngineApiKey.trim();
      if (_aiEngineApiKey.isEmpty) {
        await prefs.remove(_aiEngineApiKeyKey);
      } else {
        await prefs.setString(_aiEngineApiKeyKey, _aiEngineApiKey);
      }
    }
    if (aiEngineModel != null) {
      _aiEngineModel = aiEngineModel.trim();
      if (_aiEngineModel.isEmpty) {
        await prefs.remove(_aiEngineModelKey);
      } else {
        await prefs.setString(_aiEngineModelKey, _aiEngineModel);
      }
    }
  }

  static String get authBackendUrl =>
      _authBackendUrl.isNotEmpty ? _authBackendUrl : AppConfig.authBackendUrl;

  static String get authBackendFallbackUrls =>
      _authBackendFallbackUrls.isNotEmpty
          ? _authBackendFallbackUrls
          : AppConfig.authBackendFallbackUrls;

  static String get aiEngineUrl =>
      _aiEngineUrl.isNotEmpty ? _aiEngineUrl : AppConfig.aiEngineUrl;

  static String get aiEngineApiKey =>
      _aiEngineApiKey.isNotEmpty ? _aiEngineApiKey : AppConfig.aiEngineApiKey;

  static String get aiEngineModel =>
      _aiEngineModel.isNotEmpty ? _aiEngineModel : AppConfig.aiEngineModel;
}
