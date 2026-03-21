import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import '../config/app_config.dart';
import 'runtime_overrides.dart';

class BackendService {
  BackendService({http.Client? httpClient})
    : _http = httpClient ?? http.Client();

  final http.Client _http;
  static const Duration _listCacheTtl = Duration(seconds: 45);
  static const Duration _chatCacheTtl = Duration(seconds: 8);
  static const Duration _authPrimaryTimeout = Duration(seconds: 8);
  static const Duration _authSyncTimeout = Duration(seconds: 4);
  static const Duration _scriptPrimaryTimeout = Duration(seconds: 12);
  static const Duration _scriptOtpTimeout = Duration(seconds: 6);
  static const Duration _authBackendCooldown = Duration(seconds: 25);
  String? _masterCsvCache;
  int _masterCsvAt = 0;
  Future<String>? _masterCsvInFlight;
  List<dynamic>? _resultsCache;
  int _resultsAt = 0;
  Future<List<dynamic>>? _resultsInFlight;
  List<dynamic>? _materialsCache;
  int _materialsAt = 0;
  Future<List<dynamic>>? _materialsInFlight;
  int _authBackendRetryAfterMs = 0;
  final Map<String, _TimedListCache> _chatDirectoryCache =
      <String, _TimedListCache>{};
  final Map<String, _TimedListCache> _doubtListCache =
      <String, _TimedListCache>{};
  final Map<String, Future<List<dynamic>>> _chatDirectoryInFlight =
      <String, Future<List<dynamic>>>{};
  final Map<String, Future<List<dynamic>>> _doubtListInFlight =
      <String, Future<List<dynamic>>>{};
  final Map<String, Uri> _preferredLocalBackendUris = <String, Uri>{};

  void _invalidateQuizCaches() {
    _masterCsvCache = null;
    _masterCsvAt = 0;
    _masterCsvInFlight = null;
  }

  void _invalidateMaterialsCache() {
    _materialsCache = null;
    _materialsAt = 0;
    _materialsInFlight = null;
  }

  void _invalidateResultsCache() {
    _resultsCache = null;
    _resultsAt = 0;
    _resultsInFlight = null;
  }

  void _invalidateChatCaches() {
    _chatDirectoryCache.clear();
    _doubtListCache.clear();
    _chatDirectoryInFlight.clear();
    _doubtListInFlight.clear();
  }

  Future<Map<String, dynamic>> authenticate({
    required bool login,
    required String email,
    required String password,
    required String name,
    String username = '',
    String deviceId = '',
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': login ? 'login_direct' : 'register_direct',
        'email': email,
        'password': password,
        'name': login ? '' : name,
        if (username.isNotEmpty) 'username': username,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': login ? 'login' : 'register',
        'email': email,
        'password': password,
        'name': login ? '' : name,
        if (username.isNotEmpty) 'username': username,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
    ];

    Map<String, dynamic>? localResponse;
    if (_shouldTryAuthBackend()) {
      try {
        localResponse = await postAuthActionWithFallback(<Map<String, dynamic>>[
          payloads.first,
        ], timeout: _authPrimaryTimeout);
        _markAuthBackendHealthy();
        if (_statusIsSuccessful(localResponse) ||
            _isDefinitiveAuthFailure(localResponse, login: login)) {
          return localResponse;
        }
      } catch (e) {
        if (_isTransientNetworkError(e)) {
          _markAuthBackendUnavailable();
        }
      }
    }

    Map<String, dynamic> script = <String, dynamic>{'ok': false};
    try {
      script = await postJsonActionWithFallback(
        payloads,
        timeout: _scriptPrimaryTimeout,
      );
    } catch (e) {
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      rethrow;
    }

    if (_statusIsSuccessful(script)) {
      final String syncedName =
          ((script['name'] ?? '').toString().trim().isNotEmpty)
          ? (script['name'] ?? '').toString().trim()
          : name;
      final String syncedStudentId = (script['student_id'] ?? '')
          .toString()
          .trim();

      if (_shouldTryAuthBackend()) {
        try {
          await postAuthAction(<String, dynamic>{
            'action': 'upsert_user',
            'email': email,
            'password': password,
            'name': syncedName,
            if (username.isNotEmpty) 'username': username,
            if (syncedStudentId.isNotEmpty) 'student_id': syncedStudentId,
            'force_update': true,
            if (deviceId.isNotEmpty) 'device_id': deviceId,
          }, timeout: _authSyncTimeout);
          _markAuthBackendHealthy();
        } catch (e) {
          if (_isTransientNetworkError(e)) {
            _markAuthBackendUnavailable();
          }
        }
      }
      return script;
    }

    if (localResponse != null && !_looksUnknownAction(localResponse)) {
      return localResponse;
    }
    return script;
  }

  Future<Map<String, dynamic>> requestEmailOtp({
    required bool login,
    required String email,
    String name = '',
    String username = '',
    String deviceId = '',
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': login ? 'request_login_otp' : 'request_register_otp',
        'email': email,
        if (!login && name.isNotEmpty) 'name': name,
        if (username.isNotEmpty) 'username': username,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'request_email_otp',
        'flow': login ? 'login' : 'register',
        'email': email,
        if (!login && name.isNotEmpty) 'name': name,
        if (username.isNotEmpty) 'username': username,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
    ];

    Map<String, dynamic>? localResponse;
    if (_shouldTryAuthBackend()) {
      try {
        final Map<String, dynamic> local = await postAuthActionWithFallback(
          payloads,
          timeout: _authPrimaryTimeout,
        );
        localResponse = local;
        _markAuthBackendHealthy();
        if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
          return local;
        }
      } catch (e) {
        if (_isTransientNetworkError(e)) {
          _markAuthBackendUnavailable();
        }
      }
    }

    try {
      final Map<String, dynamic> script = await postJsonActionWithFallback(
        payloads,
        timeout: _scriptOtpTimeout,
      );
      if (_statusIsSuccessful(script) || !_looksUnknownAction(script)) {
        return script;
      }
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      return script;
    } catch (e) {
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      rethrow;
    }
  }

  Future<Map<String, dynamic>> verifyEmailOtp({
    required bool login,
    required String email,
    required String otp,
    String name = '',
    String username = '',
    String deviceId = '',
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': login ? 'verify_login_otp' : 'verify_register_otp',
        'email': email,
        'otp': otp,
        if (!login && name.isNotEmpty) 'name': name,
        if (username.isNotEmpty) 'username': username,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'verify_email_otp',
        'flow': login ? 'login' : 'register',
        'email': email,
        'otp': otp,
        if (!login && name.isNotEmpty) 'name': name,
        if (username.isNotEmpty) 'username': username,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
    ];

    Map<String, dynamic>? localResponse;
    if (_shouldTryAuthBackend()) {
      try {
        final Map<String, dynamic> local = await postAuthActionWithFallback(
          payloads,
          timeout: _authPrimaryTimeout,
        );
        localResponse = local;
        _markAuthBackendHealthy();
        if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
          return local;
        }
      } catch (e) {
        if (_isTransientNetworkError(e)) {
          _markAuthBackendUnavailable();
        }
      }
    }

    try {
      final Map<String, dynamic> script = await postJsonActionWithFallback(
        payloads,
        timeout: _scriptOtpTimeout,
      );
      if (_statusIsSuccessful(script) || !_looksUnknownAction(script)) {
        return script;
      }
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      return script;
    } catch (e) {
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      rethrow;
    }
  }

  Future<Map<String, dynamic>> requestForgotPasswordOtp({
    required String email,
    String deviceId = '',
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'request_forgot_otp',
        'email': email,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'forgot_password_request',
        'email': email,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'request_email_otp',
        'flow': 'forgot_password',
        'purpose': 'password_reset',
        'email': email,
        'sender_email': AppConfig.forgotOtpSenderEmail,
        'from_email': AppConfig.forgotOtpSenderEmail,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
    ];

    Map<String, dynamic>? localResponse;
    if (_shouldTryAuthBackend()) {
      try {
        final Map<String, dynamic> local = await postAuthActionWithFallback(
          <Map<String, dynamic>>[payloads.first],
          timeout: _authPrimaryTimeout,
        );
        localResponse = local;
        _markAuthBackendHealthy();
        final String status = (local['status'] ?? '').toString().toUpperCase();
        final String message = (local['message'] ?? '')
            .toString()
            .toLowerCase();
        final bool shouldTryLegacyFallback =
            status == 'EMAIL_SEND_FAILED' ||
            message.contains('missing otp_sender_password') ||
            message.contains('otp email backend not configured');

        if (_statusIsSuccessful(local)) {
          return local;
        }
        if (!shouldTryLegacyFallback && !_looksUnknownAction(local)) {
          return local;
        }
      } catch (e) {
        if (_isTransientNetworkError(e)) {
          _markAuthBackendUnavailable();
        }
      }
    }

    try {
      final Map<String, dynamic> script = await postJsonActionWithFallback(
        <Map<String, dynamic>>[payloads.first, payloads.last],
        timeout: _scriptOtpTimeout,
      );
      if (_statusIsSuccessful(script) || !_looksUnknownAction(script)) {
        return script;
      }
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      return script;
    } catch (e) {
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      rethrow;
    }
  }

  Future<Map<String, dynamic>> resetPasswordWithOtp({
    required String email,
    required String otp,
    required String newPassword,
    String deviceId = '',
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'verify_forgot_otp',
        'email': email,
        'otp': otp,
        'new_password': newPassword,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'forgot_password_reset',
        'email': email,
        'otp': otp,
        'password': newPassword,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'reset_password_with_otp',
        'flow': 'forgot_password',
        'email': email,
        'otp': otp,
        'new_password': newPassword,
        'password': newPassword,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'reset_password',
        'email': email,
        'otp': otp,
        'new_password': newPassword,
        'password': newPassword,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
    ];

    Map<String, dynamic>? localResponse;
    if (_shouldTryAuthBackend()) {
      try {
        final Map<String, dynamic> local = await postAuthActionWithFallback(
          <Map<String, dynamic>>[payloads.first],
          timeout: _authPrimaryTimeout,
        );
        localResponse = local;
        _markAuthBackendHealthy();
        if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
          return local;
        }
      } catch (e) {
        if (_isTransientNetworkError(e)) {
          _markAuthBackendUnavailable();
        }
      }
    }

    try {
      final Map<String, dynamic> script = await postJsonActionWithFallback(
        <Map<String, dynamic>>[payloads.first, payloads[2]],
        timeout: _scriptOtpTimeout,
      );
      if (_statusIsSuccessful(script) || !_looksUnknownAction(script)) {
        return script;
      }
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      return script;
    } catch (e) {
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      rethrow;
    }
  }

  Future<Map<String, dynamic>> ensureUserIdentity({
    required String userId,
    required String name,
    required String role,
    String email = '',
    String username = '',
    String chatId = '',
    String deviceId = '',
    Duration timeout = const Duration(seconds: 8),
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'upsert_user_identity',
        'user_id': userId,
        'name': name,
        'role': role,
        if (email.isNotEmpty) 'email': email,
        if (username.isNotEmpty) 'username': username,
        if (chatId.isNotEmpty) 'chat_id': chatId,
        if (deviceId.isNotEmpty) 'device_id': deviceId,
      },
      <String, dynamic>{
        'action': 'chat_register',
        'name': name,
        if (email.isNotEmpty) 'email': email,
        if (username.isNotEmpty) 'username': username,
        'mobile': userId,
      },
      <String, dynamic>{
        'action': 'upsert_user',
        'user_id': userId,
        'name': name,
        'role': role,
        if (email.isNotEmpty) 'email': email,
        if (username.isNotEmpty) 'username': username,
      },
    ];

    Map<String, dynamic>? localResponse;
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: timeout,
      );
      localResponse = local;
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        return local;
      }
    } catch (_) {}

    final Map<String, dynamic> response = await postJsonActionWithFallback(
      payloads,
      timeout: timeout,
    );

    if (response['ok'] == true || _statusIsSuccessful(response)) {
      return response;
    }
    if (localResponse != null && !_looksUnknownAction(localResponse)) {
      return localResponse;
    }

    return <String, dynamic>{
      'ok': true,
      'user_id': userId,
      'name': name,
      'chat_id': chatId.isNotEmpty ? chatId : userId,
      'role': role,
      if (email.isNotEmpty) 'email': email,
      if (username.isNotEmpty) 'username': username,
    };
  }

  Future<Map<String, dynamic>> backendDiagnostics() async {
    bool scriptReachable = false;
    bool masterReachable = false;
    bool appScriptActionReachable = false;
    Map<String, dynamic> actionResponse = <String, dynamic>{};

    try {
      final http.Response script = await _http
          .get(Uri.parse(AppConfig.googleScriptUrl))
          .timeout(const Duration(seconds: 20));
      _ensureSuccess(script);
      scriptReachable = script.body.trim().isNotEmpty;
    } catch (_) {}

    try {
      final http.Response master = await _http
          .get(
            Uri.parse(
              '${AppConfig.masterSheetUrl}&ts=${DateTime.now().millisecondsSinceEpoch}',
            ),
          )
          .timeout(const Duration(seconds: 20));
      _ensureSuccess(master);
      masterReachable = master.body.trim().isNotEmpty;
    } catch (_) {}

    try {
      actionResponse = await postJsonActionWithFallback(<Map<String, dynamic>>[
        <String, dynamic>{'action': 'health_check'},
        <String, dynamic>{'action': 'ping'},
        <String, dynamic>{'action': 'noop'},
      ]);
      appScriptActionReachable = true;
    } catch (_) {}

    return <String, dynamic>{
      'ok': scriptReachable && masterReachable,
      'google_script_reachable': scriptReachable,
      'master_sheet_reachable': masterReachable,
      'app_script_action_reachable': appScriptActionReachable,
      'action_response': actionResponse,
      'google_script_url': AppConfig.googleScriptUrl,
      'master_sheet_url': AppConfig.masterSheetUrl,
    };
  }

  Future<List<dynamic>> listChatDirectory({
    required String chatId,
    String? role,
    bool forceRefresh = false,
  }) async {
    final String cacheKey = '${role ?? ''}|$chatId';
    if (!forceRefresh) {
      final _TimedListCache? cached = _chatDirectoryCache[cacheKey];
      if (cached != null &&
          DateTime.now().millisecondsSinceEpoch - cached.at <
              _chatCacheTtl.inMilliseconds) {
        return List<dynamic>.from(cached.items);
      }
      final Future<List<dynamic>>? inFlight = _chatDirectoryInFlight[cacheKey];
      if (inFlight != null) {
        return inFlight;
      }
    }

    final Future<List<dynamic>> request = () async {
      List<dynamic>? localFallback;
      try {
        final Map<String, dynamic> local = await postLocalAppActionWithFallback(
          <Map<String, dynamic>>[
            <String, dynamic>{
              'action': 'list_chat_directory',
              'chat_id': chatId,
              if (role != null && role.isNotEmpty) 'role': role,
            },
            <String, dynamic>{
              'action': 'get_chat_directory',
              'chat_id': chatId,
              if (role != null && role.isNotEmpty) 'role': role,
            },
          ],
          timeout: const Duration(seconds: 4),
        );
        final List<dynamic> localList = _extractListFromResponse(local);
        if (localList.isNotEmpty) {
          final List<dynamic> out = List<dynamic>.from(localList);
          _chatDirectoryCache[cacheKey] = _TimedListCache(
            items: out,
            at: DateTime.now().millisecondsSinceEpoch,
          );
          return out;
        }
        if (_statusIsSuccessful(local)) {
          localFallback = <dynamic>[];
        }
      } catch (_) {}
      try {
        final Uri uri = _scriptUri(<String, String>{
          'action': 'list_chat_directory',
          'chat_id': chatId,
          'ts': DateTime.now().millisecondsSinceEpoch.toString(),
          if (role != null && role.isNotEmpty) 'role': role,
        });
        final Map<String, dynamic> response = await _getJson(
          uri,
          timeout: const Duration(seconds: 5),
        );
        final dynamic list = response['list'] ?? response['data'] ?? response;
        if (list is List) {
          final List<dynamic> out = List<dynamic>.from(list);
          _chatDirectoryCache[cacheKey] = _TimedListCache(
            items: out,
            at: DateTime.now().millisecondsSinceEpoch,
          );
          return out;
        }
        if (localFallback != null) {
          _chatDirectoryCache[cacheKey] = _TimedListCache(
            items: List<dynamic>.from(localFallback!),
            at: DateTime.now().millisecondsSinceEpoch,
          );
          return List<dynamic>.from(localFallback!);
        }
      } catch (_) {
        final _TimedListCache? stale = _chatDirectoryCache[cacheKey];
        if (stale != null) {
          return List<dynamic>.from(stale.items);
        }
        if (localFallback != null) {
          return List<dynamic>.from(localFallback!);
        }
        rethrow;
      }
      return <dynamic>[];
    }();

    _chatDirectoryInFlight[cacheKey] = request;
    try {
      return await request;
    } finally {
      _chatDirectoryInFlight.remove(cacheKey);
    }
  }

  Future<Map<String, dynamic>> sendPeerMessage({
    required String chatId,
    required String participants,
    required Map<String, dynamic> payload,
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'send_message',
        'is_peer': true,
        'chat_id': chatId,
        'participants': participants,
        'payload': payload,
      },
      <String, dynamic>{
        'action': 'peer_send',
        'chat_id': chatId,
        'participants': participants,
        'payload': payload,
      },
    ];
    Map<String, dynamic>? localFailure;
    for (final Map<String, dynamic> variant in payloads) {
      try {
        final Map<String, dynamic> local = await postLocalAppAction(
          variant,
          timeout: const Duration(seconds: 6),
        );
        if (_statusIsSuccessful(local)) {
          _invalidateChatCaches();
          return local;
        }
        localFailure = local;
      } catch (_) {}
    }

    Map<String, dynamic>? scriptFailure;
    for (final Map<String, dynamic> variant in payloads) {
      try {
        final Map<String, dynamic> remote = await postJsonAction(
          variant,
          timeout: const Duration(seconds: 10),
        );
        if (_statusIsSuccessful(remote)) {
          _invalidateChatCaches();
          return remote;
        }
        scriptFailure = remote;
      } catch (_) {}
    }

    _invalidateChatCaches();
    if (scriptFailure != null && !_looksUnknownAction(scriptFailure)) {
      return scriptFailure;
    }
    if (localFailure != null && !_looksUnknownAction(localFailure)) {
      return localFailure;
    }
    if (scriptFailure != null) {
      return scriptFailure;
    }
    if (localFailure != null) {
      return localFailure;
    }
    return <String, dynamic>{
      'ok': false,
      'status': 'FAILED',
      'message': 'Unable to deliver peer message',
    };
  }

  Future<Map<String, dynamic>> markPeerChatRead({
    required String chatId,
    required String userId,
  }) {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'mark_chat_read',
        'chat_id': chatId,
        'user_id': userId,
      },
      <String, dynamic>{
        'action': 'chat_mark_seen',
        'chat_id': chatId,
        'user_id': userId,
      },
    ];
    return postLocalAppActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 4),
    ).catchError((_) {
      return postJsonActionWithFallback(payloads);
    });
  }

  Future<List<dynamic>> searchChatUsers({
    required String query,
    required String role,
    required String userId,
  }) async {
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        <Map<String, dynamic>>[
          <String, dynamic>{
            'action': 'search_chat_users',
            'q': query,
            'query': query,
            'role': role,
            'user_id': userId,
          },
          <String, dynamic>{
            'action': 'chat_search_users',
            'q': query,
            'query': query,
            'role': role,
            'user_id': userId,
          },
          <String, dynamic>{
            'action': 'list_users',
            'q': query,
            'query': query,
            'role': role,
            'user_id': userId,
          },
        ],
        timeout: const Duration(seconds: 4),
      );
      final dynamic localList =
          local['list'] ?? local['users'] ?? local['data'] ?? local;
      if (localList is List && localList.isNotEmpty) {
        return localList;
      }
    } catch (_) {}
    final List<Map<String, String>> variants = <Map<String, String>>[
      <String, String>{
        'action': 'search_chat_users',
        'q': query,
        'query': query,
        'role': role,
        'user_id': userId,
        'ts': DateTime.now().millisecondsSinceEpoch.toString(),
      },
      <String, String>{
        'action': 'chat_search_users',
        'q': query,
        'query': query,
        'role': role,
        'user_id': userId,
        'ts': DateTime.now().millisecondsSinceEpoch.toString(),
      },
      <String, String>{
        'action': 'list_users',
        'q': query,
        'query': query,
        'role': role,
        'user_id': userId,
        'ts': DateTime.now().millisecondsSinceEpoch.toString(),
      },
    ];

    for (final Map<String, String> params in variants) {
      try {
        final Map<String, dynamic> response = await _getJson(
          _scriptUri(params),
        );
        final dynamic list =
            response['list'] ??
            response['users'] ??
            response['data'] ??
            response;
        if (list is List) {
          return list;
        }
      } catch (_) {}
    }
    return <dynamic>[];
  }

  Future<Map<String, dynamic>> createGroupChat({
    required String groupId,
    required String groupName,
    required String creatorId,
    required String creatorName,
    required List<String> participants,
    List<String> admins = const <String>[],
  }) {
    final String participantCsv = participants
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .join(',');
    final String adminCsv = admins
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .join(',');
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'create_chat_group',
        'chat_id': groupId,
        'group_name': groupName,
        'creator_id': creatorId,
        'creator_name': creatorName,
        'participants': participantCsv,
        if (adminCsv.isNotEmpty) 'admins': adminCsv,
      },
      <String, dynamic>{
        'action': 'group_create',
        'chat_id': groupId,
        'group_name': groupName,
        'creator_id': creatorId,
        'creator_name': creatorName,
        'participants': participantCsv,
        if (adminCsv.isNotEmpty) 'admins': adminCsv,
      },
      <String, dynamic>{
        'action': 'send_message',
        'is_peer': true,
        'chat_id': groupId,
        'participants': participantCsv,
        'payload': <String, dynamic>{
          'id': 'group_${DateTime.now().millisecondsSinceEpoch}',
          'sender': creatorId,
          'senderName': creatorName,
          'type': 'system_group_created',
          'text': 'Group created: $groupName',
          'payload': <String, dynamic>{
            'group_name': groupName,
            'participants': participants,
            if (admins.isNotEmpty) 'admins': admins,
          },
          'time': DateTime.now().millisecondsSinceEpoch,
        },
      },
    ];
    return postLocalAppActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 6),
    ).catchError((_) {
      return postJsonActionWithFallback(payloads);
    });
  }

  Future<List<dynamic>> listDoubts({
    required String userId,
    required String role,
    bool forceRefresh = false,
  }) async {
    final String cacheKey = '$role|$userId';
    if (!forceRefresh) {
      final _TimedListCache? cached = _doubtListCache[cacheKey];
      if (cached != null &&
          DateTime.now().millisecondsSinceEpoch - cached.at <
              _chatCacheTtl.inMilliseconds) {
        return List<dynamic>.from(cached.items);
      }
      final Future<List<dynamic>>? inFlight = _doubtListInFlight[cacheKey];
      if (inFlight != null) {
        return inFlight;
      }
    }

    final Future<List<dynamic>> request = () async {
      List<dynamic>? localFallback;
      try {
        final Map<String, dynamic> local = await postLocalAppActionWithFallback(
          <Map<String, dynamic>>[
            <String, dynamic>{
              'action': 'get_doubts',
              'user_id': userId,
              'role': role,
            },
          ],
          timeout: const Duration(seconds: 4),
        );
        final List<dynamic> localList = _extractListFromResponse(local);
        if (localList.isNotEmpty) {
          _doubtListCache[cacheKey] = _TimedListCache(
            items: List<dynamic>.from(localList),
            at: DateTime.now().millisecondsSinceEpoch,
          );
          return localList;
        }
        if (_statusIsSuccessful(local)) {
          localFallback = <dynamic>[];
        }
      } catch (_) {}
      try {
        final Uri uri = _scriptUri(<String, String>{
          'action': 'get_doubts',
          'user_id': userId,
          'role': role,
          'ts': DateTime.now().millisecondsSinceEpoch.toString(),
        });
        final Map<String, dynamic> response = await _getJson(
          uri,
          timeout: const Duration(seconds: 5),
        );
        final dynamic list = response['list'] ?? response['data'] ?? response;
        if (list is List) {
          final List<dynamic> out = List<dynamic>.from(list);
          _doubtListCache[cacheKey] = _TimedListCache(
            items: out,
            at: DateTime.now().millisecondsSinceEpoch,
          );
          return out;
        }
        if (localFallback != null) {
          return List<dynamic>.from(localFallback!);
        }
      } catch (_) {
        final _TimedListCache? stale = _doubtListCache[cacheKey];
        if (stale != null) {
          return List<dynamic>.from(stale.items);
        }
        if (localFallback != null) {
          return List<dynamic>.from(localFallback!);
        }
        rethrow;
      }
      return <dynamic>[];
    }();

    _doubtListInFlight[cacheKey] = request;
    try {
      return await request;
    } finally {
      _doubtListInFlight.remove(cacheKey);
    }
  }

  Future<Map<String, dynamic>> createDoubtThread({
    required String threadId,
    required String quizId,
    required String quizTitle,
    required String questionText,
    required String raisedBy,
    required String raisedByName,
    String imageUrl = '',
    String initialMessage = '',
    Map<String, dynamic>? card,
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'raise_doubt',
        'id': threadId,
        'quiz_id': quizId,
        'quiz_title': quizTitle,
        'question': questionText,
        'student': raisedByName,
        'student_id': raisedBy,
        if (imageUrl.isNotEmpty) 'image': imageUrl,
        if (initialMessage.isNotEmpty) 'message': initialMessage,
        if (card != null) 'card': card,
      },
      <String, dynamic>{
        'action': 'send_message',
        'is_peer': false,
        'id': threadId,
        'quiz_id': quizId,
        'quiz_title': quizTitle,
        'question': questionText,
        'student': raisedByName,
        'payload': <String, dynamic>{
          'sender': raisedBy,
          'senderName': raisedByName,
          'text': initialMessage,
          'type': 'card',
          if (card != null) ...card,
          'time': DateTime.now().millisecondsSinceEpoch,
        },
      },
      <String, dynamic>{
        'action': 'doubt_reply',
        'id': threadId,
        'payload': <String, dynamic>{
          'sender': raisedBy,
          'senderName': raisedByName,
          'text': initialMessage,
          'type': 'card',
          if (card != null) ...card,
          'time': DateTime.now().millisecondsSinceEpoch,
        },
      },
    ];
    Map<String, dynamic>? localResponse;
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 6),
      );
      localResponse = local;
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        _invalidateChatCaches();
        return local;
      }
    } catch (_) {}
    final Map<String, dynamic> out = await postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 10),
    );
    if (_statusIsSuccessful(out) || !_looksUnknownAction(out)) {
      _invalidateChatCaches();
      return out;
    }
    if (localResponse != null && !_looksUnknownAction(localResponse)) {
      _invalidateChatCaches();
      return localResponse;
    }
    _invalidateChatCaches();
    return out;
  }

  Future<Map<String, dynamic>> sendDoubtMessage({
    required String threadId,
    required Map<String, dynamic> payload,
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'send_message',
        'is_peer': false,
        'id': threadId,
        'payload': payload,
      },
      <String, dynamic>{
        'action': 'doubt_reply',
        'id': threadId,
        'payload': payload,
      },
    ];
    Map<String, dynamic>? localResponse;
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 5),
      );
      localResponse = local;
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        _invalidateChatCaches();
        return local;
      }
    } catch (_) {}
    final Map<String, dynamic> out = await postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 8),
    );
    if (_statusIsSuccessful(out) || !_looksUnknownAction(out)) {
      _invalidateChatCaches();
      return out;
    }
    if (localResponse != null && !_looksUnknownAction(localResponse)) {
      _invalidateChatCaches();
      return localResponse;
    }
    _invalidateChatCaches();
    return out;
  }

  Future<Map<String, dynamic>> updateDoubtStatus({
    required String threadId,
    required String role,
  }) {
    return postJsonActionWithFallback(<Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'update_status',
        'id': threadId,
        'role': role,
      },
      <String, dynamic>{
        'action': 'doubt_update_status',
        'id': threadId,
        'role': role,
      },
    ]);
  }

  Future<String> fetchMasterCsv() async {
    if (_masterCsvCache != null &&
        DateTime.now().millisecondsSinceEpoch - _masterCsvAt <
            _listCacheTtl.inMilliseconds) {
      return _masterCsvCache!;
    }
    if (_masterCsvInFlight != null) {
      return _masterCsvInFlight!;
    }
    _masterCsvInFlight = () async {
      String? localFallbackCsv;
      try {
        try {
          final Map<String, dynamic> local = await postLocalAppAction(
            <String, dynamic>{'action': 'get_master_csv'},
            timeout: const Duration(seconds: 6),
          );
          final String localCsv = (local['csv'] ?? '').toString();
          if (localCsv.trim().isNotEmpty) {
            localFallbackCsv = localCsv;
          }
          final List<dynamic> localList = _extractListFromResponse(local);
          if (localList.isNotEmpty) {
            localFallbackCsv = _masterCsvFromList(localList);
          }
        } catch (_) {}

        if (localFallbackCsv != null &&
            _looksLikeMasterCsv(localFallbackCsv!)) {
          _masterCsvCache = localFallbackCsv!;
          _masterCsvAt = DateTime.now().millisecondsSinceEpoch;
          return localFallbackCsv!;
        }

        final String ts = DateTime.now().millisecondsSinceEpoch.toString();
        final http.Response response = await _http
            .get(Uri.parse('${AppConfig.masterSheetUrl}&ts=$ts'))
            .timeout(const Duration(seconds: 10));
        _ensureSuccess(response);
        final String body = response.body.trim();
        if (_looksLikeMasterCsv(body)) {
          _masterCsvCache = response.body;
          _masterCsvAt = DateTime.now().millisecondsSinceEpoch;
          return response.body;
        }
        if (body.startsWith('{') || body.startsWith('[')) {
          try {
            final dynamic decoded = jsonDecode(body);
            final List<dynamic> fromList = _extractListFromResponse(decoded);
            if (fromList.isNotEmpty) {
              final String converted = _masterCsvFromList(fromList);
              _masterCsvCache = converted;
              _masterCsvAt = DateTime.now().millisecondsSinceEpoch;
              return converted;
            }
          } catch (_) {}
        }
        if (localFallbackCsv != null && localFallbackCsv!.trim().isNotEmpty) {
          _masterCsvCache = localFallbackCsv;
          _masterCsvAt = DateTime.now().millisecondsSinceEpoch;
          return localFallbackCsv!;
        }
        _masterCsvCache = 'ID,Title,URL,Deadline,Type,Duration\n';
        _masterCsvAt = DateTime.now().millisecondsSinceEpoch;
        return _masterCsvCache!;
      } catch (_) {
        if (localFallbackCsv != null && localFallbackCsv!.trim().isNotEmpty) {
          _masterCsvCache = localFallbackCsv;
          _masterCsvAt = DateTime.now().millisecondsSinceEpoch;
          return localFallbackCsv!;
        }
        if (_masterCsvCache != null) {
          return _masterCsvCache!;
        }
        _masterCsvCache = 'ID,Title,URL,Deadline,Type,Duration\n';
        _masterCsvAt = DateTime.now().millisecondsSinceEpoch;
        return _masterCsvCache!;
      }
    }();
    try {
      return await _masterCsvInFlight!;
    } finally {
      _masterCsvInFlight = null;
    }
  }

  Future<List<dynamic>> fetchAllResults() async {
    if (_resultsCache != null &&
        DateTime.now().millisecondsSinceEpoch - _resultsAt <
            _listCacheTtl.inMilliseconds) {
      return List<dynamic>.from(_resultsCache!);
    }
    if (_resultsInFlight != null) {
      return _resultsInFlight!;
    }
    _resultsInFlight = () async {
      List<dynamic>? localFallback;
      try {
        try {
          final Map<String, dynamic> local =
              await postLocalAppActionWithFallback(<Map<String, dynamic>>[
                <String, dynamic>{'action': 'get_results'},
                <String, dynamic>{'action': 'list_results'},
                <String, dynamic>{'action': 'get_all_results'},
              ], timeout: const Duration(seconds: 6));
          final List<dynamic> localList = _extractListFromResponse(local);
          if (localList.isNotEmpty) {
            localFallback = List<dynamic>.from(localList);
          }
        } catch (_) {}

        final List<Map<String, String>> variants = <Map<String, String>>[
          <String, String>{
            'action': 'get_results',
            'ts': DateTime.now().millisecondsSinceEpoch.toString(),
          },
          <String, String>{
            'action': 'list_results',
            'ts': DateTime.now().millisecondsSinceEpoch.toString(),
          },
          <String, String>{
            'action': 'get_all_results',
            'ts': DateTime.now().millisecondsSinceEpoch.toString(),
          },
        ];
        for (final Map<String, String> params in variants) {
          try {
            final Map<String, dynamic> response = await _getJson(
              _scriptUri(params),
              timeout: const Duration(seconds: 6),
            );
            final List<dynamic> list = _extractListFromResponse(response);
            if (list.isNotEmpty) {
              _resultsCache = List<dynamic>.from(list);
              _resultsAt = DateTime.now().millisecondsSinceEpoch;
              return list;
            }
          } catch (_) {}
        }

        final Uri uri = Uri.parse(AppConfig.googleScriptUrl);
        final http.Response response = await _http
            .get(uri)
            .timeout(const Duration(seconds: 10));
        _ensureSuccess(response);
        if (response.body.trim().isEmpty) {
          if (localFallback != null) {
            _resultsCache = List<dynamic>.from(localFallback!);
            _resultsAt = DateTime.now().millisecondsSinceEpoch;
            return List<dynamic>.from(localFallback!);
          }
          return <dynamic>[];
        }
        final dynamic decoded = jsonDecode(response.body);
        final List<dynamic> list = _extractListFromResponse(decoded);
        if (list.isNotEmpty) {
          _resultsCache = List<dynamic>.from(list);
          _resultsAt = DateTime.now().millisecondsSinceEpoch;
          return list;
        }
        if (localFallback != null) {
          _resultsCache = List<dynamic>.from(localFallback!);
          _resultsAt = DateTime.now().millisecondsSinceEpoch;
          return List<dynamic>.from(localFallback!);
        }
        return <dynamic>[];
      } catch (_) {
        if (localFallback != null) {
          _resultsCache = List<dynamic>.from(localFallback!);
          _resultsAt = DateTime.now().millisecondsSinceEpoch;
          return List<dynamic>.from(localFallback!);
        }
        if (_resultsCache != null) {
          return List<dynamic>.from(_resultsCache!);
        }
        rethrow;
      }
    }();
    try {
      return await _resultsInFlight!;
    } finally {
      _resultsInFlight = null;
    }
  }

  Future<List<dynamic>> fetchStudyMaterials() async {
    if (_materialsCache != null &&
        DateTime.now().millisecondsSinceEpoch - _materialsAt <
            _listCacheTtl.inMilliseconds) {
      return List<dynamic>.from(_materialsCache!);
    }
    if (_materialsInFlight != null) {
      return _materialsInFlight!;
    }
    _materialsInFlight = () async {
      List<dynamic>? localFallback;
      try {
        try {
          final Map<String, dynamic> local =
              await postLocalAppActionWithFallback(<Map<String, dynamic>>[
                <String, dynamic>{'action': 'get_materials'},
                <String, dynamic>{'action': 'list_materials'},
              ], timeout: const Duration(seconds: 6));
          final List<dynamic> localList = _extractListFromResponse(local);
          if (localList.isNotEmpty) {
            localFallback = List<dynamic>.from(localList);
          }
        } catch (_) {}

        final Uri uri = _scriptUri(<String, String>{
          'action': 'get_materials',
          'ts': DateTime.now().millisecondsSinceEpoch.toString(),
        });
        final http.Response response = await _http
            .get(uri)
            .timeout(const Duration(seconds: 10));
        _ensureSuccess(response);
        final dynamic decoded = jsonDecode(response.body);
        final List<dynamic> list = _extractListFromResponse(decoded);
        if (list.isNotEmpty) {
          _materialsCache = List<dynamic>.from(list);
          _materialsAt = DateTime.now().millisecondsSinceEpoch;
          return list;
        }
        if (localFallback != null) {
          _materialsCache = List<dynamic>.from(localFallback!);
          _materialsAt = DateTime.now().millisecondsSinceEpoch;
          return List<dynamic>.from(localFallback!);
        }
        return <dynamic>[];
      } catch (_) {
        if (localFallback != null) {
          _materialsCache = List<dynamic>.from(localFallback!);
          _materialsAt = DateTime.now().millisecondsSinceEpoch;
          return List<dynamic>.from(localFallback!);
        }
        if (_materialsCache != null) {
          return List<dynamic>.from(_materialsCache!);
        }
        rethrow;
      }
    }();
    try {
      return await _materialsInFlight!;
    } finally {
      _materialsInFlight = null;
    }
  }

  Future<Map<String, dynamic>> getNotifications({
    required String userId,
    required String role,
  }) {
    final Uri uri = _scriptUri(<String, String>{
      'action': 'get_notifications',
      'userId': userId,
      'role': role,
    });
    return _getJson(uri);
  }

  Future<Map<String, dynamic>> markNotificationSeen(String notificationId) {
    final Uri uri = _scriptUri(<String, String>{
      'action': 'mark_notification_seen',
      'id': notificationId,
    });
    return _getJson(uri);
  }

  Future<Map<String, dynamic>> createQuiz(Map<String, dynamic> payload) async {
    final Map<String, dynamic> normalized = _withAssessmentAliases(payload);
    final String type = (normalized['type'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    final List<String> actions = <String>[
      'create_quiz',
      'create_assessment',
      'add_quiz',
      'save_quiz',
      if (type == 'homework') ...<String>['create_homework', 'add_homework'],
      if (type != 'homework') ...<String>['create_exam', 'add_exam'],
    ];
    final List<Map<String, dynamic>> payloads = actions
        .map(
          (String action) => <String, dynamic>{'action': action, ...normalized},
        )
        .toList();

    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 5),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        _invalidateQuizCaches();
        _invalidateResultsCache();
        return local;
      }
    } catch (_) {}

    final Map<String, dynamic> res = await postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 8),
    );
    _invalidateQuizCaches();
    _invalidateResultsCache();
    return res;
  }

  Future<Map<String, dynamic>> generateAiQuiz(
    Map<String, dynamic> payload,
  ) async {
    final Map<String, dynamic> normalized = _withAiQuizAliases(payload);
    bool preferLocalOnly = false;
    final dynamic preferLocalRaw = normalized['prefer_local_only'];
    if (preferLocalRaw is bool) {
      preferLocalOnly = preferLocalRaw;
    } else if (preferLocalRaw != null) {
      final String token = preferLocalRaw.toString().trim().toLowerCase();
      preferLocalOnly = token == '1' || token == 'true' || token == 'yes';
    }
    final List<Map<String, dynamic>> payloads =
        <String>[
              'ai_generate_quiz',
              'generate_ai_quiz',
              'ai_quiz_generate',
              'create_ai_quiz',
              'generate_quiz_ai',
              'generate_quiz',
              'quiz_ai_generate',
            ]
            .map(
              (String action) => <String, dynamic>{
                'action': action,
                ...normalized,
              },
            )
            .toList();
    // Local backend supports the primary action; avoid splitting local timeout
    // budget across multiple aliases that are only needed for script fallback.
    final List<Map<String, dynamic>> localPayloads = <Map<String, dynamic>>[
      payloads.first,
    ];
    Map<String, dynamic>? localResponse;
    Object? localError;
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        localPayloads,
        timeout: const Duration(seconds: 42),
        minAttemptMs: 3500,
        minBackendAttemptMs: 25000,
      );
      localResponse = local;
      if (_statusIsSuccessful(local) ||
          _looksLikeAiQuizResponse(local) ||
          !_looksUnknownAction(local)) {
        _invalidateQuizCaches();
        return local;
      }
      if (preferLocalOnly) {
        _invalidateQuizCaches();
        return local;
      }
    } catch (e) {
      localError = e;
    }
    if (preferLocalOnly) {
      final String localErr = (localError ?? '').toString().trim();
      return <String, dynamic>{
        'ok': false,
        'status': 'LOCAL_UNAVAILABLE',
        'message': 'Local AI quiz backend unavailable in prefer-local mode.',
        if (localErr.isNotEmpty) 'local_error': localErr,
      };
    }
    Map<String, dynamic> res = <String, dynamic>{};
    try {
      res = await postJsonActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 58),
        minAttemptMs: 4500,
      );
    } catch (e) {
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        _invalidateQuizCaches();
        return localResponse;
      }
      if (localError != null) {
        final String localErr = localError.toString().trim();
        return <String, dynamic>{
          'ok': false,
          'status': 'LOCAL_BACKEND_UNAVAILABLE',
          'message':
              'Local AI quiz backend is unavailable, and script fallback also failed.',
          if (localErr.isNotEmpty) 'local_error': localErr,
          'script_error': e.toString(),
        };
      }
      rethrow;
    }
    if (_looksUnknownAction(res) && localError != null) {
      final String localErr = localError.toString().trim();
      res = <String, dynamic>{
        ...res,
        'ok': false,
        'status': 'LOCAL_BACKEND_UNAVAILABLE',
        'message':
            'Local AI quiz backend is unavailable, and script fallback does not support this action.',
        if (localErr.isNotEmpty) 'local_error': localErr,
        'script_status': res['status'],
        'script_message': res['message'],
      };
    }
    _invalidateQuizCaches();
    return res;
  }

  Future<Map<String, dynamic>> evaluateQuizSubmission(
    Map<String, dynamic> payload,
  ) async {
    final Map<String, dynamic> normalized = Map<String, dynamic>.from(payload);
    if (normalized['answers'] is Map) {
      normalized['user_answers'] = normalized['answers'];
    } else if (normalized['user_answers'] is Map) {
      normalized['answers'] = normalized['user_answers'];
    }
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'evaluate_quiz_submission', ...normalized},
      <String, dynamic>{'action': 'submit_quiz_attempt', ...normalized},
      <String, dynamic>{'action': 'submit_quiz', ...normalized},
      <String, dynamic>{'action': 'submit_assessment', ...normalized},
      <String, dynamic>{'action': 'grade_quiz', ...normalized},
    ];
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 22),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        _invalidateResultsCache();
        return local;
      }
    } catch (_) {}
    final Map<String, dynamic> res = await postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 28),
    );
    _invalidateResultsCache();
    return res;
  }

  Future<Map<String, dynamic>> submitResult(
    Map<String, dynamic> payload,
  ) async {
    final Map<String, dynamic> normalized = _withResultAliases(payload);
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'save_result', ...normalized},
      <String, dynamic>{'action': 'submit_result', ...normalized},
      <String, dynamic>{'action': 'upsert_result', ...normalized},
      <String, dynamic>{'action': 'record_result', ...normalized},
    ];
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 12),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        _invalidateResultsCache();
        return local;
      }
    } catch (_) {}
    final Map<String, dynamic> res = await postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 16),
    );
    _invalidateResultsCache();
    return res;
  }

  Future<Map<String, dynamic>> enqueueTeacherReview(
    Map<String, dynamic> payload,
  ) async {
    final Map<String, dynamic> normalized = Map<String, dynamic>.from(payload);
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'queue_teacher_review', ...normalized},
      <String, dynamic>{'action': 'add_teacher_review', ...normalized},
      <String, dynamic>{'action': 'send_to_teacher_review', ...normalized},
      <String, dynamic>{'action': 'enqueue_teacher_review', ...normalized},
    ];
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 10),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        return local;
      }
    } catch (_) {}
    return postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 12),
    );
  }

  Future<Map<String, dynamic>> lc9ParseImportQuestions({
    required String rawText,
    Map<String, dynamic> meta = const <String, dynamic>{},
    bool aiValidation = false,
    Map<String, dynamic>? aiConfig,
  }) async {
    final Map<String, dynamic> body = <String, dynamic>{
      'raw_text': rawText,
      'meta': Map<String, dynamic>.from(meta),
      'ai_validation': aiValidation,
      if (aiConfig != null) 'ai_config': Map<String, dynamic>.from(aiConfig),
    };
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'lc9_parse_questions', ...body},
      <String, dynamic>{'action': 'lc9_parse_question_import', ...body},
      <String, dynamic>{'action': 'parse_import_questions', ...body},
    ];

    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 10),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        return local;
      }
    } catch (_) {}
    return postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 14),
    );
  }

  Future<Map<String, dynamic>> lc9SaveImportDrafts({
    required List<Map<String, dynamic>> questions,
    Map<String, dynamic> meta = const <String, dynamic>{},
  }) async {
    final Map<String, dynamic> body = <String, dynamic>{
      'questions': questions,
      'meta': Map<String, dynamic>.from(meta),
    };
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'lc9_save_import_drafts', ...body},
      <String, dynamic>{'action': 'save_import_drafts', ...body},
      <String, dynamic>{'action': 'save_question_import_drafts', ...body},
    ];
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 8),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        return local;
      }
    } catch (_) {}
    return postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 12),
    );
  }

  Future<Map<String, dynamic>> lc9PublishImportQuestions({
    required List<Map<String, dynamic>> questions,
    Map<String, dynamic> meta = const <String, dynamic>{},
    String publishGateProfile = 'legacy',
    bool fixSuggestionsApplied = false,
  }) async {
    final Map<String, dynamic> body = <String, dynamic>{
      'questions': questions,
      'meta': Map<String, dynamic>.from(meta),
      'publish_gate_profile': publishGateProfile,
      'fix_suggestions_applied': fixSuggestionsApplied,
    };
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'lc9_publish_questions', ...body},
      <String, dynamic>{'action': 'publish_import_questions', ...body},
      <String, dynamic>{'action': 'publish_question_bank_questions', ...body},
    ];
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 10),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        return local;
      }
    } catch (_) {}
    return postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 14),
    );
  }

  Future<Map<String, dynamic>> lc9WebVerifyQuery({
    required String query,
    int maxRows = 8,
  }) async {
    final Map<String, dynamic> body = <String, dynamic>{
      'query': query,
      'max_rows': maxRows,
    };
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'lc9_web_verify_query', ...body},
      <String, dynamic>{'action': 'web_verify_query', ...body},
      <String, dynamic>{'action': 'verify_import_web_query', ...body},
      <String, dynamic>{'action': 'cached_web_verify_search', ...body},
    ];
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 8),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        return local;
      }
    } catch (_) {}
    return postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 12),
    );
  }

  Future<Map<String, List<String>>> lc9ListImportChapters({
    String subject = '',
    String teacherId = '',
    int minCount = 1,
    bool includeGeneric = false,
  }) async {
    final Map<String, dynamic> body = <String, dynamic>{
      if (subject.trim().isNotEmpty) 'subject': subject.trim(),
      if (teacherId.trim().isNotEmpty) 'teacher_id': teacherId.trim(),
      'min_count': minCount < 1 ? 1 : minCount,
      'include_generic': includeGeneric,
    };
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'lc9_list_import_chapters', ...body},
      <String, dynamic>{'action': 'list_import_chapters', ...body},
      <String, dynamic>{'action': 'ai_chapter_picker_catalog', ...body},
      <String, dynamic>{'action': 'get_ai_chapter_picker_catalog', ...body},
    ];

    Map<String, dynamic> response = <String, dynamic>{'ok': false};
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 8),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        response = local;
      }
    } catch (_) {}
    if (!_statusIsSuccessful(response)) {
      try {
        response = await postJsonActionWithFallback(
          payloads,
          timeout: const Duration(seconds: 12),
        );
      } catch (_) {}
    }

    final Map<String, List<String>> out = <String, List<String>>{};
    final dynamic rawMap =
        response['subject_chapter_map'] ?? response['subject_chapters'];
    if (rawMap is Map) {
      rawMap.forEach((dynamic rawSubject, dynamic rawChapters) {
        final String subjectKey = rawSubject.toString().trim();
        if (subjectKey.isEmpty) {
          return;
        }
        final Set<String> chapters = <String>{};
        if (rawChapters is List) {
          for (final dynamic item in rawChapters) {
            if (item is Map) {
              final String chapter = (item['chapter'] ?? '').toString().trim();
              if (chapter.isNotEmpty) {
                chapters.add(chapter);
              }
            } else {
              final String chapter = item.toString().trim();
              if (chapter.isNotEmpty) {
                chapters.add(chapter);
              }
            }
          }
        } else if (rawChapters is String) {
          for (final String token in rawChapters.split(',')) {
            final String chapter = token.trim();
            if (chapter.isNotEmpty) {
              chapters.add(chapter);
            }
          }
        }
        if (chapters.isNotEmpty) {
          final List<String> list = chapters.toList()..sort();
          out[subjectKey] = list;
        }
      });
    }
    return out;
  }

  Future<Map<String, dynamic>> addMaterial(Map<String, dynamic> payload) async {
    final Map<String, dynamic> normalized = _withMaterialAliases(payload);
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{'action': 'add_material', ...normalized},
      <String, dynamic>{'action': 'create_material', ...normalized},
      <String, dynamic>{'action': 'save_material', ...normalized},
      <String, dynamic>{'action': 'upload_material', ...normalized},
      <String, dynamic>{'action': 'add_study_material', ...normalized},
      <String, dynamic>{'action': 'create_study_material', ...normalized},
    ];
    try {
      final Map<String, dynamic> local = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 6),
      );
      if (_statusIsSuccessful(local) || !_looksUnknownAction(local)) {
        _invalidateMaterialsCache();
        return local;
      }
    } catch (_) {}

    final Map<String, dynamic> res = await postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 8),
    );
    _invalidateMaterialsCache();
    return res;
  }

  Future<String> uploadFileData({
    required String fileName,
    required String dataUrl,
  }) async {
    final List<Map<String, dynamic>> payloads = <Map<String, dynamic>>[
      <String, dynamic>{
        'action': 'upload_file',
        'name': fileName,
        'file_name': fileName,
        'filename': fileName,
        'data': dataUrl,
        'file_data': dataUrl,
      },
      <String, dynamic>{
        'action': 'upload_file_data',
        'name': fileName,
        'file_name': fileName,
        'data': dataUrl,
      },
      <String, dynamic>{
        'action': 'upload_to_drive',
        'name': fileName,
        'file_name': fileName,
        'data': dataUrl,
      },
      <String, dynamic>{
        'action': 'upload_material_file',
        'name': fileName,
        'file_name': fileName,
        'data': dataUrl,
      },
    ];
    Map<String, dynamic>? localResponse;
    try {
      localResponse = await postLocalAppActionWithFallback(
        payloads,
        timeout: const Duration(seconds: 40),
      );
      final String localUrl = _extractUploadUrl(localResponse);
      if (localUrl.isNotEmpty) {
        return localUrl;
      }
      if (!_looksUnknownAction(localResponse) &&
          !_statusIsSuccessful(localResponse)) {
        throw Exception('File upload failed: ${jsonEncode(localResponse)}');
      }
    } catch (_) {}

    final Map<String, dynamic> response = await postJsonActionWithFallback(
      payloads,
      timeout: const Duration(seconds: 45),
    );
    final String url = _extractUploadUrl(response);
    if (url.isNotEmpty) {
      return url;
    }
    if (localResponse != null &&
        !_looksUnknownAction(localResponse) &&
        !_statusIsSuccessful(localResponse)) {
      throw Exception('File upload failed: ${jsonEncode(localResponse)}');
    }
    throw Exception('File upload failed: ${jsonEncode(response)}');
  }

  bool isSuccessfulResponse(Map<String, dynamic> response) {
    return _statusIsSuccessful(response);
  }

  Future<Map<String, dynamic>> postAuthAction(
    Map<String, dynamic> payload, {
    Duration timeout = const Duration(seconds: 30),
  }) async {
    return _postLocalBackendAction(
      payload,
      path: _configuredAuthActionPath(),
      preferenceKey: 'auth',
      timeout: timeout,
      keepConfiguredPath: true,
    );
  }

  Future<Map<String, dynamic>> postAuthActionWithFallback(
    List<Map<String, dynamic>> payloads, {
    Duration timeout = const Duration(seconds: 30),
  }) async {
    Object? lastError;
    Map<String, dynamic>? lastResponse;
    final Stopwatch budget = Stopwatch()..start();
    for (int i = 0; i < payloads.length; i++) {
      final Duration attemptTimeout = _attemptTimeout(
        total: timeout,
        stopwatch: budget,
        remainingAttempts: payloads.length - i,
        minMs: 900,
      );
      if (attemptTimeout == Duration.zero) {
        break;
      }
      final Map<String, dynamic> payload = payloads[i];
      try {
        final Map<String, dynamic> response = await postAuthAction(
          payload,
          timeout: attemptTimeout,
        );
        lastResponse = response;
        if (_statusIsSuccessful(response)) {
          return response;
        }
        if (!_looksUnknownAction(response)) {
          return response;
        }
      } catch (e) {
        lastError = e;
        if (_isTransientNetworkError(e)) {
          break;
        }
      }
    }
    if (lastError != null) {
      throw lastError;
    }
    return lastResponse ?? <String, dynamic>{'ok': false};
  }

  Future<Map<String, dynamic>> postLocalAppAction(
    Map<String, dynamic> payload, {
    Duration timeout = const Duration(seconds: 12),
    int minBackendAttemptMs = 700,
  }) async {
    return _postLocalBackendAction(
      payload,
      path: '/app/action',
      preferenceKey: 'app_action',
      timeout: timeout,
      minBackendAttemptMs: minBackendAttemptMs,
    );
  }

  Future<Map<String, dynamic>> postLocalSolve(
    Map<String, dynamic> payload, {
    Duration timeout = const Duration(seconds: 22),
    int minBackendAttemptMs = 700,
  }) async {
    return _postLocalBackendAction(
      payload,
      path: '/solve',
      preferenceKey: 'solve',
      timeout: timeout,
      minBackendAttemptMs: minBackendAttemptMs,
      acceptResponse: _looksLikeSolveResponse,
    );
  }

  Future<Map<String, dynamic>> postLocalAppActionWithFallback(
    List<Map<String, dynamic>> payloads, {
    Duration timeout = const Duration(seconds: 12),
    int minAttemptMs = 800,
    int minBackendAttemptMs = 700,
  }) async {
    Object? lastError;
    Map<String, dynamic>? lastResponse;
    final Stopwatch budget = Stopwatch()..start();
    for (int i = 0; i < payloads.length; i++) {
      final Duration attemptTimeout = _attemptTimeout(
        total: timeout,
        stopwatch: budget,
        remainingAttempts: payloads.length - i,
        minMs: minAttemptMs,
      );
      if (attemptTimeout == Duration.zero) {
        break;
      }
      final Map<String, dynamic> payload = payloads[i];
      try {
        final Map<String, dynamic> response = await postLocalAppAction(
          payload,
          timeout: attemptTimeout,
          minBackendAttemptMs: minBackendAttemptMs,
        );
        lastResponse = response;
        if (_statusIsSuccessful(response)) {
          return response;
        }
        if (!_looksUnknownAction(response)) {
          return response;
        }
      } catch (e) {
        lastError = e;
        if (_isTransientNetworkError(e)) {
          break;
        }
      }
    }
    if (lastError != null) {
      throw lastError;
    }
    return lastResponse ?? <String, dynamic>{'ok': false};
  }

  Future<Map<String, dynamic>> postJsonAction(
    Map<String, dynamic> payload, {
    Duration timeout = const Duration(seconds: 30),
  }) async {
    final Uri uri = Uri.parse(AppConfig.googleScriptUrl);
    final http.Request request = http.Request('POST', uri)
      ..followRedirects = false
      ..headers['Content-Type'] = 'application/json'
      ..body = jsonEncode(payload);

    final http.StreamedResponse streamed = await _http
        .send(request)
        .timeout(timeout);
    final http.Response response = await http.Response.fromStream(streamed);

    if (response.statusCode == 302 &&
        response.headers.containsKey('location')) {
      final String redirectUrl = response.headers['location']!;
      final String? cookies = response.headers['set-cookie'];
      final http.Response finalResponse = await _http
          .get(
            Uri.parse(redirectUrl),
            headers: <String, String>{if (cookies != null) 'Cookie': cookies},
          )
          .timeout(timeout);
      _ensureSuccess(finalResponse);
      return _parseJson(finalResponse.body);
    }

    _ensureSuccess(response);
    return _parseJson(response.body);
  }

  Future<Map<String, dynamic>> postJsonActionWithFallback(
    List<Map<String, dynamic>> payloads, {
    Duration timeout = const Duration(seconds: 30),
    int minAttemptMs = 1000,
  }) async {
    Object? lastError;
    Map<String, dynamic>? lastResponse;
    final Stopwatch budget = Stopwatch()..start();
    for (int i = 0; i < payloads.length; i++) {
      final Duration attemptTimeout = _attemptTimeout(
        total: timeout,
        stopwatch: budget,
        remainingAttempts: payloads.length - i,
        minMs: minAttemptMs,
      );
      if (attemptTimeout == Duration.zero) {
        break;
      }
      final Map<String, dynamic> payload = payloads[i];
      try {
        final Map<String, dynamic> response = await postJsonAction(
          payload,
          timeout: attemptTimeout,
        );
        lastResponse = response;
        if (_statusIsSuccessful(response)) {
          return response;
        }
        if (!_looksUnknownAction(response)) {
          return response;
        }
      } catch (e) {
        lastError = e;
        if (_isTransientNetworkError(e)) {
          break;
        }
      }
    }
    if (lastError != null) {
      throw lastError;
    }
    return lastResponse ?? <String, dynamic>{'ok': false};
  }

  Future<Map<String, dynamic>> postActionWithLocalAndScriptFallback(
    List<Map<String, dynamic>> payloads, {
    Duration localTimeout = const Duration(seconds: 14),
    Duration scriptTimeout = const Duration(seconds: 30),
  }) async {
    Map<String, dynamic>? localResponse;
    Object? localError;
    try {
      localResponse = await postLocalAppActionWithFallback(
        payloads,
        timeout: localTimeout,
      );
      if (_statusIsSuccessful(localResponse) ||
          !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
    } catch (e) {
      localError = e;
    }

    try {
      final Map<String, dynamic> scriptResponse =
          await postJsonActionWithFallback(payloads, timeout: scriptTimeout);
      if (_statusIsSuccessful(scriptResponse) ||
          !_looksUnknownAction(scriptResponse)) {
        return scriptResponse;
      }
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      return scriptResponse;
    } catch (e) {
      if (localResponse != null && !_looksUnknownAction(localResponse)) {
        return localResponse;
      }
      if (localError != null) {
        throw localError;
      }
      rethrow;
    }
  }

  Future<Map<String, dynamic>> _postLocalBackendAction(
    Map<String, dynamic> payload, {
    required String path,
    required String preferenceKey,
    required Duration timeout,
    int minBackendAttemptMs = 700,
    bool keepConfiguredPath = false,
    bool Function(Map<String, dynamic> response)? acceptResponse,
  }) async {
    final List<Uri> candidates = _localBackendUriCandidates(
      path: path,
      preferenceKey: preferenceKey,
      keepConfiguredPath: keepConfiguredPath,
    );
    if (candidates.isEmpty) {
      throw Exception('Local backend URL is not configured');
    }

    Object? lastError;
    final Stopwatch budget = Stopwatch()..start();
    for (int i = 0; i < candidates.length; i++) {
      final Duration attemptTimeout = _attemptTimeout(
        total: timeout,
        stopwatch: budget,
        remainingAttempts: candidates.length - i,
        minMs: minBackendAttemptMs,
      );
      if (attemptTimeout == Duration.zero) {
        break;
      }
      final Uri uri = candidates[i];
      try {
        final http.Response response = await _http
            .post(
              uri,
              headers: <String, String>{'Content-Type': 'application/json'},
              body: jsonEncode(payload),
            )
            .timeout(attemptTimeout);
        _ensureSuccess(response);
        final Map<String, dynamic> parsed = _parseJson(response.body);
        if (acceptResponse != null && !acceptResponse(parsed)) {
          lastError = Exception(
            'Rejected local backend response from ${uri.toString()}',
          );
          continue;
        }
        _preferredLocalBackendUris[preferenceKey] = uri;
        return parsed;
      } catch (e) {
        lastError = e;
        if (_isTransientNetworkError(e)) {
          continue;
        }
        rethrow;
      }
    }

    if (lastError != null) {
      throw lastError;
    }
    throw Exception('Local backend URL is not configured');
  }

  bool _looksLikeSolveResponse(Map<String, dynamic> response) {
    final String status = (response['status'] ?? '').toString().trim();
    final String statusLower = status.toLowerCase();
    if (statusLower == 'unknown_action' || statusLower == 'unknown action') {
      return false;
    }
    final Map<String, dynamic> meta = _toMap(response['meta']);
    final String provider = (response['provider'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    final String model = (response['model'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    final String answer = (response['answer'] ?? '')
        .toString()
        .trim()
        .toLowerCase();
    final bool inferStub =
        answer.contains('cannot fully solve') &&
        (meta['degraded'] == true ||
            provider == 'none' ||
            model == 'none' ||
            provider == 'unknown' ||
            model == 'unknown');
    if (inferStub) {
      return false;
    }
    return response.containsKey('final_answer') ||
        response.containsKey('reasoning') ||
        response.containsKey('verification') ||
        response.containsKey('winner_provider') ||
        response.containsKey('engine') ||
        response.containsKey('calibration_metrics') ||
        status.isNotEmpty;
  }

  Duration _attemptTimeout({
    required Duration total,
    required Stopwatch stopwatch,
    required int remainingAttempts,
    required int minMs,
  }) {
    final int remainingMs =
        total.inMilliseconds - stopwatch.elapsedMilliseconds;
    if (remainingMs <= 0) {
      return Duration.zero;
    }
    if (remainingAttempts <= 1) {
      return Duration(milliseconds: remainingMs);
    }
    int slotMs = (remainingMs / remainingAttempts).ceil();
    if (slotMs < minMs) {
      slotMs = minMs;
    }
    if (slotMs > remainingMs) {
      slotMs = remainingMs;
    }
    if (slotMs <= 0) {
      slotMs = 1;
    }
    return Duration(milliseconds: slotMs);
  }

  List<Uri> _localBackendUriCandidates({
    required String path,
    required String preferenceKey,
    bool keepConfiguredPath = false,
  }) {
    final List<Uri> configured = _configuredAuthBackendUris();
    if (configured.isEmpty) {
      return <Uri>[];
    }

    final List<Uri> out = <Uri>[];
    final Set<String> seen = <String>{};
    void addCandidate(Uri uri) {
      final String key = '${uri.scheme}|${uri.host}|${uri.port}|${uri.path}';
      if (seen.add(key)) {
        out.add(uri);
      }
    }

    final Uri? preferred = _preferredLocalBackendUris[preferenceKey];
    if (preferred != null) {
      addCandidate(preferred);
    }

    for (final Uri original in configured) {
      final Iterable<String> hosts = _expandLocalAliasHosts(original.host);
      for (final String host in hosts) {
        Uri candidate = original.replace(
          host: host,
          queryParameters: null,
          fragment: null,
        );
        if (!keepConfiguredPath) {
          candidate = candidate.replace(path: _normalizedPath(path));
        } else if (candidate.path.trim().isEmpty || candidate.path == '/') {
          candidate = candidate.replace(
            path: _normalizedPath(_configuredAuthActionPath()),
          );
        }
        addCandidate(candidate);
      }
    }
    return out;
  }

  List<Uri> _configuredAuthBackendUris() {
    final List<Uri> out = <Uri>[];
    final Set<String> seen = <String>{};

    void addRaw(String raw) {
      final String trimmed = raw.trim();
      if (trimmed.isEmpty) {
        return;
      }
      Uri uri;
      try {
        uri = Uri.parse(trimmed);
      } catch (_) {
        return;
      }
      if (!uri.hasScheme || uri.host.trim().isEmpty) {
        return;
      }
      final String key = '${uri.scheme}|${uri.host}|${uri.port}|${uri.path}';
      if (seen.add(key)) {
        out.add(uri);
      }
    }

    addRaw(RuntimeOverrides.authBackendUrl);
    for (final String raw
        in RuntimeOverrides.authBackendFallbackUrls.split(',')) {
      addRaw(raw);
    }
    return out;
  }

  List<String> _expandLocalAliasHosts(String host) {
    final String normalized = host.trim().toLowerCase();
    final List<String> out = <String>[host];
    if (normalized == '10.0.2.2' ||
        normalized == '127.0.0.1' ||
        normalized == 'localhost') {
      for (final String alias in <String>[
        '10.0.2.2',
        '127.0.0.1',
        'localhost',
      ]) {
        if (!out.contains(alias)) {
          out.add(alias);
        }
      }
    }
    return out;
  }

  String _configuredAuthActionPath() {
    final String raw = RuntimeOverrides.authBackendUrl.trim();
    if (raw.isEmpty) {
      return '/auth/action';
    }
    try {
      final Uri uri = Uri.parse(raw);
      final String path = uri.path.trim();
      if (path.isEmpty || path == '/') {
        return '/auth/action';
      }
      return _normalizedPath(path);
    } catch (_) {
      return '/auth/action';
    }
  }

  String _normalizedPath(String path) {
    final String trimmed = path.trim();
    if (trimmed.isEmpty) {
      return '/';
    }
    return trimmed.startsWith('/') ? trimmed : '/$trimmed';
  }

  Future<Map<String, dynamic>> _getJson(
    Uri uri, {
    Duration timeout = const Duration(seconds: 25),
  }) async {
    final http.Response response = await _http.get(uri).timeout(timeout);
    _ensureSuccess(response);
    return _parseJson(response.body);
  }

  Map<String, dynamic> _parseJson(String body) {
    final String trimmed = body.trim();
    if (trimmed.isEmpty) {
      return <String, dynamic>{};
    }
    if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) {
      final String lower = trimmed.toLowerCase();
      final bool looksError =
          lower.contains('unknown action') ||
          lower.contains('invalid action') ||
          lower.contains('not implemented') ||
          lower.contains('error') ||
          lower.contains('failed') ||
          lower.contains('denied');
      return <String, dynamic>{'message': trimmed, 'ok': !looksError};
    }
    final dynamic decoded = jsonDecode(trimmed);
    if (decoded is Map<String, dynamic>) {
      return decoded;
    }
    if (decoded is List) {
      return <String, dynamic>{'ok': true, 'list': decoded};
    }
    return <String, dynamic>{'ok': true, 'data': decoded};
  }

  String _extractUploadUrl(Map<String, dynamic> response) {
    final String url =
        (response['url'] ??
                response['data'] ??
                response['file_url'] ??
                response['drive_url'] ??
                response['link'] ??
                response['public_url'] ??
                response['download_url'] ??
                '')
            .toString()
            .trim();
    if (url.isNotEmpty) {
      return url;
    }
    final String message = (response['message'] ?? '').toString().trim();
    return message.startsWith('http') ? message : '';
  }

  Map<String, dynamic> _toMap(dynamic value) {
    if (value is Map<String, dynamic>) {
      return value;
    }
    if (value is Map) {
      return Map<String, dynamic>.from(value);
    }
    return <String, dynamic>{};
  }

  List<dynamic> _extractListFromResponse(dynamic response) {
    if (response is List) {
      return List<dynamic>.from(response);
    }
    if (response is! Map) {
      return <dynamic>[];
    }
    final Map<String, dynamic> map = response is Map<String, dynamic>
        ? response
        : Map<String, dynamic>.from(response);
    final dynamic list =
        map['list'] ?? map['data'] ?? map['results'] ?? map['items'];
    if (list is List) {
      return List<dynamic>.from(list);
    }
    return <dynamic>[];
  }

  String _masterCsvFromList(List<dynamic> list) {
    String esc(String value) {
      final String raw = value.replaceAll('"', '""');
      if (raw.contains(',') || raw.contains('"') || raw.contains('\n')) {
        return '"$raw"';
      }
      return raw;
    }

    final StringBuffer b = StringBuffer();
    b.writeln('ID,Title,URL,Deadline,Type,Duration');
    for (final dynamic row in list) {
      if (row is! Map) {
        continue;
      }
      final Map<String, dynamic> item = Map<String, dynamic>.from(row);
      final String id = (item['id'] ?? '').toString().trim();
      final String title = (item['title'] ?? '').toString().trim();
      final String url = (item['url'] ?? item['quiz_url'] ?? '')
          .toString()
          .trim();
      if (id.isEmpty || title.isEmpty || url.isEmpty) {
        continue;
      }
      b.writeln(
        <String>[
          esc(id),
          esc(title),
          esc(url),
          esc((item['deadline'] ?? '').toString()),
          esc((item['type'] ?? 'Exam').toString()),
          esc((item['duration'] ?? 30).toString()),
        ].join(','),
      );
    }
    return b.toString();
  }

  bool _looksLikeMasterCsv(String raw) {
    final String body = raw.trim();
    if (body.isEmpty) {
      return false;
    }
    final List<String> lines = const LineSplitter().convert(body);
    if (lines.isEmpty) {
      return false;
    }
    final String header = lines.first.toLowerCase();
    final bool hasColumns =
        header.contains('id') &&
        header.contains('title') &&
        header.contains('url');
    if (hasColumns) {
      return true;
    }
    if (lines.length >= 2 &&
        lines.first.contains(',') &&
        lines[1].contains(',')) {
      return true;
    }
    return false;
  }

  bool _statusIsSuccessful(Map<String, dynamic> response) {
    if (response.isEmpty) {
      return false;
    }
    final String message = (response['message'] ?? response['error'] ?? '')
        .toString()
        .toLowerCase();
    if (message.contains('unknown action') ||
        message.contains('invalid action') ||
        message.contains('not implemented') ||
        message.contains('failed') ||
        message.contains('error') ||
        message.contains('denied')) {
      return false;
    }

    final String status = (response['status'] ?? '').toString().toUpperCase();
    if (status.contains('UNKNOWN') ||
        status.contains('ERROR') ||
        status.contains('FAIL') ||
        status.contains('INVALID') ||
        status.contains('DENIED') ||
        status.contains('NOT_IMPLEMENTED')) {
      return false;
    }

    if (response['ok'] == true) {
      return true;
    }
    if (status.isEmpty) {
      return !response.containsKey('error');
    }
    return <String>{
      'OK',
      'SUCCESS',
      'VERIFIED',
      'OTP_SENT',
      'SENT',
      'DONE',
    }.contains(status);
  }

  bool _looksUnknownAction(Map<String, dynamic> response) {
    final String status = (response['status'] ?? '').toString().toUpperCase();
    final String message = ((response['message'] ?? response['error'] ?? ''))
        .toString();
    return status.contains('UNKNOWN') ||
        message.toLowerCase().contains('unknown action') ||
        message.toLowerCase().contains('invalid action');
  }

  bool _isDefinitiveAuthFailure(
    Map<String, dynamic> response, {
    required bool login,
  }) {
    final String status = (response['status'] ?? '').toString().toUpperCase();
    if (login) {
      return <String>{'INVALID_EMAIL', 'WEAK_PASSWORD'}.contains(status);
    }
    return <String>{
      'USER_EXISTS',
      'INVALID_EMAIL',
      'WEAK_PASSWORD',
    }.contains(status);
  }

  bool _isTransientNetworkError(Object error) {
    if (error is TimeoutException) {
      return true;
    }
    final String lower = error.toString().toLowerCase();
    return lower.contains('timed out') ||
        lower.contains('timeout') ||
        lower.contains('clientexception') ||
        lower.contains('socketexception') ||
        lower.contains('connection failed') ||
        lower.contains('connection closed') ||
        lower.contains('failed host lookup') ||
        lower.contains('name resolution') ||
        lower.contains('network is unreachable') ||
        lower.contains('network is down') ||
        lower.contains('no route to host') ||
        lower.contains('connection refused') ||
        lower.contains('connection reset');
  }

  bool _shouldTryAuthBackend() {
    return DateTime.now().millisecondsSinceEpoch >= _authBackendRetryAfterMs;
  }

  void _markAuthBackendUnavailable() {
    _authBackendRetryAfterMs =
        DateTime.now().millisecondsSinceEpoch +
        _authBackendCooldown.inMilliseconds;
  }

  void _markAuthBackendHealthy() {
    _authBackendRetryAfterMs = 0;
  }

  void _ensureSuccess(http.Response response) {
    if (response.statusCode < 200 || response.statusCode >= 400) {
      throw Exception('Backend error ${response.statusCode}: ${response.body}');
    }
  }

  Uri _scriptUri(Map<String, String> extraQuery) {
    final Uri base = Uri.parse(AppConfig.googleScriptUrl);
    final Map<String, String> merged = <String, String>{
      ...base.queryParameters,
      ...extraQuery,
    };
    return base.replace(queryParameters: merged);
  }

  Map<String, dynamic> _withAssessmentAliases(Map<String, dynamic> payload) {
    final Map<String, dynamic> out = Map<String, dynamic>.from(payload);
    final String title = (out['title'] ?? '').toString().trim();
    if (title.isNotEmpty) {
      out.putIfAbsent('quiz_title', () => title);
      out.putIfAbsent('name', () => title);
    }
    final String classValue = (out['class'] ?? '').toString().trim();
    if (classValue.isNotEmpty) {
      out.putIfAbsent('class_name', () => classValue);
      out.putIfAbsent('target_class', () => classValue);
    }
    final String chapters = (out['chapters'] ?? '').toString().trim();
    if (chapters.isNotEmpty) {
      out.putIfAbsent('chapter', () => chapters);
      out.putIfAbsent('chapter_name', () => chapters);
      out.putIfAbsent('chapter_picker', () => chapters);
    }
    if (out['duration'] != null) {
      out.putIfAbsent('duration_minutes', () => out['duration']);
      out.putIfAbsent('timer', () => out['duration']);
    }
    final dynamic questions = out['questions'];
    if (questions is List) {
      final String encoded = jsonEncode(questions);
      out.putIfAbsent('questions_json', () => encoded);
      out.putIfAbsent('questions_data', () => encoded);
    }
    out.putIfAbsent('created_at', () => DateTime.now().toIso8601String());
    return out;
  }

  Map<String, dynamic> _withAiQuizAliases(Map<String, dynamic> payload) {
    final Map<String, dynamic> out = Map<String, dynamic>.from(payload);
    final String subject = (out['subject'] ?? out['title'] ?? '')
        .toString()
        .trim();
    if (subject.isNotEmpty) {
      out.putIfAbsent('subject', () => subject);
      out.putIfAbsent('title', () => subject);
    }
    final dynamic chapters = out['chapters'];
    if (chapters is String && chapters.trim().isNotEmpty) {
      final List<String> asList = chapters
          .split(',')
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty)
          .toList();
      if (asList.isNotEmpty) {
        out['chapters'] = asList;
      }
    }
    if (out['chapters'] is List && out['chapter'] == null) {
      final List<String> list = (out['chapters'] as List<dynamic>)
          .map((dynamic e) => e.toString().trim())
          .where((String e) => e.isNotEmpty)
          .toList();
      if (list.isNotEmpty) {
        out['chapter'] = list.join(', ');
      }
    }
    final dynamic subtopics = out['subtopics'];
    if (subtopics is String && subtopics.trim().isNotEmpty) {
      final List<String> asList = subtopics
          .split(',')
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty)
          .toList();
      if (asList.isNotEmpty) {
        out['subtopics'] = asList;
      }
    }
    final String classValue = (out['class'] ?? '').toString().trim();
    if (classValue.isNotEmpty) {
      out.putIfAbsent('class_name', () => classValue);
      out.putIfAbsent('target_class', () => classValue);
    }
    if (out['question_count'] != null) {
      out.putIfAbsent('num_questions', () => out['question_count']);
      out.putIfAbsent('questions_count', () => out['question_count']);
      out.putIfAbsent('total_questions', () => out['question_count']);
    }
    if (out['duration'] != null) {
      out.putIfAbsent('duration_minutes', () => out['duration']);
      out.putIfAbsent('timer', () => out['duration']);
    }
    final String role = (out['role'] ?? out['user_role'] ?? out['request_role'])
        .toString()
        .trim()
        .toLowerCase();
    if (role.isNotEmpty) {
      out['role'] = role;
      out.putIfAbsent('user_role', () => role);
      out.putIfAbsent('request_role', () => role);
    }
    if (out['self_practice_mode'] != null) {
      out.putIfAbsent('self_practice', () => out['self_practice_mode']);
      out.putIfAbsent('practice_mode', () => out['self_practice_mode']);
    }
    if (out['authoring_mode'] != null) {
      out.putIfAbsent('teacher_authoring_mode', () => out['authoring_mode']);
    }
    if (out['include_answer_key'] != null) {
      out.putIfAbsent('answer_key', () => out['include_answer_key']);
      out.putIfAbsent('with_answers', () => out['include_answer_key']);
      out.putIfAbsent('include_solutions', () => out['include_answer_key']);
    }
    final dynamic pyqFocus = out['pyq_focus'] ?? out['prefer_pyq'];
    if (pyqFocus != null) {
      out.putIfAbsent('pyq_focus', () => pyqFocus);
      out.putIfAbsent('prefer_pyq', () => pyqFocus);
      out.putIfAbsent('use_pyq_patterns', () => pyqFocus);
      out.putIfAbsent('prefer_previous_year_questions', () => pyqFocus);
    }
    if (out['difficulty'] != null) {
      out.putIfAbsent('difficulty_level', () => out['difficulty']);
      out.putIfAbsent('hardness_level', () => out['difficulty']);
    }
    final String targetLevel = (out['target_level'] ?? '').toString().trim();
    if (targetLevel.isNotEmpty) {
      out.putIfAbsent('target_difficulty', () => targetLevel);
      out.putIfAbsent('difficulty_profile', () => targetLevel);
    }
    out.putIfAbsent('difficulty', () => 3);
    out.putIfAbsent('question_count', () => 10);
    out.putIfAbsent('trap_intensity', () => 'medium');
    out.putIfAbsent('weakness_mode', () => false);
    out.putIfAbsent('cross_concept', () => false);
    out.putIfAbsent('created_at', () => DateTime.now().toIso8601String());
    return out;
  }

  bool _looksLikeAiQuizResponse(Map<String, dynamic> response) {
    List<dynamic> asList(dynamic raw) {
      if (raw is List) {
        return raw;
      }
      if (raw is String) {
        final String text = raw.trim();
        if (text.isEmpty) {
          return const <dynamic>[];
        }
        try {
          final dynamic decoded = jsonDecode(text);
          if (decoded is List) {
            return decoded;
          }
          if (decoded is Map && decoded['questions'] is List) {
            return decoded['questions'] as List<dynamic>;
          }
        } catch (_) {}
      }
      if (raw is Map) {
        final dynamic nested =
            raw['questions'] ?? raw['questions_json'] ?? raw['items'];
        if (nested is List) {
          return nested;
        }
      }
      return const <dynamic>[];
    }

    final List<dynamic> direct = asList(
      response['questions_json'] ?? response['questions'],
    );
    if (direct.isNotEmpty) {
      return true;
    }
    final Map<String, dynamic> data = _toMap(response['data']);
    if (asList(data['questions_json'] ?? data['questions']).isNotEmpty) {
      return true;
    }
    final Map<String, dynamic> raw = _toMap(response['raw']);
    if (asList(raw['questions_json'] ?? raw['questions']).isNotEmpty) {
      return true;
    }
    return false;
  }

  Map<String, dynamic> _withResultAliases(Map<String, dynamic> payload) {
    final Map<String, dynamic> out = Map<String, dynamic>.from(payload);
    final String quizId = (out['quiz_id'] ?? out['id'] ?? '').toString().trim();
    if (quizId.isNotEmpty) {
      out.putIfAbsent('quiz_id', () => quizId);
    }
    final String quizTitle = (out['quiz_title'] ?? out['topic'] ?? '')
        .toString()
        .trim();
    if (quizTitle.isNotEmpty) {
      out.putIfAbsent('quiz_title', () => quizTitle);
      out.putIfAbsent('topic', () => quizTitle);
      out.putIfAbsent('title', () => quizTitle);
    }
    final String name = (out['name'] ?? out['student_name'] ?? '')
        .toString()
        .trim();
    if (name.isNotEmpty) {
      out.putIfAbsent('name', () => name);
      out.putIfAbsent('student_name', () => name);
      out.putIfAbsent('student', () => name);
    }
    if (out['max_score'] != null && out['total'] == null) {
      out['total'] = out['max_score'];
    }
    if (out['total'] != null && out['max_score'] == null) {
      out['max_score'] = out['total'];
    }
    out.putIfAbsent('submitted_at', () => DateTime.now().toIso8601String());
    out.putIfAbsent('ts', () => DateTime.now().millisecondsSinceEpoch);
    return out;
  }

  Map<String, dynamic> _withMaterialAliases(Map<String, dynamic> payload) {
    final Map<String, dynamic> out = Map<String, dynamic>.from(payload);
    final String classValue = (out['class'] ?? '').toString().trim();
    if (classValue.isNotEmpty) {
      out.putIfAbsent('class_name', () => classValue);
      out.putIfAbsent('target_class', () => classValue);
    }
    final String chapters = (out['chapters'] ?? '').toString().trim();
    if (chapters.isNotEmpty) {
      out.putIfAbsent('chapter', () => chapters);
      out.putIfAbsent('chapter_name', () => chapters);
    }
    final String title = (out['title'] ?? '').toString().trim();
    if (title.isNotEmpty) {
      out.putIfAbsent('material_title', () => title);
      out.putIfAbsent('name', () => title);
    }
    final String url = (out['url'] ?? '').toString().trim();
    if (url.isNotEmpty) {
      out.putIfAbsent('file_url', () => url);
      out.putIfAbsent('link', () => url);
    }
    out.putIfAbsent('created_at', () => DateTime.now().toIso8601String());
    return out;
  }
}

class _TimedListCache {
  _TimedListCache({required this.items, required this.at});

  final List<dynamic> items;
  final int at;
}
