import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:shared_preferences/shared_preferences.dart';

import '../models/chat_models.dart';
import 'backend_service.dart';

class ChatService {
  ChatService({required BackendService backendService})
    : _backend = backendService;

  final BackendService _backend;
  SharedPreferences? _prefs;
  final Map<String, Object> _memoryPrefs = <String, Object>{};
  final Map<String, int> _lastReadMarkAtByThread = <String, int>{};
  final Map<String, int> _lastDirectorySeedAtByUser = <String, int>{};
  static const int _markReadMinGapMs = 22000;
  static const int _directorySeedCooldownMs = 5 * 60 * 1000;
  static const Set<String> _controlTypes = <String>{
    'poll_vote',
    'poll_close',
    'read_receipt',
    'pin_message',
    'unpin_message',
  };

  Future<SharedPreferences?> _prefsRef() async {
    if (_prefs != null) {
      return _prefs!;
    }
    try {
      WidgetsFlutterBinding.ensureInitialized();
      _prefs = await SharedPreferences.getInstance();
      return _prefs!;
    } catch (_) {
      return null;
    }
  }

  Future<String?> _getStringPref(String key) async {
    final SharedPreferences? prefs = await _prefsRef();
    if (prefs != null) {
      return prefs.getString(key);
    }
    final Object? raw = _memoryPrefs[key];
    return raw is String ? raw : null;
  }

  Future<void> _setStringPref(String key, String value) async {
    final SharedPreferences? prefs = await _prefsRef();
    if (prefs != null) {
      await prefs.setString(key, value);
      return;
    }
    _memoryPrefs[key] = value;
  }

  Future<List<String>> _getStringListPref(String key) async {
    final SharedPreferences? prefs = await _prefsRef();
    if (prefs != null) {
      return prefs.getStringList(key) ?? <String>[];
    }
    final Object? raw = _memoryPrefs[key];
    if (raw is List<String>) {
      return raw;
    }
    if (raw is List) {
      return raw.map((dynamic e) => e.toString()).toList();
    }
    return <String>[];
  }

  Future<void> _setStringListPref(String key, List<String> value) async {
    final SharedPreferences? prefs = await _prefsRef();
    if (prefs != null) {
      await prefs.setStringList(key, value);
      return;
    }
    _memoryPrefs[key] = value;
  }

  Future<int?> _getIntPref(String key) async {
    final SharedPreferences? prefs = await _prefsRef();
    if (prefs != null) {
      return prefs.getInt(key);
    }
    final Object? raw = _memoryPrefs[key];
    if (raw is int) {
      return raw;
    }
    if (raw is num) {
      return raw.toInt();
    }
    return null;
  }

  Future<void> _setIntPref(String key, int value) async {
    final SharedPreferences? prefs = await _prefsRef();
    if (prefs != null) {
      await prefs.setInt(key, value);
      return;
    }
    _memoryPrefs[key] = value;
  }

  Future<void> _removePref(String key) async {
    final SharedPreferences? prefs = await _prefsRef();
    if (prefs != null) {
      await prefs.remove(key);
      return;
    }
    _memoryPrefs.remove(key);
  }

  Future<List<ChatThreadSummary>> fetchCachedInbox({
    required String myUserId,
    required String role,
  }) async {
    try {
      final String raw =
          await _getStringPref(_inboxCacheKey(myUserId, role)) ?? '';
      if (raw.trim().isEmpty) {
        return <ChatThreadSummary>[];
      }
      final dynamic decoded = jsonDecode(raw);
      if (decoded is! List) {
        return <ChatThreadSummary>[];
      }
      return decoded
          .whereType<Map>()
          .map(
            (Map e) => ChatThreadSummary.fromJson(Map<String, dynamic>.from(e)),
          )
          .toList();
    } catch (_) {
      return <ChatThreadSummary>[];
    }
  }

  Future<List<ChatThreadSummary>> fetchInbox({
    required String myUserId,
    required String myName,
    required String role,
    bool includeDirectorySeed = true,
  }) async {
    final Stopwatch sw = Stopwatch()..start();
    final List<ChatThreadSummary> out = <ChatThreadSummary>[];
    List<dynamic> directory = <dynamic>[];
    List<dynamic> doubts = <dynamic>[];
    bool hasTeacherDirect = false;

    final Set<String> hiddenThreads = await hiddenThreadIdsForMe(
      myUserId: myUserId,
    );

    await Future.wait(<Future<void>>[
      () async {
        try {
          directory = await _backend
              .listChatDirectory(chatId: myUserId, role: role)
              .timeout(
                const Duration(seconds: 3),
                onTimeout: () => <dynamic>[],
              );
        } catch (_) {}
      }(),
      () async {
        try {
          doubts = await _backend
              .listDoubts(userId: myUserId, role: role)
              .timeout(
                const Duration(seconds: 3),
                onTimeout: () => <dynamic>[],
              );
        } catch (_) {}
      }(),
    ]);

    for (final dynamic raw in directory) {
      final Map<String, dynamic> item = _map(raw);
      final String threadId = _str(item['chat_id']);
      if (threadId.isEmpty || hiddenThreads.contains(threadId)) {
        continue;
      }

      final List<PeerMessage> parsed = _parseMessageList(
        threadId: threadId,
        input: item['messages'],
      );
      final PeerMessage? lastParsed = parsed.isEmpty ? null : parsed.last;
      final PeerMessage? lastDisplay = _lastDisplayMessage(parsed);

      final List<String> participants = _extractParticipants(item);
      final bool isGroup =
          item['is_group'] == true ||
          participants.length > 2 ||
          _str(item['thread_type']).toLowerCase() == 'group' ||
          threadId.toLowerCase().startsWith('group_') ||
          threadId.toLowerCase().startsWith('grp_') ||
          threadId.toLowerCase().startsWith('g_');

      String peerId = _str(item['friend_id']);
      String peerName = _str(item['friend_name'], fallback: peerId);

      if (isGroup) {
        final String groupName = _str(
          item['group_name'],
          fallback: _str(item['title'], fallback: 'Group chat'),
        );
        peerId = threadId;
        peerName = groupName;
      } else if (peerId.isEmpty && participants.isNotEmpty) {
        final List<String> peers = participants
            .map((String e) => e.trim())
            .where((String e) => e.isNotEmpty && e != myUserId)
            .toList();
        if (peers.isNotEmpty) {
          peerId = peers.first;
        }
        if (peerName.isEmpty && peers.isNotEmpty) {
          peerName = peers.first;
        }
      }

      if (!isGroup && peerId.isEmpty) {
        final List<String> threadParts = threadId
            .split('|')
            .map((String e) => e.trim())
            .where((String e) => e.isNotEmpty)
            .toList();
        if (threadParts.length == 2) {
          final String mine = myUserId.trim().toLowerCase();
          final String left = threadParts.first;
          final String right = threadParts.last;
          peerId = left.toLowerCase() == mine ? right : left;
          if (peerName.isEmpty) {
            peerName = peerId;
          }
        }
      }

      if (!isGroup && peerId.isEmpty) {
        // Skip broken direct-thread rows to avoid unsendable chat targets.
        continue;
      }

      if (role == 'student' && !isGroup) {
        final String idUpper = peerId.trim().toUpperCase();
        final String nameLower = peerName.trim().toLowerCase();
        final bool teacherAlias =
            idUpper == 'TEACHER' ||
            idUpper == 'ADMIN' ||
            idUpper == 'ADMINISTRATOR' ||
            nameLower == 'admin' ||
            nameLower == 'administrator' ||
            nameLower.contains('teacher');
        if (teacherAlias) {
          if (hasTeacherDirect) {
            continue;
          }
          hasTeacherDirect = true;
          peerId = 'TEACHER';
          peerName = 'Teacher (Direct)';
        }
      }

      final int updatedAt = _int(item['time']);
      final String lastMessage = _str(
        item['last_msg'],
        fallback: _displayText(lastDisplay ?? lastParsed),
      );
      final bool unread = item['unread'] == true;

      out.add(
        ChatThreadSummary(
          threadId: threadId,
          title: isGroup ? peerName : (peerName.isEmpty ? peerId : peerName),
          peerId: peerId,
          peerName: peerName,
          lastMessage: lastMessage.isEmpty ? 'Tap to chat' : lastMessage,
          updatedAtMillis: updatedAt == 0
              ? (lastParsed?.timeMillis ?? 0)
              : updatedAt,
          unread: unread,
          isDoubtThread: false,
          isGroup: isGroup,
          participants: participants,
          rawPayload: item,
        ),
      );
    }

    for (final dynamic raw in doubts) {
      final Map<String, dynamic> item = _map(raw);
      final String id = _str(item['id']);
      if (id.isEmpty || hiddenThreads.contains('doubt_$id')) {
        continue;
      }
      final List<PeerMessage> parsed = _parseMessageList(
        threadId: 'doubt_$id',
        input: item['messages'],
      );
      final PeerMessage? last = parsed.isEmpty ? null : parsed.last;

      out.add(
        ChatThreadSummary(
          threadId: 'doubt_$id',
          title: _str(item['quiz_title'], fallback: 'Doubt Thread'),
          peerId: 'TEACHER',
          peerName: role == 'teacher'
              ? _str(item['student'], fallback: 'Student')
              : 'Teacher',
          lastMessage:
              last?.text ?? _str(item['question'], fallback: 'Open doubt'),
          updatedAtMillis: last?.timeMillis ?? _int(item['time']),
          unread: item['unread'] == true,
          isDoubtThread: true,
          rawPayload: item,
        ),
      );
    }

    if (role == 'student' &&
        !hasTeacherDirect &&
        !out.any(
          (ChatThreadSummary t) => t.peerId.toUpperCase() == 'TEACHER',
        )) {
      final String teacherThreadId = _makeThreadId(myUserId, 'TEACHER');
      out.add(
        ChatThreadSummary(
          threadId: teacherThreadId,
          title: 'Teacher (Direct)',
          peerId: 'TEACHER',
          peerName: 'Teacher (Direct)',
          lastMessage: 'Tap for help',
          updatedAtMillis: 0,
          unread: false,
          isDoubtThread: false,
          participants: <String>[myUserId, 'TEACHER'],
        ),
      );
    }

    if (includeDirectorySeed &&
        _allowDirectorySeed(myUserId: myUserId, role: role)) {
      try {
        final Set<String> existingDirectPeers = out
            .where(
              (ChatThreadSummary t) =>
                  !t.isDoubtThread && !t.isGroup && t.peerId.trim().isNotEmpty,
            )
            .map((ChatThreadSummary t) => t.peerId.trim().toUpperCase())
            .toSet();
        final List<ChatUser> knownUsers = await searchUsers(
          query: '',
          myUserId: myUserId,
          role: role,
          existingThreads: out,
        );
        for (final ChatUser user in knownUsers) {
          final String peerId = user.userId.trim();
          if (peerId.isEmpty || peerId == myUserId) {
            continue;
          }
          if (existingDirectPeers.contains(peerId.toUpperCase())) {
            continue;
          }
          final String syntheticThreadId = _makeThreadId(myUserId, peerId);
          if (hiddenThreads.contains(syntheticThreadId)) {
            continue;
          }
          out.add(
            ChatThreadSummary(
              threadId: syntheticThreadId,
              title: user.name.trim().isEmpty ? peerId : user.name.trim(),
              peerId: peerId,
              peerName: user.name.trim().isEmpty ? peerId : user.name.trim(),
              lastMessage: 'Tap to start chat',
              updatedAtMillis: 0,
              unread: false,
              isDoubtThread: false,
              participants: <String>[myUserId, peerId],
              rawPayload: <String, dynamic>{
                'autogenerated': true,
                'source': 'directory_seed',
                'role': user.role,
              },
            ),
          );
          existingDirectPeers.add(peerId.toUpperCase());
        }
      } catch (_) {}
    }

    if (role == 'teacher') {
      out.removeWhere(
        (ChatThreadSummary t) =>
            !t.isDoubtThread &&
            !t.isGroup &&
            t.peerId.trim().toUpperCase() == 'TEACHER',
      );
    }

    out.sort(
      (ChatThreadSummary a, ChatThreadSummary b) =>
          b.updatedAtMillis.compareTo(a.updatedAtMillis),
    );

    await _cacheInbox(myUserId: myUserId, role: role, threads: out);
    _logLatency(
      'fetchInbox',
      sw,
      extra:
          'threads=${out.length} directory=${directory.length} doubts=${doubts.length}',
    );
    return out;
  }

  Future<List<PeerMessage>> fetchCachedThreadMessages({
    required ChatThreadSummary thread,
    required String myUserId,
    required String role,
  }) async {
    try {
      final String raw =
          await _getStringPref(
            _threadCacheKey(myUserId, role, thread.threadId),
          ) ??
          '';
      if (raw.trim().isEmpty) {
        return <PeerMessage>[];
      }
      final dynamic decoded = jsonDecode(raw);
      if (decoded is! List) {
        return <PeerMessage>[];
      }
      List<PeerMessage> items = decoded
          .whereType<Map>()
          .map((Map e) => PeerMessage.fromJson(Map<String, dynamic>.from(e)))
          .toList();
      items = await _applyLocalVisibility(
        myUserId: myUserId,
        threadId: thread.threadId,
        items: items,
      );
      return _applyDeleteForEveryone(items);
    } catch (_) {
      return <PeerMessage>[];
    }
  }

  Future<List<PeerMessage>> fetchThreadMessages({
    required ChatThreadSummary thread,
    required String myUserId,
    required String role,
  }) async {
    final Stopwatch sw = Stopwatch()..start();
    List<PeerMessage> out = <PeerMessage>[];

    if (thread.isDoubtThread) {
      final String doubtId = thread.threadId.replaceFirst('doubt_', '');
      final List<dynamic> doubts = await _backend
          .listDoubts(userId: myUserId, role: role)
          .timeout(const Duration(seconds: 3), onTimeout: () => <dynamic>[]);
      for (final dynamic raw in doubts) {
        final Map<String, dynamic> item = _map(raw);
        if (_str(item['id']) == doubtId) {
          out = _parseMessageList(
            threadId: thread.threadId,
            input: item['messages'],
          );
          break;
        }
      }
    } else {
      final List<dynamic> directory = await _backend
          .listChatDirectory(chatId: myUserId, role: role, forceRefresh: true)
          .timeout(const Duration(seconds: 3), onTimeout: () => <dynamic>[]);
      for (final dynamic raw in directory) {
        final Map<String, dynamic> item = _map(raw);
        if (_str(item['chat_id']) == thread.threadId ||
            _matchesDirectThreadRow(
              item: item,
              thread: thread,
              myUserId: myUserId,
            ) ||
            _matchesGroupThreadRow(item: item, thread: thread)) {
          out = _parseMessageList(
            threadId: thread.threadId,
            input: item['messages'],
          );
          break;
        }
      }
    }

    out = _applyDeleteForEveryone(out);
    out = await _applyLocalVisibility(
      myUserId: myUserId,
      threadId: thread.threadId,
      items: out,
    );

    await _cacheThread(
      myUserId: myUserId,
      role: role,
      threadId: thread.threadId,
      messages: out,
    );

    if (!thread.isDoubtThread) {
      unawaited(
        _markThreadReadIfNeeded(threadId: thread.threadId, userId: myUserId),
      );
    }

    _logLatency(
      'fetchThreadMessages',
      sw,
      extra: 'thread=${thread.threadId} messages=${out.length}',
    );
    return out;
  }

  Future<void> sendMessage({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
    required String text,
  }) {
    return sendMessagePayload(
      thread: thread,
      myUserId: myUserId,
      myName: myName,
      type: 'text',
      text: text,
    );
  }

  Future<void> sendMessagePayload({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
    required String type,
    required String text,
    Map<String, dynamic>? meta,
    String? messageId,
  }) async {
    final Stopwatch sw = Stopwatch()..start();
    final Map<String, dynamic> payload = <String, dynamic>{
      'id': messageId ?? 'msg_${DateTime.now().millisecondsSinceEpoch}',
      'sender': myUserId,
      'senderName': myName,
      'text': text,
      'type': type,
      'time': DateTime.now().millisecondsSinceEpoch,
      if (meta != null) 'payload': meta,
    };

    if (thread.isDoubtThread) {
      final String doubtId = thread.threadId.replaceFirst('doubt_', '');
      await _sendDoubtWithRetry(threadId: doubtId, payload: payload);
      _logLatency(
        'sendMessagePayload',
        sw,
        extra: 'thread=${thread.threadId} type=$type',
      );
      return;
    }

    final String threadId = thread.threadId.trim().isNotEmpty
        ? thread.threadId.trim()
        : (thread.isGroup
              ? 'group_${DateTime.now().millisecondsSinceEpoch}_${_safeId(myUserId)}'
              : _makeThreadId(myUserId, thread.peerId));
    final String participants = _participantsCsv(
      thread: thread,
      myUserId: myUserId,
    );
    final List<String> participantIds = participants
        .split(',')
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .toSet()
        .toList(growable: false);
    if (!thread.isGroup && participantIds.length < 2) {
      throw Exception(
        'Direct chat target not resolved for thread "${thread.threadId}"',
      );
    }

    await _sendPeerWithRetry(
      chatId: threadId,
      participants: participants,
      payload: payload,
    );

    unawaited(_markThreadReadIfNeeded(threadId: threadId, userId: myUserId));
    _logLatency('sendMessagePayload', sw, extra: 'thread=$threadId type=$type');
  }

  bool _allowDirectorySeed({required String myUserId, required String role}) {
    final String key = '$role|${myUserId.trim()}';
    final int now = DateTime.now().millisecondsSinceEpoch;
    final int? lastAt = _lastDirectorySeedAtByUser[key];
    if (lastAt != null && (now - lastAt) < _directorySeedCooldownMs) {
      return false;
    }
    _lastDirectorySeedAtByUser[key] = now;
    return true;
  }

  Future<void> _markThreadReadIfNeeded({
    required String threadId,
    required String userId,
  }) async {
    if (threadId.trim().isEmpty || userId.trim().isEmpty) {
      return;
    }
    final String cacheKey =
        '${userId.trim().toLowerCase()}|${threadId.trim().toLowerCase()}';
    final int now = DateTime.now().millisecondsSinceEpoch;
    final int? lastAt = _lastReadMarkAtByThread[cacheKey];
    if (lastAt != null && (now - lastAt) < _markReadMinGapMs) {
      return;
    }
    _lastReadMarkAtByThread[cacheKey] = now;
    try {
      await _backend.markPeerChatRead(chatId: threadId, userId: userId);
    } catch (_) {
      _lastReadMarkAtByThread.remove(cacheKey);
    }
  }

  Future<void> sendDeleteForEveryone({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
    required String targetMessageId,
  }) {
    return sendMessagePayload(
      thread: thread,
      myUserId: myUserId,
      myName: myName,
      type: 'delete_everyone',
      text: 'Message deleted',
      meta: <String, dynamic>{'target_id': targetMessageId},
    );
  }

  Future<void> sendPinToggle({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
    required String targetMessageId,
    required bool pin,
  }) {
    return sendMessagePayload(
      thread: thread,
      myUserId: myUserId,
      myName: myName,
      type: pin ? 'pin_message' : 'unpin_message',
      text: pin ? 'Pinned a message' : 'Unpinned a message',
      meta: <String, dynamic>{'target_id': targetMessageId},
    );
  }

  Future<void> sendThreadDeleteForEveryone({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
  }) {
    return sendMessagePayload(
      thread: thread,
      myUserId: myUserId,
      myName: myName,
      type: 'thread_delete_everyone',
      text: 'Chat history cleared',
      meta: <String, dynamic>{'scope': 'thread'},
    );
  }

  Future<void> sendPollVote({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
    required String pollId,
    required int optionIndex,
  }) {
    return sendMessagePayload(
      thread: thread,
      myUserId: myUserId,
      myName: myName,
      type: 'poll_vote',
      text: 'voted',
      meta: <String, dynamic>{'poll_id': pollId, 'option_index': optionIndex},
    );
  }

  Future<void> sendPollClose({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
    required String pollId,
    String question = '',
    Map<String, dynamic>? finalResults,
  }) {
    return sendMessagePayload(
      thread: thread,
      myUserId: myUserId,
      myName: myName,
      type: 'poll_close',
      text: 'Poll finalized',
      meta: <String, dynamic>{
        'poll_id': pollId,
        if (question.trim().isNotEmpty) 'question': question.trim(),
        'closed_at': DateTime.now().millisecondsSinceEpoch,
        if (finalResults != null) ...finalResults,
      },
    );
  }

  Future<void> sendReadReceipt({
    required ChatThreadSummary thread,
    required String myUserId,
    required String myName,
    required String messageId,
    int? seenAtMillis,
  }) {
    final int seenAt = seenAtMillis ?? DateTime.now().millisecondsSinceEpoch;
    return sendMessagePayload(
      thread: thread,
      myUserId: myUserId,
      myName: myName,
      type: 'read_receipt',
      text: '',
      messageId: 'rr_${seenAt}_${_safeId(myUserId)}',
      meta: <String, dynamic>{
        'message_id': messageId,
        'reader_id': myUserId,
        'reader_name': myName,
        'seen_at': seenAt,
      },
    );
  }

  Future<String> uploadBytesAsFile({
    required Uint8List bytes,
    required String fileName,
    required String mimeType,
  }) {
    final String encoded = base64Encode(bytes);
    return _backend.uploadFileData(
      fileName: fileName,
      dataUrl: 'data:$mimeType;base64,$encoded',
    );
  }

  Future<List<ChatUser>> searchUsers({
    required String query,
    required String myUserId,
    required String role,
    List<ChatThreadSummary> existingThreads = const <ChatThreadSummary>[],
  }) async {
    final String cleanQuery = query.trim();
    final String normalizedQuery = _normalizeSearchQuery(cleanQuery);
    final String compactQuery = normalizedQuery.replaceAll(' ', '');
    final Map<String, ChatUser> candidates = <String, ChatUser>{};

    void addCandidate({
      required String id,
      required String name,
      required String roleValue,
      Map<String, dynamic>? rawPayload,
    }) {
      String userId = id.trim();
      String displayName = name.trim();
      String userRole = roleValue.trim().isEmpty ? 'student' : roleValue.trim();
      final String upper = userId.toUpperCase();
      final String lowerName = displayName.toLowerCase();
      if (upper == 'ADMIN' ||
          upper == 'ADMINISTRATOR' ||
          upper == 'TEACHER' ||
          lowerName == 'admin' ||
          lowerName == 'administrator') {
        userId = 'TEACHER';
        displayName = 'Teacher (Direct)';
        userRole = 'teacher';
      }
      if (userId.isEmpty || userId == myUserId) {
        return;
      }
      if (role == 'teacher' && userRole.toLowerCase() == 'teacher') {
        return;
      }
      final ChatUser existing =
          candidates[userId] ??
          ChatUser(
            userId: userId,
            name: displayName.isEmpty ? userId : displayName,
            role: userRole,
            rawPayload: rawPayload,
          );
      final String resolvedName = displayName.isNotEmpty
          ? displayName
          : existing.name;
      final String resolvedRole = userRole.isNotEmpty
          ? userRole
          : existing.role;
      candidates[userId] = ChatUser(
        userId: userId,
        name: resolvedName,
        role: resolvedRole,
        rawPayload: rawPayload ?? existing.rawPayload,
      );
    }

    try {
      final List<dynamic> raw = await _backend.searchChatUsers(
        query: cleanQuery,
        role: role,
        userId: myUserId,
      );
      for (final dynamic item in raw) {
        final Map<String, dynamic> m = _map(item);
        final String id = _str(
          m['user_id'],
          fallback: _str(
            m['chat_id'],
            fallback: _str(m['id'], fallback: _str(m['mobile'])),
          ),
        );
        final String name = _str(
          m['name'],
          fallback: _str(
            m['friend_name'],
            fallback: _str(m['full_name'], fallback: id),
          ),
        );
        addCandidate(
          id: id,
          name: name,
          roleValue: _str(m['role'], fallback: 'student'),
          rawPayload: m,
        );
      }
    } catch (_) {}

    for (final ChatThreadSummary thread in existingThreads) {
      if (thread.isDoubtThread ||
          thread.peerId.isEmpty ||
          thread.peerId == myUserId) {
        continue;
      }
      addCandidate(
        id: thread.peerId,
        name: thread.peerName,
        roleValue: thread.peerId.toUpperCase() == 'TEACHER'
            ? 'teacher'
            : 'student',
        rawPayload: thread.rawPayload,
      );
    }

    try {
      final List<dynamic> directory = await _backend
          .listChatDirectory(chatId: myUserId, role: role)
          .timeout(const Duration(seconds: 2), onTimeout: () => <dynamic>[]);
      for (final dynamic item in directory) {
        final Map<String, dynamic> m = _map(item);
        final String friendId = _str(m['friend_id']);
        final String friendName = _str(m['friend_name'], fallback: friendId);
        if (friendId.isNotEmpty) {
          addCandidate(
            id: friendId,
            name: friendName,
            roleValue: friendId.toUpperCase() == 'TEACHER'
                ? 'teacher'
                : 'student',
            rawPayload: m,
          );
        }
        final List<String> participants = _extractParticipants(m);
        for (final String participant in participants) {
          if (participant == myUserId) {
            continue;
          }
          addCandidate(
            id: participant,
            name: participant,
            roleValue: participant.toUpperCase() == 'TEACHER'
                ? 'teacher'
                : 'student',
            rawPayload: m,
          );
        }
      }
    } catch (_) {}

    try {
      final List<dynamic> allResults = await _backend.fetchAllResults().timeout(
        const Duration(seconds: 2),
        onTimeout: () => <dynamic>[],
      );
      for (final dynamic row in allResults) {
        final Map<String, dynamic> m = _map(row);
        final String id = _str(
          m['student_id'],
          fallback: _str(
            m['account_id'],
            fallback: _str(m['user_id'], fallback: _str(m['chat_id'])),
          ),
        );
        final String name = _str(
          m['student_name'],
          fallback: _str(m['name'], fallback: id),
        );
        if (id.isEmpty) {
          continue;
        }
        addCandidate(id: id, name: name, roleValue: 'student', rawPayload: m);
      }
    } catch (_) {}

    if (role != 'teacher') {
      addCandidate(
        id: 'TEACHER',
        name: 'Teacher (Direct)',
        roleValue: 'teacher',
      );
    }

    final List<ChatUser> all = candidates.values.toList();
    if (normalizedQuery.isEmpty) {
      all.sort((ChatUser a, ChatUser b) {
        if (a.userId == 'TEACHER' && b.userId != 'TEACHER') {
          return -1;
        }
        if (b.userId == 'TEACHER' && a.userId != 'TEACHER') {
          return 1;
        }
        return a.name.toLowerCase().compareTo(b.name.toLowerCase());
      });
      return all;
    }

    final List<_ScoredChatUser> ranked = <_ScoredChatUser>[];
    for (final ChatUser user in all) {
      final int score = _searchScoreForUser(
        user: user,
        normalizedQuery: normalizedQuery,
        compactQuery: compactQuery,
      );
      if (score <= 0) {
        continue;
      }
      ranked.add(_ScoredChatUser(user: user, score: score));
    }
    ranked.sort((_ScoredChatUser a, _ScoredChatUser b) {
      final int byScore = b.score.compareTo(a.score);
      if (byScore != 0) {
        return byScore;
      }
      return a.user.name.toLowerCase().compareTo(b.user.name.toLowerCase());
    });
    return ranked.map((_ScoredChatUser e) => e.user).toList();
  }

  Future<ChatThreadSummary> createGroupThread({
    required String myUserId,
    required String myName,
    required String role,
    required String groupName,
    required List<ChatUser> members,
  }) async {
    final Set<String> ids = <String>{myUserId};
    for (final ChatUser user in members) {
      if (user.userId.trim().isNotEmpty) {
        ids.add(user.userId.trim());
      }
    }

    final String threadId =
        'group_${DateTime.now().millisecondsSinceEpoch}_${_safeId(myUserId)}';
    final List<String> participants = ids.toList()..sort();
    final List<String> admins = <String>[myUserId];

    await _backend.createGroupChat(
      groupId: threadId,
      groupName: groupName,
      creatorId: myUserId,
      creatorName: myName,
      participants: participants,
      admins: admins,
    );

    final ChatThreadSummary summary = ChatThreadSummary(
      threadId: threadId,
      title: groupName,
      peerId: threadId,
      peerName: groupName,
      lastMessage: 'Group created',
      updatedAtMillis: DateTime.now().millisecondsSinceEpoch,
      unread: false,
      isDoubtThread: false,
      isGroup: true,
      participants: participants,
      rawPayload: <String, dynamic>{
        'is_group': true,
        'group_name': groupName,
        'participants': participants,
        'admins': admins,
        'creator_id': myUserId,
      },
    );

    final List<ChatThreadSummary> existing = await fetchCachedInbox(
      myUserId: myUserId,
      role: role,
    );
    final List<ChatThreadSummary> merged = <ChatThreadSummary>[
      summary,
      ...existing.where((ChatThreadSummary t) => t.threadId != threadId),
    ];
    await _cacheInbox(myUserId: myUserId, role: role, threads: merged);

    return summary;
  }

  Future<void> hideMessageForMe({
    required String myUserId,
    required String threadId,
    required String messageId,
  }) async {
    final Set<String> ids = (await _getStringListPref(
      _hiddenMessagesKey(myUserId, threadId),
    )).toSet();
    ids.add(messageId);
    await _setStringListPref(
      _hiddenMessagesKey(myUserId, threadId),
      ids.toList(),
    );
  }

  Future<Set<String>> hiddenMessageIdsForMe({
    required String myUserId,
    required String threadId,
  }) async {
    return (await _getStringListPref(
      _hiddenMessagesKey(myUserId, threadId),
    )).toSet();
  }

  Future<void> clearChatForMe({
    required String myUserId,
    required String threadId,
  }) async {
    await _setIntPref(
      _threadClearedAtKey(myUserId, threadId),
      DateTime.now().millisecondsSinceEpoch,
    );
    await _removePref(_threadCacheKey(myUserId, 'student', threadId));
    await _removePref(_threadCacheKey(myUserId, 'teacher', threadId));
  }

  Future<int> threadClearedAtForMe({
    required String myUserId,
    required String threadId,
  }) async {
    return await _getIntPref(_threadClearedAtKey(myUserId, threadId)) ?? 0;
  }

  Future<void> hideThreadForMe({
    required String myUserId,
    required String threadId,
  }) async {
    final Set<String> hidden = (await _getStringListPref(
      _hiddenThreadsKey(myUserId),
    )).toSet();
    hidden.add(threadId);
    await _setStringListPref(_hiddenThreadsKey(myUserId), hidden.toList());
  }

  Future<Set<String>> hiddenThreadIdsForMe({required String myUserId}) async {
    return (await _getStringListPref(_hiddenThreadsKey(myUserId))).toSet();
  }

  Future<void> setPinnedMessageForThread({
    required String myUserId,
    required String threadId,
    String? messageId,
  }) async {
    final String key = _threadPinnedKey(myUserId, threadId);
    if (messageId == null || messageId.trim().isEmpty) {
      await _removePref(key);
      return;
    }
    await _setStringPref(key, messageId);
  }

  Future<String?> pinnedMessageForThread({
    required String myUserId,
    required String threadId,
  }) async {
    final String? value = await _getStringPref(
      _threadPinnedKey(myUserId, threadId),
    );
    if (value == null || value.trim().isEmpty) {
      return null;
    }
    return value;
  }

  Future<ChatThreadSummary> raiseDoubtFromQuestion({
    required String quizId,
    required String quizTitle,
    required String questionText,
    required String myUserId,
    required String myName,
    required String initialMessage,
    String imageUrl = '',
    Map<String, dynamic>? card,
  }) async {
    final String threadId =
        'd_${quizId}_${DateTime.now().millisecondsSinceEpoch}';
    await _backend.createDoubtThread(
      threadId: threadId,
      quizId: quizId,
      quizTitle: quizTitle,
      questionText: questionText,
      raisedBy: myUserId,
      raisedByName: myName,
      imageUrl: imageUrl,
      initialMessage: initialMessage,
      card: card,
    );

    if (card != null && card.isNotEmpty) {
      try {
        await _sendDoubtWithRetry(
          threadId: threadId,
          payload: <String, dynamic>{
            'id': 'ak_${DateTime.now().millisecondsSinceEpoch}',
            'sender': myUserId,
            'senderName': myName,
            'text': 'Answer key card shared',
            'type': 'answer_key_card',
            'payload': card,
            'time': DateTime.now().millisecondsSinceEpoch,
          },
        );
      } catch (_) {}
    }

    return ChatThreadSummary(
      threadId: 'doubt_$threadId',
      title: quizTitle,
      peerId: 'TEACHER',
      peerName: 'Teacher',
      lastMessage: initialMessage,
      updatedAtMillis: DateTime.now().millisecondsSinceEpoch,
      unread: false,
      isDoubtThread: true,
      rawPayload: <String, dynamic>{
        'id': threadId,
        'quiz_title': quizTitle,
        'question': questionText,
      },
    );
  }

  List<PeerMessage> _parseMessageList({
    required String threadId,
    required dynamic input,
  }) {
    final List<dynamic> raw = _decodeList(input);
    final List<PeerMessage> out = <PeerMessage>[];

    for (int i = 0; i < raw.length; i++) {
      final Map<String, dynamic> m = _map(raw[i]);
      final int time = _int(m['time']);
      final dynamic payloadRaw = m['payload'] ?? m['meta'];
      out.add(
        PeerMessage(
          id: _str(m['id'], fallback: '${threadId}_$i'),
          threadId: threadId,
          senderId: _str(m['sender']),
          senderName: _str(m['senderName'], fallback: _str(m['sender'])),
          text: _str(m['text']),
          timeMillis: time == 0
              ? DateTime.now().millisecondsSinceEpoch + i
              : time,
          type: _str(m['type'], fallback: 'text'),
          meta: payloadRaw is Map ? _map(payloadRaw) : null,
        ),
      );
    }

    out.sort(
      (PeerMessage a, PeerMessage b) => a.timeMillis.compareTo(b.timeMillis),
    );
    return out;
  }

  List<dynamic> _decodeList(dynamic value) {
    if (value is List) {
      return value;
    }
    if (value is String && value.trim().isNotEmpty) {
      try {
        final dynamic decoded = jsonDecode(value);
        if (decoded is List) {
          return decoded;
        }
      } catch (_) {}
    }
    return <dynamic>[];
  }

  Map<String, dynamic> _map(dynamic value) {
    if (value is Map<String, dynamic>) {
      return value;
    }
    if (value is Map) {
      return Map<String, dynamic>.from(value);
    }
    return <String, dynamic>{};
  }

  String _str(dynamic value, {String fallback = ''}) {
    final String text = (value ?? '').toString().trim();
    return text.isEmpty ? fallback : text;
  }

  int _int(dynamic value) {
    if (value is int) {
      return value;
    }
    if (value is num) {
      return value.toInt();
    }
    return int.tryParse((value ?? '').toString()) ?? 0;
  }

  List<String> _extractParticipants(Map<String, dynamic> source) {
    final List<dynamic> variants = <dynamic>[
      source['participants'],
      source['member_ids'],
      source['friend_ids'],
      source['users'],
    ];

    for (final dynamic value in variants) {
      if (value is List) {
        return value
            .map((dynamic e) => e.toString().trim())
            .where((String e) => e.isNotEmpty)
            .toList();
      }
      if (value is String && value.trim().isNotEmpty) {
        if (value.trim().startsWith('[')) {
          try {
            final dynamic decoded = jsonDecode(value);
            if (decoded is List) {
              return decoded
                  .map((dynamic e) => e.toString().trim())
                  .where((String e) => e.isNotEmpty)
                  .toList();
            }
          } catch (_) {}
        }
        return value
            .split(',')
            .map((String e) => e.trim())
            .where((String e) => e.isNotEmpty)
            .toList();
      }
    }
    return <String>[];
  }

  bool _isSendAccepted(Map<String, dynamic> response) {
    if (response.isEmpty || response['ok'] == false) {
      return false;
    }

    final String status = _str(response['status']).toUpperCase();
    if (status.contains('UNKNOWN') ||
        status.contains('FAIL') ||
        status.contains('ERROR') ||
        status.contains('INVALID') ||
        status.contains('DENIED')) {
      return false;
    }

    final String errorText = _str(response['error']);
    if (errorText.isNotEmpty) {
      return false;
    }

    final String msg = _str(response['message']).toLowerCase();
    if (msg.contains('unknown action') ||
        msg.contains('invalid action') ||
        msg.contains('failed') ||
        msg.contains('error') ||
        msg.contains('denied')) {
      return false;
    }

    return true;
  }

  List<PeerMessage> _applyDeleteForEveryone(List<PeerMessage> messages) {
    final Set<String> deleted = <String>{};
    int threadCutoff = 0;

    for (final PeerMessage m in messages) {
      if (m.type == 'delete_everyone') {
        final String id = _str(m.meta?['target_id']);
        if (id.isNotEmpty) {
          deleted.add(id);
        }
      }
      if (m.type == 'thread_delete_everyone') {
        threadCutoff = m.timeMillis > threadCutoff
            ? m.timeMillis
            : threadCutoff;
      }
    }

    final List<PeerMessage> out = <PeerMessage>[];
    for (final PeerMessage m in messages) {
      final bool isControl =
          m.type == 'delete_everyone' || m.type == 'thread_delete_everyone';
      if (isControl) {
        continue;
      }
      if (m.timeMillis <= threadCutoff) {
        continue;
      }
      if (deleted.contains(m.id)) {
        out.add(
          m.copyWith(
            text: 'This message was deleted',
            deletedForEveryone: true,
            type: 'deleted',
          ),
        );
      } else {
        out.add(m);
      }
    }
    return out;
  }

  Future<List<PeerMessage>> _applyLocalVisibility({
    required String myUserId,
    required String threadId,
    required List<PeerMessage> items,
  }) async {
    final Set<String> hidden = await hiddenMessageIdsForMe(
      myUserId: myUserId,
      threadId: threadId,
    );
    final int cutAt = await threadClearedAtForMe(
      myUserId: myUserId,
      threadId: threadId,
    );

    return items
        .where((PeerMessage m) => m.timeMillis > cutAt)
        .where((PeerMessage m) => !hidden.contains(m.id))
        .toList();
  }

  Future<void> _sendPeerWithRetry({
    required String chatId,
    required String participants,
    required Map<String, dynamic> payload,
  }) async {
    Object? last;
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        final Map<String, dynamic> response = await _backend.sendPeerMessage(
          chatId: chatId,
          participants: participants,
          payload: payload,
        );
        if (_isSendAccepted(response)) {
          return;
        }
        throw Exception(
          'Peer send not acknowledged: ${_str(response['message'], fallback: _str(response['status'], fallback: response.toString()))}',
        );
      } catch (e) {
        last = e;
        if (attempt < 2) {
          await Future<void>.delayed(
            Duration(milliseconds: 220 * (attempt + 1)),
          );
        }
      }
    }
    if (last != null) {
      throw last;
    }
  }

  Future<void> _sendDoubtWithRetry({
    required String threadId,
    required Map<String, dynamic> payload,
  }) async {
    Object? last;
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        final Map<String, dynamic> response = await _backend.sendDoubtMessage(
          threadId: threadId,
          payload: payload,
        );
        if (_isSendAccepted(response)) {
          return;
        }
        throw Exception(
          'Doubt send not acknowledged: ${_str(response['message'], fallback: _str(response['status'], fallback: response.toString()))}',
        );
      } catch (e) {
        last = e;
        if (attempt < 2) {
          await Future<void>.delayed(
            Duration(milliseconds: 220 * (attempt + 1)),
          );
        }
      }
    }
    if (last != null) {
      throw last;
    }
  }

  String _participantsCsv({
    required ChatThreadSummary thread,
    required String myUserId,
  }) {
    if (thread.participants.isNotEmpty) {
      final Set<String> ids = thread.participants
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty)
          .toSet();
      ids.add(myUserId);
      return ids.join(',');
    }

    if (thread.isGroup) {
      return myUserId;
    }

    String peerId = thread.peerId.trim();
    if (peerId.isEmpty) {
      final List<String> parts = thread.threadId
          .split('|')
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty)
          .toList();
      if (parts.length == 2) {
        final String mine = myUserId.trim().toLowerCase();
        peerId = parts.first.toLowerCase() == mine ? parts.last : parts.first;
      }
    }
    if (peerId.isEmpty) {
      final List<String> rawParticipants = _extractParticipants(
        thread.rawPayload ?? <String, dynamic>{},
      ).where((String e) => e != myUserId).toList();
      if (rawParticipants.isNotEmpty) {
        peerId = rawParticipants.first;
      }
    }
    if (peerId.isEmpty) {
      return myUserId;
    }
    return '$myUserId,$peerId';
  }

  Future<void> _cacheInbox({
    required String myUserId,
    required String role,
    required List<ChatThreadSummary> threads,
  }) async {
    try {
      final String value = jsonEncode(
        threads.map((ChatThreadSummary t) => t.toJson()).toList(),
      );
      await _setStringPref(_inboxCacheKey(myUserId, role), value);
    } catch (_) {}
  }

  Future<void> _cacheThread({
    required String myUserId,
    required String role,
    required String threadId,
    required List<PeerMessage> messages,
  }) async {
    try {
      final String value = jsonEncode(
        messages.map((PeerMessage m) => m.toJson()).toList(),
      );
      await _setStringPref(_threadCacheKey(myUserId, role, threadId), value);
    } catch (_) {}
  }

  String _displayText(PeerMessage? message) {
    if (message == null) {
      return '';
    }
    return switch (message.type) {
      'image' => 'Photo',
      'gif' => 'GIF',
      'pdf' => 'PDF',
      'audio' => 'Voice note',
      'file' => 'Attachment',
      'poll' => 'Poll',
      'answer_key_card' => 'Answer key card',
      'deleted' => 'This message was deleted',
      _ => message.text,
    };
  }

  String _safeId(String raw) {
    final String cleaned = raw.toLowerCase().replaceAll(
      RegExp(r'[^a-z0-9]'),
      '',
    );
    return cleaned.isEmpty ? 'user' : cleaned;
  }

  String _makeThreadId(String a, String b) {
    final String x = a.trim().toLowerCase();
    final String y = b.trim().toLowerCase();
    return x.compareTo(y) < 0 ? '$x|$y' : '$y|$x';
  }

  int _searchScoreForUser({
    required ChatUser user,
    required String normalizedQuery,
    required String compactQuery,
  }) {
    if (normalizedQuery.isEmpty) {
      return 1;
    }
    final String idNorm = _normalizeSearchQuery(user.userId);
    final String nameNorm = _normalizeSearchQuery(user.name);
    final String roleNorm = _normalizeSearchQuery(user.role);
    final String hay = _normalizeSearchQuery(
      '${user.name} ${user.userId} ${user.role}',
    );
    final String hayCompact = hay.replaceAll(' ', '');
    final List<String> queryTokens = normalizedQuery
        .split(' ')
        .where((String t) => t.isNotEmpty)
        .toList();

    int score = 0;
    if (idNorm == normalizedQuery || nameNorm == normalizedQuery) {
      score += 120;
    }
    if (idNorm.contains(normalizedQuery)) {
      score += 72;
    }
    if (nameNorm.contains(normalizedQuery)) {
      score += 64;
    }
    if (idNorm.startsWith(normalizedQuery)) {
      score += 92;
    }
    if (nameNorm.startsWith(normalizedQuery)) {
      score += 84;
    }
    if (roleNorm == normalizedQuery || roleNorm.startsWith(normalizedQuery)) {
      score += 36;
    }
    if (compactQuery.isNotEmpty && hayCompact.contains(compactQuery)) {
      score += 30;
    }
    if (queryTokens.isNotEmpty) {
      int tokenHits = 0;
      for (final String token in queryTokens) {
        if (hay.contains(token)) {
          tokenHits++;
        }
      }
      if (tokenHits == queryTokens.length) {
        score += 28 + (tokenHits * 4);
      } else if (tokenHits > 0) {
        score += tokenHits * 4;
      }
    }
    final String initials = _searchInitials(user.name);
    if (compactQuery.length >= 2 && initials.startsWith(compactQuery)) {
      score += 24;
    }
    if (compactQuery.length >= 2 && initials.contains(compactQuery)) {
      score += 14;
    }
    final String idCompact = idNorm.replaceAll(' ', '');
    final String nameCompact = nameNorm.replaceAll(' ', '');
    if (_isSubsequence(compactQuery, idCompact)) {
      score += 28;
    }
    if (_isSubsequence(compactQuery, nameCompact)) {
      score += 24;
    }
    final String digitQuery = normalizedQuery.replaceAll(RegExp(r'[^0-9]'), '');
    if (digitQuery.length >= 2) {
      if (idCompact.endsWith(digitQuery)) {
        score += 34;
      }
      if (idCompact.contains(digitQuery)) {
        score += 22;
      }
    }
    if (user.userId.toUpperCase() == 'TEACHER' &&
        !normalizedQuery.contains('teacher') &&
        !normalizedQuery.contains('admin')) {
      score -= 12;
    }
    return score;
  }

  PeerMessage? _lastDisplayMessage(List<PeerMessage> messages) {
    for (int i = messages.length - 1; i >= 0; i--) {
      final PeerMessage message = messages[i];
      if (_isControlType(message.type)) {
        continue;
      }
      return message;
    }
    return null;
  }

  bool _isControlType(String type) {
    return _controlTypes.contains(type.toLowerCase());
  }

  bool _matchesDirectThreadRow({
    required Map<String, dynamic> item,
    required ChatThreadSummary thread,
    required String myUserId,
  }) {
    if (thread.isDoubtThread || thread.isGroup) {
      return false;
    }
    final Set<String> expected = <String>{myUserId.trim()};
    final String peerId = thread.peerId.trim();
    if (peerId.isNotEmpty) {
      expected.add(peerId);
    } else {
      expected.addAll(
        thread.threadId
            .split('|')
            .map((String e) => e.trim())
            .where((String e) => e.isNotEmpty),
      );
    }
    if (expected.length < 2) {
      return false;
    }

    final Set<String> actual = <String>{};
    final List<String> participants = _extractParticipants(item);
    actual.addAll(
      participants
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty),
    );
    final String friendId = _str(item['friend_id']);
    if (friendId.isNotEmpty) {
      actual
        ..add(myUserId.trim())
        ..add(friendId.trim());
    }
    if (actual.length < 2) {
      actual.addAll(
        _str(item['chat_id'])
            .split('|')
            .map((String e) => e.trim())
            .where((String e) => e.isNotEmpty),
      );
    }
    if (actual.length < 2) {
      return false;
    }

    final Set<String> normalizedExpected = expected
        .map((String e) => e.toLowerCase())
        .toSet();
    final Set<String> normalizedActual = actual
        .map((String e) => e.toLowerCase())
        .toSet();
    return normalizedActual.containsAll(normalizedExpected);
  }

  bool _matchesGroupThreadRow({
    required Map<String, dynamic> item,
    required ChatThreadSummary thread,
  }) {
    if (!thread.isGroup) {
      return false;
    }
    final String rowId = _str(item['chat_id']);
    if (rowId.isNotEmpty &&
        rowId.toLowerCase() == thread.threadId.trim().toLowerCase()) {
      return true;
    }
    final String rowName = _str(
      item['group_name'],
      fallback: _str(item['title']),
    ).toLowerCase();
    final String threadName = thread.title.trim().toLowerCase();
    if (rowName.isNotEmpty && threadName.isNotEmpty && rowName == threadName) {
      return true;
    }
    return false;
  }

  bool _isSubsequence(String query, String target) {
    if (query.isEmpty || target.isEmpty) {
      return false;
    }
    if (query.length > target.length) {
      return false;
    }
    int i = 0;
    int j = 0;
    while (i < query.length && j < target.length) {
      if (query.codeUnitAt(i) == target.codeUnitAt(j)) {
        i += 1;
      }
      j += 1;
    }
    return i == query.length;
  }

  String _normalizeSearchQuery(String input) {
    return input
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9\s]'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  String _searchInitials(String input) {
    final List<String> words = _normalizeSearchQuery(
      input,
    ).split(' ').where((String w) => w.isNotEmpty).toList();
    if (words.isEmpty) {
      return '';
    }
    return words.map((String w) => w[0]).join();
  }

  String _inboxCacheKey(String userId, String role) =>
      'chat_cache_inbox_${role}_$userId';

  String _threadCacheKey(String userId, String role, String threadId) =>
      'chat_cache_thread_${role}_${userId}_$threadId';

  String _hiddenMessagesKey(String userId, String threadId) =>
      'chat_hidden_messages_${userId}_$threadId';

  String _threadClearedAtKey(String userId, String threadId) =>
      'chat_thread_cleared_at_${userId}_$threadId';

  String _hiddenThreadsKey(String userId) => 'chat_hidden_threads_$userId';

  String _threadPinnedKey(String userId, String threadId) =>
      'chat_pinned_message_${userId}_$threadId';

  void _logLatency(String operation, Stopwatch sw, {String extra = ''}) {
    assert(() {
      final String suffix = extra.isEmpty ? '' : ' $extra';
      debugPrint(
        '[LATENCY][chat] $operation ${sw.elapsedMilliseconds}ms$suffix',
      );
      return true;
    }());
  }
}

class _ScoredChatUser {
  const _ScoredChatUser({required this.user, required this.score});

  final ChatUser user;
  final int score;
}
