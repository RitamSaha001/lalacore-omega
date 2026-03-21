import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:image_picker/image_picker.dart';
import 'package:pdf/widgets.dart' as pw;
import 'package:printing/printing.dart';
import 'package:url_launcher/url_launcher.dart';

import '../models/chat_models.dart';
import '../services/ai_engine_service.dart';
import '../services/backend_service.dart';
import '../services/chat_service.dart';
import '../state/app_state.dart';
import 'liquid_glass.dart';
import 'smart_text.dart';

class ChatInboxScreen extends StatefulWidget {
  const ChatInboxScreen({
    super.key,
    required this.myUserId,
    required this.myName,
    required this.role,
    required this.chatService,
  });

  final String myUserId;
  final String myName;
  final String role;
  final ChatService chatService;

  @override
  State<ChatInboxScreen> createState() => _ChatInboxScreenState();
}

class _ChatInboxScreenState extends State<ChatInboxScreen>
    with WidgetsBindingObserver {
  static const Duration _pollInterval = Duration(seconds: 10);
  static const Duration _idlePollInterval = Duration(seconds: 16);
  static const Duration _deepIdlePollInterval = Duration(seconds: 24);
  bool _loading = true;
  bool _creatingGroup = false;
  String _query = '';
  List<ChatThreadSummary> _threads = <ChatThreadSummary>[];
  Timer? _poller;
  Future<void>? _loadInFlight;
  bool _appActive = true;
  Duration _currentPollInterval = _pollInterval;
  int _idlePollRounds = 0;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _warmStart();
    _load();
    _startPolling();
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _stopPolling();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    final bool active = state == AppLifecycleState.resumed;
    _appActive = active;
    if (active) {
      _resetPollingCadence();
      _startPolling();
      unawaited(_load(quiet: true));
    } else {
      _stopPolling();
    }
  }

  void _startPolling() {
    _stopPolling();
    if (!_appActive || !mounted) {
      return;
    }
    _poller = Timer.periodic(_currentPollInterval, (_) {
      if (!_appActive || !mounted) {
        return;
      }
      unawaited(_load(quiet: true));
    });
  }

  void _stopPolling() {
    _poller?.cancel();
    _poller = null;
  }

  void _resetPollingCadence() {
    _idlePollRounds = 0;
    if (_currentPollInterval != _pollInterval) {
      _currentPollInterval = _pollInterval;
      _startPolling();
    }
  }

  void _updatePollingCadence({required bool changed}) {
    if (changed) {
      _resetPollingCadence();
      return;
    }
    _idlePollRounds += 1;
    final Duration next = _idlePollRounds >= 5
        ? _deepIdlePollInterval
        : (_idlePollRounds >= 3 ? _idlePollInterval : _pollInterval);
    if (next != _currentPollInterval) {
      _currentPollInterval = next;
      _startPolling();
    }
  }

  bool _sameThreadList(List<ChatThreadSummary> a, List<ChatThreadSummary> b) {
    if (identical(a, b)) {
      return true;
    }
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      final ChatThreadSummary x = a[i];
      final ChatThreadSummary y = b[i];
      if (x.threadId != y.threadId ||
          x.updatedAtMillis != y.updatedAtMillis ||
          x.unread != y.unread ||
          x.lastMessage != y.lastMessage ||
          x.peerId != y.peerId ||
          x.title != y.title) {
        return false;
      }
    }
    return true;
  }

  Future<void> _warmStart() async {
    final List<ChatThreadSummary> cached = await widget.chatService
        .fetchCachedInbox(myUserId: widget.myUserId, role: widget.role);
    if (!mounted || cached.isEmpty) {
      return;
    }
    setState(() {
      _threads = cached;
      _loading = false;
    });
  }

  Future<void> _load({bool quiet = false}) async {
    if (_loadInFlight != null) {
      return _loadInFlight!;
    }
    final Future<void> run = () async {
      if (!quiet && mounted && _threads.isEmpty) {
        setState(() => _loading = true);
      }

      try {
        final List<ChatThreadSummary> data = await widget.chatService
            .fetchInbox(
              myUserId: widget.myUserId,
              myName: widget.myName,
              role: widget.role,
              includeDirectorySeed: !quiet,
            );
        if (!mounted) {
          return;
        }
        final bool unchanged = _sameThreadList(_threads, data);
        if (unchanged && _loading == false) {
          _updatePollingCadence(changed: false);
          return;
        }
        setState(() {
          _threads = data;
          _loading = false;
        });
        _updatePollingCadence(changed: true);
      } catch (_) {
        if (mounted && !quiet) {
          setState(() => _loading = false);
        }
      }
    }();
    _loadInFlight = run;
    try {
      await run;
    } finally {
      _loadInFlight = null;
    }
  }

  Future<void> _openThread(ChatThreadSummary thread) async {
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => ChatThreadScreen(
          thread: thread,
          myUserId: widget.myUserId,
          myName: widget.myName,
          role: widget.role,
          chatService: widget.chatService,
        ),
      ),
    );
    await _load(quiet: true);
  }

  Future<void> _openGroupCreator() async {
    if (_creatingGroup) {
      return;
    }
    setState(() => _creatingGroup = true);
    try {
      final ChatThreadSummary? thread = await Navigator.push<ChatThreadSummary>(
        context,
        MaterialPageRoute(
          builder: (_) => GroupCreateScreen(
            myUserId: widget.myUserId,
            myName: widget.myName,
            role: widget.role,
            chatService: widget.chatService,
            existingThreads: _threads,
          ),
        ),
      );
      if (!mounted) {
        return;
      }
      if (thread != null) {
        await _load(quiet: true);
        await _openThread(thread);
      }
    } finally {
      if (mounted) {
        setState(() => _creatingGroup = false);
      }
    }
  }

  String _normalizeSearch(String input) {
    return input
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9\s]'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  String _searchInitials(String input) {
    final List<String> words = _normalizeSearch(
      input,
    ).split(' ').where((String w) => w.isNotEmpty).toList();
    if (words.isEmpty) {
      return '';
    }
    return words.map((String w) => w[0]).join();
  }

  bool _matchesQuery(ChatThreadSummary thread, String rawQuery) {
    final String normalizedQuery = _normalizeSearch(rawQuery);
    if (normalizedQuery.isEmpty) {
      return true;
    }

    final String compactQuery = normalizedQuery.replaceAll(' ', '');
    final List<String> searchable = <String>[
      thread.title,
      thread.peerName,
      thread.peerId,
      thread.lastMessage,
      if (thread.rawPayload != null) ...<String>[
        '${thread.rawPayload!['class_name'] ?? ''}',
        '${thread.rawPayload!['subject'] ?? ''}',
        '${thread.rawPayload!['section'] ?? ''}',
        '${thread.rawPayload!['thread_type'] ?? ''}',
        '${thread.rawPayload!['group_name'] ?? ''}',
      ],
    ].map(_normalizeSearch).where((String v) => v.isNotEmpty).toList();

    if (searchable.any((String s) => s.contains(normalizedQuery))) {
      return true;
    }
    if (compactQuery.isNotEmpty &&
        searchable.any(
          (String s) => s.replaceAll(' ', '').contains(compactQuery),
        )) {
      return true;
    }

    final List<String> tokens = normalizedQuery
        .split(' ')
        .where((String t) => t.isNotEmpty)
        .toList();
    if (tokens.length > 1 &&
        tokens.every((String token) {
          return searchable.any((String s) => s.contains(token));
        })) {
      return true;
    }

    final String initials = _searchInitials(
      '${thread.title} ${thread.peerName}',
    );
    if (compactQuery.length >= 2 && initials.contains(compactQuery)) {
      return true;
    }
    return false;
  }

  @override
  Widget build(BuildContext context) {
    final bool isDark = Theme.of(context).brightness == Brightness.dark;
    final List<ChatThreadSummary> visible = _threads.where((
      ChatThreadSummary t,
    ) {
      if (widget.role == 'teacher' &&
          !t.isDoubtThread &&
          !t.isGroup &&
          t.peerId.trim().toUpperCase() == 'TEACHER') {
        return false;
      }
      return _matchesQuery(t, _query);
    }).toList();

    if (_loading && _threads.isEmpty) {
      return const Center(child: CircularProgressIndicator());
    }

    return RefreshIndicator(
      onRefresh: () => _load(quiet: false),
      child: ListView(
        padding: const EdgeInsets.all(14),
        children: <Widget>[
          LiquidGlass(
            solidFill: true,
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 4),
            child: TextField(
              onChanged: (String value) => setState(() => _query = value),
              decoration: InputDecoration(
                hintText: 'Search by name, ID, class, or message',
                hintStyle: TextStyle(
                  color: isDark
                      ? const Color(0xFF9FB0C7)
                      : Colors.grey.shade600,
                ),
                prefixIcon: Icon(
                  Icons.search,
                  color: isDark
                      ? const Color(0xFFB7C9E0)
                      : Colors.grey.shade600,
                ),
                border: InputBorder.none,
              ),
            ),
          ),
          const SizedBox(height: 10),
          Row(
            children: <Widget>[
              Expanded(
                child: Text(
                  'Peer & Teacher Chats',
                  style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.w800,
                    color: isDark
                        ? const Color(0xFFE9F2FF)
                        : const Color(0xFF1F2A38),
                  ),
                ),
              ),
              LiquidGlass(
                solidFill: true,
                onTap: _openGroupCreator,
                padding: const EdgeInsets.symmetric(
                  horizontal: 14,
                  vertical: 10,
                ),
                color: AppColors.primaryTone(
                  context,
                ).withOpacity(isDark ? 0.24 : 0.10),
                child: Row(
                  mainAxisSize: MainAxisSize.min,
                  children: <Widget>[
                    if (_creatingGroup)
                      const SizedBox(
                        width: 14,
                        height: 14,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      )
                    else
                      Icon(
                        Icons.group_add_rounded,
                        size: 18,
                        color: AppColors.primaryTone(context),
                      ),
                    const SizedBox(width: 8),
                    Text(
                      'New Group',
                      style: TextStyle(
                        fontWeight: FontWeight.w700,
                        color: AppColors.primaryTone(context),
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),
          const SizedBox(height: 10),
          if (visible.isEmpty)
            Padding(
              padding: const EdgeInsets.only(top: 120),
              child: Center(
                child: Text(
                  _query.isEmpty ? 'No chats yet' : 'No chat found',
                  style: TextStyle(
                    color: isDark ? const Color(0xFF9FB0C7) : Colors.grey,
                  ),
                ),
              ),
            ),
          ...visible.map((ChatThreadSummary thread) {
            final bool isTeacher =
                thread.peerId.toUpperCase() == 'TEACHER' &&
                !thread.isDoubtThread;
            return Padding(
              padding: const EdgeInsets.only(bottom: 10),
              child: LiquidGlass(
                solidFill: true,
                onTap: () => _openThread(thread),
                quality: LiquidGlassQuality.low,
                padding: const EdgeInsets.symmetric(
                  horizontal: 14,
                  vertical: 12,
                ),
                color: thread.isDoubtThread
                    ? Colors.orange.withOpacity(isDark ? 0.22 : 0.10)
                    : (thread.isGroup
                          ? Colors.cyan.withOpacity(isDark ? 0.18 : 0.08)
                          : (isTeacher
                                ? AppColors.blueTone(
                                    context,
                                  ).withOpacity(isDark ? 0.20 : 0.08)
                                : AppColors.primaryTone(
                                    context,
                                  ).withOpacity(isDark ? 0.16 : 0.06))),
                child: Row(
                  children: <Widget>[
                    CircleAvatar(
                      radius: 22,
                      backgroundColor: thread.isDoubtThread
                          ? Colors.orange.withOpacity(isDark ? 0.30 : 0.22)
                          : AppColors.primaryTone(
                              context,
                            ).withOpacity(isDark ? 0.24 : 0.16),
                      child: Icon(
                        thread.isDoubtThread
                            ? Icons.help_outline
                            : (thread.isGroup
                                  ? Icons.groups_rounded
                                  : (isTeacher ? Icons.school : Icons.person)),
                        color: thread.isDoubtThread
                            ? Colors.orange
                            : (isTeacher
                                  ? AppColors.blueTone(context)
                                  : AppColors.primaryTone(context)),
                      ),
                    ),
                    const SizedBox(width: 12),
                    Expanded(
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: <Widget>[
                          Row(
                            children: <Widget>[
                              Expanded(
                                child: Text(
                                  thread.title,
                                  maxLines: 1,
                                  overflow: TextOverflow.ellipsis,
                                  style: TextStyle(
                                    fontWeight: thread.unread
                                        ? FontWeight.bold
                                        : FontWeight.w700,
                                  ),
                                ),
                              ),
                              Text(
                                _shortTime(thread.updatedAtMillis),
                                style: TextStyle(
                                  fontSize: 11,
                                  color: isDark
                                      ? const Color(0xFF9FB0C7)
                                      : Colors.grey,
                                ),
                              ),
                            ],
                          ),
                          const SizedBox(height: 4),
                          Text(
                            thread.lastMessage.isEmpty
                                ? 'Tap to open'
                                : thread.lastMessage,
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                            style: TextStyle(
                              color: isDark
                                  ? const Color(0xFFC2CEDF)
                                  : Colors.grey.shade700,
                              fontSize: 13,
                            ),
                          ),
                        ],
                      ),
                    ),
                    const SizedBox(width: 8),
                    if (thread.unread)
                      Container(
                        width: 10,
                        height: 10,
                        decoration: BoxDecoration(
                          color: AppColors.successTone(context),
                          shape: BoxShape.circle,
                        ),
                      ),
                  ],
                ),
              ),
            );
          }),
        ],
      ),
    );
  }
}

class ChatThreadScreen extends StatefulWidget {
  const ChatThreadScreen({
    super.key,
    required this.thread,
    required this.myUserId,
    required this.myName,
    required this.role,
    required this.chatService,
  });

  final ChatThreadSummary thread;
  final String myUserId;
  final String myName;
  final String role;
  final ChatService chatService;

  @override
  State<ChatThreadScreen> createState() => _ChatThreadScreenState();
}

class _ChatThreadScreenState extends State<ChatThreadScreen>
    with WidgetsBindingObserver {
  static const Duration _pollFast = Duration(seconds: 2);
  static const Duration _pollIdle = Duration(seconds: 4);
  static const Duration _pollDeepIdle = Duration(seconds: 7);
  static const Duration _activityHoldDuration = Duration(seconds: 14);
  static const Set<String> _controlMessageTypes = <String>{
    'poll_vote',
    'poll_close',
    'read_receipt',
    'pin_message',
    'unpin_message',
  };
  final TextEditingController _ctrl = TextEditingController();
  final TextEditingController _threadSearchCtrl = TextEditingController();
  final ScrollController _scroll = ScrollController();
  final AiEngineService _aiService = AiEngineService(
    backendService: BackendService(),
  );

  List<PeerMessage> _messages = <PeerMessage>[];
  bool _loading = true;
  bool _uploading = false;
  bool _aiBusy = false;
  bool _searchMode = false;
  String _threadSearchQuery = '';
  int _searchSelection = 0;
  String? _highlightMessageId;
  String _lastReadReceiptForMessageId = '';
  int _holdFastPollUntilMs = 0;
  final Set<String> _whooshIds = <String>{};
  final Map<String, GlobalKey> _messageKeys = <String, GlobalKey>{};
  Timer? _poller;
  Future<void>? _loadInFlight;
  bool _appActive = true;
  Duration _pollInterval = _pollFast;
  int _idlePollRounds = 0;
  bool _scrollToBottomPending = false;
  int _localMessageSeq = 0;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _scroll.addListener(_onScrollChanged);
    _boot();
    _startPolling();
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _stopPolling();
    _scroll.removeListener(_onScrollChanged);
    _ctrl.dispose();
    _threadSearchCtrl.dispose();
    _scroll.dispose();
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    final bool active = state == AppLifecycleState.resumed;
    _appActive = active;
    if (active) {
      _resetPollingCadence();
      _startPolling();
      unawaited(_load(quiet: true));
    } else {
      _stopPolling();
    }
  }

  void _startPolling() {
    _stopPolling();
    if (!_appActive || !mounted) {
      return;
    }
    _poller = Timer.periodic(_pollInterval, (_) {
      if (!_appActive || !mounted) {
        return;
      }
      unawaited(_load(quiet: true));
    });
  }

  void _stopPolling() {
    _poller?.cancel();
    _poller = null;
  }

  void _resetPollingCadence() {
    _idlePollRounds = 0;
    if (_pollInterval != _pollFast) {
      _pollInterval = _pollFast;
      _startPolling();
    }
  }

  void _updatePollingCadence({required bool changed}) {
    final int now = DateTime.now().millisecondsSinceEpoch;
    if (now < _holdFastPollUntilMs) {
      if (_pollInterval != _pollFast) {
        _pollInterval = _pollFast;
        _startPolling();
      }
      return;
    }
    if (changed) {
      _resetPollingCadence();
      return;
    }
    _idlePollRounds += 1;
    final Duration next = _idlePollRounds >= 6
        ? _pollDeepIdle
        : (_idlePollRounds >= 3 ? _pollIdle : _pollFast);
    if (next != _pollInterval) {
      _pollInterval = next;
      _startPolling();
    }
  }

  void _markRealtimeActivity() {
    _holdFastPollUntilMs =
        DateTime.now().millisecondsSinceEpoch +
        _activityHoldDuration.inMilliseconds;
    _resetPollingCadence();
  }

  void _onScrollChanged() {
    if (!_isNearBottom()) {
      return;
    }
    _maybeSendReadReceiptForLatestIncoming(_messages);
  }

  bool _sameMessageList(List<PeerMessage> a, List<PeerMessage> b) {
    if (identical(a, b)) {
      return true;
    }
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      final PeerMessage x = a[i];
      final PeerMessage y = b[i];
      if (x.id != y.id ||
          x.senderId != y.senderId ||
          x.senderName != y.senderName ||
          x.timeMillis != y.timeMillis ||
          x.text != y.text ||
          x.type != y.type ||
          jsonEncode(x.meta ?? const <String, dynamic>{}) !=
              jsonEncode(y.meta ?? const <String, dynamic>{}) ||
          x.pending != y.pending ||
          x.failed != y.failed ||
          x.deletedForEveryone != y.deletedForEveryone) {
        return false;
      }
    }
    return true;
  }

  Future<void> _boot() async {
    final List<PeerMessage> cached = await widget.chatService
        .fetchCachedThreadMessages(
          thread: widget.thread,
          myUserId: widget.myUserId,
          role: widget.role,
        );

    if (mounted && cached.isNotEmpty) {
      setState(() {
        _messages = cached;
        _loading = false;
      });
      _jumpToBottom(animate: false, force: true);
    }

    await _load(quiet: cached.isNotEmpty);
  }

  Future<void> _load({bool quiet = false}) async {
    if (_loadInFlight != null) {
      return _loadInFlight!;
    }
    final Future<void> run = () async {
      if (!quiet && mounted && _messages.isEmpty) {
        setState(() => _loading = true);
      }

      try {
        final List<PeerMessage> data = await widget.chatService
            .fetchThreadMessages(
              thread: widget.thread,
              myUserId: widget.myUserId,
              role: widget.role,
            );
        if (!mounted) {
          return;
        }
        final List<PeerMessage> merged = _mergeRemoteWithUnsyncedLocal(data);
        final bool unchanged = _sameMessageList(_messages, merged);
        if (unchanged && _loading == false) {
          _maybeSendReadReceiptForLatestIncoming(merged);
          _updatePollingCadence(changed: false);
          return;
        }

        setState(() {
          _messages = merged;
          _loading = false;
        });
        _updatePollingCadence(changed: true);
        _jumpToBottom();
        _maybeSendReadReceiptForLatestIncoming(merged);
      } catch (_) {
        if (mounted && !quiet) {
          setState(() => _loading = false);
        }
      }
    }();
    _loadInFlight = run;
    try {
      await run;
    } finally {
      _loadInFlight = null;
    }
  }

  bool _isControlMessageType(String type) {
    return _controlMessageTypes.contains(type.toLowerCase());
  }

  bool _isNearBottom() {
    if (!_scroll.hasClients) {
      return true;
    }
    final ScrollPosition position = _scroll.position;
    return (position.maxScrollExtent - position.pixels) <= 240;
  }

  PeerMessage? _latestIncomingForReadReceipt(List<PeerMessage> messages) {
    for (int i = messages.length - 1; i >= 0; i--) {
      final PeerMessage message = messages[i];
      if (message.senderId == widget.myUserId) {
        continue;
      }
      if (message.senderId.trim().isEmpty) {
        continue;
      }
      if (_isControlMessageType(message.type)) {
        continue;
      }
      if (message.deletedForEveryone || message.type == 'deleted') {
        continue;
      }
      return message;
    }
    return null;
  }

  void _maybeSendReadReceiptForLatestIncoming(List<PeerMessage> messages) {
    if (!_appActive || !_isNearBottom()) {
      return;
    }
    final PeerMessage? target = _latestIncomingForReadReceipt(messages);
    if (target == null || _lastReadReceiptForMessageId == target.id) {
      return;
    }
    _lastReadReceiptForMessageId = target.id;
    unawaited(
      widget.chatService
          .sendReadReceipt(
            thread: widget.thread,
            myUserId: widget.myUserId,
            myName: widget.myName,
            messageId: target.id,
          )
          .catchError((Object _) {
            if (_lastReadReceiptForMessageId == target.id) {
              _lastReadReceiptForMessageId = '';
            }
          }),
    );
  }

  Future<void> _sendText() async {
    final String text = _ctrl.text.trim();
    if (text.isEmpty) {
      return;
    }

    _ctrl.clear();
    final String? gifUrl = _extractKeyboardGifUrl(text);
    if (gifUrl != null) {
      await _sendLocalThenRemote(
        type: 'gif',
        text: text,
        meta: <String, dynamic>{'url': gifUrl, 'name': 'GIF'},
      );
      return;
    }
    await _sendLocalThenRemote(type: 'text', text: text);
  }

  String? _extractKeyboardGifUrl(String text) {
    final RegExp urlPattern = RegExp(r'https?://\S+');
    final RegExpMatch? match = urlPattern.firstMatch(text);
    if (match == null) {
      return null;
    }
    final String url = match.group(0) ?? '';
    final Uri? uri = Uri.tryParse(url);
    if (uri == null || uri.host.trim().isEmpty) {
      return null;
    }

    final String host = uri.host.toLowerCase();
    final String path = uri.path.toLowerCase();
    final bool isGifSource =
        path.endsWith('.gif') ||
        host.contains('giphy.com') ||
        host.contains('tenor.com');
    return isGifSource ? url : null;
  }

  Future<void> _sendLocalThenRemote({
    required String type,
    required String text,
    Map<String, dynamic>? meta,
  }) async {
    _markRealtimeActivity();
    final String localId = _nextLocalId('local');
    final PeerMessage local = PeerMessage(
      id: localId,
      threadId: widget.thread.threadId,
      senderId: widget.myUserId,
      senderName: widget.myName,
      text: text,
      timeMillis: DateTime.now().millisecondsSinceEpoch,
      type: type,
      meta: meta,
      pending: true,
    );

    setState(() {
      _whooshIds.add(localId);
      _messages = <PeerMessage>[..._messages, local];
    });
    _triggerWhoosh(localId);
    _jumpToBottom(force: true);

    try {
      await widget.chatService.sendMessagePayload(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        type: type,
        text: text,
        meta: meta,
        messageId: localId,
      );

      _replaceMessage(
        localId,
        (PeerMessage old) => old.copyWith(pending: false, failed: false),
      );
      unawaited(_load(quiet: true));
    } catch (e) {
      _replaceMessage(
        localId,
        (PeerMessage old) => old.copyWith(pending: false, failed: true),
      );
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('Message failed to send: $e')));
      }
    }
  }

  void _replaceMessage(String id, PeerMessage Function(PeerMessage) mapper) {
    if (!mounted) {
      return;
    }
    setState(() {
      _messages = _messages.map((PeerMessage m) {
        if (m.id != id) {
          return m;
        }
        return mapper(m);
      }).toList();
    });
  }

  List<PeerMessage> _mergeRemoteWithUnsyncedLocal(List<PeerMessage> remote) {
    final int now = DateTime.now().millisecondsSinceEpoch;
    final List<PeerMessage> localDrafts = _messages.where((PeerMessage m) {
      if (m.senderId != widget.myUserId) {
        return false;
      }
      if (now - m.timeMillis > const Duration(minutes: 30).inMilliseconds) {
        return false;
      }
      if (m.failed || m.pending) {
        return true;
      }
      return m.id.startsWith('local_') ||
          m.id.startsWith('upload_') ||
          m.id.startsWith('vote_');
    }).toList();

    final List<PeerMessage> merged = <PeerMessage>[...remote];
    final Set<int> usedRemoteIndexes = <int>{};
    for (final PeerMessage local in localDrafts) {
      final int ackIndex = _findRemoteAckIndex(
        local: local,
        remote: remote,
        usedIndexes: usedRemoteIndexes,
      );
      if (ackIndex >= 0) {
        usedRemoteIndexes.add(ackIndex);
      } else if (!merged.any((PeerMessage m) => m.id == local.id)) {
        merged.add(local);
      }
    }

    merged.sort((PeerMessage a, PeerMessage b) {
      final int byTime = a.timeMillis.compareTo(b.timeMillis);
      if (byTime != 0) {
        return byTime;
      }
      return a.id.compareTo(b.id);
    });
    return merged;
  }

  int _findRemoteAckIndex({
    required PeerMessage local,
    required List<PeerMessage> remote,
    required Set<int> usedIndexes,
  }) {
    for (int i = 0; i < remote.length; i++) {
      if (usedIndexes.contains(i)) {
        continue;
      }
      if (remote[i].id == local.id) {
        return i;
      }
    }

    int bestIndex = -1;
    int bestDiff = 1 << 30;
    for (int i = 0; i < remote.length; i++) {
      if (usedIndexes.contains(i)) {
        continue;
      }
      final PeerMessage candidate = remote[i];
      if (!_matchesAckByContent(local: local, remote: candidate)) {
        continue;
      }
      final int diff = (candidate.timeMillis - local.timeMillis).abs();
      if (diff < bestDiff) {
        bestDiff = diff;
        bestIndex = i;
      }
    }
    return bestIndex;
  }

  bool _matchesAckByContent({
    required PeerMessage local,
    required PeerMessage remote,
  }) {
    if (remote.senderId != widget.myUserId || remote.type != local.type) {
      return false;
    }
    final int diff = (remote.timeMillis - local.timeMillis).abs();
    if (diff > const Duration(minutes: 5).inMilliseconds) {
      return false;
    }

    if (local.type == 'text' || local.type == 'poll') {
      return remote.text.trim() == local.text.trim();
    }

    if (local.type == 'image' ||
        local.type == 'pdf' ||
        local.type == 'audio' ||
        local.type == 'gif' ||
        local.type == 'file') {
      final String localName = (local.meta?['name'] ?? local.text)
          .toString()
          .trim();
      final String remoteName = (remote.meta?['name'] ?? remote.text)
          .toString()
          .trim();
      if (localName.isEmpty || remoteName.isEmpty) {
        return false;
      }
      return localName == remoteName;
    }

    return remote.text.trim() == local.text.trim();
  }

  Future<void> _retryMessage(PeerMessage message) async {
    _markRealtimeActivity();
    _replaceMessage(
      message.id,
      (PeerMessage old) => old.copyWith(pending: true, failed: false),
    );

    try {
      await widget.chatService.sendMessagePayload(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        type: message.type,
        text: message.text,
        meta: message.meta,
        messageId: message.id,
      );
      _replaceMessage(
        message.id,
        (PeerMessage old) => old.copyWith(pending: false, failed: false),
      );
      unawaited(_load(quiet: true));
    } catch (e) {
      _replaceMessage(
        message.id,
        (PeerMessage old) => old.copyWith(pending: false, failed: true),
      );
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('Retry failed: $e')));
      }
    }
  }

  Future<void> _openComposerActions() async {
    final String? action = await showModalBottomSheet<String>(
      context: context,
      backgroundColor: Colors.transparent,
      builder: (BuildContext context) {
        return SafeArea(
          child: Padding(
            padding: const EdgeInsets.fromLTRB(12, 0, 12, 12),
            child: LiquidGlass(
              solidFill: true,
              padding: const EdgeInsets.symmetric(vertical: 8),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: <Widget>[
                  _sheetAction(
                    context,
                    icon: Icons.photo_library_outlined,
                    title: 'Image',
                    value: 'image',
                  ),
                  _sheetAction(
                    context,
                    icon: Icons.picture_as_pdf_outlined,
                    title: 'PDF',
                    value: 'pdf',
                  ),
                  _sheetAction(
                    context,
                    icon: Icons.mic_none_rounded,
                    title: 'Voice Note',
                    value: 'audio',
                  ),
                  _sheetAction(
                    context,
                    icon: Icons.poll_outlined,
                    title: 'Poll',
                    value: 'poll',
                  ),
                ],
              ),
            ),
          ),
        );
      },
    );

    if (!mounted || action == null) {
      return;
    }

    if (action == 'image') {
      await _pickImageAndSend();
    } else if (action == 'pdf') {
      await _pickFileAndSend(
        type: 'pdf',
        allowedExtensions: <String>['pdf'],
        fallbackMimeType: 'application/pdf',
      );
    } else if (action == 'audio') {
      await _pickFileAndSend(
        type: 'audio',
        allowedExtensions: <String>[
          'mp3',
          'wav',
          'aac',
          'm4a',
          'ogg',
          'opus',
          'webm',
        ],
        fallbackMimeType: 'audio/m4a',
      );
    } else if (action == 'poll') {
      await _composePollAndSend();
    }
  }

  Widget _sheetAction(
    BuildContext context, {
    required IconData icon,
    required String title,
    required String value,
  }) {
    return ListTile(
      leading: Icon(icon),
      title: Text(title),
      onTap: () => Navigator.pop(context, value),
    );
  }

  Future<void> _pickImageAndSend() async {
    final XFile? x = await ImagePicker().pickImage(
      source: ImageSource.gallery,
      imageQuality: 88,
    );
    if (x == null) {
      return;
    }
    final Uint8List bytes = await x.readAsBytes();
    final String lower = x.name.toLowerCase();
    final String mime = lower.endsWith('.png')
        ? 'image/png'
        : (lower.endsWith('.webp') ? 'image/webp' : 'image/jpeg');
    await _uploadAndSendFile(
      bytes: bytes,
      fileName: x.name,
      mimeType: mime,
      type: 'image',
    );
  }

  Future<void> _pickFileAndSend({
    required String type,
    required List<String> allowedExtensions,
    required String fallbackMimeType,
  }) async {
    final FilePickerResult? result = await FilePicker.platform.pickFiles(
      type: FileType.custom,
      withData: true,
      allowedExtensions: allowedExtensions,
    );
    if (result == null || result.files.isEmpty) {
      return;
    }
    final PlatformFile file = result.files.first;
    final Uint8List? bytes = await _bytesFor(file);
    if (bytes == null) {
      return;
    }

    await _uploadAndSendFile(
      bytes: bytes,
      fileName: file.name,
      mimeType: _guessMimeType(file.name, fallbackMimeType),
      type: type,
    );
  }

  String _guessMimeType(String fileName, String fallback) {
    final String lower = fileName.toLowerCase();
    if (lower.endsWith('.png')) {
      return 'image/png';
    }
    if (lower.endsWith('.webp')) {
      return 'image/webp';
    }
    if (lower.endsWith('.jpg') || lower.endsWith('.jpeg')) {
      return 'image/jpeg';
    }
    if (lower.endsWith('.pdf')) {
      return 'application/pdf';
    }
    if (lower.endsWith('.wav')) {
      return 'audio/wav';
    }
    if (lower.endsWith('.mp3')) {
      return 'audio/mpeg';
    }
    if (lower.endsWith('.aac')) {
      return 'audio/aac';
    }
    if (lower.endsWith('.m4a')) {
      return 'audio/mp4';
    }
    if (lower.endsWith('.ogg') || lower.endsWith('.opus')) {
      return 'audio/ogg';
    }
    if (lower.endsWith('.webm')) {
      return 'audio/webm';
    }
    return fallback;
  }

  Future<Uint8List?> _bytesFor(PlatformFile file) async {
    if (file.bytes != null) {
      return file.bytes;
    }
    if (file.path == null || file.path!.isEmpty) {
      return null;
    }
    try {
      return await File(file.path!).readAsBytes();
    } catch (_) {
      return null;
    }
  }

  Future<void> _uploadAndSendFile({
    required Uint8List bytes,
    required String fileName,
    required String mimeType,
    required String type,
  }) async {
    _markRealtimeActivity();
    final String localId = _nextLocalId('upload');

    final PeerMessage local = PeerMessage(
      id: localId,
      threadId: widget.thread.threadId,
      senderId: widget.myUserId,
      senderName: widget.myName,
      text: fileName,
      timeMillis: DateTime.now().millisecondsSinceEpoch,
      type: type,
      meta: <String, dynamic>{
        'name': fileName,
        'mime': mimeType,
        'size': bytes.length,
      },
      pending: true,
    );

    setState(() {
      _whooshIds.add(localId);
      _uploading = true;
      _messages = <PeerMessage>[..._messages, local];
    });
    _triggerWhoosh(localId);
    _jumpToBottom(force: true);

    try {
      final String url = await widget.chatService.uploadBytesAsFile(
        bytes: bytes,
        fileName: fileName,
        mimeType: mimeType,
      );
      final Map<String, dynamic> payload = <String, dynamic>{
        'url': url,
        'name': fileName,
        'mime': mimeType,
        'size': bytes.length,
      };
      await widget.chatService.sendMessagePayload(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        type: type,
        text: fileName,
        meta: payload,
        messageId: localId,
      );

      _replaceMessage(
        localId,
        (PeerMessage old) =>
            old.copyWith(pending: false, failed: false, meta: payload),
      );
      unawaited(_load(quiet: true));
    } catch (_) {
      _replaceMessage(
        localId,
        (PeerMessage old) => old.copyWith(pending: false, failed: true),
      );
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Upload failed. Tap retry.')),
        );
      }
    }

    if (mounted) {
      setState(() => _uploading = false);
    }
  }

  Future<void> _composePollAndSend() async {
    final _PollDraft? poll = await showModalBottomSheet<_PollDraft>(
      context: context,
      isScrollControlled: true,
      backgroundColor: Colors.transparent,
      builder: (_) => const _PollComposerSheet(),
    );

    if (poll == null) {
      return;
    }

    final String pollId = _nextLocalId('poll');
    await _sendLocalThenRemote(
      type: 'poll',
      text: poll.question,
      meta: <String, dynamic>{
        'poll_id': pollId,
        'question': poll.question,
        'context': poll.context,
        'options': poll.options,
      },
    );
  }

  Future<void> _votePoll(PeerMessage poll, int optionIndex) async {
    _markRealtimeActivity();
    final String pollId = (poll.meta?['poll_id'] ?? '').toString();
    if (pollId.isEmpty) {
      return;
    }
    final Map<String, _PollCloseState> closeStates = _pollCloseStateMap(
      _messages,
    );
    if (closeStates[pollId]?.isClosed == true) {
      return;
    }
    final Map<String, Map<String, int>> votesByUser = _pollChoicesByUser(
      _messages,
      closeStates: closeStates,
    );
    final int? myCurrentChoice = votesByUser[pollId]?[widget.myUserId];
    if (myCurrentChoice == optionIndex) {
      return;
    }

    final String localVoteId = _nextLocalId('vote');
    final PeerMessage localVote = PeerMessage(
      id: localVoteId,
      threadId: widget.thread.threadId,
      senderId: widget.myUserId,
      senderName: widget.myName,
      text: 'voted',
      timeMillis: DateTime.now().millisecondsSinceEpoch,
      type: 'poll_vote',
      pending: true,
      meta: <String, dynamic>{'poll_id': pollId, 'option_index': optionIndex},
    );

    setState(() => _messages = <PeerMessage>[..._messages, localVote]);

    try {
      await widget.chatService.sendPollVote(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        pollId: pollId,
        optionIndex: optionIndex,
      );
      _replaceMessage(
        localVoteId,
        (PeerMessage old) => old.copyWith(pending: false, failed: false),
      );
      unawaited(_load(quiet: true));
    } catch (_) {
      _replaceMessage(
        localVoteId,
        (PeerMessage old) => old.copyWith(pending: false, failed: true),
      );
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Vote failed to send. Retry later.')),
        );
      }
    }
  }

  Future<void> _finalizePoll(PeerMessage poll) async {
    _markRealtimeActivity();
    final String pollId = (poll.meta?['poll_id'] ?? '').toString();
    final String question = (poll.meta?['question'] ?? poll.text).toString();
    if (pollId.isEmpty) {
      return;
    }
    if (poll.senderId != widget.myUserId) {
      return;
    }

    final Map<String, _PollCloseState> closeStates = _pollCloseStateMap(
      _messages,
    );
    if (closeStates[pollId]?.isClosed == true) {
      return;
    }

    final bool confirm =
        await showDialog<bool>(
          context: context,
          builder: (BuildContext context) {
            return AlertDialog(
              title: const Text('Finalize Poll'),
              content: const Text(
                'After finalizing, participants can no longer vote. Continue?',
              ),
              actions: <Widget>[
                TextButton(
                  onPressed: () => Navigator.pop(context, false),
                  child: const Text('Cancel'),
                ),
                ElevatedButton(
                  onPressed: () => Navigator.pop(context, true),
                  child: const Text('Finalize'),
                ),
              ],
            );
          },
        ) ??
        false;
    if (!confirm) {
      return;
    }

    final Map<String, Map<String, int>> votesByUser = _pollChoicesByUser(
      _messages,
      closeStates: closeStates,
    );
    final Map<int, int> counts =
        _pollCountsFromUserChoices(votesByUser)[pollId] ?? <int, int>{};
    final int totalVotes = counts.values.fold(0, (int a, int b) => a + b);
    int winnerIndex = -1;
    int winnerVotes = 0;
    bool tie = false;
    counts.forEach((int index, int count) {
      if (count > winnerVotes) {
        winnerVotes = count;
        winnerIndex = index;
        tie = false;
      } else if (count == winnerVotes && count > 0) {
        tie = true;
      }
    });
    if (tie) {
      winnerIndex = -1;
    }

    final String localCloseId = _nextLocalId('poll_close');
    final Map<String, dynamic> closeMeta = <String, dynamic>{
      'poll_id': pollId,
      'question': question,
      'closed_at': DateTime.now().millisecondsSinceEpoch,
      'total_votes': totalVotes,
      'results': counts,
      if (winnerIndex >= 0) 'winner_index': winnerIndex,
      if (winnerVotes > 0) 'winner_votes': winnerVotes,
    };
    final PeerMessage localClose = PeerMessage(
      id: localCloseId,
      threadId: widget.thread.threadId,
      senderId: widget.myUserId,
      senderName: widget.myName,
      text: 'Poll finalized',
      timeMillis: DateTime.now().millisecondsSinceEpoch,
      type: 'poll_close',
      pending: true,
      meta: closeMeta,
    );
    setState(() => _messages = <PeerMessage>[..._messages, localClose]);

    try {
      await widget.chatService.sendPollClose(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        pollId: pollId,
        question: question,
        finalResults: closeMeta,
      );
      _replaceMessage(
        localCloseId,
        (PeerMessage old) => old.copyWith(pending: false, failed: false),
      );
      unawaited(_load(quiet: true));
    } catch (_) {
      _replaceMessage(
        localCloseId,
        (PeerMessage old) => old.copyWith(pending: false, failed: true),
      );
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Could not finalize poll. Retry later.'),
          ),
        );
      }
    }
  }

  Future<void> _deleteForMe(PeerMessage message) async {
    await widget.chatService.hideMessageForMe(
      myUserId: widget.myUserId,
      threadId: widget.thread.threadId,
      messageId: message.id,
    );
    if (!mounted) {
      return;
    }
    setState(() {
      _messages = _messages
          .where((PeerMessage m) => m.id != message.id)
          .toList();
    });
  }

  Future<void> _deleteForEveryone(PeerMessage message) async {
    _replaceMessage(
      message.id,
      (PeerMessage old) => old.copyWith(
        type: 'deleted',
        text: 'This message was deleted',
        deletedForEveryone: true,
      ),
    );

    try {
      await widget.chatService.sendDeleteForEveryone(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        targetMessageId: message.id,
      );
    } catch (_) {}
  }

  Future<void> _clearChatForMe() async {
    await widget.chatService.clearChatForMe(
      myUserId: widget.myUserId,
      threadId: widget.thread.threadId,
    );
    if (!mounted) {
      return;
    }
    setState(() {
      _messages = <PeerMessage>[];
    });
  }

  Future<void> _deleteThreadForMe() async {
    await widget.chatService.hideThreadForMe(
      myUserId: widget.myUserId,
      threadId: widget.thread.threadId,
    );
    await _clearChatForMe();
    if (mounted) {
      Navigator.pop(context);
    }
  }

  Future<void> _deleteThreadForEveryone() async {
    await widget.chatService.sendThreadDeleteForEveryone(
      thread: widget.thread,
      myUserId: widget.myUserId,
      myName: widget.myName,
    );
    await _clearChatForMe();
  }

  Future<void> _showMessageActions(PeerMessage message) async {
    final bool isMine = message.senderId == widget.myUserId;
    final bool canDeleteEveryone =
        isMine &&
        !message.deletedForEveryone &&
        message.type != 'deleted' &&
        !widget.thread.isDoubtThread;
    final bool isImageOrPdf = message.type == 'image' || message.type == 'pdf';
    final bool isVoice = message.type == 'audio';
    final bool inviteMessage = message.type == 'group_invite';

    final List<_AiMessageAction> visibleActions = <_AiMessageAction>[
      if (inviteMessage) _AiMessageAction.openGroupInvite,
      if (isImageOrPdf) _AiMessageAction.aiSummarize,
      if (isImageOrPdf) _AiMessageAction.aiJeeNotes,
      if (isVoice) _AiMessageAction.aiTranscribe,
      _AiMessageAction.deleteMe,
      if (canDeleteEveryone) _AiMessageAction.deleteEveryone,
    ];

    final _AiMessageAction? action =
        await showModalBottomSheet<_AiMessageAction>(
          context: context,
          backgroundColor: Colors.transparent,
          builder: (_) {
            return SafeArea(
              child: Padding(
                padding: const EdgeInsets.fromLTRB(12, 0, 12, 12),
                child: LiquidGlass(
                  solidFill: true,
                  quality: LiquidGlassQuality.low,
                  padding: const EdgeInsets.symmetric(vertical: 8),
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    children: visibleActions.map((_AiMessageAction item) {
                      final _AiActionVisual visual = _actionVisual(item);
                      return ListTile(
                        leading: Icon(visual.icon),
                        title: Text(visual.label),
                        onTap: () => Navigator.pop(context, item),
                      );
                    }).toList(),
                  ),
                ),
              ),
            );
          },
        );

    if (!mounted || action == null) {
      return;
    }

    if (action == _AiMessageAction.openGroupInvite) {
      await _openGroupFromInvite(message);
    } else if (action == _AiMessageAction.aiSummarize) {
      await _runAiForMessage(message: message, action: action);
    } else if (action == _AiMessageAction.aiJeeNotes) {
      await _runAiForMessage(message: message, action: action);
    } else if (action == _AiMessageAction.aiTranscribe) {
      await _runAiForMessage(message: message, action: action);
    } else if (action == _AiMessageAction.deleteMe) {
      await _deleteForMe(message);
    } else if (action == _AiMessageAction.deleteEveryone) {
      await _deleteForEveryone(message);
    }
  }

  _AiActionVisual _actionVisual(_AiMessageAction action) {
    return switch (action) {
      _AiMessageAction.openGroupInvite => const _AiActionVisual(
        icon: Icons.groups_rounded,
        label: 'Open group invite',
      ),
      _AiMessageAction.aiSummarize => const _AiActionVisual(
        icon: Icons.summarize_outlined,
        label: 'AI summarize',
      ),
      _AiMessageAction.aiJeeNotes => const _AiActionVisual(
        icon: Icons.menu_book_rounded,
        label: 'Make JEE notes',
      ),
      _AiMessageAction.aiTranscribe => const _AiActionVisual(
        icon: Icons.record_voice_over_outlined,
        label: 'Transcribe voice note',
      ),
      _AiMessageAction.deleteMe => const _AiActionVisual(
        icon: Icons.delete_outline,
        label: 'Delete for me',
      ),
      _AiMessageAction.deleteEveryone => const _AiActionVisual(
        icon: Icons.delete_forever_outlined,
        label: 'Delete for everyone',
      ),
    };
  }

  void _jumpToBottom({bool animate = true, bool force = false}) {
    if (_scrollToBottomPending) {
      return;
    }
    _scrollToBottomPending = true;
    WidgetsBinding.instance.addPostFrameCallback((_) {
      _scrollToBottomPending = false;
      if (!_scroll.hasClients) {
        return;
      }
      final ScrollPosition position = _scroll.position;
      final double distanceFromBottom =
          position.maxScrollExtent - position.pixels;
      if (!force && (!animate || distanceFromBottom > 180)) {
        return;
      }
      if (!animate) {
        _scroll.jumpTo(position.maxScrollExtent);
        return;
      }
      _scroll.animateTo(
        position.maxScrollExtent + 80,
        duration: const Duration(milliseconds: 180),
        curve: Curves.easeOutCubic,
      );
    });
  }

  String _nextLocalId(String prefix) {
    _localMessageSeq = (_localMessageSeq + 1) % 1000000;
    final int micros = DateTime.now().microsecondsSinceEpoch;
    return '${prefix}_${micros}_$_localMessageSeq';
  }

  Future<void> _openExternal(String url) async {
    if (url.trim().isEmpty) {
      return;
    }
    final Uri uri = Uri.parse(url);
    await launchUrl(uri, mode: LaunchMode.externalApplication);
  }

  Future<void> _runDailyThreadSummary({required bool jeeNotes}) async {
    if (_aiBusy) {
      return;
    }
    final DateTime now = DateTime.now();
    final List<PeerMessage> candidates = _messages.where((PeerMessage m) {
      if (_isControlMessageType(m.type)) {
        return false;
      }
      final DateTime d = DateTime.fromMillisecondsSinceEpoch(m.timeMillis);
      return d.year == now.year && d.month == now.month && d.day == now.day;
    }).toList();

    if (candidates.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('No messages today to summarize yet.')),
      );
      return;
    }

    final String transcript = candidates.map(_messageToAiLine).join('\n');
    final String prompt = jeeNotes
        ? '''
You are LalaCore AI.
Create high-quality JEE revision notes from today's chat transcript.
Rules:
- Preserve who shared what when relevant.
- If any part is non-English, understand it and include clean English notes.
- Keep formulae in LaTeX where helpful.
- Format in Markdown with sections:
1) Core Concepts
2) Important Formulae
3) Traps/Mistakes to Avoid
4) Quick Revision Checklist
5) Action Items

Transcript:
$transcript
'''
        : '''
You are LalaCore AI.
Summarize today's chat transcript with clear participant attribution.
Rules:
- Mention who said what in concise form.
- Understand mixed/other languages and summarize in English.
- Preserve important equations in LaTeX.
- Format in Markdown with sections:
1) Quick Summary
2) Participant Highlights
3) Key Decisions / Tasks
4) Important Academic Points

Transcript:
$transcript
''';

    setState(() => _aiBusy = true);
    try {
      final Map<String, dynamic> response = await _aiService.sendChat(
        prompt: prompt,
        userId: widget.myUserId,
        chatId:
            'ai_thread_${widget.thread.threadId}_${jeeNotes ? 'jee' : 'summary'}_${DateTime.now().millisecondsSinceEpoch}',
        function: 'general_chat',
        responseStyle: 'exam_coach',
        enablePersona: false,
        card: <String, dynamic>{
          'surface': 'chat_thread_summary',
          'thread_id': widget.thread.threadId,
          'thread_name': widget.thread.title,
          'is_group': widget.thread.isGroup,
          'requested_mode': jeeNotes ? 'jee_notes' : 'summary',
          'message_count': candidates.length,
        },
      );

      final String content = _composeAiContent(
        response,
        title: jeeNotes ? "Today's JEE Notes" : "Today's Summary",
      );

      if (!mounted) {
        return;
      }

      await Navigator.push(
        context,
        MaterialPageRoute(
          builder: (_) => ChatAIReaderScreen(
            title: jeeNotes ? "Today's JEE Notes" : "Today's Summary",
            subtitle: widget.thread.title,
            content: content,
          ),
        ),
      );
    } catch (e) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('AI summary failed: $e')));
    } finally {
      if (mounted) {
        setState(() => _aiBusy = false);
      }
    }
  }

  String _messageToAiLine(PeerMessage message) {
    final String sender = message.senderName.trim().isEmpty
        ? message.senderId
        : message.senderName;
    final String time = _clock(message.timeMillis);
    final String text = switch (message.type) {
      'image' =>
        '[Image] ${message.meta?['name'] ?? message.text} ${(message.meta?['url'] ?? '').toString()}',
      'gif' =>
        '[GIF] ${message.meta?['name'] ?? message.text} ${(message.meta?['url'] ?? '').toString()}',
      'pdf' =>
        '[PDF] ${message.meta?['name'] ?? message.text} ${(message.meta?['url'] ?? '').toString()}',
      'audio' =>
        '[Voice note] ${(message.meta?['name'] ?? message.text).toString()} ${(message.meta?['url'] ?? '').toString()}',
      'answer_key_card' =>
        '[AnswerKeyCard] ${message.meta?['question_text'] ?? message.text}',
      'poll' =>
        '[Poll] ${message.meta?['question'] ?? message.text} options=${(message.meta?['options'] ?? <dynamic>[]).toString()}',
      _ => message.text,
    };
    return '[$time] $sender: $text';
  }

  Future<void> _runAiForMessage({
    required PeerMessage message,
    required _AiMessageAction action,
  }) async {
    if (_aiBusy) {
      return;
    }

    final String url = (message.meta?['url'] ?? '').toString();
    final String name = (message.meta?['name'] ?? message.text).toString();
    final bool requiresAsset = true;

    if (requiresAsset &&
        (message.type == 'image' ||
            message.type == 'pdf' ||
            message.type == 'audio') &&
        url.trim().isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('File URL is not ready yet. Try again.')),
      );
      return;
    }

    final String actionTitle = switch (action) {
      _AiMessageAction.aiSummarize => 'AI Summary',
      _AiMessageAction.aiJeeNotes => 'JEE Notes',
      _AiMessageAction.aiTranscribe => 'Voice Transcription',
      _ => 'AI Result',
    };

    final String prompt = switch (action) {
      _AiMessageAction.aiSummarize =>
        '''
You are LalaCore AI.
Summarize this chat attachment in clean study-friendly format.
Attachment details:
- type: ${message.type}
- sender: ${message.senderName}
- file_name: $name
- file_url: $url
- original_caption: ${message.text}
Instructions:
- Understand any non-English content and summarize in English.
- Preserve formulas in LaTeX.
- Output Markdown with:
1) What it contains
2) Key points
3) Exam-relevant takeaways
4) Next revision actions
''',
      _AiMessageAction.aiJeeNotes =>
        '''
You are LalaCore AI.
Create high-quality JEE revision notes from this chat attachment.
Attachment details:
- type: ${message.type}
- sender: ${message.senderName}
- file_name: $name
- file_url: $url
- original_caption: ${message.text}
Instructions:
- Handle multilingual source and produce final notes in English.
- Include equations/formulae in LaTeX.
- Output Markdown sections:
1) Core Concepts
2) Formula Sheet
3) Common Traps
4) Fast Revision Bullets
''',
      _AiMessageAction.aiTranscribe =>
        '''
You are LalaCore AI.
Transcribe this voice note and convert it into useful study output.
Voice note details:
- sender: ${message.senderName}
- file_name: $name
- file_url: $url
Instructions:
- Provide exact transcript first.
- If language is not English, include translated English transcript.
- Then provide concise summary and action points.
- Output Markdown with headings.
''',
      _ => '',
    };

    if (prompt.trim().isEmpty) {
      return;
    }

    setState(() => _aiBusy = true);
    try {
      final Map<String, dynamic> response = await _aiService.sendChat(
        prompt: prompt,
        userId: widget.myUserId,
        chatId:
            'ai_asset_${widget.thread.threadId}_${message.id}_${action.name}',
        function: 'general_chat',
        responseStyle: 'exam_coach',
        enablePersona: false,
        card: <String, dynamic>{
          'surface': 'chat_attachment_ai',
          'thread_id': widget.thread.threadId,
          'message_id': message.id,
          'message_type': message.type,
          'asset_url': url,
          'asset_name': name,
          'sender': message.senderName,
          'requested_action': action.name,
        },
      );

      if (!mounted) {
        return;
      }

      final String content = _composeAiContent(response, title: actionTitle);
      await Navigator.push(
        context,
        MaterialPageRoute(
          builder: (_) => ChatAIReaderScreen(
            title: actionTitle,
            subtitle: widget.thread.title,
            content: content,
          ),
        ),
      );
    } catch (e) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('AI action failed: $e')));
    } finally {
      if (mounted) {
        setState(() => _aiBusy = false);
      }
    }
  }

  String _composeAiContent(
    Map<String, dynamic> response, {
    required String title,
  }) {
    final String answer = (response['answer'] ?? '').toString().trim();
    final String explanation = (response['explanation'] ?? '')
        .toString()
        .trim();
    final String confidence = (response['confidence'] ?? '').toString().trim();
    final String concept = (response['concept'] ?? '').toString().trim();
    final List<String> sections = <String>[
      if (answer.isNotEmpty) answer,
      if (explanation.isNotEmpty) explanation,
      if (concept.isNotEmpty) '**Concept:** $concept',
      if (confidence.isNotEmpty) '**Confidence:** $confidence',
    ];
    if (sections.isEmpty) {
      return '**$title**\n\nNo AI content returned.';
    }
    return sections.join('\n\n');
  }

  List<String> _toStringList(dynamic raw) {
    if (raw is List) {
      return raw
          .map((dynamic e) => e.toString().trim())
          .where((String e) => e.isNotEmpty)
          .toList();
    }
    if (raw is String && raw.trim().isNotEmpty) {
      return raw
          .split(',')
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty)
          .toList();
    }
    return <String>[];
  }

  Future<void> _openGroupFromInvite(PeerMessage inviteMessage) async {
    final String groupId = (inviteMessage.meta?['group_id'] ?? '').toString();
    final String groupName = (inviteMessage.meta?['group_name'] ?? 'Group Chat')
        .toString();
    if (groupId.trim().isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Group details are missing in invite.')),
      );
      return;
    }

    final List<String> participants = _toStringList(
      inviteMessage.meta?['participants'],
    );
    if (!participants.contains(widget.myUserId)) {
      participants.add(widget.myUserId);
    }
    final List<String> admins = _toStringList(inviteMessage.meta?['admins']);
    if (admins.isEmpty) {
      admins.add(inviteMessage.senderId);
    }

    final ChatThreadSummary groupThread = ChatThreadSummary(
      threadId: groupId,
      title: groupName,
      peerId: groupId,
      peerName: groupName,
      lastMessage: 'Open group chat',
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
        'creator_id': inviteMessage.senderId,
      },
    );

    try {
      await widget.chatService.sendMessagePayload(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        type: 'group_invite_response',
        text: '${widget.myName} accepted invite for "$groupName".',
        meta: <String, dynamic>{
          'group_id': groupId,
          'group_name': groupName,
          'accepted': true,
          'admins': admins,
        },
      );
      await widget.chatService.sendMessagePayload(
        thread: groupThread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        type: 'system_group_join',
        text: '${widget.myName} joined the group.',
      );
    } catch (_) {}

    if (!mounted) {
      return;
    }
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => ChatThreadScreen(
          thread: groupThread,
          myUserId: widget.myUserId,
          myName: widget.myName,
          role: widget.role,
          chatService: widget.chatService,
        ),
      ),
    );
    await _load(quiet: true);
  }

  void _triggerWhoosh(String messageId) {
    Future<void>.delayed(const Duration(milliseconds: 28), () {
      if (!mounted) {
        return;
      }
      setState(() => _whooshIds.remove(messageId));
    });
  }

  Map<String, _PollCloseState> _pollCloseStateMap(
    List<PeerMessage> allMessages,
  ) {
    final Map<String, _PollCloseState> out = <String, _PollCloseState>{};
    for (final PeerMessage m in allMessages) {
      if (m.type != 'poll_close' || m.failed) {
        continue;
      }
      final String pollId = (m.meta?['poll_id'] ?? '').toString();
      if (pollId.isEmpty) {
        continue;
      }
      final int closedAt =
          int.tryParse('${m.meta?['closed_at'] ?? ''}') ?? m.timeMillis;
      final Map<int, int> finalResults = _parsePollResultMap(
        m.meta?['results'],
      );
      final int winnerIndex =
          int.tryParse('${m.meta?['winner_index'] ?? ''}') ?? -1;
      final int winnerVotes =
          int.tryParse('${m.meta?['winner_votes'] ?? ''}') ?? 0;
      final int totalVotes =
          int.tryParse('${m.meta?['total_votes'] ?? ''}') ??
          finalResults.values.fold(0, (int a, int b) => a + b);
      final _PollCloseState state = _PollCloseState(
        pollId: pollId,
        closedAtMillis: closedAt,
        closedBy: m.senderName.trim().isEmpty ? m.senderId : m.senderName,
        winnerIndex: winnerIndex,
        winnerVotes: winnerVotes,
        totalVotes: totalVotes,
        finalResults: finalResults,
      );
      final _PollCloseState? old = out[pollId];
      if (old == null || state.closedAtMillis >= old.closedAtMillis) {
        out[pollId] = state;
      }
    }
    return out;
  }

  Map<int, int> _parsePollResultMap(dynamic raw) {
    final Map<int, int> out = <int, int>{};
    if (raw is Map) {
      raw.forEach((dynamic key, dynamic value) {
        final int? idx = int.tryParse(key.toString());
        if (idx == null || idx < 0) {
          return;
        }
        final int count = int.tryParse(value.toString()) ?? 0;
        if (count > 0) {
          out[idx] = count;
        }
      });
    }
    return out;
  }

  Map<String, Map<String, int>> _pollChoicesByUser(
    List<PeerMessage> allMessages, {
    required Map<String, _PollCloseState> closeStates,
  }) {
    final Map<String, Map<String, int>> out = <String, Map<String, int>>{};
    for (final PeerMessage m in allMessages) {
      if (m.type != 'poll_vote' || m.failed) {
        continue;
      }
      final String pollId = (m.meta?['poll_id'] ?? '').toString();
      final int idx = int.tryParse('${m.meta?['option_index'] ?? ''}') ?? -1;
      if (pollId.isEmpty || idx < 0) {
        continue;
      }
      final _PollCloseState? closed = closeStates[pollId];
      if (closed != null && m.timeMillis > closed.closedAtMillis) {
        continue;
      }
      if (m.senderId.trim().isEmpty) {
        continue;
      }
      final Map<String, int> byUser = out.putIfAbsent(
        pollId,
        () => <String, int>{},
      );
      byUser[m.senderId] = idx;
    }
    return out;
  }

  Map<String, Map<int, int>> _pollCountsFromUserChoices(
    Map<String, Map<String, int>> votesByUser,
  ) {
    final Map<String, Map<int, int>> out = <String, Map<int, int>>{};
    votesByUser.forEach((String pollId, Map<String, int> byUser) {
      final Map<int, int> counts = <int, int>{};
      byUser.forEach((String _, int optionIndex) {
        counts[optionIndex] = (counts[optionIndex] ?? 0) + 1;
      });
      out[pollId] = counts;
    });
    return out;
  }

  Map<String, int> _myVotesFromUserChoices(
    Map<String, Map<String, int>> votesByUser,
  ) {
    final Map<String, int> out = <String, int>{};
    votesByUser.forEach((String pollId, Map<String, int> byUser) {
      final int? myChoice = byUser[widget.myUserId];
      if (myChoice != null && myChoice >= 0) {
        out[pollId] = myChoice;
      }
    });
    return out;
  }

  List<String> _groupMemberIdsFromContext() {
    final Set<String> ids = <String>{};
    ids.addAll(
      widget.thread.participants
          .map((String e) => e.trim())
          .where((String e) => e.isNotEmpty),
    );
    ids.addAll(_toStringList(widget.thread.rawPayload?['participants']));
    ids.add(widget.myUserId);

    for (final PeerMessage message in _messages) {
      final String sender = message.senderId.trim();
      if (sender.isNotEmpty) {
        ids.add(sender);
      }
      ids.addAll(_toStringList(message.meta?['participants']));
      final String invitedId = (message.meta?['invited_user_id'] ?? '')
          .toString()
          .trim();
      if (invitedId.isNotEmpty) {
        ids.add(invitedId);
      }
    }

    final List<String> out = ids.toList();
    out.sort();
    return out;
  }

  List<String> _groupAdminIdsFromContext() {
    final Set<String> admins = <String>{};
    admins.addAll(_toStringList(widget.thread.rawPayload?['admins']));

    for (final PeerMessage message in _messages) {
      if (message.type == 'group_admin_update') {
        final List<String> payloadAdmins = _toStringList(
          message.meta?['admins'],
        );
        if (payloadAdmins.isNotEmpty) {
          admins
            ..clear()
            ..addAll(payloadAdmins);
        }
      }
      admins.addAll(_toStringList(message.meta?['admins']));
    }

    if (admins.isEmpty && widget.thread.rawPayload != null) {
      final String creator = (widget.thread.rawPayload!['creator_id'] ?? '')
          .toString()
          .trim();
      if (creator.isNotEmpty) {
        admins.add(creator);
      }
    }
    if (admins.isEmpty) {
      admins.add(widget.myUserId);
    }
    final List<String> out = admins
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .toSet()
        .toList();
    out.sort();
    return out;
  }

  bool _isCurrentUserGroupAdmin() {
    if (!widget.thread.isGroup) {
      return false;
    }
    return _groupAdminIdsFromContext().contains(widget.myUserId);
  }

  Future<void> _addStudentsToGroup(List<ChatUser> students) async {
    if (!_isCurrentUserGroupAdmin()) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Only group admins can add students.')),
        );
      }
      return;
    }
    final List<ChatUser> toInvite = students
        .where((ChatUser e) => e.userId.trim().isNotEmpty)
        .where((ChatUser e) => e.userId != widget.myUserId)
        .where((ChatUser e) => e.role.toLowerCase() == 'student')
        .toList();
    if (toInvite.isEmpty) {
      return;
    }

    final Set<String> participants = _groupMemberIdsFromContext().toSet();
    final List<String> admins = _groupAdminIdsFromContext();
    int invitedCount = 0;
    final List<String> invitedIds = <String>[];
    final List<String> invitedNames = <String>[];
    for (final ChatUser student in toInvite) {
      if (participants.contains(student.userId)) {
        continue;
      }
      participants.add(student.userId);
      final List<String> participantList = participants.toList()..sort();
      final ChatThreadSummary direct = ChatThreadSummary(
        threadId: '',
        title: student.name,
        peerId: student.userId,
        peerName: student.name,
        lastMessage: '',
        updatedAtMillis: DateTime.now().millisecondsSinceEpoch,
        unread: false,
        isDoubtThread: false,
        participants: <String>[widget.myUserId, student.userId],
      );

      try {
        await widget.chatService.sendMessagePayload(
          thread: direct,
          myUserId: widget.myUserId,
          myName: widget.myName,
          type: 'group_invite',
          text: 'Invitation to join "${widget.thread.title}"',
          meta: <String, dynamic>{
            'group_id': widget.thread.threadId,
            'group_name': widget.thread.title,
            'participants': participantList,
            'admins': admins,
            'invited_user_id': student.userId,
            'invited_user_name': student.name,
          },
        );
        invitedCount += 1;
        invitedIds.add(student.userId);
        invitedNames.add(student.name);
      } catch (_) {}
    }

    if (invitedCount > 0) {
      try {
        await widget.chatService.sendMessagePayload(
          thread: widget.thread,
          myUserId: widget.myUserId,
          myName: widget.myName,
          type: 'system_group_invite',
          text:
              '${widget.myName} invited ${invitedNames.join(', ')} to the group.',
          meta: <String, dynamic>{
            'participants': participants.toList()..sort(),
            'admins': admins,
            'invited_members': invitedIds,
          },
        );
      } catch (_) {}
    }

    if (!mounted) {
      return;
    }
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(
          invitedCount > 0
              ? 'Sent $invitedCount invite${invitedCount == 1 ? '' : 's'}.'
              : 'No new students were invited.',
        ),
      ),
    );
    await _load(quiet: true);
  }

  Future<void> _setGroupAdminStatus({
    required ChatUser member,
    required bool makeAdmin,
  }) async {
    if (!_isCurrentUserGroupAdmin()) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Only admins can change admin roles.')),
        );
      }
      return;
    }
    if (member.userId == widget.myUserId && !makeAdmin) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('At least one admin must remain.')),
        );
      }
      return;
    }
    final Set<String> admins = _groupAdminIdsFromContext().toSet();
    if (makeAdmin) {
      admins.add(member.userId);
    } else {
      admins.remove(member.userId);
    }
    if (admins.isEmpty) {
      admins.add(widget.myUserId);
    }
    final List<String> adminList = admins.toList()..sort();
    try {
      await widget.chatService.sendMessagePayload(
        thread: widget.thread,
        myUserId: widget.myUserId,
        myName: widget.myName,
        type: 'group_admin_update',
        text: makeAdmin
            ? '${widget.myName} made ${member.name} an admin.'
            : '${widget.myName} removed admin rights from ${member.name}.',
        meta: <String, dynamic>{
          'admins': adminList,
          'target_user_id': member.userId,
          'target_user_name': member.name,
          'make_admin': makeAdmin,
        },
      );
      await _load(quiet: true);
    } catch (_) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Failed to update admin status.')),
        );
      }
    }
  }

  Future<void> _openGroupInfo() async {
    if (!widget.thread.isGroup) {
      return;
    }
    final List<String> members = _groupMemberIdsFromContext();
    final List<String> admins = _groupAdminIdsFromContext();
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => GroupInfoScreen(
          groupThread: widget.thread,
          myUserId: widget.myUserId,
          myName: widget.myName,
          role: widget.role,
          chatService: widget.chatService,
          initialMemberIds: members,
          initialAdminIds: admins,
          onAddStudents: _addStudentsToGroup,
          onSetAdminStatus: _setGroupAdminStatus,
        ),
      ),
    );
    await _load(quiet: true);
  }

  GlobalKey _keyForMessage(String messageId) {
    return _messageKeys.putIfAbsent(
      messageId,
      () => GlobalKey(debugLabel: 'msg_$messageId'),
    );
  }

  void _openThreadSearch() {
    if (_searchMode) {
      return;
    }
    setState(() {
      _searchMode = true;
    });
  }

  void _closeThreadSearch() {
    FocusManager.instance.primaryFocus?.unfocus();
    setState(() {
      _searchMode = false;
      _threadSearchQuery = '';
      _searchSelection = 0;
      _highlightMessageId = null;
      _threadSearchCtrl.clear();
    });
  }

  void _onThreadSearchChanged(String value) {
    setState(() {
      _threadSearchQuery = value;
      _searchSelection = 0;
    });
    _jumpToSearchHit(selection: 0);
  }

  void _stepThreadSearch(int delta) {
    final List<PeerMessage> renderable = _messages
        .where((PeerMessage m) => !_isControlMessageType(m.type))
        .toList();
    final List<int> hits = _searchHitIndexes(renderable, _threadSearchQuery);
    if (hits.isEmpty) {
      return;
    }
    int next = _searchSelection + delta;
    if (next < 0) {
      next = hits.length - 1;
    } else if (next >= hits.length) {
      next = 0;
    }
    _jumpToSearchHit(renderable: renderable, hits: hits, selection: next);
  }

  void _jumpToSearchHit({
    List<PeerMessage>? renderable,
    List<int>? hits,
    int? selection,
  }) {
    final List<PeerMessage> messages =
        renderable ??
        _messages
            .where((PeerMessage m) => !_isControlMessageType(m.type))
            .toList();
    final List<int> matches =
        hits ?? _searchHitIndexes(messages, _threadSearchQuery);
    if (matches.isEmpty) {
      if (mounted) {
        setState(() => _highlightMessageId = null);
      }
      return;
    }
    int idx = selection ?? _searchSelection;
    if (idx < 0) {
      idx = 0;
    }
    if (idx >= matches.length) {
      idx = matches.length - 1;
    }
    final String targetMessageId = messages[matches[idx]].id;
    if (mounted) {
      setState(() {
        _searchSelection = idx;
        _highlightMessageId = targetMessageId;
      });
    }
    WidgetsBinding.instance.addPostFrameCallback((_) {
      final BuildContext? targetCtx = _keyForMessage(
        targetMessageId,
      ).currentContext;
      if (targetCtx == null || !mounted) {
        return;
      }
      Scrollable.ensureVisible(
        targetCtx,
        duration: const Duration(milliseconds: 220),
        curve: Curves.easeOutCubic,
        alignment: 0.28,
      );
    });
  }

  String _normalizeThreadSearch(String input) {
    return input
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9\s]'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  String _messageSearchHaystack(PeerMessage message) {
    final List<String> parts = <String>[
      message.senderName,
      message.senderId,
      message.text,
      message.type,
      if (message.meta != null) ...<String>[
        '${message.meta!['name'] ?? ''}',
        '${message.meta!['question'] ?? ''}',
        '${message.meta!['context'] ?? ''}',
        if (message.meta!['options'] is List)
          (message.meta!['options'] as List)
              .map((dynamic e) => e.toString())
              .join(' '),
      ],
    ];
    return _normalizeThreadSearch(parts.join(' '));
  }

  List<int> _searchHitIndexes(List<PeerMessage> renderable, String queryRaw) {
    final String normalizedQuery = _normalizeThreadSearch(queryRaw);
    if (normalizedQuery.isEmpty) {
      return <int>[];
    }
    final String compactQuery = normalizedQuery.replaceAll(' ', '');
    final List<String> tokens = normalizedQuery
        .split(' ')
        .where((String t) => t.isNotEmpty)
        .toList(growable: false);
    final List<int> out = <int>[];
    for (int i = 0; i < renderable.length; i++) {
      final String hay = _messageSearchHaystack(renderable[i]);
      if (hay.isEmpty) {
        continue;
      }
      final String hayCompact = hay.replaceAll(' ', '');
      final bool tokenMatch =
          tokens.isEmpty ||
          tokens.every((String token) {
            return hay.contains(token);
          });
      final bool matches =
          hay.contains(normalizedQuery) ||
          (compactQuery.isNotEmpty && hayCompact.contains(compactQuery)) ||
          tokenMatch;
      if (matches) {
        out.add(i);
      }
    }
    return out;
  }

  Map<String, Map<String, int>> _readersByOwnMessage({
    required List<PeerMessage> allMessages,
    required List<PeerMessage> renderableMessages,
  }) {
    final Map<String, int> renderIndexById = <String, int>{};
    for (int i = 0; i < renderableMessages.length; i++) {
      renderIndexById[renderableMessages[i].id] = i;
    }

    final Map<String, Map<String, int>> out = <String, Map<String, int>>{};
    for (final PeerMessage message in allMessages) {
      if (message.type != 'read_receipt' || message.failed || message.pending) {
        continue;
      }
      final String targetId = (message.meta?['message_id'] ?? '').toString();
      if (targetId.isEmpty) {
        continue;
      }
      final String readerId = (message.meta?['reader_id'] ?? message.senderId)
          .toString()
          .trim();
      if (readerId.isEmpty || readerId == widget.myUserId) {
        continue;
      }
      final int targetIndex = renderIndexById[targetId] ?? -1;
      if (targetIndex < 0) {
        continue;
      }
      final int seenAt =
          int.tryParse('${message.meta?['seen_at'] ?? ''}') ??
          message.timeMillis;

      // Reading a later message implies earlier messages are also seen.
      for (int i = 0; i <= targetIndex; i++) {
        final PeerMessage candidate = renderableMessages[i];
        if (candidate.senderId != widget.myUserId) {
          continue;
        }
        final Map<String, int> readers = out.putIfAbsent(
          candidate.id,
          () => <String, int>{},
        );
        final int old = readers[readerId] ?? 0;
        if (seenAt > old) {
          readers[readerId] = seenAt;
        }
      }
    }
    return out;
  }

  Set<String> _recipientIdsForDelivery() {
    if (widget.thread.isGroup) {
      final Set<String> members = _groupMemberIdsFromContext().toSet();
      members.remove(widget.myUserId);
      return members;
    }
    final Set<String> out = <String>{};
    final String peerId = widget.thread.peerId.trim();
    if (peerId.isNotEmpty && peerId != widget.myUserId) {
      out.add(peerId);
    } else {
      out.addAll(
        widget.thread.participants
            .map((String e) => e.trim())
            .where((String e) => e.isNotEmpty && e != widget.myUserId),
      );
    }
    return out;
  }

  Map<String, String> _memberNames(List<PeerMessage> messages) {
    final Map<String, String> out = <String, String>{
      widget.myUserId: widget.myName,
    };
    if (widget.thread.peerId.trim().isNotEmpty &&
        widget.thread.peerName.trim().isNotEmpty) {
      out[widget.thread.peerId.trim()] = widget.thread.peerName.trim();
    }
    for (final PeerMessage message in messages) {
      final String id = message.senderId.trim();
      final String name = message.senderName.trim();
      if (id.isEmpty || name.isEmpty) {
        continue;
      }
      out[id] = name;
    }
    return out;
  }

  Map<String, int> _latestActivityBySender(List<PeerMessage> messages) {
    final Map<String, int> out = <String, int>{};
    for (final PeerMessage message in messages) {
      if (message.pending || message.failed) {
        continue;
      }
      final String sender = message.senderId.trim();
      if (sender.isEmpty || sender == widget.myUserId) {
        continue;
      }
      final int old = out[sender] ?? 0;
      if (message.timeMillis > old) {
        out[sender] = message.timeMillis;
      }
    }
    return out;
  }

  _DeliverySnapshot _deliverySnapshot({
    required PeerMessage message,
    required Set<String> recipientIds,
    required Map<String, Map<String, int>> readersByMessage,
    required Map<String, int> latestSenderActivity,
  }) {
    if (message.pending || message.failed) {
      return const _DeliverySnapshot(
        state: _DeliveryState.sent,
        recipientCount: 0,
        deliveredCount: 0,
        readCount: 0,
        readUserIds: <String>[],
      );
    }
    int deliveredCount = 0;
    int readCount = 0;
    final List<String> readUserIds = <String>[];
    final Map<String, int> readers =
        readersByMessage[message.id] ?? const <String, int>{};
    for (final String recipient in recipientIds) {
      final int seenAt = readers[recipient] ?? 0;
      if (seenAt > 0) {
        deliveredCount += 1;
        readCount += 1;
        readUserIds.add(recipient);
        continue;
      }
      final int activityAt = latestSenderActivity[recipient] ?? 0;
      if (activityAt >= message.timeMillis) {
        deliveredCount += 1;
      }
    }
    final _DeliveryState state = readCount > 0
        ? _DeliveryState.read
        : (deliveredCount > 0 ? _DeliveryState.delivered : _DeliveryState.sent);
    return _DeliverySnapshot(
      state: state,
      recipientCount: recipientIds.length,
      deliveredCount: deliveredCount,
      readCount: readCount,
      readUserIds: readUserIds,
    );
  }

  String _groupReadLabel(
    _DeliverySnapshot snapshot,
    Map<String, String> memberNames,
  ) {
    if (snapshot.readUserIds.isEmpty) {
      return '';
    }
    final List<String> names = snapshot.readUserIds
        .map((String id) => memberNames[id] ?? id)
        .toList();
    if (names.length <= 2) {
      return 'Seen by ${names.join(', ')}';
    }
    return 'Seen by ${names.take(2).join(', ')} +${names.length - 2}';
  }

  Widget _deliveryIndicator({
    required _DeliverySnapshot snapshot,
    required bool isDark,
    required bool isMineBubble,
  }) {
    final Color base = isMineBubble
        ? Colors.white.withOpacity(0.82)
        : (isDark ? const Color(0xFFEAF2FF) : Colors.black.withOpacity(0.68));
    final Color iconColor = snapshot.state == _DeliveryState.read
        ? const Color(0xFF76D6FF)
        : base;
    if (snapshot.state == _DeliveryState.sent) {
      return Icon(Icons.done, size: 14, color: iconColor);
    }
    final String groupCount = snapshot.recipientCount > 1
        ? '${snapshot.readCount > 0 ? snapshot.readCount : snapshot.deliveredCount}/${snapshot.recipientCount}'
        : '';
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: <Widget>[
        Icon(Icons.done_all, size: 14, color: iconColor),
        if (groupCount.isNotEmpty) ...<Widget>[
          const SizedBox(width: 2),
          Text(
            groupCount,
            style: TextStyle(
              fontSize: 10,
              color: iconColor,
              fontWeight: FontWeight.w700,
            ),
          ),
        ],
      ],
    );
  }

  @override
  Widget build(BuildContext context) {
    final bool isDark = Theme.of(context).brightness == Brightness.dark;

    final Map<String, _PollCloseState> pollCloseStates = _pollCloseStateMap(
      _messages,
    );
    final Map<String, Map<String, int>> pollVotesByUser = _pollChoicesByUser(
      _messages,
      closeStates: pollCloseStates,
    );
    final Map<String, Map<int, int>> pollCounts = _pollCountsFromUserChoices(
      pollVotesByUser,
    );
    final Map<String, int> myVotes = _myVotesFromUserChoices(pollVotesByUser);
    final MediaQueryData media = MediaQuery.of(context);
    final bool allowWhoosh =
        !(media.disableAnimations) && media.size.shortestSide > 350;
    final List<PeerMessage> renderable = _messages.where((PeerMessage m) {
      return !_isControlMessageType(m.type);
    }).toList();
    final Set<String> recipientIds = _recipientIdsForDelivery();
    final Map<String, String> memberNames = _memberNames(_messages);
    final Map<String, int> latestSenderActivity = _latestActivityBySender(
      renderable,
    );
    final Map<String, Map<String, int>> readersByMessage = _readersByOwnMessage(
      allMessages: _messages,
      renderableMessages: renderable,
    );
    final List<int> searchHits = _searchHitIndexes(
      renderable,
      _threadSearchQuery,
    );
    final int activeSearchPosition = searchHits.isEmpty
        ? -1
        : (_searchSelection < 0
              ? 0
              : (_searchSelection >= searchHits.length
                    ? searchHits.length - 1
                    : _searchSelection));

    return Scaffold(
      appBar: AppBar(
        automaticallyImplyLeading: !_searchMode,
        leading: _searchMode
            ? IconButton(
                onPressed: _closeThreadSearch,
                icon: const Icon(Icons.arrow_back_rounded),
              )
            : null,
        title: _searchMode
            ? TextField(
                controller: _threadSearchCtrl,
                autofocus: true,
                onChanged: _onThreadSearchChanged,
                decoration: const InputDecoration(
                  hintText: 'Search in chat',
                  border: InputBorder.none,
                  isDense: true,
                ),
              )
            : GestureDetector(
                onTap: widget.thread.isGroup ? _openGroupInfo : null,
                child: Row(
                  mainAxisSize: MainAxisSize.min,
                  children: <Widget>[
                    Text(widget.thread.title, overflow: TextOverflow.ellipsis),
                    if (widget.thread.isGroup) ...<Widget>[
                      const SizedBox(width: 6),
                      Icon(
                        Icons.info_outline_rounded,
                        size: 18,
                        color: Theme.of(context).colorScheme.primary,
                      ),
                    ],
                  ],
                ),
              ),
        actions: _searchMode
            ? <Widget>[
                Center(
                  child: Text(
                    searchHits.isEmpty
                        ? '0/0'
                        : '${activeSearchPosition + 1}/${searchHits.length}',
                    style: TextStyle(
                      fontSize: 12,
                      color: isDark
                          ? const Color(0xFFCCD8EA)
                          : Colors.grey.shade700,
                    ),
                  ),
                ),
                IconButton(
                  onPressed: searchHits.isEmpty
                      ? null
                      : () => _stepThreadSearch(-1),
                  icon: const Icon(Icons.keyboard_arrow_up_rounded),
                ),
                IconButton(
                  onPressed: searchHits.isEmpty
                      ? null
                      : () => _stepThreadSearch(1),
                  icon: const Icon(Icons.keyboard_arrow_down_rounded),
                ),
                IconButton(
                  onPressed: _closeThreadSearch,
                  icon: const Icon(Icons.close_rounded),
                ),
              ]
            : <Widget>[
                IconButton(
                  tooltip: 'Search in chat',
                  onPressed: _openThreadSearch,
                  icon: const Icon(Icons.search_rounded),
                ),
                IconButton(
                  tooltip: "AI summarize today's chat",
                  onPressed: _aiBusy
                      ? null
                      : () => _runDailyThreadSummary(jeeNotes: false),
                  icon: _aiBusy
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.auto_awesome_rounded),
                ),
              ],
      ),
      body: Column(
        children: <Widget>[
          if (_loading)
            LinearProgressIndicator(
              minHeight: 1.6,
              color: AppColors.primaryTone(context),
              backgroundColor: isDark
                  ? Colors.white.withOpacity(0.08)
                  : Colors.black.withOpacity(0.06),
            )
          else
            const SizedBox(height: 1.6),
          Expanded(
            child: ListView.builder(
              controller: _scroll,
              padding: const EdgeInsets.fromLTRB(12, 16, 12, 16),
              itemCount: renderable.length,
              itemBuilder: (_, int index) {
                final PeerMessage m = renderable[index];
                final bool isMine = m.senderId == widget.myUserId;
                final String day = _dayLabel(m.timeMillis);

                bool showDay = false;
                if (index == 0) {
                  showDay = true;
                } else {
                  final PeerMessage prev = renderable[index - 1];
                  showDay = _dayLabel(prev.timeMillis) != day;
                }

                final bool highlighted =
                    _searchMode &&
                    _threadSearchQuery.trim().isNotEmpty &&
                    _highlightMessageId == m.id;
                final _DeliverySnapshot delivery = isMine
                    ? _deliverySnapshot(
                        message: m,
                        recipientIds: recipientIds,
                        readersByMessage: readersByMessage,
                        latestSenderActivity: latestSenderActivity,
                      )
                    : const _DeliverySnapshot(
                        state: _DeliveryState.sent,
                        recipientCount: 0,
                        deliveredCount: 0,
                        readCount: 0,
                        readUserIds: <String>[],
                      );
                final String groupSeenLabel =
                    isMine && widget.thread.isGroup && delivery.readCount > 0
                    ? _groupReadLabel(delivery, memberNames)
                    : '';

                return Container(
                  key: _keyForMessage(m.id),
                  child: Column(
                    children: <Widget>[
                      if (showDay)
                        Padding(
                          padding: const EdgeInsets.only(bottom: 10, top: 2),
                          child: Text(
                            day,
                            style: TextStyle(
                              fontSize: 12,
                              color: isDark
                                  ? const Color(0xFF9FB0C7)
                                  : Colors.grey,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                        ),
                      Align(
                        alignment: isMine
                            ? Alignment.centerRight
                            : Alignment.centerLeft,
                        child: Padding(
                          padding: const EdgeInsets.only(bottom: 8),
                          child: GestureDetector(
                            onLongPress: () => _showMessageActions(m),
                            child: AnimatedSlide(
                              duration: const Duration(milliseconds: 330),
                              curve: Curves.easeOutCubic,
                              offset:
                                  isMine &&
                                      allowWhoosh &&
                                      _whooshIds.contains(m.id)
                                  ? const Offset(0.34, 0.0)
                                  : Offset.zero,
                              child: AnimatedOpacity(
                                duration: const Duration(milliseconds: 280),
                                curve: Curves.easeOut,
                                opacity:
                                    isMine &&
                                        allowWhoosh &&
                                        _whooshIds.contains(m.id)
                                    ? 0.24
                                    : 1.0,
                                child: ConstrainedBox(
                                  constraints: const BoxConstraints(
                                    maxWidth: 330,
                                  ),
                                  child: AnimatedContainer(
                                    duration: const Duration(milliseconds: 180),
                                    decoration: BoxDecoration(
                                      borderRadius: BorderRadius.circular(18),
                                      border: highlighted
                                          ? Border.all(
                                              color: AppColors.successTone(
                                                context,
                                              ).withOpacity(0.72),
                                              width: 1.4,
                                            )
                                          : null,
                                    ),
                                    child: LiquidGlass(
                                      solidFill: true,
                                      quality: LiquidGlassQuality.low,
                                      padding: const EdgeInsets.symmetric(
                                        horizontal: 12,
                                        vertical: 10,
                                      ),
                                      color: highlighted
                                          ? AppColors.successTone(
                                              context,
                                            ).withOpacity(isDark ? 0.24 : 0.11)
                                          : (isMine
                                                ? AppColors.primaryTone(
                                                    context,
                                                  ).withOpacity(
                                                    isDark ? 0.80 : 0.88,
                                                  )
                                                : (isDark
                                                      ? AppColors.cardDark
                                                            .withOpacity(0.86)
                                                      : Colors.white
                                                            .withOpacity(
                                                              0.92,
                                                            ))),
                                      child: Column(
                                        crossAxisAlignment:
                                            CrossAxisAlignment.end,
                                        children: <Widget>[
                                          Align(
                                            alignment: Alignment.centerLeft,
                                            child: _messageBody(
                                              message: m,
                                              isMine: isMine,
                                              isDark: isDark,
                                              pollCounts: pollCounts,
                                              myVotes: myVotes,
                                              pollCloseStates: pollCloseStates,
                                            ),
                                          ),
                                          const SizedBox(height: 6),
                                          Row(
                                            mainAxisSize: MainAxisSize.min,
                                            children: <Widget>[
                                              if (m.failed)
                                                InkWell(
                                                  onTap: () => _retryMessage(m),
                                                  child: Row(
                                                    children: <Widget>[
                                                      Icon(
                                                        Icons.refresh,
                                                        size: 14,
                                                        color:
                                                            Colors.red.shade300,
                                                      ),
                                                      const SizedBox(width: 4),
                                                      Text(
                                                        'Retry',
                                                        style: TextStyle(
                                                          fontSize: 10,
                                                          color: Colors
                                                              .red
                                                              .shade300,
                                                        ),
                                                      ),
                                                      const SizedBox(width: 6),
                                                    ],
                                                  ),
                                                ),
                                              if (m.pending)
                                                Padding(
                                                  padding:
                                                      const EdgeInsets.only(
                                                        right: 6,
                                                      ),
                                                  child: Text(
                                                    'Sending...',
                                                    style: TextStyle(
                                                      fontSize: 10,
                                                      color:
                                                          (isMine
                                                                  ? Colors.white
                                                                  : (isDark
                                                                        ? const Color(
                                                                            0xFFEAF2FF,
                                                                          )
                                                                        : Colors
                                                                              .black))
                                                              .withOpacity(
                                                                0.68,
                                                              ),
                                                    ),
                                                  ),
                                                ),
                                              Text(
                                                _clock(m.timeMillis),
                                                style: TextStyle(
                                                  fontSize: 10,
                                                  color:
                                                      (isMine
                                                              ? Colors.white
                                                              : (isDark
                                                                    ? const Color(
                                                                        0xFFEAF2FF,
                                                                      )
                                                                    : Colors
                                                                          .black))
                                                          .withOpacity(0.72),
                                                ),
                                              ),
                                              if (isMine &&
                                                  !m.pending &&
                                                  !m.failed) ...<Widget>[
                                                const SizedBox(width: 4),
                                                _deliveryIndicator(
                                                  snapshot: delivery,
                                                  isDark: isDark,
                                                  isMineBubble: isMine,
                                                ),
                                              ],
                                            ],
                                          ),
                                          if (groupSeenLabel.isNotEmpty)
                                            Padding(
                                              padding: const EdgeInsets.only(
                                                top: 4,
                                              ),
                                              child: Text(
                                                groupSeenLabel,
                                                style: TextStyle(
                                                  fontSize: 10.5,
                                                  color:
                                                      (isMine
                                                              ? Colors.white
                                                              : (isDark
                                                                    ? const Color(
                                                                        0xFFEAF2FF,
                                                                      )
                                                                    : Colors
                                                                          .black))
                                                          .withOpacity(0.72),
                                                  fontWeight: FontWeight.w600,
                                                ),
                                              ),
                                            ),
                                        ],
                                      ),
                                    ),
                                  ),
                                ),
                              ),
                            ),
                          ),
                        ),
                      ),
                    ],
                  ),
                );
              },
            ),
          ),
          SafeArea(
            top: false,
            child: Padding(
              padding: const EdgeInsets.fromLTRB(10, 6, 10, 10),
              child: Row(
                children: <Widget>[
                  IconButton(
                    onPressed: _uploading ? null : _openComposerActions,
                    icon: _uploading
                        ? const SizedBox(
                            width: 16,
                            height: 16,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : const Icon(Icons.add_circle_outline),
                  ),
                  Expanded(
                    child: LiquidGlass(
                      solidFill: true,
                      padding: const EdgeInsets.symmetric(
                        horizontal: 12,
                        vertical: 2,
                      ),
                      child: TextField(
                        controller: _ctrl,
                        minLines: 1,
                        maxLines: 5,
                        decoration: const InputDecoration(
                          hintText: 'Message',
                          border: InputBorder.none,
                        ),
                        onSubmitted: (_) => _sendText(),
                      ),
                    ),
                  ),
                  const SizedBox(width: 8),
                  FloatingActionButton.small(
                    onPressed: _sendText,
                    backgroundColor: AppColors.primaryTone(context),
                    child: const Icon(Icons.send, color: Colors.white),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _messageBody({
    required PeerMessage message,
    required bool isMine,
    required bool isDark,
    required Map<String, Map<int, int>> pollCounts,
    required Map<String, int> myVotes,
    required Map<String, _PollCloseState> pollCloseStates,
  }) {
    final Color textColor = isMine
        ? Colors.white
        : (isDark ? const Color(0xFFEAF2FF) : Colors.black);

    if (message.type == 'deleted' || message.deletedForEveryone) {
      return Text(
        'This message was deleted',
        style: TextStyle(
          color: textColor.withOpacity(0.75),
          fontSize: 14,
          fontStyle: FontStyle.italic,
        ),
      );
    }

    if (message.type == 'answer_key_card') {
      return _InlineAnswerKeyCard(meta: message.meta, textColor: textColor);
    }

    if (message.type == 'image' || message.type == 'gif') {
      final String url = (message.meta?['url'] ?? '').toString();
      return Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          if (url.isNotEmpty)
            ClipRRect(
              borderRadius: BorderRadius.circular(14),
              child: Image.network(
                url,
                width: 230,
                height: 210,
                fit: BoxFit.cover,
                cacheWidth: 460,
                cacheHeight: 420,
                filterQuality: FilterQuality.low,
                errorBuilder: (_, __, ___) => Container(
                  width: 230,
                  height: 160,
                  alignment: Alignment.center,
                  color: Colors.black.withOpacity(0.12),
                  child: Text(
                    'Unable to load image',
                    style: TextStyle(color: textColor),
                  ),
                ),
              ),
            )
          else
            Text(
              message.pending ? 'Uploading image...' : message.text,
              style: TextStyle(color: textColor),
            ),
          if (message.type == 'gif')
            Padding(
              padding: const EdgeInsets.only(top: 6),
              child: Text(
                'GIF',
                style: TextStyle(
                  color: textColor.withOpacity(0.78),
                  fontSize: 12,
                  fontWeight: FontWeight.w700,
                ),
              ),
            ),
        ],
      );
    }

    if (message.type == 'pdf' ||
        message.type == 'file' ||
        message.type == 'audio') {
      final String url = (message.meta?['url'] ?? '').toString();
      final String name = (message.meta?['name'] ?? message.text).toString();
      final IconData icon = message.type == 'audio'
          ? Icons.mic_rounded
          : (message.type == 'pdf'
                ? Icons.picture_as_pdf_rounded
                : Icons.insert_drive_file_rounded);
      final String btn = message.type == 'audio' ? 'Play' : 'Open';

      return Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Icon(icon, color: textColor, size: 20),
          const SizedBox(width: 8),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                Text(
                  name,
                  maxLines: 2,
                  overflow: TextOverflow.ellipsis,
                  style: TextStyle(
                    color: textColor,
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const SizedBox(height: 6),
                TextButton(
                  onPressed: url.isEmpty ? null : () => _openExternal(url),
                  style: TextButton.styleFrom(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 10,
                      vertical: 6,
                    ),
                    minimumSize: Size.zero,
                    tapTargetSize: MaterialTapTargetSize.shrinkWrap,
                    foregroundColor: textColor,
                  ),
                  child: Text(btn),
                ),
              ],
            ),
          ),
        ],
      );
    }

    if (message.type == 'poll') {
      final String pollId = (message.meta?['poll_id'] ?? '').toString();
      final String question = (message.meta?['question'] ?? message.text)
          .toString();
      final String context = (message.meta?['context'] ?? '').toString();
      final List<String> options =
          ((message.meta?['options'] as List?) ?? <dynamic>[])
              .map((dynamic e) => e.toString())
              .where((String e) => e.trim().isNotEmpty)
              .toList();
      final _PollCloseState? closeState = pollCloseStates[pollId];
      final bool isClosed = closeState?.isClosed == true;
      final Map<int, int> counts = isClosed && closeState != null
          ? (closeState.finalResults.isNotEmpty
                ? closeState.finalResults
                : (pollCounts[pollId] ?? <int, int>{}))
          : (pollCounts[pollId] ?? <int, int>{});
      final int myVote = myVotes[pollId] ?? -1;
      final int totalVotes = counts.values.fold(0, (int a, int b) => a + b);
      final bool canFinalize =
          !isClosed && message.senderId == widget.myUserId && pollId.isNotEmpty;

      return Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Row(
            children: <Widget>[
              Icon(
                Icons.poll_rounded,
                color: textColor.withOpacity(0.88),
                size: 16,
              ),
              const SizedBox(width: 6),
              Text(
                'Poll',
                style: TextStyle(
                  color: textColor.withOpacity(0.88),
                  fontWeight: FontWeight.w700,
                  fontSize: 12,
                  letterSpacing: 0.15,
                ),
              ),
              const Spacer(),
              if (isClosed)
                Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 8,
                    vertical: 4,
                  ),
                  decoration: BoxDecoration(
                    color: Colors.green.withOpacity(0.18),
                    borderRadius: BorderRadius.circular(999),
                  ),
                  child: Text(
                    'Finalized',
                    style: TextStyle(
                      color: Colors.green.shade200,
                      fontSize: 11.2,
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                )
              else
                Text(
                  '$totalVotes votes',
                  style: TextStyle(
                    color: textColor.withOpacity(0.72),
                    fontSize: 11.6,
                    fontWeight: FontWeight.w600,
                  ),
                ),
            ],
          ),
          const SizedBox(height: 6),
          Text(
            question,
            style: TextStyle(
              color: textColor,
              fontWeight: FontWeight.w800,
              fontSize: 15,
            ),
          ),
          if (context.trim().isNotEmpty)
            Padding(
              padding: const EdgeInsets.only(top: 4),
              child: Text(
                context,
                style: TextStyle(
                  color: textColor.withOpacity(0.85),
                  fontSize: 13,
                ),
              ),
            ),
          const SizedBox(height: 8),
          ...List<Widget>.generate(options.length, (int i) {
            final bool selected = myVote == i;
            final int voteCount = counts[i] ?? 0;
            final double pct = totalVotes <= 0 ? 0.0 : voteCount / totalVotes;
            final bool winning = isClosed && closeState?.winnerIndex == i;
            return Padding(
              padding: const EdgeInsets.only(bottom: 6),
              child: InkWell(
                onTap: isClosed ? null : () => _votePoll(message, i),
                borderRadius: BorderRadius.circular(10),
                child: Container(
                  width: double.infinity,
                  decoration: BoxDecoration(
                    borderRadius: BorderRadius.circular(10),
                    border: Border.all(
                      color: selected || winning
                          ? Colors.amber.withOpacity(0.88)
                          : textColor.withOpacity(0.20),
                    ),
                    color: (selected || winning)
                        ? Colors.amber.withOpacity(0.14)
                        : Colors.white.withOpacity(0.02),
                  ),
                  child: ClipRRect(
                    borderRadius: BorderRadius.circular(9),
                    child: Stack(
                      children: <Widget>[
                        if (pct > 0)
                          FractionallySizedBox(
                            widthFactor: pct.clamp(0.0, 1.0),
                            child: Container(
                              height: 44,
                              decoration: BoxDecoration(
                                gradient: LinearGradient(
                                  colors: <Color>[
                                    Colors.amber.withOpacity(0.18),
                                    Colors.orange.withOpacity(0.22),
                                  ],
                                ),
                              ),
                            ),
                          ),
                        Padding(
                          padding: const EdgeInsets.symmetric(
                            horizontal: 10,
                            vertical: 10,
                          ),
                          child: Row(
                            children: <Widget>[
                              Expanded(
                                child: Text(
                                  options[i],
                                  style: TextStyle(
                                    color: textColor,
                                    fontWeight: selected || winning
                                        ? FontWeight.w800
                                        : FontWeight.w600,
                                  ),
                                ),
                              ),
                              if (selected)
                                Icon(
                                  Icons.check_circle,
                                  size: 16,
                                  color: Colors.amber.shade300,
                                ),
                              if (winning) ...<Widget>[
                                const SizedBox(width: 6),
                                Icon(
                                  Icons.emoji_events_rounded,
                                  size: 16,
                                  color: Colors.amber.shade300,
                                ),
                              ],
                              const SizedBox(width: 8),
                              Text(
                                '$voteCount (${(pct * 100).round()}%)',
                                style: TextStyle(
                                  color: textColor.withOpacity(0.84),
                                  fontWeight: FontWeight.w700,
                                  fontSize: 12,
                                ),
                              ),
                            ],
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ),
            );
          }),
          if (canFinalize)
            Padding(
              padding: const EdgeInsets.only(top: 6),
              child: OutlinedButton.icon(
                onPressed: () => _finalizePoll(message),
                style: OutlinedButton.styleFrom(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 12,
                    vertical: 8,
                  ),
                ),
                icon: const Icon(Icons.check_circle_outline, size: 16),
                label: const Text('Finalize Poll'),
              ),
            ),
          if (isClosed && closeState != null)
            Padding(
              padding: const EdgeInsets.only(top: 6),
              child: Text(
                closeState.winnerIndex >= 0 &&
                        closeState.winnerIndex < options.length
                    ? 'Finalized by ${closeState.closedBy} at ${_clock(closeState.closedAtMillis)} • Winner: ${options[closeState.winnerIndex]} (${closeState.winnerVotes} votes)'
                    : (totalVotes > 0
                          ? 'Finalized by ${closeState.closedBy} at ${_clock(closeState.closedAtMillis)} • Result ended in a tie (${closeState.totalVotes} votes).'
                          : 'Finalized by ${closeState.closedBy} at ${_clock(closeState.closedAtMillis)} • No votes recorded.'),
                style: TextStyle(
                  color: textColor.withOpacity(0.76),
                  fontSize: 11.8,
                  fontWeight: FontWeight.w600,
                ),
              ),
            ),
        ],
      );
    }

    if (message.type == 'group_invite') {
      final String groupName = (message.meta?['group_name'] ?? 'Group')
          .toString();
      final String groupId = (message.meta?['group_id'] ?? '').toString();
      return Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Row(
            children: <Widget>[
              Icon(Icons.groups_rounded, color: textColor, size: 18),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  '${message.senderName} invited you to "$groupName".',
                  style: TextStyle(
                    color: textColor,
                    fontWeight: FontWeight.w700,
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 8),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: <Widget>[
              ElevatedButton.icon(
                onPressed: groupId.trim().isEmpty
                    ? null
                    : () => _openGroupFromInvite(message),
                icon: const Icon(Icons.login_rounded, size: 16),
                label: const Text('Review request'),
              ),
            ],
          ),
        ],
      );
    }

    if (message.type == 'group_invite_response' ||
        message.type == 'system_group_join' ||
        message.type == 'system_group_invite' ||
        message.type == 'group_admin_update') {
      return Text(
        message.text,
        style: TextStyle(
          color: textColor.withOpacity(0.86),
          fontSize: 13,
          fontStyle: FontStyle.italic,
        ),
      );
    }

    return Text(message.text, style: TextStyle(color: textColor, fontSize: 15));
  }
}

enum _DeliveryState { sent, delivered, read }

class _DeliverySnapshot {
  const _DeliverySnapshot({
    required this.state,
    required this.recipientCount,
    required this.deliveredCount,
    required this.readCount,
    required this.readUserIds,
  });

  final _DeliveryState state;
  final int recipientCount;
  final int deliveredCount;
  final int readCount;
  final List<String> readUserIds;
}

class _ScoredGroupUser {
  const _ScoredGroupUser({required this.user, required this.score});

  final ChatUser user;
  final int score;
}

class GroupCreateScreen extends StatefulWidget {
  const GroupCreateScreen({
    super.key,
    required this.myUserId,
    required this.myName,
    required this.role,
    required this.chatService,
    required this.existingThreads,
  });

  final String myUserId;
  final String myName;
  final String role;
  final ChatService chatService;
  final List<ChatThreadSummary> existingThreads;

  @override
  State<GroupCreateScreen> createState() => _GroupCreateScreenState();
}

class _GroupCreateScreenState extends State<GroupCreateScreen> {
  final TextEditingController _groupNameCtrl = TextEditingController();
  final TextEditingController _searchCtrl = TextEditingController();
  final TextEditingController _manualIdCtrl = TextEditingController();

  final Map<String, ChatUser> _selected = <String, ChatUser>{};
  List<ChatUser> _results = <ChatUser>[];
  bool _loading = false;
  bool _creating = false;
  Timer? _debounce;

  @override
  void initState() {
    super.initState();
    _searchUsers('');
    _searchCtrl.addListener(_onSearchChanged);
  }

  @override
  void dispose() {
    _debounce?.cancel();
    _groupNameCtrl.dispose();
    _searchCtrl.dispose();
    _manualIdCtrl.dispose();
    super.dispose();
  }

  void _onSearchChanged() {
    _debounce?.cancel();
    _debounce = Timer(const Duration(milliseconds: 260), () {
      _searchUsers(_searchCtrl.text.trim());
    });
  }

  Future<void> _searchUsers(String query) async {
    if (mounted) {
      setState(() => _loading = true);
    }
    try {
      final List<ChatUser> users = await widget.chatService.searchUsers(
        query: query,
        myUserId: widget.myUserId,
        role: widget.role,
        existingThreads: widget.existingThreads,
      );

      if (!mounted) {
        return;
      }

      setState(() {
        _results = users;
        _loading = false;
      });
    } catch (_) {
      if (!mounted) {
        return;
      }
      setState(() {
        _results = <ChatUser>[];
        _loading = false;
      });
    }
  }

  void _toggleUser(ChatUser user) {
    setState(() {
      if (_selected.containsKey(user.userId)) {
        _selected.remove(user.userId);
      } else {
        _selected[user.userId] = user;
      }
    });
  }

  void _addManualUser() {
    final String raw = _manualIdCtrl.text.trim();
    if (raw.isEmpty) {
      return;
    }
    if (raw == widget.myUserId) {
      return;
    }

    setState(() {
      _selected[raw] = ChatUser(userId: raw, name: raw, role: 'student');
      _manualIdCtrl.clear();
    });
  }

  void _toggleTeacherQuick() {
    _toggleUser(
      const ChatUser(userId: 'TEACHER', name: 'Teacher', role: 'teacher'),
    );
  }

  Future<void> _create() async {
    final String name = _groupNameCtrl.text.trim();
    if (name.isEmpty) {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text('Enter group name')));
      return;
    }

    if (_selected.length < 2) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Select at least 2 members (total 3+ with you).'),
        ),
      );
      return;
    }

    setState(() => _creating = true);
    try {
      final List<String> adminIds = <String>[widget.myUserId];
      final ChatThreadSummary thread = await widget.chatService
          .createGroupThread(
            myUserId: widget.myUserId,
            myName: widget.myName,
            role: widget.role,
            groupName: name,
            members: _selected.values.toList(),
          );

      for (final ChatUser member in _selected.values) {
        if (member.userId == widget.myUserId) {
          continue;
        }

        final ChatThreadSummary direct = ChatThreadSummary(
          threadId: '',
          title: member.name,
          peerId: member.userId,
          peerName: member.name,
          lastMessage: '',
          updatedAtMillis: DateTime.now().millisecondsSinceEpoch,
          unread: false,
          isDoubtThread: false,
          participants: <String>[widget.myUserId, member.userId],
        );

        try {
          await widget.chatService.sendMessagePayload(
            thread: direct,
            myUserId: widget.myUserId,
            myName: widget.myName,
            type: 'group_invite',
            text: 'Invitation to join "$name"',
            meta: <String, dynamic>{
              'group_id': thread.threadId,
              'group_name': name,
              'participants': thread.participants,
              'admins': adminIds,
              'creator_id': widget.myUserId,
            },
          );
        } catch (_) {}
      }

      if (mounted) {
        Navigator.pop(context, thread);
      }
    } catch (_) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Unable to create group right now.')),
        );
      }
    }

    if (mounted) {
      setState(() => _creating = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final bool isDark = Theme.of(context).brightness == Brightness.dark;

    return Scaffold(
      appBar: AppBar(title: const Text('Create New Group')),
      body: ListView(
        padding: const EdgeInsets.all(14),
        children: <Widget>[
          LiquidGlass(
            solidFill: true,
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 6),
            child: TextField(
              controller: _groupNameCtrl,
              decoration: const InputDecoration(
                border: InputBorder.none,
                hintText: 'Group name',
              ),
            ),
          ),
          const SizedBox(height: 10),
          LiquidGlass(
            solidFill: true,
            padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 6),
            child: TextField(
              controller: _searchCtrl,
              decoration: const InputDecoration(
                border: InputBorder.none,
                hintText: 'Search students / teacher',
                prefixIcon: Icon(Icons.search),
              ),
            ),
          ),
          const SizedBox(height: 8),
          Row(
            children: <Widget>[
              Text(
                'Need teacher in group?',
                style: TextStyle(
                  fontSize: 12,
                  color: isDark
                      ? const Color(0xFF9FB0C7)
                      : Colors.grey.shade700,
                ),
              ),
              const SizedBox(width: 8),
              OutlinedButton.icon(
                onPressed: _toggleTeacherQuick,
                icon: const Icon(Icons.school, size: 16),
                label: const Text('Add Teacher'),
              ),
            ],
          ),
          const SizedBox(height: 10),
          LiquidGlass(
            solidFill: true,
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
            child: Row(
              children: <Widget>[
                Expanded(
                  child: TextField(
                    controller: _manualIdCtrl,
                    decoration: const InputDecoration(
                      border: InputBorder.none,
                      hintText: 'Add by user ID (if not listed)',
                    ),
                  ),
                ),
                TextButton(
                  onPressed: _addManualUser,
                  child: const Text('Add Student'),
                ),
              ],
            ),
          ),
          const SizedBox(height: 10),
          Text(
            '${_selected.length + 1} members (including you)',
            style: TextStyle(
              fontSize: 12.5,
              fontWeight: FontWeight.w600,
              color: isDark ? const Color(0xFF9FB0C7) : Colors.grey.shade700,
            ),
          ),
          const SizedBox(height: 8),
          if (_selected.isNotEmpty)
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: _selected.values.map((ChatUser user) {
                return Chip(
                  label: Text(user.name),
                  deleteIcon: const Icon(Icons.close, size: 16),
                  onDeleted: () => _toggleUser(user),
                );
              }).toList(),
            ),
          if (_selected.isNotEmpty) const SizedBox(height: 10),
          if (_loading)
            const LinearProgressIndicator(minHeight: 1.4)
          else
            Text(
              '${_results.length} users found',
              style: TextStyle(
                color: isDark ? const Color(0xFF9FB0C7) : Colors.grey.shade700,
              ),
            ),
          const SizedBox(height: 8),
          ..._results.map((ChatUser user) {
            final bool checked = _selected.containsKey(user.userId);
            return Padding(
              padding: const EdgeInsets.only(bottom: 8),
              child: LiquidGlass(
                solidFill: true,
                onTap: () => _toggleUser(user),
                quality: LiquidGlassQuality.low,
                padding: const EdgeInsets.symmetric(
                  horizontal: 12,
                  vertical: 10,
                ),
                color: checked
                    ? AppColors.primaryTone(context).withOpacity(
                        Theme.of(context).brightness == Brightness.dark
                            ? 0.26
                            : 0.12,
                      )
                    : null,
                child: Row(
                  children: <Widget>[
                    CircleAvatar(
                      radius: 18,
                      backgroundColor: AppColors.primaryTone(
                        context,
                      ).withOpacity(0.22),
                      child: Text(
                        user.initials,
                        style: TextStyle(
                          color: AppColors.primaryTone(context),
                          fontWeight: FontWeight.w800,
                        ),
                      ),
                    ),
                    const SizedBox(width: 10),
                    Expanded(
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: <Widget>[
                          Text(
                            user.name,
                            style: const TextStyle(fontWeight: FontWeight.w700),
                          ),
                          Text(
                            '${user.userId} • ${user.role}',
                            style: TextStyle(
                              fontSize: 12,
                              color: isDark
                                  ? const Color(0xFF9FB0C7)
                                  : Colors.grey.shade700,
                            ),
                          ),
                        ],
                      ),
                    ),
                    Checkbox(
                      value: checked,
                      onChanged: (_) => _toggleUser(user),
                    ),
                  ],
                ),
              ),
            );
          }),
          const SizedBox(height: 16),
          ElevatedButton.icon(
            onPressed: _creating ? null : _create,
            icon: _creating
                ? const SizedBox(
                    width: 16,
                    height: 16,
                    child: CircularProgressIndicator(
                      strokeWidth: 2,
                      color: Colors.white,
                    ),
                  )
                : const Icon(Icons.groups_rounded),
            label: Text(
              _creating ? 'Creating...' : 'Create Group & Send Join Requests',
            ),
          ),
        ],
      ),
    );
  }
}

typedef GroupAddStudentsCallback =
    Future<void> Function(List<ChatUser> students);
typedef GroupSetAdminCallback =
    Future<void> Function({required ChatUser member, required bool makeAdmin});

class GroupInfoScreen extends StatefulWidget {
  const GroupInfoScreen({
    super.key,
    required this.groupThread,
    required this.myUserId,
    required this.myName,
    required this.role,
    required this.chatService,
    required this.initialMemberIds,
    required this.initialAdminIds,
    required this.onAddStudents,
    required this.onSetAdminStatus,
  });

  final ChatThreadSummary groupThread;
  final String myUserId;
  final String myName;
  final String role;
  final ChatService chatService;
  final List<String> initialMemberIds;
  final List<String> initialAdminIds;
  final GroupAddStudentsCallback onAddStudents;
  final GroupSetAdminCallback onSetAdminStatus;

  @override
  State<GroupInfoScreen> createState() => _GroupInfoScreenState();
}

class _GroupInfoScreenState extends State<GroupInfoScreen> {
  final TextEditingController _searchCtrl = TextEditingController();
  final TextEditingController _manualStudentCtrl = TextEditingController();
  final Set<String> _selectedStudentIds = <String>{};
  final Set<String> _busyAdminIds = <String>{};

  List<ChatUser> _directory = <ChatUser>[];
  late Set<String> _memberIds;
  late Set<String> _adminIds;
  bool _loading = true;
  bool _addingStudents = false;
  String _query = '';
  bool get _canEditSettings => _adminIds.contains(widget.myUserId);

  @override
  void initState() {
    super.initState();
    _memberIds = widget.initialMemberIds.toSet()..add(widget.myUserId);
    _adminIds = widget.initialAdminIds.toSet();
    if (_adminIds.isEmpty) {
      _adminIds.add(widget.myUserId);
    }
    _searchCtrl.addListener(() {
      if (!mounted) {
        return;
      }
      setState(() => _query = _searchCtrl.text.trim());
    });
    unawaited(_loadDirectory());
  }

  @override
  void dispose() {
    _searchCtrl.dispose();
    _manualStudentCtrl.dispose();
    super.dispose();
  }

  Future<void> _loadDirectory() async {
    setState(() => _loading = true);
    try {
      final List<ChatUser> users = await widget.chatService.searchUsers(
        query: '',
        myUserId: widget.myUserId,
        role: widget.role,
        existingThreads: const <ChatThreadSummary>[],
      );
      if (!mounted) {
        return;
      }
      setState(() {
        _directory = users;
        _loading = false;
      });
    } catch (_) {
      if (!mounted) {
        return;
      }
      setState(() => _loading = false);
    }
  }

  Map<String, ChatUser> get _directoryMap {
    final Map<String, ChatUser> map = <String, ChatUser>{};
    for (final ChatUser user in _directory) {
      map[user.userId] = user;
    }
    map[widget.myUserId] =
        map[widget.myUserId] ??
        ChatUser(
          userId: widget.myUserId,
          name: widget.myName,
          role: widget.role == 'teacher' ? 'teacher' : 'student',
        );
    for (final String id in _memberIds) {
      map[id] =
          map[id] ??
          ChatUser(
            userId: id,
            name: id == widget.myUserId ? widget.myName : id,
            role: id.toUpperCase() == 'TEACHER' ? 'teacher' : 'student',
          );
    }
    return map;
  }

  List<ChatUser> get _members {
    final Map<String, ChatUser> map = _directoryMap;
    final List<ChatUser> users = _memberIds
        .map((String id) => map[id]!)
        .toList(growable: false);
    users.sort((ChatUser a, ChatUser b) {
      final bool aAdmin = _adminIds.contains(a.userId);
      final bool bAdmin = _adminIds.contains(b.userId);
      if (aAdmin != bAdmin) {
        return aAdmin ? -1 : 1;
      }
      final bool aMe = a.userId == widget.myUserId;
      final bool bMe = b.userId == widget.myUserId;
      if (aMe != bMe) {
        return aMe ? -1 : 1;
      }
      return a.name.toLowerCase().compareTo(b.name.toLowerCase());
    });
    return users;
  }

  bool _isStudent(ChatUser user) {
    return user.role.toLowerCase() != 'teacher' &&
        user.userId.toUpperCase() != 'TEACHER';
  }

  String _normalizeSearch(String input) {
    return input
        .toLowerCase()
        .replaceAll(RegExp(r'[^a-z0-9\s]'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  String _initials(String input) {
    final List<String> words = _normalizeSearch(
      input,
    ).split(' ').where((String word) => word.isNotEmpty).toList();
    if (words.isEmpty) {
      return '';
    }
    return words.map((String word) => word[0]).join();
  }

  bool _isSubsequence(String query, String target) {
    if (query.isEmpty || target.isEmpty || query.length > target.length) {
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

  int _studentSearchScore(ChatUser user, String queryRaw) {
    final String query = _normalizeSearch(queryRaw);
    if (query.isEmpty) {
      return 1;
    }
    final String compact = query.replaceAll(' ', '');
    final String idNorm = _normalizeSearch(user.userId);
    final String nameNorm = _normalizeSearch(user.name);
    final String hay = _normalizeSearch('${user.name} ${user.userId}');
    final String hayCompact = hay.replaceAll(' ', '');
    final List<String> tokens = query
        .split(' ')
        .where((String token) => token.isNotEmpty)
        .toList();

    int score = 0;
    if (idNorm == query || nameNorm == query) {
      score += 120;
    }
    if (idNorm.startsWith(query)) {
      score += 86;
    }
    if (nameNorm.startsWith(query)) {
      score += 82;
    }
    if (idNorm.contains(query)) {
      score += 66;
    }
    if (nameNorm.contains(query)) {
      score += 62;
    }
    if (compact.isNotEmpty && hayCompact.contains(compact)) {
      score += 24;
    }
    if (tokens.isNotEmpty) {
      int tokenHits = 0;
      for (final String token in tokens) {
        if (hay.contains(token)) {
          tokenHits += 1;
        }
      }
      if (tokenHits == tokens.length) {
        score += 24 + (tokenHits * 4);
      } else if (tokenHits > 0) {
        score += tokenHits * 3;
      }
    }
    final String initials = _initials(user.name);
    if (compact.length >= 2 && initials.startsWith(compact)) {
      score += 18;
    } else if (compact.length >= 2 && initials.contains(compact)) {
      score += 10;
    }
    if (_isSubsequence(compact, idNorm.replaceAll(' ', ''))) {
      score += 22;
    }
    if (_isSubsequence(compact, nameNorm.replaceAll(' ', ''))) {
      score += 18;
    }
    return score;
  }

  List<ChatUser> get _studentCandidates {
    final List<ChatUser> options = _directory.where((ChatUser user) {
      if (!_isStudent(user)) {
        return false;
      }
      if (_memberIds.contains(user.userId)) {
        return false;
      }
      return true;
    }).toList();
    final String normalizedQuery = _normalizeSearch(_query);
    if (normalizedQuery.isEmpty) {
      options.sort(
        (ChatUser a, ChatUser b) =>
            a.name.toLowerCase().compareTo(b.name.toLowerCase()),
      );
      return options;
    }
    final List<_ScoredGroupUser> scored = <_ScoredGroupUser>[];
    for (final ChatUser user in options) {
      final int score = _studentSearchScore(user, normalizedQuery);
      if (score <= 0) {
        continue;
      }
      scored.add(_ScoredGroupUser(user: user, score: score));
    }
    scored.sort((_ScoredGroupUser a, _ScoredGroupUser b) {
      final int byScore = b.score.compareTo(a.score);
      if (byScore != 0) {
        return byScore;
      }
      return a.user.name.toLowerCase().compareTo(b.user.name.toLowerCase());
    });
    return scored.map((_ScoredGroupUser item) => item.user).toList();
  }

  Future<void> _addSelectedStudents() async {
    if (!_canEditSettings) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Only admins can add students.')),
      );
      return;
    }
    final Map<String, ChatUser> directoryMap = _directoryMap;
    final List<ChatUser> selected = _selectedStudentIds
        .map((String id) => directoryMap[id])
        .whereType<ChatUser>()
        .where(_isStudent)
        .toList();
    if (selected.isEmpty) {
      return;
    }
    setState(() => _addingStudents = true);
    try {
      await widget.onAddStudents(selected);
      if (!mounted) {
        return;
      }
      setState(() {
        _memberIds.addAll(selected.map((ChatUser e) => e.userId));
        _selectedStudentIds.clear();
      });
    } finally {
      if (mounted) {
        setState(() => _addingStudents = false);
      }
    }
  }

  Future<void> _toggleAdmin(ChatUser member) async {
    if (!_canEditSettings) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Only admins can manage admin roles.')),
      );
      return;
    }
    final bool isAdmin = _adminIds.contains(member.userId);
    if (member.userId == widget.myUserId && isAdmin && _adminIds.length <= 1) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('At least one admin is required.')),
      );
      return;
    }
    setState(() {
      _busyAdminIds.add(member.userId);
      if (isAdmin) {
        _adminIds.remove(member.userId);
      } else {
        _adminIds.add(member.userId);
      }
    });
    try {
      await widget.onSetAdminStatus(member: member, makeAdmin: !isAdmin);
    } catch (_) {
      if (!mounted) {
        return;
      }
      setState(() {
        if (isAdmin) {
          _adminIds.add(member.userId);
        } else {
          _adminIds.remove(member.userId);
        }
      });
    } finally {
      if (mounted) {
        setState(() => _busyAdminIds.remove(member.userId));
      }
    }
  }

  Future<void> _addManualStudentById() async {
    if (!_canEditSettings) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Only admins can add students.')),
      );
      return;
    }
    final String id = _manualStudentCtrl.text.trim();
    if (id.isEmpty) {
      return;
    }
    if (_memberIds.contains(id)) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('That student is already in the group.')),
      );
      return;
    }
    final ChatUser manual = ChatUser(userId: id, name: id, role: 'student');
    setState(() => _addingStudents = true);
    try {
      await widget.onAddStudents(<ChatUser>[manual]);
      if (!mounted) {
        return;
      }
      setState(() {
        _memberIds.add(id);
        _manualStudentCtrl.clear();
      });
    } finally {
      if (mounted) {
        setState(() => _addingStudents = false);
      }
    }
  }

  Future<void> _clearChatForMe() async {
    await widget.chatService.clearChatForMe(
      myUserId: widget.myUserId,
      threadId: widget.groupThread.threadId,
    );
    if (!mounted) {
      return;
    }
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text('Group chat cleared for you.')),
    );
  }

  Future<void> _leaveGroup() async {
    await widget.chatService.hideThreadForMe(
      myUserId: widget.myUserId,
      threadId: widget.groupThread.threadId,
    );
    await widget.chatService.clearChatForMe(
      myUserId: widget.myUserId,
      threadId: widget.groupThread.threadId,
    );
    if (!mounted) {
      return;
    }
    Navigator.pop(context);
  }

  @override
  Widget build(BuildContext context) {
    final bool isDark = Theme.of(context).brightness == Brightness.dark;
    final Color cardColor = isDark
        ? const Color(0xFF1B1D22)
        : const Color(0xFFF4F5F7);
    final Color subtitleColor = isDark
        ? const Color(0xFF9FA7B4)
        : const Color(0xFF6A717D);

    return Scaffold(
      appBar: AppBar(title: const Text('Group Info')),
      body: ListView(
        padding: const EdgeInsets.fromLTRB(14, 14, 14, 20),
        children: <Widget>[
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: cardColor,
              borderRadius: BorderRadius.circular(18),
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                Text(
                  widget.groupThread.title,
                  style: const TextStyle(
                    fontWeight: FontWeight.w800,
                    fontSize: 20,
                  ),
                ),
                const SizedBox(height: 6),
                Text(
                  '${_memberIds.length} members • ${_adminIds.length} admins',
                  style: TextStyle(color: subtitleColor, fontSize: 13),
                ),
                const SizedBox(height: 4),
                Text(
                  'ID: ${widget.groupThread.threadId}',
                  style: TextStyle(color: subtitleColor, fontSize: 12),
                ),
              ],
            ),
          ),
          const SizedBox(height: 10),
          Row(
            children: <Widget>[
              Expanded(
                child: OutlinedButton.icon(
                  onPressed: _clearChatForMe,
                  icon: const Icon(Icons.cleaning_services_outlined, size: 16),
                  label: const Text('Clear Chat'),
                ),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: OutlinedButton.icon(
                  onPressed: _leaveGroup,
                  icon: const Icon(Icons.logout_rounded, size: 16),
                  label: const Text('Leave Group'),
                ),
              ),
            ],
          ),
          const SizedBox(height: 16),
          Row(
            children: <Widget>[
              const Text(
                'Add Student',
                style: TextStyle(fontWeight: FontWeight.w700, fontSize: 16),
              ),
              const Spacer(),
              if (!_canEditSettings)
                Text(
                  'Admin only',
                  style: TextStyle(
                    color: subtitleColor,
                    fontSize: 12,
                    fontWeight: FontWeight.w600,
                  ),
                ),
            ],
          ),
          const SizedBox(height: 8),
          LiquidGlass(
            solidFill: true,
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
            child: TextField(
              controller: _searchCtrl,
              enabled: _canEditSettings,
              decoration: const InputDecoration(
                border: InputBorder.none,
                hintText: 'Search students to add',
                prefixIcon: Icon(Icons.search),
              ),
            ),
          ),
          const SizedBox(height: 8),
          LiquidGlass(
            solidFill: true,
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
            child: Row(
              children: <Widget>[
                Expanded(
                  child: TextField(
                    controller: _manualStudentCtrl,
                    enabled: _canEditSettings,
                    decoration: const InputDecoration(
                      border: InputBorder.none,
                      hintText: 'Add student by ID',
                    ),
                  ),
                ),
                TextButton(
                  onPressed: _canEditSettings && !_addingStudents
                      ? _addManualStudentById
                      : null,
                  child: const Text('Add'),
                ),
              ],
            ),
          ),
          const SizedBox(height: 8),
          if (_loading)
            const LinearProgressIndicator(minHeight: 1.4)
          else if (_studentCandidates.isEmpty)
            Text(
              _canEditSettings
                  ? 'No students available to add.'
                  : 'Only group admins can add students.',
              style: TextStyle(color: subtitleColor, fontSize: 12.5),
            )
          else
            ..._studentCandidates.take(8).map((ChatUser student) {
              final bool selected = _selectedStudentIds.contains(
                student.userId,
              );
              return ListTile(
                dense: true,
                enabled: _canEditSettings,
                contentPadding: EdgeInsets.zero,
                leading: CircleAvatar(
                  radius: 16,
                  child: Text(student.initials),
                ),
                title: Text(student.name),
                subtitle: Text(
                  '${student.userId} • student',
                  style: TextStyle(fontSize: 12, color: subtitleColor),
                ),
                trailing: Checkbox(
                  value: selected,
                  onChanged: _canEditSettings
                      ? (_) {
                          setState(() {
                            if (selected) {
                              _selectedStudentIds.remove(student.userId);
                            } else {
                              _selectedStudentIds.add(student.userId);
                            }
                          });
                        }
                      : null,
                ),
              );
            }),
          const SizedBox(height: 8),
          ElevatedButton.icon(
            onPressed:
                !_canEditSettings ||
                    _addingStudents ||
                    _selectedStudentIds.isEmpty
                ? null
                : _addSelectedStudents,
            icon: _addingStudents
                ? const SizedBox(
                    width: 16,
                    height: 16,
                    child: CircularProgressIndicator(
                      strokeWidth: 2,
                      color: Colors.white,
                    ),
                  )
                : const Icon(Icons.person_add_alt_1_rounded),
            label: Text(
              _addingStudents ? 'Adding...' : 'Add Selected Students',
            ),
          ),
          const SizedBox(height: 18),
          const Text(
            'Members',
            style: TextStyle(fontWeight: FontWeight.w700, fontSize: 16),
          ),
          const SizedBox(height: 8),
          ..._members.map((ChatUser member) {
            final bool isAdmin = _adminIds.contains(member.userId);
            final bool busy = _busyAdminIds.contains(member.userId);
            final bool isMe = member.userId == widget.myUserId;
            final bool teacher = !_isStudent(member);
            final String roleLabel = isAdmin
                ? (teacher ? 'Admin • Teacher' : 'Admin • Student')
                : (teacher ? 'Teacher' : 'Student');
            return Container(
              margin: const EdgeInsets.only(bottom: 8),
              padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
              decoration: BoxDecoration(
                color: cardColor,
                borderRadius: BorderRadius.circular(14),
              ),
              child: Row(
                children: <Widget>[
                  CircleAvatar(radius: 16, child: Text(member.initials)),
                  const SizedBox(width: 10),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: <Widget>[
                        Text(
                          isMe ? '${member.name} (You)' : member.name,
                          style: const TextStyle(fontWeight: FontWeight.w700),
                        ),
                        Text(
                          '${member.userId} • $roleLabel',
                          style: TextStyle(fontSize: 12, color: subtitleColor),
                        ),
                      ],
                    ),
                  ),
                  if (_canEditSettings)
                    TextButton(
                      onPressed: busy ? null : () => _toggleAdmin(member),
                      child: Text(isAdmin ? 'Remove Admin' : 'Make Admin'),
                    ),
                ],
              ),
            );
          }),
        ],
      ),
    );
  }
}

class _PollDraft {
  const _PollDraft({
    required this.question,
    required this.context,
    required this.options,
  });

  final String question;
  final String context;
  final List<String> options;
}

class _PollComposerSheet extends StatefulWidget {
  const _PollComposerSheet();

  @override
  State<_PollComposerSheet> createState() => _PollComposerSheetState();
}

class _PollComposerSheetState extends State<_PollComposerSheet> {
  final TextEditingController _questionCtrl = TextEditingController();
  final TextEditingController _contextCtrl = TextEditingController();
  final List<TextEditingController> _optionCtrls = <TextEditingController>[
    TextEditingController(),
    TextEditingController(),
  ];

  @override
  void dispose() {
    _questionCtrl.dispose();
    _contextCtrl.dispose();
    for (final TextEditingController ctrl in _optionCtrls) {
      ctrl.dispose();
    }
    super.dispose();
  }

  void _addOption() {
    if (_optionCtrls.length >= 8) {
      return;
    }
    setState(() => _optionCtrls.add(TextEditingController()));
  }

  void _removeOption(int index) {
    if (_optionCtrls.length <= 2) {
      return;
    }
    final TextEditingController ctrl = _optionCtrls.removeAt(index);
    ctrl.dispose();
    setState(() {});
  }

  void _submit() {
    final String question = _questionCtrl.text.trim();
    final String contextText = _contextCtrl.text.trim();
    final List<String> options = _optionCtrls
        .map((TextEditingController c) => c.text.trim())
        .where((String s) => s.isNotEmpty)
        .toList();

    if (question.isEmpty || options.length < 2) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Enter question and at least two options.'),
        ),
      );
      return;
    }

    Navigator.pop(
      context,
      _PollDraft(question: question, context: contextText, options: options),
    );
  }

  @override
  Widget build(BuildContext context) {
    return SafeArea(
      child: Padding(
        padding: EdgeInsets.only(
          left: 12,
          right: 12,
          bottom: MediaQuery.of(context).viewInsets.bottom + 12,
        ),
        child: LiquidGlass(
          solidFill: true,
          padding: const EdgeInsets.all(12),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: <Widget>[
              Text(
                'Create Poll',
                style: Theme.of(
                  context,
                ).textTheme.titleMedium?.copyWith(fontWeight: FontWeight.w800),
              ),
              const SizedBox(height: 10),
              TextField(
                controller: _questionCtrl,
                decoration: const InputDecoration(labelText: 'Question'),
              ),
              const SizedBox(height: 8),
              TextField(
                controller: _contextCtrl,
                decoration: const InputDecoration(
                  labelText: 'Context (optional)',
                ),
              ),
              const SizedBox(height: 8),
              ...List<Widget>.generate(_optionCtrls.length, (int i) {
                return Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: Row(
                    children: <Widget>[
                      Expanded(
                        child: TextField(
                          controller: _optionCtrls[i],
                          decoration: InputDecoration(
                            labelText: 'Option ${i + 1}',
                          ),
                        ),
                      ),
                      if (_optionCtrls.length > 2)
                        IconButton(
                          onPressed: () => _removeOption(i),
                          icon: const Icon(Icons.close),
                        ),
                    ],
                  ),
                );
              }),
              Row(
                children: <Widget>[
                  TextButton.icon(
                    onPressed: _addOption,
                    icon: const Icon(Icons.add),
                    label: const Text('Add option'),
                  ),
                  const Spacer(),
                  ElevatedButton(onPressed: _submit, child: const Text('Send')),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class _InlineAnswerKeyCard extends StatefulWidget {
  const _InlineAnswerKeyCard({required this.meta, required this.textColor});

  final Map<String, dynamic>? meta;
  final Color textColor;

  @override
  State<_InlineAnswerKeyCard> createState() => _InlineAnswerKeyCardState();
}

class _InlineAnswerKeyCardState extends State<_InlineAnswerKeyCard> {
  bool _front = true;

  String _str(String key, {String fallback = ''}) {
    final String value = (widget.meta?[key] ?? fallback).toString().trim();
    return value.isEmpty ? fallback : value;
  }

  int _int(String key, {int fallback = 0}) {
    final dynamic raw = widget.meta?[key];
    if (raw is int) {
      return raw;
    }
    if (raw is num) {
      return raw.toInt();
    }
    return int.tryParse((raw ?? '').toString()) ?? fallback;
  }

  @override
  Widget build(BuildContext context) {
    final String question = _str('question_text', fallback: _str('question'));
    final String status = _str('status_label', fallback: 'REVIEW');
    final String correct = _str('correct_answer');
    final String student = _str('student_answer');
    final String solution = _str('solution');
    final String concept = _str('concept');
    final String marks = _str('marks_delta');
    final int qIdx = _int('question_index', fallback: 0) + 1;
    final List<String> options =
        ((widget.meta?['options'] as List?) ?? <dynamic>[])
            .map((dynamic e) => e.toString().trim())
            .where((String e) => e.isNotEmpty)
            .take(4)
            .toList();

    return GestureDetector(
      onTap: () => setState(() => _front = !_front),
      child: AnimatedSwitcher(
        duration: const Duration(milliseconds: 220),
        switchInCurve: Curves.easeOutCubic,
        switchOutCurve: Curves.easeInCubic,
        child: _front
            ? Container(
                key: const ValueKey<String>('front'),
                padding: const EdgeInsets.all(10),
                decoration: BoxDecoration(
                  borderRadius: BorderRadius.circular(12),
                  border: Border.all(color: widget.textColor.withOpacity(0.28)),
                ),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: <Widget>[
                    Row(
                      children: <Widget>[
                        Text(
                          'Answer Key Card • Q$qIdx',
                          style: TextStyle(
                            color: widget.textColor,
                            fontWeight: FontWeight.w800,
                            fontSize: 12.2,
                          ),
                        ),
                        const Spacer(),
                        Text(
                          status,
                          style: TextStyle(
                            color: widget.textColor,
                            fontSize: 11,
                            fontWeight: FontWeight.w700,
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 6),
                    SmartText(
                      question.isEmpty ? 'Question unavailable' : question,
                      style: TextStyle(
                        color: widget.textColor,
                        fontWeight: FontWeight.w700,
                        fontSize: 13.2,
                      ),
                    ),
                    if (options.isNotEmpty) ...<Widget>[
                      const SizedBox(height: 6),
                      ...options.map(
                        (String e) => Text(
                          '• $e',
                          style: TextStyle(
                            color: widget.textColor.withOpacity(0.90),
                            fontSize: 12,
                          ),
                        ),
                      ),
                    ],
                    const SizedBox(height: 6),
                    if (correct.isNotEmpty)
                      Text(
                        'Correct: $correct',
                        style: TextStyle(
                          color: widget.textColor,
                          fontWeight: FontWeight.w700,
                          fontSize: 12,
                        ),
                      ),
                    if (student.isNotEmpty)
                      Text(
                        'Student: $student',
                        style: TextStyle(
                          color: widget.textColor,
                          fontWeight: FontWeight.w700,
                          fontSize: 12,
                        ),
                      ),
                    const SizedBox(height: 6),
                    Text(
                      'Tap to flip',
                      style: TextStyle(
                        color: widget.textColor.withOpacity(0.72),
                        fontSize: 10.8,
                      ),
                    ),
                  ],
                ),
              )
            : Container(
                key: const ValueKey<String>('back'),
                padding: const EdgeInsets.all(10),
                decoration: BoxDecoration(
                  borderRadius: BorderRadius.circular(12),
                  border: Border.all(color: widget.textColor.withOpacity(0.28)),
                ),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: <Widget>[
                    Text(
                      'Solution View',
                      style: TextStyle(
                        color: widget.textColor,
                        fontWeight: FontWeight.w800,
                        fontSize: 12.2,
                      ),
                    ),
                    const SizedBox(height: 6),
                    SmartText(
                      solution.isEmpty
                          ? 'No official solution was attached.'
                          : solution,
                      style: TextStyle(color: widget.textColor, fontSize: 12.5),
                    ),
                    if (concept.isNotEmpty) ...<Widget>[
                      const SizedBox(height: 6),
                      Text(
                        'Concept: $concept',
                        style: TextStyle(
                          color: widget.textColor,
                          fontSize: 12,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                    ],
                    if (marks.isNotEmpty) ...<Widget>[
                      const SizedBox(height: 4),
                      Text(
                        'Marks Delta: $marks',
                        style: TextStyle(
                          color: widget.textColor,
                          fontSize: 12,
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                    ],
                    const SizedBox(height: 6),
                    Text(
                      'Tap to flip back',
                      style: TextStyle(
                        color: widget.textColor.withOpacity(0.72),
                        fontSize: 10.8,
                      ),
                    ),
                  ],
                ),
              ),
      ),
    );
  }
}

class ChatAIReaderScreen extends StatefulWidget {
  const ChatAIReaderScreen({
    super.key,
    required this.title,
    required this.subtitle,
    required this.content,
  });

  final String title;
  final String subtitle;
  final String content;

  @override
  State<ChatAIReaderScreen> createState() => _ChatAIReaderScreenState();
}

class _ChatAIReaderScreenState extends State<ChatAIReaderScreen> {
  bool _savingPdf = false;

  Future<void> _saveAsPdf() async {
    if (_savingPdf) {
      return;
    }
    setState(() => _savingPdf = true);
    try {
      final pw.Document doc = pw.Document();
      doc.addPage(
        pw.MultiPage(
          margin: const pw.EdgeInsets.all(28),
          build: (_) => <pw.Widget>[
            pw.Text(
              widget.title,
              style: pw.TextStyle(fontSize: 20, fontWeight: pw.FontWeight.bold),
            ),
            if (widget.subtitle.trim().isNotEmpty)
              pw.Padding(
                padding: const pw.EdgeInsets.only(top: 6, bottom: 14),
                child: pw.Text(
                  widget.subtitle,
                  style: const pw.TextStyle(fontSize: 12),
                ),
              ),
            pw.Text(widget.content),
          ],
        ),
      );
      final List<int> bytes = await doc.save();
      await Printing.sharePdf(
        bytes: Uint8List.fromList(bytes),
        filename:
            '${widget.title.toLowerCase().replaceAll(RegExp(r'[^a-z0-9]+'), '_')}_${DateTime.now().millisecondsSinceEpoch}.pdf',
      );
    } catch (e) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Save PDF failed: $e')));
    } finally {
      if (mounted) {
        setState(() => _savingPdf = false);
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final bool isDark = Theme.of(context).brightness == Brightness.dark;
    final Color surfaceColor = isDark ? Colors.black : Colors.white;
    final Color primaryText = isDark ? Colors.white : Colors.black;
    final Color secondaryText = primaryText.withValues(
      alpha: isDark ? 0.86 : 0.78,
    );
    return Scaffold(
      backgroundColor: surfaceColor,
      appBar: AppBar(
        title: Text(widget.title),
        backgroundColor: Colors.transparent,
        foregroundColor: primaryText,
        actions: <Widget>[
          IconButton(
            tooltip: 'Copy',
            onPressed: () async {
              await Clipboard.setData(ClipboardData(text: widget.content));
              if (!mounted) {
                return;
              }
              ScaffoldMessenger.of(
                context,
              ).showSnackBar(const SnackBar(content: Text('Copied')));
            },
            icon: const Icon(Icons.copy_rounded),
          ),
          IconButton(
            tooltip: 'Save as PDF',
            onPressed: _savingPdf ? null : _saveAsPdf,
            icon: _savingPdf
                ? const SizedBox(
                    width: 16,
                    height: 16,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.picture_as_pdf_rounded),
          ),
        ],
      ),
      body: AnimatedSwitcher(
        duration: const Duration(milliseconds: 260),
        child: ListView(
          key: ValueKey<int>(widget.content.hashCode),
          padding: const EdgeInsets.fromLTRB(12, 10, 12, 16),
          children: <Widget>[
            LiquidGlass(
              solidFill: true,
              quality: LiquidGlassQuality.low,
              padding: const EdgeInsets.all(12),
              color: surfaceColor,
              child: Row(
                children: <Widget>[
                  Icon(Icons.auto_awesome_rounded, color: primaryText),
                  const SizedBox(width: 10),
                  Expanded(
                    child: Text(
                      widget.subtitle,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: TextStyle(
                        color: secondaryText,
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ),
                ],
              ),
            ),
            const SizedBox(height: 10),
            LiquidGlass(
              solidFill: true,
              quality: LiquidGlassQuality.low,
              padding: const EdgeInsets.all(14),
              color: surfaceColor,
              child: DefaultTextStyle(
                style: TextStyle(
                  color: primaryText,
                  fontSize: 15,
                  height: 1.52,
                ),
                child: SmartText(widget.content),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _AiActionVisual {
  const _AiActionVisual({required this.icon, required this.label});

  final IconData icon;
  final String label;
}

class _PollCloseState {
  const _PollCloseState({
    required this.pollId,
    required this.closedAtMillis,
    required this.closedBy,
    required this.winnerIndex,
    required this.winnerVotes,
    required this.totalVotes,
    required this.finalResults,
  });

  final String pollId;
  final int closedAtMillis;
  final String closedBy;
  final int winnerIndex;
  final int winnerVotes;
  final int totalVotes;
  final Map<int, int> finalResults;

  bool get isClosed => pollId.trim().isNotEmpty;
}

enum _AiMessageAction {
  deleteMe,
  deleteEveryone,
  aiSummarize,
  aiJeeNotes,
  aiTranscribe,
  openGroupInvite,
}

String _shortTime(int millis) {
  if (millis <= 0) {
    return '';
  }
  final DateTime d = DateTime.fromMillisecondsSinceEpoch(millis);
  final DateTime now = DateTime.now();
  final Duration diff = now.difference(d);
  if (diff.inDays == 0 && now.day == d.day) {
    return _clock(millis);
  }
  if (diff.inDays == 1) {
    return 'Yesterday';
  }
  return '${d.day}/${d.month}';
}

String _clock(int millis) {
  if (millis <= 0) {
    return '';
  }
  final DateTime d = DateTime.fromMillisecondsSinceEpoch(millis);
  final int h = d.hour % 12 == 0 ? 12 : d.hour % 12;
  final String m = d.minute.toString().padLeft(2, '0');
  final String ap = d.hour >= 12 ? 'PM' : 'AM';
  return '$h:$m $ap';
}

String _dayLabel(int millis) {
  if (millis <= 0) {
    return '';
  }
  final DateTime d = DateTime.fromMillisecondsSinceEpoch(millis);
  final DateTime now = DateTime.now();
  if (DateUtils.isSameDay(d, now)) {
    return 'Today';
  }
  if (DateUtils.isSameDay(d, now.subtract(const Duration(days: 1)))) {
    return 'Yesterday';
  }
  return '${d.day}/${d.month}/${d.year}';
}
