class ChatThreadSummary {
  const ChatThreadSummary({
    required this.threadId,
    required this.title,
    required this.peerId,
    required this.peerName,
    required this.lastMessage,
    required this.updatedAtMillis,
    required this.unread,
    required this.isDoubtThread,
    this.isGroup = false,
    this.participants = const <String>[],
    this.rawPayload,
  });

  final String threadId;
  final String title;
  final String peerId;
  final String peerName;
  final String lastMessage;
  final int updatedAtMillis;
  final bool unread;
  final bool isDoubtThread;
  final bool isGroup;
  final List<String> participants;
  final Map<String, dynamic>? rawPayload;

  ChatThreadSummary copyWith({
    String? threadId,
    String? title,
    String? peerId,
    String? peerName,
    String? lastMessage,
    int? updatedAtMillis,
    bool? unread,
    bool? isDoubtThread,
    bool? isGroup,
    List<String>? participants,
    Map<String, dynamic>? rawPayload,
  }) {
    return ChatThreadSummary(
      threadId: threadId ?? this.threadId,
      title: title ?? this.title,
      peerId: peerId ?? this.peerId,
      peerName: peerName ?? this.peerName,
      lastMessage: lastMessage ?? this.lastMessage,
      updatedAtMillis: updatedAtMillis ?? this.updatedAtMillis,
      unread: unread ?? this.unread,
      isDoubtThread: isDoubtThread ?? this.isDoubtThread,
      isGroup: isGroup ?? this.isGroup,
      participants: participants ?? this.participants,
      rawPayload: rawPayload ?? this.rawPayload,
    );
  }

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'threadId': threadId,
      'title': title,
      'peerId': peerId,
      'peerName': peerName,
      'lastMessage': lastMessage,
      'updatedAtMillis': updatedAtMillis,
      'unread': unread,
      'isDoubtThread': isDoubtThread,
      'isGroup': isGroup,
      'participants': participants,
      if (rawPayload != null) 'rawPayload': rawPayload,
    };
  }

  static ChatThreadSummary fromJson(Map<String, dynamic> json) {
    return ChatThreadSummary(
      threadId: (json['threadId'] ?? '').toString(),
      title: (json['title'] ?? '').toString(),
      peerId: (json['peerId'] ?? '').toString(),
      peerName: (json['peerName'] ?? '').toString(),
      lastMessage: (json['lastMessage'] ?? '').toString(),
      updatedAtMillis: _parseInt(json['updatedAtMillis']),
      unread: json['unread'] == true,
      isDoubtThread: json['isDoubtThread'] == true,
      isGroup: json['isGroup'] == true,
      participants: _toStringList(json['participants']),
      rawPayload: json['rawPayload'] is Map
          ? Map<String, dynamic>.from(json['rawPayload'] as Map)
          : null,
    );
  }
}

class ChatUser {
  const ChatUser({
    required this.userId,
    required this.name,
    required this.role,
    this.rawPayload,
  });

  final String userId;
  final String name;
  final String role;
  final Map<String, dynamic>? rawPayload;

  String get initials {
    final List<String> words = name
        .trim()
        .split(RegExp(r'\s+'))
        .where((String w) => w.isNotEmpty)
        .toList();
    if (words.isEmpty) {
      return '?';
    }
    if (words.length == 1) {
      return words.first.substring(0, 1).toUpperCase();
    }
    return '${words.first.substring(0, 1)}${words.last.substring(0, 1)}'
        .toUpperCase();
  }
}

class PeerMessage {
  const PeerMessage({
    required this.id,
    required this.threadId,
    required this.senderId,
    required this.senderName,
    required this.text,
    required this.timeMillis,
    required this.type,
    this.meta,
    this.pending = false,
    this.failed = false,
    this.deletedForEveryone = false,
  });

  final String id;
  final String threadId;
  final String senderId;
  final String senderName;
  final String text;
  final int timeMillis;
  final String type;
  final Map<String, dynamic>? meta;
  final bool pending;
  final bool failed;
  final bool deletedForEveryone;

  PeerMessage copyWith({
    String? id,
    String? threadId,
    String? senderId,
    String? senderName,
    String? text,
    int? timeMillis,
    String? type,
    Map<String, dynamic>? meta,
    bool? pending,
    bool? failed,
    bool? deletedForEveryone,
  }) {
    return PeerMessage(
      id: id ?? this.id,
      threadId: threadId ?? this.threadId,
      senderId: senderId ?? this.senderId,
      senderName: senderName ?? this.senderName,
      text: text ?? this.text,
      timeMillis: timeMillis ?? this.timeMillis,
      type: type ?? this.type,
      meta: meta ?? this.meta,
      pending: pending ?? this.pending,
      failed: failed ?? this.failed,
      deletedForEveryone: deletedForEveryone ?? this.deletedForEveryone,
    );
  }

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'id': id,
      'threadId': threadId,
      'senderId': senderId,
      'senderName': senderName,
      'text': text,
      'timeMillis': timeMillis,
      'type': type,
      'meta': meta,
      'pending': pending,
      'failed': failed,
      'deletedForEveryone': deletedForEveryone,
    };
  }

  static PeerMessage fromJson(Map<String, dynamic> json) {
    return PeerMessage(
      id: (json['id'] ?? '').toString(),
      threadId: (json['threadId'] ?? '').toString(),
      senderId: (json['senderId'] ?? '').toString(),
      senderName: (json['senderName'] ?? '').toString(),
      text: (json['text'] ?? '').toString(),
      timeMillis: _parseInt(json['timeMillis']),
      type: (json['type'] ?? 'text').toString(),
      meta: json['meta'] is Map
          ? Map<String, dynamic>.from(json['meta'] as Map)
          : null,
      pending: json['pending'] == true,
      failed: json['failed'] == true,
      deletedForEveryone: json['deletedForEveryone'] == true,
    );
  }
}

int _parseInt(dynamic value) {
  if (value is int) {
    return value;
  }
  if (value is num) {
    return value.toInt();
  }
  return int.tryParse((value ?? '').toString()) ?? 0;
}

List<String> _toStringList(dynamic raw) {
  if (raw is List) {
    return raw
        .map((dynamic e) => e.toString().trim())
        .where((String e) => e.isNotEmpty)
        .toList();
  }
  return <String>[];
}
