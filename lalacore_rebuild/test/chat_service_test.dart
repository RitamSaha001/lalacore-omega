import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:lalacore_rebuild/models/chat_models.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';
import 'package:lalacore_rebuild/services/chat_service.dart';

void main() {
  group('ChatService', () {
    test('fetchInbox merges peer chats and doubt threads', () async {
      final BackendService backend = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method == 'GET' &&
              request.url.queryParameters['action'] == 'list_chat_directory') {
            return http.Response(
              jsonEncode(<Map<String, dynamic>>[
                <String, dynamic>{
                  'chat_id': 'student_1|student_2',
                  'friend_id': 'student_2',
                  'friend_name': 'Riya',
                  'last_msg': 'Hey',
                  'time': 1000,
                  'messages': jsonEncode(<Map<String, dynamic>>[
                    <String, dynamic>{
                      'sender': 'student_2',
                      'text': 'Hey',
                      'time': 1000,
                      'type': 'text',
                    },
                  ]),
                },
              ]),
              200,
            );
          }

          if (request.method == 'GET' &&
              request.url.queryParameters['action'] == 'get_doubts') {
            return http.Response(
              jsonEncode(<Map<String, dynamic>>[
                <String, dynamic>{
                  'id': 'd_1',
                  'quiz_title': 'Math Quiz',
                  'student': 'Aman',
                  'question': 'Why answer B?',
                  'messages': jsonEncode(<Map<String, dynamic>>[
                    <String, dynamic>{
                      'sender': 'student_1',
                      'senderName': 'Aman',
                      'text': 'Please explain',
                      'time': 2000,
                      'type': 'text',
                    },
                  ]),
                },
              ]),
              200,
            );
          }

          return http.Response(jsonEncode(<String, dynamic>{'ok': true}), 200);
        }),
      );

      final ChatService service = ChatService(backendService: backend);
      final List<ChatThreadSummary> threads = await service.fetchInbox(
        myUserId: 'student_1',
        myName: 'Aman',
        role: 'student',
      );

      expect(
        threads.any(
          (ChatThreadSummary t) =>
              t.isDoubtThread || t.threadId.startsWith('doubt_'),
        ),
        true,
      );
      expect(threads.any((ChatThreadSummary t) => t.peerId == 'TEACHER'), true);
      expect(
        threads.any((ChatThreadSummary t) => t.title.contains('Math')),
        true,
      );
    });

    test(
      'sendMessage uses peer send_message payload for normal chat',
      () async {
        final List<Map<String, dynamic>> posted = <Map<String, dynamic>>[];

        final BackendService backend = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method == 'POST') {
              posted.add(jsonDecode(request.body) as Map<String, dynamic>);
            }
            if (request.method == 'GET') {
              return http.Response(jsonEncode(<dynamic>[]), 200);
            }
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true}),
              200,
            );
          }),
        );

        final ChatService service = ChatService(backendService: backend);
        await service.sendMessage(
          thread: const ChatThreadSummary(
            threadId: 'student_1|student_2',
            title: 'Riya',
            peerId: 'student_2',
            peerName: 'Riya',
            lastMessage: '',
            updatedAtMillis: 0,
            unread: false,
            isDoubtThread: false,
          ),
          myUserId: 'student_1',
          myName: 'Aman',
          text: 'hello',
        );

        expect(
          posted.any(
            (Map<String, dynamic> body) =>
                (body['action'] == 'send_message' ||
                    body['action'] == 'peer_send') &&
                body['is_peer'] == true &&
                body['chat_id'] == 'student_1|student_2',
          ),
          true,
        );
      },
    );

    test('sendMessage keeps existing backend thread id when present', () async {
      final List<Map<String, dynamic>> posted = <Map<String, dynamic>>[];

      final BackendService backend = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method == 'POST') {
            posted.add(jsonDecode(request.body) as Map<String, dynamic>);
          }
          return http.Response(jsonEncode(<String, dynamic>{'ok': true}), 200);
        }),
      );

      final ChatService service = ChatService(backendService: backend);
      await service.sendMessage(
        thread: const ChatThreadSummary(
          threadId: 'chat_room_9281',
          title: 'Riya',
          peerId: 'student_2',
          peerName: 'Riya',
          lastMessage: '',
          updatedAtMillis: 0,
          unread: false,
          isDoubtThread: false,
        ),
        myUserId: 'student_1',
        myName: 'Aman',
        text: 'hello',
      );

      expect(
        posted.any(
          (Map<String, dynamic> body) =>
              (body['action'] == 'send_message' ||
                  body['action'] == 'peer_send') &&
              body['chat_id'] == 'chat_room_9281',
        ),
        true,
      );
    });

    test('sendMessage throws when backend does not acknowledge send', () async {
      final BackendService backend = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method == 'POST') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'ok': false,
                'status': 'FAILED',
                'message': 'message not stored',
              }),
              200,
            );
          }
          return http.Response(jsonEncode(<dynamic>[]), 200);
        }),
      );

      final ChatService service = ChatService(backendService: backend);
      expect(
        () => service.sendMessage(
          thread: const ChatThreadSummary(
            threadId: 'student_1|student_2',
            title: 'Riya',
            peerId: 'student_2',
            peerName: 'Riya',
            lastMessage: '',
            updatedAtMillis: 0,
            unread: false,
            isDoubtThread: false,
          ),
          myUserId: 'student_1',
          myName: 'Aman',
          text: 'hello',
        ),
        throwsException,
      );
    });

    test('fetchInbox hides teacher direct card in teacher mode', () async {
      final BackendService backend = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method == 'GET' &&
              request.url.queryParameters['action'] == 'list_chat_directory') {
            return http.Response(
              jsonEncode(<Map<String, dynamic>>[
                <String, dynamic>{
                  'chat_id': 'teacher|teacher',
                  'friend_id': 'TEACHER',
                  'friend_name': 'Teacher (Direct)',
                  'last_msg': 'self direct',
                  'time': 1000,
                },
                <String, dynamic>{
                  'chat_id': 'teacher|student_9',
                  'friend_id': 'student_9',
                  'friend_name': 'Riya',
                  'last_msg': 'hi',
                  'time': 2000,
                },
              ]),
              200,
            );
          }

          if (request.method == 'GET' &&
              request.url.queryParameters['action'] == 'get_doubts') {
            return http.Response(jsonEncode(<dynamic>[]), 200);
          }

          return http.Response(jsonEncode(<String, dynamic>{'ok': true}), 200);
        }),
      );

      final ChatService service = ChatService(backendService: backend);
      final List<ChatThreadSummary> threads = await service.fetchInbox(
        myUserId: 'TEACHER',
        myName: 'Admin',
        role: 'teacher',
      );

      expect(
        threads.any(
          (ChatThreadSummary t) =>
              !t.isDoubtThread && t.peerId.toUpperCase() == 'TEACHER',
        ),
        false,
      );
      expect(
        threads.any((ChatThreadSummary t) => t.peerId == 'student_9'),
        true,
      );
    });

    test(
      'fetchInbox seeds teacher direct with user-specific thread id',
      () async {
        final BackendService backend = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method == 'GET' &&
                request.url.queryParameters['action'] ==
                    'list_chat_directory') {
              return http.Response(jsonEncode(<dynamic>[]), 200);
            }
            if (request.method == 'GET' &&
                request.url.queryParameters['action'] == 'get_doubts') {
              return http.Response(jsonEncode(<dynamic>[]), 200);
            }
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true}),
              200,
            );
          }),
        );

        final ChatService service = ChatService(backendService: backend);
        final List<ChatThreadSummary> threads = await service.fetchInbox(
          myUserId: 'student_42',
          myName: 'Aman',
          role: 'student',
          includeDirectorySeed: false,
        );

        final ChatThreadSummary teacher = threads.firstWhere(
          (ChatThreadSummary t) => !t.isDoubtThread && t.peerId == 'TEACHER',
        );
        expect(teacher.threadId, 'student_42|teacher');
        expect(
          teacher.participants,
          containsAll(<String>['student_42', 'TEACHER']),
        );
      },
    );

    test(
      'sendReadReceipt posts receipt payload with message metadata',
      () async {
        final List<Map<String, dynamic>> posted = <Map<String, dynamic>>[];

        final BackendService backend = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method == 'POST') {
              posted.add(jsonDecode(request.body) as Map<String, dynamic>);
            }
            if (request.method == 'GET') {
              return http.Response(jsonEncode(<dynamic>[]), 200);
            }
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true}),
              200,
            );
          }),
        );

        final ChatService service = ChatService(backendService: backend);
        await service.sendReadReceipt(
          thread: const ChatThreadSummary(
            threadId: 'student_1|student_2',
            title: 'Riya',
            peerId: 'student_2',
            peerName: 'Riya',
            lastMessage: '',
            updatedAtMillis: 0,
            unread: false,
            isDoubtThread: false,
          ),
          myUserId: 'student_1',
          myName: 'Aman',
          messageId: 'msg_123',
          seenAtMillis: 1700001000000,
        );

        final bool found = posted.any((Map<String, dynamic> body) {
          final Map<String, dynamic> payload = Map<String, dynamic>.from(
            (body['payload'] as Map?) ?? <String, dynamic>{},
          );
          final Map<String, dynamic> meta = Map<String, dynamic>.from(
            (payload['payload'] as Map?) ?? <String, dynamic>{},
          );
          return (body['action'] == 'send_message' ||
                  body['action'] == 'peer_send') &&
              body['is_peer'] == true &&
              body['chat_id'] == 'student_1|student_2' &&
              payload['type'] == 'read_receipt' &&
              meta['message_id'] == 'msg_123' &&
              meta['reader_id'] == 'student_1' &&
              meta['reader_name'] == 'Aman' &&
              meta['seen_at'] == 1700001000000;
        });

        expect(found, true);
      },
    );

    test(
      'fetchInbox preview uses last display message and skips control messages',
      () async {
        final BackendService backend = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method == 'GET' &&
                request.url.queryParameters['action'] ==
                    'list_chat_directory') {
              return http.Response(
                jsonEncode(<Map<String, dynamic>>[
                  <String, dynamic>{
                    'chat_id': 'student_1|student_2',
                    'friend_id': 'student_2',
                    'friend_name': 'Riya',
                    'last_msg': '',
                    'time': 3000,
                    'messages': jsonEncode(<Map<String, dynamic>>[
                      <String, dynamic>{
                        'id': 'm1',
                        'sender': 'student_2',
                        'senderName': 'Riya',
                        'text': 'Actual last user message',
                        'time': 1000,
                        'type': 'text',
                      },
                      <String, dynamic>{
                        'id': 'rr1',
                        'sender': 'student_1',
                        'senderName': 'Aman',
                        'text': '',
                        'time': 3000,
                        'type': 'read_receipt',
                        'payload': <String, dynamic>{
                          'message_id': 'm1',
                          'reader_id': 'student_1',
                        },
                      },
                    ]),
                  },
                ]),
                200,
              );
            }
            if (request.method == 'GET' &&
                request.url.queryParameters['action'] == 'get_doubts') {
              return http.Response(jsonEncode(<dynamic>[]), 200);
            }
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true}),
              200,
            );
          }),
        );

        final ChatService service = ChatService(backendService: backend);
        final List<ChatThreadSummary> threads = await service.fetchInbox(
          myUserId: 'student_1',
          myName: 'Aman',
          role: 'student',
          includeDirectorySeed: false,
        );
        final ChatThreadSummary thread = threads.firstWhere(
          (ChatThreadSummary t) => t.peerId == 'student_2',
        );
        expect(thread.lastMessage, 'Actual last user message');
      },
    );
  });
}
