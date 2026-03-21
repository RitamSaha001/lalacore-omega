import 'dart:async';
import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';

void main() {
  group('BackendService content operations', () {
    test(
      'createQuiz prefers local app-action endpoint when available',
      () async {
        final List<String> paths = <String>[];

        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            paths.add(request.url.path);
            if (request.url.path == '/app/action') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'ok': true,
                  'status': 'SUCCESS',
                  'id': 'quiz_local_1',
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }),
        );

        final Map<String, dynamic> response = await service.createQuiz(
          <String, dynamic>{
            'title': 'Local Quiz',
            'type': 'Exam',
            'class': 'Class 11',
            'chapters': 'Kinematics',
            'questions': <Map<String, dynamic>>[
              <String, dynamic>{'text': 'x=2', 'correct': '2'},
            ],
          },
        );

        expect(paths.first, '/app/action');
        expect(service.isSuccessfulResponse(response), true);
      },
    );

    test(
      'createQuiz falls back to alternate action and treats SUCCESS as ok',
      () async {
        final List<String> actions = <String>[];

        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method != 'POST') {
              return http.Response('{}', 200);
            }
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            final String action = (body['action'] ?? '').toString();
            actions.add(action);
            if (action == 'create_quiz') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'status': 'UNKNOWN_ACTION',
                  'message': 'unknown action',
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{'status': 'SUCCESS'}),
              200,
            );
          }),
        );

        final Map<String, dynamic> response = await service.createQuiz(
          <String, dynamic>{
            'title': 'Algebra Test',
            'type': 'Exam',
            'class': 'Class 11',
            'chapters': 'Quadratic Equations',
            'duration': 60,
            'questions': <Map<String, dynamic>>[
              <String, dynamic>{'text': 'x^2 = 4', 'correct': '2,-2'},
            ],
          },
        );

        expect(actions, contains('create_quiz'));
        expect(actions, contains('create_assessment'));
        expect(service.isSuccessfulResponse(response), true);
      },
    );

    test(
      'addMaterial falls back to alternate action when primary is unknown',
      () async {
        final List<String> actions = <String>[];

        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method != 'POST') {
              return http.Response('{}', 200);
            }
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            final String action = (body['action'] ?? '').toString();
            actions.add(action);

            if (action == 'add_material') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'status': 'UNKNOWN_ACTION',
                  'message': 'unknown action',
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true, 'status': 'SAVED'}),
              200,
            );
          }),
        );

        final Map<String, dynamic> response = await service
            .addMaterial(<String, dynamic>{
              'class': 'Class 12',
              'subject': 'Physics',
              'chapters': 'Electrostatics',
              'title': 'Electrostatics Notes',
              'type': 'pdf',
              'url': 'https://example.com/notes.pdf',
              'description': 'Revision sheet',
            });

        expect(actions.first, 'add_material');
        expect(actions, contains('create_material'));
        expect(service.isSuccessfulResponse(response), true);
      },
    );

    test('addMaterial supports study-material action aliases', () async {
      final List<String> actions = <String>[];

      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method != 'POST') {
            return http.Response('{}', 200);
          }
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          final String action = (body['action'] ?? '').toString();
          actions.add(action);
          if (action == 'add_study_material') {
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true, 'status': 'SUCCESS'}),
              200,
            );
          }
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'UNKNOWN_ACTION',
              'message': 'unknown action',
            }),
            200,
          );
        }),
      );

      final Map<String, dynamic> response = await service
          .addMaterial(<String, dynamic>{
            'class': 'Class 11',
            'subject': 'Chemistry',
            'chapters': 'Chemical Bonding',
            'title': 'Bonding Quick Notes',
            'type': 'pdf',
            'url': 'https://example.com/bonding.pdf',
            'description': 'Alias compatibility check',
          });

      expect(actions, contains('add_study_material'));
      expect(service.isSuccessfulResponse(response), true);
    });

    test('uploadFileData accepts alternate response url keys', () async {
      final List<String> actions = <String>[];

      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          actions.add((body['action'] ?? '').toString());
          if (body['action'] == 'upload_file') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'SUCCESS',
              'drive_url': 'https://drive.google.com/file/d/xyz/view',
            }),
            200,
          );
        }),
      );

      final String url = await service.uploadFileData(
        fileName: 'sample.pdf',
        dataUrl: 'data:application/pdf;base64,AAAA',
      );

      expect(actions, contains('upload_file'));
      expect(actions, contains('upload_file_data'));
      expect(url, contains('drive.google.com'));
    });

    test('postJsonAction times out redirected GET follow-up', () async {
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.method == 'POST') {
            return http.Response(
              '',
              302,
              headers: <String, String>{
                'location': 'https://example.com/final',
              },
            );
          }
          await Future<void>.delayed(const Duration(milliseconds: 120));
          return http.Response('{"ok":true}', 200);
        }),
      );

      await expectLater(
        service.postJsonAction(<String, dynamic>{
          'action': 'health_check',
        }, timeout: const Duration(milliseconds: 25)),
        throwsA(isA<TimeoutException>()),
      );
    });

    test(
      'postLocalSolve skips degraded infer-stub endpoint and tries next host',
      () async {
        final List<String> hosts = <String>[];
        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            hosts.add(request.url.host);
            if (request.url.path != '/solve') {
              return http.Response('{}', 200);
            }
            if (request.url.host == '10.0.2.2') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'ok': true,
                  'provider': 'none',
                  'model': 'none',
                  'answer':
                      'I cannot fully solve this right now, but here is what I can infer.',
                  'meta': <String, dynamic>{'degraded': true},
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'final_answer': '2',
                'reasoning': 'Validated solve payload from local backend.',
                'winner_provider': 'gemini',
              }),
              200,
            );
          }),
        );

        final Map<String, dynamic> response = await service.postLocalSolve(
          <String, dynamic>{'input_type': 'text', 'input_data': 'probe'},
        );

        expect((response['final_answer'] ?? '').toString(), '2');
        expect(hosts, contains('10.0.2.2'));
        expect(hosts, contains('127.0.0.1'));
      },
    );

    test(
      'fetchStudyMaterials can load from local app-action endpoint',
      () async {
        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            if (request.method == 'POST' && request.url.path == '/app/action') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'ok': true,
                  'status': 'SUCCESS',
                  'list': <Map<String, dynamic>>[
                    <String, dynamic>{
                      'material_id': 'mat_1',
                      'title': 'Thermodynamics',
                      'url': 'https://example.com/t.pdf',
                    },
                  ],
                }),
                200,
              );
            }
            return http.Response('[]', 200);
          }),
        );

        final List<dynamic> out = await service.fetchStudyMaterials();
        expect(out, hasLength(1));
        expect((out.first as Map)['title'], 'Thermodynamics');
      },
    );

    test('generateAiQuiz uses ai_generate_quiz action', () async {
      final List<String> actions = <String>[];
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          actions.add((body['action'] ?? '').toString());
          if (body['action'] == 'ai_generate_quiz') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'ok': true,
                'status': 'SUCCESS',
                'quiz_id': 'aiq_1',
                'questions_json': <Map<String, dynamic>>[
                  <String, dynamic>{
                    'question_id': 'q_1',
                    'question_text': 'Solve 2x=6',
                    'options': <String>['1', '2', '3', '4'],
                  },
                ],
              }),
              200,
            );
          }
          return http.Response('{}', 200);
        }),
      );

      final Map<String, dynamic> out = await service.generateAiQuiz(
        <String, dynamic>{
          'subject': 'Math',
          'difficulty': 3,
          'question_count': 1,
        },
      );

      expect(actions.first, 'ai_generate_quiz');
      expect(service.isSuccessfulResponse(out), true);
      expect(out['quiz_id'], 'aiq_1');
    });

    test(
      'generateAiQuiz falls back to generate_ai_quiz when primary is unknown',
      () async {
        final List<String> actions = <String>[];
        final BackendService service = BackendService(
          httpClient: MockClient((http.Request request) async {
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            final String action = (body['action'] ?? '').toString();
            actions.add(action);
            if (action == 'ai_generate_quiz') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'status': 'UNKNOWN_ACTION',
                  'message': 'unknown action',
                }),
                200,
              );
            }
            if (action == 'generate_ai_quiz') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'ok': true,
                  'status': 'SUCCESS',
                  'quiz_id': 'aiq_alias_1',
                  'questions_json': <Map<String, dynamic>>[
                    <String, dynamic>{
                      'question_id': 'q_alias_1',
                      'question_text': 'Find x if x + 2 = 5',
                      'options': <String>['1', '2', '3', '4'],
                    },
                  ],
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }),
        );

        final Map<String, dynamic> out = await service.generateAiQuiz(
          <String, dynamic>{
            'subject': 'Math',
            'difficulty': 2,
            'question_count': 1,
            'role': 'teacher',
          },
        );

        expect(actions.first, 'ai_generate_quiz');
        expect(actions, contains('generate_ai_quiz'));
        expect(service.isSuccessfulResponse(out), true);
        expect(out['quiz_id'], 'aiq_alias_1');
      },
    );

    test('evaluateQuizSubmission reads local answer key response', () async {
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          if (request.url.path == '/app/action') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'ok': true,
                'status': 'SUCCESS',
                'evaluation_result': <String, dynamic>{
                  'score': 8,
                  'max_score': 12,
                  'correct': 2,
                  'wrong': 1,
                  'skipped': 0,
                },
                'answer_key': <Map<String, dynamic>>[
                  <String, dynamic>{
                    'question_index': 0,
                    'correct_answer': 'B) 6',
                  },
                ],
              }),
              200,
            );
          }
          return http.Response('{}', 200);
        }),
      );

      final Map<String, dynamic> out = await service.evaluateQuizSubmission(
        <String, dynamic>{
          'quiz_id': 'aiq_1',
          'answers': <String, dynamic>{
            '0': <String>['A'],
          },
        },
      );

      expect(service.isSuccessfulResponse(out), true);
      expect((out['answer_key'] as List<dynamic>).isNotEmpty, true);
    });

    test('submitResult can persist result via local app action', () async {
      final List<String> actions = <String>[];
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          actions.add((body['action'] ?? '').toString());
          if (body['action'] == 'save_result') {
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true, 'status': 'SUCCESS'}),
              200,
            );
          }
          return http.Response('{}', 200);
        }),
      );

      final Map<String, dynamic> out = await service
          .submitResult(<String, dynamic>{
            'quiz_id': 'q1',
            'quiz_title': 'Mock Quiz',
            'name': 'Ritam',
            'score': 20,
            'max_score': 40,
          });

      expect(actions.first, 'save_result');
      expect(service.isSuccessfulResponse(out), true);
    });

    test('enqueueTeacherReview sends teacher queue action', () async {
      final List<String> actions = <String>[];
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          actions.add((body['action'] ?? '').toString());
          return http.Response(
            jsonEncode(<String, dynamic>{'ok': true, 'status': 'SUCCESS'}),
            200,
          );
        }),
      );

      final Map<String, dynamic> out = await service
          .enqueueTeacherReview(<String, dynamic>{
            'quiz_id': 'q1',
            'question_id': 1,
            'student_id': 's1',
            'student_answer': 'A',
            'correct_answer': 'B',
          });

      expect(actions.first, 'queue_teacher_review');
      expect(service.isSuccessfulResponse(out), true);
    });

    test('lc9ParseImportQuestions uses lc9 parse action', () async {
      final List<String> actions = <String>[];
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          actions.add((body['action'] ?? '').toString());
          if (body['action'] == 'lc9_parse_questions') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'ok': true,
                'status': 'SUCCESS',
                'questions': <Map<String, dynamic>>[
                  <String, dynamic>{'question_id': 'imp_q_1'},
                ],
              }),
              200,
            );
          }
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'UNKNOWN_ACTION',
              'message': 'unknown action',
            }),
            200,
          );
        }),
      );

      final Map<String, dynamic> out = await service.lc9ParseImportQuestions(
        rawText: '1. Test question\nA) a\nB) b\nAns: A',
        meta: <String, dynamic>{'subject': 'Math'},
      );

      expect(actions.first, 'lc9_parse_questions');
      expect(service.isSuccessfulResponse(out), true);
      expect((out['questions'] as List<dynamic>).length, 1);
    });

    test('lc9PublishImportQuestions falls back to alias actions', () async {
      final List<String> actions = <String>[];
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          final String action = (body['action'] ?? '').toString();
          actions.add(action);
          if (action == 'lc9_publish_questions') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }
          if (action == 'publish_import_questions') {
            return http.Response(
              jsonEncode(<String, dynamic>{'ok': true, 'status': 'SUCCESS'}),
              200,
            );
          }
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'UNKNOWN_ACTION',
              'message': 'unknown action',
            }),
            200,
          );
        }),
      );

      final Map<String, dynamic> out = await service.lc9PublishImportQuestions(
        questions: <Map<String, dynamic>>[
          <String, dynamic>{'question_id': 'imp_q_1'},
        ],
        meta: <String, dynamic>{'teacher_id': 'T1'},
      );

      expect(actions.first, 'lc9_publish_questions');
      expect(actions, contains('publish_import_questions'));
      expect(service.isSuccessfulResponse(out), true);
    });

    test('lc9WebVerifyQuery uses cached web verify action', () async {
      final List<String> actions = <String>[];
      final BackendService service = BackendService(
        httpClient: MockClient((http.Request request) async {
          final Map<String, dynamic> body =
              jsonDecode(request.body) as Map<String, dynamic>;
          final String action = (body['action'] ?? '').toString();
          actions.add(action);
          if (action == 'lc9_web_verify_query') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'ok': true,
                'status': 'SUCCESS',
                'rows': <Map<String, dynamic>>[
                  <String, dynamic>{
                    'url': 'https://jeeadv.ac.in/past_qps/2022_1_English.pdf',
                  },
                ],
              }),
              200,
            );
          }
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'UNKNOWN_ACTION',
              'message': 'unknown action',
            }),
            200,
          );
        }),
      );
      final Map<String, dynamic> out = await service.lc9WebVerifyQuery(
        query: 'JEE Advanced quadratic equation PYQ',
      );
      expect(actions.first, 'lc9_web_verify_query');
      expect(service.isSuccessfulResponse(out), true);
      expect((out['rows'] as List<dynamic>).length, 1);
    });
  });
}
