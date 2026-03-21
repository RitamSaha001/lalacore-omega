import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:lalacore_rebuild/services/ai_engine_service.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';

void main() {
  group('AiEngineService', () {
    test(
      'sendChat falls back to script backend when local backend is unreachable',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.host == 'script.google.com') {
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            expect(
              (body['action'] ?? '').toString(),
              anyOf('ai_solve', 'ai_chat'),
            );
            return http.Response(
              jsonEncode(<String, dynamic>{
                'ok': true,
                'answer': 'x = 4',
                'explanation': 'From x + 3 = 7, subtract 3 to get x = 4.',
                'provider': 'script-backend',
                'model': 'script-v1',
              }),
              200,
            );
          }
          throw http.ClientException('Connection refused');
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: 'Solve x + 3 = 7',
          userId: 'u1',
          chatId: 'c1',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect((response['answer'] ?? '').toString(), contains('x = 4'));
        expect((response['provider'] ?? '').toString(), 'script-backend');
      },
    );

    test('sendChat can use local /solve fallback for text', () async {
      final MockClient client = MockClient((http.Request request) async {
        if (request.url.path == '/solve') {
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'ok',
              'final_answer': 'x = 3',
              'reasoning': 'From 2x+1=7, x=3.',
              'winner_provider': 'openrouter',
              'engine': <String, dynamic>{'version': 'research-grade-v2'},
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
      });

      final BackendService backend = BackendService(httpClient: client);
      final AiEngineService ai = AiEngineService(
        backendService: backend,
        httpClient: client,
      );

      final Map<String, dynamic> response = await ai.sendChat(
        prompt: 'Solve 2x+1=7',
        userId: 'u1',
        chatId: 'c1',
      );

      expect(response['ok'], true);
      expect((response['answer'] ?? '').toString(), contains('x = 3'));
      expect((response['provider'] ?? '').toString(), 'openrouter');
      expect((response['model'] ?? '').toString(), 'research-grade-v2');
    });

    test('sendChat normalizes nested visualization payloads', () async {
      final MockClient client = MockClient((http.Request request) async {
        if (request.url.path == '/solve') {
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'ok',
              'final_answer': 'Parabola and line plotted.',
              'visualization': <String, dynamic>{
                'graph': <String, dynamic>{
                  'equations': <dynamic>[
                    'y=x^2',
                    <String, dynamic>{'expression': 'x=2'},
                  ],
                  'window': <String, dynamic>{
                    'left': -5,
                    'right': 5,
                    'bottom': -4,
                    'top': 8,
                  },
                },
              },
            }),
            200,
          );
        }
        return http.Response('{}', 200);
      });

      final BackendService backend = BackendService(httpClient: client);
      final AiEngineService ai = AiEngineService(
        backendService: backend,
        httpClient: client,
      );

      final Map<String, dynamic> response = await ai.sendChat(
        prompt: 'Plot y=x^2 and x=2',
        userId: 'u1',
        chatId: 'c1',
      );

      final Map<String, dynamic> visualization =
          response['visualization'] as Map<String, dynamic>;
      expect(visualization['type'], 'desmos');
      expect((visualization['expressions'] as List<dynamic>).length, 2);
      expect(
        (visualization['viewport'] as Map<String, dynamic>)['xmin'],
        isNotNull,
      );
    });

    test('sendChat keeps inequality symbols in visualization latex', () async {
      final MockClient client = MockClient((http.Request request) async {
        if (request.url.path == '/solve') {
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'ok',
              'final_answer': 'Region plotted.',
              'visualization': <String, dynamic>{
                'equations': <dynamic>['y<x+2', 'y>=x-1'],
              },
            }),
            200,
          );
        }
        return http.Response('{}', 200);
      });

      final BackendService backend = BackendService(httpClient: client);
      final AiEngineService ai = AiEngineService(
        backendService: backend,
        httpClient: client,
      );

      final Map<String, dynamic> response = await ai.sendChat(
        prompt: 'Plot inequalities',
        userId: 'u1',
        chatId: 'c1',
      );

      final Map<String, dynamic> visualization =
          response['visualization'] as Map<String, dynamic>;
      final List<dynamic> expressions =
          visualization['expressions'] as List<dynamic>;
      final String first = (expressions.first as Map<String, dynamic>)['latex']
          .toString();
      expect(first.contains('<'), true);
    });

    test(
      'sendChat handles a hard JEE-style prompt with structured answer',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'final_answer': 'For x^3-6x^2+11x-6=0, roots are 1,2,3.',
                'reasoning':
                    'Use Vieta and factorization: (x-1)(x-2)(x-3)=0 after checking integer roots.',
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
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt:
              'JEE Advanced: Solve x^3-6x^2+11x-6=0 and verify all real roots with method.',
          userId: 'u_hard',
          chatId: 'c_hard',
        );

        expect(response['ok'], true);
        final String answer = (response['answer'] ?? '')
            .toString()
            .toLowerCase();
        expect(answer, contains('roots'));
        expect(answer, contains('1'));
        expect(answer, contains('2'));
        expect(answer, contains('3'));
      },
    );

    test(
      'sendChat retries with stronger provider hints when confidence is low',
      () async {
        int qualityRetryCalls = 0;
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }
          if (request.url.path == '/app/action') {
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            final Map<String, dynamic> options = Map<String, dynamic>.from(
              body['options'] as Map? ?? const <String, dynamic>{},
            );
            final bool qualityRetry = options['quality_retry'] == true;
            if (qualityRetry) {
              qualityRetryCalls += 1;
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'status': 'ok',
                  'answer':
                      'Final answer: E(P)=E_full(P)-E_removed(P), and V(A)-V(B)= - integral_A_to_B E.dl.',
                  'explanation':
                      'Using superposition and symmetry, compute enclosed charge profile, derive E(r), then evaluate along cavity-axis coordinates.',
                  'provider': 'openrouter',
                  'model': 'gpt-4o',
                  'confidence': 'High',
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'answer':
                    '1) Electric field vector at an arbitrary point P inside the cavity:',
                'explanation':
                    'To solve this problem, we will use superposition principles.',
                'provider': 'hf',
                'model': 'research-grade-v2',
                'confidence': 'Low',
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
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: <String>[
            'Very hard JEE Advanced electrostatics problem.',
            '1) Derive E at point P inside off-center cavity.',
            '2) Find V(A)-V(B).',
            '3) Find speed at B.',
          ].join('\n'),
          userId: 'u_quality',
          chatId: 'c_quality',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect(qualityRetryCalls, greaterThan(0));
        expect((response['confidence'] ?? '').toString().toLowerCase(), 'high');
        expect((response['answer'] ?? '').toString(), contains('Final answer'));
      },
    );

    test(
      'sendChat retries when initial answer is insufficient even with medium confidence',
      () async {
        int callCount = 0;
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }
          if (request.url.path == '/app/action') {
            callCount += 1;
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            final Map<String, dynamic> options = Map<String, dynamic>.from(
              body['options'] as Map? ?? const <String, dynamic>{},
            );
            if (options['quality_retry'] == true) {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'status': 'ok',
                  'answer': 'Final answer: v_B = sqrt((2q/m)(V_A - V_B)).',
                  'explanation':
                      'By energy conservation and derived potential difference, substitute signs and evaluate speed magnitude.',
                  'provider': 'gemini',
                  'model': 'gemini-pro',
                  'confidence': 'High',
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'answer': '2) Potential difference between A and B:',
                'explanation':
                    'To solve this problem, we will use superposition and potential formulas.',
                'provider': 'hf',
                'model': 'research-grade-v2',
                'confidence': 'Medium',
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
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt:
              'Find potential difference and final speed. Return complete derivation with equations and final answer.',
          userId: 'u_insufficient',
          chatId: 'c_insufficient',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect(callCount, greaterThan(1));
        expect((response['answer'] ?? '').toString(), contains('Final answer'));
        expect((response['provider'] ?? '').toString(), isNotEmpty);
      },
    );

    test(
      'materialGenerate falls back to chat/ocr pipeline when action is unknown',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/app/action') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'final_answer': 'Electrostatics quick summary',
                'reasoning':
                    'Use Coulomb law, superposition, and field-line direction checks.',
              }),
              200,
            );
          }
          return http.Response('{}', 200);
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.materialGenerate(
          materialId: 'mat_1',
          mode: 'summarize',
          title: 'Electrostatics',
          options: <String, dynamic>{
            'source_type': 'image',
            'source_url': 'https://example.com/electrostatics.png',
          },
        );

        expect(response['ok'], true);
        expect(
          (response['content'] ?? '').toString().toLowerCase(),
          contains('electrostatics'),
        );
      },
    );

    test(
      'materialGenerate returns robust local fallback when all backends fail',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'UNKNOWN_ACTION',
              'message': 'unknown action',
            }),
            200,
          );
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.materialGenerate(
          materialId: 'mat_2',
          mode: 'jee_notes',
          title: 'Kinematics',
          options: const <String, dynamic>{'source_type': 'pdf'},
        );

        expect(response['ok'], true);
        expect((response['content'] ?? '').toString(), contains('JEE Notes'));
      },
    );

    test(
      'sendChat retries with exam_coach when structured style is rejected',
      () async {
        final List<String> solveStyles = <String>[];
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            final Map<String, dynamic> body =
                jsonDecode(request.body) as Map<String, dynamic>;
            final Map<String, dynamic> options = Map<String, dynamic>.from(
              body['options'] as Map,
            );
            final String style = (options['response_style'] ?? '').toString();
            solveStyles.add(style);
            if (style == 'exam_coach') {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'status': 'ok',
                  'final_answer': 'x = 5',
                  'reasoning': 'From x + 2 = 7, x = 5.',
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'FAILED',
                'message': 'unsupported response_style',
              }),
              200,
            );
          }
          if (request.url.path == '/app/action') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'FAILED',
                'message': 'unsupported response_style',
              }),
              200,
            );
          }
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'FAILED',
              'message': 'unsupported response_style',
            }),
            200,
          );
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: 'Solve x + 2 = 7',
          userId: 'u1',
          chatId: 'c1',
          responseStyle: 'structured_exam_solution',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect((response['answer'] ?? '').toString(), contains('x = 5'));
        expect(solveStyles, contains('structured_exam_solution'));
        expect(solveStyles, contains('exam_coach'));
      },
    );

    test(
      'sendChat keeps degraded provider-none payload and reports engine error',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          return http.Response(
            jsonEncode(<String, dynamic>{
              'ok': true,
              'provider': 'none',
              'model': 'none',
              'answer':
                  'I cannot fully solve this right now, but here is what I can infer:\n\nwhat is vernier calliper and how to measure with it',
              'meta': <String, dynamic>{'degraded': true},
            }),
            200,
          );
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: 'what is vernier calliper and how to measure with it',
          userId: 'u1',
          chatId: 'c1',
          enablePersona: false,
        );

        expect(response['ok'], false);
        expect(response['provider'], 'none');
        expect(response['status'], 'DEGRADED_ENGINE_OUTPUT');
        final String answer = (response['answer'] ?? '')
            .toString()
            .toLowerCase();
        expect(answer, contains('vernier'));
      },
    );

    test(
      'sendChat does not fabricate quiz json when quiz payload is degraded',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          return http.Response(
            jsonEncode(<String, dynamic>{
              'ok': true,
              'provider': 'none',
              'model': 'none',
              'answer':
                  'I cannot fully solve this right now, but here is what I can infer:\n\nquiz generation failed',
              'meta': <String, dynamic>{'degraded': true},
            }),
            200,
          );
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: <String>[
            'Generate a JEE Advanced custom practice quiz in strict JSON only.',
            'Question count: 5',
            'Distribution must be exact: numerical=2, mcq=1, multi=2.',
            'Chapters: Binomial Theorem',
          ].join('\n'),
          userId: 'u1',
          chatId: 'c1',
          function: 'ai_generate_quiz',
          responseStyle: 'structured_json',
          enablePersona: false,
        );

        expect(response['ok'], false);
        expect(response['provider'], 'none');
        expect(response['status'], 'DEGRADED_ENGINE_OUTPUT');
        expect(
          (response['answer'] ?? '').toString().toLowerCase(),
          contains('quiz generation failed'),
        );
      },
    );

    test(
      'sendChat retries quiz generation when payload is repetitive/easy and promotes stronger set',
      () async {
        int appActionCalls = 0;
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }
          if (request.url.path == '/app/action') {
            appActionCalls++;
            if (appActionCalls == 1) {
              return http.Response(
                jsonEncode(<String, dynamic>{
                  'status': 'ok',
                  'provider': 'gemini',
                  'model': 'gemini-pro',
                  'confidence': 'High',
                  'answer': jsonEncode(<String, dynamic>{
                    'questions': <Map<String, dynamic>>[
                      <String, dynamic>{
                        'question_text': 'What is 2 + 2?',
                        'difficulty': 1,
                      },
                      <String, dynamic>{
                        'question_text': 'What is 3 + 3?',
                        'difficulty': 1,
                      },
                      <String, dynamic>{
                        'question_text': 'What is 4 + 4?',
                        'difficulty': 1,
                      },
                      <String, dynamic>{
                        'question_text': 'What is 5 + 5?',
                        'difficulty': 1,
                      },
                      <String, dynamic>{
                        'question_text': 'What is 6 + 6?',
                        'difficulty': 1,
                      },
                    ],
                  }),
                }),
                200,
              );
            }
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'provider': 'openrouter',
                'model': 'gpt-4o',
                'confidence': 'High',
                'answer': jsonEncode(<String, dynamic>{
                  'questions': <Map<String, dynamic>>[
                    <String, dynamic>{
                      'question_text':
                          'Evaluate determinant of a 3x3 matrix with parameter and identify the singular value.',
                      'type': 'MCQ',
                      'difficulty': 4,
                    },
                    <String, dynamic>{
                      'question_text':
                          'Find number of 5-digit numbers divisible by 3 formed from 0..5 without repetition.',
                      'type': 'MULTI',
                      'difficulty': 5,
                    },
                    <String, dynamic>{
                      'question_text':
                          'Compute coefficient of x^8 in (1 + 2x)^12 under parity constraint.',
                      'type': 'INTEGER',
                      'difficulty': 4,
                    },
                    <String, dynamic>{
                      'question_text':
                          'Use case split to solve |x-2| + |2x+1| = 7 exactly.',
                      'type': 'MCQ',
                      'difficulty': 4,
                    },
                    <String, dynamic>{
                      'question_text':
                          'A constrained probability setup with conditional event and Bayes update.',
                      'type': 'NUMERICAL',
                      'difficulty': 5,
                    },
                  ],
                }),
              }),
              200,
            );
          }
          return http.Response('{}', 200);
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: <String>[
            'Generate a JEE Advanced hard quiz in strict JSON only.',
            'Question count: 5',
            'Need diverse non-routine patterns.',
          ].join('\n'),
          userId: 'u_q_retry',
          chatId: 'c_q_retry',
          function: 'ai_generate_quiz',
          responseStyle: 'structured_json',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect(appActionCalls, greaterThanOrEqualTo(2));
        final Map<String, dynamic> retry = Map<String, dynamic>.from(
          response['quality_retry'] as Map? ?? const <String, dynamic>{},
        );
        expect(
          (retry['attempted'] as num?)?.toInt() ?? 0,
          greaterThanOrEqualTo(1),
        );
        expect(
          (response['answer'] ?? '').toString().toLowerCase(),
          contains('determinant'),
        );
      },
    );

    test(
      'sendChat surfaces backend failure reason when answer is empty',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          return http.Response(
            jsonEncode(<String, dynamic>{
              'status': 'FAILED',
              'message': 'unknown action',
            }),
            200,
          );
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: 'Test prompt',
          userId: 'u1',
          chatId: 'c1',
          responseStyle: 'exam_coach',
        );

        expect(response['ok'], false);
        expect(response['status'], 'FAILED');
        expect(
          (response['error'] ?? '').toString().toLowerCase(),
          contains('unknown action'),
        );
        final String answer = (response['answer'] ?? '').toString().trim();
        final String explanation = (response['explanation'] ?? '')
            .toString()
            .trim();
        expect(answer.isNotEmpty || explanation.isNotEmpty, false);
      },
    );

    test(
      'sendChat applies local rescue for low-confidence truncated conceptual responses',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'UNKNOWN_ACTION',
                'message': 'unknown action',
              }),
              200,
            );
          }
          if (request.url.path == '/app/action') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'answer':
                    'A Vernier caliper is a precision measuring instrument used to measure linear dimensions and to read the main scale mark just',
                'explanation':
                    'Reasoning: The candidate answer is incomplete as it cuts off mid-sentence.',
                'provider': 'gemini',
                'model': 'research-grade-v2',
                'confidence': 'Low',
              }),
              200,
            );
          }
          return http.Response('{}', 200);
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: 'what is vernier calliper and how to measure with it',
          userId: 'u1',
          chatId: 'c1',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect(response['provider'], 'local-fallback');
        expect(
          (response['answer'] ?? '').toString().toLowerCase(),
          contains('vernier caliper'),
        );
        final Map<String, dynamic> rescue = Map<String, dynamic>.from(
          response['rescue_applied'] as Map? ?? const <String, dynamic>{},
        );
        expect(
          (rescue['trigger'] ?? '').toString(),
          anyOf('low_confidence_conceptual', 'low_confidence_truncated_answer'),
        );
      },
    );

    test(
      'sendChat preserves graph responses for graph prompts without local rescue override',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'final_answer':
                    'Graph plotted with line and parabola intersection.',
                'reasoning': 'Use simultaneous solution for y=x^2 and x=2.',
                'confidence': 'Low',
                'provider': 'gemini',
                'model': 'research-grade-v2',
                'visualization': <String, dynamic>{
                  'expressions': <dynamic>[
                    'y=x^2',
                    <String, dynamic>{
                      'expression': 'x=2',
                      'lineStyle': 'dashed',
                    },
                  ],
                },
              }),
              200,
            );
          }
          return http.Response('{}', 200);
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt: 'Plot y=x^2 and x=2 on same graph.',
          userId: 'u1',
          chatId: 'c_graph',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect(response['provider'], isNot('local-fallback'));
        final Map<String, dynamic> visualization = Map<String, dynamic>.from(
          response['visualization'] as Map? ?? const <String, dynamic>{},
        );
        expect(visualization.isNotEmpty, true);
      },
    );

    test(
      'sendChat infers medium confidence when calibration is zero due to missing ground truth',
      () async {
        final MockClient client = MockClient((http.Request request) async {
          if (request.url.path == '/solve') {
            return http.Response(
              jsonEncode(<String, dynamic>{
                'status': 'ok',
                'final_answer':
                    'Using inclusion-exclusion and divisibility constraints, the total valid numbers are 216.',
                'reasoning':
                    'Compute all 5-digit permutations without leading zero, apply mod-3 residue class counting over selected digit subsets, then subtract invalid zero-leading arrangements.',
                'provider': 'gemini',
                'model': 'research-grade-v2',
                'confidence': 0.0,
                'verification': <String, dynamic>{
                  'failure_reason': 'missing_ground_truth',
                  'reason': 'No expected answer found',
                  'risk_score': 0.35,
                  'plausibility': <String, dynamic>{'score': 1.0},
                },
              }),
              200,
            );
          }
          return http.Response('{}', 200);
        });

        final BackendService backend = BackendService(httpClient: client);
        final AiEngineService ai = AiEngineService(
          backendService: backend,
          httpClient: client,
        );

        final Map<String, dynamic> response = await ai.sendChat(
          prompt:
              'A 5 digit number divisible by 3 is formed from 0,1,2,3,4,5 without repetition. Find total ways.',
          userId: 'u1',
          chatId: 'c_confidence',
          enablePersona: false,
        );

        expect(response['ok'], true);
        expect(
          (response['confidence'] ?? '').toString().toLowerCase(),
          'medium',
        );
      },
    );
  });
}
