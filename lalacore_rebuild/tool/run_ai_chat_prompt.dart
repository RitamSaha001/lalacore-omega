import 'dart:convert';

import 'package:lalacore_rebuild/services/ai_engine_service.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';

Future<void> main() async {
  const String prompt = 'what is vernier calliper and how to measure with it';

  final BackendService backend = BackendService();
  final AiEngineService ai = AiEngineService(backendService: backend);

  try {
    final Map<String, dynamic> response = await ai.sendChat(
      prompt: prompt,
      userId: 'manual_test_user',
      chatId: 'manual_test_chat_${DateTime.now().millisecondsSinceEpoch}',
      function: 'general_chat',
      responseStyle: 'structured_exam_solution',
      enablePersona: false,
      card: const <String, dynamic>{
        'surface': 'manual_cli_test',
        'source': 'codex_terminal',
      },
    );

    print('ok=${response['ok']} provider=${response['provider']} model=${response['model']}');
    print('---ANSWER---');
    print((response['answer'] ?? '').toString());
    print('---EXPLANATION---');
    print((response['explanation'] ?? '').toString());
    print('---FULL JSON---');
    print(const JsonEncoder.withIndent('  ').convert(response));
  } catch (e, st) {
    print('ERROR: $e');
    print(st);
  }
}
