import 'dart:convert';

import 'package:lalacore_rebuild/services/ai_engine_service.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';

Future<void> main() async {
  final backend = BackendService();
  final ai = AiEngineService(backendService: backend);
  final res = await ai.sendChat(
    prompt: 'Plot y=x^2 and x=2 on the same graph and explain intersection.',
    userId: 'graph_test_user',
    chatId: 'graph_test_${DateTime.now().millisecondsSinceEpoch}',
    function: 'general_chat',
    responseStyle: 'exam_coach',
    enablePersona: false,
  );
  final vis = res['visualization'];
  final hasVis = vis is Map && vis.isNotEmpty;
  int exprCount = 0;
  if (hasVis) {
    final expr = (vis as Map)['expressions'];
    if (expr is List) exprCount = expr.length;
  }
  print('ok=${res['ok']} provider=${res['provider']} model=${res['model']} has_visualization=$hasVis expressions=$exprCount');
  print('keys=${res.keys.toList()}');
  print(const JsonEncoder.withIndent('  ').convert({
    'visualization': res['visualization'],
    'web_retrieval': res['web_retrieval'],
    'confidence': res['confidence'],
  }));
}
