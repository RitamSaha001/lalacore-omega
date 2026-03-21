import 'dart:convert';

import 'package:lalacore_rebuild/services/ai_engine_service.dart';
import 'package:lalacore_rebuild/services/backend_service.dart';

Future<void> main() async {
  final backend = BackendService();
  final ai = AiEngineService(backendService: backend);

  final prompts = <Map<String, String>>[
    {
      'name': 'conceptual_vernier',
      'prompt': 'what is vernier calliper and how to measure with it',
    },
    {
      'name': 'hard_combinatorics',
      'prompt': 'A 5 digit number divisible by 3 is to be formed using numerals 0,1,2,3,4,5 without repetition. Find total ways with full derivation and final answer.',
    },
    {
      'name': 'graph_plot',
      'prompt': 'Plot y=x^2 and x=2 on same graph and explain intersection.',
    },
    {
      'name': 'sum_digits_perm',
      'prompt': 'Sum of all numbers formed using digits 2,3,3,4,4,4 using all digits exactly once.',
    },
  ];

  final results = <Map<String, dynamic>>[];
  for (var round = 1; round <= 3; round++) {
    for (final p in prompts) {
      final res = await ai.sendChat(
        prompt: p['prompt']!,
        userId: 'sweep_user',
        chatId: 'sweep_${DateTime.now().millisecondsSinceEpoch}_$round',
        function: 'general_chat',
        responseStyle: 'exam_coach',
        enablePersona: false,
      );
      final ans = (res['answer'] ?? '').toString();
      final exp = (res['explanation'] ?? '').toString();
      final vis = res['visualization'];
      final status = (res['status'] ?? '').toString();
      results.add({
        'round': round,
        'case': p['name'],
        'ok': res['ok'] == true,
        'provider': (res['provider'] ?? '').toString(),
        'model': (res['model'] ?? '').toString(),
        'confidence': (res['confidence'] ?? '').toString(),
        'status': status,
        'answer_len': ans.length,
        'explanation_len': exp.length,
        'has_visualization': vis is Map && vis.isNotEmpty,
        'visualization_expr_count': vis is Map && vis['expressions'] is List
            ? (vis['expressions'] as List).length
            : 0,
        'rescue_trigger': (res['rescue_applied'] is Map)
            ? (res['rescue_applied']['trigger'] ?? '').toString()
            : '',
      });
    }
  }

  final material = await ai.materialGenerate(
    materialId: 'sweep_material',
    mode: 'summary',
    title: 'Electrostatics',
    options: const {'source_type': 'text'},
  );

  final classSummary = await ai.classSummary(const [
    {'score': 34, 'total': 100},
    {'score': 66, 'total': 100},
    {'score': 78, 'total': 100},
  ]);

  final studentIntel = await ai.studentIntelligence(
    accountId: 'student_123',
    latestResult: const {'score': 62, 'maxScore': 100},
    history: const [
      {'score': 40, 'maxScore': 100},
      {'score': 55, 'maxScore': 100},
      {'score': 62, 'maxScore': 100},
    ],
  );

  final examAnalysis = await ai.analyzeExam(const {
    'score': 58,
    'maxScore': 100,
  });

  final report = {
    'chat_runs': results,
    'material_ok': material['ok'] == true,
    'material_content_len': (material['content'] ?? '').toString().length,
    'class_summary_ok': classSummary['ok'] == true,
    'student_intel_ok': studentIntel['ok'] == true,
    'exam_analysis_ok': examAnalysis['ok'] == true,
  };

  print(const JsonEncoder.withIndent('  ').convert(report));
}
