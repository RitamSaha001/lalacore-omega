import 'package:flutter/material.dart';

import 'src/ai_engine_client.dart';

void main() {
  runApp(const AiEngineApp());
}

class AiEngineApp extends StatelessWidget {
  const AiEngineApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'AI Engine Client',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF0B7285)),
        useMaterial3: true,
      ),
      home: const AiEngineHomePage(),
    );
  }
}

class AiEngineHomePage extends StatefulWidget {
  const AiEngineHomePage({super.key});

  @override
  State<AiEngineHomePage> createState() => _AiEngineHomePageState();
}

class _AiEngineHomePageState extends State<AiEngineHomePage> {
  final TextEditingController _promptController = TextEditingController();
  final AiEngineClient _client = AiEngineClient();

  String _response = '';
  String _error = '';
  bool _isLoading = false;

  @override
  void dispose() {
    _promptController.dispose();
    super.dispose();
  }

  Future<void> _sendPrompt() async {
    final String prompt = _promptController.text.trim();
    if (prompt.isEmpty) {
      setState(() {
        _error = 'Enter a prompt before sending.';
        _response = '';
      });
      return;
    }

    setState(() {
      _isLoading = true;
      _error = '';
      _response = '';
    });

    try {
      final String result = await _client.generate(prompt);
      setState(() {
        _response = result;
      });
    } on Exception catch (exception) {
      setState(() {
        _error = exception.toString();
      });
    } finally {
      setState(() {
        _isLoading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('AI Engine Client')),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: <Widget>[
            const Text('Prompt', style: TextStyle(fontWeight: FontWeight.w600)),
            const SizedBox(height: 8),
            TextField(
              controller: _promptController,
              minLines: 3,
              maxLines: 6,
              decoration: const InputDecoration(
                border: OutlineInputBorder(),
                hintText: 'Ask your AI engine...',
              ),
            ),
            const SizedBox(height: 12),
            FilledButton.icon(
              onPressed: _isLoading ? null : _sendPrompt,
              icon: _isLoading
                  ? const SizedBox(
                      width: 16,
                      height: 16,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Icon(Icons.send),
              label: Text(_isLoading ? 'Sending...' : 'Send'),
            ),
            const SizedBox(height: 16),
            if (_error.isNotEmpty)
              Text(
                _error,
                style: TextStyle(color: Theme.of(context).colorScheme.error),
              ),
            if (_response.isNotEmpty) ...<Widget>[
              const Text(
                'Response',
                style: TextStyle(fontWeight: FontWeight.w600),
              ),
              const SizedBox(height: 8),
              Expanded(
                child: SingleChildScrollView(child: SelectableText(_response)),
              ),
            ],
          ],
        ),
      ),
    );
  }
}
