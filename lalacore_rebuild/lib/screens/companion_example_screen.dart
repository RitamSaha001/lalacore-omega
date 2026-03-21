import 'package:flutter/material.dart';

import '../widgets/ai_companion_controller.dart';
import '../widgets/ai_companion_widget.dart';

class CompanionExampleScreen extends StatefulWidget {
  const CompanionExampleScreen({super.key});

  @override
  State<CompanionExampleScreen> createState() => _CompanionExampleScreenState();
}

class _CompanionExampleScreenState extends State<CompanionExampleScreen> {
  final AICompanionController _controller = AICompanionController();

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () => FocusScope.of(context).unfocus(),
      child: Scaffold(
        appBar: AppBar(title: const Text('AI Companion Demo')),
        body: Stack(
          children: <Widget>[
            const Positioned.fill(
              child: Padding(
                padding: EdgeInsets.all(20),
                child: Text(
                  'Chat UI placeholder\n\nUse this screen to validate CompanionSM triggers and TTS sync.',
                  style: TextStyle(fontSize: 16, height: 1.4),
                ),
              ),
            ),
            Positioned(
              left: 0,
              right: 0,
              bottom: 8,
              child: Center(
                child: AICompanionWidget(controller: _controller, height: 160),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
