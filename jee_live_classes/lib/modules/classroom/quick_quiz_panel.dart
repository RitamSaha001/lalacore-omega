import 'package:flutter/material.dart';

import '../../models/live_poll_model.dart';

enum _QuickQuizMode { manual, ai, import }

class QuickQuizPanel extends StatefulWidget {
  const QuickQuizPanel({
    super.key,
    required this.onStartPoll,
    required this.onGenerateWithAi,
    required this.onLoadImportedPolls,
    required this.onCancel,
  });

  final Future<void> Function(LivePollDraft draft) onStartPoll;
  final Future<LivePollDraft> Function(String topic, String difficulty)
  onGenerateWithAi;
  final Future<List<LivePollDraft>> Function() onLoadImportedPolls;
  final VoidCallback onCancel;

  @override
  State<QuickQuizPanel> createState() => _QuickQuizPanelState();
}

class _QuickQuizPanelState extends State<QuickQuizPanel> {
  final TextEditingController _questionController = TextEditingController();
  final List<TextEditingController> _optionControllers = List.generate(
    4,
    (_) => TextEditingController(),
  );
  final TextEditingController _timerController = TextEditingController(
    text: '20',
  );
  final TextEditingController _topicController = TextEditingController(
    text: 'Electrostatics',
  );

  _QuickQuizMode _mode = _QuickQuizMode.manual;
  String _difficulty = 'medium';
  int _correctOption = 0;
  bool _submitting = false;
  bool _loadingAi = false;
  bool _loadingImports = false;
  List<LivePollDraft> _imports = const [];
  int _selectedImport = -1;

  @override
  void dispose() {
    _questionController.dispose();
    for (final controller in _optionControllers) {
      controller.dispose();
    }
    _timerController.dispose();
    _topicController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return SafeArea(
      child: Padding(
        padding: EdgeInsets.only(
          left: 16,
          right: 16,
          top: 12,
          bottom: 16 + MediaQuery.of(context).viewInsets.bottom,
        ),
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 740),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              Row(
                children: [
                  const Icon(Icons.poll_outlined),
                  const SizedBox(width: 8),
                  const Expanded(
                    child: Text(
                      'Live Quick Quiz / Poll',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ),
                  IconButton(
                    onPressed: _submitting ? null : widget.onCancel,
                    icon: const Icon(Icons.close),
                  ),
                ],
              ),
              const SizedBox(height: 8),
              SegmentedButton<_QuickQuizMode>(
                segments: const [
                  ButtonSegment(
                    value: _QuickQuizMode.manual,
                    icon: Icon(Icons.edit_note),
                    label: Text('Manual Entry'),
                  ),
                  ButtonSegment(
                    value: _QuickQuizMode.ai,
                    icon: Icon(Icons.auto_awesome),
                    label: Text('Generate with AI'),
                  ),
                  ButtonSegment(
                    value: _QuickQuizMode.import,
                    icon: Icon(Icons.download_for_offline_outlined),
                    label: Text('Import Question'),
                  ),
                ],
                selected: {_mode},
                onSelectionChanged: (selection) {
                  if (selection.isEmpty) {
                    return;
                  }
                  setState(() {
                    _mode = selection.first;
                  });
                },
              ),
              const SizedBox(height: 12),
              Flexible(child: SingleChildScrollView(child: _buildModeBody())),
              const SizedBox(height: 10),
              Row(
                children: [
                  TextButton(
                    onPressed: _submitting ? null : widget.onCancel,
                    child: const Text('Cancel'),
                  ),
                  const Spacer(),
                  FilledButton.icon(
                    onPressed: _submitting ? null : _startPoll,
                    icon: _submitting
                        ? const SizedBox(
                            width: 14,
                            height: 14,
                            child: CircularProgressIndicator(strokeWidth: 2),
                          )
                        : const Icon(Icons.play_arrow),
                    label: const Text('Start Poll'),
                  ),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildModeBody() {
    switch (_mode) {
      case _QuickQuizMode.manual:
        return _buildManualFields();
      case _QuickQuizMode.ai:
        return _buildAiFields();
      case _QuickQuizMode.import:
        return _buildImportFields();
    }
  }

  Widget _buildManualFields() {
    return Column(
      children: [
        TextField(
          controller: _questionController,
          maxLines: 3,
          decoration: const InputDecoration(
            labelText: 'Question',
            border: OutlineInputBorder(),
          ),
        ),
        const SizedBox(height: 10),
        ..._optionControllers.asMap().entries.map((entry) {
          final index = entry.key;
          final controller = entry.value;
          return Padding(
            padding: const EdgeInsets.only(bottom: 8),
            child: TextField(
              controller: controller,
              decoration: InputDecoration(
                labelText: 'Option ${String.fromCharCode(65 + index)}',
                border: const OutlineInputBorder(),
              ),
            ),
          );
        }),
        Row(
          children: [
            Expanded(
              child: TextField(
                controller: _timerController,
                keyboardType: TextInputType.number,
                decoration: const InputDecoration(
                  labelText: 'Timer (seconds)',
                  border: OutlineInputBorder(),
                ),
              ),
            ),
            const SizedBox(width: 10),
            Expanded(
              child: DropdownButtonFormField<int>(
                key: ValueKey<int>(_correctOption),
                initialValue: _correctOption,
                decoration: const InputDecoration(
                  labelText: 'Correct Option',
                  border: OutlineInputBorder(),
                ),
                items: const [
                  DropdownMenuItem(value: 0, child: Text('A')),
                  DropdownMenuItem(value: 1, child: Text('B')),
                  DropdownMenuItem(value: 2, child: Text('C')),
                  DropdownMenuItem(value: 3, child: Text('D')),
                ],
                onChanged: (value) {
                  if (value == null) {
                    return;
                  }
                  setState(() {
                    _correctOption = value;
                  });
                },
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildAiFields() {
    return Column(
      children: [
        TextField(
          controller: _topicController,
          decoration: const InputDecoration(
            labelText: 'Topic',
            border: OutlineInputBorder(),
          ),
        ),
        const SizedBox(height: 8),
        DropdownButtonFormField<String>(
          key: ValueKey<String>(_difficulty),
          initialValue: _difficulty,
          decoration: const InputDecoration(
            labelText: 'Difficulty',
            border: OutlineInputBorder(),
          ),
          items: const [
            DropdownMenuItem(value: 'easy', child: Text('Easy')),
            DropdownMenuItem(value: 'medium', child: Text('Medium')),
            DropdownMenuItem(value: 'hard', child: Text('Hard')),
          ],
          onChanged: (value) {
            if (value == null) {
              return;
            }
            setState(() {
              _difficulty = value;
            });
          },
        ),
        const SizedBox(height: 8),
        Align(
          alignment: Alignment.centerRight,
          child: OutlinedButton.icon(
            onPressed: _loadingAi ? null : _generateWithAi,
            icon: _loadingAi
                ? const SizedBox(
                    width: 14,
                    height: 14,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.auto_awesome),
            label: const Text('Generate Question'),
          ),
        ),
        const SizedBox(height: 8),
        _buildManualFields(),
      ],
    );
  }

  Widget _buildImportFields() {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          children: [
            const Text(
              'Import from quiz database',
              style: TextStyle(fontWeight: FontWeight.w600),
            ),
            const Spacer(),
            OutlinedButton.icon(
              onPressed: _loadingImports ? null : _loadImports,
              icon: _loadingImports
                  ? const SizedBox(
                      width: 14,
                      height: 14,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Icon(Icons.refresh),
              label: const Text('Load'),
            ),
          ],
        ),
        const SizedBox(height: 8),
        if (_imports.isEmpty)
          const Text(
            'No imported questions loaded yet.',
            style: TextStyle(color: Color(0xFF57718E)),
          ),
        if (_imports.isNotEmpty)
          ..._imports.asMap().entries.map((entry) {
            final index = entry.key;
            final item = entry.value;
            final selected = _selectedImport == index;
            return ListTile(
              shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(10),
                side: const BorderSide(color: Color(0xFFD8E4F4)),
              ),
              contentPadding: const EdgeInsets.symmetric(
                horizontal: 10,
                vertical: 2,
              ),
              leading: Icon(
                selected
                    ? Icons.radio_button_checked
                    : Icons.radio_button_unchecked,
                color: selected
                    ? const Color(0xFF215D9D)
                    : const Color(0xFF7D8CA0),
              ),
              title: Text(item.question),
              subtitle: Text(
                '${item.options.join(' | ')}\nTimer: ${item.timerSeconds}s',
              ),
              onTap: () {
                setState(() {
                  _selectedImport = index;
                  _applyDraft(_imports[index]);
                });
              },
            );
          }),
        const SizedBox(height: 8),
        _buildManualFields(),
      ],
    );
  }

  Future<void> _generateWithAi() async {
    final topic = _topicController.text.trim();
    if (topic.isEmpty) {
      _showSnack('Please enter a topic.');
      return;
    }
    setState(() {
      _loadingAi = true;
    });
    try {
      final draft = await widget.onGenerateWithAi(topic, _difficulty);
      _applyDraft(draft);
    } catch (_) {
      _showSnack('AI generation failed. Please try again.');
    } finally {
      if (mounted) {
        setState(() {
          _loadingAi = false;
        });
      }
    }
  }

  Future<void> _loadImports() async {
    setState(() {
      _loadingImports = true;
    });
    try {
      final imported = await widget.onLoadImportedPolls();
      if (!mounted) {
        return;
      }
      setState(() {
        _imports = imported;
      });
    } catch (_) {
      _showSnack('Could not load imported questions.');
    } finally {
      if (mounted) {
        setState(() {
          _loadingImports = false;
        });
      }
    }
  }

  Future<void> _startPoll() async {
    final question = _questionController.text.trim();
    final options = _optionControllers
        .map((controller) => controller.text.trim())
        .where((item) => item.isNotEmpty)
        .toList(growable: false);
    final timer = int.tryParse(_timerController.text.trim()) ?? 20;

    if (question.isEmpty) {
      _showSnack('Question is required.');
      return;
    }
    if (options.length < 2) {
      _showSnack('At least 2 options are required.');
      return;
    }
    if (timer < 5 || timer > 180) {
      _showSnack('Timer must be between 5 and 180 seconds.');
      return;
    }
    if (_correctOption >= options.length) {
      _showSnack('Correct option must match an available option.');
      return;
    }

    final draft = LivePollDraft(
      question: question,
      options: options,
      timerSeconds: timer,
      correctOption: _correctOption,
      topic: _topicController.text.trim().isEmpty
          ? null
          : _topicController.text.trim(),
      difficulty: _difficulty,
    );

    setState(() {
      _submitting = true;
    });
    try {
      await widget.onStartPoll(draft);
    } finally {
      if (mounted) {
        setState(() {
          _submitting = false;
        });
      }
    }
  }

  void _applyDraft(LivePollDraft draft) {
    _questionController.text = draft.question;
    for (var index = 0; index < _optionControllers.length; index += 1) {
      _optionControllers[index].text = index < draft.options.length
          ? draft.options[index]
          : '';
    }
    _timerController.text = draft.timerSeconds.toString();
    if (draft.correctOption != null) {
      _correctOption = draft.correctOption!.clamp(0, 3);
    }
    if (draft.topic != null && draft.topic!.isNotEmpty) {
      _topicController.text = draft.topic!;
    }
    if (draft.difficulty != null && draft.difficulty!.isNotEmpty) {
      _difficulty = draft.difficulty!;
    }
    setState(() {});
  }

  void _showSnack(String message) {
    if (!mounted) {
      return;
    }
    ScaffoldMessenger.of(
      context,
    ).showSnackBar(SnackBar(content: Text(message)));
  }
}
