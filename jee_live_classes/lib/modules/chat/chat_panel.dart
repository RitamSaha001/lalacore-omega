import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:flutter_math_fork/flutter_math.dart';

import '../classroom/classroom_state.dart';

class ChatPanel extends StatefulWidget {
  const ChatPanel({
    super.key,
    required this.messages,
    required this.chatEnabled,
    required this.onSend,
    required this.onSendAttachment,
    this.onAskDoubt,
    this.onQueueDoubt,
    this.showAskDoubtAction = false,
  });

  final List<ChatMessage> messages;
  final bool chatEnabled;
  final ValueChanged<String> onSend;
  final ValueChanged<ChatAttachment> onSendAttachment;
  final Future<String> Function(String question)? onAskDoubt;
  final Future<void> Function({
    required String question,
    required String aiAttempt,
  })?
  onQueueDoubt;
  final bool showAskDoubtAction;

  @override
  State<ChatPanel> createState() => _ChatPanelState();
}

class _ChatPanelState extends State<ChatPanel> {
  // BEGIN_PHASE2_IMPLEMENTATION
  final TextEditingController _controller = TextEditingController();

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        const _PanelHeader(
          icon: Icons.chat_bubble_outline,
          title: 'Class Chat',
          subtitle: 'Doubts, equations, attachments, and announcements',
        ),
        Expanded(
          child: ListView.builder(
            padding: const EdgeInsets.all(12),
            itemCount: widget.messages.length,
            itemBuilder: (context, index) {
              final message = widget.messages[index];
              return Align(
                alignment: message.isTeacher
                    ? Alignment.centerLeft
                    : Alignment.centerRight,
                child: ConstrainedBox(
                  constraints: const BoxConstraints(maxWidth: 340),
                  child: Padding(
                    padding: const EdgeInsets.only(bottom: 8),
                    child: DecoratedBox(
                      decoration: BoxDecoration(
                        color: message.isTeacher
                            ? const Color(0xFFE9F4FF)
                            : const Color(0xFFF2F4F8),
                        borderRadius: BorderRadius.circular(12),
                      ),
                      child: Padding(
                        padding: const EdgeInsets.all(10),
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              message.sender,
                              style: const TextStyle(
                                fontWeight: FontWeight.w700,
                                fontSize: 12,
                              ),
                            ),
                            const SizedBox(height: 4),
                            if (message.isLatex)
                              SingleChildScrollView(
                                scrollDirection: Axis.horizontal,
                                child: Math.tex(message.message),
                              )
                            else
                              Text(message.message),
                            if (message.attachment != null) ...[
                              const SizedBox(height: 6),
                              _AttachmentChip(attachment: message.attachment!),
                            ],
                          ],
                        ),
                      ),
                    ),
                  ),
                ),
              );
            },
          ),
        ),
        Padding(
          padding: const EdgeInsets.all(12),
          child: Row(
            children: [
              if (widget.showAskDoubtAction)
                FilledButton.tonalIcon(
                  onPressed: widget.chatEnabled ? _openAskDoubtDialog : null,
                  icon: const Icon(Icons.help_outline),
                  label: const Text('Ask Doubt'),
                ),
              if (widget.showAskDoubtAction) const SizedBox(width: 8),
              IconButton(
                onPressed: widget.chatEnabled ? _pickImage : null,
                tooltip: 'Attach image',
                icon: const Icon(Icons.image_outlined),
              ),
              IconButton(
                onPressed: widget.chatEnabled ? _pickFile : null,
                tooltip: 'Attach file',
                icon: const Icon(Icons.attach_file),
              ),
              Expanded(
                child: TextField(
                  controller: _controller,
                  enabled: widget.chatEnabled,
                  decoration: InputDecoration(
                    hintText: widget.chatEnabled
                        ? r'Type message / $$latex$$ / doubt...'
                        : 'Chat disabled by host',
                    border: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(12),
                    ),
                    isDense: true,
                  ),
                  onSubmitted: (_) => _send(),
                ),
              ),
              const SizedBox(width: 8),
              FilledButton(
                onPressed: widget.chatEnabled ? _send : null,
                child: const Icon(Icons.send),
              ),
            ],
          ),
        ),
      ],
    );
  }

  void _send() {
    final text = _controller.text.trim();
    if (text.isEmpty) {
      return;
    }
    widget.onSend(text);
    _controller.clear();
  }

  Future<void> _pickImage() async {
    final result = await FilePicker.platform.pickFiles(
      type: FileType.image,
      allowMultiple: false,
    );
    if (result == null || result.files.isEmpty) {
      return;
    }

    final file = result.files.first;
    if (file.path == null) {
      return;
    }

    widget.onSendAttachment(
      ChatAttachment(
        type: ChatAttachmentType.image,
        name: file.name,
        path: file.path!,
      ),
    );
  }

  Future<void> _pickFile() async {
    final result = await FilePicker.platform.pickFiles(
      allowMultiple: false,
      type: FileType.any,
    );
    if (result == null || result.files.isEmpty) {
      return;
    }

    final file = result.files.first;
    if (file.path == null) {
      return;
    }

    widget.onSendAttachment(
      ChatAttachment(
        type: ChatAttachmentType.file,
        name: file.name,
        path: file.path!,
      ),
    );
  }

  Future<void> _openAskDoubtDialog() async {
    final onAsk = widget.onAskDoubt;
    if (onAsk == null) {
      return;
    }

    final questionController = TextEditingController();
    final question = await showDialog<String>(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: const Text('Ask Doubt'),
          content: TextField(
            controller: questionController,
            minLines: 3,
            maxLines: 6,
            decoration: const InputDecoration(
              hintText: 'Enter your doubt for AI + teacher queue...',
              border: OutlineInputBorder(),
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Cancel'),
            ),
            FilledButton(
              onPressed: () {
                final text = questionController.text.trim();
                if (text.isEmpty) {
                  return;
                }
                Navigator.of(context).pop(text);
              },
              child: const Text('Ask AI'),
            ),
          ],
        );
      },
    );
    questionController.dispose();

    if (question == null || question.trim().isEmpty || !mounted) {
      return;
    }

    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (context) {
        return const AlertDialog(
          content: Row(
            children: [
              SizedBox(
                width: 18,
                height: 18,
                child: CircularProgressIndicator(strokeWidth: 2),
              ),
              SizedBox(width: 12),
              Expanded(child: Text('LalaCore is analyzing your doubt...')),
            ],
          ),
        );
      },
    );

    String aiAnswer;
    try {
      aiAnswer = await onAsk(question);
    } finally {
      if (mounted) {
        Navigator.of(context, rootNavigator: true).pop();
      }
    }

    if (!mounted) {
      return;
    }

    final shouldQueue = await showDialog<bool>(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: const Text('AI Doubt Response'),
          content: SingleChildScrollView(
            child: Text(aiAnswer.isEmpty ? 'No AI response.' : aiAnswer),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(false),
              child: const Text('Resolved'),
            ),
            FilledButton.icon(
              onPressed: () => Navigator.of(context).pop(true),
              icon: const Icon(Icons.queue),
              label: const Text('Queue for Teacher'),
            ),
          ],
        );
      },
    );

    if (shouldQueue == true && widget.onQueueDoubt != null) {
      await widget.onQueueDoubt!(question: question, aiAttempt: aiAnswer);
    }
  }

  // END_PHASE2_IMPLEMENTATION
}

class _AttachmentChip extends StatelessWidget {
  const _AttachmentChip({required this.attachment});

  final ChatAttachment attachment;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: const Color(0xFFEFF3FA),
        borderRadius: BorderRadius.circular(999),
      ),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(
              attachment.type == ChatAttachmentType.image
                  ? Icons.image
                  : Icons.insert_drive_file,
              size: 14,
            ),
            const SizedBox(width: 5),
            ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 180),
              child: Text(attachment.name, overflow: TextOverflow.ellipsis),
            ),
          ],
        ),
      ),
    );
  }
}

class _PanelHeader extends StatelessWidget {
  const _PanelHeader({
    required this.icon,
    required this.title,
    required this.subtitle,
  });

  final IconData icon;
  final String title;
  final String subtitle;

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: const BoxDecoration(
        border: Border(bottom: BorderSide(color: Color(0xFFDCE7F7))),
      ),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          children: [
            Icon(icon, size: 20),
            const SizedBox(width: 8),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    title,
                    style: const TextStyle(fontWeight: FontWeight.w700),
                  ),
                  Text(
                    subtitle,
                    style: const TextStyle(
                      fontSize: 12,
                      color: Color(0xFF4A607C),
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
