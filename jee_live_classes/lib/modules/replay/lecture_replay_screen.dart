import 'package:flutter/material.dart';
import 'package:video_player/video_player.dart';

import '../../core/app_config.dart';
import '../../models/lecture_index_model.dart';
import '../../models/replay_model.dart';
import '../../models/transcript_model.dart';
import '../../services/secure_api_client.dart';
import '../ai/lalacore_api_service.dart';

class LectureReplayScreen extends StatefulWidget {
  const LectureReplayScreen({
    super.key,
    required this.replay,
    this.lalacoreApiOverride,
  });

  final ReplayModel replay;
  final LalacoreApi? lalacoreApiOverride;

  @override
  State<LectureReplayScreen> createState() => _LectureReplayScreenState();
}

class _LectureReplayScreenState extends State<LectureReplayScreen> {
  VideoPlayerController? _controller;
  late final LalacoreApi _lalacoreApi;

  @override
  void initState() {
    super.initState();
    _lalacoreApi =
        widget.lalacoreApiOverride ??
        (() {
          final config = AppConfig.fromEnvironment();
          return LalacoreApi(
            config: config,
            apiClient: SecureApiClient(config: config),
            useMockResponses: config.enableMockServices,
          );
        })();
    _init();
  }

  Future<void> _init() async {
    final controller = VideoPlayerController.networkUrl(
      Uri.parse(widget.replay.videoUrl),
    );
    await controller.initialize();
    setState(() {
      _controller = controller;
    });
  }

  @override
  void dispose() {
    _controller?.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final controller = _controller;
    return Scaffold(
      appBar: AppBar(title: const Text('Lecture Replay')),
      body: Row(
        children: [
          Expanded(
            flex: 5,
            child: controller == null
                ? const Center(child: CircularProgressIndicator())
                : Column(
                    children: [
                      AspectRatio(
                        aspectRatio: controller.value.aspectRatio,
                        child: VideoPlayer(controller),
                      ),
                      VideoProgressIndicator(controller, allowScrubbing: true),
                      Row(
                        children: [
                          IconButton(
                            onPressed: () {
                              if (controller.value.isPlaying) {
                                controller.pause();
                              } else {
                                controller.play();
                              }
                              setState(() {});
                            },
                            icon: Icon(
                              controller.value.isPlaying
                                  ? Icons.pause_circle
                                  : Icons.play_circle,
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text(
                            'Class ${widget.replay.classId}',
                            style: const TextStyle(fontWeight: FontWeight.w700),
                          ),
                        ],
                      ),
                    ],
                  ),
          ),
          Expanded(
            flex: 4,
            child: DefaultTabController(
              length: 2,
              child: Column(
                children: [
                  const TabBar(
                    tabs: [
                      Tab(text: 'Concepts'),
                      Tab(text: 'Transcript'),
                    ],
                  ),
                  Expanded(
                    child: TabBarView(
                      children: [
                        _ConceptTimeline(
                          items: widget.replay.conceptIndex,
                          onJump: _jumpTo,
                        ),
                        _TranscriptPanel(
                          transcript: widget.replay.transcript,
                          onJump: _jumpTo,
                          onExplain: _explainSegment,
                        ),
                      ],
                    ),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Future<void> _jumpTo(int seconds) async {
    final controller = _controller;
    if (controller == null) {
      return;
    }
    await controller.seekTo(Duration(seconds: seconds));
  }

  Future<void> _explainSegment(TranscriptModel item) async {
    if (!mounted) {
      return;
    }
    showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (context) => const AlertDialog(
        content: Row(
          children: [
            SizedBox(
              width: 16,
              height: 16,
              child: CircularProgressIndicator(strokeWidth: 2),
            ),
            SizedBox(width: 10),
            Expanded(child: Text('AI is clarifying this segment...')),
          ],
        ),
      ),
    );
    try {
      final answer = await _lalacoreApi.askLalacore(
        prompt:
            'Explain this lecture line again step-by-step for a JEE student:\n'
            '"${item.message}"\n'
            'Include concept, formula, and one exam tip.',
        context: AiRequestContext(
          transcript: widget.replay.transcript,
          chatMessages: const [],
          ocrSnippets: const [],
          lectureMaterials: [widget.replay.classId],
          detectedConcepts: widget.replay.conceptIndex
              .map((item) => item.topic)
              .toList(growable: false),
          timestamps: widget.replay.conceptIndex
              .map((item) => item.timestampSeconds)
              .toList(growable: false),
        ),
      );
      if (!mounted) {
        return;
      }
      Navigator.of(context, rootNavigator: true).pop();
      await showDialog<void>(
        context: context,
        builder: (context) {
          return AlertDialog(
            title: const Text('AI Clarification'),
            content: SingleChildScrollView(child: Text(answer)),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(context).pop(),
                child: const Text('Close'),
              ),
            ],
          );
        },
      );
    } catch (error) {
      if (!mounted) {
        return;
      }
      Navigator.of(context, rootNavigator: true).pop();
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to generate AI clarification: $error')),
      );
    }
  }
}

class _ConceptTimeline extends StatelessWidget {
  const _ConceptTimeline({required this.items, required this.onJump});

  final List<LectureIndexModel> items;
  final ValueChanged<int> onJump;

  @override
  Widget build(BuildContext context) {
    return ListView.builder(
      itemCount: items.length,
      itemBuilder: (context, index) {
        final item = items[index];
        return ListTile(
          title: Text(item.topic),
          subtitle: Text(item.summary),
          trailing: Text(_fmt(item.timestampSeconds)),
          onTap: () => onJump(item.timestampSeconds),
        );
      },
    );
  }

  String _fmt(int seconds) {
    final m = (seconds ~/ 60).toString().padLeft(2, '0');
    final s = (seconds % 60).toString().padLeft(2, '0');
    return '$m:$s';
  }
}

class _TranscriptPanel extends StatelessWidget {
  const _TranscriptPanel({
    required this.transcript,
    required this.onJump,
    required this.onExplain,
  });

  final List<TranscriptModel> transcript;
  final ValueChanged<int> onJump;
  final ValueChanged<TranscriptModel> onExplain;

  @override
  Widget build(BuildContext context) {
    return ListView.builder(
      itemCount: transcript.length,
      itemBuilder: (context, index) {
        final item = transcript[index];
        final seconds = item.timestamp.difference(DateTime(1970)).inSeconds;
        return ListTile(
          title: Text(item.speakerName),
          subtitle: Text(item.message),
          trailing: Wrap(
            spacing: 6,
            crossAxisAlignment: WrapCrossAlignment.center,
            children: [
              IconButton(
                tooltip: 'Explain this again',
                onPressed: () => onExplain(item),
                icon: const Icon(Icons.auto_awesome, size: 18),
              ),
              Text(item.confidence.toStringAsFixed(2)),
            ],
          ),
          onTap: () => onJump(seconds),
        );
      },
    );
  }
}
