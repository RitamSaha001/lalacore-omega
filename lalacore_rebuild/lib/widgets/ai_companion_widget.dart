import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:rive/rive.dart';

import 'ai_companion_controller.dart';

class AICompanionWidget extends StatefulWidget {
  const AICompanionWidget({
    super.key,
    required this.controller,
    this.height = 160,
    this.assetPath = 'assets/companion/companion.riv',
    this.stateMachineName = 'CompanionSM',
    this.lowPowerMode = false,
    this.enableRandomInteractions = true,
  });

  final AICompanionController controller;
  final double height;
  final String assetPath;
  final String stateMachineName;
  final bool lowPowerMode;
  final bool enableRandomInteractions;

  @override
  State<AICompanionWidget> createState() => _AICompanionWidgetState();
}

class _AICompanionWidgetState extends State<AICompanionWidget> {
  Artboard? _artboard;
  Object? _loadError;

  @override
  void initState() {
    super.initState();
    _loadRive();
  }

  @override
  void didUpdateWidget(covariant AICompanionWidget oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.assetPath != widget.assetPath ||
        oldWidget.stateMachineName != widget.stateMachineName) {
      _loadRive();
      return;
    }
    if (oldWidget.lowPowerMode != widget.lowPowerMode) {
      widget.controller.setLowPowerMode(widget.lowPowerMode);
    }
    if (oldWidget.enableRandomInteractions != widget.enableRandomInteractions) {
      widget.controller.setRandomEnabled(widget.enableRandomInteractions);
    }
  }

  Future<void> _loadRive() async {
    try {
      setState(() {
        _loadError = null;
        _artboard = null;
      });

      final ByteData data = await rootBundle.load(widget.assetPath);
      final RiveFile file = RiveFile.import(data);
      final Artboard board = file.mainArtboard.instance();

      await widget.controller.attachToArtboard(
        board,
        stateMachineName: widget.stateMachineName,
        lowPowerMode: widget.lowPowerMode,
        randomEnabled: widget.enableRandomInteractions,
      );

      if (!mounted) {
        return;
      }
      setState(() => _artboard = board);
    } catch (e) {
      if (!mounted) {
        return;
      }
      setState(() => _loadError = e);
    }
  }

  @override
  Widget build(BuildContext context) {
    final Widget child = _artboard != null
        ? RepaintBoundary(
            child: Rive(
              artboard: _artboard!,
              fit: BoxFit.contain,
              antialiasing: true,
            ),
          )
        : _fallback();

    return SizedBox(
      height: widget.height,
      child: GestureDetector(
        behavior: HitTestBehavior.opaque,
        onTap: widget.controller.registerUserInteraction,
        child: child,
      ),
    );
  }

  Widget _fallback() {
    if (_loadError != null) {
      return Center(
        child: Container(
          padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
          decoration: BoxDecoration(
            color: Colors.white.withValues(alpha: 0.75),
            borderRadius: BorderRadius.circular(14),
          ),
          child: const Text(
            'Companion unavailable',
            style: TextStyle(fontSize: 12, fontWeight: FontWeight.w600),
          ),
        ),
      );
    }

    return const Center(
      child: SizedBox(
        width: 28,
        height: 28,
        child: CircularProgressIndicator(strokeWidth: 2),
      ),
    );
  }
}
