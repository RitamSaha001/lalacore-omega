import 'package:flutter/material.dart';

import '../widgets/ai_companion_controller.dart';
import '../widgets/ai_companion_widget.dart';
import '../widgets/liquid_glass.dart';

class CompanionEditScreen extends StatefulWidget {
  const CompanionEditScreen({
    super.key,
    required this.initialSize,
    required this.lowPowerMode,
  });

  final double initialSize;
  final bool lowPowerMode;

  @override
  State<CompanionEditScreen> createState() => _CompanionEditScreenState();
}

class _CompanionEditScreenState extends State<CompanionEditScreen> {
  late double _size;
  final AICompanionController _previewController = AICompanionController();

  @override
  void initState() {
    super.initState();
    _size = widget.initialSize.clamp(110, 230).toDouble();
  }

  @override
  void dispose() {
    _previewController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Companion Size'),
        actions: <Widget>[
          TextButton(
            onPressed: () => Navigator.of(context).pop(_size),
            child: const Text('Save'),
          ),
        ],
      ),
      body: ListView(
        padding: const EdgeInsets.all(16),
        children: <Widget>[
          LiquidGlass(
            padding: const EdgeInsets.all(14),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                const Text(
                  'Live Preview',
                  style: TextStyle(fontWeight: FontWeight.w700, fontSize: 14),
                ),
                const SizedBox(height: 10),
                SizedBox(
                  height: 250,
                  child: Center(
                    child: AICompanionWidget(
                      controller: _previewController,
                      height: _size,
                      lowPowerMode: widget.lowPowerMode,
                      enableRandomInteractions: false,
                    ),
                  ),
                ),
              ],
            ),
          ),
          const SizedBox(height: 16),
          LiquidGlass(
            padding: const EdgeInsets.all(14),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                Text(
                  'Size: ${_size.round()}',
                  style: const TextStyle(
                    fontSize: 13,
                    fontWeight: FontWeight.w600,
                  ),
                ),
                Slider(
                  value: _size,
                  min: 110,
                  max: 230,
                  divisions: 24,
                  label: _size.round().toString(),
                  onChanged: (double v) => setState(() => _size = v),
                ),
                const SizedBox(height: 6),
                Wrap(
                  spacing: 8,
                  runSpacing: 8,
                  children: <Widget>[
                    _sizeChip('Small', 120),
                    _sizeChip('Medium', 160),
                    _sizeChip('Large', 200),
                  ],
                ),
              ],
            ),
          ),
          const SizedBox(height: 14),
          Text(
            'Tip: keep medium size for low-end devices. In chat, companion auto-hides while typing to avoid covering controls.',
            style: TextStyle(
              fontSize: 12.5,
              color: Theme.of(context).textTheme.bodySmall?.color,
            ),
          ),
        ],
      ),
    );
  }

  Widget _sizeChip(String label, double value) {
    final bool selected = (_size - value).abs() < 0.1;
    return ChoiceChip(
      label: Text(label),
      selected: selected,
      onSelected: (_) => setState(() => _size = value),
    );
  }
}
