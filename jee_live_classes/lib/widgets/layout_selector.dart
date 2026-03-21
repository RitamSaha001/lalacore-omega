import 'package:flutter/material.dart';

import '../modules/classroom/classroom_state.dart';

class LayoutSelector extends StatelessWidget {
  const LayoutSelector({
    super.key,
    required this.selected,
    required this.onSelected,
  });

  final ClassroomLayoutMode selected;
  final ValueChanged<ClassroomLayoutMode> onSelected;

  @override
  Widget build(BuildContext context) {
    return SegmentedButton<ClassroomLayoutMode>(
      showSelectedIcon: false,
      style: ButtonStyle(
        visualDensity: VisualDensity.compact,
        tapTargetSize: MaterialTapTargetSize.shrinkWrap,
        backgroundColor: WidgetStateProperty.resolveWith((states) {
          if (states.contains(WidgetState.selected)) {
            return const Color(0xFF123F66);
          }
          return Colors.white;
        }),
        foregroundColor: WidgetStateProperty.resolveWith((states) {
          if (states.contains(WidgetState.selected)) {
            return Colors.white;
          }
          return const Color(0xFF214162);
        }),
      ),
      segments: const [
        ButtonSegment<ClassroomLayoutMode>(
          value: ClassroomLayoutMode.grid,
          label: Text('Grid'),
          icon: Icon(Icons.grid_view),
        ),
        ButtonSegment<ClassroomLayoutMode>(
          value: ClassroomLayoutMode.speaker,
          label: Text('Speaker'),
          icon: Icon(Icons.record_voice_over),
        ),
        ButtonSegment<ClassroomLayoutMode>(
          value: ClassroomLayoutMode.presentation,
          label: Text('Present'),
          icon: Icon(Icons.present_to_all),
        ),
        ButtonSegment<ClassroomLayoutMode>(
          value: ClassroomLayoutMode.focus,
          label: Text('Focus'),
          icon: Icon(Icons.center_focus_strong),
        ),
      ],
      selected: {selected},
      onSelectionChanged: (selection) {
        onSelected(selection.first);
      },
    );
  }
}
