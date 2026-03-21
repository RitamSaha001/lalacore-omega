class LectureNotesSection {
  const LectureNotesSection({
    required this.topic,
    required this.concept,
    required this.formulas,
    required this.example,
    required this.keyPoints,
  });

  final String topic;
  final String concept;
  final List<String> formulas;
  final String example;
  final List<String> keyPoints;
}

class LectureNotesModel {
  const LectureNotesModel({
    required this.classId,
    required this.classTitle,
    required this.generatedAt,
    required this.sourceSummary,
    required this.sections,
    required this.verificationNotes,
  });

  final String classId;
  final String classTitle;
  final DateTime generatedAt;
  final String sourceSummary;
  final List<LectureNotesSection> sections;
  final List<String> verificationNotes;

  String toPlainText() {
    final buffer = StringBuffer()
      ..writeln('Lecture Notes')
      ..writeln('Class: $classTitle ($classId)')
      ..writeln('Generated: ${generatedAt.toIso8601String()}')
      ..writeln()
      ..writeln(sourceSummary)
      ..writeln();

    for (var index = 0; index < sections.length; index += 1) {
      final section = sections[index];
      buffer
        ..writeln('${index + 1}. ${section.topic}')
        ..writeln('Concept: ${section.concept}')
        ..writeln('Formulas:')
        ..writeln(
          section.formulas.isEmpty
              ? '- No formula extracted'
              : section.formulas.map((item) => '- $item').join('\n'),
        )
        ..writeln('Example: ${section.example}')
        ..writeln('Key Points:')
        ..writeln(
          section.keyPoints.isEmpty
              ? '- No key points generated'
              : section.keyPoints.map((item) => '- $item').join('\n'),
        )
        ..writeln();
    }

    if (verificationNotes.isNotEmpty) {
      buffer
        ..writeln('Verification Notes')
        ..writeln(verificationNotes.map((item) => '- $item').join('\n'))
        ..writeln();
    }

    return buffer.toString().trim();
  }
}
