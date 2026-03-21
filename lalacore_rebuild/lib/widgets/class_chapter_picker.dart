import 'package:flutter/material.dart';

import '../models/syllabus.dart';

Future<Map<String, dynamic>?> showClassChapterPicker(
  BuildContext context, {
  required String currentClass,
  required List<String> currentChapters,
  String currentSubject = '',
  Map<String, List<String>> dynamicChaptersBySubject =
      const <String, List<String>>{},
}) async {
  List<String> _dynamicChaptersFor(String subject) {
    if (subject.trim().isEmpty) {
      return <String>[];
    }
    final List<String> raw = dynamicChaptersBySubject[subject] ?? <String>[];
    final List<String> out = raw
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .toSet()
        .toList();
    out.sort();
    return out;
  }

  List<String> _subjectsForClass(String className) {
    final Set<String> extraSubjects = dynamicChaptersBySubject.keys
        .map((String e) => e.trim())
        .where((String e) => e.isNotEmpty)
        .toSet();
    if (className == '11 + 12') {
      final Set<String> out = <String>{
        ...(syllabusByClassAndSubject['Class 11'] ?? <String, List<String>>{})
            .keys,
        ...(syllabusByClassAndSubject['Class 12'] ?? <String, List<String>>{})
            .keys,
        ...extraSubjects,
      };
      final List<String> list = out.toList()..sort();
      return list;
    }
    final Map<String, List<String>> scoped =
        syllabusByClassAndSubject[className] ?? <String, List<String>>{};
    final List<String> out = <String>{...scoped.keys, ...extraSubjects}.toList()
      ..sort();
    return out;
  }

  List<String> _chaptersFor(String className, String subject) {
    if (subject.trim().isEmpty) {
      return <String>[];
    }
    final Set<String> extra = _dynamicChaptersFor(subject).toSet();
    if (className == '11 + 12') {
      final Set<String> out = <String>{
        ...((syllabusByClassAndSubject['Class 11'] ??
                <String, List<String>>{})[subject] ??
            <String>[]),
        ...((syllabusByClassAndSubject['Class 12'] ??
                <String, List<String>>{})[subject] ??
            <String>[]),
        ...extra,
      };
      final List<String> list = out.toList()..sort();
      return list;
    }
    final Map<String, List<String>> scoped =
        syllabusByClassAndSubject[className] ?? <String, List<String>>{};
    final List<String> out = <String>{
      ...(scoped[subject] ?? <String>[]),
      ...extra,
    }.toList();
    out.sort();
    return out;
  }

  String _inferSubject({
    required String className,
    required List<String> chapters,
    required String preferred,
  }) {
    final List<String> subjects = _subjectsForClass(className);
    if (subjects.isEmpty) {
      return '';
    }
    final String preferredTrimmed = preferred.trim();
    if (preferredTrimmed.isNotEmpty && subjects.contains(preferredTrimmed)) {
      return preferredTrimmed;
    }
    if (chapters.isEmpty) {
      return subjects.first;
    }
    int bestScore = -1;
    String best = subjects.first;
    for (final String subject in subjects) {
      final Set<String> chapterSet = _chaptersFor(className, subject).toSet();
      final int score = chapters.where(chapterSet.contains).length;
      if (score > bestScore) {
        bestScore = score;
        best = subject;
      }
    }
    return best;
  }

  String selectedClass = currentClass == 'Class 11+12'
      ? '11 + 12'
      : currentClass;
  final List<String> selectedChapters = List<String>.from(currentChapters);
  String selectedSubject = _inferSubject(
    className: selectedClass,
    chapters: selectedChapters,
    preferred: currentSubject,
  );

  return showModalBottomSheet<Map<String, dynamic>>(
    context: context,
    isScrollControlled: true,
    builder: (BuildContext modalContext) {
      return StatefulBuilder(
        builder:
            (
              BuildContext context,
              void Function(void Function()) setModalState,
            ) {
              final List<String> subjects = _subjectsForClass(selectedClass);
              if (!subjects.contains(selectedSubject)) {
                selectedSubject = subjects.isEmpty ? '' : subjects.first;
              }
              List<String> available = _chaptersFor(
                selectedClass,
                selectedSubject,
              );
              available = available.toSet().toList()..sort();

              return SizedBox(
                height: MediaQuery.of(context).size.height * 0.8,
                child: Padding(
                  padding: const EdgeInsets.all(20),
                  child: Column(
                    children: <Widget>[
                      const Text(
                        'Select Class & Chapters',
                        style: TextStyle(
                          fontSize: 18,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                      const SizedBox(height: 12),
                      DropdownButtonFormField<String>(
                        value: selectedClass,
                        items: const <DropdownMenuItem<String>>[
                          DropdownMenuItem(
                            value: 'Class 11',
                            child: Text('Class 11'),
                          ),
                          DropdownMenuItem(
                            value: 'Class 12',
                            child: Text('Class 12'),
                          ),
                          DropdownMenuItem(
                            value: '11 + 12',
                            child: Text('11 + 12'),
                          ),
                        ],
                        onChanged: (String? value) {
                          if (value == null) {
                            return;
                          }
                          setModalState(() {
                            selectedClass = value;
                            selectedSubject = _inferSubject(
                              className: selectedClass,
                              chapters: selectedChapters,
                              preferred: selectedSubject,
                            );
                            final Set<String> visible = _chaptersFor(
                              selectedClass,
                              selectedSubject,
                            ).toSet();
                            selectedChapters.removeWhere(
                              (String chapter) => !visible.contains(chapter),
                            );
                          });
                        },
                        decoration: const InputDecoration(labelText: 'Class'),
                      ),
                      const SizedBox(height: 12),
                      DropdownButtonFormField<String>(
                        value: selectedSubject.isEmpty ? null : selectedSubject,
                        items: subjects
                            .map(
                              (String subject) => DropdownMenuItem<String>(
                                value: subject,
                                child: Text(subject),
                              ),
                            )
                            .toList(),
                        onChanged: (String? value) {
                          if (value == null) {
                            return;
                          }
                          setModalState(() {
                            selectedSubject = value;
                            final Set<String> visible = _chaptersFor(
                              selectedClass,
                              selectedSubject,
                            ).toSet();
                            selectedChapters.removeWhere(
                              (String chapter) => !visible.contains(chapter),
                            );
                          });
                        },
                        decoration: const InputDecoration(labelText: 'Subject'),
                      ),
                      const SizedBox(height: 12),
                      Expanded(
                        child: ListView(
                          children: available.isEmpty
                              ? const <Widget>[
                                  ListTile(
                                    title: Text('No chapters available'),
                                  ),
                                ]
                              : available.map((String chapter) {
                                  final bool selected = selectedChapters
                                      .contains(chapter);
                                  return CheckboxListTile(
                                    value: selected,
                                    title: Text(chapter),
                                    onChanged: (bool? checked) {
                                      setModalState(() {
                                        if (checked == true) {
                                          selectedChapters.add(chapter);
                                        } else {
                                          selectedChapters.remove(chapter);
                                        }
                                      });
                                    },
                                  );
                                }).toList(),
                        ),
                      ),
                      const SizedBox(height: 12),
                      Row(
                        children: <Widget>[
                          Expanded(
                            child: OutlinedButton(
                              onPressed: () => Navigator.pop(modalContext),
                              child: const Text('Cancel'),
                            ),
                          ),
                          const SizedBox(width: 12),
                          Expanded(
                            child: ElevatedButton(
                              onPressed: () {
                                Navigator.pop(modalContext, <String, dynamic>{
                                  'class': selectedClass,
                                  'subject': selectedSubject,
                                  'chapters': selectedChapters,
                                });
                              },
                              child: const Text('Done'),
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              );
            },
      );
    },
  );
}
