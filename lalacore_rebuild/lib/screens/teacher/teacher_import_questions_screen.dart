import 'dart:convert';
import 'dart:ui' show ImageFilter;

import 'package:flutter/material.dart';

import '../../models/import_question_models.dart';
import '../../models/session.dart';
import '../../services/backend_service.dart';
import '../../services/teacher_question_import_service.dart';
import '../../widgets/liquid_glass.dart';
import '../../widgets/smart_text.dart';

class TeacherImportQuestionsScreen extends StatefulWidget {
  const TeacherImportQuestionsScreen({
    super.key,
    required this.importService,
    this.backendService,
    this.initialRawText = '',
    this.initialSubject = 'Mathematics',
    this.initialChapter = '',
    this.initialDifficulty = 'Hard',
  });

  final TeacherQuestionImportService importService;
  final BackendService? backendService;
  final String initialRawText;
  final String initialSubject;
  final String initialChapter;
  final String initialDifficulty;

  @override
  State<TeacherImportQuestionsScreen> createState() =>
      _TeacherImportQuestionsScreenState();
}

class _TeacherImportQuestionsScreenState
    extends State<TeacherImportQuestionsScreen> {
  late final TextEditingController _rawCtrl;
  late final TextEditingController _subjectCtrl;
  late final TextEditingController _chapterCtrl;
  late final TextEditingController _difficultyCtrl;

  bool _parsing = false;
  bool _aiValidating = false;
  bool _publishing = false;
  List<ImportedQuestion> _questions = <ImportedQuestion>[];
  final Set<String> _expanded = <String>{};
  Map<String, dynamic> _qualityDashboard = <String, dynamic>{};

  bool get _busy => _parsing || _aiValidating || _publishing;

  String get _busyLabel {
    if (_parsing) {
      return 'Preparing editable questions';
    }
    if (_aiValidating) {
      return 'Running AI validation';
    }
    if (_publishing) {
      return 'Publishing questions';
    }
    return 'Working';
  }

  String get _busyDetail {
    if (_parsing) {
      return 'Processing input 1/1 before opening edit options.';
    }
    if (_aiValidating) {
      return 'Cross-checking answer keys, options, and structure.';
    }
    if (_publishing) {
      return 'Syncing drafts and question bank records.';
    }
    return '';
  }

  @override
  void initState() {
    super.initState();
    _rawCtrl = TextEditingController(text: widget.initialRawText);
    _subjectCtrl = TextEditingController(text: widget.initialSubject);
    _chapterCtrl = TextEditingController(text: widget.initialChapter);
    _difficultyCtrl = TextEditingController(text: widget.initialDifficulty);
    if (widget.initialRawText.trim().isNotEmpty) {
      _parseRawText();
    }
  }

  @override
  void dispose() {
    _rawCtrl.dispose();
    _subjectCtrl.dispose();
    _chapterCtrl.dispose();
    _difficultyCtrl.dispose();
    super.dispose();
  }

  Color _statusColor(ImportValidationStatus status) {
    switch (status) {
      case ImportValidationStatus.valid:
        return Colors.green;
      case ImportValidationStatus.review:
        return Colors.amber.shade700;
      case ImportValidationStatus.invalid:
        return Colors.red.shade700;
    }
  }

  String _statusLabel(ImportValidationStatus status) {
    switch (status) {
      case ImportValidationStatus.valid:
        return 'VALID';
      case ImportValidationStatus.review:
        return 'REVIEW';
      case ImportValidationStatus.invalid:
        return 'INVALID';
    }
  }

  Future<void> _parseRawText() async {
    final String raw = _rawCtrl.text.trim();
    if (raw.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Paste OCR/raw question text first.')),
      );
      return;
    }
    setState(() => _parsing = true);
    try {
      final List<ImportedQuestion> parsed = widget.importService
          .lc9ParseQuestions(
            raw,
            subject: _subjectCtrl.text.trim(),
            chapter: _chapterCtrl.text.trim(),
            difficulty: _difficultyCtrl.text.trim(),
          )
          .map((ImportedQuestion q) => widget.importService.validateQuestion(q))
          .toList();

      if (!mounted) {
        return;
      }
      setState(() {
        _questions = parsed;
        _qualityDashboard = _buildLocalQualityDashboard(parsed);
        _expanded
          ..clear()
          ..addAll(parsed.take(2).map((ImportedQuestion q) => q.questionId));
      });
    } finally {
      if (mounted) {
        setState(() => _parsing = false);
      }
    }
  }

  Future<void> _runAiValidation() async {
    if (_questions.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('No parsed questions to validate.')),
      );
      return;
    }
    setState(() => _aiValidating = true);
    try {
      final String userId = Session.effectiveAccountId.isNotEmpty
          ? Session.effectiveAccountId
          : Session.studentId;
      final List<ImportedQuestion> validated = await widget.importService
          .runAiValidationBatch(
            questions: _questions,
            userId: userId.isEmpty ? 'teacher_import' : userId,
          );
      if (!mounted) {
        return;
      }
      setState(() {
        _questions = validated
            .map(
              (ImportedQuestion q) => widget.importService.validateQuestion(q),
            )
            .toList();
        _qualityDashboard = _buildLocalQualityDashboard(_questions);
      });
    } finally {
      if (mounted) {
        setState(() => _aiValidating = false);
      }
    }
  }

  void _replaceQuestion(int index, ImportedQuestion next) {
    if (index < 0 || index >= _questions.length) {
      return;
    }
    final ImportedQuestion validated = widget.importService.validateQuestion(
      next,
    );
    setState(() {
      _questions[index] = validated;
      _qualityDashboard = _buildLocalQualityDashboard(_questions);
    });
  }

  void _applyMetaToAll() {
    final String subject = _subjectCtrl.text.trim();
    final String chapter = _chapterCtrl.text.trim();
    final String difficulty = _difficultyCtrl.text.trim();
    setState(() {
      _questions = _questions
          .map(
            (ImportedQuestion q) => widget.importService.validateQuestion(
              q.copyWith(
                subject: subject.isEmpty ? q.subject : subject,
                chapter: chapter.isEmpty ? q.chapter : chapter,
                difficulty: difficulty.isEmpty ? q.difficulty : difficulty,
              ),
            ),
          )
          .toList();
      _qualityDashboard = _buildLocalQualityDashboard(_questions);
    });
  }

  Map<String, dynamic> _buildLocalQualityDashboard(
    List<ImportedQuestion> questions,
  ) {
    final Map<String, int> sourceCounts = <String, int>{};
    double riskSum = 0;
    for (final ImportedQuestion q in questions) {
      final String key = q.answerFillSource.trim().isEmpty
          ? 'unknown'
          : q.answerFillSource.trim().toLowerCase();
      sourceCounts[key] = (sourceCounts[key] ?? 0) + 1;
      riskSum += q.publishRiskScore.clamp(0.0, 1.0);
    }
    final double risk = questions.isEmpty ? 0 : (riskSum / questions.length);
    return <String, dynamic>{
      'question_count': questions.length,
      'answer_fill_source_counts': sourceCounts,
      'publish_risk_score': risk,
      'symbol_repair_count': 0,
      'per_page_ocr_confidence': const <Map<String, dynamic>>[],
    };
  }

  Future<void> _publish() async {
    if (_questions.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Import at least one question first.')),
      );
      return;
    }
    final List<ImportedQuestion> latest = _questions
        .map((ImportedQuestion q) => widget.importService.validateQuestion(q))
        .toList();
    final List<ImportedQuestion> publishable = latest
        .where(
          (ImportedQuestion q) =>
              q.validationStatus != ImportValidationStatus.invalid,
        )
        .toList();
    final int invalidCount = latest
        .where(
          (ImportedQuestion q) =>
              q.validationStatus == ImportValidationStatus.invalid,
        )
        .length;
    final int reviewCount = publishable
        .where(
          (ImportedQuestion q) =>
              q.validationStatus == ImportValidationStatus.review,
        )
        .length;

    if (publishable.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text(
            'No publishable questions found. Fix invalid questions first.',
          ),
        ),
      );
      return;
    }

    if (invalidCount > 0 || reviewCount > 0) {
      final List<String> notes = <String>[];
      if (invalidCount > 0) {
        notes.add('skipping $invalidCount invalid');
      }
      if (reviewCount > 0) {
        notes.add('$reviewCount review');
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            'One-click publish mode: ${notes.join(' • ')} questions.',
          ),
        ),
      );
    }

    setState(() => _publishing = true);
    try {
      final List<Map<String, dynamic>> payload = publishable
          .map((ImportedQuestion q) => q.toJson())
          .toList();
      await _syncImportToSheets(payload);
      if (!mounted) {
        return;
      }
      Navigator.of(context).pop(payload);
    } catch (e) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Publish failed: $e')));
    } finally {
      if (mounted) {
        setState(() => _publishing = false);
      }
    }
  }

  Future<void> _syncImportToSheets(List<Map<String, dynamic>> questions) async {
    final BackendService? backend = widget.backendService;
    if (backend == null) {
      return;
    }
    final String teacherId = Session.effectiveAccountId.trim().isEmpty
        ? Session.studentId
        : Session.effectiveAccountId;
    final Map<String, dynamic> meta = <String, dynamic>{
      'teacher_id': teacherId.isEmpty ? 'teacher' : teacherId,
      'subject': _subjectCtrl.text.trim(),
      'chapter': _chapterCtrl.text.trim(),
      'difficulty': _difficultyCtrl.text.trim(),
    };

    final Map<String, dynamic> draftRes = await backend.lc9SaveImportDrafts(
      questions: questions,
      meta: meta,
    );
    if (!_isSuccessful(draftRes)) {
      throw Exception(_extractFailureMessage(draftRes, 'saving drafts'));
    }

    Map<String, dynamic> publishRes = await backend.lc9PublishImportQuestions(
      questions: questions,
      meta: meta,
      publishGateProfile: 'strict_critical_only',
      fixSuggestionsApplied: false,
    );
    if ((publishRes['status'] ?? '').toString().toUpperCase() ==
        'PUBLISH_GATE_REVIEW_CONFIRMATION_REQUIRED') {
      final bool proceed = await _confirmFixSuggestionsPublish(
        publishRes['publish_gate'] is Map
            ? Map<String, dynamic>.from(publishRes['publish_gate'] as Map)
            : const <String, dynamic>{},
      );
      if (!proceed) {
        throw Exception(
          'Publish cancelled: review-level acknowledgement needed.',
        );
      }
      publishRes = await backend.lc9PublishImportQuestions(
        questions: questions,
        meta: meta,
        publishGateProfile: 'strict_critical_only',
        fixSuggestionsApplied: true,
      );
    }
    if (!_isSuccessful(publishRes)) {
      throw Exception(_extractFailureMessage(publishRes, 'publishing to bank'));
    }
    final dynamic quality = publishRes['quality_dashboard'];
    if (quality is Map<String, dynamic>) {
      if (mounted) {
        setState(() => _qualityDashboard = quality);
      }
    } else if (quality is Map) {
      if (mounted) {
        setState(() => _qualityDashboard = Map<String, dynamic>.from(quality));
      }
    }
  }

  Future<bool> _confirmFixSuggestionsPublish(
    Map<String, dynamic> publishGate,
  ) async {
    final int reviewCount =
        int.tryParse((publishGate['review_count'] ?? 0).toString()) ?? 0;
    if (!mounted) {
      return false;
    }
    return (await showDialog<bool>(
          context: context,
          builder: (BuildContext context) {
            return AlertDialog(
              title: const Text('Apply Fix Suggestions'),
              content: Text(
                reviewCount > 0
                    ? '$reviewCount review-level item(s) detected. Apply suggestions and publish now?'
                    : 'Apply fix suggestions and publish now?',
              ),
              actions: <Widget>[
                TextButton(
                  onPressed: () => Navigator.of(context).pop(false),
                  child: const Text('Cancel'),
                ),
                FilledButton(
                  onPressed: () => Navigator.of(context).pop(true),
                  child: const Text('Apply & Publish'),
                ),
              ],
            );
          },
        )) ??
        false;
  }

  bool _isSuccessful(Map<String, dynamic> response) {
    final String status = (response['status'] ?? '').toString().toUpperCase();
    final String message = ((response['message'] ?? response['error'] ?? ''))
        .toString()
        .toLowerCase();
    if (message.contains('error') ||
        message.contains('failed') ||
        message.contains('invalid')) {
      return false;
    }
    if (response['ok'] == true) {
      return true;
    }
    if (status.isEmpty) {
      return !response.containsKey('error');
    }
    return <String>{'OK', 'SUCCESS', 'DONE'}.contains(status);
  }

  String _extractFailureMessage(Map<String, dynamic> response, String action) {
    final dynamic invalid = response['invalid'];
    if (invalid is List && invalid.isNotEmpty) {
      final dynamic first = invalid.first;
      if (first is Map) {
        final String id = (first['question_id'] ?? '').toString().trim();
        final dynamic errors = first['errors'];
        final String err = errors is List && errors.isNotEmpty
            ? errors.first.toString()
            : (response['message'] ?? response['error'] ?? '').toString();
        if (id.isNotEmpty && err.isNotEmpty) {
          return '$action failed for $id: $err';
        }
      }
    }
    final String msg = (response['message'] ?? response['error'] ?? '')
        .toString()
        .trim();
    if (msg.isNotEmpty) {
      return '$action failed: $msg';
    }
    return '$action failed due to an unknown backend error.';
  }

  Widget _buildBusyOverlay() {
    if (!_busy) {
      return const SizedBox.shrink();
    }
    return IgnorePointer(
      ignoring: false,
      child: Container(
        color: Colors.black.withOpacity(0.18),
        child: Center(
          child: ClipRect(
            child: BackdropFilter(
              filter: ImageFilter.blur(sigmaX: 4, sigmaY: 4),
              child: Container(
                constraints: const BoxConstraints(maxWidth: 360),
                margin: const EdgeInsets.symmetric(horizontal: 18),
                padding: const EdgeInsets.symmetric(
                  horizontal: 16,
                  vertical: 14,
                ),
                decoration: BoxDecoration(
                  color: Colors.white.withOpacity(0.93),
                  borderRadius: BorderRadius.circular(16),
                  border: Border.all(color: Colors.black.withOpacity(0.08)),
                ),
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: <Widget>[
                    Row(
                      children: <Widget>[
                        const SizedBox(
                          width: 18,
                          height: 18,
                          child: CircularProgressIndicator(strokeWidth: 2.2),
                        ),
                        const SizedBox(width: 10),
                        Expanded(
                          child: Text(
                            _busyLabel,
                            style: const TextStyle(
                              fontWeight: FontWeight.w700,
                              fontSize: 13,
                            ),
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 6),
                    Text(
                      _busyDetail,
                      style: const TextStyle(fontSize: 12, color: Colors.grey),
                    ),
                  ],
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final int validCount = _questions
        .where(
          (ImportedQuestion q) =>
              q.validationStatus == ImportValidationStatus.valid,
        )
        .length;
    final int reviewCount = _questions
        .where(
          (ImportedQuestion q) =>
              q.validationStatus == ImportValidationStatus.review,
        )
        .length;
    final int invalidCount = _questions
        .where(
          (ImportedQuestion q) =>
              q.validationStatus == ImportValidationStatus.invalid,
        )
        .length;

    return Scaffold(
      appBar: AppBar(
        title: const Text('Teacher Import Questions'),
        actions: <Widget>[
          IconButton(
            onPressed: _aiValidating || _parsing ? null : _runAiValidation,
            tooltip: 'Run AI Validation',
            icon: _aiValidating
                ? const SizedBox(
                    width: 18,
                    height: 18,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.psychology_alt_rounded),
          ),
          IconButton(
            onPressed: _parsing || _aiValidating || _publishing
                ? null
                : _publish,
            tooltip: 'Publish Structured Questions',
            icon: _publishing
                ? const SizedBox(
                    width: 18,
                    height: 18,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.check_circle_outline_rounded),
          ),
        ],
      ),
      body: Stack(
        children: <Widget>[
          ListView(
            padding: const EdgeInsets.all(16),
            children: <Widget>[
              LiquidGlass(
                padding: const EdgeInsets.all(12),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: <Widget>[
                    const Text(
                      'Raw OCR / Document Text',
                      style: TextStyle(fontWeight: FontWeight.w700),
                    ),
                    const SizedBox(height: 8),
                    TextField(
                      controller: _rawCtrl,
                      maxLines: 8,
                      decoration: const InputDecoration(
                        hintText:
                            'Paste OCR text here. Questions should start with 1. / Q1 and options A), B), ...',
                      ),
                    ),
                    const SizedBox(height: 10),
                    Row(
                      children: <Widget>[
                        Expanded(
                          child: TextField(
                            controller: _subjectCtrl,
                            decoration: const InputDecoration(
                              labelText: 'Subject',
                            ),
                          ),
                        ),
                        const SizedBox(width: 8),
                        Expanded(
                          child: TextField(
                            controller: _chapterCtrl,
                            decoration: const InputDecoration(
                              labelText: 'Chapter',
                            ),
                          ),
                        ),
                        const SizedBox(width: 8),
                        Expanded(
                          child: TextField(
                            controller: _difficultyCtrl,
                            decoration: const InputDecoration(
                              labelText: 'Difficulty',
                            ),
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 10),
                    Row(
                      children: <Widget>[
                        Expanded(
                          child: OutlinedButton.icon(
                            onPressed: _parsing || _aiValidating
                                ? null
                                : _applyMetaToAll,
                            icon: const Icon(Icons.tune_rounded),
                            label: const Text('Apply Meta To All'),
                          ),
                        ),
                        const SizedBox(width: 8),
                        Expanded(
                          child: ElevatedButton.icon(
                            onPressed: _parsing || _aiValidating
                                ? null
                                : _parseRawText,
                            icon: _parsing
                                ? const SizedBox(
                                    width: 14,
                                    height: 14,
                                    child: CircularProgressIndicator(
                                      strokeWidth: 2,
                                    ),
                                  )
                                : const Icon(Icons.auto_fix_high_rounded),
                            label: Text(
                              _parsing ? 'Parsing...' : 'Parse Questions',
                            ),
                          ),
                        ),
                      ],
                    ),
                    if (_questions.isNotEmpty) ...<Widget>[
                      const SizedBox(height: 8),
                      Text(
                        'Parsed: ${_questions.length} • Valid: $validCount • Review: $reviewCount • Invalid: $invalidCount',
                        style: const TextStyle(fontWeight: FontWeight.w600),
                      ),
                      const SizedBox(height: 10),
                      _buildQualityTelemetryCard(),
                    ],
                  ],
                ),
              ),
              const SizedBox(height: 12),
              if (_questions.isEmpty)
                const Padding(
                  padding: EdgeInsets.only(top: 32),
                  child: Center(child: Text('No imported questions yet.')),
                )
              else
                ListView.builder(
                  shrinkWrap: true,
                  physics: const NeverScrollableScrollPhysics(),
                  itemCount: _questions.length,
                  itemBuilder: (BuildContext context, int index) {
                    final ImportedQuestion q = _questions[index];
                    final Color statusColor = _statusColor(q.validationStatus);
                    final bool expanded = _expanded.contains(q.questionId);
                    return AnimatedContainer(
                      duration: const Duration(milliseconds: 220),
                      margin: const EdgeInsets.only(bottom: 10),
                      decoration: BoxDecoration(
                        border: Border.all(color: statusColor, width: 1.4),
                        borderRadius: BorderRadius.circular(14),
                      ),
                      child: LiquidGlass(
                        borderRadius: BorderRadius.circular(14),
                        padding: const EdgeInsets.symmetric(horizontal: 10),
                        child: ExpansionTile(
                          initiallyExpanded: expanded,
                          onExpansionChanged: (bool value) {
                            setState(() {
                              if (value) {
                                _expanded.add(q.questionId);
                              } else {
                                _expanded.remove(q.questionId);
                              }
                            });
                          },
                          title: Row(
                            children: <Widget>[
                              Expanded(
                                child: Text(
                                  'Q${index + 1} • ${importedQuestionTypeToString(q.type)}',
                                  maxLines: 1,
                                  overflow: TextOverflow.ellipsis,
                                  style: const TextStyle(
                                    fontWeight: FontWeight.w700,
                                  ),
                                ),
                              ),
                              Container(
                                padding: const EdgeInsets.symmetric(
                                  horizontal: 8,
                                  vertical: 3,
                                ),
                                decoration: BoxDecoration(
                                  color: statusColor.withValues(alpha: 0.15),
                                  borderRadius: BorderRadius.circular(999),
                                ),
                                child: Text(
                                  _statusLabel(q.validationStatus),
                                  style: TextStyle(
                                    color: statusColor,
                                    fontWeight: FontWeight.w700,
                                    fontSize: 11,
                                  ),
                                ),
                              ),
                              const SizedBox(width: 6),
                              Text(
                                '${(q.aiConfidence * 100).toStringAsFixed(0)}%',
                                style: const TextStyle(
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                            ],
                          ),
                          subtitle: Text(
                            q.questionText.trim().isEmpty
                                ? 'Empty question text'
                                : q.questionText.trim(),
                            maxLines: 2,
                            overflow: TextOverflow.ellipsis,
                          ),
                          children: <Widget>[
                            Padding(
                              padding: const EdgeInsets.fromLTRB(8, 4, 8, 12),
                              child: Column(
                                children: <Widget>[
                                  DropdownButtonFormField<ImportedQuestionType>(
                                    value: q.type,
                                    decoration: const InputDecoration(
                                      labelText: 'Question Type',
                                    ),
                                    items: ImportedQuestionType.values
                                        .map(
                                          (ImportedQuestionType type) =>
                                              DropdownMenuItem<
                                                ImportedQuestionType
                                              >(
                                                value: type,
                                                child: Text(
                                                  importedQuestionTypeToString(
                                                    type,
                                                  ),
                                                ),
                                              ),
                                        )
                                        .toList(),
                                    onChanged: (ImportedQuestionType? value) {
                                      if (value == null) {
                                        return;
                                      }
                                      _replaceQuestion(
                                        index,
                                        widget.importService.updateQuestionType(
                                          q,
                                          value,
                                        ),
                                      );
                                    },
                                  ),
                                  const SizedBox(height: 8),
                                  TextFormField(
                                    initialValue: q.questionText,
                                    maxLines: 3,
                                    decoration: const InputDecoration(
                                      labelText: 'Question Text',
                                    ),
                                    onChanged: (String value) {
                                      _replaceQuestion(
                                        index,
                                        q.copyWith(questionText: value),
                                      );
                                    },
                                  ),
                                  if (q.questionTextLatex
                                      .trim()
                                      .isNotEmpty) ...<Widget>[
                                    const SizedBox(height: 8),
                                    Align(
                                      alignment: Alignment.centerLeft,
                                      child: Text(
                                        'Canonical LaTeX Preview',
                                        style: TextStyle(
                                          fontSize: 12,
                                          color: Colors.grey.shade700,
                                          fontWeight: FontWeight.w600,
                                        ),
                                      ),
                                    ),
                                    const SizedBox(height: 4),
                                    Container(
                                      width: double.infinity,
                                      padding: const EdgeInsets.all(8),
                                      decoration: BoxDecoration(
                                        color: Colors.black.withOpacity(0.03),
                                        borderRadius: BorderRadius.circular(10),
                                      ),
                                      child: SmartText(
                                        '\$\$${q.questionTextLatex}\$\$',
                                      ),
                                    ),
                                  ],
                                  const SizedBox(height: 8),
                                  if (q.type == ImportedQuestionType.numerical)
                                    _buildNumericalEditor(index, q)
                                  else
                                    _buildMcqEditor(index, q),
                                  if (q
                                      .validationErrors
                                      .isNotEmpty) ...<Widget>[
                                    const SizedBox(height: 8),
                                    Align(
                                      alignment: Alignment.centerLeft,
                                      child: Column(
                                        crossAxisAlignment:
                                            CrossAxisAlignment.start,
                                        children: q.validationErrors
                                            .map(
                                              (String e) => Text(
                                                '• $e',
                                                style: TextStyle(
                                                  color: _statusColor(
                                                    q.validationStatus,
                                                  ),
                                                  fontSize: 12,
                                                ),
                                              ),
                                            )
                                            .toList(),
                                      ),
                                    ),
                                  ],
                                ],
                              ),
                            ),
                          ],
                        ),
                      ),
                    );
                  },
                ),
              const SizedBox(height: 12),
              if (_questions.isNotEmpty)
                ElevatedButton.icon(
                  onPressed: _parsing || _aiValidating || _publishing
                      ? null
                      : _publish,
                  icon: const Icon(Icons.publish_rounded),
                  label: Text(
                    _publishing
                        ? 'Publishing to Question Bank...'
                        : 'Publish Structured Questions',
                  ),
                ),
            ],
          ),
          _buildBusyOverlay(),
        ],
      ),
    );
  }

  Widget _buildNumericalEditor(int index, ImportedQuestion q) {
    return Column(
      children: <Widget>[
        TextFormField(
          initialValue: q.correctAnswer.numerical ?? '',
          decoration: const InputDecoration(labelText: 'Numerical Answer'),
          keyboardType: const TextInputType.numberWithOptions(decimal: true),
          onChanged: (String value) {
            _replaceQuestion(
              index,
              q.copyWith(
                correctAnswer: q.correctAnswer.copyWith(
                  numerical: value,
                  multiple: const <String>[],
                  clearSingle: true,
                ),
              ),
            );
          },
        ),
        const SizedBox(height: 8),
        TextFormField(
          initialValue: (q.correctAnswer.tolerance ?? '').toString(),
          decoration: const InputDecoration(
            labelText: 'Tolerance (Optional)',
            hintText: 'e.g. 0.01',
          ),
          keyboardType: const TextInputType.numberWithOptions(decimal: true),
          onChanged: (String value) {
            _replaceQuestion(
              index,
              q.copyWith(
                correctAnswer: q.correctAnswer.copyWith(
                  tolerance: double.tryParse(value.trim()),
                ),
              ),
            );
          },
        ),
      ],
    );
  }

  Widget _buildQualityTelemetryCard() {
    final double risk =
        double.tryParse(
          (_qualityDashboard['publish_risk_score'] ?? 0).toString(),
        ) ??
        0;
    final Map<String, dynamic> sourceCountsRaw =
        _qualityDashboard['answer_fill_source_counts'] is Map
        ? Map<String, dynamic>.from(
            _qualityDashboard['answer_fill_source_counts'] as Map,
          )
        : <String, dynamic>{};
    final int symbolRepairCount =
        int.tryParse(
          (_qualityDashboard['symbol_repair_count'] ?? 0).toString(),
        ) ??
        0;
    final List<dynamic> pageRaw =
        _qualityDashboard['per_page_ocr_confidence'] is List
        ? _qualityDashboard['per_page_ocr_confidence'] as List<dynamic>
        : const <dynamic>[];
    final String perPageSummary = pageRaw
        .whereType<Map>()
        .map((Map<dynamic, dynamic> row) {
          final int page =
              int.tryParse((row['page_number'] ?? 0).toString()) ?? 0;
          final double conf =
              double.tryParse((row['confidence'] ?? 0).toString()) ?? 0;
          return page > 0 ? 'P$page ${(conf * 100).toStringAsFixed(0)}%' : '';
        })
        .where((String e) => e.isNotEmpty)
        .join(' • ');
    final String sourceSummary = sourceCountsRaw.entries
        .map((MapEntry<String, dynamic> e) => '${e.key}: ${e.value}')
        .join(' • ');
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(10),
      decoration: BoxDecoration(
        color: Colors.black.withOpacity(0.03),
        borderRadius: BorderRadius.circular(10),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          Text(
            'Import Quality Dashboard',
            style: TextStyle(
              fontWeight: FontWeight.w700,
              color: Colors.grey.shade800,
            ),
          ),
          const SizedBox(height: 6),
          Text(
            'Publish risk: ${(risk * 100).toStringAsFixed(1)}%',
            style: const TextStyle(fontWeight: FontWeight.w600),
          ),
          const SizedBox(height: 4),
          Text(
            'Symbol repairs: $symbolRepairCount',
            style: const TextStyle(fontSize: 12, color: Colors.grey),
          ),
          if (sourceSummary.isNotEmpty) ...<Widget>[
            const SizedBox(height: 4),
            Text(
              'Answer-fill source: $sourceSummary',
              style: const TextStyle(fontSize: 12, color: Colors.grey),
            ),
          ],
          if (perPageSummary.isNotEmpty) ...<Widget>[
            const SizedBox(height: 4),
            Text(
              'Per-page OCR confidence: $perPageSummary',
              style: const TextStyle(fontSize: 12, color: Colors.grey),
            ),
          ],
        ],
      ),
    );
  }

  Widget _buildMcqEditor(int index, ImportedQuestion q) {
    final bool multi = q.type == ImportedQuestionType.mcqMulti;
    final List<ImportOption> options = q.options;
    final Set<String> selectedMulti = q.correctAnswer.multiple
        .map((String e) => e.trim().toUpperCase())
        .toSet();
    final String selectedSingle = (q.correctAnswer.single ?? '')
        .trim()
        .toUpperCase();

    return Column(
      children: <Widget>[
        ...List<Widget>.generate(options.length, (int i) {
          final ImportOption option = options[i];
          return Row(
            children: <Widget>[
              SizedBox(
                width: 26,
                child: Text(
                  option.label,
                  style: const TextStyle(fontWeight: FontWeight.w700),
                ),
              ),
              Expanded(
                child: TextFormField(
                  initialValue: option.text,
                  decoration: const InputDecoration(labelText: 'Option Text'),
                  onChanged: (String value) {
                    final List<ImportOption> next = List<ImportOption>.from(
                      options,
                    );
                    next[i] = option.copyWith(text: value);
                    _replaceQuestion(index, q.copyWith(options: next));
                  },
                ),
              ),
              if (multi)
                Checkbox(
                  value: selectedMulti.contains(option.label),
                  onChanged: (bool? value) {
                    final Set<String> next = Set<String>.from(selectedMulti);
                    if (value == true) {
                      next.add(option.label);
                    } else {
                      next.remove(option.label);
                    }
                    _replaceQuestion(
                      index,
                      q.copyWith(
                        correctAnswer: q.correctAnswer.copyWith(
                          multiple: next.toList()..sort(),
                          single: next.isEmpty ? null : next.first,
                        ),
                      ),
                    );
                  },
                )
              else
                Radio<String>(
                  value: option.label,
                  groupValue: selectedSingle,
                  onChanged: (String? value) {
                    if (value == null) {
                      return;
                    }
                    _replaceQuestion(
                      index,
                      q.copyWith(
                        correctAnswer: q.correctAnswer.copyWith(
                          single: value,
                          multiple: <String>[value],
                        ),
                      ),
                    );
                  },
                ),
            ],
          );
        }),
        const SizedBox(height: 6),
        Row(
          children: <Widget>[
            OutlinedButton.icon(
              onPressed: () {
                final List<ImportOption> next = List<ImportOption>.from(
                  options,
                );
                final String label = String.fromCharCode(65 + next.length);
                next.add(ImportOption(label: label, text: ''));
                _replaceQuestion(index, q.copyWith(options: next));
              },
              icon: const Icon(Icons.add_rounded),
              label: const Text('Add Option'),
            ),
            const SizedBox(width: 8),
            if (options.length > 2)
              OutlinedButton.icon(
                onPressed: () {
                  final List<ImportOption> next = List<ImportOption>.from(
                    options,
                  )..removeLast();
                  final Set<String> selected = Set<String>.from(selectedMulti)
                    ..removeWhere(
                      (String label) =>
                          !next.any((ImportOption e) => e.label == label),
                    );
                  String? single = selectedSingle;
                  if (single.isNotEmpty &&
                      !next.any((ImportOption e) => e.label == single)) {
                    single = null;
                  }
                  _replaceQuestion(
                    index,
                    q.copyWith(
                      options: next,
                      correctAnswer: q.correctAnswer.copyWith(
                        multiple: selected.toList()..sort(),
                        single: single,
                      ),
                    ),
                  );
                },
                icon: const Icon(Icons.remove_rounded),
                label: const Text('Remove Last'),
              ),
          ],
        ),
      ],
    );
  }
}

String prettyPrintImportedQuestions(List<Map<String, dynamic>> questions) {
  const JsonEncoder encoder = JsonEncoder.withIndent('  ');
  return encoder.convert(questions);
}
