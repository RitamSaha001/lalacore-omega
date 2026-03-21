import 'dart:typed_data';

import 'package:pdf/pdf.dart';
import 'package:pdf/widgets.dart' as pw;
import 'package:printing/printing.dart';

import '../models/lecture_notes_model.dart';

class LectureNotesPdfService {
  const LectureNotesPdfService();

  Future<Uint8List> buildPdf(LectureNotesModel notes) async {
    final document = pw.Document();

    document.addPage(
      pw.MultiPage(
        pageTheme: pw.PageTheme(
          pageFormat: PdfPageFormat.a4,
          margin: const pw.EdgeInsets.fromLTRB(28, 28, 28, 30),
        ),
        build: (context) {
          final widgets = <pw.Widget>[
            pw.Text(
              'Lecture Notes',
              style: pw.TextStyle(fontSize: 22, fontWeight: pw.FontWeight.bold),
            ),
            pw.SizedBox(height: 6),
            pw.Text(
              '${notes.classTitle} (${notes.classId})',
              style: const pw.TextStyle(fontSize: 12),
            ),
            pw.Text(
              'Generated: ${notes.generatedAt.toIso8601String()}',
              style: const pw.TextStyle(fontSize: 10, color: PdfColors.grey700),
            ),
            pw.SizedBox(height: 14),
            _boxed('Source Summary', notes.sourceSummary),
            pw.SizedBox(height: 10),
          ];

          for (var index = 0; index < notes.sections.length; index += 1) {
            final section = notes.sections[index];
            widgets.add(
              pw.Container(
                margin: const pw.EdgeInsets.only(bottom: 10),
                padding: const pw.EdgeInsets.all(10),
                decoration: pw.BoxDecoration(
                  borderRadius: pw.BorderRadius.circular(6),
                  border: pw.Border.all(color: PdfColors.blue100),
                  color: PdfColors.blue50,
                ),
                child: pw.Column(
                  crossAxisAlignment: pw.CrossAxisAlignment.start,
                  children: [
                    pw.Text(
                      '${index + 1}. ${section.topic}',
                      style: pw.TextStyle(
                        fontSize: 13,
                        fontWeight: pw.FontWeight.bold,
                      ),
                    ),
                    pw.SizedBox(height: 4),
                    pw.Text(
                      'Concept: ${section.concept}',
                      style: const pw.TextStyle(fontSize: 11),
                    ),
                    pw.SizedBox(height: 4),
                    pw.Text(
                      'Formula(s):',
                      style: pw.TextStyle(
                        fontSize: 11,
                        fontWeight: pw.FontWeight.bold,
                      ),
                    ),
                    ...section.formulas.map(
                      (item) => pw.Bullet(
                        text: item,
                        style: const pw.TextStyle(fontSize: 10),
                      ),
                    ),
                    if (section.formulas.isEmpty)
                      pw.Text(
                        '- No formula extracted',
                        style: const pw.TextStyle(fontSize: 10),
                      ),
                    pw.SizedBox(height: 4),
                    pw.Text(
                      'Example: ${section.example}',
                      style: const pw.TextStyle(fontSize: 11),
                    ),
                    pw.SizedBox(height: 4),
                    pw.Text(
                      'Key Points:',
                      style: pw.TextStyle(
                        fontSize: 11,
                        fontWeight: pw.FontWeight.bold,
                      ),
                    ),
                    ...section.keyPoints.map(
                      (item) => pw.Bullet(
                        text: item,
                        style: const pw.TextStyle(fontSize: 10),
                      ),
                    ),
                    if (section.keyPoints.isEmpty)
                      pw.Text(
                        '- No key points generated',
                        style: const pw.TextStyle(fontSize: 10),
                      ),
                  ],
                ),
              ),
            );
          }

          if (notes.verificationNotes.isNotEmpty) {
            widgets.add(
              _boxed(
                'Verification Notes',
                notes.verificationNotes.map((item) => '- $item').join('\n'),
              ),
            );
          }

          return widgets;
        },
      ),
    );

    return document.save();
  }

  Future<void> sharePdf(LectureNotesModel notes) async {
    final bytes = await buildPdf(notes);
    await Printing.sharePdf(
      bytes: bytes,
      filename: 'lecture_notes_${notes.classId}.pdf',
    );
  }

  pw.Widget _boxed(String title, String body) {
    return pw.Container(
      width: double.infinity,
      padding: const pw.EdgeInsets.all(10),
      decoration: pw.BoxDecoration(
        borderRadius: pw.BorderRadius.circular(6),
        border: pw.Border.all(color: PdfColors.grey300),
      ),
      child: pw.Column(
        crossAxisAlignment: pw.CrossAxisAlignment.start,
        children: [
          pw.Text(
            title,
            style: pw.TextStyle(fontSize: 11, fontWeight: pw.FontWeight.bold),
          ),
          pw.SizedBox(height: 4),
          pw.Text(body, style: const pw.TextStyle(fontSize: 10)),
        ],
      ),
    );
  }
}
