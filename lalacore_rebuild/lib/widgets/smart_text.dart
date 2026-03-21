import 'package:flutter/material.dart';
import 'package:flutter_markdown/flutter_markdown.dart';
import 'package:flutter_math_fork/flutter_math.dart';
import 'package:markdown/markdown.dart' as md;

import '../utils/latex_support.dart';

class SmartText extends StatelessWidget {
  const SmartText(this.text, {super.key, this.style});

  final String text;
  final TextStyle? style;

  @override
  Widget build(BuildContext context) {
    final List<_Segment> segments = _segmentize(text);
    if (segments.isEmpty) {
      return const SizedBox.shrink();
    }
    final TextStyle fallbackStyle =
        style ?? Theme.of(context).textTheme.bodyMedium ?? const TextStyle();

    return Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: segments.map((_Segment segment) {
        switch (segment.type) {
          case _SegmentType.markdown:
            return _buildMarkdown(context, segment.content, fallbackStyle);
          case _SegmentType.blockMath:
            return _buildMath(context, segment.content, fallbackStyle);
          case _SegmentType.legacyMathImage:
            return _buildLegacyMathImage(segment.content);
        }
      }).toList(),
    );
  }

  Widget _buildMarkdown(
    BuildContext context,
    String markdown,
    TextStyle baseStyle,
  ) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 2),
      child: MarkdownBody(
        data: markdown,
        shrinkWrap: true,
        selectable: false,
        softLineBreak: true,
        inlineSyntaxes: <md.InlineSyntax>[
          _InlineMathSyntax(),
          _ParenInlineMathSyntax(),
        ],
        builders: <String, MarkdownElementBuilder>{
          'math-inline': _InlineMathBuilder(style: baseStyle),
          'math-inline-paren': _InlineMathBuilder(style: baseStyle),
        },
        styleSheet: MarkdownStyleSheet.fromTheme(Theme.of(context)).copyWith(
          p: baseStyle.copyWith(height: 1.45),
          code: baseStyle.copyWith(
            fontFamily: 'monospace',
            backgroundColor: Colors.transparent,
          ),
          blockquote: baseStyle.copyWith(
            color: baseStyle.color?.withValues(alpha: 0.86),
          ),
        ),
      ),
    );
  }

  Widget _buildMath(BuildContext context, String tex, TextStyle baseStyle) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 10),
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: Math.tex(
          tex.trim(),
          mathStyle: MathStyle.display,
          textStyle: baseStyle.copyWith(
            fontSize: (baseStyle.fontSize ?? 15) + 1,
          ),
          onErrorFallback: (FlutterMathException e) => Text(
            tex,
            style: baseStyle.copyWith(
              color: baseStyle.color?.withValues(alpha: 0.92),
              fontStyle: FontStyle.italic,
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildLegacyMathImage(String url) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 12),
      child: ClipRRect(
        borderRadius: BorderRadius.circular(12),
        child: Image.network(
          url,
          fit: BoxFit.contain,
          filterQuality: FilterQuality.high,
          loadingBuilder:
              (BuildContext context, Widget child, ImageChunkEvent? progress) {
                if (progress == null) {
                  return child;
                }
                return const SizedBox(
                  height: 36,
                  width: 36,
                  child: Center(
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
                );
              },
          errorBuilder: (_, __, ___) => const Text('[Failed to load equation]'),
        ),
      ),
    );
  }

  List<_Segment> _segmentize(String source) {
    if (source.trim().isEmpty) {
      return const <_Segment>[];
    }

    String normalized = source
        .replaceAllMapped(RegExp(r'\\\[(.*?)\\\]', dotAll: true), (Match m) {
          final String inner = (m.group(1) ?? '').trim();
          return inner.isEmpty ? '' : '\$\$$inner\$\$';
        })
        .replaceAllMapped(RegExp(r'\\\((.*?)\\\)', dotAll: true), (Match m) {
          final String inner = (m.group(1) ?? '').trim();
          return inner.isEmpty ? '' : '\$$inner\$';
        });
    normalized = normalized.replaceAllMapped(
      RegExp(
        r'(\\begin\{(?:align\*?|aligned|gather\*?|equation\*?|matrix|pmatrix|bmatrix|vmatrix|Vmatrix|cases)\}[\s\S]*?\\end\{[a-zA-Z\*]+\})',
        multiLine: true,
      ),
      (Match m) {
        final String env = (m.group(1) ?? '').trim();
        return env.isEmpty ? '' : '\$\$$env\$\$';
      },
    );

    final List<_Segment> out = <_Segment>[];
    final RegExp blockMath = RegExp(r'\$\$([\s\S]*?)\$\$', multiLine: true);
    int cursor = 0;

    for (final RegExpMatch match in blockMath.allMatches(normalized)) {
      final String before = normalized.substring(cursor, match.start);
      _appendMarkdownOrLegacy(out, before);
      final String math = normalizeUniversalLatex(
        (match.group(1) ?? '').trim(),
      );
      if (math.isNotEmpty) {
        out.add(_Segment(_SegmentType.blockMath, math));
      }
      cursor = match.end;
    }
    _appendMarkdownOrLegacy(out, normalized.substring(cursor));
    return out;
  }

  void _appendMarkdownOrLegacy(List<_Segment> out, String textBlock) {
    final String block = textBlock.trim();
    if (block.isEmpty) {
      return;
    }

    final List<String> lines = block.split('\n');
    final StringBuffer markdown = StringBuffer();
    for (final String raw in lines) {
      final String line = raw.trim();
      if (line.startsWith('[[MATH_IMG_BLOCK:') && line.endsWith(']]')) {
        final String url = line
            .replaceFirst('[[MATH_IMG_BLOCK:', '')
            .replaceFirst(']]', '')
            .trim();
        if (markdown.toString().trim().isNotEmpty) {
          out.add(_Segment(_SegmentType.markdown, markdown.toString().trim()));
          markdown.clear();
        }
        if (url.isNotEmpty) {
          final String? legacyTex = _legacyMathTexFromUrl(url);
          if (legacyTex != null && legacyTex.isNotEmpty) {
            out.add(_Segment(_SegmentType.blockMath, legacyTex));
          } else {
            out.add(_Segment(_SegmentType.legacyMathImage, url));
          }
        }
        continue;
      }
      markdown.writeln(raw);
    }
    final String value = markdown.toString().trim();
    if (value.isNotEmpty) {
      out.add(
        _Segment(_SegmentType.markdown, _normalizeLooseMathMarkdown(value)),
      );
    }
  }

  String? _legacyMathTexFromUrl(String url) {
    final String trimmed = url.trim();
    if (trimmed.isEmpty) {
      return null;
    }

    String decoded = Uri.decodeFull(trimmed);
    const List<String> markers = <String>['png.image?', 'svg.image?'];
    for (final String marker in markers) {
      final int idx = decoded.indexOf(marker);
      if (idx >= 0) {
        decoded = decoded.substring(idx + marker.length);
        break;
      }
    }

    decoded = decoded.replaceAll('+', ' ');
    try {
      decoded = Uri.decodeComponent(decoded);
    } catch (_) {}

    // Strip CodeCogs rendering directives that flutter_math does not need.
    decoded = decoded.replaceAll(RegExp(r'\\dpi\{[^}]*\}'), ' ');
    decoded = decoded.replaceAll(RegExp(r'\\bg_[a-zA-Z]+'), ' ');
    decoded = decoded.replaceAll(RegExp(r'\\fg_[a-zA-Z]+'), ' ');
    decoded = decoded.replaceAll(RegExp(r'\\fn_[a-zA-Z]+'), ' ');

    decoded = decoded.replaceAllMapped(
      RegExp(r'∫\s*\[\s*([^\]]*?)\s*to\s*([^\]]*?)\s*\]'),
      (Match m) =>
          r'\int_{' + m.group(1)!.trim() + r'}^{' + m.group(2)!.trim() + '}',
    );
    decoded = decoded.replaceAllMapped(
      RegExp(r'\\int\s*\[\s*([^\]]*?)\s*to\s*([^\]]*?)\s*\]'),
      (Match m) =>
          r'\int_{' + m.group(1)!.trim() + r'}^{' + m.group(2)!.trim() + '}',
    );
    decoded = decoded.replaceAllMapped(
      RegExp(
        r'\[\s*([^\]]+?)\s*\]\s*from\s*([^\s]+)\s*to\s*([^\s]+)',
        caseSensitive: false,
      ),
      (Match m) =>
          r'\left[' +
          m.group(1)!.trim() +
          r'\right]_{' +
          m.group(2)!.trim() +
          r'}^{' +
          m.group(3)!.trim() +
          '}',
    );
    decoded = decoded.replaceAllMapped(
      RegExp(r'\[\s*([^\]]*?)\s*to\s*([^\]]*?)\s*\]'),
      (Match m) =>
          r'_{' + m.group(1)!.trim() + r'}^{' + m.group(2)!.trim() + '}',
    );

    decoded = decoded.replaceAllMapped(
      RegExp(r'∑\s*\[\s*([a-zA-Z]+)\s*=\s*([^\]\s]+)\s+to\s+([^\]\s]+)\s*\]'),
      (Match m) =>
          r'\sum_{' +
          m.group(1)! +
          '=' +
          m.group(2)! +
          r'}^{' +
          m.group(3)! +
          '}',
    );
    decoded = decoded.replaceAllMapped(
      RegExp(
        r'\(\s*([^\(\)]+?)\s+choose\s+([^\(\)]+?)\s*\)',
        caseSensitive: false,
      ),
      (Match m) =>
          r'\binom{' + m.group(1)!.trim() + '}{' + m.group(2)!.trim() + '}',
    );
    decoded = decoded.replaceAllMapped(
      RegExp(r'([a-zA-Z0-9]+)\s+choose\s+([a-zA-Z0-9]+)', caseSensitive: false),
      (Match m) => r'\binom{' + m.group(1)! + '}{' + m.group(2)! + '}',
    );
    decoded = decoded.replaceAllMapped(
      RegExp(r'\b(\d+|[a-z])\s*[c]\s*(\d+|[a-z])\b', caseSensitive: false),
      (Match m) => r'\binom{' + m.group(1)! + '}{' + m.group(2)! + '}',
    );
    decoded = decoded.replaceAll('*', r' \cdot ');
    decoded = decoded.replaceAll(RegExp(r'\s+'), ' ').trim();

    decoded = normalizeUniversalLatex(decoded);
    return decoded.isEmpty ? null : decoded;
  }

  String _normalizeLooseMathMarkdown(String input) {
    if (input.trim().isEmpty) {
      return input;
    }

    String output = input;
    output = output.replaceAll(r'\\binom', r'\binom');
    output = output.replaceAll(r'\\sum', r'\sum');
    output = output.replaceAll(r'\\int', r'\int');
    output = output.replaceAllMapped(
      RegExp(r'\\binom\[\s*([^\]]+)\s*\]\s*\{\s*([^}]+)\s*\}'),
      (Match m) =>
          r'\binom{' + m.group(1)!.trim() + '}{' + m.group(2)!.trim() + '}',
    );
    output = output.replaceAllMapped(
      RegExp(
        r'((?:\\int)|(?:∫)|(?:\bint\b))\s*\[\s*([^\]\s]+?)\s*to\s*([^\]\s]+?)\s*\]',
        caseSensitive: false,
      ),
      (Match m) => '\$\\int_{${m.group(2)!.trim()}}^{${m.group(3)!.trim()}}\$',
    );
    output = output.replaceAllMapped(
      RegExp(r'∑\s*\[\s*([a-zA-Z]+)\s*=\s*([^\]\s]+?)\s*to\s*([^\]\s]+?)\s*\]'),
      (Match m) =>
          '\$\\sum_{${m.group(1)!.trim()}=${m.group(2)!.trim()}}^{${m.group(3)!.trim()}}\$',
    );
    output = output.replaceAllMapped(
      RegExp(
        r'\(\s*([^\(\)\n]+?)\s+choose\s+([^\(\)\n]+?)\s*\)',
        caseSensitive: false,
      ),
      (Match m) => '\$\\binom{${m.group(1)!.trim()}}{${m.group(2)!.trim()}}\$',
    );
    output = output.replaceAllMapped(
      RegExp(
        r'\b([a-zA-Z0-9]+)\s+choose\s+([a-zA-Z0-9]+)\b',
        caseSensitive: false,
      ),
      (Match m) => '\$\\binom{${m.group(1)!}}{${m.group(2)!}}\$',
    );
    output = output.replaceAllMapped(
      RegExp(r'\b(\d+|[a-z])\s*[c]\s*(\d+|[a-z])\b', caseSensitive: false),
      (Match m) => '\$\\binom{${m.group(1)!}}{${m.group(2)!}}\$',
    );
    output = output.replaceAllMapped(
      RegExp(r'(?<!\\)\$([^\$\n]+)\$'),
      (Match m) => '\$${normalizeUniversalLatex((m.group(1) ?? '').trim())}\$',
    );
    output = output.replaceAllMapped(
      RegExp(r'\\\((.+?)\\\)'),
      (Match m) =>
          r'\(' + normalizeUniversalLatex((m.group(1) ?? '').trim()) + r'\)',
    );
    output = output.replaceAllMapped(
      RegExp(r'\\\[(.*?)\\\]', dotAll: true),
      (Match m) =>
          r'\[' + normalizeUniversalLatex((m.group(1) ?? '').trim()) + r'\]',
    );
    return output;
  }
}

class _InlineMathSyntax extends md.InlineSyntax {
  _InlineMathSyntax() : super(r'(?<!\\)\$([^\$\n]+)\$');

  @override
  bool onMatch(md.InlineParser parser, Match match) {
    final String math = (match[1] ?? '').trim();
    if (math.isEmpty) {
      return false;
    }
    parser.addNode(
      md.Element.text('math-inline', normalizeUniversalLatex(math)),
    );
    return true;
  }
}

class _ParenInlineMathSyntax extends md.InlineSyntax {
  _ParenInlineMathSyntax() : super(r'\\\((.+?)\\\)');

  @override
  bool onMatch(md.InlineParser parser, Match match) {
    final String math = (match[1] ?? '').trim();
    if (math.isEmpty) {
      return false;
    }
    parser.addNode(
      md.Element.text('math-inline-paren', normalizeUniversalLatex(math)),
    );
    return true;
  }
}

class _InlineMathBuilder extends MarkdownElementBuilder {
  _InlineMathBuilder({required this.style});

  final TextStyle style;

  @override
  Widget visitElementAfter(md.Element element, TextStyle? preferredStyle) {
    final String tex = element.textContent.trim();
    if (tex.isEmpty) {
      return const SizedBox.shrink();
    }
    return SingleChildScrollView(
      scrollDirection: Axis.horizontal,
      child: Math.tex(
        tex,
        textStyle: (preferredStyle ?? style).copyWith(
          fontSize: ((preferredStyle ?? style).fontSize ?? 14) + 1,
        ),
        onErrorFallback: (FlutterMathException e) => Text(
          tex,
          style: (preferredStyle ?? style).copyWith(
            color: (preferredStyle ?? style).color?.withValues(alpha: 0.92),
            fontStyle: FontStyle.italic,
          ),
        ),
      ),
    );
  }
}

enum _SegmentType { markdown, blockMath, legacyMathImage }

class _Segment {
  const _Segment(this.type, this.content);

  final _SegmentType type;
  final String content;
}
