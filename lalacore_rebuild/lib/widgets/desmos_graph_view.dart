import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:webview_flutter/webview_flutter.dart';

import 'liquid_glass.dart';

class DesmosGraphView extends StatefulWidget {
  const DesmosGraphView({
    super.key,
    required this.visualization,
    this.height = 220,
  });

  final Map<String, dynamic> visualization;
  final double height;

  @override
  State<DesmosGraphView> createState() => _DesmosGraphViewState();
}

class _DesmosGraphViewState extends State<DesmosGraphView> {
  WebViewController? _controller;
  bool _ready = false;
  bool _fallback = false;
  Timer? _debounce;
  Timer? _fallbackTimer;

  @override
  void initState() {
    super.initState();
    _init();
  }

  @override
  void didUpdateWidget(covariant DesmosGraphView oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (!mapEquals(oldWidget.visualization, widget.visualization)) {
      _debounce?.cancel();
      _debounce = Timer(const Duration(milliseconds: 120), _applyGraphPayload);
    }
  }

  @override
  void dispose() {
    _debounce?.cancel();
    _fallbackTimer?.cancel();
    super.dispose();
  }

  Future<void> _init() async {
    final List<dynamic> exprs =
        (widget.visualization['expressions'] as List<dynamic>?) ?? <dynamic>[];
    final Map<String, dynamic> viewport =
        widget.visualization['viewport'] is Map
        ? Map<String, dynamic>.from(widget.visualization['viewport'] as Map)
        : const <String, dynamic>{};
    final double range =
        ((viewport['xmax'] ?? 0) as num).toDouble() -
        ((viewport['xmin'] ?? 0) as num).toDouble();
    final bool hasImplicit = exprs.any((dynamic e) {
      final String latex = (e is Map ? e['latex'] : '').toString();
      return latex.contains('=') &&
          !latex.trimLeft().startsWith('x=') &&
          !latex.trimLeft().startsWith('y=');
    });
    final bool heavy = exprs.length > 5 || range.abs() > 200 || hasImplicit;

    _controller = WebViewController()
      ..setJavaScriptMode(JavaScriptMode.unrestricted)
      ..setBackgroundColor(const Color(0x00000000))
      ..setNavigationDelegate(
        NavigationDelegate(
          onWebResourceError: (_) {
            if (mounted) {
              setState(() => _fallback = true);
            }
          },
          onPageFinished: (_) async {
            _ready = true;
            _fallbackTimer?.cancel();
            await _applyGraphPayload();
          },
        ),
      )
      ..loadHtmlString(_htmlTemplate(heavy: heavy));

    _fallbackTimer = Timer(const Duration(milliseconds: 1500), () {
      if (!_ready && mounted) {
        setState(() => _fallback = true);
      }
    });
  }

  Future<void> _applyGraphPayload() async {
    final WebViewController? c = _controller;
    if (c == null || !_ready || _fallback) {
      return;
    }
    final String payload = jsonEncode(widget.visualization);
    final String js = 'window.__setGraph($payload);';
    try {
      await c.runJavaScript(js);
    } catch (_) {
      if (mounted) {
        setState(() => _fallback = true);
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final Widget child = _fallback || _controller == null
        ? _fallbackCard()
        : ClipRRect(
            borderRadius: BorderRadius.circular(22),
            child: WebViewWidget(controller: _controller!),
          );
    return SizedBox(height: widget.height, child: child);
  }

  Widget _fallbackCard() {
    final List<dynamic> exprs =
        (widget.visualization['expressions'] as List<dynamic>?) ?? <dynamic>[];
    return LiquidGlass(
      padding: const EdgeInsets.all(12),
      borderRadius: BorderRadius.circular(22),
      child: ListView(
        physics: const NeverScrollableScrollPhysics(),
        children: <Widget>[
          const Text(
            'Graph Preview (Light Mode)',
            style: TextStyle(fontWeight: FontWeight.w700, fontSize: 12),
          ),
          const SizedBox(height: 8),
          ...exprs.take(4).map((dynamic e) {
            final String latex = (e is Map ? e['latex'] : '').toString();
            return Padding(
              padding: const EdgeInsets.only(bottom: 4),
              child: Text(
                '• $latex',
                maxLines: 2,
                overflow: TextOverflow.ellipsis,
                style: const TextStyle(fontSize: 12),
              ),
            );
          }),
        ],
      ),
    );
  }

  String _htmlTemplate({required bool heavy}) {
    return '''
<!DOCTYPE html>
<html>
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
    <style>
      html, body, #calculator { margin:0; padding:0; width:100%; height:100%; background:transparent; overflow:hidden; }
    </style>
    <script src="https://www.desmos.com/api/v1.11/calculator.js?apiKey=desmos"></script>
  </head>
  <body>
    <div id="calculator"></div>
    <script>
      const elt = document.getElementById('calculator');
      const calculator = Desmos.GraphingCalculator(elt, {
        expressionsCollapsed: true,
        settingsMenu: false,
        zoomButtons: false,
        lockViewport: false,
        keypad: false,
        expressions: true,
        border: false,
      });
      let pending = null;
      function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
      function sanitizeLatex(s) {
        return String(s || '')
          .replace(/[\\u0000-\\u001F\\u007F]/g, '')
          .replace(/`/g, '')
          .replace(/≤/g, '<=')
          .replace(/≥/g, '>=');
      }
      window.__setGraph = function(payload) {
        if (!payload || !Array.isArray(payload.expressions)) return;
        pending = payload;
        requestAnimationFrame(() => {
          if (!pending) return;
          const p = pending;
          pending = null;
          calculator.setBlank();
          const xmin = clamp(Number(p.viewport?.xmin ?? -10), -1000, 1000);
          const xmax = clamp(Number(p.viewport?.xmax ?? 10), -1000, 1000);
          const ymin = clamp(Number(p.viewport?.ymin ?? -10), -1000, 1000);
          const ymax = clamp(Number(p.viewport?.ymax ?? 10), -1000, 1000);
          calculator.setMathBounds({ left: xmin, right: xmax, bottom: ymin, top: ymax });
          const exprs = p.expressions.slice(0, 20).map((e, i) => ({
            id: e.id || ('eq' + (i+1)),
            latex: sanitizeLatex(e.latex),
            color: e.color || '#2D70B3',
            lineStyle: e.lineStyle === 'dashed' ? Desmos.Styles.DASHED : Desmos.Styles.SOLID
          }));
          calculator.setExpressions(exprs);
          ${heavy ? 'calculator.updateSettings({ pointsOfInterest: false, trace: false });' : ''}
        });
      };
    </script>
  </body>
</html>
''';
  }
}
