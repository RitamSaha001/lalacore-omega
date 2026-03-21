String normalizeUniversalLatex(String input) {
  final String trimmed = input.trim();
  if (trimmed.isEmpty) {
    return input;
  }

  // Canonical-pass guard: if this is already plain LaTeX with no loose
  // Unicode symbols, avoid re-normalizing and only cleanup whitespace.
  if (_looksCanonicalLatex(trimmed) && !_containsLooseUnicodeMath(trimmed)) {
    return _finalCleanup(trimmed);
  }

  String out = trimmed;
  out = _normalizeEscapedCommands(out);
  out = _replaceDirectSymbols(out);
  out = _normalizeChemicalMarkup(out);
  out = _normalizeLooseMathOperators(out);
  out = _normalizeUnicodeScripts(out);
  out = _repairBracketBalance(out);
  return _finalCleanup(out);
}

bool _looksCanonicalLatex(String input) {
  return RegExp(r'\\[a-zA-Z]+').hasMatch(input);
}

bool _containsLooseUnicodeMath(String input) {
  return RegExp(
    r'[−—–×·÷±∓∞≈≃≅≠≤≥∝∂∇∑∫∮√°→←↔⇌⇄⇋⇆ℏℓΩωμλθφπΔδαβγηκνρστχψΩ₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾]',
  ).hasMatch(input);
}

String _replaceDirectSymbols(String input) {
  String out = input;
  const Map<String, String> direct = <String, String>{
    '−': '-',
    '—': '-',
    '–': '-',
    '×': r'\times',
    '·': r'\cdot',
    '÷': r'\div',
    '±': r'\pm',
    '∓': r'\mp',
    '∞': r'\infty',
    '≈': r'\approx',
    '≃': r'\simeq',
    '≅': r'\cong',
    '≠': r'\neq',
    '≤': r'\leq',
    '≥': r'\geq',
    '∝': r'\propto',
    '∂': r'\partial',
    '∇': r'\nabla',
    '∑': r'\sum',
    '∫': r'\int',
    '∮': r'\oint',
    '√': r'\sqrt{}',
    '°': r'^\circ',
    '→': r'\rightarrow',
    '←': r'\leftarrow',
    '↔': r'\leftrightarrow',
    '⇌': r'\rightleftharpoons',
    '⇄': r'\rightleftarrows',
    '⇋': r'\leftrightharpoons',
    '⇆': r'\leftrightarrows',
    'ℏ': r'\hbar',
    'ℓ': r'\ell',
    'Ω': r'\Omega',
    'Ω': r'\Omega',
    'μ': r'\mu',
    'π': r'\pi',
    'λ': r'\lambda',
    'θ': r'\theta',
    'φ': r'\phi',
    'Δ': r'\Delta',
    'δ': r'\delta',
    'α': r'\alpha',
    'β': r'\beta',
    'γ': r'\gamma',
    'η': r'\eta',
    'κ': r'\kappa',
    'ν': r'\nu',
    'ρ': r'\rho',
    'σ': r'\sigma',
    'τ': r'\tau',
    'χ': r'\chi',
    'ψ': r'\psi',
    'ω': r'\omega',
  };
  direct.forEach((String from, String to) {
    out = out.replaceAll(from, to);
  });
  return out;
}

String _normalizeEscapedCommands(String input) {
  String out = input;
  // Some providers return over-escaped LaTeX command starts (e.g. \\frac).
  out = out.replaceAllMapped(
    RegExp(r'\\\\([A-Za-z])'),
    (Match m) => r'\' + (m.group(1) ?? ''),
  );
  return out;
}

String _normalizeChemicalMarkup(String input) {
  String out = input;
  out = out.replaceAllMapped(RegExp(r'\\ce\{([^}]*)\}'), (Match m) {
    final String body = (m.group(1) ?? '').trim();
    if (body.isEmpty) {
      return '';
    }
    return _chemicalToLatex(body);
  });
  return out;
}

String _chemicalToLatex(String source) {
  String s = source
      .replaceAll('<=>', r' \rightleftharpoons ')
      .replaceAll('<->', r' \leftrightarrow ')
      .replaceAll('->', r' \rightarrow ')
      .replaceAll('<-', r' \leftarrow ');

  final StringBuffer out = StringBuffer();
  int i = 0;
  while (i < s.length) {
    final String c = s[i];
    final bool isDigit = RegExp(r'[0-9]').hasMatch(c);
    final bool subscriptContext =
        i > 0 && RegExp(r'[A-Za-z\)\]]').hasMatch(s[i - 1]);
    if (isDigit && subscriptContext) {
      final int start = i;
      while (i < s.length && RegExp(r'[0-9]').hasMatch(s[i])) {
        i++;
      }
      out.write('_{${s.substring(start, i)}}');
      continue;
    }
    out.write(c);
    i++;
  }

  // Basic charge handling (Na+, Ca2+, SO4^2- style).
  return out.toString().replaceAllMapped(
    RegExp(r'([A-Za-z\}\]])([0-9]*)([\+\-])\b'),
    (Match m) {
      final String num = (m.group(2) ?? '').trim();
      final String sign = (m.group(3) ?? '').trim();
      final String charge = num.isEmpty ? sign : '$num$sign';
      return '${m.group(1)}^{${charge}}';
    },
  );
}

String _normalizeLooseMathOperators(String input) {
  String out = input;
  out = out.replaceAllMapped(
    RegExp(r'(?<!\\)(<->|<=>)'),
    (_) => r'\leftrightarrow',
  );
  out = out.replaceAllMapped(RegExp(r'(?<!\\)(->|=>)'), (_) => r'\rightarrow');
  out = out.replaceAllMapped(
    RegExp(r'(?<!\\)\bdeg\b', caseSensitive: false),
    (_) => r'^\circ',
  );
  out = out.replaceAllMapped(
    RegExp(r'\\sqrt\{\}([a-zA-Z0-9\(\)\[\]\{\}\\\+\-\*/\^\.]+)'),
    (Match m) => r'\sqrt{' + (m.group(1) ?? '').trim() + '}',
  );
  out = out.replaceAllMapped(
    RegExp(r'\\times\s*10\^(-?\d+)'),
    (Match m) => r'\times 10^{' + (m.group(1) ?? '') + '}',
  );
  out = out.replaceAllMapped(
    RegExp(r'(?<!\\)([A-Za-z])_([0-9]+)'),
    (Match m) => '${m.group(1)}_{${m.group(2)}}',
  );
  out = out.replaceAllMapped(
    RegExp(r'(\^\s*)([0-9]+)(?!\})'),
    (Match m) => '${m.group(1)}{${m.group(2)}}',
  );
  out = out.replaceAllMapped(
    RegExp(r'(_\s*)([0-9]+)(?!\})'),
    (Match m) => '${m.group(1)}{${m.group(2)}}',
  );
  return out;
}

String _normalizeUnicodeScripts(String input) {
  String out = input;
  const Map<String, String> superscriptMap = <String, String>{
    '⁰': '0',
    '¹': '1',
    '²': '2',
    '³': '3',
    '⁴': '4',
    '⁵': '5',
    '⁶': '6',
    '⁷': '7',
    '⁸': '8',
    '⁹': '9',
    '⁺': '+',
    '⁻': '-',
    '⁼': '=',
    '⁽': '(',
    '⁾': ')',
  };
  const Map<String, String> subscriptMap = <String, String>{
    '₀': '0',
    '₁': '1',
    '₂': '2',
    '₃': '3',
    '₄': '4',
    '₅': '5',
    '₆': '6',
    '₇': '7',
    '₈': '8',
    '₉': '9',
    '₊': '+',
    '₋': '-',
    '₌': '=',
    '₍': '(',
    '₎': ')',
  };

  out = out.replaceAllMapped(RegExp(r'[⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾]+'), (Match m) {
    final String raw = m.group(0) ?? '';
    final String normalized = raw.split('').map((String c) {
      return superscriptMap[c] ?? c;
    }).join();
    return '^{${normalized}}';
  });
  out = out.replaceAllMapped(RegExp(r'[₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎]+'), (Match m) {
    final String raw = m.group(0) ?? '';
    final String normalized = raw.split('').map((String c) {
      return subscriptMap[c] ?? c;
    }).join();
    return '_{${normalized}}';
  });
  return out;
}

String _repairBracketBalance(String input) {
  int openCurly = 0;
  int openSquare = 0;
  int openParen = 0;
  for (int i = 0; i < input.length; i++) {
    final String c = input[i];
    final bool escaped = i > 0 && input[i - 1] == r'\';
    if (escaped) {
      continue;
    }
    switch (c) {
      case '{':
        openCurly++;
        break;
      case '}':
        if (openCurly > 0) {
          openCurly--;
        }
        break;
      case '[':
        openSquare++;
        break;
      case ']':
        if (openSquare > 0) {
          openSquare--;
        }
        break;
      case '(':
        openParen++;
        break;
      case ')':
        if (openParen > 0) {
          openParen--;
        }
        break;
    }
  }
  if (openCurly == 0 && openSquare == 0 && openParen == 0) {
    return input;
  }
  final StringBuffer out = StringBuffer(input);
  if (openParen > 0) {
    out.write(List<String>.filled(openParen, ')').join());
  }
  if (openSquare > 0) {
    out.write(List<String>.filled(openSquare, ']').join());
  }
  if (openCurly > 0) {
    out.write(List<String>.filled(openCurly, '}').join());
  }
  return out.toString();
}

String _finalCleanup(String input) {
  return input
      .replaceAll('\r\n', '\n')
      .replaceAll(RegExp(r'[ \t]+'), ' ')
      .replaceAll(RegExp(r' *\n *'), '\n')
      .replaceAll(RegExp(r'\n{3,}'), '\n\n')
      .trim();
}
