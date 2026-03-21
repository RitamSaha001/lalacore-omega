/**
 * LC9 Teacher Import + Review Pipeline
 *
 * Sheets:
 * - LC9_IMPORT_DRAFTS
 * - LC9_QUESTION_BANK
 */

var LC9_SHEET_IMPORT_DRAFTS = 'LC9_IMPORT_DRAFTS';
var LC9_SHEET_QUESTION_BANK = 'LC9_QUESTION_BANK';

function lc9_detectQuestionType(sectionText, questionText) {
  var bag = String(sectionText || '') + ' ' + String(questionText || '');
  bag = bag.toLowerCase();

  var multiKeywords = [
    'select all correct options',
    'more than one correct',
    'one or more options may be correct',
    'multiple correct',
    'multi correct'
  ];
  var numericalKeywords = [
    'integer type',
    'numerical answer type',
    'enter the correct value',
    'answer in',
    'answer upto',
    'answer up to',
    'decimal places',
    'numerical value'
  ];

  for (var i = 0; i < multiKeywords.length; i++) {
    if (bag.indexOf(multiKeywords[i]) >= 0) {
      return 'MCQ_MULTI';
    }
  }
  for (var j = 0; j < numericalKeywords.length; j++) {
    if (bag.indexOf(numericalKeywords[j]) >= 0) {
      return 'NUMERICAL';
    }
  }
  return 'MCQ_SINGLE';
}

function lc9_parseQuestions(rawText, metadata) {
  var meta = metadata || {};
  var subject = String(meta.subject || '');
  var chapter = String(meta.chapter || '');
  var difficulty = String(meta.difficulty || 'Hard');

  var text = String(rawText || '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n');
  var lines = text.split('\n')
    .map(function(line) {
      return String(line || '').replace(/\t/g, ' ').replace(/\s{2,}/g, ' ').trimRight();
    })
    .filter(function(line) {
      return line.trim() !== '';
    });

  var questionStartRe = /^\s*(?:q(?:uestion)?\s*)?\d+\s*[\).:\-]\s*/i;
  var optionStartRe = /^\s*(?:\(?([A-Za-z]|[1-9])\)?[\).:\-])\s*(.+)$/;
  var answerRe = /^\s*(?:ans(?:wer)?|correct(?:\s*answer)?)\s*[:\-]\s*(.+)$/i;

  var blocks = [];
  var current = null;
  var activeInstruction = '';

  function flushCurrent() {
    if (!current) {
      return;
    }
    if (String(current.question_text || '').trim() !== '') {
      blocks.push(current);
    }
    current = null;
  }

  lines.forEach(function(line) {
    if (!current && lc9_isInstructionLine(line)) {
      activeInstruction = line;
      return;
    }

    if (questionStartRe.test(line)) {
      flushCurrent();
      current = {
        question_text: line.replace(questionStartRe, '').trim(),
        options: [],
        answer_hint: '',
        section_instruction: activeInstruction
      };
      return;
    }

    if (!current) {
      if (lc9_isInstructionLine(line)) {
        activeInstruction = line;
      }
      return;
    }

    var ansMatch = line.match(answerRe);
    if (ansMatch) {
      current.answer_hint = String(ansMatch[1] || '').trim();
      return;
    }

    var optionMatch = line.match(optionStartRe);
    if (optionMatch) {
      var label = lc9_normalizeOptionLabel(optionMatch[1]);
      var optionText = String(optionMatch[2] || '').trim();
      if (label && optionText) {
        current.options.push({label: label, text: optionText});
        current._active_option = label;
        return;
      }
    }

    if (current._active_option) {
      var idx = current.options.length - 1;
      if (idx >= 0) {
        current.options[idx].text = String(current.options[idx].text + ' ' + line)
          .replace(/\s{2,}/g, ' ')
          .trim();
        return;
      }
    }

    current._active_option = '';
    current.question_text = String(current.question_text + ' ' + line)
      .replace(/\s{2,}/g, ' ')
      .trim();
  });

  flushCurrent();

  var structured = [];
  for (var i = 0; i < blocks.length; i++) {
    var block = blocks[i];
    var dedupedOptions = lc9_deduplicateOptions(block.options || []);
    var parsedHint = lc9_parseAnswerHint(block.answer_hint, dedupedOptions);

    var detectedType = lc9_detectQuestionType(block.section_instruction, block.question_text);
    if (parsedHint.multiple.length > 1) {
      detectedType = 'MCQ_MULTI';
    } else if (parsedHint.numerical !== null && parsedHint.numerical !== '') {
      detectedType = 'NUMERICAL';
    } else if (dedupedOptions.length === 0 && lc9_looksNumericalPrompt(block.question_text)) {
      detectedType = 'NUMERICAL';
    }

    var options = dedupedOptions;
    if (detectedType === 'NUMERICAL') {
      options = [];
    }

    var correct = {
      single: null,
      multiple: [],
      numerical: null
    };

    if (detectedType === 'MCQ_SINGLE') {
      correct.single = parsedHint.multiple.length > 0 ? parsedHint.multiple[0] : null;
      if (correct.single) {
        correct.multiple = [correct.single];
      }
    } else if (detectedType === 'MCQ_MULTI') {
      correct.multiple = parsedHint.multiple.slice();
      correct.single = correct.multiple.length > 0 ? correct.multiple[0] : null;
    } else {
      correct.numerical = parsedHint.numerical;
    }

    var question = {
      question_id: 'imp_q_' + (i + 1),
      type: detectedType,
      question_text: String(block.question_text || '').trim(),
      options: options,
      correct_answer: correct,
      subject: subject,
      chapter: chapter,
      difficulty: difficulty,
      ai_confidence: 0.0,
      validation_status: 'review',
      validation_errors: []
    };

    structured.push(lc9_validateQuestion(question));
  }

  return structured;
}

function lc9_validateQuestion(question) {
  var q = JSON.parse(JSON.stringify(question || {}));
  var errors = [];

  q.question_id = String(q.question_id || '').trim();
  q.question_text = String(q.question_text || '').trim();
  q.type = String(q.type || 'MCQ_SINGLE').trim().toUpperCase();
  if (['MCQ_SINGLE', 'MCQ_MULTI', 'NUMERICAL'].indexOf(q.type) < 0) {
    q.type = 'MCQ_SINGLE';
  }

  if (q.question_text === '') {
    errors.push('Question text cannot be empty.');
  }

  var options = Array.isArray(q.options) ? q.options : [];
  options = options.map(function(opt, idx) {
    var label = String((opt || {}).label || String.fromCharCode(65 + idx)).trim().toUpperCase();
    var text = String((opt || {}).text || '').trim();
    return {label: label, text: text};
  });

  var seen = {};
  options.forEach(function(opt) {
    if (!opt.text) {
      errors.push('Option ' + opt.label + ' cannot be empty.');
      return;
    }
    var key = opt.text.toLowerCase();
    if (seen[key]) {
      errors.push('Duplicate options are invalid.');
    }
    seen[key] = true;
  });

  var ca = q.correct_answer || {single: null, multiple: [], numerical: null};
  var single = ca.single ? String(ca.single).trim().toUpperCase() : '';
  var multiple = Array.isArray(ca.multiple)
    ? ca.multiple.map(function(v) { return String(v || '').trim().toUpperCase(); }).filter(function(v) { return v; })
    : [];
  var numerical = ca.numerical === null || ca.numerical === undefined
    ? ''
    : String(ca.numerical).trim();

  var labels = options.map(function(opt) { return opt.label; });

  var status = 'valid';
  if (q.type === 'MCQ_SINGLE') {
    if (options.length < 2) {
      errors.push('MCQ_SINGLE requires at least 2 options.');
    }
    if (!single) {
      errors.push('MCQ_SINGLE requires one correct answer.');
    } else if (labels.indexOf(single) < 0) {
      errors.push('MCQ_SINGLE correct answer label is invalid.');
    }
  } else if (q.type === 'MCQ_MULTI') {
    if (options.length < 2) {
      errors.push('MCQ_MULTI requires at least 2 options.');
    }
    if (multiple.length < 1) {
      errors.push('MCQ_MULTI requires one or more correct answers.');
    } else {
      for (var i = 0; i < multiple.length; i++) {
        if (labels.indexOf(multiple[i]) < 0) {
          errors.push('MCQ_MULTI contains invalid answer label: ' + multiple[i]);
        }
      }
      if (multiple.length === 1) {
        status = 'review';
        errors.push('Multi-correct has only one answer; review required.');
      }
    }
  } else {
    if (options.length > 0) {
      errors.push('NUMERICAL must not contain options.');
    }
    if (!lc9_parseNumericSafe(numerical)) {
      errors.push('NUMERICAL answer must be a valid number.');
    }
  }

  if (q.type !== 'NUMERICAL' && options.length === 0) {
    errors.push('No options detected for non-numerical question.');
  }

  if (errors.length > 0) {
    var hardFail = errors.some(function(err) {
      return /requires|must|cannot|invalid/i.test(err);
    });
    status = hardFail ? 'invalid' : 'review';
  }

  q.options = q.type === 'NUMERICAL' ? [] : options;
  q.correct_answer = {
    single: single || null,
    multiple: multiple,
    numerical: numerical || null
  };
  q.validation_status = status;
  q.validation_errors = errors;
  return q;
}

function lc9_applyAiValidation(question, aiConfig) {
  var q = JSON.parse(JSON.stringify(question || {}));
  var cfg = aiConfig || {};
  var endpoint = String(cfg.endpoint || '').trim();
  if (!endpoint) {
    return q;
  }

  var payload = {
    prompt: [
      'Validate this question structure and return strict JSON only with keys:',
      'confidence_score, structure_valid, suggested_type, suggested_difficulty, issues_detected.',
      JSON.stringify(q)
    ].join('\n'),
    question: q,
    detected_type: q.type
  };

  try {
    var res = UrlFetchApp.fetch(endpoint, {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });
    var body = String(res.getContentText() || '').trim();
    var decoded = lc9_tryJsonParse(body) || {};
    var confidence = Number(decoded.confidence_score || 0);
    var suggestedType = String(decoded.suggested_type || '').trim().toUpperCase();
    var issues = Array.isArray(decoded.issues_detected) ? decoded.issues_detected : [];

    q.ai_confidence = isNaN(confidence) ? 0 : confidence;
    if (decoded.suggested_difficulty) {
      q.difficulty = String(decoded.suggested_difficulty).trim();
    }

    var errors = Array.isArray(q.validation_errors) ? q.validation_errors.slice() : [];
    if (decoded.structure_valid === false) {
      errors.push('AI flagged structure as invalid.');
      q.validation_status = 'invalid';
    }
    if (suggestedType && suggestedType !== q.type) {
      errors.push('AI suggested type mismatch: ' + suggestedType);
      if (q.validation_status === 'valid') {
        q.validation_status = 'review';
      }
    }
    if (q.ai_confidence < 0.6) {
      errors.push('AI confidence below 0.6');
      if (q.validation_status === 'valid') {
        q.validation_status = 'review';
      }
    }
    for (var i = 0; i < issues.length; i++) {
      var issue = String(issues[i] || '').trim();
      if (issue) {
        errors.push(issue);
      }
    }
    q.validation_errors = lc9_uniqueStrings(errors);
  } catch (err) {
    var existing = Array.isArray(q.validation_errors) ? q.validation_errors.slice() : [];
    existing.push('AI validation request failed: ' + err);
    if (q.validation_status === 'valid') {
      q.validation_status = 'review';
    }
    q.validation_errors = lc9_uniqueStrings(existing);
  }

  return q;
}

function lc9_saveImportDrafts(questions, meta) {
  var sheet = lc9_getOrCreateSheet(LC9_SHEET_IMPORT_DRAFTS);
  var rows = [];
  var now = new Date();
  var actor = String((meta || {}).teacher_id || 'teacher').trim();

  for (var i = 0; i < questions.length; i++) {
    var q = lc9_validateQuestion(questions[i]);
    rows.push([
      now,
      actor,
      String(q.question_id || ''),
      String(q.validation_status || 'review'),
      JSON.stringify(q)
    ]);
  }
  if (rows.length === 0) {
    return {ok: true, saved: 0};
  }
  var start = Math.max(1, sheet.getLastRow()) + 1;
  sheet.getRange(start, 1, rows.length, rows[0].length).setValues(rows);
  return {ok: true, saved: rows.length};
}

function lc9_publishQuestions(questions, meta) {
  var validRows = [];
  var invalid = [];
  var teacherId = String((meta || {}).teacher_id || 'teacher').trim();
  var now = new Date();

  for (var i = 0; i < questions.length; i++) {
    var q = lc9_validateQuestion(questions[i]);
    if (q.validation_status === 'invalid') {
      invalid.push({index: i, question_id: q.question_id, errors: q.validation_errors});
      continue;
    }
    validRows.push([
      now,
      teacherId,
      String(q.question_id || ''),
      String(q.type || ''),
      String(q.subject || ''),
      String(q.chapter || ''),
      String(q.validation_status || 'review'),
      JSON.stringify(q)
    ]);
  }

  if (invalid.length > 0) {
    return {
      ok: false,
      status: 'VALIDATION_FAILED',
      invalid: invalid,
      published: 0
    };
  }

  if (validRows.length === 0) {
    return {ok: true, published: 0};
  }

  var sheet = lc9_getOrCreateSheet(LC9_SHEET_QUESTION_BANK);
  var start = Math.max(1, sheet.getLastRow()) + 1;
  sheet.getRange(start, 1, validRows.length, validRows[0].length).setValues(validRows);

  return {
    ok: true,
    status: 'SUCCESS',
    published: validRows.length
  };
}

function doPost(e) {
  var payload = {};
  try {
    payload = JSON.parse((e && e.postData && e.postData.contents) || '{}');
  } catch (err) {
    return lc9_jsonOut({ok: false, status: 'BAD_JSON', message: String(err)});
  }

  var action = String(payload.action || '').trim();
  if (!action) {
    return lc9_jsonOut({ok: false, status: 'MISSING_ACTION'});
  }

  if (action === 'lc9_parse_questions') {
    var parsed = lc9_parseQuestions(payload.raw_text || '', payload.meta || {});
    if (payload.ai_validation === true) {
      parsed = parsed.map(function(q) {
        return lc9_applyAiValidation(q, payload.ai_config || {});
      });
    }
    return lc9_jsonOut({ok: true, status: 'SUCCESS', questions: parsed});
  }

  if (action === 'lc9_save_import_drafts') {
    var draftList = Array.isArray(payload.questions) ? payload.questions : [];
    var saved = lc9_saveImportDrafts(draftList, payload.meta || {});
    return lc9_jsonOut(saved);
  }

  if (action === 'lc9_publish_questions') {
    var publishList = Array.isArray(payload.questions) ? payload.questions : [];
    var out = lc9_publishQuestions(publishList, payload.meta || {});
    return lc9_jsonOut(out);
  }

  return lc9_jsonOut({ok: false, status: 'UNKNOWN_ACTION', action: action});
}

function lc9_jsonOut(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj || {}))
    .setMimeType(ContentService.MimeType.JSON);
}

function lc9_getOrCreateSheet(name) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    if (name === LC9_SHEET_IMPORT_DRAFTS) {
      sheet.getRange(1, 1, 1, 5).setValues([[
        'created_at', 'teacher_id', 'question_id', 'validation_status', 'question_json'
      ]]);
    } else if (name === LC9_SHEET_QUESTION_BANK) {
      sheet.getRange(1, 1, 1, 8).setValues([[
        'published_at', 'teacher_id', 'question_id', 'type',
        'subject', 'chapter', 'validation_status', 'question_json'
      ]]);
    }
  }
  return sheet;
}

function lc9_deduplicateOptions(options) {
  var seen = {};
  var out = [];
  for (var i = 0; i < options.length; i++) {
    var opt = options[i] || {};
    var label = lc9_normalizeOptionLabel(opt.label || String.fromCharCode(65 + i));
    var text = String(opt.text || '').replace(/\s{2,}/g, ' ').trim();
    if (!label || !text) {
      continue;
    }
    var key = text.toLowerCase();
    if (seen[key]) {
      continue;
    }
    seen[key] = true;
    out.push({label: label, text: text});
  }
  return out;
}

function lc9_parseAnswerHint(answerHint, options) {
  var raw = String(answerHint || '').trim();
  var labels = [];
  var numeric = null;

  if (raw) {
    var parts = raw.split(/[,/;|]+/).map(function(v) {
      return String(v || '').trim();
    }).filter(function(v) {
      return v;
    });

    var seen = {};
    for (var i = 0; i < parts.length; i++) {
      var label = lc9_normalizeOptionLabel(parts[i]);
      if (!label) {
        var partLower = parts[i].toLowerCase();
        for (var j = 0; j < options.length; j++) {
          if (String(options[j].text || '').toLowerCase() === partLower) {
            label = options[j].label;
            break;
          }
        }
      }
      if (label && !seen[label]) {
        seen[label] = true;
        labels.push(label);
      }
    }

    var numMatch = raw.match(/[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?/);
    if (numMatch && numMatch[0]) {
      numeric = numMatch[0];
    }
  }

  return {
    multiple: labels,
    numerical: numeric
  };
}

function lc9_parseNumericSafe(raw) {
  var text = String(raw || '').trim();
  if (!text) {
    return null;
  }
  var m = text.match(/[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?/);
  if (!m || !m[0]) {
    return null;
  }
  var n = Number(m[0]);
  if (isNaN(n) || !isFinite(n)) {
    return null;
  }
  return n;
}

function lc9_looksNumericalPrompt(questionText) {
  var lower = String(questionText || '').toLowerCase();
  return lower.indexOf('integer') >= 0 ||
    lower.indexOf('numerical') >= 0 ||
    lower.indexOf('decimal') >= 0 ||
    lower.indexOf('answer in') >= 0;
}

function lc9_isInstructionLine(line) {
  var lower = String(line || '').toLowerCase();
  return lower.indexOf('section') === 0 ||
    lower.indexOf('select all correct') >= 0 ||
    lower.indexOf('more than one correct') >= 0 ||
    lower.indexOf('choose the correct option') >= 0 ||
    lower.indexOf('numerical answer type') >= 0 ||
    lower.indexOf('integer type') >= 0;
}

function lc9_normalizeOptionLabel(raw) {
  var text = String(raw || '').trim().toUpperCase();
  if (!text) {
    return '';
  }
  if (/^[A-Z]$/.test(text)) {
    return text;
  }
  if (/^[1-9]$/.test(text)) {
    return String.fromCharCode(64 + Number(text));
  }
  if (text.length >= 2 && /^[A-Z][\).:\-]$/.test(text.substring(0, 2))) {
    return text.charAt(0);
  }
  return '';
}

function lc9_tryJsonParse(raw) {
  var text = String(raw || '').trim();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch (e) {}

  var a = text.indexOf('{');
  var b = text.lastIndexOf('}');
  if (a >= 0 && b > a) {
    try {
      return JSON.parse(text.substring(a, b + 1));
    } catch (e2) {}
  }
  return null;
}

function lc9_uniqueStrings(list) {
  var seen = {};
  var out = [];
  (list || []).forEach(function(v) {
    var s = String(v || '').trim();
    if (!s || seen[s]) {
      return;
    }
    seen[s] = true;
    out.push(s);
  });
  return out;
}
