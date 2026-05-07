const PUBLIC_BACKEND = "https://web-production-75f40.up.railway.app";
const STORE_KEY = "lalacore_ai_chat_web_state_v1";
const SETTINGS_KEY = "lalacore_ai_chat_web_settings_v1";

const STRONG_MODEL_PRIORITY = [
  "gemini-2.5-pro",
  "deepseek/deepseek-r1:free",
  "deepseek-r1-distill-llama-70b",
  "gemini-2.5-flash",
  "openrouter/free",
  "gemini-2.5-flash-lite",
  "meta-llama/llama-3.1-8b-instruct",
  "llama-3.1-8b-instant",
];

const PROMPTS = [
  "Solve this JEE maths problem step by step.",
  "Explain the concept behind this question.",
  "Check my answer and point out the mistake.",
  "Create revision notes for this topic.",
];

const state = {
  backendBaseUrl: "",
  accountId: "web_student",
  userId: "web_student",
  chatFunction: "general_chat",
  bestModel: true,
  currentChatId: "",
  sessions: [],
  messages: [],
  busy: false,
  attachment: null,
};

const els = {
  shell: document.querySelector(".app-shell"),
  historyList: document.getElementById("historyList"),
  historySearch: document.getElementById("historySearch"),
  newChatButton: document.getElementById("newChatButton"),
  refreshHistoryButton: document.getElementById("refreshHistoryButton"),
  sidebarToggle: document.getElementById("sidebarToggle"),
  settingsButton: document.getElementById("settingsButton"),
  settingsPanel: document.getElementById("settingsPanel"),
  backendUrl: document.getElementById("backendUrl"),
  accountId: document.getElementById("accountId"),
  userId: document.getElementById("userId"),
  chatFunction: document.getElementById("chatFunction"),
  bestModelToggle: document.getElementById("bestModelToggle"),
  saveSettingsButton: document.getElementById("saveSettingsButton"),
  chatTitle: document.getElementById("chatTitle"),
  chatSubtitle: document.getElementById("chatSubtitle"),
  messages: document.getElementById("messages"),
  emptyState: document.getElementById("emptyState"),
  promptChips: document.getElementById("promptChips"),
  composer: document.getElementById("composer"),
  promptInput: document.getElementById("promptInput"),
  sendButton: document.getElementById("sendButton"),
  attachButton: document.getElementById("attachButton"),
  fileInput: document.getElementById("fileInput"),
  attachmentTray: document.getElementById("attachmentTray"),
  messageTemplate: document.getElementById("messageTemplate"),
};

function nowMs() {
  return Date.now();
}

function safeJsonParse(raw, fallback) {
  try {
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

function inferBackendBaseUrl() {
  const params = new URLSearchParams(window.location.search);
  const queryBackend = params.get("backend");
  if (queryBackend) return queryBackend.trim();

  const origin = window.location.origin;
  if (origin && origin !== "null") {
    const host = window.location.hostname;
    const port = window.location.port;
    const servedByLikelyBackend =
      window.location.pathname.startsWith("/ai-chat") &&
      (port === "8000" || port === "8803" || port === "" || !host.includes("localhost"));
    if (servedByLikelyBackend) return origin;
  }
  return PUBLIC_BACKEND;
}

function loadSettings() {
  const saved = safeJsonParse(localStorage.getItem(SETTINGS_KEY), {});
  const params = new URLSearchParams(window.location.search);
  state.backendBaseUrl = (params.get("backend") || saved.backendBaseUrl || inferBackendBaseUrl()).trim();
  state.accountId = (params.get("account") || saved.accountId || "web_student").trim();
  state.userId = (params.get("user") || saved.userId || state.accountId || "web_student").trim();
  state.chatFunction = (params.get("function") || saved.chatFunction || "general_chat").trim();
  state.bestModel = saved.bestModel !== false;
}

function saveSettings() {
  const next = {
    backendBaseUrl: els.backendUrl.value.trim() || inferBackendBaseUrl(),
    accountId: els.accountId.value.trim() || "web_student",
    userId: els.userId.value.trim() || els.accountId.value.trim() || "web_student",
    chatFunction: els.chatFunction.value || "general_chat",
    bestModel: els.bestModelToggle.checked,
  };
  Object.assign(state, next);
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(next));
  syncSettingsFields();
  refreshSubtitle();
  toast("Settings saved.");
  refreshRemoteHistory();
}

function loadLocalState() {
  const saved = safeJsonParse(localStorage.getItem(STORE_KEY), {});
  state.sessions = Array.isArray(saved.sessions) ? saved.sessions : [];
  state.currentChatId = saved.currentChatId || makeChatId();
  const current = state.sessions.find((session) => session.chat_id === state.currentChatId);
  state.messages = current && Array.isArray(current.messages) ? current.messages : [];
  ensureCurrentSession();
}

function saveLocalState() {
  const index = state.sessions.findIndex((session) => session.chat_id === state.currentChatId);
  const session = buildSessionRecord();
  session.messages = state.messages.filter((message) => !message.transient);
  if (index >= 0) {
    state.sessions[index] = { ...state.sessions[index], ...session };
  } else {
    state.sessions.unshift(session);
  }
  state.sessions.sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0));
  localStorage.setItem(
    STORE_KEY,
    JSON.stringify({
      currentChatId: state.currentChatId,
      sessions: state.sessions.slice(0, 250),
    }),
  );
}

function makeChatId() {
  return `ai_web_${nowMs()}_${Math.random().toString(16).slice(2, 8)}`;
}

function ensureCurrentSession() {
  if (!state.currentChatId) state.currentChatId = makeChatId();
  if (!state.sessions.some((session) => session.chat_id === state.currentChatId)) {
    state.sessions.unshift(buildSessionRecord());
  }
}

function buildSessionRecord() {
  const existing = state.sessions.find((session) => session.chat_id === state.currentChatId) || {};
  const firstUser = state.messages.find((message) => message.role === "user");
  const title =
    existing.title && existing.title !== "New AI Chat"
      ? existing.title
      : titleFromPrompt(firstUser ? firstUser.content || firstUser.text || "" : "");
  return {
    chat_id: state.currentChatId,
    chatId: state.currentChatId,
    account_id: state.accountId,
    user_id: state.userId,
    title: title || "New AI Chat",
    created_at: existing.created_at || nowMs(),
    updated_at: nowMs(),
    message_count: state.messages.filter((message) => !message.transient).length,
    ai_generated_title: Boolean(existing.ai_generated_title),
    pinned: Boolean(existing.pinned),
    pinned_at: existing.pinned_at || 0,
  };
}

function titleFromPrompt(prompt) {
  const cleaned = String(prompt)
    .replace(/\[(image sent|pdf sent:[^\]]+)\]/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return "New AI Chat";
  return cleaned.split(/\s+/).slice(0, 7).join(" ").replace(/[.!?]+$/, "");
}

function syncSettingsFields() {
  els.backendUrl.value = state.backendBaseUrl;
  els.accountId.value = state.accountId;
  els.userId.value = state.userId;
  els.chatFunction.value = state.chatFunction;
  els.bestModelToggle.checked = state.bestModel;
}

function endpointUrl() {
  const raw = (state.backendBaseUrl || inferBackendBaseUrl()).trim().replace(/\/+$/, "");
  if (/\/app\/action$/i.test(raw)) return raw;
  return `${raw}/app/action`;
}

function canAutoLoadRemoteHistory() {
  const params = new URLSearchParams(window.location.search);
  if (params.has("backend")) return true;
  const saved = safeJsonParse(localStorage.getItem(SETTINGS_KEY), {});
  if (saved.backendBaseUrl) return true;
  try {
    return new URL(endpointUrl()).origin === window.location.origin;
  } catch {
    return false;
  }
}

function refreshSubtitle(extra = "") {
  const label = state.bestModel ? "strong model routing" : "standard routing";
  const mode = state.chatFunction.replace(/_/g, " ");
  els.chatSubtitle.textContent = extra || `${mode} - ${label}`;
}

function renderPromptChips() {
  els.promptChips.replaceChildren(
    ...PROMPTS.map((prompt) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "prompt-chip";
      button.textContent = prompt;
      button.addEventListener("click", () => {
        els.promptInput.value = prompt;
        autoSizePrompt();
        els.promptInput.focus();
      });
      return button;
    }),
  );
}

function renderHistory() {
  const q = els.historySearch.value.trim().toLowerCase();
  const rows = state.sessions
    .filter((session) => String(session.account_id || state.accountId).toLowerCase() === state.accountId.toLowerCase())
    .filter((session) => {
      if (!q) return true;
      return `${session.title || ""} ${session.chat_id || ""}`.toLowerCase().includes(q);
    })
    .sort((a, b) => Number(b.pinned || false) - Number(a.pinned || false) || (b.updated_at || 0) - (a.updated_at || 0));

  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "history-empty";
    empty.textContent = "No saved chats yet.";
    els.historyList.replaceChildren(empty);
    return;
  }

  els.historyList.replaceChildren(
    ...rows.map((session) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `history-item${session.chat_id === state.currentChatId ? " active" : ""}`;
      const title = document.createElement("strong");
      title.textContent = session.pinned ? `${session.title || "AI Chat"} [Pinned]` : session.title || "AI Chat";
      const meta = document.createElement("p");
      meta.textContent = `${session.message_count || 0} messages - ${formatTime(session.updated_at)}`;
      button.append(title, meta);
      button.addEventListener("click", () => openChat(session.chat_id));
      return button;
    }),
  );
}

function renderMessages() {
  els.emptyState.hidden = state.messages.length > 0;
  els.messages.querySelectorAll(".message").forEach((node) => node.remove());
  els.messages.append(...state.messages.map((message) => renderMessage(message)));
  refreshTitle();
  renderHistory();
  requestAnimationFrame(() => {
    els.messages.scrollTop = els.messages.scrollHeight;
  });
}

function renderMessage(message) {
  const node = els.messageTemplate.content.firstElementChild.cloneNode(true);
  const role = message.role || "assistant";
  node.classList.add(role);
  if (message.ai_failure || message.error) node.classList.add("error");

  const meta = node.querySelector(".message-meta");
  const content = node.querySelector(".message-content");
  const extras = node.querySelector(".message-extras");

  meta.replaceChildren(...messageMetaNodes(message));
  if (message.typing) {
    content.innerHTML = '<span class="typing"><span></span><span></span><span></span></span>';
  } else {
    content.innerHTML = renderRichText(message.content || message.text || "");
  }
  renderExtras(extras, message);
  return node;
}

function messageMetaNodes(message) {
  const nodes = [];
  const role = document.createElement("span");
  role.textContent = message.role === "user" ? "You" : "LalaCore";
  nodes.push(role);
  if (message.meta) nodes.push(pill(message.meta, ""));
  if (message.confidence) nodes.push(pill(`confidence ${message.confidence}`, "amber"));
  if (message.concept) nodes.push(pill(message.concept, "teal"));
  return nodes;
}

function pill(text, tone) {
  const span = document.createElement("span");
  span.className = `pill ${tone}`.trim();
  span.textContent = text;
  return span;
}

function renderExtras(container, message) {
  container.replaceChildren();
  const retrieval = message.web_retrieval;
  if (retrieval && typeof retrieval === "object") {
    const panel = extraPanel("Sources and retrieval");
    const list = document.createElement("ul");
    const sourceRows = [];
    if (Array.isArray(retrieval.sources_consulted)) sourceRows.push(...retrieval.sources_consulted);
    if (Array.isArray(retrieval.citations)) {
      retrieval.citations.slice(0, 6).forEach((row) => sourceRows.push(row.url || row.source_url || row.title || JSON.stringify(row)));
    }
    sourceRows.slice(0, 6).forEach((source) => {
      const li = document.createElement("li");
      const value = String(source || "").trim();
      if (/^https?:\/\//i.test(value)) {
        const a = document.createElement("a");
        a.href = value;
        a.target = "_blank";
        a.rel = "noreferrer";
        a.textContent = value;
        li.append(a);
      } else {
        li.textContent = value;
      }
      list.append(li);
    });
    if (list.children.length) {
      panel.append(list);
      container.append(panel);
    }
  }

  if (message.reasoning_graph && typeof message.reasoning_graph === "object") {
    container.append(jsonPanel("Reasoning graph", message.reasoning_graph));
  }
  if (message.mcts_search && typeof message.mcts_search === "object") {
    container.append(jsonPanel("MCTS search", message.mcts_search));
  }
}

function extraPanel(title) {
  const panel = document.createElement("section");
  panel.className = "extra-panel";
  const h = document.createElement("h4");
  h.textContent = title;
  panel.append(h);
  return panel;
}

function jsonPanel(title, payload) {
  const panel = extraPanel(title);
  const pre = document.createElement("pre");
  const code = document.createElement("code");
  code.textContent = JSON.stringify(payload, null, 2).slice(0, 5000);
  pre.append(code);
  panel.append(pre);
  return panel;
}

function renderRichText(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const chunks = [];
  const fence = /```([\s\S]*?)```/g;
  let last = 0;
  let match;
  while ((match = fence.exec(text)) !== null) {
    if (match.index > last) chunks.push(renderParagraphs(text.slice(last, match.index)));
    chunks.push(`<pre><code>${escapeHtml(match[1].replace(/^\w+\n/, ""))}</code></pre>`);
    last = fence.lastIndex;
  }
  if (last < text.length) chunks.push(renderParagraphs(text.slice(last)));
  return chunks.join("");
}

function renderParagraphs(raw) {
  return raw
    .split(/\n{2,}/)
    .map((block) => block.trim())
    .filter(Boolean)
    .map((block) => `<p>${renderInline(block).replace(/\n/g, "<br>")}</p>`)
    .join("");
}

function renderInline(raw) {
  let out = escapeHtml(raw);
  out = out.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/`([^`]+?)`/g, "<code>$1</code>");
  out = out.replace(/(\$[^$]{1,160}\$|\\\([^)]{1,160}\\\)|\\\[[\s\S]{1,260}?\\\])/g, '<span class="math-frag">$1</span>');
  return out;
}

function escapeHtml(raw) {
  return String(raw)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function refreshTitle() {
  const session = buildSessionRecord();
  els.chatTitle.textContent = session.title || "New AI Chat";
}

function formatTime(ms) {
  if (!ms) return "new";
  const date = new Date(ms);
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function autoSizePrompt() {
  els.promptInput.style.height = "auto";
  els.promptInput.style.height = `${Math.min(168, Math.max(42, els.promptInput.scrollHeight))}px`;
}

function setBusy(value) {
  state.busy = value;
  els.sendButton.disabled = value;
  els.attachButton.disabled = value;
  els.promptInput.disabled = value;
  refreshSubtitle(value ? "LalaCore is thinking..." : "");
}

function addTypingMessage() {
  const id = `typing_${nowMs()}`;
  state.messages.push({
    id,
    role: "assistant",
    content: "",
    meta: "thinking",
    transient: true,
    typing: true,
  });
  renderMessages();
  return id;
}

function removeTransient(id) {
  state.messages = state.messages.filter((message) => message.id !== id);
}

function currentOptions({ bestModel = false, hasImage = false }) {
  const base = {
    function: state.chatFunction,
    response_style: "structured_exam_solution",
    enable_persona: false,
    persona_mode: "soft_possessive_academic_girlfriend",
    enable_pre_reasoning_context: true,
    enable_web_retrieval: false,
    enable_graph_of_thought: true,
    enable_mcts_reasoning: true,
    require_citations: "none",
    evidence_mode: "none",
    min_citation_count: 0,
    min_evidence_score: 0,
    enable_citation_map: false,
    web_search_scope: "disabled",
    web_search_timeout_s: 0,
    web_fetch_timeout_s: 0,
    web_similarity_threshold: 0.62,
    search_max_matches: 0,
    return_structured: true,
    return_markdown: true,
    return_latex: true,
    count_tokens: true,
    app_surface: "standalone_ai_chat_web",
    pipeline_timeout_s: hasImage ? 130 : 95,
    solve_stage_timeout_s: hasImage ? 105 : 80,
    solve_reevaluation_timeout_s: hasImage ? 28 : 24,
    meta_timeout_s: hasImage ? 12 : 10,
    prefer_equation_rich_derivation: true,
    prefer_final_numeric_or_symbolic_answer: true,
    prefer_stepwise_solution: true,
    suppress_generic_template_response: true,
    suppress_placeholder_response: true,
    section_labels_plain_text: true,
    provider_priority: STRONG_MODEL_PRIORITY,
  };
  if (!bestModel) return base;
  return {
    ...base,
    quality_retry: true,
    quality_retry_force_max: true,
    target_confidence_floor: 0.8,
    pipeline_timeout_s: hasImage ? 260 : 220,
    solve_stage_timeout_s: hasImage ? 200 : 170,
    solve_reevaluation_timeout_s: hasImage ? 60 : 48,
    meta_timeout_s: 18,
  };
}

async function postAction(payload, timeoutMs = 240000) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(endpointUrl(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const text = await response.text();
    const decoded = text ? safeJsonParse(text, { raw: text }) : {};
    if (!response.ok) {
      throw new Error(`Backend ${response.status}: ${text.slice(0, 400)}`);
    }
    return decoded;
  } finally {
    window.clearTimeout(timer);
  }
}

async function sendMessage(event) {
  if (event) event.preventDefault();
  const prompt = els.promptInput.value.trim();
  if ((!prompt && !state.attachment) || state.busy) return;

  const attachment = state.attachment;
  const outgoingText = outgoingUserText(prompt, attachment);
  const enginePrompt = enginePromptText(prompt, attachment);

  state.messages.push({
    id: `local_${nowMs()}`,
    role: "user",
    content: outgoingText,
  });
  els.promptInput.value = "";
  clearAttachment();
  autoSizePrompt();
  saveLocalState();
  renderMessages();
  setBusy(true);
  const typingId = addTypingMessage();

  try {
    const payload = await buildChatPayload(enginePrompt, attachment);
    const response = await postAction(payload);
    removeTransient(typingId);
    state.messages.push(normalizeAssistantResponse(response, enginePrompt));
    saveLocalState();
    renderMessages();
    await persistRemoteHistory();
    await maybeGenerateTitle();
  } catch (error) {
    removeTransient(typingId);
    state.messages.push({
      id: `assistant_${nowMs()}`,
      role: "assistant",
      content: friendlyError(error),
      meta: "AI failure",
      ai_failure: true,
      error: true,
    });
    saveLocalState();
    renderMessages();
  } finally {
    setBusy(false);
  }
}

async function buildChatPayload(prompt, attachment) {
  const hasImage = attachment && attachment.kind === "image";
  const card = {
    account_id: state.accountId,
    source: "standalone_ai_chat_web",
  };
  const payload = {
    action: "ai_solve",
    prompt,
    user_id: state.userId,
    chat_id: state.currentChatId,
    options: currentOptions({ bestModel: state.bestModel, hasImage }),
    card,
  };
  if (hasImage) {
    payload.image = attachment.base64;
    payload.card = {
      ...card,
      source: "image",
      image_name: attachment.name,
      image_mime: attachment.type,
      image_size: attachment.size,
    };
  }
  if (attachment && attachment.kind === "pdf") {
    const uploadedUrl = await uploadAttachment(attachment);
    payload.card = {
      ...card,
      source: "pdf",
      pdf_name: attachment.name,
      pdf_url: uploadedUrl,
    };
  }
  return payload;
}

async function uploadAttachment(attachment) {
  const response = await postAction(
    {
      action: "upload_file",
      name: attachment.name,
      file_name: attachment.name,
      data: attachment.dataUrl,
      file_data: attachment.dataUrl,
    },
    60000,
  );
  const url = response.url || response.file_url || response.link;
  if (!url) throw new Error(`File upload failed: ${JSON.stringify(response).slice(0, 300)}`);
  return url;
}

function outgoingUserText(prompt, attachment) {
  if (!attachment) return prompt;
  if (attachment.kind === "image") return prompt ? `${prompt}\n[Image sent]` : "[Image sent]";
  if (attachment.kind === "pdf") return prompt ? `${prompt}\n[PDF sent: ${attachment.name}]` : `[PDF sent: ${attachment.name}]`;
  return prompt;
}

function enginePromptText(prompt, attachment) {
  if (!attachment) return prompt;
  if (attachment.kind === "image") {
    return prompt
      ? `${prompt}\n\nUse the attached image as primary context. Extract all visible text and solve step-by-step for JEE.`
      : "Solve this question from the image. Extract all visible math text accurately and show full JEE-style steps.";
  }
  if (attachment.kind === "pdf") {
    return prompt
      ? `${prompt}\n\nUse the attached PDF as primary context. Extract key text before solving.`
      : "Use the attached PDF as context and solve the relevant question with JEE-style steps.";
  }
  return prompt;
}

function normalizeAssistantResponse(response, originalPrompt) {
  const raw = response && typeof response === "object" ? response : {};
  const nestedRaw = raw.raw && typeof raw.raw === "object" ? raw.raw : {};
  const answer = firstText(raw.answer, raw.final_answer, raw.display_answer, raw.text, raw.response, raw.content);
  const explanation = firstText(raw.explanation, raw.reasoning, nestedRaw.reasoning);
  const concept = enrichConcept(firstText(raw.concept, raw.topic, nestedRaw.profile && nestedRaw.profile.subject), answer, explanation, originalPrompt);
  const content = composeDisplayText(answer, explanation, concept, firstText(raw.confidence));
  const ok = raw.ok === true || Boolean(answer || explanation);
  return {
    id: `assistant_${nowMs()}`,
    role: "assistant",
    content: ok ? content || "Received empty response." : assistantFailureText(raw),
    meta: cosmeticModelLabel(raw.provider || raw.winner_provider, raw.model),
    confidence: raw.confidence ? String(raw.confidence) : "",
    concept,
    visualization: raw.visualization || nestedRaw.visualization || null,
    web_retrieval: raw.web_retrieval || nestedRaw.web_retrieval || null,
    mcts_search: raw.mcts_search || nestedRaw.mcts_search || null,
    reasoning_graph: raw.reasoning_graph || nestedRaw.reasoning_graph || null,
    citation_map: raw.citation_map || nestedRaw.citation_map || null,
    evidence: raw.evidence || nestedRaw.evidence || null,
    ai_failure: !ok,
    error: !ok,
    raw,
  };
}

function firstText(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function composeDisplayText(answer, explanation, concept, confidence = "") {
  const parts = [];
  if (answer) parts.push(`Answer:\n${answer}`);
  if (explanation && explanation !== answer) parts.push(`Explanation:\n${explanation}`);
  if (concept) parts.push(`Concept: ${concept}`);
  if (confidence) parts.push(`Confidence: ${confidence}`);
  return parts.join("\n\n").trim();
}

function enrichConcept(concept, answer, explanation, prompt = "") {
  const clean = String(concept || "").trim();
  if (clean && !/^math$|^general$/i.test(clean)) return clean;
  const merged = `${prompt}\n${answer}\n${explanation}`.toLowerCase();
  if (/\b(sin|cos|tan|cot|sec|cosec|theta|trigon)\b/.test(merged)) return "Trigonometry";
  if (/\b(parabola|ellipse|hyperbola|tangent|locus|chord|focus|normal)\b/.test(merged)) return "Coordinate geometry";
  if (/\\int|\bintegral\b|\bdx\b|\bdifferentiat|\bderivative\b/.test(merged)) return "Calculus";
  if (/\b(matrix|determinant|eigen|vector)\b/.test(merged)) return "Linear algebra";
  if (/\b(permutation|combination|ncr|npr|binom)\b/.test(merged)) return "Combinatorics";
  if (/\b(probability|bayes|random|expectation)\b/.test(merged)) return "Probability";
  if (/\b(quadratic|root|factor|discriminant)\b/.test(merged)) return "Algebra";
  return "";
}

function cosmeticModelLabel(provider, model) {
  const p = String(provider || "").trim();
  const m = String(model || "").trim();
  if (p && m) return `${p} - ${m}`;
  if (m) return m;
  if (p) return p;
  return "LalaCore engine";
}

function assistantFailureText(raw) {
  const msg = firstText(raw.error, raw.message, raw.status);
  return `Sorry, I could not solve that properly just now.\n\n${msg || "The AI did not return a reliable answer."}`;
}

function friendlyError(error) {
  const raw = String((error && error.message) || error || "");
  const lower = raw.toLowerCase();
  if (lower.includes("abort")) return "The AI request timed out before it could finish.";
  if (lower.includes("failed to fetch") || lower.includes("network") || lower.includes("cors")) {
    return "The website could not reach the AI backend. Check the Backend URL in settings and try again.";
  }
  return `The AI backend is temporarily unavailable.\n\n${raw}`;
}

async function maybeGenerateTitle() {
  const current = state.sessions.find((session) => session.chat_id === state.currentChatId);
  if (!current || current.ai_generated_title || state.messages.length < 2) return;
  const userMessage = state.messages.find((message) => message.role === "user");
  const assistantMessage = [...state.messages].reverse().find((message) => message.role === "assistant" && !message.ai_failure);
  if (!userMessage || !assistantMessage) return;
  const titlePrompt = [
    "Generate a concise title for this AI chat.",
    "Rules:",
    "- Maximum 6 words",
    "- No quotes",
    "- Return title only",
    "",
    `User message: ${String(userMessage.content || "").slice(0, 300)}`,
    `Assistant response: ${String(assistantMessage.content || "").slice(0, 300)}`,
  ].join("\n");
  try {
    const response = await postAction(
      {
        action: "ai_chat",
        prompt: titlePrompt,
        user_id: state.userId,
        chat_id: `${state.currentChatId}_title`,
        options: {
          ...currentOptions({ bestModel: false }),
          response_style: "short_answer",
          enable_graph_of_thought: false,
          enable_mcts_reasoning: false,
          pipeline_timeout_s: 24,
          solve_stage_timeout_s: 18,
          meta_timeout_s: 5,
        },
        card: {
          account_id: state.accountId,
          source: "standalone_ai_chat_title",
          base_chat_id: state.currentChatId,
        },
      },
      30000,
    );
    const title = sanitizeTitle(firstText(response.title, response.answer, response.explanation, response.message));
    if (title) {
      current.title = title;
      current.ai_generated_title = true;
      current.updated_at = nowMs();
      saveLocalState();
      renderHistory();
      refreshTitle();
      await persistSession(current);
    }
  } catch {
    saveLocalState();
  }
}

function sanitizeTitle(raw) {
  const title = String(raw || "")
    .split("\n")[0]
    .replace(/^\s*(title|topic)\s*[:-]\s*/i, "")
    .replace(/["`]+/g, "")
    .replace(/\s+/g, " ")
    .replace(/[.!?]+$/, "")
    .trim();
  if (!title || title.length > 80) return "";
  return title.length > 56 ? `${title.slice(0, 56).trim()}...` : title;
}

async function persistRemoteHistory() {
  const serialized = state.messages
    .filter((message) => !message.transient)
    .map((message) => ({
      id: message.id,
      role: message.role,
      content: message.content || message.text || "",
      meta: message.meta || undefined,
      confidence: message.confidence || undefined,
      concept: message.concept || undefined,
      visualization: message.visualization || undefined,
      web_retrieval: message.web_retrieval || undefined,
      mcts_search: message.mcts_search || undefined,
      reasoning_graph: message.reasoning_graph || undefined,
      citation_map: message.citation_map || undefined,
      evidence: message.evidence || undefined,
      ai_failure: message.ai_failure || undefined,
    }));
  await postAction(
    {
      action: "save_ai_chat_history",
      account_id: state.accountId,
      user_id: state.userId,
      chat_id: state.currentChatId,
      messages: serialized,
    },
    30000,
  );
}

async function persistSession(session = buildSessionRecord()) {
  await postAction(
    {
      action: "save_ai_chat_session",
      ...session,
      account_id: state.accountId,
      user_id: state.userId,
      chat_id: state.currentChatId,
    },
    20000,
  );
}

async function refreshRemoteHistory() {
  try {
    const response = await postAction(
      {
        action: "list_ai_chat_sessions",
        account_id: state.accountId,
        user_id: state.userId,
      },
      20000,
    );
    const remoteRows = Array.isArray(response.list) ? response.list : Array.isArray(response.sessions) ? response.sessions : [];
    mergeSessions(remoteRows);
    saveLocalState();
    renderHistory();
  } catch (error) {
    toast(friendlyError(error));
  }
}

function mergeSessions(remoteRows) {
  const byId = new Map();
  state.sessions.forEach((session) => byId.set(session.chat_id || session.chatId, session));
  remoteRows.forEach((row) => {
    const id = row.chat_id || row.chatId;
    if (!id) return;
    const existing = byId.get(id) || {};
    const chooseRemote = Number(row.updated_at || row.updatedAt || 0) >= Number(existing.updated_at || existing.updatedAt || 0);
    byId.set(id, {
      ...existing,
      ...(chooseRemote ? normalizeSession(row) : {}),
      messages: existing.messages || [],
    });
  });
  state.sessions = [...byId.values()].filter((session) => session.chat_id);
}

function normalizeSession(row) {
  return {
    chat_id: row.chat_id || row.chatId,
    chatId: row.chat_id || row.chatId,
    account_id: row.account_id || row.accountId || state.accountId,
    user_id: row.user_id || row.userId || state.userId,
    title: row.title || "AI Chat",
    created_at: Number(row.created_at || row.createdAt || nowMs()),
    updated_at: Number(row.updated_at || row.updatedAt || nowMs()),
    message_count: Number(row.message_count || row.messageCount || 0),
    ai_generated_title: Boolean(row.ai_generated_title || row.aiGeneratedTitle),
    pinned: Boolean(row.pinned),
    pinned_at: Number(row.pinned_at || row.pinnedAt || 0),
  };
}

async function openChat(chatId) {
  if (!chatId || chatId === state.currentChatId) return;
  state.currentChatId = chatId;
  const local = state.sessions.find((session) => session.chat_id === chatId);
  state.messages = Array.isArray(local && local.messages) ? local.messages : [];
  try {
    const response = await postAction(
      {
        action: "get_ai_chat_history",
        account_id: state.accountId,
        user_id: state.userId,
        chat_id: chatId,
      },
      20000,
    );
    const remoteMessages = Array.isArray(response.messages) ? response.messages : [];
    if (remoteMessages.length >= state.messages.length) {
      state.messages = remoteMessages.map((message) => ({
        id: message.id || `${message.role || "assistant"}_${nowMs()}_${Math.random().toString(16).slice(2)}`,
        role: message.role || "assistant",
        content: message.content || message.text || "",
        meta: message.meta || "",
        confidence: message.confidence || "",
        concept: message.concept || "",
        visualization: message.visualization || null,
        web_retrieval: message.web_retrieval || null,
        mcts_search: message.mcts_search || null,
        reasoning_graph: message.reasoning_graph || null,
        citation_map: message.citation_map || null,
        evidence: message.evidence || null,
        ai_failure: message.ai_failure === true,
      }));
    }
  } catch {
    // Keep local history usable when remote history is unavailable.
  }
  saveLocalState();
  renderMessages();
  if (window.matchMedia("(max-width: 880px)").matches) els.shell.dataset.sidebar = "closed";
}

function newChat() {
  state.currentChatId = makeChatId();
  state.messages = [];
  ensureCurrentSession();
  saveLocalState();
  renderMessages();
  els.promptInput.focus();
}

async function handleFileSelected() {
  const file = els.fileInput.files && els.fileInput.files[0];
  els.fileInput.value = "";
  if (!file) return;
  if (!file.type.startsWith("image/") && file.type !== "application/pdf") {
    toast("Attach an image or PDF.");
    return;
  }
  if (file.size > 8 * 1024 * 1024) {
    toast("Use a file under 8 MB for this chat surface.");
    return;
  }
  const dataUrl = await readFileAsDataUrl(file);
  const base64 = dataUrl.includes(",") ? dataUrl.split(",", 2)[1] : dataUrl;
  state.attachment = {
    kind: file.type === "application/pdf" ? "pdf" : "image",
    name: file.name || (file.type === "application/pdf" ? "document.pdf" : "image.jpg"),
    type: file.type || "application/octet-stream",
    size: file.size,
    dataUrl,
    base64,
  };
  renderAttachment();
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("Could not read file"));
    reader.readAsDataURL(file);
  });
}

function renderAttachment() {
  const a = state.attachment;
  if (!a) {
    els.attachmentTray.hidden = true;
    els.attachmentTray.replaceChildren();
    return;
  }
  els.attachmentTray.hidden = false;
  const label = document.createElement("span");
  label.textContent = `${a.kind.toUpperCase()}: ${a.name} (${Math.ceil(a.size / 1024)} KB)`;
  const remove = document.createElement("button");
  remove.type = "button";
  remove.className = "icon-button";
  remove.setAttribute("aria-label", "Remove attachment");
  remove.innerHTML = '<svg viewBox="0 0 24 24" focusable="false"><path d="M18 6 6 18M6 6l12 12" /></svg>';
  remove.addEventListener("click", clearAttachment);
  els.attachmentTray.replaceChildren(label, remove);
}

function clearAttachment() {
  state.attachment = null;
  renderAttachment();
}

function toast(message) {
  const old = document.querySelector(".toast");
  if (old) old.remove();
  const node = document.createElement("div");
  node.className = "toast";
  node.textContent = message;
  document.body.append(node);
  window.setTimeout(() => node.remove(), 4200);
}

function bindEvents() {
  els.composer.addEventListener("submit", sendMessage);
  els.promptInput.addEventListener("input", autoSizePrompt);
  els.promptInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendMessage(event);
    }
  });
  els.newChatButton.addEventListener("click", newChat);
  els.refreshHistoryButton.addEventListener("click", refreshRemoteHistory);
  els.historySearch.addEventListener("input", renderHistory);
  els.settingsButton.addEventListener("click", () => {
    els.settingsPanel.hidden = !els.settingsPanel.hidden;
  });
  els.saveSettingsButton.addEventListener("click", saveSettings);
  els.sidebarToggle.addEventListener("click", () => {
    els.shell.dataset.sidebar = els.shell.dataset.sidebar === "open" ? "closed" : "open";
  });
  els.attachButton.addEventListener("click", () => els.fileInput.click());
  els.fileInput.addEventListener("change", handleFileSelected);
}

function init() {
  loadSettings();
  loadLocalState();
  syncSettingsFields();
  renderPromptChips();
  bindEvents();
  renderAttachment();
  renderMessages();
  refreshSubtitle();
  if (canAutoLoadRemoteHistory()) {
    refreshRemoteHistory();
  }
  if (window.matchMedia("(max-width: 880px)").matches) els.shell.dataset.sidebar = "closed";
}

init();
