import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const CLASS_ID = __ENV.CLASS_ID || 'physics_live_01';
const TEACHER_ACCESS_TOKEN = __ENV.TEACHER_ACCESS_TOKEN || '';
const PARTICIPANT_ACCESS_TOKENS = (__ENV.PARTICIPANT_ACCESS_TOKENS || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);

function idempotencyKey(prefix) {
  return `${prefix}-${__VU}-${__ITER}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

function authHeaders(token, extra = {}) {
  return {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
    ...extra,
  };
}

export const options = {
  scenarios: {
    classroom_http: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '2m', target: 25 },
        { duration: '3m', target: 75 },
        { duration: '3m', target: 150 },
        { duration: '2m', target: 0 },
      ],
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],
    http_req_duration: ['p(95)<800'],
    'http_req_duration{name:class_state}': ['p(95)<400'],
    'http_req_duration{name:live_token}': ['p(95)<700'],
    'http_req_duration{name:chat_send}': ['p(95)<500'],
  },
};

export default function () {
  const token =
    PARTICIPANT_ACCESS_TOKENS[(__VU - 1) % Math.max(PARTICIPANT_ACCESS_TOKENS.length, 1)] ||
    TEACHER_ACCESS_TOKEN;

  if (!token) {
    throw new Error(
      'Provide PARTICIPANT_ACCESS_TOKENS or TEACHER_ACCESS_TOKEN before running k6 load.',
    );
  }

  const stateRes = http.get(
    `${BASE_URL}/class/state?class_id=${encodeURIComponent(CLASS_ID)}`,
    {
      headers: authHeaders(token),
      tags: { name: 'class_state' },
    },
  );
  check(stateRes, {
    'class/state is healthy': (res) => res.status === 200,
  });

  const tokenRes = http.get(
    `${BASE_URL}/live-token?class_id=${encodeURIComponent(CLASS_ID)}`,
    {
      headers: authHeaders(token),
      tags: { name: 'live_token' },
    },
  );
  check(tokenRes, {
    'live-token is available': (res) => res.status === 200 || res.status === 403,
  });

  if (__ITER % 2 === 0) {
    const chatPayload = JSON.stringify({
      class_id: CLASS_ID,
      message: `load-message vu=${__VU} iter=${__ITER}`,
      client_message_id: idempotencyKey('client-chat'),
      display_name: `LoadUser ${__VU}`,
    });
    const chatRes = http.post(`${BASE_URL}/chat/send`, chatPayload, {
      headers: authHeaders(token, {
        'Idempotency-Key': idempotencyKey('chat-send'),
      }),
      tags: { name: 'chat_send' },
    });
    check(chatRes, {
      'chat send accepted or policy-rejected': (res) =>
        res.status === 200 || res.status === 403,
    });
  }

  if (TEACHER_ACCESS_TOKEN && __VU === 1 && __ITER % 10 === 0) {
    const chatToggle = JSON.stringify({
      class_id: CLASS_ID,
      enabled: __ITER % 20 !== 0,
    });
    http.post(`${BASE_URL}/class/chat`, chatToggle, {
      headers: authHeaders(TEACHER_ACCESS_TOKEN, {
        'Idempotency-Key': idempotencyKey('teacher-chat-toggle'),
      }),
      tags: { name: 'chat_toggle' },
    });
  }

  sleep(1);
}
