import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    classroom_http: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '30s', target: 25 },
        { duration: '45s', target: 75 },
        { duration: '60s', target: 150 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '15s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],
    http_req_duration: ['p(95)<900'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const CLASS_ID = __ENV.CLASS_ID || 'physics_live_01';
const EMAIL = __ENV.EMAIL || 'student@example.com';
const PASSWORD = __ENV.PASSWORD || 'student-password';

function jsonHeaders(accessToken, step) {
  return {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${accessToken}`,
      'Idempotency-Key': `${step}-${__VU}-${__ITER}`,
      'x-request-id': `k6-${step}-${__VU}-${__ITER}`,
    },
  };
}

export default function () {
  const loginResponse = http.post(
    `${BASE_URL}/auth/login`,
    JSON.stringify({ email: EMAIL, password: PASSWORD }),
    {
      headers: {
        'Content-Type': 'application/json',
        'x-request-id': `k6-login-${__VU}-${__ITER}`,
      },
    },
  );
  check(loginResponse, {
    'login ok': (res) => res.status === 201,
  });
  const accessToken = loginResponse.json('access_token');

  const joinResponse = http.post(
    `${BASE_URL}/class/join_request`,
    JSON.stringify({
      class_id: CLASS_ID,
      display_name: `Student ${__VU}`,
    }),
    jsonHeaders(accessToken, 'join'),
  );
  check(joinResponse, {
    'join accepted': (res) => res.status === 202 || res.status === 200,
  });

  const stateResponse = http.get(
    `${BASE_URL}/class/state?class_id=${CLASS_ID}`,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'x-request-id': `k6-state-${__VU}-${__ITER}`,
      },
    },
  );
  check(stateResponse, {
    'state ok': (res) => res.status === 200,
  });

  const chatResponse = http.post(
    `${BASE_URL}/chat/send`,
    JSON.stringify({
      class_id: CLASS_ID,
      message: `load-test message vu=${__VU} iter=${__ITER}`,
      display_name: `Student ${__VU}`,
      client_message_id: `chat-${__VU}-${__ITER}`,
    }),
    jsonHeaders(accessToken, 'chat'),
  );
  check(chatResponse, {
    'chat created': (res) => res.status === 201 || res.status === 403,
  });

  const tokenResponse = http.post(
    `${BASE_URL}/live/token`,
    JSON.stringify({
      class_id: CLASS_ID,
      display_name: `Student ${__VU}`,
    }),
    jsonHeaders(accessToken, 'token'),
  );
  check(tokenResponse, {
    'token endpoint stable': (res) =>
      res.status === 200 || res.status === 403 || res.status === 409,
  });

  sleep(1);
}
