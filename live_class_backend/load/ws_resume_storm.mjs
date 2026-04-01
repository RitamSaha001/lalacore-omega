import { setTimeout as delay } from 'node:timers/promises';

import WebSocket from 'ws';

const BASE_URL = process.env.BASE_URL ?? 'http://localhost:8080';
const ACCESS_TOKEN = process.env.ACCESS_TOKEN ?? '';
const CLASS_ID = process.env.CLASS_ID ?? 'physics_live_01';
const CONNECTIONS = Number.parseInt(process.env.CONNECTIONS ?? '40', 10);
const ROUNDS = Number.parseInt(process.env.ROUNDS ?? '4', 10);

if (!ACCESS_TOKEN) {
  console.error('ACCESS_TOKEN is required');
  process.exit(1);
}

function toWebSocketBaseUrl(baseUrl) {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  url.pathname = '';
  url.search = '';
  return url.toString().replace(/\/$/, '');
}

function makeSocketUrl(clientId, lastReceivedSequence = 0) {
  const url = new URL(`${toWebSocketBaseUrl(BASE_URL)}/class/sync`);
  url.searchParams.set('class_id', CLASS_ID);
  url.searchParams.set('client_id', clientId);
  url.searchParams.set('token', ACCESS_TOKEN);
  url.searchParams.set('last_received_sequence', `${lastReceivedSequence}`);
  return url;
}

async function openSocket(clientId, lastReceivedSequence = 0) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(makeSocketUrl(clientId, lastReceivedSequence));
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

async function main() {
  const sockets = [];
  const progress = new Map();

  for (let index = 0; index < CONNECTIONS; index += 1) {
    const clientId = `storm-${index}`;
    const ws = await openSocket(clientId, 0);
    progress.set(clientId, 0);
    ws.on('message', (raw) => {
      try {
        const payload = JSON.parse(raw.toString());
        const sequence = Number.parseInt(
          `${payload.sequence_number ?? 0}`,
          10,
        );
        if (sequence > 0) {
          progress.set(clientId, sequence);
          ws.send(
            JSON.stringify({
              type: 'ack',
              sequence_number: sequence,
              message_id: payload.message_id,
            }),
          );
        }
      } catch {
        // Ignore malformed payloads in the load harness.
      }
    });
    sockets.push({ clientId, ws });
  }

  for (let round = 0; round < ROUNDS; round += 1) {
    for (const { clientId, ws } of sockets) {
      ws.send(
        JSON.stringify({
          type: 'classroom_heartbeat',
          message_id: `${clientId}-round-${round}`,
          sequence_number: round + 1,
          payload: {
            client_round: round,
          },
        }),
      );
    }

    await delay(750);

    for (let i = 0; i < sockets.length; i += 3) {
      const current = sockets[i];
      current.ws.close();
      await delay(120);
      const resumed = await openSocket(
        current.clientId,
        progress.get(current.clientId) ?? 0,
      );
      resumed.on('message', (raw) => {
        try {
          const payload = JSON.parse(raw.toString());
          const sequence = Number.parseInt(
            `${payload.sequence_number ?? 0}`,
            10,
          );
          if (sequence > 0) {
            progress.set(current.clientId, sequence);
            resumed.send(
              JSON.stringify({
                type: 'ack',
                sequence_number: sequence,
                message_id: payload.message_id,
              }),
            );
          }
        } catch {}
      });
      sockets[i] = {
        clientId: current.clientId,
        ws: resumed,
      };
    }
  }

  await delay(1500);
  for (const { ws } of sockets) {
    ws.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
