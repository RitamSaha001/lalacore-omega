import { randomUUID } from 'node:crypto';

import WebSocket from 'ws';

const BASE_URL = (process.env.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const CLASS_ID = process.env.CLASS_ID || 'physics_live_01';
const ACCESS_TOKEN = process.env.ACCESS_TOKEN || '';
const CHANNEL = process.env.WS_CHANNEL || '/class/events';
const CLIENTS = Number.parseInt(process.env.CLIENTS || '40', 10);
const DROP_PERCENT = Number.parseFloat(process.env.DROP_PERCENT || '0.35');
const TEST_DURATION_MS = Number.parseInt(process.env.TEST_DURATION_MS || '90000', 10);
const STORM_INTERVAL_MS = Number.parseInt(process.env.STORM_INTERVAL_MS || '12000', 10);
const MAX_BACKOFF_MS = Number.parseInt(process.env.MAX_BACKOFF_MS || '12000', 10);

if (!ACCESS_TOKEN) {
  throw new Error('ACCESS_TOKEN is required for ws-reconnect-storm.mjs');
}

function buildWsUrl(clientId, lastReceivedSequence) {
  const url = new URL(BASE_URL);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  url.pathname = CHANNEL;
  url.searchParams.set('class_id', CLASS_ID);
  url.searchParams.set('client_id', clientId);
  url.searchParams.set('last_received_sequence', String(lastReceivedSequence));
  url.searchParams.set('access_token', ACCESS_TOKEN);
  return url.toString();
}

class StormClient {
  constructor(index) {
    this.index = index;
    this.clientId = `storm-${index}-${randomUUID().slice(0, 8)}`;
    this.lastReceivedSequence = 0;
    this.socket = null;
    this.reconnectAttempts = 0;
    this.connectedAt = 0;
    this.lastDisconnectAt = 0;
    this.replayEvents = 0;
    this.reconnectLatencies = [];
    this.closedByStorm = 0;
  }

  connect() {
    const socket = new WebSocket(
      buildWsUrl(this.clientId, this.lastReceivedSequence),
      {
        handshakeTimeout: 8000,
      },
    );
    this.socket = socket;

    socket.on('open', () => {
      this.connectedAt = Date.now();
      if (this.lastDisconnectAt > 0) {
        this.reconnectLatencies.push(Date.now() - this.lastDisconnectAt);
      }
      this.reconnectAttempts = 0;
    });

    socket.on('message', (buffer) => {
      try {
        const payload = JSON.parse(buffer.toString());
        this.onMessage(payload);
      } catch {
        // Ignore invalid payloads during stress tests.
      }
    });

    socket.on('close', () => {
      this.socket = null;
      this.lastDisconnectAt = Date.now();
      scheduleReconnect(this);
    });

    socket.on('error', () => {
      socket.close();
    });
  }

  onMessage(payload) {
    const type = payload.type || '';
    if (type === 'replay_start') {
      this.replayEvents += 1;
      return;
    }
    if (type === 'connection_ready') {
      return;
    }
    const sequenceNumber = Number.parseInt(payload.sequence_number || '0', 10);
    if (sequenceNumber > 0) {
      this.lastReceivedSequence = Math.max(this.lastReceivedSequence, sequenceNumber);
      this.send({
        type: 'ack',
        sequence_number: sequenceNumber,
        message_id: payload.message_id || '',
      });
    }
  }

  send(payload) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return;
    }
    this.socket.send(JSON.stringify(payload));
  }

  disconnectForStorm() {
    if (!this.socket) {
      return;
    }
    this.closedByStorm += 1;
    this.lastDisconnectAt = Date.now();
    this.socket.terminate();
  }

  shutdown() {
    this.socket?.close();
  }
}

function scheduleReconnect(client) {
  client.reconnectAttempts += 1;
  const exponent = Math.min(client.reconnectAttempts, 6);
  const delay = Math.min(500 * 2 ** (exponent - 1), MAX_BACKOFF_MS);
  const jitter = Math.floor(Math.random() * 400);
  setTimeout(() => {
    client.connect();
  }, delay + jitter);
}

const clients = Array.from({ length: CLIENTS }, (_, index) => new StormClient(index + 1));
for (const client of clients) {
  client.connect();
}

const stormTimer = setInterval(() => {
  const targetCount = Math.max(1, Math.floor(CLIENTS * DROP_PERCENT));
  const pool = [...clients];
  for (let index = 0; index < targetCount && pool.length > 0; index += 1) {
    const nextIndex = Math.floor(Math.random() * pool.length);
    const [client] = pool.splice(nextIndex, 1);
    client.disconnectForStorm();
  }
}, STORM_INTERVAL_MS);

setTimeout(() => {
  clearInterval(stormTimer);
  for (const client of clients) {
    client.shutdown();
  }

  const allReconnectLatencies = clients.flatMap((client) => client.reconnectLatencies);
  const reconnectUnder300ms = allReconnectLatencies.filter((value) => value <= 300).length;
  const avgReconnectLatency =
    allReconnectLatencies.length === 0
      ? 0
      : allReconnectLatencies.reduce((sum, value) => sum + value, 0) /
        allReconnectLatencies.length;

  const summary = {
    base_url: BASE_URL,
    class_id: CLASS_ID,
    channel: CHANNEL,
    clients: CLIENTS,
    drop_percent: DROP_PERCENT,
    total_replays: clients.reduce((sum, client) => sum + client.replayEvents, 0),
    total_forced_disconnects: clients.reduce(
      (sum, client) => sum + client.closedByStorm,
      0,
    ),
    reconnect_samples: allReconnectLatencies.length,
    reconnect_under_300ms: reconnectUnder300ms,
    reconnect_under_300ms_rate:
      allReconnectLatencies.length === 0
        ? 0
        : reconnectUnder300ms / allReconnectLatencies.length,
    avg_reconnect_latency_ms: Math.round(avgReconnectLatency),
    max_reconnect_latency_ms:
      allReconnectLatencies.length === 0 ? 0 : Math.max(...allReconnectLatencies),
  };

  console.log(JSON.stringify(summary, null, 2));
  process.exit(0);
}, TEST_DURATION_MS);
