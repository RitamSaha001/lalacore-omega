import { randomUUID } from 'node:crypto';

import { WebSocketServer } from 'ws';

import { extractWebSocketToken } from '../middleware/authMiddleware.js';

const OPEN = 1;
const INTERNAL_ERROR = 1011;
const AUTH_EXPIRED = 4001;
const HEARTBEAT_TIMEOUT = 4002;
const AUTH_EXPIRING_WARNING_MS = 60_000;
const ALLOWED_CHANNELS = new Set([
  '/class/events',
  '/class/sync',
  '/chat/stream',
]);

function rejectUpgrade(socket, statusCode, message) {
  try {
    socket.write(
      `HTTP/1.1 ${statusCode} ${message}\r\n` +
        'Connection: close\r\n' +
        'Content-Type: text/plain\r\n' +
        `Content-Length: ${Buffer.byteLength(message)}\r\n\r\n` +
        message,
    );
  } catch {
    // Best effort only.
  } finally {
    socket.destroy();
  }
}

export class ClassroomHub {
  constructor({
    config,
    database,
    logger,
    metricsCollector = null,
    authService,
    reconnectionService,
    getWaitingRoomSnapshot,
    getClassroomState,
    getChatSnapshot,
    pubSub = null,
  }) {
    this.config = config;
    this.database = database;
    this.logger = logger;
    this.metricsCollector = metricsCollector;
    this.authService = authService;
    this.reconnectionService = reconnectionService;
    this.getWaitingRoomSnapshot = getWaitingRoomSnapshot;
    this.getClassroomState = getClassroomState;
    this.getChatSnapshot = getChatSnapshot;
    this.pubSub = pubSub;
    this.connections = new Map();
    this.heartbeatTimer = null;
    this.wss = new WebSocketServer({ noServer: true });
    this.wss.on('connection', (ws, _request, clientContext) => {
      this.handleConnection(ws, clientContext);
    });

    if (this.pubSub) {
      this.pubSub.onEvent((payload) => {
        void this.handleReplicatedEvent(payload);
      });
    }
  }

  bindServer(server) {
    this.startHeartbeatLoop();
    server.on('upgrade', (request, socket, head) => {
      void this.handleUpgrade(request, socket, head);
    });
  }

  async handleUpgrade(request, socket, head) {
    try {
      const url = new URL(request.url, 'http://localhost');
      const pathname = url.pathname;
      if (!ALLOWED_CHANNELS.has(pathname)) {
        rejectUpgrade(socket, 404, 'Unknown websocket endpoint');
        return;
      }

      const classId = url.searchParams.get('class_id') ?? '';
      if (!classId) {
        rejectUpgrade(socket, 400, 'class_id is required');
        return;
      }

      const token = extractWebSocketToken(request.url, request);
      if (!token) {
        rejectUpgrade(socket, 401, 'Missing websocket bearer token');
        return;
      }

      let authenticatedUser;
      try {
        authenticatedUser = await this.authService.verifyAccessToken(token);
      } catch (error) {
        this.logger.warn('websocket_auth_failed', {
          pathname,
          classId,
          error: String(error),
        });
        rejectUpgrade(socket, 401, 'Invalid websocket token');
        return;
      }

      const clientContext = {
        pathname,
        classId,
        userId: authenticatedUser.userId,
        role: authenticatedUser.role,
        displayName: authenticatedUser.displayName,
        clientId: url.searchParams.get('client_id') ?? randomUUID(),
        lastReceivedSequence:
          Number.parseInt(
            url.searchParams.get('last_received_sequence') ?? '0',
            10,
          ) || 0,
        authExpiresAtEpochSeconds: authenticatedUser.exp ?? null,
      };

      this.wss.handleUpgrade(request, socket, head, (ws) => {
        this.wss.emit('connection', ws, request, clientContext);
      });
    } catch (error) {
      this.logger.error('websocket_upgrade_failed', {
        error: String(error),
      });
      rejectUpgrade(socket, 500, 'Websocket upgrade failed');
    }
  }

  handleConnection(ws, clientContext) {
    const { pathname, classId, userId } = clientContext;
    const connectionId = randomUUID();
    ws.clientContext = {
      ...clientContext,
      connectionId,
      lastAckedSequence: clientContext.lastReceivedSequence,
      warningTimer: null,
      expiryTimer: null,
      lastPongAt: Date.now(),
    };

    const key = this.channelKey(pathname, classId);
    if (!this.connections.has(key)) {
      this.connections.set(key, new Map());
    }
    this.connections.get(key).set(connectionId, ws);

    this.armAuthExpiry(ws);

    this.logger.info('websocket_connected', {
      classId,
      pathname,
      userId,
      clientId: clientContext.clientId,
      connectionId,
      lastReceivedSequence: clientContext.lastReceivedSequence,
    });
    this.metricsCollector?.increment('ws_connections_total', {
      channel: pathname,
      classId,
    });

    ws.on('close', () => {
      void this.handleClose(ws).catch((error) => {
        this.logger.error('websocket_close_handler_failed', {
          classId,
          pathname,
          userId,
          error: String(error),
        });
      });
    });

    ws.on('pong', () => {
      ws.clientContext.lastPongAt = Date.now();
    });

    ws.on('message', (buffer) => {
      void this.handleMessage(ws, buffer).catch((error) => {
        this.logger.error('websocket_message_handler_failed', {
          classId,
          pathname,
          userId,
          error: String(error),
        });
        this.sendJson(ws, {
          type: 'error',
          message: 'Websocket handler failed',
        });
      });
    });

    void this.initializeConnection(ws).catch((error) => {
      this.logger.error('websocket_initialize_failed', {
        classId,
        pathname,
        userId,
        error: String(error),
      });
      this.sendJson(ws, {
        type: 'error',
        message: 'Websocket initialization failed',
      });
      ws.close(INTERNAL_ERROR, 'Initialization failed');
    });
  }

  armAuthExpiry(ws) {
    const { authExpiresAtEpochSeconds } = ws.clientContext;
    if (!authExpiresAtEpochSeconds) {
      return;
    }

    const expiresAtMs = authExpiresAtEpochSeconds * 1000;
    const msUntilExpiry = expiresAtMs - Date.now();
    if (msUntilExpiry <= 0) {
      this.sendJson(ws, {
        type: 'auth_token_expired',
      });
      ws.close(AUTH_EXPIRED, 'Access token expired');
      return;
    }

    const warningDelayMs = Math.max(0, msUntilExpiry - AUTH_EXPIRING_WARNING_MS);
    ws.clientContext.warningTimer = setTimeout(() => {
      this.sendJson(ws, {
        type: 'auth_token_expiring',
        refresh_before_epoch_seconds: authExpiresAtEpochSeconds,
      });
    }, warningDelayMs);

    ws.clientContext.expiryTimer = setTimeout(() => {
      this.sendJson(ws, {
        type: 'auth_token_expired',
      });
      ws.close(AUTH_EXPIRED, 'Access token expired');
    }, msUntilExpiry + 1000);
  }

  clearAuthTimers(ws) {
    if (ws.clientContext?.warningTimer) {
      clearTimeout(ws.clientContext.warningTimer);
      ws.clientContext.warningTimer = null;
    }
    if (ws.clientContext?.expiryTimer) {
      clearTimeout(ws.clientContext.expiryTimer);
      ws.clientContext.expiryTimer = null;
    }
  }

  async initializeConnection(ws) {
    const {
      pathname,
      classId,
      userId,
      role,
      displayName,
      clientId,
      connectionId,
      lastReceivedSequence,
      authExpiresAtEpochSeconds,
    } = ws.clientContext;

    await this.reconnectionService.registerConnection({
      classId,
      userId,
      connectionId,
    });

    const lastSequenceAvailable = await this.database.getLastSequence(classId);
    this.sendJson(ws, {
      type: 'connection_ready',
      class_id: classId,
      channel: pathname,
      connection_id: connectionId,
      client_id: clientId,
      user_id: userId,
      role,
      display_name: displayName,
      last_sequence_available: lastSequenceAvailable,
      reconnect_grace_period_ms: this.config.reconnectGracePeriodMs,
      reconnect_backoff: this.config.reconnectBackoff,
      heartbeat_interval_ms:
        this.config.observability.websocketHeartbeatIntervalMs,
      heartbeat_timeout_ms:
        this.config.observability.websocketHeartbeatTimeoutMs,
      auth_expires_at_epoch_seconds: authExpiresAtEpochSeconds,
    });

    if (lastReceivedSequence > 0) {
      await this.replayFrom(ws, lastReceivedSequence);
      return;
    }

    if (pathname === '/class/events') {
      this.sendJson(ws, {
        type: 'waiting_room_snapshot',
        class_id: classId,
        requests: await this.getWaitingRoomSnapshot(classId),
      });
      this.sendJson(ws, await this.getClassroomState(classId, userId));
    }

    if (pathname === '/chat/stream') {
      this.sendJson(ws, {
        type: 'chat_snapshot',
        class_id: classId,
        ...(await this.getChatSnapshot(classId)),
      });
    }
  }

  async handleClose(ws) {
    const { pathname, classId, userId, connectionId } = ws.clientContext;
    this.clearAuthTimers(ws);

    const key = this.channelKey(pathname, classId);
    this.connections.get(key)?.delete(connectionId);
    if (this.connections.get(key)?.size === 0) {
      this.connections.delete(key);
    }

    await this.reconnectionService.unregisterConnection({
      classId,
      userId,
      connectionId,
    });

    this.logger.info('websocket_closed', {
      classId,
      pathname,
      userId,
      connectionId,
    });
    this.metricsCollector?.increment('ws_disconnects_total', {
      channel: pathname,
      classId,
    });
  }

  async handleMessage(ws, buffer) {
    const { pathname, classId, clientId, userId } = ws.clientContext;
    let payload;
    try {
      payload = JSON.parse(buffer.toString());
    } catch (error) {
      this.sendJson(ws, {
        type: 'error',
        message: 'Invalid websocket payload',
      });
      this.logger.warn('websocket_invalid_payload', {
        classId,
        pathname,
        userId,
        error: String(error),
      });
      return;
    }

    if (payload.type === 'ack') {
      ws.clientContext.lastAckedSequence = Math.max(
        ws.clientContext.lastAckedSequence,
        Number.parseInt(String(payload.sequence_number ?? '0'), 10) || 0,
      );
      return;
    }

    if (payload.type === 'resume') {
      const fromSequence =
        Number.parseInt(String(payload.last_received_sequence ?? '0'), 10) || 0;
      ws.clientContext.lastAckedSequence = fromSequence;
      await this.replayFrom(ws, fromSequence);
      return;
    }

    if (pathname !== '/class/sync') {
      this.sendJson(ws, {
        type: 'error',
        message: 'Incoming messages are only accepted on /class/sync',
      });
      return;
    }

    const clientSequence =
      Number.parseInt(String(payload.sequence_number ?? '0'), 10) || 0;
    const clientMessageId = String(payload.message_id ?? '').trim();
    if (!clientSequence || !clientMessageId) {
      this.sendJson(ws, {
        type: 'error',
        message: 'sync payload requires message_id and sequence_number',
      });
      return;
    }

    const clientStreamKey = `${classId}:${pathname}:${clientId}`;
    const dedupe = await this.database.acceptClientSequence(
      clientStreamKey,
      clientSequence,
    );
    if (!dedupe.accepted) {
      this.logger.warn('duplicate_client_message_dropped', {
        classId,
        pathname,
        userId,
        clientId,
        clientSequence,
        lastProcessedSequence: dedupe.lastProcessedSequence,
      });
      this.metricsCollector?.increment('ws_duplicate_client_messages_total', {
        channel: pathname,
        classId,
      });
      this.sendJson(ws, {
        type: 'ack',
        duplicate: true,
        sequence_number: dedupe.lastProcessedSequence,
        message_id: clientMessageId,
      });
      return;
    }

    this.sendJson(ws, {
      type: 'ack',
      duplicate: false,
      sequence_number: clientSequence,
      message_id: clientMessageId,
    });

    await this.broadcastSync(
      classId,
      {
        type: 'client_sync',
        class_id: classId,
        user_id: userId,
        client_id: clientId,
        client_message_id: clientMessageId,
        payload,
      },
      ws,
    );
  }

  async replayFrom(ws, lastReceivedSequence) {
    const { pathname, classId, connectionId } = ws.clientContext;
    const operations = await this.database.getOperationsAfter(
      classId,
      lastReceivedSequence,
      new Set([pathname]),
    );
    const lastSequenceAvailable =
      operations.at(-1)?.sequenceNumber ??
      (await this.database.getLastSequence(classId));

    this.logger.info('websocket_replay_started', {
      classId,
      pathname,
      connectionId,
      lastReceivedSequence,
      replayCount: operations.length,
    });
    this.metricsCollector?.observe('ws_replay_messages', operations.length, {
      channel: pathname,
      classId,
    });

    this.sendJson(ws, {
      type: 'replay_start',
      class_id: classId,
      from_sequence: lastReceivedSequence,
      to_sequence: lastSequenceAvailable,
    });

    for (const operation of operations) {
      this.sendJson(ws, this.toEnvelope(classId, operation));
    }

    this.sendJson(ws, {
      type: 'replay_complete',
      class_id: classId,
      replayed: operations.length,
      last_sequence_available: lastSequenceAvailable,
    });
  }

  channelKey(pathname, classId) {
    return `${pathname}:${classId}`;
  }

  sendJson(ws, payload) {
    if (ws.readyState !== OPEN) {
      return;
    }

    try {
      ws.send(JSON.stringify(payload));
    } catch (error) {
      this.logger.warn('websocket_send_failed', {
        classId: ws.clientContext?.classId,
        pathname: ws.clientContext?.pathname,
        userId: ws.clientContext?.userId,
        error: String(error),
      });
      this.metricsCollector?.increment('ws_send_failures_total', {
        channel: ws.clientContext?.pathname ?? 'unknown',
        classId: ws.clientContext?.classId ?? 'unknown',
      });
    }
  }

  startHeartbeatLoop() {
    if (this.heartbeatTimer) {
      return;
    }
    this.heartbeatTimer = setInterval(() => {
      this.sweepHeartbeats();
    }, this.config.observability.websocketHeartbeatIntervalMs);
    this.heartbeatTimer.unref?.();
  }

  sweepHeartbeats() {
    const timeoutMs = this.config.observability.websocketHeartbeatTimeoutMs;
    const now = Date.now();

    for (const [key, channelConnections] of this.connections.entries()) {
      this.metricsCollector?.setGauge('ws_active_connections', channelConnections.size, {
        channel: key,
      });

      for (const ws of channelConnections.values()) {
        if (ws.readyState !== OPEN) {
          continue;
        }

        const lastPongAt = ws.clientContext?.lastPongAt ?? 0;
        if (now - lastPongAt > timeoutMs) {
          this.logger.warn('websocket_heartbeat_timeout', {
            classId: ws.clientContext?.classId,
            pathname: ws.clientContext?.pathname,
            userId: ws.clientContext?.userId,
            connectionId: ws.clientContext?.connectionId,
            lastPongAt,
            timeoutMs,
          });
          this.metricsCollector?.increment('ws_heartbeat_terminated_total', {
            channel: ws.clientContext?.pathname ?? 'unknown',
            classId: ws.clientContext?.classId ?? 'unknown',
          });
          ws.close(HEARTBEAT_TIMEOUT, 'Heartbeat timeout');
          ws.terminate();
          continue;
        }

        try {
          ws.ping();
        } catch (error) {
          this.logger.warn('websocket_ping_failed', {
            classId: ws.clientContext?.classId,
            pathname: ws.clientContext?.pathname,
            userId: ws.clientContext?.userId,
            error: String(error),
          });
        }
      }
    }
  }

  toEnvelope(classId, operation) {
    return {
      ...operation.payload,
      class_id: operation.payload.class_id ?? classId,
      message_id: operation.messageId,
      sequence_number: operation.sequenceNumber,
      channel: operation.channel,
      sent_at: operation.createdAt,
    };
  }

  broadcastLocal(pathname, classId, payload, skipWs = null) {
    const key = this.channelKey(pathname, classId);
    const clients = this.connections.get(key);
    if (!clients) {
      return;
    }

    for (const ws of clients.values()) {
      if (skipWs && ws === skipWs) {
        continue;
      }
      this.sendJson(ws, payload);
    }
  }

  async publish(pathname, classId, payload, { skipWs = null } = {}) {
    const operation = await this.database.appendOperation(
      classId,
      {
        messageId: randomUUID(),
        channel: pathname,
        payload,
        createdAt: new Date().toISOString(),
        originNodeId: this.config.nodeId,
      },
      {
        maxEntries: this.config.websocketReplayLimit,
      },
    );

    const envelope = this.toEnvelope(classId, operation);
    this.broadcastLocal(pathname, classId, envelope, skipWs);

    if (this.pubSub) {
      const replicated = await this.pubSub.publish({
        type: 'replicated_event',
        class_id: classId,
        origin_node_id: this.config.nodeId,
        operation,
      });
      if (!replicated) {
        this.logger.warn('websocket_pubsub_publish_degraded', {
          classId,
          channel: pathname,
          messageId: operation.messageId,
        });
      }
    }

    return envelope;
  }

  async handleReplicatedEvent(payload) {
    try {
      if (
        payload?.type !== 'replicated_event' ||
        payload.origin_node_id === this.config.nodeId
      ) {
        return;
      }

      const classId = payload.class_id;
      const operation = payload.operation;
      if (!classId || !operation?.channel) {
        this.logger.warn('replicated_event_ignored', {
          payload,
        });
        return;
      }

      await this.database.recordReplicatedOperation(classId, operation, {
        maxEntries: this.config.websocketReplayLimit,
      });
      this.broadcastLocal(
        operation.channel,
        classId,
        this.toEnvelope(classId, operation),
      );
    } catch (error) {
      this.logger.error('replicated_event_apply_failed', {
        error: String(error),
        payload,
      });
    }
  }

  async broadcastEvent(classId, payload) {
    return this.publish('/class/events', classId, payload);
  }

  async broadcastChat(classId, payload) {
    return this.publish('/chat/stream', classId, payload);
  }

  async broadcastSync(classId, payload, skipWs = null) {
    return this.publish('/class/sync', classId, payload, { skipWs });
  }

  async close() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    for (const channelConnections of this.connections.values()) {
      for (const ws of channelConnections.values()) {
        this.clearAuthTimers(ws);
        try {
          ws.close(1001, 'Server shutting down');
        } catch {
          // Best effort only.
        }
      }
    }
    this.connections.clear();

    await new Promise((resolve) => {
      this.wss.close(() => {
        resolve();
      });
    });
  }
}
