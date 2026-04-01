import { randomUUID } from 'node:crypto';

import express from 'express';

import { assertValidConfig, config } from './config/env.js';
import {
  createVerifyAccessToken,
  requireTeacher,
} from './middleware/authMiddleware.js';
import { createIdempotencyMiddleware } from './middleware/idempotencyMiddleware.js';
import { LiveKitClient } from './livekit/livekitClient.js';
import { RecordingQueue } from './queue/recordingQueue.js';
import { createPersistenceLayer } from './repositories/persistenceFactory.js';
import { createAuthRoutes } from './routes/authRoutes.js';
import { createBreakoutRoutes } from './routes/breakoutRoutes.js';
import { createChatRoutes } from './routes/chatRoutes.js';
import { createClassroomRoutes } from './routes/classroomRoutes.js';
import { createRecordingRoutes } from './routes/recordingRoutes.js';
import { AuthService } from './services/authService.js';
import { BreakoutService } from './services/breakoutService.js';
import { ChatService } from './services/chatService.js';
import { ClassroomAuthorityService } from './services/classroomAuthorityService.js';
import { IdentityService } from './services/identityService.js';
import { JwtService } from './services/jwtService.js';
import { LiveTokenService } from './services/liveTokenService.js';
import { ReconnectionService } from './services/reconnectionService.js';
import { RecordingService } from './services/recordingService.js';
import { createLogger } from './utils/logger.js';
import { MetricsCollector } from './utils/metricsCollector.js';
import { isHttpError } from './utils/httpError.js';
import { ClassroomHub } from './websocket/classroomHub.js';
import { RedisPubSub } from './websocket/redisPubSub.js';
import { runWithRequestContext } from './observability/requestContext.js';

function readClassIdFromRequest(req) {
  return (
    req.body?.class_id ??
    req.query?.class_id ??
    req.params?.class_id ??
    null
  );
}

function createRequestContext(logger, metricsCollector) {
  return function requestContext(req, res, next) {
    const requestId = req.header('x-request-id') ?? randomUUID();
    req.requestId = requestId;
    res.setHeader('x-request-id', requestId);
    metricsCollector.increment('http_requests_started_total', {
      method: req.method,
      path: req.path,
    });
    logger.debug('http_request_started', {
      requestId,
      method: req.method,
      path: req.originalUrl,
      classId: readClassIdFromRequest(req),
      userId: req.user?.userId ?? null,
      eventType: 'http_request_started',
    });
    runWithRequestContext(
      {
        requestId,
        method: req.method,
        path: req.originalUrl,
        classId: readClassIdFromRequest(req),
      },
      next,
    );
  };
}

function createRequestLogger(logger, metricsCollector) {
  return function requestLogger(req, res, next) {
    const startedAt = Date.now();
    res.on('finish', () => {
      const durationMs = Date.now() - startedAt;
      const classId = readClassIdFromRequest(req);
      logger.info('http_request_completed', {
        requestId: req.requestId ?? null,
        method: req.method,
        path: req.originalUrl,
        statusCode: res.statusCode,
        durationMs,
        userId: req.user?.userId ?? null,
        classId,
        idempotencyKey: req.header('Idempotency-Key') ?? null,
        eventType: 'http_request_completed',
      });
      metricsCollector.increment('http_requests_total', {
        method: req.method,
        path: req.path,
        statusCode: res.statusCode,
      });
      metricsCollector.observe('http_request_duration_ms', durationMs, {
        method: req.method,
        path: req.path,
        statusCode: res.statusCode,
      });
    });
    next();
  };
}

async function buildReadinessSnapshot({
  config,
  database,
  redisPubSub,
  recordingQueue,
  livekitClient,
}) {
  const checks = await Promise.all([
    database.healthCheck?.() ?? {
      component: 'database',
      status: 'unknown',
    },
    redisPubSub.healthCheck?.() ?? {
      component: 'redis_pubsub',
      status: 'unknown',
    },
    recordingQueue.healthCheck?.() ?? {
      component: 'recording_queue',
      status: 'unknown',
    },
    livekitClient.healthCheck?.() ?? {
      component: 'livekit',
      status: 'unknown',
    },
  ]);

  const failed = checks.filter((entry) => entry.status === 'failed');
  const degraded = checks.filter((entry) =>
    ['degraded', 'disabled', 'unknown'].includes(entry.status),
  );

  return {
    status: failed.length > 0 ? 'failed' : degraded.length > 0 ? 'degraded' : 'ready',
    node_id: config.nodeId,
    timestamp: new Date().toISOString(),
    checks,
  };
}

export async function createBackendApp() {
  const logger = createLogger('backend');
  const metricsCollector = new MetricsCollector();
  assertValidConfig(config, { service: 'server' });

  const { database, repositories } = await createPersistenceLayer({
    config,
    logger,
  });
  const {
    classSessionRepository,
    participantRepository,
    breakoutRoomRepository,
    chatMessageRepository,
    idempotencyRepository,
    userRepository,
    refreshTokenRepository,
    recordingJobRepository,
  } = repositories;

  const livekitClient = new LiveKitClient({
    config,
    logger: createLogger('livekit'),
  });
  const redisPubSub = new RedisPubSub({
    config,
    logger: createLogger('redis_pubsub'),
    metricsCollector,
  });
  const recordingQueue = new RecordingQueue({
    config,
    logger: createLogger('recording_queue'),
  });
  const jwtService = new JwtService({ config });
  const authService = new AuthService({
    config,
    database,
    userRepository,
    refreshTokenRepository,
    jwtService,
    logger: createLogger('auth'),
  });
  const verifyAccessToken = createVerifyAccessToken({ authService });
  const requireTeacherGuard = requireTeacher;

  const defaultTeacher = await userRepository.getById(config.defaultTeacherId);
  if (!defaultTeacher) {
    throw new Error(
      `Default teacher user ${config.defaultTeacherId} must exist before boot`,
    );
  }
  if (defaultTeacher.role !== 'teacher') {
    throw new Error(
      `Default teacher user ${config.defaultTeacherId} must have teacher role`,
    );
  }

  let authorityService = null;
  let chatService = null;
  let classroomHub = null;

  const reconnectionService = new ReconnectionService({
    config,
    database,
    participantRepository,
    logger: createLogger('reconnection'),
    onPresenceChanged: async (classId, participant) => {
      if (!classroomHub) {
        return;
      }
      await classroomHub.broadcastEvent(classId, {
        type: 'participant_updated',
        class_id: classId,
        participant,
      });
    },
  });
  reconnectionService.start();

  classroomHub = new ClassroomHub({
    config,
    database,
    logger: createLogger('websocket_hub'),
    metricsCollector,
    authService,
    reconnectionService,
    pubSub: redisPubSub,
    getWaitingRoomSnapshot: async (classId) => {
      return authorityService ? authorityService.getWaitingRoomSnapshot(classId) : [];
    },
    getClassroomState: async (classId, userId) => {
      return authorityService
        ? authorityService.fetchClassroomState(classId, userId)
        : {
            type: 'class_state_snapshot',
            class_id: classId,
            user_id: userId,
          };
    },
    getChatSnapshot: async (classId) => {
      return chatService
        ? chatService.getSnapshot(classId)
        : {
            classId,
            chatEnabled: true,
            messages: [],
          };
    },
  });

  authorityService = new ClassroomAuthorityService({
    config,
    database,
    classSessionRepository,
    participantRepository,
    breakoutRoomRepository,
    classroomHub,
    logger: createLogger('authority'),
  });

  const identityService = new IdentityService({
    classSessionRepository,
    participantRepository,
  });

  const liveTokenService = new LiveTokenService({
    database,
    classSessionRepository,
    participantRepository,
    breakoutRoomRepository,
    livekitClient,
    logger: createLogger('live_token'),
  });

  chatService = new ChatService({
    database,
    classSessionRepository,
    participantRepository,
    chatMessageRepository,
    classroomHub,
    logger: createLogger('chat'),
    metricsCollector,
  });

  const breakoutService = new BreakoutService({
    database,
    classSessionRepository,
    participantRepository,
    breakoutRoomRepository,
    liveTokenService,
    classroomHub,
    logger: createLogger('breakout'),
  });

  const recordingService = new RecordingService({
    database,
    classSessionRepository,
    recordingJobRepository,
    recordingQueue,
    livekitClient,
    classroomHub,
    logger: createLogger('recording'),
    metricsCollector,
  });

  await authorityService.ensureSession(config.defaultClassId);

  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '1mb' }));
  app.use(createRequestContext(logger, metricsCollector));
  app.use(createRequestLogger(logger, metricsCollector));
  app.get('/health/ping', (_req, res) => {
    res.json({ ok: true, timestamp: new Date().toISOString() });
  });
  app.get('/health/live', (_req, res) => {
    res.json({
      ok: true,
      status: 'alive',
      node_id: config.nodeId,
      timestamp: new Date().toISOString(),
    });
  });
  app.get('/health/ready', async (_req, res) => {
    const readiness = await buildReadinessSnapshot({
      config,
      database,
      redisPubSub,
      recordingQueue,
      livekitClient,
    });
    res.status(readiness.status === 'failed' ? 503 : 200).json(readiness);
  });
  app.get('/ops/metrics', (_req, res) => {
    res.json(metricsCollector.snapshot());
  });
  app.get('/ops/metrics.prometheus', (_req, res) => {
    res.type('text/plain; version=0.0.4').send(metricsCollector.toPrometheus());
  });
  app.use(
    createIdempotencyMiddleware({
      idempotencyRepository,
      logger: createLogger('idempotency_middleware'),
    }),
  );

  app.use(
    createAuthRoutes({
      authService,
      verifyAccessToken,
    }),
  );
  app.use(
    createClassroomRoutes({
      authorityService,
      identityService,
      liveTokenService,
      verifyAccessToken,
      requireTeacherGuard,
    }),
  );
  app.use(
    createChatRoutes({
      chatService,
      identityService,
      verifyAccessToken,
    }),
  );
  app.use(
    createBreakoutRoutes({
      breakoutService,
      breakoutRoomRepository,
      identityService,
      verifyAccessToken,
      requireTeacherGuard,
    }),
  );
  app.use(
    createRecordingRoutes({
      recordingService,
      verifyAccessToken,
      requireTeacherGuard,
    }),
  );

  app.use((req, res) => {
    res.status(404).json({
      error: `Route not found: ${req.method} ${req.originalUrl}`,
    });
  });

  app.use((error, req, res, _next) => {
    if (isHttpError(error)) {
      res.status(error.status).json({
        error: error.message,
        details: error.details,
        request_id: req.requestId ?? null,
      });
      return;
    }

    logger.error('unhandled_error', {
      requestId: req.requestId ?? null,
      method: req.method,
      path: req.originalUrl,
      userId: req.user?.userId ?? null,
      classId: readClassIdFromRequest(req),
      error: String(error),
      eventType: 'unhandled_error',
    });
    metricsCollector.increment('http_unhandled_errors_total', {
      method: req.method,
      path: req.path,
    });
    res.status(500).json({
      error: 'Internal server error',
      details: process.env.NODE_ENV === 'production' ? null : String(error),
      request_id: req.requestId ?? null,
    });
  });

  return {
    app,
    classroomHub,
    redisPubSub,
    recordingQueue,
    database,
    shutdown: async () => {
      await reconnectionService.close();
      await classroomHub.close();
      await redisPubSub.disconnect();
      await recordingQueue.close();
      await database.close?.();
    },
  };
}
