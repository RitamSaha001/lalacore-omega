import { randomUUID } from 'node:crypto';

import { parsePositiveInt } from '../utils/validation.js';

function read(name, fallback = '') {
  const value = process.env[name];
  if (typeof value !== 'string') {
    return fallback;
  }
  return value.trim();
}

function readBool(name, fallback = false) {
  const value = read(name, '');
  if (!value) {
    return fallback;
  }
  return value.toLowerCase() === 'true';
}

function readFloat(name, fallback) {
  const value = Number.parseFloat(read(name, ''));
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return value;
}

function looksLikePlaceholderSecret(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) {
    return true;
  }
  return (
    normalized.includes('change-me') ||
    normalized.includes('replace-me') ||
    normalized.includes('example') ||
    normalized.includes('placeholder')
  );
}

function looksLikePlaceholderUrl(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) {
    return true;
  }
  return (
    normalized.includes('change-me') ||
    normalized.includes('replace-me') ||
    normalized.includes('example') ||
    normalized.includes('placeholder') ||
    normalized.includes('localhost') ||
    normalized.includes('127.0.0.1')
  );
}

function deriveLivekitHttpUrl(wsUrl) {
  const normalized = String(wsUrl || '').trim();
  if (!normalized) {
    return '';
  }
  if (normalized.startsWith('wss://')) {
    return `https://${normalized.slice('wss://'.length)}`;
  }
  if (normalized.startsWith('ws://')) {
    return `http://${normalized.slice('ws://'.length)}`;
  }
  return normalized;
}

const configuredLivekitWsUrl = read('LIVEKIT_WS_URL', read('LIVEKIT_URL'));
const configuredLivekitHttpUrl = read(
  'LIVEKIT_HTTP_URL',
  deriveLivekitHttpUrl(configuredLivekitWsUrl),
);

export const config = {
  nodeEnv: read('NODE_ENV', 'development').toLowerCase(),
  port: parsePositiveInt(read('PORT', '8080'), 8080),
  nodeId: read('NODE_ID', `node_${randomUUID().slice(0, 8)}`),
  storage: {
    driver: read(
      'STORAGE_DRIVER',
      read('NODE_ENV', 'development').toLowerCase() === 'test'
        ? 'memory'
        : 'postgres',
    ),
  },
  database: {
    url: read('DATABASE_URL'),
    maxConnections: parsePositiveInt(read('DATABASE_MAX_CONNECTIONS', '20'), 20),
    idleTimeoutMs: parsePositiveInt(read('DATABASE_IDLE_TIMEOUT_MS', '30000'), 30000),
    ssl: readBool('DATABASE_SSL', false),
  },
  defaultClassId: read('DEFAULT_CLASS_ID', 'physics_live_01'),
  defaultClassTitle: read('DEFAULT_CLASS_TITLE', 'JEE Live Class'),
  defaultTeacherId: read('DEFAULT_TEACHER_ID', 'teacher_01'),
  defaultTeacherName: read('DEFAULT_TEACHER_NAME', 'Dr Sharma'),
  defaultWaitingRoomEnabled: readBool('DEFAULT_WAITING_ROOM_ENABLED', true),
  auth: {
    jwtSecret: read('JWT_SECRET'),
    issuer: read('JWT_ISSUER', 'live-class-backend'),
    audience: read('JWT_AUDIENCE', 'live-classroom-clients'),
    accessTokenTtl: read('AUTH_ACCESS_TOKEN_TTL', '15m'),
    accessTokenTtlSeconds: parsePositiveInt(
      read('AUTH_ACCESS_TOKEN_TTL_SECONDS', '900'),
      900,
    ),
    refreshTokenTtlMs: parsePositiveInt(
      read('AUTH_REFRESH_TOKEN_TTL_MS', `${30 * 24 * 60 * 60 * 1000}`),
      30 * 24 * 60 * 60 * 1000,
    ),
    defaultTeacherEmail: read('DEFAULT_TEACHER_EMAIL', 'teacher@example.com'),
    defaultTeacherPassword: read('DEFAULT_TEACHER_PASSWORD'),
    defaultTeacherPasswordHash: read('DEFAULT_TEACHER_PASSWORD_HASH'),
    defaultStudentId: read('DEFAULT_STUDENT_ID', 'student_01'),
    defaultStudentName: read('DEFAULT_STUDENT_NAME', 'Student One'),
    defaultStudentEmail: read('DEFAULT_STUDENT_EMAIL', 'student@example.com'),
    defaultStudentPassword: read('DEFAULT_STUDENT_PASSWORD'),
    defaultStudentPasswordHash: read('DEFAULT_STUDENT_PASSWORD_HASH'),
  },
  reconnectGracePeriodMs: parsePositiveInt(
    read('RECONNECT_GRACE_PERIOD_MS', '30000'),
    30000,
  ),
  websocketReplayLimit: parsePositiveInt(
    read('WEBSOCKET_REPLAY_LIMIT', '2000'),
    2000,
  ),
  idempotencyTtlMs: parsePositiveInt(
    read('IDEMPOTENCY_TTL_MS', '86400000'),
    86400000,
  ),
  idempotencyWaitTimeoutMs: parsePositiveInt(
    read('IDEMPOTENCY_WAIT_TIMEOUT_MS', '30000'),
    30000,
  ),
  reconnectBackoff: {
    initialDelayMs: parsePositiveInt(
      read('RECONNECT_INITIAL_DELAY_MS', '1000'),
      1000,
    ),
    maxDelayMs: parsePositiveInt(read('RECONNECT_MAX_DELAY_MS', '30000'), 30000),
    multiplier: readFloat('RECONNECT_BACKOFF_MULTIPLIER', 2),
    jitterRatio: readFloat('RECONNECT_JITTER_RATIO', 0.25),
  },
  observability: {
    otelEnabled: readBool('OTEL_ENABLED', false),
    otelServiceName: read('OTEL_SERVICE_NAME', 'live-class-backend'),
    otelServiceVersion: read('OTEL_SERVICE_VERSION', '0.1.0'),
    otelExporterUrl: read('OTEL_EXPORTER_OTLP_ENDPOINT'),
    websocketHeartbeatIntervalMs: parsePositiveInt(
      read('WS_HEARTBEAT_INTERVAL_MS', '15000'),
      15000,
    ),
    websocketHeartbeatTimeoutMs: parsePositiveInt(
      read('WS_HEARTBEAT_TIMEOUT_MS', '45000'),
      45000,
    ),
  },
  redis: {
    url: read('REDIS_URL'),
    channel: read('REDIS_CHANNEL', 'live_classroom_events'),
  },
  queue: {
    recordingQueueName: read('RECORDING_QUEUE_NAME', 'recording-processing'),
    recordingWorkerConcurrency: parsePositiveInt(
      read('RECORDING_WORKER_CONCURRENCY', '4'),
      4,
    ),
    recordingJobAttempts: parsePositiveInt(
      read('RECORDING_JOB_ATTEMPTS', '5'),
      5,
    ),
    recordingJobBackoffMs: parsePositiveInt(
      read('RECORDING_JOB_BACKOFF_MS', '1000'),
      1000,
    ),
  },
  runtime: {
    allowInMemoryStorage: readBool(
      'ALLOW_IN_MEMORY_STORAGE',
      read('NODE_ENV', 'development').toLowerCase() === 'test',
    ),
    allowMissingRedis: readBool(
      'ALLOW_MISSING_REDIS',
      read('NODE_ENV', 'development').toLowerCase() === 'test',
    ),
    allowMissingLivekit: readBool(
      'ALLOW_MISSING_LIVEKIT',
      read('NODE_ENV', 'development').toLowerCase() === 'development',
    ),
  },
  ai: {
    baseUrl: read('AI_PIPELINE_BASE_URL'),
    apiKey: read('AI_PIPELINE_API_KEY'),
  },
  youtube: {
    enabled: readBool('YOUTUBE_UPLOAD_ENABLED', false),
    required: readBool('YOUTUBE_UPLOAD_REQUIRED', false),
    accessToken: read('YOUTUBE_ACCESS_TOKEN'),
    clientId: read('YOUTUBE_CLIENT_ID'),
    clientSecret: read('YOUTUBE_CLIENT_SECRET'),
    refreshToken: read('YOUTUBE_REFRESH_TOKEN'),
    privacyStatus: read('YOUTUBE_PRIVACY_STATUS', 'unlisted'),
    categoryId: read('YOUTUBE_CATEGORY_ID', '27'),
    defaultTags: read('YOUTUBE_DEFAULT_TAGS'),
    notifySubscribers: readBool('YOUTUBE_NOTIFY_SUBSCRIBERS', false),
    madeForKids: readBool('YOUTUBE_MADE_FOR_KIDS', false),
    recordingPublicBaseUrl: read('RECORDING_PUBLIC_BASE_URL'),
    recordingWorkdir: read('RECORDING_WORKDIR'),
  },
  livekit: {
    wsUrl: configuredLivekitWsUrl,
    httpUrl: configuredLivekitHttpUrl,
    apiKey: read('LIVEKIT_API_KEY'),
    apiSecret: read('LIVEKIT_API_SECRET'),
    tokenTtl: read('LIVEKIT_TOKEN_TTL', '2h'),
    roomEmptyTimeout: parsePositiveInt(
      read('LIVEKIT_ROOM_EMPTY_TIMEOUT', '300'),
      300,
    ),
    recordingFilePrefix: read(
      'LIVEKIT_RECORDING_FILE_PREFIX',
      'recordings',
    ),
  },
  reconnectSweepIntervalMs: parsePositiveInt(
    read('RECONNECT_SWEEP_INTERVAL_MS', '5000'),
    5000,
  ),
};

export function collectConfigErrors(
  runtimeConfig = config,
  { service = 'server' } = {},
) {
  const errors = [];
  const inProduction = runtimeConfig.nodeEnv === 'production';

  if (!runtimeConfig.auth.jwtSecret) {
    errors.push('JWT_SECRET is required.');
  } else if (inProduction && looksLikePlaceholderSecret(runtimeConfig.auth.jwtSecret)) {
    errors.push('JWT_SECRET still uses a placeholder value.');
  }

  if (
    runtimeConfig.storage.driver === 'postgres' &&
    !runtimeConfig.database.url
  ) {
    errors.push(
      'DATABASE_URL is required when STORAGE_DRIVER=postgres.',
    );
  }

  if (service === 'worker') {
    if (!runtimeConfig.redis.url) {
      errors.push('REDIS_URL is required for the recording worker.');
    }
    if (!runtimeConfig.ai.baseUrl) {
      errors.push('AI_PIPELINE_BASE_URL is required for the recording worker.');
    }
    const youtubeAuthConfigured = Boolean(
      runtimeConfig.youtube.accessToken ||
        (
          runtimeConfig.youtube.clientId &&
          runtimeConfig.youtube.clientSecret &&
          runtimeConfig.youtube.refreshToken
        ),
    );
    if (runtimeConfig.youtube.required && !runtimeConfig.youtube.enabled) {
      errors.push(
        'YOUTUBE_UPLOAD_REQUIRED=true also requires YOUTUBE_UPLOAD_ENABLED=true.',
      );
    }
    if (runtimeConfig.youtube.enabled && !youtubeAuthConfigured) {
      errors.push(
        'YouTube upload is enabled but credentials are missing. Set YOUTUBE_ACCESS_TOKEN or YOUTUBE_CLIENT_ID/YOUTUBE_CLIENT_SECRET/YOUTUBE_REFRESH_TOKEN.',
      );
    }
  } else if (!runtimeConfig.redis.url && !runtimeConfig.runtime.allowMissingRedis) {
    errors.push(
      'REDIS_URL is required unless ALLOW_MISSING_REDIS=true.',
    );
  }

  const livekitMissing =
    !runtimeConfig.livekit.apiKey ||
    !runtimeConfig.livekit.apiSecret ||
    !runtimeConfig.livekit.wsUrl ||
    !runtimeConfig.livekit.httpUrl;
  if (livekitMissing && !runtimeConfig.runtime.allowMissingLivekit) {
    errors.push(
      'LIVEKIT_API_KEY, LIVEKIT_API_SECRET, and LIVEKIT_WS_URL are required unless ALLOW_MISSING_LIVEKIT=true. LIVEKIT_HTTP_URL is derived automatically from LIVEKIT_WS_URL or LIVEKIT_URL when omitted.',
    );
  } else if (
    inProduction &&
    (looksLikePlaceholderSecret(runtimeConfig.livekit.apiKey) ||
      looksLikePlaceholderSecret(runtimeConfig.livekit.apiSecret) ||
      looksLikePlaceholderUrl(runtimeConfig.livekit.wsUrl) ||
      looksLikePlaceholderUrl(runtimeConfig.livekit.httpUrl))
  ) {
    errors.push('LiveKit credentials still use placeholder or localhost values.');
  }

  const youtubeAuthConfigured = Boolean(
    runtimeConfig.youtube.accessToken ||
      (
        runtimeConfig.youtube.clientId &&
        runtimeConfig.youtube.clientSecret &&
        runtimeConfig.youtube.refreshToken
      ),
  );
  if (
    runtimeConfig.youtube.enabled &&
    inProduction &&
    (
      (runtimeConfig.youtube.accessToken &&
        looksLikePlaceholderSecret(runtimeConfig.youtube.accessToken)) ||
      (
        youtubeAuthConfigured &&
        (
          looksLikePlaceholderSecret(runtimeConfig.youtube.clientId) ||
          looksLikePlaceholderSecret(runtimeConfig.youtube.clientSecret) ||
          looksLikePlaceholderSecret(runtimeConfig.youtube.refreshToken)
        )
      )
    )
  ) {
    errors.push('YouTube upload credentials still use placeholder values.');
  }

  return errors;
}

export function assertValidConfig(runtimeConfig = config, options = {}) {
  const errors = collectConfigErrors(runtimeConfig, options);
  if (errors.length === 0) {
    return;
  }
  const service = String(options.service || 'service');
  throw new Error(
    `Configuration invalid for ${service}:\n- ${errors.join('\n- ')}`,
  );
}
