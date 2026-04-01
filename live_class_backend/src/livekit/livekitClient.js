import {
  AccessToken,
  EgressClient,
  RoomServiceClient,
} from 'livekit-server-sdk';

import { config as defaultConfig } from '../config/env.js';
import { CircuitBreaker } from '../utils/circuitBreaker.js';
import { HttpError } from '../utils/httpError.js';
import { withRetry } from '../utils/retry.js';

function isTransientLiveKitError(error) {
  const code = error?.code ?? '';
  return ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE'].includes(code);
}

export function buildMainRoomName(classId) {
  return `${classId}__main`;
}

export function buildBreakoutRoomName(classId, breakoutRoomId) {
  return `${classId}__breakout__${breakoutRoomId}`;
}

export class LiveKitClient {
  constructor({ config = defaultConfig, logger = null } = {}) {
    this.config = config;
    this.logger = logger;
    this.roomClient = null;
    this.egressClient = null;
    this.breaker = new CircuitBreaker({
      name: 'livekit',
      failureThreshold: 5,
      cooldownMs: 30000,
      successThreshold: 2,
      logger,
    });
  }

  assertConfigured() {
    if (
      !this.config.livekit.apiKey ||
      !this.config.livekit.apiSecret ||
      !this.config.livekit.wsUrl ||
      !this.config.livekit.httpUrl
    ) {
      throw new HttpError(
        503,
        'LiveKit credentials are not configured on the backend',
      );
    }
  }

  async execute(operationName, operation) {
    return withRetry(
      () => this.breaker.execute(operation),
      {
        retries: 3,
        initialDelayMs: 250,
        maxDelayMs: 2000,
        shouldRetry: isTransientLiveKitError,
        onRetry: async ({ attempt, waitMs, error }) => {
          this.logger?.warn('livekit_retry', {
            operation: operationName,
            attempt,
            waitMs,
            error: String(error),
          });
        },
      },
    ).catch((error) => {
      this.logger?.error('livekit_operation_failed', {
        operation: operationName,
        error: String(error),
      });
      throw error;
    });
  }

  getRoomClient() {
    this.assertConfigured();
    if (!this.roomClient) {
      this.roomClient = new RoomServiceClient(
        this.config.livekit.httpUrl,
        this.config.livekit.apiKey,
        this.config.livekit.apiSecret,
      );
    }
    return this.roomClient;
  }

  getEgressClient() {
    this.assertConfigured();
    if (!this.egressClient) {
      this.egressClient = new EgressClient(
        this.config.livekit.httpUrl,
        this.config.livekit.apiKey,
        this.config.livekit.apiSecret,
      );
    }
    return this.egressClient;
  }

  async healthCheck() {
    try {
      this.assertConfigured();
      return {
        component: 'livekit',
        status: 'ready',
        breaker_state: this.breaker.state,
        connectivity: 'config_validated',
      };
    } catch (error) {
      return {
        component: 'livekit',
        status: 'failed',
        error: String(error),
      };
    }
  }

  async ensureRoom(roomName) {
    const roomClient = this.getRoomClient();
    await this.execute('ensure_room', async () => {
      try {
        await roomClient.createRoom({
          name: roomName,
          emptyTimeout: this.config.livekit.roomEmptyTimeout,
        });
      } catch (error) {
        const message = String(error?.message ?? error);
        if (!message.toLowerCase().includes('already exists')) {
          throw error;
        }
      }
    });
  }

  async issueRoomToken({ roomName, userId, userName, metadata = {} }) {
    this.assertConfigured();
    await this.ensureRoom(roomName);

    const token = new AccessToken(
      this.config.livekit.apiKey,
      this.config.livekit.apiSecret,
      {
        identity: userId,
        name: userName,
        ttl: this.config.livekit.tokenTtl,
        metadata: JSON.stringify(metadata),
      },
    );

    token.addGrant({
      roomJoin: true,
      room: roomName,
      canPublish: true,
      canSubscribe: true,
      canPublishData: true,
    });

    return this.execute('issue_room_token', async () => {
      return {
        token: await token.toJwt(),
        roomName,
        wsUrl: this.config.livekit.wsUrl,
      };
    });
  }

  async startRoomRecording({ roomName, classId }) {
    const egressClient = this.getEgressClient();
    const filePath =
      `${this.config.livekit.recordingFilePrefix}/${classId}/` +
      `${roomName}-${Date.now()}.mp4`;

    const info = await this.execute('start_room_recording', async () => {
      return egressClient.startRoomCompositeEgress(roomName, {
        file: {
          filepath: filePath,
        },
      });
    });

    return {
      egressId: info.egressId,
      filePath,
      status: info.status,
    };
  }

  async stopRoomRecording(egressId) {
    const egressClient = this.getEgressClient();
    const info = await this.execute('stop_room_recording', async () => {
      return egressClient.stopEgress(egressId);
    });

    return {
      egressId: info.egressId,
      status: info.status,
    };
  }
}
