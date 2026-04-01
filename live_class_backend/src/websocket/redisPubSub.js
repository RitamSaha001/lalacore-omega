import { CircuitBreaker } from '../utils/circuitBreaker.js';
import { withRetry } from '../utils/retry.js';

function isTransientRedisError(error) {
  const code = error?.code ?? '';
  return ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE'].includes(code);
}

export class RedisPubSub {
  constructor({ config, logger, metricsCollector = null }) {
    this.config = config;
    this.logger = logger;
    this.metricsCollector = metricsCollector;
    this.publisher = null;
    this.subscriber = null;
    this.handlers = new Set();
    this.connected = false;
    this.breaker = new CircuitBreaker({
      name: 'redis_pubsub',
      failureThreshold: 5,
      cooldownMs: 30000,
      successThreshold: 2,
      logger,
    });
  }

  onEvent(handler) {
    this.handlers.add(handler);
    return () => {
      this.handlers.delete(handler);
    };
  }

  bindClientEvents(client, role) {
    client.on('error', (error) => {
      this.logger.warn('redis_client_error', {
        role,
        error: String(error),
      });
    });

    client.on('end', () => {
      this.connected = false;
      this.logger.warn('redis_client_disconnected', {
        role,
      });
      this.metricsCollector?.increment('redis_disconnects_total', { role });
    });

    client.on('ready', () => {
      this.logger.info('redis_client_ready', {
        role,
      });
      this.metricsCollector?.increment('redis_ready_total', { role });
    });
  }

  async connect() {
    if (!this.config.redis.url) {
      this.logger.info('redis_pubsub_disabled', {
        reason: 'missing_redis_url',
      });
      return false;
    }

    try {
      await withRetry(
        async () => {
          return this.breaker.execute(async () => {
            await this.disconnect();
            const { createClient } = await import('redis');
            this.publisher = createClient({
              url: this.config.redis.url,
            });
            this.subscriber = this.publisher.duplicate();
            this.bindClientEvents(this.publisher, 'publisher');
            this.bindClientEvents(this.subscriber, 'subscriber');
            await this.publisher.connect();
            await this.subscriber.connect();
            await this.subscriber.subscribe(this.config.redis.channel, (message) => {
              this.handleMessage(message);
            });
            this.connected = true;
          });
        },
        {
          retries: 3,
          initialDelayMs: 250,
          maxDelayMs: 2000,
          shouldRetry: isTransientRedisError,
          onRetry: async ({ attempt, waitMs, error }) => {
            this.logger.warn('redis_pubsub_retry', {
              attempt,
              waitMs,
              error: String(error),
            });
          },
        },
      );

      this.logger.info('redis_pubsub_connected', {
        channel: this.config.redis.channel,
      });
      this.metricsCollector?.increment('redis_pubsub_connected_total', {
        channel: this.config.redis.channel,
      });
      return true;
    } catch (error) {
      this.connected = false;
      await this.disconnect();
      this.logger.error('redis_pubsub_connect_failed', {
        error: String(error),
      });
      this.metricsCollector?.increment('redis_pubsub_connect_failures_total', {
        channel: this.config.redis.channel,
      });
      return false;
    }
  }

  async disconnect() {
    if (!this.publisher && !this.subscriber) {
      return;
    }

    const subscriber = this.subscriber;
    const publisher = this.publisher;
    this.connected = false;
    this.publisher = null;
    this.subscriber = null;

    await subscriber?.quit().catch((error) => {
      this.logger.warn('redis_subscriber_quit_failed', {
        error: String(error),
      });
    });
    await publisher?.quit().catch((error) => {
      this.logger.warn('redis_publisher_quit_failed', {
        error: String(error),
      });
    });
  }

  async publish(payload) {
    if (!this.config.redis.url) {
      return false;
    }

    if (!this.connected || !this.publisher) {
      this.logger.warn('redis_publish_skipped', {
        reason: 'not_connected',
        channel: this.config.redis.channel,
      });
      this.metricsCollector?.increment('redis_publish_skipped_total', {
        channel: this.config.redis.channel,
      });
      return false;
    }

    try {
      await withRetry(
        () =>
          this.breaker.execute(async () => {
            await this.publisher.publish(
              this.config.redis.channel,
              JSON.stringify(payload),
            );
          }),
        {
          retries: 3,
          initialDelayMs: 100,
          maxDelayMs: 1000,
          shouldRetry: isTransientRedisError,
          onRetry: async ({ attempt, waitMs, error }) => {
            this.logger.warn('redis_publish_retry', {
              attempt,
              waitMs,
              error: String(error),
            });
          },
        },
      );
      this.metricsCollector?.increment('redis_publish_total', {
        channel: this.config.redis.channel,
      });
      return true;
    } catch (error) {
      this.logger.error('redis_publish_failed', {
        error: String(error),
      });
      this.metricsCollector?.increment('redis_publish_failures_total', {
        channel: this.config.redis.channel,
      });
      return false;
    }
  }

  async healthCheck() {
    if (!this.config.redis.url) {
      return {
        component: 'redis_pubsub',
        status: 'disabled',
        reason: 'missing_redis_url',
      };
    }

    if (!this.connected || !this.publisher) {
      return {
        component: 'redis_pubsub',
        status: 'degraded',
        reason: 'not_connected',
      };
    }

    const startedAt = Date.now();
    try {
      const response = await this.publisher.ping();
      return {
        component: 'redis_pubsub',
        status: 'ready',
        latency_ms: Date.now() - startedAt,
        response,
      };
    } catch (error) {
      return {
        component: 'redis_pubsub',
        status: 'failed',
        latency_ms: Date.now() - startedAt,
        error: String(error),
      };
    }
  }

  handleMessage(message) {
    try {
      const payload = JSON.parse(message);
      for (const handler of this.handlers) {
        handler(payload);
      }
    } catch (error) {
      this.logger.warn('redis_pubsub_invalid_payload', {
        error: String(error),
      });
    }
  }
}
