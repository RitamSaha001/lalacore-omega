import IORedis from 'ioredis';
import { Queue, Worker } from 'bullmq';

import { HttpError } from '../utils/httpError.js';

export class RecordingQueue {
  constructor({ config, logger }) {
    this.config = config;
    this.logger = logger;
    this.connection = null;
    this.queue = null;
  }

  getConnection() {
    if (!this.config.redis.url) {
      if (this.config.runtime?.allowMissingRedis) {
        throw new HttpError(503, 'Redis is disabled for this runtime');
      }
      throw new HttpError(503, 'Redis URL is required for BullMQ recording queue');
    }
    if (!this.connection) {
      this.connection = new IORedis(this.config.redis.url, {
        maxRetriesPerRequest: null,
      });
    }
    return this.connection;
  }

  getQueue() {
    if (!this.queue) {
      this.queue = new Queue(this.config.queue.recordingQueueName, {
        connection: this.getConnection(),
      });
    }
    return this.queue;
  }

  async enqueue(jobPayload) {
    const queue = this.getQueue();
    const job = await queue.add('process-recording', jobPayload, {
      jobId: jobPayload.jobId,
      attempts: this.config.queue.recordingJobAttempts,
      removeOnComplete: 1000,
      removeOnFail: 1000,
      backoff: {
        type: 'exponential',
        delay: this.config.queue.recordingJobBackoffMs,
      },
    });
    this.logger.info('recording_job_enqueued', {
      jobId: job.id,
      classId: jobPayload.classId,
    });
    return job;
  }

  createWorker(processor) {
    return new Worker(
      this.config.queue.recordingQueueName,
      processor,
      {
        connection: this.getConnection(),
        concurrency: this.config.queue.recordingWorkerConcurrency,
      },
    );
  }

  async close() {
    await this.queue?.close().catch((error) => {
      this.logger.warn('recording_queue_close_failed', {
        error: String(error),
      });
    });
    await this.connection?.quit().catch((error) => {
      this.logger.warn('recording_queue_connection_close_failed', {
        error: String(error),
      });
    });
  }

  async healthCheck() {
    if (!this.config.redis.url) {
      return {
        component: 'recording_queue',
        status: 'disabled',
        reason: 'missing_redis_url',
      };
    }

    const startedAt = Date.now();
    try {
      const connection = this.getConnection();
      const pong = await connection.ping();
      return {
        component: 'recording_queue',
        status: 'ready',
        latency_ms: Date.now() - startedAt,
        response: pong,
      };
    } catch (error) {
      return {
        component: 'recording_queue',
        status: 'failed',
        latency_ms: Date.now() - startedAt,
        error: String(error),
      };
    }
  }
}
