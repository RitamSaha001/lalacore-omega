import { HttpError } from '../utils/httpError.js';

export class IdempotencyRepository {
  constructor(db, { ttlMs, waitTimeoutMs, logger }) {
    this.db = db;
    this.ttlMs = ttlMs;
    this.waitTimeoutMs = waitTimeoutMs;
    this.logger = logger;
    this.waiters = new Map();
  }

  buildStorageKey(scope, key) {
    return `${scope}:${key}`;
  }

  pruneExpired() {
    const now = Date.now();
    for (const [storageKey, record] of this.db.idempotencyRecords.entries()) {
      if (now - record.updatedAtEpochMs <= this.ttlMs) {
        continue;
      }
      this.db.idempotencyRecords.delete(storageKey);
      this.waiters.delete(storageKey);
    }
  }

  async begin({ scope, key, fingerprint }) {
    const storageKey = this.buildStorageKey(scope, key);
    return this.db.withLocks([`idempotency:${storageKey}`], async () => {
      this.pruneExpired();
      const existing = this.db.idempotencyRecords.get(storageKey);
      if (!existing) {
        const record = {
          scope,
          key,
          fingerprint,
          status: 'in_progress',
          response: null,
          createdAtEpochMs: Date.now(),
          updatedAtEpochMs: Date.now(),
        };
        this.db.idempotencyRecords.set(storageKey, record);
        return {
          type: 'started',
          record,
        };
      }

      if (existing.fingerprint !== fingerprint) {
        this.logger.warn('idempotency_key_reused_with_different_payload', {
          scope,
          key,
        });
        throw new HttpError(
          409,
          'Idempotency-Key has already been used for a different request payload',
        );
      }

      if (existing.status === 'completed') {
        this.logger.info('idempotency_replay_hit', {
          scope,
          key,
        });
        return {
          type: 'replay',
          record: existing,
        };
      }

      this.logger.info('idempotency_waiting_for_inflight_request', {
        scope,
        key,
      });
      return {
        type: 'wait',
        waitForCompletion: this.waitForCompletion(storageKey),
      };
    });
  }

  async complete({ scope, key, fingerprint, response }) {
    const storageKey = this.buildStorageKey(scope, key);
    return this.db.withLocks([`idempotency:${storageKey}`], async () => {
      const existing = this.db.idempotencyRecords.get(storageKey);
      if (!existing) {
        return null;
      }
      if (existing.fingerprint !== fingerprint) {
        throw new HttpError(409, 'Idempotency state mismatch');
      }

      const record = {
        ...existing,
        status: 'completed',
        response,
        updatedAtEpochMs: Date.now(),
      };
      this.db.idempotencyRecords.set(storageKey, record);
      const waiters = this.waiters.get(storageKey) ?? [];
      this.waiters.delete(storageKey);
      for (const waiter of waiters) {
        waiter.resolve(record);
      }
      return record;
    });
  }

  async release({ scope, key, fingerprint }) {
    const storageKey = this.buildStorageKey(scope, key);
    return this.db.withLocks([`idempotency:${storageKey}`], async () => {
      const existing = this.db.idempotencyRecords.get(storageKey);
      if (!existing || existing.fingerprint !== fingerprint) {
        return;
      }
      if (existing.status === 'completed') {
        return;
      }
      this.db.idempotencyRecords.delete(storageKey);
      const waiters = this.waiters.get(storageKey) ?? [];
      this.waiters.delete(storageKey);
      for (const waiter of waiters) {
        waiter.reject(
          new HttpError(
            409,
            'Original idempotent request did not complete, retry with the same key',
          ),
        );
      }
    });
  }

  waitForCompletion(storageKey) {
    return Promise.race([
      new Promise((resolve, reject) => {
        if (!this.waiters.has(storageKey)) {
          this.waiters.set(storageKey, []);
        }
        this.waiters.get(storageKey).push({ resolve, reject });
      }),
      new Promise((_, reject) => {
        setTimeout(() => {
          reject(
            new HttpError(
              409,
              'An identical request is still in progress, retry shortly',
            ),
          );
        }, this.waitTimeoutMs);
      }),
    ]);
  }
}
