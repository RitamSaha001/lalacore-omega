import { HttpError } from '../../utils/httpError.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class PostgresIdempotencyRepository {
  constructor(store, { waitTimeoutMs, logger }) {
    this.store = store;
    this.waitTimeoutMs = waitTimeoutMs;
    this.logger = logger;
  }

  buildStorageKey(scope, key) {
    return `${scope}:${key}`;
  }

  async begin({ scope, key, fingerprint }) {
    const storageKey = this.buildStorageKey(scope, key);
    return this.store.withLocks([`idempotency:${storageKey}`], async () => {
      const { rows } = await this.store.query(
        `
          SELECT *
          FROM idempotency_keys
          WHERE scope = $1
            AND idempotency_key = $2
          FOR UPDATE
        `,
        [scope, key],
      );
      const existing = rows[0];

      if (!existing) {
        await this.store.query(
          `
            INSERT INTO idempotency_keys (
              scope,
              idempotency_key,
              fingerprint,
              status,
              created_at,
              updated_at
            )
            VALUES ($1, $2, $3, 'in_progress', NOW(), NOW())
          `,
          [scope, key, fingerprint],
        );
        return {
          type: 'started',
        };
      }

      if (existing.fingerprint !== fingerprint) {
        throw new HttpError(
          409,
          'Idempotency-Key has already been used for a different request payload',
        );
      }

      if (existing.status === 'completed') {
        return {
          type: 'replay',
          record: {
            response: {
              kind: existing.response_kind,
              statusCode: existing.response_status_code,
              contentType: existing.response_content_type,
              body: existing.response_body,
            },
          },
        };
      }

      return {
        type: 'wait',
        waitForCompletion: this.waitForCompletion(scope, key, fingerprint),
      };
    });
  }

  async complete({ scope, key, fingerprint, response }) {
    const { rows } = await this.store.query(
      `
        UPDATE idempotency_keys
        SET status = 'completed',
            response_kind = $4,
            response_status_code = $5,
            response_content_type = $6,
            response_body = $7::jsonb,
            updated_at = NOW()
        WHERE scope = $1
          AND idempotency_key = $2
          AND fingerprint = $3
        RETURNING *
      `,
      [
        scope,
        key,
        fingerprint,
        response.kind,
        response.statusCode,
        response.contentType,
        JSON.stringify(response.body ?? null),
      ],
    );
    return rows[0] ?? null;
  }

  async release({ scope, key, fingerprint }) {
    await this.store.query(
      `
        DELETE FROM idempotency_keys
        WHERE scope = $1
          AND idempotency_key = $2
          AND fingerprint = $3
          AND status = 'in_progress'
      `,
      [scope, key, fingerprint],
    );
  }

  async waitForCompletion(scope, key, fingerprint) {
    const deadline = Date.now() + this.waitTimeoutMs;
    while (Date.now() < deadline) {
      const { rows } = await this.store.query(
        `
          SELECT *
          FROM idempotency_keys
          WHERE scope = $1
            AND idempotency_key = $2
            AND fingerprint = $3
        `,
        [scope, key, fingerprint],
      );
      const existing = rows[0];
      if (existing?.status === 'completed') {
        return {
          response: {
            kind: existing.response_kind,
            statusCode: existing.response_status_code,
            contentType: existing.response_content_type,
            body: existing.response_body,
          },
        };
      }
      await sleep(250);
    }
    this.logger.warn('idempotency_wait_timeout', {
      scope,
      key,
    });
    throw new HttpError(409, 'An identical request is still in progress, retry shortly');
  }
}
