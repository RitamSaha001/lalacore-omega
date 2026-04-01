import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';

import { Pool } from 'pg';

import { CircuitBreaker } from '../../utils/circuitBreaker.js';
import { withRetry } from '../../utils/retry.js';

function advisoryKey(lockKey) {
  const hash = createHash('sha256').update(lockKey).digest('hex').slice(0, 15);
  return BigInt(`0x${hash}`).toString();
}

function mapOperationRow(row) {
  return {
    messageId: row.message_id,
    sequenceNumber: Number(row.sequence_number),
    channel: row.channel,
    payload: row.payload,
    createdAt: row.created_at?.toISOString?.() ?? row.created_at,
    originNodeId: row.origin_node_id,
  };
}

export class PostgresStore {
  constructor({ config, logger }) {
    this.config = config;
    this.logger = logger;
    this.context = new AsyncLocalStorage();
    this.pool = new Pool({
      connectionString: config.database.url,
      max: config.database.maxConnections,
      idleTimeoutMillis: config.database.idleTimeoutMs,
      ssl: config.database.ssl ? { rejectUnauthorized: false } : undefined,
    });
    this.breaker = new CircuitBreaker({
      name: 'postgres',
      failureThreshold: 5,
      cooldownMs: 30000,
      successThreshold: 2,
      logger,
    });
  }

  getClient() {
    return this.context.getStore()?.client ?? null;
  }

  async query(text, params = [], { client = null } = {}) {
    const runner = client ?? this.getClient() ?? this.pool;
    return withRetry(
      () =>
        this.breaker.execute(async () => {
          return runner.query(text, params);
        }),
      {
        retries: 3,
        onRetry: async ({ attempt, waitMs, error }) => {
          this.logger.warn('postgres_query_retry', {
            attempt,
            waitMs,
            error: String(error),
          });
        },
      },
    );
  }

  async withTransaction(callback) {
    const existingClient = this.getClient();
    if (existingClient) {
      return callback(existingClient);
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await this.context.run({ client }, async () => callback(client));
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async withLocks(lockKeys, callback) {
    const keys = [...new Set(lockKeys)].sort();
    return this.withTransaction(async (client) => {
      for (const key of keys) {
        await this.query(
          'SELECT pg_advisory_xact_lock($1::bigint)',
          [advisoryKey(key)],
          { client },
        );
      }
      return callback();
    });
  }

  async getLastSequence(classId) {
    const { rows } = await this.query(
      `
        SELECT COALESCE(MAX(sequence_number), 0) AS last_sequence
        FROM class_event_log
        WHERE class_id = $1
      `,
      [classId],
    );
    return Number(rows[0]?.last_sequence ?? 0);
  }

  async appendOperation(classId, operation, { maxEntries = 2000 } = {}) {
    return this.withLocks([`event_log:${classId}`], async () => {
      const nextSequence = (await this.getLastSequence(classId)) + 1;
      const { rows } = await this.query(
        `
          INSERT INTO class_event_log (
            class_id,
            sequence_number,
            channel,
            message_id,
            payload,
            origin_node_id,
            created_at
          )
          VALUES ($1, $2, $3, $4, $5::jsonb, $6, NOW())
          RETURNING class_id,
                    sequence_number,
                    channel,
                    message_id,
                    payload,
                    origin_node_id,
                    created_at
        `,
        [
          classId,
          nextSequence,
          operation.channel,
          operation.messageId,
          JSON.stringify(operation.payload),
          operation.originNodeId,
        ],
      );

      await this.query(
        `
          DELETE FROM class_event_log
          WHERE class_id = $1
            AND sequence_number NOT IN (
              SELECT sequence_number
              FROM class_event_log
              WHERE class_id = $1
              ORDER BY sequence_number DESC
              LIMIT $2
            )
        `,
        [classId, maxEntries],
      );

      return mapOperationRow(rows[0]);
    });
  }

  async recordReplicatedOperation(classId, operation, { maxEntries = 2000 } = {}) {
    return this.withLocks([`event_log:${classId}`], async () => {
      const { rows } = await this.query(
        `
          INSERT INTO class_event_log (
            class_id,
            sequence_number,
            channel,
            message_id,
            payload,
            origin_node_id,
            created_at
          )
          VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
          ON CONFLICT (class_id, sequence_number) DO UPDATE
          SET payload = EXCLUDED.payload
          RETURNING class_id,
                    sequence_number,
                    channel,
                    message_id,
                    payload,
                    origin_node_id,
                    created_at
        `,
        [
          classId,
          operation.sequenceNumber,
          operation.channel,
          operation.messageId,
          JSON.stringify(operation.payload),
          operation.originNodeId,
          operation.createdAt,
        ],
      );

      await this.query(
        `
          DELETE FROM class_event_log
          WHERE class_id = $1
            AND sequence_number NOT IN (
              SELECT sequence_number
              FROM class_event_log
              WHERE class_id = $1
              ORDER BY sequence_number DESC
              LIMIT $2
            )
        `,
        [classId, maxEntries],
      );

      return mapOperationRow(rows[0]);
    });
  }

  async getOperationsAfter(classId, sequenceNumber, channels = null) {
    const params = [classId, sequenceNumber];
    let sql = `
      SELECT class_id,
             sequence_number,
             channel,
             message_id,
             payload,
             origin_node_id,
             created_at
      FROM class_event_log
      WHERE class_id = $1
        AND sequence_number > $2
    `;

    if (channels && channels.size > 0) {
      params.push([...channels]);
      sql += ' AND channel = ANY($3::text[])';
    }

    sql += ' ORDER BY sequence_number ASC';
    const { rows } = await this.query(sql, params);
    return rows.map(mapOperationRow);
  }

  async acceptClientSequence(clientKey, sequenceNumber) {
    return this.withLocks([`client_sequence:${clientKey}`], async () => {
      const { rows } = await this.query(
        `
          SELECT client_key, last_processed_sequence
          FROM client_inbound_sequences
          WHERE client_key = $1
          FOR UPDATE
        `,
        [clientKey],
      );

      const current = Number(rows[0]?.last_processed_sequence ?? 0);
      if (sequenceNumber <= current) {
        return {
          accepted: false,
          lastProcessedSequence: current,
        };
      }

      await this.query(
        `
          INSERT INTO client_inbound_sequences (
            client_key,
            last_processed_sequence,
            updated_at
          )
          VALUES ($1, $2, NOW())
          ON CONFLICT (client_key) DO UPDATE
          SET last_processed_sequence = EXCLUDED.last_processed_sequence,
              updated_at = NOW()
        `,
        [clientKey, sequenceNumber],
      );

      return {
        accepted: true,
        lastProcessedSequence: sequenceNumber,
      };
    });
  }

  async healthCheck() {
    const startedAt = Date.now();
    try {
      await this.query('SELECT 1');
      return {
        component: 'database',
        driver: 'postgres',
        status: 'ready',
        latency_ms: Date.now() - startedAt,
      };
    } catch (error) {
      return {
        component: 'database',
        driver: 'postgres',
        status: 'failed',
        latency_ms: Date.now() - startedAt,
        error: String(error),
      };
    }
  }

  async close() {
    await this.pool.end();
  }
}
