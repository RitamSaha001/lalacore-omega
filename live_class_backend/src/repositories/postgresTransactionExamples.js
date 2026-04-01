import { HttpError } from '../utils/httpError.js';

async function withTransaction(client, operation) {
  await client.query('BEGIN');
  try {
    const result = await operation();
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}

export async function beginIdempotentRequest({
  client,
  scope,
  key,
  fingerprint,
}) {
  return withTransaction(client, async () => {
    await client.query(
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
        ON CONFLICT (scope, idempotency_key) DO NOTHING
      `,
      [scope, key, fingerprint],
    );

    const { rows } = await client.query(
      `
        SELECT scope,
               idempotency_key,
               fingerprint,
               status,
               response_status_code,
               response_headers,
               response_body
        FROM idempotency_keys
        WHERE scope = $1
          AND idempotency_key = $2
        FOR UPDATE
      `,
      [scope, key],
    );

    const record = rows[0];
    if (!record) {
      throw new HttpError(500, 'Failed to create idempotency record');
    }

    if (record.fingerprint !== fingerprint) {
      throw new HttpError(
        409,
        'Idempotency-Key has already been used for a different request payload',
      );
    }

    if (record.status === 'completed') {
      return {
        type: 'replay',
        response: {
          statusCode: record.response_status_code,
          headers: record.response_headers,
          body: record.response_body,
        },
      };
    }

    return {
      type: 'started',
    };
  });
}

export async function completeIdempotentRequest({
  client,
  scope,
  key,
  statusCode,
  headers,
  body,
}) {
  return withTransaction(client, async () => {
    await client.query(
      `
        UPDATE idempotency_keys
        SET status = 'completed',
            response_status_code = $3,
            response_headers = $4::jsonb,
            response_body = $5::jsonb,
            updated_at = NOW()
        WHERE scope = $1
          AND idempotency_key = $2
      `,
      [scope, key, statusCode, JSON.stringify(headers), JSON.stringify(body)],
    );
  });
}

export async function approveJoinTransaction({
  client,
  classId,
  userId,
  actorUserId,
}) {
  return withTransaction(client, async () => {
    const sessionResult = await client.query(
      `
        SELECT id, teacher_id
        FROM class_sessions
        WHERE id = $1
        FOR UPDATE
      `,
      [classId],
    );
    const session = sessionResult.rows[0];
    if (!session) {
      throw new HttpError(404, 'Class session not found');
    }
    if (session.teacher_id !== actorUserId) {
      throw new HttpError(403, 'Only the teacher can approve join requests');
    }

    const participantResult = await client.query(
      `
        SELECT id, user_id, user_name, status
        FROM participants
        WHERE class_id = $1
          AND user_id = $2
        FOR UPDATE
      `,
      [classId, userId],
    );
    const participant = participantResult.rows[0];
    if (!participant) {
      throw new HttpError(404, 'Pending participant not found');
    }

    if (participant.status === 'approved') {
      return {
        applied: false,
        participant,
      };
    }

    const updatedResult = await client.query(
      `
        UPDATE participants
        SET status = 'approved',
            approved_at = NOW(),
            rejected_at = NULL,
            version = version + 1
        WHERE id = $1
        RETURNING id,
                  user_id,
                  user_name,
                  status,
                  approved_at,
                  version
      `,
      [participant.id],
    );

    return {
      applied: true,
      participant: updatedResult.rows[0],
    };
  });
}

export async function assignBreakoutTransaction({
  client,
  classId,
  userId,
  breakoutRoomId,
}) {
  return withTransaction(client, async () => {
    const breakoutResult = await client.query(
      `
        SELECT id
        FROM breakout_rooms
        WHERE class_id = $1
          AND id = $2
        FOR UPDATE
      `,
      [classId, breakoutRoomId],
    );
    if (breakoutResult.rowCount === 0) {
      throw new HttpError(404, 'Breakout room not found');
    }

    const participantResult = await client.query(
      `
        SELECT id, breakout_room_id, status
        FROM participants
        WHERE class_id = $1
          AND user_id = $2
        FOR UPDATE
      `,
      [classId, userId],
    );
    const participant = participantResult.rows[0];
    if (!participant || participant.status !== 'approved') {
      throw new HttpError(404, 'Approved participant not found');
    }

    if (participant.breakout_room_id === breakoutRoomId) {
      return {
        applied: false,
        participant,
      };
    }

    const updatedResult = await client.query(
      `
        UPDATE participants
        SET breakout_room_id = $3,
            version = version + 1
        WHERE class_id = $1
          AND user_id = $2
        RETURNING id,
                  user_id,
                  breakout_room_id,
                  version
      `,
      [classId, userId, breakoutRoomId],
    );

    return {
      applied: true,
      participant: updatedResult.rows[0],
    };
  });
}
