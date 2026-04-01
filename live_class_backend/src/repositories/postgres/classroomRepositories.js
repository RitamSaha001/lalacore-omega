import { createBreakoutRoom } from '../../models/breakoutRoom.js';
import { mapBreakoutRoomRow, mapChatMessageRow, mapClassSessionRow, mapParticipantRow } from './mappers.js';

export class PostgresClassSessionRepository {
  constructor(store) {
    this.store = store;
  }

  async getById(classId) {
    const { rows } = await this.store.query(
      'SELECT * FROM class_sessions WHERE id = $1',
      [classId],
    );
    return mapClassSessionRow(rows[0]);
  }

  async save(session) {
    const { rows } = await this.store.query(
      `
        INSERT INTO class_sessions (
          id,
          title,
          teacher_id,
          teacher_name,
          active_room_id,
          chat_enabled,
          meeting_locked,
          waiting_room_enabled,
          is_recording,
          recording_status,
          created_at,
          active_whiteboard_user_id,
          whiteboard_strokes,
          active_recording,
          version
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15)
        ON CONFLICT (id) DO UPDATE
        SET title = EXCLUDED.title,
            teacher_id = EXCLUDED.teacher_id,
            teacher_name = EXCLUDED.teacher_name,
            active_room_id = EXCLUDED.active_room_id,
            chat_enabled = EXCLUDED.chat_enabled,
            meeting_locked = EXCLUDED.meeting_locked,
            waiting_room_enabled = EXCLUDED.waiting_room_enabled,
            is_recording = EXCLUDED.is_recording,
            recording_status = EXCLUDED.recording_status,
            active_whiteboard_user_id = EXCLUDED.active_whiteboard_user_id,
            whiteboard_strokes = EXCLUDED.whiteboard_strokes,
            active_recording = EXCLUDED.active_recording,
            version = EXCLUDED.version
        RETURNING *
      `,
      [
        session.id,
        session.title,
        session.teacherId,
        session.teacherName,
        session.activeRoomId,
        session.chatEnabled,
        session.meetingLocked,
        session.waitingRoomEnabled,
        session.isRecording,
        session.recordingStatus,
        session.createdAt,
        session.activeWhiteboardUserId,
        JSON.stringify(session.whiteboardStrokes ?? []),
        JSON.stringify(session.activeRecording),
        session.version ?? 1,
      ],
    );
    return mapClassSessionRow(rows[0]);
  }

  async update(classId, updater, { expectedVersion = null } = {}) {
    const current = await this.getById(classId);
    if (!current) {
      return null;
    }
    if (expectedVersion !== null && current.version !== expectedVersion) {
      return {
        conflict: true,
        current,
      };
    }
    const updated = updater(current);
    return this.save({
      ...updated,
      version: current.version + 1,
    });
  }
}

export class PostgresParticipantRepository {
  constructor(store) {
    this.store = store;
  }

  async upsert(participant, { expectedVersion = null } = {}) {
    const current = await this.getByClassAndUser(participant.classId, participant.userId);
    if (expectedVersion !== null && current?.version !== expectedVersion) {
      return {
        conflict: true,
        current,
      };
    }
    const version =
      current && (participant.version ?? 0) <= current.version
        ? current.version + 1
        : participant.version ?? 1;

    const { rows } = await this.store.query(
      `
        INSERT INTO participants (
          id,
          user_id,
          class_id,
          user_name,
          role,
          status,
          breakout_room_id,
          muted,
          camera_disabled,
          whiteboard_access,
          request_id,
          requested_at,
          approved_at,
          rejected_at,
          presence_status,
          last_seen_at,
          disconnected_at,
          disconnect_grace_expires_at,
          version
        )
        VALUES (
          COALESCE($1::uuid, gen_random_uuid()), $2, $3, $4, $5, $6, $7, $8, $9,
          $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        )
        ON CONFLICT (class_id, user_id) DO UPDATE
        SET user_name = EXCLUDED.user_name,
            role = EXCLUDED.role,
            status = EXCLUDED.status,
            breakout_room_id = EXCLUDED.breakout_room_id,
            muted = EXCLUDED.muted,
            camera_disabled = EXCLUDED.camera_disabled,
            whiteboard_access = EXCLUDED.whiteboard_access,
            request_id = EXCLUDED.request_id,
            requested_at = EXCLUDED.requested_at,
            approved_at = EXCLUDED.approved_at,
            rejected_at = EXCLUDED.rejected_at,
            presence_status = EXCLUDED.presence_status,
            last_seen_at = EXCLUDED.last_seen_at,
            disconnected_at = EXCLUDED.disconnected_at,
            disconnect_grace_expires_at = EXCLUDED.disconnect_grace_expires_at,
            version = EXCLUDED.version
        RETURNING *
      `,
      [
        participant.id,
        participant.userId,
        participant.classId,
        participant.userName,
        participant.role,
        participant.status,
        participant.breakoutRoomId,
        participant.muted,
        participant.cameraDisabled,
        participant.whiteboardAccess,
        participant.requestId,
        participant.requestedAt,
        participant.approvedAt,
        participant.rejectedAt,
        participant.presenceStatus,
        participant.lastSeenAt,
        participant.disconnectedAt,
        participant.disconnectGraceExpiresAt,
        version,
      ],
    );
    return mapParticipantRow(rows[0]);
  }

  async getByClassAndUser(classId, userId) {
    const { rows } = await this.store.query(
      'SELECT * FROM participants WHERE class_id = $1 AND user_id = $2',
      [classId, userId],
    );
    return mapParticipantRow(rows[0]);
  }

  async listByClass(classId) {
    const { rows } = await this.store.query(
      'SELECT * FROM participants WHERE class_id = $1 ORDER BY approved_at NULLS LAST, requested_at NULLS LAST, created_at NULLS LAST',
      [classId],
    );
    return rows.map(mapParticipantRow);
  }

  async listWaitingByClass(classId) {
    const { rows } = await this.store.query(
      'SELECT * FROM participants WHERE class_id = $1 AND status = $2 ORDER BY requested_at ASC',
      [classId, 'pending'],
    );
    return rows.map(mapParticipantRow);
  }

  async listExpiredTemporaryDisconnects(graceDeadlineIso, { limit = 100 } = {}) {
    const { rows } = await this.store.query(
      `
        SELECT *
        FROM participants
        WHERE presence_status = 'temporarily_disconnected'
          AND disconnect_grace_expires_at IS NOT NULL
          AND disconnect_grace_expires_at <= $1
        ORDER BY disconnect_grace_expires_at ASC
        LIMIT $2
      `,
      [graceDeadlineIso, limit],
    );
    return rows.map(mapParticipantRow);
  }

  async ensureTeacher({ classId, teacherId, teacherName }) {
    const existing = await this.getByClassAndUser(classId, teacherId);
    if (existing) {
      return existing;
    }
    return this.upsert({
      userId: teacherId,
      classId,
      userName: teacherName,
      role: 'teacher',
      status: 'approved',
      approvedAt: new Date().toISOString(),
      presenceStatus: 'connected',
      lastSeenAt: new Date().toISOString(),
    });
  }

  async createOrRefreshJoinRequest({
    classId,
    userId,
    userName,
    status,
    requestId,
    role = 'student',
  }) {
    const existing = await this.getByClassAndUser(classId, userId);
    return this.upsert({
      ...(existing ?? {
        userId,
        classId,
        userName,
        role,
      }),
      userId,
      classId,
      userName,
      role: existing?.role ?? role,
      status,
      requestId,
      requestedAt: new Date().toISOString(),
      rejectedAt: status === 'rejected' ? new Date().toISOString() : null,
      approvedAt: status === 'approved' ? new Date().toISOString() : null,
      presenceStatus:
        status === 'approved' ? existing?.presenceStatus ?? 'offline' : 'offline',
      disconnectGraceExpiresAt: null,
      disconnectedAt: null,
      muted: existing?.muted ?? false,
      cameraDisabled: existing?.cameraDisabled ?? false,
      whiteboardAccess: existing?.whiteboardAccess ?? false,
      breakoutRoomId: existing?.breakoutRoomId ?? null,
      id: existing?.id,
      version: existing?.version,
    });
  }

  async approve(classId, userId) {
    const current = await this.getByClassAndUser(classId, userId);
    if (!current) {
      return null;
    }
    return this.upsert({
      ...current,
      status: 'approved',
      approvedAt: new Date().toISOString(),
      rejectedAt: null,
    });
  }

  async reject(classId, userId) {
    const current = await this.getByClassAndUser(classId, userId);
    if (!current) {
      return null;
    }
    return this.upsert({
      ...current,
      status: 'rejected',
      rejectedAt: new Date().toISOString(),
      breakoutRoomId: null,
      presenceStatus: 'offline',
      disconnectedAt: null,
      disconnectGraceExpiresAt: null,
    });
  }

  async approveAll(classId) {
    const waiting = await this.listWaitingByClass(classId);
    const approved = [];
    for (const participant of waiting) {
      approved.push(await this.approve(classId, participant.userId));
    }
    return approved;
  }

  async updateState(classId, userId, partialState, { expectedVersion = null } = {}) {
    const current = await this.getByClassAndUser(classId, userId);
    if (!current) {
      return null;
    }
    if (expectedVersion !== null && current.version !== expectedVersion) {
      return {
        conflict: true,
        current,
      };
    }
    const nextState =
      typeof partialState === 'function'
        ? partialState(current)
        : {
            ...current,
            ...partialState,
          };
    return this.upsert({
      ...nextState,
      version: current.version + 1,
    });
  }
}

export class PostgresBreakoutRoomRepository {
  constructor(store) {
    this.store = store;
  }

  async create({ classId, name, livekitRoomName }) {
    const breakout = createBreakoutRoom({
      classId,
      name,
      livekitRoomName: `${livekitRoomName}__${Date.now()}`,
    });
    const { rows } = await this.store.query(
      `
        INSERT INTO breakout_rooms (
          id,
          class_id,
          name,
          livekit_room_name,
          created_at,
          version
        )
        VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, $3, $4, $5, $6)
        RETURNING *
      `,
      [
        breakout.id,
        breakout.classId,
        breakout.name,
        breakout.livekitRoomName,
        breakout.createdAt,
        breakout.version,
      ],
    );
    return mapBreakoutRoomRow(rows[0]);
  }

  async getById(classId, breakoutRoomId) {
    const { rows } = await this.store.query(
      'SELECT * FROM breakout_rooms WHERE class_id = $1 AND id = $2',
      [classId, breakoutRoomId],
    );
    return mapBreakoutRoomRow(rows[0]);
  }

  async listByClass(classId) {
    const { rows } = await this.store.query(
      'SELECT * FROM breakout_rooms WHERE class_id = $1 ORDER BY created_at ASC',
      [classId],
    );
    return rows.map(mapBreakoutRoomRow);
  }
}

export class PostgresChatMessageRepository {
  constructor(store) {
    this.store = store;
  }

  async create({
    classId,
    senderId,
    senderName,
    message,
    attachment = null,
    dedupeKey = null,
  }) {
    if (dedupeKey) {
      const existing = await this.getByDedupeKey(classId, dedupeKey);
      if (existing) {
        return existing;
      }
    }
    const { rows } = await this.store.query(
      `
        INSERT INTO chat_messages (
          id,
          class_id,
          sender_id,
          sender_name,
          message,
          attachment,
          dedupe_key,
          timestamp
        )
        VALUES (gen_random_uuid(), $1, $2, $3, $4, $5::jsonb, $6, NOW())
        RETURNING *
      `,
      [
        classId,
        senderId,
        senderName,
        message,
        attachment ? JSON.stringify(attachment) : null,
        dedupeKey,
      ],
    );
    return mapChatMessageRow(rows[0]);
  }

  async getByDedupeKey(classId, dedupeKey) {
    if (!dedupeKey) {
      return null;
    }
    const { rows } = await this.store.query(
      'SELECT * FROM chat_messages WHERE class_id = $1 AND dedupe_key = $2',
      [classId, dedupeKey],
    );
    return mapChatMessageRow(rows[0]);
  }

  async listByClass(classId) {
    const { rows } = await this.store.query(
      `
        SELECT *
        FROM chat_messages
        WHERE class_id = $1
        ORDER BY timestamp DESC
        LIMIT 500
      `,
      [classId],
    );
    return rows.reverse().map(mapChatMessageRow);
  }
}
