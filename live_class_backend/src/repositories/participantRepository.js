import { createParticipant } from '../models/participant.js';

export class ParticipantRepository {
  constructor(db) {
    this.db = db;
  }

  async upsert(participant, { expectedVersion = null } = {}) {
    const bucket = this.db.ensureParticipants(participant.classId);
    const current = bucket.get(participant.userId) ?? null;
    if (expectedVersion !== null && current?.version !== expectedVersion) {
      return {
        conflict: true,
        current,
      };
    }
    const next = {
      ...participant,
      version:
        current && (participant.version ?? 0) <= current.version
          ? current.version + 1
          : participant.version ?? 1,
    };
    bucket.set(next.userId, next);
    return next;
  }

  async getByClassAndUser(classId, userId) {
    return this.db.ensureParticipants(classId).get(userId) ?? null;
  }

  async listByClass(classId) {
    return Array.from(this.db.ensureParticipants(classId).values());
  }

  async listWaitingByClass(classId) {
    return (await this.listByClass(classId)).filter(
      (participant) => participant.status === 'pending',
    );
  }

  async listExpiredTemporaryDisconnects(graceDeadlineIso, { limit = 100 } = {}) {
    const expired = [];
    for (const bucket of this.db.participantsByClass.values()) {
      for (const participant of bucket.values()) {
        if (
          participant?.presenceStatus === 'temporarily_disconnected' &&
          participant.disconnectGraceExpiresAt &&
          participant.disconnectGraceExpiresAt <= graceDeadlineIso
        ) {
          expired.push(participant);
          if (expired.length >= limit) {
            return expired;
          }
        }
      }
    }
    return expired;
  }

  async ensureTeacher({ classId, teacherId, teacherName }) {
    const existing = await this.getByClassAndUser(classId, teacherId);
    if (existing) {
      return existing;
    }
    return this.upsert(
      createParticipant({
        userId: teacherId,
        classId,
        userName: teacherName,
        role: 'teacher',
        status: 'approved',
        approvedAt: new Date().toISOString(),
        presenceStatus: 'connected',
        lastSeenAt: new Date().toISOString(),
      }),
    );
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
    const next = {
      ...(existing ??
          createParticipant({
            userId,
            classId,
            userName,
            role,
          })),
      userName,
      status,
      requestId,
      requestedAt: new Date().toISOString(),
      rejectedAt: status === 'rejected' ? new Date().toISOString() : null,
      approvedAt: status === 'approved' ? new Date().toISOString() : null,
      presenceStatus:
        status === 'approved'
          ? existing?.presenceStatus ?? 'offline'
          : 'offline',
      disconnectGraceExpiresAt: null,
      disconnectedAt: null,
    };
    return this.upsert(next);
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
    return Promise.all(
      waiting.map((participant) => this.approve(classId, participant.userId)),
    );
  }

  async updateState(
    classId,
    userId,
    partialState,
    { expectedVersion = null } = {},
  ) {
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
