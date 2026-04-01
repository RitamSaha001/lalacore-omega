import { createClassSession } from '../models/classSession.js';
import { buildMainRoomName } from '../livekit/livekitClient.js';
import { HttpError } from '../utils/httpError.js';

export class ClassroomAuthorityService {
  constructor({
    config,
    database,
    classSessionRepository,
    participantRepository,
    breakoutRoomRepository,
    classroomHub,
    logger,
  }) {
    this.config = config;
    this.database = database;
    this.classSessionRepository = classSessionRepository;
    this.participantRepository = participantRepository;
    this.breakoutRoomRepository = breakoutRoomRepository;
    this.classroomHub = classroomHub;
    this.logger = logger;
  }

  async ensureSession(classId) {
    let session = await this.classSessionRepository.getById(classId);
    if (!session) {
      session = await this.classSessionRepository.save(
        createClassSession({
          id: classId,
          title: this.config.defaultClassTitle,
          teacherId: this.config.defaultTeacherId,
          teacherName: this.config.defaultTeacherName,
          activeRoomId: buildMainRoomName(classId),
          waitingRoomEnabled: this.config.defaultWaitingRoomEnabled,
        }),
      );
    }
    await this.participantRepository.ensureTeacher({
      classId: session.id,
      teacherId: session.teacherId,
      teacherName: session.teacherName,
    });
    return session;
  }

  async getSession(classId) {
    return this.ensureSession(classId);
  }

  async getWaitingRoomSnapshot(classId) {
    const waiting = await this.participantRepository.listWaitingByClass(classId);
    return waiting.map((participant) => ({
      user_id: participant.userId,
      user_name: participant.userName,
      requested_at: participant.requestedAt,
      request_id: participant.requestId,
      presence_status: participant.presenceStatus,
    }));
  }

  async fetchClassroomState(classId, userId = '') {
    const session = await this.ensureSession(classId);
    const participant = userId
      ? await this.participantRepository.getByClassAndUser(classId, userId)
      : null;
    const isTeacher = session.teacherId === userId;
    const participants = await this.participantRepository.listByClass(classId);
    const breakoutRooms = await this.breakoutRoomRepository.listByClass(classId);

    return {
      type: 'class_state_snapshot',
      class_id: classId,
      user_id: userId || null,
      server_sequence: await this.database.getLastSequence(classId),
      session: {
        id: session.id,
        title: session.title,
        teacher_id: session.teacherId,
        teacher_name: session.teacherName,
        active_room_id: session.activeRoomId,
        chat_enabled: session.chatEnabled,
        meeting_locked: session.meetingLocked,
        waiting_room_enabled: session.waitingRoomEnabled,
        is_recording: session.isRecording,
        recording_status: session.recordingStatus,
        version: session.version,
      },
      participants,
      breakout_rooms: breakoutRooms,
      waiting_room_requests: await this.getWaitingRoomSnapshot(classId),
      reconnect_policy: {
        grace_period_ms: this.config.reconnectGracePeriodMs,
        backoff: this.config.reconnectBackoff,
      },
      active_breakout_room_id: participant?.breakoutRoomId ?? null,
      active_whiteboard_user_id: session.activeWhiteboardUserId,
      whiteboard_access: isTeacher || participant?.whiteboardAccess === true,
      whiteboard_strokes: session.whiteboardStrokes,
      muted: participant?.muted ?? false,
      camera_disabled: participant?.cameraDisabled ?? false,
      meeting_locked: session.meetingLocked,
      chat_enabled: session.chatEnabled,
      waiting_room_enabled: session.waitingRoomEnabled,
      is_recording: session.isRecording,
    };
  }

  async requestJoin({ classId, userId, userName }) {
    await this.ensureSession(classId);

    let outcome = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`],
      async () => {
        const session = await this.ensureSession(classId);
        const isTeacher = session.teacherId === userId;
        const existing = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );

        if (session.meetingLocked && !isTeacher) {
          const rejected = await this.participantRepository.createOrRefreshJoinRequest({
            classId,
            userId,
            userName,
            status: 'rejected',
            requestId: existing?.requestId ?? `req_${Date.now()}_${userId}`,
            role: 'student',
          });
          outcome = {
            requestId: rejected.requestId,
            status: 'rejected',
            participant: rejected,
            changed:
              existing?.status !== 'rejected' ||
              existing?.userName !== userName ||
              existing?.requestId !== rejected.requestId,
          };
          return;
        }

        const requestedStatus =
          isTeacher || !session.waitingRoomEnabled ? 'approved' : 'pending';
        const finalStatus =
          existing?.status === 'approved' ? 'approved' : requestedStatus;

        if (
          existing &&
          existing.status === finalStatus &&
          existing.userName === userName
        ) {
          outcome = {
            requestId: existing.requestId,
            status: existing.status,
            participant: existing,
            changed: false,
          };
          return;
        }

        const participant =
          await this.participantRepository.createOrRefreshJoinRequest({
            classId,
            userId,
            userName,
            status: finalStatus,
            requestId: existing?.requestId ?? `req_${Date.now()}_${userId}`,
            role: isTeacher ? 'teacher' : 'student',
          });
        outcome = {
          requestId: participant.requestId,
          status: participant.status,
          participant,
          changed: true,
        };
      },
    );

    this.logger.info('join_request_processed', {
      classId,
      userId,
      status: outcome.status,
      changed: outcome.changed,
    });

    if (outcome.changed && outcome.status === 'pending') {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_request_received',
        class_id: classId,
        user_id: outcome.participant.userId,
        user_name: outcome.participant.userName,
        request_id: outcome.requestId,
        requested_at: outcome.participant.requestedAt,
      });
      await this.broadcastWaitingRoomSnapshot(classId);
    }

    if (outcome.changed && outcome.status === 'approved') {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_approved',
        class_id: classId,
        user_id: outcome.participant.userId,
        user_name: outcome.participant.userName,
        request_id: outcome.requestId,
      });
      await this.broadcastParticipantUpdate(classId, outcome.participant);
      await this.broadcastWaitingRoomSnapshot(classId);
    }

    if (outcome.status === 'rejected') {
      if (outcome.changed) {
        await this.classroomHub.broadcastEvent(classId, {
          type: 'join_rejected',
          class_id: classId,
          user_id: outcome.participant.userId,
          user_name: outcome.participant.userName,
          request_id: outcome.requestId,
          message: 'Meeting is locked',
        });
        await this.broadcastWaitingRoomSnapshot(classId);
      }
      throw new HttpError(403, 'Meeting is locked', {
        request_id: outcome.requestId,
      });
    }

    return {
      requestId: outcome.requestId,
      status: outcome.status,
    };
  }

  async cancelJoin({ classId, userId }) {
    await this.ensureSession(classId);

    let outcome = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`],
      async () => {
        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!participant) {
          outcome = {
            participant: null,
            applied: false,
          };
          return;
        }
        if (participant.status !== 'pending') {
          outcome = {
            participant,
            applied: false,
          };
          return;
        }
        outcome = {
          participant: await this.participantRepository.updateState(
            classId,
            userId,
            {
              status: 'canceled',
              breakoutRoomId: null,
              presenceStatus: 'offline',
              disconnectedAt: null,
              disconnectGraceExpiresAt: null,
            },
          ),
          applied: true,
        };
      },
    );

    if (outcome.applied && outcome.participant) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_request_removed',
        class_id: classId,
        user_id: outcome.participant.userId,
      });
      await this.broadcastWaitingRoomSnapshot(classId);
    }

    return {
      participant: outcome.participant,
      applied: outcome.applied,
    };
  }

  async approveJoin({ classId, userId }) {
    await this.ensureSession(classId);

    let outcome = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`],
      async () => {
        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!participant) {
          throw new HttpError(404, 'Pending participant not found');
        }
        if (participant.status === 'approved') {
          outcome = {
            participant,
            applied: false,
          };
          return;
        }

        outcome = {
          participant: await this.participantRepository.approve(classId, userId),
          applied: true,
        };
      },
    );

    if (outcome.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_approved',
        class_id: classId,
        user_id: outcome.participant.userId,
        user_name: outcome.participant.userName,
        request_id: outcome.participant.requestId,
      });
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_request_removed',
        class_id: classId,
        user_id: outcome.participant.userId,
      });
      await this.broadcastWaitingRoomSnapshot(classId);
      await this.broadcastParticipantUpdate(classId, outcome.participant);
    } else {
      this.logger.info('approve_join_duplicate_ignored', {
        classId,
        userId,
      });
    }

    return {
      ...outcome.participant,
      applied: outcome.applied,
    };
  }

  async rejectJoin({ classId, userId, reason = null }) {
    await this.ensureSession(classId);

    let outcome = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`],
      async () => {
        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!participant) {
          throw new HttpError(404, 'Pending participant not found');
        }
        if (participant.status === 'rejected') {
          outcome = {
            participant,
            applied: false,
          };
          return;
        }
        outcome = {
          participant: await this.participantRepository.reject(classId, userId),
          applied: true,
        };
      },
    );

    if (outcome.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_rejected',
        class_id: classId,
        user_id: outcome.participant.userId,
        user_name: outcome.participant.userName,
        request_id: outcome.participant.requestId,
        message: reason,
      });
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_request_removed',
        class_id: classId,
        user_id: outcome.participant.userId,
      });
      await this.broadcastWaitingRoomSnapshot(classId);
      await this.broadcastParticipantUpdate(classId, outcome.participant);
    } else {
      this.logger.info('reject_join_duplicate_ignored', {
        classId,
        userId,
      });
    }

    return {
      ...outcome.participant,
      applied: outcome.applied,
    };
  }

  async approveAll(classId) {
    await this.ensureSession(classId);

    let approved = [];
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`],
      async () => {
        approved = await this.participantRepository.approveAll(classId);
      },
    );

    for (const participant of approved) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_approved',
        class_id: classId,
        user_id: participant.userId,
        user_name: participant.userName,
        request_id: participant.requestId,
      });
      await this.broadcastParticipantUpdate(classId, participant);
    }
    await this.broadcastWaitingRoomSnapshot(classId);
    return approved;
  }

  async setMeetingLocked(classId, locked) {
    await this.ensureSession(classId);
    let result = null;
    await this.database.withLocks([`session:${classId}`], async () => {
      const current = await this.classSessionRepository.getById(classId);
      if (!current) {
        throw new HttpError(404, 'Class session not found');
      }
      if (current.meetingLocked === locked) {
        result = {
          session: current,
          applied: false,
        };
        return;
      }
      result = {
        session: await this.classSessionRepository.update(classId, (session) => ({
          ...session,
          meetingLocked: locked,
        })),
        applied: true,
      };
    });

    if (result.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'meeting_locked',
        class_id: classId,
        locked,
      });
    }

    return {
      ...result.session,
      applied: result.applied,
    };
  }

  async setChatEnabled(classId, enabled) {
    await this.ensureSession(classId);
    let result = null;
    await this.database.withLocks([`session:${classId}`], async () => {
      const current = await this.classSessionRepository.getById(classId);
      if (!current) {
        throw new HttpError(404, 'Class session not found');
      }
      if (current.chatEnabled === enabled) {
        result = {
          session: current,
          applied: false,
        };
        return;
      }
      result = {
        session: await this.classSessionRepository.update(classId, (session) => ({
          ...session,
          chatEnabled: enabled,
        })),
        applied: true,
      };
    });

    if (result.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'chat_enabled_changed',
        class_id: classId,
        enabled,
      });
      await this.classroomHub.broadcastChat(classId, {
        type: 'chat_enabled_changed',
        class_id: classId,
        enabled,
      });
    }

    return {
      ...result.session,
      applied: result.applied,
    };
  }

  async setWaitingRoomEnabled(classId, enabled) {
    await this.ensureSession(classId);
    let result = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`],
      async () => {
        const current = await this.classSessionRepository.getById(classId);
        if (!current) {
          throw new HttpError(404, 'Class session not found');
        }
        const session =
          current.waitingRoomEnabled === enabled
            ? current
            : await this.classSessionRepository.update(classId, (existing) => ({
                ...existing,
                waitingRoomEnabled: enabled,
              }));

        const approvedParticipants = enabled
          ? []
          : await this.participantRepository.approveAll(classId);

        result = {
          session,
          applied: current.waitingRoomEnabled !== enabled,
          approvedParticipants,
        };
      },
    );

    for (const participant of result.approvedParticipants) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'join_approved',
        class_id: classId,
        user_id: participant.userId,
        user_name: participant.userName,
        request_id: participant.requestId,
      });
      await this.broadcastParticipantUpdate(classId, participant);
    }
    await this.broadcastWaitingRoomSnapshot(classId);
    if (result.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'waiting_room_enabled_changed',
        class_id: classId,
        enabled,
      });
    }

    return {
      ...result.session,
      applied: result.applied,
      approved_count: result.approvedParticipants.length,
    };
  }

  async setRecordingEnabled(classId, enabled) {
    await this.ensureSession(classId);
    let result = null;
    await this.database.withLocks([`session:${classId}`], async () => {
      const current = await this.classSessionRepository.getById(classId);
      if (!current) {
        throw new HttpError(404, 'Class session not found');
      }
      if (current.isRecording === enabled) {
        result = {
          session: current,
          applied: false,
        };
        return;
      }
      result = {
        session: await this.classSessionRepository.update(classId, (session) => ({
          ...session,
          isRecording: enabled,
        })),
        applied: true,
      };
    });

    if (result.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'recording_state_changed',
        class_id: classId,
        enabled,
      });
    }

    return {
      ...result.session,
      applied: result.applied,
    };
  }

  async updateParticipantModeration(classId, userId, partialState) {
    await this.ensureSession(classId);
    let result = null;
    await this.database.withLocks([`participants:${classId}`], async () => {
      const current = await this.participantRepository.getByClassAndUser(
        classId,
        userId,
      );
      if (!current) {
        throw new HttpError(404, 'Participant not found');
      }
      const nextState = {
        ...partialState,
      };
      if (partialState.status === 'removed') {
        nextState.breakoutRoomId = null;
        nextState.presenceStatus = 'left';
      }

      const changed = Object.entries(nextState).some(
        ([key, value]) => current[key] !== value,
      );
      result = {
        participant: changed
          ? await this.participantRepository.updateState(classId, userId, nextState)
          : current,
        applied: changed,
      };
    });

    if (result.applied) {
      await this.broadcastParticipantUpdate(classId, result.participant);
    }

    return {
      ...result.participant,
      applied: result.applied,
    };
  }

  async setWhiteboardAccess(classId, userId, enabled) {
    await this.ensureSession(classId);
    let result = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`],
      async () => {
        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!participant) {
          throw new HttpError(404, 'Participant not found');
        }

        const nextParticipant =
          participant.whiteboardAccess === enabled
            ? participant
            : await this.participantRepository.updateState(classId, userId, {
                whiteboardAccess: enabled,
              });
        const session = await this.classSessionRepository.update(
          classId,
          (current) => ({
            ...current,
            activeWhiteboardUserId: enabled ? userId : null,
          }),
        );
        result = {
          participant: nextParticipant,
          session,
          applied: participant.whiteboardAccess !== enabled,
        };
      },
    );

    if (result.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: enabled ? 'whiteboard_granted' : 'whiteboard_revoked',
        class_id: classId,
        user_id: userId,
      });
      await this.broadcastParticipantUpdate(classId, result.participant);
    }

    return result;
  }

  async broadcastWaitingRoomSnapshot(classId) {
    await this.classroomHub.broadcastEvent(classId, {
      type: 'waiting_room_snapshot',
      class_id: classId,
      requests: await this.getWaitingRoomSnapshot(classId),
    });
  }

  async broadcastParticipantUpdate(classId, participant) {
    await this.classroomHub.broadcastEvent(classId, {
      type: 'participant_updated',
      class_id: classId,
      participant,
    });
  }
}
