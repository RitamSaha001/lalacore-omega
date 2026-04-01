import {
  buildBreakoutRoomName,
  buildMainRoomName,
} from '../livekit/livekitClient.js';
import { HttpError } from '../utils/httpError.js';

export class BreakoutService {
  constructor({
    database,
    classSessionRepository,
    participantRepository,
    breakoutRoomRepository,
    liveTokenService,
    classroomHub,
    logger,
  }) {
    this.database = database;
    this.classSessionRepository = classSessionRepository;
    this.participantRepository = participantRepository;
    this.breakoutRoomRepository = breakoutRoomRepository;
    this.liveTokenService = liveTokenService;
    this.classroomHub = classroomHub;
    this.logger = logger;
  }

  async createRoom({ classId, name }) {
    let room = null;
    await this.database.withLocks(
      [`session:${classId}`, `breakouts:${classId}`],
      async () => {
        const session = await this.classSessionRepository.getById(classId);
        if (!session) {
          throw new HttpError(404, 'Class session not found');
        }

        const slug = name
          .trim()
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '-')
          .replace(/^-+|-+$/g, '');
        room = await this.breakoutRoomRepository.create({
          classId,
          name,
          livekitRoomName: buildBreakoutRoomName(classId, slug || 'room'),
        });
      },
    );

    await this.classroomHub.broadcastEvent(classId, {
      type: 'breakout_room_created',
      class_id: classId,
      breakout_room: room,
    });

    return room;
  }

  async assignParticipant({ classId, userId, breakoutRoomId }) {
    let outcome = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`, `breakouts:${classId}`],
      async () => {
        const breakoutRoom = await this.breakoutRoomRepository.getById(
          classId,
          breakoutRoomId,
        );
        if (!breakoutRoom) {
          throw new HttpError(404, 'Breakout room not found');
        }

        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!participant || participant.status !== 'approved') {
          throw new HttpError(404, 'Approved participant not found');
        }

        if (participant.breakoutRoomId === breakoutRoomId) {
          outcome = {
            participant,
            applied: false,
          };
          return;
        }

        outcome = {
          participant: await this.participantRepository.updateState(classId, userId, {
            breakoutRoomId,
          }),
          applied: true,
        };
      },
    );

    if (outcome.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'breakout_assignment_changed',
        class_id: classId,
        user_id: userId,
        room_id: breakoutRoomId,
      });
      await this.classroomHub.broadcastEvent(classId, {
        type: 'participant_breakout_reconnect_required',
        class_id: classId,
        user_id: userId,
        room_id: breakoutRoomId,
      });
    } else {
      this.logger.info('breakout_assignment_duplicate_ignored', {
        classId,
        userId,
        breakoutRoomId,
      });
    }

    return {
      ...outcome.participant,
      applied: outcome.applied,
    };
  }

  async joinAssignedRoom({ classId, userId, userName }) {
    return this.liveTokenService.issueAuthorizedToken({
      classId,
      userId,
      userName,
    });
  }

  async leaveBreakout({ classId, userId, userName }) {
    let outcome = null;
    await this.database.withLocks(
      [`participants:${classId}`, `breakouts:${classId}`],
      async () => {
        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );
        if (!participant) {
          throw new HttpError(404, 'Participant not found');
        }

        if (!participant.breakoutRoomId) {
          outcome = {
            participant,
            applied: false,
          };
          return;
        }

        outcome = {
          participant: await this.participantRepository.updateState(classId, userId, {
            breakoutRoomId: null,
          }),
          applied: true,
        };
      },
    );

    if (outcome.applied) {
      await this.classroomHub.broadcastEvent(classId, {
        type: 'breakout_assignment_changed',
        class_id: classId,
        user_id: userId,
        room_id: null,
        main_room_id: buildMainRoomName(classId),
      });
      await this.classroomHub.broadcastEvent(classId, {
        type: 'participant_breakout_reconnect_required',
        class_id: classId,
        user_id: userId,
        room_id: null,
      });
    }

    return this.liveTokenService.issueAuthorizedToken({
      classId,
      userId,
      userName,
    });
  }

  async broadcastMessage({ classId, message }) {
    await this.classroomHub.broadcastEvent(classId, {
      type: 'breakout_broadcast',
      class_id: classId,
      message,
    });
    return {
      classId,
      message,
      delivered: true,
    };
  }
}
