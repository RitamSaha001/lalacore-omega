import {
  buildBreakoutRoomName,
  buildMainRoomName,
} from '../livekit/livekitClient.js';
import { HttpError } from '../utils/httpError.js';

export class LiveTokenService {
  constructor({
    database,
    classSessionRepository,
    participantRepository,
    breakoutRoomRepository,
    livekitClient,
    logger,
  }) {
    this.database = database;
    this.classSessionRepository = classSessionRepository;
    this.participantRepository = participantRepository;
    this.breakoutRoomRepository = breakoutRoomRepository;
    this.livekitClient = livekitClient;
    this.logger = logger;
  }

  async issueAuthorizedToken({ classId, userId, userName }) {
    let accessContext = null;

    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`, `breakouts:${classId}`],
      async () => {
        const session = await this.classSessionRepository.getById(classId);
        if (!session) {
          throw new HttpError(404, 'Class session not found');
        }

        const isTeacher = session.teacherId === userId;
        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          userId,
        );

        if (!isTeacher && !participant) {
          throw new HttpError(
            403,
            'Join denied. Request approval before asking for a media token.',
          );
        }

        if (!isTeacher && session.meetingLocked) {
          throw new HttpError(403, 'Meeting is locked');
        }

        // Token issuance is the hard gate. No approval means no provider token.
        if (
          !isTeacher &&
          session.waitingRoomEnabled &&
          participant?.status !== 'approved'
        ) {
          throw new HttpError(403, 'Waiting-room approval required');
        }

        if (!isTeacher && participant?.status === 'rejected') {
          throw new HttpError(403, 'Join request rejected');
        }

        if (!isTeacher && participant?.presenceStatus === 'left') {
          throw new HttpError(409, 'Participant session expired');
        }

        let roomName = buildMainRoomName(classId);
        let roomId = roomName;

        if (!isTeacher && participant?.breakoutRoomId) {
          const breakoutRoom = await this.breakoutRoomRepository.getById(
            classId,
            participant.breakoutRoomId,
          );
          if (!breakoutRoom) {
            throw new HttpError(409, 'Assigned breakout room no longer exists');
          }
          roomName =
            breakoutRoom.livekitRoomName ??
            buildBreakoutRoomName(classId, breakoutRoom.id);
          roomId = breakoutRoom.id;
        }

        accessContext = {
          roomName,
          roomId,
          session,
          participant,
          isTeacher,
        };
      },
    );

    const granted = await this.livekitClient.issueRoomToken({
      roomName: accessContext.roomName,
      userId,
      userName,
      metadata: {
        classId,
        roomId: accessContext.roomId,
        role:
          accessContext.isTeacher
            ? 'teacher'
            : accessContext.participant?.role ?? 'student',
        breakoutRoomId: accessContext.participant?.breakoutRoomId ?? null,
        sessionVersion: accessContext.session.version,
        participantVersion: accessContext.participant?.version ?? null,
      },
    });

    this.logger.info('live_token_issued', {
      classId,
      userId,
      roomId: accessContext.roomId,
    });

    return {
      sessionId: classId,
      roomId: accessContext.roomId,
      token: granted.token,
      provider: 'livekit',
      serverUrl: granted.wsUrl,
      expiresAtEpochSeconds: null,
    };
  }
}
