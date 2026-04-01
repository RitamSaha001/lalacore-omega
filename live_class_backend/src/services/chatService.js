import { HttpError } from '../utils/httpError.js';

export class ChatService {
  constructor({
    database,
    classSessionRepository,
    participantRepository,
    chatMessageRepository,
    classroomHub,
    logger,
    metricsCollector = null,
  }) {
    this.database = database;
    this.classSessionRepository = classSessionRepository;
    this.participantRepository = participantRepository;
    this.chatMessageRepository = chatMessageRepository;
    this.classroomHub = classroomHub;
    this.logger = logger;
    this.metricsCollector = metricsCollector;
  }

  async sendMessage({
    classId,
    senderId,
    senderName,
    message,
    attachment = null,
    dedupeKey = null,
  }) {
    let outcome = null;
    await this.database.withLocks(
      [`session:${classId}`, `participants:${classId}`, `chat:${classId}`],
      async () => {
        const session = await this.classSessionRepository.getById(classId);
        if (!session) {
          throw new HttpError(404, 'Class session not found');
        }

        const participant = await this.participantRepository.getByClassAndUser(
          classId,
          senderId,
        );
        const isTeacher = session.teacherId === senderId;

        if (!isTeacher && participant?.status !== 'approved') {
          throw new HttpError(403, 'Only approved participants can send chat');
        }

        if (participant?.presenceStatus === 'left') {
          throw new HttpError(409, 'Participant session expired');
        }

        if (!session.chatEnabled && !isTeacher) {
          throw new HttpError(403, 'Chat is disabled');
        }

        const existing = await this.chatMessageRepository.getByDedupeKey(
          classId,
          dedupeKey,
        );
        if (existing) {
          outcome = {
            message: existing,
            applied: false,
          };
          return;
        }

        outcome = {
          message: await this.chatMessageRepository.create({
            classId,
            senderId,
            senderName,
            message,
            attachment,
            dedupeKey,
          }),
          applied: true,
        };
      },
    );

    if (outcome.applied) {
      await this.classroomHub.broadcastChat(classId, {
        type: 'chat_message',
        class_id: classId,
        message: outcome.message,
      });
      this.metricsCollector?.increment('chat_messages_total', {
        classId,
      });
      this.metricsCollector?.observe(
        'chat_delivery_lag_ms',
        Math.max(0, Date.now() - Date.parse(outcome.message.timestamp)),
        { classId },
      );
    } else {
      this.logger.info('duplicate_chat_message_ignored', {
        classId,
        senderId,
        dedupeKey,
      });
      this.metricsCollector?.increment('chat_duplicate_messages_total', {
        classId,
      });
    }

    return outcome.message;
  }

  async getSnapshot(classId) {
    const session = await this.classSessionRepository.getById(classId);
    return {
      classId,
      chatEnabled: session?.chatEnabled ?? true,
      messages: await this.chatMessageRepository.listByClass(classId),
    };
  }
}
