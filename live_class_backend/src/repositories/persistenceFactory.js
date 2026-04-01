import { hashPassword } from '../utils/passwords.js';
import { BreakoutRoomRepository } from './breakoutRoomRepository.js';
import { ChatMessageRepository } from './chatMessageRepository.js';
import { ClassSessionRepository } from './classSessionRepository.js';
import { IdempotencyRepository } from './idempotencyRepository.js';
import { InMemoryDatabase } from './inMemoryDatabase.js';
import { ParticipantRepository } from './participantRepository.js';
import { RecordingJobRepository } from './recordingJobRepository.js';
import { RefreshTokenRepository } from './refreshTokenRepository.js';
import { UserRepository } from './userRepository.js';
import { PostgresUserRepository, PostgresRefreshTokenRepository } from './postgres/authRepositories.js';
import { PostgresBreakoutRoomRepository, PostgresChatMessageRepository, PostgresClassSessionRepository, PostgresParticipantRepository } from './postgres/classroomRepositories.js';
import { PostgresStore } from './postgres/postgresStore.js';
import { PostgresIdempotencyRepository } from './postgres/postgresIdempotencyRepository.js';
import { PostgresRecordingJobRepository } from './postgres/recordingRepositories.js';

export async function createPersistenceLayer({ config, logger }) {
  if (config.storage.driver === 'postgres') {
    if (!config.database.url) {
      throw new Error('DATABASE_URL must be configured when STORAGE_DRIVER=postgres');
    }

    const database = new PostgresStore({
      config,
      logger: logger.child ? logger.child('postgres') : logger,
    });

    await seedUsers({
      config,
      userRepository: new PostgresUserRepository(database),
    });

    return {
      database,
      repositories: {
        classSessionRepository: new PostgresClassSessionRepository(database),
        participantRepository: new PostgresParticipantRepository(database),
        breakoutRoomRepository: new PostgresBreakoutRoomRepository(database),
        chatMessageRepository: new PostgresChatMessageRepository(database),
        idempotencyRepository: new PostgresIdempotencyRepository(database, {
          waitTimeoutMs: config.idempotencyWaitTimeoutMs,
          logger,
        }),
        userRepository: new PostgresUserRepository(database),
        refreshTokenRepository: new PostgresRefreshTokenRepository(database),
        recordingJobRepository: new PostgresRecordingJobRepository(database),
      },
    };
  }

  if (!config.runtime.allowInMemoryStorage) {
    throw new Error(
      'In-memory storage is disabled for this runtime. Set STORAGE_DRIVER=postgres or explicitly opt in with ALLOW_IN_MEMORY_STORAGE=true.',
    );
  }

  const database = new InMemoryDatabase();
  const userRepository = new UserRepository(database);
  await seedUsers({ config, userRepository });

  return {
    database,
    repositories: {
      classSessionRepository: new ClassSessionRepository(database),
      participantRepository: new ParticipantRepository(database),
      breakoutRoomRepository: new BreakoutRoomRepository(database),
      chatMessageRepository: new ChatMessageRepository(database),
      idempotencyRepository: new IdempotencyRepository(database, {
        ttlMs: config.idempotencyTtlMs,
        waitTimeoutMs: config.idempotencyWaitTimeoutMs,
        logger,
      }),
      userRepository,
      refreshTokenRepository: new RefreshTokenRepository(database),
      recordingJobRepository: new RecordingJobRepository(database),
    },
  };
}

async function seedUsers({ config, userRepository }) {
  const teacherPasswordHash =
    config.auth.defaultTeacherPasswordHash ||
    (config.auth.defaultTeacherPassword
      ? await hashPassword(config.auth.defaultTeacherPassword)
      : null);

  if (teacherPasswordHash) {
    await userRepository.ensureUser({
      id: config.defaultTeacherId,
      email: config.auth.defaultTeacherEmail,
      passwordHash: teacherPasswordHash,
      role: 'teacher',
      displayName: config.defaultTeacherName,
    });
  }

  const studentPasswordHash =
    config.auth.defaultStudentPasswordHash ||
    (config.auth.defaultStudentPassword
      ? await hashPassword(config.auth.defaultStudentPassword)
      : null);

  if (studentPasswordHash) {
    await userRepository.ensureUser({
      id: config.defaultStudentId,
      email: config.auth.defaultStudentEmail,
      passwordHash: studentPasswordHash,
      role: 'student',
      displayName: config.auth.defaultStudentName,
    });
  }
}
