import { HttpError } from '../utils/httpError.js';

export class IdentityService {
  constructor({ classSessionRepository, participantRepository }) {
    this.classSessionRepository = classSessionRepository;
    this.participantRepository = participantRepository;
  }

  resolveUserId(req) {
    const userId = req.user?.userId;
    if (!userId) {
      throw new HttpError(401, 'Authenticated user context is required');
    }
    return userId;
  }

  async resolveActor(req, classId) {
    const userId = this.resolveUserId(req);
    const session = await this.classSessionRepository.getById(classId);
    const participant = await this.participantRepository.getByClassAndUser(
      classId,
      userId,
    );
    const role = req.user?.role ?? (session?.teacherId === userId ? 'teacher' : 'student');
    return {
      userId,
      classId,
      session,
      participant,
      role,
      isTeacher: role === 'teacher',
    };
  }

  assertTeacher(actor) {
    if (!actor.isTeacher) {
      throw new HttpError(403, 'Only the teacher can perform this action');
    }
  }
}
