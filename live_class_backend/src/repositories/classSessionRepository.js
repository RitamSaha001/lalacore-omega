export class ClassSessionRepository {
  constructor(db) {
    this.db = db;
  }

  async getById(classId) {
    return this.db.classSessions.get(classId) ?? null;
  }

  async save(session) {
    const current = await this.getById(session.id);
    const next = {
      ...session,
      version:
        current && (session.version ?? 0) <= current.version
          ? current.version + 1
          : session.version ?? 1,
    };
    this.db.classSessions.set(session.id, next);
    return next;
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
