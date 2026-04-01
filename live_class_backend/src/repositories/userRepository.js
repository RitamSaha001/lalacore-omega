import { createUser } from '../models/user.js';

export class UserRepository {
  constructor(db) {
    this.db = db;
  }

  async getById(userId) {
    return this.db.users.get(userId) ?? null;
  }

  async getByEmail(email) {
    for (const user of this.db.users.values()) {
      if (user.email === email) {
        return user;
      }
    }
    return null;
  }

  async save(user) {
    const next = createUser(user);
    this.db.users.set(next.id, next);
    return next;
  }

  async ensureUser({
    email,
    passwordHash,
    role,
    displayName,
    id,
  }) {
    const existing = await this.getByEmail(email);
    if (existing) {
      return existing;
    }
    return this.save({
      id,
      email,
      passwordHash,
      role,
      displayName,
    });
  }

  async incrementTokenVersion(userId) {
    const current = await this.getById(userId);
    if (!current) {
      return null;
    }
    const next = {
      ...current,
      tokenVersion: current.tokenVersion + 1,
    };
    this.db.users.set(userId, next);
    return next;
  }
}
