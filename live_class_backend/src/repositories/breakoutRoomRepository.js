import { createBreakoutRoom } from '../models/breakoutRoom.js';

export class BreakoutRoomRepository {
  constructor(db) {
    this.db = db;
  }

  async create({ classId, name, livekitRoomName }) {
    const breakout = createBreakoutRoom({
      classId,
      name,
      livekitRoomName: `${livekitRoomName}__${Date.now()}`,
    });
    this.db.ensureBreakoutRooms(classId).set(breakout.id, breakout);
    return breakout;
  }

  async getById(classId, breakoutRoomId) {
    return this.db.ensureBreakoutRooms(classId).get(breakoutRoomId) ?? null;
  }

  async listByClass(classId) {
    return Array.from(this.db.ensureBreakoutRooms(classId).values());
  }
}
