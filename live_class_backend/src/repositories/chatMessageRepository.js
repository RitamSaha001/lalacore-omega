import { createChatMessage } from '../models/chatMessage.js';

export class ChatMessageRepository {
  constructor(db) {
    this.db = db;
  }

  async create({
    classId,
    senderId,
    senderName,
    message,
    attachment = null,
    dedupeKey = null,
  }) {
    const bucket = this.db.ensureChatMessages(classId);
    const dedupeIndex = this.db.ensureChatDeduplication(classId);
    if (dedupeKey && dedupeIndex.has(dedupeKey)) {
      return dedupeIndex.get(dedupeKey);
    }
    const chatMessage = createChatMessage({
      classId,
      senderId,
      senderName,
      message,
      attachment,
      dedupeKey,
    });
    bucket.push(chatMessage);
    if (dedupeKey) {
      dedupeIndex.set(dedupeKey, chatMessage);
    }
    if (bucket.length > 500) {
      bucket.splice(0, bucket.length - 500);
    }
    return chatMessage;
  }

  async getByDedupeKey(classId, dedupeKey) {
    if (!dedupeKey) {
      return null;
    }
    return this.db.ensureChatDeduplication(classId).get(dedupeKey) ?? null;
  }

  async listByClass(classId) {
    return [...this.db.ensureChatMessages(classId)];
  }
}
