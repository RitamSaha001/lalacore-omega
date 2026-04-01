import { randomUUID } from 'node:crypto';

export function createChatMessage({
  id = randomUUID(),
  classId,
  senderId,
  senderName,
  message,
  attachment = null,
  dedupeKey = null,
  timestamp = new Date().toISOString(),
}) {
  return {
    id,
    classId,
    senderId,
    senderName,
    message,
    attachment,
    dedupeKey,
    timestamp,
  };
}
