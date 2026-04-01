import { randomUUID } from 'node:crypto';

export function createBreakoutRoom({
  id = randomUUID(),
  classId,
  name,
  livekitRoomName,
  createdAt = new Date().toISOString(),
  version = 1,
}) {
  return {
    id,
    classId,
    name,
    livekitRoomName,
    createdAt,
    version,
  };
}
