import { randomUUID } from 'node:crypto';

export function createRecordingJob({
  id = randomUUID(),
  classId,
  egressId = null,
  rawRecordingPath,
  status = 'queued',
  attempts = 0,
  result = null,
  error = null,
  createdAt = new Date().toISOString(),
  updatedAt = createdAt,
}) {
  return {
    id,
    classId,
    egressId,
    rawRecordingPath,
    status,
    attempts,
    result,
    error,
    createdAt,
    updatedAt,
  };
}
