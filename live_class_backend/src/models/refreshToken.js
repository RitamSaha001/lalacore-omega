import { randomUUID } from 'node:crypto';

export function createRefreshToken({
  id = randomUUID(),
  userId,
  familyId = randomUUID(),
  tokenHash,
  expiresAt,
  createdAt = new Date().toISOString(),
  revokedAt = null,
  replacedByTokenId = null,
  tokenVersion,
}) {
  return {
    id,
    userId,
    familyId,
    tokenHash,
    expiresAt,
    createdAt,
    revokedAt,
    replacedByTokenId,
    tokenVersion,
  };
}
