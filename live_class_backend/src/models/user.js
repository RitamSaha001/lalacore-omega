import { randomUUID } from 'node:crypto';

export function createUser({
  id = randomUUID(),
  email,
  passwordHash,
  role = 'student',
  displayName,
  tokenVersion = 1,
  status = 'active',
  createdAt = new Date().toISOString(),
}) {
  return {
    id,
    email,
    passwordHash,
    role,
    displayName,
    tokenVersion,
    status,
    createdAt,
  };
}
