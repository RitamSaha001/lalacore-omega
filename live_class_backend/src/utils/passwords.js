import bcrypt from 'bcryptjs';

const DEFAULT_ROUNDS = 12;

export async function hashPassword(password, rounds = DEFAULT_ROUNDS) {
  return bcrypt.hash(password, rounds);
}

export async function verifyPassword(password, passwordHash) {
  return bcrypt.compare(password, passwordHash);
}
