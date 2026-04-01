import { createRefreshToken } from '../models/refreshToken.js';

export class RefreshTokenRepository {
  constructor(db) {
    this.db = db;
  }

  async create(tokenRecord) {
    const next = createRefreshToken(tokenRecord);
    this.db.refreshTokens.set(next.id, next);
    return next;
  }

  async getByTokenHash(tokenHash) {
    for (const token of this.db.refreshTokens.values()) {
      if (token.tokenHash === tokenHash) {
        return token;
      }
    }
    return null;
  }

  async revoke(tokenId, { replacedByTokenId = null } = {}) {
    const current = this.db.refreshTokens.get(tokenId) ?? null;
    if (!current) {
      return null;
    }
    const next = {
      ...current,
      revokedAt: current.revokedAt ?? new Date().toISOString(),
      replacedByTokenId,
    };
    this.db.refreshTokens.set(tokenId, next);
    return next;
  }

  async revokeFamily(familyId) {
    const revoked = [];
    for (const token of this.db.refreshTokens.values()) {
      if (token.familyId !== familyId || token.revokedAt) {
        continue;
      }
      revoked.push(await this.revoke(token.id));
    }
    return revoked;
  }
}
