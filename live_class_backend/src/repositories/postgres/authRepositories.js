import { mapRefreshTokenRow, mapUserRow } from './mappers.js';

export class PostgresUserRepository {
  constructor(store) {
    this.store = store;
  }

  async getById(userId) {
    const { rows } = await this.store.query(
      'SELECT * FROM users WHERE id = $1',
      [userId],
    );
    return mapUserRow(rows[0]);
  }

  async getByEmail(email) {
    const { rows } = await this.store.query(
      'SELECT * FROM users WHERE email = $1',
      [email],
    );
    return mapUserRow(rows[0]);
  }

  async save(user) {
    const { rows } = await this.store.query(
      `
        INSERT INTO users (
          id,
          email,
          password_hash,
          role,
          display_name,
          token_version,
          status,
          created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE
        SET email = EXCLUDED.email,
            password_hash = EXCLUDED.password_hash,
            role = EXCLUDED.role,
            display_name = EXCLUDED.display_name,
            token_version = EXCLUDED.token_version,
            status = EXCLUDED.status
        RETURNING *
      `,
      [
        user.id,
        user.email,
        user.passwordHash,
        user.role,
        user.displayName,
        user.tokenVersion ?? 1,
        user.status ?? 'active',
        user.createdAt ?? new Date().toISOString(),
      ],
    );
    return mapUserRow(rows[0]);
  }

  async ensureUser({ email, passwordHash, role, displayName, id }) {
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
      tokenVersion: 1,
      status: 'active',
    });
  }

  async incrementTokenVersion(userId) {
    const { rows } = await this.store.query(
      `
        UPDATE users
        SET token_version = token_version + 1
        WHERE id = $1
        RETURNING *
      `,
      [userId],
    );
    return mapUserRow(rows[0]);
  }
}

export class PostgresRefreshTokenRepository {
  constructor(store) {
    this.store = store;
  }

  async create(tokenRecord) {
    const { rows } = await this.store.query(
      `
        INSERT INTO refresh_tokens (
          id,
          user_id,
          family_id,
          token_hash,
          expires_at,
          created_at,
          revoked_at,
          replaced_by_token_id,
          token_version
        )
        VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, COALESCE($3, gen_random_uuid()::text), $4, $5, $6, $7, $8, $9)
        RETURNING *
      `,
      [
        tokenRecord.id,
        tokenRecord.userId,
        tokenRecord.familyId ?? null,
        tokenRecord.tokenHash,
        tokenRecord.expiresAt,
        tokenRecord.createdAt ?? new Date().toISOString(),
        tokenRecord.revokedAt,
        tokenRecord.replacedByTokenId,
        tokenRecord.tokenVersion,
      ],
    );
    return mapRefreshTokenRow(rows[0]);
  }

  async getByTokenHash(tokenHash) {
    const { rows } = await this.store.query(
      'SELECT * FROM refresh_tokens WHERE token_hash = $1',
      [tokenHash],
    );
    return mapRefreshTokenRow(rows[0]);
  }

  async revoke(tokenId, { replacedByTokenId = null } = {}) {
    const { rows } = await this.store.query(
      `
        UPDATE refresh_tokens
        SET revoked_at = COALESCE(revoked_at, NOW()),
            replaced_by_token_id = COALESCE($2, replaced_by_token_id)
        WHERE id = $1
        RETURNING *
      `,
      [tokenId, replacedByTokenId],
    );
    return mapRefreshTokenRow(rows[0]);
  }

  async revokeFamily(familyId) {
    const { rows } = await this.store.query(
      `
        UPDATE refresh_tokens
        SET revoked_at = COALESCE(revoked_at, NOW())
        WHERE family_id = $1
        RETURNING *
      `,
      [familyId],
    );
    return rows.map(mapRefreshTokenRow);
  }
}
