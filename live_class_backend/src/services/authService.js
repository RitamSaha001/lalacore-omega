import { HttpError } from '../utils/httpError.js';
import { verifyPassword } from '../utils/passwords.js';

function computeExpiry(ttlMs) {
  return new Date(Date.now() + ttlMs).toISOString();
}

export class AuthService {
  constructor({
    config,
    database,
    userRepository,
    refreshTokenRepository,
    jwtService,
    logger,
  }) {
    this.config = config;
    this.database = database;
    this.userRepository = userRepository;
    this.refreshTokenRepository = refreshTokenRepository;
    this.jwtService = jwtService;
    this.logger = logger;
  }

  async login({ email, password }) {
    const user = await this.userRepository.getByEmail(email);
    if (!user || user.status !== 'active') {
      throw new HttpError(401, 'Invalid credentials');
    }

    const matches = await verifyPassword(password, user.passwordHash);
    if (!matches) {
      throw new HttpError(401, 'Invalid credentials');
    }

    return this.issueSession(user);
  }

  async refresh({ refreshToken }) {
    const tokenHash = this.jwtService.hashRefreshToken(refreshToken);

    return this.database.withLocks([`refresh_token:${tokenHash}`], async () => {
      const existing = await this.refreshTokenRepository.getByTokenHash(tokenHash);
      if (!existing) {
        throw new HttpError(401, 'Refresh token not recognized');
      }
      if (existing.revokedAt) {
        await this.refreshTokenRepository.revokeFamily(existing.familyId);
        throw new HttpError(401, 'Refresh token has already been rotated');
      }
      if (new Date(existing.expiresAt).getTime() <= Date.now()) {
        throw new HttpError(401, 'Refresh token expired');
      }

      const user = await this.userRepository.getById(existing.userId);
      if (!user || user.status !== 'active') {
        throw new HttpError(401, 'User is no longer active');
      }
      if (user.tokenVersion !== existing.tokenVersion) {
        throw new HttpError(401, 'Refresh token version mismatch');
      }

      const issued = await this.issueSession(user, {
        familyId: existing.familyId,
      });
      const rotated = await this.refreshTokenRepository.getByTokenHash(
        this.jwtService.hashRefreshToken(issued.refreshToken),
      );
      await this.refreshTokenRepository.revoke(existing.id, {
        replacedByTokenId: rotated?.id ?? null,
      });
      return issued;
    });
  }

  async logout({ refreshToken }) {
    const tokenHash = this.jwtService.hashRefreshToken(refreshToken);
    const existing = await this.refreshTokenRepository.getByTokenHash(tokenHash);
    if (!existing) {
      return {
        revoked: false,
      };
    }
    await this.refreshTokenRepository.revoke(existing.id);
    return {
      revoked: true,
    };
  }

  async verifyAccessToken(token) {
    let payload;
    try {
      payload = this.jwtService.verifyAccessToken(token);
    } catch (error) {
      this.logger.warn('access_token_verification_failed', {
        error: String(error),
      });
      throw new HttpError(401, 'Invalid or expired access token');
    }
    const user = await this.userRepository.getById(payload.user_id);
    if (!user || user.status !== 'active') {
      throw new HttpError(401, 'User is no longer active');
    }
    if (user.tokenVersion !== payload.token_version) {
      throw new HttpError(401, 'Token version mismatch');
    }
    return {
      userId: user.id,
      role: user.role,
      displayName: user.displayName,
      tokenVersion: user.tokenVersion,
      exp: payload.exp,
    };
  }

  async issueSession(user, { familyId = null } = {}) {
    const accessToken = this.jwtService.signAccessToken({
      userId: user.id,
      role: user.role,
      tokenVersion: user.tokenVersion,
    });
    const refreshToken = this.jwtService.issueRefreshToken();
    const refreshTokenHash = this.jwtService.hashRefreshToken(refreshToken);
    await this.refreshTokenRepository.create({
      userId: user.id,
      familyId: familyId ?? undefined,
      tokenHash: refreshTokenHash,
      expiresAt: computeExpiry(this.config.auth.refreshTokenTtlMs),
      tokenVersion: user.tokenVersion,
    });

    this.logger.info('auth_session_issued', {
      userId: user.id,
      role: user.role,
    });

    return {
      accessToken,
      refreshToken,
      user: {
        id: user.id,
        email: user.email,
        role: user.role,
        display_name: user.displayName,
      },
      expiresInSeconds: this.config.auth.accessTokenTtlSeconds,
    };
  }
}
