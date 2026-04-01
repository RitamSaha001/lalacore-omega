import { createHash, randomBytes } from 'node:crypto';

import jwt from 'jsonwebtoken';

export class JwtService {
  constructor({ config }) {
    this.config = config;
  }

  signAccessToken({ userId, role, tokenVersion }) {
    return jwt.sign(
      {
        user_id: userId,
        role,
        token_version: tokenVersion,
      },
      this.config.auth.jwtSecret,
      {
        issuer: this.config.auth.issuer,
        audience: this.config.auth.audience,
        expiresIn: this.config.auth.accessTokenTtl,
      },
    );
  }

  verifyAccessToken(token) {
    return jwt.verify(token, this.config.auth.jwtSecret, {
      issuer: this.config.auth.issuer,
      audience: this.config.auth.audience,
    });
  }

  issueRefreshToken() {
    return randomBytes(48).toString('base64url');
  }

  hashRefreshToken(token) {
    return createHash('sha256').update(token).digest('hex');
  }
}
