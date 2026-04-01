import { Router } from 'express';

import { asyncHandler } from '../utils/asyncHandler.js';
import { optionalString, requireString } from '../utils/validation.js';

export function createAuthRoutes({
  authService,
  verifyAccessToken,
}) {
  const router = Router();

  router.post(
    '/auth/login',
    asyncHandler(async (req, res) => {
      const session = await authService.login({
        email: requireString(req.body?.email, 'email'),
        password: requireString(req.body?.password, 'password'),
      });
      res.status(201).json({
        access_token: session.accessToken,
        refresh_token: session.refreshToken,
        expires_in_seconds: session.expiresInSeconds,
        user: session.user,
      });
    }),
  );

  router.post(
    '/auth/refresh',
    asyncHandler(async (req, res) => {
      const session = await authService.refresh({
        refreshToken: requireString(req.body?.refresh_token, 'refresh_token'),
      });
      res.json({
        access_token: session.accessToken,
        refresh_token: session.refreshToken,
        expires_in_seconds: session.expiresInSeconds,
        user: session.user,
      });
    }),
  );

  router.post(
    '/auth/logout',
    asyncHandler(async (req, res) => {
      const result = await authService.logout({
        refreshToken:
          optionalString(req.body?.refresh_token) ??
          requireString(req.body?.refresh_token, 'refresh_token'),
      });
      res.json({
        ok: true,
        revoked: result.revoked,
      });
    }),
  );

  router.get(
    '/auth/me',
    verifyAccessToken,
    asyncHandler(async (req, res) => {
      res.json({
        user_id: req.user.userId,
        role: req.user.role,
        display_name: req.user.displayName,
        token_version: req.user.tokenVersion,
      });
    }),
  );

  return router;
}
