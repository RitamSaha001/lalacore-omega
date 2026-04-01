import { HttpError } from '../utils/httpError.js';
import { mergeRequestContext } from '../observability/requestContext.js';

function extractBearerToken(req) {
  const authorization = req.headers.authorization ?? '';
  if (authorization.startsWith('Bearer ')) {
    return authorization.slice('Bearer '.length).trim();
  }
  return null;
}

export function createVerifyAccessToken({ authService }) {
  return async function verifyAccessToken(req, _res, next) {
    try {
      const token = extractBearerToken(req);
      if (!token) {
        throw new HttpError(401, 'Missing bearer token');
      }

      req.user = await authService.verifyAccessToken(token);
      mergeRequestContext({
        userId: req.user.userId,
        role: req.user.role,
      });
      next();
    } catch (error) {
      next(error);
    }
  };
}

export function requireTeacher(req, _res, next) {
  if (req.user?.role !== 'teacher') {
    next(new HttpError(403, 'Teacher role required'));
    return;
  }
  next();
}

export function extractWebSocketToken(requestUrl, request) {
  const url = new URL(requestUrl, 'http://localhost');
  const queryToken = url.searchParams.get('access_token');
  if (queryToken) {
    return queryToken;
  }
  const legacyToken = url.searchParams.get('token');
  if (legacyToken) {
    return legacyToken;
  }

  const authorization = request.headers.authorization ?? '';
  if (authorization.startsWith('Bearer ')) {
    return authorization.slice('Bearer '.length).trim();
  }

  const protocols = request.headers['sec-websocket-protocol'];
  if (typeof protocols === 'string') {
    const parts = protocols.split(',').map((value) => value.trim());
    const bearerIndex = parts.findIndex((part) => part.toLowerCase() === 'bearer');
    if (bearerIndex >= 0 && parts[bearerIndex + 1]) {
      return parts[bearerIndex + 1];
    }
  }

  return null;
}
