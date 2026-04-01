import { HttpError } from '../utils/httpError.js';
import { stableStringify } from '../utils/stableStringify.js';

const MUTATING_METHODS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

function buildScope(req) {
  return `${req.method}:${req.baseUrl}${req.path}`;
}

function buildFingerprint(req) {
  return stableStringify({
    method: req.method,
    path: `${req.baseUrl}${req.path}`,
    query: req.query ?? {},
    body: req.body ?? {},
  });
}

function sendStoredResponse(res, record) {
  res.set('Idempotency-Replayed', 'true');
  if (record.response.contentType) {
    res.type(record.response.contentType);
  }
  res.status(record.response.statusCode);
  if (record.response.kind === 'json') {
    res.json(record.response.body);
    return;
  }
  res.send(record.response.body);
}

export function createIdempotencyMiddleware({
  idempotencyRepository,
  logger,
}) {
  return async function idempotencyMiddleware(req, res, next) {
    if (!MUTATING_METHODS.has(req.method)) {
      next();
      return;
    }

    const key = req.header('Idempotency-Key');
    if (!key) {
      next(new HttpError(400, 'Idempotency-Key header is required'));
      return;
    }

    const scope = buildScope(req);
    const fingerprint = buildFingerprint(req);

    try {
      const claim = await idempotencyRepository.begin({
        scope,
        key,
        fingerprint,
      });

      if (claim.type === 'replay') {
        // Return the original stored response without re-running the handler.
        sendStoredResponse(res, claim.record);
        return;
      }

      if (claim.type === 'wait') {
        const completed = await claim.waitForCompletion;
        sendStoredResponse(res, completed);
        return;
      }

      req.idempotency = {
        scope,
        key,
        fingerprint,
      };

      let captured = null;
      const originalJson = res.json.bind(res);
      const originalSend = res.send.bind(res);

      res.json = (body) => {
        captured = {
          kind: 'json',
          body,
          statusCode: res.statusCode,
          contentType: res.getHeader('content-type') ?? 'application/json',
        };
        return originalJson(body);
      };

      res.send = (body) => {
        if (!captured) {
          captured = {
            kind: 'send',
            body,
            statusCode: res.statusCode,
            contentType: res.getHeader('content-type') ?? 'text/plain',
          };
        }
        return originalSend(body);
      };

      res.on('finish', async () => {
        if (!req.idempotency) {
          return;
        }
        try {
          await idempotencyRepository.complete({
            ...req.idempotency,
            response: captured ?? {
              kind: 'send',
              body: '',
              statusCode: res.statusCode,
              contentType: res.getHeader('content-type') ?? 'text/plain',
            },
          });
        } catch (error) {
          logger.error('idempotency_complete_failed', {
            scope,
            key,
            error: String(error),
          });
        }
      });

      res.on('close', async () => {
        if (res.writableFinished || !req.idempotency) {
          return;
        }
        await idempotencyRepository.release(req.idempotency);
      });

      next();
    } catch (error) {
      next(error);
    }
  };
}
