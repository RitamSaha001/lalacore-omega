import { Router } from 'express';

import { asyncHandler } from '../utils/asyncHandler.js';
import { optionalString, requireString } from '../utils/validation.js';

function readClassId(req) {
  return requireString(req.body?.class_id ?? req.query.class_id, 'class_id');
}

function readAttachment(req) {
  const raw = req.body?.attachment;
  if (!raw || typeof raw !== 'object') {
    return null;
  }
  const type = optionalString(raw.type)?.toLowerCase();
  const name = optionalString(raw.name);
  const url = optionalString(raw.url) ?? optionalString(raw.path);
  if (!type || !name || !url) {
    return null;
  }
  if (type !== 'image' && type !== 'file') {
    return null;
  }
  return {
    type,
    name,
    url,
    mimeType: optionalString(raw.mime_type) ?? '',
    sizeBytes:
      typeof raw.size_bytes === 'number' ? raw.size_bytes : null,
  };
}

export function createChatRoutes({
  chatService,
  identityService,
  verifyAccessToken,
}) {
  const router = Router();
  router.use(verifyAccessToken);

  router.post(
    '/chat/send',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const userId = identityService.resolveUserId(req);
      const message = requireString(req.body?.message, 'message');
      const senderName =
        optionalString(req.body?.user_name) ??
        optionalString(req.body?.display_name) ??
        req.user?.displayName ??
        userId;

      const chatMessage = await chatService.sendMessage({
        classId,
        senderId: userId,
        senderName,
        message,
        attachment: readAttachment(req),
        dedupeKey:
          optionalString(req.body?.client_message_id) ??
          req.idempotency?.key ??
          null,
      });

      res.status(201).json({
        ok: true,
        message: chatMessage,
      });
    }),
  );

  router.get(
    '/chat/history',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      res.json(await chatService.getSnapshot(classId));
    }),
  );

  return router;
}
