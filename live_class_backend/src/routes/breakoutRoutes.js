import { Router } from 'express';

import { asyncHandler } from '../utils/asyncHandler.js';
import { optionalString, requireString } from '../utils/validation.js';

function readClassId(req) {
  return requireString(req.body?.class_id ?? req.query.class_id, 'class_id');
}

export function createBreakoutRoutes({
  breakoutService,
  breakoutRoomRepository,
  identityService,
  verifyAccessToken,
  requireTeacherGuard,
}) {
  const router = Router();
  router.use(verifyAccessToken);

  router.post(
    '/breakout/create',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const name = requireString(req.body?.name, 'name');
      res.status(201).json({
        ok: true,
        breakout_room: await breakoutService.createRoom({ classId, name }),
      });
    }),
  );

  router.post(
    '/breakout/assign',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      res.json({
        ok: true,
        participant: await breakoutService.assignParticipant({
          classId,
          userId: requireString(req.body?.user_id, 'user_id'),
          breakoutRoomId: requireString(req.body?.room_id, 'room_id'),
        }),
      });
    }),
  );

  router.post(
    '/breakout/join',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const userId = identityService.resolveUserId(req);
      res.json(
        await breakoutService.joinAssignedRoom({
          classId,
          userId,
          userName:
            optionalString(req.body?.user_name) ??
            optionalString(req.body?.display_name) ??
            req.user?.displayName ??
            userId,
        }),
      );
    }),
  );

  router.post(
    '/breakout/leave',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const userId = identityService.resolveUserId(req);
      res.json(
        await breakoutService.leaveBreakout({
          classId,
          userId,
          userName:
            optionalString(req.body?.user_name) ??
            optionalString(req.body?.display_name) ??
            req.user?.displayName ??
            userId,
        }),
      );
    }),
  );

  router.get(
    '/breakout/list',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      res.json({
        breakout_rooms: await breakoutRoomRepository.listByClass(classId),
      });
    }),
  );

  router.post(
    '/class/breakout/move',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      res.json({
        ok: true,
        participant: await breakoutService.assignParticipant({
          classId,
          userId: requireString(req.body?.user_id, 'user_id'),
          breakoutRoomId: requireString(req.body?.room_id, 'room_id'),
        }),
      });
    }),
  );

  router.post(
    '/class/breakout/broadcast',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      res.json({
        ok: true,
        delivery: await breakoutService.broadcastMessage({
          classId,
          message: requireString(req.body?.message, 'message'),
        }),
      });
    }),
  );

  return router;
}
