import { Router } from 'express';

import { asyncHandler } from '../utils/asyncHandler.js';
import {
  optionalBoolean,
  optionalString,
  requireString,
} from '../utils/validation.js';

function readClassId(req) {
  return requireString(req.body?.class_id ?? req.query.class_id, 'class_id');
}

function readUserId(req) {
  return requireString(req.body?.user_id ?? req.query.user_id, 'user_id');
}

function readUserName(req, fallback) {
  return (
    optionalString(req.body?.user_name) ??
    optionalString(req.body?.display_name) ??
    optionalString(req.query.user_name) ??
    optionalString(req.query.display_name) ??
    fallback
  );
}

export function createClassroomRoutes({
  authorityService,
  identityService,
  liveTokenService,
  verifyAccessToken,
  requireTeacherGuard,
}) {
  const router = Router();
  router.use(verifyAccessToken);

  router.get(
    '/class/session',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const session = await authorityService.getSession(classId);
      res.json({
        id: session.id,
        title: session.title,
        teacher_name: session.teacherName,
        is_recording: session.isRecording,
      });
    }),
  );

  const requestJoinHandler = asyncHandler(async (req, res) => {
    const classId = readClassId(req);
    const userId = identityService.resolveUserId(req);
    const userName = readUserName(req, req.user?.displayName ?? userId);
    const result = await authorityService.requestJoin({
      classId,
      userId,
      userName,
    });
    res.status(202).json({
      request_id: result.requestId,
      status: result.status,
    });
  });

  router.post('/request-join', requestJoinHandler);
  router.post('/class/join_request', requestJoinHandler);

  router.post(
    '/class/join_cancel',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const userId = identityService.resolveUserId(req);
      const canceled = await authorityService.cancelJoin({
        classId,
        userId,
      });
      res.json({
        ok: true,
        applied: canceled.applied,
        participant: canceled.participant,
      });
    }),
  );

  const approveJoinHandler = [
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const approved = await authorityService.approveJoin({
        classId,
        userId: readUserId(req),
      });
      res.json({
        ok: true,
        participant: approved,
      });
    }),
  ];

  router.post('/approve-join', approveJoinHandler);
  router.post('/class/admit', approveJoinHandler);

  const rejectJoinHandler = [
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const rejected = await authorityService.rejectJoin({
        classId,
        userId: readUserId(req),
        reason: optionalString(req.body?.reason),
      });
      res.json({
        ok: true,
        participant: rejected,
      });
    }),
  ];

  router.post('/reject-join', rejectJoinHandler);
  router.post('/class/reject', rejectJoinHandler);

  router.post(
    '/class/admit_all',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const approved = await authorityService.approveAll(classId);
      res.json({
        ok: true,
        approved_count: approved.length,
      });
    }),
  );

  const liveTokenHandler = asyncHandler(async (req, res) => {
    const classId = readClassId(req);
    const userId = identityService.resolveUserId(req);
    const access = await liveTokenService.issueAuthorizedToken({
      classId,
      userId,
      userName: readUserName(req, req.user?.displayName ?? userId),
    });
    res.json({
      session_id: access.sessionId,
      room_id: access.roomId,
      token: access.token,
      provider: access.provider,
      ws_url: access.serverUrl,
      expires_at: access.expiresAtEpochSeconds,
    });
  });

  router.get('/live-token', liveTokenHandler);
  router.post('/live/token', liveTokenHandler);

  router.get(
    '/class/state',
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const userId = identityService.resolveUserId(req);
      res.json(await authorityService.fetchClassroomState(classId, userId));
    }),
  );

  router.post(
    '/meeting/lock',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      res.json({
        ok: true,
        state: await authorityService.setMeetingLocked(classId, true),
      });
    }),
  );

  router.post(
    '/meeting/unlock',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      res.json({
        ok: true,
        state: await authorityService.setMeetingLocked(classId, false),
      });
    }),
  );

  router.post(
    '/class/lock',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const locked = optionalBoolean(req.body?.locked, null);
      res.json({
        ok: true,
        state: await authorityService.setMeetingLocked(classId, locked !== false),
      });
    }),
  );

  router.post(
    '/class/chat',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const enabled = optionalBoolean(req.body?.enabled, true);
      res.json({
        ok: true,
        state: await authorityService.setChatEnabled(classId, enabled),
      });
    }),
  );

  router.post(
    '/class/waiting_room',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const enabled = optionalBoolean(req.body?.enabled, true);
      res.json({
        ok: true,
        state: await authorityService.setWaitingRoomEnabled(classId, enabled),
      });
    }),
  );

  router.post(
    '/class/recording',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const enabled = optionalBoolean(req.body?.enabled, true);
      res.json({
        ok: true,
        state: await authorityService.setRecordingEnabled(classId, enabled),
      });
    }),
  );

  router.post(
    '/class/mute',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const targetUserId = readUserId(req);
      res.json({
        ok: true,
        participant: await authorityService.updateParticipantModeration(
          classId,
          targetUserId,
          { muted: optionalBoolean(req.body?.muted, true) },
        ),
      });
    }),
  );

  router.post(
    '/class/camera',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const targetUserId = readUserId(req);
      res.json({
        ok: true,
        participant: await authorityService.updateParticipantModeration(
          classId,
          targetUserId,
          { cameraDisabled: optionalBoolean(req.body?.disabled, true) },
        ),
      });
    }),
  );

  router.post(
    '/class/remove',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const targetUserId = readUserId(req);
      res.json({
        ok: true,
        participant: await authorityService.updateParticipantModeration(
          classId,
          targetUserId,
          { status: 'removed' },
        ),
      });
    }),
  );

  router.post(
    '/class/whiteboard/access',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const targetUserId = readUserId(req);
      const enabled = optionalBoolean(req.body?.enabled, true);
      res.json({
        ok: true,
        whiteboard: await authorityService.setWhiteboardAccess(
          classId,
          targetUserId,
          enabled,
        ),
      });
    }),
  );

  router.post(
    '/class/fallback_token',
    asyncHandler(async (_req, res) => {
      res.status(501).json({
        error: 'WebRTC failover token service is not implemented in this package',
      });
    }),
  );

  return router;
}
