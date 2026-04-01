import { Router } from 'express';

import { asyncHandler } from '../utils/asyncHandler.js';
import { requireString } from '../utils/validation.js';

function readClassId(req) {
  return requireString(req.body?.class_id ?? req.query.class_id, 'class_id');
}

export function createRecordingRoutes({
  recordingService,
  verifyAccessToken,
  requireTeacherGuard,
}) {
  const router = Router();
  router.use(verifyAccessToken);

  router.post(
    '/recording/start',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const recording = await recordingService.start({ classId });
      res.status(201).json({
        ...(recording ?? {}),
        egress_id: recording?.egressId ?? null,
        file_path: recording?.filePath ?? '',
        started_at: recording?.startedAt ?? '',
        status: recording?.status ?? '',
        egressId: recording?.egressId ?? null,
        filePath: recording?.filePath ?? '',
        startedAt: recording?.startedAt ?? '',
      });
    }),
  );

  router.post(
    '/recording/stop',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const stopResult = await recordingService.stop({
        classId,
        whiteboardComposition:
          req.body?.whiteboard_composition &&
          typeof req.body.whiteboard_composition === 'object'
            ? req.body.whiteboard_composition
            : null,
      });
      res.json({
        ...(stopResult ?? {}),
        status: stopResult.status ?? '',
        egress_id: stopResult.egressId ?? null,
        raw_recording_path: stopResult.rawRecordingPath ?? '',
        processing_job_id: stopResult.processingJobId ?? null,
        processing_job_status: stopResult.processingJobStatus ?? '',
        egressId: stopResult.egressId ?? null,
        rawRecordingPath: stopResult.rawRecordingPath ?? '',
        processingJobId: stopResult.processingJobId ?? null,
        processingJobStatus: stopResult.processingJobStatus ?? '',
      });
    }),
  );

  router.post(
    '/recording/process_async',
    requireTeacherGuard,
    asyncHandler(async (req, res) => {
      const classId = readClassId(req);
      const rawRecordingPath = requireString(
        req.body?.raw_recording_path,
        'raw_recording_path',
      );
      const queued = await recordingService.queueProcessing({
        classId,
        rawRecordingPath,
        whiteboardComposition:
          req.body?.whiteboard_composition &&
          typeof req.body.whiteboard_composition === 'object'
            ? req.body.whiteboard_composition
            : null,
      });
      res.json({
        ...queued,
        job_id: queued.jobId,
        status: queued.status,
        jobId: queued.jobId,
      });
    }),
  );

  router.get(
    '/recording/process_status',
    asyncHandler(async (req, res) => {
      const jobId = requireString(req.query.job_id, 'job_id');
      res.json({
        status: await recordingService.getProcessingStatus(jobId),
      });
    }),
  );

  router.get(
    '/recording/process_result',
    asyncHandler(async (req, res) => {
      const jobId = requireString(req.query.job_id, 'job_id');
      res.json(await recordingService.getProcessingResult(jobId));
    }),
  );

  router.get(
    '/recording/replay',
    asyncHandler(async (req, res) => {
      const classId = requireString(req.query.class_id, 'class_id');
      const replay = await recordingService.getReplay(classId);
      res.json({
        class_id: replay.classId,
        video_url: replay.videoUrl,
        recording_url: replay.videoUrl,
        playback_provider: replay.playbackProvider ?? '',
        video_provider: replay.playbackProvider ?? '',
        video_id: replay.videoId ?? '',
        youtube_video_id: replay.videoId ?? '',
        embed_url: replay.embedUrl ?? '',
        youtube_embed_url: replay.embedUrl ?? '',
        title: replay.title ?? '',
        description: replay.description ?? '',
        metadata: replay.metadata ?? null,
        transcript: replay.transcript,
        concept_index: replay.conceptIndex,
        whiteboard_overlay: replay.whiteboardOverlay ?? null,
      });
    }),
  );

  return router;
}
