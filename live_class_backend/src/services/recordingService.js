import { randomUUID } from 'node:crypto';

import { HttpError } from '../utils/httpError.js';

function normalizeString(value) {
  return String(value ?? '').trim();
}

function extractYouTubeVideoId(raw) {
  const input = normalizeString(raw);
  if (!input) {
    return '';
  }
  if (/^[A-Za-z0-9_-]{11}$/.test(input)) {
    return input;
  }
  try {
    const url = new URL(input);
    const host = url.host.toLowerCase();
    if (host.includes('youtu.be')) {
      const segment = url.pathname.split('/').filter(Boolean)[0] ?? '';
      if (/^[A-Za-z0-9_-]{11}$/.test(segment)) {
        return segment;
      }
    }
    if (host.includes('youtube.com') || host.includes('youtube-nocookie.com')) {
      const queryId = normalizeString(url.searchParams.get('v'));
      if (/^[A-Za-z0-9_-]{11}$/.test(queryId)) {
        return queryId;
      }
      for (const segment of url.pathname.split('/').filter(Boolean)) {
        if (/^[A-Za-z0-9_-]{11}$/.test(segment)) {
          return segment;
        }
      }
    }
  } catch (_) {}

  const match = input.match(
    /(?:youtu\.be\/|youtube(?:-nocookie)?\.com\/(?:watch\?.*v=|embed\/|shorts\/))([A-Za-z0-9_-]{11})/i,
  );
  return match?.[1] ?? '';
}

function buildYouTubeWatchUrl(videoId) {
  const normalized = normalizeString(videoId);
  return normalized ? `https://www.youtube.com/watch?v=${normalized}` : '';
}

function buildYouTubeEmbedUrl(videoId) {
  const normalized = normalizeString(videoId);
  return normalized
    ? `https://www.youtube.com/embed/${normalized}?playsinline=1&rel=0&modestbranding=1&controls=0&enablejsapi=1`
    : '';
}

function derivePlaybackPayload(result = null, fallbackUrl = '') {
  const rawUrl = normalizeString(result?.recordingUrl ?? fallbackUrl);
  const explicitProvider = normalizeString(
    result?.playbackProvider ?? result?.videoProvider,
  ).toLowerCase();
  const videoId =
    normalizeString(result?.videoId ?? result?.youtubeVideoId) ||
    extractYouTubeVideoId(result?.embedUrl) ||
    extractYouTubeVideoId(rawUrl);
  const playbackProvider =
    explicitProvider || (videoId ? 'youtube' : rawUrl ? 'direct' : '');
  const embedUrl =
    normalizeString(result?.embedUrl ?? result?.youtubeEmbedUrl) ||
    (playbackProvider === 'youtube' ? buildYouTubeEmbedUrl(videoId) : '');
  const publicUrl =
    (playbackProvider === 'youtube' ? buildYouTubeWatchUrl(videoId) : '') || rawUrl;

  return {
    playbackProvider,
    videoId,
    embedUrl,
    publicUrl,
    title: normalizeString(result?.title),
    description: normalizeString(result?.description),
    metadata:
      result?.metadata && typeof result.metadata === 'object'
        ? result.metadata
        : null,
  };
}

export class RecordingService {
  constructor({
    database,
    classSessionRepository,
    recordingJobRepository,
    recordingQueue,
    livekitClient,
    classroomHub,
    logger,
    metricsCollector = null,
  }) {
    this.database = database;
    this.classSessionRepository = classSessionRepository;
    this.recordingJobRepository = recordingJobRepository;
    this.recordingQueue = recordingQueue;
    this.livekitClient = livekitClient;
    this.classroomHub = classroomHub;
    this.logger = logger;
    this.metricsCollector = metricsCollector;
    this.latestReplayByClass = new Map();
  }

  async start({ classId }) {
    let transition = null;

    await this.database.withLocks([`session:${classId}`], async () => {
      const session = await this.classSessionRepository.getById(classId);
      if (!session) {
        throw new HttpError(404, 'Class session not found');
      }

      if (session.recordingStatus === 'starting' || session.activeRecording?.egressId) {
        transition = {
          reused: true,
          recording: session.activeRecording,
        };
        return;
      }

      const requestId = randomUUID();
      const updated = await this.classSessionRepository.update(classId, (current) => ({
        ...current,
        recordingStatus: 'starting',
        activeRecording: {
          requestId,
          status: 'starting',
          startedAt: new Date().toISOString(),
        },
      }));
      transition = {
        reused: false,
        requestId,
        roomName: updated.activeRoomId,
      };
    });

    if (transition.reused) {
      return transition.recording;
    }

    try {
      const recording = await this.livekitClient.startRoomRecording({
        classId,
        roomName: transition.roomName,
      });

      let finalized = null;
      await this.database.withLocks([`session:${classId}`], async () => {
        const session = await this.classSessionRepository.getById(classId);
        if (
          session?.activeRecording?.requestId !== transition.requestId ||
          session.recordingStatus !== 'starting'
        ) {
          finalized = session?.activeRecording ?? recording;
          return;
        }
        const updated = await this.classSessionRepository.update(
          classId,
          (current) => ({
            ...current,
            isRecording: true,
            recordingStatus: 'active',
            activeRecording: {
              ...recording,
              requestId: transition.requestId,
              startedAt:
                current.activeRecording?.startedAt ?? new Date().toISOString(),
            },
          }),
        );
        finalized = updated.activeRecording;
      });

      await this.classroomHub.broadcastEvent(classId, {
        type: 'recording_state_changed',
        class_id: classId,
        enabled: true,
        egress_id: finalized.egressId,
      });
      this.metricsCollector?.increment('recording_start_success_total', {
        classId,
      });
      return finalized;
    } catch (error) {
      this.metricsCollector?.increment('recording_start_failure_total', {
        classId,
      });
      await this.database.withLocks([`session:${classId}`], async () => {
        const session = await this.classSessionRepository.getById(classId);
        if (session?.activeRecording?.requestId !== transition.requestId) {
          return;
        }
        await this.classSessionRepository.update(classId, (current) => ({
          ...current,
          isRecording: false,
          recordingStatus: 'idle',
          activeRecording: null,
        }));
      });
      throw error;
    }
  }

  async stop({ classId, whiteboardComposition = null }) {
    let transition = null;

    await this.database.withLocks([`session:${classId}`], async () => {
      const session = await this.classSessionRepository.getById(classId);
      if (!session) {
        throw new HttpError(404, 'Class session not found');
      }

      if (session.recordingStatus === 'stopping') {
        transition = {
          alreadyStopping: true,
          recording: session.activeRecording,
        };
        return;
      }

      if (!session.activeRecording?.egressId) {
        transition = {
          alreadyStopped: true,
        };
        return;
      }

      const requestId = randomUUID();
      const updated = await this.classSessionRepository.update(classId, (current) => ({
        ...current,
        recordingStatus: 'stopping',
        activeRecording: {
          ...current.activeRecording,
          stopRequestId: requestId,
          status: 'stopping',
        },
      }));
      transition = {
        requestId,
        egressId: updated.activeRecording.egressId,
        rawRecordingPath: updated.activeRecording.filePath,
      };
    });

    if (transition.alreadyStopping) {
      return {
        status: 'stopping',
        egressId: transition.recording?.egressId ?? null,
        rawRecordingPath: transition.recording?.filePath ?? '',
      };
    }

    if (transition.alreadyStopped) {
      return {
        status: 'already_stopped',
        egressId: null,
        rawRecordingPath: '',
      };
    }

    try {
      const stopped = await this.livekitClient.stopRoomRecording(transition.egressId);

      await this.database.withLocks([`session:${classId}`], async () => {
        const session = await this.classSessionRepository.getById(classId);
        if (session?.activeRecording?.stopRequestId !== transition.requestId) {
          return;
        }
        await this.classSessionRepository.update(classId, (current) => ({
          ...current,
          isRecording: false,
          recordingStatus: 'idle',
          activeRecording: null,
        }));
      });

      const job = await this.recordingJobRepository.create({
        classId,
        egressId: stopped.egressId,
        rawRecordingPath: transition.rawRecordingPath,
        status: 'queued',
        attempts: 0,
        result: {
          recordingUrl: transition.rawRecordingPath,
          whiteboardOverlay: whiteboardComposition,
        },
      });
      await this.recordingQueue.enqueue({
        jobId: job.id,
        classId,
        egressId: stopped.egressId,
        rawRecordingPath: transition.rawRecordingPath,
        whiteboardComposition,
      });

      await this.classroomHub.broadcastEvent(classId, {
        type: 'recording_state_changed',
        class_id: classId,
        enabled: false,
        egress_id: stopped.egressId,
      });
      this.metricsCollector?.increment('recording_stop_success_total', {
        classId,
      });

      this.latestReplayByClass.set(classId, {
        classId,
        videoUrl: transition.rawRecordingPath,
        conceptIndex: [],
        transcript: [],
        whiteboardOverlay: whiteboardComposition,
      });

      return {
        egressId: stopped.egressId,
        rawRecordingPath: transition.rawRecordingPath,
        status: stopped.status,
        processingJobId: job.id,
        processingJobStatus: job.status,
      };
    } catch (error) {
      this.metricsCollector?.increment('recording_stop_failure_total', {
        classId,
      });
      await this.database.withLocks([`session:${classId}`], async () => {
        const session = await this.classSessionRepository.getById(classId);
        if (session?.activeRecording?.stopRequestId !== transition.requestId) {
          return;
        }
        await this.classSessionRepository.update(classId, (current) => ({
          ...current,
          isRecording: true,
          recordingStatus: 'active',
          activeRecording: {
            ...current.activeRecording,
            status: 'active',
            stopRequestId: null,
          },
        }));
      });
      throw error;
    }
  }

  async queueProcessing({ classId, rawRecordingPath, whiteboardComposition = null }) {
    const job = await this.recordingJobRepository.create({
      classId,
      rawRecordingPath,
      status: 'queued',
      attempts: 0,
      result: whiteboardComposition
        ? {
            whiteboardOverlay: whiteboardComposition,
            recordingUrl: rawRecordingPath,
          }
        : null,
    });
    await this.recordingQueue.enqueue({
      jobId: job.id,
      classId,
      rawRecordingPath,
      whiteboardComposition,
    });
    return {
      jobId: job.id,
      status: 'queued',
    };
  }

  async getProcessingStatus(jobId) {
    const job = await this.recordingJobRepository.getById(jobId);
    if (!job) {
      throw new HttpError(404, 'Recording job not found');
    }
    return job.status;
  }

  async getProcessingResult(jobId) {
    const job = await this.recordingJobRepository.getById(jobId);
    if (!job) {
      throw new HttpError(404, 'Recording job not found');
    }
    const playback = derivePlaybackPayload(job.result, job.rawRecordingPath);
    return {
      recording_url: playback.publicUrl,
      video_url: playback.publicUrl,
      playback_provider: playback.playbackProvider,
      video_provider: playback.playbackProvider,
      video_id: playback.videoId,
      youtube_video_id: playback.videoId,
      embed_url: playback.embedUrl,
      youtube_embed_url: playback.embedUrl,
      title: playback.title,
      description: playback.description,
      metadata: playback.metadata,
      transcript: job.result?.transcript ?? [],
      notes: job.result?.notes ?? null,
      flashcards: job.result?.flashcards ?? null,
      summary: job.result?.summary ?? null,
      concept_index: job.result?.conceptIndex ?? [],
      whiteboard_overlay: job.result?.whiteboardOverlay ?? null,
      error: job.error,
    };
  }

  async getReplay(classId) {
    const latestJob = await this.recordingJobRepository.getLatestByClassId(classId);
    if (latestJob?.result) {
      const playback = derivePlaybackPayload(latestJob.result, latestJob.rawRecordingPath);
      return {
        classId,
        videoUrl: playback.publicUrl,
        playbackProvider: playback.playbackProvider,
        videoId: playback.videoId,
        embedUrl: playback.embedUrl,
        title: playback.title,
        description: playback.description,
        metadata: playback.metadata,
        conceptIndex: latestJob.result.conceptIndex ?? [],
        transcript: latestJob.result.transcript ?? [],
        whiteboardOverlay: latestJob.result.whiteboardOverlay ?? null,
      };
    }
    const fallback = this.latestReplayByClass.get(classId) ?? {
      classId,
      videoUrl: latestJob?.rawRecordingPath ?? '',
      conceptIndex: latestJob?.result?.conceptIndex ?? [],
      transcript: latestJob?.result?.transcript ?? [],
      whiteboardOverlay: latestJob?.result?.whiteboardOverlay ?? null,
    };
    const playback = derivePlaybackPayload(
      {
        recordingUrl: fallback.videoUrl,
        playbackProvider: fallback.playbackProvider,
        videoId: fallback.videoId,
        embedUrl: fallback.embedUrl,
        title: fallback.title,
        description: fallback.description,
        metadata: fallback.metadata,
      },
      latestJob?.rawRecordingPath ?? fallback.videoUrl,
    );
    return (
      {
        classId,
        videoUrl: playback.publicUrl,
        playbackProvider: playback.playbackProvider,
        videoId: playback.videoId,
        embedUrl: playback.embedUrl,
        title: playback.title,
        description: playback.description,
        metadata: playback.metadata,
        conceptIndex: fallback.conceptIndex ?? [],
        transcript: fallback.transcript ?? [],
        whiteboardOverlay: fallback.whiteboardOverlay ?? null,
      }
    );
  }
}
