import { assertValidConfig, config } from '../config/env.js';
import { createLogger } from '../utils/logger.js';
import { createPersistenceLayer } from '../repositories/persistenceFactory.js';
import { RecordingQueue } from '../queue/recordingQueue.js';
import { AiPipelineClient } from '../services/aiPipelineClient.js';
import { YouTubeUploadService } from '../services/youtubeUploadService.js';

const logger = createLogger('recording_worker');
assertValidConfig(config, { service: 'worker' });
const { repositories, database } = await createPersistenceLayer({
  config,
  logger,
});
const recordingQueue = new RecordingQueue({
  config,
  logger,
});
const aiPipelineClient = new AiPipelineClient({
  config,
  logger,
});
const youTubeUploadService = new YouTubeUploadService({
  config,
  logger,
});

const worker = recordingQueue.createWorker(async (job) => {
  const existing = await repositories.recordingJobRepository.getById(job.data.jobId);
  if (!existing) {
    throw new Error(`Recording job not found: ${job.data.jobId}`);
  }

  await repositories.recordingJobRepository.update(existing.id, (current) => ({
    ...current,
    status: 'processing',
    attempts: current.attempts + 1,
    error: null,
  }));

  try {
    const transcription = await aiPipelineClient.transcribe(
      job.data.rawRecordingPath,
    );
    const transcript = transcription.transcript ?? [];
    const session = await repositories.classSessionRepository.getById(
      existing.classId,
    );
    let uploadResult = null;
    let uploadWarning = null;
    const [notes, flashcards, summary] = await Promise.all([
      aiPipelineClient.generateNotes(transcript),
      aiPipelineClient.generateFlashcards(transcript),
      aiPipelineClient.generateSummary(transcript),
    ]);
    if (youTubeUploadService.isConfigured()) {
      try {
        uploadResult = await youTubeUploadService.upload({
          classId: existing.classId,
          session,
          rawRecordingPath: job.data.rawRecordingPath,
          transcript,
        });
      } catch (error) {
        if (youTubeUploadService.isRequired()) {
          throw error;
        }
        uploadWarning = String(error);
        logger.warn('recording_youtube_upload_skipped', {
          jobId: existing.id,
          classId: existing.classId,
          error: uploadWarning,
        });
      }
    }

    await repositories.recordingJobRepository.update(existing.id, (current) => ({
      ...current,
      status: 'completed',
      result: {
        ...(current.result ?? {}),
        transcript,
        notes,
        flashcards,
        summary,
        recordingUrl:
          uploadResult?.recordingUrl ??
          current.result?.recordingUrl ??
          current.rawRecordingPath,
        playbackProvider:
          uploadResult?.playbackProvider ??
          current.result?.playbackProvider ??
          current.result?.videoProvider ??
          '',
        videoId:
          uploadResult?.videoId ??
          current.result?.videoId ??
          current.result?.youtubeVideoId ??
          '',
        youtubeVideoId:
          uploadResult?.videoId ??
          current.result?.youtubeVideoId ??
          current.result?.videoId ??
          '',
        embedUrl:
          uploadResult?.embedUrl ??
          current.result?.embedUrl ??
          current.result?.youtubeEmbedUrl ??
          '',
        youtubeEmbedUrl:
          uploadResult?.embedUrl ??
          current.result?.youtubeEmbedUrl ??
          current.result?.embedUrl ??
          '',
        title: uploadResult?.title ?? current.result?.title ?? session?.title ?? '',
        description:
          uploadResult?.description ?? current.result?.description ?? '',
        metadata: {
          ...(
            current.result?.metadata && typeof current.result.metadata === 'object'
              ? current.result.metadata
              : {}
          ),
          ...(
            uploadResult?.metadata && typeof uploadResult.metadata === 'object'
              ? uploadResult.metadata
              : {}
          ),
          ...(uploadWarning ? { youtube_upload_error: uploadWarning } : {}),
        },
        whiteboardOverlay:
          current.result?.whiteboardOverlay ??
          job.data.whiteboardComposition ??
          null,
      },
      error: null,
    }));

    logger.info('recording_job_completed', {
      jobId: existing.id,
      classId: existing.classId,
    });
  } catch (error) {
    await repositories.recordingJobRepository.update(existing.id, (current) => ({
      ...current,
      status: 'failed',
      error: {
        message: String(error),
      },
    }));
    logger.error('recording_job_failed', {
      jobId: existing.id,
      classId: existing.classId,
      error: String(error),
    });
    throw error;
  }
});

worker.on('failed', (job, error) => {
  logger.error('recording_worker_failed', {
    jobId: job?.id ?? null,
    error: String(error),
  });
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    void worker.close().finally(async () => {
      await recordingQueue.close();
      await database.close?.();
      process.exit(0);
    });
  });
}
