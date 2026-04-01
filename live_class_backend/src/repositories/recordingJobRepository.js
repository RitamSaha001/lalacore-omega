import { createRecordingJob } from '../models/recordingJob.js';

export class RecordingJobRepository {
  constructor(db) {
    this.db = db;
  }

  async create(job) {
    const next = createRecordingJob(job);
    this.db.recordingJobs.set(next.id, next);
    return next;
  }

  async update(jobId, updater) {
    const current = this.db.recordingJobs.get(jobId) ?? null;
    if (!current) {
      return null;
    }
    const updated = updater(current);
    const next = {
      ...updated,
      id: current.id,
      updatedAt: new Date().toISOString(),
    };
    this.db.recordingJobs.set(jobId, next);
    return next;
  }

  async getById(jobId) {
    return this.db.recordingJobs.get(jobId) ?? null;
  }

  async getLatestByClassId(classId) {
    const jobs = [...this.db.recordingJobs.values()].filter(
      (job) => job.classId === classId,
    );
    jobs.sort((left, right) => right.createdAt.localeCompare(left.createdAt));
    return jobs[0] ?? null;
  }
}
