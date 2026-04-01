import { mapRecordingJobRow } from './mappers.js';

export class PostgresRecordingJobRepository {
  constructor(store) {
    this.store = store;
  }

  async create(job) {
    const { rows } = await this.store.query(
      `
        INSERT INTO recording_jobs (
          id,
          class_id,
          egress_id,
          raw_recording_path,
          status,
          attempts,
          result,
          error,
          created_at,
          updated_at
        )
        VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10)
        RETURNING *
      `,
      [
        job.id,
        job.classId,
        job.egressId,
        job.rawRecordingPath,
        job.status,
        job.attempts ?? 0,
        JSON.stringify(job.result),
        JSON.stringify(job.error),
        job.createdAt ?? new Date().toISOString(),
        job.updatedAt ?? new Date().toISOString(),
      ],
    );
    return mapRecordingJobRow(rows[0]);
  }

  async update(jobId, updater) {
    const current = await this.getById(jobId);
    if (!current) {
      return null;
    }
    const updated = updater(current);
    const { rows } = await this.store.query(
      `
        UPDATE recording_jobs
        SET status = $2,
            attempts = $3,
            result = $4::jsonb,
            error = $5::jsonb,
            raw_recording_path = $6,
            egress_id = $7,
            updated_at = NOW()
        WHERE id = $1
        RETURNING *
      `,
      [
        jobId,
        updated.status,
        updated.attempts,
        JSON.stringify(updated.result),
        JSON.stringify(updated.error),
        updated.rawRecordingPath,
        updated.egressId,
      ],
    );
    return mapRecordingJobRow(rows[0]);
  }

  async getById(jobId) {
    const { rows } = await this.store.query(
      'SELECT * FROM recording_jobs WHERE id = $1',
      [jobId],
    );
    return mapRecordingJobRow(rows[0]);
  }

  async getLatestByClassId(classId) {
    const { rows } = await this.store.query(
      `
        SELECT *
        FROM recording_jobs
        WHERE class_id = $1
        ORDER BY created_at DESC
        LIMIT 1
      `,
      [classId],
    );
    return mapRecordingJobRow(rows[0]);
  }
}
