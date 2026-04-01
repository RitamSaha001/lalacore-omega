import { HttpError } from '../utils/httpError.js';
import { CircuitBreaker } from '../utils/circuitBreaker.js';
import { withRetry } from '../utils/retry.js';

export class AiPipelineClient {
  constructor({ config, logger }) {
    this.config = config;
    this.logger = logger;
    this.breaker = new CircuitBreaker({
      name: 'ai_pipeline',
      failureThreshold: 5,
      cooldownMs: 30000,
      successThreshold: 2,
      logger,
    });
  }

  assertConfigured() {
    if (!this.config.ai.baseUrl) {
      throw new HttpError(503, 'AI pipeline base URL is not configured');
    }
  }

  async transcribe(rawRecordingPath) {
    return this.post('/transcribe', {
      recording_path: rawRecordingPath,
    });
  }

  async generateNotes(transcript) {
    return this.post('/notes', {
      transcript,
    });
  }

  async generateFlashcards(transcript) {
    return this.post('/flashcards', {
      transcript,
    });
  }

  async generateSummary(transcript) {
    return this.post('/summary', {
      transcript,
    });
  }

  async post(pathname, body) {
    this.assertConfigured();
    const url = `${this.config.ai.baseUrl}${pathname}`;
    return withRetry(
      async () =>
        this.breaker.execute(async () => {
          const response = await fetch(url, {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              ...(this.config.ai.apiKey
                ? {
                    authorization: `Bearer ${this.config.ai.apiKey}`,
                  }
                : {}),
            },
            body: JSON.stringify(body),
          });

          if (!response.ok) {
            throw new HttpError(
              response.status,
              `AI pipeline request failed: ${response.status}`,
            );
          }

          return response.json();
        }),
      {
        retries: 2,
        onRetry: async ({ attempt, waitMs, error }) => {
          this.logger.warn('ai_pipeline_retry', {
            attempt,
            waitMs,
            error: String(error),
            url,
          });
        },
      },
    );
  }
}
