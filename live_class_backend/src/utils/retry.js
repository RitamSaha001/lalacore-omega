import { setTimeout as sleep } from 'node:timers/promises';

function isTransient(error) {
  const code = error?.code ?? '';
  return [
    '40001',
    '40P01',
    'ECONNRESET',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'EPIPE',
  ].includes(code);
}

export async function withRetry(
  operation,
  {
    retries = 3,
    initialDelayMs = 250,
    maxDelayMs = 2000,
    multiplier = 2,
    jitterRatio = 0.2,
    shouldRetry = isTransient,
    onRetry = null,
  } = {},
) {
  let attempt = 0;
  let delayMs = initialDelayMs;

  while (true) {
    try {
      return await operation();
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !shouldRetry(error)) {
        throw error;
      }

      const jitter = delayMs * jitterRatio * Math.random();
      const waitMs = Math.min(maxDelayMs, delayMs + jitter);
      await onRetry?.({
        attempt,
        waitMs,
        error,
      });
      await sleep(waitMs);
      delayMs = Math.min(maxDelayMs, delayMs * multiplier);
    }
  }
}
