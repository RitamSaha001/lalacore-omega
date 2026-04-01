import { HttpError } from './httpError.js';

export class CircuitBreaker {
  constructor({
    name,
    failureThreshold = 5,
    cooldownMs = 30000,
    successThreshold = 2,
    logger,
  }) {
    this.name = name;
    this.failureThreshold = failureThreshold;
    this.cooldownMs = cooldownMs;
    this.successThreshold = successThreshold;
    this.logger = logger;
    this.state = 'closed';
    this.failureCount = 0;
    this.successCount = 0;
    this.openedAt = 0;
  }

  canAttempt() {
    if (this.state === 'closed') {
      return true;
    }
    if (this.state === 'open') {
      if (Date.now() - this.openedAt >= this.cooldownMs) {
        this.state = 'half_open';
        this.successCount = 0;
        return true;
      }
      return false;
    }
    return true;
  }

  onSuccess() {
    if (this.state === 'half_open') {
      this.successCount += 1;
      if (this.successCount >= this.successThreshold) {
        this.state = 'closed';
        this.failureCount = 0;
        this.successCount = 0;
      }
      return;
    }
    this.failureCount = 0;
  }

  onFailure(error) {
    this.failureCount += 1;
    if (this.failureCount >= this.failureThreshold) {
      this.state = 'open';
      this.openedAt = Date.now();
      this.logger?.warn('circuit_breaker_opened', {
        circuit: this.name,
        error: String(error),
      });
    }
  }

  async execute(operation, { fallback = null } = {}) {
    if (!this.canAttempt()) {
      if (fallback) {
        return fallback();
      }
      throw new HttpError(
        503,
        `${this.name} is temporarily unavailable due to repeated failures`,
      );
    }

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure(error);
      if (fallback) {
        return fallback(error);
      }
      throw error;
    }
  }
}
