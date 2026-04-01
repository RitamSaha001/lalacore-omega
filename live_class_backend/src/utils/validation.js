import { HttpError } from './httpError.js';

export function requireString(value, fieldName) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new HttpError(400, `${fieldName} is required`);
  }
  return value.trim();
}

export function optionalString(value, fallback = null) {
  if (typeof value !== 'string') {
    return fallback;
  }
  const normalized = value.trim();
  return normalized.length === 0 ? fallback : normalized;
}

export function optionalBoolean(value, fallback = null) {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') {
      return true;
    }
    if (normalized === 'false') {
      return false;
    }
  }
  return fallback;
}

export function assertOneOf(value, allowed, fieldName) {
  if (!allowed.includes(value)) {
    throw new HttpError(
      400,
      `${fieldName} must be one of: ${allowed.join(', ')}`,
    );
  }
  return value;
}

export function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}
