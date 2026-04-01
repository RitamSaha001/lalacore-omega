import { AsyncLocalStorage } from 'node:async_hooks';

const requestContext = new AsyncLocalStorage();

function sanitize(fields = {}) {
  return Object.fromEntries(
    Object.entries(fields).filter(([, value]) => value !== undefined),
  );
}

export function getRequestContext() {
  return requestContext.getStore() ?? {};
}

export function runWithRequestContext(fields, callback) {
  const merged = {
    ...getRequestContext(),
    ...sanitize(fields),
  };
  return requestContext.run(merged, callback);
}

export function mergeRequestContext(fields) {
  const current = requestContext.getStore();
  if (!current) {
    return;
  }
  Object.assign(current, sanitize(fields));
}
