import { getRequestContext } from '../observability/requestContext.js';

function write(level, component, event, bindings = {}, fields = {}) {
  const entry = {
    timestamp: new Date().toISOString(),
    level,
    component,
    event,
    ...getRequestContext(),
    ...bindings,
    ...fields,
  };

  const line = JSON.stringify(entry);
  if (level === 'error') {
    console.error(line);
    return;
  }
  if (level === 'warn') {
    console.warn(line);
    return;
  }
  console.log(line);
}

export function createLogger(component, bindings = {}) {
  return {
    debug(event, fields) {
      write('debug', component, event, bindings, fields);
    },
    info(event, fields) {
      write('info', component, event, bindings, fields);
    },
    warn(event, fields) {
      write('warn', component, event, bindings, fields);
    },
    error(event, fields) {
      write('error', component, event, bindings, fields);
    },
    child(suffix, extraBindings = {}) {
      return createLogger(`${component}:${suffix}`, {
        ...bindings,
        ...extraBindings,
      });
    },
    withBindings(extraBindings = {}) {
      return createLogger(component, {
        ...bindings,
        ...extraBindings,
      });
    },
  };
}
