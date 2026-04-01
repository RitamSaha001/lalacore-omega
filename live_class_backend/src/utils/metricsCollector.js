function metricKey(name, tags = {}) {
  const normalizedTags = Object.entries(tags)
    .filter(([, value]) => value !== null && value !== undefined && `${value}` !== '')
    .sort(([left], [right]) => left.localeCompare(right));
  if (normalizedTags.length === 0) {
    return name;
  }
  return `${name}|${normalizedTags
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(',')}`;
}

function sanitizeMetricName(name) {
  return String(name).replace(/[^a-zA-Z0-9_:]/g, '_');
}

function formatTags(tags = {}) {
  const entries = Object.entries(tags).filter(
    ([, value]) => value !== null && value !== undefined && `${value}` !== '',
  );
  if (entries.length === 0) {
    return '';
  }
  const serialized = entries
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => {
      const escaped = String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
      return `${sanitizeMetricName(key)}="${escaped}"`;
    })
    .join(',');
  return `{${serialized}}`;
}

export class MetricsCollector {
  constructor() {
    this.counters = new Map();
    this.observations = new Map();
    this.gauges = new Map();
  }

  increment(name, tags = {}, value = 1) {
    const key = metricKey(name, tags);
    const current = this.counters.get(key) ?? {
      name,
      tags,
      value: 0,
    };
    current.value += value;
    this.counters.set(key, current);
  }

  observe(name, value, tags = {}) {
    const key = metricKey(name, tags);
    const current = this.observations.get(key) ?? {
      name,
      tags,
      count: 0,
      min: value,
      max: value,
      sum: 0,
    };
    current.count += 1;
    current.sum += value;
    current.min = Math.min(current.min, value);
    current.max = Math.max(current.max, value);
    this.observations.set(key, current);
  }

  setGauge(name, value, tags = {}) {
    const key = metricKey(name, tags);
    this.gauges.set(key, {
      name,
      tags,
      value,
    });
  }

  snapshot() {
    return {
      generated_at: new Date().toISOString(),
      counters: Array.from(this.counters.values()).sort((left, right) =>
        left.name.localeCompare(right.name),
      ),
      gauges: Array.from(this.gauges.values()).sort((left, right) =>
        left.name.localeCompare(right.name),
      ),
      observations: Array.from(this.observations.values())
        .map((entry) => ({
          ...entry,
          avg: entry.count === 0 ? 0 : entry.sum / entry.count,
        }))
        .sort((left, right) => left.name.localeCompare(right.name)),
    };
  }

  toPrometheus() {
    const lines = [];
    const emittedTypes = new Set();

    for (const entry of Array.from(this.counters.values()).sort((left, right) =>
      left.name.localeCompare(right.name),
    )) {
      const name = sanitizeMetricName(entry.name);
      if (!emittedTypes.has(`${name}:counter`)) {
        lines.push(`# TYPE ${name} counter`);
        emittedTypes.add(`${name}:counter`);
      }
      lines.push(`${name}${formatTags(entry.tags)} ${entry.value}`);
    }

    for (const entry of Array.from(this.gauges.values()).sort((left, right) =>
      left.name.localeCompare(right.name),
    )) {
      const name = sanitizeMetricName(entry.name);
      if (!emittedTypes.has(`${name}:gauge`)) {
        lines.push(`# TYPE ${name} gauge`);
        emittedTypes.add(`${name}:gauge`);
      }
      lines.push(`${name}${formatTags(entry.tags)} ${entry.value}`);
    }

    for (const entry of Array.from(this.observations.values()).sort(
      (left, right) => left.name.localeCompare(right.name),
    )) {
      const base = sanitizeMetricName(entry.name);
      const tags = formatTags(entry.tags);
      const avg = entry.count === 0 ? 0 : entry.sum / entry.count;
      if (!emittedTypes.has(`${base}_count:counter`)) {
        lines.push(`# TYPE ${base}_count counter`);
        emittedTypes.add(`${base}_count:counter`);
      }
      lines.push(`${base}_count${tags} ${entry.count}`);
      if (!emittedTypes.has(`${base}_sum:counter`)) {
        lines.push(`# TYPE ${base}_sum counter`);
        emittedTypes.add(`${base}_sum:counter`);
      }
      lines.push(`${base}_sum${tags} ${entry.sum}`);
      if (!emittedTypes.has(`${base}_min:gauge`)) {
        lines.push(`# TYPE ${base}_min gauge`);
        emittedTypes.add(`${base}_min:gauge`);
      }
      lines.push(`${base}_min${tags} ${entry.min}`);
      if (!emittedTypes.has(`${base}_max:gauge`)) {
        lines.push(`# TYPE ${base}_max gauge`);
        emittedTypes.add(`${base}_max:gauge`);
      }
      lines.push(`${base}_max${tags} ${entry.max}`);
      if (!emittedTypes.has(`${base}_avg:gauge`)) {
        lines.push(`# TYPE ${base}_avg gauge`);
        emittedTypes.add(`${base}_avg:gauge`);
      }
      lines.push(`${base}_avg${tags} ${avg}`);
    }

    return `${lines.join('\n')}\n`;
  }
}
