/**
 * Prometheus metrics exporter: queue lag, throughput, error rate.
 * Served at GET /metrics in the API entrypoint.
 */
import * as client from 'prom-client';

export interface PulseMeshMetrics {
  register: client.Registry;
  enqueued: client.Counter<string>;
  processed: client.Counter<string>;
  failed: client.Counter<string>;
  dlq: client.Counter<string>;
  queueDepth: client.Gauge<string>;
  queueLagMs: client.Gauge<string>;
  processingDuration: client.Histogram<string>;
}

export function createMetrics(prefix = 'pulsemesh_'): PulseMeshMetrics {
  const register = new client.Registry();
  register.setDefaultLabels({ service: 'pulsemesh' });
  client.collectDefaultMetrics({ register, prefix });

  const enqueued = new client.Counter({
    name: `${prefix}enqueued_total`,
    help: 'Total tasks accepted by the ingestion API.',
    labelNames: ['type'],
    registers: [register],
  });
  const processed = new client.Counter({
    name: `${prefix}processed_total`,
    help: 'Total tasks processed successfully by workers.',
    labelNames: ['type'],
    registers: [register],
  });
  const failed = new client.Counter({
    name: `${prefix}failed_total`,
    help: 'Total task processing failures (including retries).',
    labelNames: ['type'],
    registers: [register],
  });
  const dlq = new client.Counter({
    name: `${prefix}dlq_total`,
    help: 'Total tasks moved to the Dead Letter Queue.',
    labelNames: ['type'],
    registers: [register],
  });
  const queueDepth = new client.Gauge({
    name: `${prefix}queue_depth`,
    help: 'Number of pending (unacked) entries per stream.',
    labelNames: ['stream'],
    registers: [register],
  });
  const queueLagMs = new client.Gauge({
    name: `${prefix}queue_lag_ms`,
    help: 'Age in ms of the oldest pending entry per stream.',
    labelNames: ['stream'],
    registers: [register],
  });
  const processingDuration = new client.Histogram({
    name: `${prefix}processing_duration_seconds`,
    help: 'Task processing duration in seconds.',
    labelNames: ['type'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [register],
  });

  return { register, enqueued, processed, failed, dlq, queueDepth, queueLagMs, processingDuration };
}

/** Shape assertion helper used by tests: every expected series exists. */
export async function renderMetrics(m: PulseMeshMetrics): Promise<string> {
  return m.register.metrics();
}

export type MetricsSnapshot = Record<string, number>;

export async function snapshotCounters(m: PulseMeshMetrics): Promise<MetricsSnapshot> {
  const json = await m.register.getMetricsAsJSON();
  const out: MetricsSnapshot = {};
  for (const metric of json) {
    if (metric.name.endsWith('_total') || metric.name.includes('queue_')) {
      for (const v of metric.values ?? []) {
        const labels = Object.entries(v.labels ?? {})
          .map(([k, val]) => `${k}="${val}"`)
          .join(',');
        out[`${metric.name}{${labels}}`] = v.value;
      }
    }
  }
  return out;
}
