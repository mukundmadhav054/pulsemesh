import { createMetrics, renderMetrics } from '../src/telemetry/metrics';

describe('metrics shape', () => {
  it('exposes queue lag, throughput, and error-rate series', async () => {
    const m = createMetrics('test_pm_');
    m.enqueued.inc({ type: 'telemetry.ingest' }, 10);
    m.processed.inc({ type: 'telemetry.ingest' }, 9);
    m.failed.inc({ type: 'telemetry.ingest' }, 1);
    m.dlq.inc({ type: 'telemetry.ingest' }, 1);
    m.queueDepth.set({ stream: 'pulsemesh:tasks' }, 3);
    m.queueLagMs.set({ stream: 'pulsemesh:tasks' }, 250);
    m.processingDuration.observe({ type: 'telemetry.ingest' }, 0.02);

    const text = await renderMetrics(m);
    for (const series of [
      'test_pm_enqueued_total',
      'test_pm_processed_total',
      'test_pm_failed_total',
      'test_pm_dlq_total',
      'test_pm_queue_depth',
      'test_pm_queue_lag_ms',
      'test_pm_processing_duration_seconds',
    ]) {
      expect(text).toContain(series);
    }
    expect(text).toContain('type="telemetry.ingest"');
  });
});
