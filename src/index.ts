/** Wire-up: ingestion HTTP API + worker pool + Prometheus /metrics. */
import * as http from 'http';
import { InMemoryStreams } from './broker/streams';
import { IngestService } from './producer/index';
import { WorkerPool } from './workers/pool';
import { createMetrics } from './telemetry/metrics';

const PORT = Number(process.env['PORT'] ?? 3000);
const STREAM = 'pulsemesh:tasks';
const GROUP = 'pulsemesh-workers';

const streams = new InMemoryStreams();
const ingest = new IngestService(streams, { stream: STREAM });
const metrics = createMetrics();
const pool = new WorkerPool(
  streams,
  async (ctx) => {
    const end = metrics.processingDuration.startTimer({ type: ctx.type });
    // Default handler: succeed (replace with real side effects).
    metrics.processed.inc({ type: ctx.type });
    end();
  },
  { stream: STREAM, group: GROUP },
);

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk: unknown) => {
      data += String(chunk);
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

export function createServer(): http.Server {
  return http.createServer(async (req, res) => {
    try {
      if (req.method === 'GET' && req.url === '/health') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
        return;
      }
      if (req.method === 'GET' && req.url === '/metrics') {
        // Refresh queue gauges from broker state.
        const pending = streams.xpending(STREAM, GROUP);
        metrics.queueDepth.set({ stream: STREAM }, pending.length);
        const oldest = pending.reduce((min, r) => Math.min(min, r.enqueuedAt), Date.now());
        metrics.queueLagMs.set({ stream: STREAM }, pending.length === 0 ? 0 : Date.now() - oldest);
        const body = await metrics.register.metrics();
        res.writeHead(200, { 'content-type': metrics.register.contentType });
        res.end(body);
        return;
      }
      if (req.method === 'POST' && req.url === '/ingest') {
        const raw = await readBody(req);
        const parsed = JSON.parse(raw) as {
          type: string;
          partitionKey: string;
          payload: unknown;
        };
        const idempotencyKey =
          (req.headers['idempotency-key'] as string | undefined) ??
          (parsed as { idempotencyKey?: string }).idempotencyKey;
        const result = ingest.ingest({
          type: parsed.type,
          partitionKey: parsed.partitionKey,
          payload: parsed.payload,
          idempotencyKey,
        });
        metrics.enqueued.inc({ type: parsed.type });
        res.writeHead(202, { 'content-type': 'application/json' });
        res.end(JSON.stringify(result));
        return;
      }
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: 'not found' }));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const status =
        err instanceof Error && err.name === 'RateLimitError' ? 429 : 400;
      res.writeHead(status, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: message }));
    }
  });
}

if (require.main === module) {
  const server = createServer();
  server.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`pulsemesh api listening on :${PORT}`);
  });
  void pool.start('api-worker');
}
