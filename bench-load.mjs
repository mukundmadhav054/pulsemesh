/**
 * bench-load.mjs — zero-dependency measurement harness for PulseMesh.
 *
 * Proves the real service RUNS and records honest numbers:
 *  1. sustained ingest throughput (tasks/sec) against the live HTTP API
 *  2. end-to-end enqueue->processed latency (p50/p99) via /metrics polling
 *  3. idempotency-dedupe proof (same key twice -> one entry, one execution)
 *  4. retry->DLQ proof (poison task -> MAX_RETRIES attempts -> DLQ, in-process
 *     against the compiled dist/ WorkerPool, because the live server's default
 *     handler always succeeds and exposes no DLQ endpoint)
 *
 * Usage:
 *   node dist/index.js            # in one shell (PORT env, default 3000)
 *   BASE_URL=http://127.0.0.1:3000 node bench-load.mjs
 *
 * Env:
 *   BASE_URL   base URL of the live API (default http://127.0.0.1:3100)
 *   LOAD_N     burst size for the throughput phase (default 500; keep the
 *              TOTAL POSTs of a run under the server's default rate limit
 *              of 1000 req / 60s on the shared "default" bucket)
 *   LOAD_C     HTTP concurrency for the burst (default 25)
 *   LAT_N      latency samples (default 80)
 *   SUSTAIN_N  paced sends for the sustained phase (default 300)
 *   SUSTAIN_MS pacing window ms for the sustained phase (default 12000)
 *
 * Zero new prod deps: only node:http / node:module builtins.
 */
import http from 'node:http';
import { createRequire } from 'node:module';

const BASE_URL = process.env.BASE_URL ?? 'http://127.0.0.1:3100';
const LOAD_N = Number(process.env.LOAD_N ?? 500);
const LOAD_C = Number(process.env.LOAD_C ?? 25);
const LAT_N = Number(process.env.LAT_N ?? 80);
const SUSTAIN_N = Number(process.env.SUSTAIN_N ?? 300);
const SUSTAIN_MS = Number(process.env.SUSTAIN_MS ?? 12000);
const RUN_ID = `bench-${Date.now()}`;

const base = new URL(BASE_URL);

function request(method, path, { headers = {}, body = null } = {}) {
  return new Promise((resolve, reject) => {
    const t0 = Date.now();
    const req = http.request(
      {
        hostname: base.hostname,
        port: base.port,
        path,
        method,
        headers: { 'content-type': 'application/json', ...headers },
      },
      (res) => {
        let data = '';
        res.on('data', (c) => {
          data += c;
        });
        res.on('end', () =>
          resolve({ status: res.statusCode ?? 0, body: data, ms: Date.now() - t0 }),
        );
      },
    );
    req.on('error', reject);
    if (body !== null) req.write(body);
    req.end();
  });
}

const get = (path) => request('GET', path);
const postIngest = (key, type, partitionKey, payload) =>
  request('POST', '/ingest', {
    headers: { 'idempotency-key': key },
    body: JSON.stringify({ type, partitionKey, payload }),
  });

function counterValue(metricsText, metric, type) {
  // e.g. pulsemesh_processed_total{service="pulsemesh",type="bench.load"} 12
  const re = new RegExp(
    `^${metric}\\{[^}]*type="${type}"[^}]*\\}\\s+(\\d+(?:\\.\\d+)?)\\s*$`,
    'm',
  );
  const m = metricsText.match(re);
  return m ? Number(m[1]) : 0;
}

async function metrics() {
  const r = await get('/metrics');
  if (r.status !== 200) throw new Error(`/metrics -> ${r.status}: ${r.body}`);
  return r.body;
}

async function waitForHealthy(timeoutMs = 15000) {
  const t0 = Date.now();
  for (;;) {
    try {
      const r = await get('/health');
      if (r.status === 200 && JSON.parse(r.body).ok === true) return;
    } catch {
      /* not up yet */
    }
    if (Date.now() - t0 > timeoutMs) throw new Error('server never became healthy');
    await new Promise((r) => setTimeout(r, 200));
  }
}

async function waitForProcessed(type, target, timeoutMs = 30000) {
  const t0 = Date.now();
  for (;;) {
    const n = counterValue(await metrics(), 'pulsemesh_processed_total', type);
    if (n >= target) return { n, waitedMs: Date.now() - t0 };
    if (Date.now() - t0 > timeoutMs)
      throw new Error(`drain timeout for type=${type}: ${n}/${target}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

function percentile(sortedAsc, p) {
  if (sortedAsc.length === 0) return 0;
  const idx = Math.min(sortedAsc.length - 1, Math.ceil((p / 100) * sortedAsc.length) - 1);
  return sortedAsc[Math.max(0, idx)];
}

async function runPool(items, concurrency, fn) {
  // Simple worker pool over HTTP posts; returns results in input order.
  const out = new Array(items.length);
  let next = 0;
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    async () => {
      for (;;) {
        const i = next++;
        if (i >= items.length) return;
        out[i] = await fn(items[i], i);
      }
    },
  );
  await Promise.all(workers);
  return out;
}

// ---------------------------------------------------------------- main ----
const summary = { env: {}, phases: {} };

summary.env = {
  baseUrl: BASE_URL,
  platform: process.platform,
  arch: process.arch,
  node: process.version,
  runId: RUN_ID,
  loadN: LOAD_N,
  loadConcurrency: LOAD_C,
  latencySamples: LAT_N,
  sustainN: SUSTAIN_N,
  sustainWindowMs: SUSTAIN_MS,
};

await waitForHealthy();
console.log(`[bench] server healthy at ${BASE_URL}`);

// --- Phase A: idempotency-dedupe proof over HTTP --------------------------
const DEDUPE_TYPE = 'bench.dedupe';
const dedupeKey = `${RUN_ID}-dedupe-1`;
const d1 = await postIngest(dedupeKey, DEDUPE_TYPE, 'shard-1', { seq: 1 });
const d2 = await postIngest(dedupeKey, DEDUPE_TYPE, 'shard-1', { seq: 1 });
const d1b = JSON.parse(d1.body);
const d2b = JSON.parse(d2.body);
const dedupeSameEntry = d1.status === 202 && d2.status === 202 && d1b.entryId === d2b.entryId;
const dedupeDrain = await waitForProcessed(DEDUPE_TYPE, 1);
const dedupeProcessed = counterValue(await metrics(), 'pulsemesh_processed_total', DEDUPE_TYPE);
summary.phases.dedupe = {
  status1: d1.status,
  status2: d2.status,
  entryId1: d1b.entryId,
  entryId2: d2b.entryId,
  sameEntryId: dedupeSameEntry,
  processedTotal: dedupeProcessed,
  drainWaitedMs: dedupeDrain.waitedMs,
};
console.log(
  `[bench] dedupe: POST x2 same key -> ${d1.status}/${d2.status}, same entryId=${dedupeSameEntry} (${d1b.entryId}), processed=${dedupeProcessed}`,
);

// --- Phase B: end-to-end latency (sequential POST, poll /metrics) ---------
const LAT_TYPE = 'bench.lat';
const latencies = [];
for (let i = 0; i < LAT_N; i++) {
  const t0 = Date.now();
  const r = await postIngest(`${RUN_ID}-lat-${i}`, LAT_TYPE, `shard-${(i % 4) + 1}`, { i });
  if (r.status !== 202) throw new Error(`latency ingest ${i} -> ${r.status}: ${r.body}`);
  const before = counterValue(await metrics(), 'pulsemesh_processed_total', LAT_TYPE);
  const target = before + 1;
  for (;;) {
    const now = counterValue(await metrics(), 'pulsemesh_processed_total', LAT_TYPE);
    if (now >= target) break;
    if (Date.now() - t0 > 10000) throw new Error(`latency sample ${i} never processed`);
    await new Promise((r2) => setTimeout(r2, 5));
  }
  latencies.push(Date.now() - t0);
}
latencies.sort((a, b) => a - b);
summary.phases.latency = {
  type: LAT_TYPE,
  n: latencies.length,
  minMs: latencies[0],
  p50Ms: percentile(latencies, 50),
  p90Ms: percentile(latencies, 90),
  p99Ms: percentile(latencies, 99),
  maxMs: latencies[latencies.length - 1],
  meanMs: Number((latencies.reduce((a, b) => a + b, 0) / latencies.length).toFixed(2)),
};
console.log(
  `[bench] latency e2e (enqueue->processed): n=${latencies.length} min=${latencies[0]}ms p50=${summary.phases.latency.p50Ms}ms p90=${summary.phases.latency.p90Ms}ms p99=${summary.phases.latency.p99Ms}ms max=${latencies[latencies.length - 1]}ms mean=${summary.phases.latency.meanMs}ms`,
);

// --- Phase C: sustained ingest throughput (burst, concurrent POSTs) -------
const LOAD_TYPE = 'bench.load';
const loadItems = Array.from({ length: LOAD_N }, (_, i) => i);
const tStart = Date.now();
const loadResults = await runPool(
  loadItems,
  LOAD_C,
  (i) => postIngest(`${RUN_ID}-load-${i}`, LOAD_TYPE, `shard-${(i % 8) + 1}`, { i }),
);
const loadWallMs = Date.now() - tStart;
const accepted = loadResults.filter((r) => r.status === 202).length;
const rejected = LOAD_N - accepted;
const firstNon202 = loadResults.find((r) => r.status !== 202);
const throughput = Number(((accepted / loadWallMs) * 1000).toFixed(1));
const loadDrain = await waitForProcessed(LOAD_TYPE, accepted);
summary.phases.throughput = {
  type: LOAD_TYPE,
  sent: LOAD_N,
  accepted202: accepted,
  rejected,
  firstNon202: firstNon202 ? { status: firstNon202.status, body: firstNon202.body } : null,
  wallMs: loadWallMs,
  tasksPerSec: throughput,
  drainWaitedMs: loadDrain.waitedMs,
};
console.log(
  `[bench] throughput: ${accepted}/${LOAD_N} accepted (202), wall=${loadWallMs}ms -> ${throughput} tasks/sec, drain=${loadDrain.waitedMs}ms`,
);
if (rejected > 0) console.log(`[bench] WARNING: ${rejected} non-202 responses (see summary JSON)`);

// --- Phase C2: sustained ingest (paced evenly over ~SUSTAIN_MS) -----------
const SUSTAIN_TYPE = 'bench.sustain';
const sustainIntervalMs = SUSTAIN_MS / SUSTAIN_N;
const sStart = Date.now();
let sAccepted = 0;
let sFirstBad = null;
for (let i = 0; i < SUSTAIN_N; i++) {
  const due = sStart + Math.round(i * sustainIntervalMs);
  const wait = due - Date.now();
  if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  const r = await postIngest(`${RUN_ID}-sus-${i}`, SUSTAIN_TYPE, `shard-${(i % 8) + 1}`, { i });
  if (r.status === 202) sAccepted += 1;
  else if (!sFirstBad) sFirstBad = { index: i, status: r.status, body: r.body };
}
const sWallMs = Date.now() - sStart;
const sDrain = await waitForProcessed(SUSTAIN_TYPE, sAccepted);
summary.phases.sustained = {
  type: SUSTAIN_TYPE,
  sent: SUSTAIN_N,
  accepted202: sAccepted,
  rejected: SUSTAIN_N - sAccepted,
  firstNon202: sFirstBad,
  windowMs: sWallMs,
  tasksPerSec: Number(((sAccepted / sWallMs) * 1000).toFixed(1)),
  drainWaitedMs: sDrain.waitedMs,
};
console.log(
  `[bench] sustained: ${sAccepted}/${SUSTAIN_N} accepted (202) over ${sWallMs}ms -> ${summary.phases.sustained.tasksPerSec} tasks/sec, drain=${sDrain.waitedMs}ms`,
);

// --- Phase D: retry->DLQ proof (in-process, compiled dist/ code) ----------
// NOTE: the live server's default handler always succeeds and exposes no DLQ
// endpoint, so a poison pill cannot be driven over HTTP. This phase exercises
// the exact shipped WorkerPool/InMemoryStreams code from dist/ instead.
const require = createRequire(import.meta.url);
const { InMemoryStreams } = require('./dist/broker/streams.js');
const { WorkerPool, MAX_RETRIES } = require('./dist/workers/pool.js');
{
  const streams = new InMemoryStreams();
  const STREAM = 'pulsemesh:tasks';
  const GROUP = 'pulsemesh-workers';
  streams.xadd(STREAM, {
    idempotencyKey: `${RUN_ID}-poison-1`,
    partitionKey: 'shard-1',
    body: '{}',
    type: 'poison',
  });
  let handlerCalls = 0;
  const noSleep = async () => undefined;
  const pool = new WorkerPool(
    streams,
    () => {
      handlerCalls += 1;
      throw new Error('boom (deliberate poison pill)');
    },
    { sleep: noSleep, random: () => 0, stream: STREAM, group: GROUP },
  );
  for (let i = 0; i < MAX_RETRIES; i++) await pool.processOnce('bench-consumer');
  const stats = pool.getStats();
  const dlq = pool.dlqEntries;
  // One extra pass must not redeliver the acked DLQ entry.
  await pool.processOnce('bench-consumer');
  summary.phases.dlq = {
    mode: 'in-process against compiled dist/ WorkerPool (live default handler always succeeds)',
    maxRetries: MAX_RETRIES,
    handlerCalls,
    handlerCallsAfterExtraPass: handlerCalls,
    dlqEntries: dlq.length,
    dlqAttempts: dlq[0]?.attempts ?? null,
    stats,
    pendingAfter: streams.xpending(STREAM, GROUP).length,
  };
  console.log(
    `[bench] dlq: handlerCalls=${handlerCalls} (expect ${MAX_RETRIES}), dlqEntries=${dlq.length}, attempts=${dlq[0]?.attempts}, pendingAfter=${streams.xpending(STREAM, GROUP).length}`,
  );
}

// --- Final snapshot --------------------------------------------------------
const finalMetrics = await metrics();
const snapshot = {
  enqueuedDedupe: counterValue(finalMetrics, 'pulsemesh_enqueued_total', DEDUPE_TYPE),
  processedDedupe: counterValue(finalMetrics, 'pulsemesh_processed_total', DEDUPE_TYPE),
  enqueuedLat: counterValue(finalMetrics, 'pulsemesh_enqueued_total', LAT_TYPE),
  processedLat: counterValue(finalMetrics, 'pulsemesh_processed_total', LAT_TYPE),
  enqueuedLoad: counterValue(finalMetrics, 'pulsemesh_enqueued_total', LOAD_TYPE),
  processedLoad: counterValue(finalMetrics, 'pulsemesh_processed_total', LOAD_TYPE),
  enqueuedSustain: counterValue(finalMetrics, 'pulsemesh_enqueued_total', SUSTAIN_TYPE),
  processedSustain: counterValue(finalMetrics, 'pulsemesh_processed_total', SUSTAIN_TYPE),
};
summary.snapshot = snapshot;
console.log('[bench] snapshot:', JSON.stringify(snapshot));
console.log('[bench] SUMMARY_JSON_START');
console.log(JSON.stringify(summary, null, 2));
console.log('[bench] SUMMARY_JSON_END');
