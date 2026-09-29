# PulseMesh

Distributed background task engine: ingestion API into a Redis-Streams-style broker with an idempotent worker pool, DLQ, and Prometheus telemetry.

## Live demo

- API: https://pulsemesh.onrender.com
- Health: https://pulsemesh.onrender.com/health
- Metrics: https://pulsemesh.onrender.com/metrics

[![Node >=20](https://img.shields.io/badge/node-%3E%3D20-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![TypeScript](https://img.shields.io/badge/language-TypeScript_5.5-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Runtime](https://img.shields.io/badge/runtime-Node_HTTP_no_framework-lightgrey)](https://nodejs.org/)
[![Broker](https://img.shields.io/badge/broker-Redis_Streams_style_in--memory_default-red)](https://redis.io/docs/latest/develop/data-types/streams/)
[![Metrics](https://img.shields.io/badge/metrics-Prometheus_exposition-orange)](https://prometheus.io/)

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Benchmarks](#benchmarks)
- [Tech stack](#tech-stack)
- [Quickstart](#quickstart)
- [Project structure](#project-structure)
- [Testing](#testing)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## Features

- HTTP ingestion API (`src/index.ts`): `POST /ingest` returns `202` with the stream `entryId` on accept.
- Mandatory idempotency: `Idempotency-Key` header (or `idempotencyKey` body field) dedupes enqueues; duplicate POSTs return the same `entryId`.
- Redis-Streams-style broker (`src/broker/streams`): in-memory default with the same XACK/XAUTOCLAIM consumer-group semantics as the real `redis` client.
- Worker pool (`src/workers/pool`): consumer group `pulsemesh-workers` on stream `pulsemesh:tasks`; poison pills go to a DLQ after 5 attempts.
- Prometheus telemetry (`src/telemetry/metrics` via `prom-client`): `pulsemesh_enqueued_total`, `pulsemesh_processed_total`, `pulsemesh_queue_depth`, `pulsemesh_queue_lag_ms`, plus processing-duration histograms.
- Dependency-free health check: `GET /health` returns `{ "ok": true }` with no Redis or Postgres required.
- Live queue gauges: `GET /metrics` refreshes `queueDepth` and `queueLagMs` from `xpending` before rendering the Prometheus exposition.
- Load tooling checked in: `k6/burst.js` (ramping-arrival-rate burst with `BASE_URL` override) and `bench-load.mjs` (zero new prod deps).
- Strict TypeScript build (`tsc -p tsconfig.json`) and Jest suite (`jest --runInBand`).

## Architecture

```text
                    +-------------------+
                    |     producer      |
                    |  POST /ingest     |
                    |  Idempotency-Key  |
                    +--------+----------+
                             |
                             v
                    +--------+----------+
                    |  Redis-Streams-   |
                    |  style broker     |
                    |  pulsemesh:tasks  |
                    |  group: pulsemesh-|
                    |  workers          |
                    +--------+----------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
 +------------------------+     +------------------------+
 | worker pool            |     | storage                |
 | consumer group + XACK  |---->| persisted task status  |
 | XAUTOCLAIM reclaim     |     | (batch inserts)        |
 | 5 retries -> DLQ       |     +------------------------+
 +-----------+------------+
             |
             v
 +------------------------+
 | /metrics (Prometheus)  |
 | queue lag / depth /    |
 | throughput / errors    |
 +------------------------+
```

Flow: producer validates and appends to a partition-keyed stream; the worker pool consumes via the consumer group, ACKs on success, reclaims stalled deliveries with XAUTOCLAIM, and moves poison pills to the DLQ after 5 attempts; `/metrics` exposes lag, depth, throughput, and error series.

## Benchmarks

Source of truth: `benchmarks/RESULTS.md` (harness `bench-load.mjs` against compiled `node dist/index.js`, in-memory broker, Node v24.15.0 on Windows 11). Headlines:

- "Measured 5,682 tasks/sec burst ingest (500x HTTP 202 in 88 ms, localhost) on a Redis-Streams-style task API (Node 24, Windows)."
- "Sustained 25 tasks/sec for 12 s with zero rejected requests; burst of 500 drained by the worker pool in 118 ms (Prometheus `/metrics`-verified)."
- "End-to-end enqueue->processed latency p50 63 ms / p99 65 ms (n=80, HTTP + 50 ms worker poll loop included)."
- "Proved idempotent exactly-once execution (duplicate key -> one entry, one run) and poison-pill handling (5 retries -> DLQ, 0 left pending)."

Caveats (read before quoting — see `benchmarks/RESULTS.md` section 3): latency is end-to-end over HTTP and its ~60 ms floor is dominated by the 50 ms worker poll interval, not stream-append cost; burst throughput is localhost HTTP accept rate against the in-memory broker; the DLQ phase runs in-process against the compiled `dist/` WorkerPool plus InMemoryStreams because the live server's default handler always succeeds and exposes no poison-pill endpoint.

## Tech stack

| Layer | Technology | Notes |
|---|---|---|
| Language | TypeScript 5.5 (strict) | Compiled with `tsc -p tsconfig.json` to `dist/index.js` |
| Runtime | Node.js >= 20, plain `node:http` | No web framework; `engines.node >= 20` in `package.json` |
| Broker | Redis-Streams-style (`redis` ^4.7.0 client available) | In-memory default; same XACK/XAUTOCLAIM semantics |
| Workers | In-repo worker pool | Consumer group `pulsemesh-workers`, 5 retries then DLQ |
| Metrics | `prom-client` ^15.1.3 | Prometheus exposition at `GET /metrics` |
| Tests | Jest 29 + ts-jest | `npm test` runs `jest --runInBand` |
| Load | k6 script + Node harness | `k6/burst.js`, `bench-load.mjs` (builtins only) |
| Deploy | Render Blueprint (`render.yaml`) | Single Node web service, health check `/health` |

## Quickstart

Prerequisites: Node.js >= 20.

```powershell
npm install; if ($?) { npm run build }; if ($?) { npm test }
$env:PORT="3000"; npm start
```

By default the broker runs in-memory — no Redis or Postgres required. `PORT` defaults to `3000` when unset (`src/index.ts`).

Endpoints:

- `POST /ingest` with `Idempotency-Key` header — returns `202` with `{ entryId }`.
- `GET /health` — returns `{ "ok": true }`.
- `GET /metrics` — Prometheus exposition (queue depth, lag, throughput).

PowerShell example:

```powershell
$body = '{"type":"telemetry.ingest","partitionKey":"shard-1","payload":{"seq":1}}'
Invoke-RestMethod -Method Post -Uri "http://localhost:3000/ingest" -Headers @{"Idempotency-Key"="demo-1"; "Content-Type"="application/json"} -Body $body
Invoke-RestMethod -Method Get -Uri "http://localhost:3000/health"
Invoke-RestMethod -Method Get -Uri "http://localhost:3000/metrics"
```

curl example:

```bash
curl -X POST http://localhost:3000/ingest \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"type":"telemetry.ingest","partitionKey":"shard-1","payload":{"seq":1}}'
curl http://localhost:3000/health
curl http://localhost:3000/metrics
```

Against the live demo, replace the host with `https://pulsemesh.onrender.com` (see links at the top).

## Project structure

```text
pulsemesh/
  src/index.ts            # HTTP API (POST /ingest, GET /health, GET /metrics) + wiring
  src/producer/index.ts   # IngestService: validation, rate limiting, idempotency keys
  src/broker/streams.ts   # InMemoryStreams + real Redis Streams abstraction
  src/workers/pool.ts     # WorkerPool: consumer group, XACK, XAUTOCLAIM, DLQ
  src/telemetry/metrics.ts# prom-client series (enqueued/processed/depth/lag/duration)
  package.json            # scripts: build (tsc), test (jest), start (node dist/index.js)
  tsconfig.json           # strict TypeScript config
  k6/burst.js             # k6 ramping-arrival-rate burst profile (BASE_URL override)
  bench-load.mjs          # checked-in Node load/latency/DLQ harness (builtins only)
  benchmarks/RESULTS.md   # measured numbers + repro commands + caveats
  render.yaml             # Render Blueprint: single Node web service
```

## Testing

- `npm test` — Jest suites (`jest --runInBand`): broker, worker, idempotency, and metrics suites (4 suites / 10 tests at the time of the benchmark run).
- `npm run build` — strict `tsc -p tsconfig.json`; must be clean before testing or benching.
- `node bench-load.mjs` — measured load/latency/DLQ harness (defaults: `LOAD_N=500` burst at concurrency 25, `LAT_N=80` samples, `SUSTAIN_N=300` paced over `SUSTAIN_MS=12000`). See `benchmarks/RESULTS.md` section 2 for the exact PowerShell repro.
- `k6 run k6/burst.js` — optional burst profile (k6 only; not installed in the benchmark run). Override target with `BASE_URL`, e.g. `$env:BASE_URL="https://pulsemesh.onrender.com"`.

## Deployment

- `render.yaml` defines a single Node web service (`name: pulsemesh`, `runtime: node`, `plan: free`, `rootDir: .`).
- Build: `npm install && npm run build`. Start: `npm start`. Health check: `/health`.
- `/health` is dependency-free (no Redis/Postgres), so it is safe for Render health checks and for cron-job.org keepalive pings (see the header comment in `render.yaml`).
- Free-tier note: the Render free plan sleeps on idle, so the first request after idle may be slow (cold start / wake). For demos, ping `https://pulsemesh.onrender.com/health` shortly before presenting, or schedule a cron-job.org job to hit `/health` every few minutes as a keepalive.

## Contributing

Small, focused PRs welcome: keep the broker/consumer-group semantics (XACK/XAUTOCLAIM, DLQ after 5 retries, mandatory idempotency keys), keep `npm run build` clean under strict TypeScript, keep `npm test` green, and update `benchmarks/RESULTS.md` instead of pasting numbers into descriptions when behaviour changes.

## License

Private research project — all rights reserved unless a LICENSE file states otherwise. Ask the repository owner before reusing code or benchmark figures outside this repo.
