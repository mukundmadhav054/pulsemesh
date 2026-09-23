# PulseMesh — Redis Streams task queue

Distributed background task engine: ingestion API → partition-keyed Redis
Streams → idempotent worker pool → PostgreSQL, with Prometheus telemetry.

## Quickstart

```powershell
npm install; if ($?) { npm run build }; if ($?) { npm test }
npm start
```

- `POST /ingest` with header `Idempotency-Key: <key>` and JSON body
  `{ "type": "...", "partitionKey": "shard-N", "payload": {...} }` → `202`.
- `GET /health` → `{ "ok": true }`.
- `GET /metrics` → Prometheus exposition (queue lag, throughput, error rate).

By default the broker runs in-memory. Set `REDIS_URL=redis://localhost:6379`
to use a real Redis server (same XACK/XAUTOCLAIM semantics).

## Docker

```powershell
docker compose config
docker compose up --build
```

Services: `api` (:3000), `redis` (:6379), `postgres` (:5432).

## Layout

- `src/producer/` — ingestion API, zod-style validation, sliding-window rate limiting, mandatory idempotency keys.
- `src/broker/` — Redis Streams abstraction (in-memory fake + real client), consumer-group ack tracking, XAUTOCLAIM reclaim, partition keys.
- `src/workers/` — idempotent worker pool, exponential backoff + jitter, DLQ after 5 retries.
- `src/storage/` — Prisma-style `schema.prisma`, `MIGRATIONS.md` (partitioned time-series DDL), `batch.ts` (batch inserts + pooling).
- `src/telemetry/` — Prometheus exporter (`queue lag`, `throughput`, `error rate`).
- `src/schemas.ts` — versioned typed payload envelope.
- `k6/burst.js` — burst load script (`k6 run k6/burst.js`, `BASE_URL` override).

## Defense (4 rungs)

1. **Explain:** ingestion nodes validate payloads and push jobs into
   partition-keyed Redis Streams; worker pools consume via consumer groups
   (XACK) and persist status batches into PostgreSQL; `/metrics` exposes lag,
   throughput, and error rates.
2. **Justify:** Redis Streams give append-only log semantics with consumer-group
   auto-rebalancing without the operational overhead of Kafka at this footprint.
3. **Trade-off:** memory-bounded streams yield fast enqueues but need bounding,
   snapshotting, and a DLQ for poison pills; Postgres batching trades a little
   write freshness for much higher insert throughput.
4. **Scale & failure:** DLQ after 5 retries; idle-time XAUTOCLAIM scanners
   reclaim tasks from crashed workers; idempotency keys make every retry safe;
   rate limiting sheds overload at the edge.
