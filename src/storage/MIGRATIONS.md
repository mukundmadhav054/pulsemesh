# Storage migrations note

No live database is required for unit tests. When `DATABASE_URL` is set,
apply migrations in order:

1. `001_init.sql` — creates the partitioned `task_events` table
   (`PARTITION BY RANGE (occurred_at)`), the `task_checkpoints` table,
   and indexes on `(idempotency_key)`, `(partition_key, occurred_at)`,
   `(status, occurred_at)`.
2. `002_monthly_partition.sql` — template for creating the next monthly
   partition (`task_events_YYYYMM`) with a `DETACH`/archive step for
   partitions older than the retention window.

## Reference DDL (PostgreSQL 16)

```sql
CREATE TABLE task_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  stream TEXT NOT NULL,
  entry_id TEXT NOT NULL,
  type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ENQUEUED',
  attempt INT NOT NULL DEFAULT 0,
  error TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (stream, entry_id)
) PARTITION BY RANGE (occurred_at);

CREATE TABLE task_events_default PARTITION OF task_events DEFAULT;

-- Monthly partition example:
CREATE TABLE task_events_202601 PARTITION OF task_events
  FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE INDEX ON task_events (idempotency_key);
CREATE INDEX ON task_events (partition_key, occurred_at);
CREATE INDEX ON task_events (status, occurred_at);

CREATE TABLE task_checkpoints (
  consumer_group TEXT NOT NULL,
  stream TEXT NOT NULL,
  last_entry_id TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, stream)
);
```

## Connection pooling

Use a pool of `max(4, cpuCount)` connections with a 5s acquire timeout
(see `src/storage/batch.ts` `PoolConfig`). Writes go through
`TaskEventBatchWriter`, which flushes either every `flushIntervalMs`
or when `batchSize` rows accumulate, using a single multi-row `INSERT`.
