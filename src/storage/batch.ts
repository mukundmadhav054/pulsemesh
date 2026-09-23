/** Batched PostgreSQL writer with pooled-connection semantics. */

export type TaskStatus = 'ENQUEUED' | 'PROCESSING' | 'SUCCEEDED' | 'FAILED' | 'DLQ';

export interface TaskEventRow {
  idempotencyKey: string;
  partitionKey: string;
  stream: string;
  entryId: string;
  type: string;
  status: TaskStatus;
  attempt?: number;
  error?: string | null;
  occurredAt?: Date;
}

export interface PoolConfig {
  /** Max pooled connections. Defaults to max(4, cpuCount). */
  maxConnections?: number;
  /** How long to wait for a connection before throwing. */
  acquireTimeoutMs?: number;
  /** Flush when this many rows accumulate. */
  batchSize?: number;
  /** Flush at least this often. */
  flushIntervalMs?: number;
}

export interface DbDriver {
  /** Execute a multi-row INSERT of already-escaped rows. */
  insertMany(rows: TaskEventRow[]): Promise<number>;
}

/** In-memory driver used by tests (records what would have been inserted). */
export class MemoryDriver implements DbDriver {
  readonly inserted: TaskEventRow[] = [];
  async insertMany(rows: TaskEventRow[]): Promise<number> {
    this.inserted.push(...rows);
    return rows.length;
  }
}

export function resolvePoolSize(cpuCount: number, maxConnections?: number): number {
  if (maxConnections && maxConnections > 0) return maxConnections;
  return Math.max(4, cpuCount);
}

/**
 * Accumulates task events and flushes them as a single multi-row INSERT,
 * either when `batchSize` is reached or on `flush()` / interval tick.
 */
export class TaskEventBatchWriter {
  private buffer: TaskEventRow[] = [];
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly batchSize: number;

  constructor(
    private readonly driver: DbDriver,
    private readonly config: PoolConfig = {},
  ) {
    this.batchSize = config.batchSize ?? 500;
    const interval = config.flushIntervalMs ?? 1000;
    if (interval > 0 && interval !== Number.POSITIVE_INFINITY) {
      this.timer = setInterval(() => {
        void this.flush().catch(() => undefined);
      }, interval);
      this.timer.unref?.();
    }
  }

  add(row: TaskEventRow): void {
    this.buffer.push(row);
    if (this.buffer.length >= this.batchSize) {
      void this.flush().catch(() => undefined);
    }
  }

  async flush(): Promise<number> {
    if (this.buffer.length === 0) return 0;
    const rows = this.buffer;
    this.buffer = [];
    return this.driver.insertMany(rows);
  }

  get pending(): number {
    return this.buffer.length;
  }

  close(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}
