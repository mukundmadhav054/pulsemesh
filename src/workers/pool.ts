/**
 * Idempotent worker pool: consumer-group consumption, exponential
 * backoff with jitter, Dead Letter Queue after MAX_RETRIES deliveries.
 */
import { InMemoryStreams, MAX_DELIVERIES } from '../broker/streams';

export const MAX_RETRIES = MAX_DELIVERIES;

export interface TaskContext {
  entryId: string;
  type: string;
  attempt: number;
  idempotencyKey: string;
  partitionKey: string;
  body: string;
}

export type TaskHandler = (ctx: TaskContext) => Promise<void> | void;

export interface PoolOptions {
  stream?: string;
  group?: string;
  /** Base delay for exponential backoff. */
  baseDelayMs?: number;
  /** Upper bound for a single retry delay. */
  maxDelayMs?: number;
  /** Sleep implementation (injectable for tests). */
  sleep?: (ms: number) => Promise<void>;
  /** Jitter source (injectable for deterministic tests). */
  random?: () => number;
}

export interface PoolStats {
  processed: number;
  failed: number;
  dlq: number;
  retried: number;
}

export interface DlqEntry {
  entryId: string;
  type: string;
  idempotencyKey: string;
  reason: string;
  attempts: number;
  enqueuedAt: number;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

export function computeBackoffMs(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
  random: () => number = Math.random,
): number {
  const exp = baseDelayMs * 2 ** Math.max(0, attempt - 1);
  const jitter = exp * 0.25 * random();
  return Math.min(exp + jitter, maxDelayMs);
}

export class WorkerPool {
  private readonly stream: string;
  private readonly group: string;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly random: () => number;
  private readonly processedKeys = new Set<string>();
  private readonly dlq: DlqEntry[] = [];
  private readonly stats: PoolStats = { processed: 0, failed: 0, dlq: 0, retried: 0 };
  private running = false;

  constructor(
    private readonly streams: InMemoryStreams,
    private readonly handler: TaskHandler,
    opts: PoolOptions = {},
  ) {
    this.stream = opts.stream ?? 'pulsemesh:tasks';
    this.group = opts.group ?? 'pulsemesh-workers';
    this.baseDelayMs = opts.baseDelayMs ?? 100;
    this.maxDelayMs = opts.maxDelayMs ?? 10_000;
    this.sleep = opts.sleep ?? defaultSleep;
    this.random = opts.random ?? Math.random;
  }

  get dlqEntries(): readonly DlqEntry[] {
    return this.dlq;
  }

  getStats(): PoolStats {
    return { ...this.stats };
  }

  /** Process a single batch once (used by tests and the poll loop). */
  async processOnce(consumer: string, count = 10): Promise<number> {
    const records = this.streams.xreadgroup(this.stream, this.group, consumer, count);
    for (const rec of records) {
      // Idempotency: skip keys already processed successfully.
      if (this.processedKeys.has(rec.idempotencyKey)) {
        this.streams.xack(this.stream, this.group, rec.id);
        this.stats.processed += 1;
        continue;
      }
      const ctx: TaskContext = {
        entryId: rec.id,
        type: rec.type,
        attempt: rec.deliveries,
        idempotencyKey: rec.idempotencyKey,
        partitionKey: rec.partitionKey,
        body: rec.body,
      };
      try {
        await this.handler(ctx);
        this.processedKeys.add(rec.idempotencyKey);
        this.streams.xack(this.stream, this.group, rec.id);
        this.stats.processed += 1;
      } catch (err) {
        const reason = err instanceof Error ? err.message : String(err);
        if (rec.deliveries >= MAX_RETRIES) {
          this.dlq.push({
            entryId: rec.id,
            type: rec.type,
            idempotencyKey: rec.idempotencyKey,
            reason,
            attempts: rec.deliveries,
            enqueuedAt: Date.now(),
          });
          this.streams.xack(this.stream, this.group, rec.id);
          this.stats.dlq += 1;
          this.stats.failed += 1;
        } else {
          this.stats.retried += 1;
          this.streams.release(this.stream, rec.id);
          const delay = computeBackoffMs(
            rec.deliveries,
            this.baseDelayMs,
            this.maxDelayMs,
            this.random,
          );
          await this.sleep(delay);
        }
      }
    }
    return records.length;
  }

  /** Reclaim entries from crashed consumers (XAUTOCLAIM) then process. */
  async reclaimIdle(consumer: string, minIdleMs: number): Promise<number> {
    const reclaimed = this.streams.xautoclaim(
      this.stream,
      this.group,
      consumer,
      minIdleMs,
    );
    // Ownership transferred; release so the next processOnce picks them up.
    for (const rec of reclaimed) this.streams.release(this.stream, rec.id);
    return reclaimed.length;
  }

  /** Long-running poll loop. */
  async start(consumer: string, opts: { intervalMs?: number } = {}): Promise<void> {
    this.running = true;
    const interval = opts.intervalMs ?? 50;
    while (this.running) {
      const n = await this.processOnce(consumer);
      if (n === 0) await this.sleep(interval);
    }
  }

  stop(): void {
    this.running = false;
  }
}
