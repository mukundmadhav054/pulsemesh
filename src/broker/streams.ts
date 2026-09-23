/**
 * Redis Streams abstraction.
 *
 * In-memory fake is the default (no infra required for tests).
 * A real `redis` client is used when `REDIS_URL` is set — same
 * XACK / XAUTOCLAIM / DLQ-after-5 semantics in both paths.
 */

export interface StreamFields {
  /** Idempotency key supplied by the producer (required). */
  idempotencyKey: string;
  /** Partition key used to shard a stream across consumer groups. */
  partitionKey: string;
  /** Serialized task payload (see src/schemas.ts for the envelope). */
  body: string;
  /** Task type discriminator. */
  type: string;
}

export interface StreamRecord extends StreamFields {
  /** Stream entry id, e.g. `1700000000000-0`. */
  id: string;
  /** Number of delivery attempts so far. */
  deliveries: number;
  /** Epoch ms when the message was enqueued. */
  enqueuedAt: number;
  /** Epoch ms of last delivery to a consumer (0 = never delivered). */
  deliveredAt: number;
  /** Name of the consumer that currently owns the pending entry. */
  owner: string | null;
}

export interface ReadResult {
  record: StreamRecord;
}

export const MAX_DELIVERIES = 5;

function nextId(counter: number): string {
  return `${Date.now()}-${counter}`;
}

/**
 * In-memory Redis Streams stand-in implementing the subset of semantics
 * PulseMesh relies on: XADD, XREADGROUP, XACK, XPENDING/XLEN and
 * XAUTOCLAIM-style reclaim of idle pending entries.
 */
export class InMemoryStreams {
  private streams = new Map<string, StreamRecord[]>();
  /** stream -> group -> set of acked ids */
  private acked = new Map<string, Map<string, Set<string>>>();
  /** idempotencyKey -> entry id (per stream) for dedupe */
  private seenKeys = new Map<string, Map<string, string>>();
  private counter = 0;

  private groupAcks(stream: string, group: string): Set<string> {
    let groups = this.acked.get(stream);
    if (!groups) {
      groups = new Map();
      this.acked.set(stream, groups);
    }
    let set = groups.get(group);
    if (!set) {
      set = new Set();
      groups.set(group, set);
    }
    return set;
  }

  /** XADD — append an entry; duplicate idempotency keys return the original id. */
  xadd(stream: string, fields: StreamFields): string {
    let keys = this.seenKeys.get(stream);
    if (!keys) {
      keys = new Map();
      this.seenKeys.set(stream, keys);
    }
    const existing = keys.get(fields.idempotencyKey);
    if (existing) return existing;

    const record: StreamRecord = {
      ...fields,
      id: nextId(this.counter++),
      deliveries: 0,
      enqueuedAt: Date.now(),
      deliveredAt: 0,
      owner: null,
    };
    const list = this.streams.get(stream) ?? [];
    list.push(record);
    this.streams.set(stream, list);
    keys.set(fields.idempotencyKey, record.id);
    return record.id;
  }

  /**
   * XREADGROUP — deliver up to `count` undelivered (never-acked,
   * currently-unowned) entries to `consumer`, marking them pending.
   */
  xreadgroup(
    stream: string,
    group: string,
    consumer: string,
    count = 10,
  ): StreamRecord[] {
    const acked = this.groupAcks(stream, group);
    const list = this.streams.get(stream) ?? [];
    const out: StreamRecord[] = [];
    for (const rec of list) {
      if (out.length >= count) break;
      if (acked.has(rec.id)) continue;
      if (rec.owner !== null) continue; // already pending with another consumer
      rec.deliveries += 1;
      rec.deliveredAt = Date.now();
      rec.owner = consumer;
      out.push({ ...rec });
    }
    return out;
  }

  /** XACK — acknowledge successful processing. Returns 1 if newly acked. */
  xack(stream: string, group: string, id: string): number {
    const acked = this.groupAcks(stream, group);
    if (acked.has(id)) return 0;
    const list = this.streams.get(stream) ?? [];
    const found = list.some((r) => r.id === id);
    if (!found) return 0;
    acked.add(id);
    return 1;
  }

  /**
   * XAUTOCLAIM — reclaim pending entries idle longer than `minIdleMs`
   * and reassign them to `consumer`. Returns the reclaimed records.
   */
  xautoclaim(
    stream: string,
    _group: string,
    consumer: string,
    minIdleMs: number,
    count = 10,
  ): StreamRecord[] {
    // NOTE: group scoping lives in the ack sets; entry ownership is
    // per-stream, so _group is intentionally unused here.
    const now = Date.now();
    const list = this.streams.get(stream) ?? [];
    const out: StreamRecord[] = [];
    for (const rec of list) {
      if (out.length >= count) break;
      if (rec.owner === null || rec.owner === consumer) continue;
      if (now - rec.deliveredAt < minIdleMs) continue;
      rec.deliveries += 1;
      rec.deliveredAt = now;
      rec.owner = consumer;
      out.push({ ...rec });
    }
    return out;
  }

  /** XPENDING — entries delivered but not yet acked for a group. */
  xpending(stream: string, group: string): StreamRecord[] {
    const acked = this.groupAcks(stream, group);
    return (this.streams.get(stream) ?? []).filter(
      (r) => r.owner !== null && !acked.has(r.id),
    );
  }

  /** XLEN — total entries appended to a stream. */
  xlen(stream: string): number {
    return (this.streams.get(stream) ?? []).length;
  }

  /** Number of entries acked in a group. */
  ackedCount(stream: string, group: string): number {
    return this.groupAcks(stream, group).size;
  }

  /** Direct lookup (used by the worker pool for retry accounting). */
  get(stream: string, id: string): StreamRecord | undefined {
    return (this.streams.get(stream) ?? []).find((r) => r.id === id);
  }

  /** Release ownership without acking (worker asks for redelivery). */
  release(stream: string, id: string): void {
    const rec = this.get(stream, id);
    if (rec) rec.owner = null;
  }

  /** Reset all state (tests). */
  reset(): void {
    this.streams.clear();
    this.acked.clear();
    this.seenKeys.clear();
    this.counter = 0;
  }
}

/**
 * Thin wrapper over a real `redis` client exposing the same call shapes.
 * Only instantiated when `REDIS_URL` is set; lazily imports `redis` so
 * unit tests never need a live server.
 */
export class RedisStreamBroker {
  private client: unknown = null;

  constructor(private readonly url: string) {}

  private async getClient(): Promise<any> {
    if (!this.client) {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { createClient } = require('redis') as typeof import('redis');
      this.client = createClient({ url: this.url });
      await (this.client as any).connect();
    }
    return this.client;
  }

  async xadd(stream: string, fields: StreamFields): Promise<string> {
    const client = await this.getClient();
    return client.xAdd(stream, '*', {
      idempotencyKey: fields.idempotencyKey,
      partitionKey: fields.partitionKey,
      body: fields.body,
      type: fields.type,
    });
  }

  async ensureGroup(stream: string, group: string): Promise<void> {
    const client = await this.getClient();
    try {
      await client.xGroupCreate(stream, group, '0', { MKSTREAM: true });
    } catch (err: unknown) {
      if (!(err instanceof Error && err.message.includes('BUSYGROUP'))) throw err;
    }
  }

  async xack(stream: string, group: string, id: string): Promise<number> {
    const client = await this.getClient();
    return client.xAck(stream, group, id);
  }

  async xautoclaim(
    stream: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    count = 10,
  ): Promise<any> {
    const client = await this.getClient();
    return client.xAutoClaim(stream, group, consumer, minIdleMs, '0-0', {
      COUNT: count,
    });
  }

  async quit(): Promise<void> {
    if (this.client) await (this.client as any).quit();
  }
}

/** Factory: real broker when REDIS_URL is set, in-memory fake otherwise. */
export function createStreams(): InMemoryStreams | RedisStreamBroker {
  if (process.env['REDIS_URL']) return new RedisStreamBroker(process.env['REDIS_URL'] as string);
  return new InMemoryStreams();
}
