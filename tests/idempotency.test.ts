import { InMemoryStreams } from '../src/broker/streams';
import { WorkerPool } from '../src/workers/pool';
import {
  IngestService,
  IdempotencyKeyRequiredError,
  RateLimitError,
} from '../src/producer';

const noSleep = async (_ms: number): Promise<void> => undefined;

describe('idempotency', () => {
  it('requires an idempotency key at ingestion', () => {
    const svc = new IngestService(new InMemoryStreams());
    expect(() =>
      svc.ingest({ type: 't', partitionKey: 'p', payload: {} }),
    ).toThrow(IdempotencyKeyRequiredError);
  });

  it('duplicate idempotency keys append only one stream entry', () => {
    const streams = new InMemoryStreams();
    const svc = new IngestService(streams);
    const input = { type: 't', partitionKey: 'p', payload: { a: 1 }, idempotencyKey: 'dup-1' };
    const r1 = svc.ingest(input);
    const r2 = svc.ingest(input);
    expect(r1.entryId).toBe(r2.entryId);
    expect(streams.xlen('pulsemesh:tasks')).toBe(1);
  });

  it('workers process an idempotency key exactly once', async () => {
    const streams = new InMemoryStreams();
    const svc = new IngestService(streams);
    svc.ingest({ type: 't', partitionKey: 'p', payload: {}, idempotencyKey: 'once-1' });
    let runs = 0;
    const pool = new WorkerPool(
      streams,
      async () => {
        runs += 1;
      },
      { sleep: noSleep },
    );
    await pool.processOnce('c-1');
    // Simulate redelivery of the same logical task under a new entry.
    streams.release('pulsemesh:tasks', svc.ingest({ type: 't', partitionKey: 'p', payload: {}, idempotencyKey: 'once-1' }).entryId);
    await pool.processOnce('c-1');
    expect(runs).toBe(1);
  });

  it('sliding-window rate limiter rejects bursts over budget', () => {
    const svc = new IngestService(new InMemoryStreams(), {
      rateLimitMax: 2,
      rateLimitWindowMs: 60_000,
    });
    const base = { type: 't', partitionKey: 'p', payload: {}, bucket: 'tenant-a' };
    svc.ingest({ ...base, idempotencyKey: 'r-1' });
    svc.ingest({ ...base, idempotencyKey: 'r-2' });
    expect(() => svc.ingest({ ...base, idempotencyKey: 'r-3' })).toThrow(RateLimitError);
  });
});
