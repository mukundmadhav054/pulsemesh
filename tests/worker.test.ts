import { InMemoryStreams } from '../src/broker/streams';
import { WorkerPool, MAX_RETRIES } from '../src/workers/pool';

const noSleep = async (_ms: number): Promise<void> => undefined;

describe('worker retry -> DLQ after 5 deliveries', () => {
  it('moves poison pills to the DLQ after MAX_RETRIES attempts', async () => {
    const s = new InMemoryStreams();
    s.xadd('pulsemesh:tasks', {
      idempotencyKey: 'poison-1',
      partitionKey: 'p',
      body: '{}',
      type: 'poison',
    });
    let calls = 0;
    const pool = new WorkerPool(
      s,
      () => {
        calls += 1;
        throw new Error('boom');
      },
      { sleep: noSleep, random: () => 0 },
    );
    for (let i = 0; i < MAX_RETRIES; i++) {
      await pool.processOnce('c-1');
    }
    expect(calls).toBe(MAX_RETRIES);
    expect(pool.dlqEntries).toHaveLength(1);
    expect(pool.dlqEntries[0]?.attempts).toBe(MAX_RETRIES);
    expect(pool.getStats().dlq).toBe(1);
    // Acked DLQ entries are not redelivered.
    await pool.processOnce('c-1');
    expect(calls).toBe(MAX_RETRIES);
  });

  it('backs off with exponential growth plus jitter', async () => {
    const s = new InMemoryStreams();
    s.xadd('pulsemesh:tasks', {
      idempotencyKey: 'flaky-1',
      partitionKey: 'p',
      body: '{}',
      type: 'flaky',
    });
    const delays: number[] = [];
    let calls = 0;
    const pool = new WorkerPool(
      s,
      () => {
        calls += 1;
        if (calls < 3) throw new Error('flaky');
      },
      { sleep: async (ms) => { delays.push(ms); }, random: () => 0, baseDelayMs: 100, maxDelayMs: 10_000 },
    );
    await pool.processOnce('c-1'); // attempt 1 fails -> backoff ~100ms
    await pool.processOnce('c-1'); // attempt 2 fails -> backoff ~200ms
    await pool.processOnce('c-1'); // attempt 3 succeeds
    expect(calls).toBe(3);
    expect(delays).toHaveLength(2);
    expect(delays[1] as number).toBeGreaterThan(delays[0] as number);
    expect(pool.dlqEntries).toHaveLength(0);
    expect(pool.getStats().processed).toBe(1);
  });
});
