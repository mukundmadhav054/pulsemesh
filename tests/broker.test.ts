import { InMemoryStreams } from '../src/broker/streams';

describe('broker ack tracking (XADD/XREADGROUP/XACK)', () => {
  it('delivers, acks, and never redelivers acked entries', () => {
    const s = new InMemoryStreams();
    const id = s.xadd('tasks', {
      idempotencyKey: 'k-1',
      partitionKey: 'p-1',
      body: '{}',
      type: 't',
    });
    const batch = s.xreadgroup('tasks', 'g', 'c-1', 10);
    expect(batch).toHaveLength(1);
    expect(batch[0]?.id).toBe(id);
    expect(s.xack('tasks', 'g', id)).toBe(1);
    // Second ack is a no-op; group sees nothing pending.
    expect(s.xack('tasks', 'g', id)).toBe(0);
    expect(s.xpending('tasks', 'g')).toHaveLength(0);
    expect(s.xreadgroup('tasks', 'g', 'c-1', 10)).toHaveLength(0);
  });

  it('reclaims idle pending entries via XAUTOCLAIM', () => {
    const s = new InMemoryStreams();
    s.xadd('tasks', { idempotencyKey: 'k-1', partitionKey: 'p-1', body: '{}', type: 't' });
    const first = s.xreadgroup('tasks', 'g', 'crashed-worker', 10);
    expect(first).toHaveLength(1);
    // Fresh entries are not idle yet.
    expect(s.xautoclaim('tasks', 'g', 'rescue', 60_000)).toHaveLength(0);
    // With minIdleMs=0 the idle entry is reclaimed by the new consumer.
    const reclaimed = s.xautoclaim('tasks', 'g', 'rescue', 0);
    expect(reclaimed).toHaveLength(1);
    expect(reclaimed[0]?.owner).toBe('rescue');
  });

  it('partitions: consumers only receive unowned entries', () => {
    const s = new InMemoryStreams();
    for (let i = 0; i < 3; i++) {
      s.xadd('tasks', {
        idempotencyKey: `k-${i}`,
        partitionKey: `shard-${i % 2}`,
        body: '{}',
        type: 't',
      });
    }
    expect(s.xreadgroup('tasks', 'g', 'c-1', 2)).toHaveLength(2);
    expect(s.xreadgroup('tasks', 'g', 'c-2', 10)).toHaveLength(1);
    expect(s.xpending('tasks', 'g')).toHaveLength(3);
  });
});
