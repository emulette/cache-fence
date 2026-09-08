import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createFencedCache } from '../src/index';
import { startRedisFixture, storageKeys, type RedisFixture } from './redis-fixture';

describe('generation safety under real Redis memory pressure', () => {
  let fx: RedisFixture;
  beforeAll(async () => {
    fx = await startRedisFixture();
  });
  afterAll(async () => {
    await fx.stop();
  });

  it.each(['allkeys-lru', 'volatile-lru'])('rejects old writers with %s', async (policy) => {
    await fx.flush();
    const namespace = 'memory-pressure';
    const cache = createFencedCache({ redis: fx.commands, namespace });
    const before = await cache.generation();
    const current = await cache.bumpGeneration();
    const usedMemory = Number(/^used_memory:(\d+)/m.exec(await fx.raw.info('memory'))?.[1]);
    expect(Number.isSafeInteger(usedMemory)).toBe(true);
    const evictedBefore = Number(/^evicted_keys:(\d+)/m.exec(await fx.raw.info('stats'))?.[1]);
    await fx.raw.configSet({
      maxmemory: String(usedMemory + 256 * 1024),
      'maxmemory-policy': policy,
    });
    try {
      for (let i = 0; i < 256; i += 1) {
        await fx.raw.set(`pressure:${i}`, 'x'.repeat(16 * 1024), { PX: 60_000 });
      }
      const evictedAfter = Number(/^evicted_keys:(\d+)/m.exec(await fx.raw.info('stats'))?.[1]);
      expect(evictedAfter).toBeGreaterThan(evictedBefore);
      expect(await fx.raw.get(storageKeys.counter(namespace))).toBe(
        policy === 'allkeys-lru' ? null : current,
      );
    } finally {
      await fx.raw.configSet({ maxmemory: '0', 'maxmemory-policy': 'noeviction' });
    }
    expect(await cache.setIfGeneration('item', 'old', before, { ttlMs: 60_000 })).toBe(false);
    const recovered = await cache.generation();
    expect(recovered).not.toBe(before);
    if (policy === 'allkeys-lru') expect(recovered).not.toBe(current);
    expect(await cache.getOrCompute('item', async () => 'new', { ttlMs: 60_000 })).toBe('new');
    expect(await cache.get('item')).toBe('new');
  });
});
