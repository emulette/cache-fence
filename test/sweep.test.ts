import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache } from '../src/index';
import { startRedisFixture, type RedisFixture } from './redis-fixture';

describe('invalidation sweep', () => {
  let fx: RedisFixture;

  beforeAll(async () => {
    fx = await startRedisFixture();
  });

  afterAll(async () => {
    await fx.stop();
  });

  beforeEach(async () => {
    await fx.flush();
  });

  it('deletes all 1,500 namespace keys across SCAN and UNLINK batches while the counter survives', async () => {
    // Small batch sizes force the iteration and batching paths to actually run.
    const cache = createFencedCache({
      redis: fx.commands,
      namespace: 'catalog',
      scanCount: 100,
      unlinkBatchSize: 200,
    });

    await cache.bumpGeneration();
    const generation = await cache.generation();
    expect(generation).toBe(1);

    const writes = await Promise.all(
      Array.from({ length: 1500 }, (_, i) =>
        cache.setIfGeneration(`item-${i}`, `value-${i}`, generation, { ttlMs: 600_000 }),
      ),
    );
    expect(writes.every(Boolean)).toBe(true);

    const result = await cache.invalidate();

    expect(result.generation).toBe(2);
    expect(result.deletedKeys).toBe(1500);
    expect(await cache.get('item-0')).toBeUndefined();
    expect(await cache.get('item-749')).toBeUndefined();
    expect(await cache.get('item-1499')).toBeUndefined();
    // The counter lives outside the swept prefix; wiping it would reset the fence
    // and make every in-flight pre-invalidation write acceptable again.
    expect(await cache.generation()).toBe(2);
  });

  it('leaves another namespace keys and generation untouched', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace: 'catalog' });
    const other = createFencedCache({ redis: fx.commands, namespace: 'other' });

    const otherGeneration = await other.generation();
    await other.setIfGeneration('kept', 'x', otherGeneration, { ttlMs: 600_000 });
    const cacheGeneration = await cache.generation();
    await cache.setIfGeneration('gone', 'y', cacheGeneration, { ttlMs: 600_000 });

    await cache.invalidate();

    expect(await cache.get('gone')).toBeUndefined();
    expect(await other.get<string>('kept')).toBe('x');
    expect(await other.generation()).toBe(otherGeneration);
  });
});
