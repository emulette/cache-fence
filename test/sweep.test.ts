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

    const generation = await cache.bumpGeneration();
    expect(await cache.generation()).toBe(generation);

    const writes = await Promise.all(
      Array.from({ length: 1500 }, (_, i) =>
        cache.setIfGeneration(`item-${i}`, `value-${i}`, generation, { ttlMs: 600_000 }),
      ),
    );
    expect(writes.every(Boolean)).toBe(true);

    const result = await cache.invalidate();

    expect(result.generation).not.toBe(generation);
    expect(result.deletedKeys).toBe(1500);
    expect(await cache.get('item-0')).toBeUndefined();
    expect(await cache.get('item-749')).toBeUndefined();
    expect(await cache.get('item-1499')).toBeUndefined();
    // Cleanup preserves the current metadata so subsequent readers share its token.
    expect(await cache.generation()).toBe(result.generation);
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

  it.each([
    ['tenant[1]', 'tenant1'],
    ['tenant?', 'tenant1'],
    ['tenant\\1', 'tenant1'],
  ])('treats namespace %s literally while sweeping', async (namespace, neighbour) => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    const other = createFencedCache({ redis: fx.commands, namespace: neighbour });
    await cache.getOrCompute('item', async () => 'old', { ttlMs: 60_000, staleTtlMs: 120_000 });
    const otherGeneration = await other.generation();
    await other.setIfGeneration('item', 'kept', otherGeneration, { ttlMs: 60_000 });

    const result = await cache.invalidate();

    expect(await other.get('item')).toBe('kept');
    expect(await other.generation()).toBe(otherGeneration);
    expect(result).toEqual({ generation: await cache.generation(), deletedKeys: 2 });
    expect(await cache.get('item')).toBeUndefined();
    expect(
      await cache.getOrCompute('item', async () => 'new', { ttlMs: 60_000, staleTtlMs: 120_000 }),
    ).toBe('new');
  });
});
