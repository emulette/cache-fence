import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCache } from '../src/index';
import { deferred, startRedisFixture, type RedisFixture } from './redis-fixture';

/**
 * Stale resurrection: a computation that starts before an invalidation and finishes
 * after it writes its already-obsolete result back into the cache, resurrecting data
 * the invalidation deleted. The write is an invalidation-crossing write.
 *
 * These three tests are the whole argument for this library:
 * the race is real, the fence rejects it, and the sweep covers the remaining edge.
 */
describe('stale resurrection', () => {
  let fx: RedisFixture;
  let cache: FencedCache;

  beforeAll(async () => {
    fx = await startRedisFixture();
  });

  afterAll(async () => {
    await fx.stop();
  });

  beforeEach(async () => {
    await fx.flush();
    cache = createFencedCache({ redis: fx.commands, namespace: 'catalog' });
  });

  it('baseline: a plain GET/SET/DEL cache serves deleted data again after an invalidation-crossing write', async () => {
    // No library here on purpose: this is the ordinary delete-on-mutation pattern,
    // written against the raw Redis client, on its own keys.
    const cacheKey = 'naive:works';
    let db = 'v1';
    const gate = deferred();

    // t0 - cache miss, a slow computation starts and reads the current state.
    const snapshot = db;
    const slowCompute = (async () => {
      await gate.promise;
      // t2 - the computation finishes and writes what it read at t0.
      await fx.raw.set(cacheKey, snapshot, { expiration: { type: 'PX', value: 60_000 } });
    })();

    // t1 - a mutation lands: the database moves on and the cache key is deleted.
    db = 'v2';
    await fx.raw.del(cacheKey);

    gate.resolve();
    await slowCompute;

    // The delete could only remove what existed at t1; it cannot stop a write still
    // in flight. The cache now serves v1 although the database says v2, and will keep
    // doing so until the next invalidation or the TTL expires.
    expect(db).toBe('v2');
    expect(await fx.raw.get(cacheKey)).toBe('v1');
  });

  it('fenced: the same timeline ends with the late write rejected inside Redis', async () => {
    let db = 'v1';
    const gate = deferred();

    // t0 - capture the generation before computing, exactly as getOrCompute does.
    const generationAtStart = await cache.generation();
    const snapshot = db;
    let writeAccepted: boolean | undefined;
    const slowCompute = (async () => {
      await gate.promise;
      // t2 - the write carries the generation it started with.
      writeAccepted = await cache.setIfGeneration('works', snapshot, generationAtStart, {
        ttlMs: 60_000,
      });
    })();

    // t1 - the mutation invalidates: bump the generation first, then sweep.
    db = 'v2';
    await cache.invalidate();

    gate.resolve();
    await slowCompute;

    // The generation moved while the computation ran, so the compare-and-set refused
    // the write. Nothing was resurrected: the key is simply absent.
    expect(writeAccepted).toBe(false);
    expect(await cache.get('works')).toBeUndefined();

    // A computation that starts after the invalidation caches normally again.
    const freshGeneration = await cache.generation();
    expect(await cache.setIfGeneration('works', db, freshGeneration, { ttlMs: 60_000 })).toBe(true);
    expect(await cache.get<string>('works')).toBe('v2');
  });

  it('double safety: a write accepted moments before the bump is removed by the sweep', async () => {
    // The fence cannot help a write that arrives while the generation still matches.
    const generationAtStart = await cache.generation();
    expect(await cache.setIfGeneration('works', 'v1', generationAtStart, { ttlMs: 60_000 })).toBe(
      true,
    );
    expect(await cache.get<string>('works')).toBe('v1');

    // That is what the second half of invalidation is for: bump, then sweep.
    const result = await cache.invalidate();

    expect(result.deletedKeys).toBeGreaterThanOrEqual(1);
    expect(await cache.get('works')).toBeUndefined();
  });
});
