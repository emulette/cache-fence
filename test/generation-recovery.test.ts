import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type RedisCommands } from '../src/index';
import { deferred, startRedisFixture, storageKeys, type RedisFixture } from './redis-fixture';

describe('generation loss and incomplete invalidation', () => {
  let fx: RedisFixture;
  const namespace = 'recovery';
  const key = 'item';
  const options = { ttlMs: 60_000, staleTtlMs: 120_000 };

  beforeAll(async () => {
    fx = await startRedisFixture();
  });
  afterAll(async () => {
    await fx.stop();
  });
  beforeEach(async () => {
    await fx.flush();
  });

  it('rejects an old writer when invalidation is followed by generation loss', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    const before = await cache.generation();
    await cache.invalidate();
    await fx.raw.del(storageKeys.counter(namespace));

    expect(await cache.setIfGeneration(key, 'old', before, options)).toBe(false);
    expect(await cache.get(key)).toBeUndefined();
  });

  it('does not reuse a generation after a complete Redis data reset', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    const before = await cache.generation();
    await cache.invalidate();
    await fx.flush();
    const after = await cache.generation();

    expect(after).not.toBe(before);
    expect(await cache.setIfGeneration(key, 'old', before, options)).toBe(false);
    expect(await cache.setIfGeneration(key, 'new', after, options)).toBe(true);
    expect(await cache.get(key)).toBe('new');
  });

  it('ignores surviving data when only its generation was lost', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    await cache.getOrCompute(key, async () => 'old', options);
    await fx.raw.del(storageKeys.counter(namespace));

    expect(await cache.get(key)).toBeUndefined();
    expect(await cache.getOrCompute(key, async () => 'new', options)).toBe('new');
  });

  it.each(['fresh', 'stale'] as const)(
    'invalidates a %s entry as soon as the generation rotates, before the sweep',
    async (entry) => {
      const cache = createFencedCache({ redis: fx.commands, namespace });
      await cache.getOrCompute(key, async () => 'old', options);
      if (entry === 'stale') await fx.raw.del(storageKeys.fresh(namespace, key));
      await cache.bumpGeneration();

      expect(await cache.get(key)).toBeUndefined();
      expect(await cache.getOrCompute(key, async () => 'new', options)).toBe('new');
    },
  );

  it('keeps old entries unreadable even when the cleanup fails', async () => {
    const failure = new Error('Redis scan unavailable');
    const redis: RedisCommands = {
      ...fx.commands,
      scanIterator: () => ({
        [Symbol.asyncIterator]: () => ({ next: () => Promise.reject(failure) }),
      }),
    };
    const cache = createFencedCache({ redis, namespace });
    await cache.getOrCompute(key, async () => 'old', options);

    await expect(cache.invalidate()).rejects.toBe(failure);
    expect(await fx.raw.exists(storageKeys.fresh(namespace, key))).toBe(1);
    expect(await cache.get(key)).toBeUndefined();
    expect(await cache.getOrCompute(key, async () => 'new', options)).toBe('new');
  });

  it('preserves a current-generation value written after SCAN found its key', async () => {
    const writer = createFencedCache({ redis: fx.commands, namespace });
    await writer.getOrCompute(key, async () => 'old', options);
    let replaced = false;
    const redis: RedisCommands = {
      ...fx.commands,
      scanIterator: async function* (scanOptions) {
        for await (const keys of fx.commands.scanIterator(scanOptions)) {
          if (!replaced) {
            replaced = true;
            await writer.setIfGeneration(key, 'new', await writer.generation(), options);
          }
          yield keys;
        }
      },
    };
    await createFencedCache({ redis, namespace }).invalidate();

    expect(replaced).toBe(true);
    expect(await writer.get(key)).toBe('new');
  });

  it('does not let a request after data loss join a pre-loss computation', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    const gate = deferred();
    const started = deferred();
    const oldRequest = cache.getOrCompute(
      key,
      async () => {
        started.resolve();
        await gate.promise;
        return 'old';
      },
      options,
    );
    await started.promise;
    await fx.flush();
    const newRequest = cache.getOrCompute(key, async () => 'new', options);
    gate.resolve();

    expect(await Promise.all([oldRequest, newRequest])).toEqual(['old', 'new']);
    expect(await cache.get(key)).toBe('new');
  });
});
