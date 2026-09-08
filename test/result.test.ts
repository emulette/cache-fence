import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCacheEvent, type FencedCacheErrorEvent } from '../src/index';
import { deferred, poll, startRedisFixture, storageKeys, type RedisFixture } from './redis-fixture';

const namespace = 'result';
const key = 'item';
const options = { ttlMs: 60_000, staleTtlMs: 120_000 };

describe('per-call cache results', () => {
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

  it('distinguishes an accepted computation from a fresh hit, including cached null', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    const computed = await cache.getOrComputeResult(key, async () => null, options);
    const generation = await cache.generation();
    expect(computed).toEqual({ value: null, source: 'computed', generation, write: 'accepted' });
    expect(await cache.getOrComputeResult(key, async () => 'unexpected', options)).toEqual({
      value: null,
      source: 'fresh',
      generation,
      write: 'not-attempted',
    });
  });

  it('returns a stale snapshot immediately and reports its background rejection separately', async () => {
    const events: FencedCacheEvent[] = [];
    const cache = createFencedCache({
      redis: fx.commands,
      namespace,
      onEvent: (event) => events.push(event),
    });
    await cache.getOrCompute(key, async () => 'old', options);
    const generation = await cache.generation();
    await fx.raw.del(storageKeys.fresh(namespace, key));
    const gate = deferred();
    const result = await cache.getOrComputeResult(
      key,
      async () => {
        await gate.promise;
        return 'old-refresh';
      },
      options,
    );
    try {
      expect(result).toEqual({ value: 'old', source: 'stale', generation, write: 'not-attempted' });
      await fx.raw.del(storageKeys.counter(namespace));
      expect(await cache.getOrCompute(key, async () => 'new', options)).toBe('new');
    } finally {
      gate.resolve();
      await poll(() => events.some((event) => event.type === 'refreshCompleted'));
    }
    expect(events).toContainEqual({ type: 'refreshCompleted', key, generation, accepted: false });
    expect(await cache.get(key)).toBe('new');
  });

  it('shares one computation across both APIs and exposes its rejected write to detailed callers', async () => {
    let loads = 0;
    const events: FencedCacheEvent[] = [];
    const cache = createFencedCache({
      redis: fx.commands,
      namespace,
      onEvent: (event) => events.push(event),
    });
    const generation = await cache.generation();
    const gate = deferred();
    const loader = async (): Promise<string> => {
      loads += 1;
      await gate.promise;
      return 'old';
    };
    const plain = cache.getOrCompute(key, loader, options);
    const detailed = cache.getOrComputeResult(key, loader, options);
    try {
      await poll(() => events.filter((event) => event.type === 'miss').length === 2);
      await cache.invalidate();
    } finally {
      gate.resolve();
    }
    expect(await plain).toBe('old');
    expect(await detailed).toEqual({
      value: 'old',
      source: 'computed',
      generation,
      write: 'rejected',
    });
    expect(loads).toBe(1);
    expect(await cache.get(key)).toBeUndefined();
  });

  it.each(['transport', 'malformed'] as const)(
    'reports a %s write failure without losing the computed value',
    async (failure) => {
      const errors: FencedCacheErrorEvent[] = [];
      const cache = createFencedCache({
        namespace,
        onError: (event) => errors.push(event),
        redis: {
          ...fx.commands,
          eval: (script, request) => {
            if (request.arguments.length === 3) {
              return failure === 'transport'
                ? Promise.reject(new Error('write connection lost'))
                : Promise.resolve('unexpected');
            }
            return fx.commands.eval(script, request);
          },
        },
      });
      expect(await cache.getOrComputeResult(key, async () => 'computed', options)).toEqual({
        value: 'computed',
        source: 'computed',
        generation: await cache.generation(),
        write: 'failed',
      });
      expect(errors).toHaveLength(1);
      expect(errors[0]?.operation).toBe('setIfGeneration');
      expect(await cache.get(key)).toBeUndefined();
    },
  );

  it('marks a write as failed when the fresh copy lands but the stale copy errors', async () => {
    const cache = createFencedCache({
      namespace,
      redis: {
        ...fx.commands,
        eval: (script, request) => {
          if (
            request.arguments.length === 3 &&
            request.keys[1] === storageKeys.stale(namespace, key)
          ) {
            return Promise.reject(new Error('stale write connection lost'));
          }
          return fx.commands.eval(script, request);
        },
      },
    });
    expect((await cache.getOrComputeResult(key, async () => 'computed', options)).write).toBe(
      'failed',
    );
    expect(await cache.get(key)).toBe('computed');
    expect(await fx.raw.get(storageKeys.stale(namespace, key))).toBeNull();
  });

  it('marks a write as rejected when rotation crosses the two foreground writes', async () => {
    const invalidator = createFencedCache({ redis: fx.commands, namespace });
    const cache = createFencedCache({
      namespace,
      redis: {
        ...fx.commands,
        eval: async (script, request) => {
          const reply = await fx.commands.eval(script, request);
          if (
            request.arguments.length === 3 &&
            request.keys[1] === storageKeys.fresh(namespace, key)
          ) {
            await invalidator.bumpGeneration();
          }
          return reply;
        },
      },
    });
    expect((await cache.getOrComputeResult(key, async () => 'old', options)).write).toBe(
      'rejected',
    );
    expect(await cache.get(key)).toBeUndefined();
  });

  it('returns an unknown generation and skips caching when the snapshot fails', async () => {
    const cache = createFencedCache({
      namespace,
      redis: {
        ...fx.commands,
        eval: () => Promise.reject(new Error('snapshot connection lost')),
      },
    });
    expect(await cache.getOrComputeResult(key, async () => 'computed', options)).toEqual({
      value: 'computed',
      source: 'computed',
      generation: null,
      write: 'skipped',
    });
  });

  it('keeps the verified generation but skips writing a broken fresh entry', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    await fx.raw.rPush(storageKeys.fresh(namespace, key), 'wrong-type');
    expect(await cache.getOrComputeResult(key, async () => 'computed', options)).toEqual({
      value: 'computed',
      source: 'computed',
      generation: await cache.generation(),
      write: 'skipped',
    });
    await expect(cache.get(key)).rejects.toThrow('WRONGTYPE');
  });

  it('propagates loader failures from the detailed API', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace });
    const failure = new Error('loader failed');
    await expect(
      cache.getOrComputeResult(
        key,
        async () => {
          throw failure;
        },
        options,
      ),
    ).rejects.toBe(failure);
    expect(await cache.get(key)).toBeUndefined();
  });
});
