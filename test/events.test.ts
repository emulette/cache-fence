import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCacheErrorEvent, type FencedCacheEvent } from '../src/index';
import { deferred, poll, startRedisFixture, storageKeys, type RedisFixture } from './redis-fixture';

const NAMESPACE = 'events';
const KEY = 'item';
const OPTIONS = { ttlMs: 60_000, staleTtlMs: 120_000 };

describe('cache observation events', () => {
  let fx: RedisFixture;
  let events: FencedCacheEvent[];

  beforeAll(async () => {
    fx = await startRedisFixture();
  });
  afterAll(async () => {
    await fx.stop();
  });
  beforeEach(async () => {
    await fx.flush();
    events = [];
  });

  it('reports each request outcome while concurrent misses share one loader', async () => {
    const gate = deferred();
    const cache = createFencedCache({
      redis: fx.commands,
      namespace: NAMESPACE,
      onEvent: (event) => events.push(event),
    });
    let loads = 0;
    const requests = Array.from({ length: 5 }, () =>
      cache.getOrCompute(
        KEY,
        async () => {
          loads += 1;
          await gate.promise;
          return 'computed';
        },
        OPTIONS,
      ),
    );
    try {
      await poll(() => events.length === 5);
      expect(events).toEqual(Array.from({ length: 5 }, () => ({ type: 'miss', key: KEY })));
      expect(loads).toBe(1);
    } finally {
      gate.resolve();
      await Promise.all(requests);
    }

    expect(await cache.getOrCompute(KEY, async () => 'unexpected', OPTIONS)).toBe('computed');
    expect(events.at(-1)).toEqual({ type: 'hit', key: KEY, source: 'fresh' });
  });

  it('reports low-level fence rejection as an outcome without an error', async () => {
    const errors: FencedCacheErrorEvent[] = [];
    const cache = createFencedCache({
      redis: fx.commands,
      namespace: NAMESPACE,
      onEvent: (event) => events.push(event),
      onError: (event) => errors.push(event),
    });
    await cache.invalidate();

    expect(await cache.setIfGeneration(KEY, 'old', 0, OPTIONS)).toBe(false);
    expect(await cache.get(KEY)).toBeUndefined();
    expect(events).toEqual([{ type: 'fenceRejected', key: KEY, generation: 0, entry: 'fresh' }]);
    expect(errors).toEqual([]);
  });

  it.each([false, true])(
    'reports one refresh completion with invalidation=%s',
    async (invalidate) => {
      let refreshing = false;
      let refreshFinished = false;
      const cache = createFencedCache({
        redis: {
          ...fx.commands,
          eval: async (script, options) => {
            const reply = await fx.commands.eval(script, options);
            if (
              refreshing &&
              (reply === 0 || options.keys[1] === storageKeys.stale(NAMESPACE, KEY))
            ) {
              refreshFinished = true;
            }
            return reply;
          },
        },
        namespace: NAMESPACE,
        onEvent: (event) => events.push(event),
      });
      await cache.getOrCompute(KEY, async () => 'v1', OPTIONS);
      // Evict only the fresh copy so every request takes the SWR path.
      await fx.raw.unlink(storageKeys.fresh(NAMESPACE, KEY));
      events = [];
      refreshing = true;
      const gate = deferred();
      let refreshes = 0;
      const requests = Array.from({ length: 5 }, () =>
        cache.getOrCompute(
          KEY,
          async () => {
            refreshes += 1;
            await gate.promise;
            return 'v2';
          },
          OPTIONS,
        ),
      );

      try {
        expect(await Promise.all(requests)).toEqual(Array.from({ length: 5 }, () => 'v1'));
        expect(events).toEqual(
          Array.from({ length: 5 }, () => ({
            type: 'hit',
            key: KEY,
            source: 'stale',
          })),
        );
        expect(refreshes).toBe(1);
        if (invalidate) {
          await cache.invalidate();
        }
      } finally {
        gate.resolve();
        await poll(() => refreshFinished);
      }
      await poll(() => events.some((event) => event.type === 'refreshCompleted'));
      expect(events.filter((event) => event.type === 'refreshCompleted')).toEqual([
        {
          type: 'refreshCompleted',
          key: KEY,
          generation: 0,
          accepted: !invalidate,
        },
      ]);
      expect(events.filter((event) => event.type === 'fenceRejected')).toEqual(
        invalidate ? [{ type: 'fenceRejected', key: KEY, generation: 0, entry: 'fresh' }] : [],
      );
      expect(await cache.get(KEY)).toBe(invalidate ? undefined : 'v2');
    },
  );

  it('reports stale-copy rejection if invalidation crosses the two refresh writes', async () => {
    const invalidator = createFencedCache({ redis: fx.commands, namespace: NAMESPACE });
    let invalidateFreshWrite = false;
    const cache = createFencedCache({
      namespace: NAMESPACE,
      onEvent: (event) => events.push(event),
      redis: {
        ...fx.commands,
        eval: async (script, options) => {
          const reply = await fx.commands.eval(script, options);
          if (
            invalidateFreshWrite &&
            reply === 1 &&
            options.keys[1] === storageKeys.fresh(NAMESPACE, KEY)
          ) {
            await invalidator.invalidate();
          }
          return reply;
        },
      },
    });
    await cache.getOrCompute(KEY, async () => 'v1', OPTIONS);
    await fx.raw.unlink(storageKeys.fresh(NAMESPACE, KEY));
    events = [];
    invalidateFreshWrite = true;

    expect(await cache.getOrCompute(KEY, async () => 'v2', OPTIONS)).toBe('v1');
    await poll(() => events.some((event) => event.type === 'refreshCompleted'));

    expect(events).toEqual([
      { type: 'hit', key: KEY, source: 'stale' },
      { type: 'fenceRejected', key: KEY, generation: 0, entry: 'stale' },
      { type: 'refreshCompleted', key: KEY, generation: 0, accepted: false },
    ]);
    expect(await cache.get(KEY)).toBeUndefined();
    expect(await fx.raw.get(storageKeys.stale(NAMESPACE, KEY))).toBeNull();
  });

  it('ignores observer exceptions on hits, misses, refreshes and rejected writes', async () => {
    let observed = 0;
    const observerFailure = new Error('observer failed');
    const cache = createFencedCache({
      redis: fx.commands,
      namespace: NAMESPACE,
      onEvent: () => {
        observed += 1;
        throw observerFailure;
      },
    });
    expect(await cache.getOrCompute(KEY, async () => 'v1', OPTIONS)).toBe('v1');
    expect(await cache.getOrCompute(KEY, async () => 'unexpected', OPTIONS)).toBe('v1');
    await fx.raw.unlink(storageKeys.fresh(NAMESPACE, KEY));
    expect(await cache.getOrCompute(KEY, async () => 'v2', OPTIONS)).toBe('v1');
    await poll(() => observed === 4);
    expect(await cache.get(KEY)).toBe('v2');
    await cache.invalidate();
    expect(await cache.setIfGeneration(KEY, 'old', 0, OPTIONS)).toBe(false);
    expect(observed).toBe(5);
  });
});
