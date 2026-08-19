import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCache, type RedisCommands } from '../src/index';
import {
  deferred,
  poll,
  startRedisFixture,
  storageKeys,
  type RedisFixture,
} from './redis-fixture';

const NAMESPACE = 'catalog';
const KEY = 'works';
const TTL_MS = 60_000;
/** Long enough to be reached reliably, short enough that waiting it out costs nothing. */
const SHORT_TTL_MS = 150;

/**
 * Wraps the adapter to count completed fenced writes.
 *
 * A stale-while-revalidate refresh is fire-and-forget, so there is no promise to
 * await; tests wait for its write attempt to finish instead.
 */
function countEvals(base: RedisCommands): {
  commands: RedisCommands;
  completedEvals: () => number;
} {
  let completed = 0;
  return {
    commands: {
      ...base,
      eval: async (script, options) => {
        const reply = await base.eval(script, options);
        completed += 1;
        return reply;
      },
    },
    completedEvals: () => completed,
  };
}

describe('getOrCompute', () => {
  let fx: RedisFixture;
  let cache: FencedCache;
  let completedEvals: () => number;

  beforeAll(async () => {
    fx = await startRedisFixture();
  });

  afterAll(async () => {
    await fx.stop();
  });

  beforeEach(async () => {
    await fx.flush();
    const counted = countEvals(fx.commands);
    completedEvals = counted.completedEvals;
    cache = createFencedCache({ redis: counted.commands, namespace: NAMESPACE });
  });

  it('computes once on a miss and serves the cached value on the next call', async () => {
    let invocations = 0;
    const loader = async (): Promise<string> => {
      invocations += 1;
      return 'v1';
    };

    expect(await cache.getOrCompute(KEY, loader, { ttlMs: TTL_MS })).toBe('v1');
    expect(await cache.getOrCompute(KEY, loader, { ttlMs: TTL_MS })).toBe('v1');

    expect(invocations).toBe(1);
    expect(await cache.get<string>(KEY)).toBe('v1');
  });

  it('single-flights concurrent callers of one key into a single computation', async () => {
    const gate = deferred();
    let invocations = 0;
    const loader = async (): Promise<string> => {
      invocations += 1;
      await gate.promise;
      return 'computed';
    };

    const callers = Array.from({ length: 10 }, () =>
      cache.getOrCompute(KEY, loader, { ttlMs: TTL_MS }),
    );
    gate.resolve();
    const values = await Promise.all(callers);

    expect(invocations).toBe(1);
    expect(values).toEqual(Array.from({ length: 10 }, () => 'computed'));
  });

  it('caches nothing when an invalidation crosses a slow computation, and reloads on the next call', async () => {
    let db = 'v1';
    const gate = deferred();
    const loaderStarted = deferred();

    // t0 - the computation starts and reads the current state. getOrCompute has
    // already captured the generation at this point.
    const snapshot = db;
    const pending = cache.getOrCompute(
      KEY,
      async () => {
        loaderStarted.resolve();
        await gate.promise;
        return snapshot;
      },
      { ttlMs: TTL_MS },
    );
    await loaderStarted.promise;

    // t1 - a mutation lands while the computation is still running.
    db = 'v2';
    await cache.invalidate();

    // t2 - the computation finishes.
    gate.resolve();

    // The caller still gets what it computed; that is not a lie, it is simply old.
    // What must not happen is that value being written back over the invalidation.
    expect(await pending).toBe('v1');
    expect(await cache.get(KEY)).toBeUndefined();

    // The next call sees a miss, recomputes against the current state and caches it.
    const reloaded = await cache.getOrCompute(KEY, async () => db, { ttlMs: TTL_MS });
    expect(reloaded).toBe('v2');
    expect(await cache.get<string>(KEY)).toBe('v2');
  });

  it('serves the stale value immediately and lands the background refresh through the fence', async () => {
    await cache.getOrCompute(KEY, async () => 'v1', {
      ttlMs: SHORT_TTL_MS,
      staleTtlMs: TTL_MS,
    });
    await poll(async () => (await cache.get(KEY)) === undefined, {
      description: 'the fresh entry to expire while the stale copy lives on',
    });

    let refreshes = 0;
    const refreshLoader = async (): Promise<string> => {
      refreshes += 1;
      return 'v2';
    };

    // The refresh writes with a full TTL so the assertion below is not racing the
    // same short expiry this test just waited out.
    const served = await cache.getOrCompute(KEY, refreshLoader, {
      ttlMs: TTL_MS,
      staleTtlMs: TTL_MS,
    });

    // The caller is never made to wait for the refresh.
    expect(served).toBe('v1');

    await poll(async () => (await cache.get<string>(KEY)) === 'v2', {
      description: 'the background refresh to land',
    });
    expect(refreshes).toBe(1);
  });

  it('drops the background refresh when an invalidation crosses it, leaving fresh and stale empty', async () => {
    await cache.getOrCompute(KEY, async () => 'v1', {
      ttlMs: SHORT_TTL_MS,
      staleTtlMs: TTL_MS,
    });
    await poll(async () => (await cache.get(KEY)) === undefined, {
      description: 'the fresh entry to expire while the stale copy lives on',
    });
    // Priming wrote the fresh and the stale entry: two fenced writes.
    const evalsBeforeRefresh = completedEvals();
    expect(evalsBeforeRefresh).toBe(2);

    const gate = deferred();
    const refreshStarted = deferred();
    const served = await cache.getOrCompute(
      KEY,
      async () => {
        refreshStarted.resolve();
        await gate.promise;
        return 'refreshed-from-the-old-world';
      },
      { ttlMs: TTL_MS, staleTtlMs: TTL_MS },
    );
    expect(served).toBe('v1');
    await refreshStarted.promise;

    // The invalidation crosses the in-flight refresh, exactly as it would cross a
    // foreground computation. SWR widens this window rather than closing it.
    await cache.invalidate();

    gate.resolve();
    await poll(() => completedEvals() > evalsBeforeRefresh, {
      description: 'the background refresh to attempt its fenced write',
    });

    // The refresh carried the pre-invalidation generation, so neither copy came back.
    expect(await cache.get(KEY)).toBeUndefined();
    expect(await fx.raw.get(storageKeys.fresh(NAMESPACE, KEY))).toBeNull();
    expect(await fx.raw.get(storageKeys.stale(NAMESPACE, KEY))).toBeNull();
  });

  it('treats a cached null as a hit rather than a miss', async () => {
    let invocations = 0;
    const loader = async (): Promise<string | null> => {
      invocations += 1;
      return null;
    };

    expect(await cache.getOrCompute(KEY, loader, { ttlMs: TTL_MS })).toBeNull();
    expect(await cache.getOrCompute(KEY, loader, { ttlMs: TTL_MS })).toBeNull();

    expect(invocations).toBe(1);
    // null is a value that was cached; undefined would mean nothing is there.
    expect(await cache.get<string | null>(KEY)).toBeNull();
  });

  it('propagates loader failures to the caller and caches nothing', async () => {
    const failure = new Error('the loader could not reach the database');

    await expect(
      cache.getOrCompute(KEY, async (): Promise<string> => Promise.reject(failure), {
        ttlMs: TTL_MS,
      }),
    ).rejects.toBe(failure);

    expect(await cache.get(KEY)).toBeUndefined();
    expect(completedEvals()).toBe(0);
  });
});
