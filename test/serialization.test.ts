import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  createFencedCache,
  FENCED_CACHE_ERRORS,
  type FencedCacheErrorEvent,
  type Serializer,
} from '../src/index';
import { startRedisFixture, storageKeys, type RedisFixture } from './redis-fixture';

const NAMESPACE = 'catalog';
const KEY = 'works';
const TTL_MS = 60_000;

interface Work {
  id: string;
  title: string;
  tags: string[];
  published: boolean;
  releasedAt: number | null;
}

const WORK: Work = {
  id: 'w-1',
  title: 'Nocturne',
  tags: ['piano', 'romantic'],
  published: true,
  releasedAt: null,
};

const TAG = 'cache-fence:v1:';

/** Prefix-tagged JSON, so the stored bytes prove which serializer produced them. */
const TAGGED_SERIALIZER: Serializer = {
  serialize: (value) => `${TAG}${JSON.stringify(value)}`,
  deserialize: (raw) => {
    if (!raw.startsWith(TAG)) {
      throw new Error(`expected a ${TAG}-tagged payload, received: ${raw}`);
    }
    return JSON.parse(raw.slice(TAG.length)) as unknown;
  },
};

describe('serialization and input validation', () => {
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

  it('round-trips a structured value through the default JSON serializer', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace: NAMESPACE });

    const computed = await cache.getOrCompute(KEY, async () => WORK, { ttlMs: TTL_MS });
    expect(computed).toEqual(WORK);

    const cached = await cache.get<Work>(KEY);
    expect(cached).toEqual(WORK);
    // A round-trip, not the same object handed back from an in-process cache.
    expect(cached).not.toBe(WORK);
  });

  it('uses a custom serializer in both directions', async () => {
    const cache = createFencedCache({
      redis: fx.commands,
      namespace: NAMESPACE,
      serializer: TAGGED_SERIALIZER,
    });

    await cache.getOrCompute(KEY, async () => WORK, { ttlMs: TTL_MS });

    const stored = await fx.raw.get(storageKeys.fresh(NAMESPACE, KEY));
    expect(stored).toBe(`${TAG}${JSON.stringify(WORK)}`);
    // The read path goes back through the same serializer, tag and all.
    expect(await cache.get<Work>(KEY)).toEqual(WORK);
  });

  it('throws from setIfGeneration when a value has no serialized form', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace: NAMESPACE });
    const generation = await cache.generation();

    await expect(
      cache.setIfGeneration(KEY, undefined, generation, { ttlMs: TTL_MS }),
    ).rejects.toThrow(FENCED_CACHE_ERRORS.unserializableValue());
  });

  it('returns an unserializable computed value to the caller while caching nothing', async () => {
    const events: FencedCacheErrorEvent[] = [];
    const cache = createFencedCache({
      redis: fx.commands,
      namespace: NAMESPACE,
      onError: (event) => events.push(event),
    });

    const value = await cache.getOrCompute(KEY, async () => undefined, { ttlMs: TTL_MS });

    // The loader's result is the caller's business; only the caching of it failed.
    expect(value).toBeUndefined();
    expect(await cache.get(KEY)).toBeUndefined();
    expect(events).toHaveLength(1);
    expect(events[0]?.operation).toBe('setIfGeneration');
    expect(events[0]?.key).toBe(KEY);
    expect(String(events[0]?.error)).toContain(FENCED_CACHE_ERRORS.unserializableValue());
  });

  it('rejects a namespace that would break the cluster hash tag or the sweep pattern', () => {
    for (const namespace of ['', 'cata*log', '{catalog}', 'catalog}']) {
      expect(() => createFencedCache({ redis: fx.commands, namespace })).toThrow(
        FENCED_CACHE_ERRORS.invalidNamespace(namespace),
      );
    }
  });

  it('rejects a ttl that is not a positive whole number of milliseconds', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace: NAMESPACE });

    await expect(cache.getOrCompute(KEY, async () => WORK, { ttlMs: 0 })).rejects.toThrow(
      FENCED_CACHE_ERRORS.invalidTtl('ttlMs', 0),
    );
    await expect(cache.getOrCompute(KEY, async () => WORK, { ttlMs: -1 })).rejects.toThrow(
      FENCED_CACHE_ERRORS.invalidTtl('ttlMs', -1),
    );
    await expect(cache.getOrCompute(KEY, async () => WORK, { ttlMs: 1.5 })).rejects.toThrow(
      FENCED_CACHE_ERRORS.invalidTtl('ttlMs', 1.5),
    );
    await expect(
      cache.getOrCompute(KEY, async () => WORK, { ttlMs: TTL_MS, staleTtlMs: 0 }),
    ).rejects.toThrow(FENCED_CACHE_ERRORS.invalidTtl('staleTtlMs', 0));
    await expect(cache.setIfGeneration(KEY, WORK, 0, { ttlMs: -1 })).rejects.toThrow(
      FENCED_CACHE_ERRORS.invalidTtl('ttlMs', -1),
    );

    expect(await cache.get(KEY)).toBeUndefined();
  });
});
