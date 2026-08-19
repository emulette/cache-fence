import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCache } from '../src/index';
import { startRedisFixture, type RedisFixture } from './redis-fixture';

describe('generation counter under concurrency', () => {
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

  it('hands every one of 50 concurrent invalidators a distinct generation', async () => {
    const results = await Promise.all(Array.from({ length: 50 }, () => cache.bumpGeneration()));

    // A lost update would show up as a duplicate return value or a counter below 50.
    expect(new Set(results).size).toBe(50);
    expect(Math.max(...results)).toBe(50);
    expect(await cache.generation()).toBe(50);
  });

  it('rejects every writer that captured a pre-invalidation generation and accepts every one after it', async () => {
    const before = await Promise.all(
      Array.from({ length: 10 }, async (_, i) => ({
        key: `pre-${i}`,
        generation: await cache.generation(),
      })),
    );

    await cache.invalidate();

    const after = await Promise.all(
      Array.from({ length: 10 }, async (_, i) => ({
        key: `post-${i}`,
        generation: await cache.generation(),
      })),
    );

    // Both groups race to write at the same moment; only the generation separates them.
    const preResults = await Promise.all(
      before.map((writer) =>
        cache.setIfGeneration(writer.key, 'stale', writer.generation, { ttlMs: 60_000 }),
      ),
    );
    const postResults = await Promise.all(
      after.map((writer) =>
        cache.setIfGeneration(writer.key, 'fresh', writer.generation, { ttlMs: 60_000 }),
      ),
    );

    expect(preResults).toEqual(Array.from({ length: 10 }, () => false));
    expect(postResults).toEqual(Array.from({ length: 10 }, () => true));

    for (const writer of before) {
      expect(await cache.get(writer.key)).toBeUndefined();
    }
    for (const writer of after) {
      expect(await cache.get<string>(writer.key)).toBe('fresh');
    }
  });
});
