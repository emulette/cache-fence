import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCacheErrorEvent, type RedisCommands } from '../src/index';
import { startRedisFixture, storageKeys, type RedisFixture } from './redis-fixture';

describe('atomic cache reads', () => {
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

  it('serves a fresh hit with one Redis round trip', async () => {
    let commands = 0;
    const redis: RedisCommands = {
      ...fx.commands,
      get: (key) => {
        commands += 1;
        return fx.commands.get(key);
      },
      eval: (script, options) => {
        commands += 1;
        return fx.commands.eval(script, options);
      },
    };
    const cache = createFencedCache({ redis, namespace: 'read' });
    await cache.setIfGeneration('item', null, 0, { ttlMs: 60_000 });
    commands = 0;
    let loads = 0;

    const result = await cache.getOrCompute(
      'item',
      async () => {
        loads += 1;
        return 'loaded';
      },
      { ttlMs: 60_000 },
    );

    expect(result).toBeNull();
    expect(loads).toBe(0);
    expect(commands).toBe(1);
  });

  it.each(['fresh', 'stale'] as const)(
    'preserves fail-closed handling of a broken %s entry with one snapshot read',
    async (entry) => {
      const namespace = 'broken-read';
      const storageKey = storageKeys[entry](namespace, 'item');
      // GET on a list returns WRONGTYPE from the actual Redis server.
      await fx.raw.rPush(storageKey, 'not-a-cache-string');
      let reads = 0;
      const errors: FencedCacheErrorEvent[] = [];
      const redis: RedisCommands = {
        ...fx.commands,
        get: (key) => {
          reads += 1;
          return fx.commands.get(key);
        },
        eval: (script, options) => {
          if (options.arguments.length === 0) {
            reads += 1;
          }
          return fx.commands.eval(script, options);
        },
      };
      const cache = createFencedCache({ redis, namespace, onError: (event) => errors.push(event) });

      expect(
        await cache.getOrCompute('item', async () => 'computed', {
          ttlMs: 60_000,
          staleTtlMs: 120_000,
        }),
      ).toBe('computed');

      expect(reads).toBe(1);
      expect(errors).toHaveLength(1);
      expect(errors[0]?.operation).toBe('get');
      expect(String(errors[0]?.error)).toContain('WRONGTYPE');
      if (entry === 'fresh') {
        expect(await fx.raw.lRange(storageKey, 0, -1)).toEqual(['not-a-cache-string']);
      } else {
        expect(await cache.get('item')).toBe('computed');
        expect(await fx.raw.get(storageKey)).toBe(JSON.stringify('computed'));
      }
    },
  );
});
