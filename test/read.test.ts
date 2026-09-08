import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  createFencedCache,
  FENCED_CACHE_ERRORS,
  type FencedCacheErrorEvent,
  type RedisCommands,
} from '../src/index';
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

  it('starts with an isolated cold cache when legacy counters and entries exist', async () => {
    await fx.raw.set('{upgrade}:gen', '7');
    await fx.raw.set('{upgrade}:k:f:item', '"legacy"', { PX: 60_000 });
    const cache = createFencedCache({ redis: fx.commands, namespace: 'upgrade' });
    expect(await cache.get('item')).toBeUndefined();
    expect(await cache.getOrCompute('item', async () => 'current', { ttlMs: 60_000 })).toBe(
      'current',
    );
    await cache.invalidate();
    expect(await cache.get('item')).toBeUndefined();
    expect(await fx.raw.get('{upgrade}:k:f:item')).toBe('"legacy"');
    expect(await fx.raw.get('{upgrade}:gen')).toBe('7');
  });

  it.each(['0', 'not-a-token'])(
    'fails closed for corrupted generation %s even when an entry matches it',
    async (raw) => {
      const errors: FencedCacheErrorEvent[] = [];
      const cache = createFencedCache({
        redis: fx.commands,
        namespace: 'corrupt',
        onError: (event) => errors.push(event),
      });
      await fx.raw.set(storageKeys.counter('corrupt'), raw);
      await fx.raw.set(storageKeys.fresh('corrupt', 'item'), `${raw}\n"unverified"`);
      await expect(cache.generation()).rejects.toThrow(FENCED_CACHE_ERRORS.invalidGeneration(raw));
      await expect(cache.get('item')).rejects.toThrow(FENCED_CACHE_ERRORS.invalidGeneration(raw));
      expect(
        await cache.getOrComputeResult('item', async () => 'computed', { ttlMs: 60_000 }),
      ).toEqual({
        value: 'computed',
        source: 'computed',
        generation: null,
        write: 'skipped',
      });
      expect(errors[0]?.operation).toBe('generation');
      await cache.bumpGeneration();
      expect(await cache.getOrCompute('item', async () => 'recovered', { ttlMs: 60_000 })).toBe(
        'recovered',
      );
    },
  );

  it('treats unstamped data as a miss instead of passing it to the serializer', async () => {
    const cache = createFencedCache({ redis: fx.commands, namespace: 'unstamped' });
    await fx.raw.set(storageKeys.fresh('unstamped', 'item'), '"raw"');
    expect(await cache.get('item')).toBeUndefined();
    expect(await cache.getOrCompute('item', async () => 'verified', { ttlMs: 60_000 })).toBe(
      'verified',
    );
  });

  it('serves a fresh hit with one Redis round trip', async () => {
    let commands = 0;
    const redis: RedisCommands = {
      ...fx.commands,
      eval: (script, options) => {
        commands += 1;
        return fx.commands.eval(script, options);
      },
    };
    const cache = createFencedCache({ redis, namespace: 'read' });
    await cache.setIfGeneration('item', null, await cache.generation(), { ttlMs: 60_000 });
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
        eval: (script, options) => {
          if (options.arguments.length === 1) {
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
        expect(await fx.raw.get(storageKey)).toBe(
          `${await cache.generation()}\n${JSON.stringify('computed')}`,
        );
      }
    },
  );
});
