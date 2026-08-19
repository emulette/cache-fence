import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCacheErrorEvent, type RedisCommands } from '../src/index';

const NAMESPACE = 'catalog';
const KEY = 'works';
const TTL_MS = 60_000;

/** Stands in for a Redis that is down, unreachable or refusing the command. */
const REDIS_DOWN = new Error('the connection to Redis is closed');

/** The counter key sits outside the data prefix, which is how a stub tells them apart. */
function isCounterKey(key: string): boolean {
  return key.endsWith(':gen');
}

interface StubRedisOptions {
  get?: (key: string) => Promise<string | null>;
  incr?: () => Promise<number>;
  eval?: () => Promise<unknown>;
}

interface StubRedis {
  commands: RedisCommands;
  /** Fenced writes attempted, whether they succeeded or not. */
  evalCalls: () => number;
}

/** Every operation rejects unless the test replaces it. */
function createStubRedis(options: StubRedisOptions = {}): StubRedis {
  let evalCalls = 0;
  const commands: RedisCommands = {
    get: options.get ?? (() => Promise.reject(REDIS_DOWN)),
    incr: options.incr ?? (() => Promise.reject(REDIS_DOWN)),
    eval: () => {
      evalCalls += 1;
      return options.eval === undefined ? Promise.reject(REDIS_DOWN) : options.eval();
    },
    scanIterator: (): AsyncIterable<string | string[]> => ({
      async *[Symbol.asyncIterator]() {
        throw REDIS_DOWN;
      },
    }),
    unlink: () => Promise.reject(REDIS_DOWN),
  };
  return { commands, evalCalls: () => evalCalls };
}

describe('fail-closed behaviour when Redis misbehaves', () => {
  let events: FencedCacheErrorEvent[];
  let unhandled: unknown[];
  const collectUnhandled = (reason: unknown): void => {
    unhandled.push(reason);
  };

  beforeEach(() => {
    events = [];
    unhandled = [];
    process.on('unhandledRejection', collectUnhandled);
  });

  afterEach(() => {
    process.off('unhandledRejection', collectUnhandled);
  });

  /** Lets pending microtasks and one macrotask turn run, so a stray rejection surfaces. */
  async function settle(): Promise<void> {
    for (let i = 0; i < 2; i += 1) {
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    }
  }

  it('serves the computed value without caching when Redis is entirely unreachable', async () => {
    const stub = createStubRedis();
    const cache = createFencedCache({
      redis: stub.commands,
      namespace: NAMESPACE,
      onError: (event) => events.push(event),
    });

    const value = await cache.getOrCompute(KEY, async () => 'computed', { ttlMs: TTL_MS });

    expect(value).toBe('computed');
    // Without a readable generation no write can be fenced, so none is attempted.
    expect(stub.evalCalls()).toBe(0);
    expect(events).toHaveLength(1);
    expect(events[0]?.operation).toBe('generation');
    expect(events[0]?.key).toBe(KEY);
    expect(events[0]?.error).toBe(REDIS_DOWN);
  });

  it('reports a broken write path as a setIfGeneration error and still returns the value', async () => {
    const stub = createStubRedis({ get: () => Promise.resolve(null) });
    const cache = createFencedCache({
      redis: stub.commands,
      namespace: NAMESPACE,
      onError: (event) => events.push(event),
    });

    const value = await cache.getOrCompute(KEY, async () => 'computed', { ttlMs: TTL_MS });

    expect(value).toBe('computed');
    expect(stub.evalCalls()).toBe(1);
    expect(events).toHaveLength(1);
    expect(events[0]?.operation).toBe('setIfGeneration');
    expect(events[0]?.error).toBe(REDIS_DOWN);
  });

  it('skips the write entirely when the cache read fails after the generation was captured', async () => {
    const stub = createStubRedis({
      get: (key) => (isCounterKey(key) ? Promise.resolve('7') : Promise.reject(REDIS_DOWN)),
    });
    const cache = createFencedCache({
      redis: stub.commands,
      namespace: NAMESPACE,
      onError: (event) => events.push(event),
    });

    const value = await cache.getOrCompute(KEY, async () => 'computed', { ttlMs: TTL_MS });

    expect(value).toBe('computed');
    // A cache that cannot be read cannot be trusted to be written either.
    expect(stub.evalCalls()).toBe(0);
    expect(events).toHaveLength(1);
    expect(events[0]?.operation).toBe('get');
  });

  it('throws instead of suppressing on the low-level surfaces', async () => {
    const writeBroken = createFencedCache({
      redis: createStubRedis({ get: () => Promise.resolve(null) }).commands,
      namespace: NAMESPACE,
      onError: (event) => events.push(event),
    });
    await expect(writeBroken.setIfGeneration(KEY, 'computed', 0, { ttlMs: TTL_MS })).rejects.toBe(
      REDIS_DOWN,
    );

    const counterBroken = createFencedCache({
      redis: createStubRedis().commands,
      namespace: NAMESPACE,
      onError: (event) => events.push(event),
    });
    await expect(counterBroken.invalidate()).rejects.toBe(REDIS_DOWN);
    await expect(counterBroken.generation()).rejects.toBe(REDIS_DOWN);

    // These are the escape hatches: the caller decides, so nothing is reported for them.
    expect(events).toEqual([]);
  });

  it('survives an onError handler that throws, without leaking an unhandled rejection', async () => {
    const handlerFailure = new Error('the error handler itself is broken');
    const stub = createStubRedis({ get: () => Promise.resolve(null) });
    const cache = createFencedCache({
      redis: stub.commands,
      namespace: NAMESPACE,
      onError: () => {
        throw handlerFailure;
      },
    });

    const value = await cache.getOrCompute(KEY, async () => 'computed', { ttlMs: TTL_MS });
    await settle();

    expect(value).toBe('computed');
    expect(stub.evalCalls()).toBe(1);
    expect(unhandled).toEqual([]);
  });
});
