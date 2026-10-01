import { RedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { createClient } from 'redis';
import type { RedisCommands } from '../src/index';

/** Server image under test. CI runs the suite once per supported Redis and Valkey image. */
export const REDIS_IMAGE = process.env.REDIS_IMAGE ?? 'redis:7-alpine';

const REDIS_CLIENTS = ['node-redis', 'ioredis'] as const;
type RedisClientName = (typeof REDIS_CLIENTS)[number];

function isRedisClientName(value: string): value is RedisClientName {
  return (REDIS_CLIENTS as readonly string[]).includes(value);
}

/**
 * Client library behind the adapter the cache is constructed with.
 *
 * Only the library's two operations switch clients; raw inspection always uses node-redis,
 * so every scenario asserts the same Redis state whichever client produced it.
 */
export const REDIS_CLIENT: RedisClientName = (() => {
  const value = process.env.REDIS_CLIENT ?? 'node-redis';
  if (!isRedisClientName(value)) {
    throw new Error(`REDIS_CLIENT must be one of ${REDIS_CLIENTS.join(', ')}, got ${value}`);
  }
  return value;
})();

// The return type is left inferred on purpose: node-redis resolves the client type
// from the options it was called with, and spelling it out here would not match.
function createRawClient(url: string) {
  return createClient({ url });
}

/** The connected node-redis client, used for baseline scenarios and raw inspection. */
export type RawRedisClient = ReturnType<typeof createRawClient>;

export interface RedisFixture {
  /** The two-operation adapter the library is constructed with. */
  commands: RedisCommands;
  raw: RawRedisClient;
  flush(): Promise<void>;
  stop(): Promise<void>;
}

/** Boots a throwaway Redis container and wires a client to it. One per test file. */
export async function startRedisFixture(): Promise<RedisFixture> {
  const container = await new RedisContainer(REDIS_IMAGE).start();
  const client = createRawClient(container.getConnectionUrl());
  await client.connect();

  const io = REDIS_CLIENT === 'ioredis' ? new Redis(container.getConnectionUrl()) : undefined;
  const commands: RedisCommands = io
    ? // The README's ioredis adapter, verbatim.
      {
        eval: (script, { keys, arguments: args }) => io.eval(script, keys.length, ...keys, ...args),
        scanIterator: ({ MATCH, COUNT }) => io.scanStream({ match: MATCH, count: COUNT }),
      }
    : {
        eval: (script, options) => client.eval(script, options),
        scanIterator: (options) => client.scanIterator(options),
      };

  return {
    commands,
    raw: client,
    flush: async () => {
      await client.flushAll();
    },
    stop: async () => {
      io?.disconnect();
      await client.close();
      await container.stop();
    },
  };
}

/**
 * Mirrors the library's internal key layout.
 *
 * Only used where inspecting Redis directly *is* the assertion (a custom
 * serializer's stored bytes, a stale entry that has no public reader).
 * Everything else goes through the public API.
 */
export const storageKeys = {
  counter: (namespace: string): string => `{${namespace}}:v2:gen`,
  fresh: (namespace: string, key: string): string => `{${namespace}}:v2:k:f:${key}`,
  stale: (namespace: string, key: string): string => `{${namespace}}:v2:k:s:${key}`,
};

/** A promise plus its resolver, used as a gate to hold a computation open. */
export function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

export interface PollOptions {
  timeoutMs?: number;
  intervalMs?: number;
  /** Named in the timeout message, e.g. "the background refresh to land". */
  description?: string;
}

/**
 * Waits until `predicate` holds, or fails with a message naming what never happened.
 *
 * Fire-and-forget work (SWR refresh) has no promise to await, so tests wait on its
 * observable effect instead of on a fixed sleep.
 */
export async function poll(
  predicate: () => boolean | Promise<boolean>,
  options: PollOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 5_000;
  const intervalMs = options.intervalMs ?? 10;
  const description = options.description ?? 'condition';
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    if (await predicate()) {
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${description}`);
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, intervalMs);
    });
  }
}
