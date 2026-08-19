import { RedisContainer } from '@testcontainers/redis';
import { createClient } from 'redis';
import type { RedisCommands } from '../src/index';

// The return type is left inferred on purpose: node-redis resolves the client type
// from the options it was called with, and spelling it out here would not match.
function createRawClient(url: string) {
  return createClient({ url });
}

/** The connected node-redis client, used for baseline scenarios and raw inspection. */
export type RawRedisClient = ReturnType<typeof createRawClient>;

export interface RedisFixture {
  /** The five-operation adapter the library is constructed with. */
  commands: RedisCommands;
  raw: RawRedisClient;
  flush(): Promise<void>;
  stop(): Promise<void>;
}

/** Boots a throwaway Redis container and wires a client to it. One per test file. */
export async function startRedisFixture(): Promise<RedisFixture> {
  const container = await new RedisContainer('redis:7-alpine').start();
  const client = createRawClient(container.getConnectionUrl());
  await client.connect();

  const commands: RedisCommands = {
    get: (key) => client.get(key),
    incr: (key) => client.incr(key),
    eval: (script, options) => client.eval(script, options),
    scanIterator: (options) => client.scanIterator(options),
    unlink: (keys) => client.unlink(keys),
  };

  return {
    commands,
    raw: client,
    flush: async () => {
      await client.flushAll();
    },
    stop: async () => {
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
  counter: (namespace: string): string => `{${namespace}}:gen`,
  fresh: (namespace: string, key: string): string => `{${namespace}}:k:f:${key}`,
  stale: (namespace: string, key: string): string => `{${namespace}}:k:s:${key}`,
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
