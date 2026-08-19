/**
 * The minimal Redis surface this library needs, using node-redis v5+ signatures.
 *
 * A node-redis client satisfies it as-is. Other clients (ioredis, wrappers,
 * connection pools) are adapted by supplying these five operations.
 */
export interface RedisCommands {
  get(key: string): Promise<string | null>;
  incr(key: string): Promise<number>;
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  scanIterator(options: { MATCH: string; COUNT: number }): AsyncIterable<string | string[]>;
  unlink(keys: string[]): Promise<number>;
}

/** Converts cached values to and from the strings Redis stores. Defaults to JSON. */
export interface Serializer {
  serialize(value: unknown): string;
  deserialize(raw: string): unknown;
}

/** Cache operation an error event originated from. */
export type FencedCacheOperation = 'generation' | 'get' | 'setIfGeneration' | 'swrRefresh';

/**
 * Emitted for errors the cache suppressed to stay fail-closed, such as a skipped
 * write while Redis is unreachable or a failed background refresh.
 *
 * A fence rejection is not an error and never produces an event: it means an
 * invalidation crossed the computation and the write was correctly dropped.
 */
export interface FencedCacheErrorEvent {
  operation: FencedCacheOperation;
  /** The cache key as passed by the caller, without namespace prefixes. */
  key?: string;
  error: unknown;
}

export interface FencedCacheOptions {
  redis: RedisCommands;
  /**
   * Groups keys under one generation counter. Used as the `{namespace}` cluster
   * hash tag, so it must not contain `*`, `{` or `}`.
   */
  namespace: string;
  /** COUNT hint for the invalidation SCAN. Default 1000. */
  scanCount?: number;
  /** Keys per UNLINK call during invalidation. Default 1000. */
  unlinkBatchSize?: number;
  /** Default: JSON. */
  serializer?: Serializer;
  /** Receives errors the cache suppressed. Exceptions thrown here are ignored. */
  onError?: (event: FencedCacheErrorEvent) => void;
}

export interface GetOrComputeOptions {
  /** Lifetime of the fresh entry, in milliseconds. */
  ttlMs: number;
  /**
   * Enables stale-while-revalidate when set, and should exceed `ttlMs`. A stale
   * hit is served immediately while a background refresh runs; the refresh is
   * fenced by the same generation check as any other write.
   */
  staleTtlMs?: number;
}

export interface InvalidationResult {
  /** Generation the namespace moved to. */
  generation: number;
  /** Keys removed by the sweep. */
  deletedKeys: number;
}
