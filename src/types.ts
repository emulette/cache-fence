/**
 * The minimal Redis surface this library needs, using node-redis v5+ signatures.
 *
 * A node-redis client satisfies it as-is. Other clients (ioredis, wrappers,
 * connection pools) are adapted by supplying these two operations.
 */
export interface RedisCommands {
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  scanIterator(options: { MATCH: string; COUNT: number }): AsyncIterable<string | string[]>;
}

/** Opaque equality token. Obtain it from the cache; do not construct or order tokens. */
export type GenerationToken = string & { readonly __generationToken: unique symbol };

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
 * A fence rejection is not an error and never produces an error event: it means an
 * invalidation or metadata loss crossed the computation and the write was dropped.
 */
export interface FencedCacheErrorEvent {
  operation: FencedCacheOperation;
  /** The cache key as passed by the caller, without namespace prefixes. */
  key?: string;
  error: unknown;
}

/** Optional observations; fence rejections are normal outcomes, not errors. */
export type FencedCacheEvent =
  | { type: 'hit'; key: string; source: 'fresh' | 'stale' }
  | { type: 'miss'; key: string }
  | { type: 'fenceRejected'; key: string; generation: GenerationToken; entry: 'fresh' | 'stale' }
  | { type: 'refreshCompleted'; key: string; generation: GenerationToken; accepted: boolean };

export interface FencedCacheOptions {
  redis: RedisCommands;
  /**
   * Groups keys under one generation token. Used as the `{namespace}` cluster
   * hash tag, so it must not contain `*`, `{` or `}`.
   */
  namespace: string;
  /** COUNT hint for the invalidation SCAN. Default 1000. */
  scanCount?: number;
  /** Keys per atomic cleanup script during invalidation. Default 1000. */
  unlinkBatchSize?: number;
  /** Default: JSON. */
  serializer?: Serializer;
  /** Receives errors the cache suppressed. Exceptions thrown here are ignored. */
  onError?: (event: FencedCacheErrorEvent) => void;
  /** Receives cache outcomes synchronously. Exceptions thrown here are ignored. */
  onEvent?: (event: FencedCacheEvent) => void;
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

/** Outcome observed by this call; it does not guarantee future validity in another cache. */
export type FencedCacheResult<T> =
  | { value: T; source: 'fresh' | 'stale'; generation: GenerationToken; write: 'not-attempted' }
  | {
      value: T;
      source: 'computed';
      /** Null when the Redis snapshot could not be verified. */
      generation: GenerationToken | null;
      /** A failed write may have reached Redis before the error was observed. */
      write: 'accepted' | 'rejected' | 'failed' | 'skipped';
    };

export interface InvalidationResult {
  /** Generation the namespace moved to. */
  generation: GenerationToken;
  /** Keys removed by the sweep. */
  deletedKeys: number;
}
