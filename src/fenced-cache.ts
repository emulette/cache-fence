import { randomUUID } from 'node:crypto';
import { FENCED_CACHE_ERRORS } from './fenced-cache.errors';
import { FENCED_SET_SCRIPT, GENERATION_SCRIPT, READ_SCRIPT, SWEEP_SCRIPT } from './redis-scripts';
import { SingleFlight } from './single-flight';
import type {
  FencedCacheErrorEvent,
  FencedCacheEvent,
  FencedCacheOptions,
  FencedCacheResult,
  GenerationToken,
  GetOrComputeOptions,
  InvalidationResult,
  RedisCommands,
  Serializer,
} from './types';

const DEFAULT_SCAN_COUNT = 1000;
const DEFAULT_UNLINK_BATCH_SIZE = 1000;

/** `undefined`, functions and symbols make JSON.stringify return undefined rather than a string. */
const JSON_SERIALIZER: Serializer = {
  serialize(value: unknown): string {
    const raw = JSON.stringify(value);
    if (raw === undefined) {
      throw new Error(FENCED_CACHE_ERRORS.unserializableValue());
    }
    return raw;
  },
  deserialize(raw: string): unknown {
    return JSON.parse(raw) as unknown;
  },
};

type ReadStatus = 'fresh' | 'stale' | 'miss' | 'freshError' | 'staleError';
type CacheSnapshot = { generation: GenerationToken; status: ReadStatus; raw: string };

function parseGeneration(raw: unknown): GenerationToken {
  if (
    typeof raw !== 'string' ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(raw)
  ) {
    throw new Error(FENCED_CACHE_ERRORS.invalidGeneration(raw));
  }
  return raw as GenerationToken;
}

function assertNamespace(namespace: string): void {
  if (typeof namespace !== 'string' || namespace.length === 0 || /[*{}]/.test(namespace)) {
    throw new Error(FENCED_CACHE_ERRORS.invalidNamespace(namespace));
  }
}

function assertTtl(option: string, value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(FENCED_CACHE_ERRORS.invalidTtl(option, value));
  }
}

/**
 * A Redis cache whose writes are fenced by a per-namespace generation token.
 *
 * Invalidation rotates the token before deleting obsolete keys, and every write carries the
 * generation captured when its computation started. A write whose generation no
 * longer matches is rejected inside Redis, which is what stops a slow computation
 * from resurrecting data that was invalidated while it ran.
 */
export class FencedCache {
  private readonly redis: RedisCommands;
  private readonly generationKey: string;
  private readonly dataPrefix: string;
  private readonly scanCount: number;
  private readonly unlinkBatchSize: number;
  private readonly serializer: Serializer;
  private readonly onError?: (event: FencedCacheErrorEvent) => void;
  private readonly onEvent?: (event: FencedCacheEvent) => void;
  private readonly computeFlights = new SingleFlight();
  private readonly refreshFlights = new SingleFlight();

  constructor(options: FencedCacheOptions) {
    assertNamespace(options.namespace);
    this.redis = options.redis;
    // Versioned storage isolates the token format from 0.1.x counters and raw entries.
    // The hash tag pins every key touched by Lua to one Redis Cluster slot.
    this.generationKey = `{${options.namespace}}:v2:gen`;
    this.dataPrefix = `{${options.namespace}}:v2:k:`;
    this.scanCount = options.scanCount ?? DEFAULT_SCAN_COUNT;
    this.unlinkBatchSize = options.unlinkBatchSize ?? DEFAULT_UNLINK_BATCH_SIZE;
    this.serializer = options.serializer ?? JSON_SERIALIZER;
    this.onError = options.onError;
    this.onEvent = options.onEvent;
  }

  /**
   * Returns the cached value for `key`, or computes it with `loader` and caches the
   * result behind the fence.
   *
   * The generation is captured before any cache read, so an invalidation that lands
   * while `loader` runs causes the write-back to be rejected instead of resurrecting
   * stale data. Concurrent callers for the same key and generation share one computation.
   *
   * Loader failures and invalid options reach the caller. Cache failures are reported through
   * `onError` and degrade to computing the value without caching it.
   */
  async getOrCompute<T>(
    key: string,
    loader: () => Promise<T>,
    options: GetOrComputeOptions,
  ): Promise<T> {
    return (await this.getOrComputeResult(key, loader, options)).value;
  }

  /** Returns the value and this call's cache outcome, including rejected write-backs. */
  async getOrComputeResult<T>(
    key: string,
    loader: () => Promise<T>,
    options: GetOrComputeOptions,
  ): Promise<FencedCacheResult<T>> {
    assertTtl('ttlMs', options.ttlMs);
    if (options.staleTtlMs !== undefined) {
      assertTtl('staleTtlMs', options.staleTtlMs);
    }
    return this.compute(key, loader, options);
  }

  /** Reads the fresh entry for `key`. `undefined` means a miss; a cached `null` stays `null`. */
  async get<T>(key: string): Promise<T | undefined> {
    const snapshot = await this.readSnapshot(key, false);
    if (snapshot.status === 'freshError') {
      throw new Error(snapshot.raw);
    }
    return snapshot.status === 'fresh'
      ? (this.serializer.deserialize(snapshot.raw) as T)
      : undefined;
  }

  /**
   * Writes `value` only while the namespace is still at `generation`, and reports
   * whether the fence accepted it. `false` means the generation changed or was lost
   * after capture, so the value was dropped.
   *
   * Unlike {@link getOrCompute}, this is the low-level escape hatch: serialization
   * and Redis failures are thrown, not suppressed.
   */
  async setIfGeneration(
    key: string,
    value: unknown,
    generation: GenerationToken,
    options: { ttlMs: number },
  ): Promise<boolean> {
    assertTtl('ttlMs', options.ttlMs);
    parseGeneration(generation);
    const raw = this.serializer.serialize(value);
    return this.fencedSet(key, 'fresh', raw, generation, options.ttlMs);
  }

  /** Current opaque token. Atomically initializes one if the metadata is missing. */
  async generation(): Promise<GenerationToken> {
    return this.updateGeneration('initialize');
  }

  /** Rotates the token, making previous entries unreadable and previous writes invalid. */
  async bumpGeneration(): Promise<GenerationToken> {
    return this.updateGeneration('rotate');
  }

  /**
   * Rotates the token, then reclaims obsolete entries while preserving current writes.
   * Errors propagate. Once rotation succeeds, old entries remain unreadable even if
   * cleanup fails. Retry invalidate() to rotate again and retry the cleanup.
   */
  async invalidate(): Promise<InvalidationResult> {
    const generation = await this.bumpGeneration();
    const deletedKeys = await this.sweep();
    return { generation, deletedKeys };
  }

  private async compute<T>(
    key: string,
    loader: () => Promise<T>,
    options: GetOrComputeOptions,
  ): Promise<FencedCacheResult<T>> {
    let snapshot: CacheSnapshot;
    try {
      snapshot = await this.readSnapshot(key, options.staleTtlMs !== undefined);
    } catch (error) {
      // Without a generation no write can be fenced, so the cache is bypassed entirely.
      this.emitError({ operation: 'generation', key, error });
      return { value: await loader(), source: 'computed', generation: null, write: 'skipped' };
    }

    const { generation, status, raw } = snapshot;
    let cacheReachable = status !== 'freshError';
    if (status === 'freshError' || status === 'staleError') {
      this.emitError({ operation: 'get', key, error: new Error(raw) });
    } else if (status === 'fresh' || status === 'stale') {
      try {
        const value = this.serializer.deserialize(raw) as T;
        this.emitEvent({ type: 'hit', key, source: status });
        if (status === 'stale') {
          this.startRefresh(key, loader, generation, options);
        }
        return { value, source: status, generation, write: 'not-attempted' };
      } catch (error) {
        this.emitError({ operation: 'get', key, error });
        cacheReachable = status !== 'fresh';
      }
    }

    this.emitEvent({ type: 'miss', key });
    return this.computeFlights.run(this.flightKey(key, generation), async () => {
      const value = await loader();
      const write = cacheReachable
        ? await this.writeBack(key, value, generation, options)
        : 'skipped';
      return { value, source: 'computed', generation, write };
    });
  }

  /**
   * Fire-and-forget stale-while-revalidate refresh, deduplicated per key and generation.
   *
   * Failures are reported inside the flight, so one failed refresh emits one event no
   * matter how many callers joined it, and the shared promise can never reject.
   */
  private startRefresh<T>(
    key: string,
    loader: () => Promise<T>,
    generation: GenerationToken,
    options: GetOrComputeOptions,
  ): void {
    void this.refreshFlights.run(this.flightKey(key, generation), async () => {
      try {
        const value = await loader();
        const accepted = await this.write(key, value, generation, options);
        this.emitEvent({ type: 'refreshCompleted', key, generation, accepted });
      } catch (error) {
        this.emitError({ operation: 'swrRefresh', key, error });
      }
    });
  }

  private async writeBack(
    key: string,
    value: unknown,
    generation: GenerationToken,
    options: GetOrComputeOptions,
  ): Promise<'accepted' | 'rejected' | 'failed'> {
    try {
      return (await this.write(key, value, generation, options)) ? 'accepted' : 'rejected';
    } catch (error) {
      this.emitError({ operation: 'setIfGeneration', key, error });
      return 'failed';
    }
  }

  private async write(
    key: string,
    value: unknown,
    generation: GenerationToken,
    options: GetOrComputeOptions,
  ): Promise<boolean> {
    const raw = this.serializer.serialize(value);
    const accepted = await this.fencedSet(key, 'fresh', raw, generation, options.ttlMs);
    // A rejected fresh write means this token is no longer current; skip the stale copy.
    if (accepted && options.staleTtlMs !== undefined) {
      return this.fencedSet(key, 'stale', raw, generation, options.staleTtlMs);
    }
    return accepted;
  }

  private async fencedSet(
    key: string,
    entry: 'fresh' | 'stale',
    raw: string,
    generation: GenerationToken,
    ttlMs: number,
  ): Promise<boolean> {
    const storageKey = entry === 'fresh' ? this.freshKey(key) : this.staleKey(key);
    const reply = await this.redis.eval(FENCED_SET_SCRIPT, {
      keys: [this.generationKey, storageKey],
      arguments: [generation, raw, String(ttlMs)],
    });
    if (reply !== 0 && reply !== 1) {
      throw new Error(FENCED_CACHE_ERRORS.invalidWriteReply());
    }
    if (reply === 0) {
      this.emitEvent({ type: 'fenceRejected', key, generation, entry });
    }
    return reply === 1;
  }

  private async readSnapshot(key: string, includeStale: boolean): Promise<CacheSnapshot> {
    const keys = [this.generationKey, this.freshKey(key)];
    if (includeStale) {
      keys.push(this.staleKey(key));
    }
    const reply = await this.redis.eval(READ_SCRIPT, { keys, arguments: [randomUUID()] });
    if (
      !Array.isArray(reply) ||
      reply.length !== 3 ||
      typeof reply[0] !== 'string' ||
      typeof reply[2] !== 'string' ||
      (reply[1] !== 'fresh' &&
        reply[1] !== 'stale' &&
        reply[1] !== 'miss' &&
        reply[1] !== 'freshError' &&
        reply[1] !== 'staleError')
    ) {
      throw new Error(FENCED_CACHE_ERRORS.invalidReadReply());
    }
    return { generation: parseGeneration(reply[0]), status: reply[1], raw: reply[2] };
  }

  private async updateGeneration(mode: 'initialize' | 'rotate'): Promise<GenerationToken> {
    const reply = await this.redis.eval(GENERATION_SCRIPT, {
      keys: [this.generationKey],
      arguments: [randomUUID(), mode],
    });
    return parseGeneration(reply);
  }

  private async sweepBatch(keys: string[]): Promise<number> {
    const reply = await this.redis.eval(SWEEP_SCRIPT, {
      keys: [this.generationKey, ...keys],
      arguments: [],
    });
    if (
      typeof reply !== 'number' ||
      !Number.isSafeInteger(reply) ||
      reply < 0 ||
      reply > keys.length
    ) {
      throw new Error(FENCED_CACHE_ERRORS.invalidSweepReply());
    }
    return reply;
  }

  private async sweep(): Promise<number> {
    // SCAN may return the same key more than once, so dedupe before unlinking in batches.
    const seen = new Set<string>();
    let deleted = 0;
    let pending: string[] = [];
    const iterator = this.redis.scanIterator({
      MATCH: `${this.dataPrefix.replace(/[\\*?[\]]/g, '\\$&')}*`,
      COUNT: this.scanCount,
    });
    for await (const entry of iterator) {
      const batch = Array.isArray(entry) ? entry : [entry];
      for (const key of batch) {
        if (seen.has(key)) {
          continue;
        }
        seen.add(key);
        pending.push(key);
        if (pending.length >= this.unlinkBatchSize) {
          deleted += await this.sweepBatch(pending);
          pending = [];
        }
      }
    }
    if (pending.length > 0) {
      deleted += await this.sweepBatch(pending);
    }
    return deleted;
  }

  private freshKey(key: string): string {
    return `${this.dataPrefix}f:${key}`;
  }

  private staleKey(key: string): string {
    return `${this.dataPrefix}s:${key}`;
  }

  private flightKey(key: string, generation: GenerationToken): string {
    return `${generation}:${this.freshKey(key)}`;
  }

  private emitEvent(event: FencedCacheEvent): void {
    try {
      this.onEvent?.(event);
    } catch {
      // Observers must not change the cache outcome or interrupt a refresh.
    }
  }

  private emitError(event: FencedCacheErrorEvent): void {
    if (this.onError === undefined) {
      return;
    }
    try {
      this.onError(event);
    } catch {
      // A throwing handler must not break the cache path or escape as an unhandled rejection.
    }
  }
}

/** Creates a {@link FencedCache} for one namespace. */
export function createFencedCache(options: FencedCacheOptions): FencedCache {
  return new FencedCache(options);
}
