import { FENCED_CACHE_ERRORS } from './fenced-cache.errors';
import { SingleFlight } from './single-flight';
import type {
  FencedCacheErrorEvent,
  FencedCacheEvent,
  FencedCacheOptions,
  GetOrComputeOptions,
  InvalidationResult,
  RedisCommands,
  Serializer,
} from './types';

/**
 * Compare-and-set: writes the data key (KEYS[2]) only while the counter (KEYS[1])
 * still holds the captured generation (ARGV[1]). A missing counter is generation 0.
 * Redis runs this atomically, so no invalidation can interleave with the check.
 */
const FENCED_SET_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if current == false then
  current = '0'
end
if current ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
return 1
`;

/** Capture the generation and the first available entry in one atomic round trip. */
const READ_SCRIPT = `
local generation = redis.call('GET', KEYS[1]) or '0'
local fresh = redis.pcall('GET', KEYS[2])
if type(fresh) == 'table' then
  return {generation, 'freshError', fresh.err}
end
if fresh ~= false then
  return {generation, 'fresh', fresh}
end
if #KEYS == 3 then
  local stale = redis.pcall('GET', KEYS[3])
  if type(stale) == 'table' then
    return {generation, 'staleError', stale.err}
  end
  if stale ~= false then
    return {generation, 'stale', stale}
  end
end
return {generation, 'miss', ''}
`;

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

type CacheEntry = { hit: true; value: unknown } | { hit: false };
type ReadStatus = 'fresh' | 'stale' | 'miss' | 'freshError' | 'staleError';
type CacheSnapshot = { generation: number; status: ReadStatus; raw: string };

function parseGeneration(raw: string): number {
  const parsed = Number(raw);
  if (raw.trim() === '' || !Number.isSafeInteger(parsed)) {
    throw new Error(FENCED_CACHE_ERRORS.invalidGeneration(raw));
  }
  return parsed;
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
 * A Redis cache whose writes are fenced by a per-namespace generation counter.
 *
 * Invalidation bumps the counter before deleting keys, and every write carries the
 * generation captured when its computation started. A write whose generation no
 * longer matches is rejected inside Redis, which is what stops a slow computation
 * from resurrecting data that was invalidated while it ran.
 */
export class FencedCache {
  private readonly redis: RedisCommands;
  private readonly counterKey: string;
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
    // The {namespace} hash tag pins counter and data keys to one cluster slot, and the
    // counter sits outside the data prefix so an invalidation sweep cannot delete it.
    this.counterKey = `{${options.namespace}}:gen`;
    this.dataPrefix = `{${options.namespace}}:k:`;
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
   * Only `loader` failures reach the caller. Cache failures are reported through
   * `onError` and degrade to computing the value without caching it.
   */
  async getOrCompute<T>(
    key: string,
    loader: () => Promise<T>,
    options: GetOrComputeOptions,
  ): Promise<T> {
    assertTtl('ttlMs', options.ttlMs);
    if (options.staleTtlMs !== undefined) {
      assertTtl('staleTtlMs', options.staleTtlMs);
    }
    return this.compute(key, loader, options);
  }

  /** Reads the fresh entry for `key`. `undefined` means a miss; a cached `null` stays `null`. */
  async get<T>(key: string): Promise<T | undefined> {
    const entry = await this.readEntry(this.freshKey(key));
    return entry.hit ? (entry.value as T) : undefined;
  }

  /**
   * Writes `value` only while the namespace is still at `generation`, and reports
   * whether the fence accepted it. `false` means an invalidation happened after the
   * generation was captured, so the value was dropped as stale.
   *
   * Unlike {@link getOrCompute}, this is the low-level escape hatch: serialization
   * and Redis failures are thrown, not suppressed.
   */
  async setIfGeneration(
    key: string,
    value: unknown,
    generation: number,
    options: { ttlMs: number },
  ): Promise<boolean> {
    assertTtl('ttlMs', options.ttlMs);
    const raw = this.serializer.serialize(value);
    return this.fencedSet(key, 'fresh', raw, generation, options.ttlMs);
  }

  /** Current generation of the namespace. An untouched namespace is at generation 0. */
  async generation(): Promise<number> {
    const raw = await this.redis.get(this.counterKey);
    return raw === null ? 0 : parseGeneration(raw);
  }

  /** Advances the generation, invalidating every write captured before this call. */
  async bumpGeneration(): Promise<number> {
    return this.redis.incr(this.counterKey);
  }

  /**
   * Invalidates the namespace: bumps the generation, then sweeps its keys.
   *
   * The order is what makes invalidation complete. Writes arriving after the bump are
   * rejected by the fence, and writes that slipped in just before it are removed by
   * the sweep. Failures are thrown so the caller can retry rather than silently
   * continue with a half-invalidated namespace.
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
  ): Promise<T> {
    let snapshot: CacheSnapshot;
    try {
      snapshot = await this.readSnapshot(key, options.staleTtlMs !== undefined);
    } catch (error) {
      // Without a generation no write can be fenced, so the cache is bypassed entirely.
      this.emitError({ operation: 'generation', key, error });
      return loader();
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
        return value;
      } catch (error) {
        this.emitError({ operation: 'get', key, error });
        cacheReachable = status !== 'fresh';
      }
    }

    this.emitEvent({ type: 'miss', key });
    return this.computeFlights.run(this.flightKey(key, generation), async () => {
      const value = await loader();
      if (cacheReachable) {
        await this.writeBack(key, value, generation, options);
      }
      return value;
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
    generation: number,
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
    generation: number,
    options: GetOrComputeOptions,
  ): Promise<void> {
    try {
      await this.write(key, value, generation, options);
    } catch (error) {
      this.emitError({ operation: 'setIfGeneration', key, error });
    }
  }

  private async write(
    key: string,
    value: unknown,
    generation: number,
    options: GetOrComputeOptions,
  ): Promise<boolean> {
    const raw = this.serializer.serialize(value);
    const accepted = await this.fencedSet(key, 'fresh', raw, generation, options.ttlMs);
    // A rejected fresh write means the generation already moved; since it only ever moves
    // forward, the stale write would be rejected too.
    if (accepted && options.staleTtlMs !== undefined) {
      return this.fencedSet(key, 'stale', raw, generation, options.staleTtlMs);
    }
    return accepted;
  }

  private async fencedSet(
    key: string,
    entry: 'fresh' | 'stale',
    raw: string,
    generation: number,
    ttlMs: number,
  ): Promise<boolean> {
    const storageKey = entry === 'fresh' ? this.freshKey(key) : this.staleKey(key);
    const reply = await this.redis.eval(FENCED_SET_SCRIPT, {
      keys: [this.counterKey, storageKey],
      arguments: [String(generation), raw, String(ttlMs)],
    });
    if (reply === 0) {
      this.emitEvent({ type: 'fenceRejected', key, generation, entry });
    }
    return reply === 1;
  }

  private async readSnapshot(key: string, includeStale: boolean): Promise<CacheSnapshot> {
    const keys = [this.counterKey, this.freshKey(key)];
    if (includeStale) {
      keys.push(this.staleKey(key));
    }
    const reply = await this.redis.eval(READ_SCRIPT, { keys, arguments: [] });
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

  private async readEntry(storageKey: string): Promise<CacheEntry> {
    const raw = await this.redis.get(storageKey);
    if (raw === null) {
      return { hit: false };
    }
    return { hit: true, value: this.serializer.deserialize(raw) };
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
          deleted += await this.redis.unlink(pending);
          pending = [];
        }
      }
    }
    if (pending.length > 0) {
      deleted += await this.redis.unlink(pending);
    }
    return deleted;
  }

  private freshKey(key: string): string {
    return `${this.dataPrefix}f:${key}`;
  }

  private staleKey(key: string): string {
    return `${this.dataPrefix}s:${key}`;
  }

  private flightKey(key: string, generation: number): string {
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
