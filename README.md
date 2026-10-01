# cache-fence

Generation-fenced Redis cache for Node.js. It rejects writes whose computation crossed an invalidation,
and verifies cached values against the current generation before returning them.

Zero runtime dependencies. Bring a node-redis client or a small adapter. Fresh and stale lookups each take
one atomic Redis round trip; concurrent computations share work within one cache instance.

## Install

```sh
npm install cache-fence redis
```

Node.js >= 22. `redis` (node-redis) `>=5.0.0 <7` is an optional peer dependency.
The test suite runs against Redis 7 and 8 and Valkey 8 and 9 in isolated Docker containers, standalone and as a
three-master Cluster, through both node-redis and ioredis.

**Upgrading from 0.1.x:** 0.2.0 changes generation types and Redis storage. Read [Migration](#migration-from-01x)
before deploying readers or invalidators.

## Quickstart

```ts
import { createClient } from 'redis';
import { createFencedCache } from 'cache-fence';

const redis = createClient({ url: process.env.REDIS_URL });
redis.on('error', (error) => logger.warn({ error }, 'Redis connection error'));
await redis.connect();

const cache = createFencedCache({
  redis,
  namespace: 'catalog',
  onError: ({ operation, key, error }) => logger.warn({ operation, key, error }, 'cache degraded'),
});

const works = await cache.getOrCompute('works:list', () => db.listPublishedWorks(), {
  ttlMs: 60_000,
});

// After a database mutation commits, invalidate every affected namespace.
await cache.invalidate();
```

Stale-while-revalidate (SWR) is enabled by adding `staleTtlMs`. It is the total lifetime of the stale copy
from the write, not additional time after fresh expiry, and should exceed `ttlMs`:

```ts
const works = await cache.getOrCompute('works:list', () => db.listPublishedWorks(), {
  ttlMs: 60_000,
  staleTtlMs: 600_000,
});
```

A stale hit returns immediately and starts a fenced background refresh. Rotation invalidates both copies;
SWR never serves a copy from an older generation.

## How fencing works

Deleting a key cannot prevent a slow computation from writing old data back after the deletion:

```text
A: cache miss, starts computing old data
B: database mutation commits, invalidates cache
A: finishes, attempts to cache old data
```

cache-fence coordinates those operations with an opaque UUID generation token:

1. A Lua read atomically initializes missing generation metadata, reads the entry and checks its stamp.
2. The loader runs with that captured token. A Lua compare-and-set accepts its write only if the token still
   matches Redis. Missing metadata rejects the write.
3. Invalidation rotates the token first. Old entries immediately become cache misses, including entries
   whose cleanup has not started or fails.
4. SCAN finds data keys; a bounded Lua cleanup checks their stamps and unlinks obsolete entries atomically.
   A new-generation write that replaces a scanned key is preserved.

Tokens come from `node:crypto.randomUUID()`. They are equality tokens, not ordered counters. Generation
initialization and rotation use fresh UUIDs, avoiding the reset-to-zero problem when metadata is evicted,
expires, is deleted, or Redis is flushed. As with other random identifiers, uniqueness relies on UUID collision
resistance. Surviving data with a different stamp is ignored; new requests cannot join pre-loss computations.

A cache read is verified at the instant its Lua command executes. A request already in progress can still
return its earlier cached or computed value after invalidation. The fence controls cache reads and writes;
it does not cancel in-flight responses or make the database and Redis one transaction.

## API

```ts
import { createFencedCache, FencedCache, FENCED_CACHE_ERRORS } from 'cache-fence';
import type {
  FencedCacheOptions, GetOrComputeOptions, GenerationToken, FencedCacheResult,
  InvalidationResult, FencedCacheErrorEvent, FencedCacheEvent, FencedCacheOperation,
  RedisCommands, Serializer,
} from 'cache-fence';
```

### `createFencedCache(options): FencedCache`

| Option | Default | Meaning |
| --- | --- | --- |
| `redis` | required | Client or `RedisCommands` adapter. |
| `namespace` | required | Non-empty string without `*`, `{` or `}`; groups invalidation and forms the Redis Cluster hash tag. |
| `scanCount` | `1000` | COUNT hint for invalidation SCAN. |
| `unlinkBatchSize` | `1000` | Maximum data keys checked in each atomic cleanup script. |
| `serializer` | JSON | `{ serialize(value: unknown): string; deserialize(raw: string): unknown }`. Receives the payload without its internal stamp. |
| `onError` | none | Synchronous callback for suppressed errors. Handler exceptions are ignored. |
| `onEvent` | none | Synchronous callback for cache outcomes. Handler exceptions are ignored. |

### `getOrCompute<T>(key, loader, options): Promise<T>`

`options`: `{ ttlMs: number; staleTtlMs?: number }`. TTLs must be positive integer milliseconds.
Returns a verified fresh/stale hit, or runs `loader` and attempts a fenced write. A cached `null` is a hit.
Loader errors and invalid options propagate; cache failures are reported through `onError` while the computed
value is still returned. A rejected write also returns the computed value.

Concurrent misses for the same key and generation share a loader and write outcome on one `FencedCache`
instance. Both read APIs below share these computation flights. Use consistent loaders, value types and TTLs
for a key: joined callers use the initiating caller's loader/options. SWR refreshes use a separate flight map.
If no generation can be verified, each request computes independently without caching.

### `getOrComputeResult<T>(key, loader, options): Promise<FencedCacheResult<T>>`

Same behavior as `getOrCompute`, with a per-call outcome:

```ts
const result = await cache.getOrComputeResult('works:list', () => db.listPublishedWorks(), {
  ttlMs: 60_000,
});

// An application can use this outcome to decide whether to allow response caching.
if (result.source === 'computed' && result.write !== 'accepted') {
  response.setHeader('Cache-Control', 'no-store');
}
return result.value;
```

| Field | Values | Meaning |
| --- | --- | --- |
| `value` | `T` | Returned cached or computed value. |
| `source` | `fresh`, `stale`, `computed` | Origin of this response. |
| `generation` | `GenerationToken` or `null` | Captured token; `null` when the snapshot could not be verified. |
| `write` | `not-attempted` | Fresh/stale hit; no foreground write. |
| `write` | `accepted` | All requested foreground writes were accepted. |
| `write` | `rejected` | A foreground write failed the generation check. |
| `write` | `failed` | Serialization, Redis or reply validation failed during write-back. |
| `write` | `skipped` | The snapshot or fresh entry could not be trusted; no write attempted. |

For a stale hit, `write` describes only the serving request. Its background refresh reports through
`onEvent`/`onError`. Fresh and stale copies are separate fenced writes: invalidation can reject the second,
and an error can occur after the first was accepted. `failed` does not prove that nothing reached Redis.
An `accepted` result does not promise future validity or authorize indefinite caching elsewhere: HTTP, CDN,
framework and in-process caches need their own coordinated invalidation.

### Low-level methods

```ts
const generation = await cache.generation(); // opaque GenerationToken; never construct one
const value = await expensiveComputation();
const accepted = await cache.setIfGeneration('report', value, generation, { ttlMs: 60_000 });
```

| Method | Returns | Behavior |
| --- | --- | --- |
| `get<T>(key)` | `Promise<T \| undefined>` | Atomically verifies a fresh entry. Miss is `undefined`; cached `null` remains `null`. Does not serve stale or emit events. |
| `generation()` | `Promise<GenerationToken>` | Returns or atomically initializes the current token; malformed metadata throws. |
| `setIfGeneration(key, value, generation, { ttlMs })` | `Promise<boolean>` | Accepts only a still-current token. Missing metadata rejects the write. Invalid token input throws. |
| `bumpGeneration()` | `Promise<GenerationToken>` | Rotates the token, invalidating previous reads/writes without scanning data keys. |
| `invalidate()` | `Promise<InvalidationResult>` | Rotates, then reclaims obsolete entries; returns `{ generation, deletedKeys }`. |

These methods throw Redis and serialization errors rather than suppressing them. Once rotation succeeds,
cleanup failure cannot make old entries readable again. `invalidate()` still throws so cleanup failures can
be observed and retried; retrying rotates again. `deletedKeys` counts actual deletions, excluding preserved
current entries. Concurrent invalidations may rotate again before a prior call returns.

### Observations and failures

| `event.type` | Fields | Meaning |
| --- | --- | --- |
| `hit` | `key`, `source: 'fresh' \| 'stale'` | Request returned a cached value. |
| `miss` | `key` | Valid snapshot with no usable value. |
| `fenceRejected` | `key`, `generation`, `entry: 'fresh' \| 'stale'` | Write was rejected, including low-level writes. |
| `refreshCompleted` | `key`, `generation`, `accepted` | Background refresh finished its write attempts without an error. |

Hits and misses count requests. Rejections and refresh completion count actual work, including shared work
only once. Events use caller keys without storage prefixes. A fence rejection is an expected outcome and
does not emit `onError`.

| Failure | High-level behavior | `onError.operation` |
| --- | --- | --- |
| Snapshot unavailable, malformed, or invalid generation | Compute independently; skip writes; emit no hit/miss event | `generation` |
| Fresh read/deserialization fails | Compute; skip writes | `get` |
| Stale read/deserialization fails | Compute; attempt fenced writes | `get` |
| Write or serialization fails | Return computed value | `setIfGeneration` |
| Background loader/write fails | Report once per refresh flight; no unhandled rejection | `swrRefresh` |
| Foreground loader fails | Reject caller's promise | none |

Library-owned error messages live in `FENCED_CACHE_ERRORS`: `invalidNamespace`, `invalidGeneration`,
`invalidReadReply`, `invalidWriteReply`, `invalidSweepReply`, `invalidTtl`, `unserializableValue`.
Redis and serializer error messages are preserved.

## Redis storage and adapter

```text
{catalog}:v2:gen         opaque generation token; no TTL
{catalog}:v2:k:f:<key>   fresh entry with TTL
{catalog}:v2:k:s:<key>   stale entry with TTL, when enabled
```

Entries contain `generation + '\n' + serializedPayload`. Custom serializers only see the payload. Treat this
layout as library-owned storage. The version prefix isolates the 0.2 format from older raw payloads.
Namespace glob characters such as `?`, brackets and `\` are escaped in SCAN patterns.

The adapter needs two operations with node-redis v5+ signatures:

```ts
export interface RedisCommands {
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  scanIterator(options: { MATCH: string; COUNT: number }): AsyncIterable<string | string[]>;
}
```

Pass a standalone node-redis client directly. For ioredis (tested with 6.x), use this adapter:

```ts
const adapter: RedisCommands = {
  eval: (script, { keys, arguments: args }) => io.eval(script, keys.length, ...keys, ...args),
  scanIterator: ({ MATCH, COUNT }) => io.scanStream({ match: MATCH, count: COUNT }),
};
```

All scripts must run against a **writable primary**, including reads because they can initialize missing
metadata. Redis permissions must allow `EVAL`, `GET`, `SET`, `GETRANGE`, `UNLINK` and `SCAN`, with access to
all keys in the chosen namespaces. Configure connection deadlines and reconnection behavior in the client.

For Redis Cluster, every key a script touches shares the `{namespace}` hash tag. EVAL routes by its first
key. SCAN cursors belong to individual nodes: the adapter must visit every primary or target the primary
owning the namespace. [test/cluster-fixture.ts](test/cluster-fixture.ts) contains complete node-redis and ioredis
adapters that walk all masters; with ioredis, iterate `cluster.nodes('master')` and call each node's `scanStream`. An incomplete scan leaks obsolete entries until their TTL expires, but verified reads still
reject them after generation rotation.

## Migration from 0.1.x

0.2.0 is a breaking release. Prepare a coordinated cutover:

1. Replace numeric generation annotations with `GenerationToken`. Remove hardcoded `0`, arithmetic and
   ordering comparisons. Capture tokens through the cache and pass them back unchanged.
2. Update explicit adapter object literals to the two-method interface and grant the commands above in ACLs.
   A standalone node-redis client remains directly compatible.
3. Upgrade **all readers, mutation handlers and background invalidators for a namespace together**. Drain old
   processes/jobs before enabling the new version. The two storage versions are isolated: a 0.1 invalidator
   cannot invalidate 0.2 entries and vice versa. An ordinary mixed-version rolling deployment does not provide
   coordinated invalidation; arrange it at the application deployment layer if overlap is required.
4. Expect a cold cache. Old data keys expire on their existing TTLs; old generation counters have no TTL.
   After all old processes/jobs have stopped, old counters may be removed through application operations.
   Do not copy old counters or raw payloads into the new prefix.
5. After restoring an older Redis snapshot or rolling back the application, drain work and rotate every
   affected namespace before serving traffic. A previously used prefix may still contain old entries.

## Operating boundaries

- Invalidate after database commit on every mutation path, including jobs and imports. A loader that reads
  an already-stale database replica can still produce stale data with a current token.
- Use stable namespaces scoped to the data that must be invalidated together. One namespace occupies one
  Redis Cluster slot and retains one metadata key until removed or evicted.
- Memory eviction is supported, including loss of generation metadata under `allkeys-lru`. It can reduce the
  hit rate by invalidating otherwise surviving entries. For dedicated cache Redis, TTL-only eviction policies
  can retain metadata; size memory and choose policy for the actual workload. The library sets TTLs on data.
- Loss of keys and full resets are handled; **rollback to an older surviving token is different**. Snapshot
  restore or replication failover that loses an acknowledged invalidation can restore a previously valid
  token. The library cannot detect that history loss. Rotate after a controlled restore; stronger durability
  across failover requires an external coordination/durability design. Failover, resharding and replica reads
  are not covered by the cluster tests.
- Single-flight is per instance/process. There is no distributed lock, loader timeout or cancellation API.
  Apply deadlines inside the loader; concurrent callers share its outcome.
- SCAN covers the database keyspace. Cleanup cost grows with total database size; `bumpGeneration()` can make
  entries unreadable immediately when physical reclamation can wait for their TTLs.

## Verification and performance

With Docker running:

```sh
npm ci
npm run typecheck
npm run lint
npm test
npm run test:cluster
npm run build
npm run test:consumer
node bench/run.mjs
```

The tests reproduce an unfenced resurrection and exercise real Redis fencing, metadata loss/eviction,
failed cleanup, concurrent replacement, SWR, serialization, per-call results and Cluster slot routing.
Consumer checks install a packed tarball and run both ESM and CJS entry points against Redis.

The server image and the client behind the adapter are selected per run. CI covers every combination:

```sh
REDIS_IMAGE=valkey/valkey:9-alpine REDIS_CLIENT=ioredis npm test
REDIS_IMAGE=redis:8-alpine REDIS_CLIENT=ioredis npm run test:cluster
```

`REDIS_IMAGE` defaults to `redis:7-alpine`; `REDIS_CLIENT` is `node-redis` (default) or `ioredis`.

A local 0.2.0 run on 2026-09-08 (Apple M4 Pro, Node 26.5.0, Redis 7.4.10 in Docker) measured:

| Operation | p50 µs | Mean µs |
| --- | ---: | ---: |
| Fenced write | 98.4 | 107.7 |
| Plain SET with TTL | 91.9 | 99.1 |
| Verified cache hit | 100.8 | 114.8 |
| Raw GET + JSON.parse | 90.3 | 109.2 |

The 10,000-key cleanup median was **17.1 ms** across three rounds. These are local latency measurements,
not production capacity estimates. [bench/](bench/) documents the workload, added stamp cost and caveats.

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache License, Version 2.0](LICENSE-APACHE), at your option.
Unless explicitly stated otherwise, contributions are dual licensed under the same terms.
