# cache-fence

Generation-fenced Redis cache for Node.js: it blocks the stale write that lands *after* your invalidation, by rejecting it atomically inside Redis.

- Zero runtime dependencies. Redis client is an optional peer.
- One narrow job. Use it next to the cache library you already have.
- The guarantee is exercised against a real Redis server, not a mock — see [30-second proof](#30-second-proof).

## The problem: stale resurrection

Any backend that combines a Redis cache with explicit invalidation (delete the key on mutation) can hit this
timeline:

```text
        request A                       mutation                    redis
            |                              |                          |
 t0  -------+ GET works:list -> MISS       |                          |
            | start slow compute (3s)      |                          |
            |                              |                          |
 t1  -------+------------------------------+ DEL works:list --------> key removed (correct)
            |                              |                          |
 t2  -------+ compute finished ------------+---- SET works:list ----> pre-t1 data is back
            |                              |                          |
     -------+------------------------------+--------------------------+---->
                    every reader now gets pre-mutation data, until the next
                    invalidation or the TTL expires
```

The write at `t2` carries data that was read *before* the mutation, and it lands *after* the invalidation. It
resurrects exactly what the invalidation just removed.

The reason no amount of deleting fixes this: **delete-based invalidation can only delete keys that exist at the
moment it runs. It cannot stop a write that has not arrived yet.**

Two terms, used consistently throughout this README and the source:

- **Stale resurrection** — the phenomenon: invalidated data reappears in the cache and is served as if fresh.
- **Invalidation-crossing write** — the cause: a write whose computation started before an invalidation and
  whose `SET` completes after it.

The window is exactly as wide as your slowest computation, and it opens on every mutation that races one. It is
rare per request, structural in aggregate, and close to impossible to diagnose from the outside: the cache
contains a value that was true at some point, just not now.

## Why common techniques don't stop it

| Technique | Why the invalidation-crossing write still lands |
| --- | --- |
| TTL | Bounds how long the stale value is served. Does not prevent the resurrection. |
| stale-while-revalidate | Background refreshes make slow computations more frequent, which *widens* the crossing window. |
| single-flight / request coalescing | Removes duplicate concurrent computations. Says nothing about a computation crossing an invalidation. |
| Tag or pattern deletion | Same limit as a single `DEL`: it clears what exists now, not what arrives next. |
| Distributed lock around the computation | Serializes computations, and makes invalidation queue behind the lock. Trades one problem for another. |

These are all worth having. None of them is about ordering a write against an invalidation, which is the only
thing that stops stale resurrection.

## How cache-fence stops it

Generation fencing:

1. **A monotonic counter per namespace.** One Redis integer key, the namespace's *generation*.
2. **The generation is captured when the computation starts** — before the cache read, before the loader runs.
3. **Invalidation bumps the generation first, then sweeps the keys** (`INCR`, then `SCAN` + `UNLINK`).
4. **Every write is an atomic compare-and-set.** A Lua script reads the counter and writes the value only if the
   generation still matches the one the computation captured. If it moved, the script writes nothing and returns
   0. The whole check-and-write is one Redis command, so no invalidation can interleave with it.

The bump-then-sweep order is what makes invalidation complete, and it gives two independent safety nets:

- A write that arrives **after** the bump is rejected by the compare-and-set — its generation is behind.
- A write that slipped in **just before** the bump is removed by the sweep that follows.

The same timeline as above, fenced:

```text
 t0  request A: generation() -> 7, MISS, slow compute starts
 t1  invalidate(): INCR gen -> 8, then SCAN + UNLINK the namespace
 t2  request A finishes, writes with generation 7
     Lua: current generation is 8, 8 != 7 -> return 0, nothing written
     -> next reader misses and computes post-mutation data
```

An invalidation-crossing write is therefore never observable: it is either rejected by the compare-and-set or
removed by the sweep. A rejected write is not an error — it means the value was known to be stale before it was
ever visible, which is the outcome you wanted.

## Install

```sh
npm install cache-fence redis
```

Node.js >= 20. `redis` (node-redis) `>=5.0.0 <7` is an **optional** peer dependency — bring your own client, or
adapt another one through the five-method interface in [Redis client adapter](#redis-client-adapter).
cache-fence itself has no runtime dependencies.

## Quickstart

```ts
import { createClient } from 'redis';
import { createFencedCache } from 'cache-fence';

const redis = createClient({ url: process.env.REDIS_URL });
await redis.connect();

const cache = createFencedCache({ redis, namespace: 'catalog' });

// Read-through. The generation is captured before the cache read; the write-back at the
// end is fenced against it. Concurrent callers for the same key share one computation.
const works = await cache.getOrCompute('works:list', () => db.listPublishedWorks(), {
  ttlMs: 60_000,
});

// On mutation: bump the generation, then sweep the namespace.
await cache.invalidate();
```

Stale-while-revalidate is one option away. The background refresh goes through the same fence:

```ts
const works = await cache.getOrCompute('works:list', () => db.listPublishedWorks(), {
  ttlMs: 60_000,
  staleTtlMs: 600_000, // serve stale up to 10 min while refreshing behind the fence
});
```

Note what is *not* in this code: generation values. On the high-level path you never read, pass, or store one.
Capture, comparison and rejection happen inside `getOrCompute`. The low-level API exists for the cases it does
not cover, and is described [below](#low-level-api).

## 30-second proof

The claim is testable, so test it:

```sh
git clone https://github.com/emulette/cache-fence.git && cd cache-fence
npm install
npm test          # requires Docker: spins up a real Redis via testcontainers
```

`test/race.test.ts` does both halves against a real Redis server:

1. It **reproduces** stale resurrection with an unfenced cache — plain `GET` / `SET` / `DEL`, the pattern every
   cache library implements — and asserts that the deleted value is back in Redis after the late write.
2. It runs the **same interleaving** through cache-fence and asserts the write was rejected: the compare-and-set
   returns 0, the key stays absent, and the next read recomputes.

No mocked Redis, no mocked Lua. CI runs the suite on Node 20, 22 and 24.

## API reference

Everything the package exports:

```ts
import {
  createFencedCache,   // factory
  FencedCache,         // the class, if you prefer `new`
  FENCED_CACHE_ERRORS, // error message constants
} from 'cache-fence';

import type {
  FencedCacheOptions,
  GetOrComputeOptions,
  InvalidationResult,
  FencedCacheErrorEvent,
  FencedCacheOperation,
  RedisCommands,
  Serializer,
} from 'cache-fence';
```

### `createFencedCache(options): FencedCache`

| Option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `redis` | `RedisCommands` | — | Client, or adapter. See below. |
| `namespace` | `string` | — | Groups keys under one generation counter. Non-empty, and must not contain `*`, `{` or `}` (it becomes a cluster hash tag). Invalid values throw from the constructor. |
| `scanCount` | `number` | `1000` | `COUNT` hint for the invalidation `SCAN`. |
| `unlinkBatchSize` | `number` | `1000` | Keys per `UNLINK` call during the sweep. |
| `serializer` | `Serializer` | JSON | `{ serialize(value: unknown): string; deserialize(raw: string): unknown }`. |
| `onError` | `(event: FencedCacheErrorEvent) => void` | — | Receives errors the cache suppressed. Exceptions thrown by the handler are ignored. |

Key layout, for the namespace `catalog`:

```text
{catalog}:gen        generation counter (outside the data prefix, so a sweep cannot delete it)
{catalog}:k:f:<key>  fresh entry
{catalog}:k:s:<key>  stale entry (only when staleTtlMs is set)
```

The sweep matches `{catalog}:k:*`, which is why the counter lives outside that prefix.

### `getOrCompute<T>(key, loader, options): Promise<T>`

`options`: `{ ttlMs: number; staleTtlMs?: number }`. Both must be positive integers; `staleTtlMs` enables
stale-while-revalidate and should exceed `ttlMs`.

Sequence: capture the generation → read the fresh entry → (if SWR) read the stale entry and, on a hit, serve it
immediately while a fenced background refresh runs → otherwise run `loader` → write back through the fence.

- **Single-flight**: concurrent callers for the same key in the same process share one computation. In-process
  deduplication only — it is not a distributed lock, and does not claim to be.
- **Loader errors propagate.** They are yours; the cache does not swallow them.
- **A fence rejection is silent and correct.** The computed value is still returned to the caller, it is just not
  cached. No error, no event.
- A cached `null` is a hit and comes back as `null`.
- **With SWR**, the background refresh reuses the generation captured when the serving request started, so a
  refresh spanning an invalidation is rejected too. Refreshes are deduplicated per key, so they cannot stack, and
  the stale entry is only written when the fresh write was accepted.

### `get<T>(key): Promise<T | undefined>`

Reads the fresh entry. `undefined` means a miss (a cached `null` returns `null`). Stale entries are not
consulted here — only `getOrCompute` serves those. Redis and deserialization errors are thrown.

### Low-level API

The escape hatch, for computations `getOrCompute` doesn't wrap: write paths, jobs, or your own composition. This
is the library's actual primitive.

```ts
const gen = await cache.generation();          // 0 for an untouched namespace
const value = await expensiveComputation();
const accepted = await cache.setIfGeneration('works:list', value, gen, { ttlMs: 60_000 });
if (!accepted) {
  // An invalidation crossed the computation. The value was dropped; this is the fence working.
}
```

| Method | Returns | Notes |
| --- | --- | --- |
| `generation()` | `Promise<number>` | Current generation; `0` if the counter does not exist. Throws if the counter holds a non-integer. |
| `setIfGeneration(key, value, generation, { ttlMs })` | `Promise<boolean>` | `true` if written, `false` if the fence rejected it. Serialization and Redis errors are **thrown**, unlike on the `getOrCompute` path. |
| `bumpGeneration()` | `Promise<number>` | `INCR` on the counter, returns the new generation. Invalidates every generation captured before this call, without touching keys. |
| `invalidate()` | `Promise<InvalidationResult>` | `{ generation, deletedKeys }`. Bumps, then sweeps. |

`invalidate()` performs bump-then-sweep in that order, deduplicating keys returned more than once by `SCAN` and
unlinking them in batches. Failures are thrown rather than reported through `onError`: a half-invalidated
namespace is something the caller must be able to see and retry.

### `FENCED_CACHE_ERRORS`

Every message this library can produce, as functions. Nothing else in the package builds message strings, so
tests can assert against these instead of hardcoding text: `invalidNamespace`, `invalidGeneration`,
`invalidTtl`, `unserializableValue`.

### Redis client adapter

The whole Redis surface is five operations, in node-redis v5+ signatures:

```ts
export interface RedisCommands {
  get(key: string): Promise<string | null>;
  incr(key: string): Promise<number>;
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  scanIterator(options: { MATCH: string; COUNT: number }): AsyncIterable<string | string[]>;
  unlink(keys: string[]): Promise<number>;
}
```

A node-redis client satisfies this as-is — pass it straight to `createFencedCache`. Any other client, wrapper or
connection pool is roughly ten lines. The shape, using ioredis names (illustrative, not a shipped adapter):

```ts
const adapter: RedisCommands = {
  get: (key) => io.get(key),
  incr: (key) => io.incr(key),
  eval: (script, { keys, arguments: args }) => io.eval(script, keys.length, ...keys, ...args),
  scanIterator: ({ MATCH, COUNT }) => io.scanStream({ match: MATCH, count: COUNT }),
  unlink: (keys) => io.unlink(...keys),
};
```

`scanIterator` may yield single keys or batches; both are handled.

## Failure semantics: fail-closed

When Redis is unreachable, the correct answer is a slow answer, not a fast wrong one. `getOrCompute` degrades to
**computing without caching** and reports through `onError`. It never serves something it cannot verify.

| Situation | Behavior | `onError` event |
| --- | --- | --- |
| Generation read fails | Run `loader`, return the value, cache nothing (an unfenceable write is not attempted) | `operation: 'generation'` |
| Fresh read fails | Run `loader`, return the value, skip the write-back | `operation: 'get'` |
| Stale read fails (SWR) | Fall through to `loader`; the fenced write-back is still attempted | `operation: 'get'` |
| Write fails (Redis error or serialization) | Value is still returned to the caller | `operation: 'setIfGeneration'` |
| Background SWR refresh fails | Nothing surfaces to any caller; never an unhandled rejection | `operation: 'swrRefresh'` |
| **Fence rejects the write** | Value returned, not cached | **none — this is not an error** |
| `loader` throws | Propagates to the caller | none |

```ts
const cache = createFencedCache({
  redis,
  namespace: 'catalog',
  onError: ({ operation, key, error }) => logger.warn({ operation, key, error }, 'cache degraded'),
});
```

Two consequences worth stating explicitly:

- **The low-level API and `invalidate()` throw instead of degrading.** Callers of a primitive must see failures.
- **SWR refreshes are fenced like any other write.** Serving stale never means writing unverified data: if an
  invalidation crossed the refresh, the refresh's write is rejected and the entry simply expires.

## Design constraints, and what this is not

- **Not another cache framework.** No layers, no drivers, no decorators, no key catalog. Competing on a feature
  matrix would mean becoming the thing this exists to avoid.
- **Use it alongside your cache library.** cache-manager, cachified, bentocache and friends solve stampedes and
  SWR well, and none of them fences invalidation-crossing writes. Keep them for the bulk of your cache; put
  cache-fence on the hot spots where stale data is a bug rather than a delay — inventory, permissions, pricing,
  publication state.
- **Zero runtime dependencies.** The Redis client is an optional peer dependency. Nothing else is installed.
- **Redis-only, by design.** The guarantee is server-side atomicity via Lua. A generic storage adapter cannot
  promise that, so there isn't one.
- **Redis Cluster**: the counter and the data keys share the `{namespace}` hash tag, so they land in the same
  slot and the Lua script never hits `CROSSSLOT`. Cluster is designed for, not yet covered by tests — see
  [Status](#status-and-roadmap).
- **One generation per namespace.** Invalidation is namespace-wide by construction. Use narrower namespaces for
  narrower blast radius.
- **In-process single-flight.** Deduplicates within one process. Cross-process stampede control is not this
  library's job; the fence is orthogonal to it.

## Prior art

The mechanism has been proven for over a decade. What did not exist is a general-purpose package for Redis and
Node.

- **Facebook memcache leases** (NSDI 2013) — the same idea: a token issued on miss, invalidated on write, used to
  reject stale set-backs. Implemented inside the memcached server, so not portable to Redis.
- **Martin Kleppmann's fencing tokens** (2016) — the same ordering argument, formalized for distributed locks and
  shared storage rather than caches.
- **Uber CacheFront** — Lua compare-and-set rejection keyed on row timestamps, operated at very large read
  volume. Internal infrastructure, not a published library.

cache-fence packages that mechanism for Redis and Node, with an explicit compare-and-set API rather than an
implicit one.

## Status and roadmap

**v0.1.0, pre-1.0.** The API surface is deliberately small, but it may still change before 1.0.

The fencing mechanism is derived from a pattern running in a production backend; this *package* is new, and does
not yet have a production track record of its own. What can be checked today is the guarantee itself: it is
exercised against a real Redis server via testcontainers, in CI, on every push.

Roadmap:

- Benchmarks: fenced write vs plain `SET` (one Lua round trip), and invalidation sweep cost at scale. The price
  of correctness should be a number, not an adjective.
- Integration examples: plain node-redis, alongside cache-manager, and NestJS.
- Redis Cluster test coverage, to turn the hash-tag design from reasoned into verified.

Issues and PRs are welcome, particularly reproductions of stale resurrection in real systems.

## License

Licensed under either of

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this work by
you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or
conditions.
