# cache-fence benchmarks

Fencing is not free. These benchmarks put a number on it, so the trade-off can be
argued with data instead of adjectives: **what does the correctness guarantee cost
against the unfenced Redis primitives it replaces?**

Everything runs against a throwaway `redis:7-alpine` container started by
testcontainers. No new dependencies: `node:perf_hooks`-grade timing via
`process.hrtime.bigint()`, the `redis` and `@testcontainers/redis` devDependencies
the test suite already uses, and no test framework.

## Reproducing

Requires Docker running and Node >= 22.

```sh
npm install
npm run build
node bench/run.mjs
```

The script builds `dist/` with `npm run build` if it is missing, boots the
container, prints one markdown table per benchmark, and tears the container down.
It takes a few seconds. Nothing outside `dist/` is written, and `bench/` is not
part of the published package.

## What each benchmark measures

### 1. Fenced write vs plain SET

`setIfGeneration()` against a raw `SET key value PX ttl` of the same serialized
value, on the same connection: N=5,000, warmup 500.

The fenced write is one `EVAL` round trip whose Lua body does a counter `GET`, a
comparison against the captured generation, and a conditional `SET`. So it is the
same single round trip as the baseline, plus the Lua interpreter and one
server-side `GET`. The baseline calls `JSON.stringify` per operation too, exactly
as `setIfGeneration` does internally, so the delta is the fence and not the
serializer.

### 2. getOrCompute hit path vs raw GET

One primed key, then N=5,000 `getOrCompute()` cache hits against N raw `GET`s of
the same storage key: N=5,000, warmup 500. The loader is asserted to run exactly
once (during priming) and never again — if these were not cache hits, the script
fails rather than reporting a flattering number.

The hit path captures the generation and reads the cache inside one Lua command.
It pays one round trip, like the raw `GET` baseline. The baseline `JSON.parse`s the
reply, so the delta measures scripting and snapshot decoding rather than JSON.

Every request still reads the generation from Redis before joining a computation;
the generation is never cached locally. Concurrent misses share the loader and
write-back for their key and generation, but each performs its own snapshot read.

### 3. Invalidation sweep cost

10,000 keys populated in the namespace, then a single `invalidate()` timed
end-to-end: `INCR` on the counter followed by a `SCAN` sweep that `UNLINK`s
matching keys in batches. Reported as deleted keys, total milliseconds and
keys/second, over 3 rounds (repopulated each round) with the median round
highlighted. Setup writes are pipelined in `Promise.all` chunks of 500 for speed
and are not part of the measurement.

## Methodology

- **Warmup is excluded.** 500 unmeasured iterations run first; they never reach
  the statistics.
- **Per-operation timing** with `process.hrtime.bigint()`, one sample per
  operation, stored in a preallocated `Float64Array`.
- **Sort-based percentiles**, nearest-rank method, over the sorted sample array.
- **The two variants are interleaved** inside one loop (`A`, `B`, `A`, `B`, …)
  rather than run as two consecutive phases, so a GC pause, a scheduler hiccup or
  thermal drift hits both equally instead of penalizing whichever went second.
- **Sequential, one connection, one command in flight.** `ops/s` is therefore
  exactly `1 / mean latency` — a latency reciprocal, *not* a saturation throughput
  number. A real application with pipelining and concurrency will see far higher
  absolute throughput; the ratio is what carries over.
- **Assertions inside the loop.** Every fenced write must be accepted, every cache
  hit must return the cached value, the loader must never run, and the sweep must
  delete exactly 10,000 keys. A benchmark that stopped doing the work it claims to
  measure fails instead of reporting fast numbers.

## Honesty caveats

Read these before quoting any number.

1. **Absolute numbers are meaningless off this machine.** Latency is dominated by
   loopback round-trip time to a containerized Redis. The **ratio** between fenced
   and unfenced is the signal; the microseconds are not.
2. **Docker adds virtualization overhead** to the loopback path. A native or
   remote Redis changes every absolute number. Over a real network the fixed
   per-round-trip cost grows, making the fixed scripting overhead *less* noticeable.
3. **node-redis sends the full Lua script body on every `EVAL`** — it does not
   transparently use `EVALSHA`. The fenced write therefore carries a few hundred
   extra bytes per call. A client that caches the script SHA would shave part of
   benchmark 1's overhead.
4. **`p99` is noisy at N=5,000** on a loopback loop; it moves by tens of percent
   between runs and has been observed to go negative (fenced "faster" than plain)
   purely from tail scheduling luck. `p50` and `mean` are the stable figures.
5. **`invalidate()` SCANs the whole keyspace**, not just the namespace, so its
   cost scales with total database size rather than with the number of matching
   keys. The benchmark runs against a database flushed to contain only the
   namespace's 10,000 keys — a best case. A namespace of 10,000 keys inside a
   database of 10 million will sweep far more slowly.
6. **The first sweep round is consistently the slowest** (allocator and dict
   warmup), which is why 3 rounds are run and the median is reported.
7. **The payload is a 168-byte JSON object.** Larger values shift cost toward
   bandwidth and away from fixed per-command overhead, shrinking the *relative*
   fencing overhead in benchmark 1.
8. **Benchmark 1 measures Lua interpreter cost and the extra server-side `GET`
   together**; the script does not attempt to separate them.
9. **Default `redis:7-alpine` configuration**, no persistence or memory tuning.
10. **Single run per invocation, no statistical significance testing.** Run it
    twice and compare if a delta looks surprising.

## Sample results

Recorded for v0.1.2 on 2026-09-08, Apple M4 Pro, macOS, Node v26.5.0, Redis 7.4.10 in
Docker (OrbStack). Reproduce locally rather than trusting these.

| operation                                 | p50 µs | p95 µs | p99 µs | mean µs |  ops/s |
| ----------------------------------------- | -----: | -----: | -----: | ------: | -----: |
| setIfGeneration (fenced, Lua CAS)         |   93.6 |  123.9 |  184.0 |    98.1 | 10,194 |
| SET key value PX ttl (unfenced)           |   88.3 |  117.1 |  191.1 |    93.4 | 10,704 |
| getOrCompute hit (fenced, 1 round trip)  |   93.8 |  125.8 |  276.2 |   100.9 |  9,910 |
| GET + JSON.parse (unfenced, 1 round trip) |   86.0 |  109.9 |  222.0 |    91.2 | 10,970 |

Fenced write: **+5.9% p50** over a plain `SET`. Fenced cache hit: **+9.0% p50**
over a raw `GET`. Invalidating a 10,000-key namespace: **9.6 ms**, ~1.0M keys/s.

The unmodified v0.1.1 build measured **169.2 µs p50** for a cache hit in a separate
run on the same machine and Node version (raw `GET`: 84.9 µs). v0.1.2 measured
**93.8 µs**, about **45% lower p50 latency** after removing the second round trip.
These are single-run observations, subject to the caveats above.
