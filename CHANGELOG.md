# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-10-01

### Added

- CI runs the standalone and Cluster suites against Redis 7 and 8 and Valkey 8 and 9,
  each through both node-redis and ioredis. `REDIS_IMAGE` and `REDIS_CLIENT` select the
  combination locally; `bench/run.mjs` also honors `REDIS_IMAGE`.
- The README ioredis adapter is now tested verbatim (ioredis 6.x), and the Cluster
  fixture includes an ioredis adapter that scans every master.

### Changed

- Update development dependencies; `npm audit` reports no vulnerabilities. The
  runtime code is unchanged from 0.2.0.

## [0.2.0] - 2026-09-08

### Breaking changes

- Replace numeric counters with opaque `GenerationToken` UUIDs. `generation()`
  initializes missing metadata atomically; `bumpGeneration()` rotates the token.
  Token loss no longer permits writes captured before invalidation or data loss.
- Use versioned `{namespace}:v2:gen` and `{namespace}:v2:k:*` keys. Values carry a
  generation stamp; every read verifies it atomically before returning the payload.
  Upgrade all readers and invalidators together; 0.1.x and 0.2.x do not invalidate
  each other's storage. See the README migration procedure.
- Reduce the Redis adapter to `eval` and `scanIterator`. Lua now also uses `GETRANGE`
  for cleanup. Adapters and ACLs must support the documented command set.

### Fixed

- Generation rotation immediately makes old fresh and SWR entries unreadable,
  including when a cleanup fails or has not yet reached the entry.
- Cleanup atomically checks entry stamps and preserves current-generation writes
  that replace keys between SCAN and deletion.
- Generation loss and full Redis resets start distinct computation and refresh
  flights, preventing new requests from joining pre-loss work.

### Added

- `getOrComputeResult<T>()` and `FencedCacheResult<T>` expose the value, source,
  captured generation and write outcome per call. `getOrCompute()` retains its
  value-only API and shares the same computation flights.
- Behavioral regression coverage for metadata loss, memory eviction, incomplete
  invalidation, concurrent cleanup and result/error semantics.

## [0.1.2] - 2026-09-08

### Fixed

- Escape namespace characters in invalidation scan patterns so `?`, brackets and
  backslashes cannot delete another namespace's keys or leave their own keys behind.
- Scope foreground computations and SWR refreshes by generation. Requests after
  invalidation no longer join pre-invalidation work, including invalidation by another instance.

### Added

- Optional `onEvent` callback and exported `FencedCacheEvent` union for hits, misses,
  fence rejections and completed refreshes. Observer exceptions do not affect cache operations.

### Changed

- Read the generation and fresh/stale entry atomically in one Lua command, reducing
  cache-hit reads to one Redis round trip while preserving cache failure handling.
- Malformed snapshot responses bypass caching and report through `onError`.
- Consumer packaging checks now exercise both ESM and CJS against a real Redis server.
- Update benchmarks and API documentation for the new read path and event semantics.

## [0.1.1] - 2026-08-19

### Added

- Benchmark suite (`bench/`): fenced write vs plain `SET`, `getOrCompute` hit path
  vs raw `GET`, and invalidation sweep cost, with methodology and reproduction
  instructions. Headline numbers are published in the README.
- Redis Cluster verification: the hash-tag compare-and-set, a `CROSSSLOT` negative
  control, a cross-master invalidation sweep and the invalidation-crossing-write
  scenario now run against a live three-master cluster
  (`CLUSTER_TESTS=1 npm run test:cluster`, also in CI), including a reference
  cluster adapter demonstrating the multi-master `scanIterator` pattern.
- Packaged-consumer test (`npm run test:consumer`): installs the packed tarball
  into a clean project and exercises the ESM and CJS entry points end to end.
- Lint and formatting via Biome (`npm run lint`), enforced in CI.
- Scheduled maintenance workflow: dependency audit, a peer-range floor check
  against `redis@5.0.0` (verified passing), and the latest in-range dependency
  combination.

### Changed

- README: added measured performance numbers; upgraded the Redis Cluster claim
  from "designed for" to verified, with the remaining gaps (failover, resharding,
  replica reads) stated explicitly; documented that `getOrCompute` has no
  cancellation or timeout contract; removed the roadmap section in favor of
  current behavior and limitations only.
- CI: GitHub Actions pinned to full commit SHAs, workflow permissions reduced to
  the minimum, consumer and cluster tests added to the pipeline.

No functional changes to the library runtime.

## [0.1.0] - 2026-08-19

### Added

- Initial release: a generation-fenced Redis cache that atomically rejects stale
  writes that cross an invalidation.
- `getOrCompute`: single-flight, fail-closed cache-aside reads with optional
  stale-while-revalidate.
- Fenced writes via a Lua compare-and-set script, `setIfGeneration`.
- Bump-then-sweep `invalidate`, which advances the namespace generation before
  removing its keys so no write can resurrect stale data.
- A 5-operation `RedisCommands` adapter interface (`get`, `incr`, `eval`,
  `scanIterator`, `unlink`), so any client that implements it can be used.
- Zero runtime dependencies.
- Support for Node.js >= 22.
- Dual MIT OR Apache-2.0 licensing.

### Notes

- Published to npm without a corresponding git tag. Git tags begin at `v0.1.1`.
