# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
