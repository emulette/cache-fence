#!/usr/bin/env node
/**
 * cache-fence benchmark suite.
 *
 * Measures what fencing costs against the unfenced Redis primitives it replaces,
 * on a throwaway redis:7-alpine container. Absolute numbers are dominated by
 * loopback RTT and vary by machine; the ratios are the signal.
 *
 * Usage: node bench/run.mjs   (requires Docker; builds dist/ if missing)
 */
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import os from 'node:os';
import { RedisContainer } from '@testcontainers/redis';
import { createClient } from 'redis';

const REDIS_IMAGE = 'redis:7-alpine';

/** Workload sizes are fixed so runs are comparable across machines. */
const WRITE_OPS = 5_000;
const WRITE_WARMUP = 500;
const READ_OPS = 5_000;
const READ_WARMUP = 500;
const SWEEP_KEYS = 10_000;
const SWEEP_ROUNDS = 3;
const SWEEP_POPULATE_CHUNK = 500;

/** Long enough that nothing expires mid-run. */
const TTL_MS = 600_000;

const NS_WRITE = 'bench-write';
const NS_READ = 'bench-read';
const NS_SWEEP = 'bench-sweep';
const HOT_KEY = 'hot';

/** A small, realistic cache entry. Every benchmark stores this same value. */
const PAYLOAD = {
  id: 4711,
  name: 'benchmark-subject',
  tags: ['alpha', 'beta', 'gamma'],
  nested: { region: 'eu-central-1', shard: 7, active: true },
  updatedAt: '2026-01-01T00:00:00.000Z',
};
const PAYLOAD_BYTES = Buffer.byteLength(JSON.stringify(PAYLOAD));

/**
 * Mirrors the library's internal key layout, so the raw-GET baseline reads
 * exactly the key getOrCompute reads.
 */
const freshStorageKey = (namespace, key) => `{${namespace}}:k:f:${key}`;

async function loadLibrary() {
  const dist = new URL('../dist/index.mjs', import.meta.url);
  if (!existsSync(dist)) {
    process.stdout.write('dist/ missing, running `npm run build`...\n');
    execFileSync('npm', ['run', 'build'], {
      cwd: new URL('..', import.meta.url),
      stdio: 'inherit',
    });
  }
  return import(dist.href);
}

// ---------------------------------------------------------------- statistics

/** Nearest-rank percentile over an ascending sample array. */
function percentile(sorted, p) {
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(Math.max(rank, 1), sorted.length) - 1];
}

function summarize(samplesNs) {
  const sorted = samplesNs.slice().sort();
  let totalNs = 0;
  for (const sample of samplesNs) {
    totalNs += sample;
  }
  const meanNs = totalNs / samplesNs.length;
  return {
    p50: percentile(sorted, 50) / 1000,
    p95: percentile(sorted, 95) / 1000,
    p99: percentile(sorted, 99) / 1000,
    mean: meanNs / 1000,
    // Sequential, one command in flight: throughput is exactly 1 / mean latency.
    opsPerSec: 1e9 / meanNs,
  };
}

/**
 * Times two variants against each other, interleaved inside one loop so drift and
 * background noise hit both equally, then reports each one's own statistics.
 *
 * `warmup` iterations run unmeasured first and never reach the statistics; every
 * measured operation is timed individually with hrtime.bigint.
 */
async function measurePair({ warmup, ops, a, b }) {
  for (let i = 0; i < warmup; i += 1) {
    await a(i);
    await b(i);
  }

  const samplesA = new Float64Array(ops);
  const samplesB = new Float64Array(ops);
  for (let i = 0; i < ops; i += 1) {
    const started = process.hrtime.bigint();
    await a(warmup + i);
    const between = process.hrtime.bigint();
    await b(warmup + i);
    const ended = process.hrtime.bigint();
    samplesA[i] = Number(between - started);
    samplesB[i] = Number(ended - between);
  }

  return { a: summarize(samplesA), b: summarize(samplesB) };
}

// ------------------------------------------------------------------ printing

const micros = (value) => value.toFixed(1);
const rate = (value) => Math.round(value).toLocaleString('en-US');
const pct = (value) => `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;

/** Renders a markdown table, first column left-aligned and the rest right-aligned. */
function table(headers, rows) {
  const widths = headers.map((header, column) =>
    Math.max(3, header.length, ...rows.map((row) => row[column].length)),
  );
  const render = (cells) =>
    `| ${cells
      .map((cell, column) =>
        column === 0 ? cell.padEnd(widths[column]) : cell.padStart(widths[column]),
      )
      .join(' | ')} |`;
  const divider = `| ${widths
    .map((width, column) =>
      column === 0 ? '-'.repeat(width) : `${'-'.repeat(width - 1)}:`,
    )
    .join(' | ')} |`;
  return [render(headers), divider, ...rows.map(render)].join('\n');
}

function section(title, body) {
  process.stdout.write(`\n### ${title}\n\n${body}\n`);
}

const LATENCY_HEADERS = ['operation', 'p50 µs', 'p95 µs', 'p99 µs', 'mean µs', 'ops/s'];
const latencyRow = (label, stats) => [
  label,
  micros(stats.p50),
  micros(stats.p95),
  micros(stats.p99),
  micros(stats.mean),
  rate(stats.opsPerSec),
];
/** Latency deltas are positive when fencing is slower; the ops/s delta is negative then. */
const deltaRow = (label, fenced, baseline) => [
  label,
  pct((fenced.p50 / baseline.p50 - 1) * 100),
  pct((fenced.p95 / baseline.p95 - 1) * 100),
  pct((fenced.p99 / baseline.p99 - 1) * 100),
  pct((fenced.mean / baseline.mean - 1) * 100),
  pct((fenced.opsPerSec / baseline.opsPerSec - 1) * 100),
];
const DELTA_LEGEND =
  '\nlatency delta: + = fencing is slower · ops/s delta: − = fencing does fewer ops/s';

// ---------------------------------------------------------------- benchmarks

/**
 * 1. Fenced write vs plain SET.
 *
 * setIfGeneration is one EVAL round trip doing a counter GET plus a conditional
 * SET; the baseline is a raw `SET key value PX ttl` of the same serialized value
 * on the same connection. The baseline serializes per call too, so the delta is
 * the fence, not JSON.
 */
async function benchFencedWrite({ createFencedCache, commands, client }) {
  await client.flushAll();
  const cache = createFencedCache({ redis: commands, namespace: NS_WRITE });
  const generation = await cache.generation();

  const { a: fenced, b: plain } = await measurePair({
    warmup: WRITE_WARMUP,
    ops: WRITE_OPS,
    a: async (i) => {
      const accepted = await cache.setIfGeneration(`w:${i}`, PAYLOAD, generation, {
        ttlMs: TTL_MS,
      });
      if (!accepted) {
        throw new Error(`fence rejected write ${i}: generation moved mid-benchmark`);
      }
    },
    b: async (i) => {
      const reply = await client.set(`bench:plain:${i}`, JSON.stringify(PAYLOAD), {
        expiration: { type: 'PX', value: TTL_MS },
      });
      if (reply !== 'OK') {
        throw new Error(`plain SET ${i} returned ${String(reply)}`);
      }
    },
  });

  section(
    `1. Fenced write vs plain SET (N=${WRITE_OPS.toLocaleString('en-US')}, warmup ${WRITE_WARMUP})`,
    table(LATENCY_HEADERS, [
      latencyRow('setIfGeneration (fenced, Lua CAS)', fenced),
      latencyRow('SET key value PX ttl (unfenced)', plain),
      deltaRow('fencing delta', fenced, plain),
    ]) + DELTA_LEGEND,
  );
}

/**
 * 2. getOrCompute hit path vs raw GET.
 *
 * The hit path is generation GET + data GET, so it pays two round trips where an
 * unfenced cache pays one. The baseline deserializes too, so the delta is the
 * extra round trip plus single-flight bookkeeping, not JSON.
 */
async function benchReadHit({ createFencedCache, commands, client }) {
  await client.flushAll();
  const cache = createFencedCache({ redis: commands, namespace: NS_READ });

  let loaderCalls = 0;
  const loader = async () => {
    loaderCalls += 1;
    return PAYLOAD;
  };

  await cache.getOrCompute(HOT_KEY, loader, { ttlMs: TTL_MS });
  if (loaderCalls !== 1) {
    throw new Error(`priming should call the loader once, called ${loaderCalls}`);
  }
  loaderCalls = 0;

  const storageKey = freshStorageKey(NS_READ, HOT_KEY);
  const { a: hit, b: raw } = await measurePair({
    warmup: READ_WARMUP,
    ops: READ_OPS,
    a: async () => {
      const value = await cache.getOrCompute(HOT_KEY, loader, { ttlMs: TTL_MS });
      if (value.id !== PAYLOAD.id) {
        throw new Error('getOrCompute returned an unexpected value');
      }
    },
    b: async () => {
      const stored = await client.get(storageKey);
      if (stored === null) {
        throw new Error('baseline GET missed; the primed key expired');
      }
      if (JSON.parse(stored).id !== PAYLOAD.id) {
        throw new Error('baseline GET returned an unexpected value');
      }
    },
  });
  if (loaderCalls !== 0) {
    throw new Error(`loader ran ${loaderCalls} times on the hit path; these are not cache hits`);
  }

  section(
    `2. getOrCompute cache hit vs raw GET (N=${READ_OPS.toLocaleString('en-US')}, warmup ${READ_WARMUP}, loader never ran)`,
    table(LATENCY_HEADERS, [
      latencyRow('getOrCompute hit (fenced, 2 round trips)', hit),
      latencyRow('GET + JSON.parse (unfenced, 1 round trip)', raw),
      deltaRow('fencing delta', hit, raw),
    ]) + DELTA_LEGEND,
  );
}

async function populate(cache, generation) {
  for (let start = 0; start < SWEEP_KEYS; start += SWEEP_POPULATE_CHUNK) {
    const end = Math.min(start + SWEEP_POPULATE_CHUNK, SWEEP_KEYS);
    const writes = [];
    for (let i = start; i < end; i += 1) {
      writes.push(cache.setIfGeneration(`s:${i}`, PAYLOAD, generation, { ttlMs: TTL_MS }));
    }
    const accepted = await Promise.all(writes);
    if (accepted.some((ok) => !ok)) {
      throw new Error('fence rejected a setup write while populating the namespace');
    }
  }
}

/**
 * 3. Invalidation sweep cost.
 *
 * One invalidate() over a namespace holding SWEEP_KEYS keys: INCR, then a SCAN
 * of the keyspace unlinking matches in batches. Setup writes are pipelined via
 * Promise.all chunks and are not part of the measurement.
 */
async function benchInvalidate({ createFencedCache, commands, client }) {
  await client.flushAll();
  const cache = createFencedCache({ redis: commands, namespace: NS_SWEEP });

  const rounds = [];
  for (let round = 1; round <= SWEEP_ROUNDS; round += 1) {
    const generation = await cache.generation();
    await populate(cache, generation);

    const started = process.hrtime.bigint();
    const result = await cache.invalidate();
    const ms = Number(process.hrtime.bigint() - started) / 1e6;

    if (result.deletedKeys !== SWEEP_KEYS) {
      throw new Error(`sweep deleted ${result.deletedKeys} keys, expected ${SWEEP_KEYS}`);
    }
    rounds.push({ round, ms, deletedKeys: result.deletedKeys });
  }

  const median = rounds.slice().sort((a, b) => a.ms - b.ms)[Math.floor(SWEEP_ROUNDS / 2)];
  const row = (label, entry) => [
    label,
    entry.deletedKeys.toLocaleString('en-US'),
    entry.ms.toFixed(1),
    rate(entry.deletedKeys / (entry.ms / 1000)),
  ];

  section(
    `3. Invalidation sweep (${SWEEP_KEYS.toLocaleString('en-US')} keys, ${SWEEP_ROUNDS} rounds, repopulated each round)`,
    table(
      ['round', 'deletedKeys', 'total ms', 'keys/s'],
      [
        ...rounds.map((entry) => row(`round ${entry.round}`, entry)),
        row('**median**', median),
      ],
    ),
  );
}

// ----------------------------------------------------------------- entrypoint

async function printEnvironment(client) {
  const info = await client.info('SERVER');
  const version = /redis_version:(\S+)/.exec(info)?.[1] ?? 'unknown';
  const cpu = os.cpus()[0]?.model ?? 'unknown CPU';
  process.stdout.write(
    [
      '# cache-fence benchmarks',
      '',
      `- node ${process.version} on ${process.platform}/${process.arch}, ${cpu}`,
      `- Redis ${version} (${REDIS_IMAGE}) in Docker via testcontainers, loopback TCP`,
      `- payload ${PAYLOAD_BYTES} bytes JSON, TTL ${TTL_MS / 1000}s, sequential on one connection`,
      '- latency here is dominated by loopback round-trip time: absolute numbers vary',
      '  by machine, the RATIO between fenced and unfenced is the signal',
      '',
    ].join('\n'),
  );
}

async function main() {
  const { createFencedCache } = await loadLibrary();

  const container = await new RedisContainer(REDIS_IMAGE).start();
  const client = createClient({ url: container.getConnectionUrl() });
  await client.connect();

  const commands = {
    get: (key) => client.get(key),
    incr: (key) => client.incr(key),
    eval: (script, options) => client.eval(script, options),
    scanIterator: (options) => client.scanIterator(options),
    unlink: (keys) => client.unlink(keys),
  };
  const context = { createFencedCache, commands, client };

  try {
    await printEnvironment(client);
    const started = process.hrtime.bigint();
    await benchFencedWrite(context);
    await benchReadHit(context);
    await benchInvalidate(context);
    const totalS = Number(process.hrtime.bigint() - started) / 1e9;
    process.stdout.write(`\nTotal benchmark time: ${totalS.toFixed(1)}s\n`);
  } finally {
    await client.close();
    await container.stop();
  }
}

await main();
