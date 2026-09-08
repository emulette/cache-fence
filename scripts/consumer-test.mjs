#!/usr/bin/env node
/**
 * End-to-end smoke test for the published package.
 *
 * Packs the repo with `npm pack`, installs the resulting tarball into a throwaway
 * consumer project, then runs the library from both an ESM and a CJS entry point
 * against a real throwaway Redis server. This is the only check that
 * exercises the package as a real consumer would install it, rather than importing
 * source files directly, so it catches export-map and build-output mistakes that
 * `vitest` never sees.
 *
 * Uses the repository's Redis client and testcontainers dev dependencies; only the
 * packed library is installed into the consumer project.
 */
import { execFileSync } from 'node:child_process';
import { RedisContainer } from '@testcontainers/redis';
import { createRequire } from 'node:module';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = fileURLToPath(new URL('..', import.meta.url));

function log(message) {
  process.stdout.write(`[consumer-test] ${message}\n`);
}

const redisEntry = createRequire(import.meta.url).resolve('redis');

/** Shared assertions and checks, run against both the ESM and CJS builds. */
const ASSERTIONS_SOURCE = [
  'function assertEqual(actual, expected, message) {',
  '  if (actual !== expected) {',
  '    throw new Error(',
  "      message + ' (expected ' + JSON.stringify(expected) + ', got ' + JSON.stringify(actual) + ')',",
  '    );',
  '  }',
  '}',
  '',
  'async function runConsumerChecks(createFencedCache, label) {',
  '  const redis = createClient({ url: process.argv[2] });',
  '  await redis.connect();',
  '  try {',
  "  const cache = createFencedCache({ redis, namespace: 'consumer-test-' + label });",
  '  let computeCount = 0;',
  '  const loader = async () => {',
  '    computeCount += 1;',
  "    return { value: 'computed', hits: computeCount };",
  '  };',
  '',
  '  // Miss -> compute -> cached hit.',
  "  const first = await cache.getOrCompute('widget', loader, { ttlMs: 60000 });",
  '  assertEqual(',
  '    computeCount,',
  '    1,',
  "    '[' + label + '] loader must run exactly once on a cache miss',",
  '  );',
  '  assertEqual(',
  '    first.value,',
  "    'computed',",
  "    '[' + label + '] getOrCompute must return the loader result',",
  '  );',
  '',
  "  const second = await cache.getOrCompute('widget', loader, { ttlMs: 60000 });",
  '  assertEqual(',
  '    computeCount,',
  '    1,',
  "    '[' + label + '] a cached hit must not re-run the loader',",
  '  );',
  '  assertEqual(',
  '    second.value,',
  "    'computed',",
  "    '[' + label + '] a cached hit must return the cached value',",
  '  );',
  '',
  '  // invalidate() must bump the generation.',
  '  const generationBefore = await cache.generation();',
  '  const invalidation = await cache.invalidate();',
  '  assertEqual(',
  '    invalidation.generation,',
  '    generationBefore + 1,',
  "    '[' + label + '] invalidate() must bump the generation by exactly 1',",
  '  );',
  '  const generationAfter = await cache.generation();',
  '  assertEqual(',
  '    generationAfter,',
  '    generationBefore + 1,',
  "    '[' + label + '] generation() must reflect the bump made by invalidate()',",
  '  );',
  '',
  '  // A write carrying a stale (pre-invalidation) generation must be rejected.',
  '  const staleWriteAccepted = await cache.setIfGeneration(',
  "    'widget',",
  "    { value: 'stale' },",
  '    generationBefore,',
  '    { ttlMs: 60000 },',
  '  );',
  '  assertEqual(',
  '    staleWriteAccepted,',
  '    false,',
  "    '[' + label + '] a write carrying a stale generation must be rejected',",
  '  );',
  '',
  '  // Sanity check: a write at the current generation is still accepted.',
  '  const freshWriteAccepted = await cache.setIfGeneration(',
  "    'widget',",
  "    { value: 'fresh' },",
  '    generationAfter,',
  '    { ttlMs: 60000 },',
  '  );',
  '  assertEqual(',
  '    freshWriteAccepted,',
  '    true,',
  "    '[' + label + '] a write at the current generation must be accepted',",
  '  );',
  '',
  "  process.stdout.write('[' + label + '] ok\\n');",
  '  } finally {',
  '    await redis.close();',
  '  }',
  '}',
].join('\n');

const ESM_TEST_SOURCE = `import { createClient } from ${JSON.stringify(redisEntry)};

${ASSERTIONS_SOURCE}

import { createFencedCache } from 'cache-fence';

await runConsumerChecks(createFencedCache, 'esm');
`;

const CJS_TEST_SOURCE = `const { createClient } = require(${JSON.stringify(redisEntry)});

${ASSERTIONS_SOURCE}

const { createFencedCache } = require('cache-fence');

runConsumerChecks(createFencedCache, 'cjs').catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`;

function run(command, args, options) {
  return execFileSync(command, args, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    ...options,
  });
}

function packTarball(packDir) {
  log('packing the library with `npm pack`...');
  const output = run('npm', ['pack', '--pack-destination', packDir, '--json'], {
    cwd: repoRoot,
  });
  const [entry] = JSON.parse(output);
  if (entry === undefined || typeof entry.filename !== 'string') {
    throw new Error(`npm pack did not report a tarball filename, got: ${output}`);
  }
  const tarballPath = join(packDir, entry.filename);
  log(`packed ${entry.filename}`);
  return tarballPath;
}

function installTarball(consumerDir, tarballPath) {
  writeFileSync(
    join(consumerDir, 'package.json'),
    JSON.stringify(
      {
        name: 'cache-fence-consumer-test',
        version: '0.0.0',
        private: true,
      },
      null,
      2,
    ),
  );
  log('installing the tarball into a throwaway consumer project...');
  run('npm', ['install', '--no-audit', '--no-fund', tarballPath], { cwd: consumerDir });
}

function runCheck(consumerDir, fileName, source, redisUrl) {
  writeFileSync(join(consumerDir, fileName), source);
  log(`running ${fileName}...`);
  const output = run('node', [fileName, redisUrl], { cwd: consumerDir });
  process.stdout.write(output);
}

async function main() {
  const container = await new RedisContainer('redis:7-alpine').start();
  const redisUrl = container.getConnectionUrl();
  const workDir = mkdtempSync(join(tmpdir(), 'cache-fence-consumer-'));
  const packDir = join(workDir, 'pack');
  const consumerDir = join(workDir, 'consumer');
  mkdirSync(packDir, { recursive: true });
  mkdirSync(consumerDir, { recursive: true });

  try {
    const tarballPath = packTarball(packDir);
    installTarball(consumerDir, tarballPath);
    runCheck(consumerDir, 'esm-test.mjs', ESM_TEST_SOURCE, redisUrl);
    runCheck(consumerDir, 'cjs-test.cjs', CJS_TEST_SOURCE, redisUrl);
    log('all consumer checks passed.');
  } catch (error) {
    process.stderr.write('[consumer-test] FAILED: the package does not work as installed.\n');
    if (error && typeof error === 'object') {
      if ('stdout' in error && error.stdout) process.stderr.write(String(error.stdout));
      if ('stderr' in error && error.stderr) process.stderr.write(String(error.stderr));
    }
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
    process.exitCode = 1;
  } finally {
    rmSync(workDir, { recursive: true, force: true });
    await container.stop();
  }
}

await main();
