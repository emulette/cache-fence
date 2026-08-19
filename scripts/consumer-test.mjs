#!/usr/bin/env node
/**
 * End-to-end smoke test for the published package.
 *
 * Packs the repo with `npm pack`, installs the resulting tarball into a throwaway
 * consumer project, then runs the library from both an ESM and a CJS entry point
 * against a small in-memory fake of the Redis adapter. This is the only check that
 * exercises the package as a real consumer would install it, rather than importing
 * source files directly, so it catches export-map and build-output mistakes that
 * `vitest` never sees.
 *
 * Plain Node, no dependencies: this must run before the library can be trusted to
 * actually work once published.
 */
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = fileURLToPath(new URL('..', import.meta.url));

function log(message) {
  process.stdout.write(`[consumer-test] ${message}\n`);
}

/**
 * The five-operation `RedisCommands` fake shared by the generated ESM and CJS
 * consumer scripts. Inlined as source text into both files so neither one needs a
 * second local module to import or require.
 *
 * Uses string concatenation rather than template literals so this source can be
 * embedded inside this script's own template literals without escaping.
 */
const FAKE_REDIS_SOURCE = [
  'function createFakeRedis() {',
  '  const store = new Map();',
  '  return {',
  '    async get(key) {',
  '      return store.has(key) ? store.get(key) : null;',
  '    },',
  '    async incr(key) {',
  '      const next = (store.has(key) ? Number(store.get(key)) : 0) + 1;',
  '      store.set(key, String(next));',
  '      return next;',
  '    },',
  "    // Mirrors the library's Lua CAS script: compare the counter key (KEYS[1])",
  '    // to the captured generation (ARGV[1]), and only then set the data key',
  '    // (KEYS[2]) to the new value (ARGV[2]).',
  '    async eval(_script, options) {',
  '      const counterKey = options.keys[0];',
  '      const dataKey = options.keys[1];',
  '      const generation = options.arguments[0];',
  '      const value = options.arguments[1];',
  "      const current = store.has(counterKey) ? store.get(counterKey) : '0';",
  '      if (current !== generation) {',
  '        return 0;',
  '      }',
  '      store.set(dataKey, value);',
  '      return 1;',
  '    },',
  '    async *scanIterator(options) {',
  "      const prefix = options.MATCH.endsWith('*')",
  '        ? options.MATCH.slice(0, -1)',
  '        : options.MATCH;',
  '      for (const key of store.keys()) {',
  '        if (key.startsWith(prefix)) {',
  '          yield key;',
  '        }',
  '      }',
  '    },',
  '    async unlink(keys) {',
  '      let deleted = 0;',
  '      for (const key of keys) {',
  '        if (store.delete(key)) {',
  '          deleted += 1;',
  '        }',
  '      }',
  '      return deleted;',
  '    },',
  '  };',
  '}',
].join('\n');

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
  '  const redis = createFakeRedis();',
  "  const cache = createFencedCache({ redis, namespace: 'consumer-test' });",
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
  "  console.log('[' + label + '] ok');",
  '}',
].join('\n');

const ESM_TEST_SOURCE = `${FAKE_REDIS_SOURCE}

${ASSERTIONS_SOURCE}

import { createFencedCache } from 'cache-fence';

await runConsumerChecks(createFencedCache, 'esm');
`;

const CJS_TEST_SOURCE = `${FAKE_REDIS_SOURCE}

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

function runCheck(consumerDir, fileName, source) {
  writeFileSync(join(consumerDir, fileName), source);
  log(`running ${fileName}...`);
  const output = run('node', [fileName], { cwd: consumerDir });
  process.stdout.write(output);
}

function main() {
  const workDir = mkdtempSync(join(tmpdir(), 'cache-fence-consumer-'));
  const packDir = join(workDir, 'pack');
  const consumerDir = join(workDir, 'consumer');
  mkdirSync(packDir, { recursive: true });
  mkdirSync(consumerDir, { recursive: true });

  try {
    const tarballPath = packTarball(packDir);
    installTarball(consumerDir, tarballPath);
    runCheck(consumerDir, 'esm-test.mjs', ESM_TEST_SOURCE);
    runCheck(consumerDir, 'cjs-test.cjs', CJS_TEST_SOURCE);
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
  }
}

main();
