import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createFencedCache, type FencedCache } from '../src/index';
import { startClusterFixture, type ClusterFixture } from './cluster-fixture';
import { deferred, storageKeys } from './redis-fixture';

/**
 * Redis Cluster coverage. Boots a real three-master cluster in Docker, so it is gated
 * behind an environment variable and excluded from the default run:
 *
 *     CLUSTER_TESTS=1 npx vitest run test/cluster.test.ts
 *
 * The claim under test is that the library is cluster-safe *by construction*: every key
 * carries the `{namespace}` hash tag, so the counter and the data key the compare-and-set
 * touches always live in the same slot, and the multi-key EVAL never hits CROSSSLOT.
 * That claim is only worth anything if the test can also observe the failure, which is
 * what the negative control below is for.
 */

/** Mirrors the shape of the library's compare-and-set: two keys, one EVAL. */
const TWO_KEY_SCRIPT = `
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

const TTL_MS = 60_000;
const SWEEP_KEY_COUNT = 200;

describe.skipIf(process.env.CLUSTER_TESTS !== '1')('redis cluster', () => {
  let fx: ClusterFixture;

  beforeAll(async () => {
    fx = await startClusterFixture();
  });

  afterAll(async () => {
    await fx.stop();
  });

  beforeEach(async () => {
    await fx.flush();
  });

  describe('hash-tag compare-and-set', () => {
    let cache: FencedCache;

    beforeEach(() => {
      cache = createFencedCache({ redis: fx.commands, namespace: 'catalog' });
    });

    it('pins the counter and the data keys it is compared against to one slot', async () => {
      const counterSlot = await fx.keySlot(storageKeys.counter('catalog'));

      expect(await fx.keySlot(storageKeys.fresh('catalog', 'works'))).toBe(counterSlot);
      expect(await fx.keySlot(storageKeys.stale('catalog', 'works'))).toBe(counterSlot);
      // A different namespace is a different tag, so namespaces spread over the cluster
      // instead of piling onto one node.
      expect(await fx.keySlot(storageKeys.counter('orders'))).not.toBe(counterSlot);
    });

    it('runs the fenced write through the cluster without a CROSSSLOT error', async () => {
      expect(await cache.generation()).toBe(0);
      expect(await cache.setIfGeneration('works', 'v1', 0, { ttlMs: TTL_MS })).toBe(true);
      expect(await cache.get<string>('works')).toBe('v1');

      expect(await cache.bumpGeneration()).toBe(1);
      expect(await cache.generation()).toBe(1);
      expect(await cache.setIfGeneration('works', 'v2', 1, { ttlMs: TTL_MS })).toBe(true);
      expect(await cache.get<string>('works')).toBe('v2');
    });

    it('rejects a write carrying a generation an invalidation moved past', async () => {
      const generationAtStart = await cache.generation();
      const result = await cache.invalidate();

      expect(result.generation).toBe(generationAtStart + 1);
      expect(
        await cache.setIfGeneration('works', 'stale', generationAtStart, { ttlMs: TTL_MS }),
      ).toBe(false);
      expect(await cache.get('works')).toBeUndefined();

      // The namespace still works; only the write that crossed the invalidation was dropped.
      expect(
        await cache.setIfGeneration('works', 'fresh', result.generation, { ttlMs: TTL_MS }),
      ).toBe(true);
      expect(await cache.get<string>('works')).toBe('fresh');
    });
  });

  describe('negative control', () => {
    it('rejects the same two-key EVAL when the keys do not share a hash tag', async () => {
      // Cluster mode validates that the KEYS of a command hash to one slot, so this is a
      // real server-side rejection, not a client-side guess.
      const untagged = ['alpha', 'beta'];
      expect(await fx.keySlot(untagged[0])).not.toBe(await fx.keySlot(untagged[1]));

      await expect(
        fx.cluster.eval(TWO_KEY_SCRIPT, {
          keys: untagged,
          arguments: ['0', 'v1', String(TTL_MS)],
        }),
      ).rejects.toThrow(/CROSSSLOT/);

      // Same script, same node count, keys tagged the way the library tags them: accepted.
      const tagged = [storageKeys.counter('catalog'), storageKeys.fresh('catalog', 'works')];
      expect(await fx.keySlot(tagged[0])).toBe(await fx.keySlot(tagged[1]));
      expect(
        await fx.cluster.eval(TWO_KEY_SCRIPT, {
          keys: tagged,
          arguments: ['0', 'v1', String(TTL_MS)],
        }),
      ).toBe(1);
    });
  });

  describe('invalidation sweep', () => {
    // Chosen so their counters land on three different masters, which is asserted below.
    const namespaces = ['orders', 'catalog', 'users'];

    it('sweeps a namespace that lives on a master the scan does not start from', async () => {
      const addresses = await Promise.all(
        namespaces.map((namespace) => fx.masterAddressForKey(storageKeys.counter(namespace))),
      );
      expect(new Set(addresses).size).toBe(namespaces.length);

      // The sweep starts at the first master in the topology, so the namespace being swept
      // must live elsewhere: only then does finding its keys prove the walk continued.
      const sweptIndex = addresses.findIndex((address) => address !== fx.masterAddresses[0]);
      expect(sweptIndex).toBeGreaterThanOrEqual(0);
      const swept = namespaces[sweptIndex];
      const untouched = namespaces.filter((_unused, index) => index !== sweptIndex);

      const cache = createFencedCache({ redis: fx.commands, namespace: swept });
      await Promise.all(
        Array.from({ length: SWEEP_KEY_COUNT }, (_unused, index) =>
          cache.setIfGeneration(`item:${index}`, index, 0, { ttlMs: TTL_MS }),
        ),
      );
      const neighbours = untouched.map((namespace) =>
        createFencedCache({ redis: fx.commands, namespace }),
      );
      await Promise.all(
        neighbours.map((neighbour) =>
          neighbour.setIfGeneration('kept', 'v1', 0, { ttlMs: TTL_MS }),
        ),
      );

      const result = await cache.invalidate();

      expect(result.deletedKeys).toBe(SWEEP_KEY_COUNT);
      expect(await cache.get('item:0')).toBeUndefined();
      expect(await cache.get(`item:${SWEEP_KEY_COUNT - 1}`)).toBeUndefined();
      // The counter sits outside the swept prefix, so the fence survives its own invalidation.
      expect(result.generation).toBe(1);
      expect(await cache.generation()).toBe(1);
      // Other namespaces are scanned too, but MATCH keeps the sweep to its own keys.
      for (const neighbour of neighbours) {
        expect(await neighbour.get<string>('kept')).toBe('v1');
        expect(await neighbour.generation()).toBe(0);
      }
    });
  });

  describe('invalidation-crossing write', () => {
    it('drops the write-back of a computation an invalidation overtook', async () => {
      const cache = createFencedCache({ redis: fx.commands, namespace: 'catalog' });
      const started = deferred();
      const gate = deferred();
      let db = 'v1';

      // t0 - the computation captures the generation, then blocks on the gate.
      const snapshot = db;
      const inflight = cache.getOrCompute(
        'report',
        async () => {
          started.resolve();
          await gate.promise;
          return snapshot;
        },
        { ttlMs: TTL_MS },
      );
      await started.promise;

      // t1 - the mutation invalidates while the computation is still running.
      db = 'v2';
      await cache.invalidate();

      // t2 - the computation finishes and tries to write what it read at t0.
      gate.resolve();
      expect(await inflight).toBe('v1');

      // The caller got its value, but nothing was resurrected into the cache.
      expect(await cache.get('report')).toBeUndefined();

      // A computation that starts after the invalidation caches normally again.
      expect(await cache.getOrCompute('report', async () => db, { ttlMs: TTL_MS })).toBe('v2');
      expect(await cache.get<string>('report')).toBe('v2');
    });
  });
});
