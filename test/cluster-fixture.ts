import { createServer } from 'node:net';
import { createClient, createCluster } from 'redis';
import { GenericContainer, Wait } from 'testcontainers';
import type { RedisCommands } from '../src/index';
import { poll } from './redis-fixture';

/**
 * A real Redis Cluster the host can actually talk to, plus the cluster adapter for
 * the library's five-operation {@link RedisCommands} surface.
 *
 * ## Why one container runs every node
 *
 * A cluster client follows MOVED redirects to the address the cluster *announces*, so
 * that address has to be reachable from wherever the client runs. The usual
 * one-container-per-node setup announces container-internal IPs, which a macOS or
 * Windows host cannot route to, and every redirect dies.
 *
 * Running all masters inside a single container fixes both directions at once:
 *
 * - each node announces `127.0.0.1:<port>`, published to the host under the *same*
 *   port number, so a redirect the host follows lands on the right node;
 * - the nodes share one network namespace, so `127.0.0.1:<bus port>` is also the
 *   correct address for the gossip between them.
 *
 * Announced port and container port must therefore be identical, which is why the
 * ports are bound explicitly instead of being mapped to random host ports.
 */

const REDIS_IMAGE = 'redis:7-alpine';
const MASTER_COUNT = 3;
const PORT_SEARCH_START = 7301;
const PORT_SEARCH_END = 7399;
const CLUSTER_BUS_PORT_OFFSET = 10_000;
const CLUSTER_NODE_TIMEOUT_MS = 5_000;
const STARTUP_TIMEOUT_MS = 180_000;
const CLUSTER_READY_TIMEOUT_MS = 60_000;
/** Announced to clients and used for gossip; see the note above. */
const ANNOUNCE_HOST = '127.0.0.1';
const TOTAL_SLOTS = 16_384;

// The return type is left inferred on purpose: node-redis resolves the cluster type
// from the options it was called with, and spelling it out here would not match.
function createRawCluster(rootNodes: Array<{ url: string }>) {
  return createCluster({ rootNodes });
}

/** The connected node-redis cluster client, used for raw inspection. */
export type RawRedisCluster = ReturnType<typeof createRawCluster>;

export interface ClusterFixture {
  /** The five-operation adapter the library is constructed with. */
  commands: RedisCommands;
  cluster: RawRedisCluster;
  /** `host:port` of every master, in the client's topology order. */
  masterAddresses: string[];
  /** The slot a key hashes to, answered by the server (`CLUSTER KEYSLOT`). */
  keySlot(key: string): Promise<number>;
  /** `host:port` of the master owning the key's slot. */
  masterAddressForKey(key: string): Promise<string>;
  flush(): Promise<void>;
  stop(): Promise<void>;
}

function canBind(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const server = createServer();
    server.once('error', () => {
      resolve(false);
    });
    // Docker publishes on all interfaces, so a loopback-only probe would miss conflicts.
    server.listen(port, '0.0.0.0', () => {
      server.close(() => {
        resolve(true);
      });
    });
  });
}

async function findFreePorts(count: number): Promise<number[]> {
  const ports: number[] = [];
  for (let port = PORT_SEARCH_START; port <= PORT_SEARCH_END && ports.length < count; port += 1) {
    if (await canBind(port)) {
      ports.push(port);
    }
  }
  if (ports.length < count) {
    throw new Error(
      `needed ${count} free ports in ${PORT_SEARCH_START}-${PORT_SEARCH_END}, found ${ports.length}`,
    );
  }
  return ports;
}

/** Starts one cluster-enabled `redis-server` per port, then joins them into a cluster. */
function clusterBootScript(ports: number[]): string {
  const servers = ports.map((port) =>
    [
      'redis-server',
      `--port ${port}`,
      '--cluster-enabled yes',
      `--cluster-config-file /tmp/nodes-${port}.conf`,
      `--cluster-node-timeout ${CLUSTER_NODE_TIMEOUT_MS}`,
      `--cluster-announce-ip ${ANNOUNCE_HOST}`,
      `--cluster-announce-port ${port}`,
      `--cluster-announce-bus-port ${port + CLUSTER_BUS_PORT_OFFSET}`,
      '--protected-mode no',
      '--appendonly no',
      "--save ''",
      '--dir /tmp',
      '--daemonize yes',
      `--logfile /tmp/redis-${port}.log`,
    ].join(' '),
  );
  const readiness = ports.map(
    (port) => `until redis-cli -p ${port} ping > /dev/null 2>&1; do sleep 0.1; done`,
  );
  const addresses = ports.map((port) => `${ANNOUNCE_HOST}:${port}`).join(' ');

  return [
    'set -e',
    ...servers,
    ...readiness,
    `redis-cli --cluster create ${addresses} --cluster-yes`,
    // Keeps PID 1 alive; the servers themselves are daemonized.
    'exec tail -f /dev/null',
  ].join('\n');
}

/**
 * Waits until every node serves keys.
 *
 * `redis-cli --cluster create` returns once the nodes agree on the slot assignment, but a
 * node answers CLUSTERDOWN until its own view of the cluster is complete. The wait runs on
 * throwaway single-node clients because a cluster client caches the topology it discovers,
 * so it must not be built while the cluster is still settling.
 */
async function waitForClusterReady(ports: number[]): Promise<void> {
  for (const port of ports) {
    const client = createClient({ url: `redis://${ANNOUNCE_HOST}:${port}` });
    client.on('error', () => {});
    await client.connect();
    try {
      await poll(async () => (await client.clusterInfo()).includes('cluster_state:ok'), {
        timeoutMs: CLUSTER_READY_TIMEOUT_MS,
        intervalMs: 100,
        description: `node ${port} to reach cluster_state:ok`,
      });
    } finally {
      await client.close();
    }
  }
}

/**
 * SCAN is per-node state, so a cluster-wide sweep walks every master in turn.
 *
 * This is the part of the adapter that a single-node client hides: `scanIterator` on a
 * cluster client would only cover one node, and an invalidation that misses a master
 * silently leaves stale keys behind. Masters are taken from the client's discovered
 * topology, and each node's own iterator handles its cursor.
 */
async function* scanAllMasters(
  cluster: RawRedisCluster,
  options: { MATCH: string; COUNT: number },
): AsyncGenerator<string[]> {
  for (const master of cluster.masters) {
    const client = await cluster.nodeClient(master);
    for await (const keys of client.scanIterator(options)) {
      yield keys;
    }
  }
}

/** Boots a throwaway three-master cluster and wires a client to it. One per test file. */
export async function startClusterFixture(): Promise<ClusterFixture> {
  const ports = await findFreePorts(MASTER_COUNT);
  const container = await new GenericContainer(REDIS_IMAGE)
    .withExposedPorts(...ports.map((port) => ({ container: port, host: port })))
    .withCommand(['sh', '-c', clusterBootScript(ports)])
    .withWaitStrategy(Wait.forLogMessage(new RegExp(`All ${TOTAL_SLOTS} slots covered`)))
    .withStartupTimeout(STARTUP_TIMEOUT_MS)
    .start();

  await waitForClusterReady(ports);

  const cluster = createRawCluster(
    ports.map((port) => ({ url: `redis://${ANNOUNCE_HOST}:${port}` })),
  );
  // A cluster client emits transport errors per node (a node connection torn down during
  // teardown, for instance). Without a listener Node turns those into an uncaught exception.
  cluster.on('error', () => {});
  await cluster.connect();

  if (cluster.masters.length !== MASTER_COUNT) {
    await cluster.close();
    await container.stop();
    throw new Error(
      `expected ${MASTER_COUNT} masters, the client discovered ${cluster.masters.length}`,
    );
  }

  /**
   * The adapter. Only `scanIterator` needs cluster-specific work:
   *
   * - `eval` is routed by its first key, and the library's two keys share the
   *   `{namespace}` hash tag, so both live on the node it is routed to;
   * - `unlink` receives keys from one namespace, so they share a slot and the
   *   multi-key command stays inside one node;
   * - `get` and `incr` are single-key and route themselves.
   */
  const commands: RedisCommands = {
    get: (key) => cluster.get(key),
    incr: (key) => cluster.incr(key),
    eval: (script, options) => cluster.eval(script, options),
    scanIterator: (options) => scanAllMasters(cluster, options),
    unlink: (keys) => cluster.unlink(keys),
  };

  const keySlot = async (key: string): Promise<number> => {
    const client = await cluster.nodeClient(cluster.masters[0]);
    return client.clusterKeySlot(key);
  };

  return {
    commands,
    cluster,
    masterAddresses: cluster.masters.map((master) => `${master.host}:${master.port}`),
    keySlot,
    masterAddressForKey: async (key) => {
      const shard = cluster.slots[await keySlot(key)];
      return `${shard.master.host}:${shard.master.port}`;
    },
    flush: async () => {
      for (const master of cluster.masters) {
        const client = await cluster.nodeClient(master);
        await client.flushAll();
      }
    },
    stop: async () => {
      await cluster.close();
      await container.stop();
    },
  };
}
