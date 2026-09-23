import assert from 'node:assert'
import { Redis, type Cluster } from 'ioredis'
import {
  createClient,
  RESP_TYPES,
  type RedisClientType,
  type RedisClusterType,
} from 'redis'
import clusterKeySlot from 'cluster-key-slot'
export {
  assertBufferSetsEqual,
  assertBuffersEqual,
  commandFrame,
  errorWithMessage,
} from '../tests/shared-test-helpers'

export type RedisEndpoint = {
  host: string
  port: number
}

type ClusterSlotsNode = [string, number, ...unknown[]]
type ClusterSlotsRange = [
  min: number,
  max: number,
  master: ClusterSlotsNode,
  ...replicas: ClusterSlotsNode[],
]

export function randomKey(): string {
  return Math.random().toString(36).substring(2, 10)
}

/**
 * Wait until `read()` reports the value is gone (`null`/`undefined`), polling
 * instead of sleeping for a fixed interval.
 *
 * A fixed sleep has to out-wait the TTL *plus* whatever the machine does to the
 * event loop under load, and picking that margin is what made the hash
 * field-TTL tests flake in both directions (#411): too small a TTL and the
 * field dies before the assertions that need it alive; too small a gap between
 * TTL and sleep and it is still alive when the assertions need it gone.
 * Polling removes the guess — it waits exactly as long as the expiry takes.
 *
 * It is not a weaker assertion than `sleep(); assert.strictEqual(x, null)`: the
 * value must still be gone, just within a generous deadline instead of at one
 * arbitrary instant. A value that never expires still fails, and the message
 * reports how long it survived, so a future regression says so directly
 * instead of looking like one more flake.
 */
export async function waitUntilGone(
  read: () => Promise<unknown>,
  description: string,
  { timeoutMs = 5000, intervalMs = 20 } = {},
): Promise<void> {
  const start = Date.now()

  for (;;) {
    const value = await read()
    if (value === null || value === undefined) {
      return
    }

    const elapsed = Date.now() - start
    if (elapsed >= timeoutMs) {
      assert.fail(
        `${description} still present after ${elapsed}ms (last value: ${String(value)})`,
      )
    }

    await new Promise(resolve => setTimeout(resolve, intervalMs))
  }
}

/**
 * Build a key guaranteed to hash to a different slot than `key`.
 *
 * CROSSSLOT probes need two keys in genuinely different slots. Drawing both
 * hash tags from `randomKey()` leaves a ~1/16384 chance of landing in the same
 * slot, which turns the probe into a rare, unreproducible failure — exactly the
 * run-to-run nondeterminism the rest of this sweep removes. Resample until the
 * slots actually differ.
 */
export function keyInAnotherSlot(key: string, candidate: () => string): string {
  const slot = clusterKeySlot(key)

  for (let attempt = 0; attempt < 1000; attempt++) {
    const next = candidate()
    if (clusterKeySlot(next) !== slot) {
      return next
    }
  }

  throw new Error(`No key in a slot other than ${slot} after 1000 attempts`)
}

/** How many of `keys` exist right now. */
export async function countExistingKeys(
  redisClient: Cluster,
  keys: readonly string[],
): Promise<number> {
  const flags = await Promise.all(keys.map(key => redisClient.exists(key)))
  return flags.filter(Boolean).length
}

/**
 * Assert that exactly `expected` of `keys` exist, and that the suite's own
 * namespace holds nothing beyond them.
 *
 * This replaces an older `DBSIZE - baseline` delta assertion. Top-level DBSIZE
 * counts the whole shared keyspace, which on the real backend also holds every
 * other suite's keys — including ones with TTLs that expire mid-test — so the
 * delta drifted for reasons unrelated to the commands under test (#420).
 *
 * `pattern` restores the one thing the delta caught that key-existence alone
 * does not: a command that creates an EXTRA, unexpected key. Because every key
 * here shares one hash tag, `KEYS <pattern>` on that tag's slot owner is both
 * exact and unaffected by anything else on the node. DBSIZE is still asserted
 * exactly (== 0 after a flush) by the flush-async-sync suites.
 */
export async function assertKeyCount(
  redisClient: Cluster,
  pattern: string,
  keys: readonly string[],
  expected: number,
): Promise<void> {
  assert.strictEqual(await countExistingKeys(redisClient, keys), expected)

  const owner = await connectToSlotOwner(redisClient, keys[0])
  try {
    assert.strictEqual(
      (await owner.keys(pattern)).length,
      expected,
      `expected exactly ${expected} key(s) matching ${pattern}`,
    )
  } finally {
    owner.disconnect()
  }
}

export async function connectToSlotOwner(
  cluster: Cluster,
  key: string | Buffer,
): Promise<Redis> {
  const [host, port] = await findSlotOwner(cluster, key)
  return connectToEndpoint({ host, port })
}

export async function connectToEndpoint(
  endpoint: RedisEndpoint,
): Promise<Redis> {
  const client = new Redis({
    host: endpoint.host,
    port: endpoint.port,
    lazyConnect: true,
  })
  await client.connect()
  return client
}

export async function findSlotOwner(
  cluster: Cluster,
  key: string | Buffer,
): Promise<[host: string, port: number]> {
  const slot = clusterKeySlot(key)
  const slots = (await cluster.cluster('SLOTS')) as Array<
    [number, number, [string, number]]
  >

  for (const [min, max, master] of slots) {
    if (slot >= min && slot <= max) {
      return [master[0], master[1]]
    }
  }

  throw new Error(`No Redis Cluster slot owner found for slot ${slot}`)
}

export async function findSlotMasterAndReplica(
  cluster: Cluster,
  key: string | Buffer,
  options: { retries?: number; retryDelayMs?: number } = {},
): Promise<{
  slot: number
  master: RedisEndpoint
  replica: RedisEndpoint
}> {
  const retries = options.retries ?? 20
  const retryDelayMs = options.retryDelayMs ?? 500
  const slot = clusterKeySlot(key)

  // Newly formed clusters can briefly report a master with no replica before
  // replication finishes attaching, so poll CLUSTER SLOTS until the replica
  // shows up instead of failing on a transient half-formed topology.
  for (let attempt = 0; attempt <= retries; attempt++) {
    const slots = (await cluster.cluster('SLOTS')) as ClusterSlotsRange[]

    for (const [min, max, master, replica] of slots) {
      if (slot < min || slot > max) {
        continue
      }

      if (!replica) {
        break
      }

      return {
        slot,
        master: endpointFromClusterSlotsNode(master),
        replica: endpointFromClusterSlotsNode(replica),
      }
    }

    if (attempt < retries) {
      await new Promise(resolve => setTimeout(resolve, retryDelayMs))
    }
  }

  throw new Error(`No Redis Cluster replica found for slot ${slot}`)
}

export async function eventually<TValue>(
  callback: () => Promise<TValue>,
  options: { retries?: number; retryDelayMs?: number } = {},
): Promise<TValue> {
  const retries = options.retries ?? 20
  const retryDelayMs = options.retryDelayMs ?? 100
  let lastError: unknown

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await callback()
    } catch (err) {
      lastError = err
    }

    if (attempt < retries) {
      await new Promise(resolve => setTimeout(resolve, retryDelayMs))
    }
  }

  throw lastError
}

function endpointFromClusterSlotsNode(node: ClusterSlotsNode): RedisEndpoint {
  return {
    host: String(node[0]),
    port: node[1],
  }
}

// --- node-redis parallels -------------------------------------------------
//
// node-redis-typed counterparts of the ioredis helpers above. Kept separate
// (rather than generic) so each suite stays strongly typed against its client.

/** Open a fresh, standalone node-redis client to a specific endpoint. */
export async function connectToNodeRedisEndpoint(
  endpoint: RedisEndpoint,
): Promise<RedisClientType> {
  const client = createClient({
    url: `redis://${endpoint.host}:${endpoint.port}`,
  }) as RedisClientType
  client.on('error', () => {})
  await client.connect()
  return client
}

/**
 * node-redis equivalent of {@link connectToSlotOwner}: open a fresh, isolated
 * direct client to the master that owns `key`'s slot. A fresh client (not the
 * pooled `cluster.nodeClient`) keeps raw MULTI/MOVED sequences from disturbing
 * the cluster client and surfaces `-MOVED` instead of auto-following it.
 */
export async function connectToNodeRedisSlotOwner(
  cluster: RedisClusterType,
  key: string | Buffer,
): Promise<RedisClientType> {
  return connectToNodeRedisEndpoint(findNodeRedisSlotOwner(cluster, key))
}

/** Resolve the master endpoint that owns `key`'s slot from the cluster topology. */
export function findNodeRedisSlotOwner(
  cluster: RedisClusterType,
  key: string | Buffer,
): RedisEndpoint {
  const slot = clusterKeySlot(key)
  const shard = cluster.slots[slot]
  if (!shard) {
    throw new Error(`No Redis Cluster slot owner found for slot ${slot}`)
  }
  return { host: shard.master.host, port: shard.master.port }
}

type RawClusterSlotsNode = [string, number, ...unknown[]]
type RawClusterSlotsRange = [number, number, ...RawClusterSlotsNode[]]

function endpointFromRawNode(node: RawClusterSlotsNode): RedisEndpoint {
  return { host: String(node[0]) || '127.0.0.1', port: Number(node[1]) }
}

// node-redis' cluster topology object only tracks masters, so resolve the
// master+replica for a slot by parsing CLUSTER SLOTS directly.
export async function findNodeRedisSlotMasterAndReplica(
  cluster: RedisClusterType,
  key: string | Buffer,
  options: { retries?: number; retryDelayMs?: number } = {},
): Promise<{ slot: number; master: RedisEndpoint; replica: RedisEndpoint }> {
  const retries = options.retries ?? 20
  const retryDelayMs = options.retryDelayMs ?? 500
  const slot = clusterKeySlot(key)

  for (let attempt = 0; attempt <= retries; attempt++) {
    const slots = (await cluster.sendCommand(undefined, true, [
      'CLUSTER',
      'SLOTS',
    ])) as RawClusterSlotsRange[]

    for (const [min, max, master, replica] of slots) {
      if (slot < min || slot > max) {
        continue
      }
      if (!replica) {
        break
      }
      return {
        slot,
        master: endpointFromRawNode(master),
        replica: endpointFromRawNode(replica),
      }
    }

    if (attempt < retries) {
      await new Promise(resolve => setTimeout(resolve, retryDelayMs))
    }
  }

  throw new Error(`No Redis Cluster replica found for slot ${slot}`)
}

// node-redis equivalent of findSlotOwner, parsed from CLUSTER SLOTS so the
// host:port matches what -MOVED replies advertise.
export async function findNodeRedisSlotOwnerEndpoint(
  cluster: RedisClusterType,
  key: string | Buffer,
): Promise<RedisEndpoint> {
  const slot = clusterKeySlot(key)
  const slots = (await cluster.sendCommand(undefined, true, [
    'CLUSTER',
    'SLOTS',
  ])) as RawClusterSlotsRange[]
  for (const [min, max, master] of slots) {
    if (slot >= min && slot <= max) {
      return endpointFromRawNode(master)
    }
  }
  throw new Error(`No Redis Cluster slot owner found for slot ${slot}`)
}

/**
 * Flush every master in a node-redis cluster. node-redis has no keyPrefix
 * (unlike ioredis, which the ioredis suites use to namespace keys), so the
 * node-redis twins call this in `before()` to start from a clean keyspace and
 * avoid collisions with the ioredis suite on the shared real cluster.
 */
export async function flushNodeRedisCluster(
  cluster: RedisClusterType,
): Promise<void> {
  await Promise.all(
    cluster.masters.map(async node => {
      const client = await cluster.nodeClient(node)
      await client.flushAll()
    }),
  )
}

/** node-redis equivalent of {@link countExistingKeys}. */
export async function countExistingNodeRedisKeys(
  cluster: RedisClusterType,
  keys: readonly string[],
): Promise<number> {
  const flags = await Promise.all(keys.map(key => cluster.exists(key)))
  return flags.filter(Boolean).length
}

/** node-redis equivalent of {@link assertKeyCount}. */
export async function assertNodeRedisKeyCount(
  cluster: RedisClusterType,
  pattern: string,
  keys: readonly string[],
  expected: number,
): Promise<void> {
  assert.strictEqual(await countExistingNodeRedisKeys(cluster, keys), expected)

  const owner = await connectToNodeRedisSlotOwner(cluster, keys[0])
  try {
    assert.strictEqual(
      (await owner.keys(pattern)).length,
      expected,
      `expected exactly ${expected} key(s) matching ${pattern}`,
    )
  } finally {
    owner.destroy()
  }
}

/**
 * Return a view of a node-redis client/cluster that decodes bulk-string
 * replies as Buffers (node-redis equivalent of ioredis' `*Buffer` methods).
 */
export function bufferClient<T extends { withTypeMapping: (m: never) => T }>(
  client: T,
): T {
  return client.withTypeMapping({
    [RESP_TYPES.BLOB_STRING]: Buffer,
  } as never)
}

/**
 * The compatibility profile the current run is exercising. Profile-gated
 * integration tests are run once per preset by
 * `npm run test:integration:compatibility:mock`, which sets `REDIS_COMPAT`.
 */
export type ProfileName =
  | 'redis-6.2'
  | 'redis-7.0'
  | 'redis-7.2'
  | 'redis-7.4'
  | 'redis-8.0'
  | 'valkey-8.0'
  | 'valkey-9.0'

export const activeProfile = (process.env.REDIS_COMPAT ??
  'redis-8.0') as ProfileName
