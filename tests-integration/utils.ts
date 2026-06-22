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

export async function getTotalDbSize(redisClient: Cluster): Promise<number> {
  const masterNodes = redisClient.nodes('master')
  const sizes = await Promise.all(
    masterNodes.map(async node => {
      return await node.dbsize()
    }),
  )

  return sizes.reduce((total, size) => total + size, 0)
}

export async function assertDbSizeDelta(
  redisClient: Cluster,
  baseline: number,
  expectedDelta: number,
): Promise<void> {
  const size = await getTotalDbSize(redisClient)
  assert.strictEqual(size - baseline, expectedDelta)
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

export async function getNodeRedisTotalDbSize(
  cluster: RedisClusterType,
): Promise<number> {
  const sizes = await Promise.all(
    cluster.masters.map(async node => {
      const client = await cluster.nodeClient(node)
      return client.dbSize()
    }),
  )
  return sizes.reduce((total, size) => total + size, 0)
}

export async function assertNodeRedisDbSizeDelta(
  cluster: RedisClusterType,
  baseline: number,
  expectedDelta: number,
): Promise<void> {
  const size = await getNodeRedisTotalDbSize(cluster)
  assert.strictEqual(size - baseline, expectedDelta)
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
