import assert from 'node:assert'
import { Redis, type Cluster } from 'ioredis'
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

function endpointFromClusterSlotsNode(node: ClusterSlotsNode): RedisEndpoint {
  return {
    host: String(node[0]),
    port: node[1],
  }
}
