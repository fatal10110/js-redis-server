import assert from 'node:assert'
import { Redis, type Cluster } from 'ioredis'
import clusterKeySlot from 'cluster-key-slot'
export {
  assertBufferSetsEqual,
  assertBuffersEqual,
  errorWithMessage,
} from '../tests/shared-test-helpers'

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
  const client = new Redis({
    host,
    port,
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
