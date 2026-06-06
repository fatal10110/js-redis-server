import assert from 'node:assert'
import type { Cluster } from 'ioredis'
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
