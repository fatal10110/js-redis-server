import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

// node-redis twin of the ioredis HyperLogLog suite. Every key shares one
// hashtag so the whole suite runs over a single direct connection to one
// slot owner — multi-key PFCOUNT/PFMERGE stay same-slot and `sendCommand`
// gives exact arg control for arity/error assertions.
const TAG = '{hll-nr}'

// HyperLogLog cardinality is approximate (~0.81% standard error in real
// Redis). Assert within a tolerance rather than an exact count.
function assertApprox(actual: number, expected: number, tolerance = 0.1) {
  const diff = Math.abs(actual - expected)
  assert.ok(
    diff <= Math.max(1, expected * tolerance),
    `expected ~${expected}, got ${actual} (tolerance ${tolerance * 100}%)`,
  )
}

describe(`HyperLogLog Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType
  let client: RedisClientType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
    client = await connectToNodeRedisSlotOwner(redisClient, TAG)
  })

  after(async () => {
    client?.destroy()
    await testRunner.cleanup()
  })

  const ns = (): string => `${TAG}:${randomKey()}`

  test('PFADD creates the key, returns 1 when altered, 0 when not', async () => {
    const key = `${ns()}:pfadd`

    // Missing key: created even with zero elements, returns 1.
    assert.strictEqual(await client.sendCommand(['PFADD', key]), 1)
    assert.strictEqual(await client.sendCommand(['EXISTS', key]), 1)
    assert.strictEqual(await client.sendCommand(['TYPE', key]), 'string')

    // Existing key, zero elements: no register altered, returns 0.
    assert.strictEqual(await client.sendCommand(['PFADD', key]), 0)

    // New distinct elements alter registers.
    assert.strictEqual(
      await client.sendCommand(['PFADD', key, 'a', 'b', 'c']),
      1,
    )
    // Re-adding the same elements alters nothing.
    assert.strictEqual(
      await client.sendCommand(['PFADD', key, 'a', 'b', 'c']),
      0,
    )
    // A new element alters again.
    assert.strictEqual(await client.sendCommand(['PFADD', key, 'd']), 1)
  })

  test('PFCOUNT returns approximate cardinality, 0 for missing key', async () => {
    const key = `${ns()}:pfcount`
    const missing = `${ns()}:missing`

    assert.strictEqual(await client.sendCommand(['PFCOUNT', missing]), 0)

    await client.sendCommand(['PFADD', key, 'a', 'b', 'c'])
    assertApprox(Number(await client.sendCommand(['PFCOUNT', key])), 3)

    // Larger cardinality, still approximate within Redis's documented error.
    const many = Array.from({ length: 1000 }, (_, i) => `el-${i}`)
    await client.sendCommand(['PFADD', key, ...many])
    assertApprox(Number(await client.sendCommand(['PFCOUNT', key])), 1000)
  })

  test('PFCOUNT over multiple keys returns the union cardinality', async () => {
    const k1 = `${ns()}:u1`
    const k2 = `${ns()}:u2`

    await client.sendCommand(['PFADD', k1, 'a', 'b', 'c'])
    await client.sendCommand(['PFADD', k2, 'd', 'e', 'f'])
    assertApprox(Number(await client.sendCommand(['PFCOUNT', k1, k2])), 6)

    // Overlapping elements are not double-counted.
    const k3 = `${ns()}:u3`
    await client.sendCommand(['PFADD', k3, 'a', 'b', 'g'])
    assertApprox(Number(await client.sendCommand(['PFCOUNT', k1, k3])), 4)
  })

  test('PFMERGE merges sources into destkey (union)', async () => {
    const dest = `${ns()}:merge-dest`
    const k1 = `${ns()}:m1`
    const k2 = `${ns()}:m2`

    await client.sendCommand(['PFADD', k1, 'a', 'b', 'c'])
    await client.sendCommand(['PFADD', k2, 'd', 'e', 'f'])

    assert.strictEqual(
      await client.sendCommand(['PFMERGE', dest, k1, k2]),
      'OK',
    )
    assertApprox(Number(await client.sendCommand(['PFCOUNT', dest])), 6)

    // PFMERGE with only a destkey creates an empty HLL when missing.
    const solo = `${ns()}:solo`
    assert.strictEqual(await client.sendCommand(['PFMERGE', solo]), 'OK')
    assert.strictEqual(await client.sendCommand(['EXISTS', solo]), 1)
    assert.strictEqual(await client.sendCommand(['PFCOUNT', solo]), 0)

    // Merging into an existing destkey unions with its current contents.
    await client.sendCommand(['PFADD', dest, 'g'])
    const extra = `${ns()}:extra`
    await client.sendCommand(['PFADD', extra, 'h'])
    await client.sendCommand(['PFMERGE', dest, extra])
    assertApprox(Number(await client.sendCommand(['PFCOUNT', dest])), 8)
  })

  test('WRONGTYPE on a key holding a non-string type', async () => {
    const key = `${ns()}:wrongtype`
    await client.sendCommand(['RPUSH', key, 'a', 'b'])

    const wrongType = errorWithMessage(
      'WRONGTYPE Operation against a key holding the wrong kind of value',
    )
    await assert.rejects(
      () => client.sendCommand(['PFADD', key, 'x']),
      wrongType,
    )
    await assert.rejects(() => client.sendCommand(['PFCOUNT', key]), wrongType)
    const dest = `${ns()}:merge-into-list`
    await assert.rejects(
      () => client.sendCommand(['PFMERGE', dest, key]),
      wrongType,
    )
  })

  test('WRONGTYPE on a string that is not a valid HyperLogLog', async () => {
    const key = `${ns()}:notvalid`
    await client.sendCommand(['SET', key, 'plain string value'])

    const invalidHll = errorWithMessage(
      'WRONGTYPE Key is not a valid HyperLogLog string value.',
    )
    await assert.rejects(
      () => client.sendCommand(['PFADD', key, 'x']),
      invalidHll,
    )
    await assert.rejects(() => client.sendCommand(['PFCOUNT', key]), invalidHll)
  })

  test('arity errors match Redis', async () => {
    await assert.rejects(
      () => client.sendCommand(['PFADD']),
      errorWithMessage("ERR wrong number of arguments for 'pfadd' command"),
    )
    await assert.rejects(
      () => client.sendCommand(['PFCOUNT']),
      errorWithMessage("ERR wrong number of arguments for 'pfcount' command"),
    )
    await assert.rejects(
      () => client.sendCommand(['PFMERGE']),
      errorWithMessage("ERR wrong number of arguments for 'pfmerge' command"),
    )
  })
})
