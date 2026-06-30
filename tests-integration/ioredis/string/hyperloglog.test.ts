import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

// Every key shares one hashtag so the whole suite runs over a single direct
// (unprefixed) connection to one slot owner — multi-key PFCOUNT/PFMERGE stay
// same-slot, and we avoid churning a fresh connection per test against the
// shared real cluster.
const TAG = '{hll}'

// HyperLogLog cardinality is approximate (~0.81% standard error in real
// Redis). Assert within a tolerance rather than an exact count.
function assertApprox(actual: number, expected: number, tolerance = 0.1) {
  const diff = Math.abs(actual - expected)
  assert.ok(
    diff <= Math.max(1, expected * tolerance),
    `expected ~${expected}, got ${actual} (tolerance ${tolerance * 100}%)`,
  )
}

describe(`HyperLogLog Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined
  let client: Redis

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('hll-integration')
    client = await connectToSlotOwner(redisClient, TAG)
  })

  after(async () => {
    client?.disconnect()
    await testRunner.cleanup()
  })

  const ns = (): string => `${TAG}:${randomKey()}`

  test('PFADD creates the key, returns 1 when altered, 0 when not', async () => {
    const key = `${ns()}:pfadd`

    // Missing key: created even with zero elements, returns 1.
    assert.strictEqual(await client.call('PFADD', key), 1)
    assert.strictEqual(await client.call('EXISTS', key), 1)
    assert.strictEqual(await client.call('TYPE', key), 'string')

    // Existing key, zero elements: no register altered, returns 0.
    assert.strictEqual(await client.call('PFADD', key), 0)

    // New distinct elements alter registers.
    assert.strictEqual(await client.call('PFADD', key, 'a', 'b', 'c'), 1)
    // Re-adding the same elements alters nothing.
    assert.strictEqual(await client.call('PFADD', key, 'a', 'b', 'c'), 0)
    // A new element alters again.
    assert.strictEqual(await client.call('PFADD', key, 'd'), 1)
  })

  test('PFCOUNT returns approximate cardinality, 0 for missing key', async () => {
    const key = `${ns()}:pfcount`
    const missing = `${ns()}:missing`

    assert.strictEqual(await client.call('PFCOUNT', missing), 0)

    await client.call('PFADD', key, 'a', 'b', 'c')
    assertApprox(Number(await client.call('PFCOUNT', key)), 3)

    // Larger cardinality, still approximate within Redis's documented error.
    const many = Array.from({ length: 1000 }, (_, i) => `el-${i}`)
    await client.call('PFADD', key, ...many)
    assertApprox(Number(await client.call('PFCOUNT', key)), 1000)
  })

  test('PFCOUNT over multiple keys returns the union cardinality', async () => {
    const k1 = `${ns()}:u1`
    const k2 = `${ns()}:u2`

    await client.call('PFADD', k1, 'a', 'b', 'c')
    await client.call('PFADD', k2, 'd', 'e', 'f')
    assertApprox(Number(await client.call('PFCOUNT', k1, k2)), 6)

    // Overlapping elements are not double-counted.
    const k3 = `${ns()}:u3`
    await client.call('PFADD', k3, 'a', 'b', 'g')
    assertApprox(Number(await client.call('PFCOUNT', k1, k3)), 4)
  })

  test('PFMERGE merges sources into destkey (union)', async () => {
    const dest = `${ns()}:merge-dest`
    const k1 = `${ns()}:m1`
    const k2 = `${ns()}:m2`

    await client.call('PFADD', k1, 'a', 'b', 'c')
    await client.call('PFADD', k2, 'd', 'e', 'f')

    assert.strictEqual(await client.call('PFMERGE', dest, k1, k2), 'OK')
    assertApprox(Number(await client.call('PFCOUNT', dest)), 6)

    // PFMERGE with only a destkey creates an empty HLL when missing.
    const solo = `${ns()}:solo`
    assert.strictEqual(await client.call('PFMERGE', solo), 'OK')
    assert.strictEqual(await client.call('EXISTS', solo), 1)
    assert.strictEqual(await client.call('PFCOUNT', solo), 0)

    // Merging into an existing destkey unions with its current contents.
    await client.call('PFADD', dest, 'g')
    const extra = `${ns()}:extra`
    await client.call('PFADD', extra, 'h')
    await client.call('PFMERGE', dest, extra)
    assertApprox(Number(await client.call('PFCOUNT', dest)), 8)
  })

  test('WRONGTYPE on a key holding a non-string type', async () => {
    const key = `${ns()}:wrongtype`
    await client.call('RPUSH', key, 'a', 'b')

    await assert.rejects(
      () => client.call('PFADD', key, 'x'),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
    await assert.rejects(
      () => client.call('PFCOUNT', key),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
    const dest = `${ns()}:merge-into-list`
    await assert.rejects(
      () => client.call('PFMERGE', dest, key),
      errorWithMessage(
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      ),
    )
  })

  test('WRONGTYPE on a string that is not a valid HyperLogLog', async () => {
    const key = `${ns()}:notvalid`
    await client.call('SET', key, 'plain string value')

    await assert.rejects(
      () => client.call('PFADD', key, 'x'),
      errorWithMessage(
        'WRONGTYPE Key is not a valid HyperLogLog string value.',
      ),
    )
    await assert.rejects(
      () => client.call('PFCOUNT', key),
      errorWithMessage(
        'WRONGTYPE Key is not a valid HyperLogLog string value.',
      ),
    )
  })

  test('arity errors match Redis', async () => {
    await assert.rejects(
      () => client.call('PFADD'),
      errorWithMessage("ERR wrong number of arguments for 'pfadd' command"),
    )
    await assert.rejects(
      () => client.call('PFCOUNT'),
      errorWithMessage("ERR wrong number of arguments for 'pfcount' command"),
    )
    await assert.rejects(
      () => client.call('PFMERGE'),
      errorWithMessage("ERR wrong number of arguments for 'pfmerge' command"),
    )
  })
})
