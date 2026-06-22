import { RedisClientType, RedisClusterType } from 'redis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToNodeRedisSlotOwner, randomKey } from '../utils'
import { errorWithMessage } from '../../tests/shared-test-helpers'

const testRunner = new TestRunner()

describe('MOVE command integration (node-redis, standalone)', () => {
  let client: RedisClientType

  before(async () => {
    client = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('moves a key to another database and removes it from the source', async () => {
    const key = randomKey()

    await client.select(0)
    await client.set(key, 'value')

    assert.strictEqual(await client.move(key, 1), 1)
    assert.strictEqual(await client.get(key), null)

    await client.select(1)
    assert.strictEqual(await client.get(key), 'value')
    await client.select(0)
  })

  test('returns 0 when the source key is missing', async () => {
    const key = randomKey()

    await client.select(0)
    assert.strictEqual(await client.move(key, 1), 0)

    await client.select(1)
    assert.strictEqual(await client.exists(key), 0)
    await client.select(0)
  })

  test('returns 0 and leaves both databases unchanged when destination exists', async () => {
    const key = randomKey()

    await client.select(0)
    await client.set(key, 'source')
    await client.select(1)
    await client.set(key, 'destination')

    await client.select(0)
    assert.strictEqual(await client.move(key, 1), 0)
    assert.strictEqual(await client.get(key), 'source')

    await client.select(1)
    assert.strictEqual(await client.get(key), 'destination')
    await client.select(0)
  })

  test('preserves TTL on the moved key', async () => {
    const key = randomKey()

    await client.select(0)
    await client.set(key, 'value', { expiration: { type: 'EX', value: 1000 } })

    assert.strictEqual(await client.move(key, 1), 1)

    await client.select(1)
    const ttl = await client.ttl(key)
    assert.ok(ttl > 990 && ttl <= 1000, `expected ttl ~1000, got ${ttl}`)
    await client.select(0)
  })

  test('errors when moving to the current database', async () => {
    const key = randomKey()

    await client.select(0)
    await client.set(key, 'value')

    await assert.rejects(
      client.move(key, 0),
      errorWithMessage('ERR source and destination objects are the same'),
    )
  })

  test('errors when the destination DB index is out of range', async () => {
    const key = randomKey()

    await client.select(0)
    await client.set(key, 'value')

    await assert.rejects(
      client.move(key, 99),
      errorWithMessage('ERR DB index is out of range'),
    )

    await assert.rejects(
      client.sendCommand(['MOVE', key, '-1']),
      errorWithMessage('ERR DB index is out of range'),
    )
  })

  test('errors when the destination DB index is not an integer', async () => {
    const key = randomKey()

    await client.select(0)
    await client.set(key, 'value')

    await assert.rejects(
      client.sendCommand(['MOVE', key, 'abc']),
      errorWithMessage('ERR value is not an integer or out of range'),
    )
  })

  test('errors on wrong number of arguments', async () => {
    await assert.rejects(
      client.sendCommand(['MOVE']),
      errorWithMessage("ERR wrong number of arguments for 'move' command"),
    )

    await assert.rejects(
      client.sendCommand(['MOVE', 'only-key']),
      errorWithMessage("ERR wrong number of arguments for 'move' command"),
    )

    await assert.rejects(
      client.sendCommand(['MOVE', 'key', '1', 'extra']),
      errorWithMessage("ERR wrong number of arguments for 'move' command"),
    )
  })
})

describe(`MOVE command cluster rejection (node-redis, ${testRunner.getBackendName()})`, () => {
  let cluster: RedisClusterType

  before(async () => {
    cluster = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('rejects MOVE in cluster mode', async () => {
    const key = `{move:${randomKey()}}:cluster`
    const slotOwner = await connectToNodeRedisSlotOwner(cluster, key)

    try {
      await assert.rejects(
        slotOwner.sendCommand(['MOVE', key, '1']),
        errorWithMessage('ERR MOVE is not allowed in cluster mode'),
      )
    } finally {
      slotOwner.destroy()
    }
  })
})
