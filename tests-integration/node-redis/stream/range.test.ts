import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

// XINFO replies are RESP3 maps (objects) on node-redis; access raw via
// sendCommand so the same kebab-case keys work on mock and real backends.

describe(`Stream Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  // XTRIM

  // XADD with NOMKSTREAM and MAXLEN options

  // XREAD

  // Stream consumer groups

  test('XRANGE returns entries within an inclusive range', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })
    await redisClient.xAdd(key, '3-1', { c: '3' })

    assert.deepStrictEqual(await redisClient.xRange(key, '-', '+'), [
      { id: '1-1', message: { a: '1' } },
      { id: '2-1', message: { b: '2' } },
      { id: '3-1', message: { c: '3' } },
    ])
    assert.deepStrictEqual(await redisClient.xRange(key, '2', '2'), [
      { id: '2-1', message: { b: '2' } },
    ])
  })

  test('XRANGE honors exclusive bounds and COUNT', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })
    await redisClient.xAdd(key, '3-1', { c: '3' })

    assert.deepStrictEqual(await redisClient.xRange(key, '(1-1', '+'), [
      { id: '2-1', message: { b: '2' } },
      { id: '3-1', message: { c: '3' } },
    ])
    assert.deepStrictEqual(
      await redisClient.xRange(key, '-', '+', { COUNT: 2 }),
      [
        { id: '1-1', message: { a: '1' } },
        { id: '2-1', message: { b: '2' } },
      ],
    )
  })

  test('XRANGE and XREVRANGE reject non-integer COUNT values', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { a: '1' })

      const invalidCount = errorWithMessage(
        'ERR value is not an integer or out of range',
      )
      await assert.rejects(
        () => node.sendCommand(['XRANGE', key, '-', '+', 'COUNT', 'abc']),
        invalidCount,
      )
      await assert.rejects(
        () => node.sendCommand(['XREVRANGE', key, '+', '-', 'COUNT', 'abc']),
        invalidCount,
      )
    } finally {
      node.destroy()
    }
  })

  test('XREVRANGE returns entries in descending order', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })

    assert.deepStrictEqual(await redisClient.xRevRange(key, '+', '-'), [
      { id: '2-1', message: { b: '2' } },
      { id: '1-1', message: { a: '1' } },
    ])
  })

  test('XLEN and XRANGE on a missing key return empty results', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xLen(key), 0)
    assert.deepStrictEqual(await redisClient.xRange(key, '-', '+'), [])
  })
})
