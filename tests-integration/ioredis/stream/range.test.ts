import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Stream Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('stream-integration')
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
    await redisClient?.xadd(key, '1-1', 'a', '1')
    await redisClient?.xadd(key, '2-1', 'b', '2')
    await redisClient?.xadd(key, '3-1', 'c', '3')

    assert.deepStrictEqual(await redisClient?.xrange(key, '-', '+'), [
      ['1-1', ['a', '1']],
      ['2-1', ['b', '2']],
      ['3-1', ['c', '3']],
    ])
    assert.deepStrictEqual(await redisClient?.xrange(key, '2', '2'), [
      ['2-1', ['b', '2']],
    ])
  })

  test('XRANGE honors exclusive bounds and COUNT', async () => {
    const key = randomKey()
    await redisClient?.xadd(key, '1-1', 'a', '1')
    await redisClient?.xadd(key, '2-1', 'b', '2')
    await redisClient?.xadd(key, '3-1', 'c', '3')

    assert.deepStrictEqual(await redisClient?.xrange(key, '(1-1', '+'), [
      ['2-1', ['b', '2']],
      ['3-1', ['c', '3']],
    ])
    assert.deepStrictEqual(
      await redisClient?.xrange(key, '-', '+', 'COUNT', 2),
      [
        ['1-1', ['a', '1']],
        ['2-1', ['b', '2']],
      ],
    )
  })

  test('XRANGE and XREVRANGE reject non-integer COUNT values', async () => {
    const key = randomKey()
    const node = await connectToSlotOwner(redisClient!, key)
    try {
      await node.xadd(key, '1-1', 'a', '1')

      const invalidCount = errorWithMessage(
        'ERR value is not an integer or out of range',
      )
      await assert.rejects(
        () => node.xrange(key, '-', '+', 'COUNT', 'abc'),
        invalidCount,
      )
      await assert.rejects(
        () => node.xrevrange(key, '+', '-', 'COUNT', 'abc'),
        invalidCount,
      )
    } finally {
      node.disconnect()
    }
  })

  test('XREVRANGE returns entries in descending order', async () => {
    const key = randomKey()
    await redisClient?.xadd(key, '1-1', 'a', '1')
    await redisClient?.xadd(key, '2-1', 'b', '2')

    assert.deepStrictEqual(await redisClient?.xrevrange(key, '+', '-'), [
      ['2-1', ['b', '2']],
      ['1-1', ['a', '1']],
    ])
  })

  test('XLEN and XRANGE on a missing key return empty results', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient?.xlen(key), 0)
    assert.deepStrictEqual(await redisClient?.xrange(key, '-', '+'), [])
  })
})
