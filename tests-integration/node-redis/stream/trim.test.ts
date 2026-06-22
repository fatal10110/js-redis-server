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

  test('XDEL removes entries, keeps the empty stream, and retains last id', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { a: '1' })
    await redisClient.xAdd(key, '2-1', { b: '2' })

    assert.strictEqual(await redisClient.xDel(key, ['1-1', '9-9']), 1)
    assert.strictEqual(await redisClient.xLen(key), 1)

    assert.strictEqual(await redisClient.xDel(key, '2-1'), 1)
    assert.strictEqual(await redisClient.xLen(key), 0)
    assert.strictEqual(await redisClient.exists(key), 1)

    await assert.rejects(
      () => redisClient.xAdd(key, '2-1', { f: 'v' }),
      errorWithMessage(
        'ERR The ID specified in XADD is equal or smaller than the target stream top item',
      ),
    )
  })

  test('XDEL on a missing key returns 0', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xDel(key, '1-1'), 0)
    assert.strictEqual(await redisClient.exists(key), 0)
  })

  test('XTRIM MAXLEN removes oldest entries and returns removed count', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { f: 'v' })
    await redisClient.xAdd(key, '2-1', { f: 'v' })
    await redisClient.xAdd(key, '3-1', { f: 'v' })

    assert.strictEqual(await redisClient.xTrim(key, 'MAXLEN', 2), 1)
    assert.deepStrictEqual(await redisClient.xRange(key, '-', '+'), [
      { id: '2-1', message: { f: 'v' } },
      { id: '3-1', message: { f: 'v' } },
    ])
  })

  test('XTRIM MAXLEN with ~ (approximate) does not exact-trim tiny streams', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })

      const removed = await node.xTrim(key, 'MAXLEN', 2, {
        strategyModifier: '~',
      })
      assert.strictEqual(removed, 0)
      assert.strictEqual(await node.xLen(key), 3)
      assert.deepStrictEqual(await node.xRange(key, '-', '+'), [
        { id: '1-1', message: { f: 'v' } },
        { id: '2-1', message: { f: 'v' } },
        { id: '3-1', message: { f: 'v' } },
      ])
    } finally {
      node.destroy()
    }
  })

  test('XTRIM MAXLEN with ~ accepts LIMIT count', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })

      const removed = await node.xTrim(key, 'MAXLEN', 2, {
        strategyModifier: '~',
        LIMIT: 1,
      })
      assert.ok(typeof removed === 'number' && removed >= 0)
    } finally {
      node.destroy()
    }
  })

  test('XTRIM LIMIT validates approximate trim syntax', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-1', { f: 'v' })
      await node.xAdd(key, '2-1', { f: 'v' })
      await node.xAdd(key, '3-1', { f: 'v' })

      await assert.rejects(
        () => node.sendCommand(['XTRIM', key, 'MAXLEN', '2', 'LIMIT', '1']),
        errorWithMessage(
          'ERR syntax error, LIMIT cannot be used without the special ~ option',
        ),
      )
      await assert.rejects(
        () => node.sendCommand(['XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          node.sendCommand(['XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT', 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          node.sendCommand(['XTRIM', key, 'MAXLEN', '~', '2', 'LIMIT', '-1']),
        errorWithMessage('ERR The LIMIT argument must be >= 0.'),
      )
      const removed = (await node.sendCommand([
        'XTRIM',
        key,
        'MAXLEN',
        '~',
        '2',
        'LIMIT',
        '1',
        'LIMIT',
        '1',
      ])) as number
      assert.ok(typeof removed === 'number' && removed >= 0)
    } finally {
      node.destroy()
    }
  })

  test('XTRIM MINID removes entries with id below threshold', async () => {
    const key = randomKey()
    const node = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      await node.xAdd(key, '1-0', { f: 'v' })
      await node.xAdd(key, '2-0', { f: 'v' })
      await node.xAdd(key, '3-0', { f: 'v' })

      assert.strictEqual(await node.xTrim(key, 'MINID', 2), 1)
      assert.deepStrictEqual(await node.xRange(key, '-', '+'), [
        { id: '2-0', message: { f: 'v' } },
        { id: '3-0', message: { f: 'v' } },
      ])
    } finally {
      node.destroy()
    }
  })

  test('XTRIM on missing key returns 0 without creating it', async () => {
    const key = randomKey()
    assert.strictEqual(await redisClient.xTrim(key, 'MAXLEN', 5), 0)
    assert.strictEqual(await redisClient.exists(key), 0)
  })

  test('XTRIM MAXLEN no-op when stream is within limit', async () => {
    const key = randomKey()
    await redisClient.xAdd(key, '1-1', { f: 'v' })
    await redisClient.xAdd(key, '2-1', { f: 'v' })

    assert.strictEqual(await redisClient.xTrim(key, 'MAXLEN', 5), 0)
    assert.strictEqual(await redisClient.xLen(key), 2)
  })
})
