import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Hash Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HINCRBY command', async () => {
    const incr1 = await redisClient.hIncrBy('hash8', 'counter', 5)
    assert.strictEqual(incr1, 5)

    const incr2 = await redisClient.hIncrBy('hash8', 'counter', 3)
    assert.strictEqual(incr2, 8)

    const incr3 = await redisClient.hIncrBy('hash8', 'counter', -2)
    assert.strictEqual(incr3, 6)
  })

  test('HINCRBY respects Redis 64-bit signed integer range', async () => {
    const key = `{hincrby64:${randomKey()}}`
    try {
      await redisClient.hSet(key, 'gap', '9007199254740992') // 2^53
      await redisClient.hIncrBy(key, 'gap', 1)
      assert.strictEqual(await redisClient.hGet(key, 'gap'), '9007199254740993')

      await redisClient.hSet(key, 'big', '9000000000000000000')
      await redisClient.hIncrBy(key, 'big', 1)
      assert.strictEqual(
        await redisClient.hGet(key, 'big'),
        '9000000000000000001',
      )

      await redisClient.hSet(key, 'max', '9223372036854775807')
      await assert.rejects(
        () => redisClient.sendCommand(key, false, ['HINCRBY', key, 'max', '1']),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient.hGet(key, 'max'),
        '9223372036854775807',
      )

      await redisClient.hSet(key, 'min', '-9223372036854775808')
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, false, ['HINCRBY', key, 'min', '-1']),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient.hGet(key, 'min'),
        '-9223372036854775808',
      )

      await assert.rejects(
        () =>
          redisClient.sendCommand(key, false, [
            'HINCRBY',
            key,
            'gap',
            '99999999999999999999999',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      await redisClient.hSet(key, 'huge', '99999999999999999999999')
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, false, ['HINCRBY', key, 'huge', '1']),
        errorWithMessage('ERR hash value is not an integer'),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('HINCRBYFLOAT command', async () => {
    const incr1 = await redisClient.hIncrByFloat('hash9', 'float', 1.5)
    assert.strictEqual(incr1, '1.5')

    const incr2 = await redisClient.hIncrByFloat('hash9', 'float', 2.3)
    assert.strictEqual(incr2, '3.8')
  })
})
