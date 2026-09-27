import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()
// Unique per run: the real-backend suites share one Redis that is never
// flushed between files or between runs, so fixed literal key names collided
// with each other and with their own previous run (#420).
const RUN = randomKey()

describe(`Hash Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HINCRBY command', async () => {
    const incr1 = await redisClient.hIncrBy(`hash8:${RUN}`, 'counter', 5)
    assert.strictEqual(incr1, 5)

    const incr2 = await redisClient.hIncrBy(`hash8:${RUN}`, 'counter', 3)
    assert.strictEqual(incr2, 8)

    const incr3 = await redisClient.hIncrBy(`hash8:${RUN}`, 'counter', -2)
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
        () => redisClient.hIncrBy(key, 'max', 1),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient.hGet(key, 'max'),
        '9223372036854775807',
      )

      await redisClient.hSet(key, 'min', '-9223372036854775808')
      await assert.rejects(
        () => redisClient.hIncrBy(key, 'min', -1),
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
        () => redisClient.hIncrBy(key, 'huge', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('HINCRBYFLOAT command', async () => {
    const incr1 = await redisClient.hIncrByFloat(`hash9:${RUN}`, 'float', 1.5)
    assert.strictEqual(incr1, '1.5')

    const incr2 = await redisClient.hIncrByFloat(`hash9:${RUN}`, 'float', 2.3)
    assert.strictEqual(incr2, '3.8')
  })

  test('HINCRBYFLOAT parses operands like strtold: hex floats, infinity and error order (#234)', async () => {
    const tag = `{hincrbyfloat-strtold:${randomKey()}}`
    const key = `${tag}:hash`
    const stringKey = `${tag}:string`
    // hIncrByFloat takes a number, so a hex token has to go on the wire as is.
    const direct = await connectToNodeRedisSlotOwner(redisClient, key)
    const hincrbyfloat = (target: string, field: string, increment: string) =>
      direct.sendCommand(['HINCRBYFLOAT', target, field, increment])

    try {
      // C99 hex floats are valid increments...
      assert.strictEqual(await hincrbyfloat(key, 'f', '0x10'), '16')
      assert.strictEqual(await hincrbyfloat(key, 'f', '0x1.8p3'), '28')
      assert.strictEqual(await hincrbyfloat(key, 'f', '-0x.8'), '27.5')

      // ...and valid stored values.
      await direct.hSet(key, 'hex', '0x10')
      assert.strictEqual(await direct.hIncrByFloat(key, 'hex', 1), '17')

      for (const bad of [
        '0x',
        '0x1p',
        '0b11',
        ' 0x10',
        '1e-4952',
        '0x1p16384',
      ]) {
        await assert.rejects(
          () => hincrbyfloat(key, 'g', bad),
          errorWithMessage('ERR value is not a valid float'),
          `increment "${bad}"`,
        )
      }
      await direct.hSet(key, 'bad', '0x')
      await assert.rejects(
        () => direct.hIncrByFloat(key, 'bad', 1),
        errorWithMessage('ERR hash value is not a float'),
      )

      // An infinite increment is refused before the arithmetic...
      await assert.rejects(
        () => hincrbyfloat(key, 'g', 'inf'),
        errorWithMessage('ERR value is NaN or Infinity'),
      )
      // ...while a stored infinity is a valid operand whose sum is refused.
      await direct.hSet(key, 'inf', 'inf')
      await assert.rejects(
        () => direct.hIncrByFloat(key, 'inf', 1),
        errorWithMessage('ERR increment would produce NaN or Infinity'),
      )
      assert.strictEqual(await direct.hExists(key, 'g'), 0)

      // The increment is checked before the type.
      await direct.set(stringKey, 'v')
      await assert.rejects(
        () => hincrbyfloat(stringKey, 'f', 'abc'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => hincrbyfloat(stringKey, 'f', 'inf'),
        errorWithMessage('ERR value is NaN or Infinity'),
      )
      await assert.rejects(
        () => hincrbyfloat(stringKey, 'f', '0x10'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await direct.del([key, stringKey])
      direct.destroy()
    }
  })
})
