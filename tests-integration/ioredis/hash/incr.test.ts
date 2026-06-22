import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Hash Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('hash-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HINCRBY command', async () => {
    // HINCRBY on non-existent field
    const incr1 = await redisClient?.hincrby('hash8', 'counter', 5)
    assert.strictEqual(incr1, 5)

    // HINCRBY on existing field
    const incr2 = await redisClient?.hincrby('hash8', 'counter', 3)
    assert.strictEqual(incr2, 8)

    // Negative increment
    const incr3 = await redisClient?.hincrby('hash8', 'counter', -2)
    assert.strictEqual(incr3, 6)
  })

  test('HINCRBY respects Redis 64-bit signed integer range', async () => {
    const key = `{hincrby64:${randomKey()}}`
    try {
      // Values in the gap between 2^53 and 2^63 must keep full precision
      // (JS Number.isSafeInteger() would wrongly reject these).
      await redisClient?.hset(key, 'gap', '9007199254740992') // 2^53
      await redisClient?.hincrby(key, 'gap', '1')
      assert.strictEqual(
        await redisClient?.hget(key, 'gap'),
        '9007199254740993',
      )

      // Large value still inside int64 — no overflow (issue #29 wrongly
      // claimed this overflows; real Redis returns 9000000000000000001).
      await redisClient?.hset(key, 'big', '9000000000000000000')
      await redisClient?.hincrby(key, 'big', '1')
      assert.strictEqual(
        await redisClient?.hget(key, 'big'),
        '9000000000000000001',
      )

      // Positive overflow past INT64_MAX (2^63-1) is rejected, value untouched.
      await redisClient?.hset(key, 'max', '9223372036854775807')
      await assert.rejects(
        () => redisClient?.hincrby(key, 'max', '1'),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient?.hget(key, 'max'),
        '9223372036854775807',
      )

      // Negative overflow past INT64_MIN (-2^63) is rejected, value untouched.
      await redisClient?.hset(key, 'min', '-9223372036854775808')
      await assert.rejects(
        () => redisClient?.hincrby(key, 'min', '-1'),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient?.hget(key, 'min'),
        '-9223372036854775808',
      )

      // Increment argument outside int64 range is a value error.
      await assert.rejects(
        () => redisClient?.hincrby(key, 'gap', '99999999999999999999999'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      // Stored field value outside int64 range is "hash value is not an integer".
      await redisClient?.hset(key, 'huge', '99999999999999999999999')
      await assert.rejects(
        () => redisClient?.hincrby(key, 'huge', '1'),
        errorWithMessage('ERR hash value is not an integer'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('HINCRBYFLOAT command', async () => {
    // HINCRBYFLOAT on non-existent field
    const incr1 = await redisClient?.hincrbyfloat('hash9', 'float', 1.5)
    assert.strictEqual(incr1, '1.5')

    // HINCRBYFLOAT on existing field
    const incr2 = await redisClient?.hincrbyfloat('hash9', 'float', 2.3)
    assert.strictEqual(incr2, '3.8')
  })
})
