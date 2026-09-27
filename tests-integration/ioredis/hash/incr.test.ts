import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()
// Unique per run: the real-backend suites share one Redis that is never
// flushed between files or between runs, so fixed literal key names collided
// with each other and with their own previous run (#420).
const RUN = randomKey()

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
    const incr1 = await redisClient?.hincrby(`hash8:${RUN}`, 'counter', 5)
    assert.strictEqual(incr1, 5)

    // HINCRBY on existing field
    const incr2 = await redisClient?.hincrby(`hash8:${RUN}`, 'counter', 3)
    assert.strictEqual(incr2, 8)

    // Negative increment
    const incr3 = await redisClient?.hincrby(`hash8:${RUN}`, 'counter', -2)
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
    const incr1 = await redisClient?.hincrbyfloat(`hash9:${RUN}`, 'float', 1.5)
    assert.strictEqual(incr1, '1.5')

    // HINCRBYFLOAT on existing field
    const incr2 = await redisClient?.hincrbyfloat(`hash9:${RUN}`, 'float', 2.3)
    assert.strictEqual(incr2, '3.8')
  })

  test('HINCRBYFLOAT parses operands like strtold: hex floats, infinity and error order (#234)', async () => {
    const tag = `{hincrbyfloat-strtold:${randomKey()}}`
    const key = `${tag}:hash`
    const stringKey = `${tag}:string`

    try {
      // C99 hex floats are valid increments...
      assert.strictEqual(
        await redisClient?.hincrbyfloat(key, 'f', '0x10'),
        '16',
      )
      assert.strictEqual(
        await redisClient?.hincrbyfloat(key, 'f', '0x1.8p3'),
        '28',
      )
      assert.strictEqual(
        await redisClient?.hincrbyfloat(key, 'f', '-0x.8'),
        '27.5',
      )

      // ...and valid stored values.
      await redisClient?.hset(key, 'hex', '0x10')
      assert.strictEqual(await redisClient?.hincrbyfloat(key, 'hex', '1'), '17')

      for (const bad of [
        '0x',
        '0x1p',
        '0b11',
        ' 0x10',
        '1e-4952',
        '0x1p16384',
      ]) {
        await assert.rejects(
          () => redisClient!.hincrbyfloat(key, 'g', bad),
          errorWithMessage('ERR value is not a valid float'),
          `increment "${bad}"`,
        )
      }
      await redisClient?.hset(key, 'bad', '0x')
      await assert.rejects(
        () => redisClient!.hincrbyfloat(key, 'bad', '1'),
        errorWithMessage('ERR hash value is not a float'),
      )

      // An infinite increment is refused before the arithmetic...
      await assert.rejects(
        () => redisClient!.hincrbyfloat(key, 'g', 'inf'),
        errorWithMessage('ERR value is NaN or Infinity'),
      )
      // ...while a stored infinity is a valid operand whose sum is refused.
      await redisClient?.hset(key, 'inf', 'inf')
      await assert.rejects(
        () => redisClient!.hincrbyfloat(key, 'inf', '1'),
        errorWithMessage('ERR increment would produce NaN or Infinity'),
      )
      assert.strictEqual(await redisClient?.hexists(key, 'g'), 0)

      // The increment is checked before the type.
      await redisClient?.set(stringKey, 'v')
      await assert.rejects(
        () => redisClient!.hincrbyfloat(stringKey, 'f', 'abc'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient!.hincrbyfloat(stringKey, 'f', 'inf'),
        errorWithMessage('ERR value is NaN or Infinity'),
      )
      await assert.rejects(
        () => redisClient!.hincrbyfloat(stringKey, 'f', '0x10'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await redisClient?.del(key, stringKey)
    }
  })
})
