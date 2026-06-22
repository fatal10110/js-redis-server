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

describe(`String Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('INCR and DECR commands', async () => {
    const incr1 = await redisClient.incr('counter')
    assert.strictEqual(incr1, 1)

    const incr2 = await redisClient.incr('counter')
    assert.strictEqual(incr2, 2)

    const decr1 = await redisClient.decr('counter')
    assert.strictEqual(decr1, 1)
  })

  test('INCRBY and DECRBY commands', async () => {
    const incr1 = await redisClient.incrBy('bycounter', 5)
    assert.strictEqual(incr1, 5)

    const incr2 = await redisClient.incrBy('bycounter', 3)
    assert.strictEqual(incr2, 8)

    const decr1 = await redisClient.decrBy('bycounter', 2)
    assert.strictEqual(decr1, 6)
  })

  test('INCR/INCRBY/DECR/DECRBY operate over the full int64 range', async () => {
    const tag = `{int64:${randomKey()}}`
    const key = `${tag}:counter`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      // INCR just below the int64 max reaches it exactly (precision > 2^53)
      await directClient.set(key, '9223372036854775806')
      await directClient.incr(key)
      assert.strictEqual(await directClient.get(key), '9223372036854775807')

      // INCR at the int64 max overflows and leaves the value untouched
      await assert.rejects(
        () => directClient.incr(key),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(await directClient.get(key), '9223372036854775807')

      // DECR at the int64 min overflows
      await directClient.set(key, '-9223372036854775808')
      await assert.rejects(
        () => directClient.decr(key),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(await directClient.get(key), '-9223372036854775808')

      // INCRBY with a large in-range amount keeps full precision
      await directClient.set(key, '1')
      await directClient.sendCommand(['INCRBY', key, '9223372036854775806'])
      assert.strictEqual(await directClient.get(key), '9223372036854775807')

      // INCRBY that would cross the int64 max overflows
      await assert.rejects(
        () => directClient.sendCommand(['INCRBY', key, '1']),
        errorWithMessage('ERR increment or decrement would overflow'),
      )

      // INCRBY/DECRBY amount outside the int64 range is rejected outright
      await assert.rejects(
        () =>
          directClient.sendCommand(['INCRBY', key, '99999999999999999999999']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient.sendCommand(['DECRBY', key, '99999999999999999999999']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      // DECRBY by the int64 min cannot be negated -> dedicated overflow message
      await directClient.set(key, '0')
      await assert.rejects(
        () => directClient.sendCommand(['DECRBY', key, '-9223372036854775808']),
        errorWithMessage('ERR decrement would overflow'),
      )

      // DECRBY large in-range amount keeps full precision
      await directClient.set(key, '-1')
      await directClient.sendCommand(['DECRBY', key, '9223372036854775807'])
      assert.strictEqual(await directClient.get(key), '-9223372036854775808')

      // INCR on a value already out of int64 range is rejected
      await directClient.set(key, '99999999999999999999999')
      await assert.rejects(
        () => directClient.incr(key),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
    } finally {
      await directClient.del(key)
      directClient.destroy()
    }
  })

  test('INCRBYFLOAT command', async () => {
    const incr1 = await redisClient.incrByFloat('floatcounter', 1.5)
    assert.strictEqual(incr1, '1.5')

    const incr2 = await redisClient.incrByFloat('floatcounter', 2.3)
    assert.strictEqual(incr2, '3.8')
  })

  test('INCRBYFLOAT distinguishes invalid-float from NaN/Infinity result (#56)', async () => {
    const tag = `{incrbyfloat-inf:${randomKey()}}`
    const key = `${tag}:key`
    const direct = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      // A result of +/-Infinity (an infinity increment) reports the
      // post-arithmetic error, NOT the "value is not a valid float" parse error.
      for (const increment of [
        'inf',
        '+inf',
        '-inf',
        'infinity',
        'Inf',
        'INF',
      ]) {
        await direct.set(key, '3.0')
        await assert.rejects(
          () => direct.sendCommand(['INCRBYFLOAT', key, increment]),
          errorWithMessage('ERR increment would produce NaN or Infinity'),
          `increment "${increment}" should report the NaN/Infinity error`,
        )
        // The key is left untouched on error.
        assert.strictEqual(await direct.get(key), '3.0')
      }

      // A stored infinity plus a finite increment is still infinity.
      await direct.set(key, 'inf')
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, '1']),
        errorWithMessage('ERR increment would produce NaN or Infinity'),
      )

      // inf + (-inf) = NaN — also the post-arithmetic error.
      await direct.set(key, 'inf')
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, '-inf']),
        errorWithMessage('ERR increment would produce NaN or Infinity'),
      )

      // Genuinely non-numeric / overflow-magnitude increments are parse errors.
      await direct.set(key, '1')
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, 'nan']),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, '1e5000']),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key, 'abc']),
        errorWithMessage('ERR value is not a valid float'),
      )

      // Redis parses the whole token (strtold): trailing junk, leading or
      // trailing whitespace, and non-'.' separators are all invalid floats,
      // not silent prefix parses.
      for (const bad of ['3abc', '3.5x', ' 3.5', '3.5 ', '1,5', '', '0x']) {
        await direct.set(key, '1')
        await assert.rejects(
          () => direct.sendCommand(['INCRBYFLOAT', key, bad]),
          errorWithMessage('ERR value is not a valid float'),
          `increment "${bad}" should be an invalid float`,
        )
        // The key is left untouched on a parse error.
        assert.strictEqual(await direct.get(key), '1')
      }

      // A finite increment still works normally.
      await direct.set(key, '3')
      assert.strictEqual(
        await direct.sendCommand(['INCRBYFLOAT', key, '1.0e2']),
        '103',
      )

      // Arity.
      await assert.rejects(
        () => direct.sendCommand(['INCRBYFLOAT', key]),
        errorWithMessage(
          "ERR wrong number of arguments for 'incrbyfloat' command",
        ),
      )
    } finally {
      await direct.del(key)
      direct.destroy()
    }
  })
})
