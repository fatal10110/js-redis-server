import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

/**
 * INCRBYFLOAT / HINCRBYFLOAT reply with, and store, `%.17Lf` trimmed
 * (`ld2string(…, LD_STR_HUMAN)`) on every Redis and Valkey version: never an
 * exponent, `-0` and anything below the 17th decimal shown as `0` (#543).
 * `incrByFloat` / `hIncrByFloat` send the number's JS spelling (`1e-17`,
 * `1e+21`), which is a valid strtold operand.
 */
const testRunner = new TestRunner()

const ROWS: [number, string][] = [
  [1e-17, '0.00000000000000001'],
  [-1e-17, '-0.00000000000000001'],
  [1e-18, '0'],
  [-1e-18, '0'],
  [5e-324, '0'],
  [1e-7, '0.0000001'],
  [1.5e-8, '0.000000015'],
  [1e21, '1000000000000000000000'],
]

describe(`INCRBYFLOAT result format (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('incrByFloat replies and stores %.17Lf trimmed (#543)', async () => {
    const tag = `{incrbyfloat-format:${randomKey()}}`
    const keys: string[] = []
    try {
      for (const [index, [increment, expected]] of ROWS.entries()) {
        const key = `${tag}:${index}`
        keys.push(key)
        assert.strictEqual(
          await redisClient.incrByFloat(key, increment),
          expected,
          `increment ${increment}`,
        )
        assert.strictEqual(await redisClient.get(key), expected)
      }

      const twice = `${tag}:twice`
      keys.push(twice)
      await redisClient.incrByFloat(twice, 1e-17)
      assert.strictEqual(
        await redisClient.incrByFloat(twice, 1e-17),
        '0.00000000000000002',
      )

      const respelled = `${tag}:respelled`
      keys.push(respelled)
      await redisClient.set(respelled, '1e-7')
      assert.strictEqual(
        await redisClient.incrByFloat(respelled, 1e-7),
        '0.0000002',
      )
      assert.strictEqual(await redisClient.get(respelled), '0.0000002')
    } finally {
      for (const key of keys) {
        await redisClient.del(key)
      }
    }
  })

  test('hIncrByFloat replies and stores %.17Lf trimmed (#543)', async () => {
    const key = `{hincrbyfloat-format:${randomKey()}}:hash`
    try {
      for (const [index, [increment, expected]] of ROWS.entries()) {
        const field = `f${index}`
        assert.strictEqual(
          await redisClient.hIncrByFloat(key, field, increment),
          expected,
          `increment ${increment}`,
        )
        assert.strictEqual(await redisClient.hGet(key, field), expected)
      }
    } finally {
      await redisClient.del(key)
    }
  })
})
