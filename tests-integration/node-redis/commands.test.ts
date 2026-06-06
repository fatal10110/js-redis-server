import { after, before, describe, test } from 'node:test'
import { RedisClusterType } from 'redis'
import { errorWithMessage, randomKey } from '../utils'
import assert from 'node:assert'
import { TestRunner } from '../test-config'

const testRunner = new TestRunner()

describe(`Redis commands with node-redis (${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType | undefined

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  describe('set', () => {
    test('sets data on key and gets value', async () => {
      const key = randomKey()
      await redisClient?.set(key, 1)

      const val = await redisClient?.get(key)
      assert.strictEqual(val, '1')
    })
  })

  describe('mget', () => {
    test('returns multiple key values', async () => {
      const key1 = randomKey()
      const key2 = `another:{${key1}}`
      await redisClient?.set(key1, 1)
      await redisClient?.set(key2, 2)

      const val = await redisClient?.mGet([key1, key2])
      assert.strictEqual(val?.length, 2)
      assert.strictEqual(val[0], '1')
      assert.strictEqual(val[1], '2')
    })

    test('cross slot error', async () => {
      const key1 = randomKey()
      const key2 = randomKey()
      await redisClient?.set(key1, 1)
      await redisClient?.set(key2, 2)

      await assert.rejects(
        () => redisClient?.mGet([key1, key2]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    })
  })
})
