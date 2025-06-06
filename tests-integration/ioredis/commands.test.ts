import { after, before, describe, it } from 'node:test'
import { Cluster } from 'ioredis'
import { randomKey } from '../utils'
import assert from 'node:assert'
import { TestRunner } from '../test-config'

const testRunner = new TestRunner()

describe(`Redis commands with ioredis ${testRunner.getBackendName()}`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  describe.skip('set', () => {
    it('sets data on key and gets value', async () => {
      const key = randomKey()
      await redisClient?.set(key, 1)

      const val = await redisClient?.get(key)
      assert.strictEqual(val, '1')
    })
  })

  describe.skip('mget', () => {
    it.skip('returns multiple key values', async () => {
      const key1 = randomKey()
      const key2 = `another:{${key1}}`
      await redisClient?.set(key1, 1)
      await redisClient?.set(key2, 2)

      const val = await redisClient?.mget(key1, key2)
      assert.strictEqual(val?.length, 2)
      assert.strictEqual(val[0], '1')
      assert.strictEqual(val[1], '2')
    })

    it('cross slot error', async () => {
      const key1 = randomKey()
      const key2 = randomKey()
      await redisClient?.set(key1, 1)
      await redisClient?.set(key2, 2)
      let error: Error | undefined

      try {
        await redisClient?.mget(key1, key2)
      } catch (err) {
        error = err instanceof Error ? err : new Error(String(err))
      }

      assert.ok(error)
      assert.strictEqual(
        error.message,
        "CROSSSLOT Keys in request don't hash to the same slot",
      )
    })
  })
})
