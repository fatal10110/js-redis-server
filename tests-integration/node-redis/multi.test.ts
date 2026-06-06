import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import assert from 'node:assert'
import { MultiErrorReply, RedisClusterType } from 'redis'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

describe('multi', () => {
  let redisClient: RedisClusterType | undefined

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('Queue commands before execution without piplining', async () => {
    const anotherRedisClient = await testRunner.setupIoredisCluster()
    const key = `{node-redis-multi:${randomKey()}}:queued`

    try {
      const multi = redisClient!.multi()
      multi.set(key, 'myValue')
      const anotherRes = await anotherRedisClient.get(key)
      multi.get(key)

      const res = await multi.exec()

      assert.notEqual(res, null)

      const [first, second] = res

      assert.strictEqual(first, 'OK')
      assert.strictEqual(second, 'myValue')
      assert.strictEqual(anotherRes, null)
    } finally {
      await redisClient?.del(key)
      await anotherRedisClient.quit()
    }
  })

  test('returns execution errors from EXEC replies', async () => {
    const key = `{node-redis-multi:${randomKey()}}:error`
    let error: MultiErrorReply | undefined
    try {
      const multi = redisClient!.multi()
      multi.set(key, 'myValue')
      multi.evalSha('abc')

      try {
        await multi.exec()
      } catch (err) {
        assert.ok(err instanceof MultiErrorReply)
        error = err
      }

      assert.ok(error)
      assert.deepStrictEqual(error.errorIndexes, [1])
      assert.strictEqual(error.replies[0], 'OK')
      assert.ok(error.replies[1] instanceof Error)
      assert.match(error.replies[1].message, /NOSCRIPT/)
    } finally {
      await redisClient?.del(key)
    }
  })
})
