import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'

const testRunner = new TestRunner()

describe.skip('multi', () => {
  let redisClient: RedisClusterType | undefined

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('Queue commands before execution without piplining', async () => {
    const anotherRedisClient = await testRunner.setupIoredisCluster()

    try {
      const multi = redisClient!.multi()
      multi.set('myKey', 'myValue')
      const anotherRes = await anotherRedisClient.get('myKey')
      multi.get('myKey')

      const res = await multi.exec()

      assert.notEqual(res, null)

      const [first, second] = res

      assert.strictEqual(first, 'OK')
      assert.strictEqual(second, 'myValue')
      assert.strictEqual(anotherRes, null)
    } finally {
      await anotherRedisClient.quit()
    }
  })

  test('handle errors in multi', async () => {
    const multi = redisClient!.multi()
    multi.set('myKey', 'myValue')
    multi.evalSha('abc')

    let error

    try {
      await multi.exec()
    } catch (err) {
      error = err
    }

    assert.ok(error)
    assert.ok(error.message.includes('NOSCRIPT'))
  })
})
