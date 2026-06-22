import { Cluster } from 'ioredis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, randomKey } from '../utils'

const testRunner = new TestRunner()

describe('multi', () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster()
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
      assert.ok(Array.isArray(res))

      const [[, first], [, second]] = res

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
    multi.evalsha('abc')

    let error: Error | undefined

    try {
      await multi.exec()
    } catch (err) {
      error = err instanceof Error ? err : new Error(String(err))
    }

    assert.ok(error)
    assert.ok(error.message.includes('EXECABORT'))
  })

  test('handle graceful errors in multi', async () => {
    const multi = redisClient!.multi()
    multi.set('myKey', 'myValue')
    multi.evalsha('abc', 0)

    const res = await multi.exec()

    assert.ok(res)
    assert.strictEqual(res[0][1], 'OK')
    assert.ok(res[1][0]?.message.includes('NOSCRIPT'))
  })

  test('runtime command errors are returned as EXEC array elements', async () => {
    const key = `{tx-wrong-type:${randomKey()}}:list`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.lpush(key, 'value')
      const result = await directClient.multi().get(key).exec()

      assert.ok(result)
      assert.strictEqual(result.length, 1)
      assert.ok(result[0][0] instanceof Error)
      assert.strictEqual(
        result[0][0]?.message,
        'WRONGTYPE Operation against a key holding the wrong kind of value',
      )
      assert.strictEqual(result[0][1], undefined)
    } finally {
      await directClient.del(key)
      directClient.disconnect()
    }
  })
})
