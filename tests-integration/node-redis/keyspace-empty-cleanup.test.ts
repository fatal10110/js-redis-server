import { RedisClusterType, WatchError } from 'redis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToNodeRedisSlotOwner } from '../utils'

const testRunner = new TestRunner()

// Issue #124: empty-collection cleanup + no-op-mutation rules, verified at the
// wire level — no-op mutations on a missing key don't disturb a WATCH, emptying
// a key removes it, and a real emptying still dirties a WATCH.
describe('Empty-collection cleanup / no-op mutations (#124) (node-redis)', () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('no-op HDEL on a non-existent key must not invalidate a WATCH on that key', async () => {
    const key = 'ghost:hdel'
    const watcher = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await redisClient.del(key)
      await watcher.watch(key)

      // No-op mutation from another connection on the same node.
      assert.strictEqual(await redisClient.hDel(key, 'field'), 0)

      const result = await watcher.multi().set(key, 'value').exec()
      assert.deepStrictEqual(result, ['OK'])
    } finally {
      await redisClient.del(key)
      watcher.destroy()
    }
  })

  test('no-op SREM on a non-existent key must not invalidate a WATCH on that key', async () => {
    const key = 'ghost:srem'
    const watcher = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await redisClient.del(key)
      await watcher.watch(key)

      assert.strictEqual(await redisClient.sRem(key, 'member'), 0)

      const result = await watcher.multi().set(key, 'value').exec()
      assert.deepStrictEqual(result, ['OK'])
    } finally {
      await redisClient.del(key)
      watcher.destroy()
    }
  })

  test('emptying a hash via HDEL deletes the key (no phantom empty hash persists)', async () => {
    const key = 'cleanup:hash'

    await redisClient.del(key)
    await redisClient.hSet(key, 'f', 'v')
    assert.strictEqual(await redisClient.exists(key), 1)

    await redisClient.hDel(key, 'f')

    assert.strictEqual(await redisClient.exists(key), 0)
    assert.strictEqual(await redisClient.type(key), 'none')
  })

  test('emptying an EXISTING watched collection still invalidates the WATCH', async () => {
    const key = 'cleanup:watch:existing'
    const watcher = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await redisClient.del(key)
      await redisClient.sAdd(key, 'm')
      await watcher.watch(key)

      // Removing the last member empties (and deletes) the key — a real
      // mutation that must dirty the WATCH.
      assert.strictEqual(await redisClient.sRem(key, 'm'), 1)

      await assert.rejects(
        () => watcher.multi().set(key, 'value').exec(),
        WatchError,
      )
    } finally {
      await redisClient.del(key)
      watcher.destroy()
    }
  })
})
