import { RedisClusterType, WatchError } from 'redis'
import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToNodeRedisSlotOwner, errorWithMessage } from '../utils'

const testRunner = new TestRunner()

describe('WATCH/UNWATCH (node-redis)', () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
  })

  after(async () => {
    await testRunner.cleanup()
  })

  it('WATCH should abort transaction if watched key is modified', async () => {
    const key = 'watchkey'
    const watcher = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await redisClient.set(key, 'initial')
      await watcher.watch(key)

      // Modify the key from another connection on the same node.
      await redisClient.set(key, 'modified')

      // node-redis throws WatchError when the optimistic lock is broken.
      await assert.rejects(
        () => watcher.multi().set(key, 'transactional').get(key).exec(),
        WatchError,
      )

      assert.strictEqual(await watcher.get(key), 'modified')
    } finally {
      watcher.destroy()
    }
  })

  it('WATCH should allow transaction if watched key is not modified', async () => {
    const key = 'watchkey2'
    const watcher = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await redisClient.set(key, 'initial')
      await watcher.watch(key)

      const result = await watcher
        .multi()
        .set(key, 'transactional')
        .get(key)
        .exec()

      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0], 'OK')
      assert.strictEqual(result[1], 'transactional')
    } finally {
      watcher.destroy()
    }
  })

  it('WATCH should allow watching multiple keys in the same slot', async () => {
    const firstKey = 'watch:{multi}:3'
    const secondKey = 'watch:{multi}:4'
    const watcher = await connectToNodeRedisSlotOwner(redisClient, firstKey)

    try {
      await redisClient.set(firstKey, 'initial')
      await redisClient.set(secondKey, 'initial')

      await watcher.watch([firstKey, secondKey])

      await redisClient.set(secondKey, 'modified')

      await assert.rejects(
        () => watcher.multi().set(firstKey, 'transactional').exec(),
        WatchError,
      )
    } finally {
      watcher.destroy()
    }
  })

  it('WATCH should reject multiple keys in different slots', async () => {
    const watcher = await connectToNodeRedisSlotOwner(redisClient, 'watchkey3')
    try {
      await assert.rejects(
        () => watcher.watch(['watchkey3', 'watchkey4']),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    } finally {
      watcher.destroy()
    }
  })

  it('EXEC should clear watched keys', async () => {
    const key = 'watchkey6'
    const watcher = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await redisClient.set(key, 'initial')
      await watcher.watch(key)

      await watcher.multi().set(key, 'first').exec()

      await redisClient.set(key, 'modified')

      // Watches were cleared by the first EXEC, so this succeeds.
      const result = await watcher.multi().set(key, 'second').exec()
      assert.ok(Array.isArray(result))
    } finally {
      watcher.destroy()
    }
  })

  it('WATCH inside MULTI should return error', async () => {
    const key = 'watchkey8'
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)
    try {
      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      await assert.rejects(
        () => directClient.sendCommand(['WATCH', key]),
        errorWithMessage('ERR WATCH inside MULTI is not allowed'),
      )
      await directClient.sendCommand(['DISCARD']).catch(() => undefined)
    } finally {
      directClient.destroy()
    }
  })
})
