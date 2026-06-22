import { RedisClientType, RedisClusterType, WatchError } from 'redis'
import { after, before, describe, it, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  randomKey,
} from '../utils'

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

  it('UNWATCH should clear watched keys', async () => {
    const key = 'watchkey5'
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)
    const mutatingClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.sendCommand(['SET', key, 'initial'])
      await directClient.sendCommand(['WATCH', key])
      await directClient.sendCommand(['UNWATCH'])

      await mutatingClient.sendCommand(['SET', key, 'modified'])

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'transactional']),
        'QUEUED',
      )
      const result = await directClient.sendCommand(['EXEC'])

      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0], 'OK')
    } finally {
      directClient.destroy()
      mutatingClient.destroy()
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

  it('DISCARD should clear watched keys', async () => {
    const key = 'watchkey7'
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)
    const mutatingClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.sendCommand(['SET', key, 'initial'])
      await directClient.sendCommand(['WATCH', key])

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'first']),
        'QUEUED',
      )
      assert.strictEqual(await directClient.sendCommand(['DISCARD']), 'OK')

      await mutatingClient.sendCommand(['SET', key, 'modified'])

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'second']),
        'QUEUED',
      )
      const result = await directClient.sendCommand(['EXEC'])

      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0], 'OK')
    } finally {
      directClient.destroy()
      mutatingClient.destroy()
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

  test('no-op collection mutations should not dirty watched keys', async () => {
    const cases: Array<{
      name: string
      initialize(client: RedisClientType, key: string): Promise<unknown>
      mutate(client: RedisClientType, key: string): Promise<unknown>
      expectedNoopReply: unknown
    }> = [
      {
        name: 'HDEL missing field',
        initialize: (c, key) => c.sendCommand(['HSET', key, 'field', 'value']),
        mutate: (c, key) => c.sendCommand(['HDEL', key, 'missing']),
        expectedNoopReply: 0,
      },
      {
        name: 'HSETNX existing field',
        initialize: (c, key) => c.sendCommand(['HSET', key, 'field', 'value']),
        mutate: (c, key) => c.sendCommand(['HSETNX', key, 'field', 'other']),
        expectedNoopReply: 0,
      },
      {
        name: 'LREM missing value',
        initialize: (c, key) => c.sendCommand(['RPUSH', key, 'one', 'two']),
        mutate: (c, key) => c.sendCommand(['LREM', key, '0', 'missing']),
        expectedNoopReply: 0,
      },
      {
        name: 'SREM missing member',
        initialize: (c, key) => c.sendCommand(['SADD', key, 'member']),
        mutate: (c, key) => c.sendCommand(['SREM', key, 'missing']),
        expectedNoopReply: 0,
      },
      {
        name: 'ZREM missing member',
        initialize: (c, key) => c.sendCommand(['ZADD', key, '1', 'member']),
        mutate: (c, key) => c.sendCommand(['ZREM', key, 'missing']),
        expectedNoopReply: 0,
      },
      {
        name: 'ZADD XX missing member',
        initialize: (c, key) => c.sendCommand(['ZADD', key, '1', 'member']),
        mutate: (c, key) => c.sendCommand(['ZADD', key, 'XX', '2', 'missing']),
        expectedNoopReply: 0,
      },
      {
        name: 'XDEL missing id',
        initialize: (c, key) =>
          c.sendCommand(['XADD', key, '1-0', 'field', 'value']),
        mutate: (c, key) => c.sendCommand(['XDEL', key, '2-0']),
        expectedNoopReply: 0,
      },
      {
        name: 'XTRIM no removed entries',
        initialize: (c, key) =>
          c.sendCommand(['XADD', key, '1-0', 'field', 'value']),
        mutate: (c, key) => c.sendCommand(['XTRIM', key, 'MAXLEN', '10']),
        expectedNoopReply: 0,
      },
    ]

    for (const item of cases) {
      const key = `watch:{noop:${randomKey()}}`
      const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      try {
        await directClient.del(key)
        await item.initialize(directClient, key)

        assert.strictEqual(await directClient.sendCommand(['WATCH', key]), 'OK')
        assert.strictEqual(
          await item.mutate(directClient, key),
          item.expectedNoopReply,
          item.name,
        )

        assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
        assert.strictEqual(
          await directClient.sendCommand(['SET', key, 'after']),
          'QUEUED',
        )

        const result = await directClient.sendCommand(['EXEC'])
        assert.deepStrictEqual(result, ['OK'], item.name)
      } finally {
        await directClient.sendCommand(['UNWATCH']).catch(() => undefined)
        await directClient.del(key).catch(() => undefined)
        directClient.destroy()
      }
    }
  })

  test('RENAME key key self-rename is a no-op and does not dirty a watched key', async () => {
    const key = `watch:{selfrename:${randomKey()}}`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.del(key)
      await directClient.sendCommand(['SET', key, 'value'])

      assert.strictEqual(await directClient.sendCommand(['WATCH', key]), 'OK')

      assert.strictEqual(
        await directClient.sendCommand(['RENAME', key, key]),
        'OK',
      )
      assert.strictEqual(await directClient.sendCommand(['GET', key]), 'value')

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['SET', key, 'after']),
        'QUEUED',
      )

      const result = await directClient.sendCommand(['EXEC'])
      assert.deepStrictEqual(result, ['OK'])
      assert.strictEqual(await directClient.sendCommand(['GET', key]), 'after')
    } finally {
      await directClient.sendCommand(['UNWATCH']).catch(() => undefined)
      await directClient.del(key).catch(() => undefined)
      directClient.destroy()
    }
  })

  test('RENAME key key on a missing key still errors with no such key', async () => {
    const key = `watch:{selfrename:missing:${randomKey()}}`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.del(key)
      await assert.rejects(
        () => directClient.sendCommand(['RENAME', key, key]),
        errorWithMessage('ERR no such key'),
      )
    } finally {
      directClient.destroy()
    }
  })

  test('RENAME with wrong number of arguments errors', async () => {
    const key = `watch:{selfrename:arity:${randomKey()}}`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await assert.rejects(
        () => directClient.sendCommand(['RENAME', key]),
        errorWithMessage("ERR wrong number of arguments for 'rename' command"),
      )
    } finally {
      directClient.destroy()
    }
  })

  test('identical store writes should dirty watched destination keys', async () => {
    const cases: Array<{
      name: string
      initialize(client: RedisClientType, key: string): Promise<unknown>
      store(client: RedisClientType, key: string): Promise<unknown>
      expectedStoreReply: unknown
    }> = [
      {
        name: 'SINTERSTORE identical set',
        initialize: async (c, key) => {
          await c.sendCommand(['SADD', `${key}:source`, 'a', 'b', 'c'])
          await c.sendCommand(['SINTERSTORE', key, `${key}:source`])
        },
        store: (c, key) => c.sendCommand(['SINTERSTORE', key, `${key}:source`]),
        expectedStoreReply: 3,
      },
      {
        name: 'ZINTERSTORE identical sorted set',
        initialize: async (c, key) => {
          await c.sendCommand(['ZADD', `${key}:source`, '1', 'a', '2', 'b'])
          await c.sendCommand(['ZINTERSTORE', key, '1', `${key}:source`])
        },
        store: (c, key) =>
          c.sendCommand(['ZINTERSTORE', key, '1', `${key}:source`]),
        expectedStoreReply: 2,
      },
    ]

    for (const item of cases) {
      const key = `watch:{store:${randomKey()}}`
      const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      try {
        await directClient.del([key, `${key}:source`])
        await item.initialize(directClient, key)

        assert.strictEqual(await directClient.sendCommand(['WATCH', key]), 'OK')
        assert.strictEqual(
          await item.store(directClient, key),
          item.expectedStoreReply,
          item.name,
        )

        assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
        assert.strictEqual(
          await directClient.sendCommand(['SET', key, 'after']),
          'QUEUED',
        )

        const result = await directClient.sendCommand(['EXEC'])
        assert.strictEqual(result, null, item.name)
      } finally {
        await directClient.sendCommand(['UNWATCH']).catch(() => undefined)
        await directClient.del([key, `${key}:source`]).catch(() => undefined)
        directClient.destroy()
      }
    }
  })
})
