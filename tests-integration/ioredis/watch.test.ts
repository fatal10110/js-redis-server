import { Cluster, Redis } from 'ioredis'
import { after, before, describe, it, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe('WATCH/UNWATCH', () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  it('WATCH should abort transaction if watched key is modified', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()

    try {
      // Set initial value
      await redisClient!.set('watchkey', 'initial')

      // Watch the key
      await redisClient!.watch('watchkey')

      // Modify the key from another client
      await anotherClient.set('watchkey', 'modified')

      // Try to execute transaction
      const multi = redisClient!.multi()
      multi.set('watchkey', 'transactional')
      multi.get('watchkey')

      const result = await multi.exec()

      // Transaction should be aborted (null result)
      assert.strictEqual(result, null)

      // Verify the key has the value set by the other client
      const finalValue = await redisClient!.get('watchkey')
      assert.strictEqual(finalValue, 'modified')
    } finally {
      await anotherClient.quit()
    }
  })

  it('WATCH should allow transaction if watched key is not modified', async () => {
    // Set initial value
    await redisClient!.set('watchkey2', 'initial')

    // Watch the key
    await redisClient!.watch('watchkey2')

    // Execute transaction without modification
    const multi = redisClient!.multi()
    multi.set('watchkey2', 'transactional')
    multi.get('watchkey2')

    const result = await multi.exec()

    // Transaction should succeed
    assert.notStrictEqual(result, null)
    assert.ok(Array.isArray(result))
    assert.strictEqual(result[0][1], 'OK')
    assert.strictEqual(result[1][1], 'transactional')
  })

  it('WATCH should allow watching multiple keys in the same slot', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    const firstKey = 'watch:{multi}:3'
    const secondKey = 'watch:{multi}:4'

    try {
      // Set initial values
      await redisClient!.set(firstKey, 'initial')
      await redisClient!.set(secondKey, 'initial')

      // Watch multiple keys
      await redisClient!.watch(firstKey, secondKey)

      // Modify one of the watched keys from another client
      await anotherClient.set(secondKey, 'modified')

      // Try to execute transaction
      const multi = redisClient!.multi()
      multi.set(firstKey, 'transactional')

      const result = await multi.exec()

      // Transaction should be aborted
      assert.strictEqual(result, null)
    } finally {
      await anotherClient.quit()
    }
  })

  it('WATCH should reject multiple keys in different slots', async () => {
    await assert.rejects(
      () => redisClient!.watch('watchkey3', 'watchkey4'),
      errorWithMessage("CROSSSLOT Keys in request don't hash to the same slot"),
    )
  })

  it('UNWATCH should clear watched keys', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    const key = 'watchkey5'
    let directClient: Redis | undefined

    try {
      directClient = await connectToSlotOwner(redisClient!, key)

      // Set initial value
      await directClient.set(key, 'initial')

      // Watch the key
      await directClient.call('WATCH', key)

      // Unwatch
      await directClient.call('UNWATCH')

      // Modify the key from another client
      await anotherClient.set(key, 'modified')

      // Execute transaction
      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.set(key, 'transactional'), 'QUEUED')
      const result = await directClient.call('EXEC')

      // Transaction should succeed because we unwatched
      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0], 'OK')
    } finally {
      directClient?.disconnect()
      await anotherClient.quit()
    }
  })

  it('EXEC should clear watched keys', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()

    try {
      // Set initial value
      await redisClient!.set('watchkey6', 'initial')

      // Watch the key
      await redisClient!.watch('watchkey6')

      // Execute transaction
      const multi1 = redisClient!.multi()
      multi1.set('watchkey6', 'first')
      await multi1.exec()

      // Modify the key from another client
      await anotherClient.set('watchkey6', 'modified')

      // Execute another transaction without WATCH
      const multi2 = redisClient!.multi()
      multi2.set('watchkey6', 'second')
      const result = await multi2.exec()

      // Second transaction should succeed (watches cleared after first EXEC)
      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
    } finally {
      await anotherClient.quit()
    }
  })

  it('DISCARD should clear watched keys', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    const key = 'watchkey7'
    let directClient: Redis | undefined

    try {
      directClient = await connectToSlotOwner(redisClient!, key)

      // Set initial value
      await directClient.set(key, 'initial')

      // Watch the key
      await directClient.call('WATCH', key)

      // Discard transaction
      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.set(key, 'first'), 'QUEUED')
      assert.strictEqual(await directClient.call('DISCARD'), 'OK')

      // Modify the key from another client
      await anotherClient.set(key, 'modified')

      // Execute another transaction without WATCH
      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.set(key, 'second'), 'QUEUED')
      const result = await directClient.call('EXEC')

      // Second transaction should succeed (watches cleared after DISCARD)
      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0], 'OK')
    } finally {
      directClient?.disconnect()
      await anotherClient.quit()
    }
  })

  it('WATCH inside MULTI should return error', async () => {
    // WATCH inside MULTI should fail
    // ioredis will throw when calling watch() on a pipeline/multi
    try {
      // This is not valid in Redis protocol
      await redisClient!.multi().watch('key').exec()
      assert.fail('Should have thrown error')
    } catch (err) {
      // Expected to fail
      assert.ok(err)
    }
  })

  test('no-op collection mutations should not dirty watched keys', async () => {
    const cases: Array<{
      name: string
      initialize(client: Redis, key: string): Promise<unknown>
      mutate(client: Redis, key: string): Promise<unknown>
      expectedNoopReply: unknown
    }> = [
      {
        name: 'HDEL missing field',
        initialize: (client, key) => client.hset(key, 'field', 'value'),
        mutate: (client, key) => client.hdel(key, 'missing'),
        expectedNoopReply: 0,
      },
      {
        name: 'HSETNX existing field',
        initialize: (client, key) => client.hset(key, 'field', 'value'),
        mutate: (client, key) => client.hsetnx(key, 'field', 'other'),
        expectedNoopReply: 0,
      },
      {
        name: 'LREM missing value',
        initialize: (client, key) => client.rpush(key, 'one', 'two'),
        mutate: (client, key) => client.lrem(key, 0, 'missing'),
        expectedNoopReply: 0,
      },
      {
        name: 'SREM missing member',
        initialize: (client, key) => client.sadd(key, 'member'),
        mutate: (client, key) => client.srem(key, 'missing'),
        expectedNoopReply: 0,
      },
      {
        name: 'ZREM missing member',
        initialize: (client, key) => client.zadd(key, 1, 'member'),
        mutate: (client, key) => client.zrem(key, 'missing'),
        expectedNoopReply: 0,
      },
      {
        name: 'ZADD XX missing member',
        initialize: (client, key) => client.zadd(key, 1, 'member'),
        mutate: (client, key) => client.zadd(key, 'XX', 2, 'missing'),
        expectedNoopReply: 0,
      },
      {
        name: 'XDEL missing id',
        initialize: (client, key) => client.xadd(key, '1-0', 'field', 'value'),
        mutate: (client, key) => client.xdel(key, '2-0'),
        expectedNoopReply: 0,
      },
      {
        name: 'XTRIM no removed entries',
        initialize: (client, key) => client.xadd(key, '1-0', 'field', 'value'),
        mutate: (client, key) => client.xtrim(key, 'MAXLEN', 10),
        expectedNoopReply: 0,
      },
    ]

    for (const item of cases) {
      const key = `watch:{noop:${randomKey()}}`
      const directClient = await connectToSlotOwner(redisClient!, key)

      try {
        await directClient.del(key)
        await item.initialize(directClient, key)

        assert.strictEqual(await directClient.call('WATCH', key), 'OK')
        assert.strictEqual(
          await item.mutate(directClient, key),
          item.expectedNoopReply,
          item.name,
        )

        assert.strictEqual(await directClient.call('MULTI'), 'OK')
        assert.strictEqual(await directClient.set(key, 'after'), 'QUEUED')

        const result = await directClient.call('EXEC')
        assert.deepStrictEqual(result, ['OK'], item.name)
      } finally {
        await directClient.call('UNWATCH').catch(() => undefined)
        await directClient.del(key).catch(() => undefined)
        directClient.disconnect()
      }
    }
  })

  test('RENAME key key self-rename is a no-op and does not dirty a watched key', async () => {
    const key = `watch:{selfrename:${randomKey()}}`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.del(key)
      await directClient.set(key, 'value')

      assert.strictEqual(await directClient.call('WATCH', key), 'OK')

      // Renaming a key to itself is a true no-op: +OK, value preserved,
      // and no keyspace mutation that would invalidate the WATCH.
      assert.strictEqual(await directClient.rename(key, key), 'OK')
      assert.strictEqual(await directClient.get(key), 'value')

      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.set(key, 'after'), 'QUEUED')

      // Transaction must NOT be aborted by the self-rename.
      const result = await directClient.call('EXEC')
      assert.deepStrictEqual(result, ['OK'])
      assert.strictEqual(await directClient.get(key), 'after')
    } finally {
      await directClient.call('UNWATCH').catch(() => undefined)
      await directClient.del(key).catch(() => undefined)
      directClient.disconnect()
    }
  })

  test('RENAME key key on a missing key still errors with no such key', async () => {
    const key = `watch:{selfrename:missing:${randomKey()}}`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.del(key)
      await assert.rejects(
        () => directClient.call('RENAME', key, key),
        errorWithMessage('ERR no such key'),
      )
    } finally {
      directClient.disconnect()
    }
  })

  test('RENAME with wrong number of arguments errors', async () => {
    const key = `watch:{selfrename:arity:${randomKey()}}`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await assert.rejects(
        () => directClient.call('RENAME', key),
        errorWithMessage("ERR wrong number of arguments for 'rename' command"),
      )
    } finally {
      directClient.disconnect()
    }
  })

  test('identical store writes should dirty watched destination keys', async () => {
    const cases: Array<{
      name: string
      initialize(client: Redis, key: string): Promise<unknown>
      store(client: Redis, key: string): Promise<unknown>
      expectedStoreReply: unknown
    }> = [
      {
        name: 'SINTERSTORE identical set',
        initialize: async (client, key) => {
          await client.sadd(`${key}:source`, 'a', 'b', 'c')
          await client.sinterstore(key, `${key}:source`)
        },
        store: (client, key) => client.sinterstore(key, `${key}:source`),
        expectedStoreReply: 3,
      },
      {
        name: 'ZINTERSTORE identical sorted set',
        initialize: async (client, key) => {
          await client.zadd(`${key}:source`, 1, 'a', 2, 'b')
          await client.zinterstore(key, 1, `${key}:source`)
        },
        store: (client, key) => client.zinterstore(key, 1, `${key}:source`),
        expectedStoreReply: 2,
      },
    ]

    for (const item of cases) {
      const key = `watch:{store:${randomKey()}}`
      const directClient = await connectToSlotOwner(redisClient!, key)

      try {
        await directClient.del(key, `${key}:source`)
        await item.initialize(directClient, key)

        assert.strictEqual(await directClient.call('WATCH', key), 'OK')
        assert.strictEqual(
          await item.store(directClient, key),
          item.expectedStoreReply,
          item.name,
        )

        assert.strictEqual(await directClient.call('MULTI'), 'OK')
        assert.strictEqual(await directClient.set(key, 'after'), 'QUEUED')

        const result = await directClient.call('EXEC')
        assert.strictEqual(result, null, item.name)
      } finally {
        await directClient.call('UNWATCH').catch(() => undefined)
        await directClient.del(key, `${key}:source`).catch(() => undefined)
        directClient.disconnect()
      }
    }
  })
})
