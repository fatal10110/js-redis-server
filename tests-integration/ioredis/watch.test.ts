import { Cluster, Redis } from 'ioredis'
import clusterKeySlot from 'cluster-key-slot'
import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'

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
      err => {
        assert.ok(err instanceof Error)
        assert.match(
          err.message,
          /CROSSSLOT Keys in request don't hash to the same slot/,
        )
        return true
      },
    )
  })

  it('UNWATCH should clear watched keys', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    const key = 'watchkey5'
    let directClient: Redis | undefined

    try {
      directClient = await connectToSlotOwner(redisClient!, key)

      // Set initial value
      await directClient.call('SET', key, 'initial')

      // Watch the key
      await directClient.call('WATCH', key)

      // Unwatch
      await directClient.call('UNWATCH')

      // Modify the key from another client
      await anotherClient.set(key, 'modified')

      // Execute transaction
      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(
        await directClient.call('SET', key, 'transactional'),
        'QUEUED',
      )
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
      await directClient.call('SET', key, 'initial')

      // Watch the key
      await directClient.call('WATCH', key)

      // Discard transaction
      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(await directClient.call('SET', key, 'first'), 'QUEUED')
      assert.strictEqual(await directClient.call('DISCARD'), 'OK')

      // Modify the key from another client
      await anotherClient.set(key, 'modified')

      // Execute another transaction without WATCH
      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(
        await directClient.call('SET', key, 'second'),
        'QUEUED',
      )
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
})

async function connectToSlotOwner(
  cluster: Cluster,
  key: string,
): Promise<Redis> {
  const slot = clusterKeySlot(key)
  const slots = (await cluster.cluster('SLOTS')) as Array<
    [number, number, [string, number]]
  >

  for (const [min, max, master] of slots) {
    if (slot < min || slot > max) {
      continue
    }

    const client = new Redis({
      host: master[0],
      port: master[1],
      lazyConnect: true,
    })
    await client.connect()
    return client
  }

  throw new Error(`No Redis Cluster slot owner found for slot ${slot}`)
}
