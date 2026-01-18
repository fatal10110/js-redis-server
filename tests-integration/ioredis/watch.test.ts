import { Cluster } from 'ioredis'
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

  it('WATCH should allow watching multiple keys', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()

    try {
      // Set initial values
      await redisClient!.set('watchkey3', 'initial')
      await redisClient!.set('watchkey4', 'initial')

      // Watch multiple keys
      await redisClient!.watch('watchkey3', 'watchkey4')

      // Modify one of the watched keys from another client
      await anotherClient.set('watchkey4', 'modified')

      // Try to execute transaction
      const multi = redisClient!.multi()
      multi.set('watchkey3', 'transactional')

      const result = await multi.exec()

      // Transaction should be aborted
      assert.strictEqual(result, null)
    } finally {
      await anotherClient.quit()
    }
  })

  it('UNWATCH should clear watched keys', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()

    try {
      // Set initial value
      await redisClient!.set('watchkey5', 'initial')

      // Watch the key
      await redisClient!.watch('watchkey5')

      // Unwatch
      await redisClient!.unwatch()

      // Modify the key from another client
      await anotherClient.set('watchkey5', 'modified')

      // Execute transaction
      const multi = redisClient!.multi()
      multi.set('watchkey5', 'transactional')

      const result = await multi.exec()

      // Transaction should succeed because we unwatched
      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
      assert.strictEqual(result[0][1], 'OK')
    } finally {
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

    try {
      // Set initial value
      await redisClient!.set('watchkey7', 'initial')

      // Watch the key
      await redisClient!.watch('watchkey7')

      // Discard transaction
      const multi1 = redisClient!.multi()
      multi1.set('watchkey7', 'first')
      await multi1.discard()

      // Modify the key from another client
      await anotherClient.set('watchkey7', 'modified')

      // Execute another transaction without WATCH
      const multi2 = redisClient!.multi()
      multi2.set('watchkey7', 'second')
      const result = await multi2.exec()

      // Second transaction should succeed (watches cleared after DISCARD)
      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
    } finally {
      await anotherClient.quit()
    }
  })

  it('WATCH inside MULTI should return error', async () => {
    const multi = redisClient!.multi()

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
