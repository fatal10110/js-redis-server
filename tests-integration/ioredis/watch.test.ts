import { Cluster } from 'ioredis'
import { after, before, describe, it } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()
// Unique per run: the real-backend suites share one Redis that is never
// flushed between files or between runs, so fixed literal key names collided
// with each other and with their own previous run (#420).
const RUN = randomKey()

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
      await redisClient!.set(`watchkey:${RUN}`, 'initial')

      // Watch the key
      await redisClient!.watch(`watchkey:${RUN}`)

      // Modify the key from another client
      await anotherClient.set(`watchkey:${RUN}`, 'modified')

      // Try to execute transaction
      const multi = redisClient!.multi()
      multi.set(`watchkey:${RUN}`, 'transactional')
      multi.get(`watchkey:${RUN}`)

      const result = await multi.exec()

      // Transaction should be aborted (null result)
      assert.strictEqual(result, null)

      // Verify the key has the value set by the other client
      const finalValue = await redisClient!.get(`watchkey:${RUN}`)
      assert.strictEqual(finalValue, 'modified')
    } finally {
      await anotherClient.quit()
    }
  })

  it('WATCH should allow transaction if watched key is not modified', async () => {
    // Set initial value
    await redisClient!.set(`watchkey2:${RUN}`, 'initial')

    // Watch the key
    await redisClient!.watch(`watchkey2:${RUN}`)

    // Execute transaction without modification
    const multi = redisClient!.multi()
    multi.set(`watchkey2:${RUN}`, 'transactional')
    multi.get(`watchkey2:${RUN}`)

    const result = await multi.exec()

    // Transaction should succeed
    assert.notStrictEqual(result, null)
    assert.ok(Array.isArray(result))
    assert.strictEqual(result[0][1], 'OK')
    assert.strictEqual(result[1][1], 'transactional')
  })

  it('WATCH should allow watching multiple keys in the same slot', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    // RUN sits inside the hash tag so both keys share one slot, as WATCH needs.
    const firstKey = `watch:{multi:${RUN}}:3`
    const secondKey = `watch:{multi:${RUN}}:4`

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

  it('EXEC should clear watched keys', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()

    try {
      // Set initial value
      await redisClient!.set(`watchkey6:${RUN}`, 'initial')

      // Watch the key
      await redisClient!.watch(`watchkey6:${RUN}`)

      // Execute transaction
      const multi1 = redisClient!.multi()
      multi1.set(`watchkey6:${RUN}`, 'first')
      await multi1.exec()

      // Modify the key from another client
      await anotherClient.set(`watchkey6:${RUN}`, 'modified')

      // Execute another transaction without WATCH
      const multi2 = redisClient!.multi()
      multi2.set(`watchkey6:${RUN}`, 'second')
      const result = await multi2.exec()

      // Second transaction should succeed (watches cleared after first EXEC)
      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
    } finally {
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
