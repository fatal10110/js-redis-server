import { Cluster } from 'ioredis'
import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../../test-config'
import { randomKey } from '../../utils'

const testRunner = new TestRunner()
// Unique per run: the real-backend suites share one Redis that is never
// flushed between files or between runs, so fixed literal key names collided
// with each other and with their own previous run (#420, #453).
const RUN = randomKey()

// Issue #124: the "delete a key when its collection becomes empty" and
// "don't create a ghost key for a no-op mutation" rules used to live in each
// command (HDEL/SREM/... pre-check existence and self-delete) rather than in
// RedisDatabase.update(). The root-cause fix centralizes both rules in
// update(). These wire-level tests guard that the centralized behavior matches
// real Redis end-to-end: no-op mutations on a missing key don't disturb a
// WATCH, emptying a key removes it, and a real emptying still dirties a WATCH.
// (The latent update() footguns themselves — ghost entry on mutator throw,
// spurious write event — are covered directly in tests/keyspace-update.test.ts,
// since every shipped command currently masks them.)
describe('Empty-collection cleanup / no-op mutations (#124)', () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('no-op HDEL on a non-existent key must not invalidate a WATCH on that key', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    const key = `ghost:hdel:${RUN}`

    try {
      // Ensure the key does not exist.
      await redisClient!.del(key)

      // Watch the (non-existent) key.
      await redisClient!.watch(key)

      // No-op mutation from another client: HDEL on a missing key removes
      // nothing and must not touch the keyspace.
      assert.strictEqual(await anotherClient.hdel(key, 'field'), 0)

      // The watched key never changed, so the transaction must run.
      const multi = redisClient!.multi()
      multi.set(key, 'value')
      const result = await multi.exec()

      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
      assert.strictEqual(result![0][1], 'OK')
    } finally {
      await anotherClient.quit()
    }
  })

  test('no-op SREM on a non-existent key must not invalidate a WATCH on that key', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    const key = `ghost:srem:${RUN}`

    try {
      await redisClient!.del(key)
      await redisClient!.watch(key)

      assert.strictEqual(await anotherClient.srem(key, 'member'), 0)

      const multi = redisClient!.multi()
      multi.set(key, 'value')
      const result = await multi.exec()

      assert.notStrictEqual(result, null)
      assert.ok(Array.isArray(result))
      assert.strictEqual(result![0][1], 'OK')
    } finally {
      await anotherClient.quit()
    }
  })

  test('emptying a hash via HDEL deletes the key (no phantom empty hash persists)', async () => {
    const key = `cleanup:hash:${RUN}`

    await redisClient!.del(key)
    await redisClient!.hset(key, 'f', 'v')
    assert.strictEqual(await redisClient!.exists(key), 1)

    await redisClient!.hdel(key, 'f')

    assert.strictEqual(await redisClient!.exists(key), 0)
    assert.strictEqual(await redisClient!.type(key), 'none')
  })

  test('emptying an EXISTING watched collection still invalidates the WATCH', async () => {
    const anotherClient = await testRunner.setupIoredisCluster()
    const key = `cleanup:watch:existing:${RUN}`

    try {
      await redisClient!.del(key)
      await redisClient!.sadd(key, 'm') // key now exists as a set
      await redisClient!.watch(key)

      // Removing the last member empties (and thus deletes) the key — a real
      // mutation that must still dirty the WATCH.
      assert.strictEqual(await anotherClient.srem(key, 'm'), 1)

      const multi = redisClient!.multi()
      multi.set(key, 'value')
      const result = await multi.exec()

      assert.strictEqual(result, null)
    } finally {
      await anotherClient.quit()
    }
  })
})
