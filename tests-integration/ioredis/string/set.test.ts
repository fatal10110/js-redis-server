import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()
// Unique per run: the real-backend suites share one Redis that is never
// flushed between files or between runs, so fixed literal key names collided
// with each other and with their own previous run (#420).
const RUN = randomKey()

describe(`String Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('string-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('basic SET and GET', async () => {
    await redisClient?.set(`testkey:${RUN}`, 'testvalue')
    const value = await redisClient?.get(`testkey:${RUN}`)
    assert.strictEqual(value, 'testvalue')
  })

  test('SET with EX option', async () => {
    await redisClient?.set(`exkey:${RUN}`, 'exvalue', 'EX', 10)
    const value = await redisClient?.get(`exkey:${RUN}`)
    assert.strictEqual(value, 'exvalue')

    const ttl = await redisClient?.ttl(`exkey:${RUN}`)
    assert.ok(ttl !== undefined && ttl > 0 && ttl <= 10)
  })

  test('SET with PX option', async () => {
    await redisClient?.set(`pxkey:${RUN}`, 'pxvalue', 'PX', 5000)
    const value = await redisClient?.get(`pxkey:${RUN}`)
    assert.strictEqual(value, 'pxvalue')

    const ttl = await redisClient?.pttl(`pxkey:${RUN}`)
    assert.ok(ttl !== undefined && ttl > 0 && ttl <= 5000)
  })

  test('SET with NX option - key does not exist', async () => {
    const result = await redisClient?.set(`nxkey1:${RUN}`, 'nxvalue', 'NX')
    assert.strictEqual(result, 'OK')

    const value = await redisClient?.get(`nxkey1:${RUN}`)
    assert.strictEqual(value, 'nxvalue')
  })

  test('SET with NX option - key exists', async () => {
    await redisClient?.set(`nxkey2:${RUN}`, 'existing')
    const result = await redisClient?.set(`nxkey2:${RUN}`, 'newvalue', 'NX')
    assert.strictEqual(result, null)

    const value = await redisClient?.get(`nxkey2:${RUN}`)
    assert.strictEqual(value, 'existing')
  })

  test('SET with XX option - key exists', async () => {
    await redisClient?.set(`xxkey1:${RUN}`, 'existing')
    const result = await redisClient?.set(`xxkey1:${RUN}`, 'newvalue', 'XX')
    assert.strictEqual(result, 'OK')

    const value = await redisClient?.get(`xxkey1:${RUN}`)
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with XX option - key does not exist', async () => {
    const result = await redisClient?.set(`xxkey2:${RUN}`, 'newvalue', 'XX')
    assert.strictEqual(result, null)

    const value = await redisClient?.get(`xxkey2:${RUN}`)
    assert.strictEqual(value, null)
  })

  test('SET with GET option', async () => {
    await redisClient?.set(`getkey:${RUN}`, 'oldvalue')

    // ioredis doesn't directly support GET option, but we can test with eval
    const result = await redisClient?.eval(
      `return redis.call('set', KEYS[1], ARGV[1], 'GET')`,
      1,
      `getkey:${RUN}`,
      'newvalue',
    )

    assert.strictEqual(result, 'oldvalue')

    const value = await redisClient?.get(`getkey:${RUN}`)
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with multiple options', async () => {
    await redisClient?.set(`multikey:${RUN}`, 'existing')

    // Test XX with EX
    const result = await redisClient?.eval(
      `return redis.call('set', KEYS[1], ARGV[1], 'XX', 'EX', ARGV[2])`,
      1,
      `multikey:${RUN}`,
      'newvalue',
      '5',
    )

    assert.strictEqual(result, 'OK')

    const value = await redisClient?.get(`multikey:${RUN}`)
    assert.strictEqual(value, 'newvalue')

    const ttl = await redisClient?.ttl(`multikey:${RUN}`)
    assert.ok(ttl !== undefined && ttl > 0 && ttl <= 5)
  })

  test('SET KEEPTTL preserves the existing expiration', async () => {
    const key = `{set-keepttl:${randomKey()}}:key`
    const directClient = await connectToSlotOwner(redisClient!, key)

    try {
      await directClient.set(key, 'ttl', 'PX', 5000)
      const originalTtl = await directClient.pttl(key)
      assert.ok(originalTtl > 0 && originalTtl <= 5000)

      assert.strictEqual(await directClient.set(key, 'kept', 'KEEPTTL'), 'OK')
      assert.strictEqual(await directClient.get(key), 'kept')

      const keptTtl = await directClient.pttl(key)
      assert.ok(keptTtl > 0 && keptTtl <= originalTtl)
    } finally {
      await directClient.del(key)
      directClient.disconnect()
    }
  })

  test('SET and GET wrong-type and syntax errors match Redis', async () => {
    const tag = `{set-errors:${randomKey()}}`
    const listKey = `${tag}:list`
    const stringKey = `${tag}:string`
    const directClient = await connectToSlotOwner(redisClient!, listKey)

    try {
      await directClient.lpush(listKey, 'value')

      await assert.rejects(
        () => directClient.get(listKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient.set(listKey, 'value', 'GET'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient.set(stringKey, 'value', 'NX', 'XX'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => directClient.set(stringKey, 'value', 'EX', '0'),
        errorWithMessage("ERR invalid expire time in 'set' command"),
      )
    } finally {
      await directClient.del(listKey, stringKey)
      directClient.disconnect()
    }
  })
})
