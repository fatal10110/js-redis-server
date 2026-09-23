import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()
// Unique per run: the real-backend suites share one Redis that is never
// flushed between files or between runs, so fixed literal key names collided
// with each other and with their own previous run (#420).
const RUN = randomKey()

describe(`String Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('basic SET and GET', async () => {
    await redisClient.set(`testkey:${RUN}`, 'testvalue')
    const value = await redisClient.get(`testkey:${RUN}`)
    assert.strictEqual(value, 'testvalue')
  })

  test('SET with EX option', async () => {
    await redisClient.set(`exkey:${RUN}`, 'exvalue', {
      expiration: { type: 'EX', value: 10 },
    })
    const value = await redisClient.get(`exkey:${RUN}`)
    assert.strictEqual(value, 'exvalue')

    const ttl = await redisClient.ttl(`exkey:${RUN}`)
    assert.ok(ttl > 0 && ttl <= 10)
  })

  test('SET with PX option', async () => {
    await redisClient.set(`pxkey:${RUN}`, 'pxvalue', {
      expiration: { type: 'PX', value: 5000 },
    })
    const value = await redisClient.get(`pxkey:${RUN}`)
    assert.strictEqual(value, 'pxvalue')

    const ttl = await redisClient.pTTL(`pxkey:${RUN}`)
    assert.ok(ttl > 0 && ttl <= 5000)
  })

  test('SET with NX option - key does not exist', async () => {
    const result = await redisClient.set(`nxkey1:${RUN}`, 'nxvalue', {
      condition: 'NX',
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get(`nxkey1:${RUN}`)
    assert.strictEqual(value, 'nxvalue')
  })

  test('SET with NX option - key exists', async () => {
    await redisClient.set(`nxkey2:${RUN}`, 'existing')
    const result = await redisClient.set(`nxkey2:${RUN}`, 'newvalue', {
      condition: 'NX',
    })
    assert.strictEqual(result, null)

    const value = await redisClient.get(`nxkey2:${RUN}`)
    assert.strictEqual(value, 'existing')
  })

  test('SET with XX option - key exists', async () => {
    await redisClient.set(`xxkey1:${RUN}`, 'existing')
    const result = await redisClient.set(`xxkey1:${RUN}`, 'newvalue', {
      condition: 'XX',
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get(`xxkey1:${RUN}`)
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with XX option - key does not exist', async () => {
    const result = await redisClient.set(`xxkey2:${RUN}`, 'newvalue', {
      condition: 'XX',
    })
    assert.strictEqual(result, null)

    const value = await redisClient.get(`xxkey2:${RUN}`)
    assert.strictEqual(value, null)
  })

  test('SET with GET option', async () => {
    await redisClient.set(`getkey:${RUN}`, 'oldvalue')

    const result = await redisClient.set(`getkey:${RUN}`, 'newvalue', {
      GET: true,
    })
    assert.strictEqual(result, 'oldvalue')

    const value = await redisClient.get(`getkey:${RUN}`)
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with multiple options', async () => {
    await redisClient.set(`multikey:${RUN}`, 'existing')

    // XX with EX
    const result = await redisClient.set(`multikey:${RUN}`, 'newvalue', {
      condition: 'XX',
      expiration: { type: 'EX', value: 5 },
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get(`multikey:${RUN}`)
    assert.strictEqual(value, 'newvalue')

    const ttl = await redisClient.ttl(`multikey:${RUN}`)
    assert.ok(ttl > 0 && ttl <= 5)
  })

  test('SET KEEPTTL preserves the existing expiration', async () => {
    const key = `{set-keepttl:${randomKey()}}:key`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await directClient.set(key, 'ttl', {
        expiration: { type: 'PX', value: 5000 },
      })
      const originalTtl = await directClient.pTTL(key)
      assert.ok(originalTtl > 0 && originalTtl <= 5000)

      assert.strictEqual(
        await directClient.set(key, 'kept', { expiration: 'KEEPTTL' }),
        'OK',
      )
      assert.strictEqual(await directClient.get(key), 'kept')

      const keptTtl = await directClient.pTTL(key)
      assert.ok(keptTtl > 0 && keptTtl <= originalTtl)
    } finally {
      await directClient.del(key)
      directClient.destroy()
    }
  })

  test('SET and GET wrong-type and syntax errors match Redis', async () => {
    const tag = `{set-errors:${randomKey()}}`
    const listKey = `${tag}:list`
    const stringKey = `${tag}:string`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, listKey)

    try {
      await directClient.lPush(listKey, 'value')

      await assert.rejects(
        () => directClient.get(listKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient.set(listKey, 'value', { GET: true }),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient.sendCommand(['SET', stringKey, 'value', 'NX', 'XX']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () => directClient.set(stringKey, 'value', { EX: 0 }),
        errorWithMessage("ERR invalid expire time in 'set' command"),
      )
    } finally {
      await directClient.del([listKey, stringKey])
      directClient.destroy()
    }
  })
})
