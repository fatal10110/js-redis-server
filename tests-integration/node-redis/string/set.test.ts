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
    await redisClient.set('testkey', 'testvalue')
    const value = await redisClient.get('testkey')
    assert.strictEqual(value, 'testvalue')
  })

  test('SET with EX option', async () => {
    await redisClient.set('exkey', 'exvalue', {
      expiration: { type: 'EX', value: 10 },
    })
    const value = await redisClient.get('exkey')
    assert.strictEqual(value, 'exvalue')

    const ttl = await redisClient.ttl('exkey')
    assert.ok(ttl > 0 && ttl <= 10)
  })

  test('SET with PX option', async () => {
    await redisClient.set('pxkey', 'pxvalue', {
      expiration: { type: 'PX', value: 5000 },
    })
    const value = await redisClient.get('pxkey')
    assert.strictEqual(value, 'pxvalue')

    const ttl = await redisClient.pTTL('pxkey')
    assert.ok(ttl > 0 && ttl <= 5000)
  })

  test('SET with NX option - key does not exist', async () => {
    const result = await redisClient.set('nxkey1', 'nxvalue', {
      condition: 'NX',
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get('nxkey1')
    assert.strictEqual(value, 'nxvalue')
  })

  test('SET with NX option - key exists', async () => {
    await redisClient.set('nxkey2', 'existing')
    const result = await redisClient.set('nxkey2', 'newvalue', {
      condition: 'NX',
    })
    assert.strictEqual(result, null)

    const value = await redisClient.get('nxkey2')
    assert.strictEqual(value, 'existing')
  })

  test('SET with XX option - key exists', async () => {
    await redisClient.set('xxkey1', 'existing')
    const result = await redisClient.set('xxkey1', 'newvalue', {
      condition: 'XX',
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get('xxkey1')
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with XX option - key does not exist', async () => {
    const result = await redisClient.set('xxkey2', 'newvalue', {
      condition: 'XX',
    })
    assert.strictEqual(result, null)

    const value = await redisClient.get('xxkey2')
    assert.strictEqual(value, null)
  })

  test('SET with GET option', async () => {
    await redisClient.set('getkey', 'oldvalue')

    const result = await redisClient.set('getkey', 'newvalue', { GET: true })
    assert.strictEqual(result, 'oldvalue')

    const value = await redisClient.get('getkey')
    assert.strictEqual(value, 'newvalue')
  })

  test('SET with multiple options', async () => {
    await redisClient.set('multikey', 'existing')

    // XX with EX
    const result = await redisClient.set('multikey', 'newvalue', {
      condition: 'XX',
      expiration: { type: 'EX', value: 5 },
    })
    assert.strictEqual(result, 'OK')

    const value = await redisClient.get('multikey')
    assert.strictEqual(value, 'newvalue')

    const ttl = await redisClient.ttl('multikey')
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
