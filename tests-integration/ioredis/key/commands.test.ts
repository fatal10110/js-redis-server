import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import {
  assertDbSizeDelta,
  connectToSlotOwner,
  errorWithMessage,
  getTotalDbSize,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

describe(`Key Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('key-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('EXISTS command', async () => {
    // Set up test data
    await redisClient?.set('{test}string_key', 'value')
    await redisClient?.hset('{test}hash_key', 'field', 'value')
    await redisClient?.lpush('{test}list_key', 'item')
    await redisClient?.sadd('{test}set_key', 'member')
    await redisClient?.zadd('{test}zset_key', 1, 'member')

    // Test single key existence
    const exists1 = await redisClient?.exists('{test}string_key')
    assert.strictEqual(exists1, 1)

    const exists2 = await redisClient?.exists('{test}nonexistent')
    assert.strictEqual(exists2, 0)

    // Test multiple keys existence
    const existsMultiple = await redisClient?.exists(
      '{test}string_key',
      '{test}hash_key',
      '{test}nonexistent',
      '{test}list_key',
    )
    assert.strictEqual(existsMultiple, 3) // 3 out of 4 keys exist
  })

  test('TOUCH command counts live keys without mutating keyspace', async () => {
    const tag = `{touch:${randomKey()}}`
    const stringKey = `${tag}:string`
    const hashKey = `${tag}:hash`
    const expiringKey = `${tag}:expired`
    const missingKey = `${tag}:missing`
    const crossSlotA = `{touch-cross-a:${randomKey()}}:key`
    const crossSlotB = `{touch-cross-b:${randomKey()}}:key`
    const directClient = await connectToSlotOwner(redisClient!, stringKey)

    try {
      await directClient.set(stringKey, 'value')
      await directClient.hset(hashKey, 'field', 'value')
      await directClient.set(expiringKey, 'value', 'PX', 1)
      await new Promise(resolve => setTimeout(resolve, 20))

      assert.strictEqual(
        await directClient.touch(stringKey, hashKey, missingKey, expiringKey),
        2,
      )
      assert.strictEqual(
        await directClient.exists(stringKey, hashKey, expiringKey),
        2,
      )
      assert.strictEqual(await directClient.type(hashKey), 'hash')
      assert.strictEqual(await directClient.touch(missingKey), 0)

      await assert.rejects(
        () => directClient.call('TOUCH'),
        errorWithMessage("ERR wrong number of arguments for 'touch' command"),
      )
      await assert.rejects(
        () => directClient.touch(crossSlotA, crossSlotB),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    } finally {
      await directClient.del(stringKey, hashKey, expiringKey, missingKey)
      directClient.disconnect()
    }
  })

  test('TYPE command', async () => {
    // Set up test data of different types
    await redisClient?.set('{test}string_key', 'value')
    await redisClient?.hset('{test}hash_key', 'field', 'value')
    await redisClient?.lpush('{test}list_key', 'item')
    await redisClient?.sadd('{test}set_key', 'member')
    await redisClient?.zadd('{test}zset_key', 1, 'member')

    // Test type detection
    const stringType = await redisClient?.type('{test}string_key')
    assert.strictEqual(stringType, 'string')

    const hashType = await redisClient?.type('{test}hash_key')
    assert.strictEqual(hashType, 'hash')

    const listType = await redisClient?.type('{test}list_key')
    assert.strictEqual(listType, 'list')

    const setType = await redisClient?.type('{test}set_key')
    assert.strictEqual(setType, 'set')

    const zsetType = await redisClient?.type('{test}zset_key')
    assert.strictEqual(zsetType, 'zset')

    const noneType = await redisClient?.type('{test}nonexistent')
    assert.strictEqual(noneType, 'none')
  })

  test('Key command errors and past expiration match Redis', async () => {
    const tag = `{key-errors:${randomKey()}}`
    const key = `${tag}:key`
    const renamed = `${tag}:renamed`
    const missing = `${tag}:missing`

    try {
      await assert.rejects(
        () => redisClient?.rename(missing, renamed),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => redisClient?.renamenx(missing, renamed),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => redisClient?.expire(key, 'abc'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      assert.strictEqual(await redisClient?.expireat(missing, -1), 0)
      assert.strictEqual(await redisClient?.pexpireat(missing, -1), 0)

      await redisClient?.set(key, 'value')
      assert.strictEqual(await redisClient?.expireat(key, -1), 1)
      assert.strictEqual(await redisClient?.exists(key), 0)

      await redisClient?.set(key, 'value')
      assert.strictEqual(await redisClient?.pexpireat(key, -1), 1)
      assert.strictEqual(await redisClient?.exists(key), 0)
    } finally {
      await redisClient?.del(key, renamed, missing)
    }
  })

  test('UNLINK command removes keys and returns the deleted count', async () => {
    const tag = `{unlink:${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    const missing = `${tag}:missing`

    try {
      await redisClient?.set(first, 'one')
      await redisClient?.set(second, 'two')

      assert.strictEqual(await redisClient?.unlink(first, second, missing), 2)
      assert.strictEqual(await redisClient?.exists(first, second, missing), 0)
      assert.strictEqual(await redisClient?.unlink(first, missing), 0)
    } finally {
      await redisClient?.del(first, second, missing)
    }
  })

  test('DBSIZE command', async () => {
    const tag = `{dbsize:${randomKey()}}`
    const keys = {
      string: `${tag}:string_key`,
      hash: `${tag}:hash_key`,
      list: `${tag}:list_key`,
      set: `${tag}:set_key`,
      zset: `${tag}:zset_key`,
      expiring: `${tag}:expire_key1`,
    }
    const allKeys = Object.values(keys)
    const baseline = await getTotalDbSize(redisClient!)

    try {
      await redisClient?.set(keys.string, 'value')
      await redisClient?.hset(keys.hash, 'field', 'value')
      await redisClient?.lpush(keys.list, 'item')
      await redisClient?.sadd(keys.set, 'member')
      await redisClient?.zadd(keys.zset, 1, 'member')
      await assertDbSizeDelta(redisClient!, baseline, 5)

      await redisClient?.set(keys.expiring, 'value')
      await redisClient?.expire(keys.expiring, 3600)
      await assertDbSizeDelta(redisClient!, baseline, 6)

      await redisClient?.del(keys.string, keys.hash)
      await assertDbSizeDelta(redisClient!, baseline, 4)
    } finally {
      await redisClient?.del(...allKeys)
    }
  })
})
