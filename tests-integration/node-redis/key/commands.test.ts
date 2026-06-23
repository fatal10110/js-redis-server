import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  assertNodeRedisDbSizeDelta,
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  getNodeRedisTotalDbSize,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

describe(`Key Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('EXISTS command', async () => {
    await redisClient.set('{test}string_key', 'value')
    await redisClient.hSet('{test}hash_key', 'field', 'value')
    await redisClient.lPush('{test}list_key', 'item')
    await redisClient.sAdd('{test}set_key', 'member')
    await redisClient.zAdd('{test}zset_key', { score: 1, value: 'member' })

    const exists1 = await redisClient.exists('{test}string_key')
    assert.strictEqual(exists1, 1)

    const exists2 = await redisClient.exists('{test}nonexistent')
    assert.strictEqual(exists2, 0)

    const existsMultiple = await redisClient.exists([
      '{test}string_key',
      '{test}hash_key',
      '{test}nonexistent',
      '{test}list_key',
    ])
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
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      stringKey,
    )

    try {
      await directClient.set(stringKey, 'value')
      await directClient.hSet(hashKey, 'field', 'value')
      await directClient.set(expiringKey, 'value', {
        expiration: { type: 'PX', value: 1 },
      })
      await new Promise(resolve => setTimeout(resolve, 20))

      assert.strictEqual(
        await directClient.touch([stringKey, hashKey, missingKey, expiringKey]),
        2,
      )
      assert.strictEqual(
        await directClient.exists([stringKey, hashKey, expiringKey]),
        2,
      )
      assert.strictEqual(await directClient.type(hashKey), 'hash')
      assert.strictEqual(await directClient.touch(missingKey), 0)

      await assert.rejects(
        () => directClient.sendCommand(['TOUCH']),
        errorWithMessage("ERR wrong number of arguments for 'touch' command"),
      )
      await assert.rejects(
        () => directClient.touch([crossSlotA, crossSlotB]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    } finally {
      await directClient.del([stringKey, hashKey, expiringKey, missingKey])
      directClient.destroy()
    }
  })

  test('TYPE command', async () => {
    await redisClient.set('{test}string_key', 'value')
    await redisClient.hSet('{test}hash_key', 'field', 'value')
    await redisClient.lPush('{test}list_key', 'item')
    await redisClient.sAdd('{test}set_key', 'member')
    await redisClient.zAdd('{test}zset_key', { score: 1, value: 'member' })

    assert.strictEqual(await redisClient.type('{test}string_key'), 'string')
    assert.strictEqual(await redisClient.type('{test}hash_key'), 'hash')
    assert.strictEqual(await redisClient.type('{test}list_key'), 'list')
    assert.strictEqual(await redisClient.type('{test}set_key'), 'set')
    assert.strictEqual(await redisClient.type('{test}zset_key'), 'zset')
    assert.strictEqual(await redisClient.type('{test}nonexistent'), 'none')
  })

  test('Key command errors and past expiration match Redis', async () => {
    const tag = `{key-errors:${randomKey()}}`
    const key = `${tag}:key`
    const renamed = `${tag}:renamed`
    const missing = `${tag}:missing`

    try {
      await assert.rejects(
        () => redisClient.rename(missing, renamed),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => redisClient.renameNX(missing, renamed),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => redisClient.sendCommand(key, false, ['EXPIRE', key, 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      assert.strictEqual(await redisClient.expireAt(missing, -1), 0)
      assert.strictEqual(await redisClient.pExpireAt(missing, -1), 0)

      await redisClient.set(key, 'value')
      assert.strictEqual(await redisClient.expireAt(key, -1), 1)
      assert.strictEqual(await redisClient.exists(key), 0)

      await redisClient.set(key, 'value')
      assert.strictEqual(await redisClient.pExpireAt(key, -1), 1)
      assert.strictEqual(await redisClient.exists(key), 0)
    } finally {
      await redisClient.del([key, renamed, missing])
    }
  })

  test('UNLINK command removes keys and returns the deleted count', async () => {
    const tag = `{unlink:${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    const missing = `${tag}:missing`

    try {
      await redisClient.set(first, 'one')
      await redisClient.set(second, 'two')

      assert.strictEqual(await redisClient.unlink([first, second, missing]), 2)
      assert.strictEqual(await redisClient.exists([first, second, missing]), 0)
      assert.strictEqual(await redisClient.unlink([first, missing]), 0)
    } finally {
      await redisClient.del([first, second, missing])
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
    const baseline = await getNodeRedisTotalDbSize(redisClient)

    try {
      await redisClient.set(keys.string, 'value')
      await redisClient.hSet(keys.hash, 'field', 'value')
      await redisClient.lPush(keys.list, 'item')
      await redisClient.sAdd(keys.set, 'member')
      await redisClient.zAdd(keys.zset, { score: 1, value: 'member' })
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 5)

      await redisClient.set(keys.expiring, 'value')
      await redisClient.expire(keys.expiring, 3600)
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 6)

      await redisClient.del([keys.string, keys.hash])
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 4)
    } finally {
      await redisClient.del(allKeys)
    }
  })
})
