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

describe(`LINSERT Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('inserts before and after the first matching pivot', async () => {
    const tag = `{linsert:${randomKey()}}`
    const key = `${tag}:list`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await client.del(key)
      await client.rPush(key, ['a', 'b', 'c', 'b'])

      assert.strictEqual(
        await client.sendCommand(['LINSERT', key, 'BEFORE', 'b', 'x']),
        5,
      )
      assert.deepStrictEqual(await client.lRange(key, 0, -1), [
        'a',
        'x',
        'b',
        'c',
        'b',
      ])

      assert.strictEqual(
        await client.sendCommand(['LINSERT', key, 'after', 'b', 'y']),
        6,
      )
      assert.deepStrictEqual(await client.lRange(key, 0, -1), [
        'a',
        'x',
        'b',
        'y',
        'c',
        'b',
      ])
    } finally {
      await client.del(key)
      client.destroy()
    }
  })

  test('returns Redis-compatible values for missing keys and pivots', async () => {
    const tag = `{linsert-missing:${randomKey()}}`
    const key = `${tag}:list`
    const missing = `${tag}:missing`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await client.del([key, missing])
      await client.rPush(key, ['a', 'b', 'c'])

      assert.strictEqual(
        await client.sendCommand(['LINSERT', key, 'BEFORE', 'z', 'x']),
        -1,
      )
      assert.deepStrictEqual(await client.lRange(key, 0, -1), ['a', 'b', 'c'])

      assert.strictEqual(
        await client.sendCommand(['LINSERT', missing, 'AFTER', 'z', 'x']),
        0,
      )
      assert.strictEqual(await client.exists(missing), 0)
    } finally {
      await client.del([key, missing])
      client.destroy()
    }
  })

  test('error paths match Redis', async () => {
    const tag = `{linsert-err:${randomKey()}}`
    const key = `${tag}:list`
    const stringKey = `${tag}:string`
    const client = await connectToNodeRedisSlotOwner(redisClient, key)

    try {
      await client.del([key, stringKey])
      await client.rPush(key, 'pivot')

      await assert.rejects(
        () => client.sendCommand(['LINSERT', key, 'AROUND', 'pivot', 'x']),
        errorWithMessage('ERR syntax error'),
      )

      await assert.rejects(
        () => client.sendCommand(['LINSERT', key, 'BEFORE', 'pivot']),
        errorWithMessage("ERR wrong number of arguments for 'linsert' command"),
      )

      await assert.rejects(
        () =>
          client.sendCommand(['LINSERT', key, 'BEFORE', 'pivot', 'x', 'extra']),
        errorWithMessage("ERR wrong number of arguments for 'linsert' command"),
      )

      await client.set(stringKey, 'value')
      await assert.rejects(
        () =>
          client.sendCommand(['LINSERT', stringKey, 'BEFORE', 'pivot', 'x']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await client.del([key, stringKey])
      client.destroy()
    }
  })
})
