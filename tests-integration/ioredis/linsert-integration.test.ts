import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`LINSERT Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('linsert-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('inserts before and after the first matching pivot', async () => {
    const tag = `{linsert:${randomKey()}}`
    const key = `${tag}:list`
    const client = await connectToSlotOwner(redisClient!, key)

    try {
      await client.del(key)
      await client.rpush(key, 'a', 'b', 'c', 'b')

      assert.strictEqual(
        await client.call('LINSERT', key, 'BEFORE', 'b', 'x'),
        5,
      )
      assert.deepStrictEqual(await client.lrange(key, 0, -1), [
        'a',
        'x',
        'b',
        'c',
        'b',
      ])

      assert.strictEqual(
        await client.call('LINSERT', key, 'after', 'b', 'y'),
        6,
      )
      assert.deepStrictEqual(await client.lrange(key, 0, -1), [
        'a',
        'x',
        'b',
        'y',
        'c',
        'b',
      ])
    } finally {
      await client.del(key)
      client.disconnect()
    }
  })

  test('returns Redis-compatible values for missing keys and pivots', async () => {
    const tag = `{linsert-missing:${randomKey()}}`
    const key = `${tag}:list`
    const missing = `${tag}:missing`
    const client = await connectToSlotOwner(redisClient!, key)

    try {
      await client.del(key, missing)
      await client.rpush(key, 'a', 'b', 'c')

      assert.strictEqual(
        await client.call('LINSERT', key, 'BEFORE', 'z', 'x'),
        -1,
      )
      assert.deepStrictEqual(await client.lrange(key, 0, -1), ['a', 'b', 'c'])

      assert.strictEqual(
        await client.call('LINSERT', missing, 'AFTER', 'z', 'x'),
        0,
      )
      assert.strictEqual(await client.exists(missing), 0)
    } finally {
      await client.del(key, missing)
      client.disconnect()
    }
  })

  test('error paths match Redis', async () => {
    const tag = `{linsert-err:${randomKey()}}`
    const key = `${tag}:list`
    const stringKey = `${tag}:string`
    const client = await connectToSlotOwner(redisClient!, key)

    try {
      await client.del(key, stringKey)
      await client.rpush(key, 'pivot')

      await assert.rejects(
        () => client.call('LINSERT', key, 'AROUND', 'pivot', 'x'),
        errorWithMessage('ERR syntax error'),
      )

      await assert.rejects(
        () => client.call('LINSERT', key, 'BEFORE', 'pivot'),
        errorWithMessage("ERR wrong number of arguments for 'linsert' command"),
      )

      await assert.rejects(
        () => client.call('LINSERT', key, 'BEFORE', 'pivot', 'x', 'extra'),
        errorWithMessage("ERR wrong number of arguments for 'linsert' command"),
      )

      await client.set(stringKey, 'value')
      await assert.rejects(
        () => client.call('LINSERT', stringKey, 'BEFORE', 'pivot', 'x'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
    } finally {
      await client.del(key, stringKey)
      client.disconnect()
    }
  })
})
