import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Hash Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('hash-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HDEL command', async () => {
    const key = `{hdel:${randomKey()}}:hash`

    await redisClient?.hset(
      key,
      'field1',
      'value1',
      'field2',
      'value2',
      'field3',
      'value3',
    )

    // Delete single field
    const del1 = await redisClient?.hdel(key, 'field1')
    assert.strictEqual(del1, 1)

    // Delete multiple fields
    const del2 = await redisClient?.hdel(key, 'field2', 'field3')
    assert.strictEqual(del2, 2)

    // Verify hash is removed after its last field is deleted.
    const len = await redisClient?.hlen(key)
    assert.strictEqual(len, 0)
    assert.strictEqual(await redisClient?.exists(key), 0)
    assert.strictEqual(await redisClient?.type(key), 'none')
  })

  test('HDEL on a missing key does not create an empty hash', async () => {
    const key = `{hdel-missing:${randomKey()}}:hash`

    assert.strictEqual(await redisClient?.hdel(key, 'field1'), 0)
    assert.strictEqual(await redisClient?.exists(key), 0)
    assert.strictEqual(await redisClient?.type(key), 'none')

    await redisClient?.hset(key, 'field1', 'value1')
    assert.strictEqual(await redisClient?.hdel(key, 'field1'), 1)
    assert.strictEqual(await redisClient?.hdel(key, 'field1'), 0)
    assert.strictEqual(await redisClient?.exists(key), 0)
    assert.strictEqual(await redisClient?.type(key), 'none')
  })

  test('HGETDEL returns values and deletes fields', async () => {
    const key = `{hgetdel:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)

      await directClient.hset(
        key,
        'field1',
        'value1',
        'field2',
        'value2',
        'field3',
        'value3',
      )

      assert.deepStrictEqual(
        await directClient.call(
          'HGETDEL',
          key,
          'FIELDS',
          '3',
          'field2',
          'missing',
          'field1',
        ),
        ['value2', null, 'value1'],
      )
      assert.deepStrictEqual(
        await directClient.hmget(key, 'field1', 'field2', 'field3'),
        [null, null, 'value3'],
      )
      assert.strictEqual(await directClient.hlen(key), 1)

      await directClient.hset(key, 'field4', 'value4')
      assert.deepStrictEqual(
        await directClient.call(
          'HGETDEL',
          key,
          'FIELDS',
          '2',
          'field4',
          'field4',
        ),
        ['value4', null],
      )
      assert.strictEqual(await directClient.hexists(key, 'field4'), 0)

      assert.deepStrictEqual(
        await directClient.call('HGETDEL', key, 'FIELDS', '1', 'field3'),
        ['value3'],
      )
      assert.strictEqual(await directClient.exists(key), 0)
      assert.strictEqual(await directClient.type(key), 'none')

      assert.deepStrictEqual(
        await directClient.call(
          'HGETDEL',
          key,
          'FIELDS',
          '2',
          'field3',
          'missing',
        ),
        [null, null],
      )
      assert.strictEqual(await directClient.exists(key), 0)
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HGETDEL errors match Redis', async () => {
    const tag = `{hgetdel-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, hashKey)
      await directClient.hset(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () => directClient?.call('HGETDEL', stringKey, 'FIELDS', '1', 'field'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HGETDEL'),
        errorWithMessage("ERR wrong number of arguments for 'hgetdel' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETDEL', hashKey),
        errorWithMessage("ERR wrong number of arguments for 'hgetdel' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETDEL', hashKey, 'FIELD', '1', 'field'),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HGETDEL', hashKey, 'FIELDS', 'abc', 'field'),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () => directClient?.call('HGETDEL', hashKey, 'FIELDS', '0', 'field'),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () => directClient?.call('HGETDEL', hashKey, 'FIELDS', '-1', 'field'),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETDEL',
            hashKey,
            'FIELDS',
            '9223372036854775808',
            'field',
          ),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () => directClient?.call('HGETDEL', hashKey, 'FIELDS', '2', 'field'),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETDEL',
            hashKey,
            'FIELDS',
            '1',
            'field',
            'extra',
          ),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
    } finally {
      await directClient?.del(hashKey, stringKey)
      directClient?.disconnect()
    }
  })
})
