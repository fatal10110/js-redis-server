import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

describe(`Hash Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HDEL command', async () => {
    const key = `{hdel:${randomKey()}}:hash`

    await redisClient.hSet(key, {
      field1: 'value1',
      field2: 'value2',
      field3: 'value3',
    })

    const del1 = await redisClient.hDel(key, 'field1')
    assert.strictEqual(del1, 1)

    const del2 = await redisClient.hDel(key, ['field2', 'field3'])
    assert.strictEqual(del2, 2)

    const len = await redisClient.hLen(key)
    assert.strictEqual(len, 0)
    assert.strictEqual(await redisClient.exists(key), 0)
    assert.strictEqual(await redisClient.type(key), 'none')
  })

  test('HDEL on a missing key does not create an empty hash', async () => {
    const key = `{hdel-missing:${randomKey()}}:hash`

    assert.strictEqual(await redisClient.hDel(key, 'field1'), 0)
    assert.strictEqual(await redisClient.exists(key), 0)
    assert.strictEqual(await redisClient.type(key), 'none')

    await redisClient.hSet(key, 'field1', 'value1')
    assert.strictEqual(await redisClient.hDel(key, 'field1'), 1)
    assert.strictEqual(await redisClient.hDel(key, 'field1'), 0)
    assert.strictEqual(await redisClient.exists(key), 0)
    assert.strictEqual(await redisClient.type(key), 'none')
  })

  test('HGETDEL returns values and deletes fields', async () => {
    const key = `{hgetdel:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      await directClient.hSet(key, {
        field1: 'value1',
        field2: 'value2',
        field3: 'value3',
      })

      assert.deepStrictEqual(
        await directClient.hGetDel(key, ['field2', 'missing', 'field1']),
        ['value2', null, 'value1'],
      )
      assert.deepStrictEqual(
        await directClient.hmGet(key, ['field1', 'field2', 'field3']),
        [null, null, 'value3'],
      )
      assert.strictEqual(await directClient.hLen(key), 1)

      await directClient.hSet(key, 'field4', 'value4')
      assert.deepStrictEqual(
        await directClient.hGetDel(key, ['field4', 'field4']),
        ['value4', null],
      )
      assert.strictEqual(await directClient.hExists(key, 'field4'), 0)

      assert.deepStrictEqual(await directClient.hGetDel(key, ['field3']), [
        'value3',
      ])
      assert.strictEqual(await directClient.exists(key), 0)
      assert.strictEqual(await directClient.type(key), 'none')

      assert.deepStrictEqual(
        await directClient.hGetDel(key, ['field3', 'missing']),
        [null, null],
      )
      assert.strictEqual(await directClient.exists(key), 0)
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HGETDEL errors match Redis', async () => {
    const tag = `{hgetdel-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, hashKey)
      await directClient.hSet(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () => directClient!.hGetDel(stringKey, ['field']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HGETDEL']),
        errorWithMessage("ERR wrong number of arguments for 'hgetdel' command"),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HGETDEL', hashKey]),
        errorWithMessage("ERR wrong number of arguments for 'hgetdel' command"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            hashKey,
            'FIELD',
            '1',
            'field',
          ]),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            hashKey,
            'FIELDS',
            'abc',
            'field',
          ]),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            hashKey,
            'FIELDS',
            '0',
            'field',
          ]),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            hashKey,
            'FIELDS',
            '-1',
            'field',
          ]),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            hashKey,
            'FIELDS',
            '9223372036854775808',
            'field',
          ]),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            hashKey,
            'FIELDS',
            '2',
            'field',
          ]),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            hashKey,
            'FIELDS',
            '1',
            'field',
            'extra',
          ]),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
    } finally {
      await directClient?.del([hashKey, stringKey])
      directClient?.destroy()
    }
  })
})
