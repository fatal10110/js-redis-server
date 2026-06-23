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

  test('HGETEX returns values and updates field TTLs like Redis', async () => {
    const tag = `{hgetex:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, {
        f1: 'v1',
        f2: 'v2',
        f3: 'v3',
        f4: 'v4',
      })

      assert.deepStrictEqual(
        await directClient.hGetEx(key, ['f1', 'missing', 'f2']),
        ['v1', null, 'v2'],
      )
      assert.deepStrictEqual(
        await directClient.hGetEx(`${tag}:nokey`, ['a', 'b']),
        [null, null],
      )
      assert.strictEqual(await directClient.exists(`${tag}:nokey`), 0)

      assert.deepStrictEqual(
        await directClient.hGetEx(key, 'f1', {
          expiration: { type: 'EX', value: 100 },
        }),
        ['v1'],
      )
      const ttlSet = await directClient.hTTL(key, 'f1')
      assert.ok(ttlSet[0] > 90 && ttlSet[0] <= 100, `ttl ${ttlSet[0]}`)

      assert.deepStrictEqual(await directClient.hGetEx(key, 'f1'), ['v1'])
      const ttlKeep = await directClient.hTTL(key, 'f1')
      assert.ok(ttlKeep[0] > 90 && ttlKeep[0] <= 100, `ttl ${ttlKeep[0]}`)

      assert.deepStrictEqual(
        await directClient.hGetEx(key, 'f1', { expiration: 'PERSIST' }),
        ['v1'],
      )
      assert.deepStrictEqual(await directClient.hTTL(key, 'f1'), [-1])

      await directClient.hGetEx(key, 'f2', {
        expiration: { type: 'PX', value: 50000 },
      })
      const pttlSet = await directClient.hpTTL(key, 'f2')
      assert.ok(pttlSet[0] > 40000 && pttlSet[0] <= 50000, `pttl ${pttlSet[0]}`)

      assert.deepStrictEqual(
        await directClient.hGetEx(key, 'f3', {
          expiration: { type: 'EXAT', value: 1 },
        }),
        ['v3'],
      )
      assert.strictEqual(await directClient.hExists(key, 'f3'), 0)

      assert.deepStrictEqual(
        await directClient.hGetEx(key, 'f4', {
          expiration: { type: 'EX', value: 0 },
        }),
        ['v4'],
      )
      assert.strictEqual(await directClient.hExists(key, 'f4'), 0)

      const lone = `${tag}:lone`
      await directClient.hSet(lone, 'only', 'v')
      assert.deepStrictEqual(
        await directClient.hGetEx(lone, 'only', {
          expiration: { type: 'EXAT', value: 1 },
        }),
        ['v'],
      )
      assert.strictEqual(await directClient.exists(lone), 0)
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HGETEX errors match Redis', async () => {
    const tag = `{hgetex-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, hashKey)
      await directClient.hSet(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () => directClient!.hGetEx(stringKey, 'field'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HGETEX']),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HGETEX', hashKey]),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HGETEX', hashKey, 'FIELDS', '1']),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand(['HGETEX', hashKey, 'FIELD', '1', 'field']),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETEX',
            hashKey,
            'KEEPTTL',
            'FIELDS',
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
            'HGETEX',
            hashKey,
            'EX',
            '100',
            'PERSIST',
            'FIELDS',
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
            'HGETEX',
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
            'HGETEX',
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
            'HGETEX',
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
            'HGETEX',
            hashKey,
            'EX',
            'abc',
            'FIELDS',
            '1',
            'field',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient!.hGetEx(hashKey, 'field', {
            expiration: { type: 'EX', value: -5 },
          }),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      await assert.rejects(
        () =>
          directClient!.hGetEx(hashKey, 'field', {
            expiration: { type: 'EXAT', value: 99999999999 },
          }),
        errorWithMessage("ERR invalid expire time in 'hgetex' command"),
      )
    } finally {
      await directClient?.del([hashKey, stringKey])
      directClient?.destroy()
    }
  })
})
