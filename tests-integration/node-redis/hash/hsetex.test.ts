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

  test('HSETEX sets fields and expiration like Redis', async () => {
    const tag = `{hsetex:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      assert.strictEqual(
        await directClient.hSetEx(key, { f1: 'v1', f2: 'v2' }),
        1,
      )
      assert.deepStrictEqual(
        await directClient.hTTL(key, ['f1', 'f2']),
        [-1, -1],
      )

      assert.strictEqual(
        await directClient.hSetEx(
          key,
          { f1: 'v1b' },
          { expiration: { type: 'EX', value: 100 } },
        ),
        1,
      )
      const ttlSet = await directClient.hTTL(key, 'f1')
      assert.ok(ttlSet[0] > 90 && ttlSet[0] <= 100, `ttl ${ttlSet[0]}`)
      assert.strictEqual(await directClient.hGet(key, 'f1'), 'v1b')

      // Overwriting without an expiration clause clears the field's TTL.
      assert.strictEqual(await directClient.hSetEx(key, { f1: 'v1c' }), 1)
      assert.deepStrictEqual(await directClient.hTTL(key, 'f1'), [-1])

      await directClient.hSetEx(
        key,
        { f1: 'v1d' },
        { expiration: { type: 'EX', value: 100 } },
      )
      assert.strictEqual(
        await directClient.hSetEx(
          key,
          { f1: 'v1e' },
          { expiration: 'KEEPTTL' },
        ),
        1,
      )
      const ttlKept = await directClient.hTTL(key, 'f1')
      assert.ok(ttlKept[0] > 90 && ttlKept[0] <= 100, `ttl ${ttlKept[0]}`)
      assert.strictEqual(await directClient.hGet(key, 'f1'), 'v1e')

      await directClient.hSetEx(
        key,
        { f2: 'v2b' },
        { expiration: { type: 'PX', value: 50000 } },
      )
      const pttlSet = await directClient.hpTTL(key, 'f2')
      assert.ok(pttlSet[0] > 40000 && pttlSet[0] <= 50000, `pttl ${pttlSet[0]}`)

      assert.strictEqual(
        await directClient.hSetEx(
          key,
          { f2: 'gone' },
          { expiration: { type: 'EXAT', value: 1 } },
        ),
        1,
      )
      assert.strictEqual(await directClient.hExists(key, 'f2'), 0)

      assert.strictEqual(
        await directClient.hSetEx(
          key,
          { f1: 'gone' },
          { expiration: { type: 'EX', value: 0 } },
        ),
        1,
      )
      assert.strictEqual(await directClient.hExists(key, 'f1'), 0)

      const lone = `${tag}:lone`
      assert.strictEqual(
        await directClient.hSetEx(
          lone,
          { only: 'v' },
          { expiration: { type: 'EXAT', value: 1 } },
        ),
        1,
      )
      assert.strictEqual(await directClient.exists(lone), 0)
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HSETEX FNX/FXX conditions match Redis', async () => {
    const tag = `{hsetex-cond:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, { a: '1' })

      assert.strictEqual(
        await directClient.hSetEx(
          `${tag}:nokey`,
          { f1: 'v1' },
          { mode: 'FXX' },
        ),
        0,
      )
      assert.strictEqual(await directClient.exists(`${tag}:nokey`), 0)

      assert.strictEqual(
        await directClient.hSetEx(key, { a: '2', b: '2' }, { mode: 'FNX' }),
        0,
      )
      assert.deepStrictEqual(await directClient.hGetAll(key), { a: '1' })

      assert.strictEqual(
        await directClient.hSetEx(key, { b: '2' }, { mode: 'FNX' }),
        1,
      )
      assert.strictEqual(await directClient.hGet(key, 'b'), '2')

      assert.strictEqual(
        await directClient.hSetEx(key, { a: 'x', c: 'x' }, { mode: 'FXX' }),
        0,
      )
      assert.strictEqual(await directClient.hExists(key, 'c'), 0)

      assert.strictEqual(
        await directClient.hSetEx(
          key,
          { a: '9' },
          { mode: 'FXX', expiration: { type: 'EX', value: 60 } },
        ),
        1,
      )
      assert.strictEqual(await directClient.hGet(key, 'a'), '9')
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HSETEX errors match Redis', async () => {
    const tag = `{hsetex-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, hashKey)
      await directClient.hSet(hashKey, { field: 'value' })
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () => directClient!.hSetEx(stringKey, { field: 'v' }),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HSETEX']),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HSETEX', hashKey]),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HSETEX', hashKey, 'FIELDS', '1']),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'BOGUS',
            'FIELDS',
            '1',
            'field',
            'v',
          ]),
        errorWithMessage('ERR unknown argument: BOGUS'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'FNX',
            'FXX',
            'FIELDS',
            '1',
            'field',
            'v',
          ]),
        errorWithMessage(
          'ERR Only one of FXX or FNX arguments can be specified',
        ),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'EX',
            '60',
            'KEEPTTL',
            'FIELDS',
            '1',
            'field',
            'v',
          ]),
        errorWithMessage(
          'ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified',
        ),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'FIELDS',
            'abc',
            'field',
            'v',
          ]),
        errorWithMessage('ERR invalid number of fields'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'FIELDS',
            '0',
            'field',
            'v',
          ]),
        errorWithMessage('ERR invalid number of fields'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'FIELDS',
            '2',
            'field',
            'v',
          ]),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'FIELDS',
            '1',
            'field',
            'v',
            'extra',
            'v2',
          ]),
        errorWithMessage("ERR wrong number of arguments for 'hsetex' command"),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HSETEX',
            hashKey,
            'EX',
            'abc',
            'FIELDS',
            '1',
            'field',
            'v',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient!.hSetEx(
            hashKey,
            { field: 'v' },
            { expiration: { type: 'EX', value: -5 } },
          ),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      await assert.rejects(
        () =>
          directClient!.hSetEx(
            hashKey,
            { field: 'v' },
            { expiration: { type: 'EXAT', value: 99999999999 } },
          ),
        errorWithMessage("ERR invalid expire time in 'hsetex' command"),
      )
      await assert.rejects(
        () =>
          directClient!.hSetEx(
            `${tag}:nokey`,
            { field: 'v' },
            { mode: 'FXX', expiration: { type: 'EX', value: -5 } },
          ),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
    } finally {
      await directClient?.del([hashKey, stringKey])
      directClient?.destroy()
    }
  })
})
