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

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`Hash Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HEXPIRE family sets and lazily expires hash fields', async () => {
    const tag = `{hexpire:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, {
        keep: 'value1',
        soon: 'value2',
        zero: 'value3',
        past: 'value4',
        pxat: 'value5',
      })

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HEXPIRE',
          key,
          '10',
          'FIELDS',
          '2',
          'keep',
          'missing',
        ]),
        [1, -2],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIRE',
          key,
          '0',
          'FIELDS',
          '1',
          'zero',
        ]),
        [2],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HEXPIREAT',
          key,
          '1',
          'FIELDS',
          '1',
          'past',
        ]),
        [2],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIREAT',
          key,
          String(Date.now() + 10_000),
          'FIELDS',
          '1',
          'pxat',
        ]),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIRE',
          key,
          '5',
          'FIELDS',
          '1',
          'soon',
        ]),
        [1],
      )

      await delay(40)

      assert.strictEqual(await directClient.hGet(key, 'soon'), null)
      assert.deepStrictEqual(
        await directClient.hmGet(key, ['keep', 'soon', 'zero', 'past', 'pxat']),
        ['value1', null, null, null, 'value5'],
      )
      assert.strictEqual(await directClient.hExists(key, 'soon'), 0)
      assert.strictEqual(
        await directClient.sendCommand(['HSTRLEN', key, 'soon']),
        0,
      )
      assert.strictEqual(await directClient.hLen(key), 2)
      assert.deepStrictEqual((await directClient.hKeys(key)).sort(), [
        'keep',
        'pxat',
      ])
      assert.deepStrictEqual((await directClient.hVals(key)).sort(), [
        'value1',
        'value5',
      ])
      assert.deepStrictEqual(await directClient.hGetAll(key), {
        keep: 'value1',
        pxat: 'value5',
      })
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HSCAN omits expired hash fields', async () => {
    const key = `{hexpire-hscan:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, { keep: 'value1', gone: 'value2' })

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIRE',
          key,
          '5',
          'FIELDS',
          '1',
          'gone',
        ]),
        [1],
      )

      await delay(40)

      const [, scanEntries] = (await directClient.sendCommand([
        'HSCAN',
        key,
        '0',
      ])) as [string, string[]]
      const scanned = new Map<string, string>()
      for (let i = 0; i < scanEntries.length; i += 2) {
        scanned.set(scanEntries[i], scanEntries[i + 1])
      }

      assert.deepStrictEqual(Object.fromEntries(scanned), { keep: 'value1' })
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HEXPIRE deletes the key when every field expires', async () => {
    const key = `{hexpire-empty:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, { field1: 'value1', field2: 'value2' })

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIRE',
          key,
          '0',
          'FIELDS',
          '2',
          'field1',
          'field2',
        ]),
        [2, 2],
      )
      assert.strictEqual(await directClient.exists(key), 0)
      assert.strictEqual(await directClient.type(key), 'none')
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HEXPIRE condition flags match Redis', async () => {
    const key = `{hexpire-options:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, {
        volatile: 'value1',
        nx: 'value2',
        xx: 'value3',
        lt: 'value4',
      })

      const hexpire = (...args: string[]) =>
        directClient!.sendCommand(['HEXPIRE', key, ...args])

      assert.deepStrictEqual(
        await hexpire('20', 'FIELDS', '1', 'volatile'),
        [1],
      )
      assert.deepStrictEqual(
        await hexpire('30', 'NX', 'FIELDS', '1', 'volatile'),
        [0],
      )
      assert.deepStrictEqual(
        await hexpire('30', 'NX', 'FIELDS', '1', 'nx'),
        [1],
      )
      assert.deepStrictEqual(
        await hexpire('30', 'XX', 'FIELDS', '1', 'xx'),
        [0],
      )
      assert.deepStrictEqual(
        await hexpire('30', 'XX', 'FIELDS', '1', 'volatile'),
        [1],
      )
      assert.deepStrictEqual(
        await hexpire('10', 'GT', 'FIELDS', '1', 'nx'),
        [0],
      )
      assert.deepStrictEqual(
        await hexpire('40', 'GT', 'FIELDS', '1', 'nx'),
        [1],
      )
      assert.deepStrictEqual(
        await hexpire('10', 'LT', 'FIELDS', '1', 'lt'),
        [1],
      )
      assert.deepStrictEqual(
        await hexpire('40', 'LT', 'FIELDS', '1', 'lt'),
        [0],
      )
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('hash writes clear or preserve field TTLs like Redis', async () => {
    const key = `{hash-field-ttl-writes:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, {
        replace: 'old',
        counter: '1',
        float: '1.5',
      })
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIRE',
          key,
          '20',
          'FIELDS',
          '3',
          'replace',
          'counter',
          'float',
        ]),
        [1, 1, 1],
      )

      assert.strictEqual(await directClient.hSet(key, 'replace', 'new'), 0)
      assert.strictEqual(await directClient.hIncrBy(key, 'counter', 1), 2)
      assert.strictEqual(
        await directClient.hIncrByFloat(key, 'float', 1),
        '2.5',
      )

      await delay(50)

      assert.strictEqual(await directClient.hGet(key, 'replace'), 'new')
      assert.strictEqual(await directClient.hGet(key, 'counter'), null)
      assert.strictEqual(await directClient.hGet(key, 'float'), null)
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HEXPIRE errors match Redis', async () => {
    const tag = `{hexpire-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, hashKey)
      await directClient.hSet(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      const send = (args: string[]) => directClient!.sendCommand(args)

      await assert.rejects(
        () => send(['HEXPIRE', stringKey, '10', 'FIELDS', '1', 'field']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => send(['HEXPIRE', stringKey, 'abc', 'FIELDS', '1', 'field']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => send(['HEXPIRE', stringKey, '100', 'FIELDS', '5', 'field']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => send(['HEXPIRE', stringKey, '100', 'FIELDS', '0', 'field']),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => send(['HEXPIRE']),
        errorWithMessage("ERR wrong number of arguments for 'hexpire' command"),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '10']),
        errorWithMessage("ERR wrong number of arguments for 'hexpire' command"),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, 'abc', 'FIELDS', '1', 'field']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '-1', 'FIELDS', '1', 'field']),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      assert.strictEqual(await directClient.hGet(hashKey, 'field'), 'value')
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '99999999999', 'FIELDS', '1', 'field']),
        errorWithMessage("ERR invalid expire time in 'hexpire' command"),
      )
      await assert.rejects(
        () =>
          send([
            'HEXPIREAT',
            hashKey,
            '9999999999999999',
            'FIELDS',
            '1',
            'field',
          ]),
        errorWithMessage("ERR invalid expire time in 'hexpireat' command"),
      )
      await assert.rejects(
        () =>
          send([
            'HEXPIRE',
            hashKey,
            '9223372036854775807',
            'FIELDS',
            '1',
            'field',
          ]),
        errorWithMessage("ERR invalid expire time in 'hexpire' command"),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '100', '1', 'field']),
        errorWithMessage("ERR wrong number of arguments for 'hexpire' command"),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '10', 'FIELD', '1', 'field']),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '100', '1', 'FIELDS', 'field']),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '10', 'FIELDS', 'abc', 'field']),
        errorWithMessage('ERR Parameter `numFields` should be greater than 0'),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '10', 'FIELDS', '0', 'field']),
        errorWithMessage('ERR Parameter `numFields` should be greater than 0'),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '10', 'FIELDS', '2', 'field']),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
      await assert.rejects(
        () =>
          send(['HEXPIRE', hashKey, '10', 'NX', 'XX', 'FIELDS', '1', 'field']),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () =>
          send(['HEXPIRE', hashKey, '10', 'GT', 'LT', 'FIELDS', '1', 'field']),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () => send(['HEXPIRE', hashKey, '10', 'BOGUS', 'FIELDS', '1', 'field']),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
    } finally {
      await directClient?.del([hashKey, stringKey])
      directClient?.destroy()
    }
  })

  test('HEXPIRE arg-content errors stay inline inside MULTI/EXEC', async () => {
    const key = `{hexpire-multi-errors:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      assert.strictEqual(await directClient.sendCommand(['MULTI']), 'OK')
      assert.strictEqual(
        await directClient.sendCommand(['HSET', key, 'field', 'value']),
        'QUEUED',
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'HEXPIRE',
          key,
          'abc',
          'FIELDS',
          '1',
          'field',
        ]),
        'QUEUED',
      )
      assert.strictEqual(
        await directClient.sendCommand(['HGET', key, 'field']),
        'QUEUED',
      )

      const result = (await directClient.sendCommand(['EXEC'])) as unknown[]

      assert.ok(Array.isArray(result))
      assert.strictEqual(result.length, 3)
      assert.strictEqual(result[0], 1)
      assert.ok(result[1] instanceof Error)
      assert.strictEqual(
        (result[1] as Error).message,
        'ERR value is not an integer or out of range',
      )
      assert.strictEqual(result[2], 'value')
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })
})
