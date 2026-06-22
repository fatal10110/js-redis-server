import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { createClient, RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  findNodeRedisSlotOwner,
  flushNodeRedisCluster,
  randomKey,
} from '../utils'

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

  test('HSET and HGET commands', async () => {
    const result1 = await redisClient.hSet('hash1', 'field1', 'value1')
    assert.strictEqual(result1, 1)

    const value = await redisClient.hGet('hash1', 'field1')
    assert.strictEqual(value, 'value1')

    const result2 = await redisClient.hSet('hash1', {
      field2: 'value2',
      field3: 'value3',
    })
    assert.strictEqual(result2, 2)
  })

  test('HMSET and HMGET commands', async () => {
    await redisClient.hSet('hash2', {
      field1: 'value1',
      field2: 'value2',
      field3: 'value3',
    })

    const values = await redisClient.hmGet('hash2', [
      'field1',
      'field2',
      'nonexistent',
    ])
    assert.deepStrictEqual(values, ['value1', 'value2', null])
  })

  test('HGETALL command', async () => {
    await redisClient.hSet('hash3', { field1: 'value1', field2: 'value2' })

    const all = await redisClient.hGetAll('hash3')
    assert.deepStrictEqual(all, { field1: 'value1', field2: 'value2' })
  })

  test('HGETALL works over a RESP3 connection (HELLO 3)', async () => {
    const key = `{hash-resp3:${randomKey()}}:hash`
    const { host, port } = findNodeRedisSlotOwner(redisClient, key)
    const conn = createClient({
      url: `redis://${host}:${port}`,
      RESP: 3,
    }) as unknown as RedisClientType
    conn.on('error', () => {})
    await conn.connect()

    try {
      assert.strictEqual(
        await conn.hSet(key, { field1: 'value1', field2: 'value2' }),
        2,
      )
      const reply = await conn.hGetAll(key)
      const get = (field: string) =>
        reply instanceof Map
          ? reply.get(field)
          : (reply as Record<string, string>)[field]
      assert.strictEqual(get('field1'), 'value1')
      assert.strictEqual(get('field2'), 'value2')
    } finally {
      conn.destroy()
      await redisClient.del(key)
    }
  })

  test('HKEYS and HVALS commands', async () => {
    await redisClient.hSet('hash4', { field1: 'value1', field2: 'value2' })

    const keys = await redisClient.hKeys('hash4')
    assert.deepStrictEqual(keys.sort(), ['field1', 'field2'])

    const vals = await redisClient.hVals('hash4')
    assert.deepStrictEqual(vals.sort(), ['value1', 'value2'])
  })

  test('HRANDFIELD command matches Redis', async () => {
    const tag = `{hrandfield:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const missingKey = `${tag}:missing`
    const stringKey = `${tag}:string`
    const expected = new Map([
      ['field1', 'value1'],
      ['field2', 'value2'],
      ['field3', 'value3'],
    ])
    let directClient: RedisClientType | undefined

    function assertField(value: unknown): string {
      assert.strictEqual(typeof value, 'string')
      assert.ok(expected.has(value as string))
      return value as string
    }

    function assertFieldArray(value: unknown, length: number): string[] {
      assert.ok(Array.isArray(value))
      assert.strictEqual(value.length, length)
      for (const field of value) assertField(field)
      return value as string[]
    }

    function assertFieldValuePairs(value: unknown, pairCount: number): void {
      // node-redis returns WITHVALUES as an array of { field, value } objects.
      assert.ok(Array.isArray(value))
      assert.strictEqual(value.length, pairCount)
      for (const entry of value as Array<{ field: string; value: string }>) {
        const field = assertField(entry.field)
        assert.strictEqual(entry.value, expected.get(field))
      }
    }

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, hashKey)
      await directClient.hSet(hashKey, {
        field1: 'value1',
        field2: 'value2',
        field3: 'value3',
      })

      assertField(await directClient.hRandField(hashKey))
      assert.strictEqual(await directClient.hRandField(missingKey), null)

      const twoFields = assertFieldArray(
        await directClient.hRandFieldCount(hashKey, 2),
        2,
      )
      assert.strictEqual(new Set(twoFields).size, 2)

      const allFields = assertFieldArray(
        await directClient.hRandFieldCount(hashKey, 10),
        3,
      )
      assert.strictEqual(new Set(allFields).size, 3)

      assertFieldArray(await directClient.hRandFieldCount(hashKey, -5), 5)
      assert.deepStrictEqual(await directClient.hRandFieldCount(hashKey, 0), [])
      assert.deepStrictEqual(
        await directClient.hRandFieldCount(missingKey, 2),
        [],
      )

      assertFieldValuePairs(
        await directClient.hRandFieldCountWithValues(hashKey, 2),
        2,
      )
      assertFieldValuePairs(
        await directClient.hRandFieldCountWithValues(hashKey, -4),
        4,
      )
      assert.deepStrictEqual(
        await directClient.hRandFieldCountWithValues(missingKey, 2),
        [],
      )

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient!.sendCommand(['HRANDFIELD', stringKey]),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => directClient!.sendCommand(['HRANDFIELD']),
        errorWithMessage(
          "ERR wrong number of arguments for 'hrandfield' command",
        ),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HRANDFIELD', hashKey, 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HRANDFIELD', hashKey, 'WITHVALUES']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient!.sendCommand(['HRANDFIELD', hashKey, '1', 'BAD']),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HRANDFIELD',
            hashKey,
            '1',
            'WITHVALUES',
            'WITHVALUES',
          ]),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await directClient?.del([hashKey, missingKey, stringKey])
      directClient?.destroy()
    }
  })

  test('HLEN command', async () => {
    const len1 = await redisClient.hLen('emptyhash')
    assert.strictEqual(len1, 0)

    await redisClient.hSet('hash5', { field1: 'value1', field2: 'value2' })
    const len2 = await redisClient.hLen('hash5')
    assert.strictEqual(len2, 2)
  })

  test('HEXISTS command', async () => {
    await redisClient.hSet('hash6', 'field1', 'value1')

    const exists1 = await redisClient.hExists('hash6', 'field1')
    assert.strictEqual(exists1, 1)

    const exists2 = await redisClient.hExists('hash6', 'field2')
    assert.strictEqual(exists2, 0)
  })

  test('HSETNX command', async () => {
    const result1 = await redisClient.hSetNX('hashsetnx', 'field1', 'value1')
    assert.strictEqual(result1, 1)

    const value1 = await redisClient.hGet('hashsetnx', 'field1')
    assert.strictEqual(value1, 'value1')

    const result2 = await redisClient.hSetNX('hashsetnx', 'field1', 'value2')
    assert.strictEqual(result2, 0)

    const value2 = await redisClient.hGet('hashsetnx', 'field1')
    assert.strictEqual(value2, 'value1')

    const result3 = await redisClient.hSetNX('hashsetnx', 'field2', 'value2')
    assert.strictEqual(result3, 1)

    const len = await redisClient.hLen('hashsetnx')
    assert.strictEqual(len, 2)
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
        await directClient.sendCommand([
          'HGETDEL',
          key,
          'FIELDS',
          '3',
          'field2',
          'missing',
          'field1',
        ]),
        ['value2', null, 'value1'],
      )
      assert.deepStrictEqual(
        await directClient.hmGet(key, ['field1', 'field2', 'field3']),
        [null, null, 'value3'],
      )
      assert.strictEqual(await directClient.hLen(key), 1)

      await directClient.hSet(key, 'field4', 'value4')
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETDEL',
          key,
          'FIELDS',
          '2',
          'field4',
          'field4',
        ]),
        ['value4', null],
      )
      assert.strictEqual(await directClient.hExists(key, 'field4'), 0)

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETDEL',
          key,
          'FIELDS',
          '1',
          'field3',
        ]),
        ['value3'],
      )
      assert.strictEqual(await directClient.exists(key), 0)
      assert.strictEqual(await directClient.type(key), 'none')

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETDEL',
          key,
          'FIELDS',
          '2',
          'field3',
          'missing',
        ]),
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
        () =>
          directClient!.sendCommand([
            'HGETDEL',
            stringKey,
            'FIELDS',
            '1',
            'field',
          ]),
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
        await directClient.sendCommand([
          'HGETEX',
          key,
          'FIELDS',
          '3',
          'f1',
          'missing',
          'f2',
        ]),
        ['v1', null, 'v2'],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETEX',
          `${tag}:nokey`,
          'FIELDS',
          '2',
          'a',
          'b',
        ]),
        [null, null],
      )
      assert.strictEqual(await directClient.exists(`${tag}:nokey`), 0)

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETEX',
          key,
          'EX',
          '100',
          'FIELDS',
          '1',
          'f1',
        ]),
        ['v1'],
      )
      const ttlSet = (await directClient.sendCommand([
        'HTTL',
        key,
        'FIELDS',
        '1',
        'f1',
      ])) as number[]
      assert.ok(ttlSet[0] > 90 && ttlSet[0] <= 100, `ttl ${ttlSet[0]}`)

      assert.deepStrictEqual(
        await directClient.sendCommand(['HGETEX', key, 'FIELDS', '1', 'f1']),
        ['v1'],
      )
      const ttlKeep = (await directClient.sendCommand([
        'HTTL',
        key,
        'FIELDS',
        '1',
        'f1',
      ])) as number[]
      assert.ok(ttlKeep[0] > 90 && ttlKeep[0] <= 100, `ttl ${ttlKeep[0]}`)

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETEX',
          key,
          'PERSIST',
          'FIELDS',
          '1',
          'f1',
        ]),
        ['v1'],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand(['HTTL', key, 'FIELDS', '1', 'f1']),
        [-1],
      )

      await directClient.sendCommand([
        'HGETEX',
        key,
        'PX',
        '50000',
        'FIELDS',
        '1',
        'f2',
      ])
      const pttlSet = (await directClient.sendCommand([
        'HPTTL',
        key,
        'FIELDS',
        '1',
        'f2',
      ])) as number[]
      assert.ok(pttlSet[0] > 40000 && pttlSet[0] <= 50000, `pttl ${pttlSet[0]}`)

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETEX',
          key,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'f3',
        ]),
        ['v3'],
      )
      assert.strictEqual(await directClient.hExists(key, 'f3'), 0)

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETEX',
          key,
          'EX',
          '0',
          'FIELDS',
          '1',
          'f4',
        ]),
        ['v4'],
      )
      assert.strictEqual(await directClient.hExists(key, 'f4'), 0)

      const lone = `${tag}:lone`
      await directClient.hSet(lone, 'only', 'v')
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HGETEX',
          lone,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'only',
        ]),
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
        () =>
          directClient!.sendCommand([
            'HGETEX',
            stringKey,
            'FIELDS',
            '1',
            'field',
          ]),
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
          directClient!.sendCommand([
            'HGETEX',
            hashKey,
            'EX',
            '-5',
            'FIELDS',
            '1',
            'field',
          ]),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      await assert.rejects(
        () =>
          directClient!.sendCommand([
            'HGETEX',
            hashKey,
            'EXAT',
            '99999999999',
            'FIELDS',
            '1',
            'field',
          ]),
        errorWithMessage("ERR invalid expire time in 'hgetex' command"),
      )
    } finally {
      await directClient?.del([hashKey, stringKey])
      directClient?.destroy()
    }
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

  test('HPERSIST, HTTL and HPTTL report and clear hash field TTLs', async () => {
    const key = `{hash-field-ttl-read:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)
      await directClient.hSet(key, {
        persistent: 'value1',
        volatile: 'value2',
        soon: 'value3',
      })

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIRE',
          key,
          '5000',
          'FIELDS',
          '1',
          'volatile',
        ]),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPEXPIRE',
          key,
          '20',
          'FIELDS',
          '1',
          'soon',
        ]),
        [1],
      )

      const seconds = (await directClient.sendCommand([
        'HTTL',
        key,
        'FIELDS',
        '4',
        'persistent',
        'volatile',
        'soon',
        'missing',
      ])) as number[]
      assert.ok(Array.isArray(seconds))
      assert.strictEqual(seconds[0], -1)
      assert.strictEqual(typeof seconds[1], 'number')
      assert.ok(seconds[1] >= 0 && seconds[1] <= 5)
      assert.strictEqual(typeof seconds[2], 'number')
      assert.ok(seconds[2] >= 0 && seconds[2] <= 1)
      assert.strictEqual(seconds[3], -2)

      const milliseconds = (await directClient.sendCommand([
        'HPTTL',
        key,
        'FIELDS',
        '4',
        'persistent',
        'volatile',
        'soon',
        'missing',
      ])) as number[]
      assert.ok(Array.isArray(milliseconds))
      assert.strictEqual(milliseconds[0], -1)
      assert.strictEqual(typeof milliseconds[1], 'number')
      assert.ok(milliseconds[1] > 0 && milliseconds[1] <= 5000)
      assert.strictEqual(typeof milliseconds[2], 'number')
      assert.ok(milliseconds[2] > 0 && milliseconds[2] <= 20)
      assert.strictEqual(milliseconds[3], -2)

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPERSIST',
          key,
          'FIELDS',
          '4',
          'persistent',
          'volatile',
          'missing',
          'volatile',
        ]),
        [-1, 1, -2, -1],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPTTL',
          key,
          'FIELDS',
          '2',
          'persistent',
          'volatile',
        ]),
        [-1, -1],
      )

      await delay(60)

      assert.strictEqual(await directClient.hGet(key, 'persistent'), 'value1')
      assert.strictEqual(await directClient.hGet(key, 'volatile'), 'value2')
      assert.strictEqual(await directClient.hGet(key, 'soon'), null)
      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HTTL',
          key,
          'FIELDS',
          '3',
          'persistent',
          'volatile',
          'soon',
        ]),
        [-1, -1, -2],
      )
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HPERSIST, HTTL and HPTTL handle missing keys', async () => {
    const key = `{hash-field-ttl-missing:${randomKey()}}:hash`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, key)

      assert.deepStrictEqual(
        await directClient.sendCommand([
          'HPERSIST',
          key,
          'FIELDS',
          '2',
          'a',
          'b',
        ]),
        [-2, -2],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand(['HTTL', key, 'FIELDS', '2', 'a', 'b']),
        [-2, -2],
      )
      assert.deepStrictEqual(
        await directClient.sendCommand(['HPTTL', key, 'FIELDS', '2', 'a', 'b']),
        [-2, -2],
      )
      assert.strictEqual(await directClient.exists(key), 0)
    } finally {
      await directClient?.del(key)
      directClient?.destroy()
    }
  })

  test('HPERSIST, HTTL and HPTTL errors match Redis', async () => {
    const tag = `{hash-field-ttl-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: RedisClientType | undefined

    try {
      directClient = await connectToNodeRedisSlotOwner(redisClient, hashKey)
      await directClient.hSet(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      for (const command of ['HPERSIST', 'HTTL', 'HPTTL']) {
        const commandName = command.toLowerCase()

        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
              stringKey,
              'FIELDS',
              '1',
              'field',
            ]),
          errorWithMessage(
            'WRONGTYPE Operation against a key holding the wrong kind of value',
          ),
        )
        await assert.rejects(
          () => directClient!.sendCommand([command]),
          errorWithMessage(
            `ERR wrong number of arguments for '${commandName}' command`,
          ),
        )
        await assert.rejects(
          () => directClient!.sendCommand([command, hashKey]),
          errorWithMessage(
            `ERR wrong number of arguments for '${commandName}' command`,
          ),
        )
        await assert.rejects(
          () =>
            directClient!.sendCommand([
              command,
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
              command,
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
              command,
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
              command,
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
              command,
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
              command,
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
              command,
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
      }
    } finally {
      await directClient?.del([hashKey, stringKey])
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

  test('HINCRBY command', async () => {
    const incr1 = await redisClient.hIncrBy('hash8', 'counter', 5)
    assert.strictEqual(incr1, 5)

    const incr2 = await redisClient.hIncrBy('hash8', 'counter', 3)
    assert.strictEqual(incr2, 8)

    const incr3 = await redisClient.hIncrBy('hash8', 'counter', -2)
    assert.strictEqual(incr3, 6)
  })

  test('HINCRBY respects Redis 64-bit signed integer range', async () => {
    const key = `{hincrby64:${randomKey()}}`
    try {
      await redisClient.hSet(key, 'gap', '9007199254740992') // 2^53
      await redisClient.sendCommand(key, false, ['HINCRBY', key, 'gap', '1'])
      assert.strictEqual(await redisClient.hGet(key, 'gap'), '9007199254740993')

      await redisClient.hSet(key, 'big', '9000000000000000000')
      await redisClient.sendCommand(key, false, ['HINCRBY', key, 'big', '1'])
      assert.strictEqual(
        await redisClient.hGet(key, 'big'),
        '9000000000000000001',
      )

      await redisClient.hSet(key, 'max', '9223372036854775807')
      await assert.rejects(
        () => redisClient.sendCommand(key, false, ['HINCRBY', key, 'max', '1']),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient.hGet(key, 'max'),
        '9223372036854775807',
      )

      await redisClient.hSet(key, 'min', '-9223372036854775808')
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, false, ['HINCRBY', key, 'min', '-1']),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient.hGet(key, 'min'),
        '-9223372036854775808',
      )

      await assert.rejects(
        () =>
          redisClient.sendCommand(key, false, [
            'HINCRBY',
            key,
            'gap',
            '99999999999999999999999',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      await redisClient.hSet(key, 'huge', '99999999999999999999999')
      await assert.rejects(
        () =>
          redisClient.sendCommand(key, false, ['HINCRBY', key, 'huge', '1']),
        errorWithMessage('ERR hash value is not an integer'),
      )
    } finally {
      await redisClient.del(key)
    }
  })

  test('HINCRBYFLOAT command', async () => {
    const incr1 = await redisClient.hIncrByFloat('hash9', 'float', 1.5)
    assert.strictEqual(incr1, '1.5')

    const incr2 = await redisClient.hIncrByFloat('hash9', 'float', 2.3)
    assert.strictEqual(incr2, '3.8')
  })

  test('Hash command errors match Redis', async () => {
    const tag = `{hash-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`

    try {
      await redisClient.set(stringKey, 'value')
      await redisClient.hSet(hashKey, {
        integer: 'abc',
        float: 'abc',
        'float-trailing-garbage': '1abc',
        'float-dangling-exponent': '1.0e',
        'float-trailing-space': '1.5 ',
        'leading-zero': '007',
        'negative-zero': '-0',
      })

      await assert.rejects(
        () => redisClient.hGet(stringKey, 'field'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, ['HSET', hashKey, 'field']),
        errorWithMessage("ERR wrong number of arguments for 'hset' command"),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, [
            'HINCRBY',
            hashKey,
            'integer',
            'abc',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, [
            'HINCRBY',
            hashKey,
            'integer',
            '01',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient.hIncrBy(hashKey, 'integer', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () => redisClient.hIncrBy(hashKey, 'leading-zero', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () => redisClient.hIncrBy(hashKey, 'negative-zero', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(hashKey, false, [
            'HINCRBYFLOAT',
            hashKey,
            'float',
            'abc',
          ]),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient.hIncrByFloat(hashKey, 'float', 1.5),
        errorWithMessage('ERR hash value is not a float'),
      )
      for (const field of [
        'float-trailing-garbage',
        'float-dangling-exponent',
        'float-trailing-space',
      ]) {
        await assert.rejects(
          () => redisClient.hIncrByFloat(hashKey, field, 1),
          errorWithMessage('ERR hash value is not a float'),
        )
      }
      for (const increment of ['1abc', '1.0e', '1.5 ']) {
        await assert.rejects(
          () =>
            redisClient.sendCommand(hashKey, false, [
              'HINCRBYFLOAT',
              hashKey,
              'missing',
              increment,
            ]),
          errorWithMessage('ERR value is not a valid float'),
        )
      }
    } finally {
      await redisClient.del([hashKey, stringKey])
    }
  })

  test('Hash commands workflow - User Profile', async () => {
    const userId = 'user:1001'

    await redisClient.hSet(userId, {
      name: 'Alice Johnson',
      email: 'alice@example.com',
      score: '0',
      level: '1',
      coins: '100.50',
    })

    const exists = await redisClient.hExists(userId, 'name')
    assert.strictEqual(exists, 1)

    const userData = await redisClient.hmGet(userId, ['name', 'email', 'score'])
    assert.deepStrictEqual(userData, [
      'Alice Johnson',
      'alice@example.com',
      '0',
    ])

    await redisClient.hIncrBy(userId, 'score', 150)
    await redisClient.hIncrBy(userId, 'level', 1)
    await redisClient.hIncrByFloat(userId, 'coins', 25.75)

    const profile = await redisClient.hGetAll(userId)
    assert.strictEqual(profile.name, 'Alice Johnson')
    assert.strictEqual(profile.score, '150')
    assert.strictEqual(profile.level, '2')
    assert.strictEqual(profile.coins, '126.25')

    const profileSize = await redisClient.hLen(userId)
    assert.strictEqual(profileSize, 5)

    const fields = await redisClient.hKeys(userId)
    assert.ok(fields.includes('name'))
    assert.ok(fields.includes('email'))
    assert.ok(fields.includes('score'))

    await redisClient.hSet(userId, 'old_email', profile.email || '')
    await redisClient.hDel(userId, 'email')

    const emailExists = await redisClient.hExists(userId, 'email')
    const oldEmailExists = await redisClient.hExists(userId, 'old_email')
    assert.strictEqual(emailExists, 0)
    assert.strictEqual(oldEmailExists, 1)
  })

  test('Hash commands workflow - Shopping Cart', async () => {
    const cartId = 'cart:session123'

    await redisClient.hSet(cartId, 'item:001', '2') // quantity 2
    await redisClient.hSet(cartId, 'item:002', '1') // quantity 1
    await redisClient.hSet(cartId, 'item:003', '3') // quantity 3

    await redisClient.hIncrBy(cartId, 'item:001', 1) // now 3

    const cart = await redisClient.hGetAll(cartId)
    assert.strictEqual(cart['item:001'], '3')
    assert.strictEqual(cart['item:002'], '1')
    assert.strictEqual(cart['item:003'], '3')

    await redisClient.hDel(cartId, 'item:002')

    const cartSize = await redisClient.hLen(cartId)
    assert.strictEqual(cartSize, 2)

    const items = await redisClient.hKeys(cartId)
    assert.deepStrictEqual(items.sort(), ['item:001', 'item:003'])

    const quantities = await redisClient.hVals(cartId)
    assert.deepStrictEqual(quantities.sort(), ['3', '3'])
  })
})
