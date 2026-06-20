import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import {
  commandFrame,
  connectToSlotOwner,
  errorWithMessage,
  findSlotOwner,
  randomKey,
} from '../utils'
import {
  RawRedisConnection,
  respMapGet,
  respText,
} from '../raw-tcp/raw-connection'

const testRunner = new TestRunner()

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe(`Hash Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('hash-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('HSET and HGET commands', async () => {
    // HSET single field
    const result1 = await redisClient?.hset('hash1', 'field1', 'value1')
    assert.strictEqual(result1, 1)

    // HGET
    const value = await redisClient?.hget('hash1', 'field1')
    assert.strictEqual(value, 'value1')

    // HSET multiple fields
    const result2 = await redisClient?.hset(
      'hash1',
      'field2',
      'value2',
      'field3',
      'value3',
    )
    assert.strictEqual(result2, 2)
  })

  test('HMSET and HMGET commands', async () => {
    // HMSET
    await redisClient?.hmset(
      'hash2',
      'field1',
      'value1',
      'field2',
      'value2',
      'field3',
      'value3',
    )

    // HMGET
    const values = await redisClient?.hmget(
      'hash2',
      'field1',
      'field2',
      'nonexistent',
    )
    assert.deepStrictEqual(values, ['value1', 'value2', null])
  })

  test('HGETALL command', async () => {
    await redisClient?.hset('hash3', 'field1', 'value1', 'field2', 'value2')

    const all = await redisClient?.hgetall('hash3')
    assert.deepStrictEqual(all, { field1: 'value1', field2: 'value2' })
  })

  test('HGETALL returns a RESP3 map after HELLO 3', async () => {
    assert.ok(redisClient)

    const key = `{hash-resp3:${randomKey()}}:hash`
    const [host, port] = await findSlotOwner(redisClient, key)
    const connection = await RawRedisConnection.connect(host, port)

    try {
      connection.write(commandFrame('HELLO', '3'))
      assert.ok((await connection.readFrame()) instanceof Map)

      connection.write(
        commandFrame('HSET', key, 'field1', 'value1', 'field2', 'value2'),
      )
      assert.strictEqual(await connection.readFrame(), 2)

      connection.write(commandFrame('HGETALL', key))
      const reply = await connection.readFrame()
      assert.ok(reply instanceof Map)
      assert.strictEqual(respText(respMapGet(reply, 'field1')), 'value1')
      assert.strictEqual(respText(respMapGet(reply, 'field2')), 'value2')
    } finally {
      connection.close()
      await redisClient.del(key)
    }
  })

  test('HKEYS and HVALS commands', async () => {
    await redisClient?.hset('hash4', 'field1', 'value1', 'field2', 'value2')

    const keys = await redisClient?.hkeys('hash4')
    assert.deepStrictEqual(keys?.sort(), ['field1', 'field2'])

    const vals = await redisClient?.hvals('hash4')
    assert.deepStrictEqual(vals?.sort(), ['value1', 'value2'])
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
    let directClient: Redis | undefined

    function assertField(value: unknown): string {
      assert.strictEqual(typeof value, 'string')
      assert.ok(expected.has(value))
      return value
    }

    function assertFieldArray(value: unknown, length: number): string[] {
      assert.ok(Array.isArray(value))
      assert.strictEqual(value.length, length)
      for (const field of value) assertField(field)
      return value as string[]
    }

    function assertFieldValuePairs(value: unknown, pairCount: number): void {
      assert.ok(Array.isArray(value))
      assert.strictEqual(value.length, pairCount * 2)
      for (let i = 0; i < value.length; i += 2) {
        const field = assertField(value[i])
        assert.strictEqual(value[i + 1], expected.get(field))
      }
    }

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, hashKey)
      await directClient.hset(
        hashKey,
        'field1',
        'value1',
        'field2',
        'value2',
        'field3',
        'value3',
      )

      assertField(await directClient.call('HRANDFIELD', hashKey))
      assert.strictEqual(
        await directClient.call('HRANDFIELD', missingKey),
        null,
      )

      const twoFields = assertFieldArray(
        await directClient.call('HRANDFIELD', hashKey, '2'),
        2,
      )
      assert.strictEqual(new Set(twoFields).size, 2)

      const allFields = assertFieldArray(
        await directClient.call('HRANDFIELD', hashKey, '10'),
        3,
      )
      assert.strictEqual(new Set(allFields).size, 3)

      assertFieldArray(await directClient.call('HRANDFIELD', hashKey, '-5'), 5)
      assert.deepStrictEqual(
        await directClient.call('HRANDFIELD', hashKey, '0'),
        [],
      )
      assert.deepStrictEqual(
        await directClient.call('HRANDFIELD', missingKey, '2'),
        [],
      )

      assertFieldValuePairs(
        await directClient.call('HRANDFIELD', hashKey, '2', 'WITHVALUES'),
        2,
      )
      assertFieldValuePairs(
        await directClient.call('HRANDFIELD', hashKey, '-4', 'withvalues'),
        4,
      )
      assert.deepStrictEqual(
        await directClient.call('HRANDFIELD', missingKey, '2', 'WITHVALUES'),
        [],
      )

      await directClient.set(stringKey, 'value')
      await assert.rejects(
        () => directClient?.call('HRANDFIELD', stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )

      await assert.rejects(
        () => directClient?.call('HRANDFIELD'),
        errorWithMessage(
          "ERR wrong number of arguments for 'hrandfield' command",
        ),
      )
      await assert.rejects(
        () => directClient?.call('HRANDFIELD', hashKey, 'abc'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient?.call('HRANDFIELD', hashKey, 'WITHVALUES'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => directClient?.call('HRANDFIELD', hashKey, '1', 'BAD'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HRANDFIELD',
            hashKey,
            '1',
            'WITHVALUES',
            'WITHVALUES',
          ),
        errorWithMessage('ERR syntax error'),
      )
    } finally {
      await directClient?.del(hashKey, missingKey, stringKey)
      directClient?.disconnect()
    }
  })

  test('HLEN command', async () => {
    // Empty hash
    const len1 = await redisClient?.hlen('emptyhash')
    assert.strictEqual(len1, 0)

    await redisClient?.hset('hash5', 'field1', 'value1', 'field2', 'value2')
    const len2 = await redisClient?.hlen('hash5')
    assert.strictEqual(len2, 2)
  })

  test('HEXISTS command', async () => {
    await redisClient?.hset('hash6', 'field1', 'value1')

    const exists1 = await redisClient?.hexists('hash6', 'field1')
    assert.strictEqual(exists1, 1)

    const exists2 = await redisClient?.hexists('hash6', 'field2')
    assert.strictEqual(exists2, 0)
  })

  test('HSETNX command', async () => {
    // HSETNX on new field
    const result1 = await redisClient?.hsetnx('hashsetnx', 'field1', 'value1')
    assert.strictEqual(result1, 1)

    // Verify field was set
    const value1 = await redisClient?.hget('hashsetnx', 'field1')
    assert.strictEqual(value1, 'value1')

    // HSETNX on existing field (should fail)
    const result2 = await redisClient?.hsetnx('hashsetnx', 'field1', 'value2')
    assert.strictEqual(result2, 0)

    // Verify field was not changed
    const value2 = await redisClient?.hget('hashsetnx', 'field1')
    assert.strictEqual(value2, 'value1')

    // HSETNX on different field in same hash
    const result3 = await redisClient?.hsetnx('hashsetnx', 'field2', 'value2')
    assert.strictEqual(result3, 1)

    // Verify both fields exist
    const len = await redisClient?.hlen('hashsetnx')
    assert.strictEqual(len, 2)
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

  test('HGETEX returns values and updates field TTLs like Redis', async () => {
    const tag = `{hgetex:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(
        key,
        'f1',
        'v1',
        'f2',
        'v2',
        'f3',
        'v3',
        'f4',
        'v4',
      )

      // Plain read behaves like HMGET, including nil for missing fields.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          key,
          'FIELDS',
          '3',
          'f1',
          'missing',
          'f2',
        ),
        ['v1', null, 'v2'],
      )
      // Missing key yields all-nil without creating the hash.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          `${tag}:nokey`,
          'FIELDS',
          '2',
          'a',
          'b',
        ),
        [null, null],
      )
      assert.strictEqual(await directClient.exists(`${tag}:nokey`), 0)

      // EX sets a relative TTL and returns the value.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          key,
          'EX',
          '100',
          'FIELDS',
          '1',
          'f1',
        ),
        ['v1'],
      )
      const ttlSet = (await directClient.call(
        'HTTL',
        key,
        'FIELDS',
        '1',
        'f1',
      )) as number[]
      assert.ok(ttlSet[0] > 90 && ttlSet[0] <= 100, `ttl ${ttlSet[0]}`)

      // No expiration clause leaves the existing TTL untouched.
      assert.deepStrictEqual(
        await directClient.call('HGETEX', key, 'FIELDS', '1', 'f1'),
        ['v1'],
      )
      const ttlKeep = (await directClient.call(
        'HTTL',
        key,
        'FIELDS',
        '1',
        'f1',
      )) as number[]
      assert.ok(ttlKeep[0] > 90 && ttlKeep[0] <= 100, `ttl ${ttlKeep[0]}`)

      // PERSIST clears the TTL.
      assert.deepStrictEqual(
        await directClient.call('HGETEX', key, 'PERSIST', 'FIELDS', '1', 'f1'),
        ['v1'],
      )
      assert.deepStrictEqual(
        await directClient.call('HTTL', key, 'FIELDS', '1', 'f1'),
        [-1],
      )

      // PX sets a millisecond TTL.
      await directClient.call('HGETEX', key, 'PX', '50000', 'FIELDS', '1', 'f2')
      const pttlSet = (await directClient.call(
        'HPTTL',
        key,
        'FIELDS',
        '1',
        'f2',
      )) as number[]
      assert.ok(pttlSet[0] > 40000 && pttlSet[0] <= 50000, `pttl ${pttlSet[0]}`)

      // EXAT in the past returns the value but deletes the field.
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          key,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'f3',
        ),
        ['v3'],
      )
      assert.strictEqual(await directClient.hexists(key, 'f3'), 0)

      // EX 0 expires immediately: value is returned, field removed.
      assert.deepStrictEqual(
        await directClient.call('HGETEX', key, 'EX', '0', 'FIELDS', '1', 'f4'),
        ['v4'],
      )
      assert.strictEqual(await directClient.hexists(key, 'f4'), 0)

      // Expiring the last field removes the key entirely.
      const lone = `${tag}:lone`
      await directClient.hset(lone, 'only', 'v')
      assert.deepStrictEqual(
        await directClient.call(
          'HGETEX',
          lone,
          'EXAT',
          '1',
          'FIELDS',
          '1',
          'only',
        ),
        ['v'],
      )
      assert.strictEqual(await directClient.exists(lone), 0)
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HGETEX errors match Redis', async () => {
    const tag = `{hgetex-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, hashKey)
      await directClient.hset(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () => directClient?.call('HGETEX', stringKey, 'FIELDS', '1', 'field'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX'),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', '1'),
        errorWithMessage("ERR wrong number of arguments for 'hgetex' command"),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELD', '1', 'field'),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      // KEEPTTL is not a valid HGETEX option.
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'KEEPTTL',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      // Only one expiration clause is allowed.
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EX',
            '100',
            'PERSIST',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', 'abc', 'field'),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', '0', 'field'),
        errorWithMessage('ERR Number of fields must be a positive integer'),
      )
      await assert.rejects(
        () => directClient?.call('HGETEX', hashKey, 'FIELDS', '2', 'field'),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EX',
            'abc',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EX',
            '-5',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HGETEX',
            hashKey,
            'EXAT',
            '99999999999',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage("ERR invalid expire time in 'hgetex' command"),
      )
    } finally {
      await directClient?.del(hashKey, stringKey)
      directClient?.disconnect()
    }
  })

  test('HEXPIRE family sets and lazily expires hash fields', async () => {
    const tag = `{hexpire:${randomKey()}}`
    const key = `${tag}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(
        key,
        'keep',
        'value1',
        'soon',
        'value2',
        'zero',
        'value3',
        'past',
        'value4',
        'pxat',
        'value5',
      )

      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '10',
          'FIELDS',
          '2',
          'keep',
          'missing',
        ),
        [1, -2],
      )
      assert.deepStrictEqual(
        await directClient.call('HPEXPIRE', key, '0', 'FIELDS', '1', 'zero'),
        [2],
      )
      assert.deepStrictEqual(
        await directClient.call('HEXPIREAT', key, '1', 'FIELDS', '1', 'past'),
        [2],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HPEXPIREAT',
          key,
          String(Date.now() + 10_000),
          'FIELDS',
          '1',
          'pxat',
        ),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.call('HPEXPIRE', key, '5', 'FIELDS', '1', 'soon'),
        [1],
      )

      await delay(40)

      assert.strictEqual(await directClient.hget(key, 'soon'), null)
      assert.deepStrictEqual(
        await directClient.hmget(key, 'keep', 'soon', 'zero', 'past', 'pxat'),
        ['value1', null, null, null, 'value5'],
      )
      assert.strictEqual(await directClient.hexists(key, 'soon'), 0)
      assert.strictEqual(await directClient.hstrlen(key, 'soon'), 0)
      assert.strictEqual(await directClient.hlen(key), 2)
      assert.deepStrictEqual((await directClient.hkeys(key)).sort(), [
        'keep',
        'pxat',
      ])
      assert.deepStrictEqual((await directClient.hvals(key)).sort(), [
        'value1',
        'value5',
      ])
      assert.deepStrictEqual(await directClient.hgetall(key), {
        keep: 'value1',
        pxat: 'value5',
      })
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HSCAN omits expired hash fields', async () => {
    const key = `{hexpire-hscan:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(key, 'keep', 'value1', 'gone', 'value2')

      assert.deepStrictEqual(
        await directClient.call('HPEXPIRE', key, '5', 'FIELDS', '1', 'gone'),
        [1],
      )

      await delay(40)

      const [, scanEntries] = (await directClient.call('HSCAN', key, '0')) as [
        string,
        string[],
      ]
      const scanned = new Map<string, string>()
      for (let i = 0; i < scanEntries.length; i += 2) {
        scanned.set(scanEntries[i], scanEntries[i + 1])
      }

      assert.deepStrictEqual(Object.fromEntries(scanned), { keep: 'value1' })
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HEXPIRE deletes the key when every field expires', async () => {
    const key = `{hexpire-empty:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(key, 'field1', 'value1', 'field2', 'value2')

      assert.deepStrictEqual(
        await directClient.call(
          'HPEXPIRE',
          key,
          '0',
          'FIELDS',
          '2',
          'field1',
          'field2',
        ),
        [2, 2],
      )
      assert.strictEqual(await directClient.exists(key), 0)
      assert.strictEqual(await directClient.type(key), 'none')
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HEXPIRE condition flags match Redis', async () => {
    const key = `{hexpire-options:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(
        key,
        'volatile',
        'value1',
        'nx',
        'value2',
        'xx',
        'value3',
        'lt',
        'value4',
      )

      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '20',
          'FIELDS',
          '1',
          'volatile',
        ),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '30',
          'NX',
          'FIELDS',
          '1',
          'volatile',
        ),
        [0],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '30',
          'NX',
          'FIELDS',
          '1',
          'nx',
        ),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '30',
          'XX',
          'FIELDS',
          '1',
          'xx',
        ),
        [0],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '30',
          'XX',
          'FIELDS',
          '1',
          'volatile',
        ),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '10',
          'GT',
          'FIELDS',
          '1',
          'nx',
        ),
        [0],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '40',
          'GT',
          'FIELDS',
          '1',
          'nx',
        ),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '10',
          'LT',
          'FIELDS',
          '1',
          'lt',
        ),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HEXPIRE',
          key,
          '40',
          'LT',
          'FIELDS',
          '1',
          'lt',
        ),
        [0],
      )
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HPERSIST, HTTL and HPTTL report and clear hash field TTLs', async () => {
    const key = `{hash-field-ttl-read:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(
        key,
        'persistent',
        'value1',
        'volatile',
        'value2',
        'soon',
        'value3',
      )

      assert.deepStrictEqual(
        await directClient.call(
          'HPEXPIRE',
          key,
          '5000',
          'FIELDS',
          '1',
          'volatile',
        ),
        [1],
      )
      assert.deepStrictEqual(
        await directClient.call('HPEXPIRE', key, '20', 'FIELDS', '1', 'soon'),
        [1],
      )

      const seconds = await directClient.call(
        'HTTL',
        key,
        'FIELDS',
        '4',
        'persistent',
        'volatile',
        'soon',
        'missing',
      )
      assert.ok(Array.isArray(seconds))
      assert.strictEqual(seconds[0], -1)
      assert.strictEqual(typeof seconds[1], 'number')
      assert.ok(seconds[1] >= 0 && seconds[1] <= 5)
      assert.strictEqual(typeof seconds[2], 'number')
      assert.ok(seconds[2] >= 0 && seconds[2] <= 1)
      assert.strictEqual(seconds[3], -2)

      const milliseconds = await directClient.call(
        'HPTTL',
        key,
        'FIELDS',
        '4',
        'persistent',
        'volatile',
        'soon',
        'missing',
      )
      assert.ok(Array.isArray(milliseconds))
      assert.strictEqual(milliseconds[0], -1)
      assert.strictEqual(typeof milliseconds[1], 'number')
      assert.ok(milliseconds[1] > 0 && milliseconds[1] <= 5000)
      assert.strictEqual(typeof milliseconds[2], 'number')
      assert.ok(milliseconds[2] > 0 && milliseconds[2] <= 20)
      assert.strictEqual(milliseconds[3], -2)

      assert.deepStrictEqual(
        await directClient.call(
          'HPERSIST',
          key,
          'FIELDS',
          '4',
          'persistent',
          'volatile',
          'missing',
          'volatile',
        ),
        [-1, 1, -2, -1],
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HPTTL',
          key,
          'FIELDS',
          '2',
          'persistent',
          'volatile',
        ),
        [-1, -1],
      )

      await delay(60)

      assert.strictEqual(await directClient.hget(key, 'persistent'), 'value1')
      assert.strictEqual(await directClient.hget(key, 'volatile'), 'value2')
      assert.strictEqual(await directClient.hget(key, 'soon'), null)
      assert.deepStrictEqual(
        await directClient.call(
          'HTTL',
          key,
          'FIELDS',
          '3',
          'persistent',
          'volatile',
          'soon',
        ),
        [-1, -1, -2],
      )
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HPERSIST, HTTL and HPTTL handle missing keys', async () => {
    const key = `{hash-field-ttl-missing:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)

      assert.deepStrictEqual(
        await directClient.call('HPERSIST', key, 'FIELDS', '2', 'a', 'b'),
        [-2, -2],
      )
      assert.deepStrictEqual(
        await directClient.call('HTTL', key, 'FIELDS', '2', 'a', 'b'),
        [-2, -2],
      )
      assert.deepStrictEqual(
        await directClient.call('HPTTL', key, 'FIELDS', '2', 'a', 'b'),
        [-2, -2],
      )
      assert.strictEqual(await directClient.exists(key), 0)
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HPERSIST, HTTL and HPTTL errors match Redis', async () => {
    const tag = `{hash-field-ttl-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, hashKey)
      await directClient.hset(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      for (const command of ['HPERSIST', 'HTTL', 'HPTTL']) {
        const commandName = command.toLowerCase()

        await assert.rejects(
          () => directClient?.call(command, stringKey, 'FIELDS', '1', 'field'),
          errorWithMessage(
            'WRONGTYPE Operation against a key holding the wrong kind of value',
          ),
        )
        await assert.rejects(
          () => directClient?.call(command),
          errorWithMessage(
            `ERR wrong number of arguments for '${commandName}' command`,
          ),
        )
        await assert.rejects(
          () => directClient?.call(command, hashKey),
          errorWithMessage(
            `ERR wrong number of arguments for '${commandName}' command`,
          ),
        )
        await assert.rejects(
          () => directClient?.call(command, hashKey, 'FIELD', '1', 'field'),
          errorWithMessage(
            'ERR Mandatory argument FIELDS is missing or not at the right position',
          ),
        )
        await assert.rejects(
          () => directClient?.call(command, hashKey, 'FIELDS', 'abc', 'field'),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () => directClient?.call(command, hashKey, 'FIELDS', '0', 'field'),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () => directClient?.call(command, hashKey, 'FIELDS', '-1', 'field'),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () =>
            directClient?.call(
              command,
              hashKey,
              'FIELDS',
              '9223372036854775808',
              'field',
            ),
          errorWithMessage('ERR Number of fields must be a positive integer'),
        )
        await assert.rejects(
          () => directClient?.call(command, hashKey, 'FIELDS', '2', 'field'),
          errorWithMessage(
            'ERR The `numfields` parameter must match the number of arguments',
          ),
        )
        await assert.rejects(
          () =>
            directClient?.call(
              command,
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
      }
    } finally {
      await directClient?.del(hashKey, stringKey)
      directClient?.disconnect()
    }
  })

  test('hash writes clear or preserve field TTLs like Redis', async () => {
    const key = `{hash-field-ttl-writes:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)
      await directClient.hset(
        key,
        'replace',
        'old',
        'counter',
        '1',
        'float',
        '1.5',
      )
      assert.deepStrictEqual(
        await directClient.call(
          'HPEXPIRE',
          key,
          '20',
          'FIELDS',
          '3',
          'replace',
          'counter',
          'float',
        ),
        [1, 1, 1],
      )

      assert.strictEqual(await directClient.hset(key, 'replace', 'new'), 0)
      assert.strictEqual(await directClient.hincrby(key, 'counter', 1), 2)
      assert.strictEqual(
        await directClient.hincrbyfloat(key, 'float', 1),
        '2.5',
      )

      await delay(50)

      assert.strictEqual(await directClient.hget(key, 'replace'), 'new')
      assert.strictEqual(await directClient.hget(key, 'counter'), null)
      assert.strictEqual(await directClient.hget(key, 'float'), null)
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HEXPIRE errors match Redis', async () => {
    const tag = `{hexpire-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, hashKey)
      await directClient.hset(hashKey, 'field', 'value')
      await directClient.set(stringKey, 'value')

      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            stringKey,
            '10',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            stringKey,
            'abc',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            stringKey,
            '100',
            'FIELDS',
            '5',
            'field',
          ),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            stringKey,
            '100',
            'FIELDS',
            '0',
            'field',
          ),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => directClient?.call('HEXPIRE'),
        errorWithMessage("ERR wrong number of arguments for 'hexpire' command"),
      )
      await assert.rejects(
        () => directClient?.call('HEXPIRE', hashKey, '10'),
        errorWithMessage("ERR wrong number of arguments for 'hexpire' command"),
      )
      await assert.rejects(
        () =>
          directClient?.call('HEXPIRE', hashKey, 'abc', 'FIELDS', '1', 'field'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          directClient?.call('HEXPIRE', hashKey, '-1', 'FIELDS', '1', 'field'),
        errorWithMessage('ERR invalid expire time, must be >= 0'),
      )
      assert.strictEqual(await directClient.hget(hashKey, 'field'), 'value')
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            hashKey,
            '99999999999',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage("ERR invalid expire time in 'hexpire' command"),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIREAT',
            hashKey,
            '9999999999999999',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage("ERR invalid expire time in 'hexpireat' command"),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            hashKey,
            '9223372036854775807',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage("ERR invalid expire time in 'hexpire' command"),
      )
      await assert.rejects(
        () => directClient?.call('HEXPIRE', hashKey, '100', '1', 'field'),
        errorWithMessage("ERR wrong number of arguments for 'hexpire' command"),
      )
      await assert.rejects(
        () =>
          directClient?.call('HEXPIRE', hashKey, '10', 'FIELD', '1', 'field'),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call('HEXPIRE', hashKey, '100', '1', 'FIELDS', 'field'),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            hashKey,
            '10',
            'FIELDS',
            'abc',
            'field',
          ),
        errorWithMessage('ERR Parameter `numFields` should be greater than 0'),
      )
      await assert.rejects(
        () =>
          directClient?.call('HEXPIRE', hashKey, '10', 'FIELDS', '0', 'field'),
        errorWithMessage('ERR Parameter `numFields` should be greater than 0'),
      )
      await assert.rejects(
        () =>
          directClient?.call('HEXPIRE', hashKey, '10', 'FIELDS', '2', 'field'),
        errorWithMessage(
          'ERR The `numfields` parameter must match the number of arguments',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            hashKey,
            '10',
            'NX',
            'XX',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            hashKey,
            '10',
            'GT',
            'LT',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
      await assert.rejects(
        () =>
          directClient?.call(
            'HEXPIRE',
            hashKey,
            '10',
            'BOGUS',
            'FIELDS',
            '1',
            'field',
          ),
        errorWithMessage(
          'ERR Mandatory argument FIELDS is missing or not at the right position',
        ),
      )
    } finally {
      await directClient?.del(hashKey, stringKey)
      directClient?.disconnect()
    }
  })

  test('HEXPIRE arg-content errors stay inline inside MULTI/EXEC', async () => {
    const key = `{hexpire-multi-errors:${randomKey()}}:hash`
    let directClient: Redis | undefined

    try {
      assert.ok(redisClient)
      directClient = await connectToSlotOwner(redisClient, key)

      assert.strictEqual(await directClient.call('MULTI'), 'OK')
      assert.strictEqual(
        await directClient.call('HSET', key, 'field', 'value'),
        'QUEUED',
      )
      assert.strictEqual(
        await directClient.call('HEXPIRE', key, 'abc', 'FIELDS', '1', 'field'),
        'QUEUED',
      )
      assert.strictEqual(
        await directClient.call('HGET', key, 'field'),
        'QUEUED',
      )

      const result = await directClient.call('EXEC')

      assert.ok(Array.isArray(result))
      assert.strictEqual(result.length, 3)
      assert.strictEqual(result[0], 1)
      assert.ok(result[1] instanceof Error)
      assert.strictEqual(
        result[1].message,
        'ERR value is not an integer or out of range',
      )
      assert.strictEqual(result[2], 'value')
    } finally {
      await directClient?.del(key)
      directClient?.disconnect()
    }
  })

  test('HINCRBY command', async () => {
    // HINCRBY on non-existent field
    const incr1 = await redisClient?.hincrby('hash8', 'counter', 5)
    assert.strictEqual(incr1, 5)

    // HINCRBY on existing field
    const incr2 = await redisClient?.hincrby('hash8', 'counter', 3)
    assert.strictEqual(incr2, 8)

    // Negative increment
    const incr3 = await redisClient?.hincrby('hash8', 'counter', -2)
    assert.strictEqual(incr3, 6)
  })

  test('HINCRBY respects Redis 64-bit signed integer range', async () => {
    const key = `{hincrby64:${randomKey()}}`
    try {
      // Values in the gap between 2^53 and 2^63 must keep full precision
      // (JS Number.isSafeInteger() would wrongly reject these).
      await redisClient?.hset(key, 'gap', '9007199254740992') // 2^53
      await redisClient?.call('HINCRBY', key, 'gap', '1')
      assert.strictEqual(
        await redisClient?.hget(key, 'gap'),
        '9007199254740993',
      )

      // Large value still inside int64 — no overflow (issue #29 wrongly
      // claimed this overflows; real Redis returns 9000000000000000001).
      await redisClient?.hset(key, 'big', '9000000000000000000')
      await redisClient?.call('HINCRBY', key, 'big', '1')
      assert.strictEqual(
        await redisClient?.hget(key, 'big'),
        '9000000000000000001',
      )

      // Positive overflow past INT64_MAX (2^63-1) is rejected, value untouched.
      await redisClient?.hset(key, 'max', '9223372036854775807')
      await assert.rejects(
        () => redisClient?.call('HINCRBY', key, 'max', '1'),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient?.hget(key, 'max'),
        '9223372036854775807',
      )

      // Negative overflow past INT64_MIN (-2^63) is rejected, value untouched.
      await redisClient?.hset(key, 'min', '-9223372036854775808')
      await assert.rejects(
        () => redisClient?.call('HINCRBY', key, 'min', '-1'),
        errorWithMessage('ERR increment or decrement would overflow'),
      )
      assert.strictEqual(
        await redisClient?.hget(key, 'min'),
        '-9223372036854775808',
      )

      // Increment argument outside int64 range is a value error.
      await assert.rejects(
        () =>
          redisClient?.call('HINCRBY', key, 'gap', '99999999999999999999999'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      // Stored field value outside int64 range is "hash value is not an integer".
      await redisClient?.hset(key, 'huge', '99999999999999999999999')
      await assert.rejects(
        () => redisClient?.call('HINCRBY', key, 'huge', '1'),
        errorWithMessage('ERR hash value is not an integer'),
      )
    } finally {
      await redisClient?.del(key)
    }
  })

  test('HINCRBYFLOAT command', async () => {
    // HINCRBYFLOAT on non-existent field
    const incr1 = await redisClient?.hincrbyfloat('hash9', 'float', 1.5)
    assert.strictEqual(incr1, '1.5')

    // HINCRBYFLOAT on existing field
    const incr2 = await redisClient?.hincrbyfloat('hash9', 'float', 2.3)
    assert.strictEqual(incr2, '3.8')
  })

  test('Hash command errors match Redis', async () => {
    const tag = `{hash-errors:${randomKey()}}`
    const hashKey = `${tag}:hash`
    const stringKey = `${tag}:string`

    try {
      await redisClient?.set(stringKey, 'value')
      await redisClient?.hset(
        hashKey,
        'integer',
        'abc',
        'float',
        'abc',
        'float-trailing-garbage',
        '1abc',
        'float-dangling-exponent',
        '1.0e',
        'float-trailing-space',
        '1.5 ',
        'leading-zero',
        '007',
        'negative-zero',
        '-0',
      )

      await assert.rejects(
        () => redisClient?.hget(stringKey, 'field'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient?.call('HSET', hashKey, 'field'),
        errorWithMessage("ERR wrong number of arguments for 'hset' command"),
      )
      await assert.rejects(
        () => redisClient?.call('HINCRBY', hashKey, 'integer', 'abc'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient?.call('HINCRBY', hashKey, 'integer', '01'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient?.hincrby(hashKey, 'integer', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () => redisClient?.hincrby(hashKey, 'leading-zero', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () => redisClient?.hincrby(hashKey, 'negative-zero', 1),
        errorWithMessage('ERR hash value is not an integer'),
      )
      await assert.rejects(
        () => redisClient?.call('HINCRBYFLOAT', hashKey, 'float', 'abc'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient?.hincrbyfloat(hashKey, 'float', 1.5),
        errorWithMessage('ERR hash value is not a float'),
      )
      for (const field of [
        'float-trailing-garbage',
        'float-dangling-exponent',
        'float-trailing-space',
      ]) {
        await assert.rejects(
          () => redisClient?.hincrbyfloat(hashKey, field, 1),
          errorWithMessage('ERR hash value is not a float'),
        )
      }
      for (const increment of ['1abc', '1.0e', '1.5 ']) {
        await assert.rejects(
          () =>
            redisClient?.call('HINCRBYFLOAT', hashKey, 'missing', increment),
          errorWithMessage('ERR value is not a valid float'),
        )
      }
    } finally {
      await redisClient?.del(hashKey, stringKey)
    }
  })

  test('Hash commands workflow - User Profile', async () => {
    const userId = 'user:1001'

    // Create user profile
    await redisClient?.hmset(
      userId,
      'name',
      'Alice Johnson',
      'email',
      'alice@example.com',
      'score',
      '0',
      'level',
      '1',
      'coins',
      '100.50',
    )

    // Check profile exists
    const exists = await redisClient?.hexists(userId, 'name')
    assert.strictEqual(exists, 1)

    // Get user data
    const userData = await redisClient?.hmget(userId, 'name', 'email', 'score')
    assert.deepStrictEqual(userData, [
      'Alice Johnson',
      'alice@example.com',
      '0',
    ])

    // Update score and level
    await redisClient?.hincrby(userId, 'score', 150)
    await redisClient?.hincrby(userId, 'level', 1)

    // Add coins (float)
    await redisClient?.hincrbyfloat(userId, 'coins', 25.75)

    // Get updated profile
    const profile = await redisClient?.hgetall(userId)
    assert.strictEqual(profile?.name, 'Alice Johnson')
    assert.strictEqual(profile?.score, '150')
    assert.strictEqual(profile?.level, '2')
    assert.strictEqual(profile?.coins, '126.25')

    // Check profile size
    const profileSize = await redisClient?.hlen(userId)
    assert.strictEqual(profileSize, 5)

    // Get all field names
    const fields = await redisClient?.hkeys(userId)
    assert.ok(fields?.includes('name'))
    assert.ok(fields?.includes('email'))
    assert.ok(fields?.includes('score'))

    // Archive old email
    await redisClient?.hset(userId, 'old_email', profile?.email || '')
    await redisClient?.hdel(userId, 'email')

    // Verify email removed but old_email added
    const emailExists = await redisClient?.hexists(userId, 'email')
    const oldEmailExists = await redisClient?.hexists(userId, 'old_email')
    assert.strictEqual(emailExists, 0)
    assert.strictEqual(oldEmailExists, 1)
  })

  test('Hash commands workflow - Shopping Cart', async () => {
    const cartId = 'cart:session123'

    // Add items to cart
    await redisClient?.hset(cartId, 'item:001', '2') // quantity 2
    await redisClient?.hset(cartId, 'item:002', '1') // quantity 1
    await redisClient?.hset(cartId, 'item:003', '3') // quantity 3

    // Update item quantity
    await redisClient?.hincrby(cartId, 'item:001', 1) // now 3

    // Get cart contents
    const cart = await redisClient?.hgetall(cartId)
    assert.strictEqual(cart?.['item:001'], '3')
    assert.strictEqual(cart?.['item:002'], '1')
    assert.strictEqual(cart?.['item:003'], '3')

    // Remove an item
    await redisClient?.hdel(cartId, 'item:002')

    // Check final cart size
    const cartSize = await redisClient?.hlen(cartId)
    assert.strictEqual(cartSize, 2)

    // Get remaining items
    const items = await redisClient?.hkeys(cartId)
    assert.deepStrictEqual(items?.sort(), ['item:001', 'item:003'])

    // Get quantities
    const quantities = await redisClient?.hvals(cartId)
    assert.deepStrictEqual(quantities?.sort(), ['3', '3'])
  })
})
