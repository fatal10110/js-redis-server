import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { createClient, RedisClientType, RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  findNodeRedisSlotOwner,
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
        () => directClient!.hRandField(stringKey),
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
})
