import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster, Redis } from 'ioredis'
import { TestRunner } from '../../test-config'
import {
  commandFrame,
  connectToSlotOwner,
  errorWithMessage,
  findSlotOwner,
  randomKey,
} from '../../utils'
import {
  RawRedisConnection,
  respMapGet,
  respText,
} from '../../raw-tcp/raw-connection'

const testRunner = new TestRunner()

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
})
