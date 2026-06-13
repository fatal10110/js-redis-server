import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../test-config'
import {
  commandFrame,
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
      await redisClient?.hset(hashKey, 'integer', 'abc', 'float', 'abc')

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
        () => redisClient?.hincrby(hashKey, 'integer', 1),
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
