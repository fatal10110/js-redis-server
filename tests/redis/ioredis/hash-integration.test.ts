import { test, describe, before, after, afterEach } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'

describe('Hash Commands Integration', () => {
  const redisCluster = new ClusterNetwork(console)
  let redisClient: Cluster | undefined

  before(async () => {
    await redisCluster.init({ masters: 3, slaves: 0 })
    redisClient = new Redis.Cluster(
      [
        {
          host: '127.0.0.1',
          port: Array.from(redisCluster.getAll())[0].port,
        },
      ],
      {
        slotsRefreshTimeout: 10000000,
        lazyConnect: true,
      },
    )
    await redisClient?.connect()
  })

  after(async () => {
    await redisClient?.quit()
    await redisCluster.shutdown()
  })

  afterEach(async () => {
    // Clean up database after each test
    const masterNodes = redisClient?.nodes('master') || []
    if (masterNodes.length > 0) {
      await Promise.all(
        masterNodes.map(async node => {
          return await node.flushdb()
        }),
      )
    }
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

  test('HDEL command', async () => {
    await redisClient?.hset(
      'hash7',
      'field1',
      'value1',
      'field2',
      'value2',
      'field3',
      'value3',
    )

    // Delete single field
    const del1 = await redisClient?.hdel('hash7', 'field1')
    assert.strictEqual(del1, 1)

    // Delete multiple fields
    const del2 = await redisClient?.hdel('hash7', 'field2', 'field3')
    assert.strictEqual(del2, 2)

    // Verify hash is empty
    const len = await redisClient?.hlen('hash7')
    assert.strictEqual(len, 0)
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
