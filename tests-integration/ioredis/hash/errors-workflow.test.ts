import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'

const testRunner = new TestRunner()

describe(`Hash Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('hash-integration')
  })

  after(async () => {
    await testRunner.cleanup()
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
