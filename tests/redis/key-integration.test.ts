import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'

describe('Key Commands Integration', () => {
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

  test('EXISTS command', async () => {
    // Set up test data
    await redisClient?.set('{test}string_key', 'value')
    await redisClient?.hset('{test}hash_key', 'field', 'value')
    await redisClient?.lpush('{test}list_key', 'item')
    await redisClient?.sadd('{test}set_key', 'member')
    await redisClient?.zadd('{test}zset_key', 1, 'member')

    // Test single key existence
    const exists1 = await redisClient?.exists('{test}string_key')
    assert.strictEqual(exists1, 1)

    const exists2 = await redisClient?.exists('{test}nonexistent')
    assert.strictEqual(exists2, 0)

    // Test multiple keys existence
    const existsMultiple = await redisClient?.exists(
      '{test}string_key',
      '{test}hash_key',
      '{test}nonexistent',
      '{test}list_key',
    )
    assert.strictEqual(existsMultiple, 3) // 3 out of 4 keys exist
  })

  test('TYPE command', async () => {
    // Set up test data of different types
    await redisClient?.set('{test}string_key', 'value')
    await redisClient?.hset('{test}hash_key', 'field', 'value')
    await redisClient?.lpush('{test}list_key', 'item')
    await redisClient?.sadd('{test}set_key', 'member')
    await redisClient?.zadd('{test}zset_key', 1, 'member')

    // Test type detection
    const stringType = await redisClient?.type('{test}string_key')
    assert.strictEqual(stringType, 'string')

    const hashType = await redisClient?.type('{test}hash_key')
    assert.strictEqual(hashType, 'hash')

    const listType = await redisClient?.type('{test}list_key')
    assert.strictEqual(listType, 'list')

    const setType = await redisClient?.type('{test}set_key')
    assert.strictEqual(setType, 'set')

    const zsetType = await redisClient?.type('{test}zset_key')
    assert.strictEqual(zsetType, 'zset')

    const noneType = await redisClient?.type('{test}nonexistent')
    assert.strictEqual(noneType, 'none')
  })

  test('Key commands workflow - Data Type Validation', async () => {
    // Application needs to validate data types before operations
    const userKey = '{app}user'
    const cartKey = '{app}cart'
    const scoresKey = '{app}scores'

    // Set up different data structures
    await redisClient?.hmset(
      userKey,
      'name',
      'Alice',
      'email',
      'alice@example.com',
    )
    await redisClient?.sadd(cartKey, 'item1', 'item2', 'item3')
    await redisClient?.zadd(scoresKey, 100, 'player1', 200, 'player2')

    // Validate data types before operations
    const userType = await redisClient?.type(userKey)
    if (userType === 'hash') {
      const userEmail = await redisClient?.hget(userKey, 'email')
      assert.strictEqual(userEmail, 'alice@example.com')
    }

    const cartType = await redisClient?.type(cartKey)
    if (cartType === 'set') {
      const cartSize = await redisClient?.scard(cartKey)
      assert.strictEqual(cartSize, 3)
    }

    const scoresType = await redisClient?.type(scoresKey)
    if (scoresType === 'zset') {
      const topPlayer = await redisClient?.zrevrange(scoresKey, 0, 0)
      assert.strictEqual(topPlayer?.[0], 'player2')
    }

    // Check if all required keys exist
    const requiredKeys = [userKey, cartKey, scoresKey]
    const existingKeysCount = await redisClient?.exists(...requiredKeys)
    assert.strictEqual(existingKeysCount, 3)
  })

  test('Key commands workflow - Cache Validation', async () => {
    // Application checks cache validity before using data
    const cacheKeys = [
      'cache:user_sessions',
      'cache:popular_items',
      'cache:daily_stats',
      'cache:search_results',
    ]

    // Populate cache with different data types
    await redisClient?.hset(
      'cache:user_sessions',
      'session1',
      'active',
      'session2',
      'expired',
    )
    await redisClient?.lpush('cache:popular_items', 'item1', 'item2', 'item3')
    await redisClient?.set(
      'cache:daily_stats',
      JSON.stringify({ visits: 1000, sales: 50 }),
    )
    await redisClient?.zadd(
      'cache:search_results',
      0.95,
      'result1',
      0.87,
      'result2',
    )

    // Check which cache entries exist
    const cacheStatus: Array<{ key: string; exists: boolean; type: string }> =
      []
    for (const key of cacheKeys) {
      const exists = await redisClient?.exists(key)
      const type = exists ? await redisClient?.type(key) : 'none'
      cacheStatus.push({ key, exists: !!exists, type: type || 'none' })
    }

    // Verify all cache entries exist
    assert.strictEqual(cacheStatus.length, 4)
    assert.ok(cacheStatus.every(entry => entry.exists))

    // Verify cache types match expectations
    const sessionsCache = cacheStatus.find(entry =>
      entry.key.includes('user_sessions'),
    )
    assert.strictEqual(sessionsCache?.type, 'hash')

    const itemsCache = cacheStatus.find(entry =>
      entry.key.includes('popular_items'),
    )
    assert.strictEqual(itemsCache?.type, 'list')

    const statsCache = cacheStatus.find(entry =>
      entry.key.includes('daily_stats'),
    )
    assert.strictEqual(statsCache?.type, 'string')

    const searchCache = cacheStatus.find(entry =>
      entry.key.includes('search_results'),
    )
    assert.strictEqual(searchCache?.type, 'zset')

    // Simulate cache invalidation
    await redisClient?.del('cache:daily_stats')

    // Verify cache is invalidated
    const statsExists = await redisClient?.exists('cache:daily_stats')
    assert.strictEqual(statsExists, 0)

    const statsType = await redisClient?.type('cache:daily_stats')
    assert.strictEqual(statsType, 'none')
  })

  test('Key commands workflow - Multi-tenant Data Isolation', async () => {
    // Multi-tenant application needs to check key ownership

    // Set up tenant data
    await redisClient?.hset('{tenant1}users', 'user1', 'alice', 'user2', 'bob')
    await redisClient?.set(
      '{tenant1}settings',
      JSON.stringify({ theme: 'dark', lang: 'en' }),
    )
    await redisClient?.lpush('{tenant1}data', 'data1', 'data2')

    await redisClient?.hset(
      '{tenant2}users',
      'user1',
      'charlie',
      'user2',
      'diana',
    )
    await redisClient?.set(
      '{tenant2}settings',
      JSON.stringify({ theme: 'light', lang: 'es' }),
    )
    await redisClient?.sadd('{tenant2}data', 'item1', 'item2')

    // Function to check tenant data integrity
    const checkTenantData = async (tenantTag: string) => {
      const expectedKeys = [
        `${tenantTag}users`,
        `${tenantTag}settings`,
        `${tenantTag}data`,
      ]

      const existingCount = await redisClient?.exists(...expectedKeys)
      const keyTypes: string[] = []

      for (const key of expectedKeys) {
        const type = await redisClient?.type(key)
        keyTypes.push(type || 'none')
      }

      return { existingCount, keyTypes }
    }

    // Verify tenant 1 data
    const tenant1Status = await checkTenantData('{tenant1}')
    assert.strictEqual(tenant1Status.existingCount, 3)
    assert.deepStrictEqual(tenant1Status.keyTypes, ['hash', 'string', 'list'])

    // Verify tenant 2 data
    const tenant2Status = await checkTenantData('{tenant2}')
    assert.strictEqual(tenant2Status.existingCount, 3)
    assert.deepStrictEqual(tenant2Status.keyTypes, ['hash', 'string', 'set'])

    // Test cross-tenant access (should find no keys)
    const tenant3Status = await checkTenantData('{tenant3}')
    assert.strictEqual(tenant3Status.existingCount, 0)
    assert.deepStrictEqual(tenant3Status.keyTypes, ['none', 'none', 'none'])

    // Verify specific tenant data access
    const tenant1Users = await redisClient?.hgetall('{tenant1}users')
    assert.strictEqual(tenant1Users?.user1, 'alice')

    const tenant2Users = await redisClient?.hgetall('{tenant2}users')
    assert.strictEqual(tenant2Users?.user1, 'charlie')
  })
})
