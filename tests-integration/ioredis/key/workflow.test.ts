import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { assertDbSizeDelta, getTotalDbSize, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Key Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('key-integration')
  })

  after(async () => {
    await testRunner.cleanup()
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

  test('DBSIZE workflow - Database Monitoring and Capacity Planning', async () => {
    const tag = `{monitor:${randomKey()}}`
    const createdKeys = [
      `${tag}:config`,
      `${tag}:app_version`,
      `${tag}:popular_items`,
      `${tag}:leaderboard`,
      `${tag}:daily_stats`,
    ]

    for (let i = 1; i <= 10; i++) {
      createdKeys.push(
        `${tag}:user:${i}`,
        `${tag}:session:${i}`,
        `${tag}:activity:${i}`,
      )
    }

    const baseline = await getTotalDbSize(redisClient!)

    try {
      await redisClient?.hset(
        `${tag}:config`,
        'max_users',
        '1000',
        'timeout',
        '3600',
      )
      await redisClient?.set(`${tag}:app_version`, '1.2.3')
      await assertDbSizeDelta(redisClient!, baseline, 2)

      const userActivities: Array<Promise<unknown>> = []
      for (let i = 1; i <= 10; i++) {
        userActivities.push(
          redisClient!.hset(
            `${tag}:user:${i}`,
            'name',
            `user${i}`,
            'status',
            'active',
          ),
          redisClient!.set(
            `${tag}:session:${i}`,
            `session_data_${i}`,
            'EX',
            3600,
          ),
          redisClient!.lpush(
            `${tag}:activity:${i}`,
            'login',
            'view_page',
            'logout',
          ),
        )
      }
      await Promise.all(userActivities)
      await assertDbSizeDelta(redisClient!, baseline, 32)

      await redisClient?.sadd(
        `${tag}:popular_items`,
        'item1',
        'item2',
        'item3',
        'item4',
        'item5',
      )
      await redisClient?.zadd(
        `${tag}:leaderboard`,
        100,
        'player1',
        200,
        'player2',
        150,
        'player3',
      )
      await redisClient?.hset(
        `${tag}:daily_stats`,
        'visitors',
        '500',
        'sales',
        '25',
      )
      await assertDbSizeDelta(redisClient!, baseline, 35)

      const expiredSessions: Promise<number>[] = []
      for (let i = 6; i <= 10; i++) {
        expiredSessions.push(redisClient!.del(`${tag}:session:${i}`))
      }
      await Promise.all(expiredSessions)

      const cleanedSize = await getTotalDbSize(redisClient!)
      const cleanedDelta = cleanedSize - baseline
      const capacityThreshold = 50
      const currentUtilization = (cleanedDelta / capacityThreshold) * 100

      assert.ok(
        currentUtilization < 80,
        'Database utilization should be under 80%',
      )
      assert.strictEqual(cleanedDelta, 30)

      await redisClient?.del(`${tag}:daily_stats`)
      await assertDbSizeDelta(redisClient!, baseline, 29)
    } finally {
      await redisClient?.del(...createdKeys)
    }
  })

  test('Expiration workflow - Session Management', async () => {
    // Simulate session management with expiration
    const sessionId = '{session}user123'
    const sessionData = JSON.stringify({
      userId: 123,
      loginTime: Date.now(),
      permissions: ['read', 'write'],
    })

    // Create session with 1 hour expiration
    await redisClient?.set(sessionId, sessionData)
    await redisClient?.expire(sessionId, 3600) // 1 hour

    // Verify session exists and has correct TTL
    const sessionExists = await redisClient?.exists(sessionId)
    assert.strictEqual(sessionExists, 1)

    const sessionTtl = await redisClient?.ttl(sessionId)
    assert.ok(sessionTtl !== undefined && sessionTtl <= 3600 && sessionTtl > 0)

    // Extend session expiration
    await redisClient?.expire(sessionId, 7200) // 2 hours

    const extendedTtl = await redisClient?.ttl(sessionId)
    assert.ok(
      extendedTtl !== undefined && extendedTtl <= 7200 && extendedTtl > 3600,
    )

    // Clean up session
    await redisClient?.del(sessionId)
  })

  test('Expiration workflow - Cache with Scheduled Invalidation', async () => {
    // Simulate cache that should expire at specific time
    const cacheKeys = [
      '{cache}daily_report',
      '{cache}hourly_stats',
      '{cache}temp_data',
    ]

    // Set cache data
    for (const key of cacheKeys) {
      await redisClient?.set(key, `data for ${key}`)
    }

    // Schedule expiration at different times
    const now = Math.floor(Date.now() / 1000)

    // Daily report expires at midnight (simulate with +5 seconds)
    await redisClient?.expireat(cacheKeys[0], now + 5)

    // Hourly stats expire in 1 hour (3600 seconds)
    await redisClient?.expire(cacheKeys[1], 3600)

    // Temp data expires in 10 seconds
    await redisClient?.expire(cacheKeys[2], 10)

    // Check all keys exist initially
    const existingCount = await redisClient?.exists(...cacheKeys)
    assert.strictEqual(existingCount, 3)

    // Verify TTL values are set correctly
    const ttl1 = await redisClient?.ttl(cacheKeys[0])
    assert.ok(ttl1 !== undefined && ttl1 <= 5 && ttl1 > 0)

    const ttl2 = await redisClient?.ttl(cacheKeys[1])
    assert.ok(ttl2 !== undefined && ttl2 <= 3600 && ttl2 > 0)

    const ttl3 = await redisClient?.ttl(cacheKeys[2])
    assert.ok(ttl3 !== undefined && ttl3 <= 10 && ttl3 > 0)

    // Clean up
    await redisClient?.del(...cacheKeys)
  })
})
