import { test, describe, before, after, afterEach } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../../src/core/cluster/network'
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

  afterEach(async () => {
    // Clean up database after each test
    // Skip cleanup for flush tests as they handle their own cleanup
    const masterNodes = redisClient?.nodes('master') || []
    if (masterNodes.length > 0) {
      await Promise.all(
        masterNodes.map(async node => {
          return await node.flushdb()
        }),
      )
    }
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

  test('DBSIZE command', async () => {
    // Helper function to get total dbsize across all nodes
    const getTotalDbSize = async (): Promise<number> => {
      const masterNodes = redisClient?.nodes('master') || []
      if (masterNodes.length === 0) return 0

      const sizes = await Promise.all(
        masterNodes.map(async node => {
          return await node.dbsize()
        }),
      )

      return sizes.reduce((total, size) => total + size, 0)
    }

    // Test DBSIZE on empty database (after afterEach cleanup)
    const initialSize = await getTotalDbSize()
    assert.strictEqual(initialSize, 0)

    // Set up test data of different types
    await redisClient?.set('{test}string_key', 'value')
    await redisClient?.hset('{test}hash_key', 'field', 'value')
    await redisClient?.lpush('{test}list_key', 'item')
    await redisClient?.sadd('{test}set_key', 'member')
    await redisClient?.zadd('{test}zset_key', 1, 'member')

    // Test DBSIZE after adding keys
    const sizeWithKeys = await getTotalDbSize()
    assert.strictEqual(sizeWithKeys, 5)

    // Add keys with expiration
    await redisClient?.set('{test}expire_key1', 'value')
    await redisClient?.expire('{test}expire_key1', 3600) // Will not expire during test

    // DBSIZE should count non-expired keys
    const sizeWithExpiration = await getTotalDbSize()
    assert.strictEqual(sizeWithExpiration, 6) // 5 original + 1 non-expired

    // Delete some keys
    await redisClient?.del('{test}string_key', '{test}hash_key')

    // Test DBSIZE after deletion
    const sizeAfterDeletion = await getTotalDbSize()
    assert.strictEqual(sizeAfterDeletion, 4) // 6 - 2 deleted keys
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
    // Helper function to get total dbsize across all nodes
    const getTotalDbSize = async (): Promise<number> => {
      const masterNodes = redisClient?.nodes('master') || []
      if (masterNodes.length === 0) return 0

      const sizes = await Promise.all(
        masterNodes.map(async node => {
          return await node.dbsize()
        }),
      )

      return sizes.reduce((total, size) => total + size, 0)
    }

    // Simulate database monitoring scenario

    // Start with empty database
    const initialSize = await getTotalDbSize()
    assert.strictEqual(initialSize, 0)

    // Simulate application startup - loading configuration
    await redisClient?.hset(
      '{monitor}config',
      'max_users',
      '1000',
      'timeout',
      '3600',
    )
    await redisClient?.set('{monitor}app_version', '1.2.3')

    const configSize = await getTotalDbSize()
    assert.strictEqual(configSize, 2)

    // Simulate user activity - creating sessions and cache
    const userActivities: Array<Promise<number | string>> = []
    for (let i = 1; i <= 10; i++) {
      if (redisClient) {
        userActivities.push(
          redisClient.hset(
            `{monitor}user:${i}`,
            'name',
            `user${i}`,
            'status',
            'active',
          ),
          redisClient.set(
            `{monitor}session:${i}`,
            `session_data_${i}`,
            'EX',
            3600,
          ),
          redisClient.lpush(
            `{monitor}activity:${i}`,
            'login',
            'view_page',
            'logout',
          ),
        )
      }
    }
    await Promise.all(userActivities)

    // Check database size after user activity
    const activeSize = await getTotalDbSize()
    assert.strictEqual(activeSize, 32) // 2 config + 30 user-related keys (10 users * 3 keys each)

    // Simulate cache warming - adding frequently accessed data
    await redisClient?.sadd(
      '{monitor}popular_items',
      'item1',
      'item2',
      'item3',
      'item4',
      'item5',
    )
    await redisClient?.zadd(
      '{monitor}leaderboard',
      100,
      'player1',
      200,
      'player2',
      150,
      'player3',
    )
    await redisClient?.hset(
      '{monitor}daily_stats',
      'visitors',
      '500',
      'sales',
      '25',
    )

    const cachedSize = await getTotalDbSize()
    assert.strictEqual(cachedSize, 35) // Previous 32 + 3 cache keys

    // Simulate cleanup - removing expired sessions (using DEL to simulate expiration)
    const expiredSessions: Promise<number>[] = []
    for (let i = 6; i <= 10; i++) {
      if (redisClient) {
        expiredSessions.push(redisClient.del(`{monitor}session:${i}`))
      }
    }
    await Promise.all(expiredSessions)

    const cleanedSize = await getTotalDbSize()
    assert.strictEqual(cleanedSize, 30) // 35 - 5 expired sessions

    // Database capacity check - alert if approaching limits
    const capacityThreshold = 50
    const currentUtilization = (cleanedSize / capacityThreshold) * 100

    assert.ok(
      currentUtilization < 80,
      'Database utilization should be under 80%',
    )
    assert.strictEqual(cleanedSize, 30)

    // Final cleanup simulation
    await redisClient?.del('{monitor}daily_stats')
    const finalSize = await getTotalDbSize()
    assert.strictEqual(finalSize, 29)
  })

  test('EXPIRE and EXPIREAT commands', async () => {
    // Test EXPIRE command
    await redisClient?.set('{test}expire_key', 'value')

    const expireResult = await redisClient?.expire('{test}expire_key', 10)
    assert.strictEqual(expireResult, 1)

    const ttlResult = await redisClient?.ttl('{test}expire_key')
    assert.ok(ttlResult !== undefined && ttlResult <= 10 && ttlResult > 0)

    // Test EXPIRE on non-existent key
    const expireNonExistent = await redisClient?.expire('{test}nonexistent', 10)
    assert.strictEqual(expireNonExistent, 0)

    // Test EXPIREAT command
    await redisClient?.set('{test}expireat_key', 'value')

    const futureTimestamp = Math.floor(Date.now() / 1000) + 10
    const expireatResult = await redisClient?.expireat(
      '{test}expireat_key',
      futureTimestamp,
    )
    assert.strictEqual(expireatResult, 1)

    const ttlResult2 = await redisClient?.ttl('{test}expireat_key')
    assert.ok(ttlResult2 !== undefined && ttlResult2 <= 10 && ttlResult2 > 0)

    // Test EXPIREAT on non-existent key
    const expireatNonExistent = await redisClient?.expireat(
      '{test}nonexistent',
      futureTimestamp,
    )
    assert.strictEqual(expireatNonExistent, 0)
  })

  test('TTL integration with EXPIRE and EXPIREAT', async () => {
    // Set up keys with different expiration methods
    await redisClient?.set('{test}ttl1', 'value1')
    await redisClient?.set('{test}ttl2', 'value2')
    await redisClient?.set('{test}ttl3', 'value3')

    // Set expiration using EXPIRE
    await redisClient?.expire('{test}ttl1', 20)

    // Set expiration using EXPIREAT
    const futureTimestamp = Math.floor(Date.now() / 1000) + 30
    await redisClient?.expireat('{test}ttl2', futureTimestamp)

    // Check TTL values
    const ttl1 = await redisClient?.ttl('{test}ttl1')
    assert.ok(ttl1 !== undefined && ttl1 <= 20 && ttl1 > 0)

    const ttl2 = await redisClient?.ttl('{test}ttl2')
    assert.ok(ttl2 !== undefined && ttl2 <= 30 && ttl2 > 0)

    // Key without expiration should have TTL -1
    const ttl3 = await redisClient?.ttl('{test}ttl3')
    assert.strictEqual(ttl3, -1)

    // Non-existent key should have TTL -2
    const ttlNonExistent = await redisClient?.ttl('{test}nonexistent')
    assert.strictEqual(ttlNonExistent, -2)
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

  test('FLUSHDB command integration', async () => {
    // Set up test data with various types
    await redisClient?.set('{flush}string_key', 'test_value')
    await redisClient?.hset(
      '{flush}hash_key',
      'field1',
      'value1',
      'field2',
      'value2',
    )
    await redisClient?.lpush('{flush}list_key', 'item1', 'item2', 'item3')
    await redisClient?.sadd('{flush}set_key', 'member1', 'member2', 'member3')
    await redisClient?.zadd('{flush}zset_key', 1, 'member1', 2, 'member2')

    // Set expiration on some keys
    await redisClient?.expire('{flush}string_key', 3600)
    await redisClient?.expire('{flush}hash_key', 1800)

    // Verify all keys exist before flush
    const existsCount = await redisClient?.exists(
      '{flush}string_key',
      '{flush}hash_key',
      '{flush}list_key',
      '{flush}set_key',
      '{flush}zset_key',
    )
    assert.strictEqual(existsCount, 5)

    // Verify TTL is set
    const stringTtl = await redisClient?.ttl('{flush}string_key')
    assert.ok(stringTtl !== undefined && stringTtl > 0)

    // Execute FLUSHDB on all master nodes
    const masterNodes = redisClient?.nodes('master') || []
    assert.ok(masterNodes.length > 0, 'Should have master nodes')

    const flushResults = await Promise.all(
      masterNodes.map(async node => {
        return await node.flushdb()
      }),
    )

    // Verify all nodes returned OK
    flushResults.forEach(result => {
      assert.strictEqual(result, 'OK')
    })

    // Verify all keys are removed
    const testKeys = [
      '{flush}string_key',
      '{flush}hash_key',
      '{flush}list_key',
      '{flush}set_key',
      '{flush}zset_key',
    ]
    const existsAfterFlush = await redisClient?.exists(...testKeys)
    assert.strictEqual(existsAfterFlush, 0)

    // Verify TTL is also cleared
    const stringTtlAfter = await redisClient?.ttl('{flush}string_key')
    assert.strictEqual(stringTtlAfter, -2) // Key does not exist
  })

  test('FLUSHALL command integration', async () => {
    // Set up test data with various types
    await redisClient?.set('{flushall}string_key', 'test_value')
    await redisClient?.hset(
      '{flushall}hash_key',
      'field1',
      'value1',
      'field2',
      'value2',
    )
    await redisClient?.lpush('{flushall}list_key', 'item1', 'item2', 'item3')
    await redisClient?.sadd(
      '{flushall}set_key',
      'member1',
      'member2',
      'member3',
    )
    await redisClient?.zadd('{flushall}zset_key', 1, 'member1', 2, 'member2')

    // Set expiration on some keys
    await redisClient?.expire('{flushall}string_key', 3600)
    await redisClient?.expire('{flushall}hash_key', 1800)

    // Verify all keys exist before flush
    const existsCount = await redisClient?.exists(
      '{flushall}string_key',
      '{flushall}hash_key',
      '{flushall}list_key',
      '{flushall}set_key',
      '{flushall}zset_key',
    )
    assert.strictEqual(existsCount, 5)

    // Verify TTL is set
    const stringTtl = await redisClient?.ttl('{flushall}string_key')
    assert.ok(stringTtl !== undefined && stringTtl > 0)

    // Execute FLUSHALL on all master nodes
    const masterNodes = redisClient?.nodes('master') || []
    assert.ok(masterNodes.length > 0, 'Should have master nodes')

    const flushResults = await Promise.all(
      masterNodes.map(async node => {
        return await node.flushall()
      }),
    )

    // Verify all nodes returned OK
    flushResults.forEach(result => {
      assert.strictEqual(result, 'OK')
    })

    // Verify all keys are removed
    const testKeys = [
      '{flushall}string_key',
      '{flushall}hash_key',
      '{flushall}list_key',
      '{flushall}set_key',
      '{flushall}zset_key',
    ]
    const existsAfterFlush = await redisClient?.exists(...testKeys)
    assert.strictEqual(existsAfterFlush, 0)

    // Verify TTL is also cleared
    const stringTtlAfter = await redisClient?.ttl('{flushall}string_key')
    assert.strictEqual(stringTtlAfter, -2) // Key does not exist
  })

  test('FLUSHDB workflow - Database Reset for Testing', async () => {
    // Simulate test environment setup and cleanup
    const testKeys = [
      '{test_env}user_data',
      '{test_env}cache_data',
      '{test_env}session_data',
      '{test_env}temp_data',
    ]

    // Setup test environment with various data
    await redisClient?.hset(testKeys[0], 'user1', 'Alice', 'user2', 'Bob')
    await redisClient?.set(testKeys[1], JSON.stringify({ cached: 'data' }))
    await redisClient?.sadd(testKeys[2], 'session1', 'session2', 'session3')
    await redisClient?.lpush(testKeys[3], 'temp1', 'temp2')

    // Set some keys with expiration
    await redisClient?.expire(testKeys[1], 300) // Cache expires in 5 minutes
    await redisClient?.expire(testKeys[3], 60) // Temp data expires in 1 minute

    // Verify test environment is set up
    const setupCount = await redisClient?.exists(...testKeys)
    assert.strictEqual(setupCount, 4)

    // Verify some data content
    const userData = await redisClient?.hgetall(testKeys[0])
    assert.strictEqual(userData?.user1, 'Alice')

    const cacheData = await redisClient?.get(testKeys[1])
    assert.ok(cacheData?.includes('cached'))

    const sessionCount = await redisClient?.scard(testKeys[2])
    assert.strictEqual(sessionCount, 3)

    // Clean up test environment with FLUSHDB on all master nodes
    const masterNodes = redisClient?.nodes('master') || []
    assert.ok(masterNodes.length > 0, 'Should have master nodes')

    const flushResults = await Promise.all(
      masterNodes.map(async node => {
        return await node.flushdb()
      }),
    )

    // Verify all nodes returned OK
    flushResults.forEach(result => {
      assert.strictEqual(result, 'OK')
    })

    // Verify complete cleanup
    const cleanupCount = await redisClient?.exists(...testKeys)
    assert.strictEqual(cleanupCount, 0)

    // Verify data is completely gone
    const userDataAfter = await redisClient?.hgetall(testKeys[0])
    assert.deepStrictEqual(userDataAfter, {})

    const cacheDataAfter = await redisClient?.get(testKeys[1])
    assert.strictEqual(cacheDataAfter, null)

    const sessionCountAfter = await redisClient?.scard(testKeys[2])
    assert.strictEqual(sessionCountAfter, 0)
  })

  test('FLUSHALL workflow - Complete System Reset', async () => {
    // Simulate complete system reset scenario
    const systemKeys = [
      '{system}config',
      '{system}users',
      '{system}logs',
      '{system}metrics',
    ]

    // Setup system data
    await redisClient?.hset(
      systemKeys[1],
      'admin',
      'password_hash',
      'user',
      'user_hash',
    )
    await redisClient?.set(
      systemKeys[0],
      JSON.stringify({
        version: '1.0',
        maintenance: false,
      }),
    )
    await redisClient?.lpush(systemKeys[2], 'log1', 'log2', 'log3')
    await redisClient?.zadd(
      systemKeys[3],
      100,
      'cpu_usage',
      500,
      'memory_usage',
    )

    // Add some keys with expiration
    await redisClient?.expire(systemKeys[2], 86400) // Logs expire in 24 hours
    await redisClient?.expire(systemKeys[3], 3600) // Metrics expire in 1 hour

    // Verify system is fully operational
    const systemCount = await redisClient?.exists(...systemKeys)
    assert.strictEqual(systemCount, 4)

    // Check system integrity
    const configData = await redisClient?.get(systemKeys[0])
    const config = JSON.parse(configData || '{}')
    assert.strictEqual(config.version, '1.0')
    assert.strictEqual(config.maintenance, false)

    const userCount = await redisClient?.hlen(systemKeys[1])
    assert.strictEqual(userCount, 2)

    const logCount = await redisClient?.llen(systemKeys[2])
    assert.strictEqual(logCount, 3)

    const metricCount = await redisClient?.zcard(systemKeys[3])
    assert.strictEqual(metricCount, 2)

    // Perform complete system reset with FLUSHALL on all master nodes
    const masterNodes = redisClient?.nodes('master') || []
    assert.ok(masterNodes.length > 0, 'Should have master nodes')

    const resetResults = await Promise.all(
      masterNodes.map(async node => {
        return await node.flushall()
      }),
    )

    // Verify all nodes returned OK
    resetResults.forEach(result => {
      assert.strictEqual(result, 'OK')
    })

    // Verify complete system cleanup
    const postResetCount = await redisClient?.exists(...systemKeys)
    assert.strictEqual(postResetCount, 0)

    // Verify all data structures are empty
    const configAfter = await redisClient?.get(systemKeys[0])
    assert.strictEqual(configAfter, null)

    const usersAfter = await redisClient?.hlen(systemKeys[1])
    assert.strictEqual(usersAfter, 0)

    const logsAfter = await redisClient?.llen(systemKeys[2])
    assert.strictEqual(logsAfter, 0)

    const metricsAfter = await redisClient?.zcard(systemKeys[3])
    assert.strictEqual(metricsAfter, 0)

    // Verify TTL data is also cleared
    const logTtlAfter = await redisClient?.ttl(systemKeys[2])
    assert.strictEqual(logTtlAfter, -2) // Key does not exist

    const metricTtlAfter = await redisClient?.ttl(systemKeys[3])
    assert.strictEqual(metricTtlAfter, -2) // Key does not exist
  })
})
