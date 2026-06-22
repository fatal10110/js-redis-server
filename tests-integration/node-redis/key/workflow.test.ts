import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  assertNodeRedisDbSizeDelta,
  flushNodeRedisCluster,
  getNodeRedisTotalDbSize,
  randomKey,
} from '../../utils'

const testRunner = new TestRunner()

describe(`Key Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('Key commands workflow - Data Type Validation', async () => {
    const userKey = '{app}user'
    const cartKey = '{app}cart'
    const scoresKey = '{app}scores'

    await redisClient.hSet(userKey, {
      name: 'Alice',
      email: 'alice@example.com',
    })
    await redisClient.sAdd(cartKey, ['item1', 'item2', 'item3'])
    await redisClient.zAdd(scoresKey, [
      { score: 100, value: 'player1' },
      { score: 200, value: 'player2' },
    ])

    const userType = await redisClient.type(userKey)
    if (userType === 'hash') {
      const userEmail = await redisClient.hGet(userKey, 'email')
      assert.strictEqual(userEmail, 'alice@example.com')
    }

    const cartType = await redisClient.type(cartKey)
    if (cartType === 'set') {
      const cartSize = await redisClient.sCard(cartKey)
      assert.strictEqual(cartSize, 3)
    }

    const scoresType = await redisClient.type(scoresKey)
    if (scoresType === 'zset') {
      const topPlayer = await redisClient.zRange(scoresKey, 0, 0, { REV: true })
      assert.strictEqual(topPlayer[0], 'player2')
    }

    const requiredKeys = [userKey, cartKey, scoresKey]
    const existingKeysCount = await redisClient.exists(requiredKeys)
    assert.strictEqual(existingKeysCount, 3)
  })

  test('Key commands workflow - Cache Validation', async () => {
    const cacheKeys = [
      'cache:user_sessions',
      'cache:popular_items',
      'cache:daily_stats',
      'cache:search_results',
    ]

    await redisClient.hSet('cache:user_sessions', {
      session1: 'active',
      session2: 'expired',
    })
    await redisClient.lPush('cache:popular_items', ['item1', 'item2', 'item3'])
    await redisClient.set(
      'cache:daily_stats',
      JSON.stringify({ visits: 1000, sales: 50 }),
    )
    await redisClient.zAdd('cache:search_results', [
      { score: 0.95, value: 'result1' },
      { score: 0.87, value: 'result2' },
    ])

    const cacheStatus: Array<{ key: string; exists: boolean; type: string }> =
      []
    for (const key of cacheKeys) {
      const exists = await redisClient.exists(key)
      const type = exists ? await redisClient.type(key) : 'none'
      cacheStatus.push({ key, exists: !!exists, type: type || 'none' })
    }

    assert.strictEqual(cacheStatus.length, 4)
    assert.ok(cacheStatus.every(entry => entry.exists))

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

    await redisClient.del('cache:daily_stats')

    assert.strictEqual(await redisClient.exists('cache:daily_stats'), 0)
    assert.strictEqual(await redisClient.type('cache:daily_stats'), 'none')
  })

  test('Key commands workflow - Multi-tenant Data Isolation', async () => {
    await redisClient.hSet('{tenant1}users', { user1: 'alice', user2: 'bob' })
    await redisClient.set(
      '{tenant1}settings',
      JSON.stringify({ theme: 'dark', lang: 'en' }),
    )
    await redisClient.lPush('{tenant1}data', ['data1', 'data2'])

    await redisClient.hSet('{tenant2}users', {
      user1: 'charlie',
      user2: 'diana',
    })
    await redisClient.set(
      '{tenant2}settings',
      JSON.stringify({ theme: 'light', lang: 'es' }),
    )
    await redisClient.sAdd('{tenant2}data', ['item1', 'item2'])

    const checkTenantData = async (tenantTag: string) => {
      const expectedKeys = [
        `${tenantTag}users`,
        `${tenantTag}settings`,
        `${tenantTag}data`,
      ]

      const existingCount = await redisClient.exists(expectedKeys)
      const keyTypes: string[] = []

      for (const key of expectedKeys) {
        const type = await redisClient.type(key)
        keyTypes.push(type || 'none')
      }

      return { existingCount, keyTypes }
    }

    const tenant1Status = await checkTenantData('{tenant1}')
    assert.strictEqual(tenant1Status.existingCount, 3)
    assert.deepStrictEqual(tenant1Status.keyTypes, ['hash', 'string', 'list'])

    const tenant2Status = await checkTenantData('{tenant2}')
    assert.strictEqual(tenant2Status.existingCount, 3)
    assert.deepStrictEqual(tenant2Status.keyTypes, ['hash', 'string', 'set'])

    const tenant3Status = await checkTenantData('{tenant3}')
    assert.strictEqual(tenant3Status.existingCount, 0)
    assert.deepStrictEqual(tenant3Status.keyTypes, ['none', 'none', 'none'])

    const tenant1Users = await redisClient.hGetAll('{tenant1}users')
    assert.strictEqual(tenant1Users.user1, 'alice')

    const tenant2Users = await redisClient.hGetAll('{tenant2}users')
    assert.strictEqual(tenant2Users.user1, 'charlie')
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

    const baseline = await getNodeRedisTotalDbSize(redisClient)

    try {
      await redisClient.hSet(`${tag}:config`, {
        max_users: '1000',
        timeout: '3600',
      })
      await redisClient.set(`${tag}:app_version`, '1.2.3')
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 2)

      const userActivities: Array<Promise<unknown>> = []
      for (let i = 1; i <= 10; i++) {
        userActivities.push(
          redisClient.hSet(`${tag}:user:${i}`, {
            name: `user${i}`,
            status: 'active',
          }),
          redisClient.set(`${tag}:session:${i}`, `session_data_${i}`, {
            expiration: { type: 'EX', value: 3600 },
          }),
          redisClient.lPush(`${tag}:activity:${i}`, [
            'login',
            'view_page',
            'logout',
          ]),
        )
      }
      await Promise.all(userActivities)
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 32)

      await redisClient.sAdd(`${tag}:popular_items`, [
        'item1',
        'item2',
        'item3',
        'item4',
        'item5',
      ])
      await redisClient.zAdd(`${tag}:leaderboard`, [
        { score: 100, value: 'player1' },
        { score: 200, value: 'player2' },
        { score: 150, value: 'player3' },
      ])
      await redisClient.hSet(`${tag}:daily_stats`, {
        visitors: '500',
        sales: '25',
      })
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 35)

      const expiredSessions: Promise<number>[] = []
      for (let i = 6; i <= 10; i++) {
        expiredSessions.push(redisClient.del(`${tag}:session:${i}`))
      }
      await Promise.all(expiredSessions)

      const cleanedSize = await getNodeRedisTotalDbSize(redisClient)
      const cleanedDelta = cleanedSize - baseline
      const capacityThreshold = 50
      const currentUtilization = (cleanedDelta / capacityThreshold) * 100

      assert.ok(
        currentUtilization < 80,
        'Database utilization should be under 80%',
      )
      assert.strictEqual(cleanedDelta, 30)

      await redisClient.del(`${tag}:daily_stats`)
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 29)
    } finally {
      await redisClient.del(createdKeys)
    }
  })

  test('Expiration workflow - Session Management', async () => {
    const sessionId = '{session}user123'
    const sessionData = JSON.stringify({
      userId: 123,
      loginTime: Date.now(),
      permissions: ['read', 'write'],
    })

    await redisClient.set(sessionId, sessionData)
    await redisClient.expire(sessionId, 3600) // 1 hour

    const sessionExists = await redisClient.exists(sessionId)
    assert.strictEqual(sessionExists, 1)

    const sessionTtl = await redisClient.ttl(sessionId)
    assert.ok(sessionTtl <= 3600 && sessionTtl > 0)

    await redisClient.expire(sessionId, 7200) // 2 hours

    const extendedTtl = await redisClient.ttl(sessionId)
    assert.ok(extendedTtl <= 7200 && extendedTtl > 3600)

    await redisClient.del(sessionId)
  })

  test('Expiration workflow - Cache with Scheduled Invalidation', async () => {
    const cacheKeys = [
      '{cache}daily_report',
      '{cache}hourly_stats',
      '{cache}temp_data',
    ]

    for (const key of cacheKeys) {
      await redisClient.set(key, `data for ${key}`)
    }

    const now = Math.floor(Date.now() / 1000)

    await redisClient.expireAt(cacheKeys[0], now + 5)
    await redisClient.expire(cacheKeys[1], 3600)
    await redisClient.expire(cacheKeys[2], 10)

    const existingCount = await redisClient.exists(cacheKeys)
    assert.strictEqual(existingCount, 3)

    const ttl1 = await redisClient.ttl(cacheKeys[0])
    assert.ok(ttl1 <= 5 && ttl1 > 0)

    const ttl2 = await redisClient.ttl(cacheKeys[1])
    assert.ok(ttl2 <= 3600 && ttl2 > 0)

    const ttl3 = await redisClient.ttl(cacheKeys[2])
    assert.ok(ttl3 <= 10 && ttl3 > 0)

    await redisClient.del(cacheKeys)
  })
})
