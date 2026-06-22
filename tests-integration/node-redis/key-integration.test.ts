import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import {
  assertNodeRedisDbSizeDelta,
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
  getNodeRedisTotalDbSize,
  randomKey,
} from '../utils'

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

  test('EXISTS command', async () => {
    await redisClient.set('{test}string_key', 'value')
    await redisClient.hSet('{test}hash_key', 'field', 'value')
    await redisClient.lPush('{test}list_key', 'item')
    await redisClient.sAdd('{test}set_key', 'member')
    await redisClient.zAdd('{test}zset_key', { score: 1, value: 'member' })

    const exists1 = await redisClient.exists('{test}string_key')
    assert.strictEqual(exists1, 1)

    const exists2 = await redisClient.exists('{test}nonexistent')
    assert.strictEqual(exists2, 0)

    const existsMultiple = await redisClient.exists([
      '{test}string_key',
      '{test}hash_key',
      '{test}nonexistent',
      '{test}list_key',
    ])
    assert.strictEqual(existsMultiple, 3) // 3 out of 4 keys exist
  })

  test('TOUCH command counts live keys without mutating keyspace', async () => {
    const tag = `{touch:${randomKey()}}`
    const stringKey = `${tag}:string`
    const hashKey = `${tag}:hash`
    const expiringKey = `${tag}:expired`
    const missingKey = `${tag}:missing`
    const crossSlotA = `{touch-cross-a:${randomKey()}}:key`
    const crossSlotB = `{touch-cross-b:${randomKey()}}:key`
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      stringKey,
    )

    try {
      await directClient.set(stringKey, 'value')
      await directClient.hSet(hashKey, 'field', 'value')
      await directClient.set(expiringKey, 'value', {
        expiration: { type: 'PX', value: 1 },
      })
      await new Promise(resolve => setTimeout(resolve, 20))

      assert.strictEqual(
        await directClient.sendCommand([
          'TOUCH',
          stringKey,
          hashKey,
          missingKey,
          expiringKey,
        ]),
        2,
      )
      assert.strictEqual(
        await directClient.exists([stringKey, hashKey, expiringKey]),
        2,
      )
      assert.strictEqual(await directClient.type(hashKey), 'hash')
      assert.strictEqual(
        await directClient.sendCommand(['TOUCH', missingKey]),
        0,
      )

      await assert.rejects(
        () => directClient.sendCommand(['TOUCH']),
        errorWithMessage("ERR wrong number of arguments for 'touch' command"),
      )
      await assert.rejects(
        () => directClient.sendCommand(['TOUCH', crossSlotA, crossSlotB]),
        errorWithMessage(
          "CROSSSLOT Keys in request don't hash to the same slot",
        ),
      )
    } finally {
      await directClient.del([stringKey, hashKey, expiringKey, missingKey])
      directClient.destroy()
    }
  })

  test('TYPE command', async () => {
    await redisClient.set('{test}string_key', 'value')
    await redisClient.hSet('{test}hash_key', 'field', 'value')
    await redisClient.lPush('{test}list_key', 'item')
    await redisClient.sAdd('{test}set_key', 'member')
    await redisClient.zAdd('{test}zset_key', { score: 1, value: 'member' })

    assert.strictEqual(await redisClient.type('{test}string_key'), 'string')
    assert.strictEqual(await redisClient.type('{test}hash_key'), 'hash')
    assert.strictEqual(await redisClient.type('{test}list_key'), 'list')
    assert.strictEqual(await redisClient.type('{test}set_key'), 'set')
    assert.strictEqual(await redisClient.type('{test}zset_key'), 'zset')
    assert.strictEqual(await redisClient.type('{test}nonexistent'), 'none')
  })

  test('Key command errors and past expiration match Redis', async () => {
    const tag = `{key-errors:${randomKey()}}`
    const key = `${tag}:key`
    const renamed = `${tag}:renamed`
    const missing = `${tag}:missing`

    try {
      await assert.rejects(
        () => redisClient.rename(missing, renamed),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => redisClient.renameNX(missing, renamed),
        errorWithMessage('ERR no such key'),
      )
      await assert.rejects(
        () => redisClient.sendCommand(key, false, ['EXPIRE', key, 'abc']),
        errorWithMessage('ERR value is not an integer or out of range'),
      )

      assert.strictEqual(await redisClient.expireAt(missing, -1), 0)
      assert.strictEqual(await redisClient.pExpireAt(missing, -1), 0)

      await redisClient.set(key, 'value')
      assert.strictEqual(await redisClient.expireAt(key, -1), 1)
      assert.strictEqual(await redisClient.exists(key), 0)

      await redisClient.set(key, 'value')
      assert.strictEqual(await redisClient.pExpireAt(key, -1), 1)
      assert.strictEqual(await redisClient.exists(key), 0)
    } finally {
      await redisClient.del([key, renamed, missing])
    }
  })

  test('UNLINK command removes keys and returns the deleted count', async () => {
    const tag = `{unlink:${randomKey()}}`
    const first = `${tag}:first`
    const second = `${tag}:second`
    const missing = `${tag}:missing`

    try {
      await redisClient.set(first, 'one')
      await redisClient.set(second, 'two')

      assert.strictEqual(await redisClient.unlink([first, second, missing]), 2)
      assert.strictEqual(await redisClient.exists([first, second, missing]), 0)
      assert.strictEqual(await redisClient.unlink([first, missing]), 0)
    } finally {
      await redisClient.del([first, second, missing])
    }
  })

  test('DBSIZE command', async () => {
    const tag = `{dbsize:${randomKey()}}`
    const keys = {
      string: `${tag}:string_key`,
      hash: `${tag}:hash_key`,
      list: `${tag}:list_key`,
      set: `${tag}:set_key`,
      zset: `${tag}:zset_key`,
      expiring: `${tag}:expire_key1`,
    }
    const allKeys = Object.values(keys)
    const baseline = await getNodeRedisTotalDbSize(redisClient)

    try {
      await redisClient.set(keys.string, 'value')
      await redisClient.hSet(keys.hash, 'field', 'value')
      await redisClient.lPush(keys.list, 'item')
      await redisClient.sAdd(keys.set, 'member')
      await redisClient.zAdd(keys.zset, { score: 1, value: 'member' })
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 5)

      await redisClient.set(keys.expiring, 'value')
      await redisClient.expire(keys.expiring, 3600)
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 6)

      await redisClient.del([keys.string, keys.hash])
      await assertNodeRedisDbSizeDelta(redisClient, baseline, 4)
    } finally {
      await redisClient.del(allKeys)
    }
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

  test('EXPIRE and EXPIREAT commands', async () => {
    await redisClient.set('{test}expire_key', 'value')

    const expireResult = await redisClient.expire('{test}expire_key', 10)
    assert.strictEqual(expireResult, 1)

    const ttlResult = await redisClient.ttl('{test}expire_key')
    assert.ok(ttlResult <= 10 && ttlResult > 0)

    const expireNonExistent = await redisClient.expire('{test}nonexistent', 10)
    assert.strictEqual(expireNonExistent, 0)

    await redisClient.set('{test}expireat_key', 'value')

    const futureTimestamp = Math.floor(Date.now() / 1000) + 10
    const expireatResult = await redisClient.expireAt(
      '{test}expireat_key',
      futureTimestamp,
    )
    assert.strictEqual(expireatResult, 1)

    const ttlResult2 = await redisClient.ttl('{test}expireat_key')
    assert.ok(ttlResult2 <= 10 && ttlResult2 > 0)

    const expireatNonExistent = await redisClient.expireAt(
      '{test}nonexistent',
      futureTimestamp,
    )
    assert.strictEqual(expireatNonExistent, 0)
  })

  test('EXPIRE family supports conditional expiry options', async () => {
    const tag = `{expire-options:${randomKey()}}`
    const expireKey = `${tag}:expire`
    const pexpireKey = `${tag}:pexpire`
    const expireatKey = `${tag}:expireat`
    const pexpireatKey = `${tag}:pexpireat`
    const duplicateOptionKey = `${tag}:duplicate`
    const missing = `${tag}:missing`
    const directClient = await connectToNodeRedisSlotOwner(
      redisClient,
      expireKey,
    )

    try {
      await directClient.set(expireKey, 'value')
      assert.strictEqual(
        await directClient.sendCommand(['EXPIRE', expireKey, '120', 'GT']),
        0,
      )
      assert.strictEqual(await directClient.ttl(expireKey), -1)
      assert.strictEqual(
        await directClient.sendCommand(['EXPIRE', expireKey, '120', 'LT']),
        1,
      )
      assert.strictEqual(
        await directClient.sendCommand(['EXPIRE', expireKey, '60', 'GT']),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand(['EXPIRE', expireKey, '240', 'GT']),
        1,
      )
      assert.strictEqual(
        await directClient.sendCommand(['EXPIRE', expireKey, '300', 'LT']),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand(['EXPIRE', expireKey, '120', 'LT']),
        1,
      )
      const expireTtl = await directClient.ttl(expireKey)
      assert.ok(expireTtl > 0 && expireTtl <= 120)

      await directClient.set(pexpireKey, 'value')
      assert.strictEqual(
        await directClient.sendCommand(['PEXPIRE', pexpireKey, '120000', 'XX']),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand(['PEXPIRE', pexpireKey, '120000', 'NX']),
        1,
      )
      assert.strictEqual(
        await directClient.sendCommand(['PEXPIRE', pexpireKey, '240000', 'NX']),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand(['PEXPIRE', pexpireKey, '240000', 'XX']),
        1,
      )
      const pexpireTtl = await directClient.pTTL(pexpireKey)
      assert.ok(pexpireTtl > 120_000 && pexpireTtl <= 240_000)
      assert.strictEqual(
        await directClient.sendCommand([
          'PEXPIRE',
          pexpireKey,
          '300000',
          'XX',
          'GT',
        ]),
        1,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'PEXPIRE',
          pexpireKey,
          '100000',
          'XX',
          'LT',
        ]),
        1,
      )

      await directClient.set(duplicateOptionKey, 'value')
      assert.strictEqual(
        await directClient.sendCommand([
          'EXPIRE',
          duplicateOptionKey,
          '30',
          'NX',
          'NX',
        ]),
        1,
      )

      await directClient.set(expireatKey, 'value')
      const nowSeconds = Math.floor(Date.now() / 1000)
      assert.strictEqual(
        await directClient.sendCommand([
          'EXPIREAT',
          expireatKey,
          String(nowSeconds + 120),
          'NX',
        ]),
        1,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'EXPIREAT',
          expireatKey,
          String(nowSeconds + 240),
          'GT',
        ]),
        1,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'EXPIREAT',
          expireatKey,
          String(nowSeconds + 300),
          'LT',
        ]),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'EXPIREAT',
          expireatKey,
          String(nowSeconds + 120),
          'LT',
        ]),
        1,
      )

      await directClient.set(pexpireatKey, 'value')
      const nowMilliseconds = Date.now()
      assert.strictEqual(
        await directClient.sendCommand([
          'PEXPIREAT',
          pexpireatKey,
          String(nowMilliseconds + 120_000),
          'XX',
        ]),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'PEXPIREAT',
          pexpireatKey,
          String(nowMilliseconds + 120_000),
          'NX',
        ]),
        1,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'PEXPIREAT',
          pexpireatKey,
          String(nowMilliseconds + 240_000),
          'XX',
        ]),
        1,
      )

      assert.strictEqual(
        await directClient.sendCommand(['EXPIRE', missing, '10', 'NX']),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand(['PEXPIRE', missing, '10', 'XX']),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'EXPIREAT',
          missing,
          String(nowSeconds + 10),
          'GT',
        ]),
        0,
      )
      assert.strictEqual(
        await directClient.sendCommand([
          'PEXPIREAT',
          missing,
          String(nowMilliseconds + 10_000),
          'LT',
        ]),
        0,
      )

      await assert.rejects(
        () => directClient.sendCommand(['EXPIRE', expireKey, '10', 'BOGUS']),
        errorWithMessage('ERR Unsupported option BOGUS'),
      )
      await assert.rejects(
        () => directClient.sendCommand(['EXPIRE', expireKey, '10', 'NX', 'XX']),
        errorWithMessage(
          'ERR NX and XX, GT or LT options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () => directClient.sendCommand(['EXPIRE', expireKey, '10', 'GT', 'LT']),
        errorWithMessage(
          'ERR GT and LT options at the same time are not compatible',
        ),
      )
    } finally {
      await directClient.del([
        expireKey,
        pexpireKey,
        expireatKey,
        pexpireatKey,
        duplicateOptionKey,
        missing,
      ])
      directClient.destroy()
    }
  })

  test('EXPIREAT/PEXPIREAT past timestamp respects conditional flags (#72)', async () => {
    // Regression for #72: when the target timestamp is in the past, the
    // immediate delete must run *only* if the NX/XX/GT/LT condition permits the
    // update. A forbidding condition leaves the key (and its TTL) untouched.
    const tag = `{expire-past:${randomKey()}}`
    const ttlKey = `${tag}:ttl`
    const persistentKey = `${tag}:persistent`
    const directClient = await connectToNodeRedisSlotOwner(redisClient, ttlKey)

    // Past timestamps: year 2001, well before now.
    const pastSeconds = 1_000_000_000
    const pastMilliseconds = 1_000_000_000_000

    const expectCondition = async (
      command: 'EXPIREAT' | 'PEXPIREAT',
      flag: string,
      prepare: 'ttl' | 'persistent',
      expectedReturn: number,
      shouldSurvive: boolean,
    ) => {
      const key = prepare === 'ttl' ? ttlKey : persistentKey
      const past = command === 'EXPIREAT' ? pastSeconds : pastMilliseconds

      await directClient.set(key, 'value')
      if (prepare === 'ttl') {
        await directClient.expire(key, 100)
      }

      const result = await directClient.sendCommand([
        command,
        key,
        String(past),
        flag,
      ])
      assert.strictEqual(
        result,
        expectedReturn,
        `${command} past ${flag} on ${prepare} key should return ${expectedReturn}`,
      )
      assert.strictEqual(
        await directClient.exists(key),
        shouldSurvive ? 1 : 0,
        `${command} past ${flag} on ${prepare} key should ${shouldSurvive ? 'keep' : 'delete'} the key`,
      )
      if (shouldSurvive && prepare === 'ttl') {
        const ttl = await directClient.ttl(key)
        assert.ok(
          ttl > 0 && ttl <= 100,
          `${command} past ${flag} must leave the original TTL intact, got ${ttl}`,
        )
      }
      await directClient.del(key)
    }

    try {
      // Key has a TTL.
      await expectCondition('EXPIREAT', 'NX', 'ttl', 0, true) // NX: TTL exists -> no-op
      await expectCondition('EXPIREAT', 'GT', 'ttl', 0, true) // GT: past < future -> no-op
      await expectCondition('EXPIREAT', 'XX', 'ttl', 1, false) // XX: TTL exists -> delete
      await expectCondition('EXPIREAT', 'LT', 'ttl', 1, false) // LT: past < future -> delete

      // Key is persistent (no TTL).
      await expectCondition('EXPIREAT', 'XX', 'persistent', 0, true) // XX: no TTL -> no-op
      await expectCondition('EXPIREAT', 'NX', 'persistent', 1, false) // NX: no TTL -> delete
      await expectCondition('EXPIREAT', 'GT', 'persistent', 0, true) // GT vs persistent -> no-op
      await expectCondition('EXPIREAT', 'LT', 'persistent', 1, false) // LT vs persistent -> delete

      // PEXPIREAT shares the same code path — spot-check both outcomes.
      await expectCondition('PEXPIREAT', 'NX', 'ttl', 0, true)
      await expectCondition('PEXPIREAT', 'XX', 'ttl', 1, false)
    } finally {
      await directClient.del([ttlKey, persistentKey])
      directClient.destroy()
    }
  })

  test('TTL integration with EXPIRE and EXPIREAT', async () => {
    await redisClient.set('{test}ttl1', 'value1')
    await redisClient.set('{test}ttl2', 'value2')
    await redisClient.set('{test}ttl3', 'value3')

    await redisClient.expire('{test}ttl1', 20)

    const futureTimestamp = Math.floor(Date.now() / 1000) + 30
    await redisClient.expireAt('{test}ttl2', futureTimestamp)

    const ttl1 = await redisClient.ttl('{test}ttl1')
    assert.ok(ttl1 <= 20 && ttl1 > 0)

    const ttl2 = await redisClient.ttl('{test}ttl2')
    assert.ok(ttl2 <= 30 && ttl2 > 0)

    const ttl3 = await redisClient.ttl('{test}ttl3')
    assert.strictEqual(ttl3, -1)

    const ttlNonExistent = await redisClient.ttl('{test}nonexistent')
    assert.strictEqual(ttlNonExistent, -2)
  })

  test('EXPIRETIME and PEXPIRETIME return absolute expiry, -1, -2', async () => {
    const tag = `{exptime:${randomKey()}}`
    const withTtl = `${tag}:withttl`
    const noTtl = `${tag}:nottl`
    const missing = `${tag}:missing`

    const before = Math.floor(Date.now() / 1000)
    await redisClient.set(withTtl, 'value', {
      expiration: { type: 'EX', value: 100 },
    })
    const after = Math.floor(Date.now() / 1000)

    const expiretime = await redisClient.expireTime(withTtl)
    assert.ok(
      expiretime >= before + 98 && expiretime <= after + 102,
      `EXPIRETIME ${expiretime} not in [${before + 98}, ${after + 102}]`,
    )

    const pexpiretime = await redisClient.pExpireTime(withTtl)
    assert.ok(
      pexpiretime >= (before + 98) * 1000 &&
        pexpiretime <= (after + 102) * 1000,
      `PEXPIRETIME ${pexpiretime} not in ms range`,
    )
    assert.strictEqual(Math.round(pexpiretime / 1000), expiretime)

    await redisClient.set(noTtl, 'value')
    assert.strictEqual(await redisClient.expireTime(noTtl), -1)
    assert.strictEqual(await redisClient.pExpireTime(noTtl), -1)

    assert.strictEqual(await redisClient.expireTime(missing), -2)
    assert.strictEqual(await redisClient.pExpireTime(missing), -2)

    // Type-agnostic: works on any keyed type, not just strings (no WRONGTYPE)
    const listKey = `${tag}:list`
    await redisClient.rPush(listKey, ['a', 'b'])
    await redisClient.expire(listKey, 100)
    const listExpiretime = await redisClient.expireTime(listKey)
    assert.ok(
      listExpiretime >= before + 98 && listExpiretime <= after + 102,
      `EXPIRETIME on list ${listExpiretime} not in expected range`,
    )
    assert.strictEqual(
      Math.round((await redisClient.pExpireTime(listKey)) / 1000),
      listExpiretime,
    )

    await assert.rejects(
      () =>
        redisClient.sendCommand(withTtl, true, [
          'EXPIRETIME',
          withTtl,
          'extra',
        ]),
      errorWithMessage(
        "ERR wrong number of arguments for 'expiretime' command",
      ),
    )
    await assert.rejects(
      () =>
        redisClient.sendCommand(withTtl, true, [
          'PEXPIRETIME',
          withTtl,
          'extra',
        ]),
      errorWithMessage(
        "ERR wrong number of arguments for 'pexpiretime' command",
      ),
    )
  })

  test('TTL rounds to nearest second like real Redis (#59)', async () => {
    const tag = `{ttlround:${randomKey()}}`

    const reproKey = `${tag}:repro`
    await redisClient.set(reproKey, 'v', {
      expiration: { type: 'PX', value: 1500 },
    })
    await new Promise(resolve => setTimeout(resolve, 5))
    assert.strictEqual(
      await redisClient.ttl(reproKey),
      1,
      'TTL of PX 1500 must round to 1 (Math.ceil regression gives 2)',
    )

    const pttl = await redisClient.pTTL(reproKey)
    assert.ok(pttl > 1000 && pttl <= 1500, `PTTL should be raw ms, got ${pttl}`)

    const downKey = `${tag}:down`
    await redisClient.set(downKey, 'v', {
      expiration: { type: 'PX', value: 1200 },
    })
    assert.strictEqual(await redisClient.ttl(downKey), 1)

    const upKey = `${tag}:up`
    await redisClient.set(upKey, 'v', {
      expiration: { type: 'PX', value: 1900 },
    })
    assert.strictEqual(await redisClient.ttl(upKey), 2)

    const up2Key = `${tag}:up2`
    await redisClient.set(up2Key, 'v', {
      expiration: { type: 'PX', value: 2900 },
    })
    assert.strictEqual(await redisClient.ttl(up2Key), 3)

    assert.strictEqual(await redisClient.ttl(`${tag}:missing`), -2)
    const persistKey = `${tag}:persist`
    await redisClient.set(persistKey, 'v')
    assert.strictEqual(await redisClient.ttl(persistKey), -1)

    const listKey = `${tag}:list`
    await redisClient.rPush(listKey, 'a')
    await redisClient.pExpire(listKey, 1900)
    assert.strictEqual(await redisClient.ttl(listKey), 2)

    await assert.rejects(
      () => redisClient.sendCommand(reproKey, true, ['TTL', reproKey, 'extra']),
      errorWithMessage("ERR wrong number of arguments for 'ttl' command"),
    )
    await assert.rejects(
      () =>
        redisClient.sendCommand(reproKey, true, ['PTTL', reproKey, 'extra']),
      errorWithMessage("ERR wrong number of arguments for 'pttl' command"),
    )
  })

  test('PERSIST removes expiration and EXPIRE 0 deletes the key', async () => {
    const tag = `{persist:${randomKey()}}`
    const persistentKey = `${tag}:persistent`
    const deletedKey = `${tag}:deleted`

    await redisClient.set(persistentKey, 'value')
    assert.strictEqual(await redisClient.expire(persistentKey, 10), 1)
    const expiringTtl = await redisClient.ttl(persistentKey)
    assert.ok(expiringTtl > 0 && expiringTtl <= 10)

    assert.strictEqual(await redisClient.persist(persistentKey), 1)
    assert.strictEqual(await redisClient.ttl(persistentKey), -1)
    assert.strictEqual(await redisClient.persist(persistentKey), 0)

    await redisClient.set(deletedKey, 'value')
    assert.strictEqual(await redisClient.expire(deletedKey, 0), 1)
    assert.strictEqual(await redisClient.exists(deletedKey), 0)
    assert.strictEqual(await redisClient.ttl(deletedKey), -2)
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
