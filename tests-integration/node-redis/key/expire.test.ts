import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import {
  connectToNodeRedisSlotOwner,
  errorWithMessage,
  flushNodeRedisCluster,
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
})
