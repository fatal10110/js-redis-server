import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { connectToSlotOwner, errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Key Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('key-integration')
  })

  after(async () => {
    await testRunner.cleanup()
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

  test('EXPIRE family supports conditional expiry options', async () => {
    const tag = `{expire-options:${randomKey()}}`
    const expireKey = `${tag}:expire`
    const pexpireKey = `${tag}:pexpire`
    const expireatKey = `${tag}:expireat`
    const pexpireatKey = `${tag}:pexpireat`
    const duplicateOptionKey = `${tag}:duplicate`
    const missing = `${tag}:missing`
    const directClient = await connectToSlotOwner(redisClient!, expireKey)

    try {
      await directClient.set(expireKey, 'value')
      assert.strictEqual(
        await directClient.call('EXPIRE', expireKey, 120, 'GT'),
        0,
      )
      assert.strictEqual(await directClient.ttl(expireKey), -1)
      assert.strictEqual(
        await directClient.call('EXPIRE', expireKey, 120, 'LT'),
        1,
      )
      assert.strictEqual(
        await directClient.call('EXPIRE', expireKey, 60, 'GT'),
        0,
      )
      assert.strictEqual(
        await directClient.call('EXPIRE', expireKey, 240, 'GT'),
        1,
      )
      assert.strictEqual(
        await directClient.call('EXPIRE', expireKey, 300, 'LT'),
        0,
      )
      assert.strictEqual(
        await directClient.call('EXPIRE', expireKey, 120, 'LT'),
        1,
      )
      const expireTtl = await directClient.ttl(expireKey)
      assert.ok(expireTtl > 0 && expireTtl <= 120)

      await directClient.set(pexpireKey, 'value')
      assert.strictEqual(
        await directClient.call('PEXPIRE', pexpireKey, 120_000, 'XX'),
        0,
      )
      assert.strictEqual(
        await directClient.call('PEXPIRE', pexpireKey, 120_000, 'NX'),
        1,
      )
      assert.strictEqual(
        await directClient.call('PEXPIRE', pexpireKey, 240_000, 'NX'),
        0,
      )
      assert.strictEqual(
        await directClient.call('PEXPIRE', pexpireKey, 240_000, 'XX'),
        1,
      )
      const pexpireTtl = await directClient.pttl(pexpireKey)
      assert.ok(pexpireTtl > 120_000 && pexpireTtl <= 240_000)
      assert.strictEqual(
        await directClient.call('PEXPIRE', pexpireKey, 300_000, 'XX', 'GT'),
        1,
      )
      assert.strictEqual(
        await directClient.call('PEXPIRE', pexpireKey, 100_000, 'XX', 'LT'),
        1,
      )

      await directClient.set(duplicateOptionKey, 'value')
      assert.strictEqual(
        await directClient.call('EXPIRE', duplicateOptionKey, 30, 'NX', 'NX'),
        1,
      )

      await directClient.set(expireatKey, 'value')
      const nowSeconds = Math.floor(Date.now() / 1000)
      assert.strictEqual(
        await directClient.call(
          'EXPIREAT',
          expireatKey,
          nowSeconds + 120,
          'NX',
        ),
        1,
      )
      assert.strictEqual(
        await directClient.call(
          'EXPIREAT',
          expireatKey,
          nowSeconds + 240,
          'GT',
        ),
        1,
      )
      assert.strictEqual(
        await directClient.call(
          'EXPIREAT',
          expireatKey,
          nowSeconds + 300,
          'LT',
        ),
        0,
      )
      assert.strictEqual(
        await directClient.call(
          'EXPIREAT',
          expireatKey,
          nowSeconds + 120,
          'LT',
        ),
        1,
      )

      await directClient.set(pexpireatKey, 'value')
      const nowMilliseconds = Date.now()
      assert.strictEqual(
        await directClient.call(
          'PEXPIREAT',
          pexpireatKey,
          nowMilliseconds + 120_000,
          'XX',
        ),
        0,
      )
      assert.strictEqual(
        await directClient.call(
          'PEXPIREAT',
          pexpireatKey,
          nowMilliseconds + 120_000,
          'NX',
        ),
        1,
      )
      assert.strictEqual(
        await directClient.call(
          'PEXPIREAT',
          pexpireatKey,
          nowMilliseconds + 240_000,
          'XX',
        ),
        1,
      )

      assert.strictEqual(
        await directClient.call('EXPIRE', missing, 10, 'NX'),
        0,
      )
      assert.strictEqual(
        await directClient.call('PEXPIRE', missing, 10, 'XX'),
        0,
      )
      assert.strictEqual(
        await directClient.call('EXPIREAT', missing, nowSeconds + 10, 'GT'),
        0,
      )
      assert.strictEqual(
        await directClient.call(
          'PEXPIREAT',
          missing,
          nowMilliseconds + 10_000,
          'LT',
        ),
        0,
      )

      await assert.rejects(
        () => directClient.call('EXPIRE', expireKey, 10, 'BOGUS'),
        errorWithMessage('ERR Unsupported option BOGUS'),
      )
      await assert.rejects(
        () => directClient.call('EXPIRE', expireKey, 10, 'NX', 'XX'),
        errorWithMessage(
          'ERR NX and XX, GT or LT options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () => directClient.call('EXPIRE', expireKey, 10, 'GT', 'LT'),
        errorWithMessage(
          'ERR GT and LT options at the same time are not compatible',
        ),
      )
    } finally {
      await directClient.del(
        expireKey,
        pexpireKey,
        expireatKey,
        pexpireatKey,
        duplicateOptionKey,
        missing,
      )
      directClient.disconnect()
    }
  })

  test('EXPIREAT/PEXPIREAT past timestamp respects conditional flags (#72)', async () => {
    // Regression for #72: when the target timestamp is in the past, the
    // immediate delete must run *only* if the NX/XX/GT/LT condition permits the
    // update. A forbidding condition leaves the key (and its TTL) untouched.
    const tag = `{expire-past:${randomKey()}}`
    const ttlKey = `${tag}:ttl`
    const persistentKey = `${tag}:persistent`
    const directClient = await connectToSlotOwner(redisClient!, ttlKey)

    // Past timestamps: year 2001, well before now.
    const pastSeconds = 1_000_000_000
    const pastMilliseconds = 1_000_000_000_000

    // Each case: prepares the key, runs EXPIREAT/PEXPIREAT past + flag, asserts
    // the return value and whether the key survived — matching real Redis.
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

      const result = await directClient.call(command, key, past, flag)
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
      await directClient.del(ttlKey, persistentKey)
      directClient.disconnect()
    }
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

  test('EXPIRETIME and PEXPIRETIME return absolute expiry, -1, -2', async () => {
    const tag = `{exptime:${randomKey()}}`
    const withTtl = `${tag}:withttl`
    const noTtl = `${tag}:nottl`
    const missing = `${tag}:missing`

    // Key with a TTL set via SET ... EX 100
    const before = Math.floor(Date.now() / 1000)
    await redisClient?.set(withTtl, 'value', 'EX', 100)
    const after = Math.floor(Date.now() / 1000)

    const expiretime = await redisClient!.expiretime(withTtl)
    // Absolute Unix expiry in seconds ≈ now + 100 (±2s for clock skew/rounding)
    assert.ok(
      expiretime >= before + 98 && expiretime <= after + 102,
      `EXPIRETIME ${expiretime} not in [${before + 98}, ${after + 102}]`,
    )

    const pexpiretime = await redisClient!.pexpiretime(withTtl)
    // Absolute Unix expiry in milliseconds
    assert.ok(
      pexpiretime >= (before + 98) * 1000 &&
        pexpiretime <= (after + 102) * 1000,
      `PEXPIRETIME ${pexpiretime} not in ms range`,
    )
    // EXPIRETIME is PEXPIRETIME rounded to the nearest second (Redis: (ms+500)/1000)
    assert.strictEqual(Math.round(pexpiretime / 1000), expiretime)

    // Key without a TTL → -1
    await redisClient?.set(noTtl, 'value')
    assert.strictEqual(await redisClient?.expiretime(noTtl), -1)
    assert.strictEqual(await redisClient?.pexpiretime(noTtl), -1)

    // Missing key → -2
    assert.strictEqual(await redisClient?.expiretime(missing), -2)
    assert.strictEqual(await redisClient?.pexpiretime(missing), -2)

    // Type-agnostic: works on any keyed type, not just strings (no WRONGTYPE)
    const listKey = `${tag}:list`
    await redisClient?.rpush(listKey, 'a', 'b')
    await redisClient?.expire(listKey, 100)
    const listExpiretime = await redisClient!.expiretime(listKey)
    assert.ok(
      listExpiretime >= before + 98 && listExpiretime <= after + 102,
      `EXPIRETIME on list ${listExpiretime} not in expected range`,
    )
    assert.strictEqual(
      Math.round((await redisClient!.pexpiretime(listKey)) / 1000),
      listExpiretime,
    )

    // Wrong arity → error. (The zero-argument case can't be issued through a
    // cluster client — it injects an implicit routing key — so it's covered by
    // the raw-TCP protocol-errors suite instead.)
    await assert.rejects(
      () => redisClient!.call('EXPIRETIME', withTtl, 'extra'),
      errorWithMessage(
        "ERR wrong number of arguments for 'expiretime' command",
      ),
    )
    await assert.rejects(
      () => redisClient!.call('PEXPIRETIME', withTtl, 'extra'),
      errorWithMessage(
        "ERR wrong number of arguments for 'pexpiretime' command",
      ),
    )
  })

  test('TTL rounds to nearest second like real Redis (#59)', async () => {
    const tag = `{ttlround:${randomKey()}}`

    // Issue #59 repro: SET k v PX 1500 → TTL must round to 1, not 2.
    // Real Redis rounds (ms+500)/1000 (round-half-up); the bug used Math.ceil.
    // Settle a few ms so PTTL clears the exact .5 boundary deterministically.
    const reproKey = `${tag}:repro`
    await redisClient!.set(reproKey, 'v', 'PX', 1500)
    await new Promise(resolve => setTimeout(resolve, 5))
    assert.strictEqual(
      await redisClient!.ttl(reproKey),
      1,
      'TTL of PX 1500 must round to 1 (Math.ceil regression gives 2)',
    )

    // PTTL reports raw milliseconds, never rounded.
    const pttl = await redisClient!.pttl(reproKey)
    assert.ok(pttl > 1000 && pttl <= 1500, `PTTL should be raw ms, got ${pttl}`)

    // Fractional part < 0.5 rounds down — catches a Math.ceil regression.
    const downKey = `${tag}:down`
    await redisClient!.set(downKey, 'v', 'PX', 1200)
    assert.strictEqual(await redisClient!.ttl(downKey), 1)

    // Fractional part >= 0.5 rounds up — catches a Math.floor "fix".
    const upKey = `${tag}:up`
    await redisClient!.set(upKey, 'v', 'PX', 1900)
    assert.strictEqual(await redisClient!.ttl(upKey), 2)

    const up2Key = `${tag}:up2`
    await redisClient!.set(up2Key, 'v', 'PX', 2900)
    assert.strictEqual(await redisClient!.ttl(up2Key), 3)

    // Sentinels: missing key → -2, persistent key → -1.
    assert.strictEqual(await redisClient!.ttl(`${tag}:missing`), -2)
    const persistKey = `${tag}:persist`
    await redisClient!.set(persistKey, 'v')
    assert.strictEqual(await redisClient!.ttl(persistKey), -1)

    // TTL is type-agnostic: works on non-string keys without WRONGTYPE.
    const listKey = `${tag}:list`
    await redisClient!.rpush(listKey, 'a')
    await redisClient!.pexpire(listKey, 1900)
    assert.strictEqual(await redisClient!.ttl(listKey), 2)

    // Wrong arity → error. (The zero-argument case can't be issued through a
    // cluster client — it injects an implicit routing key — so it's covered by
    // the raw-TCP protocol-errors suite instead.)
    await assert.rejects(
      () => redisClient!.call('TTL', reproKey, 'extra'),
      errorWithMessage("ERR wrong number of arguments for 'ttl' command"),
    )
    await assert.rejects(
      () => redisClient!.call('PTTL', reproKey, 'extra'),
      errorWithMessage("ERR wrong number of arguments for 'pttl' command"),
    )
  })

  test('PERSIST removes expiration and EXPIRE 0 deletes the key', async () => {
    const tag = `{persist:${randomKey()}}`
    const persistentKey = `${tag}:persistent`
    const deletedKey = `${tag}:deleted`

    await redisClient?.set(persistentKey, 'value')
    assert.strictEqual(await redisClient?.expire(persistentKey, 10), 1)
    const expiringTtl = await redisClient!.ttl(persistentKey)
    assert.ok(expiringTtl > 0 && expiringTtl <= 10)

    assert.strictEqual(await redisClient?.persist(persistentKey), 1)
    assert.strictEqual(await redisClient?.ttl(persistentKey), -1)
    assert.strictEqual(await redisClient?.persist(persistentKey), 0)

    await redisClient?.set(deletedKey, 'value')
    assert.strictEqual(await redisClient?.expire(deletedKey, 0), 1)
    assert.strictEqual(await redisClient?.exists(deletedKey), 0)
    assert.strictEqual(await redisClient?.ttl(deletedKey), -2)
  })
})
