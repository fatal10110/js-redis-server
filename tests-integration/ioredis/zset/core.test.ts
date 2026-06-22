import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('zset-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('ZADD and ZCARD commands', async () => {
    // ZADD single member
    const add1 = await redisClient?.zadd('zset1', 10, 'member1')
    assert.strictEqual(add1, 1)

    // ZADD multiple members
    const add2 = await redisClient?.zadd('zset1', 20, 'member2', 30, 'member3')
    assert.strictEqual(add2, 2)

    // Update existing member score
    const add3 = await redisClient?.zadd('zset1', 15, 'member1')
    assert.strictEqual(add3, 0) // No new members added

    // Check cardinality
    const card = await redisClient?.zcard('zset1')
    assert.strictEqual(card, 3)
  })

  test('ZADD option flags match Redis', async () => {
    const zsetKey = `{zadd-options:${randomKey()}}`

    try {
      assert.strictEqual(await redisClient?.zadd(zsetKey, 'NX', 1, 'one'), 1)
      assert.strictEqual(await redisClient?.zadd(zsetKey, 'NX', 2, 'one'), 0)
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'one'), '1')

      assert.strictEqual(await redisClient?.zadd(zsetKey, 'XX', 2, 'one'), 0)
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'one'), '2')
      assert.strictEqual(await redisClient?.zadd(zsetKey, 'XX', 3, 'two'), 0)
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'two'), null)

      assert.strictEqual(await redisClient?.zadd(zsetKey, 'GT', 1, 'one'), 0)
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'one'), '2')
      assert.strictEqual(await redisClient?.zadd(zsetKey, 'GT', 3, 'one'), 0)
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'one'), '3')

      assert.strictEqual(await redisClient?.zadd(zsetKey, 'LT', 4, 'one'), 0)
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'one'), '3')
      assert.strictEqual(await redisClient?.zadd(zsetKey, 'LT', 2, 'one'), 0)
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'one'), '2')

      assert.strictEqual(
        await redisClient?.zadd(zsetKey, 'CH', 2, 'one', 4, 'two'),
        1,
      )
      assert.strictEqual(await redisClient?.zadd(zsetKey, 'CH', 5, 'two'), 1)
      assert.strictEqual(await redisClient?.zadd(zsetKey, 'CH', 5, 'two'), 0)

      assert.strictEqual(
        await redisClient?.zadd(zsetKey, 'INCR', 2.5, 'one'),
        '4.5',
      )
      assert.strictEqual(await redisClient?.zscore(zsetKey, 'one'), '4.5')
      assert.strictEqual(
        await redisClient?.zadd(zsetKey, 'NX', 'INCR', 1, 'one'),
        null,
      )
      assert.strictEqual(
        await redisClient?.zadd(zsetKey, 'XX', 'INCR', 1, 'missing'),
        null,
      )
    } finally {
      await redisClient?.del(zsetKey)
    }
  })

  test('ZADD option syntax errors match Redis', async () => {
    const zsetKey = `{zadd-option-errors:${randomKey()}}`

    try {
      await assert.rejects(
        () => redisClient?.zadd(zsetKey, 'NX', 'XX', 1, 'one'),
        errorWithMessage(
          'ERR XX and NX options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () => redisClient?.zadd(zsetKey, 'GT', 'LT', 1, 'one'),
        errorWithMessage(
          'ERR GT, LT, and/or NX options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () => redisClient?.zadd(zsetKey, 'NX', 'GT', 1, 'one'),
        errorWithMessage(
          'ERR GT, LT, and/or NX options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () => redisClient?.zadd(zsetKey, 'INCR', 1, 'one', 2, 'two'),
        errorWithMessage(
          'ERR INCR option supports a single increment-element pair',
        ),
      )
    } finally {
      await redisClient?.del(zsetKey)
    }
  })

  test('ZSCORE command', async () => {
    await redisClient?.zadd('zset2', 15, 'member1', 25, 'member2')

    const score1 = await redisClient?.zscore('zset2', 'member1')
    assert.strictEqual(score1, '15')

    const score2 = await redisClient?.zscore('zset2', 'nonexistent')
    assert.strictEqual(score2, null)
  })

  test('ZINCRBY command', async () => {
    await redisClient?.zadd('zset6', 10, 'member1')

    // Increment existing member
    const incr1 = await redisClient?.zincrby('zset6', 25, 'member1')
    assert.strictEqual(incr1, '35')

    // Increment non-existent member
    const incr2 = await redisClient?.zincrby('zset6', 50, 'member2')
    assert.strictEqual(incr2, '50')

    // Verify scores
    const score1 = await redisClient?.zscore('zset6', 'member1')
    const score2 = await redisClient?.zscore('zset6', 'member2')
    assert.strictEqual(score1, '35')
    assert.strictEqual(score2, '50')
  })

  test('sorted set commands accept and return Redis infinity score tokens', async () => {
    const tag = `{zset-infinity:${randomKey()}}`
    const zincrbyKey = `${tag}:zincrby`
    const zaddKey = `${tag}:zadd`

    try {
      assert.strictEqual(await redisClient?.zadd(zincrbyKey, 5, 'finite'), 1)
      assert.strictEqual(
        await redisClient?.zincrby(zincrbyKey, '+INF', 'positive'),
        'inf',
      )
      assert.strictEqual(
        await redisClient?.zincrby(zincrbyKey, '-inf', 'negative'),
        '-inf',
      )

      assert.strictEqual(
        await redisClient?.zscore(zincrbyKey, 'positive'),
        'inf',
      )
      assert.strictEqual(
        await redisClient?.zscore(zincrbyKey, 'negative'),
        '-inf',
      )
      await assert.rejects(
        () => redisClient?.zincrby(zincrbyKey, '-inf', 'positive'),
        errorWithMessage('ERR resulting score is not a number (NaN)'),
      )
      assert.strictEqual(
        await redisClient?.zscore(zincrbyKey, 'positive'),
        'inf',
      )
      assert.deepStrictEqual(
        await redisClient?.zrange(zincrbyKey, 0, -1, 'WITHSCORES'),
        ['negative', '-inf', 'finite', '5', 'positive', 'inf'],
      )
      assert.deepStrictEqual(await redisClient?.zpopmin(zincrbyKey), [
        'negative',
        '-inf',
      ])
      assert.deepStrictEqual(await redisClient?.zpopmax(zincrbyKey), [
        'positive',
        'inf',
      ])

      assert.strictEqual(
        await redisClient?.zadd(
          zaddKey,
          '+inf',
          'positive',
          '-INF',
          'negative',
        ),
        2,
      )
      assert.strictEqual(await redisClient?.zscore(zaddKey, 'positive'), 'inf')
      assert.strictEqual(await redisClient?.zscore(zaddKey, 'negative'), '-inf')

      const zscan = await redisClient?.zscan(zaddKey, '0')
      assert.ok(zscan)
      const [, zscanItems] = zscan
      const scannedScores = new Map<string, string>()
      for (let i = 0; i < zscanItems.length; i += 2) {
        scannedScores.set(zscanItems[i], zscanItems[i + 1])
      }
      assert.strictEqual(scannedScores.get('positive'), 'inf')
      assert.strictEqual(scannedScores.get('negative'), '-inf')
    } finally {
      await redisClient?.del(zincrbyKey, zaddKey)
    }
  })

  test('Sorted set command errors match Redis', async () => {
    const tag = `{zset-errors:${randomKey()}}`
    const zsetKey = `${tag}:zset`
    const stringKey = `${tag}:string`

    try {
      await redisClient?.zadd(zsetKey, 1, 'a')
      await redisClient?.set(stringKey, 'value')

      await assert.rejects(
        () => redisClient?.zcard(stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient?.call('ZADD', zsetKey, 'abc', 'member'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient?.call('ZINCRBY', zsetKey, 'abc', 'a'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient?.call('ZINCRBY', zsetKey, 'nan', 'a'),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () => redisClient?.call('ZRANGE', zsetKey, 'abc', '-1'),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () => redisClient?.call('ZRANGEBYSCORE', zsetKey, 'abc', '1'),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () => redisClient?.call('ZREMRANGEBYSCORE', zsetKey, '0', 'abc'),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () => redisClient?.call('ZCOUNT', zsetKey, 'abc', '1'),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () => redisClient?.call('ZPOPMIN', zsetKey, 'abc'),
        errorWithMessage('ERR value is out of range, must be positive'),
      )
      await assert.rejects(
        () => redisClient?.call('ZPOPMAX', zsetKey, '-1'),
        errorWithMessage('ERR value is out of range, must be positive'),
      )

      assert.deepStrictEqual(
        await redisClient?.call('ZPOPMIN', zsetKey, '0'),
        [],
      )
    } finally {
      await redisClient?.del(zsetKey, stringKey)
    }
  })

  test('ZREM command', async () => {
    await redisClient?.zadd('zset7', 1, 'one', 2, 'two', 3, 'three', 4, 'four')

    // Remove single member
    const rem1 = await redisClient?.zrem('zset7', 'two')
    assert.strictEqual(rem1, 1)

    // Remove multiple members
    const rem2 = await redisClient?.zrem('zset7', 'one', 'three')
    assert.strictEqual(rem2, 2)

    // Remove non-existent member
    const rem3 = await redisClient?.zrem('zset7', 'nonexistent')
    assert.strictEqual(rem3, 0)

    // Check remaining members
    const remaining = await redisClient?.zrange('zset7', 0, -1)
    assert.deepStrictEqual(remaining, ['four'])
  })
})
