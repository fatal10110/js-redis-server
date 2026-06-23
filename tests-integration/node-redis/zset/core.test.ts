import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../../utils'

const testRunner = new TestRunner()

describe(`Sorted Set Commands Integration (node-redis, ${testRunner.getBackendName()})`, () => {
  let redisClient: RedisClusterType

  before(async () => {
    redisClient = (await testRunner.setupNodeRedisCluster()) as RedisClusterType
    await flushNodeRedisCluster(redisClient)
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('ZADD and ZCARD commands', async () => {
    const add1 = await redisClient.zAdd('zset1', {
      score: 10,
      value: 'member1',
    })
    assert.strictEqual(add1, 1)

    const add2 = await redisClient.zAdd('zset1', [
      { score: 20, value: 'member2' },
      { score: 30, value: 'member3' },
    ])
    assert.strictEqual(add2, 2)

    // Update existing member score
    const add3 = await redisClient.zAdd('zset1', {
      score: 15,
      value: 'member1',
    })
    assert.strictEqual(add3, 0) // No new members added

    const card = await redisClient.zCard('zset1')
    assert.strictEqual(card, 3)
  })

  test('ZADD option flags match Redis', async () => {
    const zsetKey = `{zadd-options:${randomKey()}}`

    try {
      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 1, value: 'one' },
          { condition: 'NX' },
        ),
        1,
      )
      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 2, value: 'one' },
          { condition: 'NX' },
        ),
        0,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'one'), 1)

      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 2, value: 'one' },
          { condition: 'XX' },
        ),
        0,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'one'), 2)
      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 3, value: 'two' },
          { condition: 'XX' },
        ),
        0,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'two'), null)

      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 1, value: 'one' },
          { comparison: 'GT' },
        ),
        0,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'one'), 2)
      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 3, value: 'one' },
          { comparison: 'GT' },
        ),
        0,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'one'), 3)

      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 4, value: 'one' },
          { comparison: 'LT' },
        ),
        0,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'one'), 3)
      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 2, value: 'one' },
          { comparison: 'LT' },
        ),
        0,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'one'), 2)

      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          [
            { score: 2, value: 'one' },
            { score: 4, value: 'two' },
          ],
          { CH: true },
        ),
        1,
      )
      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 5, value: 'two' },
          { CH: true },
        ),
        1,
      )
      assert.strictEqual(
        await redisClient.zAdd(
          zsetKey,
          { score: 5, value: 'two' },
          { CH: true },
        ),
        0,
      )

      assert.strictEqual(
        await redisClient.zAddIncr(zsetKey, { score: 2.5, value: 'one' }),
        4.5,
      )
      assert.strictEqual(await redisClient.zScore(zsetKey, 'one'), 4.5)
      assert.strictEqual(
        await redisClient.zAddIncr(
          zsetKey,
          { score: 1, value: 'one' },
          { condition: 'NX' },
        ),
        null,
      )
      assert.strictEqual(
        await redisClient.zAddIncr(
          zsetKey,
          { score: 1, value: 'missing' },
          { condition: 'XX' },
        ),
        null,
      )
    } finally {
      await redisClient.del(zsetKey)
    }
  })

  test('ZADD option syntax errors match Redis', async () => {
    const zsetKey = `{zadd-option-errors:${randomKey()}}`

    try {
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZADD',
            zsetKey,
            'NX',
            'XX',
            '1',
            'one',
          ]),
        errorWithMessage(
          'ERR XX and NX options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZADD',
            zsetKey,
            'GT',
            'LT',
            '1',
            'one',
          ]),
        errorWithMessage(
          'ERR GT, LT, and/or NX options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZADD',
            zsetKey,
            'NX',
            'GT',
            '1',
            'one',
          ]),
        errorWithMessage(
          'ERR GT, LT, and/or NX options at the same time are not compatible',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZADD',
            zsetKey,
            'INCR',
            '1',
            'one',
            '2',
            'two',
          ]),
        errorWithMessage(
          'ERR INCR option supports a single increment-element pair',
        ),
      )
    } finally {
      await redisClient.del(zsetKey)
    }
  })

  test('ZSCORE command', async () => {
    await redisClient.zAdd('zset2', [
      { score: 15, value: 'member1' },
      { score: 25, value: 'member2' },
    ])

    assert.strictEqual(await redisClient.zScore('zset2', 'member1'), 15)
    assert.strictEqual(await redisClient.zScore('zset2', 'nonexistent'), null)
  })

  test('ZINCRBY command', async () => {
    await redisClient.zAdd('zset6', { score: 10, value: 'member1' })

    assert.strictEqual(await redisClient.zIncrBy('zset6', 25, 'member1'), 35)
    assert.strictEqual(await redisClient.zIncrBy('zset6', 50, 'member2'), 50)

    assert.strictEqual(await redisClient.zScore('zset6', 'member1'), 35)
    assert.strictEqual(await redisClient.zScore('zset6', 'member2'), 50)
  })

  test('sorted set commands accept and return Redis infinity score tokens', async () => {
    const tag = `{zset-infinity:${randomKey()}}`
    const zincrbyKey = `${tag}:zincrby`
    const zaddKey = `${tag}:zadd`

    try {
      assert.strictEqual(
        await redisClient.zAdd(zincrbyKey, { score: 5, value: 'finite' }),
        1,
      )
      assert.strictEqual(
        await redisClient.zIncrBy(zincrbyKey, Infinity, 'positive'),
        Infinity,
      )
      assert.strictEqual(
        await redisClient.zIncrBy(zincrbyKey, -Infinity, 'negative'),
        -Infinity,
      )

      assert.strictEqual(
        await redisClient.zScore(zincrbyKey, 'positive'),
        Infinity,
      )
      assert.strictEqual(
        await redisClient.zScore(zincrbyKey, 'negative'),
        -Infinity,
      )
      await assert.rejects(
        () => redisClient.zIncrBy(zincrbyKey, -Infinity, 'positive'),
        errorWithMessage('ERR resulting score is not a number (NaN)'),
      )
      assert.strictEqual(
        await redisClient.zScore(zincrbyKey, 'positive'),
        Infinity,
      )
      assert.deepStrictEqual(
        await redisClient.zRangeWithScores(zincrbyKey, 0, -1),
        [
          { value: 'negative', score: -Infinity },
          { value: 'finite', score: 5 },
          { value: 'positive', score: Infinity },
        ],
      )
      assert.deepStrictEqual(await redisClient.zPopMin(zincrbyKey), {
        value: 'negative',
        score: -Infinity,
      })
      assert.deepStrictEqual(await redisClient.zPopMax(zincrbyKey), {
        value: 'positive',
        score: Infinity,
      })

      assert.strictEqual(
        await redisClient.zAdd(zaddKey, [
          { score: Infinity, value: 'positive' },
          { score: -Infinity, value: 'negative' },
        ]),
        2,
      )
      assert.strictEqual(
        await redisClient.zScore(zaddKey, 'positive'),
        Infinity,
      )
      assert.strictEqual(
        await redisClient.zScore(zaddKey, 'negative'),
        -Infinity,
      )

      const zscan = await redisClient.zScan(zaddKey, '0')
      const scannedScores = new Map<string, number>()
      for (const { value, score } of zscan.members) {
        scannedScores.set(value, score)
      }
      assert.strictEqual(scannedScores.get('positive'), Infinity)
      assert.strictEqual(scannedScores.get('negative'), -Infinity)
    } finally {
      await redisClient.del([zincrbyKey, zaddKey])
    }
  })

  test('Sorted set command errors match Redis', async () => {
    const tag = `{zset-errors:${randomKey()}}`
    const zsetKey = `${tag}:zset`
    const stringKey = `${tag}:string`

    try {
      await redisClient.zAdd(zsetKey, { score: 1, value: 'a' })
      await redisClient.set(stringKey, 'value')

      await assert.rejects(
        () => redisClient.zCard(stringKey),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZADD',
            zsetKey,
            'abc',
            'member',
          ]),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZINCRBY',
            zsetKey,
            'abc',
            'a',
          ]),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZINCRBY',
            zsetKey,
            'nan',
            'a',
          ]),
        errorWithMessage('ERR value is not a valid float'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, true, [
            'ZRANGE',
            zsetKey,
            'abc',
            '-1',
          ]),
        errorWithMessage('ERR value is not an integer or out of range'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, true, [
            'ZRANGEBYSCORE',
            zsetKey,
            'abc',
            '1',
          ]),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, [
            'ZREMRANGEBYSCORE',
            zsetKey,
            '0',
            'abc',
          ]),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, true, [
            'ZCOUNT',
            zsetKey,
            'abc',
            '1',
          ]),
        errorWithMessage('ERR min or max is not a float'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, false, ['ZPOPMIN', zsetKey, 'abc']),
        errorWithMessage('ERR value is out of range, must be positive'),
      )
      await assert.rejects(
        () => redisClient.zPopMaxCount(zsetKey, -1),
        errorWithMessage('ERR value is out of range, must be positive'),
      )

      assert.deepStrictEqual(await redisClient.zPopMinCount(zsetKey, 0), [])
    } finally {
      await redisClient.del([zsetKey, stringKey])
    }
  })

  test('ZREM command', async () => {
    await redisClient.zAdd('zset7', [
      { score: 1, value: 'one' },
      { score: 2, value: 'two' },
      { score: 3, value: 'three' },
      { score: 4, value: 'four' },
    ])

    assert.strictEqual(await redisClient.zRem('zset7', 'two'), 1)

    assert.strictEqual(await redisClient.zRem('zset7', ['one', 'three']), 2)

    assert.strictEqual(await redisClient.zRem('zset7', 'nonexistent'), 0)

    assert.deepStrictEqual(await redisClient.zRange('zset7', 0, -1), ['four'])
  })
})
