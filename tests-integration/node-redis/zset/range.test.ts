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

  test('ZRANGE command', async () => {
    await redisClient.zAdd('zset3', [
      { score: 1, value: 'one' },
      { score: 2, value: 'two' },
      { score: 3, value: 'three' },
      { score: 4, value: 'four' },
    ])

    assert.deepStrictEqual(await redisClient.zRange('zset3', 0, 2), [
      'one',
      'two',
      'three',
    ])

    assert.deepStrictEqual(await redisClient.zRangeWithScores('zset3', 0, -1), [
      { value: 'one', score: 1 },
      { value: 'two', score: 2 },
      { value: 'three', score: 3 },
      { value: 'four', score: 4 },
    ])

    assert.deepStrictEqual(await redisClient.zRange('zset3', -2, -1), [
      'three',
      'four',
    ])
  })

  test('ZREVRANGE command', async () => {
    await redisClient.zAdd('zset4', [
      { score: 1, value: 'one' },
      { score: 2, value: 'two' },
      { score: 3, value: 'three' },
    ])

    assert.deepStrictEqual(
      await redisClient.zRange('zset4', 0, -1, { REV: true }),
      ['three', 'two', 'one'],
    )

    assert.deepStrictEqual(
      await redisClient.zRangeWithScores('zset4', 0, 1, { REV: true }),
      [
        { value: 'three', score: 3 },
        { value: 'two', score: 2 },
      ],
    )
  })

  test('ZRANK and ZREVRANK commands', async () => {
    await redisClient.zAdd('zset5', [
      { score: 1, value: 'one' },
      { score: 2, value: 'two' },
      { score: 3, value: 'three' },
    ])

    assert.strictEqual(await redisClient.zRank('zset5', 'one'), 0)
    assert.strictEqual(await redisClient.zRank('zset5', 'three'), 2)
    assert.strictEqual(await redisClient.zRank('zset5', 'nonexistent'), null)

    assert.strictEqual(await redisClient.zRevRank('zset5', 'three'), 0)
    assert.strictEqual(await redisClient.zRevRank('zset5', 'one'), 2)
  })

  test('ZRANK and ZREVRANK WITHSCORE option', async () => {
    const keyTag = `zrank-withscore:${randomKey()}`
    const zsetKey = `{${keyTag}}:zset`
    const stringKey = `{${keyTag}}:string`

    try {
      await redisClient.zAdd(zsetKey, [
        { score: 1, value: 'one' },
        { score: 2, value: 'two' },
        { score: 3.5, value: 'three' },
      ])

      assert.deepStrictEqual(await redisClient.zRankWithScore(zsetKey, 'one'), {
        rank: 0,
        score: 1,
      })
      assert.deepStrictEqual(
        await redisClient.zRankWithScore(zsetKey, 'three'),
        { rank: 2, score: 3.5 },
      )
      // node-redis has no typed zRevRankWithScore — use the raw command, which
      // returns a flat [rank, score] reply (score a RESP3 double => number).
      assert.deepStrictEqual(
        await redisClient.sendCommand(zsetKey, true, [
          'ZREVRANK',
          zsetKey,
          'three',
          'WITHSCORE',
        ]),
        [0, 3.5],
      )
      assert.deepStrictEqual(
        await redisClient.sendCommand(zsetKey, true, [
          'ZREVRANK',
          zsetKey,
          'one',
          'WITHSCORE',
        ]),
        [2, 1],
      )

      assert.strictEqual(
        await redisClient.zRankWithScore(zsetKey, 'missing'),
        null,
      )
      assert.strictEqual(
        await redisClient.sendCommand(zsetKey, true, [
          'ZREVRANK',
          zsetKey,
          'missing',
          'WITHSCORE',
        ]),
        null,
      )

      await redisClient.set(stringKey, 'not-a-zset')
      await assert.rejects(
        () => redisClient.zRankWithScore(stringKey, 'one'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, true, [
            'ZRANK',
            zsetKey,
            'one',
            'BADOPTION',
          ]),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          redisClient.sendCommand(zsetKey, true, [
            'ZREVRANK',
            zsetKey,
            'one',
            'WITHSCORE',
            'WITHSCORE',
          ]),
        errorWithMessage(
          "ERR wrong number of arguments for 'zrevrank' command",
        ),
      )
    } finally {
      await redisClient.del([zsetKey, stringKey])
    }
  })

  test('score range commands support infinite and exclusive bounds', async () => {
    const zsetKey = `{zset-score-bounds:${randomKey()}}`

    try {
      await redisClient.zAdd(zsetKey, [
        { score: 0, value: 'zero' },
        { score: 1, value: 'one' },
        { score: 2, value: 'two' },
        { score: 3, value: 'three' },
      ])

      assert.deepStrictEqual(
        await redisClient.zRangeByScore(zsetKey, '-inf', '+inf'),
        ['zero', 'one', 'two', 'three'],
      )
      assert.deepStrictEqual(
        await redisClient.zRangeByScore(zsetKey, '(1', '3'),
        ['two', 'three'],
      )
      assert.deepStrictEqual(
        await redisClient.zRangeByScore(zsetKey, '1', '(3'),
        ['one', 'two'],
      )
      assert.strictEqual(await redisClient.zCount(zsetKey, '(1', '+inf'), 2)
      assert.strictEqual(
        await redisClient.zRemRangeByScore(zsetKey, '-inf', '(2'),
        2,
      )
      assert.deepStrictEqual(await redisClient.zRange(zsetKey, 0, -1), [
        'two',
        'three',
      ])
    } finally {
      await redisClient.del(zsetKey)
    }
  })
})
