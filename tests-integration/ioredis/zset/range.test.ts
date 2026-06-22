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

  test('ZRANGE command', async () => {
    await redisClient?.zadd('zset3', 1, 'one', 2, 'two', 3, 'three', 4, 'four')

    // Get range without scores
    const range1 = await redisClient?.zrange('zset3', 0, 2)
    assert.deepStrictEqual(range1, ['one', 'two', 'three'])

    // Get range with scores
    const range2 = await redisClient?.zrange('zset3', 0, -1, 'WITHSCORES')
    assert.deepStrictEqual(range2, [
      'one',
      '1',
      'two',
      '2',
      'three',
      '3',
      'four',
      '4',
    ])

    // Negative indices
    const range3 = await redisClient?.zrange('zset3', -2, -1)
    assert.deepStrictEqual(range3, ['three', 'four'])
  })

  test('ZREVRANGE command', async () => {
    await redisClient?.zadd('zset4', 1, 'one', 2, 'two', 3, 'three')

    // Reverse range
    const revrange = await redisClient?.zrevrange('zset4', 0, -1)
    assert.deepStrictEqual(revrange, ['three', 'two', 'one'])

    // Reverse range with scores
    const revrangeWithScores = await redisClient?.zrevrange(
      'zset4',
      0,
      1,
      'WITHSCORES',
    )
    assert.deepStrictEqual(revrangeWithScores, ['three', '3', 'two', '2'])
  })

  test('ZRANK and ZREVRANK commands', async () => {
    await redisClient?.zadd('zset5', 1, 'one', 2, 'two', 3, 'three')

    // ZRANK (0-based, lowest score first)
    const rank1 = await redisClient?.zrank('zset5', 'one')
    assert.strictEqual(rank1, 0)

    const rank2 = await redisClient?.zrank('zset5', 'three')
    assert.strictEqual(rank2, 2)

    const rankNone = await redisClient?.zrank('zset5', 'nonexistent')
    assert.strictEqual(rankNone, null)

    // ZREVRANK (0-based, highest score first)
    const revrank1 = await redisClient?.zrevrank('zset5', 'three')
    assert.strictEqual(revrank1, 0)

    const revrank2 = await redisClient?.zrevrank('zset5', 'one')
    assert.strictEqual(revrank2, 2)
  })

  test('ZRANK and ZREVRANK WITHSCORE option', async () => {
    const keyTag = `zrank-withscore:${randomKey()}`
    const zsetKey = `{${keyTag}}:zset`
    const stringKey = `{${keyTag}}:string`

    try {
      await redisClient?.zadd(zsetKey, 1, 'one', 2, 'two', 3.5, 'three')

      assert.deepStrictEqual(
        await redisClient?.zrank(zsetKey, 'one', 'WITHSCORE'),
        [0, '1'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrank(zsetKey, 'three', 'withscore'),
        [2, '3.5'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrevrank(zsetKey, 'three', 'WITHSCORE'),
        [0, '3.5'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrevrank(zsetKey, 'one', 'WITHSCORE'),
        [2, '1'],
      )

      assert.strictEqual(
        await redisClient?.zrank(zsetKey, 'missing', 'WITHSCORE'),
        null,
      )
      assert.strictEqual(
        await redisClient?.zrevrank(zsetKey, 'missing', 'WITHSCORE'),
        null,
      )

      await redisClient?.set(stringKey, 'not-a-zset')
      await assert.rejects(
        () => redisClient?.zrank(stringKey, 'one', 'WITHSCORE'),
        errorWithMessage(
          'WRONGTYPE Operation against a key holding the wrong kind of value',
        ),
      )
      await assert.rejects(
        () => redisClient?.call('ZRANK', zsetKey, 'one', 'BADOPTION'),
        errorWithMessage('ERR syntax error'),
      )
      await assert.rejects(
        () =>
          redisClient?.call(
            'ZREVRANK',
            zsetKey,
            'one',
            'WITHSCORE',
            'WITHSCORE',
          ),
        errorWithMessage(
          "ERR wrong number of arguments for 'zrevrank' command",
        ),
      )
    } finally {
      await redisClient?.del(zsetKey, stringKey)
    }
  })

  test('score range commands support infinite and exclusive bounds', async () => {
    const zsetKey = `{zset-score-bounds:${randomKey()}}`

    try {
      await redisClient?.zadd(
        zsetKey,
        0,
        'zero',
        1,
        'one',
        2,
        'two',
        3,
        'three',
      )

      assert.deepStrictEqual(
        await redisClient?.zrangebyscore(zsetKey, '-inf', '+inf'),
        ['zero', 'one', 'two', 'three'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrangebyscore(zsetKey, '(1', '3'),
        ['two', 'three'],
      )
      assert.deepStrictEqual(
        await redisClient?.zrangebyscore(zsetKey, '1', '(3'),
        ['one', 'two'],
      )
      assert.strictEqual(await redisClient?.zcount(zsetKey, '(1', '+inf'), 2)
      assert.strictEqual(
        await redisClient?.zremrangebyscore(zsetKey, '-inf', '(2'),
        2,
      )
      assert.deepStrictEqual(await redisClient?.zrange(zsetKey, 0, -1), [
        'two',
        'three',
      ])
    } finally {
      await redisClient?.del(zsetKey)
    }
  })
})
