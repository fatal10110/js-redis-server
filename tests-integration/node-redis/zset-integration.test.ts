import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../test-config'
import { errorWithMessage, flushNodeRedisCluster, randomKey } from '../utils'

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
        () =>
          redisClient.sendCommand(zsetKey, false, ['ZPOPMAX', zsetKey, '-1']),
        errorWithMessage('ERR value is out of range, must be positive'),
      )

      assert.deepStrictEqual(await redisClient.zPopMinCount(zsetKey, 0), [])
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

  test('Sorted Set commands workflow - Leaderboard', async () => {
    const leaderboard = 'game:leaderboard'

    await redisClient.zAdd(leaderboard, [
      { score: 1000, value: 'player1' },
      { score: 1500, value: 'player2' },
      { score: 800, value: 'player3' },
    ])
    await redisClient.zAdd(leaderboard, [
      { score: 1200, value: 'player4' },
      { score: 2000, value: 'player5' },
    ])

    const top3 = await redisClient.zRangeWithScores(leaderboard, 0, 2, {
      REV: true,
    })
    assert.strictEqual(top3[0].value, 'player5')
    assert.strictEqual(top3[0].score, 2000)
    assert.strictEqual(top3[1].value, 'player2')
    assert.strictEqual(top3[1].score, 1500)

    assert.strictEqual(await redisClient.zRevRank(leaderboard, 'player2'), 1)

    await redisClient.zIncrBy(leaderboard, 600, 'player1')
    assert.strictEqual(await redisClient.zScore(leaderboard, 'player1'), 1600)

    assert.strictEqual(await redisClient.zRevRank(leaderboard, 'player1'), 1)

    await redisClient.zRem(leaderboard, 'player3')
    assert.strictEqual(await redisClient.zCard(leaderboard), 4)

    const finalBoard = await redisClient.zRangeWithScores(leaderboard, 0, -1, {
      REV: true,
    })
    assert.strictEqual(finalBoard[0].value, 'player5')
    assert.strictEqual(finalBoard[1].value, 'player1')
  })

  test('Sorted Set commands workflow - Priority Queue', async () => {
    const priorityQueue = 'tasks:priority'

    await redisClient.zAdd(priorityQueue, [
      { score: 1, value: 'critical_bug' },
      { score: 4, value: 'feature_request' },
      { score: 2, value: 'urgent_fix' },
    ])
    await redisClient.zAdd(priorityQueue, [
      { score: 6, value: 'documentation' },
      { score: 3, value: 'security_patch' },
    ])

    assert.strictEqual(
      (await redisClient.zRange(priorityQueue, 0, 0))[0],
      'critical_bug',
    )

    await redisClient.zRem(priorityQueue, 'critical_bug')

    assert.strictEqual(
      (await redisClient.zRange(priorityQueue, 0, 0))[0],
      'urgent_fix',
    )

    await redisClient.zIncrBy(priorityQueue, -3, 'feature_request')
    assert.strictEqual(
      await redisClient.zScore(priorityQueue, 'feature_request'),
      1,
    )

    assert.strictEqual(
      await redisClient.zRank(priorityQueue, 'feature_request'),
      0,
    )

    const allTasks = await redisClient.zRangeWithScores(priorityQueue, 0, -1)
    assert.strictEqual(allTasks[0].value, 'feature_request')
    assert.strictEqual(allTasks[0].score, 1)
    assert.strictEqual(allTasks[1].value, 'urgent_fix')
    assert.strictEqual(allTasks[1].score, 2)
    assert.strictEqual(allTasks[2].value, 'security_patch')
    assert.strictEqual(allTasks[2].score, 3)
  })

  test('Sorted Set commands workflow - Time Series Events', async () => {
    const events = 'user:events'

    const now = Date.now()
    await redisClient.zAdd(events, [
      { score: now - 3600000, value: 'login' },
      { score: now - 1800000, value: 'page_view' },
      { score: now - 900000, value: 'purchase' },
      { score: now - 300000, value: 'logout' },
    ])

    const recentEvents = await redisClient.zRangeByScore(
      events,
      now - 1800000,
      now,
    )
    assert.ok(recentEvents.includes('page_view'))
    assert.ok(recentEvents.includes('purchase'))
    assert.ok(recentEvents.includes('logout'))
    assert.ok(!recentEvents.includes('login'))

    const chronological = await redisClient.zRange(events, 0, -1)
    assert.strictEqual(chronological[0], 'login')
    assert.strictEqual(chronological[3], 'logout')

    const reverseChronological = await redisClient.zRange(events, 0, -1, {
      REV: true,
    })
    assert.strictEqual(reverseChronological[0], 'logout')
    assert.strictEqual(reverseChronological[3], 'login')

    await redisClient.zAdd(events, { score: now, value: 'new_login' })

    const latest = await redisClient.zRange(events, 0, 0, { REV: true })
    assert.strictEqual(latest[0], 'new_login')

    await redisClient.zRemRangeByScore(events, 0, now - 7200000)

    assert.ok((await redisClient.zCard(events)) >= 4)
  })

  test('Sorted Set commands workflow - Search Results Ranking', async () => {
    const searchResults = 'search:javascript'

    await redisClient.zAdd(searchResults, [
      { score: 95, value: 'js_tutorial_comprehensive' },
      { score: 87, value: 'react_getting_started' },
      { score: 92, value: 'node_js_guide' },
      { score: 78, value: 'js_basics_beginner' },
      { score: 89, value: 'advanced_js_patterns' },
    ])

    const topResults = await redisClient.zRangeWithScores(searchResults, 0, 2, {
      REV: true,
    })
    assert.strictEqual(topResults[0].value, 'js_tutorial_comprehensive')
    assert.strictEqual(topResults[0].score, 95)

    await redisClient.zIncrBy(searchResults, 10, 'react_getting_started')
    assert.strictEqual(
      await redisClient.zScore(searchResults, 'react_getting_started'),
      97,
    )

    assert.strictEqual(
      await redisClient.zRevRank(searchResults, 'react_getting_started'),
      0,
    )

    const highQualityResults = await redisClient.zRangeByScore(
      searchResults,
      90,
      100,
    )
    assert.ok(highQualityResults.includes('js_tutorial_comprehensive'))
    assert.ok(highQualityResults.includes('react_getting_started'))
    assert.ok(highQualityResults.includes('node_js_guide'))

    await redisClient.zRemRangeByScore(searchResults, 0, 80)

    assert.ok((await redisClient.zCard(searchResults)) >= 4)

    const finalRanking = await redisClient.zRangeWithScores(
      searchResults,
      0,
      -1,
      { REV: true },
    )
    assert.ok(finalRanking.length > 0)
    for (const { score } of finalRanking) {
      assert.ok(score > 80, `Score ${score} should be > 80`)
    }
  })
})
