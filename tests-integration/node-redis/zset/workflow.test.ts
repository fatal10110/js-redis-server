import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { RedisClusterType } from 'redis'
import { TestRunner } from '../../test-config'
import { flushNodeRedisCluster } from '../../utils'

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
