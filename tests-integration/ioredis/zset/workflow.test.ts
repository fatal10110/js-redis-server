import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { Cluster } from 'ioredis'
import { TestRunner } from '../../test-config'

const testRunner = new TestRunner()

describe(`Sorted Set Commands Integration (${testRunner.getBackendName()})`, () => {
  let redisClient: Cluster | undefined

  before(async () => {
    redisClient = await testRunner.setupIoredisCluster('zset-integration')
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('Sorted Set commands workflow - Leaderboard', async () => {
    const leaderboard = 'game:leaderboard'

    // Add player scores
    await redisClient?.zadd(
      leaderboard,
      1000,
      'player1',
      1500,
      'player2',
      800,
      'player3',
    )

    // Add more players
    await redisClient?.zadd(leaderboard, 1200, 'player4', 2000, 'player5')

    // Get top 3 players
    const top3 = await redisClient?.zrevrange(leaderboard, 0, 2, 'WITHSCORES')
    assert.strictEqual(top3?.[0], 'player5') // Highest score
    assert.strictEqual(top3?.[1], '2000')
    assert.strictEqual(top3?.[2], 'player2') // Second highest
    assert.strictEqual(top3?.[3], '1500')

    // Get player rank (1-based for display)
    const player2Rank = await redisClient?.zrevrank(leaderboard, 'player2')
    assert.strictEqual(player2Rank, 1) // 0-based, so 2nd place

    // Player1 gets bonus points
    await redisClient?.zincrby(leaderboard, 600, 'player1')
    const newScore = await redisClient?.zscore(leaderboard, 'player1')
    assert.strictEqual(newScore, '1600')

    // Check player1's new rank
    const player1NewRank = await redisClient?.zrevrank(leaderboard, 'player1')
    assert.strictEqual(player1NewRank, 1) // Should be 2nd now

    // Remove inactive player
    await redisClient?.zrem(leaderboard, 'player3')
    const finalCount = await redisClient?.zcard(leaderboard)
    assert.strictEqual(finalCount, 4)

    // Get final leaderboard
    const finalBoard = await redisClient?.zrevrange(
      leaderboard,
      0,
      -1,
      'WITHSCORES',
    )
    assert.strictEqual(finalBoard?.[0], 'player5') // Still first
    assert.strictEqual(finalBoard?.[2], 'player1') // Now second
  })

  test('Sorted Set commands workflow - Priority Queue', async () => {
    const priorityQueue = 'tasks:priority'

    // Add tasks with priorities (lower score = higher priority)
    await redisClient?.zadd(
      priorityQueue,
      1,
      'critical_bug',
      4,
      'feature_request',
      2,
      'urgent_fix',
    )

    // Add more tasks
    await redisClient?.zadd(
      priorityQueue,
      6,
      'documentation',
      3,
      'security_patch',
    )

    // Get highest priority task (lowest score)
    const nextTask = await redisClient?.zrange(priorityQueue, 0, 0)
    assert.strictEqual(nextTask?.[0], 'critical_bug')

    // Process task (remove it)
    await redisClient?.zrem(priorityQueue, 'critical_bug')

    // Get next highest priority
    const nextAfterProcessing = await redisClient?.zrange(priorityQueue, 0, 0)
    assert.strictEqual(nextAfterProcessing?.[0], 'urgent_fix')

    // Escalate a task (decrease its score for higher priority)
    await redisClient?.zincrby(priorityQueue, -3, 'feature_request')
    const newPriority = await redisClient?.zscore(
      priorityQueue,
      'feature_request',
    )
    assert.strictEqual(newPriority, '1')

    // Check task ranking after escalation
    const taskRank = await redisClient?.zrank(priorityQueue, 'feature_request')
    assert.strictEqual(taskRank, 0) // Should be highest priority now

    // Get all tasks in priority order
    const allTasks = await redisClient?.zrange(
      priorityQueue,
      0,
      -1,
      'WITHSCORES',
    )
    assert.strictEqual(allTasks?.[0], 'feature_request') // Priority 1
    assert.strictEqual(allTasks?.[1], '1')
    assert.strictEqual(allTasks?.[2], 'urgent_fix') // Priority 2
    assert.strictEqual(allTasks?.[3], '2')
    assert.strictEqual(allTasks?.[4], 'security_patch') // Priority 3
    assert.strictEqual(allTasks?.[5], '3')
  })

  test('Sorted Set commands workflow - Time Series Events', async () => {
    const events = 'user:events'

    // Add events with timestamps
    const now = Date.now()
    await redisClient?.zadd(
      events,
      now - 3600000,
      'login', // 1 hour ago
      now - 1800000,
      'page_view', // 30 min ago
      now - 900000,
      'purchase', // 15 min ago
      now - 300000,
      'logout', // 5 min ago
    )

    // Get recent events (last 30 minutes)
    const recentEvents = await redisClient?.zrangebyscore(
      events,
      now - 1800000,
      now,
    )
    assert.ok(recentEvents?.includes('page_view'))
    assert.ok(recentEvents?.includes('purchase'))
    assert.ok(recentEvents?.includes('logout'))
    assert.ok(!recentEvents?.includes('login')) // Too old

    // Get chronological order
    const chronological = await redisClient?.zrange(events, 0, -1)
    assert.strictEqual(chronological?.[0], 'login')
    assert.strictEqual(chronological?.[3], 'logout')

    // Get reverse chronological (most recent first)
    const reverseChronological = await redisClient?.zrevrange(events, 0, -1)
    assert.strictEqual(reverseChronological?.[0], 'logout')
    assert.strictEqual(reverseChronological?.[3], 'login')

    // Add new event
    await redisClient?.zadd(events, now, 'new_login')

    // Get latest event
    const latest = await redisClient?.zrevrange(events, 0, 0)
    assert.strictEqual(latest?.[0], 'new_login')

    // Clean up old events (older than 2 hours)
    await redisClient?.zremrangebyscore(events, 0, now - 7200000)

    // Verify cleanup
    const remainingCount = await redisClient?.zcard(events)
    assert.ok(remainingCount! >= 4) // Should have at least our 4 recent events
  })

  test('Sorted Set commands workflow - Search Results Ranking', async () => {
    const searchResults = 'search:javascript'

    // Add search results with relevance scores (using integers 0-100 scale)
    await redisClient?.zadd(
      searchResults,
      95,
      'js_tutorial_comprehensive',
      87,
      'react_getting_started',
      92,
      'node_js_guide',
      78,
      'js_basics_beginner',
      89,
      'advanced_js_patterns',
    )

    // Get top 3 most relevant results
    const topResults = await redisClient?.zrevrange(
      searchResults,
      0,
      2,
      'WITHSCORES',
    )
    assert.strictEqual(topResults?.[0], 'js_tutorial_comprehensive')
    assert.strictEqual(topResults?.[1], '95')

    // Boost a result based on user interaction
    await redisClient?.zincrby(searchResults, 10, 'react_getting_started')
    const boostedScore = await redisClient?.zscore(
      searchResults,
      'react_getting_started',
    )
    assert.strictEqual(boostedScore, '97')

    // Check new ranking
    const newRank = await redisClient?.zrevrank(
      searchResults,
      'react_getting_started',
    )
    assert.strictEqual(newRank, 0) // Should be 1st now (highest score)

    // Get results above certain relevance threshold
    const highQualityResults = await redisClient?.zrangebyscore(
      searchResults,
      90,
      100,
    )
    assert.ok(highQualityResults?.includes('js_tutorial_comprehensive'))
    assert.ok(highQualityResults?.includes('react_getting_started'))
    assert.ok(highQualityResults?.includes('node_js_guide'))

    // Remove low-quality results (score <= 80)
    await redisClient?.zremrangebyscore(searchResults, 0, 80)

    const qualityCount = await redisClient?.zcard(searchResults)

    assert.ok(qualityCount! >= 4) // Should have removed js_basics_beginner

    // Get final ranking
    const finalRanking = await redisClient?.zrevrange(
      searchResults,
      0,
      -1,
      'WITHSCORES',
    )
    // Verify all remaining results have score > 80
    assert.ok(finalRanking)
    assert.ok(finalRanking.length > 0)

    for (let i = 1; i < finalRanking.length; i += 2) {
      const score = parseInt(finalRanking[i])
      assert.ok(score > 80, `Score ${score} should be > 80`)
    }
  })
})
