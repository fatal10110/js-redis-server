import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'

describe('Sorted Set Commands Integration', () => {
  const redisCluster = new ClusterNetwork(console)
  let redisClient: Cluster | undefined

  before(async () => {
    await redisCluster.init({ masters: 3, slaves: 0 })
    redisClient = new Redis.Cluster(
      [
        {
          host: '127.0.0.1',
          port: Array.from(redisCluster.getAll())[0].port,
        },
      ],
      {
        slotsRefreshTimeout: 10000000,
        lazyConnect: true,
      },
    )
    await redisClient?.connect()
  })

  after(async () => {
    await redisClient?.quit()
    await redisCluster.shutdown()
  })

  test('ZADD and ZCARD commands', async () => {
    // ZADD single member
    const add1 = await redisClient?.zadd('zset1', 1.0, 'member1')
    assert.strictEqual(add1, 1)

    // ZADD multiple members
    const add2 = await redisClient?.zadd(
      'zset1',
      2.0,
      'member2',
      3.0,
      'member3',
    )
    assert.strictEqual(add2, 2)

    // Update existing member score
    const add3 = await redisClient?.zadd('zset1', 1.5, 'member1')
    assert.strictEqual(add3, 0) // No new members added

    // Check cardinality
    const card = await redisClient?.zcard('zset1')
    assert.strictEqual(card, 3)
  })

  test('ZSCORE command', async () => {
    await redisClient?.zadd('zset2', 1.5, 'member1', 2.5, 'member2')

    const score1 = await redisClient?.zscore('zset2', 'member1')
    assert.strictEqual(score1, '1.5')

    const score2 = await redisClient?.zscore('zset2', 'nonexistent')
    assert.strictEqual(score2, null)
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

  test('ZINCRBY command', async () => {
    await redisClient?.zadd('zset6', 1.0, 'member1')

    // Increment existing member
    const incr1 = await redisClient?.zincrby('zset6', 2.5, 'member1')
    assert.strictEqual(incr1, '3.5')

    // Increment non-existent member
    const incr2 = await redisClient?.zincrby('zset6', 5.0, 'member2')
    assert.strictEqual(incr2, '5')

    // Verify scores
    const score1 = await redisClient?.zscore('zset6', 'member1')
    const score2 = await redisClient?.zscore('zset6', 'member2')
    assert.strictEqual(score1, '3.5')
    assert.strictEqual(score2, '5')
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
      3,
      'feature_request',
      2,
      'urgent_fix',
    )

    // Add more tasks
    await redisClient?.zadd(
      priorityQueue,
      5,
      'documentation',
      1.5,
      'security_patch',
    )

    // Get highest priority task (lowest score)
    const nextTask = await redisClient?.zrange(priorityQueue, 0, 0)
    assert.strictEqual(nextTask?.[0], 'critical_bug')

    // Process task (remove it)
    await redisClient?.zrem(priorityQueue, 'critical_bug')

    // Get next highest priority
    const nextAfterProcessing = await redisClient?.zrange(priorityQueue, 0, 0)
    assert.strictEqual(nextAfterProcessing?.[0], 'security_patch')

    // Escalate a task (decrease its score for higher priority)
    await redisClient?.zincrby(priorityQueue, -1, 'feature_request')
    const newPriority = await redisClient?.zscore(
      priorityQueue,
      'feature_request',
    )
    assert.strictEqual(newPriority, '2')

    // Check task ranking after escalation
    const taskRank = await redisClient?.zrank(priorityQueue, 'feature_request')
    assert.strictEqual(taskRank, 1) // Should be 2nd priority now

    // Get all tasks in priority order
    const allTasks = await redisClient?.zrange(
      priorityQueue,
      0,
      -1,
      'WITHSCORES',
    )
    assert.strictEqual(allTasks?.[0], 'security_patch') // Priority 1.5
    assert.strictEqual(allTasks?.[2], 'feature_request') // Priority 2
    assert.strictEqual(allTasks?.[4], 'urgent_fix') // Priority 2
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

    // Add search results with relevance scores
    await redisClient?.zadd(
      searchResults,
      0.95,
      'js_tutorial_comprehensive',
      0.87,
      'react_getting_started',
      0.92,
      'node_js_guide',
      0.78,
      'js_basics_beginner',
      0.89,
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
    assert.strictEqual(topResults?.[1], '0.95')

    // Boost a result based on user interaction
    await redisClient?.zincrby(searchResults, 0.1, 'react_getting_started')
    const boostedScore = await redisClient?.zscore(
      searchResults,
      'react_getting_started',
    )
    assert.strictEqual(boostedScore, '0.97')

    // Check new ranking
    const newRank = await redisClient?.zrevrank(
      searchResults,
      'react_getting_started',
    )
    assert.strictEqual(newRank, 0) // Should be 1st now (highest score)

    // Get results above certain relevance threshold
    const highQualityResults = await redisClient?.zrangebyscore(
      searchResults,
      0.9,
      1.0,
    )
    assert.ok(highQualityResults?.includes('js_tutorial_comprehensive'))
    assert.ok(highQualityResults?.includes('react_getting_started'))
    assert.ok(highQualityResults?.includes('node_js_guide'))

    // Remove low-quality results
    await redisClient?.zremrangebyscore(searchResults, 0, 0.8)
    const qualityCount = await redisClient?.zcard(searchResults)
    assert.ok(qualityCount! >= 4) // Should have removed js_basics_beginner

    // Get final ranking
    const finalRanking = await redisClient?.zrevrange(
      searchResults,
      0,
      -1,
      'WITHSCORES',
    )
    // Verify all remaining results have score > 0.8
    assert.ok(finalRanking)
    assert.ok(finalRanking.length > 0)

    for (let i = 1; i < finalRanking.length; i += 2) {
      const score = parseFloat(finalRanking[i])
      assert.ok(score > 0.8, `Score ${score} should be > 0.8`)
    }
  })
})
