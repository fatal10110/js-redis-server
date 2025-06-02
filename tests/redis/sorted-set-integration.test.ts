import { test, describe, before, after } from 'node:test'
import assert from 'node:assert'
import { ClusterNetwork } from '../../src/core/cluster/network'
import { Redis, Cluster } from 'ioredis'

describe('Sorted Set Integration Tests', () => {
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

  test('Complete sorted set workflow', async () => {
    // Start with empty set
    let result = await redisClient?.zcard('leaderboard')
    assert.strictEqual(result, 0)

    // Add players with scores
    result = await redisClient?.zadd(
      'leaderboard',
      100,
      'alice',
      85,
      'bob',
      120,
      'charlie',
    )
    assert.strictEqual(result, 3)

    // Check cardinality
    result = await redisClient?.zcard('leaderboard')
    assert.strictEqual(result, 3)

    // Check type
    const type = await redisClient?.type('leaderboard')
    assert.strictEqual(type, 'zset')

    // Get leaderboard (sorted by score)
    const members = await redisClient?.zrange('leaderboard', 0, -1)
    assert.strictEqual(members?.length, 3)
    assert.strictEqual(members?.[0], 'bob') // 85
    assert.strictEqual(members?.[1], 'alice') // 100
    assert.strictEqual(members?.[2], 'charlie') // 120

    // Get top 2 with scores
    const topWithScores = await redisClient?.zrange(
      'leaderboard',
      -2,
      -1,
      'WITHSCORES',
    )
    assert.strictEqual(topWithScores?.length, 4) // 2 members * 2 (member + score)
    assert.strictEqual(topWithScores?.[0], 'alice')
    assert.strictEqual(topWithScores?.[1], '100')
    assert.strictEqual(topWithScores?.[2], 'charlie')
    assert.strictEqual(topWithScores?.[3], '120')

    // Check individual scores
    const aliceScore = await redisClient?.zscore('leaderboard', 'alice')
    assert.strictEqual(aliceScore, '100')

    // Increment alice's score
    const newScore = await redisClient?.zincrby('leaderboard', 25, 'alice')
    assert.strictEqual(newScore, '125')

    // Verify alice is now at the top
    const topPlayer = await redisClient?.zrange('leaderboard', -1, -1)
    assert.strictEqual(topPlayer?.[0], 'alice')

    // Remove bob
    const removed = await redisClient?.zrem('leaderboard', 'bob')
    assert.strictEqual(removed, 1)

    // Check final cardinality
    const finalCount = await redisClient?.zcard('leaderboard')
    assert.strictEqual(finalCount, 2)

    // Final leaderboard
    const finalBoard = await redisClient?.zrange(
      'leaderboard',
      0,
      -1,
      'WITHSCORES',
    )
    assert.strictEqual(finalBoard?.length, 4) // 2 members * 2
    assert.strictEqual(finalBoard?.[0], 'charlie') // 120
    assert.strictEqual(finalBoard?.[1], '120')
    assert.strictEqual(finalBoard?.[2], 'alice') // 125
    assert.strictEqual(finalBoard?.[3], '125')
  })

  test('Sorted set with same scores maintains lexicographic order', async () => {
    // Add members with same score
    await redisClient?.zadd(
      'samescores',
      1.0,
      'zebra',
      1.0,
      'apple',
      1.0,
      'banana',
    )

    // Should be sorted lexicographically when scores are equal
    const members = await redisClient?.zrange('samescores', 0, -1)
    assert.strictEqual(members?.[0], 'apple')
    assert.strictEqual(members?.[1], 'banana')
    assert.strictEqual(members?.[2], 'zebra')
  })

  test('Sorted set handles negative scores correctly', async () => {
    // Add members with negative scores
    await redisClient?.zadd(
      'negatives',
      -10.5,
      'negative',
      0,
      'zero',
      5.5,
      'positive',
    )

    const members = await redisClient?.zrange('negatives', 0, -1)
    assert.strictEqual(members?.[0], 'negative') // -10.5
    assert.strictEqual(members?.[1], 'zero') // 0
    assert.strictEqual(members?.[2], 'positive') // 5.5
  })
})
