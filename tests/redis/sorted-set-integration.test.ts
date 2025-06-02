import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'

// Import all sorted set commands
import { ZaddCommand } from '../../src/commanders/custom/commands/redis/data/zadd'
import { ZremCommand } from '../../src/commanders/custom/commands/redis/data/zrem'
import { ZrangeCommand } from '../../src/commanders/custom/commands/redis/data/zrange'
import { ZscoreCommand } from '../../src/commanders/custom/commands/redis/data/zscore'
import { ZcardCommand } from '../../src/commanders/custom/commands/redis/data/zcard'
import { ZincrbyCommand } from '../../src/commanders/custom/commands/redis/data/zincrby'
import { TypeCommand } from '../../src/commanders/custom/commands/redis/data/type'

describe('Sorted Set Integration Tests', () => {
  test('Complete sorted set workflow', async () => {
    const db = new DB()
    const zadd = new ZaddCommand(db)
    const zrem = new ZremCommand(db)
    const zrange = new ZrangeCommand(db)
    const zscore = new ZscoreCommand(db)
    const zcard = new ZcardCommand(db)
    const zincrby = new ZincrbyCommand(db)
    const type = new TypeCommand(db)

    // Start with empty set
    let result = await zcard.run(Buffer.from('ZCARD'), [
      Buffer.from('leaderboard'),
    ])
    assert.strictEqual(result.response, 0)

    // Add players with scores
    result = await zadd.run(Buffer.from('ZADD'), [
      Buffer.from('leaderboard'),
      Buffer.from('100'),
      Buffer.from('alice'),
      Buffer.from('85'),
      Buffer.from('bob'),
      Buffer.from('120'),
      Buffer.from('charlie'),
    ])
    assert.strictEqual(result.response, 3)

    // Check cardinality
    result = await zcard.run(Buffer.from('ZCARD'), [Buffer.from('leaderboard')])
    assert.strictEqual(result.response, 3)

    // Check type
    result = await type.run(Buffer.from('TYPE'), [Buffer.from('leaderboard')])
    assert.strictEqual(result.response, 'zset')

    // Get leaderboard (sorted by score)
    result = await zrange.run(Buffer.from('ZRANGE'), [
      Buffer.from('leaderboard'),
      Buffer.from('0'),
      Buffer.from('-1'),
    ])
    const members = result.response as Buffer[]
    assert.strictEqual(members.length, 3)
    assert.strictEqual(members[0].toString(), 'bob') // 85
    assert.strictEqual(members[1].toString(), 'alice') // 100
    assert.strictEqual(members[2].toString(), 'charlie') // 120

    // Get top 2 with scores
    result = await zrange.run(Buffer.from('ZRANGE'), [
      Buffer.from('leaderboard'),
      Buffer.from('-2'),
      Buffer.from('-1'),
      Buffer.from('WITHSCORES'),
    ])
    const topWithScores = result.response as Buffer[]
    assert.strictEqual(topWithScores.length, 4) // 2 members * 2 (member + score)
    assert.strictEqual(topWithScores[0].toString(), 'alice')
    assert.strictEqual(topWithScores[1].toString(), '100')
    assert.strictEqual(topWithScores[2].toString(), 'charlie')
    assert.strictEqual(topWithScores[3].toString(), '120')

    // Check individual scores
    result = await zscore.run(Buffer.from('ZSCORE'), [
      Buffer.from('leaderboard'),
      Buffer.from('alice'),
    ])
    assert.strictEqual((result.response as Buffer).toString(), '100')

    // Incrfement alice's score
    result = await zincrby.run(Buffer.from('ZINCRBY'), [
      Buffer.from('leaderboard'),
      Buffer.from('25'),
      Buffer.from('alice'),
    ])
    assert.strictEqual((result.response as Buffer).toString(), '125')

    // Verify alice is now at the top
    result = await zrange.run(Buffer.from('ZRANGE'), [
      Buffer.from('leaderboard'),
      Buffer.from('-1'),
      Buffer.from('-1'),
    ])
    const topPlayer = result.response as Buffer[]
    assert.strictEqual(topPlayer[0].toString(), 'alice')

    // Remove bob
    result = await zrem.run(Buffer.from('ZREM'), [
      Buffer.from('leaderboard'),
      Buffer.from('bob'),
    ])
    assert.strictEqual(result.response, 1)

    // Check final cardinality
    result = await zcard.run(Buffer.from('ZCARD'), [Buffer.from('leaderboard')])
    assert.strictEqual(result.response, 2)

    // Final leaderboard
    result = await zrange.run(Buffer.from('ZRANGE'), [
      Buffer.from('leaderboard'),
      Buffer.from('0'),
      Buffer.from('-1'),
      Buffer.from('WITHSCORES'),
    ])
    const finalBoard = result.response as Buffer[]
    assert.strictEqual(finalBoard.length, 4) // 2 members * 2
    assert.strictEqual(finalBoard[0].toString(), 'charlie') // 120
    assert.strictEqual(finalBoard[1].toString(), '120')
    assert.strictEqual(finalBoard[2].toString(), 'alice') // 125
    assert.strictEqual(finalBoard[3].toString(), '125')
  })

  test('Sorted set with same scores maintains lexicographic order', async () => {
    const db = new DB()
    const zadd = new ZaddCommand(db)
    const zrange = new ZrangeCommand(db)

    // Add members with same score
    await zadd.run(Buffer.from('ZADD'), [
      Buffer.from('samescores'),
      Buffer.from('1.0'),
      Buffer.from('zebra'),
      Buffer.from('1.0'),
      Buffer.from('apple'),
      Buffer.from('1.0'),
      Buffer.from('banana'),
    ])

    // Should be sorted lexicographically when scores are equal
    const result = await zrange.run(Buffer.from('ZRANGE'), [
      Buffer.from('samescores'),
      Buffer.from('0'),
      Buffer.from('-1'),
    ])
    const members = result.response as Buffer[]
    assert.strictEqual(members[0].toString(), 'apple')
    assert.strictEqual(members[1].toString(), 'banana')
    assert.strictEqual(members[2].toString(), 'zebra')
  })

  test('Sorted set handles negative scores correctly', async () => {
    const db = new DB()
    const zadd = new ZaddCommand(db)
    const zrange = new ZrangeCommand(db)

    // Add members with negative scores
    await zadd.run(Buffer.from('ZADD'), [
      Buffer.from('negatives'),
      Buffer.from('-10.5'),
      Buffer.from('negative'),
      Buffer.from('0'),
      Buffer.from('zero'),
      Buffer.from('5.5'),
      Buffer.from('positive'),
    ])

    const result = await zrange.run(Buffer.from('ZRANGE'), [
      Buffer.from('negatives'),
      Buffer.from('0'),
      Buffer.from('-1'),
    ])
    const members = result.response as Buffer[]
    assert.strictEqual(members[0].toString(), 'negative') // -10.5
    assert.strictEqual(members[1].toString(), 'zero') // 0
    assert.strictEqual(members[2].toString(), 'positive') // 5.5
  })
})
