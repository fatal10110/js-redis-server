import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import { createMockTransport } from '../mock-transport'

// Sorted set commands
import createZadd from '../../src/commanders/custom/commands/redis/data/zsets/zadd'
import createZrem from '../../src/commanders/custom/commands/redis/data/zsets/zrem'
import createZrange from '../../src/commanders/custom/commands/redis/data/zsets/zrange'
import createZscore from '../../src/commanders/custom/commands/redis/data/zsets/zscore'
import createZcard from '../../src/commanders/custom/commands/redis/data/zsets/zcard'
import createZincrby from '../../src/commanders/custom/commands/redis/data/zsets/zincrby'

// Type command
import createType from '../../src/commanders/custom/commands/redis/data/keys/type'
import { runCommand, createTestSession } from '../command-test-utils'

// Error imports
import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
  ExpectedFloat,
} from '../../src/core/errors'

describe('Sorted Set Commands', () => {
  describe('ZADD command', () => {
    test('ZADD adds new members to sorted set', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)

      // Add single member
      let result = await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('1.5'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)

      // Add multiple members
      result = await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('2.5'),
        Buffer.from('member2'),
        Buffer.from('0.5'),
        Buffer.from('member3'),
      ])
      assert.strictEqual(result.response, 2)

      // Add existing member (should update score, return 0)
      result = await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('3.0'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('ZADD with wrong number of arguments throws error', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)

      try {
        await runCommand(zaddCommand, 'ZADD', [Buffer.from('zset')])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }

      try {
        await runCommand(zaddCommand, 'ZADD', [
          Buffer.from('zset'),
          Buffer.from('1.0'),
        ])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })

    test('ZADD with non-numeric score throws error', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)

      try {
        await runCommand(zaddCommand, 'ZADD', [
          Buffer.from('zset'),
          Buffer.from('notanumber'),
          Buffer.from('member'),
        ])
        assert.fail('Should have thrown ExpectedFloat')
      } catch (error) {
        assert.ok(error instanceof ExpectedFloat)
      }
    })

    test('ZADD on wrong data type throws error', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)

      // Set a string value
      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))

      try {
        await runCommand(zaddCommand, 'ZADD', [
          Buffer.from('key'),
          Buffer.from('1.0'),
          Buffer.from('member'),
        ])
        assert.fail('Should have thrown WrongType')
      } catch (error) {
        assert.ok(error instanceof WrongType)
      }
    })
  })

  describe('ZREM command', () => {
    test('ZREM removes members from sorted set', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)
      const zremCommand = createZrem(db)

      // Set up sorted set
      await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('1.0'),
        Buffer.from('member1'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
        Buffer.from('3.0'),
        Buffer.from('member3'),
      ])

      // Remove single member
      let result = await runCommand(zremCommand, 'ZREM', [
        Buffer.from('zset'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)

      // Remove multiple members
      result = await runCommand(zremCommand, 'ZREM', [
        Buffer.from('zset'),
        Buffer.from('member2'),
        Buffer.from('member3'),
      ])
      assert.strictEqual(result.response, 2)

      // Key should be removed when empty
      assert.strictEqual(db.get(Buffer.from('zset')), null)
    })

    test('ZREM on non-existent key returns 0', async () => {
      const db = new DB()
      const zremCommand = createZrem(db)

      const result = await runCommand(zremCommand, 'ZREM', [
        Buffer.from('zset'),
        Buffer.from('member'),
      ])
      assert.strictEqual(result.response, 0)
    })
  })

  describe('ZRANGE command', () => {
    test('ZRANGE returns members in score order', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)
      const zrangeCommand = createZrange(db)

      // Set up sorted set with different scores
      await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('3.0'),
        Buffer.from('member3'),
        Buffer.from('1.0'),
        Buffer.from('member1'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
      ])

      // Get all members
      let result = await runCommand(zrangeCommand, 'ZRANGE', [
        Buffer.from('zset'),
        Buffer.from('0'),
        Buffer.from('-1'),
      ])
      assert.ok(Array.isArray(result.response))
      const members = result.response as Buffer[]
      assert.strictEqual(members.length, 3)
      assert.strictEqual(members[0].toString(), 'member1')
      assert.strictEqual(members[1].toString(), 'member2')
      assert.strictEqual(members[2].toString(), 'member3')

      // Get range with scores
      result = await runCommand(zrangeCommand, 'ZRANGE', [
        Buffer.from('zset'),
        Buffer.from('0'),
        Buffer.from('1'),
        Buffer.from('WITHSCORES'),
      ])
      const withScores = result.response as Buffer[]
      assert.strictEqual(withScores.length, 4) // 2 members * 2 (member + score)
      assert.strictEqual(withScores[0].toString(), 'member1')
      assert.strictEqual(withScores[1].toString(), '1')
      assert.strictEqual(withScores[2].toString(), 'member2')
      assert.strictEqual(withScores[3].toString(), '2')
    })

    test('ZRANGE with non-integer arguments throws error', async () => {
      const db = new DB()
      const zrangeCommand = createZrange(db)

      try {
        await runCommand(zrangeCommand, 'ZRANGE', [
          Buffer.from('zset'),
          Buffer.from('abc'),
          Buffer.from('def'),
        ])
        assert.fail('Should have thrown ExpectedInteger')
      } catch (error) {
        assert.ok(error instanceof ExpectedInteger)
      }
    })
  })

  describe('ZSCORE command', () => {
    test('ZSCORE returns member score', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)
      const zscoreCommand = createZscore(db)

      // Set up sorted set
      await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('1.5'),
        Buffer.from('member1'),
      ])

      // Get score
      const result = await runCommand(zscoreCommand, 'ZSCORE', [
        Buffer.from('zset'),
        Buffer.from('member1'),
      ])
      assert.ok(result.response instanceof Buffer)
      assert.strictEqual((result.response as Buffer).toString(), '1.5')
    })

    test('ZSCORE returns null for non-existent member', async () => {
      const db = new DB()
      const zscoreCommand = createZscore(db)

      const result = await runCommand(zscoreCommand, 'ZSCORE', [
        Buffer.from('zset'),
        Buffer.from('member'),
      ])
      assert.strictEqual(result.response, null)
    })
  })

  describe('ZCARD command', () => {
    test('ZCARD returns number of members in sorted set', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)
      const zcardCommand = createZcard(db)

      // Test on non-existent sorted set
      let result = await runCommand(zcardCommand, 'ZCARD', [
        Buffer.from('zset'),
      ])
      assert.strictEqual(result.response, 0)

      // Add members and test count
      await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('1.0'),
        Buffer.from('member1'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
      ])

      result = await runCommand(zcardCommand, 'ZCARD', [Buffer.from('zset')])
      assert.strictEqual(result.response, 2)
    })
  })

  describe('ZINCRBY command', () => {
    test('ZINCRBY increments member score', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)
      const zincrbyCommand = createZincrby(db)
      const zscoreCommand = createZscore(db)

      // Increment non-existent member
      let result = await runCommand(zincrbyCommand, 'ZINCRBY', [
        Buffer.from('zset'),
        Buffer.from('5.5'),
        Buffer.from('member1'),
      ])
      assert.ok(result.response instanceof Buffer)
      assert.strictEqual((result.response as Buffer).toString(), '5.5')

      // Increment existing member
      await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
      ])

      result = await runCommand(zincrbyCommand, 'ZINCRBY', [
        Buffer.from('zset'),
        Buffer.from('3.0'),
        Buffer.from('member2'),
      ])
      assert.ok(result.response instanceof Buffer)
      assert.strictEqual((result.response as Buffer).toString(), '5')

      // Verify score was updated
      const scoreResult = await runCommand(zscoreCommand, 'ZSCORE', [
        Buffer.from('zset'),
        Buffer.from('member2'),
      ])
      assert.ok(scoreResult.response instanceof Buffer)
      assert.strictEqual((scoreResult.response as Buffer).toString(), '5')
    })
  })

  describe('TYPE command with sorted sets', () => {
    test('TYPE returns zset for sorted set', async () => {
      const db = new DB()
      const zaddCommand = createZadd(db)
      const typeCommand = createType(db)

      // Add sorted set
      await runCommand(zaddCommand, 'ZADD', [
        Buffer.from('zset'),
        Buffer.from('1.0'),
        Buffer.from('member'),
      ])

      const result = await runCommand(typeCommand, 'TYPE', [
        Buffer.from('zset'),
      ])
      assert.strictEqual(result.response, 'zset')
    })
  })

  describe('Error Handling', () => {
    test('Commands throw WrongNumberOfArguments correctly', async () => {
      const db = new DB()

      const commands = [
        { cmd: createZadd(db), name: 'zadd', args: [] },
        { cmd: createZrem(db), name: 'zrem', args: [] },
        {
          cmd: createZrange(db),
          name: 'zrange',
          args: [Buffer.from('key')],
        },
        { cmd: createZscore(db), name: 'zscore', args: [] },
        { cmd: createZcard(db), name: 'zcard', args: [] },
        {
          cmd: createZincrby(db),
          name: 'zincrby',
          args: [Buffer.from('key')],
        },
      ]

      for (const { cmd, name, args } of commands) {
        try {
          await runCommand(cmd, name.toUpperCase(), args)
          assert.fail(`Should have thrown WrongNumberOfArguments for ${name}`)
        } catch (error) {
          assert.ok(error instanceof WrongNumberOfArguments)
          assert.strictEqual(
            error.message,
            `wrong number of arguments for '${name}' command`,
          )
        }
      }
    })
  })

  describe('New Sorted Set Commands (with commander)', () => {
    test('ZREVRANGE command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('1'), Buffer.from('one')],
        new AbortController().signal,
      )
      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('2'), Buffer.from('two')],
        new AbortController().signal,
      )
      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('3'), Buffer.from('three')],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('zrevrange'),
        [Buffer.from('zset'), Buffer.from('0'), Buffer.from('-1')],
        new AbortController().signal,
      )
      const result = transport.getLastResponse() as Buffer[]
      assert.deepStrictEqual(result, [
        Buffer.from('three'),
        Buffer.from('two'),
        Buffer.from('one'),
      ])
    })

    test('ZRANK command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('1'), Buffer.from('one')],
        new AbortController().signal,
      )
      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('2'), Buffer.from('two')],
        new AbortController().signal,
      )
      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('3'), Buffer.from('three')],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('zrank'),
        [Buffer.from('zset'), Buffer.from('one')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)

      await session.execute(
        transport,
        Buffer.from('zrank'),
        [Buffer.from('zset'), Buffer.from('two')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)

      await session.execute(
        transport,
        Buffer.from('zrank'),
        [Buffer.from('zset'), Buffer.from('three')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)
    })

    test('ZREVRANK command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('1'), Buffer.from('one')],
        new AbortController().signal,
      )
      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('2'), Buffer.from('two')],
        new AbortController().signal,
      )
      await session.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('3'), Buffer.from('three')],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('zrevrank'),
        [Buffer.from('zset'), Buffer.from('one')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)

      await session.execute(
        transport,
        Buffer.from('zrevrank'),
        [Buffer.from('zset'), Buffer.from('two')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)

      await session.execute(
        transport,
        Buffer.from('zrevrank'),
        [Buffer.from('zset'), Buffer.from('three')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)
    })
  })
})
