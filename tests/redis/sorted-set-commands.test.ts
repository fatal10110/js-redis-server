import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import { createCustomCommander } from '../../src/commanders/custom/commander'
import { createMockTransport } from '../mock-transport'

// Sorted set commands
import { ZaddCommand } from '../../src/commanders/custom/commands/redis/data/zsets/zadd'
import { ZremCommand } from '../../src/commanders/custom/commands/redis/data/zsets/zrem'
import { ZrangeCommand } from '../../src/commanders/custom/commands/redis/data/zsets/zrange'
import { ZscoreCommand } from '../../src/commanders/custom/commands/redis/data/zsets/zscore'
import { ZcardCommand } from '../../src/commanders/custom/commands/redis/data/zsets/zcard'
import { ZincrbyCommand } from '../../src/commanders/custom/commands/redis/data/zsets/zincrby'

// Type command
import { TypeCommand } from '../../src/commanders/custom/commands/redis/data/keys/type'

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
      const zaddCommand = new ZaddCommand(db)

      // Add single member
      let result = await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('1.5'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)

      // Add multiple members
      result = await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('2.5'),
        Buffer.from('member2'),
        Buffer.from('0.5'),
        Buffer.from('member3'),
      ])
      assert.strictEqual(result.response, 2)

      // Add existing member (should update score, return 0)
      result = await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('3.0'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('ZADD with wrong number of arguments throws error', async () => {
      const db = new DB()
      const zaddCommand = new ZaddCommand(db)

      try {
        await zaddCommand.run(Buffer.from('ZADD'), [Buffer.from('zset')])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }

      try {
        await zaddCommand.run(Buffer.from('ZADD'), [
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
      const zaddCommand = new ZaddCommand(db)

      try {
        await zaddCommand.run(Buffer.from('ZADD'), [
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
      const zaddCommand = new ZaddCommand(db)

      // Set a string value
      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))

      try {
        await zaddCommand.run(Buffer.from('ZADD'), [
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
      const zaddCommand = new ZaddCommand(db)
      const zremCommand = new ZremCommand(db)

      // Set up sorted set
      await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('1.0'),
        Buffer.from('member1'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
        Buffer.from('3.0'),
        Buffer.from('member3'),
      ])

      // Remove single member
      let result = await zremCommand.run(Buffer.from('ZREM'), [
        Buffer.from('zset'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)

      // Remove multiple members
      result = await zremCommand.run(Buffer.from('ZREM'), [
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
      const zremCommand = new ZremCommand(db)

      const result = await zremCommand.run(Buffer.from('ZREM'), [
        Buffer.from('zset'),
        Buffer.from('member'),
      ])
      assert.strictEqual(result.response, 0)
    })
  })

  describe('ZRANGE command', () => {
    test('ZRANGE returns members in score order', async () => {
      const db = new DB()
      const zaddCommand = new ZaddCommand(db)
      const zrangeCommand = new ZrangeCommand(db)

      // Set up sorted set with different scores
      await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('3.0'),
        Buffer.from('member3'),
        Buffer.from('1.0'),
        Buffer.from('member1'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
      ])

      // Get all members
      let result = await zrangeCommand.run(Buffer.from('ZRANGE'), [
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
      result = await zrangeCommand.run(Buffer.from('ZRANGE'), [
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
      const zrangeCommand = new ZrangeCommand(db)

      try {
        await zrangeCommand.run(Buffer.from('ZRANGE'), [
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
      const zaddCommand = new ZaddCommand(db)
      const zscoreCommand = new ZscoreCommand(db)

      // Set up sorted set
      await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('1.5'),
        Buffer.from('member1'),
      ])

      // Get score
      const result = await zscoreCommand.run(Buffer.from('ZSCORE'), [
        Buffer.from('zset'),
        Buffer.from('member1'),
      ])
      assert.ok(result.response instanceof Buffer)
      assert.strictEqual((result.response as Buffer).toString(), '1.5')
    })

    test('ZSCORE returns null for non-existent member', async () => {
      const db = new DB()
      const zscoreCommand = new ZscoreCommand(db)

      const result = await zscoreCommand.run(Buffer.from('ZSCORE'), [
        Buffer.from('zset'),
        Buffer.from('member'),
      ])
      assert.strictEqual(result.response, null)
    })
  })

  describe('ZCARD command', () => {
    test('ZCARD returns number of members in sorted set', async () => {
      const db = new DB()
      const zaddCommand = new ZaddCommand(db)
      const zcardCommand = new ZcardCommand(db)

      // Test on non-existent sorted set
      let result = await zcardCommand.run(Buffer.from('ZCARD'), [
        Buffer.from('zset'),
      ])
      assert.strictEqual(result.response, 0)

      // Add members and test count
      await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('1.0'),
        Buffer.from('member1'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
      ])

      result = await zcardCommand.run(Buffer.from('ZCARD'), [
        Buffer.from('zset'),
      ])
      assert.strictEqual(result.response, 2)
    })
  })

  describe('ZINCRBY command', () => {
    test('ZINCRBY increments member score', async () => {
      const db = new DB()
      const zaddCommand = new ZaddCommand(db)
      const zincrbyCommand = new ZincrbyCommand(db)
      const zscoreCommand = new ZscoreCommand(db)

      // Increment non-existent member
      let result = await zincrbyCommand.run(Buffer.from('ZINCRBY'), [
        Buffer.from('zset'),
        Buffer.from('5.5'),
        Buffer.from('member1'),
      ])
      assert.ok(result.response instanceof Buffer)
      assert.strictEqual((result.response as Buffer).toString(), '5.5')

      // Increment existing member
      await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('2.0'),
        Buffer.from('member2'),
      ])

      result = await zincrbyCommand.run(Buffer.from('ZINCRBY'), [
        Buffer.from('zset'),
        Buffer.from('3.0'),
        Buffer.from('member2'),
      ])
      assert.ok(result.response instanceof Buffer)
      assert.strictEqual((result.response as Buffer).toString(), '5')

      // Verify score was updated
      const scoreResult = await zscoreCommand.run(Buffer.from('ZSCORE'), [
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
      const zaddCommand = new ZaddCommand(db)
      const typeCommand = new TypeCommand(db)

      // Add sorted set
      await zaddCommand.run(Buffer.from('ZADD'), [
        Buffer.from('zset'),
        Buffer.from('1.0'),
        Buffer.from('member'),
      ])

      const result = await typeCommand.run(Buffer.from('TYPE'), [
        Buffer.from('zset'),
      ])
      assert.strictEqual(result.response, 'zset')
    })
  })

  describe('Error Handling', () => {
    test('Commands throw WrongNumberOfArguments correctly', async () => {
      const db = new DB()

      const commands = [
        { cmd: new ZaddCommand(db), name: 'zadd', args: [] },
        { cmd: new ZremCommand(db), name: 'zrem', args: [] },
        {
          cmd: new ZrangeCommand(db),
          name: 'zrange',
          args: [Buffer.from('key')],
        },
        { cmd: new ZscoreCommand(db), name: 'zscore', args: [] },
        { cmd: new ZcardCommand(db), name: 'zcard', args: [] },
        {
          cmd: new ZincrbyCommand(db),
          name: 'zincrby',
          args: [Buffer.from('key')],
        },
      ]

      for (const { cmd, name, args } of commands) {
        try {
          await cmd.run(Buffer.from(name.toUpperCase()), args)
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
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('1'), Buffer.from('one')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('2'), Buffer.from('two')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('3'), Buffer.from('three')],
        new AbortController().signal,
      )

      await commander.execute(
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

      await commander.shutdown()
      await factory.shutdown()
    })

    test('ZRANK command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('1'), Buffer.from('one')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('2'), Buffer.from('two')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('3'), Buffer.from('three')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('zrank'),
        [Buffer.from('zset'), Buffer.from('one')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)

      await commander.execute(
        transport,
        Buffer.from('zrank'),
        [Buffer.from('zset'), Buffer.from('two')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)

      await commander.execute(
        transport,
        Buffer.from('zrank'),
        [Buffer.from('zset'), Buffer.from('three')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('ZREVRANK command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('1'), Buffer.from('one')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('2'), Buffer.from('two')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('zadd'),
        [Buffer.from('zset'), Buffer.from('3'), Buffer.from('three')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('zrevrank'),
        [Buffer.from('zset'), Buffer.from('one')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)

      await commander.execute(
        transport,
        Buffer.from('zrevrank'),
        [Buffer.from('zset'), Buffer.from('two')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)

      await commander.execute(
        transport,
        Buffer.from('zrevrank'),
        [Buffer.from('zset'), Buffer.from('three')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)

      await commander.shutdown()
      await factory.shutdown()
    })
  })
})
