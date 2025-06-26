import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { createCustomCommander } from '../../src/commanders/custom/commander'
import { createMockTransport } from '../mock-transport'

// Set commands
import { SaddCommand } from '../../src/commanders/custom/commands/redis/data/sets/sadd'
import { SremCommand } from '../../src/commanders/custom/commands/redis/data/sets/srem'
import { ScardCommand } from '../../src/commanders/custom/commands/redis/data/sets/scard'
import { SmembersCommand } from '../../src/commanders/custom/commands/redis/data/sets/smembers'

// Error imports
import { WrongNumberOfArguments } from '../../src/core/errors'

describe('Set Commands', () => {
  describe('SADD command', () => {
    test('SADD on new set', async () => {
      const db = new DB()
      const saddCommand = new SaddCommand(db)

      const result = await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)
    })

    test('SADD duplicate member', async () => {
      const db = new DB()
      const saddCommand = new SaddCommand(db)

      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      const result = await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('SADD multiple members', async () => {
      const db = new DB()
      const saddCommand = new SaddCommand(db)

      const result = await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
        Buffer.from('member2'),
        Buffer.from('member3'),
      ])
      assert.strictEqual(result.response, 3)
    })
  })

  describe('SREM command', () => {
    test('SREM on non-existent set', async () => {
      const db = new DB()
      const sremCommand = new SremCommand(db)

      const result = await sremCommand.run(Buffer.from('SREM'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('SREM existing member', async () => {
      const db = new DB()
      const saddCommand = new SaddCommand(db)
      const sremCommand = new SremCommand(db)

      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
        Buffer.from('member2'),
      ])

      const result = await sremCommand.run(Buffer.from('SREM'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)
    })

    test('SREM non-existent member', async () => {
      const db = new DB()
      const saddCommand = new SaddCommand(db)
      const sremCommand = new SremCommand(db)

      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])

      const result = await sremCommand.run(Buffer.from('SREM'), [
        Buffer.from('set'),
        Buffer.from('member3'),
      ])
      assert.strictEqual(result.response, 0)
    })
  })

  describe('SCARD command', () => {
    test('SCARD on non-existent set', async () => {
      const db = new DB()
      const scardCommand = new ScardCommand(db)

      const result = await scardCommand.run(Buffer.from('SCARD'), [
        Buffer.from('set'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('SCARD on existing set', async () => {
      const db = new DB()
      const scardCommand = new ScardCommand(db)
      const saddCommand = new SaddCommand(db)

      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
        Buffer.from('member2'),
      ])
      const result = await scardCommand.run(Buffer.from('SCARD'), [
        Buffer.from('set'),
      ])
      assert.strictEqual(result.response, 2)
    })
  })

  describe('SMEMBERS command', () => {
    test('SMEMBERS on non-existent set', async () => {
      const db = new DB()
      const smembersCommand = new SmembersCommand(db)

      const result = await smembersCommand.run(Buffer.from('SMEMBERS'), [
        Buffer.from('set'),
      ])
      assert.deepStrictEqual(result.response, [])
    })

    test('SMEMBERS on existing set', async () => {
      const db = new DB()
      const smembersCommand = new SmembersCommand(db)
      const saddCommand = new SaddCommand(db)

      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
        Buffer.from('member2'),
      ])
      const result = await smembersCommand.run(Buffer.from('SMEMBERS'), [
        Buffer.from('set'),
      ])
      assert.ok(Array.isArray(result.response))
      assert.strictEqual((result.response as Buffer[]).length, 2)
    })
  })

  describe('New Set Commands (with commander)', () => {
    test('SISMEMBER command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set'), Buffer.from('member1')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('sismember'),
        [Buffer.from('set'), Buffer.from('member1')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)

      await commander.execute(
        transport,
        Buffer.from('sismember'),
        [Buffer.from('set'), Buffer.from('member2')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SPOP command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set'), Buffer.from('member1')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('spop'),
        [Buffer.from('set')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(
        transport.getLastResponse(),
        Buffer.from('member1'),
      )

      // Set should be empty now
      await commander.execute(
        transport,
        Buffer.from('scard'),
        [Buffer.from('set')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SRANDMEMBER command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set'), Buffer.from('member1')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set'), Buffer.from('member2')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('srandmember'),
        [Buffer.from('set')],
        new AbortController().signal,
      )
      const memberStr = (transport.getLastResponse() as Buffer).toString()
      assert(memberStr === 'member1' || memberStr === 'member2')

      // Verify set still has both members
      await commander.execute(
        transport,
        Buffer.from('scard'),
        [Buffer.from('set')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SDIFF command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('a')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('b')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('c')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set2'), Buffer.from('b')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set2'), Buffer.from('d')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('sdiff'),
        [Buffer.from('set1'), Buffer.from('set2')],
        new AbortController().signal,
      )
      const result = transport.getLastResponse() as Buffer[]
      const members = result.map(b => b.toString()).sort()
      assert.deepStrictEqual(members, ['a', 'c'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SINTER command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('a')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('b')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('c')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set2'), Buffer.from('b')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set2'), Buffer.from('c')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set2'), Buffer.from('d')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('sinter'),
        [Buffer.from('set1'), Buffer.from('set2')],
        new AbortController().signal,
      )
      const result = transport.getLastResponse() as Buffer[]
      const members = result.map(b => b.toString()).sort()
      assert.deepStrictEqual(members, ['b', 'c'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SUNION command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('a')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set1'), Buffer.from('b')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set2'), Buffer.from('b')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('set2'), Buffer.from('c')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('sunion'),
        [Buffer.from('set1'), Buffer.from('set2')],
        new AbortController().signal,
      )
      const result = transport.getLastResponse() as Buffer[]
      const members = result.map(b => b.toString()).sort()
      assert.deepStrictEqual(members, ['a', 'b', 'c'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SMOVE command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()
      const transport = createMockTransport()

      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('source'), Buffer.from('member1')],
        new AbortController().signal,
      )
      await commander.execute(
        transport,
        Buffer.from('sadd'),
        [Buffer.from('source'), Buffer.from('member2')],
        new AbortController().signal,
      )

      await commander.execute(
        transport,
        Buffer.from('smove'),
        [Buffer.from('source'), Buffer.from('dest'), Buffer.from('member1')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)

      // Verify member was moved
      await commander.execute(
        transport,
        Buffer.from('smembers'),
        [Buffer.from('source')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(transport.getLastResponse(), [
        Buffer.from('member2'),
      ])

      await commander.execute(
        transport,
        Buffer.from('smembers'),
        [Buffer.from('dest')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(transport.getLastResponse(), [
        Buffer.from('member1'),
      ])

      await commander.shutdown()
      await factory.shutdown()
    })
  })

  describe('Set Error Handling', () => {
    test('Set commands throw WrongNumberOfArguments with correct format', async () => {
      const db = new DB()
      const scardCommand = new ScardCommand(db)

      try {
        await scardCommand.run(Buffer.from('SCARD'), [])
        assert.fail('Should have thrown WrongNumberOfArguments for scard')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'scard' command",
        )
        assert.strictEqual(error.name, 'ERR')
      }
    })
  })
})
