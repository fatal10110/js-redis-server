import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { createCustomCommander } from '../../src/commanders/custom/commander'

// Set commands
import { SaddCommand } from '../../src/commanders/custom/commands/redis/data/sadd'
import { SremCommand } from '../../src/commanders/custom/commands/redis/data/srem'
import { ScardCommand } from '../../src/commanders/custom/commands/redis/data/scard'
import { SmembersCommand } from '../../src/commanders/custom/commands/redis/data/smembers'

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

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])

      const exists1 = await commander.execute(Buffer.from('sismember'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(exists1.response, 1)

      const exists2 = await commander.execute(Buffer.from('sismember'), [
        Buffer.from('set'),
        Buffer.from('member2'),
      ])
      assert.strictEqual(exists2.response, 0)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SPOP command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])

      const result = await commander.execute(Buffer.from('spop'), [
        Buffer.from('set'),
      ])
      assert.deepStrictEqual(result.response, Buffer.from('member1'))

      // Set should be empty now
      const card = await commander.execute(Buffer.from('scard'), [
        Buffer.from('set'),
      ])
      assert.strictEqual(card.response, 0)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SRANDMEMBER command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set'),
        Buffer.from('member2'),
      ])

      const member = await commander.execute(Buffer.from('srandmember'), [
        Buffer.from('set'),
      ])
      const memberStr = (member.response as Buffer).toString()
      assert(memberStr === 'member1' || memberStr === 'member2')

      // Verify set still has both members
      const card = await commander.execute(Buffer.from('scard'), [
        Buffer.from('set'),
      ])
      assert.strictEqual(card.response, 2)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SDIFF command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('a'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('b'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('c'),
      ])

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set2'),
        Buffer.from('b'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set2'),
        Buffer.from('d'),
      ])

      const diff = await commander.execute(Buffer.from('sdiff'), [
        Buffer.from('set1'),
        Buffer.from('set2'),
      ])
      const result = diff.response as Buffer[]
      const members = result.map(b => b.toString()).sort()
      assert.deepStrictEqual(members, ['a', 'c'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SINTER command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('a'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('b'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('c'),
      ])

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set2'),
        Buffer.from('b'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set2'),
        Buffer.from('c'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set2'),
        Buffer.from('d'),
      ])

      const inter = await commander.execute(Buffer.from('sinter'), [
        Buffer.from('set1'),
        Buffer.from('set2'),
      ])
      const result = inter.response as Buffer[]
      const members = result.map(b => b.toString()).sort()
      assert.deepStrictEqual(members, ['b', 'c'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SUNION command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('a'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set1'),
        Buffer.from('b'),
      ])

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set2'),
        Buffer.from('b'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('set2'),
        Buffer.from('c'),
      ])

      const union = await commander.execute(Buffer.from('sunion'), [
        Buffer.from('set1'),
        Buffer.from('set2'),
      ])
      const result = union.response as Buffer[]
      const members = result.map(b => b.toString()).sort()
      assert.deepStrictEqual(members, ['a', 'b', 'c'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('SMOVE command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('source'),
        Buffer.from('member1'),
      ])
      await commander.execute(Buffer.from('sadd'), [
        Buffer.from('source'),
        Buffer.from('member2'),
      ])

      const move = await commander.execute(Buffer.from('smove'), [
        Buffer.from('source'),
        Buffer.from('dest'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(move.response, 1)

      // Verify member was moved
      const sourceMembers = await commander.execute(Buffer.from('smembers'), [
        Buffer.from('source'),
      ])
      assert.deepStrictEqual(sourceMembers.response, [Buffer.from('member2')])

      const destMembers = await commander.execute(Buffer.from('smembers'), [
        Buffer.from('dest'),
      ])
      assert.deepStrictEqual(destMembers.response, [Buffer.from('member1')])

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
