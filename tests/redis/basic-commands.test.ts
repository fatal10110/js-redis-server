import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import { HashDataType } from '../../src/commanders/custom/data-structures/hash'
import { ListDataType } from '../../src/commanders/custom/data-structures/list'
import { SetDataType } from '../../src/commanders/custom/data-structures/set'

// String commands
import { IncrCommand } from '../../src/commanders/custom/commands/redis/data/incr'
import { DecrCommand } from '../../src/commanders/custom/commands/redis/data/decr'
import { AppendCommand } from '../../src/commanders/custom/commands/redis/data/append'
import { StrlenCommand } from '../../src/commanders/custom/commands/redis/data/strlen'

// Key commands
import { ExistsCommand } from '../../src/commanders/custom/commands/redis/data/exists'
import { TypeCommand } from '../../src/commanders/custom/commands/redis/data/type'

// Hash commands
import { HsetCommand } from '../../src/commanders/custom/commands/redis/data/hset'
import { HgetCommand } from '../../src/commanders/custom/commands/redis/data/hget'
import { HdelCommand } from '../../src/commanders/custom/commands/redis/data/hdel'
import { HgetallCommand } from '../../src/commanders/custom/commands/redis/data/hgetall'

// List commands
import { LpushCommand } from '../../src/commanders/custom/commands/redis/data/lpush'
import { RpushCommand } from '../../src/commanders/custom/commands/redis/data/rpush'
import { LpopCommand } from '../../src/commanders/custom/commands/redis/data/lpop'
import { RpopCommand } from '../../src/commanders/custom/commands/redis/data/rpop'
import { LlenCommand } from '../../src/commanders/custom/commands/redis/data/llen'
import { LrangeCommand } from '../../src/commanders/custom/commands/redis/data/lrange'

// Set commands
import { SaddCommand } from '../../src/commanders/custom/commands/redis/data/sadd'
import { SremCommand } from '../../src/commanders/custom/commands/redis/data/srem'
import { ScardCommand } from '../../src/commanders/custom/commands/redis/data/scard'
import { SmembersCommand } from '../../src/commanders/custom/commands/redis/data/smembers'

// Error imports
import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
  HashValueNotInteger,
  HashValueNotFloat,
} from '../../src/core/errors'

describe('Basic Redis Commands', () => {
  describe('String Commands', () => {
    test('INCR command', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      // Test increment on non-existent key
      let result = await incrCommand.run(Buffer.from('INCR'), [
        Buffer.from('counter'),
      ])
      assert.strictEqual(result.response, 1)

      // Test increment on existing key
      result = await incrCommand.run(Buffer.from('INCR'), [
        Buffer.from('counter'),
      ])
      assert.strictEqual(result.response, 2)

      // Test increment on existing string value
      db.set(Buffer.from('num'), new StringDataType(Buffer.from('10')))
      result = await incrCommand.run(Buffer.from('INCR'), [Buffer.from('num')])
      assert.strictEqual(result.response, 11)
    })

    test('DECR command', async () => {
      const db = new DB()
      const decrCommand = new DecrCommand(db)

      // Test decrement on non-existent key
      let result = await decrCommand.run(Buffer.from('DECR'), [
        Buffer.from('counter'),
      ])
      assert.strictEqual(result.response, -1)

      // Test decrement on existing key
      result = await decrCommand.run(Buffer.from('DECR'), [
        Buffer.from('counter'),
      ])
      assert.strictEqual(result.response, -2)
    })

    test('APPEND command', async () => {
      const db = new DB()
      const appendCommand = new AppendCommand(db)

      // Test append to non-existent key
      let result = await appendCommand.run(Buffer.from('APPEND'), [
        Buffer.from('key'),
        Buffer.from('hello'),
      ])
      assert.strictEqual(result.response, 5)

      // Test append to existing key
      result = await appendCommand.run(Buffer.from('APPEND'), [
        Buffer.from('key'),
        Buffer.from(' world'),
      ])
      assert.strictEqual(result.response, 11)

      const value = db.get(Buffer.from('key')) as StringDataType
      assert.strictEqual(value.data.toString(), 'hello world')
    })

    test('STRLEN command', async () => {
      const db = new DB()
      const strlenCommand = new StrlenCommand(db)

      // Test strlen on non-existent key
      let result = await strlenCommand.run(Buffer.from('STRLEN'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, 0)

      // Test strlen on existing key
      db.set(Buffer.from('key'), new StringDataType(Buffer.from('hello')))
      result = await strlenCommand.run(Buffer.from('STRLEN'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, 5)
    })
  })

  describe('Key Commands', () => {
    test('EXISTS command', async () => {
      const db = new DB()
      const existsCommand = new ExistsCommand(db)

      // Test exists on non-existent keys
      let result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key1'),
        Buffer.from('key2'),
      ])
      assert.strictEqual(result.response, 0)

      // Test exists on mixed keys
      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value')))
      result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key1'),
        Buffer.from('key2'),
      ])
      assert.strictEqual(result.response, 1)

      // Test exists on all existing keys
      db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value')))
      result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key1'),
        Buffer.from('key2'),
      ])
      assert.strictEqual(result.response, 2)
    })

    test('TYPE command', async () => {
      const db = new DB()
      const typeCommand = new TypeCommand(db)

      // Test type on non-existent key
      let result = await typeCommand.run(Buffer.from('TYPE'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, 'none')

      // Test type on string
      db.set(Buffer.from('str'), new StringDataType(Buffer.from('value')))
      result = await typeCommand.run(Buffer.from('TYPE'), [Buffer.from('str')])
      assert.strictEqual(result.response, 'string')

      // Test type on hash
      db.set(Buffer.from('hash'), new HashDataType())
      result = await typeCommand.run(Buffer.from('TYPE'), [Buffer.from('hash')])
      assert.strictEqual(result.response, 'hash')

      // Test type on list
      db.set(Buffer.from('list'), new ListDataType())
      result = await typeCommand.run(Buffer.from('TYPE'), [Buffer.from('list')])
      assert.strictEqual(result.response, 'list')

      // Test type on set
      db.set(Buffer.from('set'), new SetDataType())
      result = await typeCommand.run(Buffer.from('TYPE'), [Buffer.from('set')])
      assert.strictEqual(result.response, 'set')
    })
  })

  describe('Hash Commands', () => {
    test('HSET and HGET commands', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hgetCommand = new HgetCommand(db)

      // Test hset on new hash
      let result = await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
      ])
      assert.strictEqual(result.response, 1)

      // Test hget
      result = await hgetCommand.run(Buffer.from('HGET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
      ])
      assert.deepStrictEqual(result.response, Buffer.from('value1'))

      // Test hget on non-existent field
      result = await hgetCommand.run(Buffer.from('HGET'), [
        Buffer.from('hash'),
        Buffer.from('field2'),
      ])
      assert.strictEqual(result.response, null)
    })

    test('HDEL command', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hdelCommand = new HdelCommand(db)

      // Set up hash
      await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      // Test hdel
      let result = await hdelCommand.run(Buffer.from('HDEL'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
      ])
      assert.strictEqual(result.response, 1)

      // Test hdel on non-existent field
      result = await hdelCommand.run(Buffer.from('HDEL'), [
        Buffer.from('hash'),
        Buffer.from('field3'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('HGETALL command', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hgetallCommand = new HgetallCommand(db)

      // Test hgetall on non-existent hash
      let result = await hgetallCommand.run(Buffer.from('HGETALL'), [
        Buffer.from('hash'),
      ])
      assert.deepStrictEqual(result.response, [])

      // Set up hash
      await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      // Test hgetall
      result = await hgetallCommand.run(Buffer.from('HGETALL'), [
        Buffer.from('hash'),
      ])
      assert.ok(Array.isArray(result.response))
      assert.strictEqual((result.response as Buffer[]).length, 4) // 2 fields * 2 (field + value)
    })
  })

  describe('List Commands', () => {
    test('LPUSH and RPUSH commands', async () => {
      const db = new DB()
      const lpushCommand = new LpushCommand(db)
      const rpushCommand = new RpushCommand(db)

      // Test lpush on new list
      let result = await lpushCommand.run(Buffer.from('LPUSH'), [
        Buffer.from('list'),
        Buffer.from('item1'),
      ])
      assert.strictEqual(result.response, 1)

      // Test rpush on existing list
      result = await rpushCommand.run(Buffer.from('RPUSH'), [
        Buffer.from('list'),
        Buffer.from('item2'),
      ])
      assert.strictEqual(result.response, 2)
    })

    test('LPOP and RPOP commands', async () => {
      const db = new DB()
      const lpushCommand = new LpushCommand(db)
      const lpopCommand = new LpopCommand(db)
      const rpopCommand = new RpopCommand(db)

      // Test pop on non-existent list
      let result = await lpopCommand.run(Buffer.from('LPOP'), [
        Buffer.from('list'),
      ])
      assert.strictEqual(result.response, null)

      // Set up list
      await lpushCommand.run(Buffer.from('LPUSH'), [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
      ])

      // Test lpop
      result = await lpopCommand.run(Buffer.from('LPOP'), [Buffer.from('list')])
      assert.deepStrictEqual(result.response, Buffer.from('item2'))

      // Test rpop
      result = await rpopCommand.run(Buffer.from('RPOP'), [Buffer.from('list')])
      assert.deepStrictEqual(result.response, Buffer.from('item1'))

      // List should be empty and removed from DB
      assert.strictEqual(db.get(Buffer.from('list')), null)
    })

    test('LLEN command', async () => {
      const db = new DB()
      const llenCommand = new LlenCommand(db)
      const lpushCommand = new LpushCommand(db)

      // Test llen on non-existent list
      let result = await llenCommand.run(Buffer.from('LLEN'), [
        Buffer.from('list'),
      ])
      assert.strictEqual(result.response, 0)

      // Test llen on existing list
      await lpushCommand.run(Buffer.from('LPUSH'), [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
      ])
      result = await llenCommand.run(Buffer.from('LLEN'), [Buffer.from('list')])
      assert.strictEqual(result.response, 2)
    })

    test('LRANGE command', async () => {
      const db = new DB()
      const lrangeCommand = new LrangeCommand(db)
      const lpushCommand = new LpushCommand(db)

      // Test lrange on non-existent list
      let result = await lrangeCommand.run(Buffer.from('LRANGE'), [
        Buffer.from('list'),
        Buffer.from('0'),
        Buffer.from('-1'),
      ])
      assert.deepStrictEqual(result.response, [])

      // Set up list
      await lpushCommand.run(Buffer.from('LPUSH'), [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
        Buffer.from('item3'),
      ])

      // Test lrange
      result = await lrangeCommand.run(Buffer.from('LRANGE'), [
        Buffer.from('list'),
        Buffer.from('0'),
        Buffer.from('1'),
      ])
      assert.ok(Array.isArray(result.response))
      assert.strictEqual((result.response as Buffer[]).length, 2)
    })
  })

  describe('Set Commands', () => {
    test('SADD command', async () => {
      const db = new DB()
      const saddCommand = new SaddCommand(db)

      // Test sadd on new set
      let result = await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)

      // Test sadd duplicate member
      result = await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 0)

      // Test sadd multiple members
      result = await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member2'),
        Buffer.from('member3'),
      ])
      assert.strictEqual(result.response, 2)
    })

    test('SREM command', async () => {
      const db = new DB()
      const saddCommand = new SaddCommand(db)
      const sremCommand = new SremCommand(db)

      // Test srem on non-existent set
      let result = await sremCommand.run(Buffer.from('SREM'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 0)

      // Set up set
      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
        Buffer.from('member2'),
      ])

      // Test srem
      result = await sremCommand.run(Buffer.from('SREM'), [
        Buffer.from('set'),
        Buffer.from('member1'),
      ])
      assert.strictEqual(result.response, 1)

      // Test srem non-existent member
      result = await sremCommand.run(Buffer.from('SREM'), [
        Buffer.from('set'),
        Buffer.from('member3'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('SCARD command', async () => {
      const db = new DB()
      const scardCommand = new ScardCommand(db)
      const saddCommand = new SaddCommand(db)

      // Test scard on non-existent set
      let result = await scardCommand.run(Buffer.from('SCARD'), [
        Buffer.from('set'),
      ])
      assert.strictEqual(result.response, 0)

      // Test scard on existing set
      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
        Buffer.from('member2'),
      ])
      result = await scardCommand.run(Buffer.from('SCARD'), [
        Buffer.from('set'),
      ])
      assert.strictEqual(result.response, 2)
    })

    test('SMEMBERS command', async () => {
      const db = new DB()
      const smembersCommand = new SmembersCommand(db)
      const saddCommand = new SaddCommand(db)

      // Test smembers on non-existent set
      let result = await smembersCommand.run(Buffer.from('SMEMBERS'), [
        Buffer.from('set'),
      ])
      assert.deepStrictEqual(result.response, [])

      // Test smembers on existing set
      await saddCommand.run(Buffer.from('SADD'), [
        Buffer.from('set'),
        Buffer.from('member1'),
        Buffer.from('member2'),
      ])
      result = await smembersCommand.run(Buffer.from('SMEMBERS'), [
        Buffer.from('set'),
      ])
      assert.ok(Array.isArray(result.response))
      assert.strictEqual((result.response as Buffer[]).length, 2)
    })
  })

  describe('Error Handling', () => {
    test('INCR with non-numeric value throws correct error', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      // Set a non-numeric value
      db.set(Buffer.from('key'), new StringDataType(Buffer.from('notanumber')))

      try {
        await incrCommand.run(Buffer.from('INCR'), [Buffer.from('key')])
        assert.fail('Should have thrown ExpectedInteger error')
      } catch (error) {
        assert.ok(error instanceof ExpectedInteger)
        assert.strictEqual(
          error.message,
          'value is not an integer or out of range',
        )
        assert.strictEqual(error.name, 'ERR')
      }
    })

    test('INCR on wrong data type throws correct error', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      // Set a list (wrong type)
      db.set(Buffer.from('key'), new ListDataType())

      try {
        await incrCommand.run(Buffer.from('INCR'), [Buffer.from('key')])
        assert.fail('Should have thrown WrongType error')
      } catch (error) {
        assert.ok(error instanceof WrongType)
        assert.strictEqual(
          error.message,
          'Operation against a key holding the wrong kind of value',
        )
        assert.strictEqual(error.name, 'WRONGTYPE')
      }
    })

    test('HINCRBY with non-integer hash value throws correct error', async () => {
      const hash = new HashDataType()
      hash.hset(Buffer.from('field'), Buffer.from('notanumber'))

      try {
        hash.hincrby(Buffer.from('field'), 1)
        assert.fail('Should have thrown HashValueNotInteger error')
      } catch (error) {
        assert.ok(error instanceof HashValueNotInteger)
        assert.strictEqual(error.message, 'hash value is not an integer')
        assert.strictEqual(error.name, 'ERR')
      }
    })

    test('HINCRBYFLOAT with non-float hash value throws correct error', async () => {
      const hash = new HashDataType()
      hash.hset(Buffer.from('field'), Buffer.from('notafloat'))

      try {
        hash.hincrbyfloat(Buffer.from('field'), 1.5)
        assert.fail('Should have thrown HashValueNotFloat error')
      } catch (error) {
        assert.ok(error instanceof HashValueNotFloat)
        assert.strictEqual(error.message, 'hash value is not a float')
        assert.strictEqual(error.name, 'ERR')
      }
    })

    test('LRANGE with non-integer arguments throws correct error', async () => {
      const db = new DB()
      const lrangeCommand = new LrangeCommand(db)

      // Set up a list
      db.set(Buffer.from('list'), new ListDataType())

      try {
        await lrangeCommand.run(Buffer.from('LRANGE'), [
          Buffer.from('list'),
          Buffer.from('abc'),
          Buffer.from('def'),
        ])
        assert.fail('Should have thrown ExpectedInteger error')
      } catch (error) {
        assert.ok(error instanceof ExpectedInteger)
        assert.strictEqual(
          error.message,
          'value is not an integer or out of range',
        )
        assert.strictEqual(error.name, 'ERR')
      }
    })

    test('All commands throw WrongNumberOfArguments with correct format', async () => {
      const db = new DB()

      // Test a few different commands
      const commands = [
        { cmd: new IncrCommand(db), name: 'incr', args: [] },
        { cmd: new ExistsCommand(db), name: 'exists', args: [] },
        { cmd: new HgetCommand(db), name: 'hget', args: [Buffer.from('key')] },
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
          assert.strictEqual(error.name, 'ERR')
        }
      }
    })
  })
})
