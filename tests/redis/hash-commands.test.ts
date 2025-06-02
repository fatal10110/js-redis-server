import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { HashDataType } from '../../src/commanders/custom/data-structures/hash'
import { createCustomCommander } from '../../src/commanders/custom/commander'

// Hash commands
import { HsetCommand } from '../../src/commanders/custom/commands/redis/data/hashes/hset'
import { HgetCommand } from '../../src/commanders/custom/commands/redis/data/hashes/hget'
import { HdelCommand } from '../../src/commanders/custom/commands/redis/data/hashes/hdel'
import { HgetallCommand } from '../../src/commanders/custom/commands/redis/data/hashes/hgetall'

// Error imports
import {
  WrongNumberOfArguments,
  HashValueNotInteger,
  HashValueNotFloat,
} from '../../src/core/errors'

describe('Hash Commands', () => {
  describe('HSET and HGET commands', () => {
    test('HSET on new hash', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hgetCommand = new HgetCommand(db)

      const result = await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
      ])
      assert.strictEqual(result.response, 1)

      const getResult = await hgetCommand.run(Buffer.from('HGET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
      ])
      assert.deepStrictEqual(getResult.response, Buffer.from('value1'))
    })

    test('HGET on non-existent field', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hgetCommand = new HgetCommand(db)

      await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
      ])

      const result = await hgetCommand.run(Buffer.from('HGET'), [
        Buffer.from('hash'),
        Buffer.from('field2'),
      ])
      assert.strictEqual(result.response, null)
    })

    test('HGET on non-existent hash', async () => {
      const db = new DB()
      const hgetCommand = new HgetCommand(db)

      const result = await hgetCommand.run(Buffer.from('HGET'), [
        Buffer.from('nonexistent'),
        Buffer.from('field'),
      ])
      assert.strictEqual(result.response, null)
    })
  })

  describe('HDEL command', () => {
    test('HDEL existing field', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hdelCommand = new HdelCommand(db)

      await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      const result = await hdelCommand.run(Buffer.from('HDEL'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
      ])
      assert.strictEqual(result.response, 1)
    })

    test('HDEL non-existent field', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hdelCommand = new HdelCommand(db)

      await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
      ])

      const result = await hdelCommand.run(Buffer.from('HDEL'), [
        Buffer.from('hash'),
        Buffer.from('field3'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('HDEL on non-existent hash', async () => {
      const db = new DB()
      const hdelCommand = new HdelCommand(db)

      const result = await hdelCommand.run(Buffer.from('HDEL'), [
        Buffer.from('nonexistent'),
        Buffer.from('field'),
      ])
      assert.strictEqual(result.response, 0)
    })
  })

  describe('HGETALL command', () => {
    test('HGETALL on non-existent hash', async () => {
      const db = new DB()
      const hgetallCommand = new HgetallCommand(db)

      const result = await hgetallCommand.run(Buffer.from('HGETALL'), [
        Buffer.from('hash'),
      ])
      assert.deepStrictEqual(result.response, [])
    })

    test('HGETALL on existing hash', async () => {
      const db = new DB()
      const hsetCommand = new HsetCommand(db)
      const hgetallCommand = new HgetallCommand(db)

      await hsetCommand.run(Buffer.from('HSET'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      const result = await hgetallCommand.run(Buffer.from('HGETALL'), [
        Buffer.from('hash'),
      ])
      assert.ok(Array.isArray(result.response))
      assert.strictEqual((result.response as Buffer[]).length, 4) // 2 fields * 2 (field + value)
    })
  })

  describe('New Hash Commands (with commander)', () => {
    test('HEXISTS command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('hset'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
      ])

      const exists1 = await commander.execute(Buffer.from('hexists'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
      ])
      assert.strictEqual(exists1.response, 1)

      const exists2 = await commander.execute(Buffer.from('hexists'), [
        Buffer.from('hash'),
        Buffer.from('field2'),
      ])
      assert.strictEqual(exists2.response, 0)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('HINCRBY command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      const incr1 = await commander.execute(Buffer.from('hincrby'), [
        Buffer.from('hash'),
        Buffer.from('counter'),
        Buffer.from('5'),
      ])
      assert.strictEqual(incr1.response, 5)

      const incr2 = await commander.execute(Buffer.from('hincrby'), [
        Buffer.from('hash'),
        Buffer.from('counter'),
        Buffer.from('3'),
      ])
      assert.strictEqual(incr2.response, 8)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('HINCRBYFLOAT command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      const incr1 = await commander.execute(Buffer.from('hincrbyfloat'), [
        Buffer.from('hash'),
        Buffer.from('float'),
        Buffer.from('1.5'),
      ])
      assert.deepStrictEqual(incr1.response, Buffer.from('1.5'))

      const incr2 = await commander.execute(Buffer.from('hincrbyfloat'), [
        Buffer.from('hash'),
        Buffer.from('float'),
        Buffer.from('2.3'),
      ])
      assert.deepStrictEqual(incr2.response, Buffer.from('3.8'))

      await commander.shutdown()
      await factory.shutdown()
    })

    test('HMGET command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('hset'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      const result = await commander.execute(Buffer.from('hmget'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('field2'),
        Buffer.from('nonexistent'),
      ])

      assert.ok(Array.isArray(result.response))
      const values = result.response as (Buffer | null)[]
      assert.strictEqual(values.length, 3)
      assert.deepStrictEqual(values[0], Buffer.from('value1'))
      assert.deepStrictEqual(values[1], Buffer.from('value2'))
      assert.strictEqual(values[2], null)

      await commander.shutdown()
      await factory.shutdown()
    })

    test('HMSET command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      const result = await commander.execute(Buffer.from('hmset'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])
      assert.strictEqual(result.response, 'OK')

      const get1 = await commander.execute(Buffer.from('hget'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
      ])
      assert.deepStrictEqual(get1.response, Buffer.from('value1'))

      const get2 = await commander.execute(Buffer.from('hget'), [
        Buffer.from('hash'),
        Buffer.from('field2'),
      ])
      assert.deepStrictEqual(get2.response, Buffer.from('value2'))

      await commander.shutdown()
      await factory.shutdown()
    })

    test('HKEYS command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('hset'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      const result = await commander.execute(Buffer.from('hkeys'), [
        Buffer.from('hash'),
      ])

      assert.ok(Array.isArray(result.response))
      const keys = result.response as Buffer[]
      assert.strictEqual(keys.length, 2)
      const keyStrings = keys.map(k => k.toString()).sort()
      assert.deepStrictEqual(keyStrings, ['field1', 'field2'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('HVALS command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      await commander.execute(Buffer.from('hset'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      const result = await commander.execute(Buffer.from('hvals'), [
        Buffer.from('hash'),
      ])

      assert.ok(Array.isArray(result.response))
      const values = result.response as Buffer[]
      assert.strictEqual(values.length, 2)
      const valueStrings = values.map(v => v.toString()).sort()
      assert.deepStrictEqual(valueStrings, ['value1', 'value2'])

      await commander.shutdown()
      await factory.shutdown()
    })

    test('HLEN command', async () => {
      const factory = await createCustomCommander(console)
      const commander = factory.createCommander()

      const len1 = await commander.execute(Buffer.from('hlen'), [
        Buffer.from('hash'),
      ])
      assert.strictEqual(len1.response, 0)

      await commander.execute(Buffer.from('hset'), [
        Buffer.from('hash'),
        Buffer.from('field1'),
        Buffer.from('value1'),
        Buffer.from('field2'),
        Buffer.from('value2'),
      ])

      const len2 = await commander.execute(Buffer.from('hlen'), [
        Buffer.from('hash'),
      ])
      assert.strictEqual(len2.response, 2)

      await commander.shutdown()
      await factory.shutdown()
    })
  })

  describe('Hash Error Handling', () => {
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

    test('Hash commands throw WrongNumberOfArguments with correct format', async () => {
      const db = new DB()
      const hgetCommand = new HgetCommand(db)

      try {
        await hgetCommand.run(Buffer.from('HGET'), [Buffer.from('key')])
        assert.fail('Should have thrown WrongNumberOfArguments for hget')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'hget' command",
        )
        assert.strictEqual(error.name, 'ERR')
      }
    })
  })
})
