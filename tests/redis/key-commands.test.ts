import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import { HashDataType } from '../../src/commanders/custom/data-structures/hash'
import { ListDataType } from '../../src/commanders/custom/data-structures/list'
import { SetDataType } from '../../src/commanders/custom/data-structures/set'

// Key commands
import { ExistsCommand } from '../../src/commanders/custom/commands/redis/data/exists'
import { TypeCommand } from '../../src/commanders/custom/commands/redis/data/type'

// Error imports
import { WrongNumberOfArguments } from '../../src/core/errors'

describe('Key Commands', () => {
  describe('EXISTS command', () => {
    test('EXISTS on non-existent keys', async () => {
      const db = new DB()
      const existsCommand = new ExistsCommand(db)

      const result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key1'),
        Buffer.from('key2'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('EXISTS on mixed keys', async () => {
      const db = new DB()
      const existsCommand = new ExistsCommand(db)

      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value')))
      const result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key1'),
        Buffer.from('key2'),
      ])
      assert.strictEqual(result.response, 1)
    })

    test('EXISTS on all existing keys', async () => {
      const db = new DB()
      const existsCommand = new ExistsCommand(db)

      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value')))
      db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value')))
      const result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key1'),
        Buffer.from('key2'),
      ])
      assert.strictEqual(result.response, 2)
    })

    test('EXISTS with single key', async () => {
      const db = new DB()
      const existsCommand = new ExistsCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))
      const result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, 1)
    })

    test('EXISTS with duplicate keys', async () => {
      const db = new DB()
      const existsCommand = new ExistsCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))
      const result = await existsCommand.run(Buffer.from('EXISTS'), [
        Buffer.from('key'),
        Buffer.from('key'),
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, 3)
    })
  })

  describe('TYPE command', () => {
    test('TYPE on non-existent key', async () => {
      const db = new DB()
      const typeCommand = new TypeCommand(db)

      const result = await typeCommand.run(Buffer.from('TYPE'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, 'none')
    })

    test('TYPE on string', async () => {
      const db = new DB()
      const typeCommand = new TypeCommand(db)

      db.set(Buffer.from('str'), new StringDataType(Buffer.from('value')))
      const result = await typeCommand.run(Buffer.from('TYPE'), [
        Buffer.from('str'),
      ])
      assert.strictEqual(result.response, 'string')
    })

    test('TYPE on hash', async () => {
      const db = new DB()
      const typeCommand = new TypeCommand(db)

      db.set(Buffer.from('hash'), new HashDataType())
      const result = await typeCommand.run(Buffer.from('TYPE'), [
        Buffer.from('hash'),
      ])
      assert.strictEqual(result.response, 'hash')
    })

    test('TYPE on list', async () => {
      const db = new DB()
      const typeCommand = new TypeCommand(db)

      db.set(Buffer.from('list'), new ListDataType())
      const result = await typeCommand.run(Buffer.from('TYPE'), [
        Buffer.from('list'),
      ])
      assert.strictEqual(result.response, 'list')
    })

    test('TYPE on set', async () => {
      const db = new DB()
      const typeCommand = new TypeCommand(db)

      db.set(Buffer.from('set'), new SetDataType())
      const result = await typeCommand.run(Buffer.from('TYPE'), [
        Buffer.from('set'),
      ])
      assert.strictEqual(result.response, 'set')
    })
  })

  describe('Key Commands Error Handling', () => {
    test('Key commands throw WrongNumberOfArguments with correct format', async () => {
      const db = new DB()
      const existsCommand = new ExistsCommand(db)
      const typeCommand = new TypeCommand(db)

      // Test EXISTS with no arguments
      try {
        await existsCommand.run(Buffer.from('EXISTS'), [])
        assert.fail('Should have thrown WrongNumberOfArguments for exists')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'exists' command",
        )
        assert.strictEqual(error.name, 'ERR')
      }

      // Test TYPE with no arguments
      try {
        await typeCommand.run(Buffer.from('TYPE'), [])
        assert.fail('Should have thrown WrongNumberOfArguments for type')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'type' command",
        )
        assert.strictEqual(error.name, 'ERR')
      }

      // Test TYPE with too many arguments
      try {
        await typeCommand.run(Buffer.from('TYPE'), [
          Buffer.from('key1'),
          Buffer.from('key2'),
        ])
        assert.fail('Should have thrown WrongNumberOfArguments for type')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'type' command",
        )
        assert.strictEqual(error.name, 'ERR')
      }
    })
  })
})
