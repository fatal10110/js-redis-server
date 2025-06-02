import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import { HashDataType } from '../../src/commanders/custom/data-structures/hash'
import { ListDataType } from '../../src/commanders/custom/data-structures/list'
import { SetDataType } from '../../src/commanders/custom/data-structures/set'

// Key commands
import { ExistsCommand } from '../../src/commanders/custom/commands/redis/data/keys/exists'
import { TypeCommand } from '../../src/commanders/custom/commands/redis/data/keys/type'
import { TtlCommand } from '../../src/commanders/custom/commands/redis/data/keys/ttl'
import { ExpireCommand } from '../../src/commanders/custom/commands/redis/data/keys/expire'
import { ExpireatCommand } from '../../src/commanders/custom/commands/redis/data/keys/expireat'

// Error imports
import {
  WrongNumberOfArguments,
  ExpectedInteger,
  InvalidExpireTime,
} from '../../src/core/errors'

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

  describe('TTL command', () => {
    test('TTL on non-existent key', async () => {
      const db = new DB()
      const ttlCommand = new TtlCommand(db)

      const result = await ttlCommand.run(Buffer.from('TTL'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, -2)
    })

    test('TTL on key without expiration', async () => {
      const db = new DB()
      const ttlCommand = new TtlCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))
      const result = await ttlCommand.run(Buffer.from('TTL'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, -1)
    })

    test('TTL on key with expiration', async () => {
      const db = new DB()
      const ttlCommand = new TtlCommand(db)

      const expirationTime = Date.now() + 10000 // 10 seconds from now
      db.set(
        Buffer.from('key'),
        new StringDataType(Buffer.from('value')),
        expirationTime,
      )
      const result = await ttlCommand.run(Buffer.from('TTL'), [
        Buffer.from('key'),
      ])
      assert.strictEqual(result.response, 10)
    })
  })

  describe('EXPIRE command', () => {
    test('EXPIRE on existing key', async () => {
      const db = new DB()
      const expireCommand = new ExpireCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))
      const result = await expireCommand.run(Buffer.from('EXPIRE'), [
        Buffer.from('key'),
        Buffer.from('10'),
      ])
      assert.strictEqual(result.response, 1)
    })

    test('EXPIRE on non-existent key', async () => {
      const db = new DB()
      const expireCommand = new ExpireCommand(db)

      const result = await expireCommand.run(Buffer.from('EXPIRE'), [
        Buffer.from('nonexistent'),
        Buffer.from('10'),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('EXPIRE with 0 seconds deletes key', async () => {
      const db = new DB()
      const expireCommand = new ExpireCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))
      const result = await expireCommand.run(Buffer.from('EXPIRE'), [
        Buffer.from('key'),
        Buffer.from('0'),
      ])
      assert.strictEqual(result.response, 1)

      // Key should be deleted
      const existing = db.get(Buffer.from('key'))
      assert.strictEqual(existing, null)
    })

    test('EXPIRE with negative seconds throws error', async () => {
      const db = new DB()
      const expireCommand = new ExpireCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))

      try {
        await expireCommand.run(Buffer.from('EXPIRE'), [
          Buffer.from('key'),
          Buffer.from('-1'),
        ])
        assert.fail('Should have thrown InvalidExpireTime error')
      } catch (error) {
        assert.ok(error instanceof InvalidExpireTime)
        assert.strictEqual(
          error.message,
          'invalid expire time in expire command',
        )
      }
    })

    test('EXPIRE with non-integer throws error', async () => {
      const db = new DB()
      const expireCommand = new ExpireCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))

      try {
        await expireCommand.run(Buffer.from('EXPIRE'), [
          Buffer.from('key'),
          Buffer.from('abc'),
        ])
        assert.fail('Should have thrown ExpectedInteger error')
      } catch (error) {
        assert.ok(error instanceof ExpectedInteger)
        assert.strictEqual(
          error.message,
          'value is not an integer or out of range',
        )
      }
    })

    test('EXPIRE with wrong number of arguments throws error', async () => {
      const db = new DB()
      const expireCommand = new ExpireCommand(db)

      try {
        await expireCommand.run(Buffer.from('EXPIRE'), [Buffer.from('key')])
        assert.fail('Should have thrown WrongNumberOfArguments error')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'expire' command",
        )
      }
    })
  })

  describe('EXPIREAT command', () => {
    test('EXPIREAT on existing key', async () => {
      const db = new DB()
      const expireatCommand = new ExpireatCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))
      const futureTimestamp = Math.floor(Date.now() / 1000) + 10
      const result = await expireatCommand.run(Buffer.from('EXPIREAT'), [
        Buffer.from('key'),
        Buffer.from(futureTimestamp.toString()),
      ])
      assert.strictEqual(result.response, 1)
    })

    test('EXPIREAT on non-existent key', async () => {
      const db = new DB()
      const expireatCommand = new ExpireatCommand(db)

      const futureTimestamp = Math.floor(Date.now() / 1000) + 10
      const result = await expireatCommand.run(Buffer.from('EXPIREAT'), [
        Buffer.from('nonexistent'),
        Buffer.from(futureTimestamp.toString()),
      ])
      assert.strictEqual(result.response, 0)
    })

    test('EXPIREAT with past timestamp deletes key', async () => {
      const db = new DB()
      const expireatCommand = new ExpireatCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))
      const pastTimestamp = Math.floor(Date.now() / 1000) - 1
      const result = await expireatCommand.run(Buffer.from('EXPIREAT'), [
        Buffer.from('key'),
        Buffer.from(pastTimestamp.toString()),
      ])
      assert.strictEqual(result.response, 1)

      // Key should be deleted
      const existing = db.get(Buffer.from('key'))
      assert.strictEqual(existing, null)
    })

    test('EXPIREAT with negative timestamp throws error', async () => {
      const db = new DB()
      const expireatCommand = new ExpireatCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))

      try {
        await expireatCommand.run(Buffer.from('EXPIREAT'), [
          Buffer.from('key'),
          Buffer.from('-1'),
        ])
        assert.fail('Should have thrown InvalidExpireTime error')
      } catch (error) {
        assert.ok(error instanceof InvalidExpireTime)
        assert.strictEqual(
          error.message,
          'invalid expire time in expireat command',
        )
      }
    })

    test('EXPIREAT with non-integer throws error', async () => {
      const db = new DB()
      const expireatCommand = new ExpireatCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('value')))

      try {
        await expireatCommand.run(Buffer.from('EXPIREAT'), [
          Buffer.from('key'),
          Buffer.from('abc'),
        ])
        assert.fail('Should have thrown ExpectedInteger error')
      } catch (error) {
        assert.ok(error instanceof ExpectedInteger)
        assert.strictEqual(
          error.message,
          'value is not an integer or out of range',
        )
      }
    })

    test('EXPIREAT with wrong number of arguments throws error', async () => {
      const db = new DB()
      const expireatCommand = new ExpireatCommand(db)

      try {
        await expireatCommand.run(Buffer.from('EXPIREAT'), [Buffer.from('key')])
        assert.fail('Should have thrown WrongNumberOfArguments error')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'expireat' command",
        )
      }
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
