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
import { FlushdbCommand } from '../../src/commanders/custom/commands/redis/data/keys/flushdb'
import { FlushallCommand } from '../../src/commanders/custom/commands/redis/data/keys/flushall'
import { DbSizeCommand } from '../../src/commanders/custom/commands/redis/data/keys/dbsize'

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

  describe('FLUSHDB command', () => {
    test('FLUSHDB on empty database', async () => {
      const db = new DB()
      const flushdbCommand = new FlushdbCommand(db)

      const result = await flushdbCommand.run(Buffer.from(''), [])
      assert.strictEqual(result.response, 'OK')
    })

    test('FLUSHDB removes all keys', async () => {
      const db = new DB()
      const flushdbCommand = new FlushdbCommand(db)

      // Add various data types
      db.set(
        Buffer.from('string_key'),
        new StringDataType(Buffer.from('value')),
      )
      db.set(Buffer.from('hash_key'), new HashDataType())
      db.set(Buffer.from('list_key'), new ListDataType())
      db.set(Buffer.from('set_key'), new SetDataType())

      // Verify keys exist
      assert.ok(db.get(Buffer.from('string_key')) !== null)
      assert.ok(db.get(Buffer.from('hash_key')) !== null)
      assert.ok(db.get(Buffer.from('list_key')) !== null)
      assert.ok(db.get(Buffer.from('set_key')) !== null)

      // Execute FLUSHDB
      const result = await flushdbCommand.run(Buffer.from(''), [])
      assert.strictEqual(result.response, 'OK')

      // Verify all keys are removed
      assert.strictEqual(db.get(Buffer.from('string_key')), null)
      assert.strictEqual(db.get(Buffer.from('hash_key')), null)
      assert.strictEqual(db.get(Buffer.from('list_key')), null)
      assert.strictEqual(db.get(Buffer.from('set_key')), null)
    })

    test('FLUSHDB removes keys with expiration', async () => {
      const db = new DB()
      const flushdbCommand = new FlushdbCommand(db)

      // Add keys with expiration
      const expirationTime = Date.now() + 10000
      db.set(
        Buffer.from('key1'),
        new StringDataType(Buffer.from('value')),
        expirationTime,
      )
      db.set(
        Buffer.from('key2'),
        new StringDataType(Buffer.from('value')),
        expirationTime,
      )

      // Verify keys exist
      assert.ok(db.get(Buffer.from('key1')) !== null)
      assert.ok(db.get(Buffer.from('key2')) !== null)

      // Execute FLUSHDB
      const result = await flushdbCommand.run(Buffer.from(''), [])
      assert.strictEqual(result.response, 'OK')

      // Verify all keys and their expiration data are removed
      assert.strictEqual(db.get(Buffer.from('key1')), null)
      assert.strictEqual(db.get(Buffer.from('key2')), null)
      assert.strictEqual(db.getTtl(Buffer.from('key1')), -2) // Key does not exist
      assert.strictEqual(db.getTtl(Buffer.from('key2')), -2) // Key does not exist
    })
  })

  describe('FLUSHALL command', () => {
    test('FLUSHALL on empty database', async () => {
      const db = new DB()
      const flushallCommand = new FlushallCommand(db)

      const result = await flushallCommand.run(Buffer.from(''), [])
      assert.strictEqual(result.response, 'OK')
    })

    test('FLUSHALL removes all keys', async () => {
      const db = new DB()
      const flushallCommand = new FlushallCommand(db)

      // Add various data types
      db.set(
        Buffer.from('string_key'),
        new StringDataType(Buffer.from('value')),
      )
      db.set(Buffer.from('hash_key'), new HashDataType())
      db.set(Buffer.from('list_key'), new ListDataType())
      db.set(Buffer.from('set_key'), new SetDataType())

      // Verify keys exist
      assert.ok(db.get(Buffer.from('string_key')) !== null)
      assert.ok(db.get(Buffer.from('hash_key')) !== null)
      assert.ok(db.get(Buffer.from('list_key')) !== null)
      assert.ok(db.get(Buffer.from('set_key')) !== null)

      // Execute FLUSHALL
      const result = await flushallCommand.run(Buffer.from(''), [])
      assert.strictEqual(result.response, 'OK')

      // Verify all keys are removed
      assert.strictEqual(db.get(Buffer.from('string_key')), null)
      assert.strictEqual(db.get(Buffer.from('hash_key')), null)
      assert.strictEqual(db.get(Buffer.from('list_key')), null)
      assert.strictEqual(db.get(Buffer.from('set_key')), null)
    })

    test('FLUSHALL removes keys with expiration', async () => {
      const db = new DB()
      const flushallCommand = new FlushallCommand(db)

      // Add keys with expiration
      const expirationTime = Date.now() + 10000
      db.set(
        Buffer.from('key1'),
        new StringDataType(Buffer.from('value')),
        expirationTime,
      )
      db.set(
        Buffer.from('key2'),
        new StringDataType(Buffer.from('value')),
        expirationTime,
      )

      // Verify keys exist
      assert.ok(db.get(Buffer.from('key1')) !== null)
      assert.ok(db.get(Buffer.from('key2')) !== null)

      // Execute FLUSHALL
      const result = await flushallCommand.run(Buffer.from(''), [])
      assert.strictEqual(result.response, 'OK')

      // Verify all keys and their expiration data are removed
      assert.strictEqual(db.get(Buffer.from('key1')), null)
      assert.strictEqual(db.get(Buffer.from('key2')), null)
      assert.strictEqual(db.getTtl(Buffer.from('key1')), -2) // Key does not exist
      assert.strictEqual(db.getTtl(Buffer.from('key2')), -2) // Key does not exist
    })

    test('FLUSHALL and FLUSHDB have same behavior in single-database implementation', async () => {
      const db1 = new DB()
      const db2 = new DB()
      const flushdbCommand = new FlushdbCommand(db1)
      const flushallCommand = new FlushallCommand(db2)

      // Add same data to both databases
      db1.set(Buffer.from('key1'), new StringDataType(Buffer.from('value')))
      db1.set(Buffer.from('key2'), new HashDataType())
      db2.set(Buffer.from('key1'), new StringDataType(Buffer.from('value')))
      db2.set(Buffer.from('key2'), new HashDataType())

      // Execute both commands
      const result1 = await flushdbCommand.run(Buffer.from(''), [])
      const result2 = await flushallCommand.run(Buffer.from(''), [])

      // Both should return OK
      assert.strictEqual(result1.response, 'OK')
      assert.strictEqual(result2.response, 'OK')

      // Both should clear all data
      assert.strictEqual(db1.get(Buffer.from('key1')), null)
      assert.strictEqual(db1.get(Buffer.from('key2')), null)
      assert.strictEqual(db2.get(Buffer.from('key1')), null)
      assert.strictEqual(db2.get(Buffer.from('key2')), null)
    })
  })

  describe('DBSIZE command', () => {
    test('should return 0 for empty database', async () => {
      const db = new DB()
      const dbsizeCommand = new DbSizeCommand(db)

      const result = await dbsizeCommand.run(Buffer.from('DBSIZE'), [])

      assert.strictEqual(result.response, 0)
    })

    test('should return correct count after adding keys', async () => {
      const db = new DB()
      const dbsizeCommand = new DbSizeCommand(db)

      // Add some keys
      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value1')))
      db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value2')))
      db.set(Buffer.from('key3'), new StringDataType(Buffer.from('value3')))

      const result = await dbsizeCommand.run(Buffer.from('DBSIZE'), [])

      assert.strictEqual(result.response, 3)
    })

    test('should exclude expired keys from count', async () => {
      const db = new DB()
      const dbsizeCommand = new DbSizeCommand(db)

      // Add keys with different expiration times
      const now = Date.now()
      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value1'))) // no expiration
      db.set(
        Buffer.from('key2'),
        new StringDataType(Buffer.from('value2')),
        now + 10000,
      ) // expires in future
      db.set(
        Buffer.from('key3'),
        new StringDataType(Buffer.from('value3')),
        now - 1000,
      ) // already expired

      const result = await dbsizeCommand.run(Buffer.from('DBSIZE'), [])

      assert.strictEqual(result.response, 2) // Only non-expired keys
    })

    test('should return 0 after flushing database', async () => {
      const db = new DB()
      const dbsizeCommand = new DbSizeCommand(db)

      // Add some keys
      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value1')))
      db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value2')))

      let result = await dbsizeCommand.run(Buffer.from('DBSIZE'), [])
      assert.strictEqual(result.response, 2)

      // Flush database
      db.flushdb()

      result = await dbsizeCommand.run(Buffer.from('DBSIZE'), [])
      assert.strictEqual(result.response, 0)
    })

    test('should throw error if arguments are provided', async () => {
      const db = new DB()
      const dbsizeCommand = new DbSizeCommand(db)

      await assert.rejects(
        async () => {
          await dbsizeCommand.run(Buffer.from('DBSIZE'), [Buffer.from('arg1')])
        },
        error => {
          return error instanceof WrongNumberOfArguments
        },
      )
    })

    test('should return empty array for getKeys when no arguments', () => {
      const db = new DB()
      const dbsizeCommand = new DbSizeCommand(db)

      const keys = dbsizeCommand.getKeys(Buffer.from('DBSIZE'), [])

      assert.deepStrictEqual(keys, [])
    })

    test('should throw error in getKeys if arguments are provided', () => {
      const db = new DB()
      const dbsizeCommand = new DbSizeCommand(db)

      assert.throws(
        () => {
          dbsizeCommand.getKeys(Buffer.from('DBSIZE'), [Buffer.from('arg1')])
        },
        error => {
          return error instanceof WrongNumberOfArguments
        },
      )
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
