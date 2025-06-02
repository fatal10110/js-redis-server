import { test, describe } from 'node:test'
import assert from 'node:assert'
import { MgetCommand } from '../../src/commanders/custom/commands/redis/data/mget'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import { WrongNumberOfArguments } from '../../src/core/errors'

describe('MGET command', () => {
  test('basic MGET operation', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    // Set up test data
    db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value1')))
    db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value2')))

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('key1'),
      Buffer.from('key2'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 2)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.ok(Buffer.isBuffer(result.response[1]))
    assert.strictEqual(result.response[0].toString(), 'value1')
    assert.strictEqual(result.response[1].toString(), 'value2')
  })

  test('MGET with non-existent keys returns null', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('nonexistent1'),
      Buffer.from('nonexistent2'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 2)
    assert.strictEqual(result.response[0], null)
    assert.strictEqual(result.response[1], null)
  })

  test('MGET with mixed existing and non-existing keys', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    // Set up one key
    db.set(Buffer.from('existing'), new StringDataType(Buffer.from('value')))

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('existing'),
      Buffer.from('nonexistent'),
      Buffer.from('existing'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 3)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.strictEqual(result.response[0].toString(), 'value')
    assert.strictEqual(result.response[1], null)
    assert.ok(Buffer.isBuffer(result.response[2]))
    assert.strictEqual(result.response[2].toString(), 'value')
  })

  test('MGET with non-string data types returns null', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    // Set up a non-string data type (we don't have other types in this implementation yet)
    // For now we'll simulate by directly setting non-StringDataType
    db.set(
      Buffer.from('string_key'),
      new StringDataType(Buffer.from('string_value')),
    )

    // Manually set a non-string type (this simulates having other data types)
    const mockNonStringData = { data: Buffer.from('not_string') }
    // @ts-expect-error - We're testing internal behavior
    db.data.set('non_string_key', mockNonStringData)

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('string_key'),
      Buffer.from('non_string_key'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 2)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.strictEqual(result.response[0].toString(), 'string_value')
    assert.strictEqual(result.response[1], null)
  })

  test('MGET with single key', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    db.set(Buffer.from('solo'), new StringDataType(Buffer.from('alone')))

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('solo'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 1)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.strictEqual(result.response[0].toString(), 'alone')
  })

  test('MGET with duplicate keys', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    db.set(Buffer.from('duplicate'), new StringDataType(Buffer.from('same')))

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('duplicate'),
      Buffer.from('duplicate'),
      Buffer.from('duplicate'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 3)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.ok(Buffer.isBuffer(result.response[1]))
    assert.ok(Buffer.isBuffer(result.response[2]))
    assert.strictEqual(result.response[0].toString(), 'same')
    assert.strictEqual(result.response[1].toString(), 'same')
    assert.strictEqual(result.response[2].toString(), 'same')
  })

  test('MGET with empty binary data', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    db.set(Buffer.from('empty'), new StringDataType(Buffer.alloc(0)))

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('empty'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 1)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.strictEqual(result.response[0].length, 0)
  })

  test('MGET with binary data containing null bytes', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    const binaryData = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x00, 0xff])
    db.set(Buffer.from('binary'), new StringDataType(binaryData))

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('binary'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 1)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.ok(result.response[0].equals(binaryData))
  })

  test('MGET preserves key order in response', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    // Set up keys in different order than we'll request them
    db.set(Buffer.from('z'), new StringDataType(Buffer.from('last')))
    db.set(Buffer.from('a'), new StringDataType(Buffer.from('first')))
    db.set(Buffer.from('m'), new StringDataType(Buffer.from('middle')))

    const result = await mgetCommand.run(Buffer.from('MGET'), [
      Buffer.from('a'),
      Buffer.from('nonexistent'),
      Buffer.from('m'),
      Buffer.from('z'),
    ])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 4)
    assert.strictEqual(result.response[0].toString(), 'first')
    assert.strictEqual(result.response[1], null)
    assert.strictEqual(result.response[2].toString(), 'middle')
    assert.strictEqual(result.response[3].toString(), 'last')
  })

  test('MGET with no arguments throws error', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    try {
      await mgetCommand.run(Buffer.from('MGET'), [])
      assert.fail('Expected WrongNumberOfArguments error')
    } catch (err) {
      assert.ok(err instanceof WrongNumberOfArguments)
    }
  })

  test('MGET with large number of keys', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    const numKeys = 1000
    const keys: Buffer[] = []

    // Set up keys
    for (let i = 0; i < numKeys; i++) {
      const key = Buffer.from(`key${i}`)
      const value = Buffer.from(`value${i}`)
      keys.push(key)

      // Only set every other key to test mixed existing/non-existing
      if (i % 2 === 0) {
        db.set(key, new StringDataType(value))
      }
    }

    const result = await mgetCommand.run(Buffer.from('MGET'), keys)

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, numKeys)

    // Verify the pattern: even indices should have values, odd should be null
    for (let i = 0; i < numKeys; i++) {
      if (i % 2 === 0) {
        assert.ok(Buffer.isBuffer(result.response[i]))
        assert.strictEqual(result.response[i].toString(), `value${i}`)
      } else {
        assert.strictEqual(result.response[i], null)
      }
    }
  })

  test('getKeys method returns all argument keys', () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    const keys = [Buffer.from('key1'), Buffer.from('key2'), Buffer.from('key3')]

    const result = mgetCommand.getKeys(Buffer.from('MGET'), keys)

    assert.ok(Array.isArray(result))
    assert.strictEqual(result.length, 3)
    assert.ok(result[0].equals(Buffer.from('key1')))
    assert.ok(result[1].equals(Buffer.from('key2')))
    assert.ok(result[2].equals(Buffer.from('key3')))
  })

  test('getKeys method with no arguments returns empty array', () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    const result = mgetCommand.getKeys(Buffer.from('MGET'), [])

    assert.ok(Array.isArray(result))
    assert.strictEqual(result.length, 0)
  })

  test('MGET with UTF-8 keys and values', async () => {
    const db = new DB()
    const mgetCommand = new MgetCommand(db)

    const unicodeKey = Buffer.from('ключ', 'utf8')
    const unicodeValue = Buffer.from('значение', 'utf8')
    db.set(unicodeKey, new StringDataType(unicodeValue))

    const result = await mgetCommand.run(Buffer.from('MGET'), [unicodeKey])

    assert.ok(Array.isArray(result.response))
    assert.strictEqual(result.response.length, 1)
    assert.ok(Buffer.isBuffer(result.response[0]))
    assert.strictEqual(result.response[0].toString('utf8'), 'значение')
  })
})
