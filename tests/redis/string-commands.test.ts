import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { StringDataType } from '../../src/commanders/custom/data-structures/string'
import { ListDataType } from '../../src/commanders/custom/data-structures/list'
import { Buffer } from 'buffer'

// String commands
import { IncrCommand } from '../../src/commanders/custom/commands/redis/data/strings/incr'
import { DecrCommand } from '../../src/commanders/custom/commands/redis/data/strings/decr'
import { AppendCommand } from '../../src/commanders/custom/commands/redis/data/strings/append'
import { StrlenCommand } from '../../src/commanders/custom/commands/redis/data/strings/strlen'
import { MgetCommand } from '../../src/commanders/custom/commands/redis/data/strings/mget'
import { SetCommand } from '../../src/commanders/custom/commands/redis/data/strings/set'

// Error imports
import {
  WrongNumberOfArguments,
  WrongType,
  ExpectedInteger,
  RedisSyntaxError,
} from '../../src/core/errors'
import { createMockTransport } from '../mock-transport'
import { runCommand, createTestSession } from '../command-test-utils'

describe('String Commands', () => {
  describe('INCR command', () => {
    test('increment on non-existent key', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      const result = runCommand(incrCommand, 'INCR', [Buffer.from('counter')])
      assert.strictEqual(result.response, 1)
    })

    test('increment on existing key', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      runCommand(incrCommand, 'INCR', [Buffer.from('counter')])
      const result = runCommand(incrCommand, 'INCR', [Buffer.from('counter')])
      assert.strictEqual(result.response, 2)
    })

    test('increment on existing string value', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      db.set(Buffer.from('num'), new StringDataType(Buffer.from('10')))
      const result = runCommand(incrCommand, 'INCR', [Buffer.from('num')])
      assert.strictEqual(result.response, 11)
    })

    test('increment with non-numeric value throws error', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('notanumber')))

      try {
        runCommand(incrCommand, 'INCR', [Buffer.from('key')])
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

    test('increment on wrong data type throws error', async () => {
      const db = new DB()
      const incrCommand = new IncrCommand(db)

      db.set(Buffer.from('key'), new ListDataType())

      try {
        runCommand(incrCommand, 'INCR', [Buffer.from('key')])
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
  })

  describe('DECR command', () => {
    test('decrement on non-existent key', async () => {
      const db = new DB()
      const decrCommand = new DecrCommand(db)

      const result = runCommand(decrCommand, 'DECR', [Buffer.from('counter')])
      assert.strictEqual(result.response, -1)
    })

    test('decrement on existing key', async () => {
      const db = new DB()
      const decrCommand = new DecrCommand(db)

      runCommand(decrCommand, 'DECR', [Buffer.from('counter')])
      const result = runCommand(decrCommand, 'DECR', [Buffer.from('counter')])
      assert.strictEqual(result.response, -2)
    })
  })

  describe('APPEND command', () => {
    test('append to non-existent key', async () => {
      const db = new DB()
      const appendCommand = new AppendCommand(db)

      const result = runCommand(appendCommand, 'APPEND', [
        Buffer.from('key'),
        Buffer.from('hello'),
      ])
      assert.strictEqual(result.response, 5)
    })

    test('append to existing key', async () => {
      const db = new DB()
      const appendCommand = new AppendCommand(db)

      runCommand(appendCommand, 'APPEND', [
        Buffer.from('key'),
        Buffer.from('hello'),
      ])
      const result = runCommand(appendCommand, 'APPEND', [
        Buffer.from('key'),
        Buffer.from(' world'),
      ])
      assert.strictEqual(result.response, 11)

      const value = db.get(Buffer.from('key')) as StringDataType
      assert.strictEqual(value.data.toString(), 'hello world')
    })
  })

  describe('STRLEN command', () => {
    test('strlen on non-existent key', async () => {
      const db = new DB()
      const strlenCommand = new StrlenCommand(db)

      const result = runCommand(strlenCommand, 'STRLEN', [Buffer.from('key')])
      assert.strictEqual(result.response, 0)
    })

    test('strlen on existing key', async () => {
      const db = new DB()
      const strlenCommand = new StrlenCommand(db)

      db.set(Buffer.from('key'), new StringDataType(Buffer.from('hello')))
      const result = runCommand(strlenCommand, 'STRLEN', [Buffer.from('key')])
      assert.strictEqual(result.response, 5)
    })
  })

  describe('MGET command', () => {
    test('basic MGET operation', async () => {
      const db = new DB()
      const mgetCommand = new MgetCommand(db)

      db.set(Buffer.from('key1'), new StringDataType(Buffer.from('value1')))
      db.set(Buffer.from('key2'), new StringDataType(Buffer.from('value2')))

      const result = runCommand(mgetCommand, 'MGET', [
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

      const result = runCommand(mgetCommand, 'MGET', [
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

      db.set(Buffer.from('existing'), new StringDataType(Buffer.from('value')))

      const result = runCommand(mgetCommand, 'MGET', [
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

    test('MGET preserves key order in response', async () => {
      const db = new DB()
      const mgetCommand = new MgetCommand(db)

      db.set(Buffer.from('z'), new StringDataType(Buffer.from('last')))
      db.set(Buffer.from('a'), new StringDataType(Buffer.from('first')))
      db.set(Buffer.from('m'), new StringDataType(Buffer.from('middle')))

      const result = runCommand(mgetCommand, 'MGET', [
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
        runCommand(mgetCommand, 'MGET', [])
        assert.fail('Expected WrongNumberOfArguments error')
      } catch (err) {
        assert.ok(err instanceof WrongNumberOfArguments)
      }
    })
  })

  describe('SET command', () => {
    test('basic SET operation', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      const result = runCommand(setCommand, 'SET', [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
      ])

      assert.strictEqual(result.response, 'OK')

      const storedData = db.get(Buffer.from('mykey'))
      assert.ok(storedData instanceof StringDataType)
      assert.strictEqual(storedData.data.toString(), 'myvalue')
    })

    test('SET with EX option', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      const result = runCommand(setCommand, 'SET', [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('EX'),
        Buffer.from('10'),
      ])

      assert.strictEqual(result.response, 'OK')

      const storedData = db.get(Buffer.from('mykey'))
      assert.ok(storedData instanceof StringDataType)
      assert.strictEqual(storedData.data.toString(), 'myvalue')
    })

    test('SET with PX option', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      const result = runCommand(setCommand, 'SET', [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('PX'),
        Buffer.from('5000'),
      ])

      assert.strictEqual(result.response, 'OK')

      const storedData = db.get(Buffer.from('mykey'))
      assert.ok(storedData instanceof StringDataType)
      assert.strictEqual(storedData.data.toString(), 'myvalue')
    })

    test('SET with NX option - key does not exist', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      const result = runCommand(setCommand, 'SET', [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('NX'),
      ])

      assert.strictEqual(result.response, 'OK')

      const storedData = db.get(Buffer.from('mykey'))
      assert.ok(storedData instanceof StringDataType)
      assert.strictEqual(storedData.data.toString(), 'myvalue')
    })

    test('SET with NX option - key exists', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      db.set(Buffer.from('mykey'), new StringDataType(Buffer.from('existing')))

      const result = runCommand(setCommand, 'SET', [
        Buffer.from('mykey'),
        Buffer.from('myvalue'),
        Buffer.from('NX'),
      ])

      assert.strictEqual(result.response, null)

      const storedData = db.get(Buffer.from('mykey'))
      assert.ok(storedData instanceof StringDataType)
      assert.strictEqual(storedData.data.toString(), 'existing')
    })

    test('SET with GET option - key exists', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      db.set(Buffer.from('mykey'), new StringDataType(Buffer.from('oldvalue')))

      const result = runCommand(setCommand, 'SET', [
        Buffer.from('mykey'),
        Buffer.from('newvalue'),
        Buffer.from('GET'),
      ])

      assert.ok(Buffer.isBuffer(result.response))
      assert.strictEqual(result.response.toString(), 'oldvalue')

      const storedData = db.get(Buffer.from('mykey'))
      assert.ok(storedData instanceof StringDataType)
      assert.strictEqual(storedData.data.toString(), 'newvalue')
    })

    test('SET wrong number of arguments', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      try {
        runCommand(setCommand, 'SET', [Buffer.from('mykey')])
        assert.fail('Expected WrongNumberOfArguments error')
      } catch (err) {
        assert.ok(err instanceof WrongNumberOfArguments)
      }
    })

    test('SET invalid syntax errors', async () => {
      const db = new DB()
      const setCommand = new SetCommand(db)

      // EX without value
      try {
        runCommand(setCommand, 'SET', [
          Buffer.from('mykey'),
          Buffer.from('myvalue'),
          Buffer.from('EX'),
        ])
        assert.fail('Expected WrongNumberOfArguments')
      } catch (err) {
        assert.ok(err instanceof WrongNumberOfArguments)
      }

      // NX and XX together
      try {
        runCommand(setCommand, 'SET', [
          Buffer.from('mykey'),
          Buffer.from('myvalue'),
          Buffer.from('NX'),
          Buffer.from('XX'),
        ])
        assert.fail('Expected RedisSyntaxError')
      } catch (err) {
        assert.ok(err instanceof RedisSyntaxError)
      }
    })
  })

  describe('New String Commands (with commander)', () => {
    test('INCR and DECR commands', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('incr'),
        [Buffer.from('counter')],
        new AbortController().signal,
      )

      assert.strictEqual(transport.getLastResponse(), 1)

      await session.execute(
        transport,
        Buffer.from('incr'),
        [Buffer.from('counter')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)

      await session.execute(
        transport,
        Buffer.from('decr'),
        [Buffer.from('counter')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)
    })

    test('APPEND command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()
      await session.execute(
        transport,
        Buffer.from('append'),
        [Buffer.from('mykey'), Buffer.from('hello')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 5)

      await session.execute(
        transport,
        Buffer.from('append'),
        [Buffer.from('mykey'), Buffer.from(' world')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 11)

      await session.execute(
        transport,
        Buffer.from('get'),
        [Buffer.from('mykey')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(
        transport.getLastResponse(),
        Buffer.from('hello world'),
      )
    })

    test('STRLEN command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('strlen'),
        [Buffer.from('nonexistent')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)

      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('mykey'), Buffer.from('hello')],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('strlen'),
        [Buffer.from('mykey')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 5)
    })

    test('MGET command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('key1'), Buffer.from('value1')],
        new AbortController().signal,
      )
      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('key2'), Buffer.from('value2')],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('mget'),
        [Buffer.from('key1'), Buffer.from('key2'), Buffer.from('nonexistent')],
        new AbortController().signal,
      )

      assert.ok(Array.isArray(transport.getLastResponse()))
      const values = transport.getLastResponse() as (Buffer | null)[]
      assert.strictEqual(values.length, 3)
      assert.deepStrictEqual(values[0], Buffer.from('value1'))
      assert.deepStrictEqual(values[1], Buffer.from('value2'))
      assert.strictEqual(values[2], null)
    })

    test('SET command with options', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()
      // Basic SET
      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('mykey'), Buffer.from('myvalue')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'OK')

      // SET with NX (should fail since key exists)
      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('mykey'), Buffer.from('newvalue'), Buffer.from('NX')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), null)

      // SET with XX (should succeed since key exists)
      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('mykey'), Buffer.from('newvalue'), Buffer.from('XX')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'OK')

      // SET with GET option
      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('mykey'), Buffer.from('finalvalue'), Buffer.from('GET')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(
        transport.getLastResponse(),
        Buffer.from('newvalue'),
      )
    })

    test('MSET command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('mset'),
        [
          Buffer.from('key1'),
          Buffer.from('value1'),
          Buffer.from('key2'),
          Buffer.from('value2'),
          Buffer.from('key3'),
          Buffer.from('value3'),
        ],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'OK')

      await session.execute(
        transport,
        Buffer.from('get'),
        [Buffer.from('key1')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(transport.getLastResponse(), Buffer.from('value1'))

      await session.execute(
        transport,
        Buffer.from('get'),
        [Buffer.from('key2')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(transport.getLastResponse(), Buffer.from('value2'))
    })

    test('MSETNX command - all keys new', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('msetnx'),
        [
          Buffer.from('newkey1'),
          Buffer.from('value1'),
          Buffer.from('newkey2'),
          Buffer.from('value2'),
        ],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 1)
    })

    test('MSETNX command - some keys exist', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('existingkey'), Buffer.from('value')],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('msetnx'),
        [
          Buffer.from('existingkey'),
          Buffer.from('newvalue'),
          Buffer.from('newkey'),
          Buffer.from('value2'),
        ],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 0)
    })

    test('GETSET command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('set'),
        [Buffer.from('key'), Buffer.from('oldvalue')],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('getset'),
        [Buffer.from('key'), Buffer.from('newvalue')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(
        transport.getLastResponse(),
        Buffer.from('oldvalue'),
      )

      await session.execute(
        transport,
        Buffer.from('get'),
        [Buffer.from('key')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(
        transport.getLastResponse(),
        Buffer.from('newvalue'),
      )
    })

    test('INCRBY and DECRBY commands', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('incrby'),
        [Buffer.from('counter'), Buffer.from('5')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 5)

      await session.execute(
        transport,
        Buffer.from('incrby'),
        [Buffer.from('counter'), Buffer.from('3')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 8)

      await session.execute(
        transport,
        Buffer.from('decrby'),
        [Buffer.from('counter'), Buffer.from('2')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 6)
    })

    test('INCRBYFLOAT command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('incrbyfloat'),
        [Buffer.from('float'), Buffer.from('1.5')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(transport.getLastResponse(), Buffer.from('1.5'))

      await session.execute(
        transport,
        Buffer.from('incrbyfloat'),
        [Buffer.from('float'), Buffer.from('2.3')],
        new AbortController().signal,
      )
      assert.deepStrictEqual(transport.getLastResponse(), Buffer.from('3.8'))
    })
  })
})
