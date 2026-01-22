import { test, describe } from 'node:test'
import assert from 'node:assert'
import { DB } from '../../src/commanders/custom/db'
import { createMockTransport } from '../mock-transport'

// List commands
import { LpushCommand } from '../../src/commanders/custom/commands/redis/data/lists/lpush'
import { RpushCommand } from '../../src/commanders/custom/commands/redis/data/lists/rpush'
import { LpopCommand } from '../../src/commanders/custom/commands/redis/data/lists/lpop'
import { RpopCommand } from '../../src/commanders/custom/commands/redis/data/lists/rpop'
import { LlenCommand } from '../../src/commanders/custom/commands/redis/data/lists/llen'
import { LrangeCommand } from '../../src/commanders/custom/commands/redis/data/lists/lrange'

// Error imports
import { WrongNumberOfArguments, ExpectedInteger } from '../../src/core/errors'
import { runCommand, createTestSession } from '../command-test-utils'

describe('List Commands', () => {
  describe('LPUSH and RPUSH commands', () => {
    test('LPUSH on new list', async () => {
      const db = new DB()
      const lpushCommand = new LpushCommand(db)
      const rpushCommand = new RpushCommand(db)

      const result = runCommand(lpushCommand, 'LPUSH', [
        Buffer.from('list'),
        Buffer.from('item1'),
      ])
      assert.strictEqual(result.response, 1)

      const result2 = runCommand(rpushCommand, 'RPUSH', [
        Buffer.from('list'),
        Buffer.from('item2'),
      ])
      assert.strictEqual(result2.response, 2)
    })

    test('LPUSH multiple items', async () => {
      const db = new DB()
      const lpushCommand = new LpushCommand(db)

      const result = runCommand(lpushCommand, 'LPUSH', [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
        Buffer.from('item3'),
      ])
      assert.strictEqual(result.response, 3)
    })

    test('RPUSH multiple items', async () => {
      const db = new DB()
      const rpushCommand = new RpushCommand(db)

      const result = runCommand(rpushCommand, 'RPUSH', [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
        Buffer.from('item3'),
      ])
      assert.strictEqual(result.response, 3)
    })
  })

  describe('LPOP and RPOP commands', () => {
    test('LPOP and RPOP on non-existent list', async () => {
      const db = new DB()
      const lpopCommand = new LpopCommand(db)
      const rpopCommand = new RpopCommand(db)

      const result1 = runCommand(lpopCommand, 'LPOP', [Buffer.from('list')])
      assert.strictEqual(result1.response, null)

      const result2 = runCommand(rpopCommand, 'RPOP', [Buffer.from('list')])
      assert.strictEqual(result2.response, null)
    })

    test('LPOP and RPOP on existing list', async () => {
      const db = new DB()
      const lpushCommand = new LpushCommand(db)
      const lpopCommand = new LpopCommand(db)
      const rpopCommand = new RpopCommand(db)

      runCommand(lpushCommand, 'LPUSH', [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
      ])

      const result1 = runCommand(lpopCommand, 'LPOP', [Buffer.from('list')])
      assert.strictEqual(result1.response, 'item2')

      const result2 = runCommand(rpopCommand, 'RPOP', [Buffer.from('list')])
      assert.strictEqual(result2.response, 'item1')

      // List should be empty and removed from DB
      assert.strictEqual(db.get(Buffer.from('list')), null)
    })
  })

  describe('LLEN command', () => {
    test('LLEN on non-existent list', async () => {
      const db = new DB()
      const llenCommand = new LlenCommand(db)

      const result = runCommand(llenCommand, 'LLEN', [Buffer.from('list')])
      assert.strictEqual(result.response, 0)
    })

    test('LLEN on existing list', async () => {
      const db = new DB()
      const llenCommand = new LlenCommand(db)
      const lpushCommand = new LpushCommand(db)

      runCommand(lpushCommand, 'LPUSH', [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
      ])
      const result = runCommand(llenCommand, 'LLEN', [Buffer.from('list')])
      assert.strictEqual(result.response, 2)
    })
  })

  describe('LRANGE command', () => {
    test('LRANGE on non-existent list', async () => {
      const db = new DB()
      const lrangeCommand = new LrangeCommand(db)

      const result = runCommand(lrangeCommand, 'LRANGE', [
        Buffer.from('list'),
        Buffer.from('0'),
        Buffer.from('-1'),
      ])
      assert.deepStrictEqual(result.response, [])
    })

    test('LRANGE on existing list', async () => {
      const db = new DB()
      const lrangeCommand = new LrangeCommand(db)
      const lpushCommand = new LpushCommand(db)

      runCommand(lpushCommand, 'LPUSH', [
        Buffer.from('list'),
        Buffer.from('item1'),
        Buffer.from('item2'),
        Buffer.from('item3'),
      ])

      const result = runCommand(lrangeCommand, 'LRANGE', [
        Buffer.from('list'),
        Buffer.from('0'),
        Buffer.from('1'),
      ])
      assert.ok(Array.isArray(result.response))
      assert.strictEqual((result.response as Buffer[]).length, 2)
    })

    test('LRANGE with non-integer arguments throws error', async () => {
      const db = new DB()
      const lrangeCommand = new LrangeCommand(db)
      const lpushCommand = new LpushCommand(db)

      runCommand(lpushCommand, 'LPUSH', [
        Buffer.from('list'),
        Buffer.from('item1'),
      ])

      try {
        runCommand(lrangeCommand, 'LRANGE', [
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
  })

  describe('New List Commands (with commander)', () => {
    test('LINDEX command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('lpush'),
        [
          Buffer.from('list'),
          Buffer.from('item1'),
          Buffer.from('item2'),
          Buffer.from('item3'),
        ],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('lindex'),
        [Buffer.from('list'), Buffer.from('0')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'item3')

      await session.execute(
        transport,
        Buffer.from('lindex'),
        [Buffer.from('list'), Buffer.from('-1')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'item1')

      await session.execute(
        transport,
        Buffer.from('lindex'),
        [Buffer.from('list'), Buffer.from('10')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), null)
    })

    test('LSET command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('lpush'),
        [
          Buffer.from('list'),
          Buffer.from('item1'),
          Buffer.from('item2'),
          Buffer.from('item3'),
        ],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('lset'),
        [Buffer.from('list'), Buffer.from('1'), Buffer.from('newitem')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'OK')

      await session.execute(
        transport,
        Buffer.from('lindex'),
        [Buffer.from('list'), Buffer.from('1')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'newitem')
    })

    test('LREM command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('lpush'),
        [
          Buffer.from('list'),
          Buffer.from('item1'),
          Buffer.from('item2'),
          Buffer.from('item1'),
          Buffer.from('item3'),
        ],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('lrem'),
        [Buffer.from('list'), Buffer.from('2'), Buffer.from('item1')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)

      await session.execute(
        transport,
        Buffer.from('llen'),
        [Buffer.from('list')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 2)
    })

    test('LTRIM command', async () => {
      const db = new DB()
      const session = createTestSession(db)
      const transport = createMockTransport()

      await session.execute(
        transport,
        Buffer.from('lpush'),
        [
          Buffer.from('list'),
          Buffer.from('item1'),
          Buffer.from('item2'),
          Buffer.from('item3'),
          Buffer.from('item4'),
          Buffer.from('item5'),
        ],
        new AbortController().signal,
      )

      await session.execute(
        transport,
        Buffer.from('ltrim'),
        [Buffer.from('list'), Buffer.from('1'), Buffer.from('3')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 'OK')

      await session.execute(
        transport,
        Buffer.from('llen'),
        [Buffer.from('list')],
        new AbortController().signal,
      )
      assert.strictEqual(transport.getLastResponse(), 3)
    })
  })

  describe('List Error Handling', () => {
    test('List commands throw WrongNumberOfArguments with correct format', async () => {
      const db = new DB()
      const llenCommand = new LlenCommand(db)

      try {
        runCommand(llenCommand, 'LLEN', [])
        assert.fail('Should have thrown WrongNumberOfArguments for llen')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
        assert.strictEqual(
          error.message,
          "wrong number of arguments for 'llen' command",
        )
        assert.strictEqual(error.name, 'ERR')
      }
    })
  })
})
