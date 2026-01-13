import { test, describe } from 'node:test'
import assert from 'node:assert'
import { LuaFactory } from 'wasmoon'
import { DB } from '../src/commanders/custom/db'
import { CommandExecutionContext } from '../src/commanders/custom/execution-context'
import { createCommands } from '../src/commanders/custom/commands/redis'
import { MockTransport } from './mock-transport'

import { StringDataType } from '../src/commanders/custom/data-structures/string'

describe('Lua Script Atomicity', () => {
  test('script holds lock for entire execution when using execution context', async () => {
    const db = new DB()
    const factory = new LuaFactory()
    const lua = await factory.createEngine({ injectObjects: true })

    // Create execution context
    const transactionCommands = createCommands(lua, db)
    const context = new CommandExecutionContext(db, {}, transactionCommands)

    // Create commands with execution context reference
    const commands = createCommands(lua, db, context)

    // Update context with actual commands (including eval)
    const contextWithCommands = new CommandExecutionContext(
      db,
      commands,
      transactionCommands,
    )

    // Set initial value
    db.set(Buffer.from('mykey'), new StringDataType(Buffer.from('10')))

    // Track lock acquisitions
    const lockAcquisitions: string[] = []
    const originalAcquire = db.lock.acquire.bind(db.lock)
    db.lock.acquire = async () => {
      lockAcquisitions.push('acquire')
      return originalAcquire()
    }

    // Execute Lua script that calls redis.call twice
    const script = `
      local val = redis.call('GET', KEYS[1])
      redis.call('SET', KEYS[1], tonumber(val) + 1)
      return redis.call('GET', KEYS[1])
    `

    const transport = new MockTransport()

    await contextWithCommands.execute(
      transport,
      Buffer.from('eval'),
      [Buffer.from(script), Buffer.from('1'), Buffer.from('mykey')],
      new AbortController().signal,
    )

    // Should only acquire lock ONCE for entire script
    assert.strictEqual(
      lockAcquisitions.length,
      1,
      'Should only acquire lock once for entire Lua script',
    )

    // Verify result
    const result = transport.getLastResponse()
    assert.ok(result instanceof Buffer)
    assert.strictEqual(result.toString(), '11')

    lua.global.close()
  })

  test('nested redis.call commands do not re-acquire lock', async () => {
    const db = new DB()
    const factory = new LuaFactory()
    const lua = await factory.createEngine({ injectObjects: true })

    // Create execution context
    const transactionCommands = createCommands(lua, db)
    const context = new CommandExecutionContext(db, {}, transactionCommands)

    // Create commands with execution context reference
    const commands = createCommands(lua, db, { executionContext: context })

    // Update context with actual commands
    const contextWithCommands = new CommandExecutionContext(
      db,
      commands,
      transactionCommands,
    )

    // Set initial value
    db.set(Buffer.from('key1'), new StringDataType(Buffer.from('hello')))
    db.set(Buffer.from('key2'), new StringDataType(Buffer.from('world')))

    // Track lock acquire/release
    let lockHeld = false
    const originalAcquire = db.lock.acquire.bind(db.lock)
    db.lock.acquire = async () => {
      assert.strictEqual(
        lockHeld,
        false,
        'Lock should not be acquired while already held',
      )
      lockHeld = true
      const release = await originalAcquire()
      return () => {
        lockHeld = false
        release()
      }
    }

    // Execute Lua script with multiple redis.call
    const script = `
      local v1 = redis.call('GET', KEYS[1])
      local v2 = redis.call('GET', KEYS[2])
      redis.call('SET', KEYS[1], v2)
      redis.call('SET', KEYS[2], v1)
      return 'OK'
    `

    const transport = new MockTransport()

    await contextWithCommands.execute(
      transport,
      Buffer.from('eval'),
      [
        Buffer.from(script),
        Buffer.from('2'),
        Buffer.from('key1'),
        Buffer.from('key2'),
      ],
      new AbortController().signal,
    )

    // Verify values were swapped
    const val1 = db.get(Buffer.from('key1'))
    const val2 = db.get(Buffer.from('key2'))

    assert.ok(val1 instanceof StringDataType)
    assert.ok(val2 instanceof StringDataType)
    assert.strictEqual(val1.data.toString(), 'world')
    assert.strictEqual(val2.data.toString(), 'hello')

    lua.global.close()
  })

  test('legacy mode works without execution context', async () => {
    const db = new DB()
    const factory = new LuaFactory()
    const lua = await factory.createEngine({ injectObjects: true })

    // Create commands WITHOUT execution context (legacy mode)
    const commands = createCommands(lua, db)

    // Set initial value
    db.set(Buffer.from('counter'), new StringDataType(Buffer.from('5')))

    // Execute Lua script directly (old way)
    const evalCmd = commands['eval']
    const script = `
      local val = redis.call('GET', KEYS[1])
      return tonumber(val) * 2
    `

    const result = await evalCmd.run(
      Buffer.from('eval'),
      [Buffer.from(script), Buffer.from('1'), Buffer.from('counter')],
      new AbortController().signal,
    )

    assert.ok(result.response instanceof Buffer)
    assert.strictEqual(result.response.toString(), '10')

    lua.global.close()
  })
})
