import { test, describe } from 'node:test'
import assert from 'node:assert'
import { ScriptLoadCommand } from '../../src/commanders/custom/commands/redis/script/load'
import { ScriptExistsCommand } from '../../src/commanders/custom/commands/redis/script/exists'
import { ScriptFlushCommand } from '../../src/commanders/custom/commands/redis/script/flush'
import { ScriptKillCommand } from '../../src/commanders/custom/commands/redis/script/kill'
import { ScriptDebugCommand } from '../../src/commanders/custom/commands/redis/script/debug'
import { ScriptHelpCommand } from '../../src/commanders/custom/commands/redis/script/help'
import { WrongNumberOfArguments } from '../../src/core/errors'
import { DB } from '../../src/commanders/custom/db'

describe('Script Commands', () => {
  describe('SCRIPT LOAD command', () => {
    test('loads a script and returns SHA1 hash', async () => {
      const db = new DB()
      const command = new ScriptLoadCommand(db)

      const script = Buffer.from('return "hello"')
      const result = await command.run(Buffer.from('SCRIPT'), [script])

      assert.strictEqual(typeof result.response, 'string')
      assert.strictEqual((result.response as string).length, 40) // SHA1 is 40 chars

      // Verify script was stored
      assert.ok(db.getScript(result.response as string))
    })

    test('throws error when no script provided', async () => {
      const db = new DB()
      const command = new ScriptLoadCommand(db)

      try {
        await command.run(Buffer.from('SCRIPT'), [])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })

    test('does not duplicate scripts with same content', async () => {
      const db = new DB()
      const command = new ScriptLoadCommand(db)

      const script = Buffer.from('return "hello"')
      const result1 = await command.run(Buffer.from('SCRIPT'), [script])
      const result2 = await command.run(Buffer.from('SCRIPT'), [script])

      assert.strictEqual(result1.response, result2.response)
    })
  })

  describe('SCRIPT EXISTS command', () => {
    test('checks if scripts exist', async () => {
      const db = new DB()
      const loadCommand = new ScriptLoadCommand(db)
      const existsCommand = new ScriptExistsCommand(db)

      // Load a script first
      const script = Buffer.from('return "test"')
      const loadResult = await loadCommand.run(Buffer.from('SCRIPT'), [script])
      const hash = loadResult.response as string

      // Check if it exists
      const result = await existsCommand.run(Buffer.from('SCRIPT'), [
        Buffer.from(hash),
      ])

      assert.deepStrictEqual(result.response, [1])
    })

    test('returns 0 for non-existent scripts', async () => {
      const db = new DB()
      const command = new ScriptExistsCommand(db)

      const fakeHash = Buffer.from('nonexistent_hash')
      const result = await command.run(Buffer.from('SCRIPT'), [fakeHash])

      assert.deepStrictEqual(result.response, [0])
    })

    test('checks multiple scripts at once', async () => {
      const db = new DB()
      const loadCommand = new ScriptLoadCommand(db)
      const existsCommand = new ScriptExistsCommand(db)

      // Load one script
      const script1 = Buffer.from('return "test1"')
      const loadResult = await loadCommand.run(Buffer.from('SCRIPT'), [script1])
      const hash1 = loadResult.response as string

      const fakeHash = 'nonexistent_hash'

      const result = await existsCommand.run(Buffer.from('SCRIPT'), [
        Buffer.from(hash1),
        Buffer.from(fakeHash),
      ])

      assert.deepStrictEqual(result.response, [1, 0])
    })

    test('throws error when no hashes provided', async () => {
      const db = new DB()
      const command = new ScriptExistsCommand(db)

      try {
        await command.run(Buffer.from('SCRIPT'), [])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })
  })

  describe('SCRIPT FLUSH command', () => {
    test('flushes all scripts from cache', async () => {
      const db = new DB()
      const loadCommand = new ScriptLoadCommand(db)
      const flushCommand = new ScriptFlushCommand(db)

      // Load some scripts
      const result1 = await loadCommand.run(Buffer.from('SCRIPT'), [
        Buffer.from('return "test1"'),
      ])
      const result2 = await loadCommand.run(Buffer.from('SCRIPT'), [
        Buffer.from('return "test2"'),
      ])

      assert.ok(db.getScript(result1.response as string))
      assert.ok(db.getScript(result2.response as string))

      // Flush all scripts
      const result = await flushCommand.run(Buffer.from(''), [])

      assert.strictEqual(result.response, 'OK')
      assert.ok(db.getScript(result1.response as string) === undefined)
      assert.ok(db.getScript(result2.response as string) === undefined)
    })

    test('works when cache is already empty', async () => {
      const db = new DB()
      const command = new ScriptFlushCommand(db)

      const result = await command.run(Buffer.from(''), [])

      assert.strictEqual(result.response, 'OK')
    })
  })

  describe('SCRIPT KILL command', () => {
    test('returns OK', async () => {
      const command = new ScriptKillCommand()

      const result = await command.run(Buffer.from('SCRIPT'), [])

      assert.strictEqual(result.response, 'OK')
    })
  })

  describe('SCRIPT DEBUG command', () => {
    test('accepts valid debug modes', async () => {
      const command = new ScriptDebugCommand()

      const validModes = ['YES', 'SYNC', 'NO', 'yes', 'sync', 'no']

      for (const mode of validModes) {
        const result = await command.run(Buffer.from('SCRIPT'), [
          Buffer.from(mode),
        ])
        assert.strictEqual(result.response, 'OK')
      }
    })

    test('throws error for invalid debug mode', async () => {
      const command = new ScriptDebugCommand()

      try {
        await command.run(Buffer.from('SCRIPT'), [Buffer.from('INVALID')])
        assert.fail('Should have thrown error for invalid debug mode')
      } catch (error) {
        assert.ok(error instanceof Error)
        assert.ok(error.message.includes('debug mode must be one of'))
      }
    })

    test('throws error when no mode provided', async () => {
      const command = new ScriptDebugCommand()

      try {
        await command.run(Buffer.from('SCRIPT'), [])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })

    test('throws error when too many arguments provided', async () => {
      const command = new ScriptDebugCommand()

      try {
        await command.run(Buffer.from('SCRIPT'), [
          Buffer.from('YES'),
          Buffer.from('extra'),
        ])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })
  })

  describe('SCRIPT HELP command', () => {
    test('returns help text', async () => {
      const command = new ScriptHelpCommand()

      const result = await command.run(Buffer.from('SCRIPT'), [])

      assert.ok(Array.isArray(result.response))
      const helpText = result.response as string[]
      assert.ok(helpText.length > 0)
      assert.ok(helpText[0].includes('SCRIPT <subcommand>'))
      assert.ok(helpText.some(line => line.includes('DEBUG')))
      assert.ok(helpText.some(line => line.includes('EXISTS')))
      assert.ok(helpText.some(line => line.includes('FLUSH')))
      assert.ok(helpText.some(line => line.includes('KILL')))
      assert.ok(helpText.some(line => line.includes('LOAD')))
    })
  })
})
