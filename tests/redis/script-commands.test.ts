import { test, describe } from 'node:test'
import assert from 'node:assert'
import createScriptLoad from '../../src/commanders/custom/commands/redis/script/load'
import createScriptExists from '../../src/commanders/custom/commands/redis/script/exists'
import createScriptFlush from '../../src/commanders/custom/commands/redis/script/flush'
import createScriptKill from '../../src/commanders/custom/commands/redis/script/kill'
import createScriptDebug from '../../src/commanders/custom/commands/redis/script/debug'
import createScriptHelp from '../../src/commanders/custom/commands/redis/script/help'
import { WrongNumberOfArguments, RedisSyntaxError } from '../../src/core/errors'
import { DB } from '../../src/commanders/custom/db'
import { runCommand, createTestSession } from '../command-test-utils'

describe('Script Commands', () => {
  describe('SCRIPT LOAD command', () => {
    test('loads a script and returns SHA1 hash', async () => {
      const db = new DB()
      const command = createScriptLoad(db)

      const script = Buffer.from('return "hello"')
      const result = await runCommand(command, 'SCRIPT', [script])

      assert.strictEqual(typeof result.response, 'string')
      assert.strictEqual((result.response as string).length, 40) // SHA1 is 40 chars

      // Verify script was stored
      assert.ok(db.getScript(result.response as string))
    })

    test('throws error when no script provided', async () => {
      const db = new DB()
      const command = createScriptLoad(db)

      try {
        await runCommand(command, 'SCRIPT', [])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })

    test('does not duplicate scripts with same content', async () => {
      const db = new DB()
      const command = createScriptLoad(db)

      const script = Buffer.from('return "hello"')
      const result1 = await runCommand(command, 'SCRIPT', [script])
      const result2 = await runCommand(command, 'SCRIPT', [script])

      assert.strictEqual(result1.response, result2.response)
    })
  })

  describe('SCRIPT EXISTS command', () => {
    test('checks if scripts exist', async () => {
      const db = new DB()
      const loadCommand = createScriptLoad(db)
      const existsCommand = createScriptExists(db)

      // Load a script first
      const script = Buffer.from('return "test"')
      const loadResult = await runCommand(loadCommand, 'SCRIPT', [script])
      const hash = loadResult.response as string

      // Check if it exists
      const result = await runCommand(existsCommand, 'SCRIPT', [
        Buffer.from(hash),
      ])

      assert.deepStrictEqual(result.response, [1])
    })

    test('returns 0 for non-existent scripts', async () => {
      const db = new DB()
      const command = createScriptExists(db)

      const fakeHash = Buffer.from('nonexistent_hash')
      const result = await runCommand(command, 'SCRIPT', [fakeHash])

      assert.deepStrictEqual(result.response, [0])
    })

    test('checks multiple scripts at once', async () => {
      const db = new DB()
      const loadCommand = createScriptLoad(db)
      const existsCommand = createScriptExists(db)

      // Load one script
      const script1 = Buffer.from('return "test1"')
      const loadResult = await runCommand(loadCommand, 'SCRIPT', [script1])
      const hash1 = loadResult.response as string

      const fakeHash = 'nonexistent_hash'

      const result = await runCommand(existsCommand, 'SCRIPT', [
        Buffer.from(hash1),
        Buffer.from(fakeHash),
      ])

      assert.deepStrictEqual(result.response, [1, 0])
    })

    test('throws error when no hashes provided', async () => {
      const db = new DB()
      const command = createScriptExists(db)

      try {
        await runCommand(command, 'SCRIPT', [])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })
  })

  describe('SCRIPT FLUSH command', () => {
    test('flushes all scripts from cache', async () => {
      const db = new DB()
      const loadCommand = createScriptLoad(db)
      const flushCommand = createScriptFlush(db)

      // Load some scripts
      const result1 = await runCommand(loadCommand, 'SCRIPT', [
        Buffer.from('return "test1"'),
      ])
      const result2 = await runCommand(loadCommand, 'SCRIPT', [
        Buffer.from('return "test2"'),
      ])

      assert.ok(db.getScript(result1.response as string))
      assert.ok(db.getScript(result2.response as string))

      // Flush all scripts
      const result = await runCommand(flushCommand, '', [])

      assert.strictEqual(result.response, 'OK')
      assert.ok(db.getScript(result1.response as string) === undefined)
      assert.ok(db.getScript(result2.response as string) === undefined)
    })

    test('works when cache is already empty', async () => {
      const db = new DB()
      const command = createScriptFlush(db)

      const result = await runCommand(command, '', [])

      assert.strictEqual(result.response, 'OK')
    })
  })

  describe('SCRIPT KILL command', () => {
    test('returns OK', async () => {
      const db = new DB()
      const command = createScriptKill(db)

      const result = await runCommand(command, 'SCRIPT', [])

      assert.strictEqual(result.response, 'OK')
    })
  })

  describe('SCRIPT DEBUG command', () => {
    test('accepts valid debug modes', async () => {
      const db = new DB()
      const command = createScriptDebug(db)

      const validModes = ['YES', 'SYNC', 'NO', 'yes', 'sync', 'no']

      for (const mode of validModes) {
        const result = await runCommand(command, 'SCRIPT', [Buffer.from(mode)])
        assert.strictEqual(result.response, 'OK')
      }
    })

    test('throws error for invalid debug mode', async () => {
      const db = new DB()
      const command = createScriptDebug(db)

      try {
        await runCommand(command, 'SCRIPT', [Buffer.from('INVALID')])
        assert.fail('Should have thrown error for invalid debug mode')
      } catch (error) {
        assert.ok(error instanceof RedisSyntaxError)
      }
    })

    test('throws error when no mode provided', async () => {
      const db = new DB()
      const command = createScriptDebug(db)

      try {
        await runCommand(command, 'SCRIPT', [])
        assert.fail('Should have thrown WrongNumberOfArguments')
      } catch (error) {
        assert.ok(error instanceof WrongNumberOfArguments)
      }
    })

    test('throws error when too many arguments provided', async () => {
      const db = new DB()
      const command = createScriptDebug(db)

      try {
        await runCommand(command, 'SCRIPT', [
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
      const db = new DB()
      const command = createScriptHelp(db)

      const result = await runCommand(command, 'SCRIPT', [])

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
