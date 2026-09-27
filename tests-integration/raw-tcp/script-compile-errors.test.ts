import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { createHash } from 'node:crypto'

import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * A script that is not valid Lua never runs, so its error has none of the
 * abort decoration: every version replies `-ERR Error compiling script (new
 * function): <Lua's message>`, for EVAL, EVAL_RO and SCRIPT LOAD alike, and
 * does not cache it. A script that compiles is cached by EVAL even when it
 * then fails (#502). Byte for byte against redis-server 6.2.24, 7.0.15,
 * 7.2.16, 7.4.11, 8.0.6 and valkey-server 7.2.14, 8.0.11, 9.0.6.
 *
 * The script cache is server-wide and the real backend is never flushed, so
 * every script carries this run's tag in a leading comment (which puts the
 * syntax error on line 2).
 */
const testRunner = new TestRunner()
const RUN = randomKey()

const sha1 = (script: string) => createHash('sha1').update(script).digest('hex')

describe(`Lua compile errors (${testRunner.getBackendName()})`, () => {
  let connection: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    connection = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    connection.close()
    await testRunner.cleanup()
  })

  async function send(...args: string[]): Promise<string> {
    connection.write(commandFrame(...args))
    return (await connection.readRawFrame()).toString('binary')
  }

  const COMPILE_ERROR =
    "-ERR Error compiling script (new function): user_script:2: unexpected symbol near '+'\r\n"

  test('EVAL of invalid Lua is a compile error and is not cached', async () => {
    const script = `-- ${RUN} eval\nreturn +`
    assert.strictEqual(await send('EVAL', script, '0'), COMPILE_ERROR)
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:0\r\n',
    )
  })

  test('EVAL_RO of invalid Lua is a compile error', async () => {
    const script = `-- ${RUN} eval_ro\nreturn +`
    assert.strictEqual(await send('EVAL_RO', script, '0'), COMPILE_ERROR)
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:0\r\n',
    )
  })

  test('SCRIPT LOAD refuses invalid Lua and does not cache it', async () => {
    const script = `-- ${RUN} load\nreturn +`
    assert.strictEqual(await send('SCRIPT', 'LOAD', script), COMPILE_ERROR)
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:0\r\n',
    )
    assert.strictEqual(
      await send('EVALSHA', sha1(script), '0'),
      '-NOSCRIPT No matching script. Please use EVAL.\r\n',
    )
  })

  test('SCRIPT LOAD inside MULTI answers the compile error at EXEC', async () => {
    const script = `-- ${RUN} multi\nreturn +`
    assert.strictEqual(await send('MULTI'), '+OK\r\n')
    assert.strictEqual(await send('SCRIPT', 'LOAD', script), '+QUEUED\r\n')
    assert.strictEqual(await send('EXEC'), `*1\r\n${COMPILE_ERROR}`)
  })

  test('an unfinished script names <eof>', async () => {
    const script = `-- ${RUN} eof\nreturn (`
    assert.strictEqual(
      await send('EVAL', script, '0'),
      "-ERR Error compiling script (new function): user_script:2: unexpected symbol near '<eof>'\r\n",
    )
  })

  test('a script that compiles is cached even when it fails at run time', async () => {
    const script = `-- ${RUN} runtime\nerror('boom', 0)`
    assert.strictEqual(
      await send('EVAL', script, '0'),
      `-ERR boom script: ${sha1(script)}, on @user_script:2.\r\n`,
    )
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:1\r\n',
    )
  })

  test('SCRIPT LOAD still caches valid Lua without running it', async () => {
    const script = `-- ${RUN} valid\nerror('not run', 0)`
    assert.strictEqual(
      await send('SCRIPT', 'LOAD', script),
      `$40\r\n${sha1(script)}\r\n`,
    )
    assert.strictEqual(
      await send('SCRIPT', 'EXISTS', sha1(script)),
      '*1\r\n:1\r\n',
    )
  })
})
