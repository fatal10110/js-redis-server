import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * A Lua script that calls redis.setresp(3) can return RESP3-typed replies via
 * table conventions ({ double = ... }, { map = ... }, etc.). The Lua engine
 * surfaces these as distinct reply shapes; this exercises the host mapping of
 * each shape to its RESP3 wire encoding for a RESP3 (HELLO 3) client.
 */
const testRunner = new TestRunner()

describe(`Lua RESP3 script replies (${testRunner.getBackendName()})`, () => {
  let connection: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    connection = await RawRedisConnection.connect('127.0.0.1', port)
    connection.write(commandFrame('HELLO', '3'))
    await connection.readRawFrame()
  })

  after(async () => {
    connection.close()
    await testRunner.cleanup()
  })

  async function evalRaw(script: string): Promise<string> {
    connection.write(commandFrame('EVAL', script, '0'))
    return (await connection.readRawFrame()).toString('binary')
  }

  test('double reply', async () => {
    assert.strictEqual(
      await evalRaw('redis.setresp(3) return {double=3.5}'),
      ',3.5\r\n',
    )
  })

  test('boolean reply', async () => {
    assert.strictEqual(await evalRaw('redis.setresp(3) return true'), '#t\r\n')
    assert.strictEqual(await evalRaw('redis.setresp(3) return false'), '#f\r\n')
  })

  test('big number reply', async () => {
    assert.strictEqual(
      await evalRaw(
        "redis.setresp(3) return {big_number='123456789012345678901234567890'}",
      ),
      '(123456789012345678901234567890\r\n',
    )
  })

  test('verbatim string reply', async () => {
    assert.strictEqual(
      await evalRaw(
        "redis.setresp(3) return {verbatim_string={format='txt', string='hello'}}",
      ),
      '=9\r\ntxt:hello\r\n',
    )
  })

  test('map reply', async () => {
    // Single entry keeps Lua table iteration order deterministic.
    assert.strictEqual(
      await evalRaw('redis.setresp(3) return {map={a=1}}'),
      '%1\r\n$1\r\na\r\n:1\r\n',
    )
  })

  test('set reply', async () => {
    assert.strictEqual(
      await evalRaw('redis.setresp(3) return {set={a=true}}'),
      '~1\r\n$1\r\na\r\n',
    )
  })
})
