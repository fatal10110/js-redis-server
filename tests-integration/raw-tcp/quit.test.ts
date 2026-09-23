import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * QUIT has arity -1 in real Redis (#370): trailing arguments are ignored, not
 * refused. No client sends them, so this is a wire test.
 */
const testRunner = new TestRunner()

describe(`Raw TCP QUIT (${testRunner.getBackendName()})`, () => {
  let port: number

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    await testRunner.cleanup()
  })

  test('replies +OK and closes, ignoring trailing arguments', async () => {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    try {
      conn.write(commandFrame('QUIT', 'extra', 'args'))
      assert.deepStrictEqual(
        await conn.readUntilClose(),
        Buffer.from('+OK\r\n'),
      )
    } finally {
      conn.close()
    }
  })
})
