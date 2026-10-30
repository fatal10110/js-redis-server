import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { CONTAINER_HELP } from './container-help-fixture'

/**
 * Container HELP replies, byte for byte per profile (#491): an array of status
 * lines whose text, line count and `Prints` / `Print this help.` footer follow
 * the server version. The expected lines are the real servers' own replies
 * (container-help-fixture.ts), so this file runs unchanged against a real
 * server of every profile.
 *
 * Raw TCP: the reply type of every line (`+`, not `$`) is under test.
 */
const testRunner = new TestRunner()
const profile = activeProfile

function statusArray(lines: readonly string[]): string {
  return `*${lines.length}\r\n${lines.map(line => `+${line}\r\n`).join('')}`
}

describe(`container HELP replies (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection
  let port: number

  before(async () => {
    port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  async function send(...args: string[]): Promise<string> {
    conn.write(commandFrame(...args))
    return (await conn.readRawFrame()).toString()
  }

  for (const [container, perProfile] of Object.entries(CONTAINER_HELP)) {
    const lines = perProfile[profile]
    test(
      `${container} HELP`,
      { skip: !lines && 'not on this profile' },
      async () => {
        assert.ok(lines)
        assert.strictEqual(await send(container, 'HELP'), statusArray(lines))
        // The subcommand name is case-insensitive.
        assert.strictEqual(await send(container, 'help'), statusArray(lines))
      },
    )
  }

  test('HELP stays an array of status lines on RESP3', async () => {
    const lines = CONTAINER_HELP.CLIENT[profile]
    assert.ok(lines)
    const resp3 = await RawRedisConnection.connect('127.0.0.1', port)
    try {
      resp3.write(commandFrame('HELLO', '3'))
      await resp3.readRawFrame()
      for (const container of ['CLIENT', 'CONFIG']) {
        resp3.write(commandFrame(container, 'HELP'))
        assert.strictEqual(
          (await resp3.readRawFrame()).toString(),
          statusArray(CONTAINER_HELP[container][profile] ?? []),
          container,
        )
      }
    } finally {
      resp3.close()
    }
  })
})
