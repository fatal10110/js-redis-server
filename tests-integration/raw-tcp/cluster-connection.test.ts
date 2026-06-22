import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { connectToRawSlotOwner, expectBulk, expectReply } from './helpers'

/**
 * HELLO / RESET / SELECT protocol handshake over bare sockets on a cluster node.
 *
 * RESP-version negotiation (HELLO), connection reset, and SELECT validation are
 * protocol concerns the client manages internally — there's no typed "send
 * HELLO/RESET now, show me the raw reply". Moved out of
 * `ioredis/connection-integration.test.ts`, where they used `.call(...)`. The
 * typed CLIENT/SELECT client-behavior tests stay there.
 */
const testRunner = new TestRunner()

const INVALID_CLIENT_NAME_ERROR =
  '-ERR Client names cannot contain spaces, newlines or special characters.\r\n'
const NOPROTO = '-NOPROTO unsupported protocol version\r\n'

describe(`Raw TCP cluster connection handshake (${testRunner.getBackendName()})`, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) connection.close()
    connections.length = 0
    await testRunner.cleanup()
  })

  async function owner(): Promise<RawRedisConnection> {
    // Any node works for connection-local handshake; pin to one slot's owner.
    const conn = await connectToRawSlotOwner(ports, `conn:${randomKey()}`)
    connections.push(conn)
    return conn
  }

  async function helloFlat(
    conn: RawRedisConnection,
    args: string[],
  ): Promise<string[]> {
    conn.write(commandFrame(...args))
    const reply = await conn.readFrame()
    assert.ok(Array.isArray(reply), 'HELLO should return a reply array')
    return reply.map(v => (Buffer.isBuffer(v) ? v.toString() : String(v)))
  }

  function assertEntry(flat: string[], key: string, expected: string): void {
    const i = flat.indexOf(key)
    assert.notStrictEqual(i, -1, `HELLO reply missing ${key}`)
    assert.strictEqual(flat[i + 1], expected)
  }

  test('HELLO SETNAME validates names like CLIENT SETNAME', async () => {
    const conn = await owner()
    const validName = `hello-${randomKey()}`

    assertEntry(
      await helloFlat(conn, ['HELLO', '2', 'SETNAME', validName]),
      'server',
      'redis',
    )
    await expectBulk(conn, ['CLIENT', 'GETNAME'], validName)

    // A rejected HELLO SETNAME must not change the established name.
    await expectReply(
      conn,
      ['HELLO', '2', 'SETNAME', 'has space'],
      INVALID_CLIENT_NAME_ERROR,
    )
    await expectBulk(conn, ['CLIENT', 'GETNAME'], validName)
  })

  test('HELLO can set the connection name and reports cluster mode', async () => {
    const conn = await owner()
    const name = `hello-${randomKey()}`

    const flat = await helloFlat(conn, ['HELLO', '2', 'SETNAME', name])
    assertEntry(flat, 'server', 'redis')
    assertEntry(flat, 'proto', '2')
    assertEntry(flat, 'mode', 'cluster')
    await expectBulk(conn, ['CLIENT', 'GETNAME'], name)
  })

  test('HELLO with an unsupported protocol version returns NOPROTO', async () => {
    const conn = await owner()
    await expectReply(conn, ['HELLO', '4'], NOPROTO)
    await expectReply(conn, ['HELLO', '0'], NOPROTO)
    await expectReply(conn, ['HELLO', '-1'], NOPROTO)
  })

  test('HELLO with a non-integer protocol version returns the HELLO-specific ERR', async () => {
    const conn = await owner()
    await expectReply(
      conn,
      ['HELLO', 'abc'],
      '-ERR Protocol version is not an integer or out of range\r\n',
    )
  })

  test('RESET clears connection-local state', async () => {
    const conn = await owner()
    await expectReply(conn, ['CLIENT', 'SETNAME', 'reset-name'], '+OK\r\n')
    await expectReply(conn, ['RESET'], '+RESET\r\n')
    await expectReply(conn, ['CLIENT', 'GETNAME'], '$-1\r\n')
  })

  test('SELECT with a non-integer index is a value error in cluster mode', async () => {
    const conn = await owner()
    await expectReply(
      conn,
      ['SELECT', 'abc'],
      '-ERR value is not an integer or out of range\r\n',
    )
  })
})
