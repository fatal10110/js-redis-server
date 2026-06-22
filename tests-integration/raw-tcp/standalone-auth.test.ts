import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { STANDALONE_AUTH_PASSWORD, TestRunner } from '../test-config'
import { commandFrame } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * AUTH / HELLO-AUTH handshake over bare sockets on a standalone server.
 *
 * The auth handshake (NOAUTH gating, WRONGPASS, the HELLO-specific NOAUTH, and
 * the side-effect-free rejection of HELLO SETNAME) is protocol negotiation the
 * client manages internally — there's no typed "send AUTH/HELLO now, show me
 * the raw reply" — so it belongs over the wire. Moved out of the ioredis suite,
 * where it used `.call(...)`.
 */
const testRunner = new TestRunner()

const NOAUTH = '-NOAUTH Authentication required.\r\n'
const WRONGPASS =
  '-WRONGPASS invalid username-password pair or user is disabled.\r\n'
const NOPASS_CONFIGURED =
  '-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?\r\n'
const HELLO_NOAUTH =
  '-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time\r\n'

async function helloFlat(
  conn: RawRedisConnection,
  args: string[],
): Promise<string[]> {
  conn.write(commandFrame(...args))
  const reply = await conn.readFrame()
  assert.ok(Array.isArray(reply), 'HELLO should return a reply array')
  return reply.map(v => (Buffer.isBuffer(v) ? v.toString() : String(v)))
}

describe(`Raw TCP AUTH with no password configured (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) connection.close()
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const conn = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(conn)
    return conn
  }

  test('single-arg AUTH errors when no password is configured', async () => {
    const conn = await connect()
    await expectReply(conn, ['AUTH', 'somepassword'], NOPASS_CONFIGURED)
  })

  test('two-arg AUTH for the default user succeeds (nopass)', async () => {
    const conn = await connect()
    await expectReply(conn, ['AUTH', 'default', 'anything'], '+OK\r\n')
  })

  test('two-arg AUTH for an unknown user is WRONGPASS', async () => {
    const conn = await connect()
    await expectReply(conn, ['AUTH', 'nobody', 'anything'], WRONGPASS)
  })

  test('HELLO ... AUTH is a no-op handshake when no password is configured', async () => {
    const conn = await connect()
    const flat = await helloFlat(conn, ['HELLO', '2', 'AUTH', 'default', ''])
    assert.ok(flat.includes('server'))
    assert.ok(flat.includes('redis'))
  })
})

describe(`Raw TCP AUTH enforcement with requirepass (${testRunner.getBackendName()})`, () => {
  let conn: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandaloneAuth()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  // One ordered sequence on a single connection: the negative cases must run
  // before the AUTH unlock, since AUTH success persists for the connection.
  test('drives the NOAUTH / WRONGPASS / unlock sequence', async () => {
    // Gated before authentication.
    await expectReply(conn, ['GET', 'k'], NOAUTH)
    await expectReply(conn, ['PING'], NOAUTH)

    // Wrong password.
    await expectReply(conn, ['AUTH', 'wrong'], WRONGPASS)

    // Bare HELLO is gated with the HELLO-specific NOAUTH.
    await expectReply(conn, ['HELLO', '2'], HELLO_NOAUTH)

    // A rejected HELLO must not apply its SETNAME side effect.
    await expectReply(conn, ['HELLO', '2', 'SETNAME', 'leaked'], HELLO_NOAUTH)

    // Unlock.
    await expectReply(
      conn,
      ['AUTH', 'default', STANDALONE_AUTH_PASSWORD],
      '+OK\r\n',
    )

    // The failed SETNAME never stuck (empty/null name, never 'leaked').
    conn.write(commandFrame('CLIENT', 'GETNAME'))
    const name = (await conn.readRawFrame()).toString()
    assert.ok(!name.includes('leaked'), `name leaked: ${name}`)
    assert.ok(
      name === '$-1\r\n' || name === '$0\r\n\r\n',
      `unexpected name reply: ${name}`,
    )

    // Previously gated command now succeeds.
    await expectReply(conn, ['GET', 'k'], '$-1\r\n')
  })
})
