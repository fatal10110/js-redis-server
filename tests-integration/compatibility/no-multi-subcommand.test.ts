import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * A `no_multi` subcommand is refused while queuing (#590): the refusal looks
 * at the command-table entry of the subcommand (`client|reply`), not only the
 * container's. Only Valkey 9.0 flags `client|reply` `no_multi`; Redis and
 * Valkey 8.0 queue it. Arity is checked first, so a bare `CLIENT REPLY` is an
 * arity error from 7.0 and is queued on 6.2 (whose CLIENT checks its own
 * arguments when EXEC runs it). Checked against redis-server 6.2.14, 7.0.15,
 * 7.2.4, 7.4.4, 8.0.0 and valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const valkey9 = profile === 'valkey-9.0'

const QUEUED = '+QUEUED\r\n'
const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'
const CLIENT_REPLY_REFUSED =
  "-ERR Command 'client|reply' not allowed inside a transaction\r\n"

describe(`no_multi subcommands inside MULTI (${testRunner.getBackendName()}, ${profile})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const connection = await RawRedisConnection.connect('127.0.0.1', port)
    connections.push(connection)
    return connection
  }

  test('CLIENT REPLY ON is refused on Valkey 9.0 and queued elsewhere', async () => {
    const conn = await connect()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    if (valkey9) {
      await expectReply(conn, ['CLIENT', 'REPLY', 'ON'], CLIENT_REPLY_REFUSED)
      await expectReply(conn, ['EXEC'], EXECABORT)
      return
    }

    await expectReply(conn, ['CLIENT', 'REPLY', 'ON'], QUEUED)
    await expectReply(conn, ['EXEC'], '*1\r\n+OK\r\n')
  })

  test('every CLIENT REPLY mode is refused on Valkey 9.0', async t => {
    if (!valkey9) {
      // OFF and SKIP would silence the connection where they are queued.
      t.skip('only Valkey 9.0 flags client|reply no_multi')
      return
    }

    const conn = await connect()
    for (const mode of ['OFF', 'SKIP', 'BOGUS']) {
      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['CLIENT', 'REPLY', mode], CLIENT_REPLY_REFUSED)
      await expectReply(conn, ['EXEC'], EXECABORT)
    }
  })

  test('a bare CLIENT REPLY fails its arity check first', async () => {
    const conn = await connect()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    if (legacy) {
      await expectReply(conn, ['CLIENT', 'REPLY'], QUEUED)
      await expectReply(
        conn,
        ['EXEC'],
        "*1\r\n-ERR Unknown subcommand or wrong number of arguments for 'REPLY'. Try CLIENT HELP.\r\n",
      )
      return
    }

    await expectReply(
      conn,
      ['CLIENT', 'REPLY'],
      "-ERR wrong number of arguments for 'client|reply' command\r\n",
    )
    await expectReply(conn, ['EXEC'], EXECABORT)
  })

  test('other CLIENT subcommands are still queued', async () => {
    const conn = await connect()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['CLIENT', 'GETNAME'], QUEUED)
    await expectReply(conn, ['EXEC'], '*1\r\n$-1\r\n')
  })
})
