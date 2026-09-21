import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Raw TCP `CONFIG` error-wording tests (#388).
 *
 * The exact bytes of the unknown-subcommand reply are the thing under test, so
 * this belongs on a bare socket rather than behind a client: ioredis'
 * `.config()` and node-redis' `configGet()` only reach the subcommands they
 * know about, and neither adds anything to a bytes-in/bytes-out assertion.
 *
 * Captured from real redis-server 7.2.1 and 8.0.6 — identical on both, so no
 * compatibility-profile gate applies.
 */
const testRunner = new TestRunner()

describe(`Raw TCP CONFIG errors (${testRunner.getBackendName()})`, () => {
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

  test('an unknown subcommand replies with the CONFIG HELP hint', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CONFIG', 'BOGUS'],
      "-ERR unknown subcommand 'BOGUS'. Try CONFIG HELP.\r\n",
    )
  })

  test('the unknown subcommand is echoed with its original casing', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CONFIG', 'bogus'],
      "-ERR unknown subcommand 'bogus'. Try CONFIG HELP.\r\n",
    )
    await expectReply(
      conn,
      ['CONFIG', 'BoGuS'],
      "-ERR unknown subcommand 'BoGuS'. Try CONFIG HELP.\r\n",
    )
  })

  test('trailing arguments do not change the unknown-subcommand reply', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CONFIG', 'BOGUS', 'maxmemory', '100'],
      "-ERR unknown subcommand 'BOGUS'. Try CONFIG HELP.\r\n",
    )
  })

  test('CONFIG with no subcommand is a wrong-arity error for the container', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CONFIG'],
      "-ERR wrong number of arguments for 'config' command\r\n",
    )
  })
})
