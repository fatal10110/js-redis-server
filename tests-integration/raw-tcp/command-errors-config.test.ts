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
 * The wording IS version-specific. Redis 7.0 moved container commands into the
 * command table, which replaced the 6.2 template and added `%.128s` truncation
 * of the echoed name. Captured from real servers:
 *
 * ```
 * 6.2.24  CONFIG BOGUS -> Unknown subcommand or wrong number of arguments for 'BOGUS'. Try CONFIG HELP.
 * 7.0.15  CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * 8.0.6   CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * ```
 *
 * This suite runs on the default profile (`redis-8.0`) and against the real
 * 8.0 backend, so it pins the 7.0+ form. The 6.2 side of the gate is asserted
 * by the profile sweep in `tests-integration/compatibility/profile-gates.test.ts`.
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

  // Real Redis renders the echoed subcommand with `%.128s`. Verified exact
  // against 7.0.15 and 8.0.6: 127 and 128 come back whole, 129 and 300 are both
  // cut to 128. (Redis 6.2 does not truncate at all — covered by the gate.)
  test('the echoed subcommand is truncated at 128 bytes', async () => {
    const conn = await connect()

    for (const length of [1, 127, 128]) {
      const name = 'X'.repeat(length)
      await expectReply(
        conn,
        ['CONFIG', name],
        `-ERR unknown subcommand '${name}'. Try CONFIG HELP.\r\n`,
      )
    }

    for (const length of [129, 300]) {
      await expectReply(
        conn,
        ['CONFIG', 'X'.repeat(length)],
        `-ERR unknown subcommand '${'X'.repeat(128)}'. Try CONFIG HELP.\r\n`,
      )
    }
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
