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
 *
 * Byte fidelity of the echoed name — raw non-UTF-8 bytes, and a 128-byte cut
 * landing inside a multi-byte character — is no longer a gap; it is asserted
 * for every container in `command-errors-subcommand.test.ts` (#413).
 *
 * One known gap is deliberately NOT asserted here, recorded so the omission is
 * visible rather than accidental:
 *
 * 1. `CONFIG HELP` is unimplemented, so the `Try CONFIG HELP.` suffix points at
 *    a reply that is itself this error. Real returns an 11-element array whose
 *    last line is version-specific — `    Prints this help.` on 6.2/7.0 and
 *    `    Print this help.` from 7.2 on (verified on 6.2.24, 7.0.15, 7.2.16,
 *    7.4.11 and 8.0.6) — and `CONFIG HELP extra` is a `config|help` arity error
 *    on 7.0+ but the legacy unknown-subcommand text on 6.2. Implementing it
 *    therefore needs a third gate plus the profile-aware arity path, which is
 *    out of scope for #388; no test is added because pinning this server's
 *    current (wrong) reply would fail against the real backend this suite also
 *    runs against.
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

  // Real Redis sanitizes an error body with `sdsmapchars(s, "\r\n", "  ", 2)`,
  // a 1:1 character map, so a `\r\n` in the echoed subcommand becomes TWO
  // spaces. Collapsing the run to one is protocol-safe but changes the byte
  // count, which is exactly what this suite exists to pin.
  test('CR and LF in the echoed subcommand are mapped 1:1, not collapsed', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CONFIG', 'a\r\nbc'],
      "-ERR unknown subcommand 'a  bc'. Try CONFIG HELP.\r\n",
    )
    await expectReply(
      conn,
      ['CONFIG', 'x\ny'],
      "-ERR unknown subcommand 'x y'. Try CONFIG HELP.\r\n",
    )

    // A LONE `\r` is the case with protocol consequence: left unmapped it ends
    // the error line early and the client resynchronizes mid-frame. An
    // implementation that special-cases the `\r\n` pair and maps `\n` alone
    // still passes the two assertions above, so this one is what actually pins
    // the 1:1 map. Captured from real 8.0.6.
    await expectReply(
      conn,
      ['CONFIG', 'a\rb'],
      "-ERR unknown subcommand 'a b'. Try CONFIG HELP.\r\n",
    )

    // Repeated runs of either byte stay 1:1 rather than collapsing.
    await expectReply(
      conn,
      ['CONFIG', 'p\r\rq'],
      "-ERR unknown subcommand 'p  q'. Try CONFIG HELP.\r\n",
    )
    await expectReply(
      conn,
      ['CONFIG', 'm\n\nn'],
      "-ERR unknown subcommand 'm  n'. Try CONFIG HELP.\r\n",
    )
  })

  // CONFIG SET failure wording (#416). The 7.0+ form names the parameter by its
  // canonical lower-case name whatever casing the client sent; the 6.2 form
  // (no detail suffix for this parameter, name echoed as sent) is asserted by
  // the profile sweep in profile-gates.test.ts.
  test('an invalid notify-keyspace-events class fails with the CONFIG SET template', async () => {
    const conn = await connect()
    const expected =
      "-ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use 'Ag$lshzxeKEtmdn'.\r\n"

    await expectReply(
      conn,
      ['CONFIG', 'SET', 'notify-keyspace-events', 'Xz'],
      expected,
    )
    await expectReply(
      conn,
      ['CONFIG', 'SET', 'Notify-Keyspace-Events', 'KE A'],
      expected,
    )
  })

  test('an unknown CONFIG SET parameter is echoed as sent', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CONFIG', 'SET', 'Bogus-Param', '1'],
      "-ERR Unknown option or number of arguments for CONFIG SET - 'Bogus-Param'\r\n",
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
