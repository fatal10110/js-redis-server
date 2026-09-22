import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Raw TCP unknown-subcommand error tests (#413).
 *
 * Every container command answers an unrecognized subcommand from one shared
 * template, so this suite pins that template once per container rather than
 * per call site — the whole point of #413 was that fifteen hand-rolled copies
 * had drifted into three different wordings.
 *
 * Bare socket rather than a client: the exact bytes are the thing under test,
 * ioredis/node-redis expose no typed method that can send `CONFIG BOGUS` (let
 * alone `CONFIG <three raw bytes>`), and neither adds anything to a
 * bytes-in/bytes-out assertion.
 *
 * This suite runs on the default profile (`redis-8.0`) and against the real 8.0
 * backend, so it pins the 7.0+ form. The 6.2 side of the gate — a different
 * template, and no truncation at all — is asserted by the profile sweep in
 * `tests-integration/compatibility/profile-gates.test.ts`.
 *
 * Captured from real servers:
 *
 * ```
 * 6.2.24  CONFIG BOGUS -> Unknown subcommand or wrong number of arguments for 'BOGUS'. Try CONFIG HELP.
 * 7.0.15  CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * 8.0.6   CONFIG BOGUS -> unknown subcommand 'BOGUS'. Try CONFIG HELP.
 * ```
 */
const testRunner = new TestRunner()

/**
 * Container commands whose unknown-subcommand reply is under test, paired with
 * the name real Redis puts in the `Try ... HELP.` suffix. `CLUSTER` is absent
 * because it is only registered in cluster mode, and `DEBUG` because real 8.0
 * refuses the whole command before it ever looks at the subcommand.
 */
const CONTAINERS = [
  'COMMAND',
  'CONFIG',
  'CLIENT',
  'ACL',
  'SLOWLOG',
  'PUBSUB',
  'FUNCTION',
  'SCRIPT',
  'XGROUP',
  'XINFO',
]

function unknownSubcommandReply(container: string, echoed: string): string {
  return `-ERR unknown subcommand '${echoed}'. Try ${container} HELP.\r\n`
}

describe(`Raw TCP unknown-subcommand errors (${testRunner.getBackendName()})`, () => {
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

  test('every container command shares one unknown-subcommand template', async () => {
    const conn = await connect()

    for (const container of CONTAINERS) {
      await expectReply(
        conn,
        [container, 'BOGUS'],
        unknownSubcommandReply(container, 'BOGUS'),
      )
    }
  })

  test('the unknown subcommand is echoed with its original casing', async () => {
    const conn = await connect()

    for (const echoed of ['bogus', 'BoGuS']) {
      await expectReply(
        conn,
        ['XGROUP', echoed],
        unknownSubcommandReply('XGROUP', echoed),
      )
    }
  })

  test('trailing arguments do not change the unknown-subcommand reply', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['XGROUP', 'BOGUS', 'stream', 'group'],
      unknownSubcommandReply('XGROUP', 'BOGUS'),
    )
    await expectReply(
      conn,
      ['XINFO', 'BOGUS', 'stream'],
      unknownSubcommandReply('XINFO', 'BOGUS'),
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
        unknownSubcommandReply('CONFIG', name),
      )
      await expectReply(
        conn,
        ['XGROUP', name],
        unknownSubcommandReply('XGROUP', name),
      )
    }

    for (const length of [129, 300]) {
      await expectReply(
        conn,
        ['CONFIG', 'X'.repeat(length)],
        unknownSubcommandReply('CONFIG', 'X'.repeat(128)),
      )
      await expectReply(
        conn,
        ['XGROUP', 'X'.repeat(length)],
        unknownSubcommandReply('XGROUP', 'X'.repeat(128)),
      )
    }
  })

  // The limit is 128 *bytes*, not characters, and the cut is taken without
  // regard for character boundaries: `'A'.repeat(127) + 'é'` is 129 bytes, and
  // real 8.0.6 answers with 128 of them — the last being a bare 0xc3, the lead
  // byte of a two-byte sequence whose continuation byte was cut away. A `%s`
  // echo that round-trips through a UTF-8 string cannot produce that reply.
  test('a 128-byte cut inside a multi-byte character emits the partial byte', async () => {
    const conn = await connect()

    const subcommand = Buffer.concat([
      Buffer.from('A'.repeat(127)),
      Buffer.from('é'),
    ])
    await expectReply(
      conn,
      ['CONFIG', subcommand],
      Buffer.concat([
        Buffer.from("-ERR unknown subcommand '"),
        subcommand.subarray(0, 128),
        Buffer.from("'. Try CONFIG HELP.\r\n"),
      ]),
    )
  })

  // Real Redis echoes the bytes the client sent, whatever they are. Captured
  // from 8.0.6:
  //   -ERR unknown subcommand '\xff\xfe\xfd'. Try CONFIG HELP.
  // A `toString()` on the way out turns each of those into U+FFFD, which is
  // three bytes on the wire instead of one — a reply that is both longer and
  // different from real Redis.
  test('a non-UTF-8 subcommand is echoed byte for byte', async () => {
    const conn = await connect()

    const subcommand = Buffer.from([0xff, 0xfe, 0xfd])
    for (const container of ['CONFIG', 'XGROUP', 'CLIENT']) {
      await expectReply(
        conn,
        [container, subcommand],
        Buffer.concat([
          Buffer.from("-ERR unknown subcommand '"),
          subcommand,
          Buffer.from(`'. Try ${container} HELP.\r\n`),
        ]),
      )
    }
  })

  // A different template with the same shape: real Redis'
  // `addReplySubcommandSyntaxError`, which a container raises itself when a
  // *known* subcommand gets arguments it cannot use. It keeps the `or wrong
  // number of arguments` clause and is not truncated. Captured from 8.0.6.
  test('excess arguments to a known PUBSUB subcommand keep the syntax-error wording', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['PUBSUB', 'CHANNELS', 'a', 'b'],
      "-ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.\r\n",
    )
    await expectReply(
      conn,
      ['PUBSUB', 'SHARDCHANNELS', 'a', 'b'],
      "-ERR unknown subcommand or wrong number of arguments for 'SHARDCHANNELS'. Try PUBSUB HELP.\r\n",
    )
  })

  // Not an unknown subcommand: a *known* one with the wrong argument count is
  // an arity error naming `<container>|<subcommand>` on 7.0+. Pinned here so
  // the refactor cannot quietly widen the unknown-subcommand path over it.
  //
  // Only CONFIG is asserted. `XGROUP CREATE` and `XINFO STREAM` reach the same
  // real reply (`... for 'xgroup|create' command`) but this server answers
  // `... for 'xgroup' command`, because their arity check lives in a schema
  // parser that only knows `ctx.commandName` — the dispatched-subcommand naming
  // problem tracked as #384 part 1. Asserting it here would fail on mock for a
  // reason this change does not address.
  test('a known subcommand with the wrong arity is still an arity error', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['CONFIG', 'GET'],
      "-ERR wrong number of arguments for 'config|get' command\r\n",
    )
  })
})
