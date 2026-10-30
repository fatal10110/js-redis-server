import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'
import { randomKey } from '../utils'
import { commandCallAbort, legacy, luaRuntimeAbort } from '../profile-wording'

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
 * The suite runs once per compatibility profile (`REDIS_COMPAT`) and against
 * the real server of each: from Redis 7.0 the template is
 * `unknown subcommand '<sub>'` and the echo is cut at 128 bytes; Redis 6.2
 * words it `Unknown subcommand or wrong number of arguments for '<sub>'`, does
 * not truncate, has no FUNCTION command, and answers a known subcommand with
 * the wrong arity through the same template (or, for XGROUP, a missing key).
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
  // Redis 6.2 has no FUNCTION command at all.
  ...(legacy ? [] : ['FUNCTION']),
  'SCRIPT',
  'XGROUP',
  'XINFO',
]

/** `-ERR ...\r\n` for an unknown container subcommand, echoing raw bytes. */
function unknownSubcommandReply(
  container: string,
  echoed: string | Buffer,
): Buffer {
  return Buffer.concat([
    Buffer.from(
      legacy
        ? "-ERR Unknown subcommand or wrong number of arguments for '"
        : "-ERR unknown subcommand '",
    ),
    Buffer.from(echoed),
    Buffer.from(`'. Try ${container} HELP.\r\n`),
  ])
}

// Redis 6.2 looks up the stream key (argv[2]) before it dispatches XGROUP and
// XINFO, so a missing key wins over an unknown subcommand.
const requiresKey =
  '-ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.\r\n'
const noSuchKey = '-ERR no such key\r\n'

/** The echo limit: 128 bytes from Redis 7.0, none on 6.2. */
function echoLimit(name: string): string {
  return legacy ? name : name.slice(0, 128)
}

/** A script aborted by `error(<bytes>)`, the bytes kept raw. */
function scriptAbortFrame(script: string, bytes: Buffer): Buffer {
  const marker = '@@BYTES@@'
  const [head, tail] = luaRuntimeAbort(script, marker).split(marker)
  return Buffer.concat([
    Buffer.from('-'),
    Buffer.from(head!),
    bytes,
    Buffer.from(`${tail}\r\n`),
  ])
}

/**
 * The reply of a container that rejects options it cannot use
 * (`addReplySubcommandSyntaxError`); 6.2 words it as the unknown template.
 */
function subcommandSyntaxReply(container: string, echoed: string): string {
  return legacy
    ? unknownSubcommandReply(container, echoed).toString()
    : `-ERR unknown subcommand or wrong number of arguments for '${echoed}'. Try ${container} HELP.\r\n`
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
      legacy ? requiresKey : unknownSubcommandReply('XGROUP', 'BOGUS'),
    )
    await expectReply(
      conn,
      ['XINFO', 'BOGUS', 'stream'],
      legacy ? noSuchKey : unknownSubcommandReply('XINFO', 'BOGUS'),
    )
  })

  // Real Redis renders the echoed subcommand with `%.128s`. Verified exact
  // against 7.0.15 and 8.0.6: 127 and 128 come back whole, 129 and 300 are both
  // cut to 128. Redis 6.2 does not truncate at all.
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
        unknownSubcommandReply('CONFIG', echoLimit('X'.repeat(length))),
      )
      await expectReply(
        conn,
        ['XGROUP', 'X'.repeat(length)],
        unknownSubcommandReply('XGROUP', echoLimit('X'.repeat(length))),
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
      unknownSubcommandReply(
        'CONFIG',
        legacy ? subcommand : subcommand.subarray(0, 128),
      ),
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
        unknownSubcommandReply(container, subcommand),
      )
    }
  })

  // Both templates render the echoed name with a `%s`-family conversion over a
  // C string, so the echo stops at the first NUL — and that cut runs *before*
  // `%.128s` counts its 128. Captured from 8.0.6:
  //
  //   CONFIG 'AA\0BB'                -> unknown subcommand 'AA'.
  //   CONFIG 'A'*100 + \0 + 'A'*100  -> echoes 100, not 128
  //   CONFIG '\0' + 'A'*10           -> echoes nothing
  //   CONFIG 'A'*200 + \0 + 'A'*200  -> echoes 128 (NUL cut, then %.128s)
  //
  // Beyond fidelity this keeps a raw NUL out of a `-ERR ...\r\n` simple-error
  // frame, a body real Redis has no way to produce.
  test('the echoed subcommand stops at the first NUL byte', async () => {
    const conn = await connect()
    const NUL = Buffer.from([0])

    const cases: [Buffer, string][] = [
      [Buffer.concat([Buffer.from('AA'), NUL, Buffer.from('BB')]), 'AA'],
      [
        Buffer.concat([
          Buffer.from('A'.repeat(100)),
          NUL,
          Buffer.from('A'.repeat(100)),
        ]),
        'A'.repeat(100),
      ],
      [Buffer.concat([NUL, Buffer.from('A'.repeat(10))]), ''],
      // The NUL cut first, then `%.128s` on what is left.
      [
        Buffer.concat([
          Buffer.from('A'.repeat(200)),
          NUL,
          Buffer.from('A'.repeat(200)),
        ]),
        echoLimit('A'.repeat(200)),
      ],
    ]

    for (const [subcommand, echoed] of cases) {
      await expectReply(
        conn,
        ['CONFIG', subcommand],
        unknownSubcommandReply('CONFIG', echoed),
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
      subcommandSyntaxReply('PUBSUB', 'CHANNELS'),
    )
    await expectReply(
      conn,
      ['PUBSUB', 'SHARDCHANNELS', 'a', 'b'],
      subcommandSyntaxReply('PUBSUB', 'SHARDCHANNELS'),
    )
  })

  // Every test in this block runs unskipped against real 8.0.6 and matches it
  // byte for byte. Inputs were chosen for that: on 7.0+ an *unknown* container
  // subcommand never reaches its container from a script at all — lookup fails
  // first and the script gets `ERR Unknown Redis command called from script`
  // (#439) — so the byte-exact echo cannot be driven through `redis.call` on
  // this profile. The `redis-6.2` side, where it can, is asserted in the
  // profile sweep (`tests-integration/compatibility/profile-gates.test.ts`).

  // An error reply the script builds itself comes back through
  // `luaReplyToRedisValue`, which used to decode it. Identical bytes on real
  // 6.2.24 and 8.0.6.
  test('an error table returned by a script keeps raw bytes', async () => {
    const conn = await connect()
    const bytes = Buffer.from([0xff, 0xfe, 0xfd])

    await expectReply(
      conn,
      ['EVAL', 'return {err=ARGV[1]}', '0', bytes],
      Buffer.concat([Buffer.from('-'), bytes, Buffer.from('\r\n')]),
    )
  })

  // A script-aborting error is decorated with ` script: <sha>, on
  // @user_script:<line>.` in `renderScriptError` — the same code path a failing
  // `redis.call` takes. The decoration used to be added by decoding the body to
  // a string and re-encoding it. Captured from 8.0.6.
  test('a script-aborting error keeps raw bytes through the script decoration', async () => {
    const conn = await connect()
    const bytes = Buffer.from([0xff, 0xfe, 0xfd])
    const script = 'error(ARGV[1])'

    await expectReply(
      conn,
      ['EVAL', script, '0', bytes],
      scriptAbortFrame(script, bytes),
    )
  })

  // A known subcommand given unusable arguments reaches its container from a
  // script on every version, so this is the one container error that comes
  // back through both `redis.pcall` and a failing `redis.call`. The raw-byte
  // *argument* is there to pin that it is not echoed — real Redis echoes only
  // the subcommand name, which on this path must be one it recognizes, so no
  // non-ASCII input can reach the echo. Captured from 8.0.6.
  test('a nested subcommand syntax error comes back through pcall and call', async () => {
    const conn = await connect()
    const bytes = Buffer.from([0xff, 0xfe, 0xfd])
    const body = subcommandSyntaxReply('PUBSUB', 'CHANNELS').slice(1, -2)

    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('PUBSUB', 'CHANNELS', ARGV[1], 'b')",
        '0',
        bytes,
      ],
      `-${body}\r\n`,
    )

    const script = "return redis.call('PUBSUB', 'CHANNELS', ARGV[1], 'b')"
    await expectReply(
      conn,
      ['EVAL', script, '0', bytes],
      `-${commandCallAbort(script, body)}\r\n`,
    )
  })

  // Not an unknown subcommand: a *known* one with the wrong argument count is
  // an arity error naming `<container>|<subcommand>` on 7.0+. Pinned here so
  // the refactor cannot quietly widen the unknown-subcommand path over it.
  //
  // XGROUP and XINFO resolve their subcommand in a schema parser, which only
  // knows the container name, so they used to answer `... for 'xgroup'
  // command` (#438). The name is lower-cased whatever the client sent, and a
  // bare container with no subcommand at all names just the container.
  // Captured from 8.0.6.
  test('a known subcommand with the wrong arity is still an arity error', async () => {
    const conn = await connect()
    const key = `subcmd-arity:${randomKey()}`

    // Redis 6.2 has no per-subcommand arity: a wrong count is the unknown
    // template, and XGROUP checks the key exists before it counts anything
    // but CREATE|SETID|DESTROY's own minimum.
    const unknown = (container: string, echoed: string): string =>
      unknownSubcommandReply(container, echoed).toString()

    const cases: [string[], string, string][] = [
      [['CONFIG', 'GET'], 'config|get', unknown('CONFIG', 'GET')],
      [['XGROUP'], 'xgroup', 'same'],
      [['XGROUP', 'CREATE', key, 'g'], 'xgroup|create', requiresKey],
      [['XGROUP', 'create', key, 'g'], 'xgroup|create', requiresKey],
      [['XGROUP', 'SETID', key, 'g'], 'xgroup|setid', requiresKey],
      [
        ['XGROUP', 'DESTROY', key],
        'xgroup|destroy',
        unknown('XGROUP', 'DESTROY'),
      ],
      [['XGROUP', 'DESTROY', key, 'g', 'x'], 'xgroup|destroy', requiresKey],
      [
        ['XGROUP', 'CREATECONSUMER', key, 'g'],
        'xgroup|createconsumer',
        requiresKey,
      ],
      [
        ['XGROUP', 'CREATECONSUMER', key, 'g', 'c', 'x'],
        'xgroup|createconsumer',
        requiresKey,
      ],
      [['XGROUP', 'DELCONSUMER', key, 'g'], 'xgroup|delconsumer', requiresKey],
      [
        ['XGROUP', 'DELCONSUMER', key, 'g', 'c', 'x'],
        'xgroup|delconsumer',
        requiresKey,
      ],
      [['XINFO'], 'xinfo', 'same'],
      [['XINFO', 'STREAM'], 'xinfo|stream', unknown('XINFO', 'STREAM')],
      [['xinfo', 'stream'], 'xinfo|stream', unknown('XINFO', 'stream')],
      [['XINFO', 'GROUPS'], 'xinfo|groups', unknown('XINFO', 'GROUPS')],
      [['XINFO', 'GROUPS', key, 'x'], 'xinfo|groups', noSuchKey],
      [['XINFO', 'CONSUMERS', key], 'xinfo|consumers', noSuchKey],
      [['XINFO', 'CONSUMERS', key, 'g', 'x'], 'xinfo|consumers', noSuchKey],
    ]

    for (const [args, name, legacyReply] of cases) {
      const arity = `-ERR wrong number of arguments for '${name}' command\r\n`
      await expectReply(
        conn,
        args,
        legacy && legacyReply !== 'same' ? legacyReply : arity,
      )
    }
  })

  // Past the arity table, an XINFO STREAM / XGROUP CREATE|SETID option list
  // the subcommand cannot use (a dangling `COUNT`/`ENTRIESREAD`, a stray token,
  // trailing junk) is `addReplySubcommandSyntaxError`, not an arity error. The
  // subcommand is echoed exactly as the client sent it. Captured from 8.0.6.
  //
  // The two subcommands order their checks differently in real Redis:
  //  - XGROUP parses its options before it looks the key up, so
  //    `XGROUP CREATE <missing> g $ BOGUS` is the same syntax error. That case
  //    is in the table below.
  //  - XINFO STREAM looks the key up first, so `XINFO STREAM <missing> x` is
  //    `ERR no such key`. This server checks the option list in the schema
  //    parser before any key lookup, so it answers the syntax error instead.
  //    That is a known divergence and is not asserted here. The XINFO rows
  //    therefore run against a live stream, where both orders give the same
  //    reply.
  test('an unusable XINFO/XGROUP option list is a subcommand syntax error', async () => {
    const conn = await connect()
    const key = `subcmd-syntax:${randomKey()}`
    const missing = `subcmd-syntax-missing:${randomKey()}`
    await expectReply(
      conn,
      ['XGROUP', 'CREATE', key, 'g', '$', 'MKSTREAM'],
      '+OK\r\n',
    )

    const cases: [string[], string, string][] = [
      [['XINFO', 'STREAM', key, 'x'], 'XINFO', 'STREAM'],
      [['XINFO', 'sTrEaM', key, 'x'], 'XINFO', 'sTrEaM'],
      [['XINFO', 'STREAM', key, 'COUNT', '1'], 'XINFO', 'STREAM'],
      [['XINFO', 'STREAM', key, 'FULL', 'x'], 'XINFO', 'STREAM'],
      [['XINFO', 'STREAM', key, 'FULL', 'COUNT'], 'XINFO', 'STREAM'],
      [['XINFO', 'STREAM', key, 'FULL', 'COUNT', '1', 'x'], 'XINFO', 'STREAM'],
      [['XGROUP', 'CREATE', key, 'g2', '$', 'ENTRIESREAD'], 'XGROUP', 'CREATE'],
      [['XGROUP', 'CREATE', key, 'g2', '$', 'BOGUS'], 'XGROUP', 'CREATE'],
      [['XGROUP', 'cReAtE', key, 'g2', '$', 'BOGUS'], 'XGROUP', 'cReAtE'],
      [['XGROUP', 'CREATE', missing, 'g', '$', 'BOGUS'], 'XGROUP', 'CREATE'],
      [['XGROUP', 'SETID', key, 'g', '$', 'ENTRIESREAD'], 'XGROUP', 'SETID'],
      [['XGROUP', 'SETID', key, 'g', '$', 'MKSTREAM'], 'XGROUP', 'SETID'],
    ]

    try {
      for (const [args, container, echoed] of cases) {
        await expectReply(conn, args, subcommandSyntaxReply(container, echoed))
      }
    } finally {
      await expectReply(conn, ['DEL', key], ':1\r\n')
    }
  })
})
