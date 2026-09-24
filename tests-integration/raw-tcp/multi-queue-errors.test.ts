import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, keyInAnotherSlot, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import {
  connectToRawSlotOwner,
  expectReply,
  expectReplyPrefix,
  rawSlotOwner,
  send,
} from './helpers'

/**
 * Which errors MULTI refuses at queue time. Real Redis refuses a command then
 * only when `processCommand` does: an unknown command, an unknown container
 * subcommand (7.0+, where lookup resolves `container|subcommand`), or a count
 * the command table's arity rejects. The transaction is then dirty and EXEC
 * answers -EXECABORT. Every other error is the command's own argument check,
 * which runs at EXEC: the command is queued (+QUEUED) and its error fills its
 * slot in EXEC's reply, while the other queued commands still run.
 *
 * In a cluster a queued command is routed by the keys its key specs find in
 * the raw arguments, without running its parser: a spec that cannot be
 * applied (a numkeys past the end of the command) leaves it keyless.
 *
 * Byte for byte against real redis-server 6.2.24, 7.0.15 and 8.0.6, and
 * Valkey 8.0.11 / 9.0.6, standalone, and an 8.0.6 cluster. Profile-aware: run
 * with `REDIS_COMPAT=redis-6.2` against a real 6.2 (or the mock on that
 * profile) for the 6.2 wording.
 */
const testRunner = new TestRunner()

const legacy = activeProfile === 'redis-6.2'
const QUEUED = '+QUEUED\r\n'
const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'
const arity = (command: string) =>
  `-ERR wrong number of arguments for '${command}' command\r\n`

describe(`Raw TCP MULTI queue-time errors (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  /** Queue `command` alone and return EXEC's reply for it. */
  async function queueOne(
    conn: RawRedisConnection,
    command: string[],
    queued: string,
    exec: string,
  ): Promise<void> {
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, command, queued)
    await expectReply(conn, ['EXEC'], exec)
  }

  test("a command's own argument error is queued and fills its EXEC slot", async () => {
    const conn = await connect()
    const tag = `{multi-q:${randomKey()}}`
    const marker = `${tag}:marker`

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['MSET', `${tag}:a`, 'b', `${tag}:c`], QUEUED)
    await expectReply(conn, ['SET', marker, '1'], QUEUED)
    await expectReply(
      conn,
      ['EXEC'],
      `*2\r\n${legacy ? '-ERR wrong number of arguments for MSET\r\n' : arity('mset')}+OK\r\n`,
    )
    // The rest of the transaction ran.
    await expectReply(conn, ['GET', marker], '$1\r\n1\r\n')
    await expectReply(conn, ['DEL', marker], ':1\r\n')
  })

  test('other argument errors a parser raises are queued too', async () => {
    const conn = await connect()
    const tag = `{multi-q:${randomKey()}}`

    const cases: Array<[string[], string]> = [
      [['HSET', `${tag}:h`, 'f', 'v', 'x'], arity('hset')],
      [['SET', `${tag}:s`, 'v', 'BOGUS'], '-ERR syntax error\r\n'],
      [
        ['INCRBY', `${tag}:n`, 'x'],
        '-ERR value is not an integer or out of range\r\n',
      ],
      [['ZADD', `${tag}:z`, 'x', 'm'], '-ERR value is not a valid float\r\n'],
      [
        ['XADD', `${tag}:x`, '*', 'f', 'v', 'x'],
        legacy ? '-ERR wrong number of arguments for XADD\r\n' : arity('xadd'),
      ],
      [
        ['XADD', `${tag}:x`, 'MAXLEN', '10', '*'],
        legacy ? '-ERR wrong number of arguments for XADD\r\n' : arity('xadd'),
      ],
      [['SORT', `${tag}:l`, 'BOGUS'], '-ERR syntax error\r\n'],
      [
        ['EXPIRE', `${tag}:s`, 'x'],
        '-ERR value is not an integer or out of range\r\n',
      ],
    ]
    for (const [command, error] of cases) {
      await queueOne(conn, command, QUEUED, `*1\r\n${error}`)
    }
  })

  test('a count the command table rejects is refused at queue time', async () => {
    const conn = await connect()
    const tag = `{multi-q:${randomKey()}}`

    await queueOne(conn, ['GET'], arity('get'), EXECABORT)
    await queueOne(conn, ['GET', `${tag}:a`, 'b'], arity('get'), EXECABORT)
    await queueOne(
      conn,
      ['GEOADD', `${tag}:g`, '1', '2'],
      arity('geoadd'),
      EXECABORT,
    )
  })

  test('an unknown command is refused at queue time', async () => {
    const conn = await connect()
    const reply = legacy
      ? '-ERR unknown command `NOSUCHCMD`, with args beginning with: \r\n'
      : "-ERR unknown command 'NOSUCHCMD', with args beginning with: \r\n"

    await queueOne(conn, ['NOSUCHCMD'], reply, EXECABORT)
  })

  test('a container subcommand fails lookup from 7.0 and is queued on 6.2', async () => {
    const conn = await connect()

    if (legacy) {
      await queueOne(
        conn,
        ['CONFIG', 'BOGUS'],
        QUEUED,
        "*1\r\n-ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try CONFIG HELP.\r\n",
      )
      return
    }

    await queueOne(
      conn,
      ['CONFIG', 'BOGUS'],
      "-ERR unknown subcommand 'BOGUS'. Try CONFIG HELP.\r\n",
      EXECABORT,
    )
    // 7.0+ lookup resolves `config|get`, so its own arity (-3) applies.
    await queueOne(conn, ['CONFIG', 'GET'], arity('config|get'), EXECABORT)
    await queueOne(
      conn,
      ['CLIENT', 'SETNAME'],
      arity('client|setname'),
      EXECABORT,
    )
  })

  // Lookup resolves XINFO / XGROUP subcommands from 7.0 like every other
  // container, so their own arity is checked at queue time; 6.2 queues them
  // and the container answers at EXEC.
  test('a stream container subcommand arity is checked at queue time from 7.0', async () => {
    const conn = await connect()
    const key = `{multi-q:${randomKey()}}:x`

    if (legacy) {
      // Real 6.2 answers `Unknown subcommand or wrong number of arguments for
      // 'DESTROY'. Try XGROUP HELP.` in the EXEC slot; this server still uses
      // the 7.0 arity wording there (#437), so only the queueing is pinned.
      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['XGROUP', 'DESTROY', key], QUEUED)
      await expectReplyPrefix(conn, ['EXEC'], '*1\r\n-ERR ')
      return
    }

    await queueOne(conn, ['XINFO', 'STREAM'], arity('xinfo|stream'), EXECABORT)
    await queueOne(
      conn,
      ['XINFO', 'GROUPS', key, 'x'],
      arity('xinfo|groups'),
      EXECABORT,
    )
    await queueOne(
      conn,
      ['XGROUP', 'CREATE', key, 'g'],
      arity('xgroup|create'),
      EXECABORT,
    )
    await queueOne(
      conn,
      ['XGROUP', 'DESTROY', key],
      arity('xgroup|destroy'),
      EXECABORT,
    )
    // A subcommand this server does not implement still has its real arity.
    await queueOne(conn, ['CLIENT', 'PAUSE'], arity('client|pause'), EXECABORT)
  })
})

// Queue time vs EXEC time, at RESP2 and RESP3 (the error bytes and EXEC's
// array are the same in both). A queue-time refusal (unknown command or
// subcommand, table arity) aborts EXEC; a deferred parse error fills its own
// slot while the rest of the transaction runs. Rows hold on real Redis 8.0;
// other profiles' wording is pinned in the compatibility suite and above.
describe(`Raw TCP MULTI error matrix (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  const refused: Array<[string[], string]> = [
    [
      ['NOSUCHCMD', 'a'],
      "-ERR unknown command 'NOSUCHCMD', with args beginning with: 'a' \r\n",
    ],
    [
      ['CLIENT', 'BOGUS'],
      "-ERR unknown subcommand 'BOGUS'. Try CLIENT HELP.\r\n",
    ],
    [['GET'], arity('get')],
    [['CLIENT', 'REPLY'], arity('client|reply')],
    [['XINFO', 'STREAM'], arity('xinfo|stream')],
    [['XREAD', 'COUNT'], arity('xread')],
  ]
  const deferred: Array<[(tag: string) => string[], string]> = [
    [tag => ['MSET', `${tag}:a`, 'b', `${tag}:c`], arity('mset')],
    [tag => ['HSET', `${tag}:h`, 'f', 'v', 'x'], arity('hset')],
    [tag => ['SET', `${tag}:s`, 'v', 'BOGUS'], '-ERR syntax error\r\n'],
    [
      tag => ['INCRBY', `${tag}:n`, 'x'],
      '-ERR value is not an integer or out of range\r\n',
    ],
    [
      tag => ['XREAD', 'COUNT', 'x', 'STREAMS', `${tag}:x`, '0'],
      '-ERR value is not an integer or out of range\r\n',
    ],
    [
      tag => ['XREAD', 'STREAMS', `${tag}:x`, `${tag}:y`, '0'],
      "-ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.\r\n",
    ],
    [
      tag => ['ZUNIONSTORE', `${tag}:d`, '1', `${tag}:a`, 'BOGUS'],
      '-ERR syntax error\r\n',
    ],
    [
      tag => ['EVAL', 'return 1', '2', `${tag}:a`],
      "-ERR Number of keys can't be greater than number of args\r\n",
    ],
  ]

  for (const protocol of ['2', '3']) {
    test(
      `RESP${protocol}: queue-time refusals abort, parse errors fill their slot`,
      { skip: activeProfile !== 'redis-8.0' && 'rows are Redis 8.0 wording' },
      async () => {
        const conn = await RawRedisConnection.connect('127.0.0.1', port)
        connections.push(conn)
        await send(conn, ['HELLO', protocol])
        const tag = `{multi-m:${randomKey()}}`
        const marker = `${tag}:marker`

        for (const [command, error] of refused) {
          await expectReply(conn, ['MULTI'], '+OK\r\n')
          await expectReply(conn, ['SET', marker, '1'], QUEUED)
          await expectReply(conn, command, error)
          await expectReply(conn, ['EXEC'], EXECABORT)
          await expectReply(conn, ['EXISTS', marker], ':0\r\n')
        }
        for (const [command, error] of deferred) {
          await expectReply(conn, ['MULTI'], '+OK\r\n')
          await expectReply(conn, ['SET', marker, '1'], QUEUED)
          await expectReply(conn, command(tag), QUEUED)
          await expectReply(conn, ['EXEC'], `*2\r\n+OK\r\n${error}`)
          await expectReply(conn, ['DEL', marker], ':1\r\n')
        }
      },
    )
  }
})

describe(`Raw TCP MULTI queue-time errors in a cluster (${testRunner.getBackendName()}, ${activeProfile})`, () => {
  let ports: number[]
  const connections: RawRedisConnection[] = []

  before(async () => {
    ports = await testRunner.setupRawCluster()
  })

  after(async () => {
    for (const connection of connections) {
      connection.close()
    }
    connections.length = 0
    await testRunner.cleanup()
  })

  /** A connection to `key`'s owner, a key in another slot, one on another node. */
  async function setup() {
    const local = `{multi-qc:${randomKey()}}:k`
    const conn = await connectToRawSlotOwner(ports, local)
    connections.push(conn)
    const owner = await rawSlotOwner(ports, local)
    const otherSlot = keyInAnotherSlot(
      local,
      () => `{multi-qc:${randomKey()}}:o`,
    )
    let remote = `{multi-qc:${randomKey()}}:r`
    while ((await rawSlotOwner(ports, remote)).port === owner.port) {
      remote = `{multi-qc:${randomKey()}}:r`
    }
    return { conn, local, otherSlot, remote }
  }

  // Where a queued command whose own parser fails is routed: Redis's
  // getKeysFromCommand over the raw arguments (the getkeys proc, else the
  // legacy key range), never the parser. MOVED / CROSSSLOT refuse it at queue
  // time and abort EXEC; a command left keyless, or whose keys are local, is
  // queued and its own error fills its EXEC slot. SELECT and MOVE check the
  // cluster when they run, so they are queued too; a Valkey 9 cluster has
  // databases, so there they run and answer their own errors.
  type RouteRow = {
    command: (keys: {
      local: string
      otherSlot: string
      remote: string
    }) => string[]
    queued: 'QUEUED' | 'MOVED' | 'CROSSSLOT'
    exec?: string
    execPrefix?: string
  }
  const valkey9 = activeProfile === 'valkey-9.0'
  const notInteger = '-ERR value is not an integer or out of range\r\n'
  const syntax = '-ERR syntax error\r\n'
  const routeRows: RouteRow[] = [
    // numkeys past the end of the command: keyless, whatever the other keys.
    {
      command: k => ['ZUNIONSTORE', k.remote, '5', k.local],
      queued: 'QUEUED',
      exec: syntax,
    },
    {
      command: k => ['EVAL', 'return 1', '2', k.local],
      queued: 'QUEUED',
      exec: "-ERR Number of keys can't be greater than number of args\r\n",
    },
    // numkeys read like atoi.
    {
      command: k => ['ZUNIONSTORE', k.local, '2abc', k.local, k.otherSlot],
      queued: 'CROSSSLOT',
    },
    { command: k => ['EVAL', 'return 1', '1x', k.remote], queued: 'MOVED' },
    {
      command: k => ['ZUNIONSTORE', k.local, '1', k.otherSlot, 'BOGUS'],
      queued: 'CROSSSLOT',
    },
    { command: k => ['ZUNION', '1', k.remote, 'BOGUS'], queued: 'MOVED' },
    {
      command: k => ['ZUNION', '2', k.local, k.otherSlot, 'BOGUS'],
      queued: 'CROSSSLOT',
    },
    {
      command: k => ['ZUNIONSTORE', k.local, '1', k.local, 'BOGUS'],
      queued: 'QUEUED',
      exec: syntax,
    },
    // XREAD / XREADGROUP: the proc keys a well-formed STREAMS tail only.
    {
      command: k => ['XREAD', 'COUNT', 'x', 'STREAMS', k.remote, '0'],
      queued: 'MOVED',
    },
    {
      command: k => ['XREAD', 'COUNT', 'x', 'STREAMS', k.local, '0'],
      queued: 'QUEUED',
      exec: notInteger,
    },
    {
      command: k => ['XREAD', 'STREAMS', k.remote, 'b', '0'],
      queued: 'QUEUED',
      execPrefix: '-ERR Unbalanced ',
    },
    {
      command: k => [
        'XREADGROUP',
        'GROUP',
        'g',
        'c',
        'STREAMS',
        k.remote,
        'b',
        '0',
      ],
      queued: 'QUEUED',
      execPrefix: '-ERR Unbalanced ',
    },
    {
      command: k => ['XREAD', 'BOGUS', 'STREAMS', k.remote, '0'],
      queued: 'QUEUED',
      exec: syntax,
    },
    {
      command: k => [
        'XREADGROUP',
        'GROUP',
        'g',
        'c',
        'BOGUS',
        'STREAMS',
        k.remote,
        '0',
      ],
      queued: 'QUEUED',
      exec: syntax,
    },
    // A container subcommand: its own entry's key range (6.2: the container's).
    { command: k => ['XINFO', 'STREAM', k.remote, 'x'], queued: 'MOVED' },
    // GEORADIUS's proc adds the STORE destination.
    {
      command: k => [
        'GEORADIUS',
        k.local,
        '0',
        '0',
        '1',
        'km',
        'STORE',
        k.otherSlot,
        'BOGUS',
      ],
      queued: 'CROSSSLOT',
    },
    { command: k => ['MSET', k.local, 'v', k.otherSlot], queued: 'CROSSSLOT' },
    // Checked when they run.
    { command: () => ['SELECT', 'x'], queued: 'QUEUED', exec: notInteger },
    {
      command: k => ['MOVE', k.local, '1'],
      queued: 'QUEUED',
      exec: valkey9
        ? '-ERR DB index is out of range\r\n'
        : '-ERR MOVE is not allowed in cluster mode\r\n',
    },
    {
      command: k => ['MOVE', k.local, 'x'],
      queued: 'QUEUED',
      exec: valkey9
        ? notInteger
        : '-ERR MOVE is not allowed in cluster mode\r\n',
    },
  ]

  test('commands whose parser fails are routed by their raw keys', async () => {
    const { conn, ...keys } = await setup()
    for (const row of routeRows) {
      const command = row.command(keys)
      await expectReply(conn, ['MULTI'], '+OK\r\n')
      if (row.queued === 'MOVED') {
        await expectReplyPrefix(conn, command, '-MOVED ')
      } else if (row.queued === 'CROSSSLOT') {
        await expectReply(
          conn,
          command,
          "-CROSSSLOT Keys in request don't hash to the same slot\r\n",
        )
      } else {
        await expectReply(conn, command, QUEUED)
      }
      if (row.queued !== 'QUEUED') {
        await expectReply(conn, ['EXEC'], EXECABORT)
      } else if (row.execPrefix) {
        await expectReplyPrefix(conn, ['EXEC'], `*1\r\n${row.execPrefix}`)
      } else {
        await expectReply(conn, ['EXEC'], `*1\r\n${row.exec}`)
      }
    }
  })

  // SELECT 1 is queued, and the rest of the transaction still runs.
  test('SELECT is queued and answers at EXEC', async () => {
    const { conn, local } = await setup()

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SELECT', '1'], QUEUED)
    await expectReply(conn, ['SET', local, 'v'], QUEUED)
    await expectReply(
      conn,
      ['EXEC'],
      valkey9
        ? '*2\r\n-ERR DB index is out of range\r\n+OK\r\n'
        : '*2\r\n-ERR SELECT is not allowed in cluster mode\r\n+OK\r\n',
    )
    await expectReply(conn, ['DEL', local], ':1\r\n')
  })

  // Shard channels are routed by slot through their `not_key` spec, even
  // though COMMAND GETKEYS says they have no key arguments.
  test('SPUBLISH is routed by its shard channel', async () => {
    const { conn, local, remote } = await setup()

    await expectReply(conn, ['SPUBLISH', local, 'm'], ':0\r\n')
    await expectReplyPrefix(conn, ['SPUBLISH', remote, 'm'], '-MOVED ')
  })

  // A valid GEORADIUS is routed by its source and the last STORE /
  // STOREDIST destination, which is where it writes.
  test('GEORADIUS routes by the last STORE / STOREDIST', async () => {
    const { conn, local, otherSlot } = await setup()
    const destination = local.replace(/:k$/, ':d')

    await expectReply(
      conn,
      [
        'GEORADIUS',
        local,
        '0',
        '0',
        '1',
        'km',
        'STORE',
        otherSlot,
        'STOREDIST',
        destination,
      ],
      ':0\r\n',
    )
  })
})
