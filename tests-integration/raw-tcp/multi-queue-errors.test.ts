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
 * Rows hold byte for byte on real Redis 8.0 (standalone and clustered), which
 * is what raw-mock and raw-real run; the rows whose reply depends on the
 * profile (6.2 wording and queueing, Valkey 9 cluster databases) are in
 * `tests-integration/compatibility/multi-queue-gates.test.ts`.
 */
const testRunner = new TestRunner()

const redis80 = { skip: activeProfile !== 'redis-8.0' && 'Redis 8.0 rows' }
const QUEUED = '+QUEUED\r\n'
const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'
const arity = (command: string) =>
  `-ERR wrong number of arguments for '${command}' command\r\n`

describe(
  `Raw TCP MULTI queue-time errors (${testRunner.getBackendName()}, ${activeProfile})`,
  redis80,
  () => {
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
      await expectReply(conn, ['EXEC'], `*2\r\n${arity('mset')}+OK\r\n`)
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
        [['XADD', `${tag}:x`, '*', 'f', 'v', 'x'], arity('xadd')],
        [['XADD', `${tag}:x`, 'MAXLEN', '10', '*'], arity('xadd')],
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
      const reply =
        "-ERR unknown command 'NOSUCHCMD', with args beginning with: \r\n"

      await queueOne(conn, ['NOSUCHCMD'], reply, EXECABORT)
    })

    test('an unknown container subcommand fails lookup', async () => {
      const conn = await connect()

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

    // Lookup resolves XINFO / XGROUP subcommands like every other container,
    // so their own arity is checked at queue time.
    test('a stream container subcommand arity is checked at queue time', async () => {
      const conn = await connect()
      const key = `{multi-q:${randomKey()}}:x`

      await queueOne(
        conn,
        ['XINFO', 'STREAM'],
        arity('xinfo|stream'),
        EXECABORT,
      )
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
      await queueOne(
        conn,
        ['CLIENT', 'PAUSE'],
        arity('client|pause'),
        EXECABORT,
      )
    })
  },
)

// Queue time vs EXEC time, at RESP2 and RESP3 (the error bytes and EXEC's
// array are the same in both). A queue-time refusal (unknown command or
// subcommand, table arity) aborts EXEC; a deferred parse error fills its own
// slot while the rest of the transaction runs. Rows hold on real Redis 8.0;
// other profiles' wording is pinned in the compatibility suite and above.
describe(
  `Raw TCP MULTI error matrix (${testRunner.getBackendName()}, ${activeProfile})`,
  redis80,
  () => {
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
      test(`RESP${protocol}: queue-time refusals abort, parse errors fill their slot`, async () => {
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
      })
    }
  },
)

// WATCH decides before anything runs: a touched key makes EXEC answer nil
// and run nothing, the queued parse error included; an untouched one lets
// the transaction run with that error in its slot. (EXECABORT used to win.)
describe(
  `Raw TCP WATCH with a queued parse error (${testRunner.getBackendName()}, ${activeProfile})`,
  redis80,
  () => {
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

    for (const protocol of ['2', '3']) {
      test(`RESP${protocol}: a touched key aborts, an untouched one runs`, async () => {
        const conn = await RawRedisConnection.connect('127.0.0.1', port)
        const other = await RawRedisConnection.connect('127.0.0.1', port)
        connections.push(conn, other)
        await send(conn, ['HELLO', protocol])
        const tag = `{multi-w:${randomKey()}}`
        const watched = `${tag}:w`
        const marker = `${tag}:marker`
        const transaction = async (exec: string) => {
          await expectReply(conn, ['MULTI'], '+OK\r\n')
          await expectReply(conn, ['SET', marker, '1'], QUEUED)
          await expectReply(conn, ['MSET', `${tag}:a`, 'b', `${tag}:c`], QUEUED)
          await expectReply(conn, ['EXEC'], exec)
        }

        await expectReply(conn, ['WATCH', watched], '+OK\r\n')
        await expectReply(other, ['SET', watched, 'x'], '+OK\r\n')
        await transaction(protocol === '3' ? '_\r\n' : '*-1\r\n')
        await expectReply(conn, ['EXISTS', marker], ':0\r\n')

        await expectReply(conn, ['WATCH', watched], '+OK\r\n')
        await transaction(`*2\r\n+OK\r\n${arity('mset')}`)
        await expectReply(conn, ['DEL', marker, watched], ':2\r\n')
      })
    }
  },
)

describe(
  `Raw TCP MULTI queue-time errors in a cluster (${testRunner.getBackendName()}, ${activeProfile})`,
  redis80,
  () => {
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
    // cluster when they run, so they are queued too (Valkey 9's cluster
    // databases: compatibility/multi-queue-gates.test.ts).
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
      {
        command: k => ['MSET', k.local, 'v', k.otherSlot],
        queued: 'CROSSSLOT',
      },
      // Checked when they run.
      { command: () => ['SELECT', 'x'], queued: 'QUEUED', exec: notInteger },
      {
        command: k => ['MOVE', k.local, '1'],
        queued: 'QUEUED',
        exec: '-ERR MOVE is not allowed in cluster mode\r\n',
      },
      {
        command: k => ['MOVE', k.local, 'x'],
        queued: 'QUEUED',
        exec: '-ERR MOVE is not allowed in cluster mode\r\n',
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
        '*2\r\n-ERR SELECT is not allowed in cluster mode\r\n+OK\r\n',
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
  },
)
