import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, keyInAnotherSlot, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import {
  connectToRawSlotOwner,
  expectReply,
  expectReplyPrefix,
  rawSlotOwner,
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

  async function movedThenAbort(
    conn: RawRedisConnection,
    command: string[],
  ): Promise<void> {
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReplyPrefix(conn, command, '-MOVED ')
    await expectReply(conn, ['EXEC'], EXECABORT)
  }

  test(
    'SELECT is queued and answers at EXEC',
    {
      skip:
        activeProfile === 'valkey-9.0' &&
        'valkey 9.0 clusters have several databases',
    },
    async () => {
      const { conn, local } = await setup()

      await queueOne(
        conn,
        ['SELECT', 'x'],
        QUEUED,
        '*1\r\n-ERR value is not an integer or out of range\r\n',
      )

      await expectReply(conn, ['MULTI'], '+OK\r\n')
      await expectReply(conn, ['SELECT', '1'], QUEUED)
      await expectReply(conn, ['SET', local, 'v'], QUEUED)
      await expectReply(
        conn,
        ['EXEC'],
        '*2\r\n-ERR SELECT is not allowed in cluster mode\r\n+OK\r\n',
      )
      await expectReply(conn, ['DEL', local], ':1\r\n')
    },
  )

  test('a numkeys past the end of the command leaves it keyless', async () => {
    const { conn, local, remote } = await setup()

    // The destination alone would be MOVED; the invalid numkeys spec makes
    // the whole command keyless, so it is queued here.
    await queueOne(
      conn,
      ['ZUNIONSTORE', remote, '5', local],
      QUEUED,
      '*1\r\n-ERR syntax error\r\n',
    )
    await queueOne(
      conn,
      ['EVAL', 'return 1', '2', local],
      QUEUED,
      "*1\r\n-ERR Number of keys can't be greater than number of args\r\n",
    )
  })

  test('numkeys, STREAMS and STORE keys are routed like Redis', async () => {
    const { conn, local, otherSlot, remote } = await setup()
    const crossSlot =
      "-CROSSSLOT Keys in request don't hash to the same slot\r\n"

    await queueOne(
      conn,
      ['ZUNIONSTORE', local, '1', otherSlot, 'BOGUS'],
      crossSlot,
      EXECABORT,
    )
    await movedThenAbort(conn, ['ZUNION', '1', remote, 'BOGUS'])
    await queueOne(
      conn,
      ['ZUNION', '2', local, otherSlot, 'BOGUS'],
      crossSlot,
      EXECABORT,
    )
    await movedThenAbort(conn, ['XREAD', 'COUNT', 'x', 'STREAMS', remote, '0'])
    await queueOne(
      conn,
      ['GEORADIUS', local, '0', '0', '1', 'km', 'STORE', otherSlot, 'BOGUS'],
      crossSlot,
      EXECABORT,
    )
    await queueOne(conn, ['MSET', local, 'v', otherSlot], crossSlot, EXECABORT)

    // Local keys: queued, and the command's own error fills its EXEC slot.
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(
      conn,
      ['XREAD', 'COUNT', 'x', 'STREAMS', local, '0'],
      QUEUED,
    )
    await expectReplyPrefix(conn, ['EXEC'], '*1\r\n-ERR ')
    await queueOne(
      conn,
      ['ZUNIONSTORE', local, '1', local, 'BOGUS'],
      QUEUED,
      '*1\r\n-ERR syntax error\r\n',
    )
  })
})
