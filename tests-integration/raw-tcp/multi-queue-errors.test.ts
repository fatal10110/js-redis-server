import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Which errors MULTI refuses at queue time. Real Redis refuses a command then
 * only when `processCommand` does: an unknown command, an unknown container
 * subcommand (7.0+, where lookup resolves `container|subcommand`), or a count
 * the command table's arity rejects. The transaction is then dirty and EXEC
 * answers -EXECABORT. Every other error is the command's own argument check,
 * which runs at EXEC: the command is queued (+QUEUED) and its error fills its
 * slot in EXEC's reply, while the other queued commands still run.
 *
 * Byte for byte against real redis-server 6.2.24, 7.0.15 and 8.0.6, and
 * Valkey 8.0.11 / 9.0.6. Profile-aware: run with `REDIS_COMPAT=redis-6.2`
 * against a real 6.2 (or the mock on that profile) for the 6.2 wording.
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
})
