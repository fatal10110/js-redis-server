import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Command lookup checks the command-table arity of the entry it resolves for
 * every caller, before the command runs: from 7.0 a container subcommand's own
 * entry (`client|reply`, `function|dump`), otherwise the command's (`XREAD
 * COUNT` never reaches XREAD's option parser). Byte for byte against real
 * redis-server 6.2.24, 7.0.15 and 8.0.6, and Valkey 8.0.11 / 9.0.6 (#518).
 */
const testRunner = new TestRunner()

const legacy = activeProfile === 'redis-6.2'
const arity = (command: string) =>
  `-ERR wrong number of arguments for '${command}' command\r\n`

describe(`Raw TCP command-table lookup (${testRunner.getBackendName()}, ${activeProfile})`, () => {
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

  test("a command's own table arity is checked before its parser", async () => {
    const conn = await connect()

    await expectReply(conn, ['XREAD', 'COUNT'], arity('xread'))
    await expectReply(
      conn,
      ['XREADGROUP', 'GROUP', 'g', 'STREAMS', 'k', '>'],
      arity('xreadgroup'),
    )
  })

  test(
    'a subcommand table arity is checked outside MULTI from 7.0',
    {
      skip: legacy && '6.2 has no subcommand entries; CLIENT answers itself',
    },
    async () => {
      const conn = await connect()

      await expectReply(conn, ['CLIENT', 'REPLY'], arity('client|reply'))
      await expectReply(conn, ['FUNCTION', 'DUMP', 'x'], arity('function|dump'))
      await expectReply(conn, ['XINFO', 'STREAM'], arity('xinfo|stream'))
    },
  )

  test('XREAD / XREADGROUP option errors', async () => {
    const conn = await connect()
    const unbalanced = (command: 'xread' | 'xreadgroup') => {
      if (legacy || activeProfile === 'redis-7.0') {
        return "-ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.\r\n"
      }
      if (command === 'xreadgroup') {
        return "-ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.\r\n"
      }
      const plus = ['redis-7.4', 'redis-8.0'].includes(activeProfile)
      return plus
        ? "-ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.\r\n"
        : "-ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.\r\n"
    }

    await expectReply(
      conn,
      ['XREAD', 'STREAMS', 'k', 'b', '0'],
      unbalanced('xread'),
    )
    await expectReply(
      conn,
      ['XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', 'k', 'b', '>'],
      unbalanced('xreadgroup'),
    )
    await expectReply(
      conn,
      ['XREAD', 'BOGUS', 'STREAMS', 'k', '0'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['XREAD', 'COUNT', 'x', 'STREAMS', 'k', '0'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['XREAD', 'BLOCK', 'x', 'STREAMS', 'k', '0'],
      '-ERR timeout is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['XREAD', 'BLOCK', '-1', 'STREAMS', 'k', '0'],
      '-ERR timeout is negative\r\n',
    )
    await expectReply(
      conn,
      ['XREAD', 'NOACK', 'STREAMS', 'k', '0'],
      '-ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.\r\n',
    )
  })

  test(
    'GETKEYSANDFLAGS reports each key with the flags of its key spec',
    {
      skip: legacy && 'COMMAND GETKEYSANDFLAGS is 7.0+',
    },
    async () => {
      const conn = await connect()

      await expectReply(
        conn,
        ['COMMAND', 'GETKEYSANDFLAGS', 'ZUNIONSTORE', 'd', '2', 'a', 'b'],
        '*3\r\n*2\r\n$1\r\nd\r\n*2\r\n+OW\r\n+update\r\n*2\r\n$1\r\na\r\n*2\r\n+RO\r\n+access\r\n*2\r\n$1\r\nb\r\n*2\r\n+RO\r\n+access\r\n',
      )
      await expectReply(
        conn,
        [
          'COMMAND',
          'GETKEYSANDFLAGS',
          'GEORADIUS',
          'k',
          '0',
          '0',
          '1',
          'km',
          'STORE',
          'd',
        ],
        '*2\r\n*2\r\n$1\r\nk\r\n*2\r\n+RO\r\n+access\r\n*2\r\n$1\r\nd\r\n*2\r\n+OW\r\n+update\r\n',
      )
      await expectReply(
        conn,
        ['COMMAND', 'GETKEYSANDFLAGS', 'EVAL', 's', '2', 'a', 'b'],
        '*2\r\n*2\r\n$1\r\na\r\n*3\r\n+RW\r\n+access\r\n+update\r\n*2\r\n$1\r\nb\r\n*3\r\n+RW\r\n+access\r\n+update\r\n',
      )
    },
  )
})
