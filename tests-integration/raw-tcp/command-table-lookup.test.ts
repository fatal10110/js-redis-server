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
 * redis-server 6.2.24, 7.0.15, 7.2.4, 7.4.0 / 7.4.4, 8.0.0 / 8.0.6, and
 * Valkey 8.0.11 / 9.0.6 (#518). COMMAND GETKEYS / GETKEYSANDFLAGS, which look
 * the command up the same way, are in `command-getkeys-matrix.test.ts` and
 * `../compatibility/getkeys-gates.test.ts`. (Later patch releases - 7.2.16,
 * 7.4.11, 8.2.10, 8.4.7 - mark XREAD's and GEORADIUS's key specs
 * `incomplete`, so there GETKEYS answers from the getkeys procedures; the
 * profiles model the earlier releases.)
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
      // `'+'` is listed from 8.0.0 (7.4.0 - 7.4.11 omit it).
      const plus = activeProfile === 'redis-8.0'
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

  // GEORADIUS stores into the last STORE / STOREDIST destination, as that
  // kind (Redis's georadiusGeneric overwrites both each time).
  test('GEORADIUS takes the last STORE / STOREDIST', async () => {
    const conn = await connect()
    const key = `georadius-last-store:${Math.random().toString(36).slice(2)}`

    await expectReply(
      conn,
      ['GEOADD', key, '13.361389', '38.115556', 'm'],
      ':1\r\n',
    )
    await expectReply(
      conn,
      [
        'GEORADIUS',
        key,
        '13.361389',
        '38.115556',
        '1',
        'km',
        'STORE',
        `${key}:a`,
        'STOREDIST',
        `${key}:b`,
      ],
      ':1\r\n',
    )
    await expectReply(conn, ['EXISTS', `${key}:a`], ':0\r\n')
    // A distance (under 1 km), not a geohash score.
    await expectReply(
      conn,
      ['ZRANGEBYSCORE', `${key}:b`, '0', '1'],
      '*1\r\n$1\r\nm\r\n',
    )
    await expectReply(
      conn,
      [
        'GEORADIUS',
        key,
        '13.361389',
        '38.115556',
        '1',
        'km',
        'STOREDIST',
        `${key}:c`,
        'STORE',
        `${key}:d`,
      ],
      ':1\r\n',
    )
    await expectReply(conn, ['EXISTS', `${key}:c`], ':0\r\n')
    await expectReply(
      conn,
      ['ZSCORE', `${key}:d`, 'm'],
      '$16\r\n3479099956230698\r\n',
    )
    await expectReply(conn, ['DEL', key, `${key}:b`, `${key}:d`], ':3\r\n')
  })
})
