import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply, send } from './helpers'

/**
 * Command lookup checks the command-table arity of the entry it resolves for
 * every caller, before the command runs: from 7.0 a container subcommand's own
 * entry (`client|reply`, `function|dump`), otherwise the command's (`XREAD
 * COUNT` never reaches XREAD's option parser). `COMMAND GETKEYS` /
 * `GETKEYSANDFLAGS` look the command up the same way and find its keys
 * without running it. Byte for byte against real redis-server 6.2.24,
 * 7.0.15, 7.2.0 / 7.2.4, 7.4.0 / 7.4.4, 8.0.0 / 8.0.6, and Valkey 8.0.11 /
 * 9.0.6 (#518). Later patch releases (7.2.16, 7.4.11, 8.2.10, 8.4.7) mark
 * XREAD's and GEORADIUS's key specs `incomplete`, so their GETKEYS answers
 * come from the getkeys procs; the profiles model the earlier releases.
 */
const testRunner = new TestRunner()

const legacy = activeProfile === 'redis-6.2'
const valkey = activeProfile.startsWith('valkey-')
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

  // Redis's getKeysSubcommandImpl: lookup, then whether the entry has keys,
  // then its arity, then the keys, from the key specs on 7.0+ (the getkeys
  // proc when a spec cannot be applied) and from the getkeys proc or legacy
  // key range on 6.2. The command's own parser never runs.
  test('COMMAND GETKEYS finds keys without running the command', async () => {
    const conn = await connect()
    const keys = (...names: string[]) =>
      `*${names.length}\r\n${names.map(name => `$${name.length}\r\n${name}\r\n`).join('')}`
    const noKeys = '-ERR The command has no key arguments\r\n'
    const invalidArgs = '-ERR Invalid arguments specified for command\r\n'
    const invalidCount =
      '-ERR Invalid number of arguments specified for command\r\n'
    const getkeys = (args: string[], reply: string) =>
      expectReply(conn, ['COMMAND', 'GETKEYS', ...args], reply)

    // Keys before arity, per subcommand from 7.0.
    await getkeys(['CLIENT', 'REPLY'], noKeys)
    await getkeys(['CONFIG', 'GET'], noKeys)
    await getkeys(['CLIENT', 'KILL'], noKeys)
    await getkeys(
      ['CLIENT', 'BOGUS'],
      legacy ? noKeys : '-ERR Invalid command specified\r\n',
    )
    await getkeys(['XINFO', 'HELP'], legacy ? invalidArgs : noKeys)
    await getkeys(['XINFO', 'STREAM'], legacy ? invalidArgs : invalidCount)
    await getkeys(['GET', 'a', 'b'], invalidCount)

    // Commands whose own parser would fail.
    await getkeys(
      ['XREAD', 'STREAMS', 'a', 'b', '0'],
      legacy ? invalidArgs : keys('a'),
    )
    await getkeys(['ZUNIONSTORE', 'd', '2abc', 'a', 'b'], keys('a', 'b', 'd'))
    await getkeys(
      ['ZUNIONSTORE', 'd', '1', 'a', 'BOGUS'],
      legacy ? keys('a', 'd') : keys('d', 'a'),
    )
    await getkeys(['ZUNIONSTORE', 'd', '5', 'a'], invalidArgs)
    await getkeys(['EVAL', 's', '2', 'a'], legacy ? invalidArgs : '*0\r\n')
    await getkeys(['SET', 'k', 'v', 'BOGUS'], keys('k'))
    await getkeys(['MSET', 'a', 'b', 'c'], keys('a', 'c'))

    // Redis's STORE / STOREDIST key specs each find their own keyword; 6.2
    // and Valkey (whose specs are variable_flags) ask the proc, which keeps
    // the last.
    await getkeys(
      ['GEORADIUS', 'k', '0', '0', '1', 'km', 'STORE', 'a', 'STOREDIST', 'b'],
      legacy || valkey ? keys('k', 'b') : keys('k', 'a', 'b'),
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

  test(
    'GETKEYSANDFLAGS: subcommand entries, getkeys procs and RESP3 sets',
    {
      skip: legacy && 'COMMAND GETKEYSANDFLAGS is 7.0+',
    },
    async () => {
      const conn = await connect()
      const entry = (key: string, flags: string[], type = '*') =>
        `*2\r\n$${key.length}\r\n${key}\r\n${type}${flags.length}\r\n${flags.map(flag => `+${flag}\r\n`).join('')}`
      const reply = (...entries: string[]) =>
        `*${entries.length}\r\n${entries.join('')}`
      const flags = (args: string[], expected: string) =>
        expectReply(conn, ['COMMAND', 'GETKEYSANDFLAGS', ...args], expected)
      const geo = ['GEORADIUS', 'k', '0', '0', '1', 'km']
      const source = entry('k', ['RO', 'access'])
      const dest = (key: string) => entry(key, ['OW', 'update'])

      // Valkey's STORE / STOREDIST specs are variable_flags, so its proc
      // answers: the last destination only.
      await flags(
        [...geo, 'STORE', 'd', 'STORE', 'e'],
        valkey ? reply(source, dest('e')) : reply(source, dest('d')),
      )
      await flags(
        [...geo, 'STORE', 'd', 'STOREDIST', 'e'],
        valkey ? reply(source, dest('e')) : reply(source, dest('d'), dest('e')),
      )
      // From the subcommand's own entry.
      await flags(
        ['XGROUP', 'CREATE', 's', 'g', '$'],
        reply(entry('s', ['RW', 'insert'])),
      )
      await flags(
        ['XGROUP', 'DESTROY', 's', 'g'],
        reply(entry('s', ['RW', 'delete'])),
      )
      // SET's spec is variable_flags; its proc decides by GET.
      await flags(['SET', 'k', 'v'], reply(entry('k', ['OW', 'update'])))
      await flags(
        ['SET', 'k', 'v', 'GET'],
        reply(entry('k', ['RW', 'access', 'update'])),
      )
      // A numkeys spec that cannot be applied: the proc's keys, no flags.
      await flags(
        ['ZUNIONSTORE', 'd', '2abc', 'a', 'b'],
        reply(entry('a', []), entry('b', []), entry('d', [])),
      )

      // RESP3: each key's flags are a set.
      await send(conn, ['HELLO', '3'])
      await flags(['SET', 'k', 'v'], reply(entry('k', ['OW', 'update'], '~')))
    },
  )

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
