import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { TestRunner } from '../test-config'
import { activeProfile, commandFrame } from '../utils'
import { expectReply } from './helpers'
import { RawRedisConnection } from './raw-connection'

/**
 * CLIENT KILL: the legacy `CLIENT KILL <ip:port>` form, the ADDR / LADDR /
 * USER / TYPE filters and the per-version error wordings (#249). The replies
 * are `clientKillCommand` in networking.c of redis 6.2.14 / 7.0.15 / 7.2.4 /
 * 7.4.4 / 8.0.0 and valkey 8.0.0 / 9.0.0, and run against the real server of
 * each profile.
 *
 * The real server may be shared with other test files running at the same
 * time, so every kill that could match a foreign connection is narrowed with
 * an ID or ADDR filter: a bare `TYPE normal` or `USER default` would close
 * them all.
 */
const testRunner = new TestRunner()

const NO_CLIENT = 'addr-that-matches-no-client:1'
const UNUSED_ID = '99999999'
const SYNTAX = '-ERR syntax error\r\n'

const redis62 = activeProfile === 'redis-6.2'
const valkey = activeProfile.startsWith('valkey-')
const valkey9 = activeProfile === 'valkey-9.0'
/** MAXAGE is a filter from Redis 7.4 and Valkey 8.0. */
const maxAge = !['redis-6.2', 'redis-7.0', 'redis-7.2'].includes(activeProfile)

/**
 * The reply to a bad `ID` value: 6.2 reads it as any long long, 7.0 range
 * checks it to 1..LONG_MAX with its own message, and Valkey 9.0 ends the id
 * list at the first non-integer, which is then read as an option.
 */
function badIdReply(kind: 'not-positive' | 'not-integer'): string {
  if (redis62) {
    return kind === 'not-positive'
      ? ':0\r\n'
      : '-ERR value is not an integer or out of range\r\n'
  }
  if (valkey9 && kind === 'not-integer') {
    return SYNTAX
  }
  return '-ERR client-id should be greater than 0\r\n'
}

describe(`Raw TCP CLIENT KILL filters (${testRunner.getBackendName()})`, () => {
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

  test('replies and errors without a matching client', async () => {
    const conn = await connect()
    const cases: Array<[string[], string]> = [
      // One argument after KILL is always the legacy address form.
      [[NO_CLIENT], '-ERR No such client\r\n'],
      [['1.2.3.4'], '-ERR No such client\r\n'],
      [['ADDR'], '-ERR No such client\r\n'],
      [['ID'], '-ERR No such client\r\n'],
      [['SKIPME'], '-ERR No such client\r\n'],
      [['MAXAGE'], '-ERR No such client\r\n'],
      [
        [],
        redis62
          ? SYNTAX
          : "-ERR wrong number of arguments for 'client|kill' command\r\n",
      ],
      [['ADDR', NO_CLIENT], ':0\r\n'],
      [['LADDR', NO_CLIENT], ':0\r\n'],
      [['USER', 'default', 'ADDR', NO_CLIENT], ':0\r\n'],
      [['USER', 'nouser'], "-ERR No such user 'nouser'\r\n"],
      [['USER', 'DEFAULT'], "-ERR No such user 'DEFAULT'\r\n"],
      [['USER', 'default', 'USER', 'nouser'], "-ERR No such user 'nouser'\r\n"],
      [['TYPE', 'normal', 'ADDR', NO_CLIENT], ':0\r\n'],
      [['type', 'PUBSUB', 'ADDR', NO_CLIENT], ':0\r\n'],
      [['TYPE', 'master', 'ADDR', NO_CLIENT], ':0\r\n'],
      [['TYPE', 'replica', 'ADDR', NO_CLIENT], ':0\r\n'],
      [['TYPE', 'slave', 'ADDR', NO_CLIENT], ':0\r\n'],
      [
        ['TYPE', 'primary', 'ADDR', NO_CLIENT],
        valkey ? ':0\r\n' : "-ERR Unknown client type 'primary'\r\n",
      ],
      [['TYPE', 'bogus'], "-ERR Unknown client type 'bogus'\r\n"],
      [
        ['SKIPME', 'no', 'TYPE', 'bogus'],
        "-ERR Unknown client type 'bogus'\r\n",
      ],
      [['TYPE', 'normal', 'ID', UNUSED_ID], ':0\r\n'],
      [['ID', UNUSED_ID], ':0\r\n'],
      [['ID', '99999998', 'ID', UNUSED_ID], ':0\r\n'],
      [['ID', '9223372036854775807'], ':0\r\n'],
      [['ID', '0', 'ADDR', NO_CLIENT], badIdReply('not-positive')],
      [['ID', '-1'], badIdReply('not-positive')],
      [['ID', 'abc'], badIdReply('not-integer')],
      [['ID', '1.5'], badIdReply('not-integer')],
      [['ID', '99999999999999999999'], badIdReply('not-integer')],
      [['ID', 'abc', 'ID', '5'], badIdReply('not-integer')],
      // Valkey 9.0 takes a list of ids after one ID.
      [['ID', UNUSED_ID, 'ID'], valkey9 ? ':0\r\n' : SYNTAX],
      [['ID', UNUSED_ID, '99999998'], valkey9 ? ':0\r\n' : SYNTAX],
      [['SKIPME', 'maybe'], SYNTAX],
      [['ID', UNUSED_ID, 'SKIPME'], SYNTAX],
      [['FOO', 'bar'], SYNTAX],
      [['ID', UNUSED_ID, 'FOO'], SYNTAX],
      [[NO_CLIENT, 'SKIPME', 'no'], SYNTAX],
      [['MAXAGE', '10000000'], maxAge ? ':0\r\n' : SYNTAX],
      [['ID', UNUSED_ID, 'MAXAGE', '10'], maxAge ? ':0\r\n' : SYNTAX],
      [
        ['MAXAGE', '-1'],
        maxAge ? '-ERR maxage should be greater than 0\r\n' : SYNTAX,
      ],
      [
        ['MAXAGE', '0'],
        maxAge ? '-ERR maxage should be greater than 0\r\n' : SYNTAX,
      ],
      [
        ['MAXAGE', 'abc'],
        maxAge ? '-ERR maxage is not an integer or out of range\r\n' : SYNTAX,
      ],
      [
        [
          'ID',
          UNUSED_ID,
          'ADDR',
          NO_CLIENT,
          'LADDR',
          NO_CLIENT,
          'USER',
          'default',
          'TYPE',
          'normal',
          'SKIPME',
          'yes',
        ],
        ':0\r\n',
      ],
    ]

    for (const [args, expected] of cases) {
      await expectReply(conn, ['CLIENT', 'KILL', ...args], expected)
    }
    await expectReply(conn, ['PING'], '+PONG\r\n')
  })

  test('the legacy form kills another client by address', async () => {
    const killer = await connect()
    const victim = await connect()
    const { addr } = await clientInfo(victim)

    await expectReply(killer, ['CLIENT', 'KILL', addr], '+OK\r\n')
    await assertCloses(victim)
    await expectReply(
      killer,
      ['CLIENT', 'KILL', addr],
      '-ERR No such client\r\n',
    )
  })

  test('the legacy form kills the calling client after the reply', async () => {
    const conn = await connect()
    const { addr } = await clientInfo(conn)

    await expectReply(conn, ['CLIENT', 'KILL', addr], '+OK\r\n')
    await assertCloses(conn)
  })

  test('ADDR kills by peer address and skips the caller by default', async () => {
    const killer = await connect()
    const victim = await connect()
    const victimAddr = (await clientInfo(victim)).addr
    const ownAddr = (await clientInfo(killer)).addr

    await expectReply(killer, ['CLIENT', 'KILL', 'ADDR', ownAddr], ':0\r\n')
    await expectReply(killer, ['CLIENT', 'KILL', 'ADDR', victimAddr], ':1\r\n')
    await assertCloses(victim)

    await expectReply(
      killer,
      ['CLIENT', 'KILL', 'ADDR', ownAddr, 'SKIPME', 'no'],
      ':1\r\n',
    )
    await assertCloses(killer)
  })

  test('LADDR, USER, TYPE and ID filters are ANDed', async () => {
    const killer = await connect()
    const victim = await connect()
    const { id, addr, laddr } = await clientInfo(victim)

    const misses: string[][] = [
      ['ID', id, 'ADDR', NO_CLIENT],
      ['ID', id, 'LADDR', NO_CLIENT],
      ['ID', id, 'TYPE', 'pubsub'],
      ['ID', id, 'TYPE', 'master'],
      ['ID', id, 'TYPE', 'replica'],
      ['ADDR', addr, 'ID', UNUSED_ID],
    ]
    for (const args of misses) {
      await expectReply(killer, ['CLIENT', 'KILL', ...args], ':0\r\n')
    }

    await expectReply(
      killer,
      [
        'CLIENT',
        'KILL',
        'LADDR',
        laddr,
        'USER',
        'default',
        'TYPE',
        'NORMAL',
        'ID',
        id,
      ],
      ':1\r\n',
    )
    await assertCloses(victim)
  })

  test('TYPE pubsub matches only subscribed clients', async () => {
    const killer = await connect()
    const subscriber = await connect()
    const { id } = await clientInfo(subscriber)
    subscriber.write(commandFrame('SUBSCRIBE', 'client-kill-filters'))
    await subscriber.readRawFrame()

    await expectReply(
      killer,
      ['CLIENT', 'KILL', 'TYPE', 'normal', 'ID', id],
      ':0\r\n',
    )
    await expectReply(
      killer,
      ['CLIENT', 'KILL', 'TYPE', 'pubsub', 'ID', id],
      ':1\r\n',
    )
    await assertCloses(subscriber)
  })

  test('a MONITOR client is of type normal', async () => {
    const killer = await connect()
    const monitor = await connect()
    const { id } = await clientInfo(monitor)
    await expectReply(monitor, ['MONITOR'], '+OK\r\n')

    await expectReply(
      killer,
      ['CLIENT', 'KILL', 'TYPE', 'replica', 'ID', id],
      ':0\r\n',
    )
    await expectReply(
      killer,
      ['CLIENT', 'KILL', 'TYPE', 'normal', 'ID', id],
      ':1\r\n',
    )
    await assertCloses(monitor)
  })

  test('valkey 9.0 kills every id listed after ID', async () => {
    const killer = await connect()
    const victim = await connect()
    const { id } = await clientInfo(victim)

    if (!valkey9) {
      await expectReply(killer, ['CLIENT', 'KILL', 'ID', UNUSED_ID, id], SYNTAX)
      return
    }
    await expectReply(killer, ['CLIENT', 'KILL', 'ID', UNUSED_ID, id], ':1\r\n')
    await assertCloses(victim)
  })

  test('errors are per command inside MULTI', async () => {
    const conn = await connect()

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['CLIENT', 'KILL', NO_CLIENT], '+QUEUED\r\n')
    await expectReply(conn, ['CLIENT', 'KILL', 'USER', 'nouser'], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['EXEC'],
      "*2\r\n-ERR No such client\r\n-ERR No such user 'nouser'\r\n",
    )
  })
})

/** The `id`, `addr` and `laddr` fields of a connection's own CLIENT INFO. */
async function clientInfo(
  connection: RawRedisConnection,
): Promise<{ id: string; addr: string; laddr: string }> {
  connection.write(commandFrame('CLIENT', 'INFO'))
  const reply = (await connection.readRawFrame()).toString()
  const field = (name: string): string => {
    const match = new RegExp(`(?:^|[ \\n])${name}=(\\S*)`).exec(reply)
    assert.ok(match, `no ${name}= in ${JSON.stringify(reply)}`)
    return match[1]
  }
  return { id: field('id'), addr: field('addr'), laddr: field('laddr') }
}

async function assertCloses(connection: RawRedisConnection): Promise<void> {
  await Promise.race([
    connection.readUntilClose(),
    delay(3000).then(() => {
      throw new Error('connection did not close')
    }),
  ])
}
