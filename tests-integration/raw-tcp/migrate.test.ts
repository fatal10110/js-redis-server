import assert from 'node:assert'
import { after, before, describe, test, type TestContext } from 'node:test'

import { STANDALONE_AUTH_PASSWORD, TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { expectReply, send } from './helpers'
import { RawRedisConnection } from './raw-connection'

/**
 * MIGRATE host port key|"" db timeout [COPY] [REPLACE] [AUTH pw | AUTH2 user
 * pw] [KEYS key ...] (#205), from the standalone server under test to the
 * password-protected one (`setupRawStandaloneAuth`). The replies are the same
 * on redis 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey 8.0.0 / 9.0.0
 * (cluster.c migrateCommand); tests/migrate.test.ts checks the bytes sent to
 * the target.
 *
 * The source dials the target by the address it sees: on the mock both are
 * in-process servers on 127.0.0.1. Against real servers the source runs in a
 * container, so `MIGRATE_TARGET_ADDRESS` (host:port) names the target as the
 * source reaches it; unset, it is docker-compose's `redis-standalone-auth`
 * service. The target must run the same or a newer version than the source,
 * or it refuses the payload. A real source that cannot reach the target skips
 * the tests that need one (and says why); the mock never does.
 */
const testRunner = new TestRunner()

const SYNTAX = '-ERR syntax error\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const KEYS_NEED_EMPTY_KEY =
  '-ERR When using MIGRATE KEYS option, the key argument must be set to the empty string\r\n'
const NOKEY = '+NOKEY\r\n'
const OK = '+OK\r\n'
const fromTarget = (error: string) =>
  `-ERR Target instance replied with error: ${error}\r\n`
const BUSYKEY = fromTarget('BUSYKEY Target key name already exists.')
const IOERR_CONNECTING = '-IOERR error or timeout connecting to the client\r\n'
const IOERR_WRITING = '-IOERR error or timeout writing to target instance\r\n'
const IOERR_READING = '-IOERR error or timeout reading to target instance\r\n'

const suite = `Raw TCP MIGRATE (${testRunner.getBackendName()}, ${activeProfile})`
describe(suite, () => {
  let sourcePort: number
  let targetPort: number
  let host: string
  let port: string
  let unreachable: string | undefined
  const connections: RawRedisConnection[] = []

  before(async () => {
    sourcePort = await testRunner.setupRawStandalone()
    targetPort = await testRunner.setupRawStandaloneAuth()
    if (testRunner.backend === 'mock') {
      host = '127.0.0.1'
      port = String(targetPort)
    } else {
      const address =
        process.env.MIGRATE_TARGET_ADDRESS ?? 'redis-standalone-auth:6379'
      const colon = address.lastIndexOf(':')
      host = address.slice(0, colon)
      port = address.slice(colon + 1)
      const conn = await connect()
      const key = fresh()
      await send(conn, ['SET', key, 'probe'])
      conn.write(
        commandFrame(
          'MIGRATE',
          host,
          port,
          key,
          '9',
          '2000',
          'COPY',
          'REPLACE',
          ...auth,
        ),
      )
      const reply = (await conn.readRawFrame()).toString()
      await send(conn, ['DEL', key])
      if (reply !== OK) {
        unreachable = `the source cannot MIGRATE to ${host}:${port} (${reply.trim()}); set MIGRATE_TARGET_ADDRESS`
      } else {
        const dest = await target()
        await send(dest, ['DEL', key])
      }
    }
  })

  /** False, with the test skipped, when the source cannot reach the target. */
  function reachable(t: TestContext): boolean {
    if (unreachable) t.skip(unreachable)
    return !unreachable
  }

  after(async () => {
    for (const connection of connections) connection.close()
    connections.length = 0
    await testRunner.cleanup()
  })

  async function connect(): Promise<RawRedisConnection> {
    const conn = await RawRedisConnection.connect('127.0.0.1', sourcePort)
    connections.push(conn)
    return conn
  }

  /** A connection to the target, authenticated, on `db`. */
  async function target(db = 9): Promise<RawRedisConnection> {
    const conn = await RawRedisConnection.connect('127.0.0.1', targetPort)
    connections.push(conn)
    await expectReply(conn, ['AUTH', STANDALONE_AUTH_PASSWORD], OK)
    await expectReply(conn, ['SELECT', String(db)], OK)
    return conn
  }

  const fresh = () => `migrate:${randomKey()}`
  const auth = ['AUTH', STANDALONE_AUTH_PASSWORD]
  const migrate = (key: string, ...rest: string[]) => [
    'MIGRATE',
    host,
    port,
    key,
    '9',
    '5000',
    ...rest,
  ]

  test('moves a key with its type, value and TTL', async t => {
    if (!reachable(t)) return
    const conn = await connect()
    const dest = await target()
    const key = fresh()
    await expectReply(conn, ['RPUSH', key, 'a', 'b'], ':2\r\n')
    await expectReply(conn, ['PEXPIRE', key, '50000'], ':1\r\n')
    await expectReply(conn, migrate(key, ...auth), OK)
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
    await expectReply(
      dest,
      ['LRANGE', key, '0', '-1'],
      '*2\r\n$1\r\na\r\n$1\r\nb\r\n',
    )
    dest.write(commandFrame('PTTL', key))
    const ttl = Number(await dest.readFrame())
    assert.ok(ttl > 40000 && ttl <= 50000, `PTTL ${ttl}`)
    // Gone from the source: nothing to move, and nothing is dialed.
    await expectReply(conn, migrate(key, ...auth), NOKEY)
    await expectReply(
      conn,
      ['MIGRATE', 'nosuchhost.invalid', '1', key, '9', '100'],
      NOKEY,
    )
    await send(dest, ['DEL', key])
  })

  test('the target error passes through; COPY and REPLACE', async t => {
    if (!reachable(t)) return
    const conn = await connect()
    const dest = await target()
    const key = fresh()
    await expectReply(dest, ['SET', key, 'old'], OK)
    await expectReply(conn, ['SET', key, 'new'], OK)
    await expectReply(conn, migrate(key, ...auth), BUSYKEY)
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await expectReply(conn, migrate(key, 'copy', 'replace', ...auth), OK)
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await expectReply(dest, ['GET', key], '$3\r\nnew\r\n')
    await expectReply(conn, ['SET', key, 'newer'], OK)
    await expectReply(conn, migrate(key, 'REPLACE', ...auth), OK)
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
    await expectReply(dest, ['GET', key], '$5\r\nnewer\r\n')
    await send(dest, ['DEL', key])
  })

  test('KEYS: missing keys are skipped, acknowledged ones move', async t => {
    if (!reachable(t)) return
    const conn = await connect()
    const dest = await target()
    const [a, b, c] = [fresh(), fresh(), fresh()]
    await expectReply(conn, ['SET', a, '1'], OK)
    await expectReply(conn, ['SET', b, '2'], OK)
    await expectReply(
      conn,
      ['MIGRATE', host, port, '', '9', '5000', ...auth, 'KEYS', a, c, b],
      OK,
    )
    await expectReply(conn, ['EXISTS', a, b], ':0\r\n')
    await expectReply(dest, ['EXISTS', a, b, c], ':2\r\n')

    // a is busy on the target: b still moves, and BUSYKEY answers.
    await expectReply(conn, ['SET', a, '1'], OK)
    await expectReply(conn, ['SET', c, '3'], OK)
    await expectReply(
      conn,
      ['MIGRATE', host, port, '', '9', '5000', ...auth, 'KEYS', a, c],
      BUSYKEY,
    )
    await expectReply(conn, ['EXISTS', a], ':1\r\n')
    await expectReply(conn, ['EXISTS', c], ':0\r\n')
    await expectReply(dest, ['GET', c], '$1\r\n3\r\n')
    await send(conn, ['DEL', a])
    await send(dest, ['DEL', a, b, c])
  })

  test('AUTH2 and SELECT failures on the target keep the key', async t => {
    if (!reachable(t)) return
    const conn = await connect()
    const key = fresh()
    await expectReply(conn, ['SET', key, 'v'], OK)
    // No NOAUTH check: the source caches its socket to the target, which
    // stays authenticated by an earlier MIGRATE's AUTH, as on real servers.
    await expectReply(
      conn,
      migrate(key, 'AUTH2', 'nosuchuser', 'pw'),
      fromTarget(
        'WRONGPASS invalid username-password pair or user is disabled.',
      ),
    )
    await expectReply(
      conn,
      ['MIGRATE', host, port, key, '99', '5000', ...auth],
      fromTarget('ERR DB index is out of range'),
    )
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await send(conn, ['DEL', key])
  })

  test('argument errors come before the key lookup', async () => {
    const conn = await connect()
    const key = fresh()
    await expectReply(conn, ['SET', key, 'v'], OK)
    for (const options of [
      ['FOO'],
      ['AUTH'],
      ['AUTH2', 'u'],
      ['COPY', 'AUTH'],
    ]) {
      await expectReply(conn, migrate(key, ...options), SYNTAX)
    }
    await expectReply(conn, migrate(key, 'KEYS', key), KEYS_NEED_EMPTY_KEY)
    await expectReply(
      conn,
      ['MIGRATE', host, port, 'missing', '9', '1000', 'KEYS'],
      KEYS_NEED_EMPTY_KEY,
    )
    // Options are read before the numbers.
    await expectReply(
      conn,
      ['MIGRATE', host, port, key, '9', 'abc', 'FOO'],
      SYNTAX,
    )
    for (const [db, timeout] of [
      ['9', 'abc'],
      ['abc', '1000'],
      ['9', '1.5'],
    ]) {
      await expectReply(
        conn,
        ['MIGRATE', host, port, key, db, timeout],
        NOT_INTEGER,
      )
    }
    await expectReply(
      conn,
      ['MIGRATE', host, port, key, '9'],
      "-ERR wrong number of arguments for 'migrate' command\r\n",
    )
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await send(conn, ['DEL', key])
  })

  test('IOERR when the target cannot be reached', async () => {
    const conn = await connect()
    const key = fresh()
    await expectReply(conn, ['SET', key, 'v'], OK)
    await expectReply(
      conn,
      ['MIGRATE', 'nosuchhost.invalid', '6379', key, '9', '1000'],
      IOERR_CONNECTING,
    )
    await expectReply(
      conn,
      ['MIGRATE', host, '-1', key, '9', '1000'],
      IOERR_CONNECTING,
    )
    // A refused connect shows at the first write.
    await expectReply(
      conn,
      ['MIGRATE', '127.0.0.1', '1', key, '9', '1000'],
      IOERR_WRITING,
    )
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await send(conn, ['DEL', key])
  })

  test('to the server itself: blocks its clients, then times out reading', async () => {
    const conn = await connect()
    const other = await connect()
    const key = fresh()
    await expectReply(conn, ['SET', key, 'v'], OK)
    // The source's own port as it sees itself.
    const self = testRunner.backend === 'mock' ? String(sourcePort) : '6379'
    const started = Date.now()
    conn.write(commandFrame('MIGRATE', '127.0.0.1', self, key, '0', '300'))
    // Sent once MIGRATE runs: answered only after it.
    await new Promise(resolve => setTimeout(resolve, 50))
    other.write(commandFrame('PING'))
    assert.strictEqual((await conn.readRawFrame()).toString(), IOERR_READING)
    assert.strictEqual((await other.readRawFrame()).toString(), '+PONG\r\n')
    assert.ok(Date.now() - started >= 280, `${Date.now() - started} ms`)
    await expectReply(conn, ['EXISTS', key], ':1\r\n')
    await send(conn, ['DEL', key])
  })

  test('keyspace events and WATCH: del on a move, nothing with COPY', async t => {
    if (!reachable(t)) return
    const conn = await connect()
    const sub = await connect()
    const watcher = await connect()
    const dest = await target()
    conn.write(commandFrame('CONFIG', 'GET', 'notify-keyspace-events'))
    const previous = (await conn.readFrame()) as Buffer[]
    await expectReply(
      conn,
      ['CONFIG', 'SET', 'notify-keyspace-events', 'KEA'],
      OK,
    )
    try {
      const key = fresh()
      sub.write(commandFrame('SUBSCRIBE', `__keyspace@0__:${key}`))
      await sub.readFrame()
      await expectReply(conn, ['SET', key, 'v'], OK)
      // COPY changes nothing: the transaction still runs.
      await expectReply(watcher, ['WATCH', key], OK)
      await expectReply(conn, migrate(key, 'COPY', ...auth), OK)
      await expectReply(watcher, ['MULTI'], OK)
      await expectReply(watcher, ['PING'], '+QUEUED\r\n')
      await expectReply(watcher, ['EXEC'], '*1\r\n+PONG\r\n')
      // A move deletes the key: the transaction aborts.
      await expectReply(watcher, ['WATCH', key], OK)
      await expectReply(conn, migrate(key, 'REPLACE', ...auth), OK)
      await expectReply(watcher, ['MULTI'], OK)
      await expectReply(watcher, ['PING'], '+QUEUED\r\n')
      await expectReply(watcher, ['EXEC'], '*-1\r\n')
      await expectReply(conn, ['SET', key, 'x'], OK)
      const events: string[] = []
      for (let i = 0; i < 3; i++) {
        const message = (await sub.readFrame()) as Buffer[]
        events.push(message[2].toString())
      }
      assert.deepStrictEqual(events, ['set', 'del', 'set'])
      await send(conn, ['DEL', key])
      await send(dest, ['DEL', key])
    } finally {
      await send(conn, [
        'CONFIG',
        'SET',
        'notify-keyspace-events',
        previous[1].toString(),
      ])
    }
  })

  test('inside MULTI it is queued and answers in EXEC', async t => {
    if (!reachable(t)) return
    const conn = await connect()
    const dest = await target()
    const key = fresh()
    await expectReply(conn, ['SET', key, 'v'], OK)
    await expectReply(conn, ['MULTI'], OK)
    await expectReply(conn, migrate(key, ...auth), '+QUEUED\r\n')
    await expectReply(conn, migrate(key, 'FOO'), '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*2\r\n${OK}${SYNTAX}`)
    await expectReply(dest, ['GET', key], '$1\r\nv\r\n')
    await send(dest, ['DEL', key])
  })

  test('COMMAND GETKEYS finds the key, or the keys after KEYS', async () => {
    const conn = await connect()
    const getkeys = (...args: string[]) => [
      'COMMAND',
      'GETKEYS',
      'migrate',
      'h',
      '6379',
      ...args,
    ]
    await expectReply(
      conn,
      getkeys('', '9', '1000', 'KEYS', 'a', 'b'),
      '*2\r\n$1\r\na\r\n$1\r\nb\r\n',
    )
    await expectReply(
      conn,
      getkeys('k', '9', '1000', 'COPY'),
      '*1\r\n$1\r\nk\r\n',
    )
    await expectReply(
      conn,
      getkeys('k', '9'),
      '-ERR Invalid number of arguments specified for command\r\n',
    )
    // Redis 6.2's getkeys proc does not skip AUTH's argument and finds no
    // key after a trailing KEYS; 7.0+ and valkey fall back to the key spec.
    const legacy = activeProfile === 'redis-6.2'
    await expectReply(
      conn,
      getkeys('', '9', '1000', 'AUTH', 'KEYS', 'x'),
      legacy ? '*1\r\n$1\r\nx\r\n' : '*1\r\n$0\r\n\r\n',
    )
    await expectReply(
      conn,
      getkeys('', '9', '1000', 'COPY', 'KEYS'),
      legacy
        ? '-ERR Invalid arguments specified for command\r\n'
        : '*1\r\n$0\r\n\r\n',
    )
  })

  test('from a script: NOKEY when nothing is there', async () => {
    const conn = await connect()
    await expectReply(
      conn,
      [
        'EVAL',
        `return redis.pcall('migrate', ARGV[1], ARGV[2], KEYS[1], '9', '1000')`,
        '1',
        fresh(),
        host,
        port,
      ],
      NOKEY,
    )
  })
})
