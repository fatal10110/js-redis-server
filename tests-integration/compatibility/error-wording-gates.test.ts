import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * Error replies whose wording or timing depends on the profile (#517).
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0.
 *
 * - LPOS `RANK 0`: Redis 7.0.1 appended `... or use negative to start from
 *   the end of the list`.
 * - `invalid expire time`: Redis 6.2 names the command bare (`in set`); 7.0
 *   quotes it (`in 'set' command`).
 * - Commands flagged `no_multi` are refused while queuing, and the refusal
 *   dirties the transaction: SHUTDOWN from Redis 7.0, and on Valkey also
 *   MULTI and WATCH. Valkey 9.0 names the command in the error.
 * - BLPOP / BRPOP report a bad timeout as the other blocking commands do.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const valkey = profile.startsWith('valkey')
const valkey9 = profile === 'valkey-9.0'

const QUEUED = '+QUEUED\r\n'
const EXECABORT =
  '-EXECABORT Transaction discarded because of previous errors.\r\n'
const notAllowed = (command: string) =>
  valkey9
    ? `-ERR Command '${command}' not allowed inside a transaction\r\n`
    : '-ERR Command not allowed inside a transaction\r\n'
const invalidExpire = (command: string) =>
  legacy
    ? `-ERR invalid expire time in ${command}\r\n`
    : `-ERR invalid expire time in '${command}' command\r\n`

describe(`Error wording profile rows (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('LPOS RANK 0 uses the profile wording', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['RPUSH', key, 'a'], ':1\r\n')
    await expectReply(
      conn,
      ['LPOS', key, 'a', 'RANK', '0'],
      legacy
        ? "-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second, ...\r\n"
        : "-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list\r\n",
    )
  })

  test('invalid expire times use the profile wording', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['SET', key, 'v', 'EX', '0'], invalidExpire('set'))
    await expectReply(conn, ['SET', key, 'v', 'EX', '-1'], invalidExpire('set'))
    await expectReply(
      conn,
      ['SET', key, 'v', 'PXAT', '-1'],
      invalidExpire('set'),
    )
    await expectReply(conn, ['SETEX', key, '0', 'v'], invalidExpire('setex'))
    await expectReply(conn, ['PSETEX', key, '0', 'v'], invalidExpire('psetex'))
    await expectReply(conn, ['SET', key, 'v'], '+OK\r\n')
    await expectReply(conn, ['GETEX', key, 'EX', '0'], invalidExpire('getex'))
    await expectReply(conn, ['GETEX', key, 'PX', '-5'], invalidExpire('getex'))
    await expectReply(conn, ['TTL', key], ':-1\r\n')
  })

  test('BLPOP and BRPOP report a bad timeout', async () => {
    const conn = await connect()
    const key = randomKey()
    for (const command of ['BLPOP', 'BRPOP']) {
      await expectReply(
        conn,
        [command, key, 'x'],
        '-ERR timeout is not a float or out of range\r\n',
      )
      await expectReply(
        conn,
        [command, key, '-1'],
        '-ERR timeout is negative\r\n',
      )
    }
  })

  test('SHUTDOWN is refused inside MULTI from 7.0', async () => {
    const conn = await connect()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    if (legacy) {
      await expectReply(conn, ['SHUTDOWN', 'BOGUS'], QUEUED)
      await expectReply(conn, ['EXEC'], '*1\r\n-ERR syntax error\r\n')
      return
    }

    await expectReply(conn, ['SHUTDOWN', 'BOGUS'], notAllowed('shutdown'))
    await expectReply(conn, ['EXEC'], EXECABORT)

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SHUTDOWN', 'ABORT'], notAllowed('shutdown'))
    await expectReply(conn, ['EXEC'], EXECABORT)
  })

  test('nested MULTI uses the profile wording', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    if (valkey) {
      await expectReply(conn, ['MULTI'], notAllowed('multi'))
      await expectReply(conn, ['EXEC'], EXECABORT)
    } else {
      await expectReply(
        conn,
        ['MULTI'],
        '-ERR MULTI calls can not be nested\r\n',
      )
      await expectReply(conn, ['EXEC'], '*0\r\n')
    }

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SET', key, '1'], QUEUED)
    if (valkey) {
      await expectReply(conn, ['MULTI'], notAllowed('multi'))
      await expectReply(conn, ['EXEC'], EXECABORT)
      await expectReply(conn, ['EXISTS', key], ':0\r\n')
    } else {
      await expectReply(
        conn,
        ['MULTI'],
        '-ERR MULTI calls can not be nested\r\n',
      )
      await expectReply(conn, ['EXEC'], '*1\r\n+OK\r\n')
    }
  })

  test('WATCH inside MULTI uses the profile wording', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    if (valkey) {
      await expectReply(conn, ['WATCH', key], notAllowed('watch'))
      await expectReply(conn, ['EXEC'], EXECABORT)
    } else {
      await expectReply(
        conn,
        ['WATCH', key],
        '-ERR WATCH inside MULTI is not allowed\r\n',
      )
      await expectReply(conn, ['EXEC'], '*0\r\n')
    }

    // The refusal follows the arity check: a bare WATCH is an arity error.
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(
      conn,
      ['WATCH'],
      "-ERR wrong number of arguments for 'watch' command\r\n",
    )
    await expectReply(conn, ['EXEC'], EXECABORT)
  })
})
