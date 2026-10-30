import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * The count argument of LPOP / RPOP / SPOP / ZPOPMIN / ZPOPMAX (#610).
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 8.1.0, 9.0.0.
 *
 * - LPOP / RPOP / SPOP read the count with `getPositiveLongFromObjectOrReply`
 *   on every version: a non-integer, an out-of-range or a negative count all
 *   answer `value is out of range, must be positive`, before the type check.
 * - ZPOPMIN / ZPOPMAX did the same from Redis 7.0.0. Redis 6.2 read the count
 *   with `getLongFromObjectOrReply` (the integer error) and answered an empty
 *   array for a count <= 0 before looking at the key's type.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'

const POSITIVE = '-ERR value is out of range, must be positive\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const EMPTY = '*0\r\n'
const BAD_COUNTS = [
  'x',
  '1.5',
  '99999999999999999999',
  '9223372036854775808',
  '0x1',
  '+1',
  ' 1',
  '-0',
]
const ZPOP_BAD = legacy ? NOT_INTEGER : POSITIVE

describe(`Pop count wording (${testRunner.getBackendName()}, ${profile})`, () => {
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

  async function seed(conn: RawRedisConnection) {
    const missing = randomKey()
    const str = randomKey()
    const list = randomKey()
    const set = randomKey()
    const zset = randomKey()
    await expectReply(conn, ['SET', str, 'v'], '+OK\r\n')
    await expectReply(conn, ['RPUSH', list, 'a', 'b'], ':2\r\n')
    await expectReply(conn, ['SADD', set, 'a'], ':1\r\n')
    await expectReply(conn, ['ZADD', zset, '1', 'a'], ':1\r\n')
    return { missing, str, list, set, zset }
  }

  test('LPOP / RPOP / SPOP answer the positive wording for a bad count', async () => {
    const conn = await connect()
    const { missing, str, list, set, zset } = await seed(conn)
    for (const count of [...BAD_COUNTS, '-1']) {
      for (const command of ['LPOP', 'RPOP']) {
        for (const key of [missing, str, list, zset]) {
          await expectReply(conn, [command, key, count], POSITIVE)
        }
      }
      for (const key of [missing, str, set, zset]) {
        await expectReply(conn, ['SPOP', key, count], POSITIVE)
      }
    }
    await expectReply(conn, ['LLEN', list], ':2\r\n')
    await expectReply(conn, ['SCARD', set], ':1\r\n')
  })

  test('LPOP / RPOP / SPOP accept 0 and counts up to LONG_MAX', async () => {
    const conn = await connect()
    const { missing, str, list, set } = await seed(conn)
    await expectReply(conn, ['LPOP', missing, '0'], '*-1\r\n')
    await expectReply(conn, ['SPOP', missing, '0'], EMPTY)
    await expectReply(conn, ['LPOP', str, '0'], WRONGTYPE)
    await expectReply(conn, ['SPOP', str, '0'], WRONGTYPE)
    await expectReply(conn, ['LPOP', list, '0'], EMPTY)
    await expectReply(
      conn,
      ['RPOP', list, '9223372036854775807'],
      '*2\r\n$1\r\nb\r\n$1\r\na\r\n',
    )
    await expectReply(
      conn,
      ['SPOP', set, '9223372036854775807'],
      '*1\r\n$1\r\na\r\n',
    )
  })

  test('a bad LPOP / SPOP count inside MULTI fails at EXEC', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['LPOP', key, 'x'], '+QUEUED\r\n')
    await expectReply(conn, ['SPOP', key, '1.5'], '+QUEUED\r\n')
    await expectReply(conn, ['ZPOPMIN', key, 'x'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*3\r\n${POSITIVE}${POSITIVE}${ZPOP_BAD}`)
  })

  test('a bad count reaches a script as the same error', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('LPOP', KEYS[1], 'x')", '1', key],
      POSITIVE,
    )
    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('ZPOPMAX', KEYS[1], 'x')", '1', key],
      ZPOP_BAD,
    )
  })

  test('ZPOPMIN / ZPOPMAX count follows the profile', async () => {
    const conn = await connect()
    const { missing, str, zset } = await seed(conn)
    for (const command of ['ZPOPMIN', 'ZPOPMAX']) {
      for (const count of BAD_COUNTS) {
        for (const key of [missing, str, zset]) {
          await expectReply(conn, [command, key, count], ZPOP_BAD)
        }
      }
      for (const count of ['-1', '-5']) {
        for (const key of [missing, str, zset]) {
          await expectReply(
            conn,
            [command, key, count],
            legacy ? EMPTY : POSITIVE,
          )
        }
      }
      await expectReply(conn, [command, missing, '0'], EMPTY)
      await expectReply(conn, [command, zset, '0'], EMPTY)
      await expectReply(conn, [command, str, '0'], legacy ? EMPTY : WRONGTYPE)
      await expectReply(conn, [command, str, '1'], WRONGTYPE)
      await expectReply(conn, [command, str], WRONGTYPE)
    }
    await expectReply(conn, ['ZCARD', zset], ':1\r\n')
    await expectReply(
      conn,
      ['ZPOPMAX', zset, '9223372036854775807'],
      '*2\r\n$1\r\na\r\n$1\r\n1\r\n',
    )
  })
})
