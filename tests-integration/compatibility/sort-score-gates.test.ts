import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * SORT's numeric conversion and LIMIT (#513), over a bare socket.
 *
 * sort.c converts an element or BY weight with C `strtod` up to Redis 7.4 /
 * Valkey 8.0 (hex floats and leading whitespace accepted) and with fast_float
 * from Redis 8.0 / Valkey 8.1 (`double.fast-float`). Valkey 8.1+ also refuses
 * an empty element (`double.valkey-strtod`). LIMIT takes the full signed
 * 64-bit range on every profile.
 *
 * Range errors (`1e400`, `1e-310`) are in their own describe, last: real
 * Redis <= 7.4 and Valkey 8.0 never reset errno before SORT's ERANGE check,
 * so after one a valid SORT on the same server can fail. Run this file on a
 * fresh server.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0, each in a fresh container.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const fastFloat = profile === 'redis-8.0' || profile === 'valkey-9.0'
const valkeyStrtod = profile === 'valkey-9.0'
const hasSortRo = profile !== 'redis-6.2'

const NOT_DOUBLE = "-ERR One or more scores can't be converted into double\r\n"
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const INT64_MAX = '9223372036854775807'
const INT64_MIN = '-9223372036854775808'

function bulkArray(items: string[]): string {
  return `*${items.length}\r\n${items.map(item => `$${Buffer.byteLength(item)}\r\n${item}\r\n`).join('')}`
}

describe(`SORT scores and LIMIT (${testRunner.getBackendName()}, ${profile})`, () => {
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

  /** `SORT` of the list `[element, 2, -5]`. */
  async function sortWith(
    conn: RawRedisConnection,
    element: string,
    expected: string,
  ): Promise<void> {
    const key = `sortdbl:${randomKey()}`
    await expectReply(conn, ['RPUSH', key, element, '2', '-5'], ':3\r\n')
    await expectReply(conn, ['SORT', key], expected)
  }

  /** `SORT [e, x] BY w_*` with `w_e = weight` and no `w_x` (score 0). */
  async function sortByWeight(
    conn: RawRedisConnection,
    weight: string,
    expected: string,
  ): Promise<void> {
    const prefix = `sortdbl:${randomKey()}`
    await expectReply(conn, ['RPUSH', `${prefix}:l`, 'e', 'x'], ':2\r\n')
    await expectReply(conn, ['SET', `${prefix}:w_e`, weight], '+OK\r\n')
    await expectReply(
      conn,
      ['SORT', `${prefix}:l`, 'BY', `${prefix}:w_*`],
      expected,
    )
  }

  test('infinity spellings, signs and short decimals sort on every profile', async () => {
    const conn = await connect()
    const rows: [string, string[]][] = [
      ['inf', ['-5', '2', 'inf']],
      ['-inf', ['-inf', '-5', '2']],
      ['+inf', ['-5', '2', '+inf']],
      ['INF', ['-5', '2', 'INF']],
      ['Infinity', ['-5', '2', 'Infinity']],
      ['-Infinity', ['-Infinity', '-5', '2']],
      ['infinity', ['-5', '2', 'infinity']],
      ['+3', ['-5', '2', '+3']],
      ['.5', ['-5', '.5', '2']],
      ['5.', ['-5', '2', '5.']],
      ['1e5', ['-5', '2', '1e5']],
    ]
    for (const [element, sorted] of rows) {
      await sortWith(conn, element, bulkArray(sorted))
    }
  })

  test('trailing bytes and NaN are refused on every profile', async () => {
    const conn = await connect()
    for (const element of [
      '1 ',
      'nan',
      'NaN',
      '0b11',
      '1_0',
      '0x',
      '1e',
      '0x1p',
    ]) {
      await sortWith(conn, element, NOT_DOUBLE)
    }
  })

  test('hex floats and leading whitespace follow the profile', async () => {
    const conn = await connect()
    const rows: [string, string[]][] = [
      ['0x10', ['-5', '2', '0x10']],
      ['0x1p3', ['-5', '2', '0x1p3']],
      ['-0x10', ['-0x10', '-5', '2']],
      ['0x1.8', ['-5', '0x1.8', '2']],
      [' 1', ['-5', ' 1', '2']],
    ]
    for (const [element, sorted] of rows) {
      await sortWith(conn, element, fastFloat ? NOT_DOUBLE : bulkArray(sorted))
    }
  })

  test('an empty element is 0, except on Valkey 8.1+', async () => {
    const conn = await connect()
    await sortWith(
      conn,
      '',
      valkeyStrtod ? NOT_DOUBLE : bulkArray(['-5', '', '2']),
    )
  })

  test('BY weights convert the same way', async () => {
    const conn = await connect()
    const ex = bulkArray(['x', 'e'])
    await sortByWeight(conn, '3', ex)
    await sortByWeight(conn, 'inf', ex)
    await sortByWeight(conn, '-inf', bulkArray(['e', 'x']))
    await sortByWeight(conn, '0x10', fastFloat ? NOT_DOUBLE : ex)
    await sortByWeight(conn, '0x1p3', fastFloat ? NOT_DOUBLE : ex)
    await sortByWeight(conn, ' 1', fastFloat ? NOT_DOUBLE : ex)
    // Both score 0: the tie breaks on the element, `e` before `x`.
    await sortByWeight(
      conn,
      '',
      valkeyStrtod ? NOT_DOUBLE : bulkArray(['e', 'x']),
    )
    await sortByWeight(conn, '1 ', NOT_DOUBLE)
    await sortByWeight(conn, 'nan', NOT_DOUBLE)
  })

  test('LIMIT takes the full int64 range on every profile', async () => {
    const conn = await connect()
    const key = `sortdbl:${randomKey()}:limit`
    await expectReply(conn, ['RPUSH', key, '3', '1', '2'], ':3\r\n')
    const all = bulkArray(['1', '2', '3'])
    const rows: [string, string, string][] = [
      ['0', INT64_MAX, all],
      [INT64_MIN, '1', bulkArray(['1'])],
      [INT64_MAX, '1', '*0\r\n'],
      ['0', INT64_MIN, all],
      ['1', INT64_MAX, bulkArray(['2', '3'])],
      ['2', INT64_MAX, bulkArray(['3'])],
      [INT64_MAX, INT64_MAX, '*0\r\n'],
      [INT64_MIN, INT64_MIN, all],
      ['0', '0', '*0\r\n'],
      ['0', '9223372036854775808', NOT_INTEGER],
      ['0', '99999999999999999999', NOT_INTEGER],
      ['-9223372036854775809', '1', NOT_INTEGER],
      ['0x1', '1', NOT_INTEGER],
      [' 1', '1', NOT_INTEGER],
      ['1.0', '1', NOT_INTEGER],
      ['+1', '1', NOT_INTEGER],
      ['01', '1', NOT_INTEGER],
    ]
    for (const [offset, count, expected] of rows) {
      await expectReply(conn, ['SORT', key, 'LIMIT', offset, count], expected)
    }
    await expectReply(
      conn,
      ['SORT', key, 'LIMIT', '0', INT64_MAX, 'DESC'],
      bulkArray(['3', '2', '1']),
    )
    await expectReply(
      conn,
      ['SORT', key, 'BY', 'nosort', 'LIMIT', '1', INT64_MAX],
      bulkArray(['1', '2']),
    )
    await expectReply(
      conn,
      ['SORT', key, 'ALPHA', 'LIMIT', INT64_MIN, INT64_MAX],
      all,
    )
    const dest = `sortdbl:${randomKey()}:dest`
    await expectReply(
      conn,
      ['SORT', key, 'LIMIT', '0', INT64_MAX, 'STORE', dest],
      ':3\r\n',
    )
    await expectReply(conn, ['LRANGE', dest, '0', '-1'], all)
    await expectReply(
      conn,
      [
        'SORT',
        `sortdbl:${randomKey()}:missing`,
        'LIMIT',
        '0',
        '9223372036854775808',
      ],
      NOT_INTEGER,
    )
    if (hasSortRo) {
      await expectReply(
        conn,
        ['SORT_RO', key, 'LIMIT', '1', INT64_MAX],
        bulkArray(['2', '3']),
      )
    }
  })

  // Last in the file: see the header about errno.
  describe('range errors (ERANGE)', () => {
    test('overflow and underflow follow the profile', async () => {
      const conn = await connect()
      // Redis 8.0's fast_float never sets errno; glibc strtod (Redis <= 7.4,
      // Valkey 8.0) flags an inexact subnormal too, valkey_strtod does not.
      const redis8 = profile === 'redis-8.0'
      await sortWith(
        conn,
        '1e400',
        redis8 ? bulkArray(['-5', '2', '1e400']) : NOT_DOUBLE,
      )
      await sortWith(
        conn,
        '-1e400',
        redis8 ? bulkArray(['-1e400', '-5', '2']) : NOT_DOUBLE,
      )
      await sortWith(
        conn,
        '1e-400',
        redis8 ? bulkArray(['-5', '1e-400', '2']) : NOT_DOUBLE,
      )
      await sortWith(
        conn,
        '1e-310',
        fastFloat ? bulkArray(['-5', '1e-310', '2']) : NOT_DOUBLE,
      )
      await sortByWeight(
        conn,
        '1e400',
        redis8 ? bulkArray(['x', 'e']) : NOT_DOUBLE,
      )
    })
  })
})
