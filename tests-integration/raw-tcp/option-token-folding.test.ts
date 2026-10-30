import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Option keywords fold ASCII only (#508), like command names (#382).
 *
 * Real Redis compares option tokens with C `strcasecmp`, so only `A-Z` is
 * case-insensitive. JavaScript's `toUpperCase()` / `toLowerCase()` are
 * Unicode-aware: U+017F LATIN SMALL LETTER LONG S upper-cases to `S` and
 * U+212A KELVIN SIGN lower-cases to `k`, so the mock used to take
 * `WITH<U+017F>CORES` for WITHSCORES. Captured from real 6.2.14, 8.0.0 and
 * Valkey 9.0.0, which all reject these:
 *
 * ```
 * ZRANGE z 0 -1 WITH<U+017F>CORES        -> -ERR syntax error
 * SORT z <U+017F>tore dst                -> -ERR syntax error
 * SORT z BY nosort <U+017F>tore dst      -> -ERR syntax error
 * HRANDFIELD h 1 WITHVALUE<U+017F>       -> -ERR syntax error
 * BITFIELD k OVERFLOW <U+017F>AT          -> -ERR Invalid OVERFLOW type specified
 * GEODIST g a b <U+212A>M                 -> -ERR unsupported unit provided. please use M, KM, FT, MI
 * ```
 *
 * The lower-case ASCII spellings still work. The GEODIST rows pin the 7.0+
 * wording of this default-profile suite: 6.2 matches units case-sensitively
 * and lists them in lower case. The CONFIG parameter-name case
 * (`tcp-<U+212A>eepalive`), whose wording differs per version, is in
 * `tests-integration/compatibility/config-fidelity-gates.test.ts`.
 */
const testRunner = new TestRunner()

// Built from code points, not written as literals (see command-name-folding).
const KELVIN = String.fromCodePoint(0x212a)
const LONG_S = String.fromCodePoint(0x017f)
const SYNTAX_ERROR = '-ERR syntax error\r\n'

describe(`Raw TCP option token folding (${testRunner.getBackendName()})`, () => {
  let conn: RawRedisConnection

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  test('ZRANGE WITHSCORES', async () => {
    const key = randomKey('fold-zrange')
    await expectReply(conn, ['ZADD', key, '1', 'a'], ':1\r\n')
    await expectReply(
      conn,
      ['ZRANGE', key, '0', '-1', `WITH${LONG_S}CORES`],
      SYNTAX_ERROR,
    )
    await expectReply(
      conn,
      ['ZRANGE', key, '0', '-1', 'withscores'],
      '*2\r\n$1\r\na\r\n$1\r\n1\r\n',
    )
  })

  test('SORT STORE', async () => {
    const key = randomKey('{fold-sort}')
    const dst = `${key}:dst`
    await expectReply(conn, ['RPUSH', key, '2', '1'], ':2\r\n')
    await expectReply(conn, ['SORT', key, `${LONG_S}tore`, dst], SYNTAX_ERROR)
    await expectReply(
      conn,
      ['SORT', key, 'BY', 'nosort', `${LONG_S}tore`, dst],
      SYNTAX_ERROR,
    )
    await expectReply(conn, ['SORT', key, 'store', dst], ':2\r\n')
    await expectReply(
      conn,
      ['LRANGE', dst, '0', '-1'],
      '*2\r\n$1\r\n1\r\n$1\r\n2\r\n',
    )
  })

  test('HRANDFIELD WITHVALUES', async () => {
    const key = randomKey('fold-hrandfield')
    await expectReply(conn, ['HSET', key, 'f', 'v'], ':1\r\n')
    await expectReply(
      conn,
      ['HRANDFIELD', key, '1', `WITHVALUE${LONG_S}`],
      SYNTAX_ERROR,
    )
    await expectReply(
      conn,
      ['HRANDFIELD', key, '1', 'withvalues'],
      '*2\r\n$1\r\nf\r\n$1\r\nv\r\n',
    )
  })

  test('BITFIELD OVERFLOW', async () => {
    const key = randomKey('fold-bitfield')
    await expectReply(
      conn,
      ['BITFIELD', key, 'OVERFLOW', `${LONG_S}AT`, 'INCRBY', 'u8', '0', '1'],
      '-ERR Invalid OVERFLOW type specified\r\n',
    )
    await expectReply(
      conn,
      ['BITFIELD', key, 'overflow', 'sat', 'INCRBY', 'u8', '0', '1'],
      '*1\r\n:1\r\n',
    )
  })

  test('GEODIST unit', async () => {
    const key = randomKey('fold-geo')
    await expectReply(
      conn,
      [
        'GEOADD',
        key,
        '13.361389',
        '38.115556',
        'a',
        '15.087269',
        '37.502669',
        'b',
      ],
      ':2\r\n',
    )
    for (const unit of [`${KELVIN}M`, 'constructor']) {
      await expectReply(
        conn,
        ['GEODIST', key, 'a', 'b', unit],
        '-ERR unsupported unit provided. please use M, KM, FT, MI\r\n',
      )
    }
    await expectReply(
      conn,
      ['GEODIST', key, 'a', 'b', 'Km'],
      '$8\r\n166.2742\r\n',
    )
  })
})
