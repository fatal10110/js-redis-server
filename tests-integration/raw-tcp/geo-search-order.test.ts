import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * GEOSEARCH / GEOSEARCHSTORE argument order (#607).
 *
 * Real georadiusGeneric() reads FROM*, BY* and the options in one loop, in
 * any order: FROMMEMBER is looked up when the loop reaches it (only when the
 * key exists), FROMLONLAT / BYRADIUS / BYBOX are parsed on the spot, repeats
 * are allowed (the last one wins) and ANY is a plain flag. After the loop
 * come, in order: STORE with WITH* (GEOSEARCHSTORE), no FROM, no BY, ANY
 * without COUNT, then the missing-key early return. WRONGTYPE precedes it all.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const isRedis62 = activeProfile === 'redis-6.2'
const isValkey90 = activeProfile === 'valkey-9.0'

const PALERMO = '*1\r\n$7\r\nPalermo\r\n'
const SYNTAX = '-ERR syntax error\r\n'
const UNIT_ERROR = isRedis62
  ? '-ERR unsupported unit provided. please use m, km, ft, mi\r\n'
  : '-ERR unsupported unit provided. please use M, KM, FT, MI\r\n'
const STORE_WITH_OPTIONS = isRedis62
  ? '-ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options\r\n'
  : '-ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n'
const ANY_ERROR = '-ERR the ANY argument requires COUNT argument\r\n'

function missingMember(member: string): string {
  return isValkey90
    ? `-ERR member ${member} does not exist\r\n`
    : '-ERR could not decode requested zset member\r\n'
}

function noFrom(command: string): string {
  return `-ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for ${command}\r\n`
}

function noBy(command: string): string {
  return `-ERR exactly one of BYRADIUS and BYBOX can be specified for ${command}\r\n`
}

describe(`Raw TCP GEOSEARCH argument order (${testRunner.getBackendName()})`, () => {
  let conn: RawRedisConnection
  let key: string
  let miss: string

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
    key = randomKey('geo-order')
    miss = randomKey('geo-order-missing')
    await expectReply(
      conn,
      [
        'GEOADD',
        key,
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
      ],
      ':2\r\n',
    )
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  test('FROM, BY and options in any order', async () => {
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '1', 'km', 'FROMMEMBER', 'Palermo'],
      PALERMO,
    )
    await expectReply(
      conn,
      ['geosearch', key, 'byradius', '1', 'km', 'frommember', 'Palermo'],
      PALERMO,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'ASC',
        'BYRADIUS',
        '1',
        'km',
        'FROMLONLAT',
        '13.361389',
        '38.115556',
      ],
      PALERMO,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'COUNT',
        '2',
        'DESC',
        'BYBOX',
        '1000',
        '1000',
        'km',
        'FROMMEMBER',
        'Palermo',
        'WITHDIST',
      ],
      '*2\r\n*2\r\n$7\r\nCatania\r\n$8\r\n166.2742\r\n*2\r\n$7\r\nPalermo\r\n$6\r\n0.0000\r\n',
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'WITHDIST',
        'FROMLONLAT',
        '13.36',
        '38.11',
        'BYBOX',
        '500',
        '500',
        'km',
        'COUNT',
        '1',
      ],
      '*1\r\n*2\r\n$7\r\nPalermo\r\n$6\r\n0.6299\r\n',
    )

    const d = randomKey('geo-order-d')
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        key,
        'COUNT',
        '1',
        'BYBOX',
        '1',
        '1',
        'km',
        'FROMMEMBER',
        'Palermo',
      ],
      ':1\r\n',
    )
    await expectReply(conn, ['ZRANGE', d, '0', '-1'], PALERMO)
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        key,
        'BYBOX',
        '1000',
        '1000',
        'km',
        'STOREDIST',
        'COUNT',
        '5',
        'FROMLONLAT',
        '13.36',
        '38.11',
      ],
      ':2\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', d, '0', '-1'],
      '*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n',
    )
  })

  test('missing key', async () => {
    await expectReply(
      conn,
      ['GEOSEARCH', miss, 'BYRADIUS', '1', 'km', 'FROMLONLAT', '0', '0'],
      '*0\r\n',
    )
    await expectReply(
      conn,
      ['GEOSEARCH', miss, 'BYRADIUS', '1', 'km', 'FROMMEMBER', 'a'],
      '*0\r\n',
    )
    const d = randomKey('geo-order-d')
    await expectReply(conn, ['SET', d, '1'], '+OK\r\n')
    await expectReply(
      conn,
      ['GEOSEARCHSTORE', d, miss, 'BYRADIUS', '1', 'km', 'FROMMEMBER', 'a'],
      ':0\r\n',
    )
    await expectReply(conn, ['EXISTS', d], ':0\r\n')
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        miss,
        'BYRADIUS',
        '1',
        'km',
        'ASC',
        'DESC',
        'FROMLONLAT',
        '0',
        '0',
      ],
      ':0\r\n',
    )
  })

  test('repeated clauses: the last one wins', async () => {
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'FROMMEMBER',
        'Catania',
        'FROMMEMBER',
        'Palermo',
        'BYRADIUS',
        '1',
        'km',
      ],
      PALERMO,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'FROMLONLAT',
        '0',
        '0',
        'FROMLONLAT',
        '13.361389',
        '38.115556',
        'BYRADIUS',
        '1',
        'km',
      ],
      PALERMO,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'FROMMEMBER',
        'Palermo',
        'BYRADIUS',
        '1',
        'km',
        'BYRADIUS',
        '500',
        'km',
      ],
      '*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n',
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'ASC',
        'DESC',
        'FROMMEMBER',
        'Palermo',
        'BYBOX',
        '1',
        '1',
        'km',
      ],
      PALERMO,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'FROMMEMBER',
        'Palermo',
        'BYRADIUS',
        '500',
        'km',
        'COUNT',
        '2',
        'COUNT',
        '1',
      ],
      PALERMO,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        key,
        'FROMMEMBER',
        'Palermo',
        'BYRADIUS',
        '500',
        'km',
        'COUNT',
        '1',
        'COUNT',
        '0',
      ],
      '-ERR COUNT must be > 0\r\n',
    )
    const d = randomKey('geo-order-d')
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        key,
        'BYRADIUS',
        '1',
        'km',
        'FROMMEMBER',
        'Palermo',
        'FROMMEMBER',
        'Catania',
      ],
      ':1\r\n',
    )
    await expectReply(conn, ['ZRANGE', d, '0', '-1'], '*1\r\n$7\r\nCatania\r\n')

    // FROMMEMBER with FROMLONLAT, or BYRADIUS with BYBOX, still conflict.
    for (const args of [
      ['FROMMEMBER', 'Palermo', 'FROMLONLAT', '0', '0', 'BYRADIUS', '1', 'km'],
      ['FROMLONLAT', '0', '0', 'FROMMEMBER', 'Palermo', 'BYRADIUS', '1', 'km'],
      ['BYRADIUS', '1', 'km', 'BYBOX', '1', '1', 'km', 'FROMMEMBER', 'Palermo'],
    ]) {
      await expectReply(conn, ['GEOSEARCH', key, ...args], SYNTAX)
    }
  })

  test('ANY is a flag checked after the loop', async () => {
    for (const args of [
      [
        'COUNT',
        '1',
        'ANY',
        'BYRADIUS',
        '1',
        'km',
        'FROMLONLAT',
        '13.36',
        '38.11',
      ],
      ['ANY', 'COUNT', '1', 'FROMMEMBER', 'Palermo', 'BYRADIUS', '1', 'km'],
      [
        'COUNT',
        '1',
        'ASC',
        'ANY',
        'FROMMEMBER',
        'Palermo',
        'BYRADIUS',
        '1',
        'km',
      ],
    ]) {
      await expectReply(conn, ['GEOSEARCH', key, ...args], PALERMO)
    }
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'FROMMEMBER', 'Palermo', 'BYRADIUS', '1', 'km', 'ANY'],
      ANY_ERROR,
    )
    await expectReply(
      conn,
      ['GEOSEARCH', miss, 'FROMMEMBER', 'a', 'BYRADIUS', '1', 'km', 'ANY'],
      ANY_ERROR,
    )
    // The exactly-one checks come first.
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'ANY', 'BYRADIUS', '1', 'km', 'ASC'],
      noFrom('GEOSEARCH'),
    )
    // GEORADIUS shares the loop.
    await expectReply(
      conn,
      ['GEORADIUS', key, '13.36', '38.11', '1', 'km', 'ANY', 'COUNT', '1'],
      PALERMO,
    )
    await expectReply(
      conn,
      ['GEORADIUS', key, '13.36', '38.11', '1', 'km', 'ANY', 'ASC'],
      ANY_ERROR,
    )
  })

  test('errors in argument order', async () => {
    // A clause cut short is a syntax error once arity is satisfied.
    for (const args of [
      ['ASC', 'DESC', 'BYRADIUS', '1', 'km', 'FROMMEMBER'],
      ['ASC', 'BYRADIUS', '1', 'km', 'FROMLONLAT', '5'],
      ['FROMMEMBER', 'Palermo', 'ASC', 'BYRADIUS', '1'],
      ['ASC', 'FROMLONLAT', '0', '0', 'BYBOX', '1', '1'],
    ]) {
      await expectReply(conn, ['GEOSEARCH', key, ...args], SYNTAX)
    }

    // FROMMEMBER is looked up where it stands.
    for (const args of [
      ['BYRADIUS', '1', 'km', 'FROMMEMBER', 'nosuch'],
      ['BYRADIUS', '1', 'km', 'FROMMEMBER', 'nosuch', 'BYBOX', '1', '1', 'km'],
      ['FROMMEMBER', 'nosuch', 'BYRADIUS', 'abc', 'km'],
      ['FROMMEMBER', 'Palermo', 'FROMMEMBER', 'nosuch', 'BYRADIUS', '1', 'km'],
      ['FROMMEMBER', 'nosuch', 'FROMMEMBER', 'Palermo', 'BYRADIUS', '1', 'km'],
    ]) {
      await expectReply(
        conn,
        ['GEOSEARCH', key, ...args],
        missingMember('nosuch'),
      )
    }
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'ASC', 'BYRADIUS', 'abc', 'km', 'FROMMEMBER', 'x'],
      '-ERR need numeric radius\r\n',
    )
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '1', 'mm', 'FROMMEMBER', 'nosuch'],
      UNIT_ERROR,
    )
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '-1', 'km', 'FROMMEMBER', 'nosuch'],
      '-ERR radius cannot be negative\r\n',
    )
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '1', 'km', 'FROMLONLAT', 'x', 'y'],
      '-ERR value is not a valid float\r\n',
    )
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '1', 'km', 'FROMLONLAT', '200', '100'],
      '-ERR invalid longitude,latitude pair 200.000000,100.000000\r\n',
    )

    // A missing key skips the lookup, not the argument errors.
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        miss,
        'BYRADIUS',
        '1',
        'km',
        'FROMMEMBER',
        'a',
        'COUNT',
        '0',
      ],
      '-ERR COUNT must be > 0\r\n',
    )
    await expectReply(
      conn,
      [
        'GEOSEARCH',
        miss,
        'BYRADIUS',
        '1',
        'km',
        'FROMMEMBER',
        'a',
        'FROMLONLAT',
        '0',
        '0',
      ],
      SYNTAX,
    )

    // After the loop: no FROM, then no BY, with the name as sent.
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '1', 'km', 'ASC', 'WITHDIST'],
      noFrom('GEOSEARCH'),
    )
    await expectReply(
      conn,
      ['GEOSEARCH', miss, 'BYRADIUS', '1', 'km', 'ASC', 'WITHDIST'],
      noFrom('GEOSEARCH'),
    )
    await expectReply(
      conn,
      ['geosearch', key, 'BYRADIUS', '1', 'km', 'ASC', 'WITHDIST'],
      noFrom('geosearch'),
    )
    // Valkey 9.0 names BYPOLYGON in this error (the BYPOLYGON work).
    if (!isValkey90) {
      await expectReply(
        conn,
        ['GEOSEARCH', key, 'FROMLONLAT', '0', '0', 'ASC', 'WITHDIST'],
        noBy('GEOSEARCH'),
      )
    }

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '1', 'km', 'FROMMEMBER', 'Palermo'],
      '+QUEUED\r\n',
    )
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'BYRADIUS', '1', 'km', 'ASC', 'WITHDIST'],
      '+QUEUED\r\n',
    )
    await expectReply(conn, ['EXEC'], `*2\r\n${PALERMO}${noFrom('GEOSEARCH')}`)
  })

  test('GEOSEARCHSTORE errors after the loop', async () => {
    const d = randomKey('geo-order-d')
    await expectReply(conn, ['SET', d, '1'], '+OK\r\n')
    await expectReply(
      conn,
      ['GEOSEARCHSTORE', d, key, 'WITHDIST', 'ASC', 'BYRADIUS', '1', 'km'],
      STORE_WITH_OPTIONS,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        miss,
        'WITHDIST',
        'BYRADIUS',
        '1',
        'km',
        'FROMMEMBER',
        'a',
      ],
      STORE_WITH_OPTIONS,
    )
    await expectReply(conn, ['GET', d], '$1\r\n1\r\n')

    // The member lookup, wherever it stands, beats the post-loop check.
    for (const args of [
      ['WITHDIST', 'FROMMEMBER', 'nosuch', 'BYRADIUS', '1', 'km'],
      ['BYRADIUS', '1', 'km', 'WITHDIST', 'FROMMEMBER', 'nosuch'],
      ['FROMMEMBER', 'nosuch', 'WITHDIST', 'BYRADIUS', '1', 'km'],
    ]) {
      await expectReply(
        conn,
        ['GEOSEARCHSTORE', d, key, ...args],
        missingMember('nosuch'),
      )
    }

    await expectReply(
      conn,
      ['GEOSEARCHSTORE', d, key, 'BYRADIUS', '1', 'km', 'ANY', 'ASC'],
      noFrom('GEOSEARCHSTORE'),
    )
    await expectReply(
      conn,
      ['GeoSearchStore', d, key, 'BYRADIUS', '1', 'km', 'ANY', 'ASC'],
      noFrom('GeoSearchStore'),
    )
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        key,
        'STOREDIST',
        'ASC',
        'DESC',
        'BYRADIUS',
        '1',
        'km',
      ],
      noFrom('GEOSEARCHSTORE'),
    )
    if (!isValkey90) {
      await expectReply(
        conn,
        [
          'GEOSEARCHSTORE',
          d,
          key,
          'FROMMEMBER',
          'Palermo',
          'ASC',
          'DESC',
          'STOREDIST',
        ],
        noBy('GEOSEARCHSTORE'),
      )
    }
    await expectReply(conn, ['GET', d], '$1\r\n1\r\n')
  })

  test('WRONGTYPE before the arguments, in any order', async () => {
    const str = randomKey('geo-order-str')
    const d = randomKey('geo-order-d')
    await expectReply(conn, ['SET', str, 'v'], '+OK\r\n')
    for (const args of [
      ['GEOSEARCH', str, 'ASC', 'BYRADIUS', 'abc', 'km', 'FROMMEMBER', 'x'],
      ['GEOSEARCH', str, 'BYRADIUS', '1', 'km', 'ASC', 'WITHDIST'],
      ['GEOSEARCHSTORE', d, str, 'BYRADIUS', '1', 'km', 'FROMMEMBER', 'a'],
      ['GEOSEARCHSTORE', d, str, 'WITHDIST', 'ASC', 'BYRADIUS', '1', 'km'],
    ]) {
      await expectReply(
        conn,
        args,
        '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n',
      )
    }
  })
})
