import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * GEO search distances and missing source keys.
 *
 * STOREDIST (#586) stores `geohashGetDistance() / unit` as the score, and the
 * digits only match when the haversine is computed like Redis does it:
 * `deg_rad(a) = a * (M_PI / 180.0)`, the latitude-only shortcut for a
 * same-longitude pair, and the box test measuring longitude at the point's
 * latitude. A missing source key (#519) skips the member lookup and the
 * radius / unit parse of GEORADIUSBYMEMBER and GEOSEARCH FROMMEMBER: `*0`, or
 * `:0` after deleting the STORE destination. Option errors still fire.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0. The rows below spell the same on every profile
 * except where `percent17g` picks the 6.2 / 7.0 `%.17g` double spelling.
 */
const testRunner = new TestRunner()
const percent17g =
  activeProfile === 'redis-6.2' || activeProfile === 'redis-7.0'

function bulk(value: string): string {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`
}

describe(`Raw TCP GEO search edges (${testRunner.getBackendName()})`, () => {
  let conn: RawRedisConnection
  let key: string

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
    key = randomKey('geo-edges')
    await expectReply(
      conn,
      [
        'GEOADD',
        key,
        '15',
        '37',
        'a',
        '15',
        '38',
        'b',
        '16',
        '37',
        'c',
        '13.361389',
        '38.115556',
        'Palermo',
        '15.087269',
        '37.502669',
        'Catania',
      ],
      ':5\r\n',
    )
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  test('STOREDIST digits', async () => {
    const d = randomKey('geo-edges-d')
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', key, 'Palermo', '200', 'km', 'STOREDIST', d],
      ':4\r\n',
    )
    await expectReply(
      conn,
      ['ZSCORE', d, 'Catania'],
      bulk('166.27415156960032'),
    )

    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '500',
        'km',
        'STOREDIST',
      ],
      ':5\r\n',
    )
    await expectReply(
      conn,
      ['ZSCORE', d, 'Palermo'],
      bulk('190.44242984775784'),
    )
    await expectReply(conn, ['ZSCORE', d, 'b'], bulk('111.22641039916049'))
    await expectReply(
      conn,
      ['ZSCORE', d, 'a'],
      bulk(percent17g ? '8.4753771541937767e-05' : '8.475377154193777e-5'),
    )

    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        key,
        'FROMLONLAT',
        '15.5',
        '37.2',
        'BYBOX',
        '400',
        '400',
        'km',
        'STOREDIST',
      ],
      ':5\r\n',
    )
    await expectReply(
      conn,
      ['ZSCORE', d, 'Palermo'],
      bulk('214.07746530937885'),
    )
    await expectReply(conn, ['ZSCORE', d, 'c'], bulk('49.621793201597534'))

    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        d,
        key,
        'FROMMEMBER',
        'a',
        'BYRADIUS',
        '500',
        'mi',
        'STOREDIST',
      ],
      ':5\r\n',
    )
    await expectReply(
      conn,
      ['ZSCORE', d, 'Palermo'],
      bulk('118.33568455019316'),
    )
    await expectReply(conn, ['ZSCORE', d, 'a'], bulk('0'))
  })

  test('BYBOX measures longitude at the point latitude', async () => {
    const box = randomKey('geo-edges-box')
    await expectReply(conn, ['GEOADD', box, '1.5', '60', 'p'], ':1\r\n')
    // 1.5 degrees of longitude: about 83 km at 60N, 167 km at the equator.
    await expectReply(
      conn,
      ['GEOSEARCH', box, 'FROMLONLAT', '0', '0', 'BYBOX', '200', '20000', 'km'],
      '*1\r\n$1\r\np\r\n',
    )
  })

  test('missing source key', async () => {
    const missing = randomKey('geo-edges-missing')
    for (const args of [
      ['GEORADIUSBYMEMBER', missing, 'Palermo', '200', 'km'],
      ['GEORADIUSBYMEMBER', missing, 'Palermo', 'abc', 'xx'],
      ['GEORADIUSBYMEMBER_RO', missing, 'Palermo', '200', 'xx'],
      ['GEOSEARCH', missing, 'FROMMEMBER', 'Palermo', 'BYRADIUS', '200', 'km'],
      ['GEORADIUS', missing, '15', '37', '200', 'km'],
    ]) {
      await expectReply(conn, args, '*0\r\n')
    }

    const dest = randomKey('geo-edges-dest')
    await expectReply(conn, ['SET', dest, '1'], '+OK\r\n')
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', missing, 'Palermo', '200', 'km', 'STORE', dest],
      ':0\r\n',
    )
    await expectReply(conn, ['EXISTS', dest], ':0\r\n')
    await expectReply(conn, ['SET', dest, '1'], '+OK\r\n')
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', missing, 'Palermo', 'abc', 'km', 'STOREDIST', dest],
      ':0\r\n',
    )
    await expectReply(conn, ['GET', dest], '$-1\r\n')
    await expectReply(conn, ['SET', dest, '1'], '+OK\r\n')
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        dest,
        missing,
        'FROMMEMBER',
        'Palermo',
        'BYRADIUS',
        '200',
        'km',
      ],
      ':0\r\n',
    )
    await expectReply(conn, ['EXISTS', dest], ':0\r\n')

    // Options are still parsed, and so are GEOSEARCH's BY* arguments.
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', missing, 'Palermo', '200', 'km', 'FOO'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', missing, 'Palermo', 'abc', 'km', 'COUNT', '0'],
      '-ERR COUNT must be > 0\r\n',
    )
    await expectReply(
      conn,
      ['GEOSEARCH', missing, 'FROMMEMBER', 'Palermo', 'BYRADIUS', 'abc', 'km'],
      '-ERR need numeric radius\r\n',
    )

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', missing, 'Palermo', '200', 'km'],
      '+QUEUED\r\n',
    )
    await expectReply(conn, ['EXEC'], '*1\r\n*0\r\n')
  })

  test('WRONGTYPE before any argument error', async () => {
    const str = randomKey('geo-edges-str')
    const dest = randomKey('geo-edges-dest')
    await expectReply(conn, ['SET', str, 'v'], '+OK\r\n')
    for (const args of [
      ['GEORADIUSBYMEMBER', str, 'Palermo', 'abc', 'xx'],
      ['GEORADIUSBYMEMBER_RO', str, 'Palermo', '200', 'km', 'FOO'],
      ['GEORADIUS', str, '15', '37', '200', 'xx'],
      ['GEORADIUS_RO', str, '15', 'abc', '200', 'km'],
      ['GEOSEARCH', str, 'FROMLONLAT', '15', '37', 'BYRADIUS', '200', 'xx'],
      ['GEOSEARCH', str, 'FROMMEMBER', 'a', 'BYBOX', '1', '1', 'km', 'FOO'],
      [
        'GEOSEARCHSTORE',
        dest,
        str,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'km',
        'FOO',
      ],
    ]) {
      await expectReply(
        conn,
        args,
        '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n',
      )
    }
  })
})
