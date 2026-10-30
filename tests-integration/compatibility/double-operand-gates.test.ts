import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * Double operands (#534), over a bare socket so spaces, empty tokens and NUL
 * bytes reach the server as sent.
 *
 *  - `string2d()` (ZADD, ZINCRBY, WEIGHTS, GEO*): the same on every profile,
 *    except that hex floats (`0x10`, `0x1p3`) are refused from Valkey 8.1
 *    (`double.valkey-strtod`).
 *  - `zslParseRange()` (ZRANGEBYSCORE, ZCOUNT, ZRANGE BYSCORE, ...): C
 *    `strtod` up to Redis 7.4 / Valkey 8.0, which also takes hex floats and
 *    skips leading whitespace; fast_float from Redis 8.0 / Valkey 8.1
 *    (`double.fast-float`). Neither checks errno: `1e400` is infinity, an
 *    empty bound is 0, and a NUL byte ends the bound.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const fastFloat = profile === 'redis-8.0' || profile === 'valkey-9.0'
const valkeyStrtod = profile === 'valkey-9.0'

const NOT_FLOAT = '-ERR value is not a valid float\r\n'
const WEIGHT_NOT_FLOAT = '-ERR weight value is not a float\r\n'
const MIN_MAX = '-ERR min or max is not a float\r\n'

function bulkArray(items: string[]): string {
  return `*${items.length}\r\n${items.map(item => `$${Buffer.byteLength(item)}\r\n${item}\r\n`).join('')}`
}

function bulk(value: string): string {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`
}

describe(`double operands (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('ZADD takes every infinity spelling and refuses spaces and range errors', async () => {
    const conn = await connect()
    const key = `dbl:${randomKey()}:z`
    const accepted = ['inf', '+INF', '-Infinity', 'infinity', '.5', '5.', '1e5']
    for (const [i, score] of accepted.entries()) {
      await expectReply(conn, ['ZADD', key, score, `m${i}`], ':1\r\n')
    }
    for (const score of [
      ' 1',
      '1 ',
      '',
      'nan',
      '1e400',
      '1e-400',
      '0x',
      '0b11',
      '1_0',
      '0x1p9999',
    ]) {
      await expectReply(conn, ['ZADD', key, score, 'bad'], NOT_FLOAT)
    }
  })

  test('hex floats in string2d() operands follow the profile', async () => {
    const conn = await connect()
    const key = `dbl:${randomKey()}:hex`
    const rows = [
      ['0x10', '16'],
      ['0x1p3', '8'],
      ['0X1P-2', '0.25'],
      ['0x1.8', '1.5'],
      ['-0x10', '-16'],
      ['+0x10', '16'],
    ]
    for (const [i, [score, read]] of rows.entries()) {
      await expectReply(
        conn,
        ['ZADD', key, score!, `m${i}`],
        valkeyStrtod ? NOT_FLOAT : ':1\r\n',
      )
      await expectReply(
        conn,
        ['ZSCORE', key, `m${i}`],
        valkeyStrtod ? '$-1\r\n' : bulk(read!),
      )
    }

    const incr = `dbl:${randomKey()}:incr`
    await expectReply(
      conn,
      ['ZINCRBY', incr, '0x10', 'm'],
      valkeyStrtod ? NOT_FLOAT : bulk('16'),
    )
    await expectReply(
      conn,
      ['ZADD', incr, 'INCR', '0x1', 'n'],
      valkeyStrtod ? NOT_FLOAT : bulk('1'),
    )
    await expectReply(
      conn,
      ['ZADD', incr, 'GT', '0x1p3', 'n'],
      valkeyStrtod ? NOT_FLOAT : ':0\r\n',
    )

    const src = `dbl:${randomKey()}:src`
    await expectReply(conn, ['ZADD', src, '1', 'a'], ':1\r\n')
    await expectReply(
      conn,
      ['ZUNIONSTORE', `dbl:${randomKey()}:dst`, '1', src, 'WEIGHTS', '0x2'],
      valkeyStrtod ? WEIGHT_NOT_FLOAT : ':1\r\n',
    )
  })

  test('ZUNIONSTORE WEIGHTS takes infinity and refuses spaces and range errors', async () => {
    const conn = await connect()
    const src = `dbl:${randomKey()}:src`
    const dst = `dbl:${randomKey()}:dst`
    await expectReply(conn, ['ZADD', src, '1', 'a'], ':1\r\n')
    await expectReply(
      conn,
      ['ZUNIONSTORE', dst, '1', src, 'WEIGHTS', 'infinity'],
      ':1\r\n',
    )
    await expectReply(conn, ['ZSCORE', dst, 'a'], bulk('inf'))
    for (const weight of [' 1', '', 'nan', '1e400', 'abc']) {
      await expectReply(
        conn,
        ['ZUNIONSTORE', dst, '1', src, 'WEIGHTS', weight],
        WEIGHT_NOT_FLOAT,
      )
    }
  })

  test('GEO coordinates and distances parse like string2d()', async () => {
    const conn = await connect()
    const key = `dbl:${randomKey()}:geo`
    await expectReply(conn, ['GEOADD', key, '1', '1', 'm'], ':1\r\n')
    await expectReply(
      conn,
      ['GEOADD', key, 'inf', '1', 'x'],
      '-ERR invalid longitude,latitude pair inf,1.000000\r\n',
    )
    await expectReply(
      conn,
      ['GEOADD', key, '1', '-inf', 'x'],
      '-ERR invalid longitude,latitude pair 1.000000,-inf\r\n',
    )
    await expectReply(
      conn,
      ['GEOADD', key, '1e25', '1', 'x'],
      '-ERR invalid longitude,latitude pair 10000000000000000905969664.000000,1.000000\r\n',
    )
    await expectReply(conn, ['GEOADD', key, 'nan', '1', 'x'], NOT_FLOAT)
    await expectReply(conn, ['GEOADD', key, ' 1', '1', 'x'], NOT_FLOAT)
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'FROMLONLAT', 'inf', '1', 'BYRADIUS', '1', 'km'],
      '-ERR invalid longitude,latitude pair inf,1.000000\r\n',
    )
    for (const radius of ['1e400', ' 1', 'abc', '']) {
      await expectReply(
        conn,
        ['GEORADIUS', key, '1', '1', radius, 'km'],
        '-ERR need numeric radius\r\n',
      )
    }

    // `h` lands about 111 km east of `m`, outside every search below.
    await expectReply(
      conn,
      ['GEOADD', key, '0x2', '1', 'h'],
      valkeyStrtod ? NOT_FLOAT : ':1\r\n',
    )
    const match = bulkArray(['m'])
    const hexRows: [string[], string][] = [
      [
        ['GEOSEARCH', key, 'FROMLONLAT', '0x1', '1', 'BYRADIUS', '1', 'km'],
        valkeyStrtod ? NOT_FLOAT : match,
      ],
      [
        ['GEOSEARCH', key, 'FROMMEMBER', 'm', 'BYRADIUS', '0x10', 'km'],
        valkeyStrtod ? '-ERR need numeric radius\r\n' : match,
      ],
      [
        ['GEOSEARCH', key, 'FROMLONLAT', '1', '1', 'BYBOX', '0x10', '1', 'km'],
        valkeyStrtod ? '-ERR need numeric width\r\n' : match,
      ],
      [
        ['GEOSEARCH', key, 'FROMLONLAT', '1', '1', 'BYBOX', '1', '0x10', 'km'],
        valkeyStrtod ? '-ERR need numeric height\r\n' : match,
      ],
      [
        ['GEORADIUS', key, '1', '1', '0x10', 'km'],
        valkeyStrtod ? '-ERR need numeric radius\r\n' : match,
      ],
      [
        ['GEORADIUSBYMEMBER', key, 'm', '0x10', 'km'],
        valkeyStrtod ? '-ERR need numeric radius\r\n' : match,
      ],
    ]
    for (const [args, expected] of hexRows) {
      await expectReply(conn, args, expected)
    }
  })

  describe('score ranges (zslParseRange)', () => {
    async function fixture(conn: RawRedisConnection): Promise<string> {
      const key = `dbl:${randomKey()}:range`
      await expectReply(
        conn,
        ['ZADD', key, '1', 'a', '8', 'm2', '16', 'm', '3', 'x'],
        ':4\r\n',
      )
      return key
    }

    test('identical on every profile', async () => {
      const conn = await connect()
      const key = await fixture(conn)
      const all = bulkArray(['a', 'x', 'm2', 'm'])
      const low = bulkArray(['a', 'x'])
      const rows: [string, string, string][] = [
        ['1e400', '+inf', '*0\r\n'],
        ['-inf', '1e400', all],
        ['-1e400', '5', low],
        ['1e-400', '+inf', all],
        ['', '5', low],
        ['(', '5', low],
        ['(1', '(5', bulkArray(['x'])],
        ['inf', '5', '*0\r\n'],
        ['-infinity', '5', low],
        ['+INF', '5', '*0\r\n'],
        ['-inf', 'INFINITY', all],
        ['-inf', 'INF', all],
        ['-inf', 'infinity', all],
        ['1', '5\0', low],
        ['1 ', '+inf', MIN_MAX],
        ['1', '5\t', MIN_MAX],
        ['nan', '+inf', MIN_MAX],
        ['0x', '5', MIN_MAX],
        ['("', '5', MIN_MAX],
        ['\t', '5', MIN_MAX],
      ]
      for (const [min, max, expected] of rows) {
        await expectReply(conn, ['ZRANGEBYSCORE', key, min, max], expected)
      }
      await expectReply(conn, ['ZCOUNT', key, '', '1e400'], ':4\r\n')
    })

    test('hex floats and leading whitespace follow the profile', async () => {
      const conn = await connect()
      const key = await fixture(conn)
      const legacy = (expected: string) => (fastFloat ? MIN_MAX : expected)
      const all = bulkArray(['a', 'x', 'm2', 'm'])
      const rows: [string[], string][] = [
        [['ZRANGEBYSCORE', key, '0x1', '+inf'], all],
        [['ZRANGEBYSCORE', key, '(0x1', '+inf'], bulkArray(['x', 'm2', 'm'])],
        [['ZRANGEBYSCORE', key, '0x1p3', '+inf'], bulkArray(['m2', 'm'])],
        [['ZCOUNT', key, '0x1', '0x20'], ':4\r\n'],
        [['ZRANGE', key, '0x1', '0x20', 'BYSCORE'], all],
        [
          ['ZREVRANGEBYSCORE', key, '0x20', '0x1'],
          bulkArray(['m', 'm2', 'x', 'a']),
        ],
        [['ZRANGEBYSCORE', key, ' 1', '+inf'], all],
        [['ZRANGEBYSCORE', key, ' 0x10', '+inf'], bulkArray(['m'])],
        [['ZRANGEBYSCORE', key, '\t1', '5'], bulkArray(['a', 'x'])],
        [['ZREMRANGEBYSCORE', key, '0x1', '0x2'], ':1\r\n'],
      ]
      for (const [args, expected] of rows) {
        await expectReply(conn, args, legacy(expected))
      }
    })
  })
})
