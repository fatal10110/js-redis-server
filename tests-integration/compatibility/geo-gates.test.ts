import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * GEO wording and parsing that differ per version (#596).
 *
 *  - Units: Redis 6.2 matches `m`, `km`, `ft`, `mi` with `strcmp` and lists
 *    them in lower case in the error; 7.0+ and every Valkey fold ASCII case
 *    and list them in upper case (`geo.unit-case-folding`).
 *  - STORE / STOREDIST / GEOSEARCHSTORE with WITH*: 6.2 always says
 *    `STORE option in GEORADIUS ... WITHCOORDS options`; 7.0+ names
 *    GEOSEARCHSTORE for that command and spells `WITHCOORD`.
 *  - A FROMMEMBER / BYMEMBER member the key lacks: Valkey 9.0 replies
 *    `member <name> does not exist`, the others `could not decode requested
 *    zset member`.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const redis62 = profile === 'redis-6.2'

const UNIT_ERROR = redis62
  ? '-ERR unsupported unit provided. please use m, km, ft, mi\r\n'
  : '-ERR unsupported unit provided. please use M, KM, FT, MI\r\n'
const STORE_WITH_ERROR = redis62
  ? '-ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options\r\n'
  : '-ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n'
const SEARCHSTORE_WITH_ERROR = redis62
  ? STORE_WITH_ERROR
  : '-ERR GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options\r\n'

describe(`GEO gates (${testRunner.getBackendName()}, ${profile})`, () => {
  let conn: RawRedisConnection
  let key: string

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
    key = randomKey('geo-gates')
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

  test('units: case folding and the error wording', async () => {
    await expectReply(
      conn,
      ['GEODIST', key, 'Palermo', 'Catania', 'km'],
      '$8\r\n166.2742\r\n',
    )
    await expectReply(
      conn,
      ['GEODIST', key, 'Palermo', 'Catania', 'xx'],
      UNIT_ERROR,
    )
    for (const unit of ['Km', 'KM']) {
      await expectReply(
        conn,
        ['GEODIST', key, 'Palermo', 'Catania', unit],
        redis62 ? UNIT_ERROR : '$8\r\n166.2742\r\n',
      )
    }
    await expectReply(
      conn,
      ['GEORADIUS', key, '15', '37', '200', 'Km'],
      redis62 ? UNIT_ERROR : '*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n',
    )
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', key, 'Palermo', '200', 'FT'],
      redis62 ? UNIT_ERROR : '*1\r\n$7\r\nPalermo\r\n',
    )
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER_RO', key, 'Palermo', '200', 'xx'],
      UNIT_ERROR,
    )
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'FROMLONLAT', '15', '37', 'BYBOX', '100', '100', 'Mi'],
      redis62 ? UNIT_ERROR : '*1\r\n$7\r\nCatania\r\n',
    )
    const dest = randomKey('geo-gates-dest')
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        dest,
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'Km',
      ],
      redis62 ? UNIT_ERROR : ':2\r\n',
    )
  })

  test('STORE with WITH* options', async () => {
    const dest = randomKey('geo-gates-dest')
    await expectReply(
      conn,
      ['GEORADIUS', key, '15', '37', '200', 'km', 'WITHDIST', 'STORE', dest],
      STORE_WITH_ERROR,
    )
    await expectReply(
      conn,
      [
        'GEORADIUSBYMEMBER',
        key,
        'Palermo',
        '200',
        'km',
        'STOREDIST',
        dest,
        'WITHHASH',
      ],
      STORE_WITH_ERROR,
    )
    await expectReply(
      conn,
      [
        'GEOSEARCHSTORE',
        dest,
        key,
        'FROMLONLAT',
        '15',
        '37',
        'BYRADIUS',
        '200',
        'km',
        'WITHDIST',
      ],
      SEARCHSTORE_WITH_ERROR,
    )
  })

  test('a member the key lacks', async () => {
    const error =
      profile === 'valkey-9.0'
        ? '-ERR member nosuch does not exist\r\n'
        : '-ERR could not decode requested zset member\r\n'
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER', key, 'nosuch', '200', 'km'],
      error,
    )
    // The member is looked up before the radius is parsed.
    await expectReply(
      conn,
      ['GEORADIUSBYMEMBER_RO', key, 'nosuch', 'abc', 'xx'],
      error,
    )
    await expectReply(
      conn,
      ['GEOSEARCH', key, 'FROMMEMBER', 'nosuch', 'BYRADIUS', '200', 'km'],
      error,
    )
  })
})
