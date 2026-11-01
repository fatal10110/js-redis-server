import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * GEOPOS answers a member it cannot find with `addReplyNullArray` (#635):
 * `*-1` at RESP2 and `_` at RESP3, for a missing member and for every member
 * of a missing key. A script sees it as `false`, so `redis.pcall` hands it
 * back as a null bulk. GEODIST and GEOHASH keep their null bulk strings.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 9.0.0. Only the coordinate digits differ by profile:
 * redis 8.0 prints the shortest round-trip spelling.
 */
const testRunner = new TestRunner()
const shortest = activeProfile === 'redis-8.0'

function bulk(value: string): string {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`
}

const PALERMO = `*2\r\n${
  shortest
    ? bulk('13.361389338970184') + bulk('38.1155563954963')
    : bulk('13.36138933897018433') + bulk('38.11555639549629859')
}`

describe(`Raw TCP GEOPOS null replies (${testRunner.getBackendName()})`, () => {
  let resp2: RawRedisConnection
  let resp3: RawRedisConnection
  const p = `{${randomKey('geopos')}}`
  const geo = `${p}:geo`
  const missing = `${p}:missing`

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    resp2 = await RawRedisConnection.connect('127.0.0.1', port)
    resp3 = await RawRedisConnection.connect('127.0.0.1', port)
    resp3.write(commandFrame('HELLO', '3'))
    await resp3.readRawFrame()
    await expectReply(
      resp2,
      ['GEOADD', geo, '13.361389', '38.115556', 'Palermo'],
      ':1\r\n',
    )
  })

  after(async () => {
    resp2.close()
    resp3.close()
    await testRunner.cleanup()
  })

  test('a missing member is a null array at RESP2', async () => {
    await expectReply(
      resp2,
      ['GEOPOS', geo, 'Palermo', 'nope'],
      `*2\r\n${PALERMO}*-1\r\n`,
    )
    await expectReply(resp2, ['GEOPOS', geo, 'nope'], '*1\r\n*-1\r\n')
    await expectReply(resp2, ['GEOPOS', geo], '*0\r\n')
  })

  test('every member of a missing key is a null array at RESP2', async () => {
    await expectReply(resp2, ['GEOPOS', missing, 'a'], '*1\r\n*-1\r\n')
    await expectReply(
      resp2,
      ['GEOPOS', missing, 'a', 'b'],
      '*2\r\n*-1\r\n*-1\r\n',
    )
  })

  test('a missing member is a null inside EXEC', async () => {
    await expectReply(resp2, ['MULTI'], '+OK\r\n')
    await expectReply(resp2, ['GEOPOS', geo, 'nope'], '+QUEUED\r\n')
    await expectReply(resp2, ['EXEC'], '*1\r\n*1\r\n*-1\r\n')
  })

  test('a script sees the null array as a null bulk', async () => {
    await expectReply(
      resp2,
      [
        'EVAL',
        "return redis.pcall('GEOPOS', KEYS[1], 'nope', 'Palermo')",
        '1',
        geo,
      ],
      `*2\r\n$-1\r\n${PALERMO}`,
    )
  })

  test('RESP3 sends a null', async () => {
    const rows: [string[], string][] = [
      [['GEOPOS', geo, 'nope'], '*1\r\n_\r\n'],
      [['GEOPOS', missing, 'a', 'b'], '*2\r\n_\r\n_\r\n'],
    ]
    for (const [args, expected] of rows) {
      resp3.write(commandFrame(...args))
      const reply = (await resp3.readRawFrame()).toString('binary')
      assert.strictEqual(reply, expected, args.join(' '))
    }
  })

  test('GEODIST and GEOHASH keep null bulk strings', async () => {
    await expectReply(resp2, ['GEODIST', geo, 'Palermo', 'nope'], '$-1\r\n')
    await expectReply(resp2, ['GEODIST', missing, 'a', 'b'], '$-1\r\n')
    await expectReply(
      resp2,
      ['GEOHASH', geo, 'nope', 'Palermo'],
      `*2\r\n$-1\r\n${bulk('sqc8b49rny0')}`,
    )
    await expectReply(resp2, ['GEOHASH', missing, 'a'], '*1\r\n$-1\r\n')
  })
})
