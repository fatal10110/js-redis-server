import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * SCAN-family option rows, over a bare socket so option shapes a typed client
 * will not send (a dangling `MATCH`, `NOVALUES` on SSCAN) still reach the
 * server.
 *
 * `NOVALUES` (#214) is Redis 7.4 / Valkey 8.0 (`hscan.novalues`). Before it
 * the word is an unknown option, `syntax error`, on every command. With it,
 * HSCAN returns field names only and SCAN / SSCAN / ZSCAN answer `NOVALUES
 * option can only be used in HSCAN`. Keyed scans parse options only after the
 * key lookup, so a missing key answers the empty scan reply on every profile.
 * Checked against redis-server 6.2.24, 7.2, 7.4.0, 7.4.4, 8.0.6 and
 * valkey-server 7.2.14, 8.0.0, 8.0.11, 8.1 and 9.0.6.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const hasNoValues = !['redis-6.2', 'redis-7.0', 'redis-7.2'].includes(profile)

const SYNTAX = '-ERR syntax error\r\n'
const HSCAN_ONLY = '-ERR NOVALUES option can only be used in HSCAN\r\n'
const EMPTY_SCAN = '*2\r\n$1\r\n0\r\n*0\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

function bulkArray(items: string[]): string {
  return `*${items.length}\r\n${items.map(item => `$${Buffer.byteLength(item)}\r\n${item}\r\n`).join('')}`
}

function scanReply(items: string[]): string {
  return `*2\r\n$1\r\n0\r\n${bulkArray(items)}`
}

describe(`SCAN option rows (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('HSCAN NOVALUES follows the profile', async () => {
    const conn = await connect()
    const hash = `scan-opt:${randomKey()}:hash`
    await expectReply(conn, ['HSET', hash, 'f1', 'v1'], ':1\r\n')

    await expectReply(
      conn,
      ['HSCAN', hash, '0', 'NOVALUES'],
      hasNoValues ? scanReply(['f1']) : SYNTAX,
    )
    await expectReply(
      conn,
      ['HSCAN', hash, '0', 'novalues', 'MATCH', 'f*', 'NOVALUES'],
      hasNoValues ? scanReply(['f1']) : SYNTAX,
    )
    // Without NOVALUES the reply is unchanged: field, value.
    await expectReply(conn, ['HSCAN', hash, '0'], scanReply(['f1', 'v1']))
  })

  test('NOVALUES on SCAN / SSCAN / ZSCAN follows the profile', async () => {
    const conn = await connect()
    const set = `scan-opt:${randomKey()}:set`
    const zset = `scan-opt:${randomKey()}:zset`
    await expectReply(conn, ['SADD', set, 'a'], ':1\r\n')
    await expectReply(conn, ['ZADD', zset, '1', 'a'], ':1\r\n')
    const refused = hasNoValues ? HSCAN_ONLY : SYNTAX

    await expectReply(conn, ['SSCAN', set, '0', 'NOVALUES'], refused)
    await expectReply(conn, ['ZSCAN', zset, '0', 'NOVALUES'], refused)
    await expectReply(conn, ['SCAN', '0', 'NOVALUES'], refused)
    await expectReply(conn, ['SCAN', '0', 'NOVALUES', 'BADOPT'], refused)
    // Options are parsed left to right: the first bad one answers.
    await expectReply(conn, ['SCAN', '0', 'BADOPT', 'NOVALUES'], SYNTAX)
    await expectReply(
      conn,
      ['SSCAN', set, '0', 'COUNT', '0', 'NOVALUES'],
      SYNTAX,
    )
  })

  test('keyed scans parse options after the key lookup', async () => {
    const conn = await connect()
    const missing = `scan-opt:${randomKey()}:missing`
    const string = `scan-opt:${randomKey()}:string`
    await expectReply(conn, ['SET', string, 'x'], '+OK\r\n')

    await expectReply(conn, ['HSCAN', missing, '0', 'BADOPT'], EMPTY_SCAN)
    await expectReply(conn, ['HSCAN', missing, '0', 'MATCH'], EMPTY_SCAN)
    await expectReply(conn, ['SSCAN', missing, '0', 'NOVALUES'], EMPTY_SCAN)
    await expectReply(conn, ['SSCAN', missing, '0', 'TYPE', 'set'], EMPTY_SCAN)
    await expectReply(conn, ['ZSCAN', missing, '0', 'NOSCORES'], EMPTY_SCAN)
    await expectReply(conn, ['HSCAN', string, '0', 'BADOPT'], WRONGTYPE)
    await expectReply(conn, ['HSCAN', string, '0', 'NOVALUES'], WRONGTYPE)
    // The cursor is checked before the lookup.
    await expectReply(
      conn,
      ['HSCAN', missing, 'abc', 'BADOPT'],
      '-ERR invalid cursor\r\n',
    )
  })

  test('an option missing its value is a syntax error', async () => {
    const conn = await connect()
    const hash = `scan-opt:${randomKey()}:hash`
    await expectReply(conn, ['HSET', hash, 'f1', 'v1'], ':1\r\n')

    await expectReply(conn, ['HSCAN', hash, '0', 'MATCH'], SYNTAX)
    await expectReply(conn, ['HSCAN', hash, '0', 'COUNT'], SYNTAX)
    await expectReply(conn, ['SCAN', '0', 'MATCH'], SYNTAX)
    await expectReply(conn, ['SCAN', '0', 'COUNT'], SYNTAX)
    await expectReply(conn, ['SCAN', '0', 'TYPE'], SYNTAX)
  })

  test('a queued NOVALUES error is reported at EXEC', async () => {
    const conn = await connect()
    const set = `scan-opt:${randomKey()}:set`
    await expectReply(conn, ['SADD', set, 'a'], ':1\r\n')

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SSCAN', set, '0', 'NOVALUES'], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['EXEC'],
      `*1\r\n${hasNoValues ? HSCAN_ONLY : SYNTAX}`,
    )
  })
})
