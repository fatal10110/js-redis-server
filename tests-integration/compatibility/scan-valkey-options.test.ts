import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * The Valkey 8.0 SCAN-family options (#642), over a bare socket.
 *
 * `ZSCAN ... NOSCORES` (`zscan.noscores`) returns members only; the option
 * may repeat and mixes with MATCH / COUNT. SCAN, SSCAN and HSCAN answer
 * `NOSCORES option can only be used in ZSCAN` for it (valkey@8.0.0:src/db.c
 * `!o || o->type != OBJ_ZSET`), after the key lookup of a keyed scan. Redis
 * has no NOSCORES through 8.0: it is an unknown option, `syntax error`.
 *
 * `SCAN ... TYPE <unknown>` (`scan.type-unknown-error`) answers `unknown type
 * name '<arg>'` at the TYPE token, left to right; redis through 8.0 keeps the
 * error commented out and answers an empty scan, the last TYPE winning.
 *
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0, 8.1 and 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const isValkey = profile.startsWith('valkey')

const SYNTAX = '-ERR syntax error\r\n'
const ZSCAN_ONLY = '-ERR NOSCORES option can only be used in ZSCAN\r\n'
const EMPTY_SCAN = '*2\r\n$1\r\n0\r\n*0\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

function bulkArray(items: string[]): string {
  return `*${items.length}\r\n${items.map(item => `$${Buffer.byteLength(item)}\r\n${item}\r\n`).join('')}`
}

function scanReply(items: string[]): string {
  return `*2\r\n$1\r\n0\r\n${bulkArray(items)}`
}

function unknownType(name: string): string {
  return `-ERR unknown type name '${name}'\r\n`
}

describe(`Valkey SCAN options (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('ZSCAN NOSCORES returns members only on valkey', async () => {
    const conn = await connect()
    const zset = `scan-vk:${randomKey()}:zset`
    await expectReply(conn, ['ZADD', zset, '1', 'a', '2', 'b'], ':2\r\n')

    const rows: [string[], string[]][] = [
      [['NOSCORES'], ['a', 'b']],
      [
        ['noscores', 'NOSCORES'],
        ['a', 'b'],
      ],
      [['NOSCORES', 'MATCH', 'a*'], ['a']],
      [['COUNT', '10', 'NOSCORES', 'MATCH', 'b*'], ['b']],
      // (A listpack is one page whatever COUNT says on real servers; the
      // mock still pages it, #657, so COUNT stays above the size here.)
      [
        ['NOSCORES', 'COUNT', '10'],
        ['a', 'b'],
      ],
    ]
    for (const [options, members] of rows) {
      await expectReply(
        conn,
        ['ZSCAN', zset, '0', ...options],
        isValkey ? scanReply(members) : SYNTAX,
      )
    }
    // Without NOSCORES the reply is unchanged: member, score.
    await expectReply(
      conn,
      ['ZSCAN', zset, '0'],
      scanReply(['a', '1', 'b', '2']),
    )
    // Options are parsed left to right: the first bad one answers.
    await expectReply(
      conn,
      ['ZSCAN', zset, '0', 'NOSCORES', 'NOVALUES'],
      isValkey ? '-ERR NOVALUES option can only be used in HSCAN\r\n' : SYNTAX,
    )
    await expectReply(conn, ['ZSCAN', zset, '0', 'BADOPT', 'NOSCORES'], SYNTAX)
  })

  test('NOSCORES on SCAN / SSCAN / HSCAN follows the profile', async () => {
    const conn = await connect()
    const set = `scan-vk:${randomKey()}:set`
    const hash = `scan-vk:${randomKey()}:hash`
    const missing = `scan-vk:${randomKey()}:missing`
    const zset = `scan-vk:${randomKey()}:zset`
    await expectReply(conn, ['SADD', set, 'a'], ':1\r\n')
    await expectReply(conn, ['HSET', hash, 'f', 'v'], ':1\r\n')
    await expectReply(conn, ['ZADD', zset, '1', 'a'], ':1\r\n')
    const refused = isValkey ? ZSCAN_ONLY : SYNTAX

    await expectReply(conn, ['SCAN', '0', 'NOSCORES'], refused)
    await expectReply(conn, ['SSCAN', set, '0', 'NOSCORES'], refused)
    await expectReply(conn, ['HSCAN', hash, '0', 'NOSCORES'], refused)
    await expectReply(conn, ['SCAN', '0', 'BADOPT', 'NOSCORES'], SYNTAX)
    // Keyed scans look the key up first.
    await expectReply(conn, ['SSCAN', missing, '0', 'NOSCORES'], EMPTY_SCAN)
    await expectReply(conn, ['ZSCAN', missing, '0', 'NOSCORES'], EMPTY_SCAN)
    await expectReply(conn, ['HSCAN', zset, '0', 'NOSCORES'], WRONGTYPE)
    await expectReply(conn, ['ZSCAN', hash, '0', 'NOSCORES'], WRONGTYPE)
  })

  test('SCAN TYPE with an unknown type name follows the profile', async () => {
    const conn = await connect()
    const prefix = `scan-vk:${randomKey()}`
    const key = `${prefix}:str`
    await expectReply(conn, ['SET', key, 'v'], '+OK\r\n')
    const scan = (...options: string[]) => [
      'SCAN',
      '0',
      'MATCH',
      `${prefix}:*`,
      'COUNT',
      '1000000',
      ...options,
    ]

    await expectReply(
      conn,
      scan('TYPE', 'foo'),
      isValkey ? unknownType('foo') : EMPTY_SCAN,
    )
    // The name is quoted as sent.
    await expectReply(
      conn,
      scan('TYPE', 'Foo'),
      isValkey ? unknownType('Foo') : EMPTY_SCAN,
    )
    // Valkey checks each TYPE as it reaches it; redis keeps the last one.
    await expectReply(
      conn,
      scan('TYPE', 'foo', 'TYPE', 'string'),
      isValkey ? unknownType('foo') : scanReply([key]),
    )
    await expectReply(
      conn,
      scan('TYPE', 'string', 'TYPE', 'foo'),
      isValkey ? unknownType('foo') : EMPTY_SCAN,
    )
    // Known names match case-insensitively on every profile.
    await expectReply(conn, scan('TYPE', 'STRING'), scanReply([key]))
    for (const name of ['list', 'set', 'zset', 'hash', 'stream']) {
      await expectReply(conn, scan('TYPE', name), EMPTY_SCAN)
    }
    // Left to right: an earlier bad option answers first.
    await expectReply(conn, ['SCAN', '0', 'COUNT', '0', 'TYPE', 'foo'], SYNTAX)
    await expectReply(
      conn,
      ['SCAN', '0', 'TYPE', 'foo', 'COUNT', '0'],
      isValkey ? unknownType('foo') : SYNTAX,
    )
    await expectReply(conn, ['SCAN', '0', 'TYPE'], SYNTAX)
  })

  test('queued and scripted errors match the profile', async () => {
    const conn = await connect()
    const zset = `scan-vk:${randomKey()}:zset`
    await expectReply(conn, ['ZADD', zset, '1', 'a', '2', 'b'], ':2\r\n')

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(
      conn,
      [
        'SCAN',
        '0',
        'MATCH',
        `${zset}-none*`,
        'COUNT',
        '1000000',
        'TYPE',
        'foo',
      ],
      '+QUEUED\r\n',
    )
    await expectReply(conn, ['ZSCAN', zset, '0', 'NOSCORES'], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['EXEC'],
      isValkey
        ? `*2\r\n${unknownType('foo')}${scanReply(['a', 'b'])}`
        : `*2\r\n${EMPTY_SCAN}${SYNTAX}`,
    )

    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('ZSCAN', KEYS[1], '0', 'NOSCORES')",
        '1',
        zset,
      ],
      isValkey ? scanReply(['a', 'b']) : SYNTAX,
    )
  })

  test('ZSCAN NOSCORES at RESP3 is the same flat array', async () => {
    const conn = await connect()
    const zset = `scan-vk:${randomKey()}:zset`
    conn.write(commandFrame('HELLO', '3'))
    await conn.readRawFrame()
    await expectReply(conn, ['ZADD', zset, '1', 'a', '2', 'b'], ':2\r\n')

    conn.write(commandFrame('ZSCAN', zset, '0', 'NOSCORES'))
    const reply = (await conn.readRawFrame()).toString('binary')
    assert.strictEqual(reply, isValkey ? scanReply(['a', 'b']) : SYNTAX)
  })
})
