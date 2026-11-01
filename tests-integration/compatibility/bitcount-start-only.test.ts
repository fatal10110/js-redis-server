import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * `BITCOUNT key start` (no end) by profile (#643). Valkey 8.0 takes it, the
 * end defaulting to the last byte, and reads the BYTE|BIT unit before the end
 * (valkey@8.0.0 bitops.c:798-823), so `BITCOUNT s 0 BIT x` is a syntax error
 * there. Redis (every version) and valkey before 8.0 take only `start end`:
 * the start-only form is a syntax error. Redis 6.2 has no unit, so any
 * 4-argument form is a syntax error before the indexes are read. Checked
 * against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server
 * 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const startOnly = profile === 'valkey-8.0' || profile === 'valkey-9.0'
const legacy = profile === 'redis-6.2'
// Redis up to 7.2 looks the key up before it reads the range, so a missing
// or wrong-type key answers before the syntax error (#658).
const lookupFirst = legacy || profile === 'redis-7.0' || profile === 'redis-7.2'

const SYNTAX = '-ERR syntax error\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

describe(`BITCOUNT start-only by profile (${profile}, ${testRunner.getBackendName()})`, () => {
  let conn: RawRedisConnection
  let s: string
  let list: string

  before(async () => {
    const port = await testRunner.setupRawStandalone()
    conn = await RawRedisConnection.connect('127.0.0.1', port)
    s = `bitcount:${randomKey()}:s`
    list = `bitcount:${randomKey()}:l`
    await expectReply(conn, ['SET', s, 'abc'], '+OK\r\n')
    await expectReply(conn, ['LPUSH', list, 'a'], ':1\r\n')
  })

  after(async () => {
    conn.close()
    await testRunner.cleanup()
  })

  test('BITCOUNT key start', async () => {
    const rows: [string[], string][] = [
      [[s, '0'], ':10\r\n'],
      [[s, '1'], ':7\r\n'],
      [[s, '-1'], ':4\r\n'],
      [[s, '-100'], ':10\r\n'],
      [[s, '100'], ':0\r\n'],
      [[s, '16'], ':0\r\n'],
      [[s, '24'], ':0\r\n'],
      ...(lookupFirst
        ? []
        : ([
            [[`bitcount:${randomKey()}:missing`, '0'], ':0\r\n'],
            [[list, '0'], WRONGTYPE],
          ] as [string[], string][])),
      [[s, 'a'], NOT_INTEGER],
      [[s, '9223372036854775808'], NOT_INTEGER],
    ]
    for (const [args, reply] of rows) {
      await expectReply(conn, ['BITCOUNT', ...args], startOnly ? reply : SYNTAX)
    }
  })

  test('inside MULTI and from a script', async () => {
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['BITCOUNT', s, '1'], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['EXEC'],
      startOnly ? '*1\r\n:7\r\n' : `*1\r\n${SYNTAX}`,
    )
    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('BITCOUNT', KEYS[1], 1)", '1', s],
      startOnly ? ':7\r\n' : SYNTAX,
    )
  })

  test('the start/end forms and the unit order', async () => {
    await expectReply(conn, ['BITCOUNT', s], ':10\r\n')
    await expectReply(conn, ['BITCOUNT', s, '0', '-1'], ':10\r\n')
    await expectReply(conn, ['BITCOUNT', s, 'a', 'b'], NOT_INTEGER)
    await expectReply(conn, ['BITCOUNT', s, '0', '1', '2'], SYNTAX)
    // Three arguments are start, end: BIT is the end.
    await expectReply(conn, ['BITCOUNT', s, '0', 'BIT'], NOT_INTEGER)
    if (legacy) {
      // No BYTE|BIT on 6.2: four arguments are a syntax error outright.
      for (const tail of [
        ['1', '5', 'BIT'],
        ['0', 'BIT', 'x'],
        ['a', '1', 'BIT'],
        ['0', 'a', 'FOO'],
      ]) {
        await expectReply(conn, ['BITCOUNT', s, ...tail], SYNTAX)
      }
      return
    }
    await expectReply(conn, ['BITCOUNT', s, '1', '5', 'BIT'], ':2\r\n')
    await expectReply(conn, ['BITCOUNT', s, '1', '5', 'BYTE'], ':7\r\n')
    await expectReply(conn, ['BITCOUNT', s, 'a', '1', 'BIT'], NOT_INTEGER)
    await expectReply(conn, ['BITCOUNT', s, '0', 'a', 'BIT'], NOT_INTEGER)
    // Valkey 8 reads the unit before the end; Redis reads the end first.
    await expectReply(
      conn,
      ['BITCOUNT', s, '0', 'BIT', 'x'],
      startOnly ? SYNTAX : NOT_INTEGER,
    )
    await expectReply(
      conn,
      ['BITCOUNT', s, '0', 'a', 'FOO'],
      startOnly ? SYNTAX : NOT_INTEGER,
    )
  })
})
