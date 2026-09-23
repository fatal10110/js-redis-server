import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply, expectReplyPrefix } from './helpers'

/**
 * Command and subcommand names fold ASCII only (#382).
 *
 * Real Redis matches names with a per-byte ASCII `tolower()`/`strcasecmp`, so
 * only `A-Z` is case-insensitive. JavaScript's `toLowerCase()`/`toUpperCase()`
 * are Unicode-aware and fold a few non-ASCII characters onto ASCII letters:
 *
 *  - U+212A KELVIN SIGN lowercases to `k`, so `<U+212A>eys` resolved to KEYS;
 *  - U+017F LATIN SMALL LETTER LONG S uppercases to `S`, so
 *    `XINFO <U+017F>TREAM` dispatched XINFO STREAM.
 *
 * U+017F only matters on the upper-casing paths (XINFO/XGROUP): it lowercases
 * to itself, so `<U+017F>et` never resolved to SET even before the fix. The
 * long-s command-name cases below are regression guards, not bug repros.
 *
 * Captured from real 8.0.6 — every one of these is rejected by name:
 *
 * ```
 * COMMAND INFO h<U+212A>eys          -> *1 $-1
 * COMMAND GETKEYS h<U+212A>eys h     -> -ERR Invalid command specified
 * COMMAND DOCS h<U+212A>eys          -> *0
 * COMMAND GET<U+212A>EYS SET k v     -> -ERR unknown subcommand 'GET<U+212A>EYS'. Try COMMAND HELP.
 * XINFO <U+017F>TREAM k              -> -ERR unknown subcommand '<U+017F>TREAM'. Try XINFO HELP.
 * EVAL "redis.pcall('h<U+212A>eys')" -> -ERR Unknown Redis command called from script
 * ```
 *
 * The *unknown command* replies are asserted by prefix only; their exact bytes
 * (the name echoed raw, NUL and length cuts) are pinned in
 * `unknown-command.test.ts`.
 */
const testRunner = new TestRunner()

// Built from code points, not written as literals: a raw look-alike in source
// is unreadable in review, and an editor or formatter normalizing it to ASCII
// would silently turn the Kelvin case into a real `KEYS *` on the shared
// backend.
const KELVIN = String.fromCodePoint(0x212a)
const LONG_S = String.fromCodePoint(0x017f)
const UNKNOWN_COMMAND = "-ERR unknown command '"

function unknownSubcommand(container: string, echoed: string): Buffer {
  return Buffer.from(
    `-ERR unknown subcommand '${echoed}'. Try ${container} HELP.\r\n`,
  )
}

describe(`Raw TCP ASCII-only command-name folding (${testRunner.getBackendName()})`, () => {
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

  // The issue's own repro. Sent only as the Kelvin spelling — real Redis
  // rejects it before anything runs, so the shared real server never sees a
  // KEYS scan.
  test('a Kelvin-sign spelling of KEYS is an unknown command', async () => {
    const conn = await connect()

    await expectReplyPrefix(conn, [`${KELVIN}eys`, '*'], UNKNOWN_COMMAND)
    await expectReplyPrefix(conn, [`${KELVIN}EYS`, '*'], UNKNOWN_COMMAND)
  })

  // Regression guard only: '<U+017F>' lowercases to itself, so this spelling was
  // already rejected before #382. It pins that the command path never grows
  // an upper-casing fold, which *would* map it onto SET.
  test('a long-s spelling of SET is an unknown command and writes nothing', async () => {
    const conn = await connect()
    const key = `fold:${randomKey()}`

    await expectReplyPrefix(conn, [`${LONG_S}et`, key, 'v'], UNKNOWN_COMMAND)
    await expectReplyPrefix(conn, [`${LONG_S}ET`, key, 'v'], UNKNOWN_COMMAND)
    await expectReply(conn, ['GET', key], '$-1\r\n')
  })

  test('COMMAND INFO / DOCS / GETKEYS do not resolve a non-ASCII fold', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['COMMAND', 'INFO', `h${KELVIN}eys`],
      '*1\r\n$-1\r\n',
    )
    await expectReply(conn, ['COMMAND', 'DOCS', `h${KELVIN}eys`], '*0\r\n')
    await expectReply(
      conn,
      ['COMMAND', 'GETKEYS', `h${KELVIN}eys`, 'h'],
      '-ERR Invalid command specified\r\n',
    )
  })

  test('subcommand dispatch does not resolve a non-ASCII fold', async () => {
    const conn = await connect()
    const key = `fold:${randomKey()}`

    // Lowercasing dispatch (COMMAND GETKEYS via the Kelvin sign).
    await expectReply(
      conn,
      ['COMMAND', `GET${KELVIN}EYS`, 'SET', key, 'v'],
      unknownSubcommand('COMMAND', `GET${KELVIN}EYS`),
    )
    // Uppercasing dispatch (XINFO STREAM / XGROUP DESTROY via the long s).
    await expectReply(
      conn,
      ['XINFO', `${LONG_S}TREAM`, key],
      unknownSubcommand('XINFO', `${LONG_S}TREAM`),
    )
    await expectReply(
      conn,
      ['XGROUP', `DE${LONG_S}TROY`, key, 'g'],
      unknownSubcommand('XGROUP', `DE${LONG_S}TROY`),
    )
  })

  test('redis.pcall from Lua does not resolve a non-ASCII fold', async () => {
    const conn = await connect()
    const key = `fold:${randomKey()}`

    await expectReply(
      conn,
      ['EVAL', `return redis.pcall('h${KELVIN}eys', KEYS[1])`, '1', key],
      '-ERR Unknown Redis command called from script\r\n',
    )
  })

  test('ASCII case-insensitivity is unaffected', async () => {
    const conn = await connect()
    const key = `fold:${randomKey()}`

    await expectReply(conn, ['sEt', key, 'v'], '+OK\r\n')
    await expectReply(conn, ['GeT', key], '$1\r\nv\r\n')
    await expectReply(conn, ['hKeYs', `${key}:missing`], '*0\r\n')
    await expectReply(
      conn,
      ['CoMmAnD', 'gEtKeYs', 'GET', key],
      `*1\r\n$${key.length}\r\n${key}\r\n`,
    )
    await expectReply(
      conn,
      ['XiNfO', 'StReAm', `${key}:missing`],
      '-ERR no such key\r\n',
    )
    await expectReply(conn, ['DEL', key], ':1\r\n')
  })
})
