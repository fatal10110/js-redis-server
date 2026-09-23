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
 *  - U+212A KELVIN SIGN lowercases to `k`, so `Keys` resolved to KEYS;
 *  - U+017F LATIN SMALL LETTER LONG S uppercases to `S`, so `XINFO ſTREAM`
 *    dispatched XINFO STREAM.
 *
 * Captured from real 8.0.6 — every one of these is rejected by name:
 *
 * ```
 * COMMAND INFO hKeys           -> *1 $-1
 * COMMAND GETKEYS hKeys h      -> -ERR Invalid command specified
 * COMMAND DOCS hKeys           -> *0
 * COMMAND GETKEYS SET k v      -> -ERR unknown subcommand 'GETKEYS'. Try COMMAND HELP.
 * XINFO ſTREAM k               -> -ERR unknown subcommand 'ſTREAM'. Try XINFO HELP.
 * EVAL "redis.pcall('hKeys')"  -> -ERR Unknown Redis command called from script
 * ```
 *
 * The *unknown command* replies are asserted by prefix only: real Redis echoes
 * the name's raw bytes, while the mock hex-escapes non-printable names
 * (`'0xe284aa...'`). That rendering is a separate fidelity gap, out of scope
 * here.
 */
const testRunner = new TestRunner()

const KELVIN = 'K'
const LONG_S = 'ſ'
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
