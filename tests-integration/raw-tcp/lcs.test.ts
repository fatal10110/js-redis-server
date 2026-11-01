import assert from 'node:assert'
import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN] (#206). Every
 * reply here is the same on redis 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey
 * 8.0.0 / 9.0.0 (t_string.c lcsCommand). The per-profile rows (LCS unknown on
 * 6.2, STRALGO only there, COMMAND INFO / DOCS) are in
 * compatibility/lcs-gates.test.ts.
 */
const testRunner = new TestRunner()

const NOT_STRINGS = '-ERR The specified keys must contain string values\r\n'
const LEN_WITH_IDX =
  '-ERR If you want both the length and indexes, please just use IDX.\r\n'
const SYNTAX = '-ERR syntax error\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const ARITY = "-ERR wrong number of arguments for 'lcs' command\r\n"

// ohmytext / mynewtext: LCS "mytext", matches [4,7]~[5,8] (4) and [2,3]~[0,1] (2).
const IDX_REPLY_BODY =
  '$7\r\nmatches\r\n*2\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n' +
  '*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n$3\r\nlen\r\n:6\r\n'

describe(`Raw TCP LCS (${testRunner.getBackendName()})`, () => {
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

  async function strings(
    conn: RawRedisConnection,
    a: string,
    b: string,
  ): Promise<{ ka: string; kb: string }> {
    const ka = `lcs:${randomKey()}:a`
    const kb = `lcs:${randomKey()}:b`
    await expectReply(conn, ['SET', ka, a], '+OK\r\n')
    await expectReply(conn, ['SET', kb, b], '+OK\r\n')
    return { ka, kb }
  }

  test('LCS answers the subsequence, its length, or its match ranges', async () => {
    const conn = await connect()
    const { ka, kb } = await strings(conn, 'ohmytext', 'mynewtext')

    await expectReply(conn, ['LCS', ka, kb], '$6\r\nmytext\r\n')
    await expectReply(conn, ['LCS', ka, kb, 'LEN'], ':6\r\n')
    await expectReply(conn, ['LCS', ka, kb, 'IDX'], `*4\r\n${IDX_REPLY_BODY}`)
    await expectReply(
      conn,
      ['LCS', ka, kb, 'IDX', 'WITHMATCHLEN'],
      '*4\r\n$7\r\nmatches\r\n*2\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n' +
        '*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n$3\r\nlen\r\n:6\r\n',
    )
    // MINMATCHLEN drops the shorter range; len stays the full LCS length.
    await expectReply(
      conn,
      ['LCS', ka, kb, 'IDX', 'MINMATCHLEN', '4'],
      '*4\r\n$7\r\nmatches\r\n*1\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n$3\r\nlen\r\n:6\r\n',
    )
    await expectReply(
      conn,
      ['lcs', ka, kb, 'idx', 'minmatchlen', '4', 'withmatchlen'],
      '*4\r\n$7\r\nmatches\r\n*1\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n$3\r\nlen\r\n:6\r\n',
    )
    // A negative MINMATCHLEN means no minimum; the last one given wins.
    await expectReply(
      conn,
      ['LCS', ka, kb, 'IDX', 'MINMATCHLEN', '-3'],
      `*4\r\n${IDX_REPLY_BODY}`,
    )
    await expectReply(
      conn,
      ['LCS', ka, kb, 'IDX', 'MINMATCHLEN', '9', 'MINMATCHLEN', '1'],
      `*4\r\n${IDX_REPLY_BODY}`,
    )
    // Without IDX, WITHMATCHLEN and MINMATCHLEN are accepted and ignored.
    await expectReply(
      conn,
      ['LCS', ka, kb, 'WITHMATCHLEN', 'MINMATCHLEN', '2'],
      '$6\r\nmytext\r\n',
    )
  })

  test('IDX lists the ranges real Redis finds, last range first, ties included', async () => {
    const conn = await connect()
    const first = await strings(conn, 'abcabba', 'cbabac')
    await expectReply(conn, ['LCS', first.ka, first.kb], '$4\r\ncbba\r\n')
    await expectReply(
      conn,
      ['LCS', first.ka, first.kb, 'IDX', 'WITHMATCHLEN'],
      '*4\r\n$7\r\nmatches\r\n*3\r\n*3\r\n*2\r\n:5\r\n:6\r\n*2\r\n:3\r\n:4\r\n:2\r\n' +
        '*3\r\n*2\r\n:4\r\n:4\r\n*2\r\n:1\r\n:1\r\n:1\r\n' +
        '*3\r\n*2\r\n:2\r\n:2\r\n*2\r\n:0\r\n:0\r\n:1\r\n$3\r\nlen\r\n:4\r\n',
    )

    const second = await strings(conn, 'abab', 'baba')
    await expectReply(conn, ['LCS', second.ka, second.kb], '$3\r\nbab\r\n')
    await expectReply(
      conn,
      ['LCS', second.ka, second.kb, 'IDX', 'WITHMATCHLEN'],
      '*4\r\n$7\r\nmatches\r\n*1\r\n*3\r\n*2\r\n:1\r\n:3\r\n*2\r\n:0\r\n:2\r\n:3\r\n$3\r\nlen\r\n:3\r\n',
    )
  })

  test('missing keys are empty strings', async () => {
    const conn = await connect()
    const x = `lcs:${randomKey()}:x`
    const y = `lcs:${randomKey()}:y`
    const { ka } = await strings(conn, 'ohmytext', 'unused')

    await expectReply(conn, ['LCS', x, y], '$0\r\n\r\n')
    await expectReply(conn, ['LCS', x, y, 'LEN'], ':0\r\n')
    await expectReply(
      conn,
      ['LCS', x, y, 'IDX'],
      '*4\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n',
    )
    await expectReply(conn, ['LCS', ka, y], '$0\r\n\r\n')
    // Option values are still parsed when the keys are missing.
    await expectReply(conn, ['LCS', x, y, 'MINMATCHLEN', 'abc'], NOT_INTEGER)
  })

  test('errors: arity, options, LEN with IDX, and keys of another type', async () => {
    const conn = await connect()
    const { ka, kb } = await strings(conn, 'ohmytext', 'mynewtext')
    const list = `lcs:${randomKey()}:list`
    await expectReply(conn, ['RPUSH', list, 'x'], ':1\r\n')

    await expectReply(conn, ['LCS'], ARITY)
    await expectReply(conn, ['LCS', ka], ARITY)
    await expectReply(conn, ['LCS', ka, kb, 'LEN', 'IDX'], LEN_WITH_IDX)
    await expectReply(conn, ['LCS', ka, kb, 'IDX', 'IDX', 'LEN'], LEN_WITH_IDX)
    await expectReply(conn, ['LCS', ka, kb, 'FOO'], SYNTAX)
    await expectReply(conn, ['LCS', ka, kb, 'MINMATCHLEN'], SYNTAX)
    await expectReply(conn, ['LCS', ka, kb, 'IDX', 'MINMATCHLEN'], SYNTAX)
    await expectReply(conn, ['LCS', ka, kb, 'MINMATCHLEN', 'abc'], NOT_INTEGER)
    await expectReply(
      conn,
      ['LCS', ka, kb, 'MINMATCHLEN', '99999999999999999999'],
      NOT_INTEGER,
    )
    // The first option error in argv order wins over LEN with IDX.
    await expectReply(conn, ['LCS', ka, kb, 'LEN', 'IDX', 'FOO'], SYNTAX)

    // Not WRONGTYPE, and checked before any option.
    await expectReply(conn, ['LCS', list, ka], NOT_STRINGS)
    await expectReply(conn, ['LCS', ka, list], NOT_STRINGS)
    await expectReply(conn, ['LCS', list, list, 'LEN', 'IDX'], NOT_STRINGS)
    await expectReply(conn, ['LCS', list, ka, 'FOO'], NOT_STRINGS)
  })

  test('a table over proto-max-bulk-len is refused before it is built', async () => {
    const conn = await connect()
    // (11586 + 1)^2 * 4 bytes is just over the default 512MB.
    const big = `lcs:${randomKey()}:big`
    await expectReply(conn, ['SETRANGE', big, '11585', 'x'], ':11586\r\n')
    const tooBig =
      '-ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len\r\n'
    await expectReply(conn, ['LCS', big, big], tooBig)
    await expectReply(conn, ['LCS', big, big, 'LEN'], tooBig)
    await expectReply(conn, ['LCS', big, big, 'IDX'], tooBig)
    // Option errors come first.
    await expectReply(conn, ['LCS', big, big, 'LEN', 'IDX'], LEN_WITH_IDX)
  })

  test('IDX is a map on RESP3; the other replies do not change', async () => {
    const conn = await connect()
    const { ka, kb } = await strings(conn, 'ohmytext', 'mynewtext')
    conn.write(commandFrame('HELLO', '3'))
    await conn.readRawFrame()

    await expectReply(conn, ['LCS', ka, kb, 'IDX'], `%2\r\n${IDX_REPLY_BODY}`)
    await expectReply(
      conn,
      ['LCS', `${ka}:missing`, `${kb}:missing`, 'IDX', 'WITHMATCHLEN'],
      '%2\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n',
    )
    await expectReply(conn, ['LCS', ka, kb], '$6\r\nmytext\r\n')
    await expectReply(conn, ['LCS', ka, kb, 'LEN'], ':6\r\n')
    await expectReply(conn, ['LCS', ka, kb, 'LEN', 'IDX'], LEN_WITH_IDX)
  })

  test('LCS from a script and inside MULTI', async () => {
    const conn = await connect()
    const { ka, kb } = await strings(conn, 'ohmytext', 'mynewtext')

    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('LCS', KEYS[1], KEYS[2], 'IDX')",
        '2',
        ka,
        kb,
      ],
      `*4\r\n${IDX_REPLY_BODY}`,
    )
    await expectReply(
      conn,
      [
        'EVAL',
        "return redis.pcall('LCS', KEYS[1], KEYS[2], 'LEN', 'IDX')",
        '2',
        ka,
        kb,
      ],
      LEN_WITH_IDX,
    )

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['LCS', ka, kb, 'IDX'], '+QUEUED\r\n')
    await expectReply(conn, ['LCS', ka, kb, 'LEN'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*2\r\n*4\r\n${IDX_REPLY_BODY}:6\r\n`)

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['LCS', ka], ARITY)
    conn.write(commandFrame('EXEC'))
    assert.strictEqual(
      (await conn.readRawFrame()).toString(),
      '-EXECABORT Transaction discarded because of previous errors.\r\n',
    )
  })
})
