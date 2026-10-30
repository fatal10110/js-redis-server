import assert from 'node:assert/strict'
import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, commandFrame, randomKey } from '../utils'
import { RawRedisConnection, respText } from '../raw-tcp/raw-connection'
import { expectReply, send } from '../raw-tcp/helpers'

/**
 * Replies that depend on a sorted set's encoding, listpack or skiplist (#512).
 * Checked against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and
 * valkey-server 8.0.0 / 9.0.0.
 *
 * - ZSCAN walks a listpack in (score, member) order and spells a score like
 *   ZSCORE does; it walks a skiplist in hash order (undefined, so compared
 *   order-insensitively here) and spells every score `%.17Lg` on every
 *   version: 0.1 is `0.10000000000000001`, -0 is `-0`.
 * - A listpack becomes a skiplist past 128 members or with a member over 64
 *   bytes, and stays one when it shrinks; SORT converts it too.
 * - ZADD's pair count picks the encoding up front from 7.2, and Valkey 8.0
 *   also reads ZADD's longest member: both show through -0, which a listpack
 *   stores as 0 from 7.0.
 *
 * The 129-member sets are built by ZADD, never CONFIG SET, so the file is safe
 * on a shared real server.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'
const sizeHint = profile !== 'redis-6.2' && profile !== 'redis-7.0'
const fpconv = sizeHint
const longestMemberHint = /^valkey-(8|9)/.test(profile)
/** Redis 7.2.0-7.2.4: ZRANGESTORE BYSCORE / BYLEX always stores a skiplist. */
const rangestoreSkiplist = profile === 'redis-7.2'

const LONG = 'a'.repeat(65)
const ZERO = '$1\r\n0\r\n'
const NEGATIVE_ZERO = '$2\r\n-0\r\n'
/** A listpack's -0: kept as text by 6.2's ziplist, an integer 0 from 7.0. */
const LISTPACK_ZERO = legacy ? NEGATIVE_ZERO : ZERO
const G17_TENTH = '0.10000000000000001'

function bulk(text: string): string {
  return `$${Buffer.byteLength(text)}\r\n${text}\r\n`
}

describe(`Sorted-set encoding (${testRunner.getBackendName()}, ${profile})`, () => {
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

  /** Every member/score pair ZSCAN reports, following the cursor to 0. */
  async function zscanAll(
    conn: RawRedisConnection,
    key: string,
  ): Promise<Map<string, string>> {
    const pairs = new Map<string, string>()
    let cursor = '0'
    do {
      conn.write(commandFrame('ZSCAN', key, cursor, 'COUNT', '50'))
      const reply = (await conn.readFrame()) as unknown[]
      cursor = respText(reply[0])
      const items = reply[1] as unknown[]
      for (let i = 0; i < items.length; i += 2) {
        pairs.set(respText(items[i]), respText(items[i + 1]))
      }
    } while (cursor !== '0')
    return pairs
  }

  /** ZADD of 129 members m0..m128: a skiplist on every version. */
  async function addMany(conn: RawRedisConnection, key: string) {
    const args = ['ZADD', key]
    for (let i = 0; i < 129; i++) args.push(String(i), `m${i}`)
    await expectReply(conn, args, ':129\r\n')
  }

  test('a listpack ZSCAN is in score order, spelled like ZSCORE', async () => {
    const conn = await connect()
    const key = randomKey()
    await send(conn, ['ZADD', key, '3', 'c', '1', 'b', '1', 'a', '2', 'aa'])
    await send(conn, ['ZADD', key, '0.1', 'z'])
    const tenth = fpconv ? '0.1' : G17_TENTH
    await expectReply(
      conn,
      ['ZSCAN', key, '0'],
      '*2\r\n$1\r\n0\r\n*10\r\n' +
        [
          ['z', tenth],
          ['a', '1'],
          ['b', '1'],
          ['aa', '2'],
          ['c', '3'],
        ]
          .flat()
          .map(bulk)
          .join(''),
    )
  })

  test('a skiplist ZSCAN spells scores %.17Lg on every version', async () => {
    const conn = await connect()
    const key = randomKey()
    await addMany(conn, key)
    const spelled: [string, string, string][] = [
      ['x1', '0.1', G17_TENTH],
      ['x2', '1e-5', '1.0000000000000001e-05'],
      ['x3', '123456789012345678', '1.2345678901234568e+17'],
      ['x4', '5e-324', '4.9406564584124654e-324'],
      ['x5', '1e17', '1e+17'],
      ['x6', '1e16', '10000000000000000'],
      ['x7', 'inf', 'inf'],
      ['x8', '-inf', '-inf'],
      ['x9', '-0', '-0'],
    ]
    for (const [member, score] of spelled) {
      await send(conn, ['ZADD', key, score, member])
    }
    const pairs = await zscanAll(conn, key)
    assert.equal(pairs.size, 129 + spelled.length)
    for (const [member, , expected] of spelled) {
      assert.equal(pairs.get(member), expected, member)
    }
    assert.equal(pairs.get('m7'), '7')

    // Removing members never converts back: -0 is still kept.
    await send(conn, ['ZREMRANGEBYRANK', key, '0', '-2'])
    await expectReply(conn, ['ZADD', key, '-0', 'n'], ':1\r\n')
    await expectReply(conn, ['ZSCORE', key, 'n'], NEGATIVE_ZERO)
    await send(conn, ['ZREM', key, 'x7'])
    await send(conn, ['ZADD', key, '0.1', 'n'])
    await expectReply(
      conn,
      ['ZSCAN', key, '0'],
      `*2\r\n$1\r\n0\r\n*2\r\n$1\r\nn\r\n${bulk(G17_TENTH)}`,
    )
  })

  test('SORT converts a listpack to a skiplist, and RENAME keeps it', async () => {
    const conn = await connect()
    const key = randomKey()
    const renamed = `${key}:renamed`
    await send(conn, ['ZADD', key, '0.1', 'a'])
    await expectReply(conn, ['SORT', key, 'ALPHA'], '*1\r\n$1\r\na\r\n')
    await expectReply(conn, ['RENAME', key, renamed], '+OK\r\n')
    await expectReply(
      conn,
      ['ZSCAN', renamed, '0'],
      `*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n${bulk(G17_TENTH)}`,
    )
    await send(conn, ['ZADD', renamed, '-0', 'b'])
    await expectReply(conn, ['ZSCORE', renamed, 'b'], NEGATIVE_ZERO)
  })

  test('ZADD of 129 pairs is a skiplist up front from 7.2', async () => {
    const conn = await connect()
    const key = randomKey()
    const args = ['ZADD', key]
    for (let i = 0; i < 129; i++) args.push('-0', 'm')
    await expectReply(conn, args, ':1\r\n')
    await expectReply(
      conn,
      ['ZSCORE', key, 'm'],
      sizeHint ? NEGATIVE_ZERO : LISTPACK_ZERO,
    )
  })

  test('a long member converts; Valkey 8 reads every ZADD member', async () => {
    const conn = await connect()
    // Created from its first member (Redis) or its longest (Valkey 8+).
    const created = randomKey()
    await send(conn, ['ZADD', created, '-0', 'a', '1', LONG])
    await expectReply(
      conn,
      ['ZSCORE', created, 'a'],
      longestMemberHint ? NEGATIVE_ZERO : LISTPACK_ZERO,
    )

    // A long member that XX does not add still converts on Valkey 8+.
    const existing = randomKey()
    await send(conn, ['ZADD', existing, '0.1', 'a'])
    await expectReply(conn, ['ZADD', existing, 'XX', '0.2', LONG], ':0\r\n')
    const tenth = longestMemberHint || !fpconv ? G17_TENTH : '0.1'
    await expectReply(
      conn,
      ['ZSCAN', existing, '0'],
      `*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n${bulk(tenth)}`,
    )
  })

  test('STORE results: ZRANGESTORE inserts, ZUNIONSTORE converts if it fits', async () => {
    const conn = await connect()
    const source = randomKey()
    // The long member first: a skiplist from the start, -0 kept.
    await send(conn, ['ZADD', source, '1', LONG, '-0', 'a'])
    await expectReply(conn, ['ZSCORE', source, 'a'], NEGATIVE_ZERO)

    // ZRANGESTORE creates a listpack for 2 members and stores `a` (-0) before
    // the long member converts it.
    const ranged = `${source}:range`
    await expectReply(
      conn,
      ['ZRANGESTORE', ranged, source, '0', '-1'],
      ':2\r\n',
    )
    await expectReply(conn, ['ZSCORE', ranged, 'a'], LISTPACK_ZERO)

    // ZUNIONSTORE builds a skiplist and keeps it: the long member does not fit.
    const union = `${source}:union`
    await expectReply(conn, ['ZUNIONSTORE', union, '1', source], ':2\r\n')
    await expectReply(conn, ['ZSCORE', union, 'a'], NEGATIVE_ZERO)

    // Without the long member it fits and is a listpack again.
    await send(conn, ['ZREM', source, LONG])
    const small = `${source}:small`
    await expectReply(conn, ['ZUNIONSTORE', small, '1', source], ':1\r\n')
    await expectReply(conn, ['ZSCORE', small, 'a'], LISTPACK_ZERO)
  })

  test('ZRANGESTORE BYSCORE / BYLEX starts a listpack, the rank form a skiplist past the limit', async () => {
    const conn = await connect()
    const source = randomKey()
    // 130 members, -0 first: a skiplist holding -0.
    await addMany(conn, source)
    await expectReply(conn, ['ZADD', source, '-0', 'z'], ':1\r\n')

    // The rank form knows its 130 members: a skiplist up front, -0 kept.
    const ranked = `${source}:rank`
    await expectReply(
      conn,
      ['ZRANGESTORE', ranked, source, '0', '-1'],
      ':130\r\n',
    )
    await expectReply(conn, ['ZSCORE', ranked, 'z'], NEGATIVE_ZERO)

    // BYSCORE / BYLEX do not: a listpack stores -0 before the 129th member
    // converts it (always a skiplist on 7.2.0-7.2.4).
    const unknownZero = rangestoreSkiplist ? NEGATIVE_ZERO : LISTPACK_ZERO
    const forms = [
      ['-inf', '+inf', 'BYSCORE'],
      ['-inf', '+inf', 'BYSCORE', 'LIMIT', '0', '200'],
      ['-', '+', 'BYLEX'],
    ]
    for (const [i, form] of forms.entries()) {
      const stored = `${source}:by${i}`
      await expectReply(
        conn,
        ['ZRANGESTORE', stored, source, ...form],
        ':130\r\n',
      )
      await expectReply(conn, ['ZSCORE', stored, 'z'], unknownZero)
    }

    // A small BYSCORE result: a listpack, spelled like ZSCORE, or a skiplist
    // (%.17Lg) on 7.2.0-7.2.4. The source is a listpack, so `b` is stored
    // as a listpack keeps -0.
    const pair = randomKey()
    await send(conn, ['ZADD', pair, '0.1', 'a', '-0', 'b'])
    const small = `${pair}:small`
    await expectReply(
      conn,
      ['ZRANGESTORE', small, pair, '-inf', '+inf', 'BYSCORE'],
      ':2\r\n',
    )
    const tenth = rangestoreSkiplist || !fpconv ? G17_TENTH : '0.1'
    assert.deepStrictEqual(
      await zscanAll(conn, small),
      new Map([
        ['a', tenth],
        ['b', legacy ? '-0' : '0'],
      ]),
    )
  })
})
