import { after, before, describe, test } from 'node:test'

import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from '../raw-tcp/raw-connection'
import { expectReply } from '../raw-tcp/helpers'

/**
 * A sorted-set score of -0 (#512). Checked against redis-server 6.2.14,
 * 7.0.15, 8.0.0 and valkey-server 9.0.0.
 *
 * - 6.2's ziplist keeps the score as the text `-0`, so every reply shows it.
 * - From 7.0 a listpack stores an integral score as an integer and -0 reads
 *   back as 0; a skiplist (more than 128 members) keeps -0 on every version.
 * - ZADD INCR / ZINCRBY reply with the new score before it is stored, and a
 *   new member takes the increment as is: -0 everywhere.
 * - ZUNION replies from the computed set: -0 everywhere.
 * - ZUNIONSTORE, ZINTERSTORE and ZRANGESTORE store -0 as ZADD does.
 */
const testRunner = new TestRunner()
const profile = activeProfile
const legacy = profile === 'redis-6.2'

const NEGATIVE_ZERO = '$2\r\n-0\r\n'
const stored = legacy ? NEGATIVE_ZERO : '$1\r\n0\r\n'

describe(`Sorted-set -0 scores (${testRunner.getBackendName()}, ${profile})`, () => {
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

  test('a small sorted set keeps -0 only on 6.2', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['ZADD', key, '-0', 'm'], ':1\r\n')
    await expectReply(conn, ['ZSCORE', key, 'm'], stored)
    await expectReply(conn, ['ZMSCORE', key, 'm'], `*1\r\n${stored}`)
    await expectReply(
      conn,
      ['ZRANGE', key, '0', '-1', 'WITHSCORES'],
      `*2\r\n$1\r\nm\r\n${stored}`,
    )
    // An equal score is no change, so 6.2 keeps the sign.
    await expectReply(conn, ['ZADD', key, '0', 'm'], ':0\r\n')
    await expectReply(conn, ['ZSCORE', key, 'm'], stored)

    await expectReply(conn, ['ZADD', key, 'INCR', '-0', 'n'], NEGATIVE_ZERO)
    await expectReply(conn, ['ZSCORE', key, 'n'], stored)
    await expectReply(conn, ['ZINCRBY', key, '-0', 'o'], NEGATIVE_ZERO)
    await expectReply(conn, ['ZSCORE', key, 'o'], stored)
    await expectReply(conn, ['ZPOPMIN', key], `*2\r\n$1\r\nm\r\n${stored}`)

    // Arithmetic back to zero is +0 everywhere.
    await expectReply(conn, ['ZADD', key, '1', 'p'], ':1\r\n')
    await expectReply(conn, ['ZINCRBY', key, '-1', 'p'], '$1\r\n0\r\n')
  })

  test('ZUNION replies -0; ZUNIONSTORE stores it like ZADD', async () => {
    const conn = await connect()
    const source = randomKey()
    const destination = randomKey()
    await expectReply(conn, ['ZADD', source, '0', 'a'], ':1\r\n')
    await expectReply(
      conn,
      ['ZUNION', '1', source, 'WEIGHTS', '-1', 'WITHSCORES'],
      `*2\r\n$1\r\na\r\n${NEGATIVE_ZERO}`,
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', destination, '1', source, 'WEIGHTS', '-1'],
      ':1\r\n',
    )
    await expectReply(conn, ['ZSCORE', destination, 'a'], stored)

    const intersection = randomKey()
    await expectReply(
      conn,
      ['ZINTERSTORE', intersection, '1', source, 'WEIGHTS', '-1'],
      ':1\r\n',
    )
    await expectReply(conn, ['ZSCORE', intersection, 'a'], stored)
  })

  test('ZRANGESTORE stores -0 from a skiplist source like ZADD', async () => {
    const conn = await connect()
    const source = randomKey()
    const destination = randomKey()
    const members: string[] = []
    for (let index = 1; index <= 129; index++) {
      members.push(String(index), `m${index}`)
    }
    await expectReply(conn, ['ZADD', source, ...members], ':129\r\n')
    await expectReply(conn, ['ZADD', source, '-0', 'neg'], ':1\r\n')
    await expectReply(
      conn,
      ['ZRANGESTORE', destination, source, '0', '0'],
      ':1\r\n',
    )
    await expectReply(conn, ['ZSCORE', destination, 'neg'], stored)
  })

  test('a sorted set past zset-max-listpack-entries keeps -0', async () => {
    const conn = await connect()
    const key = randomKey()
    const members: string[] = []
    for (let index = 1; index <= 129; index++) {
      members.push(String(index), `m${index}`)
    }
    await expectReply(conn, ['ZADD', key, ...members], ':129\r\n')
    await expectReply(conn, ['ZADD', key, '-0', 'neg'], ':1\r\n')
    await expectReply(conn, ['ZSCORE', key, 'neg'], NEGATIVE_ZERO)
  })
})
