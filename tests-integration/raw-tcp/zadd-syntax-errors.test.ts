import { createHash } from 'node:crypto'
import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { activeProfile, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * ZADD's argument checks run in real Redis's order (#517): the options, then
 * the score/member parity (`syntax error` for an odd tail, before any score
 * is parsed or option conflict checked), then the option conflicts, the INCR
 * pair count, and last the scores. The same on every profile; checked
 * against redis-server 6.2.14, 7.0.15, 7.2.4, 7.4.4, 8.0.0 and valkey-server
 * 8.0.0, 9.0.0.
 */
const testRunner = new TestRunner()

const SYNTAX = '-ERR syntax error\r\n'
const ARITY = "-ERR wrong number of arguments for 'zadd' command\r\n"
const INCR_PAIR =
  '-ERR INCR option supports a single increment-element pair\r\n'

describe(`Raw TCP ZADD argument order (${testRunner.getBackendName()})`, () => {
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

  test('an odd score/member tail is a syntax error', async () => {
    const conn = await connect()
    const key = randomKey()
    const rows: string[][] = [
      ['ZADD', key, '1', 'a', '2'],
      ['ZADD', key, 'NX', '1', 'a', '2'],
      ['ZADD', key, 'CH', '1', 'a', '2'],
      ['ZADD', key, 'INCR', '1', 'a', '2'],
      ['ZADD', key, 'NX', '1'],
      ['ZADD', key, 'NX', 'CH'],
      ['ZADD', key, '1', 'a', 'b'],
      ['ZADD', key, '1', 'a', '2', '3', 'b', '4', 'c'],
      ['ZADD', key, 'foo', '1', 'a'],
      ['ZADD', key, 'NX', 'XX', '1', 'a', '2'],
    ]
    for (const row of rows) {
      await expectReply(conn, row, SYNTAX)
    }
    await expectReply(conn, ['EXISTS', key], ':0\r\n')
  })

  test('the parity check precedes the type check', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['SET', key, 'v'], '+OK\r\n')
    await expectReply(conn, ['ZADD', key, '1', 'a', '2'], SYNTAX)
  })

  test('the INCR pair count precedes the score parse', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(
      conn,
      ['ZADD', key, 'INCR', '1', 'a', 'b', '2'],
      INCR_PAIR,
    )
    await expectReply(
      conn,
      ['ZADD', key, 'INCR', '1', 'a', '2', 'b'],
      INCR_PAIR,
    )
  })

  test('the arity, float and conflict rows are unchanged', async () => {
    const conn = await connect()
    const key = randomKey()
    for (const row of [
      ['ZADD', key, '1'],
      ['ZADD', key, 'NX'],
      ['ZADD', key, 'CH'],
    ]) {
      await expectReply(conn, row, ARITY)
    }
    await expectReply(
      conn,
      ['ZADD', key, 'a', '1'],
      '-ERR value is not a valid float\r\n',
    )
    await expectReply(
      conn,
      ['ZADD', key, 'XX', 'NX', '1', 'a'],
      '-ERR XX and NX options at the same time are not compatible\r\n',
    )
    // The conflicts are checked after every option is read, NX/XX first.
    await expectReply(
      conn,
      ['ZADD', key, 'GT', 'LT', 'NX', 'XX', '1', 'a'],
      '-ERR XX and NX options at the same time are not compatible\r\n',
    )
    await expectReply(
      conn,
      ['ZADD', key, 'GT', 'LT', 'INCR', '1', 'a', '2', 'b'],
      '-ERR GT, LT, and/or NX options at the same time are not compatible\r\n',
    )
  })

  test('an odd tail queues and fails in its EXEC slot', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['ZADD', key, '1', 'a', '2'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*1\r\n${SYNTAX}`)
  })

  test('scripts see the syntax error', async () => {
    const conn = await connect()
    const key = randomKey()
    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('ZADD', KEYS[1], 1, 'a', 2)", '1', key],
      SYNTAX,
    )

    const script = "return redis.call('ZADD', KEYS[1], 1, 'a', 2)"
    const sha = createHash('sha1').update(script).digest('hex')
    await expectReply(
      conn,
      ['EVAL', script, '1', key],
      activeProfile === 'redis-6.2'
        ? `-ERR Error running script (call to f_${sha}): @user_script:1: ERR syntax error\r\n`
        : `-ERR syntax error script: ${sha}, on @user_script:1.\r\n`,
    )
  })
})
