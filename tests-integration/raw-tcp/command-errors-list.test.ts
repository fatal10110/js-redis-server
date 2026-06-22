import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * List / SORT command-error integration tests over a bare socket. Direction
 * keywords, timeouts, arity, unknown options and wrong-type sources all
 * surface slot-independent server RESP errors with no client-side value, moved
 * out of the ioredis suite where they previously went through `.call(...)`.
 */
const testRunner = new TestRunner()

const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const SYNTAX = '-ERR syntax error\r\n'

describe(`Raw TCP list command errors (${testRunner.getBackendName()})`, () => {
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

  test('BLMOVE error paths match Redis', async () => {
    const conn = await connect()
    const src = `blmove-errors:${randomKey()}:src`
    const dst = `blmove-errors:${randomKey()}:dst`
    const stringKey = `blmove-errors:${randomKey()}:string`

    await expectReply(conn, ['RPUSH', src, 'x'], ':1\r\n')

    await expectReply(conn, ['BLMOVE', src, dst, 'UP', 'LEFT', '1'], SYNTAX)
    await expectReply(
      conn,
      ['BLMOVE', src, dst, 'LEFT', 'RIGHT'],
      "-ERR wrong number of arguments for 'blmove' command\r\n",
    )
    await expectReply(
      conn,
      ['BLMOVE', src, dst, 'LEFT', 'RIGHT', '-1'],
      '-ERR timeout is negative\r\n',
    )
    await expectReply(
      conn,
      ['BLMOVE', src, dst, 'LEFT', 'RIGHT', 'abc'],
      '-ERR timeout is not a float or out of range\r\n',
    )

    await expectReply(conn, ['SET', stringKey, 'value'], '+OK\r\n')
    await expectReply(
      conn,
      ['BLMOVE', stringKey, dst, 'LEFT', 'RIGHT', '1'],
      WRONGTYPE,
    )
    await expectReply(
      conn,
      ['BLMOVE', src, stringKey, 'LEFT', 'RIGHT', '1'],
      WRONGTYPE,
    )
    // source left unchanged after the failed moves
    await expectReply(conn, ['LRANGE', src, '0', '-1'], '*1\r\n$1\r\nx\r\n')
  })

  test('SORT argument errors', async () => {
    const conn = await connect()
    const alpha = `sort-errors:${randomKey()}:alpha`
    const num = `sort-errors:${randomKey()}:num`
    const dst = `sort-errors:${randomKey()}:dst`

    await expectReply(conn, ['RPUSH', alpha, 'apple', 'banana'], ':2\r\n')
    await expectReply(conn, ['RPUSH', num, '1', '2'], ':2\r\n')

    await expectReply(
      conn,
      ['SORT', alpha],
      "-ERR One or more scores can't be converted into double\r\n",
    )
    await expectReply(
      conn,
      ['SORT', num, 'LIMIT', 'a', 'b'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['SORT'],
      "-ERR wrong number of arguments for 'sort' command\r\n",
    )
    await expectReply(conn, ['SORT', num, 'FOO'], SYNTAX)
    await expectReply(conn, ['SORT_RO', num, 'STORE', dst], SYNTAX)
  })
})
