import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Sorted-set command-error integration tests, exercised over a bare socket.
 *
 * These assert the server's raw RESP error reply for malformed arguments
 * (bad floats, bad indexes, wrong arity, bad option combinations). A real
 * client adds nothing here — it is a dumb pipe relaying the same `-ERR ...`
 * bytes — and a cluster-aware client can even distort the wire form (implicit
 * routing keys), so the bare socket is the honest place to pin the error
 * wording. Moved out of the ioredis suite, where they previously had to go
 * through `.call(...)`.
 */
const testRunner = new TestRunner()

const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
const LIMIT_ONLY =
  '-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX\r\n'

describe(`Raw TCP zset command errors (${testRunner.getBackendName()})`, () => {
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

  test('sorted set command errors match Redis', async () => {
    const conn = await connect()
    const zsetKey = `zset-errors:${randomKey()}:zset`
    const stringKey = `zset-errors:${randomKey()}:string`

    await expectReply(conn, ['ZADD', zsetKey, '1', 'a'], ':1\r\n')
    await expectReply(conn, ['SET', stringKey, 'value'], '+OK\r\n')

    await expectReply(conn, ['ZCARD', stringKey], WRONGTYPE)
    await expectReply(
      conn,
      ['ZADD', zsetKey, 'abc', 'member'],
      '-ERR value is not a valid float\r\n',
    )
    await expectReply(
      conn,
      ['ZINCRBY', zsetKey, 'abc', 'a'],
      '-ERR value is not a valid float\r\n',
    )
    await expectReply(
      conn,
      ['ZINCRBY', zsetKey, 'nan', 'a'],
      '-ERR value is not a valid float\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', zsetKey, 'abc', '-1'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGEBYSCORE', zsetKey, 'abc', '1'],
      '-ERR min or max is not a float\r\n',
    )
    await expectReply(
      conn,
      ['ZREMRANGEBYSCORE', zsetKey, '0', 'abc'],
      '-ERR min or max is not a float\r\n',
    )
    await expectReply(
      conn,
      ['ZCOUNT', zsetKey, 'abc', '1'],
      '-ERR min or max is not a float\r\n',
    )
    await expectReply(
      conn,
      ['ZPOPMIN', zsetKey, 'abc'],
      '-ERR value is out of range, must be positive\r\n',
    )
    await expectReply(
      conn,
      ['ZPOPMAX', zsetKey, '-1'],
      '-ERR value is out of range, must be positive\r\n',
    )

    // count of 0 is not an error: returns an empty array.
    await expectReply(conn, ['ZPOPMIN', zsetKey, '0'], '*0\r\n')
  })

  test('zset set-operation argument errors', async () => {
    const conn = await connect()
    const z1 = `zsetop:${randomKey()}:z1`
    const z2 = `zsetop:${randomKey()}:z2`
    const str = `zsetop:${randomKey()}:str`
    const dest = `zsetop:${randomKey()}:dest`

    await expectReply(conn, ['ZADD', z1, '1', 'a'], ':1\r\n')
    await expectReply(conn, ['ZADD', z2, '1', 'b'], ':1\r\n')
    await expectReply(conn, ['SET', str, 'hello'], '+OK\r\n')

    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, '0', z1],
      "-ERR at least 1 input key is needed for 'zunionstore' command\r\n",
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, '-1', z1],
      "-ERR at least 1 input key is needed for 'zunionstore' command\r\n",
    )
    await expectReply(
      conn,
      ['ZUNION', '0', z1],
      "-ERR at least 1 input key is needed for 'zunion' command\r\n",
    )
    await expectReply(
      conn,
      ['ZINTERCARD', '0', z1],
      "-ERR at least 1 input key is needed for 'zintercard' command\r\n",
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, 'abc', z1],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, '3', z1, z2],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, '2', z1, z2, 'WEIGHTS', '1'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, '2', z1, z2, 'WEIGHTS', 'x', 'y'],
      '-ERR weight value is not a float\r\n',
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, '2', z1, z2, 'AGGREGATE', 'foo'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest, '2', z1, z2, 'WITHSCORES'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZDIFF', '2', z1, z2, 'WEIGHTS', '1', '2'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZINTERCARD', '2', z1, z2, 'LIMIT', '-1'],
      "-ERR LIMIT can't be negative\r\n",
    )
    await expectReply(conn, ['ZUNION', '2', z1, str], WRONGTYPE)
    await expectReply(
      conn,
      ['ZUNIONSTORE', dest],
      "-ERR wrong number of arguments for 'zunionstore' command\r\n",
    )
    await expectReply(
      conn,
      ['ZUNION'],
      "-ERR wrong number of arguments for 'zunion' command\r\n",
    )
  })

  test('ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZREMRANGEBYRANK errors', async () => {
    const conn = await connect()
    const key = `zsr:${randomKey()}`
    const str = `zsr:${randomKey()}:str`

    await expectReply(
      conn,
      ['ZADD', key, '1', 'a', '2', 'b', '3', 'c', '4', 'd', '5', 'e'],
      ':5\r\n',
    )
    await expectReply(conn, ['SET', str, 'v'], '+OK\r\n')

    await expectReply(
      conn,
      ['ZRANGEBYSCORE', key, '1', '2', 'LIMIT', 'a', 'b'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGEBYSCORE', key, '1', '2', 'LIMIT'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGEBYSCORE', key, '1'],
      "-ERR wrong number of arguments for 'zrangebyscore' command\r\n",
    )
    await expectReply(
      conn,
      ['ZRANGEBYSCORE', key, 'x', '2'],
      '-ERR min or max is not a float\r\n',
    )
    await expectReply(conn, ['ZRANGEBYSCORE', str, '1', '2'], WRONGTYPE)
    await expectReply(
      conn,
      ['ZREVRANGEBYSCORE', key, 'x', '2'],
      '-ERR min or max is not a float\r\n',
    )
    await expectReply(conn, ['ZREVRANGEBYSCORE', str, '2', '1'], WRONGTYPE)
    await expectReply(
      conn,
      ['ZREMRANGEBYRANK', key, 'x', '1'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZREMRANGEBYRANK', key, '0'],
      "-ERR wrong number of arguments for 'zremrangebyrank' command\r\n",
    )
    await expectReply(conn, ['ZREMRANGEBYRANK', str, '0', '1'], WRONGTYPE)
  })

  test('ZMSCORE / ZRANDMEMBER argument errors', async () => {
    const conn = await connect()
    const key = `zmod:${randomKey()}`

    await expectReply(conn, ['ZADD', key, '1', 'a'], ':1\r\n')

    await expectReply(
      conn,
      ['ZMSCORE', key],
      "-ERR wrong number of arguments for 'zmscore' command\r\n",
    )
    await expectReply(
      conn,
      ['ZRANDMEMBER', key, 'x'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANDMEMBER', key, 'WITHSCORES'],
      '-ERR value is not an integer or out of range\r\n',
    )
  })

  test('ZRANGESTORE argument errors', async () => {
    const conn = await connect()
    const source = `zrangestore:${randomKey()}:source`
    const stringSource = `zrangestore:${randomKey()}:string`
    const dest = `zrangestore:${randomKey()}:dest`

    await expectReply(conn, ['ZADD', source, '1', 'a', '2', 'b'], ':2\r\n')
    await expectReply(conn, ['SET', stringSource, 'not-a-zset'], '+OK\r\n')

    await expectReply(
      conn,
      ['ZRANGESTORE', dest, source, '0', '-1', 'LIMIT', '0', '1'],
      LIMIT_ONLY,
    )
    await expectReply(
      conn,
      ['ZRANGESTORE', dest, source, '0', '-1', 'WITHSCORES'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGESTORE', dest, source, '-', '+', 'BYSCORE', 'BYLEX'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGESTORE', dest, source, '(1', '4'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGESTORE', dest, source, 'x', '5', 'BYSCORE'],
      '-ERR min or max is not a float\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGESTORE', dest, source, 'a', 'c', 'BYLEX'],
      '-ERR min or max not valid string range item\r\n',
    )
    await expectReply(
      conn,
      [
        'ZRANGESTORE',
        dest,
        source,
        '-inf',
        '+inf',
        'BYSCORE',
        'LIMIT',
        'a',
        'b',
      ],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGESTORE', dest, source, '0'],
      "-ERR wrong number of arguments for 'zrangestore' command\r\n",
    )
    await expectReply(
      conn,
      ['ZRANGESTORE', dest, stringSource, '0', '-1'],
      WRONGTYPE,
    )
  })

  test('ZRANGE option-combination errors', async () => {
    const conn = await connect()
    const key = `zrange-opts:${randomKey()}`

    await expectReply(
      conn,
      ['ZADD', key, '1', 'a', '2', 'b', '3', 'c'],
      ':3\r\n',
    )

    await expectReply(
      conn,
      ['ZRANGE', key, '0', '-1', 'LIMIT', '0', '2'],
      LIMIT_ONLY,
    )
    await expectReply(
      conn,
      ['ZRANGE', key, '-', '+', 'BYLEX', 'WITHSCORES'],
      '-ERR syntax error, WITHSCORES not supported in combination with BYLEX\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', key, '-', '+', 'BYSCORE', 'BYLEX'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', key, '(1', '4'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', key, 'x', '5', 'BYSCORE'],
      '-ERR min or max is not a float\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', key, 'a', 'c', 'BYLEX'],
      '-ERR min or max not valid string range item\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', key, '-inf', '+inf', 'BYSCORE', 'LIMIT', 'a', 'b'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGE', key, '0'],
      "-ERR wrong number of arguments for 'zrange' command\r\n",
    )
  })

  test('lex command argument errors', async () => {
    const conn = await connect()
    const key = `zlex:${randomKey()}`

    await expectReply(
      conn,
      ['ZADD', key, '0', 'a', '0', 'b', '0', 'c'],
      ':3\r\n',
    )

    await expectReply(
      conn,
      ['ZRANGEBYLEX', key, '-'],
      "-ERR wrong number of arguments for 'zrangebylex' command\r\n",
    )
    await expectReply(
      conn,
      ['ZLEXCOUNT', key, '-', '+', 'LIMIT', '0', '1'],
      "-ERR wrong number of arguments for 'zlexcount' command\r\n",
    )
    await expectReply(
      conn,
      ['ZREMRANGEBYLEX', key, '-', '+', 'extra'],
      "-ERR wrong number of arguments for 'zremrangebylex' command\r\n",
    )
    await expectReply(
      conn,
      ['ZRANGEBYLEX', key, '-', '+', 'LIMIT', 'x', 'y'],
      '-ERR value is not an integer or out of range\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGEBYLEX', key, '-', '+', 'LIMIX', '0', '1'],
      '-ERR syntax error\r\n',
    )
    await expectReply(
      conn,
      ['ZRANGEBYLEX', key, '-', '+', 'LIMIT', '0'],
      '-ERR syntax error\r\n',
    )
  })
})
