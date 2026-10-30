import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectReply } from './helpers'

/**
 * Which check answers first when a command has several bad inputs, and the
 * database-index wording, pinned byte for byte (#589, #571). The same replies
 * on redis 6.2 to 8.0 and valkey 8.0 / 9.0:
 *
 * - XACK looks the key and the group up before it parses the ids: a missing
 *   key or group answers `:0`, even with an invalid id.
 * - LINDEX looks the key up (null reply, or WRONGTYPE) before it parses the
 *   index, and reads the index as a full int64.
 * - SELECT / MOVE / COPY ... DB read the index as a C int: a negative index
 *   answers `DB index is out of range`, a value outside int32 the int32 range
 *   error, and a value outside int64 the integer error.
 */
const testRunner = new TestRunner()

const DB_OUT_OF_RANGE = '-ERR DB index is out of range\r\n'
const NOT_INTEGER = '-ERR value is not an integer or out of range\r\n'
const INT32_RANGE =
  '-ERR value is out of range, value must between -2147483648 and 2147483647\r\n'
const INVALID_ID =
  '-ERR Invalid stream ID specified as stream command argument\r\n'
const WRONGTYPE =
  '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'

describe(`Raw TCP argument check order (${testRunner.getBackendName()})`, () => {
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

  test('SELECT answers DB index is out of range for a negative index (#571)', async () => {
    const conn = await connect()

    await expectReply(conn, ['SELECT', '-1'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['SELECT', '-2147483648'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['SELECT', '16'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['SELECT', '2147483647'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['SELECT', '2147483648'], INT32_RANGE)
    await expectReply(conn, ['SELECT', '-2147483649'], INT32_RANGE)
    await expectReply(conn, ['SELECT', '9223372036854775807'], INT32_RANGE)
    await expectReply(conn, ['SELECT', '99999999999999999999'], NOT_INTEGER)
    for (const bad of ['abc', '1.5', '', '-0', '+1', ' 1']) {
      await expectReply(conn, ['SELECT', bad], NOT_INTEGER)
    }
    // The connection stays on its database after every refusal.
    await expectReply(conn, ['SELECT', '0'], '+OK\r\n')
  })

  test('SELECT from a script and inside MULTI (#571)', async () => {
    const conn = await connect()

    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('select','-1')", '0'],
      DB_OUT_OF_RANGE,
    )
    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('select','2147483648')", '0'],
      INT32_RANGE,
    )
    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('select','abc')", '0'],
      NOT_INTEGER,
    )

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['SELECT', '-1'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], `*1\r\n${DB_OUT_OF_RANGE}`)
  })

  test('MOVE and COPY DB read the index as an int32 (#571)', async () => {
    const conn = await connect()
    const key = `arg-order-move:${randomKey()}`

    await expectReply(conn, ['SET', key, 'value'], '+OK\r\n')
    await expectReply(conn, ['MOVE', key, '-2147483648'], DB_OUT_OF_RANGE)
    await expectReply(conn, ['MOVE', key, '2147483648'], INT32_RANGE)
    await expectReply(conn, ['MOVE', key, '-2147483649'], INT32_RANGE)
    await expectReply(conn, ['MOVE', key, '99999999999999999999'], NOT_INTEGER)

    const dst = `${key}-d`
    await expectReply(conn, ['COPY', key, dst, 'DB', '2147483648'], INT32_RANGE)
    await expectReply(
      conn,
      ['COPY', key, dst, 'DB', '-2147483649'],
      INT32_RANGE,
    )
    await expectReply(conn, ['COPY', key, dst, 'DB', '-0'], NOT_INTEGER)
  })

  test('XACK answers 0 for a missing key or group before parsing the ids (#589)', async () => {
    const conn = await connect()
    const missing = `arg-order-xack-missing:${randomKey()}`
    const stream = `arg-order-xack:${randomKey()}`
    const zset = `arg-order-xack-zset:${randomKey()}`

    await expectReply(conn, ['XACK', missing, 'g', '1-1'], ':0\r\n')
    await expectReply(conn, ['XACK', missing, 'g', 'abc'], ':0\r\n')
    await expectReply(conn, ['XACK', missing, 'g', 'abc', '1-1'], ':0\r\n')

    await expectReply(
      conn,
      ['XGROUP', 'CREATE', stream, 'g', '$', 'MKSTREAM'],
      '+OK\r\n',
    )
    await expectReply(conn, ['XACK', stream, 'nog', '1-1'], ':0\r\n')
    await expectReply(conn, ['XACK', stream, 'nog', 'abc'], ':0\r\n')
    await expectReply(conn, ['XACK', stream, 'g', '1-1'], ':0\r\n')
    await expectReply(conn, ['XACK', stream, 'g', 'abc'], INVALID_ID)
    await expectReply(conn, ['XACK', stream, 'g', '1-1', 'abc'], INVALID_ID)

    await expectReply(conn, ['ZADD', zset, '1', 'm'], ':1\r\n')
    await expectReply(conn, ['XACK', zset, 'g', '1-1'], WRONGTYPE)
    await expectReply(
      conn,
      ['XACK', missing, 'g'],
      "-ERR wrong number of arguments for 'xack' command\r\n",
    )

    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('xack', KEYS[1], 'g', '1-1')", '1', missing],
      ':0\r\n',
    )
  })

  test('LINDEX looks the key up before parsing the index (#589)', async () => {
    const conn = await connect()
    const missing = `arg-order-lindex-missing:${randomKey()}`
    const list = `arg-order-lindex:${randomKey()}`
    const zset = `arg-order-lindex-zset:${randomKey()}`
    const str = `arg-order-lindex-str:${randomKey()}`

    for (const index of ['x', '1.5', ' 1', '9223372036854775808']) {
      await expectReply(conn, ['LINDEX', missing, index], '$-1\r\n')
    }

    await expectReply(conn, ['ZADD', zset, '1', 'm'], ':1\r\n')
    await expectReply(conn, ['SET', str, 'v'], '+OK\r\n')
    await expectReply(conn, ['LINDEX', zset, 'x'], WRONGTYPE)
    await expectReply(conn, ['LINDEX', str, 'x'], WRONGTYPE)

    await expectReply(conn, ['RPUSH', list, 'a', 'b', 'c'], ':3\r\n')
    for (const index of [
      'x',
      ' 1',
      '1.5',
      '9223372036854775808',
      '99999999999999999999',
    ]) {
      await expectReply(conn, ['LINDEX', list, index], NOT_INTEGER)
    }
    await expectReply(conn, ['LINDEX', list, '1'], '$1\r\nb\r\n')
    await expectReply(conn, ['LINDEX', list, '-1'], '$1\r\nc\r\n')
    await expectReply(conn, ['LINDEX', list, '-3'], '$1\r\na\r\n')
    await expectReply(conn, ['LINDEX', list, '-4'], '$-1\r\n')
    await expectReply(conn, ['LINDEX', list, '3'], '$-1\r\n')
    for (const index of [
      '9007199254740993',
      '9223372036854775807',
      '-9223372036854775808',
    ]) {
      await expectReply(conn, ['LINDEX', list, index], '$-1\r\n')
    }

    await expectReply(
      conn,
      ['EVAL', "return redis.pcall('lindex', KEYS[1], 'x')", '1', zset],
      WRONGTYPE,
    )
  })

  test('LINDEX and XACK on a missing key inside MULTI and over RESP3 (#589)', async () => {
    const conn = await connect()
    const missing = `arg-order-multi-missing:${randomKey()}`

    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['LINDEX', missing, 'x'], '+QUEUED\r\n')
    await expectReply(conn, ['XACK', missing, 'g', '1-1'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*2\r\n$-1\r\n:0\r\n')

    // HELLO's own reply is a server-specific map: read and drop it.
    conn.write('*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n')
    await conn.readRawFrame()
    await expectReply(conn, ['LINDEX', missing, 'x'], '_\r\n')
    await expectReply(conn, ['XACK', missing, 'g', 'abc'], ':0\r\n')
  })
})
