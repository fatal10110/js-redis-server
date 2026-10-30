import assert from 'node:assert/strict'
import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectBulk, expectReply, send } from './helpers'

/**
 * INCRBYFLOAT / HINCRBYFLOAT add in x86-64 80-bit `long double` (#512): both
 * operands round to a 64-bit significand, so does the sum, and the reply
 * prints its exact binary value at 17 decimals (`%.17Lf`, trimmed). Values
 * past the `double` range up to about 1.19e4932 are accepted. Identical on
 * redis 6.2.14 … 8.0.0 and valkey 8.0.0 / 9.0.0, so no profile gate.
 */
const testRunner = new TestRunner()

// [stored value, increment, reply]
const ROWS: [string, string, string][] = [
  ['0.1', '0.2', '0.3'],
  ['1234.5678', '0', '1234.56779999999999997'],
  ['1.1', '2.2', '3.3'],
  ['9007199254740993', '1', '9007199254740994'],
  ['0.7', '0.1', '0.8'],
  ['100', '1.23456789012345678', '101.23456789012345678'],
  ['5.5', '4.5e-17', '5.50000000000000005'],
  ['0', '1.5e-17', '0.00000000000000002'],
]

// [increment on a missing key, reply length, first digits, last digits]
const HUGE: [string, number, string, string][] = [
  [
    '1e400',
    401,
    '100000000000000000002818806839475865145864534336',
    '806343732497430221946880',
  ],
  [
    '0x1p1024',
    309,
    '179769313486231590772930519078902473361797697894',
    '304835356329624224137216',
  ],
]

const TWO_E308_HEAD = '199999999999999999993371759311691291321120199694'
const TWO_E308_TAIL = '905456510092687857156096'

describe(`Raw TCP INCRBYFLOAT long double arithmetic (${testRunner.getBackendName()})`, () => {
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

  async function expectHugeBulk(
    conn: RawRedisConnection,
    args: string[],
    length: number,
    head: string,
    tail: string,
  ): Promise<string> {
    conn.write(commandFrame(...args))
    const raw = (await conn.readRawFrame()).toString()
    assert.ok(raw.startsWith(`$${length}\r\n${head}`), raw.slice(0, 80))
    assert.ok(raw.endsWith(`${tail}\r\n`), raw.slice(-40))
    return raw.slice(raw.indexOf('\r\n') + 2, -2)
  }

  test('INCRBYFLOAT and HINCRBYFLOAT sum in long double', async () => {
    const conn = await connect()
    for (const [stored, increment, reply] of ROWS) {
      const key = randomKey()
      await send(conn, ['SET', key, stored])
      await expectBulk(conn, ['INCRBYFLOAT', key, increment], reply)
      await expectBulk(conn, ['GET', key], reply)

      const hash = randomKey()
      await send(conn, ['HSET', hash, 'f', stored])
      await expectBulk(conn, ['HINCRBYFLOAT', hash, 'f', increment], reply)
      await expectBulk(conn, ['HGET', hash, 'f'], reply)
    }

    // The stored text is re-parsed each time: 0.7 + 0.1 three times.
    const key = randomKey()
    await send(conn, ['SET', key, '0.7'])
    for (const reply of ['0.8', '0.9', '1']) {
      await expectBulk(conn, ['INCRBYFLOAT', key, '0.1'], reply)
    }
  })

  test('values past the double range are accepted', async () => {
    const conn = await connect()
    for (const [increment, length, head, tail] of HUGE) {
      await expectHugeBulk(
        conn,
        ['INCRBYFLOAT', randomKey(), increment],
        length,
        head,
        tail,
      )
      await expectHugeBulk(
        conn,
        ['HINCRBYFLOAT', randomKey(), 'f', increment],
        length,
        head,
        tail,
      )
    }

    const key = randomKey()
    await send(conn, ['SET', key, '1e308'])
    const sum = await expectHugeBulk(
      conn,
      ['INCRBYFLOAT', key, '1e308'],
      309,
      TWO_E308_HEAD,
      TWO_E308_TAIL,
    )
    await expectBulk(conn, ['GET', key], sum)
    // 1 is below the sum's precision: nothing changes.
    await expectBulk(conn, ['INCRBYFLOAT', key, '1'], sum)

    const hash = randomKey()
    await send(conn, ['HSET', hash, 'f', '1e308'])
    await expectBulk(conn, ['HINCRBYFLOAT', hash, 'f', '1e308'], sum)

    const max = randomKey()
    conn.write(commandFrame('INCRBYFLOAT', max, '1.18973149535723176502e+4932'))
    assert.ok(
      (await conn.readRawFrame())
        .toString()
        .startsWith('$4933\r\n11897314953572317650212638530'),
    )
    await expectReply(
      conn,
      ['INCRBYFLOAT', max, '1e4932'],
      '-ERR increment would produce NaN or Infinity\r\n',
    )
  })

  test('the long double range ends near 1.19e4932', async () => {
    const conn = await connect()
    await expectReply(
      conn,
      ['INCRBYFLOAT', randomKey(), '1e4933'],
      '-ERR value is not a valid float\r\n',
    )
    await expectReply(
      conn,
      ['HINCRBYFLOAT', randomKey(), 'f', '1e4933'],
      '-ERR value is not a valid float\r\n',
    )
    const key = randomKey()
    await send(conn, ['SET', key, '1e4932'])
    await expectReply(
      conn,
      ['INCRBYFLOAT', key, '1e4932'],
      '-ERR increment would produce NaN or Infinity\r\n',
    )
  })

  test('the same sum inside MULTI/EXEC and from a script', async () => {
    const conn = await connect()
    const key = randomKey()
    await send(conn, ['SET', key, '0.1'])
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['INCRBYFLOAT', key, '0.2'], '+QUEUED\r\n')
    await expectReply(conn, ['EXEC'], '*1\r\n$3\r\n0.3\r\n')

    await expectBulk(
      conn,
      ['EVAL', "return redis.pcall('INCRBYFLOAT', KEYS[1], '0.2')", '1', key],
      '0.5',
    )
  })
})
