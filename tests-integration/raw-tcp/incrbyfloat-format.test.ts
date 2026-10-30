import { after, before, describe, test } from 'node:test'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'
import { expectBulk, expectReply, send } from './helpers'

/**
 * INCRBYFLOAT / HINCRBYFLOAT spell their result with
 * `ld2string(…, LD_STR_HUMAN)` — `%.17Lf`, trailing zeros (and a bare `.`)
 * dropped, `-0` shown as `0` — and store that same text (#543). Identical on
 * redis 6.2.14 … 8.0.0 and valkey 8.0.0 / 9.0.0, so no profile gate.
 *
 * Rows whose digits need the 64-bit `long double` arithmetic (`0.1 + 0.2`,
 * values past the double range) are in incrbyfloat-long-double.test.ts (#512).
 */
const testRunner = new TestRunner()

// [increment on a missing key, reply text]
const ROWS: [string, string][] = [
  ['1e-17', '0.00000000000000001'],
  ['-1e-17', '-0.00000000000000001'],
  ['1e-18', '0'],
  ['-1e-18', '0'],
  ['5e-324', '0'],
  ['0x1p-1074', '0'],
  ['0.00000000000000000149', '0'],
  ['1e-7', '0.0000001'],
  ['1.5e-8', '0.000000015'],
  ['1e-5', '0.00001'],
  ['5e-1', '0.5'],
  ['3.0e0', '3'],
  ['-0.0', '0'],
  ['1e17', '100000000000000000'],
  ['1e20', '100000000000000000000'],
  ['1e21', '1000000000000000000000'],
]

describe(`Raw TCP INCRBYFLOAT result format (${testRunner.getBackendName()})`, () => {
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

  test('INCRBYFLOAT replies and stores %.17Lf trimmed (#543)', async () => {
    const conn = await connect()
    for (const [increment, expected] of ROWS) {
      const key = `incrbyfloat-format:${randomKey()}`
      await expectBulk(conn, ['INCRBYFLOAT', key, increment], expected)
      await expectBulk(conn, ['GET', key], expected)
    }
  })

  test('HINCRBYFLOAT replies and stores %.17Lf trimmed (#543)', async () => {
    const conn = await connect()
    for (const [increment, expected] of ROWS) {
      const key = `hincrbyfloat-format:${randomKey()}`
      await expectBulk(conn, ['HINCRBYFLOAT', key, 'a', increment], expected)
      await expectBulk(conn, ['HGET', key, 'a'], expected)
    }
  })

  test('a stored value is respelled by the next increment (#543)', async () => {
    const conn = await connect()
    const key = `incrbyfloat-format:${randomKey()}`
    await expectBulk(conn, ['INCRBYFLOAT', key, '1e-17'], '0.00000000000000001')
    await expectBulk(conn, ['INCRBYFLOAT', key, '1e-17'], '0.00000000000000002')

    const plain = `incrbyfloat-format:${randomKey()}`
    await send(conn, ['SET', plain, '1e-17'])
    await expectBulk(conn, ['INCRBYFLOAT', plain, '0'], '0.00000000000000001')
    await expectBulk(conn, ['GET', plain], '0.00000000000000001')

    const small = `incrbyfloat-format:${randomKey()}`
    await send(conn, ['SET', small, '1e-7'])
    await expectBulk(conn, ['INCRBYFLOAT', small, '1e-7'], '0.0000002')
    await expectBulk(conn, ['GET', small], '0.0000002')

    const hash = `hincrbyfloat-format:${randomKey()}`
    await send(conn, ['HSET', hash, 'a', '1e-7'])
    await expectBulk(conn, ['HINCRBYFLOAT', hash, 'a', '1e-7'], '0.0000002')
    await expectBulk(conn, ['HGET', hash, 'a'], '0.0000002')
  })

  test('the same spelling inside MULTI/EXEC and from a script (#543)', async () => {
    const conn = await connect()
    const key = `incrbyfloat-format:${randomKey()}`
    const hash = `hincrbyfloat-format:${randomKey()}`
    await expectReply(conn, ['MULTI'], '+OK\r\n')
    await expectReply(conn, ['INCRBYFLOAT', key, '1e-17'], '+QUEUED\r\n')
    await expectReply(
      conn,
      ['HINCRBYFLOAT', hash, 'a', '-1e-18'],
      '+QUEUED\r\n',
    )
    await expectReply(
      conn,
      ['EXEC'],
      '*2\r\n$19\r\n0.00000000000000001\r\n$1\r\n0\r\n',
    )

    const scripted = `incrbyfloat-format:${randomKey()}`
    await expectBulk(
      conn,
      [
        'EVAL',
        "return redis.pcall('INCRBYFLOAT', KEYS[1], '1e-17')",
        '1',
        scripted,
      ],
      '0.00000000000000001',
    )
  })
})
