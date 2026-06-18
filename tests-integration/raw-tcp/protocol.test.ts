import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Raw TCP protocol integration tests.
 *
 * These exercise wire-level behavior that a normal Redis client (ioredis /
 * node-redis) can never produce or observe: inline commands, byte-exact RESP2
 * replies, and explicit request pipelining in a single TCP write. They run
 * against both backends — the in-process mock server and a real redis-server —
 * since both listen on an ordinary TCP port.
 */
const testRunner = new TestRunner()

describe(`Raw TCP protocol integration (${testRunner.getBackendName()})`, () => {
  let port: number
  const connections: RawRedisConnection[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  // Close every raw socket before cleanup(): the in-process mock server's
  // net.Server.close() only resolves once all client connections have ended,
  // so a leaked socket would otherwise hang the suite.
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

  test('replies to a multibulk PING with the exact +PONG bytes', async () => {
    const conn = await connect()

    conn.write(commandFrame('PING'))

    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+PONG\r\n'))
  })

  test('parses inline commands that clients never send', async () => {
    const conn = await connect()
    const key = randomKey()

    // Inline protocol: a bare CRLF-terminated line, not a RESP array.
    conn.write(`PING\r\n`)
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+PONG\r\n'))

    conn.write(`SET ${key} rawval\r\n`)
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))

    conn.write(`GET ${key}\r\n`)
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$6\r\nrawval\r\n'),
    )
  })

  test('parses quoted inline arguments containing spaces', async () => {
    const conn = await connect()
    const key = randomKey()

    conn.write(`SET ${key} "hello world"\r\n`)
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))

    conn.write(commandFrame('GET', key))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$11\r\nhello world\r\n'),
    )
  })

  test('pipelines multiple commands in one write and replies in order', async () => {
    const conn = await connect()
    const key = randomKey()

    // One TCP write carrying three back-to-back commands.
    conn.write(
      Buffer.concat([
        commandFrame('SET', key, 'pipelined'),
        commandFrame('GET', key),
        commandFrame('PING'),
      ]),
    )

    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$9\r\npipelined\r\n'),
    )
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+PONG\r\n'))
  })

  test('applies pipelined command effects before later commands', async () => {
    const conn = await connect()
    const key = randomKey()

    conn.write(
      Buffer.concat([
        commandFrame('SET', key, '1'),
        commandFrame('INCR', key),
        commandFrame('GET', key),
        commandFrame('DEL', key),
        commandFrame('GET', key),
      ]),
    )

    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from(':2\r\n'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$1\r\n2\r\n'),
    )
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from(':1\r\n'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('$-1\r\n'))
  })

  test('returns a null bulk string for a missing key', async () => {
    const conn = await connect()

    conn.write(commandFrame('GET', `${randomKey()}:absent`))

    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('$-1\r\n'))
  })

  test('encodes EXEC elements with the protocol active when each command ran', async () => {
    const conn = await connect()
    const missing = `${randomKey()}:absent`

    conn.write(
      Buffer.concat([
        commandFrame('MULTI'),
        commandFrame('GET', missing),
        commandFrame('HELLO', '3'),
        commandFrame('GET', missing),
        commandFrame('EXEC'),
      ]),
    )

    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('+QUEUED\r\n'),
    )
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('+QUEUED\r\n'),
    )
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('+QUEUED\r\n'),
    )

    const rawExec = await conn.readRawFrame()
    assert.ok(
      rawExec.subarray(0, 9).equals(Buffer.from('*3\r\n$-1\r\n')),
      `expected pre-HELLO GET to stay RESP2 null, got ${rawExec.toString()}`,
    )
    assert.ok(
      rawExec.includes(Buffer.from('%7\r\n')),
      `expected HELLO 3 to be encoded as a RESP3 map, got ${rawExec.toString()}`,
    )
    assert.ok(
      rawExec.subarray(-3).equals(Buffer.from('_\r\n')),
      `expected post-HELLO GET to use RESP3 null, got ${rawExec.toString()}`,
    )
  })

  // #80: RESET must execute immediately inside MULTI (like EXEC/DISCARD/WATCH),
  // aborting the transaction — it must not be queued.
  test('RESET inside MULTI executes immediately and aborts the transaction', async () => {
    const conn = await connect()
    const key = `${randomKey()}:reset-multi`

    conn.write(commandFrame('MULTI'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))

    // A normal command is queued.
    conn.write(commandFrame('SET', key, 'queued'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('+QUEUED\r\n'),
    )

    // RESET is NOT queued — it runs now and replies +RESET.
    conn.write(commandFrame('RESET'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+RESET\r\n'))

    // The transaction was discarded, so EXEC has nothing to run.
    conn.write(commandFrame('EXEC'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('-ERR EXEC without MULTI\r\n'),
    )

    // The queued SET never executed.
    conn.write(commandFrame('GET', key))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('$-1\r\n'))

    // The session is back to normal mode — ordinary commands run, not queued.
    conn.write(commandFrame('SET', key, 'normal'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))
  })
})
