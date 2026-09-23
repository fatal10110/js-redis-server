import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * ECHO wire behavior (#434): the reply must be the argument bytes verbatim —
 * including bytes that are not valid UTF-8 and embedded CR/LF — which no
 * string-decoding client can pin, so these assert the exact reply bytes.
 */
const testRunner = new TestRunner()

const ARITY_ERROR = "-ERR wrong number of arguments for 'echo' command\r\n"

describe(`Raw TCP ECHO (${testRunner.getBackendName()})`, () => {
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

  test('replies with the argument bytes verbatim, including non-UTF-8', async () => {
    const conn = await connect()
    const payload = Buffer.from([0xff, 0x00, 0xfe, 0x0d, 0x0a, 0x80, 0xc3])

    conn.write(commandFrame('ECHO', payload))

    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.concat([Buffer.from('$7\r\n'), payload, Buffer.from('\r\n')]),
    )
  })

  test('replies with an empty bulk string for an empty message', async () => {
    const conn = await connect()

    conn.write(commandFrame('ECHO', ''))

    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('$0\r\n\r\n'))
  })

  test('command name is case-insensitive', async () => {
    const conn = await connect()

    conn.write(commandFrame('eChO', 'hello'))

    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$5\r\nhello\r\n'),
    )
  })

  test('rejects wrong arity with the Redis error', async () => {
    const conn = await connect()

    conn.write(commandFrame('ECHO'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from(ARITY_ERROR))

    conn.write(commandFrame('ECHO', 'a', 'b'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from(ARITY_ERROR))
  })

  test('is rejected in RESP2 subscribed mode, after the arity check', async () => {
    const conn = await connect()
    const channel = `raw-echo:${randomKey()}`

    conn.write(commandFrame('SUBSCRIBE', channel))
    await conn.readRawFrame()

    conn.write(commandFrame('ECHO', 'hi'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from(
        "-ERR Can't execute 'echo': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n",
      ),
    )

    conn.write(commandFrame('ECHO'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from(ARITY_ERROR))
  })

  test('is allowed in RESP3 subscribed mode', async () => {
    const conn = await connect()
    const channel = `raw-echo:${randomKey()}`

    conn.write(commandFrame('HELLO', '3'))
    await conn.readFrame()

    conn.write(commandFrame('SUBSCRIBE', channel))
    await conn.readRawFrame()

    conn.write(commandFrame('ECHO', 'hi'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$2\r\nhi\r\n'),
    )
  })

  test('works inside MULTI/EXEC and from Lua', async () => {
    const conn = await connect()

    conn.write(commandFrame('MULTI'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))
    conn.write(commandFrame('ECHO', 'queued'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('+QUEUED\r\n'),
    )
    conn.write(commandFrame('EXEC'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('*1\r\n$6\r\nqueued\r\n'),
    )

    conn.write(
      commandFrame('EVAL', "return redis.call('echo', ARGV[1])", '0', 'lua'),
    )
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$3\r\nlua\r\n'),
    )
  })
})
