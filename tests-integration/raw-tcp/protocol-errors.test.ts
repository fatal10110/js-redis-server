import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import { RawRedisConnection } from './raw-connection'

/**
 * Raw TCP protocol-error integration tests.
 *
 * A Redis client never emits a malformed frame, so the only way to exercise
 * the server's protocol-error path is over a bare socket. This covers issue
 * #79: when a single pipelined write ends in a bad frame, the server must still
 * answer the valid commands that preceded it, then send the protocol error and
 * close the connection — so the client can tell which commands succeeded.
 */
const testRunner = new TestRunner()

describe(`Raw TCP protocol errors (${testRunner.getBackendName()})`, () => {
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

  test('answers valid pipelined commands before a trailing bad frame, then closes (#79)', async () => {
    const conn = await connect()
    const key = randomKey()

    // One TCP write: two valid commands, then a malformed frame whose bulk
    // length is not numeric.
    conn.write(
      Buffer.concat([
        commandFrame('SET', key, 'value'),
        commandFrame('GET', key),
        Buffer.from('*1\r\n$abc\r\n'),
      ]),
    )

    // The valid commands are answered in order...
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))
    assert.deepStrictEqual(
      await conn.readRawFrame(),
      Buffer.from('$5\r\nvalue\r\n'),
    )

    // ...then the protocol error is sent and the server hangs up.
    const tail = await conn.readUntilClose()
    assert.strictEqual(
      tail.toString(),
      '-ERR Protocol error: invalid bulk length\r\n',
    )
  })

  // A cluster-aware client (e.g. ioredis) injects an implicit routing key when a
  // keyed command is sent with no key, so it can never put EXPIRETIME/TTL on the
  // wire with zero arguments. The bare socket is the only way to assert the
  // server's wrong-arity reply for these commands.
  test('keyed commands with no arguments return a wrong-arity error', async () => {
    const conn = await connect()

    for (const command of ['EXPIRETIME', 'PEXPIRETIME', 'TTL', 'PTTL']) {
      conn.write(commandFrame(command))
      assert.strictEqual(
        (await conn.readRawFrame()).toString(),
        `-ERR wrong number of arguments for '${command.toLowerCase()}' command\r\n`,
      )
    }
  })
})
