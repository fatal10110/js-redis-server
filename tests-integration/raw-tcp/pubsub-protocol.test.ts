import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import {
  RawRedisConnection,
  type RespWireValue,
  respText,
} from './raw-connection'

const testRunner = new TestRunner()

describe(`Raw TCP Pub/Sub protocol (${testRunner.getBackendName()})`, () => {
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

  test('allows subscribed-mode commands and rejects ordinary commands', async () => {
    const conn = await connect()
    const channel = `raw-pubsub:${randomKey()}`

    conn.write(commandFrame('SUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'subscribe',
      channel,
      1,
    ])

    conn.write(commandFrame('PING', 'hello'))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'pong',
      'hello',
    ])

    conn.write(commandFrame('GET', 'blocked'))
    assert.match(
      respText(await conn.readFrame()),
      /^ERR Can't execute 'get': only .* allowed in this context$/,
    )

    conn.write(commandFrame('UNSUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'unsubscribe',
      channel,
      0,
    ])

    conn.write(commandFrame('SET', 'after-unsubscribe', 'ok'))
    assert.deepStrictEqual(await conn.readFrame(), 'OK')
  })

  test('RESET exits subscribed mode and removes all subscriptions', async () => {
    const subscriber = await connect()
    const publisher = await connect()
    const channel = `raw-reset:${randomKey()}`

    subscriber.write(commandFrame('SUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'subscribe',
      channel,
      1,
    ])

    subscriber.write(commandFrame('RESET'))
    assert.deepStrictEqual(await subscriber.readFrame(), 'RESET')

    publisher.write(commandFrame('PUBLISH', channel, 'dropped'))
    assert.deepStrictEqual(await publisher.readFrame(), 0)

    subscriber.write(commandFrame('GET', 'still-normal'))
    assert.deepStrictEqual(await subscriber.readFrame(), null)
  })

  test('allows QUIT while subscribed', async () => {
    const conn = await connect()
    const channel = `raw-quit:${randomKey()}`

    conn.write(commandFrame('SUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'subscribe',
      channel,
      1,
    ])

    conn.write(commandFrame('QUIT'))
    assert.deepStrictEqual(await conn.readRawFrame(), Buffer.from('+OK\r\n'))
    assert.deepStrictEqual(await conn.readUntilClose(), Buffer.alloc(0))
  })

  test('emits one acknowledgement per channel in multi-channel commands', async () => {
    const conn = await connect()
    const first = `raw-multi:${randomKey()}:1`
    const second = `raw-multi:${randomKey()}:2`

    conn.write(commandFrame('SUBSCRIBE', first, second))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'subscribe',
      first,
      1,
    ])
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'subscribe',
      second,
      2,
    ])

    conn.write(commandFrame('UNSUBSCRIBE'))
    const unsubscribeFrames = [
      normalizeFrame(await conn.readFrame()),
      normalizeFrame(await conn.readFrame()),
    ]
    assert.deepStrictEqual(
      unsubscribeFrames.map(frame => {
        assert.ok(Array.isArray(frame))
        return frame[0]
      }),
      ['unsubscribe', 'unsubscribe'],
    )
    assert.deepStrictEqual(
      unsubscribeFrames
        .map(frame => {
          assert.ok(Array.isArray(frame))
          return frame[1]
        })
        .sort(),
      [first, second].sort(),
    )
    assert.deepStrictEqual(
      unsubscribeFrames.map(frame => {
        assert.ok(Array.isArray(frame))
        return frame[2]
      }),
      [1, 0],
    )
  })

  test('rejects Pub/Sub arity errors with Redis errors', async () => {
    const conn = await connect()

    conn.write(commandFrame('SUBSCRIBE'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'subscribe' command",
    )

    conn.write(commandFrame('PSUBSCRIBE'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'psubscribe' command",
    )

    conn.write(commandFrame('PUBLISH', 'channel-only'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'publish' command",
    )

    conn.write(commandFrame('PUBSUB', 'NUMPAT', 'extra'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'pubsub|numpat' command",
    )

    conn.write(commandFrame('PUBSUB'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'pubsub' command",
    )

    conn.write(commandFrame('PUBSUB', 'CHANNELS', '*', 'extra'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.",
    )

    conn.write(commandFrame('PUBSUB', 'HELP', 'extra'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'pubsub|help' command",
    )

    conn.write(commandFrame('PUBSUB', 'SHARDCHANNELS', '*', 'extra'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR unknown subcommand or wrong number of arguments for 'SHARDCHANNELS'. Try PUBSUB HELP.",
    )
  })

  test('handles empty unsubscribe commands and unknown PUBSUB subcommands', async () => {
    const conn = await connect()

    conn.write(commandFrame('UNSUBSCRIBE'))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'unsubscribe',
      null,
      0,
    ])

    conn.write(commandFrame('PUNSUBSCRIBE'))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'punsubscribe',
      null,
      0,
    ])

    conn.write(commandFrame('PUBSUB', 'BOGUS'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR unknown subcommand 'BOGUS'. Try PUBSUB HELP.",
    )
  })
})

function normalizeFrame(value: RespWireValue): RespWireValue {
  if (Buffer.isBuffer(value)) {
    return value.toString()
  }

  if (Array.isArray(value)) {
    return value.map(normalizeFrame)
  }

  return value
}
