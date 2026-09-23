import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { TestRunner } from '../test-config'
import { commandFrame, randomKey } from '../utils'
import {
  RawRedisConnection,
  type RespWireValue,
  respMapGet,
  respNumber,
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

  test('allows sharded subscribed-mode commands and receives shard messages', async () => {
    const subscriber = await connect()
    const publisher = await connect()
    const channel = `raw-shard-pubsub:${randomKey()}`
    const missingChannel = `${channel}:missing`

    subscriber.write(commandFrame('SSUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'ssubscribe',
      channel,
      1,
    ])

    publisher.write(
      commandFrame('PUBSUB', 'SHARDNUMSUB', channel, missingChannel),
    )
    assert.deepStrictEqual(normalizeFrame(await publisher.readFrame()), [
      channel,
      1,
      missingChannel,
      0,
    ])

    publisher.write(commandFrame('PUBSUB', 'SHARDCHANNELS', channel))
    assert.deepStrictEqual(normalizeFrame(await publisher.readFrame()), [
      channel,
    ])

    subscriber.write(commandFrame('PING', 'hello'))
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'pong',
      'hello',
    ])

    subscriber.write(commandFrame('GET', 'blocked'))
    assert.match(
      respText(await subscriber.readFrame()),
      /^ERR Can't execute 'get': only .* allowed in this context$/,
    )

    publisher.write(commandFrame('SPUBLISH', channel, 'hello-shard'))
    assert.deepStrictEqual(await publisher.readFrame(), 1)
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'smessage',
      channel,
      'hello-shard',
    ])

    subscriber.write(commandFrame('SUNSUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'sunsubscribe',
      channel,
      0,
    ])

    subscriber.write(commandFrame('SET', 'after-shard-unsubscribe', 'ok'))
    assert.deepStrictEqual(await subscriber.readFrame(), 'OK')

    publisher.write(commandFrame('SPUBLISH', channel, 'after'))
    assert.deepStrictEqual(await publisher.readFrame(), 0)

    publisher.write(commandFrame('PUBSUB', 'SHARDCHANNELS'))
    assert.deepStrictEqual(await publisher.readFrame(), [])
  })

  test('keeps regular and shard subscription reply counts independent', async () => {
    const conn = await connect()
    const prefix = `raw-mixed-pubsub:${randomKey()}`
    const firstChannel = `${prefix}:channel:1`
    const secondChannel = `${prefix}:channel:2`
    const firstShardChannel = `${prefix}:shard:1`
    const secondShardChannel = `${prefix}:shard:2`
    const pattern = `${prefix}:pattern:*`

    conn.write(commandFrame('SUBSCRIBE', firstChannel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'subscribe',
      firstChannel,
      1,
    ])

    conn.write(commandFrame('SSUBSCRIBE', firstShardChannel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'ssubscribe',
      firstShardChannel,
      1,
    ])

    conn.write(commandFrame('SUBSCRIBE', secondChannel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'subscribe',
      secondChannel,
      2,
    ])

    conn.write(commandFrame('PSUBSCRIBE', pattern))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'psubscribe',
      pattern,
      3,
    ])

    conn.write(commandFrame('SSUBSCRIBE', secondShardChannel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'ssubscribe',
      secondShardChannel,
      2,
    ])

    conn.write(commandFrame('SUNSUBSCRIBE', firstShardChannel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'sunsubscribe',
      firstShardChannel,
      1,
    ])

    conn.write(commandFrame('UNSUBSCRIBE', firstChannel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'unsubscribe',
      firstChannel,
      2,
    ])

    conn.write(commandFrame('PUNSUBSCRIBE', pattern))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'punsubscribe',
      pattern,
      1,
    ])

    conn.write(commandFrame('SUNSUBSCRIBE'))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'sunsubscribe',
      secondShardChannel,
      0,
    ])

    conn.write(commandFrame('UNSUBSCRIBE'))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'unsubscribe',
      secondChannel,
      0,
    ])
  })

  test('RESP3 subscribed clients can run commands and use normal PING replies', async () => {
    const conn = await connect()
    const channel = `raw-resp3-pubsub:${randomKey()}`

    conn.write(commandFrame('HELLO', '3'))
    const hello = await conn.readFrame()
    assert.ok(hello instanceof Map)
    assert.strictEqual(respNumber(respMapGet(hello, 'proto')), 3)

    conn.write(commandFrame('SUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'subscribe',
      channel,
      1,
    ])

    conn.write(commandFrame('GET', 'missing'))
    assert.strictEqual(await conn.readFrame(), null)

    conn.write(commandFrame('PING'))
    assert.strictEqual(await conn.readFrame(), 'PONG')

    conn.write(commandFrame('PING', 'hello'))
    assert.strictEqual(respText(await conn.readFrame()), 'hello')

    conn.write(commandFrame('UNSUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'unsubscribe',
      channel,
      0,
    ])
  })

  test('RESET exits subscribed mode and removes all subscriptions', async () => {
    const subscriber = await connect()
    const publisher = await connect()
    const channel = `raw-reset:${randomKey()}`

    const patternPrefix = `raw-reset-pattern:${randomKey()}`
    const pattern = `${patternPrefix}:*`
    const patternMatch = `${patternPrefix}:matched`
    const shardChannel = `raw-reset-shard:${randomKey()}`

    subscriber.write(commandFrame('SUBSCRIBE', channel))
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'subscribe',
      channel,
      1,
    ])

    subscriber.write(commandFrame('PSUBSCRIBE', pattern))
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'psubscribe',
      pattern,
      2,
    ])

    subscriber.write(commandFrame('SSUBSCRIBE', shardChannel))
    assert.deepStrictEqual(normalizeFrame(await subscriber.readFrame()), [
      'ssubscribe',
      shardChannel,
      1,
    ])

    subscriber.write(commandFrame('RESET'))
    assert.deepStrictEqual(await subscriber.readFrame(), 'RESET')

    publisher.write(commandFrame('PUBLISH', channel, 'dropped'))
    assert.deepStrictEqual(await publisher.readFrame(), 0)

    publisher.write(commandFrame('PUBLISH', patternMatch, 'dropped'))
    assert.deepStrictEqual(await publisher.readFrame(), 0)

    publisher.write(commandFrame('SPUBLISH', shardChannel, 'dropped'))
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

  for (const protocol of [2, 3] as const) {
    // Every confirmation of a multi-target SUBSCRIBE is part of that command's
    // reply, so a command pipelined behind it in the same write must answer
    // after the last one, never between them (#455).
    test(`RESP${protocol}: multi-target subscribe confirmations precede a pipelined reply (#455)`, async () => {
      const conn = await connect()
      const prefix = `raw-order:${randomKey()}`
      const channels = [1, 2, 3].map(i => `${prefix}:channel:${i}`)
      const patterns = [1, 2].map(i => `${prefix}:pattern:${i}:*`)
      const shards = [1, 2].map(i => `${prefix}:shard:${i}`)
      await hello(conn, protocol)

      conn.write(
        Buffer.concat([
          commandFrame('SUBSCRIBE', ...channels),
          commandFrame('PING', 'after-subscribe'),
          commandFrame('PSUBSCRIBE', ...patterns),
          commandFrame('PING', 'after-psubscribe'),
          commandFrame('SSUBSCRIBE', ...shards),
          commandFrame('PING', 'after-ssubscribe'),
        ]),
      )

      const expected = [
        ...channels.map((channel, i) =>
          confirmationBytes(protocol, 'subscribe', channel, i + 1),
        ),
        subscribedPingBytes(protocol, 'after-subscribe'),
        ...patterns.map((pattern, i) =>
          confirmationBytes(
            protocol,
            'psubscribe',
            pattern,
            channels.length + i + 1,
          ),
        ),
        subscribedPingBytes(protocol, 'after-psubscribe'),
        ...shards.map((shard, i) =>
          confirmationBytes(protocol, 'ssubscribe', shard, i + 1),
        ),
        subscribedPingBytes(protocol, 'after-ssubscribe'),
      ]
      const actual: string[] = []
      for (let i = 0; i < expected.length; i++) {
        actual.push((await conn.readRawFrame()).toString())
      }
      assert.deepStrictEqual(actual, expected)
    })

    // Real Redis runs a queued SUBSCRIBE a b and appends both confirmations
    // inside EXEC's array, whose header still counts queued commands — so the
    // extra frame pushes the next item out of the array. Pinned byte for byte.
    test(`RESP${protocol}: EXEC embeds every confirmation of a queued multi-channel SUBSCRIBE`, async () => {
      const conn = await connect()
      const prefix = `raw-exec-subscribe:${randomKey()}`
      const [first, second] = [`${prefix}:1`, `${prefix}:2`]
      await hello(conn, protocol)

      conn.write(
        Buffer.concat([
          commandFrame('MULTI'),
          commandFrame('SUBSCRIBE', first, second),
          commandFrame('PING', 'queued'),
          commandFrame('EXEC'),
          commandFrame('PING', 'after-exec'),
        ]),
      )

      // +OK, +QUEUED, +QUEUED, EXEC's `*2` (which parses as the two
      // confirmations), the queued PING, and the trailing PING.
      const actual: string[] = []
      for (let i = 0; i < 6; i++) {
        actual.push((await conn.readRawFrame()).toString())
      }
      assert.strictEqual(
        actual.join(''),
        [
          '+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n',
          confirmationBytes(protocol, 'subscribe', first, 1),
          confirmationBytes(protocol, 'subscribe', second, 2),
          subscribedPingBytes(protocol, 'queued'),
          subscribedPingBytes(protocol, 'after-exec'),
        ].join(''),
      )
    })
  }

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

    conn.write(commandFrame('SSUBSCRIBE'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'ssubscribe' command",
    )

    conn.write(commandFrame('SPUBLISH', 'channel-only'))
    assert.strictEqual(
      respText(await conn.readFrame()),
      "ERR wrong number of arguments for 'spublish' command",
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

    conn.write(commandFrame('SUNSUBSCRIBE'))
    assert.deepStrictEqual(normalizeFrame(await conn.readFrame()), [
      'sunsubscribe',
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

async function hello(conn: RawRedisConnection, protocol: 2 | 3) {
  if (protocol === 3) {
    conn.write(commandFrame('HELLO', '3'))
    await conn.readFrame()
  }
}

function bulkBytes(value: string): string {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`
}

/** A subscribe-family confirmation: an array on RESP2, a push on RESP3. */
function confirmationBytes(
  protocol: 2 | 3,
  name: string,
  target: string,
  count: number,
): string {
  const header = protocol === 3 ? '>3\r\n' : '*3\r\n'
  return `${header}${bulkBytes(name)}${bulkBytes(target)}:${count}\r\n`
}

/** `PING <message>` on a subscribed connection. */
function subscribedPingBytes(protocol: 2 | 3, message: string): string {
  return protocol === 3
    ? bulkBytes(message)
    : `*2\r\n${bulkBytes('pong')}${bulkBytes(message)}`
}

function normalizeFrame(value: RespWireValue): RespWireValue {
  if (Buffer.isBuffer(value)) {
    return value.toString()
  }

  if (Array.isArray(value)) {
    return value.map(normalizeFrame)
  }

  return value
}
