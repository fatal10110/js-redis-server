import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { RedisClientType } from 'redis'
import { TestRunner, duplicateNodeRedisClient } from '../test-config'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Pub/Sub integration (node-redis, ${testRunner.getBackendName()})`, () => {
  // Every client is a duplicate() of one standalone client: a new connection to
  // the same server (mock/real), or to the same in-memory keyspace (socketless).
  let base: RedisClientType
  const clients: RedisClientType[] = []

  before(async () => {
    base = await testRunner.setupNodeRedisStandalone()
  })

  after(async () => {
    for (const client of clients) {
      client.destroy()
    }
    clients.length = 0
    await testRunner.cleanup()
  })

  test('delivers channel messages and reports channel subscribers', async () => {
    const publisher = await connect()
    const subscriber = await connect()
    const channel = `pubsub:${randomKey()}`
    const missingChannel = `${channel}:missing`

    let resolveMessage: (value: string) => void
    const received = new Promise<string>(resolve => {
      resolveMessage = resolve
    })
    await subscriber.subscribe(channel, (message: string, ch: string) => {
      if (ch === channel) resolveMessage(message)
    })

    assert.deepStrictEqual(
      await publisher.sendCommand([
        'PUBSUB',
        'NUMSUB',
        channel,
        missingChannel,
      ]),
      [channel, 1, missingChannel, 0],
    )
    assert.deepStrictEqual(
      await publisher.sendCommand(['PUBSUB', 'CHANNELS', channel]),
      [channel],
    )

    assert.strictEqual(await publisher.publish(channel, 'hello'), 1)
    assert.strictEqual(await received, 'hello')

    await subscriber.unsubscribe(channel)
    assert.strictEqual(await publisher.publish(channel, 'after'), 0)
  })

  test('delivers pattern messages and reports pattern subscribers', async () => {
    const publisher = await connect()
    const subscriber = await connect()
    const prefix = `pubsub-pattern:${randomKey()}`
    const pattern = `${prefix}:*`
    const channel = `${prefix}:updates`

    let resolveMessage: (value: string) => void
    const received = new Promise<string>(resolve => {
      resolveMessage = resolve
    })
    await subscriber.pSubscribe(pattern, (message: string, ch: string) => {
      if (ch === channel) resolveMessage(message)
    })

    assert.strictEqual(await publisher.sendCommand(['PUBSUB', 'NUMPAT']), 1)

    assert.strictEqual(await publisher.publish(channel, 'pattern-hit'), 1)
    assert.strictEqual(await received, 'pattern-hit')

    await subscriber.pUnsubscribe(pattern)
    assert.strictEqual(await publisher.sendCommand(['PUBSUB', 'NUMPAT']), 0)
  })

  test('reports empty shard Pub/Sub state', async () => {
    const client = await connect()
    const first = `pubsub-shard:${randomKey()}:1`
    const second = `pubsub-shard:${randomKey()}:2`

    assert.deepStrictEqual(
      await client.sendCommand(['PUBSUB', 'SHARDCHANNELS']),
      [],
    )
    assert.deepStrictEqual(
      await client.sendCommand(['PUBSUB', 'SHARDNUMSUB', first, second]),
      [first, 0, second, 0],
    )
  })

  async function connect(): Promise<RedisClientType> {
    const client = await duplicateNodeRedisClient(base)
    clients.push(client)
    return client
  }
})
