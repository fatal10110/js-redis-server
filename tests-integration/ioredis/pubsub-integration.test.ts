import { after, before, describe, test } from 'node:test'
import assert from 'node:assert'
import { Redis } from 'ioredis'
import { TestRunner } from '../test-config'
import { randomKey } from '../utils'

const testRunner = new TestRunner()

describe(`Pub/Sub integration (${testRunner.getBackendName()})`, () => {
  let port: number
  const clients: Redis[] = []

  before(async () => {
    port = await testRunner.setupRawStandalone()
  })

  after(async () => {
    for (const client of clients) {
      client.disconnect()
    }
    clients.length = 0
    await testRunner.cleanup()
  })

  test('delivers channel messages and reports channel subscribers', async () => {
    const publisher = await connect()
    const subscriber = await connect()
    const channel = `pubsub:${randomKey()}`
    const missingChannel = `${channel}:missing`

    assert.strictEqual(await subscriber.subscribe(channel), 1)
    assert.deepStrictEqual(
      await publisher.call('PUBSUB', 'NUMSUB', channel, missingChannel),
      [channel, 1, missingChannel, 0],
    )
    assert.deepStrictEqual(
      await publisher.call('PUBSUB', 'CHANNELS', channel),
      [channel],
    )

    const message = waitForMessage(subscriber, channel)
    assert.strictEqual(await publisher.publish(channel, 'hello'), 1)
    assert.strictEqual(await message, 'hello')

    await subscriber.unsubscribe(channel)
    assert.strictEqual(await publisher.publish(channel, 'after'), 0)
  })

  test('delivers pattern messages and reports pattern subscribers', async () => {
    const publisher = await connect()
    const subscriber = await connect()
    const prefix = `pubsub-pattern:${randomKey()}`
    const pattern = `${prefix}:*`
    const channel = `${prefix}:updates`

    assert.strictEqual(await subscriber.psubscribe(pattern), 1)
    assert.strictEqual(await publisher.call('PUBSUB', 'NUMPAT'), 1)

    const message = waitForPatternMessage(subscriber, pattern, channel)
    assert.strictEqual(await publisher.publish(channel, 'pattern-hit'), 1)
    assert.strictEqual(await message, 'pattern-hit')

    await subscriber.punsubscribe(pattern)
    assert.strictEqual(await publisher.call('PUBSUB', 'NUMPAT'), 0)
  })

  test('reports empty shard Pub/Sub state', async () => {
    const client = await connect()
    const first = `pubsub-shard:${randomKey()}:1`
    const second = `pubsub-shard:${randomKey()}:2`

    assert.deepStrictEqual(await client.call('PUBSUB', 'SHARDCHANNELS'), [])
    assert.deepStrictEqual(
      await client.call('PUBSUB', 'SHARDNUMSUB', first, second),
      [first, 0, second, 0],
    )
  })

  async function connect(): Promise<Redis> {
    const client = new Redis({ host: '127.0.0.1', port, lazyConnect: true })
    await client.connect()
    clients.push(client)
    return client
  }
})

function waitForMessage(client: Redis, channel: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup()
      reject(new Error(`Timed out waiting for message on ${channel}`))
    }, 1000)

    const onMessage = (actualChannel: string, message: string) => {
      if (actualChannel !== channel) {
        return
      }

      cleanup()
      resolve(message)
    }

    const cleanup = () => {
      clearTimeout(timer)
      client.off('message', onMessage)
    }

    client.on('message', onMessage)
  })
}

function waitForPatternMessage(
  client: Redis,
  pattern: string,
  channel: string,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup()
      reject(new Error(`Timed out waiting for pattern message on ${channel}`))
    }, 1000)

    const onMessage = (
      actualPattern: string,
      actualChannel: string,
      message: string,
    ) => {
      if (actualPattern !== pattern || actualChannel !== channel) {
        return
      }

      cleanup()
      resolve(message)
    }

    const cleanup = () => {
      clearTimeout(timer)
      client.off('pmessage', onMessage)
    }

    client.on('pmessage', onMessage)
  })
}
