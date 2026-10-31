import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { ClientClosedError } from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../../src/client-mocks/node-redis-mock'

// #600: connect() on a closed client re-subscribes the channels and patterns
// that still have listeners, before 'ready', whichever way it was closed. Each
// expectation was observed through real node-redis 6.0.0 against Redis 8.0.0
// and 7.2.4, at RESP2 and RESP3.

type Close = 'quit' | 'destroy' | 'disconnect'
const CLOSES: readonly Close[] = ['quit', 'destroy', 'disconnect']

async function close(client: NodeRedisMockClient, how: Close): Promise<void> {
  if (how === 'destroy') {
    client.destroy()
  } else {
    await client[how]()
  }
}

/** Waits until `got` holds `count` messages (delivery is asynchronous). */
async function received(got: unknown[], count: number): Promise<void> {
  for (let i = 0; i < 200 && got.length < count; i++) {
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock: re-subscribe on connect() (RESP${RESP})`, () => {
    const openClients: NodeRedisMockClient[] = []

    afterEach(async () => {
      while (openClients.length > 0) {
        try {
          await openClients.pop()?.quit()
        } catch (err) {
          if (!(err instanceof ClientClosedError)) {
            throw err
          }
        }
      }
    })

    async function makeClients(): Promise<{
      client: NodeRedisMockClient
      other: NodeRedisMockClient
    }> {
      const base = (await createNodeRedisMock({ RESP })) as NodeRedisMockClient
      const client = base.duplicate()
      openClients.push(base, client)
      // Let the constructors' 'connect'/'ready' pass.
      await new Promise(resolve => setImmediate(resolve))
      return { client, other: base }
    }

    for (const how of CLOSES) {
      test(`${how} → connect: channel and pattern listeners are subscribed again`, async () => {
        const { client, other } = await makeClients()
        const got: string[][] = []
        await client.subscribe('ch', (message, channel) =>
          got.push(['c', message, channel]),
        )
        await client.pSubscribe('p*', (message, channel) =>
          got.push(['p', message, channel]),
        )
        await client.subscribe('gone', (message, channel) =>
          got.push(['g', message, channel]),
        )
        await client.unsubscribe('gone')

        await close(client, how)
        assert.strictEqual(client.isOpen, false)
        const events: string[] = []
        client.on('connect', () => events.push('connect'))
        client.on('ready', () => events.push('ready'))
        await client.connect()

        assert.deepStrictEqual(events, ['connect', 'ready'])
        await other.publish('ch', 'm1')
        await other.publish('pxy', 'm2')
        // Unsubscribed before the close: not subscribed again.
        assert.strictEqual(await other.publish('gone', 'm3'), 0)
        await received(got, 2)
        assert.deepStrictEqual(got, [
          ['c', 'm1', 'ch'],
          ['p', 'm2', 'pxy'],
        ])
      })
    }

    test("the subscriptions are in place by 'ready'", async () => {
      const { client, other } = await makeClients()
      await client.subscribe('ch', () => {})
      await client.pSubscribe('p*', () => {})
      client.destroy()

      let atReady: Promise<unknown> | undefined
      let numPatAtReady: Promise<unknown> | undefined
      client.once('ready', () => {
        atReady = other.sendCommand(['PUBSUB', 'NUMSUB', 'ch'])
        numPatAtReady = other.sendCommand(['PUBSUB', 'NUMPAT'])
      })
      await client.connect()
      assert.deepStrictEqual(await atReady, ['ch', 1])
      assert.strictEqual(await numPatAtReady, 1)
    })

    test('every reconnect subscribes again', async () => {
      const { client, other } = await makeClients()
      const got: string[] = []
      await client.subscribe('ch', message => got.push(message))
      for (const [index, how] of CLOSES.entries()) {
        await close(client, how)
        await client.connect()
        await other.publish('ch', `m${index}`)
        await received(got, index + 1)
      }
      assert.deepStrictEqual(got, ['m0', 'm1', 'm2'])
    })

    test('nothing is subscribed again after an explicit unsubscribe', async () => {
      const { client, other } = await makeClients()
      const got: string[] = []
      await client.subscribe('ch', message => got.push(message))
      await client.pSubscribe('p*', message => got.push(message))
      await client.unsubscribe('ch')
      await client.pUnsubscribe('p*')
      await client.quit()
      await client.connect()

      assert.deepStrictEqual(
        await other.sendCommand(['PUBSUB', 'NUMSUB', 'ch']),
        ['ch', 0],
      )
      assert.strictEqual(await other.sendCommand(['PUBSUB', 'NUMPAT']), 0)
      assert.strictEqual(await other.publish('ch', 'm'), 0)
      // Commands still run on the re-opened client.
      assert.strictEqual(await client.set('k', 'v'), 'OK')
    })

    test('a listener added after the reconnect sends no second SUBSCRIBE', async () => {
      const { client, other } = await makeClients()
      const got: string[][] = []
      await client.subscribe('ch', message => got.push(['l1', message]))
      client.destroy()
      await client.connect()
      await client.subscribe('ch', message => got.push(['l2', message]))
      assert.deepStrictEqual(
        await other.sendCommand(['PUBSUB', 'NUMSUB', 'ch']),
        ['ch', 1],
      )
      await other.publish('ch', 'm')
      await received(got, 2)
      assert.deepStrictEqual(got, [
        ['l1', 'm'],
        ['l2', 'm'],
      ])
    })
  })
}
