import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { ClientClosedError } from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../../src/client-mocks/node-redis-mock'

// #490: pub/sub calls on a closed client wait for the next connect(), as node-redis
// queues them, instead of rejecting (subscribe) or settling at once (unsubscribe).
// Each expectation was observed through real node-redis 6.0.0 against Redis 8.0.0,
// at RESP2 and RESP3, closed by quit() and by destroy(). One exception: real
// node-redis never settles a bare unsubscribe() (pUnsubscribe()) while patterns
// (channels) are still subscribed, open or closed, because it waits for an
// unsubscribe reply whose count is 0. subscribedThenClosed() subscribes both a
// channel and a pattern, so the bare-unsubscribe test below hangs on the real
// client; the facade resolves it after connect(), like the forms that name a
// channel.

type Close = 'quit' | 'destroy'
const CLOSES: readonly Close[] = ['quit', 'destroy']

async function close(client: NodeRedisMockClient, how: Close): Promise<void> {
  if (how === 'destroy') {
    client.destroy()
  } else {
    await client.quit()
  }
}

/** Waits until `got` holds `count` messages (delivery is asynchronous). */
async function received(got: unknown[], count: number): Promise<void> {
  for (let i = 0; i < 200 && got.length < count; i++) {
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

/** Tracks whether `promise` settled, and how. */
function track(promise: Promise<unknown>): {
  state: () => string
  promise: Promise<unknown>
} {
  let state = 'pending'
  const tracked = promise.then(
    value => {
      state = `resolved ${String(value)}`
    },
    (err: unknown) => {
      state = `rejected ${String(err)}`
    },
  )
  return { state: () => state, promise: tracked }
}

/** A few event-loop turns: long enough for any queued work to run. */
async function settle(): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 50))
}

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock: pub/sub on a closed client (RESP${RESP})`, () => {
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
      publisher: NodeRedisMockClient
    }> {
      const publisher = (await createNodeRedisMock({
        RESP,
      })) as NodeRedisMockClient
      const client = publisher.duplicate()
      openClients.push(publisher, client)
      await new Promise(resolve => setImmediate(resolve))
      return { client, publisher }
    }

    /** `client` subscribed to `a` and `p*`, recording into `got`, then closed. */
    async function subscribedThenClosed(how: Close): Promise<{
      client: NodeRedisMockClient
      publisher: NodeRedisMockClient
      got: string[]
    }> {
      const { client, publisher } = await makeClients()
      const got: string[] = []
      await client.subscribe('a', (message, channel) =>
        got.push(`a ${channel} ${message}`),
      )
      await client.pSubscribe('p*', (message, channel) =>
        got.push(`p* ${channel} ${message}`),
      )
      await close(client, how)
      return { client, publisher, got }
    }

    for (const how of CLOSES) {
      test(`${how}: subscribe() to a new channel waits for connect()`, async () => {
        const { client, publisher, got } = await subscribedThenClosed(how)
        const call = track(
          client.subscribe('b', (message, channel) =>
            got.push(`b ${channel} ${message}`),
          ),
        )
        await settle()
        assert.strictEqual(call.state(), 'pending')

        await client.connect()
        await call.promise
        assert.strictEqual(call.state(), 'resolved undefined')
        await publisher.publish('a', '1')
        await publisher.publish('b', '2')
        await publisher.publish('pq', '3')
        await received(got, 3)
        assert.deepStrictEqual(got, ['a a 1', 'b b 2', 'p* pq 3'])
      })

      test(`${how}: subscribe() to a channel with listeners resolves at once`, async () => {
        const { client, publisher, got } = await subscribedThenClosed(how)
        await client.subscribe('a', (message, channel) =>
          got.push(`a2 ${channel} ${message}`),
        )

        await client.connect()
        await publisher.publish('a', '1')
        await received(got, 2)
        assert.deepStrictEqual(got, ['a a 1', 'a2 a 1'])
      })

      test(`${how}: pSubscribe() waits for connect()`, async () => {
        const { client, publisher, got } = await subscribedThenClosed(how)
        const call = track(
          client.pSubscribe('q*', (message, channel) =>
            got.push(`q* ${channel} ${message}`),
          ),
        )
        await settle()
        assert.strictEqual(call.state(), 'pending')

        await client.connect()
        await call.promise
        assert.strictEqual(call.state(), 'resolved undefined')
        await publisher.publish('qq', '1')
        await received(got, 1)
        assert.deepStrictEqual(got, ['q* qq 1'])
      })

      test(`${how}: unsubscribe(channel) waits for connect(), then unsubscribes`, async () => {
        const { client, publisher, got } = await subscribedThenClosed(how)
        const call = track(client.unsubscribe('a'))
        const unknown = track(client.unsubscribe('nosuch'))
        await settle()
        assert.strictEqual(call.state(), 'pending')
        assert.strictEqual(unknown.state(), 'pending')

        await client.connect()
        await Promise.all([call.promise, unknown.promise])
        assert.strictEqual(call.state(), 'resolved undefined')
        assert.strictEqual(unknown.state(), 'resolved undefined')
        assert.strictEqual(await publisher.publish('a', '1'), 0)
        await publisher.publish('pq', '2')
        await received(got, 1)
        assert.deepStrictEqual(got, ['p* pq 2'])
      })

      test(`${how}: pUnsubscribe(pattern) waits for connect(), then unsubscribes`, async () => {
        const { client, publisher, got } = await subscribedThenClosed(how)
        const call = track(client.pUnsubscribe('p*'))
        const unknown = track(client.pUnsubscribe('nosuch*'))
        await settle()
        assert.strictEqual(call.state(), 'pending')
        assert.strictEqual(unknown.state(), 'pending')

        await client.connect()
        await Promise.all([call.promise, unknown.promise])
        assert.strictEqual(call.state(), 'resolved undefined')
        assert.strictEqual(unknown.state(), 'resolved undefined')
        assert.strictEqual(await publisher.publish('pq', '1'), 0)
        await publisher.publish('a', '2')
        await received(got, 1)
        assert.deepStrictEqual(got, ['a a 2'])
      })

      test(`${how}: a bare unsubscribe() / pUnsubscribe() resolves after connect()`, async () => {
        const { client, publisher, got } = await subscribedThenClosed(how)
        const channels = track(client.unsubscribe())
        await settle()
        assert.strictEqual(channels.state(), 'pending')

        await client.connect()
        await channels.promise
        assert.strictEqual(channels.state(), 'resolved undefined')
        assert.strictEqual(await publisher.publish('a', '1'), 0)
        await publisher.publish('pq', '2')
        await received(got, 1)
        assert.deepStrictEqual(got, ['p* pq 2'])

        await close(client, how)
        const patterns = track(client.pUnsubscribe())
        await settle()
        assert.strictEqual(patterns.state(), 'pending')
        await client.connect()
        await patterns.promise
        assert.strictEqual(patterns.state(), 'resolved undefined')
        assert.strictEqual(await publisher.publish('pq', '3'), 0)
      })
    }

    test('subscribe() then unsubscribe() while closed both settle after connect()', async () => {
      const { client, publisher } = await makeClients()
      client.destroy()
      const got: string[] = []
      const sub = track(client.subscribe('s1', message => got.push(message)))
      const unsub = track(client.unsubscribe('s1'))
      await settle()
      assert.strictEqual(sub.state(), 'pending')
      assert.strictEqual(unsub.state(), 'pending')

      await client.connect()
      await Promise.all([sub.promise, unsub.promise])
      assert.strictEqual(sub.state(), 'resolved undefined')
      assert.strictEqual(unsub.state(), 'resolved undefined')
      assert.strictEqual(await publisher.publish('s1', 'x'), 0)
      assert.deepStrictEqual(got, [])
    })

    test('a closed client still rejects every other command', async () => {
      const { client } = await makeClients()
      client.destroy()
      await assert.rejects(() => client.publish('a', 'x'), ClientClosedError)
      await assert.rejects(() => client.ping(), ClientClosedError)
    })
  })
}
