import { test, describe, afterEach } from 'node:test'
import assert from 'node:assert'
import { ClientClosedError, WatchError } from 'redis'
import {
  createNodeRedisMock,
  type NodeRedisMockClient,
} from '../../src/client-mocks/node-redis-mock'

// #440: connect() re-opens a closed client. Each expectation was observed
// through real node-redis 6.0.0 against Redis 8.0.0, at RESP2 and RESP3.

const EVENTS = ['connect', 'ready', 'end'] as const

function recordEvents(client: NodeRedisMockClient): string[] {
  const events: string[] = []
  for (const name of EVENTS) {
    client.on(name, () => events.push(name))
  }
  return events
}

for (const RESP of [2, 3] as const) {
  describe(`node-redis mock: connect() after a close (RESP${RESP})`, () => {
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

    async function makeClient(): Promise<NodeRedisMockClient> {
      const client = (await createNodeRedisMock({
        RESP,
      })) as NodeRedisMockClient
      openClients.push(client)
      // Let the constructor's 'connect'/'ready' pass before recording.
      await new Promise(resolve => setImmediate(resolve))
      return client
    }

    test("quit → connect → command → quit: serves commands, 'end' twice", async () => {
      const client = await makeClient()
      const events = recordEvents(client)
      await client.set('reopen:k', '1')
      assert.strictEqual(await client.quit(), 'OK')
      assert.strictEqual(await client.connect(), client)
      // Same server: the keyspace survived the close.
      assert.strictEqual(await client.get('reopen:k'), '1')
      assert.strictEqual(await client.quit(), 'OK')
      assert.deepStrictEqual(events, ['end', 'connect', 'ready', 'end'])
    })

    test('quit/connect repeatedly, then commands', async () => {
      const client = await makeClient()
      const events = recordEvents(client)
      for (let i = 0; i < 3; i++) {
        await client.quit()
        await client.connect()
      }
      assert.strictEqual(await client.set('reopen:x3', 'v'), 'OK')
      assert.strictEqual(await client.get('reopen:x3'), 'v')
      assert.deepStrictEqual(events, [
        ...['end', 'connect', 'ready'],
        ...['end', 'connect', 'ready'],
        ...['end', 'connect', 'ready'],
      ])
    })

    test('destroy() or disconnect(), then connect()', async () => {
      for (const close of [
        (c: NodeRedisMockClient) => c.destroy(),
        (c: NodeRedisMockClient) => c.disconnect(),
      ]) {
        const client = await makeClient()
        const events = recordEvents(client)
        await close(client)
        await client.connect()
        assert.strictEqual(await client.get('reopen:none'), null)
        assert.deepStrictEqual(events, ['end', 'connect', 'ready'])
      }
    })

    test('destroy(); connect(); get in one tick: the get runs on the new connection', async () => {
      const client = await makeClient()
      await client.set('reopen:tick', '1')
      client.destroy()
      const connected = client.connect()
      const get = client.get('reopen:tick')
      assert.strictEqual(await connected, client)
      assert.strictEqual(await get, '1')
    })

    test('connect() while quit() drains waits for the close, then re-opens', async () => {
      const client = await makeClient()
      const events = recordEvents(client)
      const quit = client.quit()
      const connected = client.connect()
      assert.strictEqual(await quit, 'OK')
      assert.strictEqual(await connected, client)
      assert.strictEqual(await client.get('reopen:none'), null)
      assert.deepStrictEqual(events, ['end', 'connect', 'ready'])
    })

    test('a command issued between quit() and connect() still rejects', async () => {
      const client = await makeClient()
      const quit = client.quit()
      const early = client.get('reopen:none')
      const connected = client.connect()
      await assert.rejects(early, ClientClosedError)
      assert.strictEqual(await quit, 'OK')
      assert.strictEqual(await connected, client)
    })

    test('the new connection uses the configured protocol and database', async () => {
      const client = await makeClient()
      await client.hSet('reopen:h', 'f', 'v')
      // A raw HELLO / SELECT moves only the old connection; node-redis
      // re-handshakes with its options (only select() is remembered).
      await client.sendCommand(['HELLO', RESP === 3 ? '2' : '3'])
      await client.sendCommand(['SELECT', '2'])
      await client.quit()
      await client.connect()
      const reply = await client.sendCommand(['HGETALL', 'reopen:h'])
      assert.deepStrictEqual(reply, RESP === 3 ? { f: 'v' } : ['f', 'v'])
    })

    test('a WATCH from before the reconnect fails the next transaction', async () => {
      const client = await makeClient()
      await client.watch('reopen:w')
      await client.quit()
      await client.connect()
      await assert.rejects(
        client.multi().set('reopen:w', '1').exec(),
        (err: unknown) => {
          assert.ok(err instanceof WatchError)
          assert.strictEqual(err.message, 'Client reconnected after WATCH')
          return true
        },
      )
      // The failed exec() consumed it.
      assert.deepStrictEqual(await client.multi().set('reopen:w', '1').exec(), [
        'OK',
      ])
    })

    test('a WATCH re-issued after the reconnect does not reset it; unwatch() does', async () => {
      const client = await makeClient()
      await client.watch('reopen:w2')
      await client.quit()
      await client.connect()
      await client.watch('reopen:w2')
      await assert.rejects(
        client.multi().set('reopen:w2', '1').exec(),
        WatchError,
      )

      await client.watch('reopen:w3')
      await client.unwatch()
      await client.quit()
      await client.connect()
      assert.deepStrictEqual(
        await client.multi().set('reopen:w3', '1').exec(),
        ['OK'],
      )
    })

    test('a duplicate keeps working across its parent re-opening', async () => {
      const client = await makeClient()
      const dup = await client.duplicate()
      openClients.push(dup)
      await client.quit()
      await client.connect()
      await dup.set('reopen:dup', '1')
      assert.strictEqual(await client.get('reopen:dup'), '1')
    })
  })
}
